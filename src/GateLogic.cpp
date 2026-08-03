#include "gate_compat.h"
#include "gate_db_interface.h"
#include "GateFormat.h"
#include "GateQueue.h"
#include <string>
#include <map>
#include <vector>
#include <memory>
#include <mutex>
#include <condition_variable>
#include <chrono>
#include <algorithm>
#include <atomic>
#include <deque>
#include <functional>
#include <set>
#include <epicsThread.h>
#include <epicsString.h>
#include <iostream>
#include <fstream>
#include <sstream>
#include <cstring>
#include <cstdio>
#include <epicsTimer.h>
#include <epicsStdio.h>
// caPutLog support (gateLoadPutLogText/gateLoadPutLogJson) is only compiled in when CAPUTLOG
// is set in RELEASE.local -- see src/Makefile, which turns that into -DWITH_CAPUTLOG. Without
// it, gate_load_put_log_text_cmd()/gate_load_put_log_json_cmd() (below, outside this #ifdef)
// still exist -- so the iocsh commands are always registered -- but just report the feature
// isn't built in, rather than the gateway failing to build at all.
#ifdef WITH_CAPUTLOG
#include <asTrapWrite.h>
#include <caPutLog.h>
#include <caPutLogTask.h>
#include <logClient.h>
#endif
#define PCRE2_CODE_UNIT_WIDTH 8
#include <pcre2.h>
#include <yajl_parse.h>

struct GateData {
    int dbrType;
    long count;
    std::vector<char> data;
    short status = 0;
    short severity = 0;
    epicsTimeStamp stamp = {0, 0};
};

// The Gateway's own eager upstream subscription (created on connect, independent of any
// downstream client's requested mask): includes DBE_PROPERTY so gchan->meta gets refreshed
// on any upstream limit/precision/units/enum-string change, regardless of whether any
// downstream client happens to subscribe with DBE_PROPERTY itself. gate_get_count() always
// serves gchan->meta (a single per-channel cache) no matter which masksub's event triggered
// delivery, so this is what keeps CTRL/GR-formatted responses accurate for e.g. a plain
// DBE_VALUE-only downstream monitor after a .HIHI change upstream.
static const unsigned int GATE_DEFAULT_MASK = DBE_VALUE | DBE_PROPERTY;

// Appends ":port" to each whitespace-separated address that doesn't already specify one.
// EPICS_CA_SERVER_PORT is process-wide and also governs the Gateway's own CAS listening
// port (set via the environment before the process starts) -- if the upstream client just
// inherited it, and the upstream IOC listens on a different port, ca_create_channel() would
// silently search on the wrong port and never connect. Per-address ":port" in
// EPICS_CA_ADDR_LIST overrides the default port for that address regardless.
static std::string with_port(const std::string& addr_list, int port) {
    if (port <= 0) return addr_list;
    std::istringstream iss(addr_list);
    std::string token, result;
    while (iss >> token) {
        if (!result.empty()) result += " ";
        result += token;
        if (token.find(':') == std::string::npos)
            result += ":" + std::to_string(port);
    }
    return result;
}

class GateClient {
public:
    GateClient(const std::string& name, const std::string& addr_list, bool auto_addr, int port) : name(name) {
        if (!ca_current_context()) ca_context_create(ca_enable_preemptive_callback);
        if (!addr_list.empty()) setenv("EPICS_CA_ADDR_LIST", with_port(addr_list, port).c_str(), 1);
        setenv("EPICS_CA_AUTO_ADDR_LIST", auto_addr ? "YES" : "NO", 1);
        ctx = ca_current_context();
    }
    ~GateClient() { ca_context_destroy(); }
    ca_client_context* getCtx() { return ctx; }
private:
    std::string name;
    ca_client_context* ctx;
};

// Forward-declared so GateChannel's constructor can register it as the connection
// callback; defined below (after GateChannel) since it operates on a complete GateChannel.
static void ca_conn_cb(struct connection_handler_args args);

struct GateChannel {
    std::string name;
    chid caChid;
    ASMEMBERPVT asMember;

    struct MaskSub {
        evid caEvid;
        unsigned int mask = 0;
        struct GateChannel* gchan = nullptr;
        std::shared_ptr<GateData> lastData;
        struct UserSub {
            void (*cb)(void*, void*, int, void*);
            void* user_arg;
            void* dbchan; // the real struct dbChannel* rsrv gave db_add_event(), NOT this MaskSub
            struct MaskSub* owner; // so gate_cancel_event() can find+erase itself
            // The subscribing downstream client's delivery queue -- events are handed off to it
            // rather than written straight to that client's socket from this (upstream) thread.
            // See GateClientQueue's comment. NULL only if rsrv somehow never started an event
            // context for the client, in which case delivery falls back to the old direct call.
            GateClientQueue* queue;
        };
        std::vector<UserSub*> userSubs;
        // Guards both lastData and userSubs together (not two separate mutexes): ca_event_cb()
        // ("record the new value, then deliver to whoever is currently subscribed") and
        // gate_add_event() ("register a subscriber, then deliver the current value to it if
        // any") must each run as one atomic step with respect to the other, or a subscriber
        // that registers in the gap between "set lastData" and "iterate userSubs" can be
        // delivered the same event twice -- once by ca_event_cb's loop (which already sees it
        // in userSubs) and once by gate_add_event's own immediate-delivery fallback (which now
        // sees the just-updated lastData). Seen in practice for freshly-created, non-default
        // mask subscriptions (e.g. a bare DBE_ALARM monitor) against a fast loopback upstream.
        std::mutex mtx;
    };
    std::map<unsigned int, std::shared_ptr<MaskSub>> maskSubs;
    std::mutex maskMutex;

    GateStaticMeta meta;
    std::mutex metaMutex;

    // Number of downstream dbChannels (one per client currently claiming this PV name)
    // currently open -- incremented/decremented 1:1 with dbChannel_create/dbChannelDelete
    // (see gate_channel_claim()/gate_delete_channel()). Drives the "active" (>0) vs
    // "inactive" (==0, still upstream-connected) statistics PVs, mirroring the old PCAS-based
    // Gateway's per-PV client-reference counting.
    std::atomic<int> downstreamRefs{0};

    // Signaled once the eager default (DBE_VALUE) subscription delivers its first event, so
    // gate_create_channel() can wait briefly for real data instead of handing back a channel
    // that will fail every get/monitor request until the upstream connection happens to land.
    std::mutex readyMutex;
    std::condition_variable readyCv;
    bool ready = false;

    GateChannel(const char* name, const char* upstream_name, const char* as_group) : name(name), caChid(NULL), asMember(NULL) {
        ca_create_channel(upstream_name, ca_conn_cb, this, 20, &caChid);
        long as_status = asAddMember(&asMember, as_group);
        if (as_status && as_status != S_asLib_asNotActive)
            errlogPrintf("GateChannel: asAddMember('%s', as_group='%s') failed (status=%ld)\n",
                         name, as_group, as_status);
    }
};

static std::map<std::string, std::shared_ptr<GateChannel>> channels;
static std::mutex channelsMutex;
static std::map<std::string, std::shared_ptr<GateClient>> clients;

// Hands one event for `owner` to its client's queue -- or, if that client somehow has no event
// context, falls back to delivering inline (the pre-queue behavior). `data` may be null for a
// statistics-PV update, whose value is recomputed at delivery time.
static void gate_deliver(GateClientQueue* queue, const void* owner,
                          void (*cb)(void*, void*, int, void*), void* user_arg, void* dbchan,
                          const std::shared_ptr<GateData>& data) {
    if (!queue) {
        cb(user_arg, dbchan, 0, data ? (void*)data.get() : NULL);
        return;
    }
    GateClientQueue::Entry e;
    e.data = data;
    e.cb = cb;
    e.user_arg = user_arg;
    e.dbchan = dbchan;
    e.owner = owner;
    e.bytes = data ? data->data.size() : 0;
    gate_queue_push(queue, std::move(e));
}

// --- Gateway statistics PVs (gateInitStats), comparable to the old PCAS-based Gateway's
// STAT_PVS (vctotal/pvtotal/connected/active/inactive; see CLAUDE.md) and RATE_STATS
// (clientEventRate/clientPostRate, here split into an upstream/downstream pair each for
// update-count and byte-volume -- see the RateCounters comment below). Unlike a real
// GateChannel, a GateStatEntry has no upstream chid at all -- its value is a live snapshot
// (or, for the rate entries, a periodically-refreshed cached one) computed from `channels`/
// rsrv's own client list, not cached CA event data. CAS_DIAGNOSTICS (a PCAS-library-native
// diagnostic with no rsrv equivalent), CONTROL_PVS (debug/report/quit flags -- iocsh already
// covers that role here) and HEARTBEAT_PV don't map onto anything meaningful in this
// architecture and are skipped, per CLAUDE.md.
struct GateStatEntry {
    std::string name;
    std::function<double()> getter;
    bool isDouble = false; // false: DBF_LONG counter; true: DBF_DOUBLE rate (Hz or bytes/sec)
    std::string units;     // only consulted for GR_*/CTRL_* requests of a rate entry
    short precision = 0;   // ditto
    ASMEMBERPVT asMember = NULL;
    double lastValue = -1;

    struct Sub {
        void (*cb)(void*, void*, int, void*);
        void* user_arg;
        void* dbchan;
        GateStatEntry* owner;
        GateClientQueue* queue; // subscribing client's delivery queue, as for MaskSub::UserSub
    };
    std::vector<Sub*> subs;
    std::mutex mtx;
};
static std::map<std::string, std::unique_ptr<GateStatEntry>> statEntries;
static std::mutex statEntriesMutex;
// Identifies a void* handle/event-id as belonging to a GateStatEntry/GateStatEntry::Sub rather
// than a GateChannel/GateChannel::MaskSub::UserSub -- simpler than adding a common tagged base
// class to structures that are otherwise plain and, in UserSub's case, aggregate-initialized.
static std::set<const void*> statHandles;
static std::set<const void*> statSubHandles;
static std::mutex statHandlesMutex;

static bool is_stat_handle(const void* h) {
    std::lock_guard<std::mutex> lock(statHandlesMutex);
    return statHandles.count(h) != 0;
}
static bool is_stat_sub(const void* h) {
    std::lock_guard<std::mutex> lock(statHandlesMutex);
    return statSubHandles.count(h) != 0;
}

// Defined below (after count_channels()/sum_channels(), which its callers -- and this file's
// other statistics-PV plumbing -- build on); forward-declared so RateStatsTimer::expire() can
// call it.
static void notify_stats_changed();

// Comparable to the old PCAS-based Gateway's RATE_STATS (clientEventRate/clientPostRate),
// but split explicitly by direction (upstream: gateway<-IOC, downstream: gateway->client) and
// paired update-count/byte-volume rates rather than just one event rate each: an "update" is
// one event delivered (an upstream monitor callback landing, or one downstream client
// receiving one posted value), a "volume" is the size in bytes of that event's raw
// DBR_TIME_<native> payload (0 for a connection-state-change event, which has no payload).
// `record()` is called from the hot event-delivery paths (ca_event_cb/ca_meta_cb/ca_conn_cb/
// gate_add_event) and just accumulates atomically; `tick()` is called once per
// RATE_STATS_INTERVAL by RateStatsTimer::expire() to turn the accumulated deltas into a
// per-second rate, mirroring the old gateRateStatsTimer's delta-count/delta-time computation.
struct RateCounters {
    std::atomic<uint64_t> events{0};
    std::atomic<uint64_t> bytes{0};
    uint64_t lastEvents = 0;
    uint64_t lastBytes = 0;
    std::atomic<double> eventRate{0.0};
    std::atomic<double> byteRate{0.0};

    void record(size_t n) {
        events.fetch_add(1, std::memory_order_relaxed);
        bytes.fetch_add(n, std::memory_order_relaxed);
    }
    void tick(double dtSeconds) {
        uint64_t e = events.load(std::memory_order_relaxed);
        uint64_t b = bytes.load(std::memory_order_relaxed);
        eventRate.store((double)(e - lastEvents) / dtSeconds, std::memory_order_relaxed);
        byteRate.store((double)(b - lastBytes) / dtSeconds, std::memory_order_relaxed);
        lastEvents = e;
        lastBytes = b;
    }
};
static RateCounters g_upstreamRate;   // events/bytes received from upstream IOCs
static RateCounters g_downstreamRate; // events/bytes posted to downstream clients

static const double RATE_STATS_INTERVAL = 2.0; // seconds, matches old code's typical period

// Periodically converts the RateCounters' raw accumulators into per-second rates and pushes a
// statistics-PV update (notify_stats_changed() also runs off of real channel/connection
// activity, but rates must keep refreshing -- decaying towards 0 -- even when nothing else
// happens to trigger it). Only instantiated once gateInitStats actually registers a rate
// entry (see gate_init_stats_cmd()), so a gateway that never enables statistics never pays for
// a background timer thread.
class RateStatsTimer : public epicsTimerNotify {
public:
    explicit RateStatsTimer(epicsTimerQueueActive& queue)
        : timer(queue.createTimer()), prev(epicsTime::getCurrent()) {
        timer.start(*this, RATE_STATS_INTERVAL);
    }
    ~RateStatsTimer() { timer.destroy(); }
    expireStatus expire(const epicsTime& currentTime) override {
        double dt = currentTime - prev;
        prev = currentTime;
        if (dt > 0) {
            g_upstreamRate.tick(dt);
            g_downstreamRate.tick(dt);
            notify_stats_changed();
        }
        return expireStatus(epicsTimerNotify::restart, RATE_STATS_INTERVAL);
    }
private:
    epicsTimer& timer;
    epicsTime prev;
};
static RateStatsTimer* g_rateStatsTimer = nullptr;
static std::once_flag g_rateStatsTimerOnce;

template <typename Pred>
static long count_channels(Pred pred) {
    std::lock_guard<std::mutex> lock(channelsMutex);
    long n = 0;
    for (auto& kv : channels) if (pred(kv.second.get())) n++;
    return n;
}

template <typename F>
static long sum_channels(F f) {
    std::lock_guard<std::mutex> lock(channelsMutex);
    long total = 0;
    for (auto& kv : channels) total += f(kv.second.get());
    return total;
}

// Re-evaluates every registered statistics PV and delivers a monitor update to whichever of
// its subscribers changed value. Called after anything that can move one of these counters:
// a channel is created (gate_create_channel_for_client), its downstream reference count
// changes (gate_channel_claim/gate_delete_channel), or its upstream connection state changes
// (ca_conn_cb). Not called on VC (rsrv TCP client) connect/disconnect, since that happens in
// vendored, unmodified rsrv code we don't hook into -- vctotal is still always correct on a
// fresh get, just not proactively pushed to monitors the instant it changes.
static void notify_stats_changed() {
    std::vector<GateStatEntry*> changed;
    {
        std::lock_guard<std::mutex> lock(statEntriesMutex);
        for (auto& kv : statEntries) {
            GateStatEntry* e = kv.second.get();
            double v = e->getter();
            if (v != e->lastValue) {
                e->lastValue = v;
                changed.push_back(e);
            }
        }
    }
    for (auto* e : changed) {
        std::vector<GateStatEntry::Sub*> subsCopy;
        {
            std::lock_guard<std::mutex> lock(e->mtx);
            subsCopy = e->subs;
        }
        for (auto* sub : subsCopy)
            gate_deliver(sub->queue, sub, sub->cb, sub->user_arg, sub->dbchan, nullptr);
    }
}

// --- caPutLog integration (gateLoadPutLogText / gateLoadPutLogJson) ---
//
// This whole section (through gate_trap_write_cb() below) only exists when built with
// WITH_CAPUTLOG (see the #include block near the top of this file, and src/Makefile).
#ifdef WITH_CAPUTLOG
//
// caPutLog (CAPUTLOG in RELEASE.local) is the real EPICS put-logging module. Its normal entry
// points -- caPutLogInit() (traditional text), caPutJsonLogInit()/CaPutJsonLogTask::initialize()
// (JSON) -- all internally call caPutLogAsInit(), which registers *its own*
// asTrapWriteListener (caPutLogAs(), in caPutLogAs.c) that builds a LOGDATA via dbGetField()
// against the trapped channel's real dbAddr/rset. That crashes here: gateShim.c's dbGetRset()
// always returns NULL (there is no real database/records behind a GateChannel), and
// caPutLogAs.c's caPutLogActualArraySize() dereferences it unconditionally for any array PV.
//
// This also rules out caPutLogTaskStart()/caPutLogTaskSend() (the real burst-coalescing
// background sender) and caPutLogDataCalloc()/caPutLogDataFree(), even though none of those
// three touch dbAccess themselves: they all operate on a single process-wide free list
// (logDataFreeList, a `static` in caPutLogAs.c) that is *only* initialized inside
// caPutLogAsInit() -- there is no other exported entry point that sets it up. Calling
// caPutLogDataCalloc()/caPutLogTaskSend() without first calling caPutLogAsInit() crashes
// immediately (freeListCalloc() against an uninitialized/NULL free-list pointer -- confirmed
// with gdb: SIGSEGV in freeListCalloc(), called from gate_trap_write_cb()'s first put).
//
// So neither sink can lean on any of caPutLog's runtime code paths -- only its plain data
// definitions (LOGDATA/VALUE, caPutLogTask.h) and config enum (caPutLog.h) are used here. Both
// sinks are our own formatter (text matching the old Gateway/caPutLogTask.c wire format,
// json matching caPutJsonLogTask.h's documented message schema) sent over the real transport
// Base itself provides (logClientCreate()/logClientSend(), <logClient.h> -- the same transport
// caPutLogClientSend() uses internally). Neither sink has real burst-coalescing (that needs
// caPutLogTask.c's own background thread, which needs the free list); "log all" and
// "log all, no filter" are therefore equivalent here -- both just mean "no on-change filter".
//
// We register our own asTrapWriteListener (gate_trap_write_cb below) and build the LOGDATA
// ourselves from data we already have -- the channel's own cached upstream value (for "old")
// and the asTrapWriteMessage's own dbrType/no_elements/data (for "new" -- exactly the client's
// put payload, already host-byte-order, since write_action()/write_notify_action() in
// camessage.c call asTrapWriteWithData() right after caNetConvert()).

static asTrapWriteId g_trapWriteListenerId = NULL;
static std::once_flag g_trapWriteListenerOnce;

struct PutLogSink {
    std::vector<std::pair<logClientId, std::string>> clients; // id, address (address for dedup)
    std::mutex mutex;
    std::atomic<int> config{caPutLogNone}; // caPutLogNone/OnChange/All/AllNoFilter
};
static PutLogSink g_textSink;
static PutLogSink g_jsonSink;

// gateLoadPutLogFile: a drop-in replacement for the old PCAS-based Gateway's own "-putlog
// <file>" local log file (gateResources::putLog(), master:src/gateResources.cc) -- a plain
// FILE*, not a PutLogSink (no network destinations, no on-change config: the old file sink
// always logged every trapped write unconditionally, so this does too).
struct PutLogFileSink {
    FILE* fp = nullptr;
    std::atomic<bool> enabled{false};
    std::mutex mutex;
};
static PutLogFileSink g_fileSink;

// Same 12-entry table as caPutLogAs.c's caPutLogMaxArraySize() (indexed by a *real*
// dbFldTypes.h value) -- reimplemented locally rather than calling that function, so this
// file's caPutLog usage is visibly limited to plain data/config definitions, never runtime
// code with the free-list dependency described above.
static long gate_putlog_max_array_size(short realDbf) {
    static const long table[] = {
        MAX_ARRAY_SIZE_BYTES / MAX_STRING_SIZE,     /* STRING */
        MAX_ARRAY_SIZE_BYTES / sizeof(epicsInt8),   /* CHAR */
        MAX_ARRAY_SIZE_BYTES / sizeof(epicsUInt8),  /* UCHAR */
        MAX_ARRAY_SIZE_BYTES / sizeof(epicsInt16),  /* SHORT */
        MAX_ARRAY_SIZE_BYTES / sizeof(epicsUInt16), /* USHORT */
        MAX_ARRAY_SIZE_BYTES / sizeof(epicsInt32),  /* LONG */
        MAX_ARRAY_SIZE_BYTES / sizeof(epicsUInt32), /* ULONG */
#ifdef DBR_INT64
        MAX_ARRAY_SIZE_BYTES / sizeof(epicsInt64),  /* INT64 */
        MAX_ARRAY_SIZE_BYTES / sizeof(epicsUInt64), /* UINT64 */
#endif
        MAX_ARRAY_SIZE_BYTES / sizeof(epicsFloat32),/* FLOAT */
        MAX_ARRAY_SIZE_BYTES / sizeof(epicsFloat64),/* DOUBLE */
        MAX_ARRAY_SIZE_BYTES / sizeof(epicsUInt16)  /* ENUM */
    };
    long count = sizeof(table) / sizeof(table[0]);
    if (realDbf < 0 || realDbf >= count) return 1;
    return table[realDbf];
}

// Converts `count` elements of `srcCanonical`-typed raw data (portable G_S_DBF_* numbering,
// e.g. from gate_dbr_to_dbf()) into `dst`, typed `dstRealDbf` (a *real*, per-version
// dbFldTypes.h value -- see gate_compat.h) -- reusing Base's own dbGetConvertRoutine table,
// exactly as GateFormat.cpp's gate_format_response() does for downstream responses. Returns
// the number of elements actually converted (capped at dstMaxElems).
static long gate_convert_for_putlog(int srcCanonical, const void* srcRaw, long count,
                                     int dstRealDbf, void* dst, long dstMaxElems) {
    long available = count > 0 ? count : 1;
    long n = available > dstMaxElems ? dstMaxElems : available;
    GETCONVERTFUNC convert = dbGetConvertRoutine[gate_dbf_to_real_dbf(srcCanonical)][dstRealDbf];
    if (!convert) return 0;
    struct dbAddr addr;
    memset(&addr, 0, sizeof(addr));
    addr.field_type = (short)gate_dbf_to_real_dbf(srcCanonical);
    addr.pfield = (void*)srcRaw;
    addr.no_elements = available;
    convert(&addr, dst, n, available, 0);
    return n;
}

// Byte-for-byte equality of the first `count` elements of two same-typed VALUEs -- used for
// the "on change" filter (caPutLogOnChange). The real caPutLogTask.c has its own per-type
// val_equal(), but it's a private `static` function, not an exported symbol.
static bool gate_putlog_values_equal(const VALUE& a, const VALUE& b, int canonical, long count) {
    long n = count > 0 ? count : 1;
    switch (canonical) {
        case G_S_DBF_STRING: return strncmp(a.v_string, b.v_string, sizeof(a.v_string)) == 0;
        case G_S_DBF_CHAR:   return memcmp(a.a_int8, b.a_int8, n * sizeof(epicsInt8)) == 0;
        case G_S_DBF_SHORT:  return memcmp(a.a_int16, b.a_int16, n * sizeof(epicsInt16)) == 0;
        case G_S_DBF_LONG:   return memcmp(a.a_int32, b.a_int32, n * sizeof(epicsInt32)) == 0;
        case G_S_DBF_FLOAT:  return memcmp(a.a_float, b.a_float, n * sizeof(epicsFloat32)) == 0;
        case G_S_DBF_DOUBLE: return memcmp(a.a_double, b.a_double, n * sizeof(epicsFloat64)) == 0;
        case G_S_DBF_ENUM:   return memcmp(a.a_uint16, b.a_uint16, n * sizeof(epicsUInt16)) == 0;
        default: return false;
    }
}

// Renders one element (index `idx`) of `v` (typed `canonical`) in either style. `idx==0` reads
// identically for a scalar or an array (VALUE's a_TYPE[0] aliases v_TYPE, same union offset).
// Traditional style matches both caPutLogTask.c's val_to_string() (network sinks) and the old
// Gateway's own VALUE_to_string() (master:src/gateResources.cc, file sink) -- the two happen to
// agree on every numeric format specifier; only the string case differs (quoted vs. bare), see
// gate_append_value()'s caller-supplied `quoteStrings`.
enum class ValueStyle { Traditional, Json };

static void gate_append_scalar(std::string& out, const VALUE& v, int canonical, long idx,
                                ValueStyle style, bool quoteStrings) {
    char buf[64];
    switch (canonical) {
        case G_S_DBF_STRING: {
            const char* s = v.a_string[idx];
            if (style == ValueStyle::Json || quoteStrings) {
                out += '"';
                for (const char* p = s; *p; ++p) {
                    if (*p == '"' || *p == '\\') out += '\\';
                    out += *p;
                }
                out += '"';
            } else {
                out += s;
            }
            break;
        }
        case G_S_DBF_CHAR:
            snprintf(buf, sizeof(buf), "%d", (int)v.a_int8[idx]); out += buf; break;
        case G_S_DBF_SHORT:
            snprintf(buf, sizeof(buf), "%d", (int)v.a_int16[idx]); out += buf; break;
        case G_S_DBF_LONG:
            snprintf(buf, sizeof(buf), "%ld", (long)v.a_int32[idx]); out += buf; break;
        case G_S_DBF_FLOAT:
            snprintf(buf, sizeof(buf), "%g", v.a_float[idx]); out += buf; break;
        case G_S_DBF_DOUBLE:
            snprintf(buf, sizeof(buf), "%g", v.a_double[idx]); out += buf; break;
        case G_S_DBF_ENUM:
            snprintf(buf, sizeof(buf), "%u", (unsigned)v.a_uint16[idx]); out += buf; break;
        default:
            if (style == ValueStyle::Json) out += "null";
            break;
    }
}

// `asArray` brackets the rendering ("[e0, e1, ...]", matching Python's str(list) exactly, which
// is also valid JSON) regardless of `count` -- driven by the *channel's* dimensionality
// (NELM > 1), not this particular event's count, matching how the old Gateway's
// gdd_to_log_string() branched on the gdd's dimension() rather than its current element count.
static void gate_append_value(std::string& out, const VALUE& v, int canonical, long count,
                               bool asArray, ValueStyle style, bool quoteStrings = false) {
    long n = count > 0 ? count : 1;
    if (asArray) {
        out += '[';
        for (long i = 0; i < n; ++i) {
            if (i) out += ", ";
            gate_append_scalar(out, v, canonical, i, style, quoteStrings);
        }
        out += ']';
    } else {
        gate_append_scalar(out, v, canonical, 0, style, quoteStrings);
    }
}

// Formats `pld` (traditional or JSON, depending on `sink`) and sends it to every configured
// destination of that sink. `canonical` is the portable G_S_DBF_* type both
// old_value/new_value.value are stored as; `asArray` brackets multi-element values (see
// gate_append_value()).
static void gate_send_put_log(PutLogSink& sink, bool json, const LOGDATA& pld, int canonical, bool asArray) {
    std::string msg;
    if (json) {
        char datebuf[16], timebuf[16];
        epicsTimeToStrftime(datebuf, sizeof(datebuf), "%d-%m-%Y", &pld.new_value.time);
        epicsTimeToStrftime(timebuf, sizeof(timebuf), "%H:%M:%S", &pld.new_value.time);
        msg = "{\"date\":\"";
        msg += datebuf;
        msg += "\",\"time\":\"";
        msg += timebuf;
        msg += "\",\"host\":\"";
        msg += pld.hostid;
        msg += "\",\"user\":\"";
        msg += pld.userid;
        msg += "\",\"pv\":\"";
        msg += pld.pv_name;
        msg += "\",\"new\":";
        gate_append_value(msg, pld.new_value.value, canonical, pld.new_log_size, asArray, ValueStyle::Json);
        msg += ",\"old\":";
        gate_append_value(msg, pld.old_value, canonical, pld.old_log_size, asArray, ValueStyle::Json);
        msg += "}\n";
    } else {
        // "<time> <hostid> <userid> <pv_name> new=<value> old=<value>", matching
        // caPutLogTask.c's log_msg()/do_log() field order exactly.
        char timebuf[64];
        epicsTimeToStrftime(timebuf, sizeof(timebuf), "%d-%b-%y %H:%M:%S", &pld.new_value.time);
        msg = timebuf;
        msg += ' ';
        msg += pld.hostid;
        msg += ' ';
        msg += pld.userid;
        msg += ' ';
        msg += pld.pv_name;
        msg += " new=";
        gate_append_value(msg, pld.new_value.value, canonical, pld.new_log_size, asArray, ValueStyle::Traditional, true);
        msg += " old=";
        gate_append_value(msg, pld.old_value, canonical, pld.old_log_size, asArray, ValueStyle::Traditional, true);
        msg += "\n";
    }

    std::lock_guard<std::mutex> lock(sink.mutex);
    for (auto& c : sink.clients) {
        // logClientSend() alone only batches (periodic flush every 5s, or on cache overflow,
        // per its own doc comment) -- flush immediately so a put-log entry isn't lost to that
        // delay if the gateway happens to restart shortly after.
        logClientSend(c.first, msg.c_str());
        logClientFlush(c.first);
    }
}

// gateLoadPutLogFile sink: a drop-in replacement for the old PCAS-based Gateway's own
// "-putlog <file>" local log file (master:src/gateResources.cc's putLog(), called from
// gateVc.cc's write()/writeNotify()) -- same line format exactly: "<time> <user>@<host>
// <pv_name> <new value> old=<old value>\n", time as "%b %d %H:%M:%S" (no year, matching
// master:src/gateResources.cc's timeStamp()), values unquoted even for strings (matching its
// VALUE_to_string()), "old=?" when no old value was available at all (matching its own
// "acOldVal[0]='?'" fallback when old_value was NULL) rather than a zeroed value.
static void gate_send_put_log_file(const LOGDATA& pld, bool hasOldValue, int canonical, bool asArray) {
    char timebuf[32];
    epicsTimeToStrftime(timebuf, sizeof(timebuf), "%b %d %H:%M:%S", &pld.new_value.time);

    std::string msg = timebuf;
    msg += ' ';
    msg += pld.userid;
    msg += '@';
    msg += pld.hostid;
    msg += ' ';
    msg += pld.pv_name;
    msg += ' ';
    gate_append_value(msg, pld.new_value.value, canonical, pld.new_log_size, asArray, ValueStyle::Traditional);
    msg += " old=";
    if (hasOldValue)
        gate_append_value(msg, pld.old_value, canonical, pld.old_log_size, asArray, ValueStyle::Traditional);
    else
        msg += '?';
    msg += '\n';

    std::lock_guard<std::mutex> lock(g_fileSink.mutex);
    if (g_fileSink.fp) {
        fputs(msg.c_str(), g_fileSink.fp);
        fflush(g_fileSink.fp);
    }
}

// Carries a LOGDATA plus the extra bit gate_trap_write_cb() needs across its before/after
// calls that LOGDATA itself has no field for: whether a real "old" value was actually cached
// (vs. just zero-initialized -- see gate_send_put_log_file()'s "old=?" convention).
struct PutLogEntry {
    LOGDATA data;
    bool hasOldValue = false;
};

// The asTrapWriteListener registered once (lazily) by gateLoadPutLogText/gateLoadPutLogJson/
// gateLoadPutLogFile. Called twice per trapped write (real, unmodified asLib/rsrv machinery --
// see the architecture comment above): once before (after==0, to snapshot the "old" value) and
// once after (after==1, once pmessage->data/dbrType/no_elements -- the "new" value -- are valid
// and the write has actually been forwarded upstream).
static void gate_trap_write_cb(asTrapWriteMessage* pmessage, int after) {
    // asTrapWriteMessage::serverSpecific is a plain `void*` on Base 3.15 and a
    // `struct dbChannel*` on 7.0, so it has to be cast explicitly to compile on both (rsrv
    // sets it to pciu->dbch either way -- see write_action()/write_notify_action()).
    struct dbChannel* dbch = (struct dbChannel*)pmessage->serverSpecific;
    if (!dbch) return;
    void* handle = gate_handle_from_dbchannel(dbch);
    if (!handle || is_stat_handle(handle)) return; // statistics PVs are read-only, never trapped
    GateChannel* gchan = (GateChannel*)handle;
    int nativeCaType = gate_native_ca_type(gchan);
    int canonical = gate_dbr_to_dbf(nativeCaType >= 0 ? nativeCaType : DBR_DOUBLE);
    int realDbf = gate_dbf_to_real_dbf(canonical);

    if (!after) {
        PutLogEntry* entry = new PutLogEntry();
        pmessage->userPvt = entry;
        LOGDATA* pld = &entry->data;

        epicsSnprintf(pld->userid, MAX_USERID_SIZE, "%s", pmessage->userid ? pmessage->userid : "");
        epicsSnprintf(pld->hostid, MAX_HOSTID_SIZE, "%s", pmessage->hostid ? pmessage->hostid : "");
        epicsSnprintf(pld->pv_name, PVNAME_STRINGSZ, "%s", dbch->name ? dbch->name : "");
        pld->type = (short)realDbf;
        // Bracket multi-element values based on the channel's own dimensionality (NELM > 1),
        // not this event's current count, matching the old Gateway's gdd_to_log_string()
        // (which branched on the gdd's dimension(), not its element count).
        pld->is_array = gate_native_count(gchan) > 1;

        long maxElems = gate_putlog_max_array_size(pld->type);
        long oldCount = 0;
        // Best-effort "old" value: our own cached copy of the channel's most recent upstream
        // value -- there is no real record behind a GateChannel to read a pre-put snapshot
        // from, unlike caPutLogAs.c's dbGetField() (see architecture comment above).
        std::shared_ptr<GateChannel::MaskSub> msub;
        {
            std::lock_guard<std::mutex> lock(gchan->maskMutex);
            auto it = gchan->maskSubs.find(GATE_DEFAULT_MASK);
            if (it != gchan->maskSubs.end()) msub = it->second;
        }
        std::shared_ptr<GateData> data;
        if (msub) {
            std::lock_guard<std::mutex> lock(msub->mtx);
            data = msub->lastData;
        }
        if (data) {
            int srcCanonical = gate_dbr_to_dbf(data->dbrType);
            const void* rawValue = (const void*)((const char*)data->data.data() + dbr_size[data->dbrType] - dbr_value_size[data->dbrType]);
            oldCount = gate_convert_for_putlog(srcCanonical, rawValue, data->count, pld->type, &pld->old_value, maxElems);
        }
        pld->old_size = pld->old_log_size = (int)oldCount;
        entry->hasOldValue = (data != nullptr);
        return;
    }

    std::unique_ptr<PutLogEntry> entry((PutLogEntry*)pmessage->userPvt);
    if (!entry) return;
    LOGDATA* pld = &entry->data;

    int textConfig = g_textSink.config.load(std::memory_order_relaxed);
    int jsonConfig = g_jsonSink.config.load(std::memory_order_relaxed);
    bool wantText = textConfig != caPutLogNone;
    bool wantJson = jsonConfig != caPutLogNone;
    bool wantFile = g_fileSink.enabled.load(std::memory_order_relaxed);
    if (!wantText && !wantJson && !wantFile) return;

    long maxElems = gate_putlog_max_array_size(pld->type);
    // The put's own wire type (mp->m_dataType, forwarded verbatim into the trap message) --
    // not necessarily the channel's native type, e.g. a client may PUT a DBR_STRING to a
    // DBR_DOUBLE channel. Always converted into pld->type (the channel's native type) so
    // old_value and new_value.value share one consistent representation.
    int newSrcCanonical = gate_dbr_to_dbf(pmessage->dbrType);
    long newCount = gate_convert_for_putlog(newSrcCanonical, pmessage->data, pmessage->no_elements,
                                             pld->type, &pld->new_value.value, maxElems);
    pld->new_size = pld->new_log_size = (int)newCount;
    epicsTimeGetCurrent(&pld->new_value.time);

    bool unchanged = gate_putlog_values_equal(pld->old_value, pld->new_value.value, canonical, newCount);
    if (wantText && !(textConfig == caPutLogOnChange && unchanged))
        gate_send_put_log(g_textSink, false, *pld, canonical, pld->is_array);
    if (wantJson && !(jsonConfig == caPutLogOnChange && unchanged))
        gate_send_put_log(g_jsonSink, true, *pld, canonical, pld->is_array);
    if (wantFile)
        gate_send_put_log_file(*pld, entry->hasOldValue, canonical, pld->is_array);
}
#endif // WITH_CAPUTLOG

struct Route {
    pcre2_code* re;
    std::string client_name;
    std::string as_group;
    std::string target; // optional PCRE2 $1-style rewrite of the upstream name; empty = same name
    // A DENY route (legacy pvlist DENY/DENY FROM, not ASG/UAG/HAG): controls whether a channel
    // can be claimed at all, not read/write rights on an existing one. deny=false for ordinary
    // ALLOW/ALIAS routes.
    bool deny = false;
    std::vector<std::string> deny_hosts; // deny==true && empty => blanket; else DENY FROM list
};
static std::vector<Route> routes;
static std::mutex routesMutex;

// Metadata refresh is always async (ca_array_get_callback, never a blocking ca_pend_io call):
// this is triggered from CA callbacks (ca_conn_cb/ca_event_cb) that themselves run on libca's
// single callback-dispatch thread even in preemptive-callback mode -- blocking that thread on
// a wait that itself depends on that same thread processing more I/O just times out (tried
// this, it silently left gchan->meta stale/zeroed rather than working). So a DBE_PROPERTY
// event's downstream delivery is deferred: instead of notifying subscribers immediately with
// (possibly stale) metadata, ca_event_cb stashes the event and hands off to ca_meta_cb, which
// delivers it once the refresh actually lands.
struct MetaRefreshCtx {
    GateChannel* gchan;
    GateChannel::MaskSub* msub;   // whose subscribers to notify once refreshed; null = no pending notify (e.g. initial connect-time refresh)
    std::shared_ptr<GateData> data;
};

static void ca_meta_cb(struct event_handler_args args) {
    std::unique_ptr<MetaRefreshCtx> ctx((MetaRefreshCtx*)args.usr);
    if (args.status == ECA_NORMAL) {
        int dbf = gate_dbr_to_dbf(args.type);
        std::lock_guard<std::mutex> lock(ctx->gchan->metaMutex);
        gate_format_extract_meta(dbf, args.dbr, &ctx->gchan->meta);
    }
    if (ctx->msub && ctx->data) {
        std::lock_guard<std::mutex> lock(ctx->msub->mtx);
        size_t sz = ctx->data->data.size();
        for (auto usub : ctx->msub->userSubs) {
            // This is the deferred delivery of the same upstream event ca_event_cb() already
            // counted as received -- only count it as a downstream post here, once per
            // subscriber it actually reaches.
            g_downstreamRate.record(sz);
            // See ca_event_cb()'s comment: hand ctx->data itself over as the "db_field_log"
            // argument so gate_get_count() serves this specific (deferred) event's own data.
            // The queue holds a shared_ptr to it, keeping it alive until actually delivered.
            gate_deliver(usub->queue, usub, usub->cb, usub->user_arg, usub->dbchan, ctx->data);
        }
    }
}

static void refresh_static_meta(GateChannel* gchan, chid ch, GateChannel::MaskSub* pending_msub, std::shared_ptr<GateData> pending_data) {
    short native = ca_field_type(ch);
    short ctrl_type = (short)(native + 28); // DBR_CTRL_STRING(28) + native basic type (0..6)
    auto* ctx = new MetaRefreshCtx{gchan, pending_msub, pending_data};
    ca_array_get_callback(ctrl_type, 1, ch, ca_meta_cb, ctx);
    ca_flush_io();
}

// Upstream subscriptions are always created with type (DBR_TIME_STRING + native field type),
// regardless of the event mask or of what format downstream clients eventually request: this
// is what gives gate_get_count() real status/severity/timestamp to work with (see GateFormat.h).
static void ca_event_cb(struct event_handler_args args) {
    GateChannel::MaskSub* msub = (GateChannel::MaskSub*)args.usr;
    if (args.status != ECA_NORMAL) return;
    auto newData = std::make_shared<GateData>();
    newData->dbrType = args.type;
    newData->count = args.count;
    size_t sz = dbr_size_n(args.type, args.count);
    newData->data.assign((char*)args.dbr, (char*)args.dbr + sz);
    // One upstream update received, regardless of how many downstream clients it's fanned out
    // to below (that fan-out is what g_downstreamRate counts instead).
    g_upstreamRate.record(sz);
    int dbf = gate_dbr_to_dbf(args.type);
    gate_format_extract_time(dbf, args.dbr, &newData->status, &newData->severity, &newData->stamp);
    bool isProperty = (msub->mask & DBE_PROPERTY) != 0;
    {
        // See MaskSub::mtx's comment: setting lastData and delivering to the current
        // userSubs must happen as one atomic step (skipped here for a DBE_PROPERTY mask,
        // whose delivery is deferred to ca_meta_cb instead -- see below).
        std::lock_guard<std::mutex> lock(msub->mtx);
        msub->lastData = newData;
        if (!isProperty) {
            for (auto usub : msub->userSubs) {
                g_downstreamRate.record(sz);
                // usub->cb is really rsrv's read_reply(pArg, dbChannel*, eventsRemaining,
                // db_field_log*): pass the real dbChannel* (not msub), eventsRemaining=0 (so
                // its `if (!eventsRemaining) cas_send_bs_msg(...)` actually flushes this event
                // promptly), and newData itself as the 4th ("db_field_log") argument -- read_reply
                // forwards it unchanged into dbChannel_get_count()/gate_get_count(), which must
                // use THIS event's own data instead of whatever's cached for the *default* mask
                // (GATE_DEFAULT_MASK): a DBE_ALARM/DBE_LOG-only event (e.g. a pure severity
                // transition with no accompanying value change) never touches the default
                // DBE_VALUE|DBE_PROPERTY subscription's own cache at all, so falling back to it
                // silently re-delivers stale status/severity for every mask other than the
                // default one. Handing it to the client's own queue (rather than calling
                // read_reply() straight from this upstream thread) is what keeps one slow
                // client from stalling this whole upstream circuit -- see GateClientQueue.
                gate_deliver(usub->queue, usub, usub->cb, usub->user_arg, usub->dbchan, newData);
            }
        }
    }
    if (msub->mask == GATE_DEFAULT_MASK) {
        GateChannel* gchan = msub->gchan;
        std::lock_guard<std::mutex> lock(gchan->readyMutex);
        if (!gchan->ready) {
            gchan->ready = true;
            gchan->readyCv.notify_all();
        }
    }
    if (isProperty) {
        // A property change may have altered limits/precision/units/enum strings upstream;
        // defer this event's delivery until the refreshed metadata actually lands (see
        // refresh_static_meta()'s comment) instead of notifying subscribers immediately with
        // what could still be the old cached values.
        refresh_static_meta(msub->gchan, msub->gchan->caChid, msub, newData);
    }
}

// Returns the MaskSub for `select`, creating the upstream CA subscription (type
// DBR_TIME_STRING + native) on first use. Used both for downstream-requested masks
// (gate_add_event) and for the eager default subscription created on connect (ca_conn_cb),
// so a plain get always has cached data even with no downstream monitor yet.
static std::shared_ptr<GateChannel::MaskSub> get_or_create_mask_sub(GateChannel* gchan, chid ch, unsigned int select) {
    std::lock_guard<std::mutex> lock(gchan->maskMutex);
    auto it = gchan->maskSubs.find(select);
    if (it != gchan->maskSubs.end()) return it->second;
    auto msub = std::make_shared<GateChannel::MaskSub>();
    msub->mask = select;
    msub->gchan = gchan;
    gchan->maskSubs[select] = msub;
    short native = ca_field_type(ch);
    short time_type = (short)(native + 14); // DBR_TIME_STRING(14) + native basic type (0..6)
    // Count 0 = autosize: the real upstream IOC then reports each event's actual
    // current element count (e.g. a waveform's NORD), not the fixed native max
    // (NELM). Requesting a fixed count instead pins every delivered GateData::count
    // at NELM (CA pads non-autosize subscriptions to the requested count with
    // zeros), permanently masking the real, current count from everything
    // downstream that reads it.
    ca_create_subscription(time_type, 0, ch, select, ca_event_cb, msub.get(), &msub->caEvid);
    ca_flush_io();
    return msub;
}

static void ca_conn_cb(struct connection_handler_args args) {
    GateChannel* gchan = (GateChannel*)ca_puser(args.chid);
    if (args.op == CA_OP_CONN_UP) {
        get_or_create_mask_sub(gchan, args.chid, GATE_DEFAULT_MASK);
        refresh_static_meta(gchan, args.chid, nullptr, nullptr);
    }
    // A connection-state transition is itself an upstream-originated event (no value payload,
    // hence size 0 -- it doesn't contribute to the upstream volume rate, only the event rate).
    g_upstreamRate.record(0);
    // Either direction can move the "connected"/"active"/"inactive" statistics PVs.
    notify_stats_changed();
}

static ca_client_context* g_ca_ctx = NULL;

// Every entry point below can run on whichever rsrv server thread happens to be handling
// the client request (e.g. "CAS-UDP"/"CAS-TCP"/per-client threads) -- not the main thread
// that gate_init() created the CA client context on. CA contexts are per-thread, so any
// ca_*() call from a thread with no attached context silently fails (upstream channel never
// connects, put/subscribe never happens) rather than crashing. Call this first.
static void ensure_ca_context() {
    if (!ca_current_context() && g_ca_ctx) ca_attach_context(g_ca_ctx);
}

extern "C" {
void gate_init(void) {
    if (!ca_current_context()) ca_context_create(ca_enable_preemptive_callback);
    g_ca_ctx = ca_current_context();
}


struct RouteMatch {
    bool has_allow = false;
    bool denied = false; // blanket DENY, or DENY FROM matching the given hostname
    const Route* allow_route = nullptr; // valid only while routesMutex is held
};

// Scans *all* routes matching `name` (not first-match) and combines them: a blanket DENY
// (or a DENY FROM whose host list contains `hostname`) sets denied=true regardless of any
// ALLOW/ALIAS match, modeling this codebase's only usage of "EVALUATION ORDER ALLOW, DENY"
// (deny overrides allow when both match a name). `hostname` NULL means "unknown" (the
// anonymous UDP search path): DENY FROM never matches in that case, since we can't yet tell
// which client is asking -- only a blanket DENY can hide a name at search time.
static RouteMatch match_routes_locked(const char* name, const char* hostname) {
    RouteMatch result;
    for (const auto& route : routes) {
        pcre2_match_data* match_data = pcre2_match_data_create_from_pattern(route.re, NULL);
        int rc = pcre2_match(route.re, (PCRE2_SPTR)name, strlen(name), 0, 0, match_data, NULL);
        pcre2_match_data_free(match_data);
        if (rc < 0) continue;
        if (route.deny) {
            if (route.deny_hosts.empty()) {
                result.denied = true;
            } else if (hostname) {
                for (const auto& h : route.deny_hosts) {
                    if (h == hostname) { result.denied = true; break; }
                }
            }
        } else {
            result.has_allow = true;
            result.allow_route = &route;
        }
    }
    return result;
}

// Finds or creates the GateChannel for `name`, but never blocks: used both by the fast
// UDP-search-reply path (gate_channel_exists(), which only needs a yes/no route match, always
// with hostname=NULL) and as the first half of the TCP claim-channel path (see
// gate_wait_channel_ready() below), where gateShim.c passes the requesting client's
// self-reported hostname so a DENY FROM route can hide an otherwise-cached channel from just
// that client. Routes are re-evaluated even on a channels-map cache hit -- a cache hit must
// not bypass the per-client DENY FROM check.
void* gate_create_channel_for_client(const char* name, const char* hostname) {
    ensure_ca_context();

    // Statistics PVs (gateInitStats) are gateway-internal: they bypass route/DENY matching
    // entirely (they're not upstream-routed PVs at all), which also means the UDP
    // search-reply path (gate_channel_exists()) and dbNameToAddr()/dbChannel_create() all see
    // them for free, with no vendored-file or gateShim.c dispatch changes needed.
    {
        std::lock_guard<std::mutex> lock(statEntriesMutex);
        auto sit = statEntries.find(name);
        if (sit != statEntries.end()) return (void*)sit->second.get();
    }

    GateChannel* result = nullptr;
    bool created = false;
    {
        std::lock_guard<std::mutex> lock(channelsMutex);
        std::lock_guard<std::mutex> rlock(routesMutex);
        RouteMatch m = match_routes_locked(name, hostname);
        if (!m.has_allow || m.denied) return NULL;

        auto it = channels.find(name);
        if (it != channels.end()) return (void*)it->second.get();

        const Route& route = *m.allow_route;
        std::string upstream_name = name;
        if (!route.target.empty()) {
            PCRE2_UCHAR outbuf[512];
            PCRE2_SIZE outlen = sizeof(outbuf) / sizeof(PCRE2_UCHAR);
            int rc2 = pcre2_substitute(route.re, (PCRE2_SPTR)name, strlen(name), 0,
                                       0, NULL, NULL,
                                       (PCRE2_SPTR)route.target.c_str(), route.target.size(),
                                       outbuf, &outlen);
            if (rc2 >= 0) {
                upstream_name.assign((const char*)outbuf, outlen);
            } else {
                errlogPrintf("gate_create_channel: pcre2_substitute failed (%d) for '%s' -> '%s', using unmodified name\n",
                             rc2, name, route.target.c_str());
            }
        }
        auto gchan = std::make_shared<GateChannel>(name, upstream_name.c_str(), route.as_group.c_str());
        channels[name] = gchan;
        result = gchan.get();
        created = true;
    }
    ca_flush_io();
    // notify_stats_changed() re-locks channelsMutex itself (via count_channels()) -- must run
    // only after the lock_guards above have gone out of scope, not while still held.
    if (created) notify_stats_changed(); // a new channel changes "pvtotal"
    return (void*)result;
}

void* gate_create_channel(const char* name) {
    return gate_create_channel_for_client(name, NULL);
}

int gate_channel_exists(const char* name) {
    return gate_create_channel(name) != NULL;
}

// Waits briefly for the eager DBE_VALUE subscription's first event, so a channel a
// downstream client is actually claiming/connecting to (as opposed to just being searched
// for) doesn't come back "connected" while still guaranteed to fail every get/monitor
// request until the async upstream connect+subscribe happens to land later -- the common
// case in short-lived test fixtures, where every test starts against a cold gateway. Gives
// up after a few seconds regardless (e.g. the upstream PV doesn't exist) rather than
// blocking this thread forever. Deliberately separate from gate_create_channel() itself:
// that also runs on the UDP search-reply path (gate_channel_exists()), which must stay fast
// since the client's own connection-establishment doesn't wait on it at all.
void gate_wait_channel_ready(void* handle) {
    if (is_stat_handle(handle)) return; // no async upstream connect to wait for
    GateChannel* gchan = (GateChannel*)handle;
    std::unique_lock<std::mutex> lock(gchan->readyMutex);
    // 10s to match this test suite's own standard wait budget (cond.wait(timeout=10.0)),
    // rather than a shorter, arbitrary value that starts timing out under real system load.
    gchan->readyCv.wait_for(lock, std::chrono::seconds(10), [&]{ return gchan->ready; });
}

// Real (CA-wire-ordered, 0..6) native type/element count of the upstream channel, once
// connected -- used by gateShim.c's dbNameToAddr()/dbChannel_create() so a downstream client
// is told the record's real type/array size instead of a hardcoded DBR_DOUBLE/1, which broke
// every non-double and every array/waveform PV (clients that promote their request type from
// the reported native type, e.g. pyepics, would ask for the wrong representation entirely).
// Statistics PVs (gateInitStats) are always a scalar DBF_LONG counter or (for the rate
// entries) a DBF_DOUBLE -- there's no upstream chid to ask.
int gate_native_ca_type(void* handle) {
    if (is_stat_handle(handle)) return ((GateStatEntry*)handle)->isDouble ? DBR_DOUBLE : DBR_LONG;
    GateChannel* gchan = (GateChannel*)handle;
    if (!gchan->caChid || ca_state(gchan->caChid) != cs_conn) return -1;
    return ca_field_type(gchan->caChid);
}

long gate_native_count(void* handle) {
    if (is_stat_handle(handle)) return 1;
    GateChannel* gchan = (GateChannel*)handle;
    if (!gchan->caChid || ca_state(gchan->caChid) != cs_conn) return 1;
    long count = ca_element_count(gchan->caChid);
    return count > 0 ? count : 1;
}

// Called by gateShim.c's dbChannelDelete(), 1:1 paired with dbChannel_create()'s
// gate_channel_claim() -- see that function's comment. A no-op for statistics PVs, which
// aren't claim-counted (they're never removed and don't drive their own "active" state).
void gate_delete_channel(void* channel) {
    if (is_stat_handle(channel)) return;
    GateChannel* gchan = (GateChannel*)channel;
    gchan->downstreamRefs.fetch_sub(1, std::memory_order_relaxed);
    notify_stats_changed();
}

// Marks a real channel as claimed by one more downstream dbChannel (see gate_delete_channel()
// above for the release side). A no-op for statistics PVs.
void gate_channel_claim(void* handle) {
    if (is_stat_handle(handle)) return;
    GateChannel* gchan = (GateChannel*)handle;
    gchan->downstreamRefs.fetch_add(1, std::memory_order_relaxed);
    notify_stats_changed();
}

void* gate_get_as_member(void* handle) {
    if (is_stat_handle(handle)) return (void*)((GateStatEntry*)handle)->asMember;
    return (void*)((GateChannel*)handle)->asMember;
}

// `pfl` mirrors real EPICS Base's db_field_log mechanism: when a monitor event's own
// read_reply() call flows through here (see ca_event_cb()/ca_meta_cb()'s comments), it's a
// GateData* snapshot of exactly the event that triggered this delivery, and must be used
// as-is -- falling back to "whatever's cached for the default mask" (as this function used to
// do unconditionally) silently serves stale/wrong status-severity for any event delivered on
// a non-default mask that never itself touches the default DBE_VALUE|DBE_PROPERTY
// subscription's own cache (e.g. a DBE_ALARM-only severity transition with no value change).
// `pfl` is NULL for plain (non-monitor) gets (e.g. read_notify_action's call, camessage.c),
// which keeps the previous "prefer the eager default subscription" behavior.
int gate_get_count(void* handle, int buffer_type, void* pbuffer, long* nRequest, void* pfl) {
    if (is_stat_handle(handle)) {
        // No cached event data at all -- a statistics PV's value is always computed fresh,
        // live, from current gateway state (or, for a rate entry, from the last periodic
        // RateStatsTimer tick), whether this call came from a plain get or from a monitor
        // delivery (pfl is always NULL for these; see gate_add_event()/notify_stats_changed()
        // below).
        GateStatEntry* stat = (GateStatEntry*)handle;
        double v = stat->getter();
        epicsTimeStamp stamp;
        epicsTimeGetCurrent(&stamp);
        GateStaticMeta meta;
        meta.precision = stat->precision;
        strncpy(meta.units, stat->units.c_str(), sizeof(meta.units) - 1);
        if (stat->isDouble) {
            epicsFloat64 value = v;
            return gate_format_response(G_S_DBF_DOUBLE, buffer_type, pbuffer, nRequest, 1,
                                         0, 0, stamp, &value, meta);
        }
        epicsInt32 value = (epicsInt32)v;
        return gate_format_response(G_S_DBF_LONG, buffer_type, pbuffer, nRequest, 1,
                                     0, 0, stamp, &value, meta);
    }
    GateChannel* gchan = (GateChannel*)handle;
    std::shared_ptr<GateData> data;
    if (pfl) {
        // Non-owning: the caller (ca_event_cb/ca_meta_cb) keeps its own shared_ptr alive for
        // the full, synchronous duration of this call chain.
        data = std::shared_ptr<GateData>((GateData*)pfl, [](GateData*){});
    } else {
        std::shared_ptr<GateChannel::MaskSub> msub;
        {
            std::lock_guard<std::mutex> lock(gchan->maskMutex);
            if (gchan->maskSubs.empty()) return -1;
            // Prefer the eager default subscription for plain gets, since a
            // DBE_LOG/DBE_ALARM-only subscription may have staler data.
            auto it = gchan->maskSubs.find(GATE_DEFAULT_MASK);
            if (it == gchan->maskSubs.end()) it = gchan->maskSubs.begin();
            msub = it->second;
        }
        {
            std::lock_guard<std::mutex> lock(msub->mtx);
            if (!msub->lastData) return -1;
            data = msub->lastData;
        }
    }
    GateStaticMeta meta;
    {
        std::lock_guard<std::mutex> lock(gchan->metaMutex);
        meta = gchan->meta;
    }
    int dbf = gate_dbr_to_dbf(data->dbrType);
    const void* rawValue = (const void*)((const char*)data->data.data() + dbr_size[data->dbrType] - dbr_value_size[data->dbrType]);
    return gate_format_response(dbf, buffer_type, pbuffer, nRequest, data->count,
                                 data->status, data->severity, data->stamp, rawValue, meta);
}

int gate_put(void* channel, int src_type, const void* psrc, long no_elements) {
    if (is_stat_handle(channel)) return -1; // statistics PVs are read-only
    ensure_ca_context();
    GateChannel* gchan = (GateChannel*)channel;
    int rc = ca_array_put(src_type, no_elements, gchan->caChid, psrc);
    ca_flush_io();
    return rc;
}

struct PutNotifyCtx {
    gate_put_notify_callback* cb;
    void* user_arg;
};

// Runs on the upstream client's own libca callback-dispatch thread (a per-CA-context
// thread, separate from whatever rsrv server thread called gate_put_notify below) --
// i.e. genuinely once the upstream IOC has acknowledged the write, not merely once
// ca_array_put_callback()'s request was queued.
static void ca_put_notify_cb(struct event_handler_args args) {
    std::unique_ptr<PutNotifyCtx> ctx((PutNotifyCtx*)args.usr);
    ctx->cb(ctx->user_arg, args.status == ECA_NORMAL ? 0 : -1);
}

// Asynchronous put-with-completion-notification: forwards to the upstream IOC via
// ca_array_put_callback() and invokes `cb` only once that upstream put-notify actually
// completes (or fails), rather than replying to the downstream client as soon as the
// write is merely queued. Used for downstream CA_PROTO_WRITE_NOTIFY (wait=True / put
// callback) requests, which real CA clients rely on to mean "the write has actually
// taken effect", not just "was accepted for delivery".
void gate_put_notify(void* channel, int src_type, const void* psrc, long no_elements,
                      gate_put_notify_callback* cb, void* user_arg) {
    if (is_stat_handle(channel)) { cb(user_arg, -1); return; } // statistics PVs are read-only
    ensure_ca_context();
    GateChannel* gchan = (GateChannel*)channel;
    auto* ctx = new PutNotifyCtx{cb, user_arg};
    int rc = ca_array_put_callback(src_type, no_elements, gchan->caChid, psrc, ca_put_notify_cb, ctx);
    if (rc != ECA_NORMAL) {
        delete ctx;
        cb(user_arg, -1);
        return;
    }
    ca_flush_io();
}

void* gate_add_event(void* channel, gate_event_callback* cb, void* user_arg, void* real_dbchan,
                      unsigned int select, void* queue) {
    GateClientQueue* q = (GateClientQueue*)queue;
    if (is_stat_handle(channel)) {
        GateStatEntry* stat = (GateStatEntry*)channel;
        auto* sub = new GateStatEntry::Sub{(void (*)(void*, void*, int, void*))cb, user_arg, real_dbchan, stat, q};
        {
            std::lock_guard<std::mutex> lock(statHandlesMutex);
            statSubHandles.insert(sub);
        }
        {
            std::lock_guard<std::mutex> lock(stat->mtx);
            stat->subs.push_back(sub);
        }
        // Same convention as a real channel's first subscription delivery: every new monitor
        // gets one immediate value, regardless of any other subscriber.
        gate_deliver(sub->queue, sub, sub->cb, sub->user_arg, sub->dbchan, nullptr);
        return (void*)sub;
    }
    ensure_ca_context();
    GateChannel* gchan = (GateChannel*)channel;
    auto msub = get_or_create_mask_sub(gchan, gchan->caChid, select);
    auto* usub = new GateChannel::MaskSub::UserSub{(void (*)(void*, void*, int, void*))cb, user_arg, real_dbchan, msub.get(), q};
    // Every new CA monitor subscription gets one immediate delivery of the channel's current
    // value, regardless of whether other subscribers already exist for the same upstream mask
    // (e.g. this downstream client happens to request the same mask as the eager default
    // subscription set up in ca_conn_cb). Without this, a subscriber joining after that first
    // upstream event already fired and got cached would never see an initial value at all --
    // it'd be one event permanently short, since ca_event_cb() only notifies subscribers
    // present in userSubs *at delivery time*.
    //
    // Registering usub and snapshotting lastData must happen as one atomic step (same mutex
    // ca_event_cb() uses for "set lastData, deliver to current userSubs") -- otherwise a
    // concurrent event landing between the two would be delivered twice: once by
    // ca_event_cb()'s loop, which would already see usub in userSubs, and once by the
    // immediate-delivery fallback below, which would see that same event's data as lastData.
    std::shared_ptr<GateData> data;
    {
        std::lock_guard<std::mutex> lock(msub->mtx);
        msub->userSubs.push_back(usub);
        data = msub->lastData;
    }
    if (data) {
        // The initial delivery on subscribe is a real downstream post too.
        g_downstreamRate.record(data->data.size());
        // See ca_event_cb()'s comment: hand data itself over as the "db_field_log" argument.
        gate_deliver(usub->queue, usub, usub->cb, usub->user_arg, usub->dbchan, data);
    }
    return (void*)usub;
}

// A no-op here (as it was before) would leave a dangling GateChannel::MaskSub::UserSub in
// msub->userSubs after the downstream client unsubscribes/disconnects and rsrv frees its own
// per-subscription state -- the next upstream event delivered to that channel would then call
// back into freed memory (usub->cb/usub->user_arg), corrupting the process. Must actually
// remove and free the entry.
// Each branch must, in this order: (1) unregister so no *new* event can be queued for this
// subscription, (2) drop whatever is already sitting in the client's queue for it and wait out
// any delivery of it currently in progress on the reader thread, and only then (3) free it.
// Skipping (2) would let the reader dereference a freed UserSub/Sub -- rsrv cancels
// subscriptions (event_cancel_reply, destroyAllChannels) from the client's own TCP thread,
// concurrently with that same client's reader thread draining the queue.
void gate_cancel_event(void* event_id) {
    if (!event_id) return;
    if (is_stat_sub(event_id)) {
        auto* sub = (GateStatEntry::Sub*)event_id;
        GateStatEntry* stat = sub->owner;
        {
            std::lock_guard<std::mutex> lock(stat->mtx);
            auto& v = stat->subs;
            v.erase(std::remove(v.begin(), v.end(), sub), v.end());
        }
        {
            std::lock_guard<std::mutex> lock(statHandlesMutex);
            statSubHandles.erase(sub);
        }
        if (sub->queue) gate_queue_purge_owner(sub->queue, sub);
        delete sub;
        return;
    }
    auto* usub = (GateChannel::MaskSub::UserSub*)event_id;
    GateChannel::MaskSub* msub = usub->owner;
    if (msub) {
        std::lock_guard<std::mutex> lock(msub->mtx);
        auto& v = msub->userSubs;
        v.erase(std::remove(v.begin(), v.end(), usub), v.end());
    }
    if (usub->queue) gate_queue_purge_owner(usub->queue, usub);
    delete usub;
}

void gate_create_client_cmd(const char* name, const char* addr_list, int auto_addr, int port) {
    std::lock_guard<std::mutex> lock(channelsMutex);
    clients[name] = std::make_shared<GateClient>(name, addr_list ? addr_list : "", auto_addr != 0, port);
    errlogPrintf("gate_create_client_cmd: client '%s' created (addr_list='%s' auto_addr=%d port=%d)\n",
                 name, addr_list ? addr_list : "", auto_addr, port);
}

// Registers the gateInitStats iocsh command's statistics PVs under "<prefix>:" -- see the
// GateStatEntry/RateCounters comments above. Comparable to the old PCAS-based Gateway's
// STAT_PVS (vctotal/pvtotal/connected/active/inactive) and RATE_STATS (clientEventRate/
// clientPostRate, here as an upstream/downstream x update/volume 2x2); CAS_DIAGNOSTICS
// (a PCAS-library-native diagnostic with no rsrv equivalent), CONTROL_PVS (report/reload/quit
// flags -- iocsh commands already cover that role here) and HEARTBEAT_PV (dead code even in
// the old implementation -- never actually updated) have no sensible equivalent in this
// architecture and are skipped.
void gate_init_stats_cmd(const char* prefix, const char* as_group) {
    if (!prefix || !prefix[0]) {
        errlogPrintf("gate_init_stats_cmd: empty prefix, ignoring\n");
        return;
    }
    std::string ag = (as_group && as_group[0]) ? as_group : "DEFAULT";
    auto add = [&](const char* suffix, std::function<double()> getter,
                    bool isDouble = false, const char* units = "", short precision = 0) {
        std::string full = std::string(prefix) + ":" + suffix;
        // Not std::make_unique: that is C++14, and CI builds this with -std=c++11.
        std::unique_ptr<GateStatEntry> entry(new GateStatEntry());
        entry->name = full;
        entry->getter = std::move(getter);
        entry->isDouble = isDouble;
        entry->units = units;
        entry->precision = precision;
        long as_status = asAddMember(&entry->asMember, ag.c_str());
        if (as_status && as_status != S_asLib_asNotActive)
            errlogPrintf("gate_init_stats_cmd: asAddMember('%s', as_group='%s') failed (status=%ld)\n",
                         full.c_str(), ag.c_str(), as_status);
        GateStatEntry* raw = entry.get();
        {
            std::lock_guard<std::mutex> lock(statHandlesMutex);
            statHandles.insert(raw);
        }
        std::lock_guard<std::mutex> lock(statEntriesMutex);
        statEntries[full] = std::move(entry);
    };
    // "vctotal" is a count of Virtual Connection *objects* (one per downstream client's claim
    // on one PV), not of raw TCP connections -- it equals "active" (the PV-count) except when
    // more than one client claims the same PV, in which case vctotal > active. See
    // docs/Gateway.html's own description of the old PCAS-based Gateway's vctotal for this
    // same distinction. Deliberately excludes any client's connections to the statistics PVs
    // themselves (GateStatEntry has no downstreamRefs at all), so merely querying
    // gwtest:vctotal never perturbs its own answer.
    add("vctotal", []{ return (double)sum_channels([](GateChannel* c){
        return (long)c->downstreamRefs.load(std::memory_order_relaxed);
    }); });
    add("pvtotal", []{ return (double)count_channels([](GateChannel*){ return true; }); });
    add("connected", []{ return (double)count_channels([](GateChannel* c){
        return c->caChid && ca_state(c->caChid) == cs_conn;
    }); });
    add("active", []{ return (double)count_channels([](GateChannel* c){
        return c->caChid && ca_state(c->caChid) == cs_conn && c->downstreamRefs.load(std::memory_order_relaxed) > 0;
    }); });
    add("inactive", []{ return (double)count_channels([](GateChannel* c){
        return c->caChid && ca_state(c->caChid) == cs_conn && c->downstreamRefs.load(std::memory_order_relaxed) == 0;
    }); });
    // Two pairs, each upstream (gateway<-IOC) + downstream (gateway->client): one pair counts
    // updates (events/sec), the other counts volume (bytes/sec of each event's raw
    // DBR_TIME_<native> payload) -- see RateCounters' comment for exactly what's counted where.
    add("upstreamEventRate", []{ return g_upstreamRate.eventRate.load(std::memory_order_relaxed); },
        true, "Hz", 2);
    add("downstreamEventRate", []{ return g_downstreamRate.eventRate.load(std::memory_order_relaxed); },
        true, "Hz", 2);
    add("upstreamVolumeRate", []{ return g_upstreamRate.byteRate.load(std::memory_order_relaxed); },
        true, "B/s", 1);
    add("downstreamVolumeRate", []{ return g_downstreamRate.byteRate.load(std::memory_order_relaxed); },
        true, "B/s", 1);
    // Downstream delivery queues (see GateClientQueue): depth/bytes are the live totals summed
    // over every connected client's queue, dropped is cumulative since startup. A persistently
    // non-zero depth or a climbing "queueDropped" is the signal that some client can't keep up.
    add("queueDepth", []{ return (double)g_qDepth.load(std::memory_order_relaxed); });
    add("queueBytes", []{ return (double)g_qBytes.load(std::memory_order_relaxed); }, false, "B");
    add("queueMaxDepth", []{ return (double)g_qDepthHigh.load(std::memory_order_relaxed); });
    add("queueMaxBytes", []{ return (double)g_qBytesHigh.load(std::memory_order_relaxed); }, false, "B");
    add("queueDropped", []{
        return (double)(g_qDroppedOldest.load(std::memory_order_relaxed) +
                        g_qDroppedNewest.load(std::memory_order_relaxed));
    });
    // Started lazily, once, on the first gateInitStats call -- a gateway that never enables
    // statistics never spins up the background rate-ticking timer thread.
    std::call_once(g_rateStatsTimerOnce, []{
        epicsTimerQueueActive& queue = epicsTimerQueueActive::allocate(true);
        g_rateStatsTimer = new RateStatsTimer(queue);
    });
    errlogPrintf("gate_init_stats_cmd: statistics PVs created under prefix '%s:' (as_group='%s')\n",
                 prefix, ag.c_str());
}

void gate_add_pv_cmd(const char* pattern, const char* client_name, const char* as_group, const char* target) {
    int error;
    PCRE2_SIZE erroroffset;
    pcre2_code* re = pcre2_compile((PCRE2_SPTR)pattern, PCRE2_ZERO_TERMINATED, 0, &error, &erroroffset, NULL);
    if (re) {
        Route route;
        route.re = re;
        route.client_name = client_name ? client_name : "";
        route.as_group = as_group ? as_group : "";
        route.target = target ? target : "";
        std::lock_guard<std::mutex> lock(routesMutex);
        routes.push_back(route);
        errlogPrintf("gate_add_pv_cmd: route added: pattern='%s' client='%s' as_group='%s' target='%s'\n",
                     pattern, client_name, as_group ? as_group : "", target ? target : "");
    } else {
        errlogPrintf("gate_add_pv_cmd: failed to compile pattern '%s' (pcre2 error %d)\n", pattern, error);
    }
}

// Splits a comma-separated host list (whitespace around each token trimmed); empty tokens
// dropped. `hosts_csv` empty/NULL yields an empty vector (blanket deny).
static std::vector<std::string> split_csv_trimmed(const char* hosts_csv) {
    std::vector<std::string> out;
    if (!hosts_csv) return out;
    std::istringstream iss(hosts_csv);
    std::string token;
    while (std::getline(iss, token, ',')) {
        size_t b = token.find_first_not_of(" \t");
        size_t e = token.find_last_not_of(" \t");
        if (b == std::string::npos) continue;
        out.push_back(token.substr(b, e - b + 1));
    }
    return out;
}

void gate_add_deny_cmd(const char* pattern, const char* hosts_csv) {
    int error;
    PCRE2_SIZE erroroffset;
    pcre2_code* re = pcre2_compile((PCRE2_SPTR)pattern, PCRE2_ZERO_TERMINATED, 0, &error, &erroroffset, NULL);
    if (re) {
        Route route;
        route.re = re;
        route.deny = true;
        route.deny_hosts = split_csv_trimmed(hosts_csv);
        std::lock_guard<std::mutex> lock(routesMutex);
        routes.push_back(route);
        errlogPrintf("gate_add_deny_cmd: deny route added: pattern='%s' hosts='%s'\n",
                     pattern, hosts_csv ? hosts_csv : "(blanket)");
    } else {
        errlogPrintf("gate_add_deny_cmd: failed to compile pattern '%s' (pcre2 error %d)\n", pattern, error);
    }
}

namespace {

struct ConfigParseCtx {
    enum Ctx { CTX_ROOT_OBJ, CTX_CLIENTS_ARRAY, CTX_CLIENT_OBJ, CTX_PVS_ARRAY, CTX_PV_OBJ, CTX_HOSTS_ARRAY, CTX_OTHER };
    std::vector<Ctx> stack;
    std::string key;

    std::string name, addr_list;
    bool auto_addr;
    int port;
    bool has_name;

    std::string pattern, client_name, as_group, target;
    bool has_pattern, has_client;
    // A pv entry with "action":"deny" carries an optional "hosts":[...] list instead of
    // client/as_group/target -- see gate_add_deny_cmd().
    bool is_deny;
    std::vector<std::string> hosts;

    Ctx top() const { return stack.back(); }
};

int cb_start_map(void* vctx) {
    ConfigParseCtx* ctx = (ConfigParseCtx*)vctx;
    if (ctx->stack.empty()) {
        ctx->stack.push_back(ConfigParseCtx::CTX_ROOT_OBJ);
    } else if (ctx->top() == ConfigParseCtx::CTX_CLIENTS_ARRAY) {
        ctx->stack.push_back(ConfigParseCtx::CTX_CLIENT_OBJ);
        ctx->name.clear(); ctx->addr_list.clear();
        ctx->auto_addr = true; ctx->port = 5064; ctx->has_name = false;
    } else if (ctx->top() == ConfigParseCtx::CTX_PVS_ARRAY) {
        ctx->stack.push_back(ConfigParseCtx::CTX_PV_OBJ);
        ctx->pattern.clear(); ctx->client_name.clear(); ctx->as_group = "DEFAULT"; ctx->target.clear();
        ctx->has_pattern = false; ctx->has_client = false;
        ctx->is_deny = false; ctx->hosts.clear();
    } else {
        ctx->stack.push_back(ConfigParseCtx::CTX_OTHER);
    }
    return 1;
}

int cb_end_map(void* vctx) {
    ConfigParseCtx* ctx = (ConfigParseCtx*)vctx;
    ConfigParseCtx::Ctx t = ctx->top();
    ctx->stack.pop_back();
    if (t == ConfigParseCtx::CTX_CLIENT_OBJ && ctx->has_name) {
        gate_create_client_cmd(ctx->name.c_str(), ctx->addr_list.c_str(), ctx->auto_addr ? 1 : 0, ctx->port);
    } else if (t == ConfigParseCtx::CTX_PV_OBJ && ctx->has_pattern && ctx->is_deny) {
        std::string hosts_csv;
        for (size_t i = 0; i < ctx->hosts.size(); ++i) {
            if (i) hosts_csv += ",";
            hosts_csv += ctx->hosts[i];
        }
        gate_add_deny_cmd(ctx->pattern.c_str(), hosts_csv.c_str());
    } else if (t == ConfigParseCtx::CTX_PV_OBJ && ctx->has_pattern && ctx->has_client) {
        gate_add_pv_cmd(ctx->pattern.c_str(), ctx->client_name.c_str(), ctx->as_group.c_str(), ctx->target.c_str());
    }
    return 1;
}

int cb_start_array(void* vctx) {
    ConfigParseCtx* ctx = (ConfigParseCtx*)vctx;
    if (ctx->top() == ConfigParseCtx::CTX_ROOT_OBJ && ctx->key == "clients") {
        ctx->stack.push_back(ConfigParseCtx::CTX_CLIENTS_ARRAY);
    } else if (ctx->top() == ConfigParseCtx::CTX_ROOT_OBJ && ctx->key == "pvs") {
        ctx->stack.push_back(ConfigParseCtx::CTX_PVS_ARRAY);
    } else if (ctx->top() == ConfigParseCtx::CTX_PV_OBJ && ctx->key == "hosts") {
        ctx->stack.push_back(ConfigParseCtx::CTX_HOSTS_ARRAY);
    } else {
        ctx->stack.push_back(ConfigParseCtx::CTX_OTHER);
    }
    return 1;
}

int cb_end_array(void* vctx) {
    ConfigParseCtx* ctx = (ConfigParseCtx*)vctx;
    ctx->stack.pop_back();
    return 1;
}

int cb_map_key(void* vctx, const unsigned char* key, gate_yajl_len_t len) {
    ConfigParseCtx* ctx = (ConfigParseCtx*)vctx;
    ctx->key.assign((const char*)key, len);
    return 1;
}

int cb_string(void* vctx, const unsigned char* val, gate_yajl_len_t len) {
    ConfigParseCtx* ctx = (ConfigParseCtx*)vctx;
    std::string s((const char*)val, len);
    if (ctx->top() == ConfigParseCtx::CTX_CLIENT_OBJ) {
        if (ctx->key == "name") { ctx->name = s; ctx->has_name = true; }
        else if (ctx->key == "addr_list") ctx->addr_list = s;
    } else if (ctx->top() == ConfigParseCtx::CTX_PV_OBJ) {
        if (ctx->key == "pattern") { ctx->pattern = s; ctx->has_pattern = true; }
        else if (ctx->key == "client") { ctx->client_name = s; ctx->has_client = true; }
        else if (ctx->key == "as_group") ctx->as_group = s;
        else if (ctx->key == "target") ctx->target = s;
        else if (ctx->key == "action") ctx->is_deny = (s == "deny");
    } else if (ctx->top() == ConfigParseCtx::CTX_HOSTS_ARRAY) {
        ctx->hosts.push_back(s);
    }
    return 1;
}

int cb_boolean(void* vctx, int boolVal) {
    ConfigParseCtx* ctx = (ConfigParseCtx*)vctx;
    if (ctx->top() == ConfigParseCtx::CTX_CLIENT_OBJ && ctx->key == "auto_addr") ctx->auto_addr = boolVal != 0;
    return 1;
}

int cb_integer(void* vctx, gate_yajl_int_t integerVal) {
    ConfigParseCtx* ctx = (ConfigParseCtx*)vctx;
    if (ctx->top() == ConfigParseCtx::CTX_CLIENT_OBJ && ctx->key == "port") ctx->port = (int)integerVal;
    return 1;
}

const yajl_callbacks configCallbacks = {
    NULL,           /* yajl_null */
    cb_boolean,
    cb_integer,
    NULL,           /* yajl_double */
    NULL,           /* yajl_number */
    cb_string,
    cb_start_map,
    cb_map_key,
    cb_end_map,
    cb_start_array,
    cb_end_array
};

} // namespace

// Loads an EPICS access security (ACF) file via the real libCom asLib, flipping on
// asActive -- until this is called, asCheckGet/asCheckPut/asAddClient/asAddMember (all
// vendored, unmodified rsrv/asLib code, already wired to GateChannel::asMember via
// asDbGetMemberPvt() in gateShim.c) short-circuit to "allow everything" (see CLAUDE.md).
void gate_load_access(const char* filename) {
    long status = asInitFile(filename, "");
    if (status) {
        errlogPrintf("gate_load_access: asInitFile('%s') failed (status=%ld)\n", filename, status);
    } else {
        errlogPrintf("gate_load_access: '%s' loaded successfully\n", filename);
    }
}

void gate_load_config(const char* filename) {
    std::ifstream ifs(filename);
    if (!ifs.is_open()) {
        errlogPrintf("gate_load_config: could not open '%s'\n", filename);
        return;
    }
    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    ConfigParseCtx ctx;
    yajl_handle hand = gate_yajl_alloc(&configCallbacks, &ctx);
    yajl_status status = yajl_parse(hand, (const unsigned char*)content.data(), content.size());
    if (status == yajl_status_ok)
        status = gate_yajl_complete_parse(hand);
    if (status == yajl_status_ok) {
        errlogPrintf("gate_load_config: '%s' loaded successfully\n", filename);
    } else {
        unsigned char* err = yajl_get_error(hand, 1, (const unsigned char*)content.data(), content.size());
        errlogPrintf("gate_load_config: failed to parse '%s': %s\n", filename, err ? (const char*)err : "unknown error");
        if (err) yajl_free_error(hand, err);
    }
    yajl_free(hand);
}

#ifdef WITH_CAPUTLOG
// Shared implementation for gateLoadPutLogText/gateLoadPutLogJson (see the architecture
// comment above gate_trap_write_cb() for why neither sink can use any of caPutLog's own
// runtime code). addr: whitespace-separated "host[:port]" list of log-server destinations
// (default port 7011, matching both caPutLogClient.c's and CaPutJsonLogTask::default_port's
// own default). config: -1 disable / 0 on-change / 1 log-all / 2 log-all-no-filter
// (caPutLogNone/OnChange/All/AllNoFilter; 1 and 2 are equivalent here -- see above).
static void gate_load_put_log_cmd(const char* label, PutLogSink& sink, const char* addr, int config) {
    if (!addr || !addr[0]) {
        errlogPrintf("%s: no address given\n", label);
        return;
    }
    std::istringstream iss(addr);
    std::string token;
    int added = 0;
    {
        std::lock_guard<std::mutex> lock(sink.mutex);
        while (iss >> token) {
            struct sockaddr_in saddr;
            if (aToIPAddr(token.c_str(), 7011, &saddr) < 0) {
                errlogPrintf("%s: bad address '%s'\n", label, token.c_str());
                continue;
            }
            bool dup = false;
            for (auto& c : sink.clients) if (c.second == token) { dup = true; break; }
            if (dup) continue;
            logClientId id = logClientCreate(saddr.sin_addr, ntohs(saddr.sin_port));
            if (!id) {
                errlogPrintf("%s: logClientCreate('%s') failed\n", label, token.c_str());
                continue;
            }
            sink.clients.push_back({id, token});
            added++;
        }
        if (!added && sink.clients.empty()) {
            errlogPrintf("%s: no valid addresses configured\n", label);
            return;
        }
    }
    sink.config = (config >= caPutLogNone && config <= caPutLogAllNoFilter) ? config : caPutLogAll;
    std::call_once(g_trapWriteListenerOnce, []{
        g_trapWriteListenerId = asTrapWriteRegisterListener(gate_trap_write_cb);
    });
    errlogPrintf("%s: configured (addr='%s', config=%d)\n", label, addr, config);
}

void gate_load_put_log_text_cmd(const char* addr, int config, double timeout) {
    (void)timeout; // no burst coalescing (see architecture comment above)
    gate_load_put_log_cmd("gate_load_put_log_text_cmd", g_textSink, addr, config);
}

void gate_load_put_log_json_cmd(const char* addr, int config, double timeout) {
    (void)timeout;
    gate_load_put_log_cmd("gate_load_put_log_json_cmd", g_jsonSink, addr, config);
}

// gateLoadPutLogFile <filename>: a drop-in replacement for the old PCAS-based Gateway's
// "-putlog <file>" (see the PutLogFileSink/gate_send_put_log_file() comments above for the
// exact format match). Opens (truncating, matching the old gateway.cc's fopen(...,"w"))
// `filename` and writes the same header shape: a couple of startup-info lines, then a literal
// "Attempted Writes:" line -- testTop's test_logging.py parses up to that marker as a header
// (content not otherwise checked) and every line after it as one put each. Always logs every
// trapped write unconditionally (no on-change/burst config -- the old file writer had none;
// that's a caPutLogTask.c-specific feature only ever wired to the network sink).
void gate_load_put_log_file_cmd(const char* filename) {
    if (!filename || !filename[0]) {
        errlogPrintf("gate_load_put_log_file_cmd: no filename given\n");
        return;
    }
    FILE* fp = fopen(filename, "w");
    if (!fp) {
        errlogPrintf("gate_load_put_log_file_cmd: fopen('%s') failed\n", filename);
        return;
    }
    char timebuf[32];
    epicsTimeStamp now;
    epicsTimeGetCurrent(&now);
    epicsTimeToStrftime(timebuf, sizeof(timebuf), "%b %d %H:%M:%S", &now);
    fprintf(fp, "%s CA Gateway (rsrv-based)\n", timebuf);
    fprintf(fp, "Attempted Writes:\n");
    fflush(fp);

    FILE* oldFp;
    {
        std::lock_guard<std::mutex> lock(g_fileSink.mutex);
        oldFp = g_fileSink.fp;
        g_fileSink.fp = fp;
        g_fileSink.enabled = true;
    }
    if (oldFp) fclose(oldFp);

    std::call_once(g_trapWriteListenerOnce, []{
        g_trapWriteListenerId = asTrapWriteRegisterListener(gate_trap_write_cb);
    });
    errlogPrintf("gate_load_put_log_file_cmd: put-log file configured ('%s')\n", filename);
}
#else // !WITH_CAPUTLOG
// gateLoadPutLogText/gateLoadPutLogJson/gateLoadPutLogFile are still registered as iocsh
// commands (GateMain.cpp) regardless of how this was built, so a config script calling them
// doesn't hit an "unknown command" error either way -- it just gets a clear explanation
// instead of the feature silently doing nothing.
void gate_load_put_log_text_cmd(const char* addr, int config, double timeout) {
    (void)addr; (void)config; (void)timeout;
    errlogPrintf("gate_load_put_log_text_cmd: caPutLog support not built in -- set CAPUTLOG in RELEASE.local and rebuild\n");
}

void gate_load_put_log_json_cmd(const char* addr, int config, double timeout) {
    (void)addr; (void)config; (void)timeout;
    errlogPrintf("gate_load_put_log_json_cmd: caPutLog support not built in -- set CAPUTLOG in RELEASE.local and rebuild\n");
}

void gate_load_put_log_file_cmd(const char* filename) {
    (void)filename;
    errlogPrintf("gate_load_put_log_file_cmd: caPutLog support not built in -- set CAPUTLOG in RELEASE.local and rebuild\n");
}
#endif // WITH_CAPUTLOG
}
