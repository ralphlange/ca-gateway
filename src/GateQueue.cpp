#include "GateQueue.h"

#include <epicsThread.h>
#include <errlog.h>
#include <epicsString.h>
#include <cstdio>
#include <sstream>
#include <string>


// Limits are per queue (i.e. per downstream client); 0 means "no limit" for either one.
// Whichever limit is hit first triggers the overflow policy.
std::atomic<size_t> g_queueMaxElements{2000};
std::atomic<size_t> g_queueMaxBytes{16u * 1024u * 1024u};
std::atomic<int> g_queueOverflowPolicy{GATE_Q_DROP_OLDEST};

// Aggregate counters across all client queues (exposed as statistics PVs by gateInitStats,
// and per-queue in gateQueueReport).
std::atomic<long long> g_qDepth{0};        // entries currently queued
std::atomic<long long> g_qBytes{0};        // bytes currently queued
std::atomic<long long> g_qDepthHigh{0};    // high-water marks
std::atomic<long long> g_qBytesHigh{0};
std::atomic<unsigned long long> g_qPushed{0};
std::atomic<unsigned long long> g_qDelivered{0};
std::atomic<unsigned long long> g_qDroppedOldest{0};
std::atomic<unsigned long long> g_qDroppedNewest{0};

static void gate_bump_high(std::atomic<long long>& high, long long v) {
    long long prev = high.load(std::memory_order_relaxed);
    while (v > prev && !high.compare_exchange_weak(prev, v, std::memory_order_relaxed)) {}
}


// Every live queue, for gateQueueReport. Entries are added by gate_queue_create() and removed
// by gate_queue_destroy().
std::set<GateClientQueue*> g_queues;
std::mutex g_queuesMutex;

// Discards `it` (caller holds q->mtx), keeping the byte/depth accounting straight.
static std::deque<GateClientQueue::Entry>::iterator
gate_queue_erase_locked(GateClientQueue* q, std::deque<GateClientQueue::Entry>::iterator it) {
    q->bytes -= it->bytes;
    g_qBytes.fetch_sub((long long)it->bytes, std::memory_order_relaxed);
    g_qDepth.fetch_sub(1, std::memory_order_relaxed);
    return q->q.erase(it);
}

// Hands one event to a downstream client's reader thread. Never blocks on that client's
// socket -- that is the entire point (see GateClientQueue's comment).
void gate_queue_push(GateClientQueue* q, GateClientQueue::Entry&& e) {
    size_t maxElems = g_queueMaxElements.load(std::memory_order_relaxed);
    size_t maxBytes = g_queueMaxBytes.load(std::memory_order_relaxed);
    int policy = g_queueOverflowPolicy.load(std::memory_order_relaxed);
    {
        std::lock_guard<std::mutex> lock(q->mtx);
        if (q->stop) return;

        bool overElems = maxElems && q->q.size() + 1 > maxElems;
        bool overBytes = maxBytes && q->bytes + e.bytes > maxBytes;
        if (overElems || overBytes) {
            if (policy == GATE_Q_DROP_NEWEST) {
                q->droppedNewest++;
                g_qDroppedNewest.fetch_add(1, std::memory_order_relaxed);
                return;
            }
            // GATE_Q_DROP_OLDEST: make room by evicting from the front. Stops if the queue
            // empties (a single entry larger than maxBytes is still admitted -- dropping it
            // outright would mean that PV could never be delivered to this client at all).
            while (!q->q.empty() &&
                   ((maxElems && q->q.size() + 1 > maxElems) ||
                    (maxBytes && q->bytes + e.bytes > maxBytes))) {
                gate_queue_erase_locked(q, q->q.begin());
                q->droppedOldest++;
                g_qDroppedOldest.fetch_add(1, std::memory_order_relaxed);
            }
        }

        q->bytes += e.bytes;
        q->q.push_back(std::move(e));
        q->pushed++;
        if (q->q.size() > q->depthHigh) q->depthHigh = q->q.size();
        if (q->bytes > q->bytesHigh) q->bytesHigh = q->bytes;
    }
    long long depth = g_qDepth.fetch_add(1, std::memory_order_relaxed) + 1;
    long long bytes = g_qBytes.fetch_add((long long)e.bytes, std::memory_order_relaxed) + (long long)e.bytes;
    g_qPushed.fetch_add(1, std::memory_order_relaxed);
    gate_bump_high(g_qDepthHigh, depth);
    gate_bump_high(g_qBytesHigh, bytes);
    q->cv.notify_one();
}

// Drops every queued entry belonging to `owner` and waits for any in-progress delivery of it
// to finish, so the caller can safely free the subscription object afterwards.
void gate_queue_purge_owner(GateClientQueue* q, const void* owner) {
    std::unique_lock<std::mutex> lock(q->mtx);
    for (auto it = q->q.begin(); it != q->q.end(); ) {
        if (it->owner == owner) it = gate_queue_erase_locked(q, it);
        else ++it;
    }
    q->inFlightCv.wait(lock, [q, owner]{ return q->inFlight != owner; });
}

static void gate_queue_reader(void* arg) {
    GateClientQueue* q = (GateClientQueue*)arg;
    for (;;) {
        GateClientQueue::Entry e;
        {
            std::unique_lock<std::mutex> lock(q->mtx);
            q->cv.wait(lock, [q]{ return q->stop || (!q->q.empty() && !q->flowCtrl); });
            if (q->stop) break;
            e = std::move(q->q.front());
            gate_queue_erase_locked(q, q->q.begin());
            q->inFlight = e.owner;
        }
        // Outside the lock: this is the call that can block on the client's socket, and
        // pushers must never wait behind it.
        e.cb(e.user_arg, e.dbchan, 0, e.data ? (void*)e.data.get() : NULL);
        {
            std::lock_guard<std::mutex> lock(q->mtx);
            q->inFlight = nullptr;
            q->delivered++;
        }
        g_qDelivered.fetch_add(1, std::memory_order_relaxed);
        q->inFlightCv.notify_all();
    }
    {
        std::lock_guard<std::mutex> lock(q->mtx);
        q->exited = true;
    }
    q->exitedCv.notify_all();
}


void gate_queue_reset_stats(void) {
    g_qDepth = 0; g_qBytes = 0; g_qDepthHigh = 0; g_qBytesHigh = 0;
    g_qPushed = 0; g_qDelivered = 0; g_qDroppedOldest = 0; g_qDroppedNewest = 0;
}

extern "C" {
// --- Per-downstream-client queue lifecycle, driven by gateShim.c's db_init_events()/
// db_start_events()/db_close_events() (rsrv creates exactly one event context per TCP client).

void* gate_queue_create(void) {
    GateClientQueue* q = new GateClientQueue();
    std::lock_guard<std::mutex> lock(g_queuesMutex);
    g_queues.insert(q);
    return (void*)q;
}

void gate_queue_start(void* handle, const char* name, unsigned int priority) {
    GateClientQueue* q = (GateClientQueue*)handle;
    if (!q) return;
    {
        std::lock_guard<std::mutex> lock(q->mtx);
        if (q->started) return;
        q->started = true;
    }
    epicsThreadCreate(name && name[0] ? name : "CAS-gateQ",
                      priority ? priority : epicsThreadPriorityCAServerLow,
                      epicsThreadGetStackSize(epicsThreadStackMedium),
                      gate_queue_reader, q);
}

void gate_queue_destroy(void* handle) {
    GateClientQueue* q = (GateClientQueue*)handle;
    if (!q) return;
    bool started;
    {
        std::unique_lock<std::mutex> lock(q->mtx);
        started = q->started;
        q->stop = true;
        // Anything still queued at teardown was never delivered; keep the global accounting
        // straight rather than leaking the counts.
        while (!q->q.empty()) gate_queue_erase_locked(q, q->q.begin());
        q->cv.notify_all();
        if (started) q->exitedCv.wait(lock, [q]{ return q->exited; });
    }
    {
        std::lock_guard<std::mutex> lock(g_queuesMutex);
        g_queues.erase(q);
    }
    delete q;
}

// CA_PROTO_EVENTS_OFF/EVENTS_ON (camessage.c's events_off_action/events_on_action, via
// gateShim.c's db_event_flow_ctrl_mode_on/off): pause/resume delivery for this client. Events
// keep queueing (and overflowing per policy) meanwhile, exactly as a real IOC's event queue does.
void gate_queue_set_flow_ctrl(void* handle, int on) {
    GateClientQueue* q = (GateClientQueue*)handle;
    if (!q) return;
    {
        std::lock_guard<std::mutex> lock(q->mtx);
        q->flowCtrl = (on != 0);
    }
    if (!on) q->cv.notify_one();
}

// gateSetQueueLimits <maxElements> <maxBytes> <policy>: bounds each downstream client's
// delivery queue (see GateClientQueue). Either limit may be 0 for "unlimited"; whichever is
// reached first triggers `policy`, "oldest" (default -- evict the stalest queued update, so a
// lagging client still converges on current values) or "newest" (reject the incoming update).
// Applies to queues created afterwards *and* to existing ones, since both are read at push time.
void gate_set_queue_limits_cmd(int maxElements, int maxBytes, const char* policy) {
    if (maxElements >= 0) g_queueMaxElements = (size_t)maxElements;
    if (maxBytes >= 0) g_queueMaxBytes = (size_t)maxBytes;
    if (policy && policy[0]) {
        if (epicsStrCaseCmp(policy, "oldest") == 0) {
            g_queueOverflowPolicy = GATE_Q_DROP_OLDEST;
        } else if (epicsStrCaseCmp(policy, "newest") == 0) {
            g_queueOverflowPolicy = GATE_Q_DROP_NEWEST;
        } else {
            errlogPrintf("gate_set_queue_limits_cmd: unknown policy '%s' (want 'oldest' or 'newest'), unchanged\n",
                         policy);
        }
    }
    errlogPrintf("gate_set_queue_limits_cmd: maxElements=%lu maxBytes=%lu policy=%s\n",
                 (unsigned long)g_queueMaxElements.load(), (unsigned long)g_queueMaxBytes.load(),
                 g_queueOverflowPolicy.load() == GATE_Q_DROP_NEWEST ? "newest" : "oldest");
}

// gateQueueReport <level>: totals at any level, plus one line per connected client's queue at
// level > 0 -- which is what actually identifies *which* client is lagging.
void gate_queue_report_cmd(int level) {
    printf("Downstream queue limits: maxElements=%lu maxBytes=%lu policy=%s\n",
           (unsigned long)g_queueMaxElements.load(), (unsigned long)g_queueMaxBytes.load(),
           g_queueOverflowPolicy.load() == GATE_Q_DROP_NEWEST ? "newest" : "oldest");
    printf("Totals: depth=%lld (max %lld) bytes=%lld (max %lld)\n",
           g_qDepth.load(), g_qDepthHigh.load(), g_qBytes.load(), g_qBytesHigh.load());
    printf("        pushed=%llu delivered=%llu dropped=%llu (oldest %llu, newest %llu)\n",
           g_qPushed.load(), g_qDelivered.load(),
           g_qDroppedOldest.load() + g_qDroppedNewest.load(),
           g_qDroppedOldest.load(), g_qDroppedNewest.load());
    if (level <= 0) return;
    std::lock_guard<std::mutex> lock(g_queuesMutex);
    printf("%zu client queue(s):\n", g_queues.size());
    for (auto* q : g_queues) {
        std::lock_guard<std::mutex> qlock(q->mtx);
        printf("  %p: depth=%zu (max %zu) bytes=%zu (max %zu) pushed=%llu delivered=%llu"
               " dropped=%llu/%llu%s\n",
               (void*)q, q->q.size(), q->depthHigh, q->bytes, q->bytesHigh,
               q->pushed, q->delivered, q->droppedOldest, q->droppedNewest,
               q->flowCtrl ? " [events off]" : "");
    }
}

}
