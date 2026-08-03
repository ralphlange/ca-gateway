#ifndef GATE_QUEUE_H
#define GATE_QUEUE_H

// --- Per-downstream-client delivery queue -------------------------------------------------
//
// Decouples the upstream side (libca's per-IOC-circuit receive thread, which runs
// ca_event_cb/ca_meta_cb in GateLogic.cpp) from the downstream side (writing to one client's
// TCP socket).
//
// Without this, delivery was a straight synchronous call from the upstream callback into
// rsrv's read_reply(), which ends in cas_send_bs_msg() -> a *blocking* send() on that one
// client's socket (no SO_SNDTIMEO, no non-blocking mode anywhere in rsrv). Because libca runs
// a single thread per upstream TCP circuit that both reads the wire and dispatches callbacks
// (tcpRecvThread::run(), modules/ca/src/client/tcpiiu.cpp), one downstream client that stopped
// draining its socket would stall that thread -- blocking delivery to every *other* client
// subscribed to the same PV, and blocking all further data from that entire upstream IOC
// (every PV multiplexed over that circuit), until TCP eventually gave up. A real IOC avoids
// this with dbEvent.c's per-client event task; this is the equivalent for the Gateway.
//
// One queue per downstream client, keyed off rsrv's own per-client dbEventCtx
// (client->evuser, created in create_tcp_client()) -- see gateShim.c's db_init_events()/
// db_start_events()/db_close_events(). Many upstream threads push; exactly one thread (this
// queue's own reader) pops and performs the blocking write, so a stalled client now only ever
// stalls itself.
//
// Queue entries hold a shared_ptr to the event payload, so a single upstream update fanned out
// to N subscribers is stored once and reference-counted, not copied N times; it stays alive
// until the last queue has delivered it, independent of the cache having moved on to a newer
// value. The payload is deliberately typed `void` here: this file knows nothing about GateData,
// CA or rsrv, which is what lets it be unit-tested on its own (see
// testTop/unitTestsApp/gateQueueTest.cpp). GateLogic.cpp's gate_deliver() is where the
// GateData-specific part lives.

#include <atomic>
#include <condition_variable>
#include <deque>
#include <memory>
#include <mutex>
#include <set>

enum GateQueueOverflowPolicy {
    GATE_Q_DROP_OLDEST = 0, // evict from the front until the new entry fits (newest data wins)
    GATE_Q_DROP_NEWEST = 1  // reject the incoming entry, keep what's already queued
};

// Limits are per queue (i.e. per downstream client); 0 means "no limit" for either one.
// Whichever limit is hit first triggers the overflow policy. Read at push time, so changing
// them affects queues that already exist.
extern std::atomic<size_t> g_queueMaxElements;
extern std::atomic<size_t> g_queueMaxBytes;
extern std::atomic<int> g_queueOverflowPolicy;

// Aggregate counters across all client queues (exposed as statistics PVs by gateInitStats,
// and per-queue in gateQueueReport).
extern std::atomic<long long> g_qDepth;        // entries currently queued
extern std::atomic<long long> g_qBytes;        // bytes currently queued
extern std::atomic<long long> g_qDepthHigh;    // high-water marks
extern std::atomic<long long> g_qBytesHigh;
extern std::atomic<unsigned long long> g_qPushed;
extern std::atomic<unsigned long long> g_qDelivered;
extern std::atomic<unsigned long long> g_qDroppedOldest;
extern std::atomic<unsigned long long> g_qDroppedNewest;

struct GateClientQueue {
    struct Entry {
        // Null for a statistics-PV update, whose value is computed live at delivery time
        // (gate_get_count() ignores pfl for those) -- there is no payload to carry.
        std::shared_ptr<void> data;
        void (*cb)(void*, void*, int, void*) = nullptr;
        void* user_arg = nullptr;
        void* dbchan = nullptr;
        // The UserSub*/GateStatEntry::Sub* this entry was queued for, so cancelling that
        // subscription can purge its still-queued entries before the object is freed.
        const void* owner = nullptr;
        size_t bytes = 0;
    };

    std::mutex mtx;
    std::condition_variable cv;
    std::deque<Entry> q;
    size_t bytes = 0;
    bool stop = false;
    bool flowCtrl = false; // client sent CA_PROTO_EVENTS_OFF; hold delivery (keep accumulating)
    bool started = false;
    bool exited = false;
    std::condition_variable exitedCv;
    // Which subscription the reader is delivering right now (outside the lock), so
    // gate_queue_purge_owner() can wait it out rather than free from under it.
    const void* inFlight = nullptr;
    std::condition_variable inFlightCv;

    // Per-queue counters (aggregate equivalents above).
    unsigned long long pushed = 0, delivered = 0, droppedOldest = 0, droppedNewest = 0;
    size_t depthHigh = 0, bytesHigh = 0;
};

// Every live queue, for gateQueueReport. Maintained by gate_queue_create()/gate_queue_destroy().
extern std::set<GateClientQueue*> g_queues;
extern std::mutex g_queuesMutex;

// Hands one event to a downstream client's reader thread. Never blocks on that client's
// socket -- that is the entire point (see the comment at the top of this file). Applies the
// element/byte limits and overflow policy above.
void gate_queue_push(GateClientQueue* q, GateClientQueue::Entry&& e);

// Drops every queued entry belonging to `owner` and waits for any in-progress delivery of it
// to finish, so the caller can safely free the subscription object afterwards.
void gate_queue_purge_owner(GateClientQueue* q, const void* owner);

// Resets the aggregate counters above (not the per-queue ones). Only used to isolate unit
// tests from each other -- the running gateway never calls this.
void gate_queue_reset_stats(void);

// The queue's lifecycle/configuration entry points. Also declared in gate_db_interface.h,
// which is the plain-C view used by gateShim.c and so cannot include this C++ header; the two
// declarations must stay in step.
extern "C" {
void* gate_queue_create(void);
void gate_queue_start(void* queue, const char* name, unsigned int priority);
void gate_queue_destroy(void* queue);
void gate_queue_set_flow_ctrl(void* queue, int on);
void gate_set_queue_limits_cmd(int maxElements, int maxBytes, const char* policy);
void gate_queue_report_cmd(int level);
}

#endif // GATE_QUEUE_H
