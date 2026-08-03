/* Unit tests for the per-downstream-client delivery queue (src/GateQueue.cpp).
 *
 * These are white-box tests: they push synthetic entries and inspect the queue directly,
 * rather than driving a real gateway. That is what makes the overflow limits and policy
 * testable deterministically -- end-to-end they only trigger when a real client stops
 * draining its socket, which is inherently racy to arrange.
 *
 * Note most tests deliberately do NOT call gate_queue_start(): with no reader thread nothing
 * is ever dequeued, so pushes accumulate and the limit/policy behaviour can be asserted
 * exactly. The delivery and flow-control tests at the end do start a reader.
 */

#include <atomic>
#include <memory>
#include <string>
#include <vector>

#include <epicsEvent.h>
#include <epicsThread.h>
#include <epicsUnitTest.h>
#include <testMain.h>

#include "GateQueue.h"

namespace {

// What a delivered entry looked like, recorded by the callback below.
struct Delivery {
    void* user_arg;
    void* dbchan;
    void* pfl;
};
std::vector<Delivery> g_delivered;
epicsMutex g_deliveredLock;
epicsEvent g_deliveredEvent;

void record_delivery(void* user_arg, void* dbchan, int /*eventsRemaining*/, void* pfl) {
    {
        epicsGuard<epicsMutex> guard(g_deliveredLock);
        Delivery d = { user_arg, dbchan, pfl };
        g_delivered.push_back(d);
    }
    g_deliveredEvent.signal();
}

size_t delivered_count() {
    epicsGuard<epicsMutex> guard(g_deliveredLock);
    return g_delivered.size();
}

/* Waits until at least `n` deliveries have been recorded, or `timeout` elapses. Returns the
 * count actually reached, so callers can assert on it either way. */
size_t wait_for_deliveries(size_t n, double timeout = 5.0) {
    const double step = 0.01;
    for (double waited = 0.0; waited < timeout; waited += step) {
        if (delivered_count() >= n) break;
        g_deliveredEvent.wait(step);
    }
    return delivered_count();
}

void reset_deliveries() {
    epicsGuard<epicsMutex> guard(g_deliveredLock);
    g_delivered.clear();
}

/* A fresh queue plus a clean global-counter slate, so each test is independent. Tests that
 * never start a reader can just delete the queue at the end (nothing to join). */
GateClientQueue* fresh_queue() {
    gate_queue_reset_stats();
    reset_deliveries();
    return (GateClientQueue*)gate_queue_create();
}

/* Pushes one entry carrying `bytes` of notional payload. `owner` and `user_arg` double as
 * identity markers so tests can tell which entries survived an eviction. */
void push(GateClientQueue* q, size_t bytes, const void* owner = NULL, void* user_arg = NULL) {
    GateClientQueue::Entry e;
    // A real payload allocation, so the shared_ptr refcounting is genuinely exercised;
    // `bytes` is the accounted size, which the caller (gate_deliver) supplies separately.
    e.data = std::shared_ptr<void>(new char[bytes ? bytes : 1], [](void* p) {
        delete[] static_cast<char*>(p);
    });
    e.cb = record_delivery;
    e.user_arg = user_arg;
    e.dbchan = NULL;
    e.owner = owner;
    e.bytes = bytes;
    gate_queue_push(q, std::move(e));
}

size_t depth(GateClientQueue* q) {
    std::lock_guard<std::mutex> lock(q->mtx);
    return q->q.size();
}

/* Identity of each queued entry, front to back, as passed in via user_arg. */
std::vector<void*> contents(GateClientQueue* q) {
    std::lock_guard<std::mutex> lock(q->mtx);
    std::vector<void*> out;
    for (std::deque<GateClientQueue::Entry>::iterator it = q->q.begin(); it != q->q.end(); ++it)
        out.push_back(it->user_arg);
    return out;
}

void set_limits(size_t maxElements, size_t maxBytes, int policy) {
    g_queueMaxElements = maxElements;
    g_queueMaxBytes = maxBytes;
    g_queueOverflowPolicy = policy;
}

/* --- tests ------------------------------------------------------------------------------ */

void testPushAccounting() {
    testDiag("--- push accounting (depth, bytes, high-water) ---");
    GateClientQueue* q = fresh_queue();
    set_limits(0, 0, GATE_Q_DROP_OLDEST);

    push(q, 24);
    push(q, 40);
    push(q, 8);

    testOk(depth(q) == 3, "three entries queued (got %zu)", depth(q));
    testOk(q->bytes == 72, "per-queue bytes = 24+40+8 = 72 (got %zu)", q->bytes);
    testOk(g_qDepth.load() == 3, "global depth = 3 (got %lld)", g_qDepth.load());
    testOk(g_qBytes.load() == 72, "global bytes = 72 (got %lld)", g_qBytes.load());
    testOk(g_qPushed.load() == 3, "pushed counter = 3 (got %llu)", g_qPushed.load());
    testOk(g_qDepthHigh.load() == 3, "depth high-water = 3 (got %lld)", g_qDepthHigh.load());
    testOk(g_qBytesHigh.load() == 72, "bytes high-water = 72 (got %lld)", g_qBytesHigh.load());
    testOk(g_qDroppedOldest.load() == 0 && g_qDroppedNewest.load() == 0, "nothing dropped");

    // Tearing down a queue with entries still in it must not leak the global accounting.
    gate_queue_destroy(q);
    testOk(g_qDepth.load() == 0, "destroy drains global depth to 0 (got %lld)", g_qDepth.load());
    testOk(g_qBytes.load() == 0, "destroy drains global bytes to 0 (got %lld)", g_qBytes.load());
}

void testUnlimited() {
    testDiag("--- limits of 0 mean unlimited ---");
    GateClientQueue* q = fresh_queue();
    set_limits(0, 0, GATE_Q_DROP_OLDEST);

    for (int i = 0; i < 500; i++) push(q, 1000);

    testOk(depth(q) == 500, "all 500 entries retained (got %zu)", depth(q));
    testOk(g_qDroppedOldest.load() == 0 && g_qDroppedNewest.load() == 0,
           "no drops with both limits at 0");
    gate_queue_destroy(q);
}

void testElementLimitDropOldest() {
    testDiag("--- element limit, policy=oldest ---");
    GateClientQueue* q = fresh_queue();
    set_limits(3, 0, GATE_Q_DROP_OLDEST);

    int id[5];
    for (int i = 0; i < 5; i++) push(q, 8, NULL, &id[i]);

    testOk(depth(q) == 3, "capped at 3 entries (got %zu)", depth(q));
    testOk(g_qDroppedOldest.load() == 2, "2 dropped as oldest (got %llu)", g_qDroppedOldest.load());
    testOk(g_qDroppedNewest.load() == 0, "none dropped as newest");

    // Dropping the oldest must keep the *newest* data -- that is the whole point of the
    // policy for a lagging client.
    std::vector<void*> c = contents(q);
    testOk(c.size() == 3 && c[0] == &id[2] && c[1] == &id[3] && c[2] == &id[4],
           "the three newest entries survived");
    testOk(q->bytes == 24, "bytes tracks the survivors (got %zu)", q->bytes);
    gate_queue_destroy(q);
}

void testElementLimitDropNewest() {
    testDiag("--- element limit, policy=newest ---");
    GateClientQueue* q = fresh_queue();
    set_limits(3, 0, GATE_Q_DROP_NEWEST);

    int id[5];
    for (int i = 0; i < 5; i++) push(q, 8, NULL, &id[i]);

    testOk(depth(q) == 3, "capped at 3 entries (got %zu)", depth(q));
    testOk(g_qDroppedNewest.load() == 2, "2 dropped as newest (got %llu)", g_qDroppedNewest.load());
    testOk(g_qDroppedOldest.load() == 0, "none dropped as oldest");

    std::vector<void*> c = contents(q);
    testOk(c.size() == 3 && c[0] == &id[0] && c[1] == &id[1] && c[2] == &id[2],
           "the three oldest entries survived");
    gate_queue_destroy(q);
}

void testByteLimitDropOldest() {
    testDiag("--- byte limit, policy=oldest ---");
    GateClientQueue* q = fresh_queue();
    set_limits(0, 100, GATE_Q_DROP_OLDEST);

    int id[4];
    for (int i = 0; i < 4; i++) push(q, 40, NULL, &id[i]);   // 40+40 fits; the 3rd forces eviction

    testOk(q->bytes <= 100, "bytes stay within the 100-byte cap (got %zu)", q->bytes);
    testOk(depth(q) == 2, "two 40-byte entries fit (got %zu)", depth(q));
    testOk(g_qDroppedOldest.load() == 2, "2 evicted from the front (got %llu)",
           g_qDroppedOldest.load());
    std::vector<void*> c = contents(q);
    testOk(c.size() == 2 && c[0] == &id[2] && c[1] == &id[3], "newest two survived");
    gate_queue_destroy(q);
}

void testByteLimitDropNewest() {
    testDiag("--- byte limit, policy=newest ---");
    GateClientQueue* q = fresh_queue();
    set_limits(0, 100, GATE_Q_DROP_NEWEST);

    for (int i = 0; i < 4; i++) push(q, 40);

    testOk(depth(q) == 2, "two 40-byte entries fit (got %zu)", depth(q));
    testOk(q->bytes == 80, "bytes = 80 (got %zu)", q->bytes);
    testOk(g_qDroppedNewest.load() == 2, "2 incoming rejected (got %llu)",
           g_qDroppedNewest.load());
    gate_queue_destroy(q);
}

void testOversizedEntry() {
    testDiag("--- a single entry larger than the byte cap ---");
    // Documented behaviour: under drop-oldest an oversized entry is still admitted once the
    // queue is empty, because dropping it outright would mean that PV could never reach this
    // client at all. Under drop-newest it is simply rejected.
    GateClientQueue* q = fresh_queue();
    set_limits(0, 10, GATE_Q_DROP_OLDEST);
    push(q, 40);
    testOk(depth(q) == 1, "oversized entry admitted under drop-oldest (got %zu)", depth(q));
    testOk(g_qDroppedOldest.load() == 0, "and nothing counted as dropped");
    gate_queue_destroy(q);

    q = fresh_queue();
    set_limits(0, 10, GATE_Q_DROP_NEWEST);
    push(q, 40);
    testOk(depth(q) == 0, "oversized entry rejected under drop-newest (got %zu)", depth(q));
    testOk(g_qDroppedNewest.load() == 1, "and counted as dropped (got %llu)",
           g_qDroppedNewest.load());
    gate_queue_destroy(q);
}

void testLimitsAppliedLive() {
    testDiag("--- limit changes apply to an existing queue ---");
    GateClientQueue* q = fresh_queue();
    set_limits(0, 0, GATE_Q_DROP_OLDEST);
    for (int i = 0; i < 5; i++) push(q, 8);
    testOk(depth(q) == 5, "5 queued while unlimited (got %zu)", depth(q));

    // Tightening the cap does not retroactively trim, but the next push enforces it.
    set_limits(3, 0, GATE_Q_DROP_OLDEST);
    push(q, 8);
    testOk(depth(q) == 3, "next push enforces the new cap (got %zu)", depth(q));
    gate_queue_destroy(q);
}

void testPurgeOwner() {
    testDiag("--- purging one subscription's entries ---");
    GateClientQueue* q = fresh_queue();
    set_limits(0, 0, GATE_Q_DROP_OLDEST);

    int ownerA = 0, ownerB = 0;
    int id[4];
    push(q, 8,  &ownerA, &id[0]);
    push(q, 16, &ownerB, &id[1]);
    push(q, 8,  &ownerA, &id[2]);
    push(q, 32, &ownerB, &id[3]);
    testOk(depth(q) == 4 && q->bytes == 64, "4 entries, 64 bytes before purge");

    gate_queue_purge_owner(q, &ownerA);

    testOk(depth(q) == 2, "ownerA's entries gone (got %zu)", depth(q));
    testOk(q->bytes == 48, "bytes reduced to ownerB's 16+32 (got %zu)", q->bytes);
    std::vector<void*> c = contents(q);
    testOk(c.size() == 2 && c[0] == &id[1] && c[1] == &id[3], "only ownerB's entries remain");
    testOk(g_qDepth.load() == 2 && g_qBytes.load() == 48, "global accounting follows the purge");
    gate_queue_destroy(q);
}

void testDelivery() {
    testDiag("--- delivery through a started reader thread ---");
    GateClientQueue* q = fresh_queue();
    set_limits(0, 0, GATE_Q_DROP_OLDEST);
    gate_queue_start(q, "gateQTest1", epicsThreadPriorityLow);

    int id[3];
    for (int i = 0; i < 3; i++) push(q, 8, NULL, &id[i]);

    size_t got = wait_for_deliveries(3);
    testOk(got == 3, "all three entries delivered (got %zu)", got);
    {
        epicsGuard<epicsMutex> guard(g_deliveredLock);
        testOk(g_delivered.size() == 3 &&
               g_delivered[0].user_arg == &id[0] &&
               g_delivered[1].user_arg == &id[1] &&
               g_delivered[2].user_arg == &id[2], "delivered in FIFO order");
        testOk(g_delivered[0].pfl != NULL, "payload pointer handed to the callback");
    }
    testOk(depth(q) == 0, "queue drained (got %zu)", depth(q));
    testOk(g_qDelivered.load() == 3, "delivered counter = 3 (got %llu)", g_qDelivered.load());
    testOk(g_qDepth.load() == 0 && g_qBytes.load() == 0, "global depth/bytes back to 0");

    gate_queue_destroy(q);
}

void testFlowControl() {
    testDiag("--- flow control holds delivery, then releases it ---");
    GateClientQueue* q = fresh_queue();
    set_limits(0, 0, GATE_Q_DROP_OLDEST);
    gate_queue_start(q, "gateQTest2", epicsThreadPriorityLow);

    gate_queue_set_flow_ctrl(q, 1);           // CA_PROTO_EVENTS_OFF
    for (int i = 0; i < 3; i++) push(q, 8);

    // Give the reader a chance to (wrongly) run; it must not deliver while paused.
    epicsThreadSleep(0.2);
    testOk(delivered_count() == 0, "nothing delivered while paused (got %zu)", delivered_count());
    testOk(depth(q) == 3, "entries still queued while paused (got %zu)", depth(q));

    gate_queue_set_flow_ctrl(q, 0);           // CA_PROTO_EVENTS_ON
    size_t got = wait_for_deliveries(3);
    testOk(got == 3, "all three delivered after resume (got %zu)", got);

    gate_queue_destroy(q);
}

void testPayloadLifetime() {
    testDiag("--- queued payload outlives the caller's reference ---");
    GateClientQueue* q = fresh_queue();
    set_limits(0, 0, GATE_Q_DROP_OLDEST);

    // The cache routinely replaces its shared_ptr with a newer value while older events are
    // still queued; the queue's own reference must keep the payload alive until delivery.
    std::shared_ptr<int> payload(new int(4242));
    std::weak_ptr<int> watch(payload);

    GateClientQueue::Entry e;
    e.data = payload;
    e.cb = record_delivery;
    e.user_arg = NULL;
    e.dbchan = NULL;
    e.owner = NULL;
    e.bytes = sizeof(int);
    gate_queue_push(q, std::move(e));

    payload.reset();                          // caller drops its reference
    testOk(!watch.expired(), "payload still alive, held by the queue");
    testOk(depth(q) == 1, "entry still queued");

    gate_queue_destroy(q);
    testOk(watch.expired(), "payload released once the queue let go");
}

} // namespace

MAIN(gateQueueTest)
{
    testPlan(51);

    testPushAccounting();
    testUnlimited();
    testElementLimitDropOldest();
    testElementLimitDropNewest();
    testByteLimitDropOldest();
    testByteLimitDropNewest();
    testOversizedEntry();
    testLimitsAppliedLive();
    testPurgeOwner();
    testDelivery();
    testFlowControl();
    testPayloadLifetime();

    return testDone();
}
