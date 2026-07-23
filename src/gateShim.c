#include <stdlib.h>
#include <string.h>
#include <stddef.h>
#include "gate_compat.h"
#include "gate_db_interface.h"
/* For rsrvCurrentClient / struct client (pHostName) -- see gate_current_client_hostname()
 * below. This is rsrv's own per-client-thread identity, already set up (unmodified,
 * vendored) by casAttachThreadToClient()/host_name_action() in caservertask.c/camessage.c. */
#include "server.h"

struct dbBase dummy_dbbase;
struct dbBase *pdbbase = &dummy_dbbase;
volatile int interruptAccept = 1;
dbServer *rsrv_psrv = NULL;

struct dbChannelGate {
    struct dbChannel chan;
    void* gateHandle;
    char name[64];
};

int dbRegisterServer(dbServer *psrv) {
    rsrv_psrv = psrv;
    return 0;
}

/*
 * rsrv's UDP search-reply handler (search_reply_udp() in camessage.c) calls the real
 * dbChannelTest() directly -- unlike dbNameToAddr/dbChannel_create below, it never goes
 * through a dbAddr/dbChannel at all. The real dbChannelTest() does its own static-database
 * lookup against pdbbase (dbFindRecordPart -> dbPvdFind), which segfaults here since
 * dummy_dbbase has no process-variable directory. Overriding it is required for ANY CA
 * client's normal UDP "who has this PV" search to work at all (returns 0 = found, matching
 * the real function's convention).
 */
long dbChannelTest(const char *pname) {
    return gate_channel_exists(pname) ? 0 : -1;
}

/*
 * Fills in a dbAddr for gh's upstream native type/count. Used both for the plain dbAddr
 * dbNameToAddr() returns AND for struct dbChannel's embedded .addr member below --
 * read_reply() in camessage.c (rsrv's real, unmodified monitor/get-reply code) reads
 * dbch->addr.no_elements directly to size the outgoing message buffer; leaving it
 * zero-initialized (as plain calloc would) undersizes that buffer for every real event,
 * corrupting the reply once dbChannel_get_count() (gate_get_count()) writes the real
 * element count into it -- caServerIO's cas_commit_msg() catches the mismatch via
 * assert(size <= ntohs(pMsg->m_postsize)) and aborts the process.
 */
static void gate_fill_addr(struct dbAddr *paddr, void *gh) {
    memset(paddr, 0, sizeof(*paddr));
    int ca_type = gate_native_ca_type(gh);
    if (ca_type < 0) ca_type = DBR_DOUBLE; /* not connected yet: reasonable fallback */
    paddr->no_elements = gate_native_count(gh);
    /* field_type/dbr_field_type feed real, unshimmed Base macros (dbChannelExportType/
     * dbChannelExportCAType, camessage.c) that index Base's own dbDBRnewToDBRold[] table --
     * they need the real, per-version dbFldTypes.h ordering, not gate_dbr_to_dbf()'s portable
     * one (see gate_compat.h's gate_dbf_to_real_dbf() comment). */
    paddr->field_type = (short)gate_dbf_to_real_dbf(gate_dbr_to_dbf(ca_type));
    paddr->field_size = dbr_value_size[ca_type];
    paddr->dbr_field_type = paddr->field_type;
}

/*
 * Recovers the requesting client's self-reported CA hostname (from the CA_PROTO_HOST_NAME
 * message, processed by host_name_action() in camessage.c) for use by DENY-FROM route
 * matching (see gate_create_channel_for_client() in GateLogic.cpp). dbNameToAddr()/
 * dbChannel_create() below run synchronously on the same per-TCP-client thread that already
 * ran host_name_action() for this client (one thread per client, camsgtask()), so
 * rsrvCurrentClient (set by casAttachThreadToClient(), caservertask.c) already reflects the
 * right client here -- no changes to any vendored file needed. Returns NULL (treated as
 * "unknown host", DENY FROM never matches) if called from a thread with no attached client,
 * e.g. any caller other than the normal per-client TCP message loop.
 */
static const char* gate_current_client_hostname(void) {
    struct client *c;
    if (!rsrvCurrentClient) return NULL;
    c = (struct client *) epicsThreadPrivateGet(rsrvCurrentClient);
    if (!c || !c->pHostName || !c->pHostName[0]) return NULL;
    return c->pHostName;
}

long dbNameToAddr(const char *pname, struct dbAddr *paddr) {
    void* gh = gate_create_channel_for_client(pname, gate_current_client_hostname());
    if (!gh) return -1;
    gate_wait_channel_ready(gh);
    gate_fill_addr(paddr, gh);
    return 0;
}

struct dbChannel * dbChannel_create(const char *name) {
    void* gh = gate_create_channel_for_client(name, gate_current_client_hostname());
    if (!gh) return NULL;
    gate_wait_channel_ready(gh);
    struct dbChannelGate *gchan = (struct dbChannelGate *)calloc(1, sizeof(struct dbChannelGate));
    gchan->gateHandle = gh;
    strncpy(gchan->name, name, sizeof(gchan->name)-1);
    gchan->chan.name = gchan->name;
    gate_fill_addr(&gchan->chan.addr, gh);
    gchan->chan.final_no_elements = gchan->chan.addr.no_elements;
    gchan->chan.final_type = gchan->chan.addr.field_type;
    return &gchan->chan;
}

void dbChannelDelete(struct dbChannel *chan) {
    struct dbChannelGate *gchan = (struct dbChannelGate *)((char*)chan - offsetof(struct dbChannelGate, chan));
    gate_delete_channel(gchan->gateHandle);
    free(gchan);
}

void dbChannelShow(dbChannel *chan, int level, unsigned short indent) {}

int dbChannel_get_count(struct dbChannel *chan, int buffer_type, void *pbuffer, long *nRequest, void *pfl) {
    struct dbChannelGate *gchan = (struct dbChannelGate *)((char*)chan - offsetof(struct dbChannelGate, chan));
    return gate_get_count(gchan->gateHandle, buffer_type, pbuffer, nRequest, pfl);
}

int dbChannel_get(struct dbChannel *chan, int buffer_type, void *pbuffer, long no_elements, void *pfl) {
    long nRequest = no_elements;
    return dbChannel_get_count(chan, buffer_type, pbuffer, &nRequest, pfl);
}

int dbChannel_put(struct dbChannel *chan, int src_type, const void *psrc, long no_elements) {
    struct dbChannelGate *gchan = (struct dbChannelGate *)((char*)chan - offsetof(struct dbChannelGate, chan));
    return gate_put(gchan->gateHandle, src_type, psrc, no_elements);
}

/*
 * Used by write_notify_action() (camessage.c) in place of the real dbProcessNotify(),
 * so a downstream put-with-callback (wait=True) only gets its completion reply once the
 * upstream IOC has genuinely acknowledged the write.
 */
void dbChannel_put_notify(struct dbChannel *chan, int buffer_type, const void *pbuffer,
                          long no_elements, gate_put_notify_callback *cb, void *user_arg) {
    struct dbChannelGate *gchan = (struct dbChannelGate *)((char*)chan - offsetof(struct dbChannelGate, chan));
    gate_put_notify(gchan->gateHandle, buffer_type, pbuffer, no_elements, cb, user_arg);
}

/*
 * A real dbEventCtx runs a background "event task" thread that drains queued work
 * (single-event callbacks, and the "extra labor" queue used for e.g. put-notify
 * completion replies -- see rsrv_extra_labor() in camessage.c) without blocking the
 * database. We have no database to protect and no such thread; db_start_events()
 * already reflects this by invoking its init_func synchronously instead of spawning a
 * task. For the same reason, db_post_extra_labor() below invokes the registered
 * extra-labor callback (rsrv_extra_labor) synchronously, right on the calling thread,
 * instead of queuing it for a background task that doesn't exist -- so it needs a
 * real per-client struct (not the old shared fake `1` handle) to remember which
 * function/arg db_add_extra_labor_event() registered.
 */
struct GateEventCtx {
    EXTRALABORFUNC *extraLaborFunc;
    void *extraLaborArg;
};

dbEventCtx db_init_events(void) {
    struct GateEventCtx *ctx = (struct GateEventCtx *)calloc(1, sizeof(struct GateEventCtx));
    return (dbEventCtx)ctx;
}

dbEventSubscription db_add_event(dbEventCtx ctx, struct dbChannel *chan, EVENTFUNC* user_sub, void *user_arg, unsigned int select) {
    struct dbChannelGate *gchan = (struct dbChannelGate *)((char*)chan - offsetof(struct dbChannelGate, chan));
    /* user_sub is really rsrv's read_reply(pArg, dbChannel*, eventsRemaining, db_field_log*);
     * it must be invoked with the real dbChannel* (chan) -- NOT our internal handle -- since
     * read_reply() calls dbChannel_get_count(dbch, ...), which does pointer arithmetic on
     * dbch assuming it's really the `chan` member embedded in a dbChannelGate. */
    return (dbEventSubscription)gate_add_event(gchan->gateHandle, (gate_event_callback*)user_sub, user_arg, chan, select);
}

void db_cancel_event(dbEventSubscription sub) { gate_cancel_event((void*)sub); }
void db_close_events(dbEventCtx ctx) { free(ctx); }
void db_event_enable(dbEventSubscription sub) {}
void db_event_disable(dbEventSubscription sub) {}
void db_post_single_event(dbEventSubscription sub) {}
int db_post_extra_labor(dbEventCtx ctx) {
    struct GateEventCtx *gctx = (struct GateEventCtx *)ctx;
    if (gctx && gctx->extraLaborFunc) gctx->extraLaborFunc(gctx->extraLaborArg);
    return 0;
}
/*
 * dbEventCtx here is always the real per-client GateEventCtx* from db_init_events()
 * above, not a real event_user* -- the real dbEvent.c versions of these dereference a
 * different struct entirely and would crash. db_event_enable/disable and
 * db_post_single_event are safe no-ops since nothing in gate_add_event()'s delivery
 * path checks an enabled/disabled flag.
 */
int db_add_extra_labor_event(dbEventCtx ctx, EXTRALABORFUNC *func, void *arg) {
    struct GateEventCtx *gctx = (struct GateEventCtx *)ctx;
    if (gctx) {
        gctx->extraLaborFunc = func;
        gctx->extraLaborArg = arg;
    }
    return DB_EVENT_OK;
}
void db_flush_extra_labor_event(dbEventCtx ctx) {}
void db_event_flow_ctrl_mode_on(dbEventCtx ctx) {}
void db_event_flow_ctrl_mode_off(dbEventCtx ctx) {}
void db_event_change_priority(dbEventCtx ctx, unsigned epicsPriority) {}
int db_start_events(dbEventCtx ctx, const char *taskname, void (*init_func)(void *), void *init_func_arg, unsigned osiPriority) {
    if (init_func) init_func(init_func_arg);
    return 0;
}

rset * dbGetRset(const struct dbAddr *paddr) { return NULL; }
void dbScanLock(struct dbCommon *prec) {}
void dbScanUnlock(struct dbCommon *prec) {}
void recGblDbaddrError(long status, const struct dbAddr *paddr, const char *pmsg) {}

void* asDbGetMemberPvt(struct dbChannel *chan) {
    struct dbChannelGate *gchan = (struct dbChannelGate *)((char*)chan - offsetof(struct dbChannelGate, chan));
    return gate_get_as_member(gchan->gateHandle);
}
int asDbGetAsl(struct dbChannel *chan) { return 0; }

const ENV_PARAM gate_mcast_ttl = { (char*)"EPICS_CA_MCAST_TTL", (char*)"32" };
const ENV_PARAM gate_auto_array_bytes = { (char*)"EPICS_CA_AUTO_ARRAY_BYTES", (char*)"YES" };
int gate_asCheckClientIP = 0;
