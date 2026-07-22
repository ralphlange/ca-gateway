#include <stdlib.h>
#include <string.h>
#include <stddef.h>
#include "gate_compat.h"
#include "gate_db_interface.h"

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

long dbNameToAddr(const char *pname, struct dbAddr *paddr) {
    void* gh = gate_create_channel(pname);
    if (!gh) return -1;
    memset(paddr, 0, sizeof(*paddr));
    paddr->no_elements = 1;
    paddr->field_type = G_S_DBF_DOUBLE;
    paddr->field_size = sizeof(double);
    paddr->dbr_field_type = DBR_DOUBLE;
    return 0;
}

struct dbChannel * dbChannel_create(const char *name) {
    void* gh = gate_create_channel(name);
    if (!gh) return NULL;
    struct dbChannelGate *gchan = (struct dbChannelGate *)calloc(1, sizeof(struct dbChannelGate));
    gchan->gateHandle = gh;
    strncpy(gchan->name, name, sizeof(gchan->name)-1);
    gchan->chan.name = gchan->name;
    gchan->chan.final_no_elements = 1;
    gchan->chan.final_type = DBR_DOUBLE;
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
    return gate_get_count(gchan->gateHandle, buffer_type, pbuffer, nRequest);
}

int dbChannel_get(struct dbChannel *chan, int buffer_type, void *pbuffer, long no_elements, void *pfl) {
    long nRequest = no_elements;
    return dbChannel_get_count(chan, buffer_type, pbuffer, &nRequest, pfl);
}

int dbChannel_put(struct dbChannel *chan, int src_type, const void *psrc, long no_elements) {
    struct dbChannelGate *gchan = (struct dbChannelGate *)((char*)chan - offsetof(struct dbChannelGate, chan));
    return gate_put(gchan->gateHandle, src_type, psrc, no_elements);
}

dbEventCtx db_init_events(void) { return (dbEventCtx)1; }

dbEventSubscription db_add_event(dbEventCtx ctx, struct dbChannel *chan, EVENTFUNC* user_sub, void *user_arg, unsigned int select) {
    struct dbChannelGate *gchan = (struct dbChannelGate *)((char*)chan - offsetof(struct dbChannelGate, chan));
    return (dbEventSubscription)gate_add_event(gchan->gateHandle, (gate_event_callback*)user_sub, user_arg, select);
}

void db_cancel_event(dbEventSubscription sub) { gate_cancel_event((void*)sub); }
void db_close_events(dbEventCtx ctx) {}
void db_event_enable(dbEventSubscription sub) {}
void db_event_disable(dbEventSubscription sub) {}
void db_post_single_event(dbEventSubscription sub) {}
int db_post_extra_labor(dbEventCtx ctx) { return 0; }
/*
 * dbEventCtx here is always the fake, non-NULL handle from db_init_events() above, not a
 * real event_user*. The real dbEvent.c versions of these dereference ctx and crash; since
 * db_post_extra_labor()/db_close_events() are already no-ops above (nothing ever actually
 * queues/delivers the extra-labor callback or changes priority), these are safe no-ops too.
 */
int db_add_extra_labor_event(dbEventCtx ctx, EXTRALABORFUNC *func, void *arg) { return DB_EVENT_OK; }
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
