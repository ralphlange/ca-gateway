#include <cstdlib>
#include <cstring>
#include <string>
#include <memory>
#include "epicsVersion.h"
#include "dbAccess.h"
#include "dbEvent.h"
#include "dbServer.h"
#include "asDbLib.h"
#include "db_access_routines.h"
#include <caeventmask.h>
#include "gate_db_access.h"
#include "gateDbChannel.h"
#include "GateCache.h"
#include "GateVirtualPV.h"
#include "GateConfig.h"
#include "GateCAClient.h"
#include "gate_compat.h"

extern "C" {
struct dbBase *pdbbase = nullptr;
volatile int interruptAccept = 1;
int dbAccessDebugPUTF = 0;
unsigned short dbDBRnewToDBRold[] = {0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20};

struct dbChannel * dbChannel_create(const char *name) {
    std::string clientName;
    if (!GateConfig::instance().isAllowed(name, "anonymous", "localhost", clientName)) return nullptr;
    dbChannelGate *gchan = new dbChannelGate();
    gchan->chan.name = strdup(name);
    gchan->cacheEntry = GateCache::instance().findOrCreate(name);
    gchan->chan.addr.precord = (struct dbCommon *)calloc(1, sizeof(struct dbCommon));
    strncpy((char*)gchan->chan.addr.precord->name, name, 61);
    ellInit(&gchan->chan.addr.precord->mlis);
    gchan->chan.addr.no_elements = 1;
    gchan->chan.addr.field_type = 10;
    gchan->chan.addr.field_size = sizeof(double);
    gchan->chan.addr.pfield = nullptr;
    gchan->chan.addr.dbr_field_type = 8;
    gchan->chan.final_no_elements = 1;
    gchan->chan.final_field_size = sizeof(double);
    gchan->chan.final_type = 8;
    auto client = GateCAClientManager::instance().getClient(clientName);
    if (client) client->connect(name, DBE_VALUE | DBE_ALARM | DBE_LOG);
    return (struct dbChannel *)gchan;
}

#if EPICS_VERSION >= 7
void dbChannelDelete(dbChannel *chan) {
    if (chan) {
        dbChannelGate* gchan = (dbChannelGate*)chan;
        free((void *)gchan->chan.name);
        free(gchan->chan.addr.precord);
        delete gchan;
    }
}
void dbChannelShow(dbChannel *chan, int level, unsigned short indent) {}
#else
void dbChannelDelete(struct dbChannel *chan) {
    if (chan) {
        dbChannelGate* gchan = (dbChannelGate*)chan;
        free((void *)gchan->chan.name);
        free(gchan->chan.addr.precord);
        delete gchan;
    }
}
void dbChannelShow(struct dbChannel *chan, int level, int indent) {}
#endif

typedef long (*GETCONVERTFUNC)(const struct dbAddr *paddr, void *pbuffer, long nRequest, long no_elements, long offset);
extern GETCONVERTFUNC dbGetConvertRoutine[14][10];

int dbChannel_get_count(struct dbChannel *chan, int buffer_type, void *pbuffer, long *nRequest, void *pfl) {
    dbChannelGate* gchan = (dbChannelGate*)chan;
    auto data = gchan->cacheEntry->getData();
    if (!data) return -1;
    dbAddr addr = chan->addr;
    addr.field_type = (short)dbr_type_to_DBF(data->dbrType);
    addr.pfield = (void*)((char*)data->data.data() + dbr_size[data->dbrType] - dbr_value_size[data->dbrType]);
    addr.no_elements = data->count;
    if (addr.field_type <= 13 && buffer_type <= 9) {
        GETCONVERTFUNC convert = dbGetConvertRoutine[addr.field_type][buffer_type];
        if (convert) {
            convert(&addr, pbuffer, *nRequest, addr.no_elements, 0);
            return 0;
        }
    }
    return -1;
}

int dbChannel_get(struct dbChannel *chan, int buffer_type, void *pbuffer, long no_elements, void *pfl) {
    long nRequest = no_elements;
    return dbChannel_get_count(chan, buffer_type, pbuffer, &nRequest, pfl);
}

int dbChannel_put(struct dbChannel *chan, int src_type, const void *psrc, long no_elements) {
    std::string clientName;
    if (GateConfig::instance().isAllowed(chan->name, "anonymous", "localhost", clientName)) {
        auto client = GateCAClientManager::instance().getClient(clientName);
        if (client) {
            client->put(chan->name, src_type, psrc, no_elements);
            return 0;
        }
    }
    return -1;
}

#if EPICS_VERSION >= 7
int db_put_process(struct processNotify *ppn, notifyPutType type, int dbrType, const void *pbuffer, int nRequest) {
    int status = dbChannel_put(ppn->chan, dbrType, pbuffer, nRequest);
    if(ppn->doneCallback) ppn->doneCallback(ppn);
    return status;
}
#else
int db_put_process(struct processNotify *ppn, int type, int dbrType, const void *pbuffer, long nRequest) {
    int status = dbChannel_put(ppn->chan, dbrType, pbuffer, nRequest);
    if(ppn->doneCallback) ppn->doneCallback(ppn);
    return status;
}
#endif

struct dbEventSubscriptionInternal {
    GateSubscription gateSub;
    std::shared_ptr<GateCacheEntry> entry;
};

dbEventSubscription db_add_event(dbEventCtx ctx, struct dbChannel *chan, EVENTFUNC *user_sub, void *user_arg, unsigned int select) {
    dbChannelGate* gchan = (dbChannelGate*)chan;
    dbEventSubscriptionInternal *sub = new dbEventSubscriptionInternal;
    sub->gateSub.callback = (GateEventFunc*)user_sub;
    sub->gateSub.user_arg = user_arg;
    sub->gateSub.chan = chan;
    sub->gateSub.mask = select;
    sub->entry = gchan->cacheEntry;
    sub->entry->addSubscription(&sub->gateSub);
    std::string clientName;
    if (GateConfig::instance().isAllowed(chan->name, "anonymous", "localhost", clientName)) {
        auto client = GateCAClientManager::instance().getClient(clientName);
        if (client) client->connect(chan->name, select);
    }
    return (dbEventSubscription)sub;
}

void db_cancel_event(dbEventSubscription sub) {
    if (sub) {
        dbEventSubscriptionInternal* isub = (dbEventSubscriptionInternal*)sub;
        isub->entry->removeSubscription(&isub->gateSub);
        delete isub;
    }
}

dbEventCtx db_init_events(void) { return (dbEventCtx)1; }
int db_start_events(dbEventCtx ctx, const char *taskname, void (*init_func)(void *), void *init_func_arg, unsigned osiPriority) {
    if (init_func) init_func(init_func_arg);
    return 0;
}
void db_close_events(dbEventCtx ctx) {}
void db_event_flow_ctrl_mode_on(dbEventCtx ctx) {}
void db_event_flow_ctrl_mode_off(dbEventCtx ctx) {}
void db_event_change_priority(dbEventCtx ctx, unsigned int priority) {}
void db_event_enable(dbEventSubscription sub) {}
void db_event_disable(dbEventSubscription sub) {}
void db_post_single_event(dbEventSubscription sub) {}
#if EPICS_VERSION >= 7
int db_post_extra_labor(dbEventCtx ctx) { return 0; }
#else
void db_post_extra_labor(dbEventCtx ctx) {}
#endif
int db_add_extra_labor_event(dbEventCtx ctx, void (*func)(void *), void *arg) { return 0; }
void db_flush_extra_labor_event(dbEventCtx ctx) {}
int asDbGetAsl(struct dbChannel *chan) { return 0; }
void * asDbGetMemberPvt(struct dbChannel *chan) { return nullptr; }
unsigned long dbLockGetLockId(struct dbCommon *precord) { return 0; }
void dbProcessNotify(struct processNotify *ppn) {
#if EPICS_VERSION >= 7
    db_put_process(ppn, putType, 0, nullptr, 0);
#else
    db_put_process(ppn, 0, 0, nullptr, 0);
#endif
}
void dbNotifyCancel(struct processNotify *ppn) {}
long dbChannelTest(const char *name) {
    std::string clientName;
    return GateConfig::instance().isAllowed(name, "anonymous", "localhost", clientName) ? 0 : -1;
}
int dbRegisterServer(dbServer *psrv) {
    if (psrv) {
        if (psrv->init) psrv->init();
        if (psrv->run) psrv->run();
    }
    return 0;
}
void recGblRecordError(long status, void *precord, const char *pmsg) {}
void recGblRecSupError(long status, void *paddr, const char *pmsg, const char *proutine) {}
void recGblDbaddrError(long status, const struct dbAddr *paddr, const char *pmsg) {}
#if EPICS_VERSION >= 7
rset * dbGetRset(const struct dbAddr *paddr) { return nullptr; }
#else
void * dbGetRset(const struct dbAddr *paddr) { return nullptr; }
#endif
void dbScanLock(struct dbCommon *prec) {}
void dbScanUnlock(struct dbCommon *prec) {}
}
