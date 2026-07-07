#include <cstdlib>
#include <cstring>
#include "db_access.h"
#include "dbAccess.h"
#include "dbEvent.h"
#include "asDbLib.h"
#include "db_access_routines.h"
#include "dbConvert.h"
#include <caeventmask.h>
#include "gateDbChannel.h"
#include "GateCache.h"
#include "GateVirtualPV.h"
#include "GateConfig.h"
#include "GateCAClient.h"

extern "C" {

struct dbBase *pdbbase = nullptr;
volatile int interruptAccept = 1;
int dbAccessDebugPUTF = 0;

unsigned short dbDBRnewToDBRold[] = {0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20};
size_t dbr_size[] = {61, 2, 2, 2, 2, 4, 8, 0, 0, 0, 0, 0, 0, 0, 61+16, 16+16, 16+16, 16+16, 16+16, 16+16, 16+16};
size_t dbr_value_size[] = {61, 2, 2, 2, 2, 4, 8, 0, 0, 0, 0, 0, 0, 0, 61, 2, 2, 2, 2, 4, 8};

struct dbChannel * dbChannel_create(const char *name) {
    std::string clientName;
    if (!GateConfig::instance().isAllowed(name, clientName)) return nullptr;
    dbChannelGate *chan = new dbChannelGate();
    chan->name = strdup(name);
    chan->cacheEntry = GateCache::instance().findOrCreate(name);
    chan->addr.precord = (struct dbCommon *)calloc(1, sizeof(struct dbCommon));
    strncpy(chan->addr.precord->name, name, PVNAME_SZ);
    ellInit(&chan->addr.precord->mlis);
    chan->addr.no_elements = 1;
    chan->addr.field_type = DBF_DOUBLE;
    chan->addr.field_size = sizeof(double);
    chan->addr.pfield = nullptr;
    chan->addr.dbr_field_type = DBR_DOUBLE;
    chan->final_no_elements = 1;
    chan->final_field_size = sizeof(double);
    chan->final_type = DBR_DOUBLE;
    auto client = GateCAClientManager::instance().getClient(clientName);
    if (client) client->connect(name, DBE_VALUE | DBE_ALARM | DBE_LOG);
    return (struct dbChannel *)chan;
}

void dbChannelDelete(struct dbChannel *chan) {
    if (chan) {
        dbChannelGate* gchan = (dbChannelGate*)chan;
        free((void *)gchan->name);
        free(gchan->addr.precord);
        delete gchan;
    }
}

int dbChannel_get_count(struct dbChannel *chan, int buffer_type,
                        void *pbuffer, long *nRequest, void *pfl) {
    dbChannelGate* gchan = (dbChannelGate*)chan;
    auto data = gchan->cacheEntry->getData();
    if (!data) return -1;
    dbAddr addr = chan->addr;
    addr.field_type = dbr_type_to_DBF(data->dbrType);
    addr.pfield = (void*)((char*)data->data.data() + dbr_size[data->dbrType] - dbr_value_size[data->dbrType]);
    addr.no_elements = data->count;
    if (addr.field_type <= DBF_DEVICE && buffer_type <= DBR_ENUM) {
        GETCONVERTFUNC convert = dbGetConvertRoutine[addr.field_type][buffer_type];
        if (convert) {
            convert(&addr, pbuffer, *nRequest, addr.no_elements, 0);
            return 0;
        }
    }
    return -1;
}

int dbChannel_get(struct dbChannel *chan, int buffer_type,
                  void *pbuffer, long no_elements, void *pfl) {
    long nRequest = no_elements;
    return dbChannel_get_count(chan, buffer_type, pbuffer, &nRequest, pfl);
}

int dbChannel_put(struct dbChannel *chan, int src_type,
                  const void *psrc, long no_elements) {
    std::string clientName;
    if (GateConfig::instance().isAllowed(chan->name, clientName)) {
        auto client = GateCAClientManager::instance().getClient(clientName);
        if (client) {
            client->put(chan->name, src_type, psrc, no_elements);
            return 0;
        }
    }
    return -1;
}

int db_put_process(struct processNotify *ppn, int type,
    int dbrType, const void *pbuffer, long nRequest) {
    int status = dbChannel_put(ppn->chan, dbrType, pbuffer, nRequest);
    if(ppn->doneCallback) ppn->doneCallback(ppn);
    return status;
}

struct dbEventSubscriptionInternal {
    GateSubscription gateSub;
    std::shared_ptr<GateCacheEntry> entry;
};

dbEventSubscription db_add_event(dbEventCtx ctx, struct dbChannel *chan,
                                 EVENTFUNC *user_sub, void *user_arg,
                                 unsigned int select) {
    dbChannelGate* gchan = (dbChannelGate*)chan;
    dbEventSubscriptionInternal *sub = new dbEventSubscriptionInternal;
    sub->gateSub.callback = (GateEventFunc*)user_sub;
    sub->gateSub.user_arg = user_arg;
    sub->gateSub.chan = chan;
    sub->gateSub.mask = select;
    sub->entry = gchan->cacheEntry;
    sub->entry->addSubscription(&sub->gateSub);
    std::string clientName;
    if (GateConfig::instance().isAllowed(chan->name, clientName)) {
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
int db_start_events(dbEventCtx ctx, const char *taskname, void (*init_func)(void *),
    void *init_func_arg, unsigned osiPriority) {
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
void db_post_extra_labor(dbEventCtx ctx) {}
int db_add_extra_labor_event(dbEventCtx ctx, void (*func)(void *), void *arg) { return 0; }
void db_flush_extra_labor_event(dbEventCtx ctx) {}
int asDbGetAsl(struct dbChannel *chan) { return 0; }
void * asDbGetMemberPvt(struct dbChannel *chan) { return nullptr; }
unsigned long dbLockGetLockId(struct dbCommon *precord) { return 0; }
void dbProcessNotify(struct processNotify *ppn) {
    db_put_process(ppn, 0, 0, nullptr, 0); // Trigger put and done
}
void dbNotifyCancel(struct processNotify *ppn) {}
void dbChannelShow(struct dbChannel *chan, int level, int indent) {}
long dbChannelTest(const char *name) {
    std::string clientName;
    return GateConfig::instance().isAllowed(name, clientName) ? 1 : 0;
}
void dbRegisterServer(void (*start)(void *), void *arg) { if(start) start(arg); }
void recGblRecordError(long status, void *precord, const char *pmsg) {}
void recGblRecSupError(long status, void *paddr, const char *pmsg, const char *proutine) {}
void recGblDbaddrError(long status, const struct dbAddr *paddr, const char *pmsg) {}
void * dbGetRset(const struct dbAddr *paddr) { return nullptr; }

}
