#ifndef INCLdbEventh
#define INCLdbEventh
#include "dbChannel.h"
#ifdef __cplusplus
extern "C" {
#endif
struct db_field_log;
typedef void EVENTFUNC(void *user_arg, struct dbChannel *chan,
    int eventsRemaining, struct db_field_log *pfl);
typedef void * dbEventSubscription;
typedef void * dbEventCtx;
#define DB_EVENT_OK 0
#define DB_EVENT_ERROR -1
dbEventCtx db_init_events(void);
int db_start_events(dbEventCtx ctx, const char *taskname, void (*init_func)(void *),
    void *init_func_arg, unsigned osiPriority);
void db_close_events(dbEventCtx ctx);
dbEventSubscription db_add_event(dbEventCtx ctx, struct dbChannel *chan,
    EVENTFUNC *user_sub, void *user_arg, unsigned int select);
void db_cancel_event(dbEventSubscription sub);
void db_event_flow_ctrl_mode_on(dbEventCtx ctx);
void db_event_flow_ctrl_mode_off(dbEventCtx ctx);
void db_event_change_priority(dbEventCtx ctx, unsigned int priority);
void db_event_enable(dbEventSubscription sub);
void db_event_disable(dbEventSubscription sub);
void db_post_single_event(dbEventSubscription sub);
void db_post_extra_labor(dbEventCtx ctx);
int db_add_extra_labor_event(dbEventCtx ctx, void (*func)(void *), void *arg);
void db_flush_extra_labor_event(dbEventCtx ctx);
#ifdef __cplusplus
}
#endif
#endif
