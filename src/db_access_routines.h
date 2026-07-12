#ifndef INCLdb_access_routinesh
#define INCLdb_access_routinesh
#include "epicsVersion.h"
#include "db_access.h"
#ifdef __cplusplus
extern "C" {
#endif
#if EPICS_VERSION >= 7
#include "dbChannel.h"
#include "dbNotify.h"
#else
struct dbChannel;
struct processNotify;
typedef enum { putValue, putValueProcess, putValueVariable } notifyPutType;
#endif
struct dbChannel * dbChannel_create(const char *pname);
#if EPICS_VERSION >= 7
int dbChannel_get(dbChannel *chan, int buffer_type, void *pbuffer, long no_elements, void *pfl);
int dbChannel_put(dbChannel *chan, int src_type, const void *psrc, long no_elements);
int dbChannel_get_count(dbChannel *chan, int buffer_type, void *pbuffer, long *nRequest, void *pfl);
int db_put_process(struct processNotify *ppn, notifyPutType type, int dbrType, const void *pbuffer, int nRequest);
#else
int dbChannel_get(struct dbChannel *chan, int buffer_type, void *pbuffer, long no_elements, void *pfl);
int dbChannel_put(struct dbChannel *chan, int src_type, const void *psrc, long no_elements);
int dbChannel_get_count(struct dbChannel *chan, int buffer_type, void *pbuffer, long *nRequest, void *pfl);
int db_put_process(struct processNotify *ppn, int type, int dbrType, const void *pbuffer, long nRequest);
#endif
#ifdef __cplusplus
}
#endif
#endif
