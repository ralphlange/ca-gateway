#ifndef INCLdb_access_routinesh
#define INCLdb_access_routinesh
#include "dbChannel.h"
#include "dbNotify.h"
#ifdef __cplusplus
extern "C" {
#endif
struct dbChannel * dbChannel_create(const char *pname);
int dbChannel_get(struct dbChannel *chan,
    int buffer_type, void *pbuffer, long no_elements, void *pfl);
int dbChannel_put(struct dbChannel *chan, int src_type,
    const void *psrc, long no_elements);
int dbChannel_get_count(struct dbChannel *chan,
    int buffer_type, void *pbuffer, long *nRequest, void *pfl);
int db_put_process(struct processNotify *ppn, int type,
    int dbrType, const void *pbuffer, long nRequest);
#ifdef __cplusplus
}
#endif
#endif
