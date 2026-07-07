#ifndef INCdbConverth
#define INCdbConverth
#include "dbFldTypes.h"
#include "dbAddr.h"
#ifdef __cplusplus
extern "C" {
#endif
typedef long (*GETCONVERTFUNC)(const dbAddr *paddr, void *pbuffer,
    long nRequest, long no_elements, long offset);
typedef long (*PUTCONVERTFUNC)(dbAddr *paddr, const void *pbuffer,
    long nRequest, long no_elements, long offset);
extern GETCONVERTFUNC dbGetConvertRoutine[DBF_DEVICE+1][DBR_ENUM+1];
extern PUTCONVERTFUNC dbPutConvertRoutine[DBR_ENUM+1][DBF_DEVICE+1];
struct dbr_enumStrs {
    int no_str;
    char strs[16][26];
};
void * dbGetRset(const struct dbAddr *paddr);
void recGblDbaddrError(long status, const struct dbAddr *paddr, const char *pmsg);
#ifdef __cplusplus
}
#endif
#endif
