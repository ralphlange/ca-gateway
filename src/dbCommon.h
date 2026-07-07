#ifndef INCdbCommonh
#define INCdbCommonh
#include <epicsTypes.h>
#include <epicsTime.h>
#include <ellLib.h>
#include <dbDefs.h>
#ifdef __cplusplus
extern "C" {
#endif
typedef struct dbCommon {
    char            name[PVNAME_STRINGSZ];
    char            asg[PVNAME_STRINGSZ];
    void            *asp;
    epicsTimeStamp  time;
    short           stat;
    short           sevr;
    char            amsg[40];
    epicsUInt64     utag;
    ELLLIST         mlis;
} dbCommon;
void dbScanLock(struct dbCommon *prec);
void dbScanUnlock(struct dbCommon *prec);
#ifdef __cplusplus
}
#endif
#endif
