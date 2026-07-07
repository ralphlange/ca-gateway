#ifndef INCdbAccessDefsh
#define INCdbAccessDefsh
#include <epicsTypes.h>
#include <epicsTime.h>
#include "dbAddr.h"
#ifdef __cplusplus
extern "C" {
#endif
extern struct dbBase *pdbbase;
extern volatile int interruptAccept;
extern int dbAccessDebugPUTF;
#define DBR_STATUS      0x00000001
#define DBR_TIME        0x00000010
#define S_db_noRSET 1
#define S_db_badDbrtype 2
#define S_db_onlyOne 3
#define S_db_badChoice 4
#ifdef __cplusplus
}
#endif
#endif
