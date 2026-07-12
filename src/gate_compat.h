#ifndef GATE_COMPAT_H
#define GATE_COMPAT_H

#include "epicsVersion.h"
#include "epicsTypes.h"
#include "gate_db_access.h"

/* Headers that might not exist in older base */
#if EPICS_VERSION >= 7
#  include "dbChannel.h"
#  include "dbNotify.h"
#  include "dbCoreAPI.h"
#  include "dbServer.h"
#endif

#ifndef DBR_STSACK_STRING
#  define DBR_STSACK_STRING (DBR_PUT_ACKS + 1)
#endif
#ifndef DBR_CLASS_NAME
#  define DBR_CLASS_NAME (DBR_STSACK_STRING + 1)
#endif
#ifndef LAST_BUFFER_TYPE
#  define LAST_BUFFER_TYPE DBR_CLASS_NAME
#endif
#ifndef INVALID_DB_REQ
#  define INVALID_DB_REQ(x) ((x < 0) || (x > LAST_BUFFER_TYPE))
#endif

/* Standard DBR typedefs missing in older Base */
typedef epicsOldString dbr_string_t;
typedef epicsInt16 dbr_short_t;
typedef epicsUInt16 dbr_enum_t;
typedef epicsUInt8 dbr_char_t;
typedef epicsInt32 dbr_long_t;
typedef epicsFloat32 dbr_float_t;
typedef epicsFloat64 dbr_double_t;

#ifndef dbr_type_to_DBF
#  define dbr_type_to_DBF(type) ((type)%7)
#endif

/* Missing dbChannel macros/functions in older Base */
#ifndef dbChannelRecord
#  define dbChannelRecord(CHAN) ((CHAN)->addr.precord)
#endif
#ifndef dbChannelName
#  define dbChannelName(CHAN) ((CHAN)->name)
#endif
#ifndef dbChannelFinalElements
#  define dbChannelFinalElements(CHAN) ((CHAN)->final_no_elements)
#endif
#ifndef dbChannelFinalCAType
#  define dbChannelFinalCAType(CHAN) dbDBRnewToDBRold[(CHAN)->final_type]
#endif
#ifndef dbChannelSpecial
#  define dbChannelSpecial(CHAN) ((CHAN)->addr.special)
#endif
#ifndef dbChannelField
#  define dbChannelField(CHAN) ((CHAN)->addr.pfield)
#endif

#ifndef SPC_NOMOD
#  define SPC_NOMOD 0
#endif

#ifdef __cplusplus
extern "C" {
#endif

/* Forward declaration for older base */
#if EPICS_VERSION < 7
struct dbChannel;
typedef struct dbChannel dbChannel;
#endif

extern unsigned short dbDBRnewToDBRold[];

#if EPICS_VERSION < 7
long dbChannelTest(const char *name);
void dbChannelDelete(dbChannel *chan);
void dbChannelShow(dbChannel *chan, int level, int indent);
#endif

#ifdef __cplusplus
}
#endif

#endif
