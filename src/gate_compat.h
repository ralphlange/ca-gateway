#ifndef GATE_COMPAT_H
#define GATE_COMPAT_H

#include <epicsVersion.h>
#include <epicsTypes.h>
#include <epicsTime.h>
#include <errlog.h>
#include <ellLib.h>
#include <envDefs.h>

#ifdef EPICS_VERSION_INT
#  define EPICS_VERSION_AT_LEAST(v,r,l,p) (EPICS_VERSION_INT >= VERSION_INT(v,r,l,p))
#else
#  define EPICS_VERSION_AT_LEAST(v,r,l,p) (EPICS_VERSION > (v) || (EPICS_VERSION == (v) && EPICS_REVISION >= (r)))
#endif

#if EPICS_VERSION_AT_LEAST(7,0,0,0)
#  include <dbAccessDefs.h>
#  include <dbAddr.h>
#  include <dbBase.h>
#  include <dbFldTypes.h>
#  include <dbChannel.h>
#  include <dbNotify.h>
#  include <dbEvent.h>
#  include <dbServer.h>
#  include <net_convert.h>
#else
#  include <dbAccess.h>
#  include <dbEvent.h>
#endif

#include <asLib.h>
#include <asDbLib.h>

#ifdef __cplusplus
extern "C" {
#endif

enum {
    G_S_DBF_STRING = 0,
    G_S_DBF_CHAR   = 1,
    G_S_DBF_UCHAR  = 2,
    G_S_DBF_SHORT  = 3,
    G_S_DBF_USHORT = 4,
    G_S_DBF_LONG   = 5,
    G_S_DBF_ULONG  = 6,
    G_S_DBF_INT64  = 7,
    G_S_DBF_UINT64 = 8,
    G_S_DBF_FLOAT  = 9,
    G_S_DBF_DOUBLE = 10,
    G_S_DBF_ENUM   = 11
};

#undef DBR_STRING
#undef DBR_SHORT
#undef DBR_FLOAT
#undef DBR_ENUM
#undef DBR_CHAR
#undef DBR_LONG
#undef DBR_DOUBLE
#undef DBR_PUT_ACKT
#undef DBR_PUT_ACKS
#undef VALID_DB_REQ
#undef INVALID_DB_REQ
#undef DBF_STRING
#undef DBF_CHAR
#undef DBF_UCHAR
#undef DBF_SHORT
#undef DBF_USHORT
#undef DBF_LONG
#undef DBF_ULONG
#undef DBF_FLOAT
#undef DBF_DOUBLE
#undef DBF_ENUM
#undef DBF_MENU
#undef DBF_DEVICE

#include <db_access.h>
#include <cadef.h>

#undef DBF_STRING
#define DBF_STRING 0
#undef DBF_SHORT
#define DBF_SHORT 1
#undef DBF_FLOAT
#define DBF_FLOAT 2
#undef DBF_ENUM
#define DBF_ENUM 3
#undef DBF_CHAR
#define DBF_CHAR 4
#undef DBF_LONG
#define DBF_LONG 5
#undef DBF_DOUBLE
#define DBF_DOUBLE 6

#undef DBR_STRING
#define DBR_STRING 0
#undef DBR_SHORT
#define DBR_SHORT 1
#undef DBR_FLOAT
#define DBR_FLOAT 2
#undef DBR_ENUM
#define DBR_ENUM 3
#undef DBR_CHAR
#define DBR_CHAR 4
#undef DBR_LONG
#define DBR_LONG 5
#undef DBR_DOUBLE
#define DBR_DOUBLE 6

#ifndef DBR_PUT_ACKT
#  define DBR_PUT_ACKT 35
#endif
#ifndef DBR_PUT_ACKS
#  define DBR_PUT_ACKS 36
#endif
#ifndef DBR_STSACK_STRING
#  define DBR_STSACK_STRING 37
#endif
#ifndef DBR_CLASS_NAME
#  define DBR_CLASS_NAME 38
#endif
#ifndef LAST_BUFFER_TYPE
#  define LAST_BUFFER_TYPE DBR_CLASS_NAME
#endif

#ifndef VALID_DB_REQ
#  define VALID_DB_REQ(x) ((x) >= 0 && (x) <= LAST_BUFFER_TYPE)
#endif
#ifndef INVALID_DB_REQ
#  define INVALID_DB_REQ(x) ((x) < 0 || (x) > LAST_BUFFER_TYPE)
#endif

typedef epicsOldString dbr_string_t;
typedef epicsInt16 dbr_short_t;
typedef epicsUInt16 dbr_enum_t;
typedef epicsUInt8 dbr_char_t;
typedef epicsInt32 dbr_long_t;
typedef epicsFloat32 dbr_float_t;
typedef epicsFloat64 dbr_double_t;

#if !EPICS_VERSION_AT_LEAST(7,0,0,0)
#ifndef INC_dbChannel_H
struct dbChannel {
    const char *name;
    long final_no_elements;
    short final_type;
};
typedef struct dbChannel dbChannel;
#endif
#ifndef INC_dbServer_H
typedef struct dbServer {
    ELLNODE node;
    const char *name;
    void (* report) (unsigned level);
    void (* stats) (unsigned *channels, unsigned *clients);
    int (* client) (char *pBuf, size_t bufSize);
    void (* init) (void);
    void (* run) (void);
    void (* pause) (void);
    void (* stop) (void);
} dbServer;
#endif
int dbRegisterServer(dbServer *psrv);
#ifndef dbChannelName
#  define dbChannelName(CHAN) ((CHAN)->name)
#endif
#ifndef dbChannelFinalElements
#  define dbChannelFinalElements(CHAN) ((CHAN)->final_no_elements)
#endif
#ifndef dbChannelFinalCAType
#  define dbChannelFinalCAType(CHAN) ((CHAN)->final_type)
#endif
#ifndef SPC_NOMOD
#  define SPC_NOMOD 1
#endif
typedef unsigned long arrayElementCount;
int caNetConvert (unsigned type, const void *pSrc, void *pDest, int hton, arrayElementCount count );
#endif

static inline int gate_dbr_to_dbf(int dbr) {
    switch(dbr % 7) {
        case 0: return G_S_DBF_STRING;
        case 1: return G_S_DBF_SHORT;
        case 2: return G_S_DBF_FLOAT;
        case 3: return G_S_DBF_ENUM;
        case 4: return G_S_DBF_CHAR;
        case 5: return G_S_DBF_LONG;
        case 6: return G_S_DBF_DOUBLE;
        default: return G_S_DBF_STRING;
    }
}
#ifndef dbr_type_to_DBF
#  define dbr_type_to_DBF(type) gate_dbr_to_dbf(type)
#endif
extern const unsigned short dbr_size[];
extern const unsigned short dbr_value_size[];
extern unsigned short dbDBRnewToDBRold[];
#undef EPICS_CA_MCAST_TTL
#define EPICS_CA_MCAST_TTL gate_mcast_ttl
extern const ENV_PARAM gate_mcast_ttl;
#undef EPICS_CA_AUTO_ARRAY_BYTES
#define EPICS_CA_AUTO_ARRAY_BYTES gate_auto_array_bytes
extern const ENV_PARAM gate_auto_array_bytes;
#ifndef asCheckClientIP
#  define asCheckClientIP gate_asCheckClientIP
   extern int gate_asCheckClientIP;
#endif
extern dbServer *rsrv_psrv;

#ifdef __cplusplus
}
#endif
#endif
