#ifndef GATE_COMPAT_H
#define GATE_COMPAT_H

/* asLib.h (below) declares several functions taking a plain FILE* (asInitFP, asDumpFP, ...)
 * without including <stdio.h> itself -- it relies on whatever includes it having already
 * pulled FILE in transitively. Include it explicitly rather than depend on a particular Base
 * version's transitive header layout happening to do that first.
 */
#include <stdio.h>

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

/* dbChannel (struct dbChannel/dbChannel_create/etc., what this Gateway's shim layer is built
 * around throughout -- see CLAUDE.md) exists in every Base version supported here (3.15+);
 * it doesn't exist at all before 3.15, which is exactly why Base 3.14 isn't supported.
 */
#include <dbAccessDefs.h>
#include <dbAddr.h>
#include <dbBase.h>
#include <dbFldTypes.h>
#include <dbChannel.h>
#include <dbNotify.h>
#include <dbEvent.h>
#include <net_convert.h>
/* recSup.h (for the bare `rset` typedef gateShim.c's dbGetRset() stub returns) is pulled in
 * transitively by dbBase.h on Base 7.0 but not 3.15 (3.15's dbBase.h doesn't include it at
 * all) -- include it explicitly rather than relying on either version's transitive layout.
 */
#include <recSup.h>
/* dbConvert.h (GETCONVERTFUNC, dbGetConvertRoutine[][]) MUST be included here, before the
 * DBF_* / DBR_* #undef #define block further down remaps DBF_DEVICE/DBR_ENUM to our own
 * CA-wire-ordering values -- dbConvert.h's own array declaration is sized
 * [DBF_DEVICE+1][DBR_ENUM+1] using whatever those macros mean *at its own include point*,
 * and it must see Base's real, per-version dbFldTypes.h values (3.15: [12][10], 7.0: [14][12])
 * to match the actual array Base's library exports. GateFormat.cpp used to hand-declare this
 * extern itself with 7.0's dimensions hardcoded -- harmless on 7.0 only by coincidence, but on
 * 3.15 it silently computed every index against the wrong (larger) row stride, reading
 * whatever happened to be at that offset instead of the real conversion routine.
 */
#include <dbConvert.h>

/* Deliberately NOT including Base's own <dbServer.h>: gateShim.c/GateMain.cpp own both ends
 * of this struct already (dbRegisterServer()/rsrv_psrv are entirely our own shim, never
 * Base's real dbServer machinery), and its actual shape differs materially by version --
 * Base 7.0 added init/run/pause/stop control-method fields (to support multiple simultaneous
 * server layers, e.g. CA + PVA) that don't exist on Base 3.15's dbServer at all, and changed
 * dbRegisterServer() from void to int. Providing our own single definition, unconditionally,
 * sidesteps that mismatch entirely rather than needing two incompatible struct layouts.
 */
/* caservertask.c (vendored, unmodified) also does its own #include "dbServer.h" directly --
 * pre-defining Base's own include guard makes that a no-op instead of pulling in the real,
 * conflicting struct a second time.
 */
#define INC_dbServer_H
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
int dbRegisterServer(dbServer *psrv);

#if !EPICS_VERSION_AT_LEAST(7,0,0,0)
/* ERL_ERROR/ERL_WARNING (errlog.h) are ANSI-color-coded log-severity tags introduced in
 * Base 7.0 and used verbatim (inside errlogPrintf format strings) by caserverio.c/
 * caservertask.c/camsgtask.c; 3.15's errlog.h has no such macros at all. Plain, uncolored
 * text is a fine substitute -- these only affect what the printed message looks like.
 */
#ifndef ERL_ERROR
#  define ERL_ERROR "ERROR"
#endif
#ifndef ERL_WARNING
#  define ERL_WARNING "WARNING"
#endif
/* osiSockOptMcastTTL_t (caservertask.c, IP_MULTICAST_TTL setsockopt) is a Base 7.0
 * os/Linux/osdSock.h addition -- plain int prior to that, per 7.0's own typedef.
 */
typedef int osiSockOptMcastTTL_t;
/* CA_VSUPPORTED (caProto.h, used throughout camessage.c to reject unsupported protocol
 * minor versions) is a Base 7.0 addition too; 3.15's caProto.h has no equivalent at all
 * (every minor version it ever spoke was already >= this threshold). Same definition 7.0
 * itself uses.
 */
#ifndef CA_MINIMUM_SUPPORTED_VERSION
#  define CA_MINIMUM_SUPPORTED_VERSION 4u
#endif
#ifndef CA_VSUPPORTED
#  define CA_VSUPPORTED(MINOR) ((MINOR)>=CA_MINIMUM_SUPPORTED_VERSION)
#endif

/* Base 3.15 bundles a materially older yajl (JSON parser): yajl_integer's callback takes a
 * plain `long` (not `long long`), string-length callbacks take `unsigned int` (not `size_t`),
 * yajl_alloc() takes an extra (now-removed) yajl_parser_config* argument, and the parser's
 * "check the whole input was consumed" call is named yajl_parse_complete() rather than
 * yajl_complete_parse(). GateLogic.cpp's JSON config parser uses these consistently through
 * the aliases below instead of hardcoding either API.
 */
typedef long gate_yajl_int_t;
typedef unsigned int gate_yajl_len_t;
#define gate_yajl_alloc(callbacks, ctx) yajl_alloc((callbacks), NULL, NULL, (ctx))
#define gate_yajl_complete_parse(hand) yajl_parse_complete(hand)
#else
typedef long long gate_yajl_int_t;
typedef size_t gate_yajl_len_t;
#define gate_yajl_alloc(callbacks, ctx) yajl_alloc((callbacks), NULL, (ctx))
#define gate_yajl_complete_parse(hand) yajl_complete_parse(hand)
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

/* G_S_DBF_* above is a portable, made-up numbering used purely for our own internal
 * dispatch (gate_dbr_to_dbf()'s return value, matched against these same names in
 * switch/case elsewhere) -- it is NOT dbFldTypes.h's real per-version database field-type
 * ordering, and must never be used to index a table Base itself provides (dbGetConvertRoutine,
 * or dbDBRnewToDBRold via dbAddr::field_type/dbr_field_type -- see gateShim.c's
 * gate_fill_addr() and GateFormat.cpp's gate_format_response()). Those real tables are
 * indexed by dbFldTypes.h's actual enum, which differs by version: Base 7.0 inserted
 * DBF_INT64/DBF_UINT64 before FLOAT/DOUBLE, so e.g. real DBF_DOUBLE is 8 on 3.15 but 10 on
 * 7.0. Captured here, before the CA-wire-numbering #undef/#define block below shadows
 * DBF_STRING et al with our own portable 0..6 values, so these still refer to the real,
 * per-version dbFldTypes.h constants.
 */
static const int gate_real_dbf[7] = {
    DBF_STRING, DBF_SHORT, DBF_FLOAT, DBF_ENUM, DBF_CHAR, DBF_LONG, DBF_DOUBLE
};
static inline int gate_dbf_to_real_dbf(int canonical) {
    switch (canonical) {
        case G_S_DBF_STRING: return gate_real_dbf[0];
        case G_S_DBF_SHORT:  return gate_real_dbf[1];
        case G_S_DBF_FLOAT:  return gate_real_dbf[2];
        case G_S_DBF_ENUM:   return gate_real_dbf[3];
        case G_S_DBF_CHAR:   return gate_real_dbf[4];
        case G_S_DBF_LONG:   return gate_real_dbf[5];
        case G_S_DBF_DOUBLE: return gate_real_dbf[6];
        default: return gate_real_dbf[0];
    }
}

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
