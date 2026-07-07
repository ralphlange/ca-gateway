#ifndef INC_db_access_H
#define INC_db_access_H
#include <epicsTypes.h>
#include <epicsTime.h>
#include <stddef.h>
#ifdef __cplusplus
extern "C" {
#endif
#define DBR_STRING      0
#define DBR_CHAR        1
#define DBR_UCHAR       2
#define DBR_SHORT       3
#define DBR_USHORT      4
#define DBR_LONG        5
#define DBR_ULONG       6
#define DBR_FLOAT       7
#define DBR_DOUBLE      8
#define DBR_ENUM        9
#define DBR_STS_STRING  7
#define DBR_STS_SHORT   8
#define DBR_STS_FLOAT   9
#define DBR_STS_ENUM    10
#define DBR_STS_CHAR    11
#define DBR_STS_LONG    12
#define DBR_STS_DOUBLE  13
#define DBR_TIME_STRING 14
#define DBR_TIME_SHORT  15
#define DBR_TIME_FLOAT  16
#define DBR_TIME_ENUM   17
#define DBR_TIME_CHAR   18
#define DBR_TIME_LONG   19
#define DBR_TIME_DOUBLE 20
#define DBR_GR_STRING   21
#define DBR_GR_SHORT    22
#define DBR_GR_FLOAT    23
#define DBR_GR_ENUM     24
#define DBR_GR_CHAR     25
#define DBR_GR_LONG     26
#define DBR_GR_DOUBLE   27
#define DBR_CTRL_STRING 28
#define DBR_CTRL_SHORT  29
#define DBR_CTRL_FLOAT  30
#define DBR_CTRL_ENUM   31
#define DBR_CTRL_CHAR   32
#define DBR_CTRL_LONG   33
#define DBR_CTRL_DOUBLE 34
#define DBR_PUT_ACKT    35
#define DBR_PUT_ACKS    36
#define dbr_type_is_TIME(type) ((type)>=14 && (type)<=20)
#define dbr_type_to_TIME(type) (type)
#define dbf_type_to_DBR_TIME(type) ((type)+14)
#define dbr_type_to_DBF(type) ((type)%7)
extern size_t dbr_size[];
extern size_t dbr_value_size[];
#define dbr_size_n(type, n) (dbr_size[type] + ((n)>0 ? (n)-1 : 0) * dbr_value_size[type])

struct dbr_time_string{ short status; short severity; epicsTimeStamp stamp; epicsOldString value; };
struct dbr_time_short{ short status; short severity; epicsTimeStamp stamp; short RISC_pad; short value; };
struct dbr_time_float{ short status; short severity; epicsTimeStamp stamp; float value; };
struct dbr_time_enum{ short status; short severity; epicsTimeStamp stamp; short RISC_pad; unsigned short value; };
struct dbr_time_char{ short status; short severity; epicsTimeStamp stamp; short RISC_pad; epicsUInt8 value; };
struct dbr_time_long{ short status; short severity; epicsTimeStamp stamp; epicsInt32 value; };
struct dbr_time_double{ short status; short severity; epicsTimeStamp stamp; long RISC_pad; double value; };

#ifdef __cplusplus
}
#endif
#endif
