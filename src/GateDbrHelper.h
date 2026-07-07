#ifndef GATE_DBR_HELPER_H
#define GATE_DBR_HELPER_H
#include "db_access.h"
#include <epicsTime.h>

#ifndef dbr_type_to_TIME
#define dbr_type_to_TIME(type) (type)
#endif

inline void extract_metadata(int type, const void* dbr, epicsTimeStamp& stamp, short& status, short& severity) {
    if (!dbr_type_is_TIME(type)) return;
    switch(dbr_type_to_TIME(type)) {
        case DBR_TIME_STRING: {
            struct dbr_time_string* p = (struct dbr_time_string*)dbr;
            stamp = p->stamp; status = p->status; severity = p->severity;
            break;
        }
        case DBR_TIME_SHORT: {
            struct dbr_time_short* p = (struct dbr_time_short*)dbr;
            stamp = p->stamp; status = p->status; severity = p->severity;
            break;
        }
        case DBR_TIME_FLOAT: {
            struct dbr_time_float* p = (struct dbr_time_float*)dbr;
            stamp = p->stamp; status = p->status; severity = p->severity;
            break;
        }
        case DBR_TIME_ENUM: {
            struct dbr_time_enum* p = (struct dbr_time_enum*)dbr;
            stamp = p->stamp; status = p->status; severity = p->severity;
            break;
        }
        case DBR_TIME_CHAR: {
            struct dbr_time_char* p = (struct dbr_time_char*)dbr;
            stamp = p->stamp; status = p->status; severity = p->severity;
            break;
        }
        case DBR_TIME_LONG: {
            struct dbr_time_long* p = (struct dbr_time_long*)dbr;
            stamp = p->stamp; status = p->status; severity = p->severity;
            break;
        }
        case DBR_TIME_DOUBLE: {
            struct dbr_time_double* p = (struct dbr_time_double*)dbr;
            stamp = p->stamp; status = p->status; severity = p->severity;
            break;
        }
    }
}
#endif
