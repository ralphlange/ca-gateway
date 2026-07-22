#ifndef GATE_FORMAT_H
#define GATE_FORMAT_H

#include "gate_compat.h"

#ifdef __cplusplus

// Cached "static" (rarely-changing) metadata for a channel, refreshed from an
// upstream DBR_CTRL_<native> get. Only the fields relevant to the channel's
// native field type are meaningful; the rest stay at their default value.
struct GateStaticMeta {
    bool valid = false;
    short precision = 0;
    char units[MAX_UNITS_SIZE] = {};
    double upper_disp_limit = 0, lower_disp_limit = 0;
    double upper_alarm_limit = 0, upper_warning_limit = 0;
    double lower_warning_limit = 0, lower_alarm_limit = 0;
    double upper_ctrl_limit = 0, lower_ctrl_limit = 0;
    short no_str = 0;
    char strs[MAX_ENUM_STATES][MAX_ENUM_STRING_SIZE] = {};
};

// Extracts status/severity/timestamp from a DBR_TIME_<native> buffer, as delivered by an
// upstream subscription created with type (DBR_TIME_STRING + dbf).
void gate_format_extract_time(int dbf, const void* dbr, short* status, short* severity, epicsTimeStamp* stamp);

// Extracts precision/units/limits/enum-strings from a DBR_CTRL_<native> buffer, as delivered
// by an upstream ca_array_get_callback with type (DBR_CTRL_STRING + dbf).
void gate_format_extract_meta(int dbf, const void* dbr, GateStaticMeta* meta);

// Fills *pbuffer with a full DBR_STS_*/TIME_*/GR_*/CTRL_* response.
//   dbf         - native field type (gate_compat.h's G_S_DBF_*, 0..6)
//   buffer_type - requested DBR_* type (0..38)
//   pbuffer     - destination, must already be large enough for dbr_size_n(buffer_type, count)
//   nRequest    - in: max element count; out: actual element count written
//   count       - number of elements available in rawValue
//   status/severity/stamp - from the cached event
//   rawValue    - pointer to `count` elements of native-typed value data
//   meta        - cached static metadata (only consulted for GR_*/CTRL_*; may be !valid)
// Returns 0 on success, -1 if buffer_type isn't supported here.
int gate_format_response(int dbf, int buffer_type, void* pbuffer, long* nRequest, long count,
                          short status, short severity, const epicsTimeStamp& stamp,
                          const void* rawValue, const GateStaticMeta& meta);

#endif // __cplusplus

#endif
