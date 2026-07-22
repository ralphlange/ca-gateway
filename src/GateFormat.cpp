// Hand-assembles DBR_STS_/TIME_/GR_/CTRL_ responses for gate_get_count().
//
// The Gateway has no real record (dbCommon/rset) behind a channel, so it can't rely on
// dbAccess.c's normal dbGet() to fill in status/severity/timestamp/limits/precision/units/
// enum-strings the way a real IOC does. Those fields are supplied here from data captured
// off the upstream CA subscription/get instead (see GateLogic.cpp's ca_event_cb/ca_meta_cb).
//
// Field layout for every dbr_sts_*/dbr_time_*/dbr_gr_*/dbr_ctrl_* struct comes straight from
// the real <db_access.h> types (via gate_compat.h) -- never hand-computed byte offsets -- so
// the compiler handles any per-platform struct padding correctly.
#include "gate_compat.h"
#include "GateFormat.h"
#include <cstring>

extern "C" {
typedef long (*GETCONVERTFUNC)(const struct dbAddr *paddr, void *pbuffer, long nRequest, long no_elements, long offset);
extern GETCONVERTFUNC dbGetConvertRoutine[14][12];
}

namespace {

// --- DBR_STS_*/DBR_TIME_* : identical status/severity/(timestamp) layout across all 7 types ---

template <typename T>
void set_sts(void* buf, short status, short severity) {
    T* p = (T*)buf;
    p->status = status;
    p->severity = severity;
}

template <typename T>
void set_time(void* buf, short status, short severity, const epicsTimeStamp& stamp) {
    T* p = (T*)buf;
    p->status = status;
    p->severity = severity;
    p->stamp.secPastEpoch = stamp.secPastEpoch;
    p->stamp.nsec = stamp.nsec;
}

void fill_sts(int basic, void* buf, short status, short severity) {
    switch (basic) {
        case 0: set_sts<struct dbr_sts_string>(buf, status, severity); break;
        case 1: set_sts<struct dbr_sts_short>(buf, status, severity); break;
        case 2: set_sts<struct dbr_sts_float>(buf, status, severity); break;
        case 3: set_sts<struct dbr_sts_enum>(buf, status, severity); break;
        case 4: set_sts<struct dbr_sts_char>(buf, status, severity); break;
        case 5: set_sts<struct dbr_sts_long>(buf, status, severity); break;
        case 6: set_sts<struct dbr_sts_double>(buf, status, severity); break;
        default: break;
    }
}

void fill_time(int basic, void* buf, short status, short severity, const epicsTimeStamp& stamp) {
    switch (basic) {
        case 0: set_time<struct dbr_time_string>(buf, status, severity, stamp); break;
        case 1: set_time<struct dbr_time_short>(buf, status, severity, stamp); break;
        case 2: set_time<struct dbr_time_float>(buf, status, severity, stamp); break;
        case 3: set_time<struct dbr_time_enum>(buf, status, severity, stamp); break;
        case 4: set_time<struct dbr_time_char>(buf, status, severity, stamp); break;
        case 5: set_time<struct dbr_time_long>(buf, status, severity, stamp); break;
        case 6: set_time<struct dbr_time_double>(buf, status, severity, stamp); break;
        default: break;
    }
}

// --- DBR_GR_*/DBR_CTRL_* : layout differs by native basic type ---
//   STRING : status+severity only, no graphic fields at all
//   ENUM   : status+severity+no_str+strs[MAX_ENUM_STATES][MAX_ENUM_STRING_SIZE]
//   SHORT/CHAR/LONG : status+severity+units+6 limits (no precision)
//   FLOAT/DOUBLE    : status+severity+precision+units+6 limits
//   CTRL additionally carries upper_ctrl_limit/lower_ctrl_limit for the numeric types.

template <typename T>
void set_numeric_common(T* p, const GateStaticMeta& meta) {
    memcpy(p->units, meta.units, sizeof(p->units));
    p->upper_disp_limit = (decltype(p->upper_disp_limit))meta.upper_disp_limit;
    p->lower_disp_limit = (decltype(p->lower_disp_limit))meta.lower_disp_limit;
    p->upper_alarm_limit = (decltype(p->upper_alarm_limit))meta.upper_alarm_limit;
    p->upper_warning_limit = (decltype(p->upper_warning_limit))meta.upper_warning_limit;
    p->lower_warning_limit = (decltype(p->lower_warning_limit))meta.lower_warning_limit;
    p->lower_alarm_limit = (decltype(p->lower_alarm_limit))meta.lower_alarm_limit;
}

template <typename T>
void fill_gr_numeric_no_precision(void* buf, short status, short severity, const GateStaticMeta& meta) {
    T* p = (T*)buf;
    p->status = status;
    p->severity = severity;
    set_numeric_common(p, meta);
}

template <typename T>
void fill_gr_numeric_with_precision(void* buf, short status, short severity, const GateStaticMeta& meta) {
    T* p = (T*)buf;
    p->status = status;
    p->severity = severity;
    p->precision = meta.precision;
    set_numeric_common(p, meta);
}

template <typename T>
void fill_ctrl_numeric_no_precision(void* buf, short status, short severity, const GateStaticMeta& meta) {
    T* p = (T*)buf;
    p->status = status;
    p->severity = severity;
    set_numeric_common(p, meta);
    p->upper_ctrl_limit = (decltype(p->upper_ctrl_limit))meta.upper_ctrl_limit;
    p->lower_ctrl_limit = (decltype(p->lower_ctrl_limit))meta.lower_ctrl_limit;
}

template <typename T>
void fill_ctrl_numeric_with_precision(void* buf, short status, short severity, const GateStaticMeta& meta) {
    T* p = (T*)buf;
    p->status = status;
    p->severity = severity;
    p->precision = meta.precision;
    set_numeric_common(p, meta);
    p->upper_ctrl_limit = (decltype(p->upper_ctrl_limit))meta.upper_ctrl_limit;
    p->lower_ctrl_limit = (decltype(p->lower_ctrl_limit))meta.lower_ctrl_limit;
}

template <typename T>
void set_enum_common(void* buf, short status, short severity, const GateStaticMeta& meta) {
    // dbr_gr_enum and dbr_ctrl_enum are structurally identical (no ctrl-only fields for enums),
    // but written through their own real type rather than reusing one for both.
    T* p = (T*)buf;
    p->status = status;
    p->severity = severity;
    p->no_str = meta.no_str;
    memcpy(p->strs, meta.strs, sizeof(p->strs));
}

void fill_gr(int basic, void* buf, short status, short severity, const GateStaticMeta& meta) {
    switch (basic) {
        // Real Base has no dbr_gr_string/dbr_ctrl_string (see db_access.h: "not implemented;
        // use struct dbr_sts_string") -- GR_STRING/CTRL_STRING carry no extra metadata.
        case 0: set_sts<struct dbr_sts_string>(buf, status, severity); break;
        case 1: fill_gr_numeric_no_precision<struct dbr_gr_short>(buf, status, severity, meta); break;
        case 2: fill_gr_numeric_with_precision<struct dbr_gr_float>(buf, status, severity, meta); break;
        case 3: set_enum_common<struct dbr_gr_enum>(buf, status, severity, meta); break;
        case 4: fill_gr_numeric_no_precision<struct dbr_gr_char>(buf, status, severity, meta); break;
        case 5: fill_gr_numeric_no_precision<struct dbr_gr_long>(buf, status, severity, meta); break;
        case 6: fill_gr_numeric_with_precision<struct dbr_gr_double>(buf, status, severity, meta); break;
        default: break;
    }
}

void fill_ctrl(int basic, void* buf, short status, short severity, const GateStaticMeta& meta) {
    switch (basic) {
        case 0: set_sts<struct dbr_sts_string>(buf, status, severity); break;
        case 1: fill_ctrl_numeric_no_precision<struct dbr_ctrl_short>(buf, status, severity, meta); break;
        case 2: fill_ctrl_numeric_with_precision<struct dbr_ctrl_float>(buf, status, severity, meta); break;
        case 3: set_enum_common<struct dbr_ctrl_enum>(buf, status, severity, meta); break;
        case 4: fill_ctrl_numeric_no_precision<struct dbr_ctrl_char>(buf, status, severity, meta); break;
        case 5: fill_ctrl_numeric_no_precision<struct dbr_ctrl_long>(buf, status, severity, meta); break;
        case 6: fill_ctrl_numeric_with_precision<struct dbr_ctrl_double>(buf, status, severity, meta); break;
        default: break;
    }
}

// --- extracting metadata back out of an upstream DBR_CTRL_<native> get ---

template <typename T>
void extract_meta_numeric_no_precision(const void* dbr, GateStaticMeta* meta) {
    const T* p = (const T*)dbr;
    meta->precision = 0;
    memcpy(meta->units, p->units, sizeof(p->units));
    meta->upper_disp_limit = (double)p->upper_disp_limit;
    meta->lower_disp_limit = (double)p->lower_disp_limit;
    meta->upper_alarm_limit = (double)p->upper_alarm_limit;
    meta->upper_warning_limit = (double)p->upper_warning_limit;
    meta->lower_warning_limit = (double)p->lower_warning_limit;
    meta->lower_alarm_limit = (double)p->lower_alarm_limit;
    meta->upper_ctrl_limit = (double)p->upper_ctrl_limit;
    meta->lower_ctrl_limit = (double)p->lower_ctrl_limit;
    meta->valid = true;
}

template <typename T>
void extract_meta_numeric_with_precision(const void* dbr, GateStaticMeta* meta) {
    const T* p = (const T*)dbr;
    meta->precision = p->precision;
    memcpy(meta->units, p->units, sizeof(p->units));
    meta->upper_disp_limit = (double)p->upper_disp_limit;
    meta->lower_disp_limit = (double)p->lower_disp_limit;
    meta->upper_alarm_limit = (double)p->upper_alarm_limit;
    meta->upper_warning_limit = (double)p->upper_warning_limit;
    meta->lower_warning_limit = (double)p->lower_warning_limit;
    meta->lower_alarm_limit = (double)p->lower_alarm_limit;
    meta->upper_ctrl_limit = (double)p->upper_ctrl_limit;
    meta->lower_ctrl_limit = (double)p->lower_ctrl_limit;
    meta->valid = true;
}

} // namespace

template <typename T>
static void extract_time_impl(const void* dbr, short* status, short* severity, epicsTimeStamp* stamp) {
    const T* p = (const T*)dbr;
    *status = p->status;
    *severity = p->severity;
    // Copied field-by-field (not aliased via pointer cast): dbr_time_stamp and epicsTimeStamp
    // are layout-identical by convention, but named/typedef'd independently across Base
    // versions, so a reinterpret_cast between the two struct types isn't guaranteed portable.
    stamp->secPastEpoch = p->stamp.secPastEpoch;
    stamp->nsec = p->stamp.nsec;
}

void gate_format_extract_time(int dbf, const void* dbr, short* status, short* severity, epicsTimeStamp* stamp) {
    *status = 0;
    *severity = 0;
    stamp->secPastEpoch = 0;
    stamp->nsec = 0;
    switch (dbf) {
        case G_S_DBF_STRING: extract_time_impl<struct dbr_time_string>(dbr, status, severity, stamp); break;
        case G_S_DBF_SHORT:  extract_time_impl<struct dbr_time_short>(dbr, status, severity, stamp); break;
        case G_S_DBF_FLOAT:  extract_time_impl<struct dbr_time_float>(dbr, status, severity, stamp); break;
        case G_S_DBF_ENUM:   extract_time_impl<struct dbr_time_enum>(dbr, status, severity, stamp); break;
        case G_S_DBF_CHAR:   extract_time_impl<struct dbr_time_char>(dbr, status, severity, stamp); break;
        case G_S_DBF_LONG:   extract_time_impl<struct dbr_time_long>(dbr, status, severity, stamp); break;
        case G_S_DBF_DOUBLE: extract_time_impl<struct dbr_time_double>(dbr, status, severity, stamp); break;
        default: break;
    }
}

void gate_format_extract_meta(int dbf, const void* dbr, GateStaticMeta* meta) {
    switch (dbf) {
        case G_S_DBF_STRING:
            meta->valid = true; // dbr_ctrl_string carries no metadata beyond status/severity
            break;
        case G_S_DBF_SHORT:
            extract_meta_numeric_no_precision<struct dbr_ctrl_short>(dbr, meta);
            break;
        case G_S_DBF_FLOAT:
            extract_meta_numeric_with_precision<struct dbr_ctrl_float>(dbr, meta);
            break;
        case G_S_DBF_ENUM: {
            const struct dbr_ctrl_enum* p = (const struct dbr_ctrl_enum*)dbr;
            meta->no_str = p->no_str;
            memcpy(meta->strs, p->strs, sizeof(meta->strs));
            meta->valid = true;
            break;
        }
        case G_S_DBF_CHAR:
            extract_meta_numeric_no_precision<struct dbr_ctrl_char>(dbr, meta);
            break;
        case G_S_DBF_LONG:
            extract_meta_numeric_no_precision<struct dbr_ctrl_long>(dbr, meta);
            break;
        case G_S_DBF_DOUBLE:
            extract_meta_numeric_with_precision<struct dbr_ctrl_double>(dbr, meta);
            break;
        default:
            break;
    }
}

int gate_format_response(int dbf, int buffer_type, void* pbuffer, long* nRequest, long count,
                          short status, short severity, const epicsTimeStamp& stamp,
                          const void* rawValue, const GateStaticMeta& meta) {
    if (buffer_type < 0 || buffer_type > DBR_CTRL_DOUBLE) return -1; // ACKT/ACKS/STSACK/CLASS_NAME: unsupported
    if (dbf < 0 || dbf >= 14) return -1;

    // buffer_type is CA-wire ordered (db_access.h): within each category, the basic type
    // cycles string/short/float/enum/char/long/double -- that's the ordering used both to
    // pick which dbr_sts_*/dbr_gr_*/dbr_ctrl_* struct to fill, and (via dbr_size/
    // dbr_value_size, also CA-wire indexed) to find the header/value split.
    int basic = buffer_type % 7;      // 0..6, CA-wire order: struct-dispatch index
    int category = buffer_type / 7;   // 0=plain 1=STS 2=TIME 3=GR 4=CTRL

    long n = *nRequest;
    if (n <= 0 || n > count) n = count;

    size_t header_size = (size_t)dbr_size[buffer_type] - (size_t)dbr_value_size[buffer_type];
    void* value_dest = (char*)pbuffer + header_size;

    switch (category) {
        case 1: fill_sts(basic, pbuffer, status, severity); break;
        case 2: fill_time(basic, pbuffer, status, severity, stamp); break;
        case 3: fill_gr(basic, pbuffer, status, severity, meta); break;
        case 4: fill_ctrl(basic, pbuffer, status, severity, meta); break;
        default: break; // plain (0..6): no header at all
    }

    // dbGetConvertRoutine, by contrast, is declared [DBF_DEVICE+1][DBR_ENUM+1] using
    // dbFldTypes.h's *database* field-type ordering for BOTH dimensions (STRING=0, CHAR=1,
    // UCHAR=2, SHORT=3, ..., FLOAT=9, DOUBLE=10, ENUM=11) -- not the CA-wire ordering above.
    // gate_dbr_to_dbf() already does exactly this CA-wire -> database-order translation for
    // the source native type; reuse it for the requested type too.
    int conv_col = gate_dbr_to_dbf(basic);
    GETCONVERTFUNC convert = dbGetConvertRoutine[dbf][conv_col];
    if (!convert) return -1;

    struct dbAddr addr;
    memset(&addr, 0, sizeof(addr));
    addr.field_type = (short)dbf;
    addr.pfield = (void*)rawValue;
    addr.no_elements = count;
    convert(&addr, value_dest, n, count, 0);

    *nRequest = n;
    return 0;
}
