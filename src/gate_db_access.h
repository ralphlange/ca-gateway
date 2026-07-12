#ifndef GATE_DB_ACCESS_H
#define GATE_DB_ACCESS_H
#include <epicsVersion.h>
#if EPICS_VERSION >= 7
#include <dbAccess.h>
#include <dbFldTypes.h>
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
#undef DBR_SHORT
#undef DBR_PUT_ACKT
#undef DBR_PUT_ACKS
#undef VALID_DB_REQ
#undef INVALID_DB_REQ
#undef DBR_GR_LONG
#undef DBR_GR_DOUBLE
#undef DBR_CTRL_LONG
#undef DBR_CTRL_DOUBLE
#include <db_access.h>
#else
#include <db_access.h>
#endif
#endif
