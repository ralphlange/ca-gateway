#include "gate_compat.h"
#include "gate_db_interface.h"
#include <iocsh.h>
#include <epicsExit.h>
#include <iostream>

/* iocshFuncDef gained a trailing `usage` string field in Base 7.0 (flagged by its own
 * IOCSHFUNCDEF_HAS_USAGE feature-detection macro); Base 3.15's is one field shorter. */
#ifdef IOCSHFUNCDEF_HAS_USAGE
#  define GATE_IOCSH_FUNCDEF(name, nargs, args, usage) { name, nargs, args, usage }
#else
#  define GATE_IOCSH_FUNCDEF(name, nargs, args, usage) { name, nargs, args }
#endif

extern "C" {
void rsrvIocRegister(void);
static const iocshArg clA0 = { "name", iocshArgString }, clA1 = { "addr_list", iocshArgString }, clA2 = { "auto_addr", iocshArgInt }, clA3 = { "port", iocshArgInt };
static const iocshArg * const clAs[] = { &clA0, &clA1, &clA2, &clA3 };
static const iocshFuncDef clDef = GATE_IOCSH_FUNCDEF("gateCreateClient", 4, clAs, "Create client");
static void clCall(const iocshArgBuf *a) { gate_create_client_cmd(a[0].sval, a[1].sval, a[2].ival, a[3].ival); }

static const iocshArg pvA0 = { "pattern", iocshArgString }, pvA1 = { "client", iocshArgString }, pvA2 = { "as_group", iocshArgString }, pvA3 = { "target", iocshArgString };
static const iocshArg * const pvAs[] = { &pvA0, &pvA1, &pvA2, &pvA3 };
static const iocshFuncDef pvDef = GATE_IOCSH_FUNCDEF("gateAddPV", 4, pvAs, "Add PV (optional target: PCRE2 $1-style rewrite of the upstream name)");
static void pvCall(const iocshArgBuf *a) { gate_add_pv_cmd(a[0].sval, a[1].sval, a[2].sval, a[3].sval); }

static const iocshArg ldA0 = { "filename", iocshArgString };
static const iocshArg * const ldAs[] = { &ldA0 };
static const iocshFuncDef ldDef = GATE_IOCSH_FUNCDEF("gateLoadConfig", 1, ldAs, "Load JSON config");
static void ldCall(const iocshArgBuf *a) { gate_load_config(a[0].sval); }

static const iocshArg laA0 = { "filename", iocshArgString };
static const iocshArg * const laAs[] = { &laA0 };
static const iocshFuncDef laDef = GATE_IOCSH_FUNCDEF("gateLoadAccess", 1, laAs, "Load access security (ACF) file");
static void laCall(const iocshArgBuf *a) { gate_load_access(a[0].sval); }

static const iocshArg siA0 = { "prefix", iocshArgString }, siA1 = { "as_group", iocshArgString };
static const iocshArg * const siAs[] = { &siA0, &siA1 };
static const iocshFuncDef siDef = GATE_IOCSH_FUNCDEF("gateInitStats", 2, siAs,
    "Create gateway statistics PVs (<prefix>:vctotal/pvtotal/connected/active/inactive); as_group defaults to DEFAULT");
static void siCall(const iocshArgBuf *a) { gate_init_stats_cmd(a[0].sval, a[1].sval); }

static const iocshArg ptA0 = { "addr", iocshArgString }, ptA1 = { "config", iocshArgInt }, ptA2 = { "timeout", iocshArgDouble };
static const iocshArg * const ptAs[] = { &ptA0, &ptA1, &ptA2 };
static const iocshFuncDef ptDef = GATE_IOCSH_FUNCDEF("gateLoadPutLogText", 3, ptAs,
    "Configure the traditional-format caPutLog network sink (config: -1 none/0 on-change/1 all/2 all-no-filter)");
static void ptCall(const iocshArgBuf *a) { gate_load_put_log_text_cmd(a[0].sval, a[1].ival, a[2].dval); }

static const iocshArg pjA0 = { "addr", iocshArgString }, pjA1 = { "config", iocshArgInt }, pjA2 = { "timeout", iocshArgDouble };
static const iocshArg * const pjAs[] = { &pjA0, &pjA1, &pjA2 };
static const iocshFuncDef pjDef = GATE_IOCSH_FUNCDEF("gateLoadPutLogJson", 3, pjAs,
    "Configure the JSON-format caPutLog network sink (config: -1 none/0 on-change/1 all/2 all-no-filter)");
static void pjCall(const iocshArgBuf *a) { gate_load_put_log_json_cmd(a[0].sval, a[1].ival, a[2].dval); }

static const iocshArg pfA0 = { "filename", iocshArgString };
static const iocshArg * const pfAs[] = { &pfA0 };
static const iocshFuncDef pfDef = GATE_IOCSH_FUNCDEF("gateLoadPutLogFile", 1, pfAs,
    "Configure a local put-log file (drop-in replacement for the old Gateway's -putlog <file>)");
static void pfCall(const iocshArgBuf *a) { gate_load_put_log_file_cmd(a[0].sval); }

void gateIocRegister(void) {
    iocshRegister(&clDef, clCall);
    iocshRegister(&pvDef, pvCall);
    iocshRegister(&ldDef, ldCall);
    iocshRegister(&laDef, laCall);
    iocshRegister(&siDef, siCall);
    iocshRegister(&ptDef, ptCall);
    iocshRegister(&pjDef, pjCall);
    iocshRegister(&pfDef, pfCall);
}
}

int main(int argc, char *argv[]) {
    gate_init(); rsrvIocRegister(); gateIocRegister();
    if (rsrv_psrv) { if (rsrv_psrv->init) rsrv_psrv->init(); if (rsrv_psrv->run) rsrv_psrv->run(); }
    std::cout << "CA Gateway (rsrv-based) starting..." << std::endl;
    iocsh(NULL); return 0;
}
