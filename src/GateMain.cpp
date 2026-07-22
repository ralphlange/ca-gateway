#include "gate_compat.h"
#include "gate_db_interface.h"
#include <iocsh.h>
#include <epicsExit.h>
#include <iostream>

extern "C" {
void rsrvIocRegister(void);
static const iocshArg clA0 = { "name", iocshArgString }, clA1 = { "addr_list", iocshArgString }, clA2 = { "auto_addr", iocshArgInt }, clA3 = { "port", iocshArgInt };
static const iocshArg * const clAs[] = { &clA0, &clA1, &clA2, &clA3 };
static const iocshFuncDef clDef = { "gateCreateClient", 4, clAs, "Create client" };
static void clCall(const iocshArgBuf *a) { gate_create_client_cmd(a[0].sval, a[1].sval, a[2].ival, a[3].ival); }

static const iocshArg pvA0 = { "pattern", iocshArgString }, pvA1 = { "client", iocshArgString }, pvA2 = { "as_group", iocshArgString }, pvA3 = { "target", iocshArgString };
static const iocshArg * const pvAs[] = { &pvA0, &pvA1, &pvA2, &pvA3 };
static const iocshFuncDef pvDef = { "gateAddPV", 4, pvAs, "Add PV (optional target: PCRE2 $1-style rewrite of the upstream name)" };
static void pvCall(const iocshArgBuf *a) { gate_add_pv_cmd(a[0].sval, a[1].sval, a[2].sval, a[3].sval); }

static const iocshArg ldA0 = { "filename", iocshArgString };
static const iocshArg * const ldAs[] = { &ldA0 };
static const iocshFuncDef ldDef = { "gateLoadConfig", 1, ldAs, "Load JSON config" };
static void ldCall(const iocshArgBuf *a) { gate_load_config(a[0].sval); }

void gateIocRegister(void) {
    iocshRegister(&clDef, clCall);
    iocshRegister(&pvDef, pvCall);
    iocshRegister(&ldDef, ldCall);
}
}

int main(int argc, char *argv[]) {
    gate_init(); rsrvIocRegister(); gateIocRegister();
    if (rsrv_psrv) { if (rsrv_psrv->init) rsrv_psrv->init(); if (rsrv_psrv->run) rsrv_psrv->run(); }
    std::cout << "CA Gateway (rsrv-based) starting..." << std::endl;
    iocsh(NULL); return 0;
}
