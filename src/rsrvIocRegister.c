#include "gate_compat.h"
#include "osiSock.h"
#include "iocsh.h"
#include "rsrv.h"
#include "server.h"
#include "epicsExport.h"
static const iocshArg casrArg0 = { "level",iocshArgInt};
static const iocshArg * const casrArgs[1] = {&casrArg0};
static const iocshFuncDef casrFuncDef = {"casr",1,casrArgs, "Channel Access Server Report"};
static void casrCallFunc(const iocshArgBuf *args) { casr(args[0].ival); }
static void rsrvRegistrar(void) {
    rsrv_register_server();
    iocshRegister(&casrFuncDef,casrCallFunc);
}
void rsrvIocRegister(void) { rsrvRegistrar(); }
