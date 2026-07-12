#include "gate_compat.h"
#include "osiSock.h"
#include "iocsh.h"
#include "rsrv.h"
#include "server.h"
#include "epicsExport.h"
#include "GateConfig.h"
#include "GateCache.h"
#include <iostream>

extern "C" {
/* casr */
static const iocshArg casrArg0 = { "level",iocshArgInt};
static const iocshArg * const casrArgs[1] = {&casrArg0};
static const iocshFuncDef casrFuncDef = {"casr",1,casrArgs,
                                         "Channel Access Server Report with following levels:\n"
                                         "  0  - server’s protocol version level and summary for each attached client\n"
                                         "  1  - extends report with information about connected clients and beacons\n"
                                         "  2  - extends report with specific channel names and UDP search requests\n"
                                         "  3+ - expert\n"};
static void casrCallFunc(const iocshArgBuf *args)
{
    casr(args[0].ival);
}

/* gateReport */
static const iocshArg gateReportArg0 = { "level",iocshArgInt};
static const iocshArg * const gateReportArgs[1] = {&gateReportArg0};
static const iocshFuncDef gateReportFuncDef = {"gateReport",1,gateReportArgs, "Gateway Statistics Report"};
static void gateReportCallFunc(const iocshArgBuf *args)
{
    GateCache::instance().report(args[0].ival);
    GateConfig::instance().report(args[0].ival);
}

/* gateReload */
static const iocshFuncDef gateReloadFuncDef = {"gateReload",0,NULL, "Reload Gateway Configuration"};
static void gateReloadCallFunc(const iocshArgBuf *args)
{
    GateConfig::instance().reload();
}

/* gateAsCheck */
static const iocshArg gateAsCheckArg0 = { "pvname",iocshArgString};
static const iocshArg gateAsCheckArg1 = { "user",iocshArgString};
static const iocshArg gateAsCheckArg2 = { "host",iocshArgString};
static const iocshArg * const gateAsCheckArgs[3] = {&gateAsCheckArg0, &gateAsCheckArg1, &gateAsCheckArg2};
static const iocshFuncDef gateAsCheckFuncDef = {"gateAsCheck",3,gateAsCheckArgs, "Check Access Security for a PV"};
static void gateAsCheckCallFunc(const iocshArgBuf *args)
{
    std::string clientName;
    bool allowed = GateConfig::instance().isAllowed(args[0].sval, args[1].sval, args[2].sval, clientName);
    std::cout << "PV: " << args[0].sval << " User: " << args[1].sval << " Host: " << args[2].sval
              << " => " << (allowed ? "ALLOWED" : "DENIED") << " (Client Group: " << clientName << ")" << std::endl;
}

void rsrvIocRegister(void)
{
    rsrv_register_server();
    iocshRegister(&casrFuncDef,casrCallFunc);
    iocshRegister(&gateReportFuncDef,gateReportCallFunc);
    iocshRegister(&gateReloadFuncDef,gateReloadCallFunc);
    iocshRegister(&gateAsCheckFuncDef,gateAsCheckCallFunc);
}
}
