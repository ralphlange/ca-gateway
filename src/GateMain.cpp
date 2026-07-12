#include <iocsh.h>
#include <epicsExit.h>
#include <epicsThread.h>
#include "rsrvIocRegister.h"
#include "GateCAClient.h"
#include "GateConfig.h"

extern "C" {
void rsrv_register_server();

static const iocshArg createClientArg0 = {"name", iocshArgString};
static const iocshArg createClientArg1 = {"addrList", iocshArgString};
static const iocshArg *const createClientArgs[2] = {&createClientArg0, &createClientArg1};
static const iocshFuncDef createClientFuncDef = {"gateCreateClient", 2, createClientArgs};
static void createClientCallFunc(const iocshArgBuf *args) {
    GateCAClientManager::instance().createClient(args[0].sval, args[1].sval ? args[1].sval : "");
}

static const iocshArg addPVArg0 = {"pattern", iocshArgString};
static const iocshArg addPVArg1 = {"clientName", iocshArgString};
static const iocshArg *const addPVArgs[2] = {&addPVArg0, &addPVArg1};
static const iocshFuncDef addPVFuncDef = {"gateAddPV", 2, addPVArgs};
static void addPVCallFunc(const iocshArgBuf *args) {
    GateConfig::instance().addPVPattern(args[0].sval, args[1].sval);
}

void GateRegisterCommands() {
    iocshRegister(&createClientFuncDef, createClientCallFunc);
    iocshRegister(&addPVFuncDef, addPVCallFunc);
}
}

int main(int argc, char *argv[]) {
    GateRegisterCommands();
    rsrvIocRegister();
    if (argc > 1) {
        iocsh(argv[1]);
    }
    iocsh(NULL);
    return 0;
}
