#include <asLib.h>
#include <iocsh.h>
#include "dbCommon.h"
#include "GateCache.h"

extern "C" {

static const iocshArg asSetFilenameArg0 = {"filename", iocshArgString};
static const iocshArg *const asSetFilenameArgs[1] = {&asSetFilenameArg0};
static const iocshFuncDef asSetFilenameFuncDef = {"asSetFilename", 1, asSetFilenameArgs};
static void asSetFilenameCallFunc(const iocshArgBuf *args) {
    asInitFile(args[0].sval, nullptr);
}

static const iocshFuncDef asInitFuncDef = {"asInit", 0, nullptr};
static void asInitCallFunc(const iocshArgBuf *args) {
    // asInitialize(nullptr); // Standard asLib init
}

void GateAsRegisterCommands() {
    iocshRegister(&asSetFilenameFuncDef, asSetFilenameCallFunc);
    iocshRegister(&asInitFuncDef, asInitCallFunc);
}

}
