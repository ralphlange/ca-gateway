TOP = ..
include $(TOP)/configure/CONFIG

PROD_HOST = gateway

# RSRV source files
gateway_SRCS += camessage.c
gateway_SRCS += camsgtask.c
gateway_SRCS += caserverio.c
gateway_SRCS += caservertask.c
gateway_SRCS += cast_server.c
gateway_SRCS += online_notify.c
gateway_SRCS += rsrvIocRegister.c

# EPICS Database conversion
gateway_SRCS += dbConvert.c

# Gateway implementation files
gateway_SRCS += GateMain.cpp
gateway_SRCS += GateCache.cpp
gateway_SRCS += GateCAClient.cpp
gateway_SRCS += GateAs.cpp
gateway_SRCS += GateVirtualPV.cpp
gateway_SRCS += GateConfig.cpp
gateway_SRCS += gateShim.cpp

USR_INCLUDES += -I.

PROD_LIBS += $(EPICS_BASE_HOST_LIBS)

include $(TOP)/configure/RULES
