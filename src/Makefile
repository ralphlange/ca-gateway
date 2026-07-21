TOP=..
include $(TOP)/configure/CONFIG
PROD_IOC = gateway
gateway_SRCS += camessage.c camsgtask.c caserverio.c caservertask.c
gateway_SRCS += cast_server.c online_notify.c rsrvIocRegister.c
gateway_SRCS += gateShim.c dbConvert.c
gateway_SRCS += GateMain.cpp GateLogic.cpp
gateway_LIBS += $(EPICS_BASE_IOC_LIBS)
gateway_SYS_LIBS += pcre2-8 cjson
include $(TOP)/configure/RULES
