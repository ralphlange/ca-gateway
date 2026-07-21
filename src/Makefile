#*************************************************************************
# Copyright (c) 2026 ITER Organization.
# This file is distributed subject to a Software License Agreement found
# in the file LICENSE that is included with this distribution.
#*************************************************************************

TOP = ..
include $(TOP)/configure/CONFIG

PROD_IOC = gateway

# Copy of rsrv
gateway_SRCS += camessage.c camsgtask.c caserverio.c caservertask.c
gateway_SRCS += cast_server.c online_notify.c

gateway_SRCS += rsrvIocRegister.c
gateway_SRCS += gateShim.c
gateway_SRCS += dbConvert.c
gateway_SRCS += GateMain.cpp GateLogic.cpp

PROD_LIBS += $(EPICS_BASE_IOC_LIBS)

gateway_SYS_LIBS += pcre2-8

include $(TOP)/configure/RULES
