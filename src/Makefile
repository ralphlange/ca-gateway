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
gateway_SRCS += GateMain.cpp GateLogic.cpp GateFormat.cpp

PROD_LIBS += $(EPICS_BASE_IOC_LIBS)

gateway_SYS_LIBS += pcre2-8

# Only for static builds:
# As gateShim.c intentionally re-implements functions of EPICS Base,
# add --allow-multiple-definition to keep gateShim.o's definitions
# and just drop the archives' conflicting ones.
ifeq ($(STATIC_BUILD),YES)
gateway_LDFLAGS += -Wl,--allow-multiple-definition
endif

include $(TOP)/configure/RULES
