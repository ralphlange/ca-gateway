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
gateway_SRCS += GateMain.cpp GateLogic.cpp GateFormat.cpp GateQueue.cpp

PROD_LIBS += $(EPICS_BASE_IOC_LIBS)

gateway_SYS_LIBS += pcre2-8

# caPutLog (CAPUTLOG in RELEASE.local): only its plain data definitions (LOGDATA/VALUE,
# caPutLogTask.h) and config enum (caPutLog.h) are used -- header-only, no library to link --
# see GateLogic.cpp's caPutLog integration comment for why none of its runtime code (which all
# depends, directly or via a shared free-list, on caPutLogAsInit()) can be used here. Adding
# $(CAPUTLOG)/include automatically (RULES.ioc, from CAPUTLOG in RELEASE.local).
#
# Support is entirely optional and configured by whether CAPUTLOG is set in RELEASE.local:
# without it, gateLoadPutLogText/gateLoadPutLogJson are still registered as iocsh commands
# (GateMain.cpp) but just report the feature isn't built in (see GateLogic.cpp).
ifdef CAPUTLOG
USR_CPPFLAGS += -DWITH_CAPUTLOG
endif

# Only for static builds:
# As gateShim.c intentionally re-implements functions of EPICS Base,
# add --allow-multiple-definition to keep gateShim.o's definitions
# and just drop the archives' conflicting ones.
ifeq ($(STATIC_BUILD),YES)
gateway_LDFLAGS += -Wl,--allow-multiple-definition
endif

include $(TOP)/configure/RULES
