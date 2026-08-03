TOP=..

include $(TOP)/configure/CONFIG
#----------------------------------------
#  ADD MACRO DEFINITIONS AFTER THIS LINE
#=============================

# Unit tests for the Gateway's own C++ pieces that can be exercised without a running
# gateway/IOC pair (unlike pyTestsApp, which is end-to-end). GateQueue.cpp is deliberately
# free of any CA/rsrv dependency, so it links here on its own.
GATEWAY_SRC = $(TOP)/../src
USR_CPPFLAGS += -I$(GATEWAY_SRC)

TESTPROD_HOST += gateQueueTest
gateQueueTest_SRCS += gateQueueTest.cpp
gateQueueTest_SRCS += GateQueue.cpp
gateQueueTest_LIBS += $(EPICS_BASE_HOST_LIBS)
TESTS += gateQueueTest

TESTSCRIPTS_HOST += $(TESTS:%=%.t)

include $(TOP)/configure/RULES
#----------------------------------------
#  ADD RULES AFTER THIS LINE

# GateQueue.cpp lives with the gateway sources, not here.
vpath %.cpp $(GATEWAY_SRC)
