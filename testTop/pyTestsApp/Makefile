TOP=..

include $(TOP)/configure/CONFIG
#----------------------------------------
#  ADD MACRO DEFINITIONS AFTER THIS LINE
#=============================

# Test programs
PYTESTS += TestCSStudio.py
PYTESTS += TestDBEAlarm.py
PYTESTS += TestDBELog.py
PYTESTS += TestDBEProp.py
PYTESTS += TestDBEValue.py
PYTESTS += TestEnumPropertyCache.py
PYTESTS += TestEnumUndefinedTimestamp.py
PYTESTS += TestPropertyCache.py
PYTESTS += TestStructures.py
PYTESTS += TestWaveformWithCAMaxArrayBytes.py

TAPFILES = $(PYTESTS:.py=.tap)

TESTFILES += test.db
TESTFILES += access.txt

# Which pvlist.txt file depends on USE_PCRE or USE_PCRE2
USE_PCRE2 ?= NO
USE_PCRE ?= NO

ifeq ($(USE_PCRE2),YES)
  PVLIST_FILE = pvlist_pcre.txt
else ifeq ($(USE_PCRE),YES)
  PVLIST_FILE = pvlist_pcre.txt
else
  PVLIST_FILE = pvlist_bre.txt
endif

CLEANS += *.pyc $(TESTFILES) pvlist.txt gateway.* *.tap

include $(TOP)/configure/RULES
#----------------------------------------
#  ADD RULES AFTER THIS LINE

ifeq ($(T_A),$(EPICS_HOST_ARCH))

# Environment required by some test programs
export BASE ?= $(EPICS_VERSION).$(EPICS_REVISION)
export EPICS_BASE

ifeq ($(BASE_3_14),YES)
clean::
	@$(RM) $(CLEANS)
endif

runtests:
#	nosetests $(PYTESTS:%=../%)
	nose2 -v --pretty-assert --start-dir .. --config ../nose2.cfg

$(TAPFILES): .runtests.tap ;
.INTERMEDIATE: .runtests.tap
.runtests.tap:
#	nosetests --with-tap $(PYTESTS:%=../%)
	nose2 -v --pretty-assert --start-dir .. --config ../nose2.cfg

# Workaround for 3.14 make rules: special target to avoid circular dependencies
build runtests tapfiles: testfiles pvlist.txt
testfiles: $(TESTFILES:%=../%)
	$(ECHO) "Copying test files $(TESTFILES)"
	@$(CP) $^ .

pvlist.txt: ../$(PVLIST_FILE)
	$(ECHO) "Copying $< to $@ (USE_PCRE2: $(USE_PCRE2), USE_PCRE: $(USE_PCRE))"
	@$(CP) $< $@

endif
