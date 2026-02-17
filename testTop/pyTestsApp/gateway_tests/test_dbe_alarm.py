#!/usr/bin/env python
import logging
import threading

import epics

from . import conftest

logger = logging.getLogger(__name__)


def test_alarm_level(standard_env: conftest.EnvironmentInfo):
    """
    DBE_ALARM monitor on an ai with two alarm levels - crossing the level
    generates updates
    """
    events_received = 0
    severity_unchanged = 0
    last_severity = 4
    cond = threading.Condition()

    def on_change(pvname=None, **kws):
        nonlocal events_received
        nonlocal severity_unchanged
        nonlocal last_severity

        with cond:
            events_received += 1
            logger.info(f'{pvname} changed to {kws["value"]} {kws["severity"]}')
            if last_severity == kws["severity"]:
                severity_unchanged += 1
            last_severity = kws["severity"]
            cond.notify()

    # gateway:passiveALRM has HIGH=5 (MINOR) and HIHI=10 (MAJOR)
    ioc, gw = conftest.get_pv_pair(
        "passiveALRM", auto_monitor=epics.dbr.DBE_ALARM, gateway_callback=on_change
    )
    ioc.get()
    gw.get()
    for val in [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0]:
        ioc.put(val, wait=True)

    # We get 6 events: at connection (INVALID), at first write (NO_ALARM),
    # and at the level crossings MINOR-MAJOR-MINOR-NO_ALARM.
    with cond:
        while events_received < 6:
            assert cond.wait(timeout=10.0)
    assert events_received == 6

    # no more events expected
    with cond:
        assert not cond.wait(timeout=1.0)
    # Any updates with unchanged severity are an error
    assert (
        severity_unchanged == 0
    ), f"{severity_unchanged} events with no severity changes received"
