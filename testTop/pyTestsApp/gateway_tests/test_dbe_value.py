#!/usr/bin/env python
import logging
import threading
import time

import epics

from . import conftest

logger = logging.getLogger(__name__)


def test_value_no_deadband(standard_env: conftest.EnvironmentInfo):
    """DBE_VALUE monitor on an ai - value changes generate events."""
    events_received = 0
    cond = threading.Condition()

    def on_change(pvname=None, **kws):
        nonlocal events_received
        with cond:
            events_received += 1
            cond.notify()
        logger.info(f' GW update: {pvname} changed to {kws["value"]}')

    # gateway:passive0 is a blank ai record
    ioc, gw = conftest.get_pv_pair(
        "passive0", auto_monitor=epics.dbr.DBE_VALUE, gateway_callback=on_change
    )
    ioc.get()
    gw.get()

    for val in range(10):
        ioc.put(val, wait=True)

    # We get 11 events: at connection, then at 10 value changes (puts)
    with cond:
        while events_received < 11:
            assert cond.wait(timeout=10.0)
    assert events_received == 11

    # no more events expected
    with cond:
        assert not cond.wait(timeout=1.0)
