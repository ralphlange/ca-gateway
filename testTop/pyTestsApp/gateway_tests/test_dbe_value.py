#!/usr/bin/env python
import logging
import threading

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

    # Start from 1, not 0: passive0's default VAL is already 0, so a put of 0 would be a
    # no-op (no real value change, hence no event) rather than one of the 10 intended changes.
    for val in range(1, 11):
        ioc.put(val, wait=True)

    # We get 11 events: at connection, then at 10 value changes (puts)
    with cond:
        while events_received < 11:
            assert cond.wait(timeout=10.0)
    assert events_received == 11

    # no more events expected
    with cond:
        assert not cond.wait(timeout=1.0)
