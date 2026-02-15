#!/usr/bin/env python
import logging
import threading
import time

import epics

from . import conftest

logger = logging.getLogger(__name__)


def test_prop_alarm_levels(standard_env: conftest.EnvironmentInfo):
    """
    Test property updates (client using DBE_PROPERTY flag) direct and through the Gateway

    DBE_PROPERTY monitor on an ai - value changes generate no events;
    property changes generate events.
    """
    events_received_gw = 0
    events_received_ioc = 0
    cond = threading.Condition()

    def on_change_gw(pvname=None, **kws):
        nonlocal events_received_gw
        with cond:
            events_received_gw += 1
            cond.notify()
        logger.info(f' GW update: {pvname} changed to {kws["value"]}')

    def on_change_ioc(pvname: str = "", value=None, **kwargs):
        nonlocal events_received_ioc
        with cond:
            events_received_ioc += 1
            cond.notify()
        logger.info(f"IOC update: {pvname} changed to {value}")

    # gateway:passive0 is a blank ai record
    ioc, gw = conftest.get_pv_pair(
        "passive0",
        auto_monitor=epics.dbr.DBE_PROPERTY,
        ioc_callback=on_change_ioc,
        gateway_callback=on_change_gw,
    )
    pvhihi = epics.PV("ioc:passive0.HIHI", auto_monitor=None)
    pvlolo = epics.PV("ioc:passive0.LOLO", auto_monitor=None)
    pvhigh = epics.PV("ioc:passive0.HIGH", auto_monitor=None)
    pvlow = epics.PV("ioc:passive0.LOW", auto_monitor=None)
    ioc.get()
    gw.get()

    for val in range(10):
        ioc.put(val, wait=True)

    # We get 1 event: at connection
    with cond:
        while events_received_gw < 1 or events_received_ioc < 1:
            assert cond.wait(timeout=10.0)
    assert events_received_gw == 1
    assert events_received_ioc == 1

    pvhihi.put(20.0, wait=True)
    pvhigh.put(18.0, wait=True)
    pvlolo.put(10.0, wait=True)
    pvlow.put(12.0, wait=True)

    # Depending on the IOC (supporting PROPERTY changes on limits or not) we get 0 or 4 events.
    with cond:
        while events_received_gw != events_received_ioc:
            assert cond.wait(timeout=10.0)
        # Wait a little bit longer for any more events
        cond.wait(timeout=0.2)
    # Pass test if updates from IOC act the same as updates from GW
    assert events_received_gw == events_received_ioc, (
        f"Expected equal number of updates; received {events_received_gw} "
        f"from GW and {events_received_ioc} from IOC"
    )
