#!/usr/bin/env python
import os
import unittest
import epics
import IOCControl
import GatewayControl
import gwtests
import time

UNDEFINED_TIMESTAMP = epics.dbr.EPICS2UNIX_EPOCH

gwtests.verbose = False

def timestamp_to_string(timestamp: float) -> str:
    if timestamp == UNDEFINED_TIMESTAMP or timestamp is None:
        return "<undefined>"
    return time.ctime(timestamp)

class TestEnumUndefinedTimestamp(unittest.TestCase):
    '''Test multiple caget of enum have valid timestamps through the Gateway'''

    def setUp(self):
        gwtests.setup()
        self.iocControl = IOCControl.IOCControl()
        self.gatewayControl = GatewayControl.GatewayControl()
        self.iocControl.startIOC()
        self.gatewayControl.startGateway()
        os.environ["EPICS_CA_AUTO_ADDR_LIST"] = "NO"
        os.environ["EPICS_CA_ADDR_LIST"] = "localhost:{0} localhost:{1}".format(gwtests.iocPort,gwtests.gwPort)
        epics.ca.initialize_libca()
        self.eventsReceivedGW = 0
        self.eventsReceivedIOC = 0

    def tearDown(self):
        epics.ca.finalize_libca()
        self.gatewayControl.stop()
        self.iocControl.stop()

    def onChangeGW(self, pvname=None, timestamp=None, **kws):
        self.eventsReceivedGW += 1
        if gwtests.verbose:
            print(	" GW update: ", pvname, " changed to ", kws['value'],
                    " at ", timestamp_to_string(timestamp) )

    def onChangeIOC(self, pvname=None, timestamp=None, **kws):
        self.eventsReceivedIOC += 1
        if gwtests.verbose:
            print(	"IOC update: ", pvname, " changed to ", kws['value'],
                    " at ", timestamp_to_string(timestamp) )

    def testUndefTimestamp(self):
        '''Two caget on an mbbi - both timestamps should be defined.'''
        iocPV = epics.PV("ioc:HUGO:ENUM", auto_monitor=None)
        iocPV.add_callback(self.onChangeIOC)
        gwPV1  = epics.PV("gateway:HUGO:ENUM", auto_monitor=None)
        gwPV1.add_callback(self.onChangeGW)
        iocEnumValue = iocPV.get()
        gwEnumValue  = gwPV1.get()

        # Verify timestamp and value match
        self.assertTrue( iocEnumValue == gwEnumValue,
                'ioc enum {0} !=\ngw enum {1}'.format( iocEnumValue, gwEnumValue ) )

        # Close current CA context and open a new one
        epics.ca.detach_context()
        epics.ca.create_context()

        '''Two caget on an mbbi - both timestamps should be defined.'''
        # Now get the gateway value again and make sure the timestamp is not undefined
        gwPV2  = epics.PV("gateway:HUGO:ENUM", auto_monitor=None)
        gwPV2.add_callback(self.onChangeGW)
        gwEnumValue  = gwPV2.get()
        if iocPV.status != epics.dbr.AlarmStatus.UDF:
            self.assertTrue( gwPV2.status != epics.dbr.AlarmStatus.UDF,
                '2nd CA get is undefined!' )
        self.assertTrue( gwPV2.timestamp != 0, '2nd CA get timestamp is undefined!' )
        self.assertTrue( iocEnumValue == gwEnumValue,
                'ioc enum {0} !=\ngw enum {1}'.format( iocEnumValue, gwEnumValue ) )

if __name__ == '__main__':
    unittest.main(verbosity=2)
