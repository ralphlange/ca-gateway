export BASE=/home/jules/.cache/base-7.0
export PATH=$BASE/bin/linux-x86_64:$PATH
export LD_LIBRARY_PATH=$BASE/lib/linux-x86_64:$LD_LIBRARY_PATH

# 1. Start Soft IOC
cat > ioc.cmd <<IOCEOF
record(ao, "testpv") {
    field(VAL, "42.0")
}
IOCEOF
softIoc -d ioc.cmd > ioc.log 2>&1 &
IOC_PID=$!

# 2. Start Gateway
cat > gate.json <<JSONEOF
{
    "clients": [
        { "name": "default", "addr_list": "127.0.0.1", "auto_addr": 0, "port": 5064 }
    ],
    "pvs": [
        { "pattern": "testpv", "client": "default", "as_group": "DEFAULT" }
    ]
}
JSONEOF

export EPICS_CA_SERVER_PORT=5065
export EPICS_CAS_INTF_ADDR_LIST=127.0.0.1
./bin/linux-x86_64/gateway > gate.log 2>&1 <<GATEEOF &
gateLoadConfig gate.json
GATEEOF
GATE_PID=$!

sleep 3

echo "--- GATEWAY LOG ---"
cat gate.log

# 3. Client Request
echo "--- CAGET ---"
EPICS_CA_ADDR_LIST=127.0.0.1:5065 EPICS_CA_AUTO_ADDR_LIST=NO caget testpv

kill $GATE_PID $IOC_PID 2>/dev/null
