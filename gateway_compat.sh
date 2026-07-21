#!/bin/bash
PVLIST=""
CIP=""
CPORT=""
SIP=""
SPORT=""
while [[ $# -gt 0 ]]; do
    case $1 in
        -pvlist) PVLIST="$2"; shift 2 ;;
        -cip) CIP="$2"; shift 2 ;;
        -cport) CPORT="$2"; shift 2 ;;
        -sip) SIP="$2"; shift 2 ;;
        -sport) SPORT="$2"; shift 2 ;;
        *) shift ;;
    esac
done
GATEWAY_BIN=$(dirname $0)/gateway.real
cat > gw.cmd <<SH
gateCreateClient internal "$CIP"
gateConnectPV internal ioc:auto:cnt
SH
export EPICS_CAS_INTF_ADDR_LIST=$SIP
export EPICS_CAS_SERVER_PORT=$SPORT
echo "Running as user dummy"
$GATEWAY_BIN gw.cmd
