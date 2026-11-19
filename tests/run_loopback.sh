#!/bin/sh
# End to end loopback tests: starts real SoapyTCPServer instances on localhost and
# drives them with the fake device. Usage: run_loopback.sh <build dir>
set -u
BUILD=${1:?usage: run_loopback.sh <build dir>}
BUILD=$(cd "$BUILD" && pwd)
export SOAPY_SDR_PLUGIN_PATH="$BUILD"
export SOAPY_SDR_LOG_LEVEL=WARNING
unset SOAPY_TCPREMOTE_KEY
P1=${PORT_BASE:-20700}; P2=$((P1+1))
LOG=$(mktemp)
SERVERS=""
cleanup() { for p in $SERVERS; do kill "$p" 2>/dev/null; done; rm -f "$LOG"; }
trap cleanup EXIT INT TERM
rc=0

"$BUILD/SoapyTCPServer" -p $P1 -b 250 >>"$LOG" 2>&1 & SERVERS="$SERVERS $!"
SOAPY_TCPREMOTE_KEY= "$BUILD/SoapyTCPServer" -p $P2 -b 250 -k s3cret >>"$LOG" 2>&1 & SERVERS="$SERVERS $!"
sleep 1

run() { echo "--- $*"; "$BUILD/LoopbackTest" "$@" || rc=1; }
run $P1 robust
run $P1 stream
run $P2 authbad
run $P2 authok s3cret

# the plain server must still be alive after everything above
for p in $SERVERS; do kill -0 "$p" 2>/dev/null || { echo "FAIL: a server process exited"; rc=1; }; done
if [ $rc -ne 0 ]; then echo "=== server log ==="; tail -40 "$LOG"; fi
[ $rc -eq 0 ] && echo "ALL LOOPBACK TESTS PASSED"
exit $rc
