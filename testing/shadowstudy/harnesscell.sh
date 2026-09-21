#!/bin/bash
# The in-process harness on the same cell: harnesscell.sh ARM SEED [N=500] [PAY=p1m]
# Runs the variant's test (arms.sh's VARIANT) with the cell's environment (NET selects the scenario) and
# prints the driver's summary line, so a Shadow cell has its harness twin at any node count on the
# same source tree (the published 500-node cells, from the research tree, are in the study's
# fu_results.json). BIN is a harness test binary (go test -c of the package).
set -uo pipefail
STUDY=${STUDY:-$(cd "$(dirname "$0")" && pwd)}
BIN=${BIN:-$STUDY/../bin/segshadow.test}
RES=${RES:-$STUDY/../results}
arm=${1:?arm}; seed=${2:?seed}; N=${3:-500}; pay=${4:-p1m}
NET=${NET:-home}
source "$STUDY/arms.sh"
[ -z "$EDGES" ] || { echo "harnesscell.sh runs mesh cells only"; exit 2; }
cell="${arm}_${seed}_harness_${pay}_n${N}"
[ "$NET" = home ] || cell="${cell}_${NET}"
mkdir -p "$RES"
case $VARIANT in a) TESTP=TestQ6RealisticMesh;; b) TESTP=TestVariantBDiffusion;; c) TESTP=TestVariantCDiffusion;; *) echo "bad VARIANT $VARIANT"; exit 2;; esac
env $COMMON $EXTRA GOMAXPROCS=${GOMAXPROCS:-2} "$BIN" -test.run "^$TESTP\$" -test.count=1 -test.v -test.timeout 0 > "$RES/$cell.log" 2>&1
rc=$?
grep -E 'Mbps L=' "$RES/$cell.log" | sed 's/^ *q6_mesh_test.go:[0-9]*: *//' | cut -c1-260
[ $rc -eq 0 ] || { echo "harness exited $rc, see $RES/$cell.log"; grep -E 'FAIL|panic|TIMEOUT' "$RES/$cell.log" | head -3; }
exit $rc
