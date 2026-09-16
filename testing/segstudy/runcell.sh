#!/bin/bash
# One cell of the study, the way the fleet ran it: ARM SEED FAULT NET PAY [N]
# Writes logs/<tag>.log (the harness output the extractor reads), logs/<tag>.time (GNU time -v)
# and logs/<tag>.done (the exit code). Skips a cell whose .done exists. BIN names the test binary
# built with `go test -c -o segfu.test ./beacon-chain/p2p/segmentintegrationtest` from this tree.
cd "$(dirname "$0")" || exit 1
N=${6:-500}; sfx=""; [ "$N" != 500 ] && sfx="_n$N"
tag="$1_$2_$3_$4_$5$sfx"
mkdir -p logs
[ -f logs/$tag.done ] && exit 0
N=$N BIN=${BIN:-./segfu.test} /usr/bin/time -v -o logs/$tag.time ./runfu.sh "$1" "$2" "$3" "$4" "$5" > logs/$tag.log 2>&1
echo rc=$? > logs/$tag.done
