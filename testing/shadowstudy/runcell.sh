#!/bin/bash
# One Shadow cell of the cross-check: runcell.sh ARM SEED [TRANSPORT=quic] [N=500] [PAY=p1m]
#
# The arm table (arms.sh) is the fleet runner's for the arms the cross-check reproduces, so a cell's
# SEGMENT_* environment is the published cell's; NET selects the network scenario (home, dc20,
# dcb20). The topology is exported once per (N, seed, scenario) by TestShadowExport; the lab's
# shadowsim tools generate the simulation, run it under the stall watchdog, and fold the nodes'
# reports into results/<cell>.json.
#
# Layout (STUDY defaults to this script's directory; LAB to the lab checkout beside the tree):
#   $STUDY/../bin/segshadow.test   the harness test binary, built on the machine that runs Shadow
#   $STUDY/../topo/                exported topologies
#   $STUDY/../out/<cell>/          shadow.yaml, net.gml, cell.env, data/, shadow.std{out,err}
#   $STUDY/../results/<cell>.json  the record
# Knobs: PUBLISH_DELAY (60 s), HOLD (8 s), PARALLELISM (0 = all cores), QDISC (fifo, or round-robin
# for a fair share across sockets, closer to simnet's fq_codel; suffixes the cell), STALL_S (600),
# GENFLAGS (extra gen_shadow.py flags), EDGES=line|star for the known-answer cells, BIN/OUT/RES/TOPO
# to relocate.
set -uo pipefail
STUDY=${STUDY:-$(cd "$(dirname "$0")" && pwd)}
LAB=${LAB:-$(cd "$STUDY/../../.." && pwd)/eth-networking-lab}
TOOLS=$LAB/shadowsim/tools
BIN=${BIN:-$STUDY/../bin/segshadow.test}
OUT=${OUT:-$STUDY/../out}
RES=${RES:-$STUDY/../results}
TOPO=${TOPO:-$STUDY/../topo}
export SHADOW=${SHADOW:-$HOME/.local/bin/shadow}
[ -d "$TOOLS" ] || { echo "no shadowsim tools at $TOOLS (set LAB)"; exit 2; }

arm=${1:?arm}; seed=${2:?seed}; tr=${3:-quic}; N=${4:-500}; pay=${5:-p1m}
NET=${NET:-home}
COND=${COND:-cold}
source "$STUDY/arms.sh"

# The transport condition (arms.sh's COND): a warm cell publishes at 150 s and warms its links from
# 90 s before that (the harness's warm-up took ~30 s of simulated time at 50 nodes; 500 nodes x 70
# peers x 320 KiB is 22 MB per node); a cwndN cell over TCP runs the Shadow build whose Reno starts
# at SHADOW_TCP_INITCWND segments (the local patch; over QUIC the node applies the fork's knob).
case $COND in
  warm*) PUBLISH_DELAY=${PUBLISH_DELAY:-150}; GENFLAGS="${GENFLAGS:-} --warm-lead ${WARM_LEAD:-90}" ;;
  cwnd*) export SHADOW_TCP_INITCWND=${COND#cwnd}; export SHADOW=${SHADOW_CWND:-$HOME/.local/bin/shadow-cwnd} ;;
esac
QDISC=${QDISC:-fifo}
cell="${arm}_${seed}_${tr}_${pay}_n${N}"
[ "$NET" = home ] || cell="${cell}_${NET}"
[ "$COND" = cold ] || cell="${cell}_${COND}"
[ -z "$EDGES" ] || cell="${cell}_${EDGES}"
[ "$QDISC" = fifo ] || cell="${cell}_${QDISC}"
mkdir -p "$OUT/$cell" "$RES" "$TOPO"
topo="$TOPO/topo_${N}_${seed}_${LATENCY_MODEL:-geo}_d${DEGREE:-70}_${NET}${EDGES:+_$EDGES}.json"
if [ ! -f "$topo" ]; then
  # Written beside and moved into place, so a cell that shares the topology and starts in the same
  # second never reads a half-written file; the export is deterministic, so a double write is the same file.
  env $COMMON SHADOW_EXPORT="$topo.$$" ${EDGES:+SHADOW_EDGES=$EDGES} "$BIN" -test.run '^TestShadowExport$' -test.count=1 -test.v > "$OUT/$cell/export.log" 2>&1 || { echo "export failed, see $OUT/$cell/export.log"; exit 3; }
  mv -f "$topo.$$" "$topo"
fi
[ "$(python3 -c "import json; print(json.load(open('$topo'))['n'])")" = "$N" ] || { echo "topology $topo is not for $N nodes"; exit 3; }
# SEGMENT_DIFFUSION is not an arm knob, so arms.sh does not carry it: it asks each node for its
# per-segment curves, which is a question about any arm rather than a different arm. It reaches
# the nodes only if it is in the env file, since a Shadow process inherits nothing from here.
# Knobs that are questions about a cell rather than a different arm, so arms.sh does not carry
# them. A Shadow process inherits nothing from here, so they only reach the nodes through the env
# file. SHADOW_UDP_SNDBUF/RCVBUF override what quic-go asks for and are the only way to vary the
# socket buffer sizes, since quic-go's own request overwrites Shadow's configured defaults.
passthrough=""
for k in SEGMENT_DIFFUSION SHADOW_UDP_SNDBUF SHADOW_UDP_RCVBUF; do
  [ -z "$(eval echo "\$$k")" ] || passthrough="$passthrough $k=$(eval echo "\$$k")"
done
printf '%s\n' $COMMON $EXTRA SHADOW_VARIANT=$VARIANT $passthrough > "$OUT/$cell/cell.env"
gen() {
  python3 "$TOOLS/gen_shadow.py" --topology "$topo" --out "$OUT/$cell" --binary "$BIN" --transport "$tr" \
    --env-file "$OUT/$cell/cell.env" --seed "$seed" --publish-delay "${PUBLISH_DELAY:-60}" --hold "${HOLD:-8}" \
    --parallelism "${PARALLELISM:-0}" --qdisc "$QDISC" ${EDGES:+--edges $EDGES} ${GENFLAGS:-} "$@"
}
gen || exit 3
STALL_S=${STALL_S:-600} "$TOOLS/shadowrun.sh" "$OUT/$cell"; rc=$?
if [ $rc -eq 124 ]; then
  echo "retrying $cell with shadow seed $((seed + 1000))"
  gen --shadow-seed $((seed + 1000)) || exit 3
  STALL_S=${STALL_S:-600} "$TOOLS/shadowrun.sh" "$OUT/$cell"; rc=$?
fi
python3 "$TOOLS/extract.py" --data "$OUT/$cell/data" --n "$N" --arm "$arm" --seed "$seed" --transport "$tr" --pay "$pay" \
  --payload-bytes "$P" --deadline 3 --out "$RES/$cell.json" --shadow-rc "$rc" --stderr "$OUT/$cell/shadow.stderr" \
  --label "$cell" --binary "$BIN" --shadow "$SHADOW" --extra "net=$NET" --extra "cond=$COND" --extra "alias=$ALIAS" \
  --commit "$(head -n 1 "$BIN.commit" 2>/dev/null || echo unknown)" | tee "$OUT/$cell/summary.txt"
