#!/bin/bash
# One cell on either substrate, idempotent: cell.sh SUBSTRATE ARM SEED TRANSPORT N PAY [NET=home] [COND=cold]
#
# The lane driver (eth-networking-lab shadowsim/tools/lane.sh) feeds it lines of a cells file. The
# label names the record; a cell whose record says ok is skipped, so re-running a lane resumes it.
# SUBSTRATE is shadow (runcell.sh: export, generate, run, extract) or harness (harnesscell.sh, the
# in-process driver, then harnessrecord.py). The harness carries QUIC over simnet and refuses tcp.
set -uo pipefail
STUDY=${STUDY:-$(cd "$(dirname "$0")" && pwd)}
LAB=${LAB:-$(cd "$STUDY/../../.." && pwd)/eth-networking-lab}
RES=${RES:-$STUDY/../results}
sub=${1:?substrate}; arm=${2:?arm}; seed=${3:?seed}; tr=${4:?transport}; N=${5:?n}; pay=${6:?pay}; net=${7:-home}; cond=${8:-cold}
suffix=""; [ "${DEGREE:-70}" = 70 ] || suffix="_d${DEGREE}"
[ -z "${V05:-}" ] || suffix="${suffix}_v05${V05//,/}"; [ -z "${SRCHAVE:-}" ] || suffix="${suffix}_sh${SRCHAVE//[:,]/}"
[ "$net" = home ] || suffix="${suffix}_$net"; [ "$cond" = cold ] || suffix="${suffix}_$cond"
# The queue discipline is part of a Shadow cell's identity -- runcell.sh puts it in the record's
# name -- so it has to be part of the label the skip is decided on. Without it a lane run under a
# second discipline finds the first one's records, calls every cell done, and exits having run
# nothing, which reads exactly like a lane that had no work to do.
[ "${QDISC:-fifo}" = fifo ] || suffix="${suffix}_${QDISC}"
case $sub in
  shadow)  label="${arm}_${seed}_${tr}_${pay}_n${N}${suffix}" ;;
  harness) label="${arm}_${seed}_harness_${pay}_n${N}${suffix}"
           [ "$tr" = quic ] || { echo "== $label: the in-process harness carries QUIC only"; exit 2; } ;;
  *) echo "bad substrate $sub (shadow|harness)"; exit 2 ;;
esac
if [ -f "$RES/$label.json" ] && python3 -c "import json,sys; sys.exit(0 if json.load(open('$RES/$label.json')).get('ok') else 1)"; then
  echo "== skip $label (ok)"; exit 0
fi
echo "== $(date -u) $label"
t0=$(date +%s)
case $sub in
  shadow)
    NET=$net COND=$cond RES=$RES "$STUDY/runcell.sh" "$arm" "$seed" "$tr" "$N" "$pay" 2>&1 | grep -v "^wrote" | cut -c1-400 ;;
  harness)
    NET=$net COND=$cond RES=$RES "$STUDY/harnesscell.sh" "$arm" "$seed" "$N" "$pay"
    read -r P ALIAS MULTI < <(NET=$net COND=$cond arm=$arm seed=$seed N=$N pay=$pay bash -c "source '$STUDY/arms.sh'; echo \$P \$ALIAS \$MULTI")
    BIN=${BIN:-$STUDY/../bin/segshadow.test}
    if [ -n "${MULTI:-}" ]; then
      # A multi-message cell: the driver's per-message lines, folded as the Shadow side folds its own.
      python3 "$STUDY/msgextract.py" --harness-log "$RES/$label.log" --tools "$LAB/shadowsim/tools" --n "$N" \
        --arm "$arm" --seed "$seed" --transport quic --pay "$pay" --payload-bytes "$P" --label "$label" \
        --extra "alias=$ALIAS" --extra "cond=$cond" --extra "degree=${DEGREE:-70}" \
        --commit "$(head -n 1 "$BIN.commit" 2>/dev/null || echo unknown)" --out "$RES/$label.json" > /dev/null
    else
    python3 "$LAB/shadowsim/tools/harnessrecord.py" --log "$RES/$label.log" --arm "$arm" --seed "$seed" --n "$N" \
      --pay "$pay" --payload-bytes "$P" --label "$label" --extra "alias=$ALIAS" --extra "cond=$cond" \
      --extra "commit=$(head -n 1 "$BIN.commit" 2>/dev/null || echo unknown)" --out "$RES/$label.json" > /dev/null
    fi
    python3 - "$RES/$label.json" "$net" <<'EOF'
import json, sys
p, net = sys.argv[1], sys.argv[2]
r = json.load(open(p)); r["net"] = net
json.dump(r, open(p, "w"), indent=1, sort_keys=True)
EOF
    grep -o "Mbps L=.*" "$RES/$label.log" | cut -c1-400 ;;
esac
# Under scopecell.sh the cell has its own cgroup: its peak memory (every process of the cell, both
# substrates) goes into the record as cell_peak_kb.
#
# Only when this really is the cell's own scope. Without LANE, scopecell.sh makes no scope and the
# cell runs in whatever cgroup the lane's shell sits in, whose peak is every cell that shell has
# ever run -- recorded, before this guard, as if it were this cell's. A missing number is a gap; a
# lane's peak wearing a cell's name is the kind of figure a memory floor gets sized on. CELL_SCOPE
# is the unit scopecell.sh created, so the two agree only inside it.
cg=$(cut -d: -f3 /proc/self/cgroup)
peak=""
[ -n "${CELL_SCOPE:-}" ] && [ "${cg##*/}" = "${CELL_SCOPE:-}" ] && peak=$(cat "/sys/fs/cgroup$cg/memory.peak" 2>/dev/null)
[ -z "$peak" ] || [ ! -f "$RES/$label.json" ] || python3 - "$RES/$label.json" "$peak" <<'EOF'
import json, sys
p, peak = sys.argv[1], int(sys.argv[2]) // 1024
r = json.load(open(p)); r["cell_peak_kb"] = peak
json.dump(r, open(p, "w"), indent=1, sort_keys=True)
EOF
echo "== $(date -u) $label done in $(( $(date +%s) - t0 )) s${peak:+, peak $((peak / 1048576)) MB}"
