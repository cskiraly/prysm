#!/bin/bash
# One cell on either substrate, idempotent: cell.sh SUBSTRATE ARM SEED TRANSPORT N PAY [NET=home]
#
# The lane driver (eth-networking-lab shadowsim/tools/lane.sh) feeds it lines of a cells file. The
# label names the record; a cell whose record says ok is skipped, so re-running a lane resumes it.
# SUBSTRATE is shadow (runcell.sh: export, generate, run, extract) or harness (harnesscell.sh, the
# in-process driver, then harnessrecord.py). The harness carries QUIC over simnet and refuses tcp.
set -uo pipefail
STUDY=${STUDY:-$(cd "$(dirname "$0")" && pwd)}
LAB=${LAB:-$(cd "$STUDY/../../.." && pwd)/eth-networking-lab}
RES=${RES:-$STUDY/../results}
sub=${1:?substrate}; arm=${2:?arm}; seed=${3:?seed}; tr=${4:?transport}; N=${5:?n}; pay=${6:?pay}; net=${7:-home}
suffix=""; [ "$net" = home ] || suffix="_$net"
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
    NET=$net RES=$RES "$STUDY/runcell.sh" "$arm" "$seed" "$tr" "$N" "$pay" 2>&1 | grep -v "^wrote" | cut -c1-400 ;;
  harness)
    NET=$net RES=$RES "$STUDY/harnesscell.sh" "$arm" "$seed" "$N" "$pay"
    read -r P ALIAS < <(NET=$net arm=$arm seed=$seed N=$N pay=$pay bash -c "source '$STUDY/arms.sh'; echo \$P \$ALIAS")
    BIN=${BIN:-$STUDY/../bin/segshadow.test}
    python3 "$LAB/shadowsim/tools/harnessrecord.py" --log "$RES/$label.log" --arm "$arm" --seed "$seed" --n "$N" \
      --pay "$pay" --payload-bytes "$P" --label "$label" --extra "alias=$ALIAS" \
      --extra "commit=$(head -n 1 "$BIN.commit" 2>/dev/null || echo unknown)" --out "$RES/$label.json" > /dev/null
    python3 - "$RES/$label.json" "$net" <<'EOF'
import json, sys
p, net = sys.argv[1], sys.argv[2]
r = json.load(open(p)); r["net"] = net
json.dump(r, open(p, "w"), indent=1, sort_keys=True)
EOF
    grep -o "Mbps L=.*" "$RES/$label.log" | cut -c1-400 ;;
esac
echo "== $(date -u) $label done in $(( $(date +%s) - t0 )) s"
