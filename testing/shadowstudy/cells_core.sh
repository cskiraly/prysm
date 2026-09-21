#!/bin/bash
# The report's core cells: the follow-up's Figures 1, 8 and 9 under Shadow in both transports,
# with their same-tree twins. cells_core.sh SUBSTRATE SIZE SEED...
#   SUBSTRATE  shadow (each cell in QUIC and TCP) or harness (the twins; QUIC only)
#   SIZE       small: every point up to 500 nodes, figure by figure (1, then 9, then 8's 250-node
#              points); big: Figure 8's 1 000- and 2 000-node points, a lane of their own (memory;
#              4 000 nodes does not fit the machine running Shadow)
# Lines are cell.sh's arguments; a cell two figures share (whole message at 1 MiB) is printed once.
# Figure 9's tier 3 is aadaptcfd_16k below 512 KiB and aadaptcf_16k from there up, as published.
set -u
sub=${1:?shadow|harness}; size=${2:?small|big}; shift 2
[ $# -gt 0 ] || { echo "usage: cells_core.sh shadow|harness small|big SEED..." >&2; exit 2; }
case $sub in shadow) trs="quic tcp";; harness) trs=quic;; *) echo "bad substrate $sub" >&2; exit 2;; esac
PAYS="p128k p256k p384k p512k p640k p768k p896k p1m p1536k p2m"
emit() { for tr in $trs; do echo "$sub $1 $2 $tr $3 $4 $5"; done; }  # arm seed n pay net
for seed in "$@"; do
  case $size in
  small)
    echo "# Figure 1 (fu_datacenter): seven arms, three scenarios, 500 nodes, 1 MiB; seed $seed"
    for net in home dc20 dcb20; do for arm in wholend atuned_32k acoded_32k bfixed_32k bcodedfc1_32k c_32k crs_32k; do emit $arm $seed 500 p1m $net; done; done
    echo "# Figure 9 (fu_closing): the three tiers and their references against payload size, 500 nodes; seed $seed"
    for pay in $PAYS; do
      t3=aadaptcf_16k; case $pay in p128k|p256k|p384k) t3=aadaptcfd_16k;; esac
      for arm in wholend lad_batch_16k atuned_16k aadaptds_16k acodedcf_32k $t3; do emit $arm $seed 500 $pay home; done
    done
    echo "# Figure 8 (fu_nodes): four arms against network size at 1 MiB, the points up to 500 nodes; seed $seed"
    for arm in wholend lad_phase_32k atuned_32k acoded_32k; do emit $arm $seed 250 p1m home; done
    emit lad_phase_32k $seed 500 p1m home ;;
  big)
    echo "# Figure 8 (fu_nodes): the 1 000- and 2 000-node points; seed $seed"
    for n in 1000 2000; do for arm in wholend lad_phase_32k atuned_32k acoded_32k; do emit $arm $seed $n p1m home; done; done ;;
  *) echo "bad size $size" >&2; exit 2 ;;
  esac
done | awk '/^#/ || !seen[$0]++'
