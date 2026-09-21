#!/bin/bash
# Follow-up post cells (2026-09-07). args: ARM SEED FAULT NET PAY
#  ARM   atuned | acoded | aplain400 | whole | btuned | bcoded | bcomp | bpark | bfixed | c64 | c32rs | ccust | ccust0 (R=0, fleet block)
#  FAULT clean | wh10 | wh20 | wh30 | wh50 | sil30 | whsp30 | omit4 | omit8 | omit9
#  NET   home | dc20 | dcb20 | up20 | up15 | up10
#  PAY   p1m | p128k | p256k | p512k | p2m ; suffix -fc = fixed-count rule (A K=32, C K=64)
# Reference configurations (frozen 2026-09-07, plan step 1):
#  atuned  A + phase r=2, 200 ms move-on, offer table, park k=1 promise 400 ms TTL 30 s  (Q73 moveban_ot)
#  acoded  atuned + RS K+K, structured ids, stop-pull                                    (Q73 codedphase_ot)
#  aplain400  A + phase r=2 with the plain 400 ms one-outstanding discipline; no memory   (control)
#  whole   whole-message gossip, discipline 200 + 200*(bytes/MiB) ms                      (Q50 convention)
#  btuned  B phase r=2, claim cap 4, strikes cap 2 TTL 2 s, adaptive RTO                  (Q74 final battery)
#  bcoded  btuned + RS K+K
#  c64     C, 16 KiB segments (64 topics at 1 MiB), phase r=2, final rules
#  c32rs   C, 32 KiB segments, RS K+K (64 topics at 1 MiB), phase r=2, final rules
#  ccust   c32rs with custody S = K + K/4 (R=8 at 1 MiB)
# Coded arms keep rate 1/2 at every size (parity = K); custody keeps R = K/4.
arm=$1; seed=$2; fault=${3:-clean}; net=${4:-home}; pay=${5:-p1m}
cd "$(dirname "$0")" || exit 1
BIN=${BIN:-./segfu.test}
case ${pay%-fc} in p128k) P=131072;; p256k) P=262144;; p384k) P=393216;; p512k) P=524288;; p640k) P=655360;; p768k) P=786432;; p896k) P=917504;; p1m) P=1048576;; p1536k) P=1572864;; p2m) P=2097152;; *) echo "bad pay $pay"; exit 2;; esac   # 384..896 KiB and 1536 KiB added 2026-09-10 for the payload sweep (Q84); K stays integer for 32 and 16 KiB units and K/4 for custody
KA=$((P/32768)); FC=""; [[ $pay == *-fc ]] && FC=1
N=${N:-500}
COMMON="SEGMENT_SLOW_TESTS=1 SEGMENT_MESH_SIZES=$N SEGMENT_DEGREE=70 SEGMENT_LATENCY_MODEL=geo SEGMENT_DET_RAND=1 SEGMENT_SEED=$seed GOMAXPROCS=2 SEGMENT_PAYLOAD_BYTES=$P SEGMENT_DEADLINE_MS=3000"
UP=50; DOWN=100
FINAL="SEGMENT_IWANT_DISCIPLINE_MS=200 SEGMENT_OFFER_TABLE=1 SEGMENT_IHAVE_PARK=1 SEGMENT_IHAVE_PARK_K=1 SEGMENT_IHAVE_PARK_PROMISE_MS=400 SEGMENT_IHAVE_PARK_TTL_MS=30000"
BTUNED="SEGMENT_POLICIES=phase SEGMENT_REPLICATION=2 SEGMENT_CLAIM_PER_PEER=4 SEGMENT_STRIKE_CAP=2 SEGMENT_STRIKE_TTL_MS=2000 SEGMENT_ADAPTIVE_TIMEOUT=1"
# B's reference from 2026-09-08 on (Q74 addendum): compressed parts, park at k=1 for 30 s on the honest strike count.
BFIXED="SEGMENT_POLICIES=phase SEGMENT_REPLICATION=2 SEGMENT_CLAIM_PER_PEER=4 SEGMENT_STRIKE_CAP=1 SEGMENT_STRIKE_TTL_MS=30000 SEGMENT_ADAPTIVE_TIMEOUT=1 SEGMENT_PARTIAL_COMPRESS=1"
case $arm in
  atuned)    TESTP="TestQ6RealisticMesh$";   EXTRA="SEGMENT_ARMS=phase SEGMENT_A_PHASE_R=2 $FINAL"; [ -n "$FC" ] && EXTRA="$EXTRA SEGMENT_SIZE_BYTES=$((P/32))" ;;
  acoded)    TESTP="TestQ6RealisticMesh$";   EXTRA="SEGMENT_ARMS=phase SEGMENT_A_PHASE_R=2 SEGMENT_PARITY=$KA SEGMENT_STRUCTURED_IDS=1 SEGMENT_STOP_PULL=1 $FINAL" ;;
  # Q91 (2026-09-11 night): predictive stop-pull, K + h cap on held + asks in flight (SEGMENT_STOP_PULL_H=h), on coded A.
  acodedpp[0-9]*) [[ $arm =~ ^acodedpp([0-9]+)$ ]]; TESTP="TestQ6RealisticMesh$"; EXTRA="SEGMENT_ARMS=phase SEGMENT_A_PHASE_R=2 SEGMENT_PARITY=$KA SEGMENT_STRUCTURED_IDS=1 SEGMENT_STOP_PULL=1 SEGMENT_STOP_PULL_H=${BASH_REMATCH[1]} $FINAL" ;;
  # compress-first (2026-09-09): snappy the payload, then segment into exactly K=P/32KiB pieces (and code); shards are ~23 KB and incompressible.
  acodedcf)  TESTP="TestQ6RealisticMesh$";   EXTRA="SEGMENT_ARMS=phase SEGMENT_A_PHASE_R=2 SEGMENT_COMPRESS_FIRST=1 SEGMENT_FIXED_COUNT=$KA SEGMENT_PARITY=$KA SEGMENT_STRUCTURED_IDS=1 SEGMENT_STOP_PULL=1 $FINAL" ;;
  # 2026-09-12 evening: compress-first coded A at the other units of the sweep (Part 1 Figure 4): shard counts as the 8/16/64 KiB coded arms, over the compressed payload.
  acodedcf128) TESTP="TestQ6RealisticMesh$"; EXTRA="SEGMENT_ARMS=phase SEGMENT_A_PHASE_R=2 SEGMENT_COMPRESS_FIRST=1 SEGMENT_FIXED_COUNT=$((P/8192)) SEGMENT_PARITY=$((P/8192)) SEGMENT_STRUCTURED_IDS=1 SEGMENT_STOP_PULL=1 $FINAL" ;;
  acodedcf64)  TESTP="TestQ6RealisticMesh$"; EXTRA="SEGMENT_ARMS=phase SEGMENT_A_PHASE_R=2 SEGMENT_COMPRESS_FIRST=1 SEGMENT_FIXED_COUNT=$((P/16384)) SEGMENT_PARITY=$((P/16384)) SEGMENT_STRUCTURED_IDS=1 SEGMENT_STOP_PULL=1 $FINAL" ;;
  acodedcf16)  TESTP="TestQ6RealisticMesh$"; EXTRA="SEGMENT_ARMS=phase SEGMENT_A_PHASE_R=2 SEGMENT_COMPRESS_FIRST=1 SEGMENT_FIXED_COUNT=$((P/65536)) SEGMENT_PARITY=$((P/65536)) SEGMENT_STRUCTURED_IDS=1 SEGMENT_STOP_PULL=1 $FINAL" ;;
  # 2026-09-12 night: the unit sweep at 128 KiB down to 2 KiB units (64 segments): A tuned, hedged, coded, compress-first coded.
  am_k4k)      TESTP="TestQ6RealisticMesh$"; EXTRA="SEGMENT_ARMS=phase SEGMENT_A_PHASE_R=2 SEGMENT_SIZE_BYTES=4096 $FINAL" ;;
  am_k2k)      TESTP="TestQ6RealisticMesh$"; EXTRA="SEGMENT_ARMS=phase SEGMENT_A_PHASE_R=2 SEGMENT_SIZE_BYTES=2048 $FINAL" ;;
  ath4)        TESTP="TestQ6RealisticMesh$"; EXTRA="SEGMENT_ARMS=phase SEGMENT_A_PHASE_R=2 SEGMENT_SIZE_BYTES=4096 SEGMENT_TAIL_HEDGE_K=3 SEGMENT_TAIL_H=4 $FINAL" ;;
  ath2)        TESTP="TestQ6RealisticMesh$"; EXTRA="SEGMENT_ARMS=phase SEGMENT_A_PHASE_R=2 SEGMENT_SIZE_BYTES=2048 SEGMENT_TAIL_HEDGE_K=3 SEGMENT_TAIL_H=4 $FINAL" ;;
  acoded256)   TESTP="TestQ6RealisticMesh$"; EXTRA="SEGMENT_ARMS=phase SEGMENT_A_PHASE_R=2 SEGMENT_SIZE_BYTES=4096 SEGMENT_PARITY=$((P/4096)) SEGMENT_STRUCTURED_IDS=1 SEGMENT_STOP_PULL=1 $FINAL" ;;
  acoded512)   TESTP="TestQ6RealisticMesh$"; EXTRA="SEGMENT_ARMS=phase SEGMENT_A_PHASE_R=2 SEGMENT_SIZE_BYTES=2048 SEGMENT_PARITY=$((P/2048)) SEGMENT_STRUCTURED_IDS=1 SEGMENT_STOP_PULL=1 $FINAL" ;;
  acodedcf256) TESTP="TestQ6RealisticMesh$"; EXTRA="SEGMENT_ARMS=phase SEGMENT_A_PHASE_R=2 SEGMENT_COMPRESS_FIRST=1 SEGMENT_FIXED_COUNT=$((P/4096)) SEGMENT_PARITY=$((P/4096)) SEGMENT_STRUCTURED_IDS=1 SEGMENT_STOP_PULL=1 $FINAL" ;;
  acodedcf512) TESTP="TestQ6RealisticMesh$"; EXTRA="SEGMENT_ARMS=phase SEGMENT_A_PHASE_R=2 SEGMENT_COMPRESS_FIRST=1 SEGMENT_FIXED_COUNT=$((P/2048)) SEGMENT_PARITY=$((P/2048)) SEGMENT_STRUCTURED_IDS=1 SEGMENT_STOP_PULL=1 $FINAL" ;;
  atunedcf)  TESTP="TestQ6RealisticMesh$";   EXTRA="SEGMENT_ARMS=phase SEGMENT_A_PHASE_R=2 SEGMENT_COMPRESS_FIRST=1 SEGMENT_FIXED_COUNT=$KA $FINAL" ;;
  aplain400) TESTP="TestQ6RealisticMesh$";   EXTRA="SEGMENT_ARMS=phase SEGMENT_A_PHASE_R=2 SEGMENT_IWANT_DISCIPLINE_MS=400" ;;
  whole)     TESTP="TestQ6RealisticMesh$";   EXTRA="SEGMENT_ARMS=whole SEGMENT_IWANT_DISCIPLINE_MS=$((200 + 200*P/1048576))" ;;
  wholend)   TESTP="TestQ6RealisticMesh$";   EXTRA="SEGMENT_ARMS=whole" ;;   # stock gossipsub: no IWANT discipline, every announcer asked (Q84 control)
  # Q84 (2026-09-10): whole message with A tuned's full request policy on the size-scaled window (promise deadline 2x the window, as A's 400/200).
  wholeot)   TESTP="TestQ6RealisticMesh$";   W=$((200 + 200*P/1048576)); EXTRA="SEGMENT_ARMS=whole SEGMENT_IWANT_DISCIPLINE_MS=$W SEGMENT_OFFER_TABLE=1 SEGMENT_IHAVE_PARK=1 SEGMENT_IHAVE_PARK_K=1 SEGMENT_IHAVE_PARK_PROMISE_MS=$((2*W)) SEGMENT_IHAVE_PARK_TTL_MS=30000" ;;
  # Q84: the bare minimum — segments on stock gossipsub: full-mesh push, sequential publish (no batch), no discipline, no phase; the wire still carries descriptor + Merkle proof and the structural ids.
  bare)      TESTP="TestQ6RealisticMesh$";   EXTRA="SEGMENT_ARMS=segmented SEGMENT_PUBLISH_MODE=fcfs SEGMENT_STRUCTURED_IDS=1"; [ -n "$FC" ] && EXTRA="$EXTRA SEGMENT_SIZE_BYTES=$((P/32))" ;;
  # Q84 ladder (2026-09-10): the first post's Table 2 rows as curves against size. bare = + segmentation; lad_batch = + batch publishing; lad_phase = + phase forwarding r=2 (no discipline); atuned = + disciplined pulls.
  lad_batch) TESTP="TestQ6RealisticMesh$";   EXTRA="SEGMENT_ARMS=segmented SEGMENT_STRUCTURED_IDS=1"; [ -n "$FC" ] && EXTRA="$EXTRA SEGMENT_SIZE_BYTES=$((P/32))" ;;
  # 2026-09-16 (author): the batch-publishing rung at 16 KiB segments, so the closing figure's tier 1 sits on the post's recommended segment size (Q104).
  lad_batch16) TESTP="TestQ6RealisticMesh$"; EXTRA="SEGMENT_ARMS=segmented SEGMENT_STRUCTURED_IDS=1 SEGMENT_SIZE_BYTES=16384" ;;
  lad_phase) TESTP="TestQ6RealisticMesh$";   EXTRA="SEGMENT_ARMS=phase SEGMENT_A_PHASE_R=2 SEGMENT_STRUCTURED_IDS=1"; [ -n "$FC" ] && EXTRA="$EXTRA SEGMENT_SIZE_BYTES=$((P/32))" ;;
  # Q89 (2026-09-11): the split without the phase: push r=2, announce the rest, no IDONTWANT-driven decay of r (WithPhaseFixedBudget).
  lad_split) TESTP="TestQ6RealisticMesh$";   EXTRA="SEGMENT_ARMS=phase SEGMENT_A_PHASE_R=2 SEGMENT_PHASE_FIXED_BUDGET=1 SEGMENT_STRUCTURED_IDS=1"; [ -n "$FC" ] && EXTRA="$EXTRA SEGMENT_SIZE_BYTES=$((P/32))" ;;
  btuned)    TESTP="TestVariantBDiffusion$"; EXTRA="$BTUNED" ;;
  bcoded)    TESTP="TestVariantBDiffusion$"; EXTRA="$BTUNED SEGMENT_PARITY=$KA" ;;
  # B with the 2026-09-08 fixes (one seed first): compression inside the partial message, and the
  # park at k=1 (strike cap 1, TTL 30 s) that A's tuned discipline uses; bcomp/bpark isolate each.
  btuned2)   TESTP="TestVariantBDiffusion$"; EXTRA="$BTUNED" ;;   # btuned on the fixed binary (one lapse = one strike), for the re-baseline
  bcomp)     TESTP="TestVariantBDiffusion$"; EXTRA="$BTUNED SEGMENT_PARTIAL_COMPRESS=1" ;;
  bpark)     TESTP="TestVariantBDiffusion$"; EXTRA="SEGMENT_POLICIES=phase SEGMENT_REPLICATION=2 SEGMENT_CLAIM_PER_PEER=4 SEGMENT_STRIKE_CAP=1 SEGMENT_STRIKE_TTL_MS=30000 SEGMENT_ADAPTIVE_TIMEOUT=1" ;;
  bfixed)    TESTP="TestVariantBDiffusion$"; EXTRA="$BFIXED" ;;
  bcodedf)   TESTP="TestVariantBDiffusion$"; EXTRA="$BFIXED SEGMENT_PARITY=$KA" ;;
  # 2026-09-16: coded B with a request surplus s (claims kept outstanding beyond K; the tail over-ask coded A gets from stop-pull).
  bcodedfs[0-9]*) [[ $arm =~ ^bcodedfs([0-9]+)$ ]]; TESTP="TestVariantBDiffusion$"; EXTRA="$BFIXED SEGMENT_PARITY=$KA SEGMENT_CLAIM_SURPLUS=${BASH_REMATCH[1]}" ;;
  # 2026-09-16: coded B with receivers' claims deferred d ms so the builder's pushes leave before its uplink is mobbed (first-hop screen); optional surplus s.
  # 2026-09-16: push depth r=1 (halves the per-peer bundle the extension sends in one RPC; first-hop screen), coded and plain.
  bcodedfr1)  TESTP="TestVariantBDiffusion$"; EXTRA="$BFIXED SEGMENT_PARITY=$KA SEGMENT_REPLICATION=1" ;;
  bfixedr1)   TESTP="TestVariantBDiffusion$"; EXTRA="$BFIXED SEGMENT_REPLICATION=1" ;;
  # 2026-09-16: pushes in RPCs of c segments, round-robin across peers (A's batch-publishing order on B), plain and coded; coded also with surplus s.
  bfixedc[0-9]*)  [[ $arm =~ ^bfixedc([0-9]+)$ ]]; TESTP="TestVariantBDiffusion$"; EXTRA="$BFIXED SEGMENT_PUSH_CHUNK=${BASH_REMATCH[1]}" ;;
  bcodedfc[0-9]*) [[ $arm =~ ^bcodedfc([0-9]+)$ ]]; TESTP="TestVariantBDiffusion$"; EXTRA="$BFIXED SEGMENT_PARITY=$KA SEGMENT_PUSH_CHUNK=${BASH_REMATCH[1]}" ;;
  bcodedfc[0-9]*s[0-9]*) [[ $arm =~ ^bcodedfc([0-9]+)s([0-9]+)$ ]]; TESTP="TestVariantBDiffusion$"; EXTRA="$BFIXED SEGMENT_PARITY=$KA SEGMENT_PUSH_CHUNK=${BASH_REMATCH[1]} SEGMENT_CLAIM_SURPLUS=${BASH_REMATCH[2]}" ;;
  bcodedfc[0-9]*d[0-9]*) [[ $arm =~ ^bcodedfc([0-9]+)d([0-9]+)$ ]]; TESTP="TestVariantBDiffusion$"; EXTRA="$BFIXED SEGMENT_PARITY=$KA SEGMENT_PUSH_CHUNK=${BASH_REMATCH[1]} SEGMENT_REQUEST_DEFER_MS=${BASH_REMATCH[2]}" ;;
  bcodedfd[0-9]*) [[ $arm =~ ^bcodedfd([0-9]+)$ ]]; TESTP="TestVariantBDiffusion$"; EXTRA="$BFIXED SEGMENT_PARITY=$KA SEGMENT_REQUEST_DEFER_MS=${BASH_REMATCH[1]}" ;;
  bcodedfs[0-9]*d[0-9]*) [[ $arm =~ ^bcodedfs([0-9]+)d([0-9]+)$ ]]; TESTP="TestVariantBDiffusion$"; EXTRA="$BFIXED SEGMENT_PARITY=$KA SEGMENT_CLAIM_SURPLUS=${BASH_REMATCH[1]} SEGMENT_REQUEST_DEFER_MS=${BASH_REMATCH[2]}" ;;
  # The A map at 500 nodes on the tuned rules (2026-09-09; the frontier map was at the old 400 ms discipline).
  am_r0)        TESTP="TestQ6RealisticMesh$"; EXTRA="SEGMENT_ARMS=phase SEGMENT_A_PHASE_R=0 $FINAL" ;;
  am_r1)        TESTP="TestQ6RealisticMesh$"; EXTRA="SEGMENT_ARMS=phase SEGMENT_A_PHASE_R=1 $FINAL" ;;
  am_r3)        TESTP="TestQ6RealisticMesh$"; EXTRA="SEGMENT_ARMS=phase SEGMENT_A_PHASE_R=3 $FINAL" ;;
  am_r4)        TESTP="TestQ6RealisticMesh$"; EXTRA="SEGMENT_ARMS=phase SEGMENT_A_PHASE_R=4 $FINAL" ;;
  am_nd)        TESTP="TestQ6RealisticMesh$"; EXTRA="SEGMENT_ARMS=phase SEGMENT_A_PHASE_R=2" ;;
  am_pull_nd)   TESTP="TestQ6RealisticMesh$"; EXTRA="SEGMENT_ARMS=phase SEGMENT_A_PHASE_R=0" ;;
  am_disc200)   TESTP="TestQ6RealisticMesh$"; EXTRA="SEGMENT_ARMS=phase SEGMENT_A_PHASE_R=2 SEGMENT_IWANT_DISCIPLINE_MS=200 SEGMENT_OFFER_TABLE=1" ;;
  am_coded_nosp) TESTP="TestQ6RealisticMesh$"; EXTRA="SEGMENT_ARMS=phase SEGMENT_A_PHASE_R=2 SEGMENT_PARITY=$KA SEGMENT_STRUCTURED_IDS=1 $FINAL" ;;
  am_codedpull) TESTP="TestQ6RealisticMesh$"; EXTRA="SEGMENT_ARMS=phase SEGMENT_A_PHASE_R=0 SEGMENT_PARITY=$KA SEGMENT_STRUCTURED_IDS=1 SEGMENT_STOP_PULL=1 $FINAL" ;;
  am_coded_nd)  TESTP="TestQ6RealisticMesh$"; EXTRA="SEGMENT_ARMS=phase SEGMENT_A_PHASE_R=2 SEGMENT_PARITY=$KA SEGMENT_STRUCTURED_IDS=1 SEGMENT_STOP_PULL=1" ;;
  am_k64)       TESTP="TestQ6RealisticMesh$"; EXTRA="SEGMENT_ARMS=phase SEGMENT_A_PHASE_R=2 SEGMENT_SIZE_BYTES=16384 $FINAL" ;;
  am_k8k)       TESTP="TestQ6RealisticMesh$"; EXTRA="SEGMENT_ARMS=phase SEGMENT_A_PHASE_R=2 SEGMENT_SIZE_BYTES=8192 $FINAL" ;;   # Q85: 8 KiB segments (size-adaptive rules)
  # Q86 (2026-09-11): tail hedging on A tuned — atail_k<K>h<H>: once within H messages of completion, ask up to K announcers per missing id.
  # E1 (2026-09-13): the tail hedge's window on a given unit. atail_u<U>_k<k>h<h> = A tuned at U-byte units with fan-out k and window h
  # (absolute pieces; the cell list carries the fraction of K each stands for); atuned_u<U> = the no-hedge control at that unit.
  atail_u[0-9]*_k[0-9]*h[0-9]*) [[ $arm =~ ^atail_u([0-9]+)_k([0-9]+)h([0-9]+)$ ]]; TESTP="TestQ6RealisticMesh$"; EXTRA="SEGMENT_ARMS=phase SEGMENT_A_PHASE_R=2 SEGMENT_SIZE_BYTES=${BASH_REMATCH[1]} $FINAL SEGMENT_TAIL_HEDGE_K=${BASH_REMATCH[2]} SEGMENT_TAIL_H=${BASH_REMATCH[3]}" ;;
  atuned_u[0-9]*) [[ $arm =~ ^atuned_u([0-9]+)$ ]]; TESTP="TestQ6RealisticMesh$"; EXTRA="SEGMENT_ARMS=phase SEGMENT_A_PHASE_R=2 SEGMENT_SIZE_BYTES=${BASH_REMATCH[1]} $FINAL" ;;
  # E2 (2026-09-13): the bounded tail hedge and the progressive fan-out. atailb[_u<U>]_k<k>h<h> = the hedge with bounds 4 extra asks per id, h(k-1) extra asks per group, 2 per serving
  # peer; atailp[_u<U>]_k<k>h<h> = the same bounds with the schedule k(m) = 1 + ceil(h/m) capped at k; acodedt_k<k>h<h> / acodedtb_k<k>h<h> = coded A with the tail hedge, unbounded / bounded (deficit-limited).
  atailb_k[0-9]*h[0-9]*|atailb_u[0-9]*_k[0-9]*h[0-9]*) [[ $arm =~ ^atailb_(u([0-9]+)_)?k([0-9]+)h([0-9]+)$ ]]; U=${BASH_REMATCH[2]:-32768}; K=${BASH_REMATCH[3]}; H=${BASH_REMATCH[4]}; TESTP="TestQ6RealisticMesh$"; EXTRA="SEGMENT_ARMS=phase SEGMENT_A_PHASE_R=2 SEGMENT_SIZE_BYTES=$U $FINAL SEGMENT_TAIL_HEDGE_K=$K SEGMENT_TAIL_H=$H SEGMENT_TAIL_BOUNDS=4,$((H*(K-1))),2" ;;
  atailp_k[0-9]*h[0-9]*|atailp_u[0-9]*_k[0-9]*h[0-9]*) [[ $arm =~ ^atailp_(u([0-9]+)_)?k([0-9]+)h([0-9]+)$ ]]; U=${BASH_REMATCH[2]:-32768}; K=${BASH_REMATCH[3]}; H=${BASH_REMATCH[4]}; TESTP="TestQ6RealisticMesh$"; EXTRA="SEGMENT_ARMS=phase SEGMENT_A_PHASE_R=2 SEGMENT_SIZE_BYTES=$U $FINAL SEGMENT_TAIL_HEDGE_K=$K SEGMENT_TAIL_H=$H SEGMENT_TAIL_BOUNDS=4,$((H*(K-1))),2 SEGMENT_TAIL_SCHEDULE=1" ;;
  acodedt_k[0-9]*h[0-9]*)  [[ $arm =~ ^acodedt_k([0-9]+)h([0-9]+)$ ]]; TESTP="TestQ6RealisticMesh$"; EXTRA="SEGMENT_ARMS=phase SEGMENT_A_PHASE_R=2 SEGMENT_PARITY=$KA SEGMENT_STRUCTURED_IDS=1 SEGMENT_STOP_PULL=1 $FINAL SEGMENT_TAIL_HEDGE_K=${BASH_REMATCH[1]} SEGMENT_TAIL_H=${BASH_REMATCH[2]}" ;;
  acodedtb_k[0-9]*h[0-9]*) [[ $arm =~ ^acodedtb_k([0-9]+)h([0-9]+)$ ]]; K=${BASH_REMATCH[1]}; H=${BASH_REMATCH[2]}; TESTP="TestQ6RealisticMesh$"; EXTRA="SEGMENT_ARMS=phase SEGMENT_A_PHASE_R=2 SEGMENT_PARITY=$KA SEGMENT_STRUCTURED_IDS=1 SEGMENT_STOP_PULL=1 $FINAL SEGMENT_TAIL_HEDGE_K=$K SEGMENT_TAIL_H=$H SEGMENT_TAIL_BOUNDS=4,$((H*(K-1))),2" ;;
  atail_k[0-9]*h[0-9]*) [[ $arm =~ ^atail_k([0-9]+)h([0-9]+)$ ]]; TESTP="TestQ6RealisticMesh$"; EXTRA="SEGMENT_ARMS=phase SEGMENT_A_PHASE_R=2 $FINAL SEGMENT_TAIL_HEDGE_K=${BASH_REMATCH[1]} SEGMENT_TAIL_H=${BASH_REMATCH[2]}" ;;
  # Q85 (2026-09-11): A with size-adaptive rules, as a rule not a pick: 16 KiB segments everywhere; push r = 4 up to 256 KiB, 3 up to 1 MiB, 2 above;
  # the request discipline (move-on, offer table, park) only from 512 KiB up — below, stock behaviour (every announcer asked) is faster and capture-proof.
  aadapt)    TESTP="TestQ6RealisticMesh$"; AR=2; [ $P -le 1048576 ] && AR=3; [ $P -le 262144 ] && AR=4; AD="$FINAL"; [ $P -lt 524288 ] && AD=""; EXTRA="SEGMENT_ARMS=phase SEGMENT_A_PHASE_R=$AR SEGMENT_SIZE_BYTES=16384 $AD" ;;
  # 2026-09-16 (author): the one-regime rule — aadapt's push schedule (r = 4 to 256 KiB, 3 to 1 MiB, 2 above) with the
  # request discipline (move-on, offer table, park) at EVERY size; 16 KiB segments. Prices the discipline below 512 KiB.
  aadaptd)   TESTP="TestQ6RealisticMesh$"; AR=2; [ $P -le 1048576 ] && AR=3; [ $P -le 262144 ] && AR=4; EXTRA="SEGMENT_ARMS=phase SEGMENT_A_PHASE_R=$AR SEGMENT_SIZE_BYTES=16384 $FINAL" ;;
  # 2026-09-16 (author): aadaptd with the harness's structured ids (101-byte hybrid ids, as the ladder rungs and the coded arms run),
  # to test whether id width explains the control-byte gap between the tiers (Q106). Same policy, same sizes, same seeds.
  aadaptds)  TESTP="TestQ6RealisticMesh$"; AR=2; [ $P -le 1048576 ] && AR=3; [ $P -le 262144 ] && AR=4; EXTRA="SEGMENT_ARMS=phase SEGMENT_A_PHASE_R=$AR SEGMENT_SIZE_BYTES=16384 SEGMENT_STRUCTURED_IDS=1 $FINAL" ;;
  # Q85/Q86 (2026-09-11): the refined size-adaptive rule — 16 KiB units everywhere; r = 4 up to 256 KiB, 3 at 384 KiB, 2 from 512 KiB; the discipline and tail hedging (k=3, h=4) from 512 KiB up.
  # E4/E5 (2026-09-13): the oracle regime rule — every node picks r and whether it runs the discipline, offer table, park and tail hedge from R = wire time on its own uplink / RTT
  # (thresholds reproduce aadapt2 on the 50 Mbps home uplink); 16 KiB units everywhere like aadapt2.
  aregime)   TESTP="TestQ6RealisticMesh$"; EXTRA="SEGMENT_ARMS=phase SEGMENT_A_PHASE_R=2 SEGMENT_SIZE_BYTES=16384 $FINAL SEGMENT_TAIL_HEDGE_K=3 SEGMENT_TAIL_H=4 SEGMENT_A_REGIME_RULE=1" ;;
  # rules plus code (2026-09-16, closing figure's open question): aadapt's push schedule and discipline threshold with the
  # compress-first code over 16 KiB segments of the compressed bytes (K = P/16 KiB, parity = K, stop-pull), as acodedcf64.
  aadaptcf)  TESTP="TestQ6RealisticMesh$"; AR=2; [ $P -le 1048576 ] && AR=3; [ $P -le 262144 ] && AR=4; AD="$FINAL"; [ $P -lt 524288 ] && AD=""; EXTRA="SEGMENT_ARMS=phase SEGMENT_A_PHASE_R=$AR SEGMENT_COMPRESS_FIRST=1 SEGMENT_FIXED_COUNT=$((P/16384)) SEGMENT_PARITY=$((P/16384)) SEGMENT_STRUCTURED_IDS=1 SEGMENT_STOP_PULL=1 $AD" ;;
  # 2026-09-16 (author): tier 3 with the discipline at EVERY size — aadaptcf's push schedule and compress-first code with $FINAL everywhere (Q105).
  # Identical to aadaptcf from 512 KiB up, so only 128 / 256 / 384 KiB need cells.
  aadaptcfd) TESTP="TestQ6RealisticMesh$"; AR=2; [ $P -le 1048576 ] && AR=3; [ $P -le 262144 ] && AR=4; EXTRA="SEGMENT_ARMS=phase SEGMENT_A_PHASE_R=$AR SEGMENT_COMPRESS_FIRST=1 SEGMENT_FIXED_COUNT=$((P/16384)) SEGMENT_PARITY=$((P/16384)) SEGMENT_STRUCTURED_IDS=1 SEGMENT_STOP_PULL=1 $FINAL" ;;
  aadapt2)   TESTP="TestQ6RealisticMesh$"; AR=2; [ $P -le 393216 ] && AR=3; [ $P -le 262144 ] && AR=4; AD="$FINAL SEGMENT_TAIL_HEDGE_K=3 SEGMENT_TAIL_H=4"; [ $P -lt 524288 ] && AD=""; EXTRA="SEGMENT_ARMS=phase SEGMENT_A_PHASE_R=$AR SEGMENT_SIZE_BYTES=16384 $AD" ;;
  # E1 composed arm (2026-09-13): aadapt2 unchanged from 512 KiB up; below it the discipline and a
  # fractional tail hedge h = max(1, K/8) at k = 3 (E1: the gain is flat from h/K = 1/8 up and the bytes climb past it) — acomp keeps
  # aadapt2's push degrees (4 to 256 KiB, 3 at 384), acompd drops one push where the hedge is added, as Figure 6's dashed line does above 512 KiB.
  acomp|acompd) TESTP="TestQ6RealisticMesh$"; AR=2; [ $P -le 393216 ] && AR=3; [ $P -le 262144 ] && AR=4; H=4; if [ $P -lt 524288 ]; then H=$((P/16384/8)); [ $H -lt 1 ] && H=1; [ $arm = acompd ] && AR=$((AR-1)); fi; EXTRA="SEGMENT_ARMS=phase SEGMENT_A_PHASE_R=$AR SEGMENT_SIZE_BYTES=16384 $FINAL SEGMENT_TAIL_HEDGE_K=3 SEGMENT_TAIL_H=$H" ;;
  am_sp)        TESTP="TestQ6RealisticMesh$"; EXTRA="SEGMENT_ARMS=phase SEGMENT_A_PHASE_R=2 SEGMENT_STRUCTURED_IDS=1 SEGMENT_STOP_PULL=1 $FINAL" ;;
  am_seg)       TESTP="TestQ6RealisticMesh$"; EXTRA="SEGMENT_ARMS=segmented" ;;
  # The 64-segment exploration (2026-09-09, one seed first): every variant at 16 KiB units, K=64 at 1 MiB.
  aplain16)  TESTP="TestQ6RealisticMesh$";   EXTRA="SEGMENT_ARMS=phase SEGMENT_A_PHASE_R=2 SEGMENT_SIZE_BYTES=16384 SEGMENT_IWANT_DISCIPLINE_MS=400" ;;
  acoded64)  TESTP="TestQ6RealisticMesh$";   EXTRA="SEGMENT_ARMS=phase SEGMENT_A_PHASE_R=2 SEGMENT_SIZE_BYTES=16384 SEGMENT_PARITY=$((P/16384)) SEGMENT_STRUCTURED_IDS=1 SEGMENT_STOP_PULL=1 $FINAL" ;;
  bfixed16)  TESTP="TestVariantBDiffusion$"; EXTRA="$BFIXED SEGMENT_SIZE_BYTES=16384" ;;
  # Q87 (2026-09-11): the fourth mapping -- variant B's partial messages on G topics, one group per topic (bgroups<G>; B tuned otherwise).
  bgroups[0-9]*) [[ $arm =~ ^bgroups([0-9]+)$ ]]; TESTP="TestVariantBDiffusion$"; EXTRA="$BFIXED SEGMENT_B_GROUPS=${BASH_REMATCH[1]}" ;;
  c64rs)     TESTP="TestVariantCDiffusion$"; EXTRA="SEGMENT_SIZE_BYTES=16384 SEGMENT_C_PHASE_R=2 SEGMENT_PARITY=$((P/16384)) $FINAL" ;;
  ccust64)   TESTP="TestVariantCDiffusion$"; EXTRA="SEGMENT_SIZE_BYTES=16384 SEGMENT_C_PHASE_R=2 SEGMENT_PARITY=$((P/16384)) SEGMENT_SUB_EXTRA=$((P/16384/4)) $FINAL" ;;
  # Table A's two other request configurations (Q73 names aphase_ot, hedge200_ot): the 400 ms
  # one-outstanding discipline with an offer table, and a hedged second ask at 200 ms.
  aphaseot)  TESTP="TestQ6RealisticMesh$";   EXTRA="SEGMENT_ARMS=phase SEGMENT_A_PHASE_R=2 SEGMENT_OFFER_TABLE=1 SEGMENT_IWANT_DISCIPLINE_MS=400" ;;
  ahedge)    TESTP="TestQ6RealisticMesh$";   EXTRA="SEGMENT_ARMS=phase SEGMENT_A_PHASE_R=2 SEGMENT_OFFER_TABLE=1 SEGMENT_IWANT_DISCIPLINE_MS=400 SEGMENT_IWANT_HEDGE_MS=200" ;;
  c64)       TESTP="TestVariantCDiffusion$"; SZ=16384; [ -n "$FC" ] && SZ=$((P/64)); EXTRA="SEGMENT_SIZE_BYTES=$SZ SEGMENT_C_PHASE_R=2 $FINAL" ;;
  # Q90 (2026-09-11 evening): the unit-size sweep at the base, 8/16/32/64 KiB for A tuned, coded A, B tuned and C + RS.
  am_k16)    TESTP="TestQ6RealisticMesh$"; EXTRA="SEGMENT_ARMS=phase SEGMENT_A_PHASE_R=2 SEGMENT_SIZE_BYTES=65536 $FINAL" ;;   # A tuned, 64 KiB units (K = 16 at 1 MiB)
  # Q90 fifth line: A tuned + tail hedge (k = 3, h = 4) across unit sizes; 32 KiB is atail_k3h4.
  ath8)      TESTP="TestQ6RealisticMesh$"; EXTRA="SEGMENT_ARMS=phase SEGMENT_A_PHASE_R=2 SEGMENT_SIZE_BYTES=8192 SEGMENT_TAIL_HEDGE_K=3 SEGMENT_TAIL_H=4 $FINAL" ;;
  ath16)     TESTP="TestQ6RealisticMesh$"; EXTRA="SEGMENT_ARMS=phase SEGMENT_A_PHASE_R=2 SEGMENT_SIZE_BYTES=16384 SEGMENT_TAIL_HEDGE_K=3 SEGMENT_TAIL_H=4 $FINAL" ;;
  ath64)     TESTP="TestQ6RealisticMesh$"; EXTRA="SEGMENT_ARMS=phase SEGMENT_A_PHASE_R=2 SEGMENT_SIZE_BYTES=65536 SEGMENT_TAIL_HEDGE_K=3 SEGMENT_TAIL_H=4 $FINAL" ;;
  acoded128) TESTP="TestQ6RealisticMesh$"; EXTRA="SEGMENT_ARMS=phase SEGMENT_A_PHASE_R=2 SEGMENT_SIZE_BYTES=8192 SEGMENT_PARITY=$((P/8192)) SEGMENT_STRUCTURED_IDS=1 SEGMENT_STOP_PULL=1 $FINAL" ;;   # coded A, 8 KiB (128+128)
  acoded16)  TESTP="TestQ6RealisticMesh$"; EXTRA="SEGMENT_ARMS=phase SEGMENT_A_PHASE_R=2 SEGMENT_SIZE_BYTES=65536 SEGMENT_PARITY=$((P/65536)) SEGMENT_STRUCTURED_IDS=1 SEGMENT_STOP_PULL=1 $FINAL" ;;   # coded A, 64 KiB (16+16)
  bfixed8)   TESTP="TestVariantBDiffusion$"; EXTRA="$BFIXED SEGMENT_SIZE_BYTES=8192" ;;
  bfixed64)  TESTP="TestVariantBDiffusion$"; EXTRA="$BFIXED SEGMENT_SIZE_BYTES=65536" ;;
  c128rs)    TESTP="TestVariantCDiffusion$"; EXTRA="SEGMENT_SIZE_BYTES=8192 SEGMENT_C_PHASE_R=2 SEGMENT_PARITY=$((P/8192)) $FINAL" ;;   # C + RS 128+128 on 256 topics, 8 KiB
  c16rs)     TESTP="TestVariantCDiffusion$"; EXTRA="SEGMENT_SIZE_BYTES=65536 SEGMENT_C_PHASE_R=2 SEGMENT_PARITY=$((P/65536)) $FINAL" ;;   # C + RS 16+16 on 32 topics, 64 KiB
  # 2026-09-11 evening: the uncoded C at 32 KiB units (32 topics at 1 MiB) so the six configurations of the main story share one unit.
  c32)       TESTP="TestVariantCDiffusion$"; EXTRA="SEGMENT_SIZE_BYTES=32768 SEGMENT_C_PHASE_R=2 $FINAL" ;;
  c32rs)     TESTP="TestVariantCDiffusion$"; EXTRA="SEGMENT_C_PHASE_R=2 SEGMENT_PARITY=$KA $FINAL" ;;
  ccust)     TESTP="TestVariantCDiffusion$"; EXTRA="SEGMENT_C_PHASE_R=2 SEGMENT_PARITY=$KA SEGMENT_SUB_EXTRA=$((KA/4)) $FINAL" ;;
  ccust0)    TESTP="TestVariantCDiffusion$"; EXTRA="SEGMENT_C_PHASE_R=2 SEGMENT_PARITY=$KA SEGMENT_SUB_EXTRA=0 $FINAL" ;;
  *) echo "unknown arm $arm"; exit 2 ;;
esac
case $fault in
  clean) ;;
  wh[0-9]*)   COMMON="$COMMON SEGMENT_FAIL_WITHHOLD_PCT=${fault#wh}" ;;
  sil[0-9]*)  COMMON="$COMMON SEGMENT_FAIL_WITHHOLD_PCT=${fault#sil} SEGMENT_FAIL_RELAY_SILENCE=1" ;;
  whsp[0-9]*) f=${fault#whsp}; COMMON="$COMMON SEGMENT_FAIL_WITHHOLD_PCT=$f SEGMENT_FAIL_IDW_SPOOF_PCT=$f" ;;
  # E7 screens (2026-09-13): slow<P>d<D>[f<n>|s<n>] = P% announcers serve after D ms (f<n>: the first n asks fast then slow; s<n>: the reverse);
  # cap<P> = P% withholders that also re-announce every id they hear (offer capture); last<P>w<W>h<H> = the last W ids leave from H placed holders, P% withholders withhold
  # only those ids and hearsay-announce them (needs structured ids, 32 KiB units: index floor KA-W); narrow0w<W>h<H> = the placement alone; rd<P> = P% slow readers (100 ms per read).
  slow[0-9]*d[0-9]*) [[ $fault =~ ^slow([0-9]+)d([0-9]+)(f([0-9]+)|s([0-9]+))?$ ]] || { echo "bad fault $fault"; exit 2; }
                     COMMON="$COMMON SEGMENT_FAIL_WITHHOLD_PCT=${BASH_REMATCH[1]} SEGMENT_FAIL_WITHHOLD_MODE=slow SEGMENT_FAIL_WITHHOLD_SLOW_MS=${BASH_REMATCH[2]}"
                     [ -n "${BASH_REMATCH[4]}" ] && COMMON="$COMMON SEGMENT_FAIL_WITHHOLD_PREFIX=${BASH_REMATCH[4]} SEGMENT_FAIL_WITHHOLD_PREFIX_MODE=fast"
                     [ -n "${BASH_REMATCH[5]}" ] && COMMON="$COMMON SEGMENT_FAIL_WITHHOLD_PREFIX=${BASH_REMATCH[5]} SEGMENT_FAIL_WITHHOLD_PREFIX_MODE=slow" ;;
  cap[0-9]*)  COMMON="$COMMON SEGMENT_FAIL_WITHHOLD_PCT=${fault#cap} SEGMENT_FAIL_HEARSAY=1" ;;
  # capw<P>w<W>: the conformant liar — P% withholders that re-announce every id they hear, but only to W peers (mesh first), an honest relay's fan-out rather than a flood.
  capw[0-9]*w[0-9]*) [[ $fault =~ ^capw([0-9]+)w([0-9]+)$ ]] || { echo "bad fault $fault"; exit 2; }
                     COMMON="$COMMON SEGMENT_FAIL_WITHHOLD_PCT=${BASH_REMATCH[1]} SEGMENT_FAIL_HEARSAY=${BASH_REMATCH[2]}" ;;
  last[0-9]*w[0-9]*h[0-9]*) [[ $fault =~ ^last([0-9]+)w([0-9]+)h([0-9]+)$ ]] || { echo "bad fault $fault"; exit 2; }
                     COMMON="$COMMON SEGMENT_STRUCTURED_IDS=1 SEGMENT_FAIL_PUBLISH_NARROW=${BASH_REMATCH[2]}:${BASH_REMATCH[3]} SEGMENT_FAIL_WITHHOLD_PCT=${BASH_REMATCH[1]} SEGMENT_FAIL_WITHHOLD_INDEX_FROM=$((KA-BASH_REMATCH[2])) SEGMENT_FAIL_HEARSAY=1" ;;
  narrow0w[0-9]*h[0-9]*) [[ $fault =~ ^narrow0w([0-9]+)h([0-9]+)$ ]] || { echo "bad fault $fault"; exit 2; }
                     COMMON="$COMMON SEGMENT_STRUCTURED_IDS=1 SEGMENT_FAIL_PUBLISH_NARROW=${BASH_REMATCH[1]}:${BASH_REMATCH[2]}" ;;
  rd[0-9]*)   COMMON="$COMMON SEGMENT_FAIL_SLOW_READER_PCT=${fault#rd} SEGMENT_FAIL_SLOW_READER_MS=100" ;;
  omit1|omit4|omit8|omit9) COMMON="$COMMON SEGMENT_FAIL_PUBLISH_WITHHOLD=${fault#omit}" ;;
  *) echo "unknown fault $fault"; exit 2 ;;
esac
# net may carry a processing-cost suffix: <net>-p<US> sets SEGMENT_PROC_US (virtual µs per received
# data message, 4 validators per node), e.g. home-p100, up200-p300. Zero/absent = free processing.
PROC=""
if [[ $net =~ ^(.+)-p([0-9]+)$ ]]; then PROC="SEGMENT_PROC_US=${BASH_REMATCH[2]}"; net=${BASH_REMATCH[1]}; fi
case $net in
  home) ;;
  dc20)  COMMON="$COMMON SEGMENT_DATACENTER_PCT=20" ;;
  dcb20) COMMON="$COMMON SEGMENT_DATACENTER_PCT=20 SEGMENT_BUILDER_UP_MBPS=1000" ;;
  up[0-9]*) UP=${net#up}; DOWN=$((UP*2)) ;;   # upN: N Mbps uplink, 2N downlink (home is 50/100)
  sl[0-9]*x[0-9]*) [[ $net =~ ^sl([0-9]+)x([0-9]+)(d([0-9]+))?$ ]] || { echo "bad net $net"; exit 2; }   # slPxU[dD]: P% of nodes at U Mbps up, D down (default 2U), rest home
                   COMMON="$COMMON SEGMENT_SLOW_PCT=${BASH_REMATCH[1]} SEGMENT_SLOW_UP_MBPS=${BASH_REMATCH[2]}"
                   [ -n "${BASH_REMATCH[4]}" ] && COMMON="$COMMON SEGMENT_SLOW_DOWN_MBPS=${BASH_REMATCH[4]}" ;;
  *) echo "unknown net $net"; exit 2 ;;
esac
COMMON="$COMMON SEGMENT_UP_MBPS=$UP SEGMENT_DOWN_MBPS=$DOWN $PROC"
[ -n "$DRY" ] && { echo "$arm $seed $fault $net $pay :: $TESTP :: $COMMON $EXTRA"; exit 0; }
env $COMMON $EXTRA nice -n 10 $BIN -test.run "$TESTP" -test.v -test.count=1 -test.timeout 7200s
