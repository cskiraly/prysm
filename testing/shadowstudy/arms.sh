# The arm table shared by runcell.sh (Shadow) and harnesscell.sh (in-process): the fleet runner's
# definitions for the arms the cross-check reproduces, so a cell's SEGMENT_* environment is the
# published cell's on both substrates.
# Inputs: arm seed N pay, NET (home|dc20|dcb20|upN); outputs: P KA FC COMMON EXTRA EDGES VARIANT ALIAS
# (VARIANT: a = variant A on TestQ6RealisticMesh, b = TestVariantBDiffusion, c = TestVariantCDiffusion;
# ALIAS: the name the published fleet used for the arm, for pairing with its records).
#
# An arm is a policy at a unit. The name carries both (atuned_32k: A-tuned's rules on 32 KiB
# segments) and the definition sets every wire-affecting knob -- the unit, the parity, the id
# function -- so no tree default decides what a cell is; SEGMENT_STRICT in COMMON makes the
# harness refuse a cell that would need one. The published fleet named arms by policy alone,
# with the unit implied by the research tree's 32 KiB default, which cost a day when the same
# names ran on a tree that defaults to 16 KiB.
case ${pay%-fc} in p128k) P=131072;; p256k) P=262144;; p384k) P=393216;; p512k) P=524288;; p640k) P=655360;; p768k) P=786432;; p896k) P=917504;; p1m) P=1048576;; p1536k) P=1572864;; p2m) P=2097152;; *) echo "bad pay $pay"; exit 2;; esac
KA=$((P/32768)); FC=""; [[ $pay == *-fc ]] && FC=1
UP=${UP:-50}; DOWN=${DOWN:-100}
NET=${NET:-home}
# The network scenarios of the posts' Figure 1 (runfu.sh's NET): home = every node on 50/100;
# dc20 = 20% of nodes datacenter-hosted; dcb20 = that plus a 1 Gbps builder; upN = N Mbps up, 2N down.
SCEN=""
case $NET in
  home)  ;;
  dc20)  SCEN="SEGMENT_DATACENTER_PCT=20" ;;
  dcb20) SCEN="SEGMENT_DATACENTER_PCT=20 SEGMENT_BUILDER_UP_MBPS=1000" ;;
  up[0-9]*) UP=${NET#up}; DOWN=$((UP*2)) ;;
  *) echo "bad NET $NET (home|dc20|dcb20|upN)"; exit 2 ;;
esac
COMMON="SEGMENT_STRICT=1 SEGMENT_SLOW_TESTS=1 SEGMENT_MESH_SIZES=$N SEGMENT_DEGREE=${DEGREE:-70} SEGMENT_LATENCY_MODEL=${LATENCY_MODEL:-geo} SEGMENT_DET_RAND=1 SEGMENT_SEED=$seed SEGMENT_PAYLOAD_BYTES=$P SEGMENT_DEADLINE_MS=3000 SEGMENT_UP_MBPS=$UP SEGMENT_DOWN_MBPS=$DOWN $SCEN"
# BUILDER_UP gives node 0 a symmetric datacenter link (SEGMENT_BUILDER_UP_MBPS) on any scenario.
[ -z "${BUILDER_UP:-}" ] || COMMON="$COMMON SEGMENT_BUILDER_UP_MBPS=$BUILDER_UP"
FINAL="SEGMENT_IWANT_DISCIPLINE_MS=200 SEGMENT_OFFER_TABLE=1 SEGMENT_IHAVE_PARK=1 SEGMENT_IHAVE_PARK_K=1 SEGMENT_IHAVE_PARK_PROMISE_MS=400 SEGMENT_IHAVE_PARK_TTL_MS=30000"
# Size-adaptive push width shared by tiers 2 and 3: 4 up to 256 KiB, 3 up to 1 MiB, 2 above.
AR=2; [ $P -le 1048576 ] && AR=3; [ $P -le 262144 ] && AR=4
EDGES=${EDGES:-}
VARIANT=a
# Variant B's tuned rules, verbatim from runfu.sh (BFIXED).
BFIXED="SEGMENT_POLICIES=phase SEGMENT_REPLICATION=2 SEGMENT_CLAIM_PER_PEER=4 SEGMENT_STRIKE_CAP=1 SEGMENT_STRIKE_TTL_MS=30000 SEGMENT_ADAPTIVE_TIMEOUT=1 SEGMENT_PARTIAL_COMPRESS=1"
ALIAS=$arm
case $arm in
  # The 500-node cell's four arms (the follow-up's Figures 1, 8 and 9 share them), runfu.sh's
  # definitions with the unit made explicit: wholend, atuned, aadaptds, aadaptcf there.
  wholend)      EXTRA="SEGMENT_ARMS=whole" ;;
  atuned_32k)   ALIAS=atuned; EXTRA="SEGMENT_ARMS=phase SEGMENT_A_PHASE_R=2 SEGMENT_SIZE_BYTES=32768 $FINAL"; [ -n "$FC" ] && EXTRA="$EXTRA SEGMENT_SIZE_BYTES=$((P/32))" ;;
  atuned_16k)   ALIAS=am_k64; EXTRA="SEGMENT_ARMS=phase SEGMENT_A_PHASE_R=2 SEGMENT_SIZE_BYTES=16384 $FINAL" ;;
  aadaptds_16k) ALIAS=aadaptds; EXTRA="SEGMENT_ARMS=phase SEGMENT_A_PHASE_R=$AR SEGMENT_SIZE_BYTES=16384 SEGMENT_STRUCTURED_IDS=1 $FINAL" ;;
  # Tier 3 cuts the compressed payload into P/16 KiB pieces (the post's "16 KiB" for the code).
  aadaptcf_16k) ALIAS=aadaptcf; AD="$FINAL"; [ $P -lt 524288 ] && AD=""; EXTRA="SEGMENT_ARMS=phase SEGMENT_A_PHASE_R=$AR SEGMENT_COMPRESS_FIRST=1 SEGMENT_FIXED_COUNT=$((P/16384)) SEGMENT_PARITY=$((P/16384)) SEGMENT_STRUCTURED_IDS=1 SEGMENT_STOP_PULL=1 $AD" ;;
  # The rest of Figure 1's A-family bar (coded A), Figure 8's other lines and Figure 9's other lines.
  acoded_32k)   ALIAS=acoded; EXTRA="SEGMENT_ARMS=phase SEGMENT_A_PHASE_R=2 SEGMENT_SIZE_BYTES=32768 SEGMENT_PARITY=$KA SEGMENT_STRUCTURED_IDS=1 SEGMENT_STOP_PULL=1 $FINAL" ;;
  lad_phase_32k) ALIAS=lad_phase; EXTRA="SEGMENT_ARMS=phase SEGMENT_A_PHASE_R=2 SEGMENT_SIZE_BYTES=32768 SEGMENT_STRUCTURED_IDS=1"; [ -n "$FC" ] && EXTRA="$EXTRA SEGMENT_SIZE_BYTES=$((P/32))" ;;
  lad_batch_16k) ALIAS=lad_batch16; EXTRA="SEGMENT_ARMS=segmented SEGMENT_STRUCTURED_IDS=1 SEGMENT_SIZE_BYTES=16384" ;;
  acodedcf_32k) ALIAS=acodedcf; EXTRA="SEGMENT_ARMS=phase SEGMENT_A_PHASE_R=2 SEGMENT_COMPRESS_FIRST=1 SEGMENT_FIXED_COUNT=$KA SEGMENT_PARITY=$KA SEGMENT_STRUCTURED_IDS=1 SEGMENT_STOP_PULL=1 $FINAL" ;;
  aadaptcfd_16k) ALIAS=aadaptcfd; EXTRA="SEGMENT_ARMS=phase SEGMENT_A_PHASE_R=$AR SEGMENT_COMPRESS_FIRST=1 SEGMENT_FIXED_COUNT=$((P/16384)) SEGMENT_PARITY=$((P/16384)) SEGMENT_STRUCTURED_IDS=1 SEGMENT_STOP_PULL=1 $FINAL" ;;
  # The series' production path (the Shadow branch only, SEGMENT_PRODUCTION is the series'):
  # tier 2 plain, tier 3 coded, on the series' 16 KiB, with and without the 101-byte shim ids.
  prod2_16k)    ALIAS=prod2; EXTRA="SEGMENT_ARMS=phase SEGMENT_PRODUCTION=tier2 SEGMENT_SIZE_BYTES=16384" ;;
  prod2s_16k)   ALIAS=prod2s; EXTRA="SEGMENT_ARMS=phase SEGMENT_PRODUCTION=tier2 SEGMENT_SIZE_BYTES=16384 SEGMENT_STRUCTURED_IDS=1" ;;
  prod3_16k)    ALIAS=prod3; EXTRA="SEGMENT_ARMS=phase SEGMENT_PRODUCTION=tier3 SEGMENT_SIZE_BYTES=16384" ;;
  prod3s_16k)   ALIAS=prod3s; EXTRA="SEGMENT_ARMS=phase SEGMENT_PRODUCTION=tier3 SEGMENT_SIZE_BYTES=16384 SEGMENT_STRUCTURED_IDS=1" ;;
  # Figure 1's B and C bars (runfu.sh): B tuned and coded B in one-part-per-frame form; C at 32 KiB
  # segments and coded C, both over phase forwarding at r=2.
  bfixed_32k)   ALIAS=bfixed; VARIANT=b; EXTRA="$BFIXED SEGMENT_SIZE_BYTES=32768" ;;
  bcodedfc1_32k) ALIAS=bcodedfc1; VARIANT=b; EXTRA="$BFIXED SEGMENT_SIZE_BYTES=32768 SEGMENT_PARITY=$KA SEGMENT_PUSH_CHUNK=1" ;;
  c_32k)        ALIAS=c32; VARIANT=c; EXTRA="SEGMENT_SIZE_BYTES=32768 SEGMENT_C_PHASE_R=2 $FINAL" ;;
  crs_32k)      ALIAS=c32rs; VARIANT=c; EXTRA="SEGMENT_SIZE_BYTES=32768 SEGMENT_C_PHASE_R=2 SEGMENT_PARITY=$KA $FINAL" ;;
  # Known answer: whole message over one link (N=2): completion ~ wire bytes / 50 Mbps + latency.
  line)         EXTRA="SEGMENT_ARMS=whole"; EDGES=line ;;
  *) echo "unknown arm $arm"; exit 2 ;;
esac
