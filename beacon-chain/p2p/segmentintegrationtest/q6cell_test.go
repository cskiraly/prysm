package segmentintegrationtest

// The Q6 cell as the environment describes it, and its variant at one operating point. Factored
// out of TestQ6RealisticMesh so the Shadow node (shadow_node_test.go), the star and known-answer
// cells build the same wire forms from the same knobs as the in-process driver.

import (
	"fmt"
	"hash/fnv"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/OffchainLabs/prysm/v7/consensus-types/primitives"
	"github.com/OffchainLabs/prysm/v7/crypto/bls"
	"github.com/golang/snappy"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	simlibp2p "github.com/libp2p/go-libp2p/x/simlibp2p"
)

// q6Cell is what the environment says about a Q6 cell: the arms, variant A's knobs and the wire
// forms of the payload.
type q6Cell struct {
	// Fields are ordered widest-first: nogo's maligned check rejects the natural grouping.
	arms            []string
	phaseR          int
	phaseRSource    int
	phaseRQuota     int
	srcOfferPaceMS  int
	linkDisclose    int
	srcDiluteMS     int
	phaseRFast      int
	phaseRFastMbps  int
	stopPullH       int
	haveProject     int
	havePredictMS   int
	havePredictPeer float64
	havePairOrder   int
	tailK, tailH    int
	tailBounds      [3]int
	linkMod         uint64
	payloadLen      int
	unit            int // the segment size the segmented arms were cut at
	segs            wireArm
	whole           wireArm
	warmSegs        wireArm
	warmWhole       wireArm
	stopPull        bool
	haveComplete    bool
	tailBounded     bool
	tailSchedule    bool
	groupPush       bool
	groupProject    bool
	linkEnforce     bool
	phaseSrcConns   bool
	phaseSelFewest  bool
	phaseSelBacklog int64
	phaseSelGuard   time.Duration
	phaseSelDraw    pubsub.PhaseSelectTolerance
	hose            *hoseKnobs
	rebuilt         *rebuiltKnobs
	pair            *pairState
	buildJoint      func(t *testing.T, m int) (wireArm, int)
	build           func(t *testing.T, m int) (whole, segs wireArm, unit int)
}

// q6CellFromEnv reads the cell's arms, knobs and payload from the environment and builds the wire
// forms. params.SetupTestConfigCleanup must have run.
func q6CellFromEnv(t *testing.T) q6Cell {
	t.Helper()
	var c q6Cell
	// Which arms to run. "phase" is variant A over the forked gossipsub's phase forwarding
	// (WithPhaseForwarding): push max(0, r - IDONTWANT-holders) mesh peers, immediate IHAVE
	// to the rest. r comes from SEGMENT_A_PHASE_R (default 2).
	c.arms = []string{"whole", "segmented"}
	if v := os.Getenv("SEGMENT_ARMS"); v != "" {
		c.arms = nil
		for _, f := range strings.Split(v, ",") {
			f = strings.TrimSpace(f)
			switch f {
			case "whole", "segmented", "phase":
				c.arms = append(c.arms, f)
			default:
				t.Fatalf("bad SEGMENT_ARMS entry %q", f)
			}
		}
	}
	c.phaseR = 2
	if v := os.Getenv("SEGMENT_A_PHASE_R"); v != "" {
		k, err := strconv.Atoi(v)
		if err != nil || k < 0 {
			t.Fatalf("bad SEGMENT_A_PHASE_R %q", v)
		}
		c.phaseR = k // 0 = gossipsub-native pull-only: announce everything, push nothing.
	}
	// SEGMENT_STOP_PULL is the receiver-side decline-on-complete half of the coded-group
	// byte-suppression design: once a node holds enough distinct segments
	// to complete, its request gate vetoes IWANTs for every further segment id. Pull side only;
	// mesh pushes are untouched. Structured ids are required because the veto has to recognize
	// a segment announcement it will never fetch.
	// SEGMENT_HAVE_COMPLETE=1: a node that completed announces a HAVE of every index of the
	// group, the frames it never received included. Needs the HAVE bitmap.
	c.haveComplete = os.Getenv("SEGMENT_HAVE_COMPLETE") != ""
	if c.haveComplete && (os.Getenv("SEGMENT_HAVE_BITMAP") == "" || !structuredIDsEnabled()) {
		t.Fatal("SEGMENT_HAVE_COMPLETE needs SEGMENT_HAVE_BITMAP and structured ids")
	}
	c.rebuilt = rebuiltFromEnv(t, c.haveComplete)
	c.stopPull = os.Getenv("SEGMENT_STOP_PULL") != ""
	if c.stopPull && !structuredIDsEnabled() {
		t.Fatal("SEGMENT_STOP_PULL requires SEGMENT_STRUCTURED_IDS")
	}
	// SEGMENT_STOP_PULL_H=h makes the gate predictive (Q91): a node declines an ask while its
	// held shards plus its asks in flight would exceed K + h, and a declined announcement is
	// replayed through the fork's deferral ledger on the next delivery, so a stalled ask costs a
	// move-on rather than a strand. h = 0 (unset) is the reactive gate: decline only after K.
	if v := os.Getenv("SEGMENT_STOP_PULL_H"); v != "" {
		h, err := strconv.Atoi(v)
		if err != nil || h < 1 || !c.stopPull {
			t.Fatalf("bad SEGMENT_STOP_PULL_H %q (want >= 1, with SEGMENT_STOP_PULL)", v)
		}
		c.stopPullH = h
	}
	// SEGMENT_TAIL_HEDGE_K=k with SEGMENT_TAIL_H=h: once a node is within h messages of
	// completing, the IWANT discipline asks up to k announcers per missing id at once (the
	// fork's WithIWantTailHedge). The watch goroutine, which already counts each node's
	// progress, is what calls EnterTailHedge. Needs the discipline and the offer table.
	if v := os.Getenv("SEGMENT_TAIL_HEDGE_K"); v != "" {
		k, err := strconv.Atoi(v)
		if err != nil || k < 2 {
			t.Fatalf("bad SEGMENT_TAIL_HEDGE_K %q (want >= 2)", v)
		}
		h, err := strconv.Atoi(os.Getenv("SEGMENT_TAIL_H"))
		if err != nil || h < 1 {
			t.Fatalf("bad SEGMENT_TAIL_H %q (want >= 1)", os.Getenv("SEGMENT_TAIL_H"))
		}
		if os.Getenv("SEGMENT_IWANT_DISCIPLINE_MS") == "" || os.Getenv("SEGMENT_OFFER_TABLE") == "" {
			t.Fatal("SEGMENT_TAIL_HEDGE_K requires SEGMENT_IWANT_DISCIPLINE_MS and SEGMENT_OFFER_TABLE")
		}
		c.tailK, c.tailH = k, h
	}
	// SEGMENT_TAIL_BOUNDS=perID,perGroup,perPeer bounds the tail hedge (0 = off) and
	// SEGMENT_TAIL_SCHEDULE=1 makes its fan-out follow the pieces still missing, k(m) = 1 + ⌈h/m⌉
	// capped at k (hedge-and-adaptivity plan, E2). Either makes the watch goroutine report the
	// node's deficit to the router on every arrival in the tail and clear the tail on completion.
	if v := os.Getenv("SEGMENT_TAIL_BOUNDS"); v != "" {
		if c.tailK == 0 {
			t.Fatal("SEGMENT_TAIL_BOUNDS requires SEGMENT_TAIL_HEDGE_K")
		}
		if n, err := fmt.Sscanf(v, "%d,%d,%d", &c.tailBounds[0], &c.tailBounds[1], &c.tailBounds[2]); err != nil || n != 3 {
			t.Fatalf("bad SEGMENT_TAIL_BOUNDS %q (want perID,perGroup,perPeer)", v)
		}
		c.tailBounded = true
	}
	c.tailSchedule = os.Getenv("SEGMENT_TAIL_SCHEDULE") != ""
	if c.tailSchedule && c.tailK == 0 {
		t.Fatal("SEGMENT_TAIL_SCHEDULE requires SEGMENT_TAIL_HEDGE_K")
	}
	// SEGMENT_GROUP_PUSH is the sender-side half: stop pushing a group's members to a peer
	// that has IDONTWANT-evidenced enough distinct members to complete, announce instead.
	// Rides the phase path's announce machinery, so it exists only on the phase arm.
	c.groupPush = os.Getenv("SEGMENT_GROUP_PUSH") != ""
	if c.groupPush && !structuredIDsEnabled() {
		t.Fatal("SEGMENT_GROUP_PUSH requires SEGMENT_STRUCTURED_IDS")
	}
	// SEGMENT_GROUP_PUSH_PROJECT=1: the veto counts the pusher's own frames in flight to the
	// peer along with the evidenced ones.
	c.groupProject = os.Getenv("SEGMENT_GROUP_PUSH_PROJECT") != ""
	if c.groupProject && !c.groupPush {
		t.Fatal("SEGMENT_GROUP_PUSH_PROJECT requires SEGMENT_GROUP_PUSH")
	}
	// SEGMENT_HAVE_PROJECT=<f>: the projection at dispatch with fan-in f — the writer drops a
	// queued frame when the peer's announced indices plus f times the frames sent beyond them
	// reach the arm's completion count. Needs the HAVE bitmap.
	if v := os.Getenv("SEGMENT_HAVE_PROJECT"); v != "" {
		f, err := strconv.Atoi(v)
		if err != nil || f < 1 || os.Getenv("SEGMENT_HAVE_BITMAP") == "" {
			t.Fatalf("bad SEGMENT_HAVE_PROJECT %q (want a fan-in >= 1, with SEGMENT_HAVE_BITMAP)", v)
		}
		c.haveProject = f
	}
	// SEGMENT_HAVE_PREDICT_MS=<H>: a node sends the full IDONTWANT bitmap (no claim) once the
	// frames it still needs are what its arrival rate delivers in H ms — the frames already in
	// flight complete it, so pushers stop one flight early. Needs the bitmap.
	if v := os.Getenv("SEGMENT_HAVE_PREDICT_MS"); v != "" {
		h, err := strconv.Atoi(v)
		if err != nil || h < 1 || os.Getenv("SEGMENT_HAVE_BITMAP") == "" || !structuredIDsEnabled() {
			t.Fatalf("bad SEGMENT_HAVE_PREDICT_MS %q (want ms >= 1, with SEGMENT_HAVE_BITMAP and structured ids)", v)
		}
		c.havePredictMS = h
	}
	// SEGMENT_HAVE_PREDICT_PEER=<f>: the claim timed per pusher — pusher p is told once the frames
	// still needed are what the arrival rate delivers in f × p's one-way delay (half its minimum
	// RTT); f = 2 is the claim's flight plus the frames already in flight behind it.
	if v := os.Getenv("SEGMENT_HAVE_PREDICT_PEER"); v != "" {
		f, err := strconv.ParseFloat(v, 64)
		if err != nil || f <= 0 || os.Getenv("SEGMENT_HAVE_BITMAP") == "" || !structuredIDsEnabled() {
			t.Fatalf("bad SEGMENT_HAVE_PREDICT_PEER %q (want f > 0, with SEGMENT_HAVE_BITMAP and structured ids)", v)
		}
		c.havePredictPeer = f
	}
	// SEGMENT_HAVE_PAIR_ORDER=tie|first|ntie|nfirst: each peer's writer pops by a hash of
	// (frame, self, peer) — or (frame, self) under the n-modes, one order toward every peer — as
	// the tie-break within rarest-first, or as the primary key. Needs
	// SEGMENT_HAVE_RAREST.
	switch v := os.Getenv("SEGMENT_HAVE_PAIR_ORDER"); v {
	case "":
	case "tie", "first", "ntie", "nfirst":
		if os.Getenv("SEGMENT_HAVE_RAREST") == "" {
			t.Fatal("SEGMENT_HAVE_PAIR_ORDER needs SEGMENT_HAVE_RAREST")
		}
		c.havePairOrder = map[string]int{"tie": 1, "first": 2, "ntie": 3, "nfirst": 4}[v]
	default:
		t.Fatalf("bad SEGMENT_HAVE_PAIR_ORDER %q (want tie, first, ntie or nfirst)", v)
	}
	// SEGMENT_A_PHASE_R_SOURCE=<r>: the publisher pushes each of its own messages to r mesh
	// peers instead of SEGMENT_A_PHASE_R, announcing the rest as usual.
	if v := os.Getenv("SEGMENT_A_PHASE_R_SOURCE"); v != "" {
		r, err := strconv.Atoi(v)
		if err != nil || r < 1 {
			t.Fatalf("bad SEGMENT_A_PHASE_R_SOURCE %q (want r >= 1)", v)
		}
		c.phaseRSource = r
	}
	// SEGMENT_A_PHASE_R_SOURCE_QUOTA=<q>: the publisher pushes each mesh peer at most q frames of a
	// group and offers the rest to the least-loaded peers, so they pull from it as from a relay
	// (fork WithPhaseSourcePushQuota; privacy).
	if v := os.Getenv("SEGMENT_A_PHASE_R_SOURCE_QUOTA"); v != "" {
		q, err := strconv.Atoi(v)
		if err != nil || q < 1 {
			t.Fatalf("bad SEGMENT_A_PHASE_R_SOURCE_QUOTA %q (want q >= 1)", v)
		}
		c.phaseRQuota = q
	}
	// SEGMENT_A_PHASE_SRC_OFFER_PACE_MS=<ms>: the publisher holds its offers so that the frames it
	// has named reach need only <ms> after publish, as a relay's holding does (fork
	// WithPhaseSourceOfferPace; privacy).
	if v := os.Getenv("SEGMENT_A_PHASE_SRC_OFFER_PACE_MS"); v != "" {
		ms, err := strconv.Atoi(v)
		if err != nil || ms < 1 {
			t.Fatalf("bad SEGMENT_A_PHASE_SRC_OFFER_PACE_MS %q (want ms >= 1)", v)
		}
		c.srcOfferPaceMS = ms
	}
	// SEGMENT_LINK_DISCLOSE=<B>: every node tells each peer at most B frames of a group beyond those
	// they exchanged until it completes the group, on every channel; the source's holding is its
	// pops (fork WithLinkDisclosure over WithOriginHavePaced; privacy).
	if v := os.Getenv("SEGMENT_LINK_DISCLOSE"); v != "" {
		b, err := strconv.Atoi(v)
		if err != nil || b < 1 {
			t.Fatalf("bad SEGMENT_LINK_DISCLOSE %q (want a budget >= 1)", v)
		}
		if m := os.Getenv("SEGMENT_HAVE_BITMAP"); !structuredIDsEnabled() || m == "" || m == "ihave" || os.Getenv("SEGMENT_SRC_HAVE") != "" || os.Getenv("SEGMENT_ORIGIN_HAVE") != "" {
			t.Fatal("SEGMENT_LINK_DISCLOSE needs structured ids and the IDONTWANT-side HAVE bitmap, and excludes SEGMENT_SRC_HAVE and SEGMENT_ORIGIN_HAVE")
		}
		c.linkDisclose = b
	}
	// SEGMENT_SRC_HAVE=dilute:<ms>: the publisher tells each mesh peer the frames exchanged with it
	// plus one admitted set that grows to need at <ms> after publish, on every channel, then its
	// whole HAVE (fork WithOriginDilute; privacy).
	if v, ok := strings.CutPrefix(os.Getenv("SEGMENT_SRC_HAVE"), "dilute:"); ok {
		ms, err := strconv.Atoi(v)
		if err != nil || ms < 1 {
			t.Fatalf("bad SEGMENT_SRC_HAVE dilute:%s (want a pace in ms >= 1)", v)
		}
		c.srcDiluteMS = ms
	}
	// SEGMENT_A_PHASE_R_FAST=<r>: relays whose own configured uplink is at least
	// SEGMENT_A_PHASE_R_FAST_MBPS (default 500) push at r instead of SEGMENT_A_PHASE_R (Q151).
	if v := os.Getenv("SEGMENT_A_PHASE_R_FAST"); v != "" {
		r, err := strconv.Atoi(v)
		if err != nil || r < 1 {
			t.Fatalf("bad SEGMENT_A_PHASE_R_FAST %q (want r >= 1)", v)
		}
		c.phaseRFast, c.phaseRFastMbps = r, 500
		if m := os.Getenv("SEGMENT_A_PHASE_R_FAST_MBPS"); m != "" {
			if c.phaseRFastMbps, err = strconv.Atoi(m); err != nil || c.phaseRFastMbps < 1 {
				t.Fatalf("bad SEGMENT_A_PHASE_R_FAST_MBPS %q", m)
			}
		}
	}
	// SEGMENT_LINK_MOD=m activates each (segment, mesh link) with probability 1/m, decided by
	// a symmetric hash of the unordered peer pair and the segment's structural prefix — the
	// geth-style subgraph trick: a wide mesh whose per-segment effective degree is mesh/m,
	// with the subgraph rotating per segment. Requires structured ids to recognize segments.
	if v := os.Getenv("SEGMENT_LINK_MOD"); v != "" {
		k, err := strconv.Atoi(v)
		if err != nil || k < 2 {
			t.Fatalf("bad SEGMENT_LINK_MOD %q (want >= 2)", v)
		}
		if !structuredIDsEnabled() {
			t.Fatal("SEGMENT_LINK_MOD requires SEGMENT_STRUCTURED_IDS")
		}
		c.linkMod = uint64(k) // lint:ignore uintcast -- bounded small positive by the check above.
	}
	// SEGMENT_LINK_ENFORCE makes the link filter bilateral: announcements over inactive mesh
	// links are declined and pushes over them counted — the receiver-side hardening, since
	// the symmetric predicate lets a receiver verify what a compliant sender would have sent.
	// SEGMENT_A_PHASE_SRC_CONNS=1: the publisher ranks its own pushes over every connected topic
	// peer instead of its mesh, announcing to mesh peers only.
	c.phaseSrcConns = os.Getenv("SEGMENT_A_PHASE_SRC_CONNS") != ""
	// SEGMENT_PHASE_SEL=fewest: push to the mesh peers known to hold the fewest frames of the group
	// (sel=fewest, the NETWORKING 2009 Earliest-Latest peer rule); needs SEGMENT_HAVE_BITMAP.
	switch v := os.Getenv("SEGMENT_PHASE_SEL"); v {
	case "", "rank", "rr":
	case "fewest":
		if os.Getenv("SEGMENT_HAVE_BITMAP") == "" {
			t.Fatal("SEGMENT_PHASE_SEL=fewest needs SEGMENT_HAVE_BITMAP")
		}
		c.phaseSelFewest = true
	default:
		t.Fatalf("bad SEGMENT_PHASE_SEL %q (want rank, rr or fewest)", v)
	}
	// SEGMENT_PHASE_SEL_BACKLOG_BYTES=<n>: sel=fewest ranks a peer with more than n bytes queued from
	// us after the rest (the straggler guard).
	if v := os.Getenv("SEGMENT_PHASE_SEL_BACKLOG_BYTES"); v != "" {
		b, err := strconv.ParseInt(v, 10, 64)
		if err != nil || b <= 0 || !c.phaseSelFewest {
			t.Fatalf("bad SEGMENT_PHASE_SEL_BACKLOG_BYTES %q (want bytes > 0, with SEGMENT_PHASE_SEL=fewest)", v)
		}
		c.phaseSelBacklog = b
	}
	// SEGMENT_PHASE_SEL_GUARD_MS=<ms>: sel=fewest ranks last a peer pushed r frames of a group, the
	// first more than ms ago, with no HAVE or hose ack for it (the proof guard against the mute peer).
	if v := os.Getenv("SEGMENT_PHASE_SEL_GUARD_MS"); v != "" {
		ms, err := strconv.Atoi(v)
		if err != nil || ms <= 0 || !c.phaseSelFewest || os.Getenv("SEGMENT_HAVE_BITMAP") != "both" || os.Getenv("SEGMENT_HOSE") == "" {
			t.Fatalf("bad SEGMENT_PHASE_SEL_GUARD_MS %q (want ms > 0, with SEGMENT_PHASE_SEL=fewest, SEGMENT_HAVE_BITMAP=both and SEGMENT_HOSE)", v)
		}
		c.phaseSelGuard = time.Duration(ms) * time.Millisecond
	}
	// SEGMENT_PHASE_SEL_TOLERANT=lowerhalf|prop: sel=fewest's relays draw their pushes uniformly among
	// the peers at or below the median apparent holding, or in proportion to the frames each appears
	// to lack, in place of the argmax an under-reporter wins.
	switch v := os.Getenv("SEGMENT_PHASE_SEL_TOLERANT"); v {
	case "":
	case "lowerhalf", "prop":
		if !c.phaseSelFewest {
			t.Fatalf("SEGMENT_PHASE_SEL_TOLERANT=%s needs SEGMENT_PHASE_SEL=fewest", v)
		}
		c.phaseSelDraw = pubsub.PhaseSelectLowerHalf
		if v == "prop" {
			c.phaseSelDraw = pubsub.PhaseSelectProportional
		}
	default:
		t.Fatalf("bad SEGMENT_PHASE_SEL_TOLERANT %q (want lowerhalf or prop)", v)
	}
	c.hose = hoseFromEnv(t)
	c.linkEnforce = os.Getenv("SEGMENT_LINK_ENFORCE") != ""
	if c.linkEnforce && c.linkMod == 0 {
		t.Fatal("SEGMENT_LINK_ENFORCE requires SEGMENT_LINK_MOD")
	}
	sk, err := bls.RandKey()
	if err != nil {
		t.Fatal(err)
	}
	// Payload size is fixed at 1 MiB by default; the knob exists to find where whole-message
	// diffusion crosses the deadline, which sets how urgent segmentation is.
	c.payloadLen = 1 << 20
	if v := os.Getenv("SEGMENT_PAYLOAD_BYTES"); v != "" {
		k, err := strconv.Atoi(v)
		if err != nil || k <= 0 {
			t.Fatalf("bad SEGMENT_PAYLOAD_BYTES %q: %v", v, err)
		}
		c.payloadLen = k
	}
	// SEGMENT_COMPRESS_FIRST snappy-compresses the payload before it is segmented and coded, so
	// parity is computed over compressed bytes and no shard is compressible on the wire (the
	// gossip encoder still runs, as in production). SEGMENT_FIXED_COUNT=K sizes the segments to
	// give exactly K of them whatever the length being segmented. Completion counts distinct
	// messages, so the receiving side is unchanged; the whole-message arm keeps the raw payload.
	compress := os.Getenv("SEGMENT_COMPRESS_FIRST") != ""
	// The segmented forms are built only when an arm uses them: a whole-message cell has no
	// unit, and under SEGMENT_STRICT an unset unit is a failure.
	needSegs := os.Getenv("SEGMENT_WARMUP") != ""
	for _, a := range c.arms {
		if a != "whole" {
			needSegs = true
		}
	}
	fixedK, parity := 0, 0
	if v := os.Getenv("SEGMENT_FIXED_COUNT"); v != "" && needSegs {
		k, err := strconv.Atoi(v)
		if err != nil || k <= 0 {
			t.Fatalf("bad SEGMENT_FIXED_COUNT %q", v)
		}
		fixedK = k
	}
	if v := os.Getenv("SEGMENT_PARITY"); v != "" && needSegs {
		// Reed-Solomon coded gossip messages: K+parity on the wire, any K completing a node.
		par, err := strconv.Atoi(v)
		if err != nil || par <= 0 {
			t.Fatalf("bad SEGMENT_PARITY %q", v)
		}
		parity = par
	}
	// build makes message m's wire forms and its segment size. Message 1 is the single-message
	// cell's payload; each later message of a multi-message cell has its own content and slot,
	// so the seen cache and the HAVE bitmaps treat it as new.
	c.build = func(t *testing.T, m int) (whole, segs wireArm, unit int) {
		seed, slot := uint64(11), primitives.Slot(2048)
		if m > 1 {
			seed, slot = uint64(1000+m), primitives.Slot(4096+m) // lint:ignore uintcast -- m is a small positive message number.
		}
		payload := mainnetLikePayload(c.payloadLen, seed)
		whole = wholeArm(t, payload)
		if !needSegs {
			return whole, wireArm{}, 0
		}
		wirePayload := payload
		if compress {
			wirePayload = snappy.Encode(nil, payload)
		}
		if fixedK > 0 {
			unit = (len(wirePayload) + fixedK - 1) / fixedK
		} else {
			unit = segmentSizeBytes(t)
		}
		if parity > 0 {
			return whole, codedArm(t, wirePayload, unit, parity, sk, slot), unit
		}
		return whole, segmentedArm(t, wirePayload, unit, sk, slot), unit
	}
	// buildJoint codes message m's pair as one group (SEGMENT_PAIR=joint): a and b compressed
	// apart, a padded to a whole number of frames, then K = 2·fixedK systematic frames (a's first)
	// and 2·parity parity frames over both. ka is a's systematic frame count.
	c.buildJoint = func(t *testing.T, m int) (wireArm, int) {
		seedA, slot := uint64(11), primitives.Slot(2048)
		if m > 1 {
			seedA, slot = uint64(1000+m), primitives.Slot(4096+m) // lint:ignore uintcast -- m is a small positive message number.
		}
		pa := mainnetLikePayload(c.payloadLen, seedA)
		pb := mainnetLikePayload(c.payloadLen, uint64(1100+m)) // lint:ignore uintcast -- m is a small positive message number.
		if compress {
			pa, pb = snappy.Encode(nil, pa), snappy.Encode(nil, pb)
		}
		if fixedK == 0 || parity == 0 {
			t.Fatal("SEGMENT_PAIR=joint needs SEGMENT_FIXED_COUNT and SEGMENT_PARITY")
		}
		unit := max((len(pa)+fixedK-1)/fixedK, (len(pb)+fixedK-1)/fixedK)
		joint := append(append(append([]byte(nil), pa...), make([]byte, fixedK*unit-len(pa))...), pb...)
		arm := codedArm(t, joint, unit, 2*parity, sk, slot)
		if arm.completeAt() != 2*fixedK {
			t.Fatalf("joint group has K = %d, want %d", arm.completeAt(), 2*fixedK)
		}
		return arm, fixedK
	}
	c.whole, c.segs, c.unit = c.build(t, 1)
	c.pair = pairFromEnv(t)
	if !needSegs {
		return c
	}
	// SEGMENT_WARMUP diffuses an unrelated payload of the same shape first, so every QUIC
	// connection has left slow start before the measured publish. Different content on purpose:
	// the same bytes would be suppressed by the seen cache and warm nothing.
	if os.Getenv("SEGMENT_WARMUP") != "" {
		warmPayload := mainnetLikePayload(c.payloadLen, 12)
		c.warmWhole = wholeArm(t, warmPayload)
		c.warmSegs = segmentedArm(t, warmPayload, segmentSizeBytes(t), sk, primitives.Slot(2049))
	}

	return c
}

// variant is the cell's Q6 variant for one arm at one operating point: n nodes at rate and oneWay,
// which the regime rule needs.
func (c q6Cell) variant(t *testing.T, armName string, rate int, oneWay time.Duration, n int) *q6Variant {
	t.Helper()
	arm, warm, unit := c.whole, c.warmWhole, 0
	if armName != "whole" {
		arm, warm, unit = c.segs, c.warmSegs, c.unit
	}
	frameBytes := 0
	if len(arm.msgs) > 0 {
		frameBytes = arm.total / len(arm.msgs)
	}
	v := &q6Variant{
		armName:         armName,
		unit:            unit,
		arm:             arm,
		warm:            warm,
		whole:           c.whole,
		phaseR:          c.phaseR,
		phaseRSource:    c.phaseRSource,
		phaseRQuota:     c.phaseRQuota,
		srcOfferPace:    time.Duration(c.srcOfferPaceMS) * time.Millisecond,
		linkDisclose:    c.linkDisclose,
		srcDilute:       time.Duration(c.srcDiluteMS) * time.Millisecond,
		regime:          regimeRuleFor(t, meshLinks(t, n, rate), n),
		rate:            rate,
		oneWay:          oneWay,
		stopPull:        c.stopPull,
		haveComplete:    c.haveComplete,
		rebuilt:         c.rebuilt,
		stopPullH:       c.stopPullH,
		haveProject:     c.haveProject,
		havePredictMS:   c.havePredictMS,
		havePredictPeer: c.havePredictPeer,
		havePairOrder:   c.havePairOrder,
		tailK:           c.tailK,
		tailBounds:      c.tailBounds,
		tailBounded:     c.tailBounded,
		tailSchedule:    c.tailSchedule,
		tailH:           c.tailH,
		groupPush:       c.groupPush,
		groupProject:    c.groupProject,
		linkMod:         c.linkMod,
		linkEnforce:     c.linkEnforce,
		phaseSrcConns:   c.phaseSrcConns,
		phaseSelFewest:  c.phaseSelFewest,
		phaseSelBacklog: c.phaseSelBacklog,
		phaseSelGuard:   c.phaseSelGuard,
		phaseSelDraw:    c.phaseSelDraw,
		hose:            c.hose.sized(t, meshLinks(t, n, rate), frameBytes),
		uplinks:         nodeUplinks(meshLinks(t, n, rate)),
		build:           c.build,
		stagger:         staggerFromEnv(t),
	}
	if c.phaseRFast > 0 {
		// Node order is the link buckets' order, as the regime rule reads it.
		for _, l := range meshLinks(t, n, rate) {
			for range l.Count {
				v.fast = append(v.fast, l.LinkSettings.Uplink.BitsPerSecond >= c.phaseRFastMbps*simlibp2p.OneMbps)
			}
		}
		if len(v.fast) != n {
			t.Fatalf("SEGMENT_A_PHASE_R_FAST: %d link settings for %d nodes", len(v.fast), n)
		}
		v.fastR = c.phaseRFast
		nFast := 0
		for i, f := range v.fast {
			if f && i != 0 {
				nFast++
			}
		}
		t.Logf("uplink-class push degree: %d relays at >= %d Mbps push at r = %d, the rest at r = %d", nFast, c.phaseRFastMbps, c.phaseRFast, c.phaseR)
	}
	if c.pair != nil {
		if armName == "whole" && c.pair.prio {
			t.Fatal("SEGMENT_PAIR_PRIO needs a segmented arm: the class rides on rarest-first dispatch")
		}
		v.pair = &pairState{mode: c.pair.mode, prio: c.pair.prio, grants: c.pair.grants, release: c.pair.release, uep: c.pair.uep, pullOnly: c.pair.pullOnly, sourceOnly: c.pair.sourceOnly, windowFirst: c.pair.windowFirst}
		v.buildJoint = c.buildJoint
		v.pair.set(t, v, arm, 1)
	}
	return v
}

// rebuiltKnobs is what a decoded node does with the frames it never received (the decoding
// note's section 5). Without it no frame is rebuilt, as in every run before it.
type rebuiltKnobs struct {
	check   time.Duration // the modelled level-2 check: decode, re-encode, rebuild the tree
	jitter  time.Duration // an extra uniform random delay before the hand-over, per node and group
	seed    uint64
	pushMax int
	mode    pubsub.RebuiltMode
}

// rebuiltFromEnv reads SEGMENT_REBUILT=hold|announce|push: SEGMENT_REBUILT_CHECK_MS (default 5)
// after its decode claim, a node is handed the group's frames it did not receive, as its decode
// would rebuild them, and holds (serves on an ask by index), announces or pushes them.
// SEGMENT_REBUILT_PUSH_MAX caps a push per peer (default 0, its shortfall);
// SEGMENT_REBUILT_JITTER_MS adds a uniform random delay before the hand-over (default 0).
func rebuiltFromEnv(t *testing.T, haveComplete bool) *rebuiltKnobs {
	t.Helper()
	v := os.Getenv("SEGMENT_REBUILT")
	if v == "" {
		return nil
	}
	k := &rebuiltKnobs{check: 5 * time.Millisecond, seed: meshGraphSeed(t)}
	switch v {
	case "hold":
		k.mode = pubsub.RebuiltHold
	case "announce":
		k.mode = pubsub.RebuiltAnnounce
	case "push":
		k.mode = pubsub.RebuiltPush
	default:
		t.Fatalf("bad SEGMENT_REBUILT %q (want hold, announce or push)", v)
	}
	if !haveComplete {
		t.Fatal("SEGMENT_REBUILT needs SEGMENT_HAVE_COMPLETE: frames are rebuilt after the decode claim")
	}
	ms := func(name string) time.Duration {
		s := os.Getenv(name)
		if s == "" {
			return -1
		}
		n, err := strconv.Atoi(s)
		if err != nil || n < 0 {
			t.Fatalf("bad %s %q", name, s)
		}
		return time.Duration(n) * time.Millisecond
	}
	if d := ms("SEGMENT_REBUILT_CHECK_MS"); d >= 0 {
		k.check = d
	}
	if d := ms("SEGMENT_REBUILT_JITTER_MS"); d >= 0 {
		k.jitter = d
	}
	if s := os.Getenv("SEGMENT_REBUILT_PUSH_MAX"); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil || n < 0 {
			t.Fatalf("bad SEGMENT_REBUILT_PUSH_MAX %q", s)
		}
		k.pushMax = n
	}
	t.Logf("rebuilt frames: %s, %v after the decode claim (+ up to %v at random), push cap %d", v, k.check, k.jitter, k.pushMax)
	return k
}

// handOver gives node idx the frames of root it did not receive, once its check is done. It runs
// on its own goroutine, so the node's completion is signalled at the decode claim, as before.
func (k *rebuiltKnobs) handOver(ps *pubsub.PubSub, topic, root string, idx int, missing [][]byte, need int) {
	delay := k.check
	if k.jitter > 0 {
		h := fnv.New64a()
		_, _ = h.Write([]byte(root))
		r := rand.New(rand.NewSource(int64(k.seed<<20 ^ uint64(idx)<<8 ^ h.Sum64()))) // lint:ignore uintcast -- a seed, any bits will do.
		delay += time.Duration(r.Int63n(int64(k.jitter) + 1))
	}
	go func() {
		time.Sleep(delay)
		ps.Rebuilt(topic, missing, pubsub.RebuiltConfig{Mode: k.mode, Need: need, PushMax: k.pushMax})
	}()
}
