package segmentintegrationtest

// The Q6 cell as the environment describes it, and its variant at one operating point. Factored
// out of TestQ6RealisticMesh so the Shadow node (shadow_node_test.go), the star and known-answer
// cells build the same wire forms from the same knobs as the in-process driver.

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/OffchainLabs/prysm/v7/consensus-types/primitives"
	"github.com/OffchainLabs/prysm/v7/crypto/bls"
	"github.com/golang/snappy"
)

// q6Cell is what the environment says about a Q6 cell: the arms, variant A's knobs and the wire
// forms of the payload.
type q6Cell struct {
	arms         []string
	phaseR       int
	stopPull     bool
	stopPullH    int
	tailK, tailH int
	tailBounds   [3]int
	tailBounded  bool
	tailSchedule bool
	groupPush    bool
	linkMod      uint64
	linkEnforce  bool
	payloadLen   int
	unit         int // the segment size the segmented arms were cut at
	segs         wireArm
	whole        wireArm
	warmSegs     wireArm
	warmWhole    wireArm
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
	// capped at k (E2). Either makes the watch goroutine report the node's deficit to the
	// router on every arrival in the tail and clear the tail on completion.
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
	payload := mainnetLikePayload(c.payloadLen, 11)
	// SEGMENT_COMPRESS_FIRST snappy-compresses the payload before it is segmented and coded, so
	// parity is computed over compressed bytes and no shard is compressible on the wire (the
	// gossip encoder still runs, as in production). SEGMENT_FIXED_COUNT=K sizes the segments to
	// give exactly K of them whatever the length being segmented. Completion counts distinct
	// messages, so the receiving side is unchanged; the whole-message arm keeps the raw payload.
	wirePayload := payload
	if os.Getenv("SEGMENT_COMPRESS_FIRST") != "" {
		wirePayload = snappy.Encode(nil, payload)
	}
	// The segmented forms are built only when an arm uses them: a whole-message cell has no
	// unit, and under SEGMENT_STRICT an unset unit is a failure.
	needSegs := os.Getenv("SEGMENT_WARMUP") != ""
	for _, a := range c.arms {
		if a != "whole" {
			needSegs = true
		}
	}
	c.whole = wholeArm(t, payload)
	if !needSegs {
		return c
	}
	segSize := 0
	if v := os.Getenv("SEGMENT_FIXED_COUNT"); v != "" {
		k, err := strconv.Atoi(v)
		if err != nil || k <= 0 {
			t.Fatalf("bad SEGMENT_FIXED_COUNT %q", v)
		}
		segSize = (len(wirePayload) + k - 1) / k
	} else {
		segSize = segmentSizeBytes(t)
	}
	c.unit = segSize
	c.segs = segmentedArm(t, wirePayload, segSize, sk, primitives.Slot(2048))
	if v := os.Getenv("SEGMENT_PARITY"); v != "" {
		// Reed-Solomon coded gossip messages: K+parity on the wire, any K completing a node.
		par, err := strconv.Atoi(v)
		if err != nil || par <= 0 {
			t.Fatalf("bad SEGMENT_PARITY %q", v)
		}
		c.segs = codedArm(t, wirePayload, segSize, par, sk, primitives.Slot(2048))
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
	return &q6Variant{
		armName:      armName,
		unit:         unit,
		arm:          arm,
		warm:         warm,
		whole:        c.whole,
		phaseR:       c.phaseR,
		regime:       regimeRuleFor(t, meshLinks(t, n, rate), n),
		rate:         rate,
		oneWay:       oneWay,
		stopPull:     c.stopPull,
		stopPullH:    c.stopPullH,
		tailK:        c.tailK,
		tailBounds:   c.tailBounds,
		tailBounded:  c.tailBounded,
		tailSchedule: c.tailSchedule,
		tailH:        c.tailH,
		groupPush:    c.groupPush,
		linkMod:      c.linkMod,
		linkEnforce:  c.linkEnforce,
	}
}
