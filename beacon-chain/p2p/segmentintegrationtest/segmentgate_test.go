package segmentintegrationtest

// The receiver authority gate -- stage 2 of the gated-pull design.
//
// Any binding of the segment commitment to the bid makes segment authentication depend on holding
// the block, so a receiver that does not yet have the block cannot verify what it is being offered.
// In announce-and-pull the receiver decides whether to ask, so it can decline -- and the router's
// WithRequestGate hook is where that decision goes, ahead of every mutating request policy so a
// decline stays retryable.
//
// This stage prices the central cost without the two-publisher driver: the gate opens on a
// synthetic schedule at a swept offset from publish, standing in for block arrival. What that buys
// is the shape of the answer -- how much completion time does waiting for authority cost, and does
// a node recover once the gate opens -- cheaply enough to decide whether stages 3-5 are worth
// building at all.
//
// The gate is deliberately the *only* thing that changes. It suppresses pulls; it does not stop an
// unsolicited push, because a receiver cannot refuse arrival. That asymmetry is the real behaviour,
// and it is why the pull-heavy arms are where the cost shows up.

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
)

// segmentGateStats aggregates gate decisions across a cell's nodes.
type segmentGateStats struct {
	admitted atomic.Int64 // segment ids the gate let through
	declined atomic.Int64 // segment ids declined for want of authority
	retried  atomic.Int64 // ids declined at least once and later admitted
	passthru atomic.Int64 // non-segment ids the gate never had an opinion about
}

func (s *segmentGateStats) line() string {
	return fmt.Sprintf("gate: admitted %d, declined %d, later-admitted %d, passthrough %d",
		s.admitted.Load(), s.declined.Load(), s.retried.Load(), s.passthru.Load())
}

// segmentGate is one node's authority gate.
//
// Authority is a *time*, set by whoever knows when it arrived: the synthetic schedule sets it at
// publish plus a swept offset, and the two-publisher driver sets it at the node's own block
// installation. Zero means closed -- before the authorizing block exists nothing can be
// authenticated, which is also the honest state of a node that never receives it.
//
// In shadow mode the gate records the decision it would have made and then admits anyway, which is
// what lets a control cell price the gate without changing the race it is measuring.
type segmentGate struct {
	mu     sync.Mutex
	shadow bool
	// Authority is per block root, not one global boolean. Installing any block must not authorize
	// candidates claiming *every* root: that would admit competing forks and arbitrary
	// attacker-chosen roots on the strength of one unrelated block.
	openAt map[[32]byte]time.Time
	// committed models the bid's commitment set, keyed by the slot it belongs to. See
	// slotCommitment for why the slot rather than the root is the key.
	committed map[uint64]slotCommitment
	perKey    map[segKey]map[string]struct{} // distinct content ids requested per structural claim
	perKeyMx  int
	declined  map[string]struct{} // ids this node turned down, to spot recovery
	stats     *segmentGateStats
}

// slotCommitment is what a node learns when it installs a slot's block: the segment-group
// commitments the chain accepted for that slot, and the moment this node held them.
//
// # Why the slot and not the block root
//
// openAt keys authority by block root, which can only ever answer "not yet" for a root this node
// has not installed -- and an attacker names its own roots, so a root-keyed check can defer a
// fabricated candidate but never refuse one. Refusing needs *completeness*: for slot S this node
// holds the whole committed set, so a group id outside it is not something the chain committed to.
// That is the fact the bid carries and the reason this is keyed by slot.
//
// # What this stands in for, and why it is not cheating
//
// The commitment is specified to ride in SignedExecutionPayloadBid, which changes the bid's
// hash_tree_root and is therefore gated on a spec PR. Both ends of a simulation are ours, so the
// set is modelled here instead -- the same argument that let the structured message id be adopted
// harness-locally. Modelling it is the precondition for measuring anything about a flood: without
// it a node can only ever defer, and a cell would report the cost of the unimplemented half rather
// than a property of the design.
//
// # The fork exposure, stated rather than hidden
//
// A slot may have competing blocks, and the design's set is the union over the blocks a node
// holds. A node holding only its own head therefore refuses a segment belonging to a competing
// valid fork -- correct for what it is building on, and re-acquirable through req/resp after a
// reorg, but it does downscore an honest forwarder. The harness models a single head; a cell that
// wants the fork case installs both groups.
type slotCommitment struct {
	at     time.Time
	groups map[[32]byte]struct{}
}

// newSegmentGate builds one node's gate, closed.
func newSegmentGate(perKeyMax int, stats *segmentGateStats, shadow bool) *segmentGate {
	return &segmentGate{
		shadow:    shadow,
		openAt:    make(map[[32]byte]time.Time),
		committed: make(map[uint64]slotCommitment),
		perKey:    make(map[segKey]map[string]struct{}),
		perKeyMx:  perKeyMax,
		declined:  make(map[string]struct{}),
		stats:     stats,
	}
}

// open records the moment this node gained authority for one block root.
func (g *segmentGate) open(root [32]byte, at time.Time) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.openAt[root] = at
}

// commit records the commitment set this node installed for one slot, at time at.
//
// Additive: a second call adds groups rather than replacing them, so the union a fork produces is
// expressible. An empty set is not "the chain committed to nothing" but "this node does not hold
// this slot's commitments" -- the safe reading, since treating an unset slot as complete would
// refuse every honest segment in a cell that never installed anything.
func (g *segmentGate) commit(slot uint64, at time.Time, groups ...[32]byte) {
	g.mu.Lock()
	defer g.mu.Unlock()
	c, ok := g.committed[slot]
	if !ok {
		c = slotCommitment{at: at, groups: make(map[[32]byte]struct{}, len(groups))}
	}
	for _, id := range groups {
		c.groups[id] = struct{}{}
	}
	g.committed[slot] = c
}

// committedGroup reports whether group is one the chain committed to for slot, as of now. This is
// the design's authentication rule: membership, not a claim the sender can make.
func (g *segmentGate) committedGroup(slot uint64, group [32]byte) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	c, ok := g.committed[slot]
	if !ok || time.Now().Before(c.at) {
		return false
	}
	_, ok = c.groups[group]
	return ok
}

// slotCommitted reports whether this node holds slot's commitment set as of now, which is what
// makes a non-member refusable rather than merely unverifiable.
func (g *segmentGate) slotCommitted(slot uint64) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	c, ok := g.committed[slot]
	return ok && len(c.groups) > 0 && !time.Now().Before(c.at)
}

// authorizedLocked reports whether authority for root has arrived. Caller holds the lock.
func (g *segmentGate) authorizedLocked(root [32]byte) bool {
	at, ok := g.openAt[root]
	return ok && !time.Now().Before(at)
}

// authorized is authorizedLocked for callers that do not hold the lock.
func (g *segmentGate) authorized(root [32]byte) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.authorizedLocked(root)
}

// Allow is the router predicate, and it is pure: it answers, it charges nothing. The router runs
// its own request policies after this one and any of them may still refuse the id, so a budget
// spent here would be spent on requests that were never made -- two content variants of one
// structural claim could then permanently exclude the honest third. Charging happens in Committed.
func (g *segmentGate) Allow(_ peer.ID, _, mid string) bool {
	k, ok := segIDKey(mid)
	if !ok {
		g.stats.passthru.Add(1)
		return true
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.authorizedLocked(k.authorityKey()) {
		g.declined[mid] = struct{}{}
		g.stats.declined.Add(1)
		// Shadow mode: the decision is recorded, the request goes through anyway.
		return g.shadow
	}
	// The per-structural-key cap is what stops an attacker announcing unlimited content-hash
	// variants of one (block root, group, index) and walking past every per-id cap: the id is
	// unique each time, the claim is not. Read here, charged in Committed.
	if !g.withinPerKeyLocked(k, mid) {
		g.stats.declined.Add(1)
		return g.shadow
	}
	return true
}

// Committed charges the per-claim budget for the ids actually dispatched, and notes recoveries.
func (g *segmentGate) Committed(_ peer.ID, mids []string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	for _, mid := range mids {
		k, ok := segIDKey(mid)
		if !ok {
			continue
		}
		sk := k.structuralKey()
		seen := g.perKey[sk]
		if seen == nil {
			seen = make(map[string]struct{})
			g.perKey[sk] = seen
		}
		seen[mid] = struct{}{}
		if _, wasDeclined := g.declined[mid]; wasDeclined {
			delete(g.declined, mid)
			g.stats.retried.Add(1)
		}
		g.stats.admitted.Add(1)
	}
}

// withinPerKeyLocked reports whether mid would fit under its claim's allowance. An id already
// charged is always within it -- re-asking a peer for something we already requested is a request
// policy's business, not the gate's.
func (g *segmentGate) withinPerKeyLocked(k segKey, mid string) bool {
	seen := g.perKey[k.structuralKey()]
	if seen == nil {
		return g.perKeyMx > 0
	}
	if _, dup := seen[mid]; dup {
		return true
	}
	return len(seen) < g.perKeyMx
}

// gateOffset is the synthetic block-arrival delay, or -1 when the cell runs no gate.
func gateOffset(t *testing.T) time.Duration {
	t.Helper()
	v := os.Getenv("SEGMENT_GATE_OFFSET_MS")
	if v == "" {
		return -1
	}
	ms, err := strconv.Atoi(v)
	if err != nil || ms < 0 {
		t.Fatalf("bad SEGMENT_GATE_OFFSET_MS %q", v)
	}
	// The gate reads a structural claim out of the message id, so it is meaningless without the
	// ids that carry one. Fail loudly rather than measuring a gate that silently passes everything.
	if !structuredIDsEnabled() {
		t.Fatal("SEGMENT_GATE_OFFSET_MS requires SEGMENT_STRUCTURED_IDS")
	}
	return time.Duration(ms) * time.Millisecond
}

// gateSpread gives each node its own offset, so authority arrives across a window rather than
// everywhere at once. Zero means a uniform gate, which isolates the cost of waiting from the cost
// of nodes disagreeing about when they may ask.
func gateSpread(t *testing.T) time.Duration {
	t.Helper()
	v := os.Getenv("SEGMENT_GATE_SPREAD_MS")
	if v == "" {
		return 0
	}
	ms, err := strconv.Atoi(v)
	if err != nil || ms < 0 {
		t.Fatalf("bad SEGMENT_GATE_SPREAD_MS %q", v)
	}
	return time.Duration(ms) * time.Millisecond
}

// segmentGates builds one gate per node, or nil when the cell runs no gate.
type segmentGates struct {
	nodes     []*segmentGate
	deferrals []*pubsub.RequestDeferral // nil entries when the cell runs the no-retry baseline
	admits    []*segmentAdmission       // bounded deferred admission, one per node
	stats     *segmentGateStats
	admitted  *admissionStats
	base      time.Duration
	after     []time.Duration // per-node synthetic offset; unused when a real block drives the gate
	// commitSlot and commitGroups are the modelled bid: installing authority also installs the
	// slot's commitment set, which is what lets a node refuse a group instead of only deferring
	// it. Empty unless a cell asked for it, and then the gate behaves exactly as it did before.
	commitSlot   uint64
	commitGroups [][32]byte
}

// modelCommitments makes authority installation carry the slot's committed groups, the way the bid
// is specified to. Call before the gates are armed.
func (g *segmentGates) modelCommitments(slot uint64, groups ...[32]byte) {
	if g == nil || len(groups) == 0 {
		return
	}
	g.commitSlot = slot
	g.commitGroups = groups
}

// newSegmentGates builds a cell's gates.
//
// driven means the variant supplies authority itself -- the two-publisher driver opens each node's
// gate at its own block installation -- in which case there is no synthetic offset and no timer.
// Otherwise the gates run the synthetic schedule and exist only if the cell asked for one.
func newSegmentGates(t *testing.T, n int, seed uint64, driven bool) *segmentGates {
	t.Helper()
	base := gateOffset(t)
	if driven {
		if !structuredIDsEnabled() {
			// Without structured ids the gate cannot read a claim out of an id, so it would
			// silently pass everything. Fail rather than measure a gate that is not there.
			t.Fatal("the two-publisher driver requires SEGMENT_STRUCTURED_IDS")
		}
		base = 0
	} else if base < 0 {
		return nil
	}
	spread := gateSpread(t)
	if driven {
		spread = 0 // authority spread comes from real block diffusion, not from a draw
	}
	perKeyMax := 2
	if v := os.Getenv("SEGMENT_GATE_PERKEY_MAX"); v != "" {
		k, err := strconv.Atoi(v)
		if err != nil || k < 1 {
			t.Fatalf("bad SEGMENT_GATE_PERKEY_MAX %q", v)
		}
		perKeyMax = k
	}
	// SEGMENT_GATE_RETRY=0 keeps the no-retry baseline, which is the arm that shows what a decline
	// costs when nothing replays it. Default on: without a ledger the measurement prices heartbeat
	// gossip's rediscovery, not the authority delay.
	retry := os.Getenv("SEGMENT_GATE_RETRY") != "0"
	deferredMax := 4096
	if v := os.Getenv("SEGMENT_GATE_DEFER_MAX"); v != "" {
		k, err := strconv.Atoi(v)
		if err != nil || k < 0 {
			t.Fatalf("bad SEGMENT_GATE_DEFER_MAX %q", v)
		}
		deferredMax = k
	}
	// A driven cell defaults to shadow: step 1 of the build order measures the ungated race while
	// recording what the gate would have decided. SEGMENT_GATE_SHADOW=0 turns the gate on for real.
	shadow := shadowGates()
	if driven && os.Getenv("SEGMENT_GATE_SHADOW") == "" {
		shadow = true
	}
	if os.Getenv("SEGMENT_GATE_SHADOW") == "0" {
		shadow = false
	}
	caps := defaultAdmissionCaps(t)
	g := &segmentGates{
		stats:     &segmentGateStats{},
		admitted:  &admissionStats{},
		base:      base,
		nodes:     make([]*segmentGate, n),
		deferrals: make([]*pubsub.RequestDeferral, n),
		admits:    make([]*segmentAdmission, n),
		after:     make([]time.Duration, n),
	}
	for i := range g.nodes {
		if retry {
			g.deferrals[i] = pubsub.NewRequestDeferral(deferredMax)
		}
		g.after[i] = base
		if spread > 0 {
			// Deterministic per-node draw over [base, base+spread), through the same avalanche
			// step the geography model uses so a cell stays reproducible under a fixed seed.
			g.after[i] += time.Duration(geoMix(seed^0x9a7e_0000^uint64(i)) % uint64(spread)) // lint:ignore uintcast -- bounded by spread.
		}
		g.nodes[i] = newSegmentGate(perKeyMax, g.stats, shadow)
		// The gate and the admission queue share the shadow flag on purpose: gating requests while
		// still relaying what you cannot verify is not a coherent configuration, and measuring it
		// would produce exactly the half-enforced numbers that had to be retracted once.
		// SEGMENT_ADMIT_SHADOW overrides the shared flag, so the two halves of the rule can be
		// isolated when one of them breaks a cell.
		admitShadow := shadow
		switch os.Getenv("SEGMENT_ADMIT_SHADOW") {
		case "0":
			admitShadow = false
		case "1":
			admitShadow = true
		}
		g.admits[i] = newSegmentAdmission(caps, g.nodes[i], g.admitted, admitShadow)
	}
	return g
}

// shadowGates reports whether gates record their decisions without acting on them. The ungated
// shadow control is what prices the gate without changing the race being measured: an unauthorized
// request is counted as it would have been declined, and then sent anyway.
func shadowGates() bool { return os.Getenv("SEGMENT_GATE_SHADOW") != "" }

// openNode grants node i authority now and replays whatever it declined.
//
// The two-publisher driver calls this from the node's own block-installation moment; the synthetic
// schedule calls it from a timer. Either way this is the block-installed event the design turns on,
// and it is deliberately event-driven rather than heartbeat-polled: a polled replay would put its
// own interval into every measurement, which is exactly the artifact the no-retry arm exposed.
func (g *segmentGates) openNode(i int, root [32]byte) {
	if g == nil {
		return
	}
	g.nodes[i].open(root, time.Now())
	// The block carries the bid, so the same event that grants authority for this root installs
	// the slot's commitment set. Both or neither: a node that could refuse a group before it could
	// authenticate one would be modelling a bid that arrives ahead of its own block.
	if len(g.commitGroups) > 0 {
		g.nodes[i].commit(g.commitSlot, time.Now(), g.commitGroups...)
	}
	if d := g.deferrals[i]; d != nil {
		d.Replay()
	}
	// Draining is the other half of the same event. A node that gained authority must both ask for
	// what it declined and release what it was already holding; releasing is what finally lets it
	// relay, so skipping it would strand the segments it buffered and its downstream peers with them.
	// Exactly this root: a block installation authorizes its own segments and nothing else.
	if a := g.admits[i]; a != nil {
		a.drain(context.Background(), root)
	}
}

// armAll opens every node's gate on the synthetic schedule: publish plus that node's offset.
//
// This is the *stress* schedule, not the realistic one. It models universal inversion -- every node
// lacking the block when the first segment arrives -- which is the reverse of the honest ordering,
// where the builder cannot reveal until it holds the block. The two-publisher driver calls openNode
// from real block arrival instead; this path stays for counterfactual offsets and partition cells,
// where no real block schedule exists.
func (g *segmentGates) armAll() {
	if g == nil {
		return
	}
	// The synthetic schedule authorizes the one root a cell's segments claim.
	root := cellBlockRoot(syntheticSlot)
	for i := range g.nodes {
		if g.after[i] <= 0 {
			g.openNode(i, root)
			continue
		}
		time.AfterFunc(g.after[i], func() { g.openNode(i, root) })
	}
}

// admit runs the two-phase sequence the router runs: ask, and if allowed, report the dispatch.
// The unit tests go through this so they exercise the same ordering the router does.
func (g *segmentGate) admit(topic, mid string) bool {
	if !g.Allow(peer.ID("p"), topic, mid) {
		return false
	}
	g.Committed(peer.ID("p"), []string{mid})
	return true
}

// TestSegmentGateDecision covers the gate's own logic without a network: the three ways an id can
// be treated, and the two properties the design depends on -- a decline is retryable, and a
// structural claim cannot be reopened without limit under fresh content ids.
func TestSegmentGateDecision(t *testing.T) {
	k := segKey{slot: 7, index: 1}
	k.blockRoot[0] = 0xab
	k.group[0] = 0xcd
	idFor := func(tag byte) string {
		b := append(k.encode(), make([]byte, segIDDigestLen)...)
		b[len(b)-1] = tag
		return string(b)
	}

	newGate := func(perKey int) *segmentGate {
		return newSegmentGate(perKey, &segmentGateStats{}, false)
	}
	openGate := func(perKey int) *segmentGate {
		g := newGate(perKey)
		g.open(k.authorityKey(), time.Now())
		return g
	}

	t.Run("a gate with no authority is closed", func(t *testing.T) {
		g := newGate(2)
		if g.admit("t", idFor(1)) {
			t.Fatal("a gate with no authority admitted a segment id")
		}
	})

	t.Run("shadow mode records the decision and admits anyway", func(t *testing.T) {
		g := newSegmentGate(2, &segmentGateStats{}, true)
		if !g.admit("t", idFor(1)) {
			t.Fatal("a shadow gate blocked a request")
		}
		if g.stats.declined.Load() != 1 {
			t.Fatalf("shadow decline counted %d times, want 1", g.stats.declined.Load())
		}
	})

	t.Run("non-segment ids pass through", func(t *testing.T) {
		g := newGate(2)
		if !g.admit("t", "twenty-byte-id-here!") {
			t.Fatal("gate blocked an id it has no claim to read")
		}
		if g.stats.passthru.Load() != 1 {
			t.Fatalf("passthrough counted %d times", g.stats.passthru.Load())
		}
	})

	t.Run("declines before authority, admits after, and counts the recovery", func(t *testing.T) {
		g := newGate(2)
		id := idFor(1)
		if g.admit("t", id) {
			t.Fatal("gate admitted before authority")
		}
		g.open(k.authorityKey(), time.Now())
		if !g.admit("t", id) {
			t.Fatal("gate declined after authority")
		}
		if g.stats.retried.Load() != 1 {
			t.Fatalf("recovery counted %d times, want 1", g.stats.retried.Load())
		}
	})

	t.Run("the per-key cap bounds distinct content under one claim", func(t *testing.T) {
		g := openGate(2)
		if !g.admit("t", idFor(1)) || !g.admit("t", idFor(2)) {
			t.Fatal("gate declined within its per-key allowance")
		}
		if g.admit("t", idFor(3)) {
			t.Fatal("gate admitted a third content id under one structural claim")
		}
		t.Run("but re-asking an already admitted id is free", func(t *testing.T) {
			if !g.admit("t", idFor(1)) {
				t.Fatal("gate declined an id it had already admitted")
			}
		})
	})

	t.Run("a different index is a different claim", func(t *testing.T) {
		g := openGate(1)
		other := k
		other.index++
		mid := string(append(other.encode(), make([]byte, segIDDigestLen)...))
		if !g.admit("t", idFor(1)) || !g.admit("t", mid) {
			t.Fatal("one claim's allowance consumed another's")
		}
	})
}
