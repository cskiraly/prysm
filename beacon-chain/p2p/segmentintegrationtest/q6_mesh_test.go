package segmentintegrationtest

// Q6: what happens on a realistic mesh, and how large a network can this harness drive?
//
// Every other timing arm uses a line, which isolates one mechanism at the cost of a degree far
// below Dlo, so gossipsub never forms a real mesh. This drives a random 8-regular graph -- degree
// equal to Prysm's D -- with a real 1 MiB mainnet-like payload, and measures the time for *every*
// node to hold a complete envelope.
//
// It also answers the harness question directly: 150 nodes completes, at about 15 s of wall clock
// per cell, so nothing here is near a ceiling. The earlier TestTopologyScaleCost figure of 30
// nodes for 2677 goroutines was not evidence for this -- it pushed a five-byte message, so it
// showed the topology comes up rather than that a payload can be driven through it.
//
// Caveat: one seed. The plan calls for several, and the N=20 point below sits off the trend,
// which is the sort of thing a single seed cannot distinguish from noise.
//
// The shared cell driver lives in driver_test.go; this file supplies only variant A's strategy:
// one topic, whole/segmented/coded arms with optional phase forwarding, plus the A-only
// co-resident background traffic, D-copy floor, and publisher control ledger.

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/OffchainLabs/prysm/v7/config/params"
	"github.com/OffchainLabs/prysm/v7/testing/require"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
	simlibp2p "github.com/libp2p/go-libp2p/x/simlibp2p"
)

func TestQ6RealisticMesh(t *testing.T) {
	// Small sizes run by default; the large ones cost too much for the package's Bazel budget.
	sizes := []int{30}
	rates := []int{defaultRate}
	if os.Getenv("SEGMENT_SLOW_TESTS") != "" {
		// n=500 is the working size: the ratio is established there, a two-arm cell costs about
		// three minutes, and peak RSS stays near 3.5 GiB. n=1000 runs and confirms (4.09x) but
		// costs 26 minutes for the pair and pushes a 16 GiB machine into GC thrash, where the
		// wall-clock scaling exponent jumps from ~1.7 to ~3.4. Diameter grows as log_8(N), so
		// doubling past 500 adds a third of a hop and reveals nothing new.
		sizes = []int{30, 500}
		// Sweep around the default so the publisher's D-copy floor can be separated from the
		// mechanism: at 20 Mbps that floor alone is 2.39 s, which is the entire segmented
		// completion time.
		rates = []int{20 * simlibp2p.OneMbps, defaultRate, 500 * simlibp2p.OneMbps}
	}
	latencies := []time.Duration{defaultLatency}
	if os.Getenv("SEGMENT_LATENCY_SWEEP") != "" {
		latencies = []time.Duration{5 * time.Millisecond, defaultLatency}
	}
	if v := os.Getenv("SEGMENT_MESH_SIZES"); v != "" {
		sizes = nil
		for _, f := range strings.Split(v, ",") {
			k, err := strconv.Atoi(strings.TrimSpace(f))
			if err != nil {
				t.Fatalf("bad SEGMENT_MESH_SIZES %q: %v", v, err)
			}
			sizes = append(sizes, k)
		}
	}
	if v := os.Getenv("SEGMENT_MESH_MBPS"); v != "" {
		rates = nil
		for _, f := range strings.Split(v, ",") {
			k, err := strconv.Atoi(strings.TrimSpace(f))
			if err != nil {
				t.Fatalf("bad SEGMENT_MESH_MBPS %q: %v", v, err)
			}
			rates = append(rates, k*simlibp2p.OneMbps)
		}
	}
	// Asymmetric, upload-bound base: a single run labelled by its uplink (see meshLinks).
	if envMbps("SEGMENT_UP_MBPS", 0) > 0 || envMbps("SEGMENT_DOWN_MBPS", 0) > 0 {
		rates = []int{envMbps("SEGMENT_UP_MBPS", defaultRate)}
	}
	params.SetupTestConfigCleanup(t)
	cell := q6CellFromEnv(t)

	for _, oneWay := range latencies {
		for _, rate := range rates {
			for _, n := range sizes {
				for _, armName := range cell.arms {
					var v variant
					switch armName {
					case "ecrs":
						v = newEcrsVariant(t, cell, rate, oneWay)
					case "fulldas":
						v = newFullDASVariant(t, cell, rate, oneWay, n)
					default:
						v = cell.variant(t, armName, rate, oneWay, n)
					}
					runDiffusion(t, diffusionParams{
						n:        n,
						rate:     rate,
						latency:  oneWay,
						deadline: failureDeadline(t),
						seed:     meshGraphSeed(t),
					}, v)
				}
			}
		}
	}
}

// q6Variant is variant A: the whole payload, or K segments, or K+parity coded segments, all on
// one gossip topic, optionally over the forked phase-forwarding router.
type q6Variant struct {
	// Fields are ordered widest-first: nogo's maligned check rejects the natural grouping.
	unit    int // the segment size the arm was cut at; 0 for the whole message
	armName string
	arm     wireArm
	// warm is an unrelated payload of the same shape, diffused before the measured one so
	// every QUIC connection that will carry the payload has already left slow start. Zero
	// value disables it: the study's figures are all cold-transport runs.
	warm   wireArm
	whole  wireArm
	phaseR int
	// regime, when active, picks each node's push degree and whether it runs the request
	// discipline and the tail hedge from R on its own uplink (the oracle rule, E4/E5).
	regime regimeRule
	// fast marks the relays whose own uplink reaches SEGMENT_A_PHASE_R_FAST_MBPS; they push at
	// fastR instead of phaseR (the uplink-class push degree, Q151). Nil when unset.
	fast   []bool
	fastR  int
	rate   int
	oneWay time.Duration
	// stopPull installs a per-node stopPullGate; the watch goroutine flips it when the node
	// completes. Gates are created lazily because pubsubOpts is the first place the node index
	// appears. stopPullH > 0 is the predictive cap K + h on held + asks in flight (Q91).
	stopPullH int
	// haveProject > 0 installs the dispatch-time projection with that fan-in (D6 step 4b).
	haveProject int
	// phaseRSource > 0 is the publisher's own push degree (D7 idea 1); havePredictMS > 0 makes
	// the watch goroutine send the no-claim IDONTWANT bitmap H ms of arrivals ahead of decode
	// (D7 idea 2); havePairOrder is 1 (tie) or 2 (first) for the per-pair order at pop (D7 idea 6).
	phaseRSource  int
	phaseRQuota   int
	srcOfferPace  time.Duration
	havePredictMS int
	havePairOrder int
	// linkDisclose > 0 is every node's per-link disclosure budget (SEGMENT_LINK_DISCLOSE); disclose
	// counts what the budget did over the cell's nodes, per message.
	linkDisclose int
	disclose     pubsub.DisclosureStats
	// srcDilute > 0 is the publisher's dilution pace (SEGMENT_SRC_HAVE=dilute:<ms>); disclose counts it.
	srcDilute time.Duration
	// havePredictPeer > 0 times the claim per pusher instead: pusher p is claimed once the frames
	// still needed are what the arrival rate delivers in havePredictPeer × p's one-way delay.
	havePredictPeer float64
	// predictGaps is, per node that signalled ahead of decode, decode time minus signal time.
	predictGaps []time.Duration
	gates       map[int]*stopPullGate
	deferrals   map[int]*pubsub.RequestDeferral
	// tailK > 0 enables the tail hedge with fanout tailK once a node is within tailH messages
	// of completion; tailExtra counts the hedged asks, tailEntered the nodes that entered.
	tailK, tailH int
	tailExtra    atomic.Int64
	tailEntered  atomic.Int64
	// tailBounded installs the bounds; tailSchedule the fan-out schedule; either makes the watch
	// goroutine report progress and exit the tail. The refusal and top-up counters are reported.
	tailBounds                           [3]int
	tailRefID, tailRefGroup, tailRefPeer atomic.Int64
	tailTopUps                           atomic.Int64
	// groupPush enables sender-side group-aware push suppression on the phase arm;
	// groupVetoes counts suppressed (message, peer) pushes across the cell's nodes.
	groupVetoes atomic.Int64
	// linkMod > 0 activates each (segment, mesh link) with probability 1/linkMod;
	// linkEnforce verifies inbound traffic against the same predicate, counting violations.
	linkMod        uint64
	linkViolations atomic.Int64
	gateMu         sync.Mutex

	stopPull                  bool
	haveComplete              bool
	tailBounded, tailSchedule bool
	groupPush                 bool
	groupProject              bool
	linkEnforce               bool
	// phaseSrcConns ranks the publisher's pushes over every connection.
	phaseSrcConns bool
	// phaseSelFewest pushes to the mesh peers known to hold the fewest frames of the group.
	phaseSelFewest bool
	// phaseSelBacklog, when positive, is sel=fewest's straggler guard in bytes queued.
	phaseSelBacklog int64
	// phaseSelGuard, when positive, is sel=fewest's proof guard grace; guard counts the (peer,
	// group) pairs it demoted this message, by the demoted peer's class.
	phaseSelGuard time.Duration
	guard         guardCounts
	// phaseSelDraw, when set, is sel=fewest's tolerant draw on relayed messages.
	phaseSelDraw pubsub.PhaseSelectTolerance
	// pubID is the publisher's peer id, recorded at setup for the first-hop line.
	pubID peer.ID
	// hose is the cell's hose window (hose_test.go); nw the network, recorded at setup for its
	// base round trips and its report.
	hose    *hoseKnobs
	rebuilt *rebuiltKnobs
	nw      *simNetwork
	// uplinks is each node's uplink in bits per second as meshLinks built it; geo the cell's
	// latency model, recorded at setup (topodump_test.go).
	uplinks []int
	geo     cellGeo
	// build makes message m's wire forms (multimessage_test.go); pub is the current message's
	// publisher and offBase the offer counters at its start, nil on the first message.
	build   func(t *testing.T, m int) (whole, segs wireArm, unit int)
	pub     int
	offBase []pubsub.OfferStat
	// askTracers is each node's ask log under SEGMENT_ASK_TRACE (asktrace_test.go), by node index.
	askTracers map[int]*askTracer
	// spyTracers is each node's spy log under SEGMENT_SPY_TRACE (spytrace_test.go), by node index.
	spyTracers map[int]*spyTracer
	// stem is the source's one-hop stem under SEGMENT_SRC_STEM (stem_test.go), nil otherwise;
	// tracers are the cell's, recorded at settle for the stem's view of the publisher's mesh.
	stem    *stemState
	tracers []*recordingTracer
	// routes is the source's routes under SEGMENT_SRC_ROUTES (routes_test.go), nil otherwise.
	routes *routeState
	// stagger is the source's staggered first contact under SEGMENT_SRC_STAGGER (stagger_test.go), nil otherwise.
	stagger *staggerState
	// replay is the publish schedule under SEGMENT_PUBLISH_REPLAY or SEGMENT_SRC_ADMIT (replay_test.go), nil otherwise;
	// replayLine the current message's release report, msgNum its number.
	replay     *publishReplay
	replayLine string
	msgNum     int
	// pair is the cell's pair state under SEGMENT_PAIR (pair_test.go), nil otherwise; buildJoint
	// codes a pair as one group (SEGMENT_PAIR=joint).
	pair       *pairState
	buildJoint func(t *testing.T, m int) (wireArm, int)
}

// segmentLinkFilter is the symmetric per-(segment, link) activation predicate: sha256 over
// the sorted peer pair and the segment's structural prefix, mod m. sha256 rather than FNV
// because the decision is a mod of the low bits, which need avalanche. Non-segment ids are
// always active — the filter has no opinion about other traffic.
func segmentLinkFilter(mod uint64) func(local, remote peer.ID, mid string) bool {
	return func(local, remote peer.ID, mid string) bool {
		if _, ok := segIDKey(mid); !ok {
			return true
		}
		a, b := string(local), string(remote)
		if a > b {
			a, b = b, a
		}
		h := sha256.New()
		h.Write([]byte(a))
		h.Write([]byte(b))
		h.Write([]byte(mid[:segIDPrefixLen]))
		return binary.LittleEndian.Uint64(h.Sum(nil)[:8])%mod == 0
	}
}

// stopPullGate vetoes segment IWANTs once its node has completed the measured group and, with
// h > 0, ahead of completion: it declines while held shards plus asks in flight (younger than
// the promise window) would exceed K + h, so the tail is bought with at most h spare asks rather
// than one per remaining id (the predictive variant, Q91). Allow must stay pure (the RequestGate
// contract), so the completion flag and the held count are written from outside, by the watch
// goroutine that already decides completion, and Committed records the asks the router actually
// dispatched. Ids without a structural claim pass through: the gate has no opinion about
// non-segment traffic.
type stopPullGate struct {
	declined atomic.Int64
	capped   atomic.Int64
	k, h     int
	inflight map[string]time.Time
	// past holds the groups of a multi-message cell's earlier messages the node completed; their
	// late announcements are declined as the current group's are once done.
	past map[string]bool
	mu   sync.Mutex
	held atomic.Int32
	done atomic.Bool
}

// stopPullAskWindow: an ask older than the promise deadline no longer holds a slot; the
// discipline has moved on from it or given up.
const stopPullAskWindow = 400 * time.Millisecond

func (g *stopPullGate) Allow(_ peer.ID, _, mid string) bool {
	if _, ok := segIDKey(mid); !ok {
		// An ask by index from the IHAVE bitmap is segment traffic under another name; before
		// this check it passed the gate and a completed node kept pulling (D6 step 1, Q121).
		if _, _, want := pubsub.DecodeHaveWant(mid); !want {
			return true
		}
	}
	if g.done.Load() || g.pastGroup(mid) {
		g.declined.Add(1)
		return false
	}
	if g.h == 0 {
		return true
	}
	now := time.Now()
	g.mu.Lock()
	if t0, ok := g.inflight[mid]; ok && now.Sub(t0) < stopPullAskWindow {
		g.mu.Unlock()
		return true // a re-ask of an id that already holds a slot
	}
	n := 0
	for _, t0 := range g.inflight {
		if now.Sub(t0) < stopPullAskWindow {
			n++
		}
	}
	g.mu.Unlock()
	if int(g.held.Load())+n >= g.k+g.h {
		g.capped.Add(1)
		return false
	}
	return true
}

// pastGroup reports whether mid belongs to a group completed in an earlier message.
func (g *stopPullGate) pastGroup(mid string) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if len(g.past) == 0 {
		return false
	}
	if k, ok := segIDKey(mid); ok {
		return g.past[string(k.group[:])]
	}
	root, _, want := pubsub.DecodeHaveWant(mid)
	return want && g.past[root]
}

// nextMessage files the finished message's group, if the node completed it, and opens the gate
// for the next one.
func (g *stopPullGate) nextMessage(group string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.done.Load() && group != "" {
		if g.past == nil {
			g.past = make(map[string]bool)
		}
		g.past[group] = true
	}
	g.done.Store(false)
	g.held.Store(0)
	clear(g.inflight)
}

// completeGroup files one group of a pair cell as past as soon as the node completes it, so its
// late announcements are declined while the other group is still pulled.
func (g *stopPullGate) completeGroup(group string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.past == nil {
		g.past = make(map[string]bool)
	}
	g.past[group] = true
}

func (g *stopPullGate) Committed(_ peer.ID, mids []string) {
	if g.h == 0 {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	for _, mid := range mids {
		if _, ok := segIDKey(mid); ok {
			g.inflight[mid] = time.Now()
		}
	}
}

// delivered records one more held shard; the id's ask, if any, no longer holds a slot.
func (g *stopPullGate) delivered(mid string, held int) {
	g.held.Store(int32(held))
	if g.h == 0 {
		return
	}
	g.mu.Lock()
	delete(g.inflight, mid)
	g.mu.Unlock()
}

func (v *q6Variant) gate(i int) *stopPullGate {
	v.gateMu.Lock()
	defer v.gateMu.Unlock()
	if v.gates == nil {
		v.gates = make(map[int]*stopPullGate)
	}
	g, ok := v.gates[i]
	if !ok {
		g = &stopPullGate{k: v.groupNeed(), h: v.stopPullH, inflight: make(map[string]time.Time)}
		v.gates[i] = g
	}
	return g
}

// deferral is the per-node replay ledger the predictive gate needs; nil for the reactive gate,
// whose declines are permanent by design.
func (v *q6Variant) deferral(i int) *pubsub.RequestDeferral {
	if v.stopPullH == 0 {
		return nil
	}
	v.gateMu.Lock()
	defer v.gateMu.Unlock()
	if v.deferrals == nil {
		v.deferrals = make(map[int]*pubsub.RequestDeferral)
	}
	d, ok := v.deferrals[i]
	if !ok {
		d = pubsub.NewRequestDeferral(4096)
		v.deferrals[i] = d
	}
	return d
}

func (v *q6Variant) name() string { return v.armName }

// groupNeed is one group's completion count: the arm's, or one half's in a pair cell.
func (v *q6Variant) groupNeed() int {
	if v.pair != nil {
		return v.pair.need
	}
	return v.arm.completeAt()
}

func (v *q6Variant) wireForm() wireForm {
	return wireForm{msgs: len(v.arm.msgs), need: v.groupNeed(), unit: v.unit, total: v.arm.total, idBytes: idWidth(v.arm.msgs[0])}
}

func (v *q6Variant) pubsubOpts(t *testing.T, i int) []pubsub.Option {
	var opts []pubsub.Option
	if v.armName == "phase" {
		r := v.phaseR
		if v.regime.active {
			r = v.regime.degree(i)
		}
		if v.fast != nil && i != 0 && v.fast[i] {
			r = v.fastR
		}
		opts = phasePubsubOpts(t, r)
	}
	if v.stopPull {
		// Reactive gate: nil deferral on purpose, a decline after completion is permanent by
		// design. Predictive gate (h > 0): a decline ahead of completion is replayed on delivery.
		opts = append(opts, pubsub.WithRequestGate(v.gate(i), v.deferral(i)))
	}
	if v.tailK > 0 && (!v.regime.active || v.regime.large(i)) {
		opts = append(opts, pubsub.WithIWantTailHedge(v.tailK, &v.tailExtra))
		if v.tailBounded {
			opts = append(opts, pubsub.WithIWantTailHedgeBounds(v.tailBounds[0], v.tailBounds[1], v.tailBounds[2], &v.tailRefID, &v.tailRefGroup, &v.tailRefPeer))
		}
		if v.tailSchedule {
			opts = append(opts, pubsub.WithIWantTailSchedule(v.tailH, &v.tailTopUps))
		}
	}
	if v.groupPush && v.armName == "phase" {
		// Appended after phasePubsubOpts: the option requires WithPhaseForwarding first.
		opts = append(opts, pubsub.WithGroupCompletionPush(func(mid string) (string, bool) {
			k, ok := segIDKey(mid)
			if !ok {
				return "", false
			}
			return string(k.group[:]), true
		}, v.arm.completeAt(), &v.groupVetoes))
		if v.groupProject {
			opts = append(opts, pubsub.WithGroupCompletionProjection())
		}
	}
	if v.haveProject > 0 {
		// After the HAVE options: the projection requires WithHaveBitmaps first.
		opts = append(opts, pubsub.WithHaveDispatchProjection(uint32(v.groupNeed()), v.haveProject))
	}
	if v.havePairOrder > 0 {
		// After the HAVE options: the order requires WithHaveRarestDispatch first.
		opts = append(opts, pubsub.WithHavePairOrder(v.havePairOrder == 2 || v.havePairOrder == 4, v.havePairOrder >= 3))
	}
	if v.pair != nil && v.pair.prio && (!v.pair.sourceOnly || i == 0) {
		// After the HAVE options: the class requires WithHaveRarestDispatch first.
		opts = append(opts, v.pair.classOpts()...)
	}
	if v.phaseRSource > 0 && v.armName == "phase" {
		// After phasePubsubOpts: the option requires WithPhaseForwarding first.
		opts = append(opts, pubsub.WithPhaseSourcePushDegree(v.phaseRSource))
	}
	if v.phaseRQuota > 0 && v.armName == "phase" {
		// After phasePubsubOpts and the HAVE options: the quota requires both.
		opts = append(opts, pubsub.WithPhaseSourcePushQuota(v.phaseRQuota))
	}
	if v.srcOfferPace > 0 && v.armName == "phase" {
		opts = append(opts, pubsub.WithPhaseSourceOfferPace(v.srcOfferPace, v.groupNeed()))
	}
	if v.linkDisclose > 0 {
		// After the HAVE options: disclosure requires WithHaveBitmaps, and the paced origin HAVE first.
		opts = append(opts, pubsub.WithOriginHavePaced(), pubsub.WithLinkDisclosure(v.linkDisclose, v.groupNeed(), &v.disclose))
	}
	if v.srcDilute > 0 {
		// After the HAVE options: dilution requires the paced origin HAVE (SEGMENT_SRC_HAVE=dilute:<ms> sets it).
		var count uint32
		if v.haveComplete {
			count = uint32(len(v.arm.msgs)) // lint:ignore uintcast -- a group's frame count, at most 256.
		}
		opts = append(opts, pubsub.WithOriginDilute(v.srcDilute, v.groupNeed(), count, &v.disclose))
	}
	if v.phaseSelFewest && v.armName == "phase" {
		// After phasePubsubOpts and the HAVE options: it requires both.
		opts = append(opts, pubsub.WithPhaseSelectFewest())
		if v.phaseSelBacklog > 0 {
			opts = append(opts, pubsub.WithPhaseSelectFewestBacklog(v.phaseSelBacklog))
		}
		if v.phaseSelGuard > 0 {
			opts = append(opts, pubsub.WithPhaseSelectFewestGuard(v.phaseSelGuard, func(p peer.ID, _ string) { v.guard.demoted(i, p) }))
		}
		if v.phaseSelDraw != pubsub.PhaseSelectArgmax {
			// The group's frame count is the deficit's reference for the proportional draw.
			opts = append(opts, pubsub.WithPhaseSelectFewestTolerant(v.phaseSelDraw, len(v.arm.msgs)))
		}
	}
	if v.phaseSrcConns && v.armName == "phase" {
		// After phasePubsubOpts: the option requires WithPhaseForwarding first.
		opts = append(opts, pubsub.WithPhaseSourceConnections())
	}
	if v.stagger != nil {
		opts = append(opts, v.stagger.opt(i))
	}
	opts = append(opts, v.hose.opts(i, func() *simNetwork { return v.nw })...)
	if os.Getenv("SEGMENT_ASK_TRACE") != "" {
		a := newAskTracer()
		v.gateMu.Lock()
		if v.askTracers == nil {
			v.askTracers = make(map[int]*askTracer)
		}
		v.askTracers[i] = a
		v.gateMu.Unlock()
		opts = append(opts, pubsub.WithRawTracer(a))
	}
	if spyTraceOut() != "" {
		s := &spyTracer{withSets: spyTraceSets()}
		v.gateMu.Lock()
		if v.spyTracers == nil {
			v.spyTracers = make(map[int]*spyTracer)
		}
		v.spyTracers[i] = s
		v.gateMu.Unlock()
		opts = append(opts, pubsub.WithRawTracer(s))
	}
	// SEGMENT_DISPATCH_CREDITS_SOURCE=<A>[,sticky]: the publisher alone feeds at most A peers at a
	// time, so a slow source serves few flows fast instead of many slowly.
	if v := os.Getenv("SEGMENT_DISPATCH_CREDITS_SOURCE"); v != "" && i == 0 {
		if os.Getenv("SEGMENT_DISPATCH_CREDITS") != "" {
			t.Fatal("SEGMENT_DISPATCH_CREDITS_SOURCE and SEGMENT_DISPATCH_CREDITS are exclusive")
		}
		a, sticky := strings.CutSuffix(v, ",sticky")
		n, err := strconv.Atoi(a)
		if err != nil || n < 1 {
			t.Fatalf("bad SEGMENT_DISPATCH_CREDITS_SOURCE %q (want <A>[,sticky], A >= 1)", v)
		}
		opts = append(opts, pubsub.WithDispatchCredits(n, sticky))
	}
	if v.linkMod > 0 {
		opts = append(opts, pubsub.WithMessageLinkFilter(segmentLinkFilter(v.linkMod)))
		if v.linkEnforce {
			// After the filter: the option requires it installed first.
			opts = append(opts, pubsub.WithMessageLinkFilterEnforcement(&v.linkViolations))
		}
	}
	if os.Getenv("SEGMENT_RAREST_FIRST") != "" {
		opts = append(opts, pubsub.WithRarestFirstPull())
	}
	switch os.Getenv("SEGMENT_RARITY_DRAIN") {
	case "":
	case "delivered":
		opts = append(opts, pubsub.WithRarityOrderedDrain(), pubsub.WithRarityDrainDelivered())
	default:
		opts = append(opts, pubsub.WithRarityOrderedDrain())
	}
	if v := os.Getenv("SEGMENT_PULL_BUDGET"); v != "" {
		k, err := strconv.Atoi(v)
		if err != nil || k <= 0 {
			t.Fatalf("bad SEGMENT_PULL_BUDGET %q", v)
		}
		opts = append(opts, pubsub.WithPullBudget(k))
	}
	return opts
}

func (v *q6Variant) setup(t *testing.T, nw *simNetwork, p diffusionParams) diffusionRun {
	topics, subs, cancelSubs := joinAndSubscribeAll(t, nw, false)
	v.pubID = nw.Hosts[v.pub].ID()
	v.nw = nw
	v.geo = cellGeoFor(t, nw.Len(), v.oneWay)
	if v.stem = stemFromEnv(t, nw.Len(), p.seed, v.uplinks, v.geo); v.stem != nil {
		if v.pair != nil {
			t.Fatal("SEGMENT_SRC_STEM cannot run with SEGMENT_PAIR")
		}
		v.stem.register(nw)
	}
	if v.routes = routesFromEnv(t, p.seed, v.geo); v.routes != nil && v.pair != nil {
		t.Fatal("SEGMENT_SRC_ROUTES cannot run with SEGMENT_PAIR")
	}
	if v.stagger != nil {
		if v.pair != nil {
			t.Fatal("SEGMENT_SRC_STAGGER cannot run with SEGMENT_PAIR")
		}
		v.stagger.setup(p.seed, nw)
	}
	v.msgNum = 1
	if v.replay = publishReplayFromEnv(t); v.replay != nil && (v.stem != nil || v.routes != nil || v.pair != nil) {
		t.Fatal("SEGMENT_PUBLISH_REPLAY and SEGMENT_SRC_ADMIT cannot run with SEGMENT_SRC_STEM, SEGMENT_SRC_ROUTES or SEGMENT_PAIR")
	}
	for _, a := range v.askTracers {
		a.setMessage(v.arm)
	}
	if v.phaseSelGuard > 0 {
		v.guard.setup(t, nw)
		v.guard.reset(v.pub)
	}
	v.setSpyMessage()
	r := &q6Run{v: v, nw: nw, topics: topics, subs: subs, cancelSubs: cancelSubs, n: nw.Len()}
	if v.pair != nil && v.pair.mode == "topics" {
		var cancelB func()
		r.topicsB, r.subsB, cancelB = v.pair.joinB(t, nw)
		r.cancelSubs = func() { cancelB(); cancelSubs() }
	}
	return r
}

// setFirstPublisher makes pub the first message's publisher (SEGMENT_PUBLISHER=<node>).
func (v *q6Variant) setFirstPublisher(pub int) { v.pub = pub }

// nextMessage switches the variant to message m of a multi-message cell, published by node pub:
// the message's own payload, and every per-message instrument and gate started afresh. What the
// network learnt stays: meshes, transport windows, the hose window, scores.
func (v *q6Variant) nextMessage(t *testing.T, m, pub int) {
	group := ""
	if ids := segmentIDs(v.arm); len(ids) > 0 {
		if k, ok := segIDKey(ids[0]); ok {
			group = string(k.group[:])
		}
	}
	whole, segs, _ := v.build(t, m)
	v.whole = whole
	v.arm = whole
	if v.armName != "whole" {
		v.arm = segs
	}
	if v.pair != nil {
		v.pair.set(t, v, v.arm, m)
	}
	v.pub = pub
	v.pubID = v.nw.Hosts[pub].ID()
	v.gateMu.Lock()
	for _, g := range v.gates {
		g.nextMessage(group)
	}
	v.predictGaps = nil
	v.gateMu.Unlock()
	for _, c := range []*atomic.Int64{&v.tailExtra, &v.tailEntered, &v.tailRefID, &v.tailRefGroup, &v.tailRefPeer, &v.tailTopUps, &v.groupVetoes, &v.linkViolations} {
		c.Store(0)
	}
	v.guard.reset(pub)
	v.disclose.Reset()
	v.offBase = hoseNewWindow(v.nw)
	for _, a := range v.askTracers {
		a.setMessage(v.arm)
	}
	v.setSpyMessage()
	if v.stem != nil {
		v.stem.next(t, v.nw, m, pub, func(i int) []peer.ID { return v.tracers[i].MeshPeerIDs() })
	}
	if v.routes != nil {
		v.routes.next(t, v.nw, m, pub)
	}
	v.msgNum, v.replayLine = m, ""
}

// setSpyMessage starts every spy log on the message in flight.
func (v *q6Variant) setSpyMessage() {
	if len(v.spyTracers) == 0 {
		return
	}
	ids, group := spyMessageIDs(v.arm)
	for _, s := range v.spyTracers {
		s.setMessage(v.arm, ids, group)
	}
}

func (v *q6Variant) onTimeout(*testing.T, int, int) {
	// No A-specific diagnostics; the driver reports and fails censored cells itself.
}

func (v *q6Variant) report(t *testing.T, s diffusionStats) {
	if v.pair != nil {
		t.Logf("        %s", v.pair.line(s.startAt, s.pub, s.n))
	}
	if out := diffusionOut(); diffusionCurves() && out != "" && len(v.arm.msgs) > 0 {
		ids := segmentIDs(v.arm)
		if s.msg > 1 {
			out = fmt.Sprintf("%s.m%d", out, s.msg)
		}
		mx := diffusionMatrix{Arm: v.armName, N: s.n, Segments: len(v.arm.msgs), Need: v.arm.completeAt(), Publisher: s.pub}
		for _, tr := range s.tracers {
			got, heard := segmentDiffusion(tr, v.arm, ids, s.startAt)
			mx.FirstGot = append(mx.FirstGot, got)
			mx.Heard = append(mx.Heard, heard)
		}
		if err := writeDiffusionMatrix(out, mx); err != nil {
			t.Fatalf("writing the diffusion matrix: %v", err)
		}
		t.Logf("      per-segment curves: %d nodes x %d segments -> %s", len(mx.FirstGot), mx.Segments, out)
	}
	if out := spyTraceOut(); out != "" && len(v.spyTracers) > 0 {
		spies := make([]*spyTracer, s.n)
		hosts := make([]peer.ID, s.n)
		for i := range s.n {
			spies[i] = v.spyTracers[i]
			hosts[i] = v.nw.Hosts[i].ID()
		}
		rows, changes, sets, err := writeSpyTrace(out, max(s.msg, 1), s.pub, v.arm, spies, hosts, s.startAt)
		if err != nil {
			t.Fatalf("writing the spy trace: %v", err)
		}
		t.Logf("      spy trace: %d rows and %d mesh changes from %d nodes -> %s", rows, changes, len(v.spyTracers), out)
		if spyTraceSets() {
			t.Logf("      spy trace sets: %d rows", sets)
		}
	}
	t.Logf("      setup timeline (virtual, all before the measured publish): peering %v  mesh settle %v  pre-publish %v",
		s.tPeer, s.tMesh, s.tPre)
	floor := time.Duration(float64(8*v.whole.total*8) / float64(v.rate) * float64(time.Second))
	flag := ""
	if s.drops > 0 {
		flag = fmt.Sprintf("  *** %d DROPS", s.drops)
	}
	t.Logf("%4dMbps L=%-5v n=%-3d %-10s %3d/%d  virtual %9v (D-copy floor %8v)  %s  dups %6d  IDONTWANT tx %6d  dup-per-idw %5.2f  publisherTx %8dB  rx/node %8dB  ctrl rx/node %6dB  wall %v%s  [degree %d, mesh %s]",
		v.rate/simlibp2p.OneMbps, v.oneWay, s.n, v.armName, s.completed, s.n-1,
		s.virt.Round(time.Millisecond), floor.Round(time.Millisecond),
		completionStats(s.compTimes, s.n-1, s.deadline),
		s.dups, s.idwSent, float64(s.dups)/math.Max(1, float64(s.idwSent)),
		s.publisherTx, s.rxBytes/s.n, s.ctrlRx/s.n, s.wall.Round(time.Millisecond), flag,
		connectivityDegree(t, s.n), s.meshSummary)
	// The publisher's control ledger decomposes its upload: an IWANT received is a copy served
	// on top of the D mesh pushes. One id per IWANT entry on the whole-message arm, so entries
	// read directly as copies there.
	t.Logf("        publisher control: IHAVE sent %d, IWANT recv %d carrying %d ids, IDONTWANT recv %d",
		s.pubControl.IhaveSent, s.pubControl.IwantRecv, s.pubControl.IwantIdsRecv, s.pubControl.IdontwantRecv)
	if v.stopPull {
		v.gateMu.Lock()
		var declined, capped int64
		var flipped int
		for _, g := range v.gates {
			declined += g.declined.Load()
			capped += g.capped.Load()
			if g.done.Load() {
				flipped++
			}
		}
		v.gateMu.Unlock()
		t.Logf("        stop-pull: %d IWANTs declined after completion, %d capped ahead of it (h=%d), %d nodes flipped", declined, capped, v.stopPullH, flipped)
	}
	if v.rebuilt != nil && v.nw != nil {
		var sum pubsub.RebuiltStat
		nodes := 0
		for _, ps := range v.nw.Pubsubs {
			if r, ok := ps.RebuiltStats(); ok {
				nodes++
				sum.Held += r.Held
				sum.AnnouncedTo += r.AnnouncedTo
				sum.Announced += r.Announced
				sum.Pushed += r.Pushed
				sum.Served += r.Served
			}
		}
		t.Logf("        rebuilt (cumulative): %d nodes, %d frames held, %d bitmaps announcing %d frames, %d frames pushed, %d served on an ask",
			nodes, sum.Held, sum.AnnouncedTo, sum.Announced, sum.Pushed, sum.Served)
	}
	if v.havePredictMS > 0 {
		v.gateMu.Lock()
		gaps := append([]time.Duration(nil), v.predictGaps...)
		v.gateMu.Unlock()
		sort.Slice(gaps, func(i, j int) bool { return gaps[i] < gaps[j] })
		late := 0
		limit := 2 * time.Duration(v.havePredictMS) * time.Millisecond
		for _, g := range gaps {
			if g > limit {
				late++
			}
		}
		pct := func(p float64) time.Duration {
			if len(gaps) == 0 {
				return 0
			}
			return gaps[int(float64(len(gaps)-1)*p)]
		}
		t.Logf("        predict@: %d nodes signalled ahead of decode (H=%d ms); decode after signal p50 %s p90 %s max %s; %d past 2H",
			len(gaps), v.havePredictMS, pct(0.5), pct(0.9), pct(1), late)
	}
	if v.groupPush {
		t.Logf("        group-push: %d (message, peer) pushes vetoed", v.groupVetoes.Load())
	}
	if v.linkDisclose > 0 {
		d := &v.disclose
		t.Logf("        disclosure: budget %d; max disclosed per link before completion %d (%d of %d links over budget, by exchanged frames alone; max exchanged %d, max beyond exchanged %d); %d completions; %d indices withheld, %d asks refused, %d source announces deferred to the pop",
			v.linkDisclose, d.MaxShown.Load(), d.OverBudget.Load(), d.Links.Load(), d.MaxTraded.Load(), d.MaxBeyond.Load(), d.Completed.Load(), d.Withheld.Load(), d.Refused.Load(), d.Deferred.Load())
	}
	if v.srcDilute > 0 {
		d := &v.disclose
		t.Logf("        dilution: pace %s; max told per link before completion %d (max exchanged %d, max beyond exchanged %d); %d completions; %d indices withheld, %d asks refused, %d source announces deferred to the pop",
			v.srcDilute, d.MaxShown.Load(), d.MaxTraded.Load(), d.MaxBeyond.Load(), d.Completed.Load(), d.Withheld.Load(), d.Refused.Load(), d.Deferred.Load())
	}
	if v.phaseSelGuard > 0 {
		t.Logf("        %s", v.guard.line(v.phaseSelGuard))
	}
	if v.pubID != "" {
		t.Logf("        %s", firstHopLine(v.arm, s, v.pubID, "publisher"))
	}
	if v.replayLine != "" {
		t.Logf("        %s", v.replayLine)
	}
	if v.stem != nil {
		lines, err := v.stem.lines(v, s)
		for _, l := range lines {
			t.Logf("        %s", l)
		}
		if err != nil {
			t.Errorf("message %d: %v", s.msg, err)
		}
	}
	if v.routes != nil {
		lines, err := v.routes.lines(v, s)
		for _, l := range lines {
			t.Logf("        %s", l)
		}
		if err != nil {
			t.Errorf("message %d: %v", s.msg, err)
		}
	}
	if v.stagger != nil {
		for _, l := range v.stagger.lines(v, s) {
			t.Logf("        %s", l)
		}
	}
	if overlapOn() && s.tracers != nil && v.nw != nil {
		hosts := make([]peer.ID, len(v.nw.Hosts))
		for i, h := range v.nw.Hosts {
			hosts[i] = h.ID()
		}
		t.Logf("        %s", overlapLine(v.arm, s, hosts))
	}
	if len(v.askTracers) > 0 && s.tracers != nil {
		ats := make([]*askTracer, s.n)
		for i, a := range v.askTracers {
			ats[i] = a
		}
		idx := make(map[peer.ID]int, s.n)
		var relays []int
		for i, h := range v.nw.Hosts {
			idx[h.ID()] = i
			if i != s.pub {
				relays = append(relays, i)
			}
		}
		t.Logf("        %s", askLine("publisher", []int{s.pub}, ats, s.tracers, idx, s.compAt, s.startAt))
		t.Logf("        %s", askLine("relays", relays, ats, s.tracers, idx, s.compAt, s.startAt))
	}
	if (v.hose != nil || os.Getenv("SEGMENT_DELAY_SPLIT") != "") && s.tracers != nil && v.nw != nil {
		var relays []int
		for i := range v.nw.Hosts {
			if i != s.pub {
				relays = append(relays, i)
			}
		}
		t.Logf("        %s", delaySplitLine("publisher", []int{s.pub}, v.nw, s.tracers, s.startAt))
		t.Logf("        %s", delaySplitLine("relays", relays, v.nw, s.tracers, s.startAt))
	}
	if l := pacedGossipLine(v.nw); l != "" {
		t.Logf("        %s", l)
	}
	for _, l := range hoseLines(t, v.nw, v.pub, v.offBase) {
		t.Logf("        %s", l)
	}
	for _, l := range hoseEstimateLines(t, v.nw, v.pub, v.hose) {
		t.Logf("        %s", l)
	}
	for _, l := range uploadLines(t, v.nw, v.pub) {
		t.Logf("        %s", l)
	}
	if l := pushShareLine(t, v.nw, v.pub); l != "" {
		t.Logf("        %s", l)
	}
	if s.tracers != nil {
		// Frames pushed or answered back to the source; zero when relays skip it as the author.
		f, b := s.tracers[s.pub].RecvFrames()
		t.Logf("        publisher rx: frames %d bytes %d", f, b)
	}
	if promiseCountOn() && v.nw != nil {
		t.Logf("        %s", promiseLine(v.nw))
	}
	if v.tailK > 0 {
		t.Logf("        tail-hedge: %d extra asks issued, %d nodes entered the tail (k=%d, h=%d)",
			v.tailExtra.Load(), v.tailEntered.Load(), v.tailK, v.tailH)
		if v.tailBounded || v.tailSchedule {
			t.Logf("        tail-bounds: refused per-id %d per-group %d per-peer %d (bounds %d,%d,%d); schedule %v, top-ups %d",
				v.tailRefID.Load(), v.tailRefGroup.Load(), v.tailRefPeer.Load(), v.tailBounds[0], v.tailBounds[1], v.tailBounds[2], v.tailSchedule, v.tailTopUps.Load())
		}
	}
	if v.linkEnforce {
		t.Logf("        link-enforce: %d violations (compliant network expects ~0; GRAFT/PRUNE transients only)",
			v.linkViolations.Load())
	}
	if os.Getenv("SEGMENT_PER_SEGMENT_STATS") != "" && len(v.arm.known) > 1 && s.tracers != nil {
		last, got := perSegmentSpread(v.arm, s)
		sorted := append([]time.Duration(nil), last...)
		sort.Slice(sorted, func(a, b int) bool { return sorted[a] < sorted[b] })
		short := 0
		for _, g := range got {
			if g < s.n-1 {
				short++
			}
		}
		t.Logf("        per-segment last arrival: min %v med %v p90 %v max %v over %d segments; %d segments not received by every node",
			sorted[0].Round(time.Millisecond), sorted[len(sorted)/2].Round(time.Millisecond),
			sorted[len(sorted)*9/10].Round(time.Millisecond), sorted[len(sorted)-1].Round(time.Millisecond),
			len(sorted), short)
	}
}

// perSegmentSpread computes, per known message, when the last node to receive it did so and how
// many nodes received it at all — the per-segment diffusion profile that per-node completion
// (a max over segments) cannot show. Under stop-pull or coding not every node receives every
// segment, so the receive count travels with the time.
func perSegmentSpread(arm wireArm, s diffusionStats) ([]time.Duration, []int) {
	last := make([]time.Duration, len(arm.msgs))
	got := make([]int, len(arm.msgs))
	for i, tr := range s.tracers {
		if i == s.pub {
			continue // the publisher holds everything by construction
		}
		tr.Mu.Lock()
		seen := make(map[digest]bool, len(arm.msgs))
		for _, e := range tr.Recvs {
			idx, ok := arm.known[e.What]
			if !ok || seen[e.What] {
				continue
			}
			seen[e.What] = true
			got[idx]++
			if d := e.At.Sub(s.startAt); d > last[idx] {
				last[idx] = d
			}
		}
		tr.Mu.Unlock()
	}
	return last, got
}

// firstHopLine counts the nodes other than the publisher that received at least one of the arm's
// frames straight from pub, first copies and duplicates alike, and how many frames each took from
// it; name is pub's role in the line.
func firstHopLine(arm wireArm, s diffusionStats, pub peer.ID, name string) string {
	var per []int
	for i, tr := range s.tracers {
		if i == s.pub {
			continue
		}
		tr.Mu.Lock()
		c := 0
		for _, evs := range [][]recvEvent{tr.Recvs, tr.Dups} {
			for _, e := range evs {
				if _, ok := arm.known[e.What]; ok && e.From == pub {
					c++
				}
			}
		}
		tr.Mu.Unlock()
		if c > 0 {
			per = append(per, c)
		}
	}
	if len(per) == 0 {
		return "first-hop: 0 nodes received frames from the " + name
	}
	sort.Ints(per)
	sum := 0
	for _, c := range per {
		sum += c
	}
	return fmt.Sprintf("first-hop: %d nodes received %d frames from the %s; per node min %d med %d max %d",
		len(per), sum, name, per[0], per[len(per)/2], per[len(per)-1])
}

// q6Run is variant A's live cell state.
type q6Run struct {
	v          *q6Variant
	nw         *simNetwork
	topics     []*pubsub.Topic
	subs       []*pubsub.Subscription
	cancelSubs func()
	n          int
	// topicsB and subsB are the second topic of a SEGMENT_PAIR=topics cell.
	topicsB []*pubsub.Topic
	subsB   []*pubsub.Subscription
}

func (r *q6Run) settle(t *testing.T, tracers []*recordingTracer) string {
	// Assert the mesh rather than trusting the sleep inside the join, and record the sizes: they
	// are the evidence that connectivity degree and mesh degree now differ, which is what makes
	// non-mesh peers exist.
	if r.subsB != nil {
		return summariseMesh(awaitMeshMany(t, tracers, 2, false))
	}
	sum := summariseMesh(awaitMesh(t, tracers, false))
	r.v.tracers = tracers
	if out := topoDumpOut(); out != "" {
		line, err := writeTopoDump(out, r.nw, r.v.uplinks, r.v.geo)
		if err != nil {
			t.Fatalf("writing the topology dump: %v", err)
		}
		t.Logf("      %s", line)
	}
	if r.v.stem != nil {
		// The first message's stem is drawn once the publisher's mesh is up, so it can avoid it.
		r.v.stem.next(t, r.nw, 1, r.v.pub, func(i int) []peer.ID { return tracers[i].MeshPeerIDs() })
	}
	if r.v.routes != nil {
		r.v.routes.next(t, r.nw, 1, r.v.pub)
	}
	return sum
}

func (r *q6Run) cancel() { r.cancelSubs() }

// prePublish starts the A-only co-resident background load (F2b gap): it competes for uplinks
// and queues during the whole diffusion. Cancelled with the run context via the driver's
// deferred cancel, which drains its goroutines before wg.Wait.
// warmUp diffuses the warm arm and drains it from every subscription.
//
// Draining matters: the driver's watch goroutines read the same subscriptions afterwards, so a
// warm-up message left queued would be counted as the measured payload arriving instantly.
func (r *q6Run) warmUp(t *testing.T, ctx context.Context) {
	t.Helper()
	want := len(r.v.warm.msgs)
	var batch pubsub.MessageBatch
	for _, m := range r.v.warm.msgs {
		require.NoError(t, r.topics[0].AddToBatch(ctx, &batch, m))
	}
	require.NoError(t, r.nw.Pubsubs[0].PublishBatch(&batch))
	for i := 1; i < r.n; i++ {
		seen := make(map[digest]struct{}, want)
		for len(seen) < want {
			msg, err := r.subs[i].Next(ctx)
			require.NoError(t, err)
			d := digestOf(msg.Data)
			if _, ok := r.v.warm.known[d]; !ok {
				t.Fatalf("node %d received a non-warm-up message during warm-up", i)
			}
			seen[d] = struct{}{}
		}
	}
	settle(false)
}

func (r *q6Run) prePublish(t *testing.T, ctx context.Context, wg *sync.WaitGroup) {
	if len(r.v.warm.msgs) > 0 {
		r.warmUp(t, ctx)
	}
	bgSent, bgStop := startBackgroundTraffic(t, r.nw, ctx, wg)
	_ = bgStop // the run context's deferred cancel stops the goroutines; wg.Wait drains them.
	if bgSent != nil {
		t.Cleanup(func() {
			t.Logf("background traffic: %d B published/node-avg", bgSent.Load()/int64(r.n))
		})
		// Lead-in: let the background process reach steady state before the measured
		// publish, so the payload meets loaded queues rather than a cold start. Virtual
		// time, so the wait is free; without it, calibrated per-node intervals longer
		// than the diffusion window round the whole load down to zero.
		if d := envDuration("SEGMENT_BG_LEADIN_MS", 0); d > 0 {
			time.Sleep(d)
		}
	}
}

// watchNode reads one node's subscription until the arm's completion count of distinct known
// messages has arrived, driving the node's stop-pull gate and tail hedge on the way. False means
// the context ended first. q6Run.watch runs it for every receiver in-process; the Shadow node
// runs it once, for itself.
func (v *q6Variant) watchNode(ctx context.Context, idx int, ps *pubsub.PubSub, sub *pubsub.Subscription) bool {
	arm := v.arm
	seen := make(map[digest]bool, len(arm.msgs))
	inTail := false
	lastMid := ""
	// D7 idea 2: the prediction. firstAt is the first known frame's arrival; the node signals
	// once when the frames it still needs are what its average arrival rate delivers in H.
	var firstAt, predictedAt time.Time
	horizon := time.Duration(v.havePredictMS) * time.Millisecond
	// The per-pusher form: each topic peer's one-way delay, read at the first arrival, and whether
	// it has had its claim.
	var peerFirst time.Time
	var peerOWD map[peer.ID]time.Duration
	claimed := map[peer.ID]bool{}
	for len(seen) < arm.completeAt() {
		msg, err := sub.Next(ctx)
		if err != nil {
			return false
		}
		if _, ok := arm.known[digestOf(msg.Data)]; ok {
			seen[digestOf(msg.Data)] = true
			lastMid = msg.ID
			if v.stopPull {
				v.gate(idx).delivered(msg.ID, len(seen))
				if v.stopPullH > 0 {
					v.deferral(idx).Replay() // a slot freed: re-offer what the cap declined
				}
			}
			if v.havePredictPeer > 0 {
				now := time.Now()
				if peerFirst.IsZero() {
					peerFirst = now
					peerOWD = make(map[peer.ID]time.Duration)
					for _, p := range ps.ListPeers(sub.Topic()) {
						if r := quicMinRTT(v.nw, idx, p); r > 0 {
							peerOWD[p] = r / 2
						}
					}
				} else if h := len(seen); h >= 2 && h < arm.completeAt() {
					remaining := float64(arm.completeAt() - h)
					rate := float64(h-1) / now.Sub(peerFirst).Seconds()
					if root, _, ok := haveIndexOf(msg.ID); ok {
						for p, d := range peerOWD {
							if !claimed[p] && remaining <= rate*v.havePredictPeer*d.Seconds() {
								claimed[p] = true
								ps.HaveSuppressTo(sub.Topic(), root, uint32(len(arm.msgs)), p)
							}
						}
					}
				}
			}
			if horizon > 0 && predictedAt.IsZero() {
				now := time.Now()
				if firstAt.IsZero() {
					firstAt = now
				} else if h := len(seen); h >= 2 && len(seen) < arm.completeAt() {
					// rate = (h-1)/(now-firstAt); remaining <= rate*H  <=>  remaining*(now-firstAt) <= (h-1)*H
					remaining := time.Duration(arm.completeAt() - h)
					if remaining*now.Sub(firstAt) <= time.Duration(h-1)*horizon {
						if root, _, ok := haveIndexOf(msg.ID); ok {
							predictedAt = now
							ps.HaveSuppress(sub.Topic(), root, uint32(len(arm.msgs)))
						}
					}
				}
			}
		}
		if v.tailK > 0 && !inTail && len(seen) >= arm.completeAt()-v.tailH {
			inTail = true
			v.tailEntered.Add(1)
			ps.EnterTailHedge()
		}
		if inTail && (v.tailBounded || v.tailSchedule) && len(seen) < arm.completeAt() {
			ps.TailProgress(arm.completeAt() - len(seen))
		}
	}
	if inTail && (v.tailBounded || v.tailSchedule) {
		ps.ExitTailHedge()
	}
	if v.stopPull {
		v.gate(idx).done.Store(true)
	}
	if !predictedAt.IsZero() {
		gap := time.Since(predictedAt)
		v.gateMu.Lock()
		v.predictGaps = append(v.predictGaps, gap)
		v.gateMu.Unlock()
	}
	// D6 step 2: the completed node holds every frame of the group and says so at once.
	if v.haveComplete {
		if root, _, ok := haveIndexOf(lastMid); ok {
			ps.HaveAll(sub.Topic(), root, uint32(len(arm.msgs)))
			if v.rebuilt != nil {
				// What the node's decode rebuilds: the frames it never received.
				var missing [][]byte
				for _, d := range arm.msgs {
					if !seen[digestOf(d)] {
						missing = append(missing, d)
					}
				}
				v.rebuilt.handOver(ps, sub.Topic(), root, idx, missing, arm.completeAt())
			}
		}
	}
	return true
}

func (r *q6Run) watch(ctx context.Context, wg *sync.WaitGroup, start time.Time, signal func(int, time.Duration)) {
	stem := -1
	var handOff []int
	if r.v.stem != nil {
		stem = r.v.stem.stemNode()
		handOff = r.v.stem.handOffNodes()
		r.v.stem.armSignal(signal, start)
	}
	if r.v.routes != nil {
		// The routes' publishers stand in for the source, as the stem does.
		stem = r.v.pub
		handOff = r.v.routes.publishers()
		r.v.routes.armSignal(signal, start)
	}
	if r.v.stagger != nil {
		// The chosen are drawn from the publisher's mesh as it stands at publish.
		r.v.stagger.arm(r.v.msgNum, r.v.pub, r.v.tracers[r.v.pub].MeshPeerIDs(), r.v.arm, start)
	}
	for i := range r.n {
		signal := signal
		switch {
		case slices.Contains(handOff, i):
			// The stem stands in for the source: it completes when it holds the payload.
			continue
		case i == r.v.pub && stem < 0:
			continue
		case i == r.v.pub:
			// Under the stem the source relays the message like any node, but is not waited for.
			signal = func(int, time.Duration) {}
		}
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			ok := false
			if r.v.pair != nil && r.v.pair.mode == "joint" {
				ok = r.v.pair.watchJoint(ctx, r.v, idx, r.nw.Pubsubs[idx], r.subs[idx])
			} else if r.v.pair != nil {
				subs := []*pubsub.Subscription{r.subs[idx]}
				if r.subsB != nil {
					subs = append(subs, r.subsB[idx])
				}
				ok = r.v.pair.watchNode(ctx, r.v, idx, r.nw.Pubsubs[idx], subs)
			} else {
				ok = r.v.watchNode(ctx, idx, r.nw.Pubsubs[idx], r.subs[idx])
			}
			if ok {
				signal(idx, time.Since(start))
			}
		}(i)
	}
}

func (r *q6Run) publish(ctx context.Context, t *testing.T) error {
	arm := r.v.arm
	if r.v.pair != nil {
		var topicB *pubsub.Topic
		if r.topicsB != nil {
			topicB = r.topicsB[r.v.pub]
		}
		return r.v.pair.publish(ctx, r.v, r.topics[r.v.pub], topicB, r.nw.Pubsubs[r.v.pub])
	}
	omit := failPublishOmit(t, len(arm.msgs))
	if perPeer := srcPerPeer(t); perPeer > 0 {
		if omit != nil {
			t.Fatal("SEGMENT_SRC_PER_PEER excludes SEGMENT_FAIL_PUBLISH_WITHHOLD")
		}
		omit = srcPerPeerOmit(t, len(arm.msgs), perPeer*len(r.v.tracers[r.v.pub].MeshPeerIDs()))
	}
	// The last-piece screen's placement: the last w ids leave from h placed holders, not from
	// the publisher; they are published right after the batch, so the placed copies exist from
	// the start and only the source differs.
	narrowW, holders := narrowHolders(t, r.n)
	narrowFrom := len(arm.msgs) - narrowW
	if narrowW > 0 {
		if narrowW >= len(arm.msgs) {
			t.Fatalf("SEGMENT_FAIL_PUBLISH_NARROW: %d of %d ids", narrowW, len(arm.msgs))
		}
		t.Logf("failure exposure: the last %d ids published by %d placed holders %v, not by the publisher", narrowW, len(holders), holders)
		omitNarrow := make(map[int]bool, len(omit)+narrowW)
		for i := range omit {
			omitNarrow[i] = omit[i]
		}
		for mi := narrowFrom; mi < len(arm.msgs); mi++ {
			omitNarrow[mi] = true
		}
		omit = omitNarrow
	}
	seedHolders := func() error {
		for mi := narrowFrom; narrowW > 0 && mi < len(arm.msgs); mi++ {
			for _, h := range holders {
				if err := r.topics[h].Publish(ctx, arm.msgs[mi]); err != nil {
					return err
				}
			}
		}
		return nil
	}
	if st := r.v.stem; st != nil {
		if narrowW > 0 {
			t.Fatal("SEGMENT_SRC_STEM cannot run with SEGMENT_FAIL_PUBLISH_NARROW")
		}
		stem := st.publisherNode()
		spyOf := func(i int) *spyTracer {
			r.v.gateMu.Lock()
			defer r.v.gateMu.Unlock()
			return r.v.spyTracers[i]
		}
		if st.failsafe > 0 {
			pub := r.v.pub
			st.armFailsafe(ctx, arm.completeAt(), func() int {
				return gossipFrames(r.v.tracers[pub], arm, r.nw.Hosts[pub].ID())
			}, func() error {
				return r.v.publishFrom(ctx, r.topics[pub], r.nw.Pubsubs[pub], omit)
			})
		}
		return st.handOff(ctx, r.nw, arm, spyOf, func(idx []int) error {
			return r.v.publishFrames(ctx, r.topics[stem], r.nw.Pubsubs[stem], idx, omit)
		})
	}
	if rs := r.v.routes; rs != nil {
		if narrowW > 0 {
			t.Fatal("SEGMENT_SRC_ROUTES cannot run with SEGMENT_FAIL_PUBLISH_NARROW")
		}
		return rs.release(ctx, len(arm.msgs), func(node int, idx []int) error {
			if idx == nil {
				return r.v.publishFrom(ctx, r.topics[node], r.nw.Pubsubs[node], omit)
			}
			return r.v.publishFrames(ctx, r.topics[node], r.nw.Pubsubs[node], idx, omit)
		})
	}
	if rp := r.v.replay; rp != nil {
		if narrowW > 0 {
			t.Fatal("SEGMENT_PUBLISH_REPLAY cannot run with SEGMENT_FAIL_PUBLISH_NARROW")
		}
		line, err := rp.publish(r.v.msgNum, arm.completeAt(), len(arm.msgs), func(idx []int) error {
			return r.v.publishFrames(ctx, r.topics[r.v.pub], r.nw.Pubsubs[r.v.pub], idx, omit)
		})
		r.v.replayLine = line
		return err
	}
	if err := r.v.publishFrom(ctx, r.topics[r.v.pub], r.nw.Pubsubs[r.v.pub], omit); err != nil {
		return err
	}
	return seedHolders()
}

// publishFrom sends the arm's messages from one node, the ids in omit left out: as one batch, or
// sequentially under SEGMENT_PUBLISH_MODE=fcfs. q6Run.publish wraps it with the last-piece
// screen's placed holders; the Shadow node, one process per node, calls it directly.
// SEGMENT_PUBLISH_MODE=fcfs is the batch-publishing counterfactual: segments published
// sequentially in index order, each fanning to the full mesh before the next enters —
// the in-order cut-through pipeline, with none of the batch's first-copy prioritisation
// or peer rotation. Segment k's first copy leaves the source ~D times later than under
// the batch, so this prices the batch's reordering/path-diversity contribution.
func (v *q6Variant) publishFrom(ctx context.Context, topic *pubsub.Topic, ps *pubsub.PubSub, omit map[int]bool) error {
	idx := make([]int, len(v.arm.msgs))
	for mi := range idx {
		idx[mi] = mi
	}
	return v.publishFrames(ctx, topic, ps, idx, omit)
}

// publishFrames is publishFrom for the arm's messages at the indices idx, in that order.
func (v *q6Variant) publishFrames(ctx context.Context, topic *pubsub.Topic, ps *pubsub.PubSub, idx []int, omit map[int]bool) error {
	if os.Getenv("SEGMENT_PUBLISH_MODE") == "fcfs" {
		for _, mi := range idx {
			if omit[mi] {
				continue
			}
			if err := topic.Publish(ctx, v.arm.msgs[mi]); err != nil {
				return err
			}
		}
		return nil
	}
	var batch pubsub.MessageBatch
	for _, mi := range idx {
		if omit[mi] {
			continue
		}
		if err := topic.AddToBatch(ctx, &batch, v.arm.msgs[mi]); err != nil {
			return err
		}
	}
	return ps.PublishBatch(&batch)
}

// guardCounts is the proof guard's demotions this message: (peer, group) pairs by the demoted
// peer's class, attackers being the withholder and spoofer sets as populations() reads them.
type guardCounts struct {
	mu                         sync.Mutex
	idx                        map[peer.ID]int
	attackers                  map[int]bool
	pub                        int
	honest, attacker, bySource int
	unknown                    int
	distinct, honestPeers      map[int]bool
}

// setup records the cell's peer ids and attacker set; demotions before it count as unknown.
func (g *guardCounts) setup(t *testing.T, nw *simNetwork) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.idx = make(map[peer.ID]int, len(nw.Hosts))
	for i, h := range nw.Hosts {
		g.idx[h.ID()] = i
	}
	g.attackers = map[int]bool{}
	for i := range failWithholdSet(t, len(nw.Hosts)) {
		g.attackers[i] = true
	}
	for i := range failSpoofSet(t, len(nw.Hosts)) {
		g.attackers[i] = true
	}
	g.resetLocked()
}

// reset starts the counts afresh for a message published by pub.
func (g *guardCounts) reset(pub int) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.pub = pub
	g.resetLocked()
}

func (g *guardCounts) resetLocked() {
	g.honest, g.attacker, g.bySource, g.unknown = 0, 0, 0, 0
	g.distinct, g.honestPeers = map[int]bool{}, map[int]bool{}
}

// demoted hears one (peer, group) pair demoted by pusher; any node's event loop.
func (g *guardCounts) demoted(pusher int, p peer.ID) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if pusher == g.pub {
		g.bySource++
	}
	j, ok := g.idx[p]
	switch {
	case !ok:
		g.unknown++
	case g.attackers[j]:
		g.attacker++
		g.distinct[j] = true
	default:
		g.honest++
		g.honestPeers[j] = true
	}
}

// line is the message's report.
func (g *guardCounts) line(grace time.Duration) string {
	g.mu.Lock()
	defer g.mu.Unlock()
	return fmt.Sprintf("sel-guard (grace %v): %d (peer, group) pairs demoted = attacker %d (%d distinct of %d) + honest %d (%d distinct) + unknown %d; by the publisher %d",
		grace, g.attacker+g.honest+g.unknown, g.attacker, len(g.distinct), len(g.attackers), g.honest, len(g.honestPeers), g.unknown, g.bySource)
}
