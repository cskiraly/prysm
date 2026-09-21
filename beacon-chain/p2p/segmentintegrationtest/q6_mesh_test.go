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
					runDiffusion(t, diffusionParams{
						n:        n,
						rate:     rate,
						latency:  oneWay,
						deadline: failureDeadline(t),
						seed:     meshGraphSeed(t),
					}, cell.variant(t, armName, rate, oneWay, n))
				}
			}
		}
	}
}

// q6Variant is variant A: the whole payload, or K segments, or K+parity coded segments, all on
// one gossip topic, optionally over the forked phase-forwarding router.
type q6Variant struct {
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
	rate   int
	oneWay time.Duration
	// stopPull installs a per-node stopPullGate; the watch goroutine flips it when the node
	// completes. Gates are created lazily because pubsubOpts is the first place the node index
	// appears.
	stopPull  bool
	stopPullH int // > 0: the predictive cap K + h on held + asks in flight (Q91)
	gateMu    sync.Mutex
	gates     map[int]*stopPullGate
	deferrals map[int]*pubsub.RequestDeferral
	// tailK > 0 enables the tail hedge with fanout tailK once a node is within tailH messages
	// of completion; tailExtra counts the hedged asks, tailEntered the nodes that entered.
	tailK, tailH int
	tailExtra    atomic.Int64
	tailEntered  atomic.Int64
	// tailBounded installs the bounds; tailSchedule the fan-out schedule; either makes the watch
	// goroutine report progress and exit the tail. The refusal and top-up counters are reported.
	tailBounds                           [3]int
	tailBounded, tailSchedule            bool
	tailRefID, tailRefGroup, tailRefPeer atomic.Int64
	tailTopUps                           atomic.Int64
	// groupPush enables sender-side group-aware push suppression on the phase arm;
	// groupVetoes counts suppressed (message, peer) pushes across the cell's nodes.
	groupPush   bool
	groupVetoes atomic.Int64
	// linkMod > 0 activates each (segment, mesh link) with probability 1/linkMod;
	// linkEnforce verifies inbound traffic against the same predicate, counting violations.
	linkMod        uint64
	linkEnforce    bool
	linkViolations atomic.Int64
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
	done     atomic.Bool
	declined atomic.Int64
	capped   atomic.Int64
	k, h     int
	held     atomic.Int32
	mu       sync.Mutex
	inflight map[string]time.Time
}

// stopPullAskWindow: an ask older than the promise deadline no longer holds a slot; the
// discipline has moved on from it or given up.
const stopPullAskWindow = 400 * time.Millisecond

func (g *stopPullGate) Allow(_ peer.ID, _, mid string) bool {
	if _, ok := segIDKey(mid); !ok {
		return true
	}
	if g.done.Load() {
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
		g = &stopPullGate{k: v.arm.completeAt(), h: v.stopPullH, inflight: make(map[string]time.Time)}
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

func (v *q6Variant) wireForm() wireForm {
	return wireForm{msgs: len(v.arm.msgs), need: v.arm.completeAt(), unit: v.unit, total: v.arm.total, idBytes: idWidth(v.arm.msgs[0])}
}

func (v *q6Variant) pubsubOpts(t *testing.T, i int) []pubsub.Option {
	var opts []pubsub.Option
	if v.armName == "phase" {
		r := v.phaseR
		if v.regime.active {
			r = v.regime.degree(i)
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

func (v *q6Variant) setup(t *testing.T, nw *simNetwork, _ diffusionParams) diffusionRun {
	topics, subs, cancelSubs := joinAndSubscribeAll(t, nw, false)
	return &q6Run{v: v, nw: nw, topics: topics, subs: subs, cancelSubs: cancelSubs, n: nw.Len()}
}

func (v *q6Variant) onTimeout(*testing.T, int, int) {
	// No A-specific diagnostics; the driver reports and fails censored cells itself.
}

func (v *q6Variant) report(t *testing.T, s diffusionStats) {
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
	if v.groupPush {
		t.Logf("        group-push: %d (message, peer) pushes vetoed", v.groupVetoes.Load())
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
		if i == 0 {
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

// q6Run is variant A's live cell state.
type q6Run struct {
	v          *q6Variant
	nw         *simNetwork
	topics     []*pubsub.Topic
	subs       []*pubsub.Subscription
	cancelSubs func()
	n          int
}

func (r *q6Run) settle(t *testing.T, tracers []*recordingTracer) string {
	// Assert the mesh rather than trusting the sleep inside the join, and record the sizes: they
	// are the evidence that connectivity degree and mesh degree now differ, which is what makes
	// non-mesh peers exist.
	return summariseMesh(awaitMesh(t, tracers, false))
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
	for len(seen) < arm.completeAt() {
		msg, err := sub.Next(ctx)
		if err != nil {
			return false
		}
		if _, ok := arm.known[digestOf(msg.Data)]; ok {
			seen[digestOf(msg.Data)] = true
			if v.stopPull {
				v.gate(idx).delivered(msg.ID, len(seen))
				if v.stopPullH > 0 {
					v.deferral(idx).Replay() // a slot freed: re-offer what the cap declined
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
	return true
}

func (r *q6Run) watch(ctx context.Context, wg *sync.WaitGroup, start time.Time, signal func(int, time.Duration)) {
	for i := 1; i < r.n; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			if r.v.watchNode(ctx, idx, r.nw.Pubsubs[idx], r.subs[idx]) {
				signal(idx, time.Since(start))
			}
		}(i)
	}
}

func (r *q6Run) publish(ctx context.Context, t *testing.T) error {
	arm := r.v.arm
	omit := failPublishOmit(t, len(arm.msgs))
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
	if err := r.v.publishFrom(ctx, r.topics[0], r.nw.Pubsubs[0], omit); err != nil {
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
	if os.Getenv("SEGMENT_PUBLISH_MODE") == "fcfs" {
		for mi, m := range v.arm.msgs {
			if omit[mi] {
				continue
			}
			if err := topic.Publish(ctx, m); err != nil {
				return err
			}
		}
		return nil
	}
	var batch pubsub.MessageBatch
	for mi, m := range v.arm.msgs {
		if omit[mi] {
			continue
		}
		if err := topic.AddToBatch(ctx, &batch, m); err != nil {
			return err
		}
	}
	return ps.PublishBatch(&batch)
}
