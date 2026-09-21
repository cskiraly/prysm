package segmentintegrationtest

// Variant C: one gossipsub topic per segment index, DAS-style.
//
// Variant A puts every segment on one topic, so all K segments share a single mesh and every
// link of that mesh carries all of them. Variant C gives each segment index its own topic;
// gossipsub then samples an independent mesh per topic, so segments diffuse along different
// trees and the load spreads over many more links. By default there is no custody: every node
// needs the whole payload, so every node subscribes to every topic.
//
// What this isolates: per-segment path diversity, with everything else exactly variant A —
// ordinary gossip messages, eager push, IDONTWANT suppression. Expected per-segment
// duplication is unchanged (~D copies per node per segment); what may change is completion
// (independent trees remove head-of-line sharing on mesh links) and load variance. The cost
// axis: T meshes' worth of heartbeat, GRAFT and IHAVE traffic.
//
// SEGMENT_C_PHASE_R > 0 additionally enables the forked gossipsub's phase forwarding on all
// segment topics, which composes the two mechanisms (per-topic meshes x push-decay).
//
// SEGMENT_PARITY > 0 codes the group (Reed-Solomon, K systematic + parity, one topic per coded
// index) and a node completes at any K distinct segments. Every node still subscribes to all
// N topics, so the byte cost sits at the N/K floor while the per-index meshes stay as healthy
// as in the uncoded arm.
//
// SEGMENT_SUB_EXTRA (comma list of R values) adds custody on top of a coded group -- the
// FullDAS shape, formerly variant D. Each node other than the publisher subscribes to a
// deterministic S-of-N subset, S = K + R, receives only those topics' coded segments by gossip
// and reconstructs when any K distinct ones have arrived. Nothing is ever pulled across a
// subscription boundary, which is why custody needs the code: at K = N the only subset that
// completes is every topic, i.e. plain variant C. R is the redundancy margin: at R = 0 every
// one of a node's topics must deliver, and each extra R tolerates one slow or failed topic at
// the price of S/K times the gossip bytes. Per-topic meshes can then only form between
// connected co-subscribers -- degree*S/N expected neighbours per topic, with a binomial tail
// below Dlo -- so thin meshes are a property of custody at this degree, not a harness failure,
// and the mesh wait uses a loose floor and reports what formed rather than asserting the
// [Dlo, Dhi] band.
//
// The shared cell driver lives in driver_test.go; this file supplies only variant C's strategy.

import (
	"context"
	"fmt"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/OffchainLabs/prysm/v7/config/params"
	"github.com/OffchainLabs/prysm/v7/consensus-types/primitives"
	"github.com/OffchainLabs/prysm/v7/crypto/bls"
	"github.com/OffchainLabs/prysm/v7/testing/require"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
)

// segmentTopicIndexed names the topic for one segment index.
func segmentTopicIndexed(i int) string {
	return fmt.Sprintf("%s/%d", segmentTopic(), i)
}

// awaitMeshMany is awaitMesh for a node subscribed to T topics: the tracer's mesh count is
// topic-agnostic, so the settled band scales to [T*Dlo, T*Dhi].
func awaitMeshMany(t *testing.T, tracers []*recordingTracer, topicCount int, wallClock bool) []int {
	t.Helper()
	gsp := meshParams()
	lo, hi := topicCount*gsp.Dlo, topicCount*gsp.Dhi
	// Two properties this wait must have, both learned the hard way at D=4 (see notes: a cell
	// burned 9.7 CPU-hours without reaching diffusion).
	//
	// Tolerate stragglers. Demanding that *every* node sit in [T*Dlo, T*Dhi] gets steadily
	// harsher as D falls: the floor is T*Dlo while the mesh converges on T*D, so the slack is
	// T*(D-Dlo) -- two slots per topic at D=8 but only one at D=4. A single topic transiently
	// one peer short on any one of 500 nodes then blocks the whole wait, which would make the
	// settle criterion, not the network, decide a D sweep.
	//
	// Fail fast. The old deadline was topicCount*meshSettleTimeout: 192 virtual minutes at 64
	// topics, and since every virtual second costs wall-clock proportional to nodes*topics, a
	// mesh that will never settle grinds for hours instead of reporting in minutes. A mesh that
	// has not formed within a few dozen heartbeats is not going to.
	// 1% was too tight at two topics: a 500-node cell failed with 6 stragglers against a
	// tolerance of 5, at 98.8% settled -- the criterion deciding the cell rather than the network.
	// Scale the allowance with the number of meshes each node has to form, since every extra topic
	// is another chance for one node to sit a peer short at the instant we look -- but cap it at a
	// tenth of the nodes. Uncapped, 1% per topic reached 640 of 500 nodes at 128 topics and the
	// wait returned at its first poll with no mesh formed anywhere (Q75's c64rs cell).
	tolerated := max(1, min(topicCount*len(tracers)/100, len(tracers)/10))
	deadline := time.Now().Add(2 * meshSettleTimeout)
	sizes := make([]int, len(tracers))
	for {
		settled := 0
		for i, tr := range tracers {
			sizes[i] = tr.MeshSlots()
			if sizes[i] >= lo && sizes[i] <= hi {
				settled++
			}
		}
		if settled >= len(tracers)-tolerated {
			if out := len(tracers) - settled; out > 0 {
				t.Logf("mesh settled with %d/%d nodes outside [%d,%d] (tolerated %d)",
					out, len(tracers), lo, hi, tolerated)
			}
			return sizes
		}
		if time.Now().After(deadline) {
			// Publish anyway, as the custody wait always has. A mesh that has not formed within
			// a few dozen heartbeats is not going to, and a deployed network would not wait for
			// it either: the diffusion on the mesh that did form is the measurement, with the
			// shortfall on record. Until 2026-09-08 this was a t.Fatalf; on one 500-node draw at
			// 64 topics (experiments Q77, seed 13, 20% datacenter nodes) it left two Figure D
			// points at nine seeds because ~56 nodes sat under Dlo in their meshes.
			lo6, med, hi6 := meshSpread(sizes)
			t.Logf("mesh unsettled after %v, publishing anyway: %d/%d nodes in [%d,%d] "+
				"(D=%d Dlo=%d Dhi=%d, %d topics); observed slots/node min %d median %d max %d "+
				"= %.2f slots/topic at the median",
				2*meshSettleTimeout, settled, len(tracers), lo, hi,
				gsp.D, gsp.Dlo, gsp.Dhi, topicCount, lo6, med, hi6, float64(med)/float64(topicCount))
			return sizes
		}
		time.Sleep(meshPollInterval)
		settle(wallClock)
	}
}

// awaitCustodyMesh is the mesh wait for custody cells. Meshes here are thin by construction
// (degree * S/N co-subscribers per topic), so it waits on a loose floor -- half of Dlo per
// subscribed topic, on every node but the publisher -- and reports what formed rather than
// asserting the [Dlo, Dhi] band.
func awaitCustodyMesh(t *testing.T, tracers []*recordingTracer, subscriptions int, wallClock bool) []int {
	t.Helper()
	floor := subscriptions * meshParams().Dlo / 2
	deadline := time.Now().Add(meshSettleTimeout)
	sizes := make([]int, len(tracers))
	for {
		settled := 0
		for i, tr := range tracers {
			sizes[i] = tr.MeshSlots()
			if i > 0 && sizes[i] >= floor {
				settled++
			}
		}
		if settled == len(tracers)-1 {
			return sizes
		}
		if time.Now().After(deadline) {
			t.Logf("custody meshes: %d/%d nodes below the floor of %d slots after %v; publishing anyway",
				len(tracers)-1-settled, len(tracers)-1, floor, meshSettleTimeout)
			return sizes
		}
		time.Sleep(meshPollInterval)
		settle(wallClock)
	}
}

// meshSpread returns min, median and max of sizes without mutating the caller's slice.
func meshSpread(sizes []int) (int, int, int) {
	if len(sizes) == 0 {
		return 0, 0, 0
	}
	s := append([]int(nil), sizes...)
	sort.Ints(s)
	return s[0], s[len(s)/2], s[len(s)-1]
}

// custodyTopics returns the S topic indices node idx subscribes to: the first S of 0..N-1
// ranked by an avalanched hash of (seed, node, topic). Deterministic, balanced in
// expectation, and different per node, which is what makes the union cover every topic.
func custodyTopics(seed uint64, node, n, s int) []int {
	idxs := make([]int, n)
	for i := range idxs {
		idxs[i] = i
	}
	rank := func(topic int) uint64 {
		x := seed ^ uint64(node)<<32 ^ uint64(topic)
		x ^= x >> 30
		x *= 0xbf58476d1ce4e5b9
		x ^= x >> 27
		x *= 0x94d049bb133111eb
		x ^= x >> 31
		return x
	}
	sort.Slice(idxs, func(a, b int) bool { return rank(idxs[a]) > rank(idxs[b]) })
	out := idxs[:s]
	sort.Ints(out)
	return out
}

// TestVariantCDiffusion drives the topic-per-segment variant on the standard mesh.
func TestVariantCDiffusion(t *testing.T) {
	sizes := []int{30}
	if os.Getenv("SEGMENT_SLOW_TESTS") != "" {
		sizes = []int{30, 500}
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
	params.SetupTestConfigCleanup(t)
	cell := cCellFromEnv(t)
	for _, n := range sizes {
		for _, extra := range cell.custodies() {
			v := cell.variant(extra)
			t.Run(cell.name(n, v), func(t *testing.T) {
				runDiffusion(t, diffusionParams{
					n:        n,
					rate:     defaultRate,
					latency:  defaultLatency,
					deadline: failureDeadline(t),
					seed:     meshGraphSeed(t),
				}, v)
			})
		}
	}
}

// cCell is what the environment says about a variant C cell: the group (plain, or coded with
// SEGMENT_PARITY), one topic per index, the phase push degree and the custody margins to sweep.
// Factored out of TestVariantCDiffusion so the Shadow node builds the same group from the same
// knobs.
type cCell struct {
	arm        wireArm
	unit       int // the segment size
	topicCount int
	parity     int
	phaseR     int
	extras     []int // SEGMENT_SUB_EXTRA: custody margins R, S = K + R per node
}

// cCellFromEnv reads the cell's knobs and builds the group. params.SetupTestConfigCleanup must
// have run.
func cCellFromEnv(t *testing.T) cCell {
	t.Helper()
	var c cCell
	payloadLen := 1 << 19
	if os.Getenv("SEGMENT_SLOW_TESTS") != "" {
		payloadLen = 1 << 20
	}
	if v := os.Getenv("SEGMENT_PAYLOAD_BYTES"); v != "" {
		k, err := strconv.Atoi(v)
		if err != nil {
			t.Fatalf("bad SEGMENT_PAYLOAD_BYTES %q: %v", v, err)
		}
		payloadLen = k
	}
	if v := os.Getenv("SEGMENT_C_PHASE_R"); v != "" {
		k, err := strconv.Atoi(v)
		if err != nil || k <= 0 {
			t.Fatalf("bad SEGMENT_C_PHASE_R %q", v)
		}
		c.phaseR = k
	}
	// Custody margins to sweep: S = K + R per node. Absent, every node joins every topic.
	if v := os.Getenv("SEGMENT_SUB_EXTRA"); v != "" {
		for _, f := range strings.Split(v, ",") {
			k, err := strconv.Atoi(strings.TrimSpace(f))
			if err != nil || k < 0 {
				t.Fatalf("bad SEGMENT_SUB_EXTRA %q", v)
			}
			c.extras = append(c.extras, k)
		}
	}
	sk, err := bls.RandKey()
	require.NoError(t, err)
	payload := mainnetLikePayload(payloadLen, 11)
	c.unit = segmentSizeBytes(t)
	c.arm = segmentedArm(t, payload, c.unit, sk, primitives.Slot(2048))
	if v := os.Getenv("SEGMENT_PARITY"); v != "" {
		// Coded topics: K+parity indices on the wire, one topic each, any K completing a node.
		par, err := strconv.Atoi(v)
		if err != nil || par <= 0 {
			t.Fatalf("bad SEGMENT_PARITY %q", v)
		}
		c.arm = codedArm(t, payload, c.unit, par, sk, primitives.Slot(2048))
		c.parity = par
	}
	c.topicCount = len(c.arm.msgs)
	if len(c.extras) > 0 && c.parity == 0 {
		t.Fatalf("SEGMENT_SUB_EXTRA needs a coded group (SEGMENT_PARITY > 0): at K = N the only custody set that completes is every topic, which is plain variant C")
	}
	for _, extra := range c.extras {
		if extra >= c.parity {
			t.Fatalf("R=%d must stay below the parity margin N-K=%d, or S exceeds what coding can excuse", extra, c.parity)
		}
	}
	return c
}

// custodies is one cell per custody margin; a single cell with no custody when none is asked for.
func (c cCell) custodies() []*int {
	if len(c.extras) == 0 {
		return []*int{nil}
	}
	out := make([]*int, len(c.extras))
	for i := range c.extras {
		out[i] = &c.extras[i]
	}
	return out
}

// variant is the cell's variant C at one custody margin (nil: every node joins every topic).
func (c cCell) variant(extra *int) *variantC {
	v := &variantC{arm: c.arm, unit: c.unit, topicCount: c.topicCount, phaseR: c.phaseR}
	if extra != nil {
		v.custody, v.extra = true, *extra
	}
	return v
}

// name is the subtest's name for one variant at n nodes.
func (c cCell) name(n int, v *variantC) string {
	name := fmt.Sprintf("n=%d/T=%d", n, c.topicCount)
	if c.arm.need > 0 {
		name = fmt.Sprintf("%s/K=%d", name, c.arm.need)
	}
	if v.custody {
		name = fmt.Sprintf("%s/S=K+%d", name, v.extra)
	}
	if c.phaseR > 0 {
		name = fmt.Sprintf("%s/phase-r=%d", name, c.phaseR)
	}
	return name
}

// variantC is the topic-per-segment strategy: every node subscribes to every segment topic,
// or under custody to an S-of-N subset of a coded group.
type variantC struct {
	unit       int // the segment size
	arm        wireArm
	topicCount int
	phaseR     int
	// custody restricts every node but the publisher to S = K + extra topics.
	custody bool
	extra   int
}

func (v *variantC) name() string { return "variantC" }

func (v *variantC) wireForm() wireForm {
	return wireForm{msgs: len(v.arm.msgs), need: v.arm.completeAt(), unit: v.unit, total: v.arm.total, idBytes: idWidth(v.arm.msgs[0])}
}

// subscriptions is how many topics a node other than the publisher joins.
func (v *variantC) subscriptions() int {
	if v.custody {
		return v.arm.completeAt() + v.extra
	}
	return v.topicCount
}

func (v *variantC) pubsubOpts(t *testing.T, _ int) []pubsub.Option {
	if v.phaseR > 0 {
		return phasePubsubOpts(t, v.phaseR)
	}
	return nil
}

func (v *variantC) setup(t *testing.T, nw *simNetwork, p diffusionParams) diffusionRun {
	n := nw.Len()
	subs := make([][]*pubsub.Subscription, n)
	var pubTopics []*pubsub.Topic
	for i, ps := range nw.Pubsubs {
		topics, ss := v.joinNode(t, ps, i, p.seed)
		subs[i] = ss
		if i == 0 {
			pubTopics = topics
		}
	}
	return &cRun{v: v, nw: nw, subs: subs, pubTopics: pubTopics, n: n}
}

// topicsOf is the topic indices node idx joins: every index for the publisher and, without
// custody, for everyone; a deterministic S-of-N subset under custody.
func (v *variantC) topicsOf(seed uint64, idx int) []int {
	if v.custody && idx > 0 {
		return custodyTopics(seed, idx, v.topicCount, v.subscriptions())
	}
	all := make([]int, v.topicCount)
	for j := range all {
		all[j] = j
	}
	return all
}

// joinNode joins node idx's topics on ps and subscribes to each: the handles and subscriptions
// in topic order (the publisher's handles are what publishFrom needs). The in-process setup
// calls it for every node; the Shadow node, one process per node, once.
func (v *variantC) joinNode(t *testing.T, ps *pubsub.PubSub, idx int, seed uint64) ([]*pubsub.Topic, []*pubsub.Subscription) {
	t.Helper()
	var topics []*pubsub.Topic
	var subs []*pubsub.Subscription
	for _, j := range v.topicsOf(seed, idx) {
		th, err := ps.Join(segmentTopicIndexed(j))
		require.NoError(t, err)
		registerProcValidator(t, ps, segmentTopicIndexed(j))
		sub, err := th.Subscribe(pubsub.WithBufferSize(subscriptionBuffer))
		require.NoError(t, err)
		topics = append(topics, th)
		subs = append(subs, sub)
	}
	return topics, subs
}

// watchNode drains one node's subscriptions, each on its own goroutine on wg, into a shared
// counter of distinct known segments, and calls onDone once the arm's completion count has
// arrived (every one for a plain group, any K for a coded one). The goroutines run until ctx
// ends, so a completed node keeps reading and its subscriptions never overflow.
func (v *variantC) watchNode(ctx context.Context, wg *sync.WaitGroup, subs []*pubsub.Subscription, onDone func()) {
	need := v.arm.completeAt()
	var mu sync.Mutex
	seen := make(map[digest]bool, len(subs))
	for _, sub := range subs {
		wg.Add(1)
		go func(sub *pubsub.Subscription) {
			defer wg.Done()
			for {
				msg, err := sub.Next(ctx)
				if err != nil {
					return
				}
				d := digestOf(msg.Data)
				if _, ok := v.arm.known[d]; !ok {
					continue
				}
				mu.Lock()
				if !seen[d] {
					seen[d] = true
					if len(seen) == need {
						onDone()
					}
				}
				mu.Unlock()
			}
		}(sub)
	}
}

// drainSubs reads subscriptions that nobody watches (the publisher's own) so they do not fill
// and report messages undeliverable, which reads exactly like a slow link.
func drainSubs(ctx context.Context, wg *sync.WaitGroup, subs []*pubsub.Subscription) {
	for _, sub := range subs {
		wg.Add(1)
		go func(sub *pubsub.Subscription) {
			defer wg.Done()
			for {
				if _, err := sub.Next(ctx); err != nil {
					return
				}
			}
		}(sub)
	}
}

// publishFrom publishes the group from the publisher's per-topic handles as one batch, the ids
// in omit left out.
func (v *variantC) publishFrom(ctx context.Context, topics []*pubsub.Topic, ps *pubsub.PubSub, omit map[int]bool) error {
	var batch pubsub.MessageBatch
	for j, m := range v.arm.msgs {
		if omit[j] {
			continue
		}
		if err := topics[j].AddToBatch(ctx, &batch, m); err != nil {
			return err
		}
	}
	return ps.PublishBatch(&batch)
}

func (v *variantC) onTimeout(t *testing.T, completed, n int) {
	// Diagnostic only -- the driver fails the test after the censored report.
	t.Logf("TIMEOUT: only %d/%d completed", completed, n-1)
}

func (v *variantC) report(t *testing.T, s diffusionStats) {
	// Censored cells still report: onTimeout already failed the test.
	custody, mesh := "", s.meshSummary
	if v.custody {
		custody = fmt.Sprintf(" K=%d S=K+%d", v.arm.completeAt(), v.extra)
		mesh = fmt.Sprintf("%s, mean %.0f slots/node after diffusion", mesh, float64(s.meshSlots)/float64(s.n))
	}
	t.Logf("variantC n=%-3d T=%d%s %3d/%d  virtual %9v  %s  dups %6d  IDONTWANT tx %6d  dup-per-idw %5.2f  publisherTx %8dB  rx/node %8dB  ctrl rx/node %6dB  wall %v  [degree %d, mesh-total %s]",
		s.n, v.topicCount, custody, s.completed, s.n-1, s.virt.Round(time.Millisecond),
		completionStats(s.compTimes, s.n-1, s.deadline),
		s.dups, s.idwSent, float64(s.dups)/math.Max(1, float64(s.idwSent)),
		s.publisherTx, s.rxBytes/s.n, s.ctrlRx/s.n, s.wall.Round(time.Millisecond),
		connectivityDegree(t, s.n), mesh)
	if s.drops > 0 {
		t.Logf("*** %d DROPS", s.drops)
	}
}

// cRun is variant C's live cell state.
type cRun struct {
	v         *variantC
	nw        *simNetwork
	subs      [][]*pubsub.Subscription // per node, in topic order; ragged under custody
	pubTopics []*pubsub.Topic          // the publisher's handle per topic index
	n         int
}

func (r *cRun) settle(t *testing.T, tracers []*recordingTracer) string {
	if r.v.custody {
		return summariseMesh(awaitCustodyMesh(t, tracers, r.v.subscriptions(), false))
	}
	return summariseMesh(awaitMeshMany(t, tracers, r.v.topicCount, false))
}

func (r *cRun) cancel() {
	for _, ss := range r.subs {
		for _, s := range ss {
			s.Cancel()
		}
	}
}

func (r *cRun) watch(ctx context.Context, wg *sync.WaitGroup, start time.Time, signal func(int, time.Duration)) {
	for i := 1; i < r.n; i++ {
		node := i
		r.v.watchNode(ctx, wg, r.subs[i], func() { signal(node, time.Since(start)) })
	}
	drainSubs(ctx, wg, r.subs[0])
}

func (r *cRun) publish(ctx context.Context, t *testing.T) error {
	return r.v.publishFrom(ctx, r.pubTopics, r.nw.Pubsubs[0], failPublishOmit(t, len(r.v.arm.msgs)))
}
