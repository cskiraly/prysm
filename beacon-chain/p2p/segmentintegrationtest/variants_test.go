package segmentintegrationtest

// Variant S, stage 1: segments as partial-message parts on one topic, diffused against
// standing wants.
//
// The same cell variant B runs -- same payload, same segmentation, same mesh, same driver --
// with the request half of the exchange removed. A want bitmap is a standing order: a peer is
// assumed to want everything until its first metadata says otherwise, so the forwarding
// decision is `mine ∧ wanted ∧ ¬theirs` evaluated when a publish pass runs, and nothing waits
// on a round trip. See beacon-chain/p2p/scheduleddiffusion.
//
// Run it beside `SEGMENT_POLICIES=none` B, which is the same machinery with the round trip in
// it: the pair is the stage's known-answer check, `got − heard` at the median.
// SEGMENT_S_NORMAL_LANE runs the second arm, standalone metadata behind queued data instead of
// in the urgent lane, which prices the lane rather than testing a different design.

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/OffchainLabs/prysm/v7/beacon-chain/p2p/scheduleddiffusion"
	"github.com/OffchainLabs/prysm/v7/beacon-chain/verification/segmentauth"
	"github.com/OffchainLabs/prysm/v7/config/params"
	"github.com/OffchainLabs/prysm/v7/consensus-types/primitives"
	"github.com/OffchainLabs/prysm/v7/container/segments"
	"github.com/OffchainLabs/prysm/v7/testing/require"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/sirupsen/logrus"
)

// variantSNode is one node's variant S machinery.
type variantSNode struct {
	broadcaster *scheduleddiffusion.Broadcaster
	// done closes when this node has reassembled the envelope: every group, when the payload
	// travels as several groups on several topics (SEGMENT_B_GROUPS).
	done      chan struct{}
	once      sync.Once
	remaining atomic.Int32 // groups still to reassemble before done closes
}

// newVariantSNodes builds a broadcaster per node. The authenticator is the real one, as in B.
//
// No withholding row: S1 has no serve decision to withhold, since a peer's want is standing
// rather than asked for, and a node that simply stops passing is F1 for a later stage.
func newVariantSNodes(t *testing.T, n int, groups int) []*variantSNode {
	t.Helper()
	logger := bLogger()
	out := make([]*variantSNode, n)
	for i := range out {
		out[i] = newVariantSNode(logger, groups)
	}
	return out
}

// newVariantSNode is one node's broadcaster on the cell's knobs; groups is how many envelopes
// complete it.
func newVariantSNode(logger *logrus.Logger, groups int) *variantSNode {
	node := &variantSNode{
		done: make(chan struct{}),
		broadcaster: scheduleddiffusion.New(context.Background(), logger, scheduleddiffusion.Config{
			// An incomplete group re-runs its pass this often. There is no claim to expire
			// and no timeout to wait out, so this only bounds how long a dropped RPC goes
			// unnoticed; B's knob and B's default, which is the package default too.
			RetryInterval: envDuration("SEGMENT_RETRY_INTERVAL_MS", 4*defaultLatency),
			// Per-peer batching window for standalone metadata. Zero sends every change
			// immediately, which is the default the known-answer check is read at.
			AnnounceWindow: envDuration("SEGMENT_S_ANNOUNCE_MS", 0),
			// Standalone updates behind queued data instead of in the urgent lane. Off is
			// the design; on is the arm that prices the lane.
			NormalLaneUpdates: os.Getenv("SEGMENT_S_NORMAL_LANE") != "",
			// Every negotiated peer in the state map instead of the mesh: the meshless arm,
			// a flood without dispatch-time decisions. Off is tier 1's forwarding set.
			ForwardBeyondMesh: os.Getenv("SEGMENT_S_ALL_PEERS") != "",
			// How long a push is trusted against the peer's own want before it is read as
			// lost and made sendable again. Must cover a latency plus the queue.
			PushGrace: envDuration("SEGMENT_S_PUSH_GRACE_MS", time.Second),
			// Snappy each segment inside the part, as the gossip encoder does on the
			// ordinary path. B's knob, same meaning.
			CompressSegments: os.Getenv("SEGMENT_PARTIAL_COMPRESS") != "",
		}),
	}
	node.remaining.Store(int32(max(groups, 1))) // lint:ignore uintcast -- a small env count
	return node
}

// TestVariantSDiffusion is the end-to-end claim for variant S stage 1.
func TestVariantSDiffusion(t *testing.T) {
	sizes := []int{30}
	if os.Getenv("SEGMENT_SLOW_TESTS") != "" {
		sizes = []int{30, 150, 500}
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
	cell := sCellFromEnv(t)
	for _, n := range sizes {
		t.Run(fmt.Sprintf("n=%d/%s", n, cell.armSuffix()), func(t *testing.T) {
			runDiffusion(t, diffusionParams{
				n:        n,
				rate:     defaultRate,
				latency:  defaultLatency,
				deadline: failureDeadline(t),
				seed:     meshGraphSeed(t),
			}, cell.variant(t, n))
		})
	}
}

// sCell is what the environment says about a variant S cell: the segment size, the payload, the
// group count and the lane. No policies, replications, parities or push divisor -- S1 has one
// rule and the knobs that remain are the substrate's, not the strategy's.
type sCell struct {
	segmentSize int
	payload     []byte
	groups      int
	slot        primitives.Slot
	normalLane  bool
	allPeers    bool
}

// armSuffix is the arm's short name: the stage, then each knob that is not at its default.
func (c sCell) armSuffix() string {
	s := "s1"
	if c.normalLane {
		s += "/normal-lane"
	}
	if c.allPeers {
		s += "/all-peers"
	}
	return s
}

// sCellFromEnv reads the cell's knobs. params.SetupTestConfigCleanup must have run.
func sCellFromEnv(t *testing.T) sCell {
	t.Helper()
	c := sCell{segmentSize: segmentSizeBytes(t), slot: primitives.Slot(2048)}
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
	c.groups = envCount("SEGMENT_B_GROUPS", 1)
	if c.groups < 1 {
		t.Fatalf("bad SEGMENT_B_GROUPS %d (want >= 1)", c.groups)
	}
	c.normalLane = os.Getenv("SEGMENT_S_NORMAL_LANE") != ""
	c.allPeers = os.Getenv("SEGMENT_S_ALL_PEERS") != ""
	// Seed 11, as variant B's: the two arms must diffuse the same bytes or their copies are
	// not on the same basis.
	c.payload = mainnetLikePayload(payloadLen, 11)
	return c
}

// variant is the cell's variant S at n nodes: the payload cut into groups and each group's
// segment messages. Plain groups only -- a coded group is a parity knob S1 does not have.
func (c sCell) variant(t *testing.T, n int) *variantS {
	t.Helper()
	parts := splitPayload(c.payload, c.groups)
	msgsByGroup := make([][]*segments.SegmentMessage, 0, c.groups)
	wireBytes := 0
	for _, part := range parts {
		msgs := segmentMessagesFor(t, nil, c.slot, part, c.segmentSize, 0)
		require.Equal(t, true, len(msgs) > 1)
		msgsByGroup = append(msgsByGroup, msgs)
		for _, m := range msgs {
			enc, err := m.Marshal()
			require.NoError(t, err)
			wireBytes += len(enc)
		}
	}
	return &variantS{
		n:           n,
		unit:        c.segmentSize,
		wireBytes:   wireBytes,
		normalLane:  c.normalLane,
		slot:        c.slot,
		payload:     c.payload,
		groups:      c.groups,
		parts:       parts,
		msgs:        msgsByGroup[0],
		msgsByGroup: msgsByGroup,
	}
}

type variantS struct {
	unit        int // the segment size
	wireBytes   int // the groups' segment messages, encoded
	n           int
	slot        primitives.Slot
	payload     []byte
	groups      int                          // SEGMENT_B_GROUPS: partial messages on this many topics
	parts       [][]byte                     // the payload cut into groups; one part when groups == 1
	msgs        []*segments.SegmentMessage   // the first group's messages (the single group's, usually)
	msgsByGroup [][]*segments.SegmentMessage // every group's messages, in topic order
	nodes       []*variantSNode              // built lazily on the first pubsubOpts call, inside the bubble
	normalLane  bool                         // standalone updates in the ordinary lane
}

// arm is the label every line and the diffusion matrix carry.
func (v *variantS) arm() string {
	if v.normalLane {
		return "variantS/s1/normal-lane"
	}
	return "variantS/s1"
}

func (v *variantS) name() string { return v.arm() }

// wireForm: every group's messages, the required count summed, the segment size, the encoded
// segment messages' bytes; no gossip message ids on this path.
func (v *variantS) wireForm() wireForm {
	w := wireForm{unit: v.unit, total: v.wireBytes}
	for _, msgs := range v.msgsByGroup {
		w.msgs += len(msgs)
		w.need += int(msgs[0].Descriptor.Required()) // lint:ignore uintcast -- bounded by MaxSegments.
	}
	return w
}

// partialObserve and segKeys make S visible to the diffusion instrument, exactly as they do for
// B: the segments ride in rpc.Partial, so neither the message-id path nor DeliverMessage sees
// them. Only the have bitmap counts as heard -- a standing want defaults to everything, so want
// bits would report every segment heard the moment a peer connects.
func (v *variantS) partialObserve() partialObserveFn { return partialSegmentObserver() }

// segKeys is every group's segments in index order, groups in topic order -- the same summing
// convention wireForm uses, so a multi-group cell reads back whole rather than as group 0.
func (v *variantS) segKeys() []string {
	out := make([]string, 0, len(v.msgsByGroup)*len(v.msgs))
	for _, msgs := range v.msgsByGroup {
		h, err := segments.HasherByID(msgs[0].Descriptor.HashID)
		if err != nil {
			return nil
		}
		out = append(out, partialSegKeys(msgs[0].Descriptor.GroupID(h), len(msgs))...)
	}
	return out
}

// reportDiffusion writes the cell's per-segment curves, keyed on the partial path.
func (v *variantS) reportDiffusion(t *testing.T, s diffusionStats) {
	out := diffusionOut()
	if !diffusionCurves() || out == "" {
		return
	}
	keys := v.segKeys()
	if len(keys) == 0 {
		t.Logf("      per-segment curves: no keys for this arm; not written")
		return
	}
	need := 0
	for _, msgs := range v.msgsByGroup {
		need += int(msgs[0].Descriptor.Required()) // lint:ignore uintcast -- bounded by MaxSegments.
	}
	mx := diffusionMatrix{Arm: v.arm(), N: s.n, Segments: len(keys), Need: need}
	for _, tr := range s.tracers {
		got, heard := segmentDiffusion(tr, wireArm{}, keys, s.startAt)
		mx.FirstGot = append(mx.FirstGot, got)
		mx.Heard = append(mx.Heard, heard)
	}
	if err := writeDiffusionMatrix(out, mx); err != nil {
		t.Fatalf("writing the diffusion matrix: %v", err)
	}
	t.Logf("      per-segment curves: %d nodes x %d segments -> %s", len(mx.FirstGot), mx.Segments, out)
}

// topicNames is the envelope topic for a single group, or one topic per group.
func (v *variantS) topicNames() []string {
	if v.groups <= 1 {
		return []string{envelopeTopic()}
	}
	out := make([]string, v.groups)
	for g := range out {
		out[g] = fmt.Sprintf("%s/g%d", envelopeTopic(), g)
	}
	return out
}

// isPart reports whether a reassembled envelope is one of the payload's groups.
func (v *variantS) isPart(envelope []byte) bool {
	for _, p := range v.parts {
		if bytes.Equal(p, envelope) {
			return true
		}
	}
	return false
}

func (v *variantS) pubsubOpts(t *testing.T, i int) []pubsub.Option {
	// Build the broadcasters on first use. The driver calls this while it constructs the network,
	// which is inside the synctest bubble -- and the broadcasters MUST be constructed there: New
	// captures a context whose Done channel the loop selects on, and a channel created outside the
	// bubble never counts as a durable block, so synctest could not advance virtual time and every
	// cell would crawl in real time. The driver adds the tracer to what AppendPubSubOpts returns.
	if v.nodes == nil {
		v.nodes = newVariantSNodes(t, v.n, v.groups)
	}
	return v.nodes[i].broadcaster.AppendPubSubOpts(nil)
}

func (v *variantS) setup(t *testing.T, nw *simNetwork, _ diffusionParams) diffusionRun {
	n := nw.Len()
	topics := v.topicNames()
	r := &sRun{v: v, nw: nw, topic: topics[0], topics: topics, n: n}
	for i, node := range v.nodes {
		node.broadcaster.SetSelf(nw.Hosts[i].ID())
	}
	for i, node := range v.nodes {
		v.startNode(t, node, i == 0, &r.loopWg)
	}
	for i, ps := range nw.Pubsubs {
		r.subs = append(r.subs, v.joinNode(t, ps, v.nodes[i])...)
	}
	return r
}

// startNode starts a node's broadcaster loop on wg, with the cell's commitment and the
// completion rule: one envelope callback per group, done when every group is in. The loop is
// stopped inside the bubble by the run's cancel: a goroutine still running when the bubble
// exits is a deadlock panic, not a leak warning.
func (v *variantS) startNode(t *testing.T, node *variantSNode, isPublisher bool, wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := node.broadcaster.Start(scheduleddiffusion.Callbacks{
			Auth: segmentauth.New(commitmentForGroups(t, v.msgsByGroup)),
			OnEnvelope: func(envelope []byte) error {
				require.Equal(t, true, v.isPart(envelope))
				if !isPublisher && node.remaining.Add(-1) == 0 {
					node.once.Do(func() { close(node.done) })
				}
				return nil
			},
		})
		require.NoError(t, err)
	}()
}

// joinNode joins the group topics on ps requesting partial messages -- the option that makes
// gossipsub skip this peer on the full-message path and route segments to it instead -- drains
// each subscription (an unread one fills and reads exactly like a slow link) and serves the
// topics from the node's broadcaster. Returns the subscriptions for the teardown.
func (v *variantS) joinNode(t *testing.T, ps *pubsub.PubSub, node *variantSNode) []*pubsub.Subscription {
	t.Helper()
	var subs []*pubsub.Subscription
	for _, topic := range v.topicNames() {
		th, err := ps.Join(topic, pubsub.RequestPartialMessages())
		require.NoError(t, err)
		sub, err := th.Subscribe(pubsub.WithBufferSize(subscriptionBuffer))
		require.NoError(t, err)
		// Not on any wait group -- it exits when the subscription is cancelled, which synctest
		// still waits for.
		go func() {
			for {
				if _, err := sub.Next(context.Background()); err != nil {
					return
				}
			}
		}()
		subs = append(subs, sub)
		node.broadcaster.ServeTopic(topic)
	}
	return subs
}

// publishFrom publishes every group at once from the publisher's broadcaster, in topic order.
func (v *variantS) publishFrom(node *variantSNode) error {
	for g, topic := range v.topicNames() {
		if err := node.broadcaster.Publish(topic, v.msgsByGroup[g]); err != nil {
			return err
		}
	}
	return nil
}

func (v *variantS) onTimeout(t *testing.T, completed, n int) {
	// Dump a stuck node's counters: a stall's diagnosis starts with whether it was starved of
	// metadata, of admitted sends, or of verify budget, and by the next run the state is gone.
	for i := 1; i < n; i++ {
		select {
		case <-v.nodes[i].done:
		default:
			// Diagnostic only -- the driver fails the test after the censored report.
			t.Logf("node %d never reassembled the envelope; %d/%d completed; stuck counters %+v",
				i, completed, n-1, v.nodes[i].broadcaster.Counters())
			return
		}
	}
}

func (v *variantS) report(t *testing.T, s diffusionStats) {
	v.reportDiffusion(t, s)
	// Censored cells still report: onTimeout already failed the test.
	var passes, actions, sent, received, duplicate int
	var metaSent, metaHeld, expired, notAdmitted int
	var rejected, queueDropped int
	for _, node := range v.nodes {
		c := node.broadcaster.Counters()
		passes += c.PublishPasses
		actions += c.PeerActions
		sent += c.SegmentsSent
		received += c.SegmentsReceived
		duplicate += c.SegmentsDuplicate
		metaSent += c.MetadataSent
		metaHeld += c.MetadataHeldBack
		expired += c.PushesExpired
		notAdmitted += c.SendsNotAdmitted
		rejected += c.SegmentsRejected
		queueDropped += c.VerifyQueueDropped
	}
	// The headline number: how many copies of the payload a node takes in. Normalised by the
	// *required* count, so it stays comparable across arms, and summed over groups.
	//
	// Arrivals, not additions: this package counts a segment it already held as
	// SegmentsDuplicate and leaves it out of SegmentsReceived, where variant B counts every
	// arrival in SegmentsReceived and never increments its duplicate counter. Copies is an
	// arrival figure in both arms or it is not a comparison.
	arrived := received + duplicate
	required, planned := 0, 0
	for _, msgs := range v.msgsByGroup {
		required += int(msgs[0].Descriptor.Required()) // lint:ignore uintcast -- bounded by MaxSegments.
		planned += len(msgs)
	}
	copiesPerNode := float64(arrived) / float64(s.n) / float64(required)
	// Payload bytes for one whole envelope, so rx/node can be read as copies. The payload's
	// length, not the sum over messages.
	payloadBytes := len(v.payload)
	t.Logf("arm=%-22s n=%d K=%d/%d completion %v  %s  segments sent %d; arrived %d = received %d + duplicate %d (%.2f copies/node)",
		v.arm(), s.n, required, planned, s.virt.Round(time.Millisecond),
		completionStats(s.compTimes, s.n-1, s.deadline),
		sent, arrived, received, duplicate, copiesPerNode)
	// Payload and bitmap signalling on separate lines because they are separate bases. The
	// copies figure is payload only; the bitmap bytes are this variant's control cost and
	// belong beside variant A's ctrl/node, not inside its copies.
	t.Logf("        partial payload: tx %d B over %d RPCs, rx %d B over %d RPCs (%.2fx payload rx/node); publisherTx %d B; dropped RPCs %d; full-gossip rx/node %d B; groups %d",
		s.partialTx, s.partialTxRPCs, s.partialRx, s.partialRPCs,
		float64(s.partialRx)/float64(s.n)/float64(payloadBytes), s.partialPublisherTx, s.drops, s.rxBytes/s.n, len(v.msgsByGroup))
	t.Logf("        bitmap signalling: tx %d B, rx %d B (%d B/node rx); gossip control rx/node %d B",
		s.partialMetaTx, s.partialMetaRx, s.partialMetaRx/s.n, s.ctrlRx/s.n)
	// The stage's own counters. SendsNotAdmitted is the one to watch: an action the peer's
	// queue refused leaves the state untouched by design, so a non-zero count at a small n
	// says the queues are the binding constraint rather than the rule being measured.
	t.Logf("        S counters: passes %d, peer actions %d, segments sent %d; updates sent %d (held back %d); pushes expired %d; sends not admitted %d; duplicates received %d; rejected %d; verify-queue dropped %d",
		passes, actions, sent, metaSent, metaHeld, expired, notAdmitted, duplicate, rejected, queueDropped)

	// Nothing here is policy-conditional: a want is standing, so if anything moved at all the
	// pass ran. The driver's deadline logic owns the censoring verdict.
	require.Equal(t, true, sent > 0)
}

// sRun is variant S's live cell state.
type sRun struct {
	v      *variantS
	nw     *simNetwork
	topic  string   // the first (usually only) topic
	topics []string // one per group
	subs   []*pubsub.Subscription
	loopWg sync.WaitGroup
	n      int
}

func (r *sRun) settle(t *testing.T, tracers []*recordingTracer) string {
	var meshSizes []int
	if len(r.topics) > 1 {
		meshSizes = awaitMeshMany(t, tracers, len(r.topics), false)
	} else {
		meshSizes = awaitMesh(t, tracers, false)
	}
	t.Logf("mesh settled: connectivity degree %d, mesh sizes %v",
		connectivityDegree(t, r.n), summariseMesh(meshSizes))
	return summariseMesh(meshSizes)
}

func (r *sRun) cancel() {
	// Order matters and mirrors B's teardown: cancel subscriptions (unblocks the drain
	// goroutines), stop the broadcasters (ends their loops), then drain the loop goroutines. The
	// driver's own wg.Wait then drains the watchers, and stop() tears down the network.
	for _, s := range r.subs {
		s.Cancel()
	}
	for _, node := range r.v.nodes {
		node.broadcaster.Stop()
	}
	r.loopWg.Wait()
}

func (r *sRun) watch(ctx context.Context, wg *sync.WaitGroup, start time.Time, signal func(int, time.Duration)) {
	// The broadcaster's OnEnvelope callback closes each node's done channel on reassembly; a
	// per-node watcher stamps that instant and hands it to the driver's signal.
	for i := 1; i < r.n; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			select {
			case <-r.v.nodes[idx].done:
				signal(idx, time.Since(start))
			case <-ctx.Done():
			}
		}(i)
	}
}

func (r *sRun) publish(_ context.Context, _ *testing.T) error {
	return r.v.publishFrom(r.v.nodes[0])
}
