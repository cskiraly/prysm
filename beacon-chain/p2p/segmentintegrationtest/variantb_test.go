package segmentintegrationtest

// Variant B end to end: segments as partial-message parts on one topic.
//
// What this establishes, and what it deliberately does not. It establishes that the mechanism
// works over a real simulated network -- negotiation, bitmap exchange, push, pull, reassembly,
// authentication -- and it reports the quantity the design is judged on: bytes received per node.
// It does not yet establish the Q13 result. Two known harness gaps stand in the way: connectivity
// degree still equals D, so there are no non-mesh peers and the announce path to them is untested,
// and the mesh arms have no warm-up so everything here includes QUIC slow start.
//
// The three arms are the policy knob, not three designs. See segmentbroadcaster.Policy.

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
	"testing/synctest"
	"time"

	"github.com/OffchainLabs/prysm/v7/beacon-chain/p2p/segmentbroadcaster"
	"github.com/OffchainLabs/prysm/v7/beacon-chain/verification/segmentauth"
	"github.com/OffchainLabs/prysm/v7/config/params"
	"github.com/OffchainLabs/prysm/v7/consensus-types/primitives"
	"github.com/OffchainLabs/prysm/v7/container/segments"
	"github.com/OffchainLabs/prysm/v7/crypto/bls"
	"github.com/OffchainLabs/prysm/v7/testing/require"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/sirupsen/logrus"
)

// variantBNode is one node's variant B machinery.
type variantBNode struct {
	broadcaster *segmentbroadcaster.Broadcaster
	// done closes when this node has reassembled the envelope: every group, when the payload
	// travels as several groups on several topics (SEGMENT_B_GROUPS).
	done      chan struct{}
	once      sync.Once
	remaining atomic.Int32 // groups still to reassemble before done closes
}

// splitPayload cuts the payload into g contiguous parts of near-equal length, each a group of
// its own with its own commitment and topic. g = 1 is the payload itself.
func splitPayload(payload []byte, g int) [][]byte {
	parts := make([][]byte, 0, g)
	base, extra := len(payload)/g, len(payload)%g
	off := 0
	for i := 0; i < g; i++ {
		n := base
		if i < extra {
			n++
		}
		parts = append(parts, payload[off:off+n])
		off += n
	}
	return parts
}

// commitmentForGroups admits every group of a multi-group payload.
func commitmentForGroups(t *testing.T, groups [][]*segments.SegmentMessage) segmentauth.CommittedGroups {
	t.Helper()
	h, err := segments.HasherByID(segments.HashSHA256)
	require.NoError(t, err)
	want := make(map[string]struct{}, len(groups))
	for _, msgs := range groups {
		want[string(msgs[0].Descriptor.GroupID(h))] = struct{}{}
	}
	return func(g []byte) bool { _, ok := want[string(g)]; return ok }
}

// segmentMessagesFor builds a signed segmentation, in the form variant B publishes.
// A parity above zero builds a Reed-Solomon coded group: any K of the K+parity segments
// reassemble the payload.
func segmentMessagesFor(t *testing.T, sk bls.SecretKey, slot primitives.Slot, payload []byte, segmentSize, parity int) []*segments.SegmentMessage {
	t.Helper()
	h, err := segments.HasherByID(segments.HashSHA256)
	require.NoError(t, err)
	var msgs []*segments.SegmentMessage
	if parity > 0 {
		msgs, err = segments.BuildCodedSegmentMessages(payload, segmentSize, parity, h)
		require.NoError(t, err)
	} else {
		msgs, err = segments.BuildSegmentMessages(payload, segmentSize, h)
		require.NoError(t, err)
	}

	return msgs
}

// newVariantBNodes builds a broadcaster per node and starts each one's loop.
//
// The authenticator is the real one, resolving the anchored block root to the group the
// payload commits to, so a descriptor that would be refused on the network is refused here.
// Nothing in this path is stubbed except the commitment lookup, which stands in for the bid
// field the block will eventually carry.
func newVariantBNodes(t *testing.T, n int, policy segmentbroadcaster.Policy, replication, pushDivisor int, groups int) []*variantBNode {
	t.Helper()
	logger := bLogger()
	withholdSet := failWithholdSet(t, n)
	if len(withholdSet) > 0 {
		t.Cleanup(func() {
			t.Logf("failure exposure: %d withholding nodes (serves withheld are in Counters)", len(withholdSet))
		})
	}
	out := make([]*variantBNode, n)
	for i := range out {
		out[i] = newVariantBNode(logger, policy, replication, pushDivisor, withholdSet[i], groups)
	}
	return out
}

// bLogger is the broadcasters' logger: errors only, or debug under SEGMENT_DEBUG.
func bLogger() *logrus.Logger {
	logger := logrus.New()
	if os.Getenv("SEGMENT_DEBUG") != "" {
		logger.SetLevel(logrus.DebugLevel)
	} else {
		logger.SetLevel(logrus.ErrorLevel)
	}
	return logger
}

// newVariantBNode is one node's broadcaster on the cell's policy and knobs; withhold puts it on
// the withholding set, groups is how many envelopes complete it. The in-process cell builds
// every node; the Shadow node, one process per node, builds its own.
func newVariantBNode(logger *logrus.Logger, policy segmentbroadcaster.Policy, replication, pushDivisor int, withhold bool, groups int) *variantBNode {
	node := &variantBNode{
		done: make(chan struct{}),
		broadcaster: segmentbroadcaster.New(context.Background(), logger, segmentbroadcaster.Config{
			Policy:         policy,
			WithholdServes: withhold,
			Replication:    replication,
			// Coordinated pushing. The divisor is the connectivity degree, so a receiver
			// expects one pushed copy per segment: each of its ~degree in-neighbours
			// volunteers with probability 1/degree. Zero keeps the uncoordinated rule.
			PushDivisor: pushDivisor,
			// Must cover a round trip plus the sender's transmission of what it volunteered,
			// on the same reasoning as RequestTimeout -- and past it everything missing is
			// requested regardless, so a lost push cannot strand a segment.
			PushGrace: envDuration("SEGMENT_PUSH_GRACE_MS", 16*defaultLatency),
			// The request timeout has to cover a round trip *plus* the time for the peer
			// to transmit everything asked of it, not just a round trip. Here that is
			// 50 ms RTT + 512 KB at 50 Mbps = ~132 ms at minimum, and more when several
			// peers ask the same peer at once.
			//
			// Getting this wrong is expensive and does not look like a timing problem: a
			// claim that lapses early is re-aimed at a *different* peer, and both send, so
			// the duplication the variant exists to remove comes straight back. Measured
			// on the split arm: 1.55 copies per node at 75 ms against 1.05 at 400 ms.
			RequestTimeout: requestTimeoutOverride(16 * defaultLatency),
			RetryInterval:  envDuration("SEGMENT_RETRY_INTERVAL_MS", 4*defaultLatency),
			// Fresh-claim cap per peer per pass: pipelines the pull instead of mobbing
			// the first holder. Zero keeps the claim-everything behaviour.
			ClaimPerPeer: envCount("SEGMENT_CLAIM_PER_PEER", 0),
			// Coded groups only: claims kept outstanding beyond K (the tail surplus coded A
			// gets from stop-pull's over-ask). Zero claims exactly what completes the node.
			RequestSurplus: envCount("SEGMENT_CLAIM_SURPLUS", 0),
			// Pushes in RPCs of at most this many segments, round-robin across peers, instead of
			// one bundle per peer (A's batch-publishing order). Zero keeps the bundle.
			PushChunk: envCount("SEGMENT_PUSH_CHUNK", 0),
			// Per-peer announce batching window (rowdas announce policy). Zero sends
			// every metadata change immediately.
			AnnounceWindow: envDuration("SEGMENT_ANNOUNCE_WINDOW_MS", 0),
			// Failure memory for the withholding row: avoid a peer after this many
			// lapsed claims, for SEGMENT_STRIKE_TTL_MS. Zero keeps no memory.
			StrikeCap: envCount("SEGMENT_STRIKE_CAP", 0),
			StrikeTTL: envDuration("SEGMENT_STRIKE_TTL_MS", 0),
			// Coded groups only: hold new requests back this long so in-flight pushes
			// count toward K first. Zero requests immediately, as plain groups always do.
			RequestDefer: envDuration("SEGMENT_REQUEST_DEFER_MS", 0),
			// Per-peer claim expiry from measured claim-to-arrival times, for
			// heterogeneous-latency runs where no fixed constant fits every pair.
			AdaptiveRequestTimeout: os.Getenv("SEGMENT_ADAPTIVE_TIMEOUT") != "",
			// Snappy each segment inside the part, as the gossip encoder does for
			// variant A's messages; off is the raw wire every B row before 2026-09-08 ran on.
			CompressSegments: os.Getenv("SEGMENT_PARTIAL_COMPRESS") != "",
		}),
	}
	node.remaining.Store(int32(max(groups, 1))) // lint:ignore uintcast -- a small env count
	return node
}

// requestTimeoutOverride lets a sweep vary the one timing knob announce-then-pull has.
func requestTimeoutOverride(def time.Duration) time.Duration {
	return envDuration("SEGMENT_REQUEST_TIMEOUT_MS", def)
}

// envDuration reads a millisecond count from the environment, or returns the default.
// envFloat reads a float in (0,1) from the environment, or returns the default.
func envFloat(name string, def float64) float64 {
	if v := os.Getenv(name); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil && f > 0 && f < 1 {
			return f
		}
	}
	return def
}

// envCount reads a non-negative integer from the environment, or returns the default.
func envCount(name string, def int) int {
	if v := os.Getenv(name); v != "" {
		if k, err := strconv.Atoi(v); err == nil && k >= 0 {
			return k
		}
	}
	return def
}

func envDuration(name string, def time.Duration) time.Duration {
	v := os.Getenv(name)
	if v == "" {
		return def
	}
	ms, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return time.Duration(ms) * time.Millisecond
}

// TestVariantBDiffusion is the end-to-end claim for variant B, across all three arms.
func TestVariantBDiffusion(t *testing.T) {
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
	cell := bCellFromEnv(t)
	for _, n := range sizes {
		for _, policy := range cell.policies {
			for _, replication := range cell.replications {
				usesR := policy == segmentbroadcaster.PushSplit || policy == segmentbroadcaster.PushPhase
				if !usesR && replication != cell.replications[0] {
					continue
				}
				for _, parity := range cell.parities {
					name := fmt.Sprintf("n=%d/%s", n, policy.String())
					if usesR {
						name = fmt.Sprintf("n=%d/%s/r=%d", n, policy.String(), replication)
					}
					if parity > 0 {
						name = fmt.Sprintf("%s/p=%d", name, parity)
					}
					t.Run(name, func(t *testing.T) {
						runDiffusion(t, diffusionParams{
							n:        n,
							rate:     defaultRate,
							latency:  defaultLatency,
							deadline: failureDeadline(t),
							seed:     meshGraphSeed(t),
						}, cell.variant(t, n, policy, replication, parity))
					})
				}
			}
		}
	}
}

// bCell is what the environment says about a variant B cell: the segment size, the payload, the
// policies, replications and parities to sweep, the push divisor and the group count. Factored
// out of TestVariantBDiffusion so the Shadow node builds the same groups from the same knobs.
type bCell struct {
	segmentSize  int
	payload      []byte
	policies     []segmentbroadcaster.Policy
	replications []int
	parities     []int
	pushDivisor  int
	groups       int
	pk           []byte
	slot         primitives.Slot
}

// bCellFromEnv reads the cell's knobs. params.SetupTestConfigCleanup must have run.
func bCellFromEnv(t *testing.T) bCell {
	t.Helper()
	c := bCell{segmentSize: segmentSizeBytes(t), replications: []int{1}, parities: []int{0}, slot: primitives.Slot(2048)}
	payloadLen := 1 << 19
	if os.Getenv("SEGMENT_SLOW_TESTS") != "" {
		payloadLen = 1 << 20
	}
	if v := os.Getenv("SEGMENT_REPLICATION"); v != "" {
		c.replications = nil
		for _, f := range strings.Split(v, ",") {
			k, err := strconv.Atoi(strings.TrimSpace(f))
			if err != nil {
				t.Fatalf("bad SEGMENT_REPLICATION %q: %v", v, err)
			}
			c.replications = append(c.replications, k)
		}
	}
	if v := os.Getenv("SEGMENT_PUSH_DIVISOR"); v != "" {
		k, err := strconv.Atoi(v)
		if err != nil {
			t.Fatalf("bad SEGMENT_PUSH_DIVISOR %q: %v", v, err)
		}
		c.pushDivisor = k
	}
	if v := os.Getenv("SEGMENT_PARITY"); v != "" {
		c.parities = nil
		for _, f := range strings.Split(v, ",") {
			k, err := strconv.Atoi(strings.TrimSpace(f))
			if err != nil {
				t.Fatalf("bad SEGMENT_PARITY %q: %v", v, err)
			}
			c.parities = append(c.parities, k)
		}
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
	sk, err := bls.RandKey()
	require.NoError(t, err)
	c.pk = sk.PublicKey().Marshal()
	c.payload = mainnetLikePayload(payloadLen, 11)
	c.policies = []segmentbroadcaster.Policy{
		segmentbroadcaster.PushSplit,
		segmentbroadcaster.PushAll,
		segmentbroadcaster.PushNone,
	}
	if v := os.Getenv("SEGMENT_POLICIES"); v != "" {
		byName := map[string]segmentbroadcaster.Policy{
			"split": segmentbroadcaster.PushSplit,
			"all":   segmentbroadcaster.PushAll,
			"none":  segmentbroadcaster.PushNone,
			"phase": segmentbroadcaster.PushPhase,
		}
		c.policies = nil
		for _, f := range strings.Split(v, ",") {
			p, ok := byName[strings.TrimSpace(f)]
			if !ok {
				t.Fatalf("bad SEGMENT_POLICIES %q", v)
			}
			c.policies = append(c.policies, p)
		}
	}
	return c
}

// variant is the cell's variant B at n nodes on one policy, replication and parity: the payload
// cut into groups and each group's segment messages.
func (c bCell) variant(t *testing.T, n int, policy segmentbroadcaster.Policy, replication, parity int) *variantB {
	t.Helper()
	parts := splitPayload(c.payload, c.groups)
	msgsByGroup := make([][]*segments.SegmentMessage, 0, c.groups)
	wireBytes := 0
	for _, part := range parts {
		msgs := segmentMessagesFor(t, nil, c.slot, part, c.segmentSize, parity)
		require.Equal(t, true, len(msgs) > 1)
		msgsByGroup = append(msgsByGroup, msgs)
		for _, m := range msgs {
			enc, err := m.Marshal()
			require.NoError(t, err)
			wireBytes += len(enc)
		}
	}
	return &variantB{
		n:           n,
		unit:        c.segmentSize,
		wireBytes:   wireBytes,
		replication: replication,
		pushDivisor: c.pushDivisor,
		policy:      policy,
		pk:          c.pk,
		slot:        c.slot,
		payload:     c.payload,
		groups:      c.groups,
		parts:       parts,
		msgs:        msgsByGroup[0],
		msgsByGroup: msgsByGroup,
	}
}

type variantB struct {
	unit        int // the segment size
	wireBytes   int // the groups' segment messages, encoded
	n           int
	replication int
	pushDivisor int
	policy      segmentbroadcaster.Policy
	pk          []byte
	slot        primitives.Slot
	payload     []byte
	groups      int                          // SEGMENT_B_GROUPS: partial messages on this many topics
	parts       [][]byte                     // the payload cut into groups; one part when groups == 1
	msgs        []*segments.SegmentMessage   // the first group's messages (the single group's, usually)
	msgsByGroup [][]*segments.SegmentMessage // every group's messages, in topic order
	nodes       []*variantBNode              // built lazily on the first pubsubOpts call, inside the bubble
}

func (v *variantB) name() string { return "variantB" }

// wireForm: every group's messages, the required count summed, the segment size, the encoded
// segment messages' bytes; no gossip message ids on this path.
func (v *variantB) wireForm() wireForm {
	w := wireForm{unit: v.unit, total: v.wireBytes}
	for _, msgs := range v.msgsByGroup {
		w.msgs += len(msgs)
		w.need += int(msgs[0].Descriptor.Required()) // lint:ignore uintcast -- bounded by MaxSegments.
	}
	return w
}

// topicNames is the envelope topic for a single group, or one topic per group.
func (v *variantB) topicNames() []string {
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
func (v *variantB) isPart(envelope []byte) bool {
	for _, p := range v.parts {
		if bytes.Equal(p, envelope) {
			return true
		}
	}
	return false
}

func (v *variantB) pubsubOpts(t *testing.T, i int) []pubsub.Option {
	// Build the broadcasters on first use. The driver calls this while it constructs the network,
	// which is inside the synctest bubble -- and the broadcasters MUST be constructed there: New
	// captures a context whose Done channel the loop selects on, and a channel created outside the
	// bubble never counts as a durable block, so synctest could not advance virtual time and every
	// cell would crawl in real time. The driver adds the tracer to what AppendPubSubOpts returns.
	if v.nodes == nil {
		v.nodes = newVariantBNodes(t, v.n, v.policy, v.replication, v.pushDivisor, v.groups)
	}
	return v.nodes[i].broadcaster.AppendPubSubOpts(nil)
}

func (v *variantB) setup(t *testing.T, nw *simNetwork, _ diffusionParams) diffusionRun {
	n := nw.Len()
	topics := v.topicNames()
	r := &bRun{v: v, nw: nw, topic: topics[0], topics: topics, n: n}
	// The coordinated push predicate is a function of the sender's own id, and the broadcaster is
	// constructed before any host exists.
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
func (v *variantB) startNode(t *testing.T, node *variantBNode, isPublisher bool, wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := node.broadcaster.Start(segmentbroadcaster.Callbacks{
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
func (v *variantB) joinNode(t *testing.T, ps *pubsub.PubSub, node *variantBNode) []*pubsub.Subscription {
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

// publishFrom publishes every group at once from the publisher's broadcaster, in topic order:
// the publisher's queue then carries one copy of each group's segments before any group's
// second copy, as batch publishing does for variant A.
func (v *variantB) publishFrom(node *variantBNode) error {
	for g, topic := range v.topicNames() {
		if err := node.broadcaster.Publish(topic, v.msgsByGroup[g]); err != nil {
			return err
		}
	}
	return nil
}

func (v *variantB) onTimeout(t *testing.T, completed, n int) {
	// Dump a stuck node's counters: a stall's diagnosis starts with whether it was starved of
	// announces, of served segments, or of request budget, and by the next run the state is gone.
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

func (v *variantB) report(t *testing.T, s diffusionStats) {
	// Censored cells still report: onTimeout already failed the test.
	var pushed, served, requested, received, reissued int
	for _, node := range v.nodes {
		c := node.broadcaster.Counters()
		pushed += c.SegmentsPushed
		served += c.SegmentsServed
		requested += c.SegmentsRequested
		received += c.SegmentsReceived
		reissued += c.RequestsReissued
	}
	// The headline number: how many copies of the payload a node takes in. Variant A measured 6.3
	// here, and that was the binding constraint. Normalised by the *required* count, so a coded
	// arm's copies read as multiples of what a node needs and stay comparable across parities.
	// Summed over groups: one group is the usual case, and K then reads as before.
	required, planned := 0, 0
	for _, msgs := range v.msgsByGroup {
		required += int(msgs[0].Descriptor.Required()) // lint:ignore uintcast -- bounded by MaxSegments.
		planned += len(msgs)
	}
	copiesPerNode := float64(received) / float64(s.n) / float64(required)
	// Payload bytes for one whole envelope, so rx/node can be read as copies. The payload's
	// length, not the sum over messages: parity is not payload.
	payloadBytes := len(v.payload)
	t.Logf("arm=%-5s n=%d K=%d/%d completion %v  %s  segments planned: pushed %d served %d requested %d; received %d (%.2f copies/node)",
		v.policy.String(), s.n, required, planned, s.virt.Round(time.Millisecond),
		completionStats(s.compTimes, s.n-1, s.deadline),
		pushed, served, requested, received, copiesPerNode)
	t.Logf("        reissued requests %d; partial traffic: tx %d B over %d RPCs, rx %d B over %d RPCs (%.2fx payload rx/node); publisherTx %d B; dropped RPCs %d; full-gossip rx/node %d B; groups %d",
		reissued, s.partialTx, s.partialTxRPCs, s.partialRx, s.partialRPCs,
		float64(s.partialRx)/float64(s.n)/float64(payloadBytes), s.partialPublisherTx, s.drops, s.rxBytes/s.n, len(v.msgsByGroup))

	switch v.policy {
	case segmentbroadcaster.PushNone:
		// Nothing may move unasked, so every segment that moved was requested. This is what makes
		// the arm a clean measurement of the pull round trip.
		require.Equal(t, 0, pushed)
		require.Equal(t, true, served > 0)
	case segmentbroadcaster.PushSplit, segmentbroadcaster.PushAll:
		require.Equal(t, true, pushed > 0)
	}
}

// bRun is variant B's live cell state.
type bRun struct {
	v      *variantB
	nw     *simNetwork
	topic  string   // the first (usually only) topic
	topics []string // one per group
	subs   []*pubsub.Subscription
	loopWg sync.WaitGroup
	n      int
}

func (r *bRun) settle(t *testing.T, tracers []*recordingTracer) string {
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

func (r *bRun) cancel() {
	// Order matters and mirrors the original teardown: cancel subscriptions (unblocks the drain
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

func (r *bRun) watch(ctx context.Context, wg *sync.WaitGroup, start time.Time, signal func(int, time.Duration)) {
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

func (r *bRun) publish(_ context.Context, _ *testing.T) error {
	return r.v.publishFrom(r.v.nodes[0])
}

// TestVariantBServesAPeerThatOnlyAsks isolates the pull path from the push path.
//
// Two nodes, PushNone, so the receiver gets nothing until it has seen an announce and asked
// for each segment by index. If this passes, announce-then-pull works on its own; if the
// diffusion test then fails, the fault is in propagation rather than in the exchange.
func TestVariantBAnnounceThenPull(t *testing.T) {
	params.SetupTestConfigCleanup(t)
	sk, err := bls.RandKey()
	require.NoError(t, err)
	const slot = primitives.Slot(2048)
	payload := mainnetLikePayload(1<<17, 5)
	msgs := segmentMessagesFor(t, sk, slot, payload, segments.DefaultSegmentSize, 0)

	synctest.Test(t, func(t *testing.T) {
		nodes := newVariantBNodes(t, 2, segmentbroadcaster.PushNone, 1, 0, 1)
		nw, stop := newSimNetwork(t, networkConfig{
			links: uniformLinks(2, defaultRate),
			edges: line(2),
			perNodeOpts: func(i int) []pubsub.Option {
				return nodes[i].broadcaster.AppendPubSubOpts(nil)
			},
		})
		defer stop()

		var wg sync.WaitGroup
		for i, node := range nodes {
			wg.Add(1)
			target := node
			isPublisher := i == 0
			go func() {
				defer wg.Done()
				require.NoError(t, target.broadcaster.Start(segmentbroadcaster.Callbacks{
					Auth: segmentauth.New(commitmentFor(t, msgs[0].Descriptor)),
					OnEnvelope: func(envelope []byte) error {
						require.DeepEqual(t, payload, envelope)
						if !isPublisher {
							target.once.Do(func() { close(target.done) })
						}
						return nil
					},
				}))
			}()
		}
		defer wg.Wait()
		defer func() {
			for _, node := range nodes {
				node.broadcaster.Stop()
			}
		}()

		topic := envelopeTopic()
		var subs []*pubsub.Subscription
		defer func() {
			for _, s := range subs {
				s.Cancel()
			}
		}()
		for i, ps := range nw.Pubsubs {
			th, err := ps.Join(topic, pubsub.RequestPartialMessages())
			require.NoError(t, err)
			sub, err := th.Subscribe(pubsub.WithBufferSize(subscriptionBuffer))
			require.NoError(t, err)
			go func() {
				for {
					if _, err := sub.Next(context.Background()); err != nil {
						return
					}
				}
			}()
			subs = append(subs, sub)
			nodes[i].broadcaster.ServeTopic(topic)
		}
		time.Sleep(meshFormation)
		synctest.Wait()

		require.NoError(t, nodes[0].broadcaster.Publish(topic, msgs))

		ctx, cancel := context.WithTimeout(context.Background(), networkOpTimeout)
		defer cancel()
		select {
		case <-nodes[1].done:
		case <-ctx.Done():
			t.Logf("publisher counters: %+v", nodes[0].broadcaster.Counters())
			t.Logf("receiver  counters: %+v", nodes[1].broadcaster.Counters())
			t.Fatal("receiver never reassembled the envelope from announce plus pull alone")
		}
		synctest.Wait()

		pub := nodes[0].broadcaster.Counters()
		recv := nodes[1].broadcaster.Counters()
		t.Logf("publisher %+v", pub)
		t.Logf("receiver  %+v", recv)
		// Every segment the publisher sent was one the receiver had asked for by index, and
		// exactly one copy of each went out. A re-ask may happen -- nothing acknowledges a
		// request, so a claim that lapses is re-issued -- but it must not produce a second
		// copy on the wire, which is what Pushed on the serving side is for.
		require.Equal(t, 0, pub.SegmentsPushed)
		require.Equal(t, len(msgs), pub.SegmentsServed)
		require.Equal(t, len(msgs), recv.SegmentsRequested)
		require.Equal(t, len(msgs), recv.SegmentsReceived)
		require.Equal(t, 1, recv.GroupsCompleted)
	})
}
