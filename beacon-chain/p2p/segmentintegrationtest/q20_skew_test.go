package segmentintegrationtest

// The two-publisher driver -- measurement-plan section 20.
//
// Every cell before this measured payload diffusion from a single publisher at t=0, with the block
// nowhere in the model. That made the authority gate untestable in its real form: stages 2 and 3 of
// section 21 had to fake authority with a synthetic offset, which models *universal inversion* --
// every node lacking the block when the first segment arrives -- and that is the reverse of the
// honest Gloas ordering.
//
// Here the ordering is real. The proposer publishes a block; the builder is a node in the network
// that publishes segments only once the block reaches it; every node's authority to authenticate a
// segment arrives when *its own* copy of the block installs. The block therefore has a structural
// head start of one builder hop, and inversion becomes something to measure rather than to assume.
//
// Step 1 of the plan's build order: no behavioural gate. Gates run in shadow -- every decision
// recorded, none acted on -- so the race being measured is the ungated one while still producing
// the numbers that decide whether the gate and the sender-side evidence are worth building.

import (
	"context"
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

	"github.com/OffchainLabs/prysm/v7/beacon-chain/p2p"
	"github.com/OffchainLabs/prysm/v7/beacon-chain/p2p/encoder"
	"github.com/OffchainLabs/prysm/v7/config/params"
	"github.com/OffchainLabs/prysm/v7/consensus-types/primitives"
	"github.com/OffchainLabs/prysm/v7/crypto/bls"
	"github.com/OffchainLabs/prysm/v7/testing/require"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/x/simlibp2p"
)

// blockTopic is the beacon block topic, on the same fork digest as the segment topic.
func blockTopic() string {
	return fmt.Sprintf(p2p.BlockSubnetTopicFormat, params.ForkDigest(params.BeaconConfig().FuluForkEpoch)) +
		encoder.SszNetworkEncoder{}.ProtocolSuffix()
}

// skewTimes is one node's record of the race.
//
// Three block timestamps rather than one, because "the block arrived" is not a single event: Prysm
// gossip-validates a block, hands it to the subscriber, and only when block processing succeeds does
// it drain block-dependent objects. Authority to authenticate a segment exists at the *end* of that.
// The harness installs no application validator, so wire arrival and gossip acceptance coincide
// here; the gap that matters is delta_install, which the virtual clock hides and which is therefore
// swept as a modelled parameter rather than measured.
// Every timestamp carries an explicit "did it happen" flag rather than using zero as a sentinel.
// Zero is a legitimate value here: under a virtual clock a publisher's own subscription delivers
// synchronously, so `time.Since(start)` is exactly 0 and a zero-means-absent test silently reports
// the proposer as never having received its own block.
type skewTimes struct {
	hasBlock    bool
	hasAuth     bool
	hasSeg      bool
	hasRaw      bool
	hasDone     bool
	hasUsable   bool
	blockWire   time.Duration // first delivery of the block to this node
	blockAccept time.Duration // gossip validation passed -- the builder's reveal trigger
	authority   time.Duration // block installed; the gate opens here
	firstSeg    time.Duration // first segment DELIVERED to the subscription
	firstRaw    time.Duration // first segment seen on the wire, before admission may defer it
	dataDone    time.Duration // holds enough segments to reconstruct
	usable      time.Duration // reconstructable AND authorized
	earlyCount  int           // segments that arrived before authority
	earlyBytes  int           // and how many bytes they occupied
	maxBuffered int           // peak bytes held unauthenticated
}

// skewRecorder collects the per-node records for a cell.
type skewRecorder struct {
	mu    sync.Mutex
	times []skewTimes
}

func newSkewRecorder(n int) *skewRecorder { return &skewRecorder{times: make([]skewTimes, n)} }

func (r *skewRecorder) with(node int, f func(*skewTimes)) {
	r.mu.Lock()
	defer r.mu.Unlock()
	f(&r.times[node])
}

// skewVariant is the two-publisher experiment.
type skewVariant struct {
	arm       wireArm
	blockMsg  []byte
	proposer  int
	builder   int
	reveal    time.Duration // delta_reveal: 0 is the honest worst case
	install   time.Duration // delta_install: modelled, swept
	learn     string        // how the builder learns it won: gossip | direct | oracle
	phaseR    int           // segment-topic push width; 0 = pull-only
	directHop time.Duration // modelled proposer->builder one-way latency, for learn=direct
	rec       *skewRecorder
	gates     *segmentGates
	nw        *simNetwork // retained so the report can map node index to peer id
	flood     floodConfig // the adversary; disabled unless the cell asked for one
	floodStat *floodStats
	group     [32]byte // the honest group id, which the modelled bid commits to
	rate      int
	oneWay    time.Duration
	revealAt  time.Duration // when the builder actually published, filled in by the run
	revealed  atomic.Bool
	revealSet sync.Once
	startMu   sync.Mutex
	startAt   time.Time // the cell's publish start, for callbacks that fire outside watch
}

// setStarted / started share the cell's time origin with callbacks that do not receive it. Written
// in watch, before publish, so it is set before any segment can arrive.
func (v *skewVariant) setStarted(t time.Time) {
	v.startMu.Lock()
	defer v.startMu.Unlock()
	v.startAt = t
}

func (v *skewVariant) started() time.Time {
	v.startMu.Lock()
	defer v.startMu.Unlock()
	return v.startAt
}

func (v *skewVariant) name() string { return "skew/" + v.learn }

// setGates takes the cell's gates so each node's authority can be opened from its own block
// installation rather than from a synthetic timer.
func (v *skewVariant) setGates(g *segmentGates) { v.gates = g }

func (v *skewVariant) pubsubOpts(t *testing.T, _ int) []pubsub.Option {
	// The phase policy applies to the segment topic only. The block must propagate the way a real
	// block does -- ordinary gossipsub, eager push to the whole mesh -- or the race is against a
	// strawman and every inversion is an artifact of a hobbled block.
	segPrefix := segmentTopic()
	return phasePubsubOptsMatch(t, v.phaseR, func(topic string) bool {
		return strings.HasPrefix(topic, segPrefix)
	})
}

func (v *skewVariant) setup(t *testing.T, nw *simNetwork, p diffusionParams) diffusionRun {
	segTopics, segSubs, cancelSeg := joinAndSubscribeAll(t, nw, false)
	blkTopics, blkSubs, cancelBlk := joinAndSubscribeTopic(t, nw, blockTopic(), false)
	v.nw = nw
	installAdmission(t, nw, v.gates, segTopics)
	checkFlood(t, v.flood, nw.Len(), v.gates)
	// The bid's commitment set, installed with authority. Only when an adversary is present: it is
	// what lets a node refuse a fabricated group, and switching it on for honest cells would change
	// nothing except the code path a measured number came through.
	if v.flood.enabled() {
		v.gates.modelCommitments(uint64(syntheticSlot), v.group)
		// Attribute losses, so "the queue stayed bounded" can be told apart from "the queue stayed
		// bounded by discarding honest segments".
		adv := adversaryPeers(nw, v.flood)
		for _, a := range v.gates.admits {
			a.adversary = func(p peer.ID) bool { return adv[p] }
		}
	}
	// Early arrivals are visible only at the validator once the rule is enforced, so route them
	// back into the per-node record that the report reads.
	if v.gates != nil {
		for _, a := range v.gates.admits {
			a.onDefer = func(node, size, held int) {
				// The raw arrival MUST be recorded here as well as at the subscription. Under
				// enforcement a deferred segment reaches the subscription only after the drain, so a
				// firstRaw set only there is always >= authority and the inverted count is zero by
				// construction -- the metric reports success rather than measuring it.
				at := time.Since(v.started())
				v.rec.with(node, func(tm *skewTimes) {
					if !tm.hasRaw {
						tm.firstRaw, tm.hasRaw = at, true
					}
					tm.earlyCount++
					tm.earlyBytes += size
					tm.maxBuffered = max(tm.maxBuffered, held)
				})
			}
		}
	}
	return &skewRun{
		v: v, nw: nw, n: nw.Len(),
		segTopics: segTopics, segSubs: segSubs,
		blkTopics: blkTopics, blkSubs: blkSubs,
		cancelSub: func() { cancelSeg(); cancelBlk() },
	}
}

func (v *skewVariant) onTimeout(t *testing.T, completed, n int) {
	t.Logf("        skew: %d/%d nodes reached usable payload", completed, n-1)
}

// reportExposureCurve joins the admission queues' per-node record to the cell's completion record.
func (v *skewVariant) reportExposureCurve(t *testing.T) {
	t.Helper()
	if v.gates == nil || v.nw == nil {
		return
	}
	self := make([]peer.ID, v.nw.Len())
	for i := range self {
		self[i] = v.nw.Hosts[i].ID()
	}
	// The builder originates the payload, so it is neither a victim nor censorable; every other
	// node counts as completed only if it reached a usable payload.
	completed := func(i int) bool {
		if i == v.builder {
			return true
		}
		v.rec.mu.Lock()
		defer v.rec.mu.Unlock()
		return v.rec.times[i].hasUsable
	}
	reportExposure(t, v.gates.admits, adversaryPeers(v.nw, v.flood), self, completed)
}

func (v *skewVariant) report(t *testing.T, s diffusionStats) {
	// Reported before anything else, and unconditionally: a flood cell whose admission counters
	// stay at zero is a pass only if the flood actually ran, and those two cases are otherwise
	// indistinguishable in the log.
	if v.flood.enabled() && v.floodStat != nil {
		v.reportExposureCurve(t)
		t.Logf("        adversary: %d nodes, %d groups x %d B every %v after %v -- published %d, failed %d",
			v.flood.nodes, v.flood.groups, v.flood.bytes, v.flood.interval, v.flood.after,
			v.floodStat.published.Load(), v.floodStat.failed.Load())
	}
	v.rec.mu.Lock()
	defer v.rec.mu.Unlock()

	var (
		inverted, gotBlock, gotSeg int
		invDur                     []time.Duration
		early                      []int
		usable                     []time.Duration
		blockSpread                []time.Duration
		earlyBytes                 []int
		peakBuffered               []int
	)
	for i, tm := range v.rec.times {
		if i == v.builder {
			continue
		}
		if tm.hasBlock {
			gotBlock++
			blockSpread = append(blockSpread, tm.blockAccept)
		}
		if tm.hasSeg {
			gotSeg++
		}
		// Inversion is judged against authority, not against the block's arrival on the wire.
		// Either source counts: the subscription sees the early arrival in shadow mode, the
		// validator sees it under enforcement.
		if tm.hasRaw && (!tm.hasAuth || tm.firstRaw < tm.authority) {
			inverted++
			early = append(early, tm.earlyCount)
			earlyBytes = append(earlyBytes, tm.earlyBytes)
			peakBuffered = append(peakBuffered, tm.maxBuffered)
			// Against the RAW arrival, not the delivered one: under enforcement a deferred segment
			// reaches the subscription only after the drain, so measuring from delivery makes the
			// holding duration structurally zero -- success by construction.
			if tm.hasAuth {
				invDur = append(invDur, tm.authority-tm.firstRaw)
			}
		}
		if tm.hasUsable {
			usable = append(usable, tm.usable)
		}
	}
	pop := max(1, v.rec.n()-1)
	// Name the nodes that never got the block, and where they sit. A node that holds every segment
	// and no block is the whole failure mode this section exists to size, so it must not be a
	// silent count.
	var noBlock []int
	for i, tm := range v.rec.times {
		if i != v.builder && !tm.hasBlock {
			noBlock = append(noBlock, i)
		}
	}
	t.Logf("%4dMbps n=%-4d %-14s reveal +%v (delta_reveal %v, delta_install %v)  block %d/%d  seg %d/%d",
		v.rate/simlibp2p.OneMbps, s.n, v.name(), v.revealAt.Round(time.Millisecond),
		v.reveal, v.install, gotBlock, pop, gotSeg, pop)
	t.Logf("        block arrival %s  (builder node %d, proposer node %d, one-way apart %v)",
		durQuantiles(blockSpread), v.builder, v.proposer, v.directHop.Round(time.Millisecond))
	if len(noBlock) > 0 {
		t.Logf("        *** %d node(s) never accepted the block: %v", len(noBlock), noBlock)
	}
	t.Logf("        INVERTED %d/%d (%.2f%%)  early segments/node %s  inversion duration %s",
		inverted, pop, 100*float64(inverted)/float64(pop), intQuantiles(early), durQuantiles(invDur))
	// The queue-sizing number, and the reason prevalence alone decides nothing: a node inverted for
	// 200 ms holding one 32 KiB segment is a different design problem from one holding all 32.
	if len(peakBuffered) > 0 {
		t.Logf("        buffered bytes/node: early %s  peak held %s",
			byteQuantiles(earlyBytes), byteQuantiles(peakBuffered))
	}
	// Usable completion is reported from the reveal, not from block publication: the deadline the
	// study uses is reveal-relative, and mixing the two silently credits the payload with the
	// block's head start.
	t.Logf("        usable from reveal %s  (rate@%v %d/%d)",
		durQuantiles(shiftBy(usable, v.revealAt)), s.deadline,
		countUnder(shiftBy(usable, v.revealAt), s.deadline), pop)
}

func (r *skewRecorder) n() int { return len(r.times) }

// repairUsable sets usable once BOTH data completion and authority have happened, whichever came
// second. Computing it at data completion alone assigned an absent, zero-valued authority and never
// revisited it, so a node completing before authority was credited with usable=0.
func (tm *skewTimes) repairUsable() {
	if !tm.hasDone || !tm.hasAuth {
		return
	}
	tm.usable, tm.hasUsable = max(tm.dataDone, tm.authority), true
}

// blockWithRoot builds a block payload whose first 32 bytes are the root it authorizes.
func blockWithRoot(root [32]byte, size int) []byte {
	out := make([]byte, 0, max(size, 32))
	out = append(out, root[:]...)
	if size > 32 {
		out = append(out, highEntropyPayload(size-32, 4242)...)
	}
	return out
}

// rootFromBlock recovers the authorized root from a block payload.
func rootFromBlock(data []byte) ([32]byte, bool) {
	var root [32]byte
	if len(data) < 32 {
		return root, false
	}
	copy(root[:], data[:32])
	return root, true
}

// skewRun is the live cell state.
type skewRun struct {
	v         *skewVariant
	nw        *simNetwork
	n         int
	segTopics []*pubsub.Topic
	segSubs   []*pubsub.Subscription
	blkTopics []*pubsub.Topic
	blkSubs   []*pubsub.Subscription
	cancelSub func()
	start     time.Time
}

func (r *skewRun) settle(t *testing.T, tracers []*recordingTracer) string {
	// Two topics, so the settled mesh band is twice a single topic's.
	return summariseMesh(awaitMeshMany(t, tracers, 2, false))
}

func (r *skewRun) cancel() { r.cancelSub() }

func (r *skewRun) watch(ctx context.Context, wg *sync.WaitGroup, start time.Time, signal func(int, time.Duration)) {
	r.start = start
	v := r.v
	v.setStarted(start)
	// The adversary starts with the cell's clock, so its window is measured from the same origin
	// as everything else. SEGMENT_ADV_AFTER_MS aims it before or after authority installs.
	v.floodStat = runFlood(ctx, r.nw, r.segTopics, v.flood, syntheticSlot, wg)
	// Per-node block watcher. It records the three timestamps, opens the node's gate at
	// authority, and -- for the builder -- triggers the reveal. Every node runs one, including
	// the proposer, so the proposer's own delivery is recorded too.
	for i := range r.n {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			msg, err := r.blkSubs[idx].Next(ctx)
			if err != nil {
				return
			}
			at := time.Since(start)
			root, ok := rootFromBlock(msg.Data)
			if !ok {
				return
			}
			v.rec.with(idx, func(tm *skewTimes) {
				tm.blockWire, tm.blockAccept, tm.hasBlock = at, at, true
			})
			// Authority is acceptance plus the modelled install delay. Scheduling it rather than
			// sleeping keeps this goroutine free to exit.
			time.AfterFunc(v.install, func() {
				v.rec.with(idx, func(tm *skewTimes) {
					tm.authority, tm.hasAuth = time.Since(start), true
					tm.repairUsable()
				})
				// Exactly the root this block carries, and no other.
				v.gates.openNode(idx, root)
			})
			if idx == v.builder {
				r.reveal(ctx, start)
			}
		}(i)
	}

	// Per-node segment watcher: first arrival, early-arrival accounting against authority, and
	// completion.
	arm := v.arm
	for i := range r.n {
		if i == v.builder {
			continue
		}
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			seen := make(map[digest]bool, len(arm.msgs))
			buffered := 0
			for len(seen) < arm.completeAt() {
				msg, err := r.segSubs[idx].Next(ctx)
				if err != nil {
					return
				}
				if _, ok := arm.known[digestOf(msg.Data)]; !ok {
					continue
				}
				if seen[digestOf(msg.Data)] {
					continue
				}
				seen[digestOf(msg.Data)] = true
				at := time.Since(start)
				v.rec.with(idx, func(tm *skewTimes) {
					if !tm.hasSeg {
						tm.firstSeg, tm.hasSeg = at, true
					}
					if !tm.hasRaw {
						tm.firstRaw, tm.hasRaw = at, true
					}
					// Early means "arrived before this node could authenticate it", which is what
					// the bounded admission queue has to hold.
					if !tm.hasAuth || at < tm.authority {
						tm.earlyCount++
						tm.earlyBytes += len(msg.Data)
						buffered += len(msg.Data)
						tm.maxBuffered = max(tm.maxBuffered, buffered)
					}
				})
			}
			done := time.Since(start)
			v.rec.with(idx, func(tm *skewTimes) {
				tm.dataDone, tm.hasDone = done, true
				tm.repairUsable()
			})
			// Signal data completion, which is what the driver's fan-in counts. Usable completion is
			// reported separately and decides everything -- the two differ exactly for the nodes this
			// section exists to find.
			signal(idx, done)
		}(i)
	}
}

// reveal publishes the segments from the builder. Called from the builder's block watcher, so the
// dependency the whole experiment turns on -- no reveal before the block -- is structural.
func (r *skewRun) reveal(ctx context.Context, start time.Time) {
	v := r.v
	// Idempotent: in the direct arm both the side channel and the builder's own gossip copy call
	// this, and only the first may publish.
	if !v.revealed.CompareAndSwap(false, true) {
		return
	}
	if v.reveal > 0 {
		select {
		case <-time.After(v.reveal):
		case <-ctx.Done():
			return
		}
	}
	v.revealSet.Do(func() { v.revealAt = time.Since(start) })
	// Deliberately NOT granting the builder receiver-authority here. Its own publish is admitted by
	// the self-origin bypass, which is the correct reason -- a node never defers what it originated.
	// Opening its gate instead would grant authority ahead of its own modelled install delay and
	// quietly exempt one node from the thing being measured.
	var batch pubsub.MessageBatch
	for _, m := range v.arm.msgs {
		if err := r.segTopics[v.builder].AddToBatch(ctx, &batch, m); err != nil {
			return
		}
	}
	_ = r.nw.Pubsubs[v.builder].PublishBatch(&batch)
}

// publish sends the block from the proposer. The segments follow from the builder's watcher.
func (r *skewRun) publish(ctx context.Context, t *testing.T) error {
	v := r.v
	switch v.learn {
	case "oracle":
		// Stress envelope: the builder is told at t=0 by a channel the spec does not define, so
		// the segments race a block that has not gone anywhere yet. Not the operating assumption.
		go r.reveal(ctx, r.start)
	case "direct":
		// The fastest plausible honest path: the proposer hands the block straight to the builder
		// over one connection, so the builder learns after exactly one modelled hop instead of
		// after however many the mesh takes. Gossip still carries the block to everyone else, and
		// the builder's own gossip copy is ignored for the reveal (revealSet makes it idempotent).
		hop := v.directHop
		go func() {
			select {
			case <-time.After(hop):
			case <-ctx.Done():
				return
			}
			r.reveal(ctx, r.start)
		}()
	}
	return r.blkTopics[v.proposer].Publish(ctx, v.blockMsg)
}

// TestQ20BlockSegmentSkew runs the two-publisher cell.
func TestQ20BlockSegmentSkew(t *testing.T) {
	params.SetupTestConfigCleanup(t)
	// The gate reads a structural claim out of the message id, so this cell is meaningless without
	// structured ids. Turn them on for itself rather than requiring the caller to remember.
	forceStructuredIDs = true
	t.Cleanup(func() { forceStructuredIDs = false })
	n := 30
	if v := os.Getenv("SEGMENT_MESH_SIZES"); v != "" {
		k, err := strconv.Atoi(v)
		require.NoError(t, err)
		n = k
	}
	learn := os.Getenv("SEGMENT_BUILDER_LEARNS")
	switch learn {
	case "":
		learn = "gossip"
	case "gossip", "direct", "oracle":
	default:
		t.Fatalf("bad SEGMENT_BUILDER_LEARNS %q (want gossip|direct|oracle)", learn)
	}

	sk, err := bls.RandKey()
	require.NoError(t, err)
	payload := mainnetLikePayload(1<<20, 11)
	arm := segmentedArm(t, payload, segmentSizeBytes(t), sk, primitives.Slot(2048))
	if v := os.Getenv("SEGMENT_PARITY"); v != "" {
		par, err := strconv.Atoi(v)
		require.NoError(t, err)
		arm = codedArm(t, payload, segmentSizeBytes(t), par, sk, primitives.Slot(2048))
	}

	// A measured mainnet-like Gloas block: ~6.5 KiB median. High entropy so snappy cannot collapse
	// it into something that arrives faster than a real block would. The first 32 bytes carry the
	// root this block authorizes, so a receiving node opens the root it actually received rather
	// than "the" root -- which is what makes exact-root authority, competing forks and hostile roots
	// expressible instead of collapsing into one global boolean.
	blockBytes := 6656
	if v := os.Getenv("SEGMENT_BLOCK_BYTES"); v != "" {
		k, err := strconv.Atoi(v)
		require.NoError(t, err)
		blockBytes = k
	}

	phaseR := 2
	if s := os.Getenv("SEGMENT_A_PHASE_R"); s != "" {
		k, err := strconv.Atoi(s)
		if err != nil || k < 0 {
			t.Fatalf("bad SEGMENT_A_PHASE_R %q", s)
		}
		phaseR = k
	}
	rate := envMbps("SEGMENT_UP_MBPS", defaultRate)
	proposer := proposerNode(t, n)
	var group [32]byte
	copy(group[:], groupIDFor(t, payload, segmentSizeBytes(t)))
	v := &skewVariant{
		flood:     floodConfigFromEnv(t),
		group:     group,
		phaseR:    phaseR,
		proposer:  proposer,
		directHop: oneWayBetween(t, n, proposer, 0),
		arm:       arm,
		blockMsg:  blockWithRoot(cellBlockRoot(syntheticSlot), blockBytes),
		builder:   0, // node 0 keeps the builder link settings meshLinks already gives it
		reveal:    envDuration("SEGMENT_REVEAL_MS", 0),
		install:   envDuration("SEGMENT_INSTALL_MS", 0),
		learn:     learn,
		rec:       newSkewRecorder(n),
		rate:      rate,
		oneWay:    defaultLatency,
	}
	runDiffusion(t, diffusionParams{
		n:        n,
		rate:     rate,
		latency:  defaultLatency,
		deadline: failureDeadline(t),
		seed:     meshGraphSeed(t),
	}, v)
}

// proposerNode picks the block publisher: never the builder (node 0), and placed at a chosen
// modelled distance from it.
//
// This is a swept parameter rather than an incidental one. The first n=500 sweep saw reveal times
// swing 59-169 ms *within* a single arm, purely because the random graph happened to put the
// builder near or far from the proposer -- which is the variable that sets when segments can start
// at all, so leaving it to the seed put its whole range into the noise.
//
// SEGMENT_PLACEMENT: "near" and "far" pick the argmin/argmax of one-way latency from the builder;
// "index" (the default) keeps node 1, which is what every earlier cell used.
func proposerNode(t *testing.T, n int) int {
	t.Helper()
	switch os.Getenv("SEGMENT_PLACEMENT") {
	case "", "index":
		p := 1
		if v := os.Getenv("SEGMENT_PROPOSER_NODE"); v != "" {
			k, err := strconv.Atoi(v)
			if err != nil || k <= 0 || k >= n {
				t.Fatalf("bad SEGMENT_PROPOSER_NODE %q for n=%d", v, n)
			}
			p = k
		}
		return p
	case "near":
		return nodeAtDistance(t, n, 0, false)
	case "far":
		return nodeAtDistance(t, n, 0, true)
	default:
		t.Fatalf("bad SEGMENT_PLACEMENT %q (want near|far|index)", os.Getenv("SEGMENT_PLACEMENT"))
		return 1
	}
}

// --- small reporting helpers ---

func shiftBy(ds []time.Duration, by time.Duration) []time.Duration {
	out := make([]time.Duration, 0, len(ds))
	for _, d := range ds {
		out = append(out, d-by)
	}
	return out
}

func countUnder(ds []time.Duration, limit time.Duration) int {
	k := 0
	for _, d := range ds {
		if d <= limit {
			k++
		}
	}
	return k
}

func durQuantiles(ds []time.Duration) string {
	if len(ds) == 0 {
		return "n/a"
	}
	s := append([]time.Duration(nil), ds...)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	q := func(f float64) time.Duration {
		i := int(math.Round(f * float64(len(s)-1)))
		return s[i].Round(time.Millisecond)
	}
	return fmt.Sprintf("p50 %v p95 %v p99 %v max %v", q(0.5), q(0.95), q(0.99), q(1))
}

func byteQuantiles(xs []int) string {
	if len(xs) == 0 {
		return "n/a"
	}
	s := append([]int(nil), xs...)
	sort.Ints(s)
	q := func(f float64) string {
		v := s[int(math.Round(f*float64(len(s)-1)))]
		return fmt.Sprintf("%.0fKiB", float64(v)/1024)
	}
	return fmt.Sprintf("p50 %s p95 %s max %s", q(0.5), q(0.95), q(1))
}

func intQuantiles(xs []int) string {
	if len(xs) == 0 {
		return "n/a"
	}
	s := append([]int(nil), xs...)
	sort.Ints(s)
	q := func(f float64) int { return s[int(math.Round(f*float64(len(s)-1)))] }
	return fmt.Sprintf("p50 %d p95 %d max %d", q(0.5), q(0.95), q(1))
}
