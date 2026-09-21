package segmentintegrationtest

// Segment-study bindings for the shared gossipsub harness in testing/gossipsim.
//
// The harness itself -- topology generators, link and latency models, the production pubsub
// configuration, the network builder and the recording tracer -- was written here and promoted
// to testing/gossipsim so the RowDAS study could use the same substrate. Behaviour is unchanged.
//
// What stays here is everything specific to *this* study: the SEGMENT_* environment variables
// that select an arm, the segment size, the phase-forwarding options, and the failure-injection
// node sets. The builder no longer reads the environment, so those decisions are made in
// newSimNetwork below and handed over as pubsub options -- which also means an arm's
// configuration is visible at one call site instead of scattered through the harness.
//
// The aliases and one-line wrappers keep the fourteen experiment files that call these names
// compiling unchanged.

import (
	"context"
	"fmt"
	"net"
	"os"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/OffchainLabs/prysm/v7/container/segments"
	"github.com/OffchainLabs/prysm/v7/testing/gossipsim"
	"github.com/OffchainLabs/prysm/v7/testing/require"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
	simlibp2p "github.com/libp2p/go-libp2p/x/simlibp2p"
	"github.com/marcopolo/simnet"
)

// Harness types and constants, under the names this package has always used.
type (
	edge       = gossipsim.Edge
	simNetwork = gossipsim.Network
)

const (
	defaultLatency      = gossipsim.DefaultLatency
	defaultRate         = gossipsim.DefaultRate
	meshFormation       = gossipsim.MeshFormation
	subscriptionBuffer  = gossipsim.SubscriptionBuffer
	networkOpTimeout    = gossipsim.NetworkOpTimeout
	productionQueueSize = gossipsim.ProductionQueueSize
)

func newEdge(i, j int) edge { return gossipsim.NewEdge(i, j) }
func line(n int) []edge     { return gossipsim.Line(n) }
func star(n int) []edge     { return gossipsim.Star(n) }
func grid(w, h int) []edge  { return gossipsim.Grid(w, h) }

func circulant(n, d int) []edge { return gossipsim.Circulant(n, d) }

func randomRegular(n, d int, seed uint64) ([]edge, error) { return gossipsim.RandomRegular(n, d, seed) }

func sortedEdges(present map[edge]bool) []edge { return gossipsim.SortedEdges(present) }
func degrees(n int, es []edge) []int           { return gossipsim.Degrees(n, es) }
func connected(n int, es []edge) bool          { return gossipsim.Connected(n, es) }
func isEqualEdges(a, b []edge) bool            { return gossipsim.EqualEdges(a, b) }
func mix64(x uint64) uint64                    { return gossipsim.Mix64(x) }
func geoMix(x uint64) uint64                   { return gossipsim.GeoMix(x) }

var gossipDhi = meshParams().Dhi

func uniformLinks(n, bitsPerSecond int) []simlibp2p.NodeLinkSettingsAndCount {
	return gossipsim.UniformLinks(n, bitsPerSecond)
}

func asymmetricLinks(n, upBitsPerSecond, downBitsPerSecond int) []simlibp2p.NodeLinkSettingsAndCount {
	return gossipsim.AsymmetricLinks(n, upBitsPerSecond, downBitsPerSecond)
}

func latencyMatrix(n int, oneWay func(from, to int) time.Duration) simlibp2p.LatencyFunc {
	return gossipsim.LatencyMatrix(n, oneWay)
}

func geoLatency(t *testing.T, seed uint64, n int) (simlibp2p.LatencyFunc, string) {
	return gossipsim.GeoLatency(t, seed, n)
}

func geoLatencyPair(t *testing.T, seed uint64, n int) (simlibp2p.LatencyFunc, string, func(i, j int) time.Duration) {
	return gossipsim.GeoLatencyPair(t, seed, n)
}

func settle(wallClock bool) { gossipsim.Settle(wallClock) }

func awaitMesh(t *testing.T, tracers []*recordingTracer, wallClock bool) []int {
	return gossipsim.AwaitMesh(t, tracers, wallClock, meshParams())
}

func summariseMesh(sizes []int) string { return gossipsim.SummariseMesh(sizes) }

func joinAndSubscribeTopic(t *testing.T, nw *simNetwork, topicStr string, wallClock bool) ([]*pubsub.Topic, []*pubsub.Subscription, func()) {
	return gossipsim.JoinAndSubscribe(t, nw, topicStr, wallClock)
}

func joinAndSubscribeAll(t *testing.T, nw *simNetwork, wallClock bool) ([]*pubsub.Topic, []*pubsub.Subscription, func()) {
	return gossipsim.JoinAndSubscribe(t, nw, segmentTopic(), wallClock)
}

// startBackgroundTraffic reads this study's cadence and size, then defers to the harness.
// SEGMENT_BG_MSG_BYTES and SEGMENT_BG_INTERVAL_MS set each node's own publish size and cadence;
// the aggregate seen by any node is that times its mesh in-degree, as with real gossip. No-op
// when SEGMENT_BG_INTERVAL_MS is unset.
func startBackgroundTraffic(t *testing.T, nw *simNetwork, ctx context.Context, wg *sync.WaitGroup) (sent *atomic.Int64, stop func()) {
	t.Helper()
	msgBytes := 8 << 10
	if v := os.Getenv("SEGMENT_BG_MSG_BYTES"); v != "" {
		if k, err := strconv.Atoi(v); err == nil && k > 0 {
			msgBytes = k
		}
	}
	return gossipsim.StartBackgroundTraffic(t, nw, ctx, wg, "segmenttest/background",
		envDuration("SEGMENT_BG_INTERVAL_MS", 0), msgBytes)
}

// meshParams is the harness mesh configuration with this study's overrides applied.
// SEGMENT_MESH_D sets the mesh degree; **Dlazy is left at its default on purpose**: it sets the
// IHAVE fan-out, so holding it fixed isolates a change in push width from a change in announce
// width -- and the announce plane is what the pull arms feed on. SEGMENT_IDONTWANT_THRESHOLD
// moves the message size above which IDONTWANT is emitted.
func meshParams() pubsub.GossipSubParams {
	o := meshOverrides()
	return gossipsim.MeshParams(o.MeshDegree, o.IDontWantThreshold)
}

func meshOverrides() gossipsim.Overrides {
	o := gossipsim.NewOverrides()
	if v := os.Getenv("SEGMENT_IDONTWANT_THRESHOLD"); v != "" {
		k, err := strconv.Atoi(v)
		if err != nil || k < 0 {
			panic(fmt.Sprintf("bad SEGMENT_IDONTWANT_THRESHOLD %q", v))
		}
		o.IDontWantThreshold = k
	}
	if d, err := strconv.Atoi(os.Getenv("SEGMENT_MESH_D")); err == nil && d >= 1 {
		o.MeshDegree = d
	}
	// SEGMENT_FAIL_QUEUE_SIZE lowers the outbound queue depth so F2b's slowed writers can
	// drive genuine admission overflow (doDropRPC), not just backpressure latency: the 600
	// production default is hard to overflow via a slow drain alone.
	if v := os.Getenv("SEGMENT_FAIL_QUEUE_SIZE"); v != "" {
		if k, err := strconv.Atoi(v); err == nil && k > 0 {
			o.PeerOutboundQueueSize = k
		}
	}
	return o
}

func productionPubsubOpts() []pubsub.Option {
	opts := gossipsim.ProductionPubsubOptsWith(meshOverrides())
	if procCost() > 0 {
		opts = append(opts, pubsub.WithValidateWorkers(procWorkers()))
	}
	return opts
}

// procCost is SEGMENT_PROC_US: the virtual time a node spends on each received data message
// before it acts on it -- hashing, proof check, decompression, router work. Spent inside a topic
// validator, so a segment is forwarded only after it has been "processed", with procWorkers
// validators running at once per node. Zero, the default and the harness's historical
// assumption, is free processing; the knob exists to price configurations that trade bytes for
// message count (16 KiB segments, 64 topics, 128 coded shards).
func procCost() time.Duration {
	v := os.Getenv("SEGMENT_PROC_US")
	if v == "" {
		return 0
	}
	us, err := strconv.Atoi(v)
	if err != nil || us < 0 {
		panic(fmt.Sprintf("bad SEGMENT_PROC_US %q", v))
	}
	return time.Duration(us) * time.Microsecond
}

// procWorkers is SEGMENT_PROC_WORKERS, the validators a node runs concurrently under procCost;
// default 4. Set explicitly because gossipsub's own default is the host's CPU count, which
// would make the result depend on the host running the test.
func procWorkers() int {
	if n, err := strconv.Atoi(os.Getenv("SEGMENT_PROC_WORKERS")); err == nil && n > 0 {
		return n
	}
	return 4
}

// registerProcValidator installs the processing-cost validator on one topic when the knob is set.
func registerProcValidator(t *testing.T, ps *pubsub.PubSub, topic string) {
	t.Helper()
	d := procCost()
	if d == 0 {
		return
	}
	require.NoError(t, ps.RegisterTopicValidator(topic, func(context.Context, peer.ID, *pubsub.Message) pubsub.ValidationResult {
		time.Sleep(d)
		return pubsub.ValidationAccept
	}))
}

// networkConfig is the harness config plus nothing: the study's own knobs are read from the
// environment by newSimNetwork and translated into harness options there.
type networkConfig struct {
	links           []simlibp2p.NodeLinkSettingsAndCount
	edges           []edge
	latency         simlibp2p.LatencyFunc
	pubsubOpts      []pubsub.Option
	perNodeOpts     func(idx int) []pubsub.Option
	libraryDefaults bool
	wallClock       bool
}

// newSimNetwork translates this study's environment into harness options and brings the network
// up. Every knob below used to be read inside the builder; the behaviour is identical, but the
// decisions are now visible in one place.
func newSimNetwork(t *testing.T, cfg networkConfig) (*simNetwork, func()) {
	t.Helper()
	n := 0
	for _, l := range cfg.links {
		n += l.Count
	}

	latency := cfg.latency
	// SEGMENT_LATENCY_MODEL=geo replaces whatever latency the experiment configured with the
	// synthetic-geography model, for every harness at once. Uniform latency is known to
	// distort push/pull races (the PPPT writeup moved to RIPE-Atlas latencies for exactly
	// this reason), and those races are what the phase policies manipulate.
	switch os.Getenv("SEGMENT_LATENCY_MODEL") {
	case "", "uniform":
	case "geo":
		var summary string
		latency, summary = geoLatency(t, meshGraphSeed(t), n)
		t.Logf("latency model geo: %s", summary)
	default:
		t.Fatalf("bad SEGMENT_LATENCY_MODEL %q", os.Getenv("SEGMENT_LATENCY_MODEL"))
	}

	opts, perNode := substrateOpts(t, n, cfg.links, cfg.pubsubOpts, cfg.perNodeOpts)

	return gossipsim.New(t, gossipsim.NetworkConfig{
		Links:           cfg.links,
		Edges:           cfg.edges,
		Latency:         latency,
		PubsubOpts:      opts,
		PerNodeOpts:     perNode,
		Overrides:       meshOverrides(),
		LibraryDefaults: cfg.libraryDefaults,
		WallClock:       cfg.wallClock,
	})
}

// substrateOpts translates this study's environment into the network-wide pubsub options and the
// per-node ones, in the order the baseline used: the caller's options first, then the
// environment's, so an environment setting still wins a conflict with a caller setting. links
// feed the regime rule; perNodeBase is the caller's per-node options (tracer, ids, the variant's).
// Shared by the in-process constructor below and the Shadow node, which builds one node of the
// same network in its own process.
func substrateOpts(t *testing.T, n int, links []simlibp2p.NodeLinkSettingsAndCount, base []pubsub.Option, perNodeBase func(int) []pubsub.Option) ([]pubsub.Option, func(int) []pubsub.Option) {
	t.Helper()
	opts := append([]pubsub.Option{}, base...)

	t.Cleanup(func() { t.Logf("        %s", announceStats.Line()) })
	// SEGMENT_CONTROL_COALESCE=1 folds a control-only RPC into the control-only RPC already
	// queued for that peer (fork controlcoalesce.go), which is the measured answer to the
	// announce-rate finding: it covers IHAVE, IWANT and IDONTWANT alike and costs nothing on an
	// idle link. Off by default; the arms that do not set it are unchanged.
	var coalesced atomic.Int64
	coalesceControl := os.Getenv("SEGMENT_CONTROL_COALESCE") != ""
	if coalesceControl {
		t.Cleanup(func() {
			t.Logf("        control coalescing: %d control RPCs folded into one already queued", coalesced.Load())
		})
	}

	var adaptMinE, adaptMaxOcc atomic.Int64
	if os.Getenv("SEGMENT_ADAPTIVE_HEDGE") != "" {
		t.Cleanup(func() {
			t.Logf("adaptive hedge: min e %.3f, max queue occupancy %.3f (Job 2 engaged iff min e < 1.000)",
				float64(adaptMinE.Load())/1000, float64(adaptMaxOcc.Load())/1000)
		})
	}

	// The IWANT discipline/budget knobs apply to every arm -- whole-message and plain
	// segmented included -- because they are substrate fixes, not phase-arm features.
	var parkBreaks, parks atomic.Int64
	if os.Getenv("SEGMENT_IHAVE_PARK") != "" {
		t.Cleanup(func() {
			t.Logf("commitment enforcement: %d broken promises, %d peers parked (node-events)",
				parkBreaks.Load(), parks.Load())
		})
	}
	var pullRedrives atomic.Int64
	if os.Getenv("SEGMENT_PULL_MEMORY") != "" {
		t.Cleanup(func() {
			t.Logf("pull memory: %d re-driven asks", pullRedrives.Load())
		})
	}
	var offerRetries atomic.Int64
	if os.Getenv("SEGMENT_OFFER_TABLE") != "" {
		t.Cleanup(func() {
			t.Logf("offer table: %d timer-driven retries", offerRetries.Load())
		})
	}
	// The discipline family (discipline, park, pull memory, offer table) is the request policy a
	// regime rule chooses per node (regimeRuleFor): collected here and applied to every node
	// unless the rule is on, in which case perNode gives it to the large-R nodes only.
	var reqOpts []pubsub.Option
	rr := regimeRuleFor(t, links, n)
	if v := os.Getenv("SEGMENT_IWANT_DISCIPLINE_MS"); v != "" {
		ms, err := strconv.Atoi(v)
		if err != nil || ms <= 0 {
			t.Fatalf("bad SEGMENT_IWANT_DISCIPLINE_MS %q", v)
		}
		reqOpts = append(reqOpts, pubsub.WithIWantDiscipline(time.Duration(ms)*time.Millisecond))
		// IHAVE-commitment enforcement (adversarial tier-B #8): a request
		// expiring undelivered is a broken promise by its announcer; k breaks park the
		// peer's announcements for the TTL. Requires the discipline, hence nested here.
		if os.Getenv("SEGMENT_IHAVE_PARK") != "" {
			k := 2 // forgive one break: honest-late serves exist (v1 smoke over-parked at k=1)
			if kv := os.Getenv("SEGMENT_IHAVE_PARK_K"); kv != "" {
				kk, err := strconv.Atoi(kv)
				if err != nil || kk < 1 {
					t.Fatalf("bad SEGMENT_IHAVE_PARK_K %q", kv)
				}
				k = kk
			}
			window := time.Duration(ms) * time.Millisecond
			promise := envDuration("SEGMENT_IHAVE_PARK_PROMISE_MS", window*3/4)
			ttl := envDuration("SEGMENT_IHAVE_PARK_TTL_MS", 30*time.Second)
			reqOpts = append(reqOpts, pubsub.WithIHaveCommitmentPark(k, promise, ttl, &parkBreaks, &parks))
		}
	}
	if os.Getenv("SEGMENT_PULL_MEMORY") != "" {
		reqOpts = append(reqOpts, pubsub.WithPullMemory(&pullRedrives))
	}
	if os.Getenv("SEGMENT_OFFER_TABLE") != "" {
		reqOpts = append(reqOpts, pubsub.WithOfferTable(&offerRetries))
	}
	if !rr.active {
		opts = append(opts, reqOpts...)
	} else {
		large := 0
		for i := 0; i < n; i++ {
			if rr.large(i) {
				large++
			}
		}
		t.Logf("regime rule: %d/%d nodes in the large regime (discipline, offer table, park, tail hedge); R = 8 x %d B / uplink / %v; node 1 R %.2f", large, n, rr.bytes, rr.rtt, rr.R(1))
	}
	if v := os.Getenv("SEGMENT_IWANT_BUDGET_N"); v != "" {
		k, err := strconv.Atoi(v)
		if err != nil || k <= 0 {
			t.Fatalf("bad SEGMENT_IWANT_BUDGET_N %q", v)
		}
		opts = append(opts, pubsub.WithIWantBudget(k))
	}
	if v := os.Getenv("SEGMENT_IWANT_HEDGE_MS"); v != "" {
		ms, err := strconv.Atoi(v)
		if err != nil || ms <= 0 {
			t.Fatalf("bad SEGMENT_IWANT_HEDGE_MS %q", v)
		}
		opts = append(opts, pubsub.WithIWantHedge(time.Duration(ms)*time.Millisecond))
	}
	if v := os.Getenv("SEGMENT_REQUEST_NQG"); v != "" {
		// (N,q,g) request kernel: v is N. q/floor/ceil/init tunable by env.
		nn, err := strconv.Atoi(v)
		if err != nil || nn < 1 {
			t.Fatalf("bad SEGMENT_REQUEST_NQG %q (N>=1)", v)
		}
		q := envFloat("SEGMENT_NQG_Q", 0.9)
		floor := envDuration("SEGMENT_NQG_FLOOR_MS", 40*time.Millisecond)
		ceil := envDuration("SEGMENT_NQG_CEIL_MS", 2000*time.Millisecond)
		dInit := envDuration("SEGMENT_NQG_INIT_MS", 150*time.Millisecond)
		opts = append(opts, pubsub.WithRequestNQG(nn, q, floor, ceil, dInit))
	}
	if os.Getenv("SEGMENT_ADAPTIVE_HEDGE") != "" {
		// Two-controller adaptive hedge: delay tracks a latency quantile, AIMD rate throttles
		// under queue pressure. Defaults tunable by env; target hedge-launch rate 0.05 (p95).
		target := envFloat("SEGMENT_ADAPTIVE_HEDGE_TARGET", 0.05)
		qHigh := envFloat("SEGMENT_ADAPTIVE_HEDGE_QHIGH", 0.5)
		dInit := envDuration("SEGMENT_ADAPTIVE_HEDGE_INIT_MS", 120*time.Millisecond)
		dMin := envDuration("SEGMENT_ADAPTIVE_HEDGE_MIN_MS", 40*time.Millisecond)
		dMax := envDuration("SEGMENT_ADAPTIVE_HEDGE_MAX_MS", 800*time.Millisecond)
		// minE/maxOcc (x1000) let us see whether Job 2 ever engaged and how deep the queue
		// got -- the difference between "brake didn't help" and "brake never fired".
		opts = append(opts, pubsub.WithAdaptiveHedge(dInit, dMin, dMax, target, qHigh, &adaptMinE, &adaptMaxOcc))
	}
	// F2b real queue pressure: a hash-selected fraction of nodes run a slowed outbound
	// writer, so their rpcQueue overflows naturally. The
	// overflow drops surface through the tracer's droppedRPCs, already summed as `drops`.
	sendDelaySet := failSendDelaySet(t, n)
	sendDelay := envDuration("SEGMENT_FAIL_SEND_DELAY_MS", 0)
	if len(sendDelaySet) > 0 && sendDelay > 0 {
		t.Cleanup(func() {
			t.Logf("failure exposure: %d nodes with %v per-RPC send delay", len(sendDelaySet), sendDelay)
		})
	}

	// SEGMENT_DET_RAND pins the gossipsub mesh draw to the topology seed (common random
	// numbers): per-node seeded shuffle sources plus sort-before-shuffle in the fork.
	detRand := os.Getenv("SEGMENT_DET_RAND") != ""
	detSeed := int64(meshGraphSeed(t))

	// F1 withholding: a fixed-size node set, selected by ranking hash(fail seed, index)
	// so the schedule is identical across arms, answers
	// IWANTs never (silent) or whole after a delay (slow). Publisher excluded -- that is
	// F5's job. Exposure is counted in the fork and logged, not assumed.
	var withheld atomic.Int64
	withholdSet := failWithholdSet(t, n)
	withholdDelay := time.Duration(0)
	if os.Getenv("SEGMENT_FAIL_WITHHOLD_MODE") == "slow" {
		withholdDelay = envDuration("SEGMENT_FAIL_WITHHOLD_SLOW_MS", 800*time.Millisecond)
	}
	// E7 extensions of the withholder: a structural-index floor that restricts the injection
	// to the ids at or above it (the last-piece screen), a count-ordered
	// prefix (fast then slow, or slow then fast: the median-poisoning screen's timed strategies),
	// and hearsay announcing (the capture screen). All ride the withholder set.
	withholdFrom := -1
	if v := os.Getenv("SEGMENT_FAIL_WITHHOLD_INDEX_FROM"); v != "" {
		k, err := strconv.Atoi(v)
		if err != nil || k < 0 || !structuredIDsEnabled() {
			t.Fatalf("bad SEGMENT_FAIL_WITHHOLD_INDEX_FROM %q (want >= 0, with SEGMENT_STRUCTURED_IDS)", v)
		}
		withholdFrom = k
	}
	withholdPrefix, withholdPrefixFast := 0, true
	if v := os.Getenv("SEGMENT_FAIL_WITHHOLD_PREFIX"); v != "" {
		k, err := strconv.Atoi(v)
		if err != nil || k <= 0 {
			t.Fatalf("bad SEGMENT_FAIL_WITHHOLD_PREFIX %q", v)
		}
		withholdPrefix = k
		switch os.Getenv("SEGMENT_FAIL_WITHHOLD_PREFIX_MODE") {
		case "", "fast":
		case "slow":
			withholdPrefixFast = false
		default:
			t.Fatalf("bad SEGMENT_FAIL_WITHHOLD_PREFIX_MODE %q (fast|slow)", os.Getenv("SEGMENT_FAIL_WITHHOLD_PREFIX_MODE"))
		}
	}
	// SEGMENT_FAIL_HEARSAY=1 announces to every peer of the topic; =<n> to at most n peers, mesh
	// first — the conformant liar, at an honest relay's fan-out rather than a flood.
	hearsay := os.Getenv("SEGMENT_FAIL_HEARSAY") != ""
	hearsayWidth := 0
	if v := os.Getenv("SEGMENT_FAIL_HEARSAY"); v != "" && v != "1" {
		w, err := strconv.Atoi(v)
		if err != nil || w < 1 {
			t.Fatalf("bad SEGMENT_FAIL_HEARSAY %q (1 = every peer, or a width)", v)
		}
		hearsayWidth = w
	}
	var hearsaid atomic.Int64
	if hearsay {
		if len(withholdSet) == 0 {
			t.Fatal("SEGMENT_FAIL_HEARSAY requires SEGMENT_FAIL_WITHHOLD_PCT")
		}
		t.Cleanup(func() {
			t.Logf("failure exposure: hearsay announcing on the withholder set (width %d, 0 = every peer), %d (id, peer) announcements", hearsayWidth, hearsaid.Load())
		})
	}
	// Slow readers (the queue-poisoning screen): their own hash-selected set reads every inbound
	// stream with a delay before each read, so their senders' writers block on flow control.
	readerSet := failSelect(t, n, "SEGMENT_FAIL_SLOW_READER_PCT", 0x5107510751075107)
	readDelay := envDuration("SEGMENT_FAIL_SLOW_READER_MS", 100*time.Millisecond)
	if len(readerSet) > 0 {
		t.Cleanup(func() {
			t.Logf("failure exposure: %d slow-reading nodes, %v before each read", len(readerSet), readDelay)
		})
	}
	if len(withholdSet) > 0 {
		t.Cleanup(func() {
			t.Logf("failure exposure: %d withholding nodes, %d IWANT ids withheld/delayed",
				len(withholdSet), withheld.Load())
		})
	}

	// Relay silence rides the SAME withholder set (identical CRN pairing with the F1 cells):
	// selected nodes additionally forward nothing they receive — the bait-and-blackhole
	// adversary of the adversary-cell catalogue (silent-relay withholding).
	relaySilence := os.Getenv("SEGMENT_FAIL_RELAY_SILENCE") != ""
	var relaySkipped atomic.Int64
	if relaySilence {
		if len(withholdSet) == 0 {
			t.Fatal("SEGMENT_FAIL_RELAY_SILENCE requires SEGMENT_FAIL_WITHHOLD_PCT")
		}
		t.Cleanup(func() {
			t.Logf("failure exposure: relay silence on the withholder set, %d messages unforwarded",
				relaySkipped.Load())
		})
	}

	// IDONTWANT spoofing: its own hash-selected set claims to hold every id it hears
	// announced, poisoning phase forwarding's holder-count budget decay.
	var idwSpoofed atomic.Int64
	spoofSet := failSpoofSet(t, n)
	if len(spoofSet) > 0 {
		t.Cleanup(func() {
			t.Logf("failure exposure: %d IDONTWANT-spoofing nodes, %d spoofed id claims sent",
				len(spoofSet), idwSpoofed.Load())
		})
	}

	// F2a per-class admission loss: every node sheds the configured class at queue admission
	// with the given probability, decisions stateless
	// per (node seed, class, message id, destination address). Systemic, unlike F1's node
	// subset -- queue pressure hits everyone.
	var classDropped atomic.Int64
	lossClass := os.Getenv("SEGMENT_FAIL_RPC_CLASS")
	lossPct := 0
	if v := os.Getenv("SEGMENT_FAIL_RPC_DROP_PCT"); v != "" {
		k, err := strconv.Atoi(v)
		if err != nil || k <= 0 || k >= 100 || lossClass == "" {
			t.Fatalf("bad SEGMENT_FAIL_RPC_DROP_PCT %q (needs SEGMENT_FAIL_RPC_CLASS too)", v)
		}
		lossPct = k
		t.Cleanup(func() {
			t.Logf("failure exposure: rpc class %q dropped %d entries at %d%%",
				lossClass, classDropped.Load(), lossPct)
		})
	}

	perNode := func(i int) []pubsub.Option {
		out := []pubsub.Option{pubsub.WithIHaveRPCObservation(announceStats)}
		if coalesceControl {
			out = append(out, pubsub.WithControlCoalescing(&coalesced))
		}
		if rr.active && rr.large(i) {
			out = append(out, reqOpts...)
		}
		if perNodeBase != nil {
			out = append(out, perNodeBase(i)...)
		}
		if detRand {
			out = append(out, pubsub.WithDeterministicRand(detSeed*1000003+int64(i)))
		}
		if withholdSet[i] {
			out = append(out, pubsub.WithIWantWithholding(withholdDelay, &withheld))
			if withholdFrom >= 0 {
				from := uint32(withholdFrom)
				out = append(out, pubsub.WithIWantWithholdingSelect(func(mid string) bool {
					k, ok := segIDKey(mid)
					return ok && k.index >= from
				}))
			}
			if withholdPrefix > 0 {
				out = append(out, pubsub.WithIWantWithholdingPrefix(withholdPrefix, withholdPrefixFast))
			}
			if hearsay {
				out = append(out, pubsub.WithIHaveHearsay(hearsayWidth, &hearsaid))
			}
			if relaySilence {
				out = append(out, pubsub.WithRelaySilence(&relaySkipped))
			}
		}
		if spoofSet[i] {
			out = append(out, pubsub.WithIDontWantSpoofing(&idwSpoofed))
		}
		if readerSet[i] {
			out = append(out, pubsub.WithReadDelay(readDelay))
		}
		if lossPct > 0 {
			out = append(out, pubsub.WithRPCClassLoss(lossClass, lossPct,
				mix64(mix64(meshGraphSeed(t)^0xF2AF2AF2AF2AF2A)^uint64(i)), &classDropped))
		}
		if sendDelaySet[i] && sendDelay > 0 {
			out = append(out, pubsub.WithSendDelay(sendDelay))
		}
		return out
	}
	return opts, perNode
}

// Connectivity degree, mesh degree, and why they must differ.
//
// Every mesh experiment on this branch until now built the graph with `randomRegular(n, 8, 7)` --
// eight libp2p connections per node -- against Prysm's mesh target of `D = 8`. The graph degree
// therefore *equalled* the mesh degree, which quietly removed a whole half of gossipsub:
//
//   - Every peer was a mesh peer, so there were **zero non-mesh topic peers**.
//   - `emitGossip` targets only non-mesh peers, so **no IHAVE was ever emitted** and none of
//     `Dlazy = 6`'s per-heartbeat traffic existed.
//   - Mesh maintenance could not do anything: the mesh had to be the whole graph, so GRAFT and
//     PRUNE were decorative and a node could never drop a bad peer for a better one.
//   - Variant B's announce path runs through `EmitGossip`, which is handed exactly those non-mesh
//     peers, so **the announce half of announce-then-pull had never run at all**.
//
// Real Prysm holds up to 70 connections (`--p2p-max-peers`) and grafts `D = 8` of them per topic,
// leaving ~62 outside the mesh. What matters for the mechanisms above is only that the degree
// exceed `Dhi = 12`, so that some peers are guaranteed to sit outside the mesh however grafting
// lands.
const (
	// defaultConnectivityDegree is the graph degree these experiments build at.
	//
	// 20, not 70. It is comfortably above `Dhi = 12`, so every node keeps ~12 non-mesh peers --
	// twice `Dlazy`, which is all the gossip and announce paths need to be exercised. 70 is more
	// faithful and costs 3.5x the QUIC connections; at n=500 that is 17500 of them against a peak
	// RSS that was already over 3 GiB at degree 8. Use SEGMENT_DEGREE for a fidelity check at a
	// smaller n rather than paying it everywhere.
	defaultConnectivityDegree = 20
	// meshSettleTimeout bounds the wait for grafting to finish. It must exceed gossipsub's
	// PruneBackoff (one minute): a node that gets pruned while grafting collects backoffs and
	// sits below Dlo until they expire, which is a transient, not a failure. The previous
	// 30 s deadline was shorter than the backoff and produced a nondeterministic
	// one-node-below-Dlo fatal that read exactly like a diffusion stall -- six occurrences
	// across every arm, all passing identical re-runs, growing with n because more nodes
	// means more graft collisions. Virtual time in a bubble costs nothing when the mesh
	// settles sooner, so generous is free.
	meshSettleTimeout = gossipsim.MeshSettleTimeout
	// meshPollInterval is how often mesh sizes are re-read while waiting.
	meshPollInterval = gossipsim.MeshPollInterval
)

// envMbps reads a megabits-per-second env var and returns bits per second, or def if unset
// or unparseable.
func envMbps(name string, def int) int {
	v := os.Getenv(name)
	if v == "" {
		return def
	}
	k, err := strconv.Atoi(v)
	if err != nil || k <= 0 {
		return def
	}
	return k * simlibp2p.OneMbps
}

// meshLinks resolves the per-node access link from the standard bandwidth knobs. If either
// SEGMENT_UP_MBPS or SEGMENT_DOWN_MBPS is set the link is asymmetric (an unset side falls back
// to the symmetric rate for the uplink and a generous 10x for the downlink); otherwise the link
// is symmetric at SEGMENT_MESH_MBPS, defaulting to defaultRate. The asymmetric, upload-bound
// form is the base — see the note on asymmetricLinks.
//
// SEGMENT_BUILDER_UP_MBPS carves node 0 out as a well-provisioned builder with a symmetric
// datacenter link, leaving the rest on the validator link above. In Gloas the payload is
// published by the builder; the source is mobbed for direct IWANT pulls (not floodpublish),
// so throttling node 0 to the validator uplink manufactures a first-hop bottleneck. Unset, node
// 0 is a plain node — the local (home) builder. Node 0 must be the publisher.
//
// SEGMENT_DATACENTER_PCT makes that fraction of the remaining nodes datacenter-hosted (symmetric
// SEGMENT_DATACENTER_MBPS, default 1 Gbps), the rest on the validator link — mainnet's mix of
// hosted and home nodes. Placement is by index, which the random-regular graph scatters through
// the topology; datacenter relays speed the median but the deadline tail stays home-bound.
//
// SEGMENT_SLOW_PCT is the mirror: that fraction of the remaining nodes on a thin link
// (SEGMENT_SLOW_UP_MBPS, default 10; SEGMENT_SLOW_DOWN_MBPS, default 2x up) — stragglers whose
// segments a receiver may have to wait for, or route around.
func meshLinks(t *testing.T, n, defaultRate int) []simlibp2p.NodeLinkSettingsAndCount {
	t.Helper()
	sym := func(bps int) simnet.NodeBiDiLinkSettings {
		return simnet.NodeBiDiLinkSettings{
			Downlink: simnet.LinkSettings{BitsPerSecond: bps},
			Uplink:   simnet.LinkSettings{BitsPerSecond: bps},
		}
	}

	var validator simnet.NodeBiDiLinkSettings
	up := envMbps("SEGMENT_UP_MBPS", 0)
	down := envMbps("SEGMENT_DOWN_MBPS", 0)
	if up > 0 || down > 0 {
		if up == 0 {
			up = envMbps("SEGMENT_MESH_MBPS", defaultRate)
		}
		if down == 0 {
			down = 10 * up
		}
		validator = simnet.NodeBiDiLinkSettings{
			Downlink: simnet.LinkSettings{BitsPerSecond: down},
			Uplink:   simnet.LinkSettings{BitsPerSecond: up},
		}
	} else {
		validator = sym(envMbps("SEGMENT_MESH_MBPS", defaultRate))
	}

	// node 0 is the publisher: a datacenter builder if configured, else a plain (home) node.
	node0 := validator
	if b := envMbps("SEGMENT_BUILDER_UP_MBPS", 0); b > 0 {
		node0 = sym(b)
	}

	rest := n - 1
	dcCount, slowCount := meshLinkCounts(t, n)

	out := []simlibp2p.NodeLinkSettingsAndCount{{LinkSettings: node0, Count: 1}}
	if dcCount > 0 {
		out = append(out, simlibp2p.NodeLinkSettingsAndCount{
			LinkSettings: sym(envMbps("SEGMENT_DATACENTER_MBPS", 1000*simlibp2p.OneMbps)),
			Count:        dcCount,
		})
	}
	if slowCount > 0 {
		slowUp := envMbps("SEGMENT_SLOW_UP_MBPS", 10*simlibp2p.OneMbps)
		out = append(out, simlibp2p.NodeLinkSettingsAndCount{
			LinkSettings: simnet.NodeBiDiLinkSettings{
				Downlink: simnet.LinkSettings{BitsPerSecond: envMbps("SEGMENT_SLOW_DOWN_MBPS", 2*slowUp)},
				Uplink:   simnet.LinkSettings{BitsPerSecond: slowUp},
			},
			Count: slowCount,
		})
	}
	out = append(out, simlibp2p.NodeLinkSettingsAndCount{LinkSettings: validator, Count: rest - dcCount - slowCount})
	return out
}

// regimeRule is the oracle regime rule (E4/E5): SEGMENT_A_REGIME_RULE=1
// makes each node choose its A parameters from R, the payload's wire time on its own configured
// uplink over the propagation round trip (SEGMENT_A_REGIME_RTT_MS, default 50), instead of from
// the payload size alone: push degree r = 4 for R ≤ 0.85, 3 for R ≤ 1.3, 2 above; the request
// discipline, the offer table, the park and the tail hedge for R ≥ 1.5 only. On the 50 Mbps home
// uplink at the default round trip the thresholds fall at 256, 384 and 512 KiB of raw payload,
// which is aadapt2's size rule; a node on another link picks differently. An oracle: the uplink
// and the payload length are configuration a node does not learn from the wire, and the
// settings are fixed at construction.
type regimeRule struct {
	active bool
	rtt    time.Duration
	bytes  int
	uplink []int // bits per second, per node
}

func regimeRuleFor(t *testing.T, links []simlibp2p.NodeLinkSettingsAndCount, n int) regimeRule {
	t.Helper()
	if os.Getenv("SEGMENT_A_REGIME_RULE") == "" {
		return regimeRule{}
	}
	bytes, err := strconv.Atoi(os.Getenv("SEGMENT_PAYLOAD_BYTES"))
	if err != nil || bytes <= 0 {
		t.Fatalf("SEGMENT_A_REGIME_RULE needs SEGMENT_PAYLOAD_BYTES, got %q", os.Getenv("SEGMENT_PAYLOAD_BYTES"))
	}
	rr := regimeRule{active: true, rtt: envDuration("SEGMENT_A_REGIME_RTT_MS", 50*time.Millisecond), bytes: bytes}
	for _, l := range links {
		for c := 0; c < l.Count; c++ {
			rr.uplink = append(rr.uplink, l.LinkSettings.Uplink.BitsPerSecond)
		}
	}
	if len(rr.uplink) != n {
		t.Fatalf("regime rule: %d link settings for %d nodes", len(rr.uplink), n)
	}
	return rr
}

// R is the node's wire time for the payload over the round trip.
func (rr regimeRule) R(i int) float64 {
	if !rr.active || rr.uplink[i] <= 0 {
		return 0
	}
	return (8 * float64(rr.bytes) / float64(rr.uplink[i])) / rr.rtt.Seconds()
}

// degree is the push degree the rule picks for the node.
func (rr regimeRule) degree(i int) int {
	r := rr.R(i)
	switch {
	case r <= 0.85:
		return 4
	case r <= 1.3:
		return 3
	}
	return 2
}

// large reports whether the node is in the large-payload regime: the request discipline on.
func (rr regimeRule) large(i int) bool { return rr.active && rr.R(i) >= 1.5 }

// meshLinkCounts is the link-class arithmetic of meshLinks on its own: how many non-publisher
// nodes are datacenter-hosted and how many are on thin links. Placement is by index, the
// datacenter block first from node 1, the slow block after it; the horizon report reads the
// classes back through the same function.
func meshLinkCounts(t *testing.T, n int) (dcCount, slowCount int) {
	t.Helper()
	rest := n - 1
	if v := os.Getenv("SEGMENT_DATACENTER_PCT"); v != "" {
		pct, err := strconv.Atoi(v)
		if err != nil || pct < 0 || pct > 100 {
			t.Fatalf("bad SEGMENT_DATACENTER_PCT %q (want 0-100)", v)
		}
		dcCount = pct * rest / 100
	}
	if v := os.Getenv("SEGMENT_SLOW_PCT"); v != "" {
		pct, err := strconv.Atoi(v)
		if err != nil || pct < 0 || pct > 100 {
			t.Fatalf("bad SEGMENT_SLOW_PCT %q (want 0-100)", v)
		}
		slowCount = pct * rest / 100
	}
	if dcCount+slowCount > rest {
		t.Fatalf("SEGMENT_DATACENTER_PCT and SEGMENT_SLOW_PCT together exceed the %d non-publisher nodes", rest)
	}
	return dcCount, slowCount
}

// segmentSizeBytes returns the segment size for the run: DefaultSegmentSize (32 KiB) unless
// SEGMENT_SIZE_BYTES overrides it. Dimension 8's knob: for a 1 MiB payload, 32 KiB is K=32,
// 16 KiB is K=64, 8 KiB is K=128. Proof overhead per segment grows with depth as K rises.
func segmentSizeBytes(t *testing.T) int {
	t.Helper()
	v := os.Getenv("SEGMENT_SIZE_BYTES")
	if v == "" {
		if strictCells() {
			t.Fatal("SEGMENT_STRICT: SEGMENT_SIZE_BYTES (or SEGMENT_FIXED_COUNT) must be set; the unit is part of the arm, not a default")
		}
		return segments.DefaultSegmentSize
	}
	k, err := strconv.Atoi(v)
	if err != nil || k <= 0 {
		t.Fatalf("bad SEGMENT_SIZE_BYTES %q", v)
	}
	return k
}

// phasePubsubOpts builds the per-node options for a phase-forwarding arm: the forked
// gossipsub's phase transition at push degree r, the IHAVE budget raised to carry one
// announce per forwarded message, and the source-side staggering policy from the
// environment: SEGMENT_PHASE_SRC=immediate|delay|rotate|wide, with SEGMENT_PHASE_SRC_DELAY_MS
// (default 100) for delay and SEGMENT_PHASE_SRC_DEGREE (default 1 rotate / 8 wide) for the
// other two.
func phasePubsubOpts(t *testing.T, r int) []pubsub.Option {
	return phasePubsubOptsMatch(t, r, func(string) bool { return true })
}

// phasePubsubOptsMatch is phasePubsubOpts with an explicit topic matcher.
//
// The matcher matters as soon as a cell carries more than one kind of traffic. The two-publisher
// driver puts a block on its own topic, and the block must propagate the way a real block does --
// ordinary gossipsub, eager push to the whole mesh. Phase-forwarding it too would push it to only r
// peers and announce to the rest, making the block artificially slow and inflating exactly the
// inversion count the experiment exists to measure.
func phasePubsubOptsMatch(t *testing.T, r int, match func(string) bool) []pubsub.Option {
	t.Helper()
	// meshParams(), not p2p.GossipSubParams(): these per-node opts are applied *after* the base
	// opts, so building from the raw production params would silently override SEGMENT_MESH_D for
	// every phase arm -- which it did, leaving the mesh at D=8 while the settle band was computed
	// for the requested D.
	pp := meshParams()
	// Phase forwarding announces per message, immediately: gossipsub.go sends one IHAVE RPC per
	// (message, peer) carrying a single id, which is the point — the receiver pulls at once rather
	// than waiting for the heartbeat's gossip. But MaxIHaveMessages counts IHAVE *RPCs* received
	// from a peer within one heartbeat (700 ms here) and defaults to 10, a figure sized for stock
	// gossipsub, which announces only from the heartbeat: one RPC per peer per heartbeat carrying
	// up to MaxIHaveLength = 5000 ids. Under phase forwarding a relay sends one RPC per segment, so
	// a K = 32 group inside one heartbeat puts 32 RPCs on each link and the default drops 22 of
	// them silently. Hence the raise. The honest fix is to batch the immediate announcements per
	// peer, which the wire format already allows; SEGMENT_MAX_IHAVE_MESSAGES exists to measure what
	// the default costs, for honest relays and for a liar alike.
	pp.MaxIHaveMessages = 4096
	if v := os.Getenv("SEGMENT_MAX_IHAVE_MESSAGES"); v != "" {
		k, err := strconv.Atoi(v)
		if err != nil || k < 1 {
			t.Fatalf("bad SEGMENT_MAX_IHAVE_MESSAGES %q", v)
		}
		pp.MaxIHaveMessages = k
	}
	opts := []pubsub.Option{
		pubsub.WithGossipSubParams(pp),
		pubsub.WithPhaseForwarding(match, r),
	}
	if os.Getenv("SEGMENT_PHASE_ANNOUNCE_HOLDERS") != "" {
		opts = append(opts, pubsub.WithPhaseAnnounceHolders())
	}
	// SEGMENT_PHASE_FIXED_BUDGET=1: the split without the phase (push r, announce the rest,
	// no IDONTWANT-driven decay of r).
	if os.Getenv("SEGMENT_PHASE_FIXED_BUDGET") != "" {
		opts = append(opts, pubsub.WithPhaseFixedBudget())
	}
	// Selection policy for the pushed subset: default sel=rank; "rr" is sender-side
	// round-robin.
	switch os.Getenv("SEGMENT_PHASE_SEL") {
	case "", "rank":
	case "rr":
		opts = append(opts, pubsub.WithPhaseSelectRR())
	default:
		t.Fatalf("bad SEGMENT_PHASE_SEL %q", os.Getenv("SEGMENT_PHASE_SEL"))
	}
	if v := os.Getenv("SEGMENT_PHASE_MESHLESS"); v != "" {
		a, err := strconv.Atoi(v)
		if err != nil || a < 0 {
			t.Fatalf("bad SEGMENT_PHASE_MESHLESS %q (announce width, 0 = all)", v)
		}
		opts = append(opts, pubsub.WithPhaseMeshless(a))
	}
	mode := os.Getenv("SEGMENT_PHASE_SRC")
	if mode == "" || mode == "immediate" {
		return opts
	}
	delay := envDuration("SEGMENT_PHASE_SRC_DELAY_MS", 100*time.Millisecond)
	degree := 0
	if v := os.Getenv("SEGMENT_PHASE_SRC_DEGREE"); v != "" {
		k, err := strconv.Atoi(v)
		if err != nil || k <= 0 {
			t.Fatalf("bad SEGMENT_PHASE_SRC_DEGREE %q", v)
		}
		degree = k
	}
	var m pubsub.PhaseSourceMode
	switch mode {
	case "delay":
		m = pubsub.PhaseSourceDelay
	case "rotate":
		m = pubsub.PhaseSourceRotate
		if degree == 0 {
			degree = 1
		}
	case "wide":
		m = pubsub.PhaseSourceWidePush
		if degree == 0 {
			degree = 8
		}
	default:
		t.Fatalf("bad SEGMENT_PHASE_SRC %q", mode)
	}
	return append(opts, pubsub.WithPhaseSourcePolicy(m, delay, degree))
}

// TestTopologyGenerators checks the graphs before any of them carries an experiment. A wrong
// edge list produces a plausible-looking measurement of the wrong thing.
func TestTopologyGenerators(t *testing.T) {
	t.Run("line", func(t *testing.T) {
		es := line(5)
		require.Equal(t, 4, len(es))
		require.Equal(t, true, connected(5, es))
		d := degrees(5, es)
		require.DeepEqual(t, []int{1, 2, 2, 2, 1}, d)
		require.Equal(t, 0, len(line(1)), "a single node has no edges")
	})

	t.Run("star", func(t *testing.T) {
		es := star(5)
		require.Equal(t, 4, len(es))
		require.Equal(t, true, connected(5, es))
		d := degrees(5, es)
		require.DeepEqual(t, []int{4, 1, 1, 1, 1}, d)
	})

	t.Run("grid", func(t *testing.T) {
		es := grid(3, 3)
		// 2 horizontal per row x 3 rows, plus 2 vertical per column x 3 columns.
		require.Equal(t, 12, len(es))
		require.Equal(t, true, connected(9, es))
		d := degrees(9, es)
		require.Equal(t, 2, d[0], "corner has two neighbours")
		require.Equal(t, 4, d[4], "centre has four neighbours")
		require.Equal(t, 3, d[1], "edge has three neighbours")
	})

	t.Run("random regular is regular and connected", func(t *testing.T) {
		es, err := randomRegular(20, 8, 42)
		require.NoError(t, err)
		require.Equal(t, 20*8/2, len(es))
		require.Equal(t, true, connected(20, es))
		for i, d := range degrees(20, es) {
			require.Equal(t, 8, d, fmt.Sprintf("node %d degree", i))
		}
	})

	t.Run("random regular is deterministic in seed", func(t *testing.T) {
		a, err := randomRegular(20, 8, 42)
		require.NoError(t, err)
		b, err := randomRegular(20, 8, 42)
		require.NoError(t, err)
		require.DeepEqual(t, a, b)
		c, err := randomRegular(20, 8, 43)
		require.NoError(t, err)
		require.Equal(t, false, isEqualEdges(a, c), "a different seed should give a different graph")
	})

	t.Run("random regular rejects impossible parameters", func(t *testing.T) {
		_, err := randomRegular(5, 5, 1)
		require.NotNil(t, err, "degree must be below the node count")
		_, err = randomRegular(5, 3, 1)
		require.NotNil(t, err, "n*d must be even")
	})

	t.Run("mixing moves away from the ring it starts from", func(t *testing.T) {
		es, err := randomRegular(20, 8, 42)
		require.NoError(t, err)
		require.Equal(t, false, isEqualEdges(es, circulant(20, 8)), "swaps should have changed the graph")
	})

	t.Run("link settings", func(t *testing.T) {
		u := uniformLinks(6, 20*simlibp2p.OneMbps)
		require.Equal(t, 6, u[0].Count)
		require.Equal(t, u[0].LinkSettings.Downlink.BitsPerSecond, u[0].LinkSettings.Uplink.BitsPerSecond)

		// E3 and E5 need the sender's uplink to be the scarce resource rather than the
		// receiver's downlink -- see the note on asymmetricLinks.
		a := asymmetricLinks(6, 5*simlibp2p.OneMbps, 100*simlibp2p.OneMbps)
		require.Equal(t, true,
			a[0].LinkSettings.Uplink.BitsPerSecond < a[0].LinkSettings.Downlink.BitsPerSecond)
	})

	t.Run("connected detects a split graph", func(t *testing.T) {
		require.Equal(t, false, connected(4, []edge{newEdge(0, 1), newEdge(2, 3)}))
	})
}

// TestLatencyMatrix checks the per-pair lookup, which is the capability the harness offers and
// the experiments have not been using.
func TestLatencyMatrix(t *testing.T) {
	// Node 2 is "far": every packet touching it costs more.
	f := latencyMatrix(4, func(from, to int) time.Duration {
		if from == 2 || to == 2 {
			return 100 * time.Millisecond
		}
		return 10 * time.Millisecond
	})
	addr := func(i int) net.Addr {
		return &net.UDPAddr{IP: simnet.IntToPublicIPv4(i), Port: 8000}
	}
	require.Equal(t, 10*time.Millisecond, f(&simnet.Packet{From: addr(0), To: addr(1)}))
	require.Equal(t, 100*time.Millisecond, f(&simnet.Packet{From: addr(0), To: addr(2)}))
	require.Equal(t, 100*time.Millisecond, f(&simnet.Packet{From: addr(2), To: addr(3)}))

	// An endpoint outside the matrix falls back rather than reporting no latency at all.
	unknown := &net.UDPAddr{IP: net.IPv4(203, 0, 113, 9), Port: 8000}
	require.Equal(t, defaultLatency, f(&simnet.Packet{From: addr(0), To: unknown}))
}

// TestTopologyScaleCost stands up a degree-8 topology and records the cost of *existing*.
//
// **This is not a capacity measurement, and it has been misread as one three times.** It pushes a
// five-byte message, so its ~89 goroutines and ~0.9 MiB per node cover standing the topology up and
// nothing else. Carrying a real payload costs about **6.6 MiB per node** -- measured at n=500 in
// TestQ6RealisticMesh, seven times higher -- because in-flight segments, reassembly buffers and
// per-peer queues all scale with the transfer, not with the graph.
//
// Use this to check the graph forms. For "how large a network can we drive", use Q6's figures.
// Do not read a latency number out of this test either: a five-byte message measures mesh state.
//
// The resource figures are logged rather than asserted -- they are process-wide, so other tests in
// this binary contribute -- but they are the right order of magnitude for what they do cover.
func TestTopologyScaleCost(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const nodes = 30
		edges, err := randomRegular(nodes, 8, 7)
		require.NoError(t, err)
		nw, stop := newSimNetwork(t, networkConfig{
			links: uniformLinks(nodes, 20*simlibp2p.OneMbps),
			edges: edges,
		})
		defer stop()

		topicStr := segmentTopic()
		topics := make([]*pubsub.Topic, nodes)
		subs := make([]*pubsub.Subscription, nodes)
		for i, ps := range nw.Pubsubs {
			topic, err := ps.Join(topicStr)
			require.NoError(t, err)
			registerProcValidator(t, ps, topicStr)
			topics[i] = topic
			sub, err := topic.Subscribe()
			require.NoError(t, err)
			defer sub.Cancel()
			subs[i] = sub
		}
		time.Sleep(meshFormation)
		synctest.Wait()

		var ms runtime.MemStats
		runtime.ReadMemStats(&ms)
		t.Logf("%d nodes at degree 8: %d goroutines, %d MiB heap (process-wide)",
			nodes, runtime.NumGoroutine(), ms.HeapAlloc>>20)

		// Every node receives, which is the assertion that makes the cost figure meaningful:
		// a graph that came up but does not deliver would cost the same.
		payload := []byte("scale cost probe")
		require.NoError(t, topics[0].Publish(context.Background(), payload))
		for i := 1; i < nodes; i++ {
			msg, err := subs[i].Next(context.Background())
			require.NoError(t, err)
			require.DeepEqual(t, payload, msg.Data, fmt.Sprintf("node %d", i))
		}
	})
}

// TestMultiNodeTopologyForms is the smoke test for the helper itself: a topology larger than two
// nodes comes up, meshes, and carries a message to a node that is not adjacent to the publisher.
// It says nothing about timing.
func TestMultiNodeTopologyForms(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const nodes = 4
		nw, stop := newSimNetwork(t, networkConfig{
			links: uniformLinks(nodes, 20*simlibp2p.OneMbps),
			edges: line(nodes),
		})
		defer stop()
		require.Equal(t, nodes, nw.Len())

		topicStr := segmentTopic()
		topics := make([]*pubsub.Topic, nodes)
		subs := make([]*pubsub.Subscription, nodes)
		for i, ps := range nw.Pubsubs {
			topic, err := ps.Join(topicStr)
			require.NoError(t, err)
			registerProcValidator(t, ps, topicStr)
			topics[i] = topic
			sub, err := topic.Subscribe()
			require.NoError(t, err)
			defer sub.Cancel()
			subs[i] = sub
		}
		time.Sleep(meshFormation)
		synctest.Wait()

		// Publish at one end and read at the other: node 3 has no edge to node 0, so anything
		// it receives was forwarded, which is what makes this a topology test.
		payload := []byte("segment topology smoke test")
		require.NoError(t, topics[0].Publish(context.Background(), payload))

		msg, err := subs[nodes-1].Next(context.Background())
		require.NoError(t, err)
		require.DeepEqual(t, payload, msg.Data)
	})
}

// requireSlowTests skips a measurement that is too expensive for the package's Bazel budget.
//
// The package is size = "small" (60 s), and the wall-clock sweeps run for minutes: real time is
// the price of leaving the synctest bubble. They are measurements, not regression tests, so they
// are opt-in rather than trimmed until meaningless.
//
//	SEGMENT_SLOW_TESTS=1 go test ./beacon-chain/p2p/segmentintegrationtest/ -run <name> -v -timeout 30m
func requireSlowTests(t *testing.T) {
	t.Helper()
	if os.Getenv("SEGMENT_SLOW_TESTS") == "" {
		t.Skip("set SEGMENT_SLOW_TESTS=1 to run wall-clock measurements (minutes, not seconds)")
	}
}

// connectivityDegree returns the graph degree to build at, honouring SEGMENT_DEGREE.
//
// Clamped below n so randomRegular can satisfy it, and nudged to keep n*d even, which a regular
// graph requires.
func connectivityDegree(t *testing.T, n int) int {
	t.Helper()
	d := defaultConnectivityDegree
	if v := os.Getenv("SEGMENT_DEGREE"); v != "" {
		parsed, err := strconv.Atoi(v)
		if err != nil {
			t.Fatalf("bad SEGMENT_DEGREE %q: %v", v, err)
		}
		d = parsed
	}
	if d >= n {
		d = n - 1
	}
	// A regular graph needs n*d even, and an odd degree additionally needs an even n.
	if n*d%2 != 0 {
		d--
	}
	if d < 2 {
		d = 2
	}
	return d
}

// meshGraph builds the connectivity graph for a mesh experiment.
//
// Use this rather than calling randomRegular with a literal degree: the whole point is that the
// number chosen here is *not* D, and a literal at the call site is how that invariant was lost in
// the first place.
func meshGraph(t *testing.T, n int) []edge {
	t.Helper()
	d := connectivityDegree(t, n)
	require.Equal(t, true, d > gossipDhi,
		fmt.Sprintf("connectivity degree %d must exceed Dhi=%d, or every peer can be a mesh peer "+
			"and there are no non-mesh peers to gossip or announce to", d, gossipDhi))
	edges, err := randomRegular(n, d, meshGraphSeed(t))
	require.NoError(t, err)
	return edges
}

// failWithholdSet selects the F1 withholding nodes: SEGMENT_FAIL_WITHHOLD_PCT percent of
// the non-publisher population, hash-ranked and stateless (see failSelect).
func failWithholdSet(t *testing.T, n int) map[int]bool {
	return failSelect(t, n, "SEGMENT_FAIL_WITHHOLD_PCT", 0xF1F1F1F1F1F1F1F1)
}

// failSpoofSet selects the IDONTWANT spoofers, SEGMENT_FAIL_IDW_SPOOF_PCT percent, on their
// own domain so the set is independent of the withholders' at equal percentages.
func failSpoofSet(t *testing.T, n int) map[int]bool {
	return failSelect(t, n, "SEGMENT_FAIL_IDW_SPOOF_PCT", 0xD07D07D07D07D07D)
}

// narrowHolders reads SEGMENT_FAIL_PUBLISH_NARROW=w:h, the last-piece screen's placement: the
// publisher does not send the last w ids; h placed honest holders publish them instead. The
// holders are the first h honest non-publisher nodes of the slow block when the cell has one
// (honest holders on thin uplinks, where a hedged burst hurts), else of the whole population.
func narrowHolders(t *testing.T, n int) (w int, holders []int) {
	t.Helper()
	v := os.Getenv("SEGMENT_FAIL_PUBLISH_NARROW")
	if v == "" {
		return 0, nil
	}
	var h int
	if _, err := fmt.Sscanf(v, "%d:%d", &w, &h); err != nil || w <= 0 || h <= 0 {
		t.Fatalf("bad SEGMENT_FAIL_PUBLISH_NARROW %q (want w:h)", v)
	}
	attacker, spoof := failWithholdSet(t, n), failSpoofSet(t, n)
	dcCount, slowCount := meshLinkCounts(t, n)
	lo, hi := 1, n
	if slowCount > 0 {
		lo, hi = 1+dcCount, 1+dcCount+slowCount
	}
	for i := lo; i < hi && len(holders) < h; i++ {
		if attacker[i] || spoof[i] {
			continue
		}
		holders = append(holders, i)
	}
	if len(holders) < h {
		t.Fatalf("SEGMENT_FAIL_PUBLISH_NARROW: only %d honest holders available for %d", len(holders), h)
	}
	return w, holders
}

// failPublishOmit selects w shard indices the publisher never sends (F5 source omission).
// Hash-ranked from a domain-separated fail seed so the set is deterministic and stable across
// arms at a given w. For a coded arm with margin R,
// w <= R is tolerated by construction; for an uncoded arm any w >= 1 leaves the omitted
// segments unreachable, so completion is right-censored — the positive control that the
// harness detects permanent omission as non-completion, not as a slow tail.
func failPublishOmit(t *testing.T, total int) map[int]bool {
	t.Helper()
	v := os.Getenv("SEGMENT_FAIL_PUBLISH_WITHHOLD")
	if v == "" {
		return nil
	}
	w, err := strconv.Atoi(v)
	if err != nil || w <= 0 || w >= total {
		t.Fatalf("bad SEGMENT_FAIL_PUBLISH_WITHHOLD %q (1..%d)", v, total-1)
	}
	seed := mix64(meshGraphSeed(t) ^ 0xF5F5F5F5F5F5F5F5)
	idxs := make([]int, total)
	for i := range idxs {
		idxs[i] = i
	}
	sort.Slice(idxs, func(a, b int) bool {
		return mix64(seed^uint64(idxs[a])) > mix64(seed^uint64(idxs[b]))
	})
	out := make(map[int]bool, w)
	for _, i := range idxs[:w] {
		out[i] = true
	}
	t.Cleanup(func() { t.Logf("failure exposure: publisher omitted %d of %d shards", w, total) })
	return out
}

// failSelect returns the fixed-size non-publisher node set for a failure mode: pct percent
// of n-1, ranked by hash(domain seed, index), stateless and identical across arms
// (a determinism requirement). domain separates one mode's set from another's
// at the same seed. Returns nil when pctEnv is unset.
func failSelect(t *testing.T, n int, pctEnv string, domain uint64) map[int]bool {
	t.Helper()
	v := os.Getenv(pctEnv)
	if v == "" {
		return nil
	}
	pct, err := strconv.Atoi(v)
	if err != nil || pct <= 0 || pct >= 100 {
		t.Fatalf("bad %s %q", pctEnv, v)
	}
	seed := mix64(meshGraphSeed(t) ^ domain)
	count := (pct*(n-1) + 50) / 100
	if count < 1 {
		count = 1
	}
	idxs := make([]int, 0, n-1)
	for i := 1; i < n; i++ {
		idxs = append(idxs, i)
	}
	sort.Slice(idxs, func(a, b int) bool {
		return mix64(seed^uint64(idxs[a])) > mix64(seed^uint64(idxs[b]))
	})
	out := make(map[int]bool, count)
	for _, i := range idxs[:count] {
		out[i] = true
	}
	return out
}

// failSendDelaySet selects the F2b slowed-writer nodes.
func failSendDelaySet(t *testing.T, n int) map[int]bool {
	return failSelect(t, n, "SEGMENT_FAIL_SEND_PCT", 0xF2B0F2B0F2B0F2B0)
}

// failureDeadline is the slot-relevant completion deadline that rate@ reports against,
// 4 s unless SEGMENT_DEADLINE_MS overrides it (phase 0). It does not stop the run — completion
// beyond it still lands in the CDF; censoring happens only at the harness timeout.
func failureDeadline(t *testing.T) time.Duration {
	t.Helper()
	v := os.Getenv("SEGMENT_DEADLINE_MS")
	if v == "" {
		return 4 * time.Second
	}
	ms, err := strconv.Atoi(v)
	if err != nil || ms <= 0 {
		t.Fatalf("bad SEGMENT_DEADLINE_MS %q", v)
	}
	return time.Duration(ms) * time.Millisecond
}

// announceStats is the cell's announce-rate histogram (measurement only): every node's router
// folds its per-(peer, heartbeat) control-RPC counts into it at each heartbeat, the driver resets
// it at publish so it describes the measured diffusion, and it is read after the run — no
// round-trip into any router, so it cannot perturb what it measures.
var announceStats = &pubsub.IHaveRPCStats{}

// meshGraphSeed is the seed every mesh experiment uses, 7 unless SEGMENT_SEED overrides it.
// One seed per run is a known limitation; the override is what a multi-seed sweep varies, so
// cells with different seeds differ in topology but nothing else.
func meshGraphSeed(t *testing.T) uint64 {
	t.Helper()
	v := os.Getenv("SEGMENT_SEED")
	if v == "" {
		return 7
	}
	k, err := strconv.ParseUint(v, 10, 64)
	if err != nil {
		t.Fatalf("bad SEGMENT_SEED %q: %v", v, err)
	}
	return k
}

// oneWayBetween is the modelled one-way latency between two nodes under the configured latency
// model. Under the uniform model every pair is defaultLatency; under geo it is the pair's own
// value, which is what makes builder-proposer distance a controllable parameter rather than a
// per-seed accident.
func oneWayBetween(t *testing.T, n, i, j int) time.Duration {
	t.Helper()
	if os.Getenv("SEGMENT_LATENCY_MODEL") != "geo" {
		return defaultLatency
	}
	_, _, pair := geoLatencyPair(t, meshGraphSeed(t), n)
	return pair(i, j)
}

// nodeAtDistance picks the node whose modelled one-way latency from `from` is smallest or largest,
// skipping `from` itself. It turns builder-proposer placement into a swept parameter: the first
// sweep saw reveal times swing 59-169 ms within one arm purely because the graph happened to put
// the builder near or far from the proposer.
func nodeAtDistance(t *testing.T, n, from int, farthest bool) int {
	t.Helper()
	best, bestD := -1, time.Duration(0)
	for k := range n {
		if k == from {
			continue
		}
		d := oneWayBetween(t, n, from, k)
		if best < 0 || (farthest && d > bestD) || (!farthest && d < bestD) {
			best, bestD = k, d
		}
	}
	return best
}
