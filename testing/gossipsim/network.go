package gossipsim

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/OffchainLabs/prysm/v7/testing/require"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/net/connmgr"
	simlibp2p "github.com/libp2p/go-libp2p/x/simlibp2p"
	"github.com/marcopolo/simnet"
)

// NetworkConfig describes a topology to bring up.
//
// Everything a study varies by environment variable -- which failure is injected, which fork
// knob is swept, which latency model is selected -- reaches this struct as an already-decided
// value through PubsubOpts and PerNodeOpts. That is deliberate: the builder does not read the
// environment, so an experiment's configuration is visible at its call site rather than
// scattered through the harness.
type NetworkConfig struct {
	// Links describes per-node access links in node-index order. Node count is the sum of the
	// group counts, and must cover every index the edges refer to.
	Links []simlibp2p.NodeLinkSettingsAndCount
	// Edges is the connectivity graph, from one of the generators in topology.go.
	Edges []Edge
	// Latency defaults to a static DefaultLatency when nil.
	Latency simlibp2p.LatencyFunc
	// PubsubOpts is appended to the options every node gets.
	PubsubOpts []pubsub.Option
	// PerNodeOpts adds options for one node only, which is how a per-node tracer is attached
	// and how per-node failure injection is expressed.
	PerNodeOpts func(idx int) []pubsub.Option
	// Overrides replaces the production values an experiment sweeps. Build it with
	// NewOverrides; a zero value is normalised to production by New.
	Overrides Overrides
	// LibraryDefaults opts out of ProductionPubsubOpts. Only for a test that is deliberately
	// about library behaviour -- any measurement wants the production configuration.
	LibraryDefaults bool
	// WallClock runs outside a synctest bubble, on the real clock.
	//
	// synctest is only a win where virtual time exceeds the real time spent simulating it. Its
	// cost is roughly (timer events) x (goroutines in the bubble), and at ~89 goroutines per
	// node plus libp2p's housekeeping tickers that grows fast: a 1 MiB payload over five hops at
	// 20 ms one-way did not finish in fifteen minutes while pegging a core, for about 2.5 s of
	// virtual time. On the real clock the same run costs the 2.5 s it simulates.
	//
	// simnet rate-limits with ordinary timers, so bandwidth and latency are still simulated
	// faithfully. What is given up is determinism and the ability to skip idle time -- so use
	// this for high-latency or many-hop configurations, and for anything measuring CPU, which
	// virtual time cannot see at all.
	WallClock bool
}

// Network is a running topology with gossipsub on every node.
type Network struct {
	Hosts   []host.Host
	Pubsubs []*pubsub.PubSub
}

func (s *Network) Len() int { return len(s.Hosts) }

// New brings up the topology, starts gossipsub on every node and dials every edge.
//
// The returned stop function must be deferred *inside* the synctest bubble. t.Cleanup runs after
// the bubble exits, and a bubble whose goroutines are still blocked panics as a deadlock --
// gossipsub keeps a process loop and validation workers running until its context is cancelled.
func New(t *testing.T, cfg NetworkConfig) (*Network, func()) {
	t.Helper()
	n := 0
	for _, l := range cfg.Links {
		n += l.Count
	}
	require.Equal(t, true, n > 0, "NetworkConfig needs at least one node")
	for _, e := range cfg.Edges {
		require.Equal(t, true, e.A >= 0 && e.B < n, fmt.Sprintf("edge %v outside %d nodes", e, n))
	}

	latency := cfg.Latency
	if latency == nil {
		latency = simnet.StaticLatency(DefaultLatency)
	}
	cfg.Links = stampBurstWindow(cfg.Links, cfg.WallClock)
	settings := simlibp2p.NetworkSettings{UseBlankHost: true}
	if cfg.WallClock {
		// Teardown is real time here, and libp2p's default connection manager holds a ten second
		// grace period per host. Across a sweep of many small cells that dominates everything
		// being measured, so supply a manager that closes promptly.
		settings.BlankHostOptsForHostIdx = func(int) simlibp2p.BlankHostOpts {
			cm, err := connmgr.NewConnManager(100, 200, connmgr.WithGracePeriod(10*time.Millisecond))
			require.NoError(t, err)
			return simlibp2p.BlankHostOpts{ConnMgr: cm}
		}
	}
	network, meta, err := simlibp2p.SimpleLibp2pNetwork(cfg.Links, latency, settings)
	require.NoError(t, err)
	network.Start()
	Settle(cfg.WallClock)

	ctx, cancel := context.WithCancel(context.Background())
	opts := []pubsub.Option{
		pubsub.WithMessageSigning(false),
		pubsub.WithStrictSignatureVerification(false),
	}
	if !cfg.LibraryDefaults {
		overrides := cfg.Overrides
		// A caller that left Overrides at its zero value means production, and production for
		// the IDONTWANT threshold is the negative sentinel rather than zero.
		if overrides == (Overrides{}) {
			overrides = NewOverrides()
		}
		opts = append(opts, ProductionPubsubOptsWith(overrides)...)
	}
	opts = append(opts, cfg.PubsubOpts...)

	out := &Network{Hosts: meta.Nodes, Pubsubs: make([]*pubsub.PubSub, n)}
	for i, h := range meta.Nodes {
		nodeOpts := opts
		if cfg.PerNodeOpts != nil {
			nodeOpts = append(append([]pubsub.Option{}, opts...), cfg.PerNodeOpts(i)...)
		}
		ps, err := pubsub.NewGossipSub(ctx, h, nodeOpts...)
		require.NoError(t, err)
		out.Pubsubs[i] = ps
	}

	// Dial in edge-list order, which SortedEdges made deterministic.
	for _, e := range cfg.Edges {
		to := meta.Nodes[e.B]
		require.NoError(t, meta.Nodes[e.A].Connect(
			context.Background(), peer.AddrInfo{ID: to.ID(), Addrs: to.Addrs()}))
	}
	Settle(cfg.WallClock)

	stop := func() {
		cancel()
		for _, h := range meta.Nodes {
			_ = h.Close()
		}
		network.Close()
		Settle(cfg.WallClock)
	}
	return out, stop
}

// Settle waits for the network to go quiet. Inside a bubble that is exact; on the real clock it
// is a bounded sleep, which is the honest cost of leaving the bubble.
func Settle(wallClock bool) {
	if wallClock {
		time.Sleep(50 * time.Millisecond)
		return
	}
	synctest.Wait()
}

// JoinAndSubscribe joins and subscribes every node to one topic, returning both.
//
// Subscribing matters even for a node that only publishes: gossipsub routes a publish from a
// non-subscribed topic through fanout rather than the mesh, so no GRAFT happens and mesh
// behaviour is not what gets measured. A real Prysm node subscribes to the topics it publishes
// on, so every experiment should subscribe everywhere.
//
// The returned cancel function must be deferred *inside* the synctest bubble, for the same
// reason New's stop must be: t.Cleanup runs after the bubble exits, and touching bubbled state
// from there is the documented way to turn this into a deadlock panic.
func JoinAndSubscribe(t *testing.T, nw *Network, topicStr string, wallClock bool) ([]*pubsub.Topic, []*pubsub.Subscription, func()) {
	t.Helper()
	topics := make([]*pubsub.Topic, nw.Len())
	subs := make([]*pubsub.Subscription, nw.Len())
	for i, ps := range nw.Pubsubs {
		topic, err := ps.Join(topicStr)
		require.NoError(t, err)
		// A generous subscription buffer. The default is small, and a batch publish of many
		// messages delivers all of them to the publisher's *own* subscription synchronously --
		// faster than any consumer goroutine can drain -- so the buffer overflows and pubsub
		// reports UndeliverableMessage. That is a property of the harness, not of the network,
		// but it reads identically to a slow link. Observed as 95 drops at K=128 on two nodes.
		sub, err := topic.Subscribe(pubsub.WithBufferSize(SubscriptionBuffer))
		require.NoError(t, err)
		topics[i], subs[i] = topic, sub
	}
	time.Sleep(MeshFormation)
	Settle(wallClock)
	cancel := func() {
		for _, s := range subs {
			s.Cancel()
		}
	}
	return topics, subs, cancel
}

// StartBackgroundTraffic makes every node flood a separate gossip topic with random-entropy
// messages for the run's duration, so the payload under study competes with co-resident load for
// the same per-node uplinks and outbound queues -- the pressure a single payload cannot create
// (F2b finding, standing in for attestation, blob and DAS gossip). interval and msgBytes set each
// node's own publish cadence and size; the aggregate seen by any node is that times its mesh
// in-degree, as with real gossip. Returns a stop func and a per-node sent-bytes counter for
// verified exposure. A non-positive interval is a no-op.
//
// The publisher goroutines live in the caller's synctest bubble and must be stopped before it
// exits: cancel the returned context, then the caller's own defer wg.Wait already drains them.
// The background context is derived from the passed ctx so a run timeout also tears them down.
func StartBackgroundTraffic(t *testing.T, nw *Network, ctx context.Context, wg *sync.WaitGroup, bgTopic string, interval time.Duration, msgBytes int) (sent *atomic.Int64, stop func()) {
	t.Helper()
	if interval <= 0 {
		return nil, func() {}
	}
	if msgBytes <= 0 {
		msgBytes = 8 << 10
	}
	bgCtx, cancel := context.WithCancel(ctx)
	var counter atomic.Int64
	// Every node joins, subscribes (drained, or the buffer overflows and reads as loss), and
	// publishes on its own cadence. Payloads are per-(node,seq) random so snappy cannot
	// collapse them -- a compressible background would measure nothing.
	for i, ps := range nw.Pubsubs {
		th, err := ps.Join(bgTopic)
		require.NoError(t, err)
		sub, err := th.Subscribe(pubsub.WithBufferSize(SubscriptionBuffer))
		require.NoError(t, err)
		wg.Add(2)
		go func() {
			defer wg.Done()
			for {
				if _, err := sub.Next(bgCtx); err != nil {
					return
				}
			}
		}()
		go func(idx int, topic *pubsub.Topic) {
			defer wg.Done()
			seq := 0
			for {
				select {
				case <-bgCtx.Done():
					return
				case <-time.After(interval):
				}
				msg := HighEntropyPayload(msgBytes, uint64(idx)<<32|uint64(seq))
				seq++
				if err := topic.Publish(bgCtx, msg); err != nil {
					return
				}
				counter.Add(int64(len(msg)))
			}
		}(i, th)
	}
	// Let the background mesh form and reach steady state before the caller publishes.
	time.Sleep(MeshFormation)
	Settle(false)
	return &counter, cancel
}

// MeshSettleTimeout bounds the wait for grafting to finish. It must exceed gossipsub's
// PruneBackoff (one minute): a node that gets pruned while grafting collects backoffs and
// sits below Dlo until they expire, which is a transient, not a failure. A previous 30 s
// deadline was shorter than the backoff and produced a nondeterministic one-node-below-Dlo
// fatal that read exactly like a diffusion stall -- six occurrences across every arm, all
// passing identical re-runs, growing with n because more nodes means more graft collisions.
// Virtual time in a bubble costs nothing when the mesh settles sooner, so generous is free.
const MeshSettleTimeout = 3 * time.Minute

// MeshPollInterval is how often mesh sizes are re-read while waiting.
const MeshPollInterval = 100 * time.Millisecond

// AwaitMesh waits until every node has grafted a mesh of a plausible size, and returns the sizes.
//
// This replaces sleeping for MeshFormation. Sleeping was always a guess, and a wrong guess is
// silent: an earlier revision published into an empty mesh and reported it as a scaling ceiling.
// Asserting turns "we waited long enough" into "the precondition holds", and the returned sizes
// are worth logging -- they are the direct evidence that connectivity degree and mesh degree
// differ.
//
// gsp is the caller's gossipsub parameters, so a study that overrides the mesh degree settles
// against its own band rather than production's.
func AwaitMesh(t *testing.T, tracers []*RecordingTracer, wallClock bool, gsp pubsub.GossipSubParams) []int {
	t.Helper()
	deadline := time.Now().Add(MeshSettleTimeout)
	sizes := make([]int, len(tracers))
	for {
		settled := 0
		for i, tr := range tracers {
			sizes[i] = tr.MeshPeers()
			// Dlo..Dhi is the band gossipsub maintains. Accepting the whole band rather than
			// insisting on exactly D is deliberate: D is a target the heartbeat converges on,
			// not an invariant, and demanding it would make the wait flaky rather than correct.
			if sizes[i] >= gsp.Dlo && sizes[i] <= gsp.Dhi {
				settled++
			}
		}
		if settled == len(tracers) {
			return sizes
		}
		if time.Now().After(deadline) {
			low, high := 0, 0
			for _, size := range sizes {
				if size < gsp.Dlo {
					low++
				} else if size > gsp.Dhi {
					high++
				}
			}
			t.Fatalf("mesh did not settle in %v: %d/%d nodes in [%d,%d], %d below, %d above; sizes %v",
				MeshSettleTimeout, settled, len(tracers), gsp.Dlo, gsp.Dhi, low, high, sizes)
		}
		time.Sleep(MeshPollInterval)
		Settle(wallClock)
	}
}

// AwaitTopicMesh is AwaitMesh for one topic, and with an explicit band.
//
// Two reasons it cannot reuse AwaitMesh. It reads MeshPeersInTopic rather than MeshPeers, which
// counts distinct peers across all topics and so saturates at the connectivity degree as soon as
// a peer is grafted anywhere. And the band is the caller's, because a topic with fewer
// subscribers than D can never reach Dlo: a subnet of k subscribers settles at k-1 mesh peers,
// which is correct rather than unsettled, and gossipsub's own band does not describe it.
//
// Nodes with no subscription to the topic are skipped, which is what `subscribed` says.
func AwaitTopicMesh(t *testing.T, tracers []*RecordingTracer, wallClock bool, topic string, subscribed func(i int) bool, lo, hi int) []int {
	t.Helper()
	deadline := time.Now().Add(MeshSettleTimeout)
	sizes := make([]int, len(tracers))
	for {
		settled, want := 0, 0
		for i, tr := range tracers {
			if !subscribed(i) {
				sizes[i] = -1
				continue
			}
			want++
			sizes[i] = tr.MeshPeersInTopic(topic)
			if sizes[i] >= lo && sizes[i] <= hi {
				settled++
			}
		}
		if settled == want {
			return sizes
		}
		if time.Now().After(deadline) {
			t.Fatalf("mesh for %q did not settle in %v: %d/%d subscribers in [%d,%d]; sizes %v",
				topic, MeshSettleTimeout, settled, want, lo, hi, sizes)
		}
		time.Sleep(MeshPollInterval)
		Settle(wallClock)
	}
}

// SummariseMesh reduces a list of mesh sizes to min/median/max, so a 500-node log line stays
// readable while still showing the spread that matters.
func SummariseMesh(sizes []int) string {
	if len(sizes) == 0 {
		return "none"
	}
	sorted := slices.Clone(sizes)
	slices.Sort(sorted)
	return fmt.Sprintf("min %d, median %d, max %d over %d nodes",
		sorted[0], sorted[len(sorted)/2], sorted[len(sorted)-1], len(sorted))
}
