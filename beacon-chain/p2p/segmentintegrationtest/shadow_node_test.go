package segmentintegrationtest

// Shadow cross-check: one harness node per process under the Shadow simulator.
//
// TestShadowNode is the node. It resolves the cell from the same environment the fleet runner
// hands the in-process harness -- the arm builders, the knob-to-option mapping, the tracer and the
// completion rule are the ones behind the published figures -- and replaces only what the
// simulator provides. The generic half (the node's schedule and host on real sockets, TCP+yamux or
// UDP+quic-go, the topology export format and the report line) is eth-networking-lab's shadowsim
// package; this file is the study's half: which cell, which arm, what counts as complete.
//
// TestShadowExport writes the cell's topology -- the graph, the per-node links and the one-way
// latency matrix -- for the lab's gen_shadow.py to turn into Shadow's network graph, so the
// simulated network is the harness's network by construction rather than by re-implementation.
// testing/shadowstudy/README.md has the mechanics.

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/OffchainLabs/prysm/v7/config/params"
	"github.com/OffchainLabs/prysm/v7/testing/require"
	"github.com/cskiraly/eth-networking-lab/shadowsim"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
)

// shadowN is the cell's node count: SEGMENT_MESH_SIZES, one entry, as the fleet runner sets it.
func shadowN(t *testing.T) int {
	t.Helper()
	v := os.Getenv("SEGMENT_MESH_SIZES")
	k, err := strconv.Atoi(v)
	if err != nil || k < 2 {
		t.Fatalf("SEGMENT_MESH_SIZES must be one node count >= 2 for a Shadow cell, got %q", v)
	}
	return k
}

// shadowSchedule is this node's timetable from the SHADOW_* variables; the hold defaults to the
// cell's failure deadline plus a margin.
func shadowSchedule(t *testing.T) shadowsim.Schedule {
	t.Helper()
	s, err := shadowsim.ScheduleFromEnv(shadowN(t), failureDeadline(t)+5*time.Second)
	require.NoError(t, err)
	return s
}

// shadowEdges is the cell's graph: the harness's mesh graph, or a line or star under SHADOW_EDGES
// for the known-answer cells, which the mesh builder's degree floor would refuse.
func shadowEdges(t *testing.T, n int) []edge {
	t.Helper()
	switch os.Getenv(shadowsim.EnvEdges) {
	case "":
		return meshGraph(t, n)
	case "line":
		return line(n)
	case "star":
		return star(n)
	default:
		t.Fatalf("bad %s %q", shadowsim.EnvEdges, os.Getenv(shadowsim.EnvEdges))
	}
	return nil
}

// shadowRefuse fails on the driver features a one-process node cannot honour, rather than
// running a cell that silently differs from its in-process twin: the transport warm-ups (simnet's
// congestion window is not quic-go's over a real socket), the warm-up arm and the background load
// (both need the driver's drain between nodes), the last-piece screen's placed holders (other
// nodes publish) and the authority gates (armed by the driver at the publish instant).
func shadowRefuse(t *testing.T, n int, seed uint64) {
	t.Helper()
	for _, k := range []string{"SEGMENT_INITIAL_CWND_PACKETS", "SEGMENT_WARMUP_BYTES", "SEGMENT_WARMUP", "SEGMENT_BG_INTERVAL_MS"} {
		if os.Getenv(k) != "" {
			t.Fatalf("%s is not supported under Shadow", k)
		}
	}
	if w, _ := narrowHolders(t, n); w > 0 {
		t.Fatal("SEGMENT_FAIL_PUBLISH_NARROW is not supported under Shadow")
	}
	if newSegmentGates(t, n, seed, false) != nil {
		t.Fatal("the authority gates are not supported under Shadow")
	}
}

// shadowRun is one node's part of a variant once it has joined: the publisher's publish, a
// receiver's watch (the publisher's only drains), and the teardown.
type shadowRun struct {
	publish func(ctx context.Context, t *testing.T) error
	watch   func(ctx context.Context, onDone func())
	cancel  func()
	extra   func() map[string]int64 // the variant's own counters for the record; nil for none
}

// shadowCell is the cell's variant as one node sees it. SHADOW_VARIANT (arms.sh's VARIANT)
// selects it: a (default) is variant A on the Q6 knobs, b is variant B's announce-then-pull, c
// is variant C's topic per segment. label is the record's arm field, arm the wire form the
// record counts (B's is its first group, for the sizes; B's own counters go in the extras).
type shadowCell struct {
	label    string
	arm      wireArm
	v        variant // pubsubOpts, per node
	join     func(t *testing.T, ps *pubsub.PubSub, self peer.ID, idx int, seed uint64) shadowRun
	declined func(idx int) int64
}

func shadowCellFromEnv(t *testing.T, n, idx int) shadowCell {
	t.Helper()
	rate := envMbps("SEGMENT_UP_MBPS", defaultRate)
	switch os.Getenv("SHADOW_VARIANT") {
	case "", "a":
		cell := q6CellFromEnv(t)
		if len(cell.arms) != 1 {
			t.Fatalf("a Shadow cell runs one arm, SEGMENT_ARMS names %d", len(cell.arms))
		}
		v := cell.variant(t, cell.arms[0], rate, defaultLatency, n)
		return shadowCell{
			label: cell.arms[0], arm: v.arm, v: v,
			join: func(t *testing.T, ps *pubsub.PubSub, _ peer.ID, idx int, _ uint64) shadowRun {
				topic, err := ps.Join(segmentTopic())
				require.NoError(t, err)
				sub, err := topic.Subscribe(pubsub.WithBufferSize(subscriptionBuffer))
				require.NoError(t, err)
				return shadowRun{
					publish: func(ctx context.Context, t *testing.T) error {
						return v.publishFrom(ctx, topic, ps, failPublishOmit(t, len(v.arm.msgs)))
					},
					watch: func(ctx context.Context, onDone func()) {
						if idx == 0 {
							return // the publisher's subscription is read by nobody, as in the driver
						}
						go func() {
							if v.watchNode(ctx, idx, ps, sub) {
								onDone()
							}
						}()
					},
					cancel: sub.Cancel,
				}
			},
			declined: func(idx int) int64 {
				if v.stopPull {
					return v.gate(idx).declined.Load()
				}
				return 0
			},
		}
	case "c":
		cell := cCellFromEnv(t)
		custodies := cell.custodies()
		if len(custodies) != 1 {
			t.Fatalf("a Shadow cell runs one custody margin, SEGMENT_SUB_EXTRA names %d", len(custodies))
		}
		v := cell.variant(custodies[0])
		return shadowCell{
			label: "variantC", arm: v.arm, v: v,
			join: func(t *testing.T, ps *pubsub.PubSub, _ peer.ID, idx int, seed uint64) shadowRun {
				topics, subs := v.joinNode(t, ps, idx, seed)
				var wg sync.WaitGroup // drained by the context at the end of the node
				return shadowRun{
					publish: func(ctx context.Context, t *testing.T) error {
						return v.publishFrom(ctx, topics, ps, failPublishOmit(t, len(v.arm.msgs)))
					},
					watch: func(ctx context.Context, onDone func()) {
						if idx == 0 {
							drainSubs(ctx, &wg, subs)
							return
						}
						v.watchNode(ctx, &wg, subs, onDone)
					},
					cancel: func() {
						for _, s := range subs {
							s.Cancel()
						}
					},
				}
			},
			declined: func(int) int64 { return 0 },
		}
	case "b":
		cell := bCellFromEnv(t)
		if len(cell.policies) != 1 || len(cell.replications) != 1 || len(cell.parities) != 1 {
			t.Fatalf("a Shadow cell runs one policy, replication and parity; got %d, %d, %d",
				len(cell.policies), len(cell.replications), len(cell.parities))
		}
		v := cell.variant(t, n, cell.policies[0], cell.replications[0], cell.parities[0])
		// This node's broadcaster only, in the slot pubsubOpts reads; the other slots stay empty.
		v.nodes = make([]*variantBNode, n)
		v.nodes[idx] = newVariantBNode(bLogger(), v.policy, v.replication, v.pushDivisor, failWithholdSet(t, n)[idx], v.groups)
		// The wire form for the record: the first group's segments as gossip would carry them
		// is not what B puts on the wire (partial messages), so only the counts are meaningful.
		arm := wireArm{name: fmt.Sprintf("variantB/%s/K=%d/%d", v.policy.String(), v.msgs[0].Descriptor.Required(), len(v.msgs)),
			msgs: make([][]byte, len(v.msgs)), need: int(v.msgs[0].Descriptor.Required())} // lint:ignore uintcast -- bounded by MaxSegments.
		return shadowCell{
			label: "variantB", arm: arm, v: v,
			join: func(t *testing.T, ps *pubsub.PubSub, self peer.ID, idx int, _ uint64) shadowRun {
				node := v.nodes[idx]
				node.broadcaster.SetSelf(self)
				var wg sync.WaitGroup
				v.startNode(t, node, idx == 0, &wg)
				subs := v.joinNode(t, ps, node)
				return shadowRun{
					publish: func(context.Context, *testing.T) error { return v.publishFrom(node) },
					watch: func(ctx context.Context, onDone func()) {
						if idx == 0 {
							return
						}
						go func() {
							select {
							case <-node.done:
								onDone()
							case <-ctx.Done():
							}
						}()
					},
					cancel: func() {
						for _, s := range subs {
							s.Cancel()
						}
						node.broadcaster.Stop()
						wg.Wait()
					},
					extra: func() map[string]int64 {
						c := node.broadcaster.Counters()
						return map[string]int64{"pushed": int64(c.SegmentsPushed), "served": int64(c.SegmentsServed),
							"requested": int64(c.SegmentsRequested), "received": int64(c.SegmentsReceived),
							"reissued": int64(c.RequestsReissued), "withheld": int64(c.ServesWithheld)}
					},
				}
			},
			declined: func(int) int64 { return 0 },
		}
	default:
		t.Fatalf("bad SHADOW_VARIANT %q (a|b|c)", os.Getenv("SHADOW_VARIANT"))
	}
	return shadowCell{}
}

// snapshotInterval is how often the counters are sampled after the publish.
const snapshotInterval = 100 * time.Millisecond

func snapshot(tr *recordingTracer, since time.Duration) shadowsim.Snapshot {
	dups, _, rx, tx := tr.Counts()
	prx, ptx, _, _ := tr.PartialCounts() // variant B's bytes, as in the final line
	rx, tx = rx+prx, tx+ptx
	c := tr.ControlSeen()
	d, u := tr.Losses()
	return shadowsim.Snapshot{
		T: since.Seconds(), Dups: dups, RxBytes: rx, TxBytes: tx, CtrlRx: c.BytesRecv, CtrlTx: c.BytesSent,
		IhaveSent: c.IhaveSent, IwantSent: c.IwantSent, IwantRecv: c.IwantRecv, IwantIdsRecv: c.IwantIdsRecv,
		IdwSent: c.IdontwantSent, IdwRecv: c.IdontwantRecv, Drops: d + u,
	}
}

func TestShadowNode(t *testing.T) {
	if !shadowsim.IsNode() {
		t.Skip("SHADOW_NODE=1 runs one Shadow node process")
	}
	start := time.Now()
	e := shadowSchedule(t)
	if !start.Before(e.PublishAt) {
		t.Fatalf("publish instant %v is not after this node's start %v", e.PublishAt, start)
	}
	params.SetupTestConfigCleanup(t)
	sc := shadowCellFromEnv(t, e.N, e.Index)
	v := sc.v
	seed := meshGraphSeed(t)
	edges := shadowEdges(t, e.N)
	shadowRefuse(t, e.N, seed)

	priv, err := shadowsim.Identity(seed, e.Index)
	require.NoError(t, err)
	h, err := shadowsim.NewHost(priv, e.Transport, e.IP(e.Index), e.Port)
	require.NoError(t, err)
	defer h.Close()
	tracer := newRecordingTracer()

	// The option order is the harness's (gossipsim.New, then the driver): signing off, the
	// production configuration, the study's substrate options, then this node's own -- the
	// tracer, request observation, the id function and the arm's options, as the driver hands
	// them per node.
	subOpts, perNode := substrateOpts(t, e.N, meshLinks(t, e.N, defaultRate), nil, func(i int) []pubsub.Option {
		opts := []pubsub.Option{pubsub.WithRawTracer(tracer), pubsub.WithRequestObservation()}
		opts = append(opts, structuredIDOpts()...)
		return append(opts, v.pubsubOpts(t, i)...)
	})
	opts := []pubsub.Option{
		pubsub.WithMessageSigning(false),
		pubsub.WithStrictSignatureVerification(false),
	}
	opts = append(opts, productionPubsubOpts()...)
	opts = append(opts, subOpts...)
	opts = append(opts, perNode(e.Index)...)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ps, err := pubsub.NewGossipSub(ctx, h, opts...)
	require.NoError(t, err)
	t.Logf("node %d/%d %s %s listening on %v", e.Index, e.N, e.Transport, sc.arm.name, h.Addrs())

	// Dial the edges this node owns (the lower index dials, as the harness's edge order does),
	// all at once, retrying while the far end is still coming up. Every dial is finished before
	// the subscription goes out, as in the harness, where every edge is up before any node joins.
	shadowsim.SleepUntil(start.Add(e.DialAt))
	pairs := make([][2]int, len(edges))
	for i, ed := range edges {
		pairs[i] = [2]int{ed.A, ed.B}
	}
	peerOf := func(j int) (peer.AddrInfo, error) {
		return shadowsim.AddrInfo(seed, e.Transport, e.IP(j), e.Port, j)
	}
	// The deadline is the cell's, not a constant: a real host sets up a few hundred connection
	// endpoints per second, so a large mesh needs longer than the 30s default.
	require.NoError(t, shadowsim.DialEdges(ctx, h, e.Index, pairs, peerOf, e.DialTimeout, e.DialSlots))
	t.Logf("node %d: %d connections up at %.3fs", e.Index, len(h.Network().Peers()), shadowsim.SimSeconds(time.Now()))

	shadowsim.SleepUntil(start.Add(e.SubscribeAt))
	run := sc.join(t, ps, h.ID(), e.Index, seed)
	defer run.cancel()

	// Receivers watch from subscription on: completion is dated from the publish instant, which
	// every node knows in advance. The publisher only drains its own subscriptions.
	var compAt atomic.Int64
	var completed atomic.Bool
	run.watch(ctx, func() {
		compAt.Store(int64(time.Since(e.PublishAt)))
		completed.Store(true)
	})

	shadowsim.SleepUntil(e.PublishAt)
	meshAtPublish := tracer.MeshPeers()
	peersAtPublish := len(h.Network().Peers())
	if e.Index == 0 {
		require.NoError(t, run.publish(ctx, t))
		t.Logf("published %s: %d messages, %d bytes, complete at %d", sc.arm.name, len(sc.arm.msgs), sc.arm.total, sc.arm.completeAt())
	}

	// Sample the counters on the simulated clock until the end of the hold.
	var snaps []shadowsim.Snapshot
	for k := 1; ; k++ {
		at := e.PublishAt.Add(time.Duration(k) * snapshotInterval)
		if at.After(e.PublishAt.Add(e.Hold)) {
			break
		}
		shadowsim.SleepUntil(at)
		snaps = append(snaps, snapshot(tracer, time.Since(e.PublishAt)))
	}
	shadowsim.SleepUntil(e.PublishAt.Add(e.Hold))
	end := time.Now()
	dups, rejects, rx, tx := tracer.Counts()
	// Variant B's segments ride the partial-message extension, which the message counters do
	// not see; the byte totals include them so rx/node and the publisher's bytes mean the same
	// on every variant, and the split is kept in the extras.
	prx, ptx, _, _ := tracer.PartialCounts()
	rx, tx = rx+prx, tx+ptx
	c := tracer.ControlSeen()
	dropped, undeliverable := tracer.Losses()
	var wire wireForm
	if w, ok := sc.v.(wireFormer); ok {
		wire = w.wireForm()
	}
	st := shadowsim.Stat{
		Index: e.Index, Transport: e.Transport, Arm: sc.label, ArmName: sc.arm.name,
		ArmMsgs: wire.msgs, ArmNeed: wire.need, ArmTotalBytes: wire.total, UnitBytes: wire.unit, IDBytes: wire.idBytes,
		Completed: completed.Load(), CompS: float64(compAt.Load()) / 1e9,
		MeshAtPublish: meshAtPublish, PeersAtPublish: peersAtPublish, MeshAtEnd: tracer.MeshPeers(),
		Dups: dups, Rejects: rejects, RxBytes: rx, TxBytes: tx,
		CtrlRx: c.BytesRecv, CtrlTx: c.BytesSent,
		IhaveSent: c.IhaveSent, IhaveRecv: c.IhaveRecv, IwantSent: c.IwantSent, IwantRecv: c.IwantRecv,
		IwantIdsSent: c.IwantIdsSent, IwantIdsRecv: c.IwantIdsRecv, IdwSent: c.IdontwantSent, IdwRecv: c.IdontwantRecv,
		GraftSent: c.GraftSent, PruneSent: c.PruneSent,
		DroppedRPCs: dropped, Undeliverable: undeliverable,
		StartS: shadowsim.SimSeconds(start), PublishS: shadowsim.SimSeconds(e.PublishAt), EndS: shadowsim.SimSeconds(end),
		Snapshots: snaps,
	}
	st.Declined = sc.declined(e.Index)
	if run.extra != nil {
		st.Extra = run.extra()
	}
	if prx+ptx > 0 {
		if st.Extra == nil {
			st.Extra = map[string]int64{}
		}
		st.Extra["partial_rx"], st.Extra["partial_tx"] = int64(prx), int64(ptx)
	}
	require.NoError(t, st.Write(os.Stdout))
}

// TestShadowExport writes the cell's topology for the lab's generator: the graph (for the record;
// the nodes dial it themselves), the per-node access links and the one-way latency between every
// pair, all from the harness's own generators at the cell's seed.
func TestShadowExport(t *testing.T) {
	path := os.Getenv("SHADOW_EXPORT")
	if path == "" {
		t.Skip("SHADOW_EXPORT=<file> writes the cell's topology for the Shadow generator")
	}
	params.SetupTestConfigCleanup(t)
	n := shadowN(t)
	top := shadowsim.Topology{N: n, Seed: meshGraphSeed(t), Model: os.Getenv("SEGMENT_LATENCY_MODEL")}
	if os.Getenv(shadowsim.EnvEdges) == "" {
		top.Degree = connectivityDegree(t, n)
	}
	for _, ed := range shadowEdges(t, n) {
		top.Edges = append(top.Edges, [2]int{ed.A, ed.B})
	}
	for _, l := range meshLinks(t, n, defaultRate) {
		for range l.Count {
			top.UpBps = append(top.UpBps, l.LinkSettings.Uplink.BitsPerSecond)
			top.DownBps = append(top.DownBps, l.LinkSettings.Downlink.BitsPerSecond)
		}
	}
	require.Equal(t, n, len(top.UpBps), "link groups cover every node")
	pair := func(i, j int) time.Duration { return defaultLatency }
	switch top.Model {
	case "", "uniform":
		top.Model = "uniform"
	case "geo":
		var summary string
		_, summary, pair = geoLatencyPair(t, top.Seed, n)
		t.Logf("latency model geo: %s", summary)
	default:
		t.Fatalf("bad SEGMENT_LATENCY_MODEL %q", top.Model)
	}
	top.SetLatency(pair)
	top.SetIPs(os.Getenv(shadowsim.EnvIPPrefix))
	require.NoError(t, top.Write(path))
	t.Logf("wrote %s: %d nodes, %d edges, degree %d, %s latency", path, n, len(top.Edges), top.Degree, top.Model)
}
