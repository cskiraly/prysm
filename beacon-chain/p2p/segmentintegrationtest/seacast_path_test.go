package segmentintegrationtest

import (
	"bytes"
	"context"
	"math/rand/v2"
	"os"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/OffchainLabs/prysm/v7/beacon-chain/p2p"
	"github.com/OffchainLabs/prysm/v7/beacon-chain/p2p/encoder"
	"github.com/OffchainLabs/prysm/v7/beacon-chain/p2p/internal/segmentgossip"
	"github.com/OffchainLabs/prysm/v7/beacon-chain/verification/segmentauth"
	"github.com/OffchainLabs/prysm/v7/config/params"
	"github.com/OffchainLabs/prysm/v7/container/segments"
	enginev1 "github.com/OffchainLabs/prysm/v7/proto/engine/v1"
	ethpb "github.com/OffchainLabs/prysm/v7/proto/prysm/v1alpha1"
	"github.com/OffchainLabs/prysm/v7/testing/require"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pubsubpb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	simlibp2p "github.com/libp2p/go-libp2p/x/simlibp2p"
	"github.com/marcopolo/simnet"
)

// The payload path's network: ten nodes of one class, each linked to the next three, a mesh of
// degree 4, prysm's heartbeat.
const (
	pathNodes     = 10
	pathLinks     = 3
	pathUpMbps    = 50
	pathDownMbps  = 100
	pathLatency   = 20 * time.Millisecond
	pathHeartbeat = 700 * time.Millisecond
	pathSlot      = 7
	// pathSettle is the virtual time given to the meshes before a publish, and to the group after it.
	pathSettle = 5 * time.Second
)

// pathConfig is what the path network runs.
type pathConfig struct {
	// path runs the payload type with the node's router options; without it, stock gossipsub.
	path bool
	// scored runs the node's peer scoring, with the segment topic's parameters.
	scored bool
	// rejectAt, when above zero, is the node whose validator rejects every other node's segment at
	// index rejectIndex.
	rejectAt    int
	rejectIndex uint32
	// block has every node join the block topic too, on stock gossipsub, and record when the first
	// block reaches it.
	block bool
	// ids, when set, is every node's message-id function, counting its calls.
	ids *idCounter
}

// decodeRecorder is the node's payload type on the router, recording how many segments each decode
// had when it started.
type decodeRecorder struct {
	*segmentgossip.PayloadType
	mu   sync.Mutex
	held []int
}

func (r *decodeRecorder) Decode(root string, held []uint32) func() (map[uint32]*pubsub.Message, bool) {
	r.mu.Lock()
	r.held = append(r.held, len(held))
	r.mu.Unlock()
	return r.PayloadType.Decode(root, held)
}

// starts is the held count of every decode started so far, in order.
func (r *decodeRecorder) starts() []int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]int(nil), r.held...)
}

// pathNode is one node of the payload path: its router, its payload type and what its options count.
type pathNode struct {
	self    peer.ID
	ps      *pubsub.PubSub
	typ     *segmentgossip.PayloadType
	decodes *decodeRecorder
	m       *segmentgossip.PathMetrics
	topic   *pubsub.Topic
	sub     *pubsub.Subscription
	// reject makes validate reject another node's segment at index rejectIndex.
	reject      bool
	rejectIndex uint32

	scoreMu sync.Mutex
	// lowest is the lowest score the node gave each peer at any inspection; last is the latest
	// inspection.
	lowest map[peer.ID]float64
	last   map[peer.ID]*pubsub.PeerScoreSnapshot

	arrivals sync.Mutex
	// segAt is when each segment the subscription delivered arrived, in order.
	segAt []time.Time
	// blockTopic and blockSub are the block topic's, with cfg.block; blockAt is when the first block
	// arrived.
	blockTopic *pubsub.Topic
	blockSub   *pubsub.Subscription
	blockAt    time.Time
}

// kthSegmentAt is when the subscription delivered the node's k-th segment, and false before then.
func (n *pathNode) kthSegmentAt(k int) (time.Time, bool) {
	n.arrivals.Lock()
	defer n.arrivals.Unlock()
	if len(n.segAt) < k {
		return time.Time{}, false
	}
	return n.segAt[k-1], true
}

// firstBlockAt is when the first block reached the node, and false before then.
func (n *pathNode) firstBlockAt() (time.Time, bool) {
	n.arrivals.Lock()
	defer n.arrivals.Unlock()
	return n.blockAt, !n.blockAt.IsZero()
}

// inspect records the node's scores of its peers, as its score inspector reports them.
func (n *pathNode) inspect(snaps map[peer.ID]*pubsub.PeerScoreSnapshot) {
	n.scoreMu.Lock()
	defer n.scoreMu.Unlock()
	for pid, s := range snaps {
		if lo, ok := n.lowest[pid]; !ok || s.Score < lo {
			n.lowest[pid] = s.Score
		}
	}
	n.last = snaps
}

// validate does what the node's segment validator does for the path: it accepts the node's own
// segments at once, verifies any other against its descriptor, and records the group's shape and
// topic, telling the router the first time.
func (n *pathNode) validate(_ context.Context, pid peer.ID, msg *pubsub.Message) pubsub.ValidationResult {
	if pid == n.self {
		return pubsub.ValidationAccept
	}
	pb := &ethpb.ExecutionPayloadSegment{}
	if err := (encoder.SszNetworkEncoder{}).DecodeGossip(msg.Data, pb); err != nil {
		return pubsub.ValidationReject
	}
	seg, h, err := segments.FromProto(pb)
	if err != nil || seg.Verify(h) != nil {
		return pubsub.ValidationReject
	}
	if n.reject && seg.Index == n.rejectIndex {
		return pubsub.ValidationReject
	}
	d := seg.Descriptor
	group := string(seg.Group)
	if n.typ.NoteGroup(group, msg.GetTopic(), int(d.Count), int(d.Required())) { // lint:ignore uintcast -- bounded by segments.MaxSegments.
		n.ps.SeacastHeaderValidated([]string{group})
	}
	return pubsub.ValidationAccept
}

func pathGossipParams() pubsub.GossipSubParams {
	p := pubsub.DefaultGossipSubParams()
	p.D, p.Dlo, p.Dhi, p.Dout = 4, 3, 6, 1
	p.HeartbeatInterval = pathHeartbeat
	p.MaxIHaveMessages = segmentgossip.MaxIHaveMessages
	return p
}

// pathNetwork brings up the network with every node subscribed to the segment topic, running what
// cfg says. The returned stop function must be deferred inside the synctest bubble.
func pathNetwork(t *testing.T, cfg pathConfig) ([]*pathNode, string, func()) {
	t.Helper()
	network, meta, err := simlibp2p.SimpleLibp2pNetwork([]simlibp2p.NodeLinkSettingsAndCount{
		{LinkSettings: simnet.NodeBiDiLinkSettings{
			Downlink: simnet.LinkSettings{BitsPerSecond: pathDownMbps * simlibp2p.OneMbps},
			Uplink:   simnet.LinkSettings{BitsPerSecond: pathUpMbps * simlibp2p.OneMbps},
		}, Count: pathNodes},
	}, simnet.StaticLatency(pathLatency), simlibp2p.NetworkSettings{UseBlankHost: true})
	require.NoError(t, err)
	network.Start()
	synctest.Wait()

	topic := segmentTopic()
	ctx, cancel := context.WithCancel(context.Background())
	nodes := make([]*pathNode, len(meta.Nodes))
	for i, h := range meta.Nodes {
		typ := segmentgossip.NewPayloadType(segmentgossip.PayloadConfig{
			Topics:       []string{topic},
			CurrentTopic: func() string { return topic },
			CurrentSlot:  func() uint64 { return pathSlot },
			UplinkMbps:   pathUpMbps,
		})
		setting := segmentgossip.Setting{
			SegmentTopics: []string{topic},
			Allowed:       func(string) bool { return true },
			TypeTopics:    typ.Topics(),
			Params:        typ.Params(),
			Limits:        pubsub.DefaultSeacastLimits,
			Cancel:        true,
			UplinkMbps:    pathUpMbps,
		}
		require.NoError(t, segmentgossip.CheckSetting(setting))
		n := &pathNode{
			self: h.ID(), typ: typ, decodes: &decodeRecorder{PayloadType: typ}, m: &segmentgossip.PathMetrics{},
			reject: cfg.rejectAt > 0 && i == cfg.rejectAt, rejectIndex: cfg.rejectIndex,
			lowest: map[peer.ID]float64{},
		}
		opts := []pubsub.Option{
			pubsub.WithMessageSignaturePolicy(pubsub.StrictNoSign),
			pubsub.WithNoAuthor(),
			pubsub.WithPeerOutboundQueueSize(pubsubQueueSize),
			pubsub.WithMessageIdFn(func(pmsg *pubsubpb.Message) string { return p2p.MsgID(pmsg) }),
		}
		if cfg.ids != nil {
			opts = append(opts, pubsub.WithMessageIdFn(cfg.ids.id))
		}
		peerParams, thresholds, topicParams := p2p.SegmentScoring()
		if cfg.scored {
			// In the node's order: scoring before the gossip parameters and the path's options.
			opts = append(opts,
				pubsub.WithPeerScore(peerParams, thresholds),
				pubsub.WithPeerScoreInspect(pubsub.ExtendedPeerScoreInspectFn(n.inspect), pathHeartbeat))
		}
		opts = append(opts, pubsub.WithGossipSubParams(pathGossipParams()))
		if cfg.path {
			opts = append(opts, segmentgossip.RouterOptions(setting, n.decodes, pathHeartbeat, n.m)...)
		}
		ps, err := pubsub.NewGossipSub(ctx, h, opts...)
		require.NoError(t, err)
		n.ps = ps
		require.NoError(t, ps.RegisterTopicValidator(topic, n.validate))
		n.topic, err = ps.Join(topic)
		require.NoError(t, err)
		if cfg.scored {
			require.NoError(t, n.topic.SetScoreParams(topicParams))
		}
		n.sub, err = n.topic.Subscribe()
		require.NoError(t, err)
		go func() {
			for {
				if _, err := n.sub.Next(ctx); err != nil {
					return
				}
				n.arrivals.Lock()
				n.segAt = append(n.segAt, time.Now())
				n.arrivals.Unlock()
			}
		}()
		if cfg.block {
			n.blockTopic, err = ps.Join(blockTopic())
			require.NoError(t, err)
			n.blockSub, err = n.blockTopic.Subscribe()
			require.NoError(t, err)
			go func() {
				for {
					if _, err := n.blockSub.Next(ctx); err != nil {
						return
					}
					n.arrivals.Lock()
					if n.blockAt.IsZero() {
						n.blockAt = time.Now()
					}
					n.arrivals.Unlock()
				}
			}()
		}
		nodes[i] = n
	}
	for i, h := range meta.Nodes {
		for j := 1; j <= pathLinks; j++ {
			o := meta.Nodes[(i+j)%len(meta.Nodes)]
			require.NoError(t, h.Connect(context.Background(), peer.AddrInfo{ID: o.ID(), Addrs: o.Addrs()}))
		}
	}
	if cfg.path {
		for _, n := range nodes {
			n.ps.SeacastAdvanceSlot(pathSlot)
		}
	}
	time.Sleep(pathSettle)
	synctest.Wait()

	stop := func() {
		cancel()
		for _, n := range nodes {
			n.sub.Cancel()
			if n.blockSub != nil {
				n.blockSub.Cancel()
			}
		}
		closeHosts(meta.Nodes)
		network.Close()
		synctest.Wait()
	}
	return nodes, topic, stop
}

func closeHosts(hs []host.Host) {
	for _, h := range hs {
		_ = h.Close()
	}
}

// pathEnvelope is a signed envelope with txCount incompressible transactions of txSize bytes.
func pathEnvelope(t *testing.T, txCount, txSize int) *ethpb.SignedExecutionPayloadEnvelope {
	t.Helper()
	rng := rand.New(rand.NewPCG(1, uint64(txCount)))
	txs := make([][]byte, txCount)
	for i := range txs {
		txs[i] = make([]byte, txSize)
		for j := range txs[i] {
			txs[i][j] = byte(rng.Uint32())
		}
	}
	txList, err := enginev1.NewProgressiveTransactionList(txs)
	require.NoError(t, err)
	return &ethpb.SignedExecutionPayloadEnvelope{
		Message: &ethpb.ExecutionPayloadEnvelope{
			Payload: &enginev1.ExecutionPayloadGloas{
				Transactions:  txList,
				ParentHash:    make([]byte, 32),
				FeeRecipient:  make([]byte, 20),
				StateRoot:     make([]byte, 32),
				ReceiptsRoot:  make([]byte, 32),
				LogsBloom:     make([]byte, 256),
				PrevRandao:    make([]byte, 32),
				ExtraData:     []byte{},
				BaseFeePerGas: make([]byte, 32),
				BlockHash:     make([]byte, 32),
			},
			ExecutionRequests:     &enginev1.ExecutionRequestsGloas{},
			BeaconBlockRoot:       bytes.Repeat([]byte{0xbe}, 32),
			ParentBeaconBlockRoot: make([]byte, 32),
		},
		Signature: make([]byte, 96),
	}
}

// publishGroup publishes an envelope's segments from src as BroadcastSegments does: the group
// recorded in the source's payload type first, then one batch.
func publishGroup(t *testing.T, src *pathNode, topic string, segs []*segments.SegmentMessage) {
	t.Helper()
	d := segs[0].Descriptor
	group := string(segs[0].Group)
	if src.typ.NoteGroup(group, topic, int(d.Count), int(d.Required())) { // lint:ignore uintcast -- bounded by segments.MaxSegments.
		src.ps.SeacastHeaderValidated([]string{group})
	}
	var batch pubsub.MessageBatch
	for _, s := range segs {
		pb, err := s.ToProto()
		require.NoError(t, err)
		buf := new(bytes.Buffer)
		_, err = encoder.SszNetworkEncoder{}.EncodeGossip(buf, pb)
		require.NoError(t, err)
		require.NoError(t, src.topic.AddToBatch(context.Background(), &batch, buf.Bytes()))
	}
	require.NoError(t, src.ps.PublishBatch(&batch))
}

// heldEnvelope reassembles the envelope from what n's payload type holds of the group, and
// reports how many of its n segments are held.
func heldEnvelope(t *testing.T, n *pathNode, root string, count int) ([]byte, int) {
	t.Helper()
	r, err := segments.NewReassembler(segments.ReassemblerConfig{AllowUnauthenticated: true})
	require.NoError(t, err)
	var committed []byte
	held := 0
	for i := range count {
		m, ok := n.typ.Store().Get(root, uint32(i)) // lint:ignore uintcast -- an index below the group's count.
		if !ok || m == nil || m.Message == nil {
			continue
		}
		held++
		if committed != nil {
			continue
		}
		committed = feed(t, r, decodeSegment(t, m.GetData()))
	}
	if committed == nil {
		return nil, held
	}
	ssz, err := encoder.DecodeSnappy(committed, params.BeaconConfig().MaxPayloadSize)
	require.NoError(t, err)
	return ssz, held
}

// TestSeacastPayloadPath: over ten nodes running the node's payload type and router options, a source
// publishes a real envelope's segments as the node does, and every other node completes, decodes at K,
// rebuilds at least half of what it lacks, and holds the source's envelope.
// SEACAST_TEST_WITHOUT_PATH_OPTIONS=1 runs it with the path's options left out, which must fail.
func TestSeacastPayloadPath(t *testing.T) {
	cfg := pathConfig{path: os.Getenv("SEACAST_TEST_WITHOUT_PATH_OPTIONS") == ""}
	synctest.Test(t, func(t *testing.T) {
		params.SetupTestConfigCleanup(t)
		nodes, topic, stop := pathNetwork(t, cfg)
		defer stop()

		env := pathEnvelope(t, 128, 4096)
		want, err := env.MarshalSSZ()
		require.NoError(t, err)
		segs, err := segmentauth.SegmentMessagesForEnvelope(env, segmentauth.DefaultParams())
		require.NoError(t, err)
		d := segs[0].Descriptor
		require.Equal(t, segments.VersionCoded, d.Version)
		key, count, k := string(segs[0].Group), int(d.Count), int(d.Required()) // lint:ignore uintcast -- bounded by segments.MaxSegments.
		t.Logf("a group of %d segments, K %d, %d bytes of envelope, on %d nodes at %d/%d Mbps",
			count, k, len(want), len(nodes), pathUpMbps, pathDownMbps)

		publishGroup(t, nodes[0], topic, segs)
		time.Sleep(pathSettle)
		synctest.Wait()

		for i, n := range nodes[1:] {
			c := &n.m.Seacast
			got, held := heldEnvelope(t, n, key, count)
			t.Logf("node %d: accepted %d, completed %d, decodes %d (failed %d), rebuilt %d, repair pushes %d, held %d of %d, refused joins %d",
				i+1, c.Accepted.Load(), c.GroupsCompleted.Load(), c.DecodesDone.Load(), c.DecodesFailed.Load(),
				c.CellsRebuilt.Load(), c.RepairPushes.Load(), held, count, c.Refused.Load())
			require.Equal(t, int64(1), c.GroupsCompleted.Load(), "node %d did not complete the group", i+1)
			require.Equal(t, int64(1), c.DecodesDone.Load(), "node %d did not decode", i+1)
			require.DeepEqual(t, []int{k}, n.decodes.starts(), "node %d's decodes did not start at K", i+1)
			require.Equal(t, int64(0), c.DecodesFailed.Load(), "node %d", i+1)
			require.Equal(t, true, c.Accepted.Load() > 0, "node %d accepted nothing on the path", i+1)
			require.Equal(t, int64(0), c.Refused.Load(), "node %d refused a join", i+1)
			require.Equal(t, true, 2*c.CellsRebuilt.Load() >= int64(count-k),
				"node %d rebuilt %d of the %d it lacked at K", i+1, c.CellsRebuilt.Load(), count-k)
			require.Equal(t, count, held, "node %d holds %d of %d", i+1, held, count)
			require.Equal(t, true, bytes.Equal(want, got), "node %d's envelope is not the source's", i+1)
		}
	})
}

// TestSeacastPayloadPathPlain: a plain group (no parity segments, K = N), as a publisher sends a payload
// the code cannot hold, completes at every other node when its last segment is in, and so stops its asks
// (the router counts a completion where it stops them), with no decode, and every node holds the
// source's envelope.
func TestSeacastPayloadPathPlain(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		params.SetupTestConfigCleanup(t)
		nodes, topic, stop := pathNetwork(t, pathConfig{path: true})
		defer stop()

		env := pathEnvelope(t, 64, 4096)
		want, err := env.MarshalSSZ()
		require.NoError(t, err)
		p := segmentauth.DefaultParams()
		p.Coded = false
		segs, err := segmentauth.SegmentMessagesForEnvelope(env, p)
		require.NoError(t, err)
		d := segs[0].Descriptor
		require.NotEqual(t, segments.VersionCoded, d.Version)
		key, count := string(segs[0].Group), int(d.Count)           // lint:ignore uintcast -- bounded by segments.MaxSegments.
		require.Equal(t, count, int(d.Required()), "a plain group") // lint:ignore uintcast -- bounded by segments.MaxSegments.
		t.Logf("a plain group of %d segments, %d bytes of envelope", count, len(want))

		publishGroup(t, nodes[0], topic, segs)
		time.Sleep(pathSettle)
		synctest.Wait()

		for i, n := range nodes[1:] {
			c := &n.m.Seacast
			got, held := heldEnvelope(t, n, key, count)
			t.Logf("node %d: accepted %d, completed %d, decodes started %d, held %d of %d",
				i+1, c.Accepted.Load(), c.GroupsCompleted.Load(), c.DecodesStarted.Load(), held, count)
			require.Equal(t, int64(1), c.GroupsCompleted.Load(), "node %d did not complete the group", i+1)
			require.Equal(t, int64(0), c.DecodesStarted.Load(), "node %d decoded a plain group", i+1)
			require.Equal(t, 0, len(n.decodes.starts()), "node %d decoded a plain group", i+1)
			require.Equal(t, count, held, "node %d holds %d of %d", i+1, held, count)
			require.Equal(t, true, bytes.Equal(want, got), "node %d's envelope is not the source's", i+1)
		}
	})
}
