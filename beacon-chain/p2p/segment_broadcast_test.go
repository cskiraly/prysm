package p2p

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/OffchainLabs/prysm/v7/beacon-chain/p2p/internal/segmentgossip"
	p2ptest "github.com/OffchainLabs/prysm/v7/beacon-chain/p2p/testing"
	"github.com/OffchainLabs/prysm/v7/config/params"
	"github.com/OffchainLabs/prysm/v7/container/segments"
	"github.com/OffchainLabs/prysm/v7/encoding/bytesutil"
	ethpb "github.com/OffchainLabs/prysm/v7/proto/prysm/v1alpha1"
	"github.com/OffchainLabs/prysm/v7/testing/require"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
)

// segmentMessages builds the segmentation a publisher would hand to the broadcaster.
func segmentMessages(t *testing.T, msgLen, segSize int) []*segments.SegmentMessage {
	h, err := segments.HasherByID(segments.HashSHA256)
	require.NoError(t, err)
	msg := make([]byte, msgLen)
	for i := range msg {
		msg[i] = byte(i)
	}
	built, err := segments.BuildSegmentMessages(msg, segSize, h)
	require.NoError(t, err)
	require.NoError(t, segments.WithContext(built, h, 1, bytes.Repeat([]byte{0xbb}, 32)))
	return built
}

func TestBroadcastSegments(t *testing.T) {
	params.SetupTestConfigCleanup(t)

	t.Run("empty batch is a no-op", func(t *testing.T) {
		// Must not reach fork digest lookup or publish, so a nil service is safe here.
		s := &Service{}
		require.NoError(t, s.BroadcastSegments(context.Background(), nil))
	})

	t.Run("test double records the whole batch", func(t *testing.T) {
		// Confirms the batch reaches the broadcaster intact: every segment, in order.
		p := p2ptest.NewTestP2P(t)
		segs := segmentMessages(t, 500, 64)
		require.NoError(t, p.BroadcastSegments(context.Background(), segs))
		got := p.BroadcastedSegments()
		require.Equal(t, len(segs), len(got))
		for i := range segs {
			require.Equal(t, segs[i].Index, got[i].Index)
			require.DeepEqual(t, segs[i].Data, got[i].Data)
		}
	})

	t.Run("segments survive the SSZ wire type", func(t *testing.T) {
		// A reader must recover every segment exactly, or reassembly on the far side would fail
		// for reasons unrelated to gossip.
		for _, m := range segmentMessages(t, 300, 64) {
			pb, err := m.ToProto()
			require.NoError(t, err)
			enc, err := pb.MarshalSSZ()
			require.NoError(t, err)
			back := &ethpb.ExecutionPayloadSegment{}
			require.NoError(t, back.UnmarshalSSZ(enc))
			seg, hasher, err := segments.FromProto(back)
			require.NoError(t, err)
			require.NoError(t, seg.Verify(hasher))
		}
	})

	t.Run("no topic peers is an error", func(t *testing.T) {
		// A caller sending only the segments falls back to the whole envelope on this error, so
		// a topic without peers must not look like a publish.
		svc := &Service{
			cfg:                   &Config{},
			genesisTime:           time.Now(),
			genesisValidatorsRoot: bytesutil.PadTo([]byte{'A'}, 32),
		}
		p1 := p2ptest.NewTestP2P(t)
		svc.host = p1.BHost
		svc.pubsub = p1.PubSub()
		svc.joinedTopics = map[string]*pubsub.Topic{}
		err := svc.BroadcastSegments(context.Background(), segmentMessages(t, 500, 64))
		require.ErrorIs(t, err, ErrSegmentsNoPeers)
	})

	t.Run("publishes through the router's allowlist", func(t *testing.T) {
		// Production installs a subscription allowlist of full topic names, with the encoding
		// suffix, and the router refuses to join any other name. This is the one place the
		// series joins a topic by hand, so it runs against a router carrying that filter: a join
		// by the bare name fails here before a segment is published.
		svc := &Service{
			cfg:                   &Config{},
			genesisTime:           time.Now(),
			genesisValidatorsRoot: bytesutil.PadTo([]byte{'A'}, 32),
		}
		digest, err := svc.currentForkDigest()
		require.NoError(t, err)
		fullTopic := fmt.Sprintf(ExecutionPayloadSegmentTopicFormat, digest) + svc.Encoding().ProtocolSuffix()

		filt := pubsub.NewAllowlistSubscriptionFilter(fullTopic)
		p1 := p2ptest.NewTestP2PWithPubsubOptions(t, []pubsub.Option{pubsub.WithSubscriptionFilter(filt)})
		p2 := p2ptest.NewTestP2P(t)
		p1.Connect(p2)
		svc.host = p1.BHost
		svc.pubsub = p1.PubSub()
		svc.joinedTopics = map[string]*pubsub.Topic{}

		sub, err := p2.SubscribeToTopic(fullTopic)
		require.NoError(t, err)
		require.Eventually(t, func() bool {
			return len(svc.pubsub.ListPeers(fullTopic)) > 0
		}, 5*time.Second, 10*time.Millisecond, "libp2p mesh did not establish")

		// The source's own segments skip its validator, so the payload's path must learn the group
		// from the broadcast itself, with the topic it went out on.
		svc.segmentPayloadType = segmentgossip.NewPayloadType(segmentgossip.PayloadConfig{
			Topics:       []string{fullTopic},
			CurrentTopic: func() string { return "/eth2/ffffffff/execution_payload_segment/ssz_snappy" },
			CurrentSlot:  func() uint64 { return 3 },
			UplinkMbps:   100,
		})
		segs := segmentMessages(t, 500, 64)
		group := string(segs[0].Group) // the payload path knows a group by its key
		require.NoError(t, svc.BroadcastSegments(context.Background(), segs))
		require.Equal(t, len(segs), svc.segmentPayloadType.IndexCount(group))
		require.Equal(t, int(segs[0].Descriptor.Required()), svc.segmentPayloadType.Threshold(group))
		topic, ok := svc.segmentPayloadType.TopicOf(group)
		require.Equal(t, true, ok)
		require.Equal(t, fullTopic, topic)
		for range segs {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			msg, err := sub.Next(ctx)
			cancel()
			require.NoError(t, err)
			require.Equal(t, fullTopic, *msg.Topic)
		}
	})
}

// TestAwaitEnqueued maps the router's report to BroadcastSegments' result: a batch that no peer's
// queue took, or no report before the deadline, is ErrSegmentsNotEnqueued. A router whose every
// peer queue is full reports zero (the router's own test, TestBatchEnqueued).
func TestAwaitEnqueued(t *testing.T) {
	report := make(chan int, 1)
	report <- 2
	require.NoError(t, awaitEnqueued(context.Background(), report))

	report <- 0
	require.ErrorIs(t, awaitEnqueued(context.Background(), report), ErrSegmentsNotEnqueued)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, awaitEnqueued(ctx, report), ErrSegmentsNotEnqueued)
}
