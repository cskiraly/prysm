package p2p

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"slices"
	"testing"
	"time"

	p2ptest "github.com/OffchainLabs/prysm/v7/beacon-chain/p2p/testing"
	"github.com/OffchainLabs/prysm/v7/config/features"
	fieldparams "github.com/OffchainLabs/prysm/v7/config/fieldparams"
	"github.com/OffchainLabs/prysm/v7/config/params"
	ethpb "github.com/OffchainLabs/prysm/v7/proto/prysm/v1alpha1"
	"github.com/OffchainLabs/prysm/v7/testing/require"
	"github.com/golang/snappy"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
)

func TestSeacastAggregateSubscription(t *testing.T) {
	seacastGloasConfig(t)
	cfg := params.BeaconConfig().Copy()
	cfg.PQForkEpoch = 7
	params.SetActiveTestCleanup(t, cfg)
	t.Cleanup(features.InitWithReset(&features.Flags{EnableSegmentedAggregateGossip: true, EnableSegmentedPayloadGossip: true, SeacastUplinkMbps: 1000}))
	genesis := time.Now().Add(-time.Duration(7*cfg.SlotsPerEpoch+1) * cfg.SlotDuration())
	node := func() (*Service, *p2ptest.TestP2P) {
		s := &Service{cfg: &Config{QueueSize: 600}, genesisTime: genesis, genesisValidatorsRoot: make([]byte, 32), joinedTopics: map[string]*pubsub.Topic{}}
		filter := pubsub.NewAllowlistSubscriptionFilter(s.allTopicStrings()...)
		opts, err := s.seacastOptions(filter)
		require.NoError(t, err)
		require.NotNil(t, s.aggregateType)
		p := p2ptest.NewTestP2PWithPubsubOptions(t, append(opts, pubsub.WithMessageIdFn(MsgID), pubsub.WithMessageSignaturePolicy(pubsub.StrictNoSign), pubsub.WithNoAuthor(), pubsub.WithSubscriptionFilter(filter)))
		t.Cleanup(func() { _ = p.BHost.Close() })
		s.host, s.pubsub = p.BHost, p.PubSub()
		return s, p
	}
	source, p1 := node()
	sink, p2 := node()
	observer := p2ptest.NewTestP2PWithPubsubOptions(t, []pubsub.Option{pubsub.WithMessageIdFn(MsgID), pubsub.WithNoAuthor()})
	t.Cleanup(func() { _ = observer.BHost.Close() })
	d, err := source.currentForkDigest()
	require.NoError(t, err)
	seg := fmt.Sprintf(AggregateSegmentTopicFormat, d) + sszEnc.ProtocolSuffix()
	whole := fmt.Sprintf(AggregateAndProofSubnetTopicFormat, d) + sszEnc.ProtocolSuffix()
	subscribe := func(s *Service, topic string) *pubsub.Subscription {
		handle, err := s.JoinTopic(topic)
		require.NoError(t, err)
		sub, err := handle.Subscribe()
		require.NoError(t, err)
		t.Cleanup(sub.Cancel)
		return sub
	}
	chunks := subscribe(source, seg)
	sub, err := observer.SubscribeToTopic(whole)
	require.NoError(t, err)
	p1.Connect(observer)
	p2.Connect(observer)
	require.Eventually(t, func() bool { return len(source.pubsub.ListPeers(whole)) > 0 }, 5*time.Second, 10*time.Millisecond)
	aggregate := &ethpb.SignedAggregateAttestationAndProofPQ{
		Signature: make([]byte, fieldparams.XMSSSignatureLength),
		Message: &ethpb.AggregateAttestationAndProofPQ{
			Aggregate: &ethpb.AttestationPQ{
				Data: &ethpb.AttestationData{
					Slot: 9, BeaconBlockRoot: make([]byte, 32),
					Source: &ethpb.Checkpoint{Root: make([]byte, 32)}, Target: &ethpb.Checkpoint{Root: make([]byte, 32)},
				},
				CommitteeBits: ethpb.NewCommitteeBitsPQ(), AggregationBits: []byte{7},
			},
			Proof: make([]byte, 80000),
		},
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	require.ErrorIs(t, source.Broadcast(ctx, aggregate), ErrSegmentsNoPeers)
	validated := make(chan *pubsub.Message, 2)
	require.NoError(t, sink.pubsub.RegisterTopicValidator(whole, func(ctx context.Context, from peer.ID, m *pubsub.Message) bool {
		validated <- m
		return from == p1.BHost.ID()
	}))
	local := subscribe(sink, whole)
	subscribe(sink, seg)
	p1.Connect(p2)
	require.Eventually(t, func() bool { return len(source.pubsub.ListPeers(seg)) > 0 }, 5*time.Second, 10*time.Millisecond)
	require.NoError(t, source.Broadcast(ctx, aggregate))
	msg, err := local.Next(ctx)
	require.NoError(t, err)
	select {
	case checked := <-validated:
		require.Equal(t, msg, checked)
	default:
		t.Fatal("aggregate bypassed whole-topic validation")
	}
	require.Equal(t, true, msg.Local)
	require.Equal(t, p1.BHost.ID(), msg.ReceivedFrom)
	body, err := snappy.Decode(nil, msg.Data)
	require.NoError(t, err)
	want, err := aggregate.MarshalSSZ()
	require.NoError(t, err)
	require.DeepEqual(t, want, body)
	// A locally produced aggregate covered by the received group is suppressed as well.
	aggregate.Message.Aggregate.AggregationBits = []byte{3}
	require.NoError(t, sink.Broadcast(ctx, aggregate))
	// A whole-only observer sees its first aggregate after the source unsubscribes.
	chunks.Cancel()
	require.Eventually(t, func() bool { return !slices.Contains(source.pubsub.GetTopics(), seg) }, 5*time.Second, 10*time.Millisecond)
	aggregate.Message.Proof = []byte{42}
	require.NoError(t, source.Broadcast(ctx, aggregate))
	msg, err = sub.Next(ctx)
	require.NoError(t, err)
	body, err = snappy.Decode(nil, msg.Data)
	require.NoError(t, err)
	want, err = aggregate.MarshalSSZ()
	require.NoError(t, err)
	if !bytes.Equal(body, want) {
		t.Fatal("observed a whole aggregate published while subscribed to the segment topic")
	}
}

func TestAggregateSegmentMessageID(t *testing.T) {
	params.SetupTestConfigCleanup(t)
	topic := fmt.Sprintf(AggregateSegmentTopicFormat, params.ForkDigest(params.BeaconConfig().FuluForkEpoch)) + sszEnc.ProtocolSuffix()
	frame := bytes.Repeat([]byte{0x5a}, 170)
	binary.LittleEndian.PutUint32(frame[83:], 5)
	binary.LittleEndian.PutUint32(frame[87:], 95)
	binary.LittleEndian.PutUint32(frame[91:], 127)
	preimage := binary.LittleEndian.AppendUint64([]byte{1, 0, 0, 0}, uint64(len(topic)))
	preimage = append(preimage, topic...)
	hash := sha256.Sum256(append(preimage, frame...))
	expected := append(bytes.Clone(frame[:32]), frame[83:87]...)
	expected = append(expected, hash[:20]...)
	for _, meta := range [][]byte{nil, bytes.Repeat([]byte{7}, 2800)} {
		body := binary.LittleEndian.AppendUint16(nil, uint16(len(meta)))
		body = append(body, meta...)
		wire := snappy.Encode(nil, append(body, frame...))
		require.Equal(t, string(expected), MsgID(&pb.Message{Topic: &topic, Data: wire}))
	}
}

func TestSeacastAggregateOnlyOptions(t *testing.T) {
	seacastGloasConfig(t)
	cfg := params.BeaconConfig().Copy()
	cfg.PQForkEpoch = 7
	params.SetActiveTestCleanup(t, cfg)
	t.Cleanup(features.InitWithReset(&features.Flags{EnableSegmentedAggregateGossip: true, SeacastUplinkMbps: 100}))
	s := &Service{cfg: &Config{QueueSize: 600}}
	opts, err := s.pubsubOptions()
	require.NoError(t, err)
	require.NoError(t, newRouter(t, opts))
	if s.segmentPayloadType != nil || s.aggregateType == nil {
		t.Fatal("unexpected set of registered SEACAST types")
	}
	pq := fmt.Sprintf(AggregateSegmentTopicFormat, params.ForkDigest(cfg.PQForkEpoch)) + sszEnc.ProtocolSuffix()
	before := fmt.Sprintf(AggregateSegmentTopicFormat, params.ForkDigest(cfg.GloasForkEpoch)) + sszEnc.ProtocolSuffix()
	require.Equal(t, true, slices.Contains(s.aggregateType.Topics(), pq))
	require.Equal(t, false, slices.Contains(s.aggregateType.Topics(), before))
}
