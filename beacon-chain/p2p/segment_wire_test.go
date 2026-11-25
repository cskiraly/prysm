package p2p

import (
	"fmt"
	"strings"
	"testing"

	"github.com/OffchainLabs/prysm/v7/config/features"
	"github.com/OffchainLabs/prysm/v7/config/params"
	"github.com/OffchainLabs/prysm/v7/container/segments"
	ethpb "github.com/OffchainLabs/prysm/v7/proto/prysm/v1alpha1"
	"github.com/OffchainLabs/prysm/v7/testing/require"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
)

// TestSegmentTopicIsMapped guards the decode path: decodePubsubMessage returns
// ErrMessageNotMapped for a topic with no proto type, so the mapping must exist.
func TestSegmentTopicIsMapped(t *testing.T) {
	msg := GossipTopicMappings(ExecutionPayloadSegmentTopicFormat, 0)
	require.NotNil(t, msg)
	_, ok := msg.(*ethpb.ExecutionPayloadSegment)
	require.Equal(t, true, ok, "segment topic maps to the wrong type")
}

// TestSegmentWireBounds checks the wire type against the container's limits: a segment at
// MaxWireSegmentSize with a proof of MaxProofDepth elements marshals, one byte or one element
// more does not. The bounds are literals in the .proto file, so this is the drift guard.
func TestSegmentWireBounds(t *testing.T) {
	full := func() *ethpb.ExecutionPayloadSegment {
		proof := make([][]byte, segments.MaxProofDepth)
		for i := range proof {
			proof[i] = make([]byte, 32)
		}
		return &ethpb.ExecutionPayloadSegment{
			BeaconBlockRoot: make([]byte, 32),
			Group:           make([]byte, 32),
			SegmentDescriptor: &ethpb.SegmentDescriptor{
				Version: make([]byte, 1), HashId: make([]byte, 1), Encoding: make([]byte, 1), Root: make([]byte, 32),
			},
			Proof: proof,
			Data:  make([]byte, segments.MaxWireSegmentSize),
		}
	}

	t.Run("maximum size segment marshals", func(t *testing.T) {
		enc, err := full().MarshalSSZ()
		require.NoError(t, err)
		require.Equal(t, true, len(enc) > segments.MaxWireSegmentSize)
	})

	t.Run("one byte of data over the bound is rejected", func(t *testing.T) {
		m := full()
		m.Data = append(m.Data, 0)
		_, err := m.MarshalSSZ()
		require.Equal(t, true, err != nil, "ssz_max on data is larger than MaxWireSegmentSize")
	})

	t.Run("one proof element over the bound is rejected", func(t *testing.T) {
		m := full()
		m.Proof = append(m.Proof, make([]byte, 32))
		_, err := m.MarshalSSZ()
		require.Equal(t, true, err != nil, "ssz_max on proof is larger than MaxProofDepth")
	})

	t.Run("round trips", func(t *testing.T) {
		want := full()
		enc, err := want.MarshalSSZ()
		require.NoError(t, err)
		got := &ethpb.ExecutionPayloadSegment{}
		require.NoError(t, got.UnmarshalSSZ(enc))
		require.Equal(t, segments.MaxProofDepth, len(got.Proof))
		require.DeepEqual(t, want.Data, got.Data)
	})
}

// TestSegmentTopicHasScoringParams checks the segment topic does not fall through to the
// default arm of topicScoreParams, which errors for unrecognised topics.
//
// Note the name collision this depends on: "execution_payload_segment" contains
// "execution_payload", so the segment case in topicScoreParams must stay above the
// envelope case. Both currently return the same params, so ordering is not observable
// here; it becomes load-bearing the moment segments get their own params.
func TestSegmentTopicHasScoringParams(t *testing.T) {
	require.Equal(t, true, strings.Contains(GossipExecutionPayloadSegmentMessage, GossipExecutionPayloadEnvelopeMessage),
		"segment topic name no longer contains the envelope name; the ordering note above is stale")

	s := &Service{}
	params, err := s.topicScoreParams(ExecutionPayloadSegmentTopicFormat)
	require.NoError(t, err)
	require.NotNil(t, params)
}

// TestSegmentScoring checks that SegmentScoring, which the payload path's integration tests score
// with, is what the node installs: its peer parameters and thresholds, and a segment topic's.
func TestSegmentScoring(t *testing.T) {
	peerParams, thresholds, topicParams := SegmentScoring()
	wantPeer, wantThresholds := peerScoringParams(nil)
	require.DeepEqual(t, wantThresholds, thresholds)
	// AppSpecificScore is a function, which DeepEqual can't compare.
	require.Equal(t, 0.0, peerParams.AppSpecificScore(""))
	peerParams.AppSpecificScore, wantPeer.AppSpecificScore = nil, nil
	require.DeepEqual(t, wantPeer, peerParams)

	wantTopic, err := (&Service{}).topicScoreParams(ExecutionPayloadSegmentTopicFormat)
	require.NoError(t, err)
	require.DeepEqual(t, wantTopic, topicParams)
}

// TestSegmentTopicAllowedOnlyWithTheFlag: the subscription allowlist holds the segment topic only on a
// node with segmented payload gossip on, so a node without it neither joins the topic nor accepts
// peers' subscriptions to it.
func TestSegmentTopicAllowedOnlyWithTheFlag(t *testing.T) {
	params.SetupTestConfigCleanup(t)
	cfg := params.MainnetConfig()
	cfg.FuluForkEpoch = 100
	cfg.GloasForkEpoch = 200
	params.OverrideBeaconConfig(cfg)
	digest := params.ForkDigest(cfg.GloasForkEpoch)
	segmentTopic := fmt.Sprintf(ExecutionPayloadSegmentTopicFormat, digest) + (&Service{}).Encoding().ProtocolSuffix()
	envelopeTopic := fmt.Sprintf(ExecutionPayloadEnvelopeTopicFormat, digest) + (&Service{}).Encoding().ProtocolSuffix()
	for _, on := range []bool{false, true} {
		t.Run(fmt.Sprintf("flag=%v", on), func(t *testing.T) {
			reset := features.InitWithReset(&features.Flags{EnableSegmentedPayloadGossip: on})
			defer reset()
			filt := pubsub.NewAllowlistSubscriptionFilter((&Service{}).allTopicStrings()...)
			require.Equal(t, true, filt.CanSubscribe(envelopeTopic), "the envelope topic")
			require.Equal(t, on, filt.CanSubscribe(segmentTopic), "the segment topic")
		})
	}
}
