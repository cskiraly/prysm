package p2p

import (
	"strings"
	"testing"

	"github.com/OffchainLabs/prysm/v7/container/segments"
	ethpb "github.com/OffchainLabs/prysm/v7/proto/prysm/v1alpha1"
	"github.com/OffchainLabs/prysm/v7/testing/require"
)

// TestSegmentTopicIsMapped guards the decode path: decodePubsubMessage returns
// ErrMessageNotMapped for a topic with no proto type, so the mapping must exist.
func TestSegmentTopicIsMapped(t *testing.T) {
	msg := GossipTopicMappings(ExecutionPayloadSegmentTopicFormat, 0)
	require.NotNil(t, msg)
	_, ok := msg.(*ethpb.ExecutionPayloadSegment)
	require.Equal(t, true, ok, "segment topic maps to the wrong type")
}

// TestSegmentSszMaxCoversCodec is a drift guard. The ssz_max on
// ethpb.ExecutionPayloadSegment is a literal in the .proto file, while the codec's real
// bound lives in container/segments. If either moves without the other, a maximum-size
// segment stops marshalling and the failure would otherwise only show up on a live
// network carrying a worst-case message.
func TestSegmentSszMaxCoversCodec(t *testing.T) {
	t.Run("maximum size segment marshals", func(t *testing.T) {
		m := &ethpb.ExecutionPayloadSegment{Segment: make([]byte, segments.MaxSegmentMessageSize)}
		enc, err := m.MarshalSSZ()
		require.NoError(t, err)
		require.Equal(t, true, len(enc) >= segments.MaxSegmentMessageSize)
	})

	t.Run("one byte over the bound is rejected", func(t *testing.T) {
		// Confirms the bound is actually enforced, so the test above is meaningful.
		m := &ethpb.ExecutionPayloadSegment{Segment: make([]byte, segments.MaxSegmentMessageSize+1)}
		_, err := m.MarshalSSZ()
		require.Equal(t, true, err != nil, "ssz_max is larger than the codec bound")
	})

	t.Run("round trips", func(t *testing.T) {
		want := []byte("a segment blob")
		enc, err := (&ethpb.ExecutionPayloadSegment{Segment: want}).MarshalSSZ()
		require.NoError(t, err)
		got := &ethpb.ExecutionPayloadSegment{}
		require.NoError(t, got.UnmarshalSSZ(enc))
		require.DeepEqual(t, want, got.Segment)
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
