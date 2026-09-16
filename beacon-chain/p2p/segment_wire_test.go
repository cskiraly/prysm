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

// TestSegmentWireBounds checks the wire type against the container's limits: a segment at
// MaxSegmentSize with a proof of MaxProofDepth elements marshals, one byte or one element
// more does not. The bounds are literals in the .proto file, so this is the drift guard.
func TestSegmentWireBounds(t *testing.T) {
	full := func() *ethpb.ExecutionPayloadSegment {
		proof := make([][]byte, segments.MaxProofDepth)
		for i := range proof {
			proof[i] = make([]byte, 32)
		}
		return &ethpb.ExecutionPayloadSegment{
			SegmentDescriptor: &ethpb.SegmentDescriptor{Root: make([]byte, 32)},
			Proof:             proof,
			Data:              make([]byte, segments.MaxSegmentSize),
		}
	}

	t.Run("maximum size segment marshals", func(t *testing.T) {
		enc, err := full().MarshalSSZ()
		require.NoError(t, err)
		require.Equal(t, true, len(enc) > segments.MaxSegmentSize)
	})

	t.Run("one byte of data over the bound is rejected", func(t *testing.T) {
		m := full()
		m.Data = append(m.Data, 0)
		_, err := m.MarshalSSZ()
		require.Equal(t, true, err != nil, "ssz_max on data is larger than MaxSegmentSize")
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
