package segmentgossip_test

import (
	"testing"

	"github.com/OffchainLabs/prysm/v7/beacon-chain/p2p/internal/segmentgossip"
	"github.com/OffchainLabs/prysm/v7/config/params"
	ethpb "github.com/OffchainLabs/prysm/v7/proto/prysm/v1alpha1"
	"github.com/OffchainLabs/prysm/v7/testing/require"
	"github.com/golang/snappy"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pubsubpb "github.com/libp2p/go-libp2p-pubsub/pb"
)

// TestPushWidth pins the size rule and its boundaries: 4 to 256 KiB, 3 to 1 MiB, 2 above.
func TestPushWidth(t *testing.T) {
	for _, tc := range []struct {
		length uint64
		want   int
	}{
		{1, 4},
		{128 << 10, 4},
		{256 << 10, 4},
		{256<<10 + 1, 3},
		{512 << 10, 3},
		{1 << 20, 3},
		{1<<20 + 1, 2},
		{2 << 20, 2},
		{64 << 20, 2},
	} {
		require.Equal(t, tc.want, segmentgossip.PushWidth(tc.length), "payload of %d bytes", tc.length)
	}
}

// TestPushWidthOf checks where the length is read from: the validator's decoded segment when
// present, the SSZ body otherwise, and the floor for a body that is not a segment.
func TestPushWidthOf(t *testing.T) {
	params.SetupTestConfigCleanup(t)
	segOf := func(totalLength uint64) *ethpb.ExecutionPayloadSegment {
		seg := testSegment()
		seg.SegmentDescriptor.TotalLength = totalLength
		return seg
	}
	wireOf := func(t *testing.T, seg *ethpb.ExecutionPayloadSegment) []byte {
		t.Helper()
		body, err := seg.MarshalSSZ()
		require.NoError(t, err)
		return snappy.Encode(nil, body)
	}

	t.Run("validated segment on the message", func(t *testing.T) {
		// The body says 2 MiB, the validator's segment says 128 KiB: the decoded segment wins,
		// so no message forwarded after validation is decoded twice.
		msg := &pubsub.Message{
			Message:       &pubsubpb.Message{Data: wireOf(t, segOf(2<<20))},
			ValidatorData: segOf(128 << 10),
		}
		require.Equal(t, 4, segmentgossip.PushWidthOf(msg))
	})

	t.Run("own segment, read from the body", func(t *testing.T) {
		for _, tc := range []struct {
			length uint64
			want   int
		}{{128 << 10, 4}, {768 << 10, 3}, {2 << 20, 2}} {
			msg := &pubsub.Message{Message: &pubsubpb.Message{Data: wireOf(t, segOf(tc.length))}}
			require.Equal(t, tc.want, segmentgossip.PushWidthOf(msg), "payload of %d bytes", tc.length)
		}
	})

	t.Run("completing segment carries the envelope, read from the body", func(t *testing.T) {
		msg := &pubsub.Message{
			Message:       &pubsubpb.Message{Data: wireOf(t, segOf(128<<10))},
			ValidatorData: &ethpb.SignedExecutionPayloadEnvelope{},
		}
		require.Equal(t, 4, segmentgossip.PushWidthOf(msg))
	})

	t.Run("not a segment: the floor", func(t *testing.T) {
		for _, data := range [][]byte{nil, []byte("not snappy at all"), snappy.Encode(nil, []byte("short"))} {
			msg := &pubsub.Message{Message: &pubsubpb.Message{Data: data}}
			require.Equal(t, 2, segmentgossip.PushWidthOf(msg))
		}
		require.Equal(t, 2, segmentgossip.PushWidthOf(nil))
	})
}
