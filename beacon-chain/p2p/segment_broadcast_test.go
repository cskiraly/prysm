package p2p

import (
	"context"
	"testing"

	p2ptest "github.com/OffchainLabs/prysm/v7/beacon-chain/p2p/testing"
	"github.com/OffchainLabs/prysm/v7/config/params"
	"github.com/OffchainLabs/prysm/v7/container/segments"
	ethpb "github.com/OffchainLabs/prysm/v7/proto/prysm/v1alpha1"
	"github.com/OffchainLabs/prysm/v7/testing/require"
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

	t.Run("segments survive variant A's SSZ frame", func(t *testing.T) {
		// The publisher frames codec bytes in SSZ; a reader must recover them exactly, or
		// reassembly on the far side would fail for reasons unrelated to gossip.
		for _, m := range segmentMessages(t, 300, 64) {
			codec, err := m.Marshal()
			require.NoError(t, err)
			enc, err := (&ethpb.ExecutionPayloadSegment{Segment: codec}).MarshalSSZ()
			require.NoError(t, err)
			back := &ethpb.ExecutionPayloadSegment{}
			require.NoError(t, back.UnmarshalSSZ(enc))
			seg, hasher, err := segments.UnmarshalSegmentMessage(back.Segment)
			require.NoError(t, err)
			require.NoError(t, seg.Verify(hasher))
		}
	})
}
