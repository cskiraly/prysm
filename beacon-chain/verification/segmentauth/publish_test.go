package segmentauth

import (
	"testing"

	"github.com/OffchainLabs/prysm/v7/consensus-types/primitives"
	"github.com/OffchainLabs/prysm/v7/container/segments"
	enginev1 "github.com/OffchainLabs/prysm/v7/proto/engine/v1"
	ethpb "github.com/OffchainLabs/prysm/v7/proto/prysm/v1alpha1"
	"github.com/OffchainLabs/prysm/v7/testing/require"
)

// testEnvelope builds a signed envelope big enough to need several segments.
func testEnvelope(t *testing.T, slot primitives.Slot, builder primitives.BuilderIndex, txCount int) *ethpb.SignedExecutionPayloadEnvelope {
	txs := make([][]byte, txCount)
	for i := range txs {
		txs[i] = make([]byte, 4096)
		for j := range txs[i] {
			txs[i][j] = byte(i + j)
		}
	}
	root := make([]byte, 32)
	root[0] = 0xbe
	root[1] = 0xef
	return &ethpb.SignedExecutionPayloadEnvelope{
		Message: &ethpb.ExecutionPayloadEnvelope{
			Payload: &enginev1.ExecutionPayloadGloas{
				SlotNumber:    slot,
				Transactions:  txs,
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
			BuilderIndex:          builder,
			BeaconBlockRoot:       root,
			ParentBeaconBlockRoot: make([]byte, 32),
		},
		Signature: make([]byte, 96),
	}
}

// TestSegmentsForEnvelopeRoundTrip drives both sides: the publisher derives a segmentation, and
// a receiver's reassembler authenticates the group against the commitments it holds and rebuilds
// the exact envelope bytes.
func TestSegmentMessagesForEnvelopeRoundTrip(t *testing.T) {
	const slot = primitives.Slot(4096)
	const builder = primitives.BuilderIndex(11)
	signed := testEnvelope(t, slot, builder, 40)

	hasher, err := segments.HasherByID(segments.HashSHA256)
	require.NoError(t, err)
	encoded, err := signed.MarshalSSZ()
	require.NoError(t, err)
	descriptor, _, err := segments.Commit(encoded, 2*DefaultSegmentSize, hasher)
	require.NoError(t, err)
	require.Equal(t, true, descriptor.Count > 1, "envelope should need several segments")
	groupID := descriptor.GroupID(hasher)

	auth := Params{SegmentSize: descriptor.SegmentSize, HashID: segments.HashSHA256}

	t.Run("publisher builds segments a receiver authenticates", func(t *testing.T) {
		msgs, err := SegmentMessagesForEnvelope(signed, auth)
		require.NoError(t, err)
		require.Equal(t, int(descriptor.Count), len(msgs))

		r, err := segments.NewReassembler(segments.ReassemblerConfig{
			Auth: New(func(g []byte) bool { return string(g) == string(groupID) }),
		})
		require.NoError(t, err)
		var out []byte
		for _, built := range msgs {
			// Through the wire type, as the broadcaster sends it and a receiver decodes it.
			pb, err := built.ToProto()
			require.NoError(t, err)
			m, h, err := segments.FromProto(pb)
			require.NoError(t, err)
			got, err := r.Add(h, m)
			require.NoError(t, err)
			if got != nil {
				out = got
			}
		}
		require.DeepEqual(t, encoded, out)

		back := &ethpb.SignedExecutionPayloadEnvelope{}
		require.NoError(t, back.UnmarshalSSZ(out))
		require.Equal(t, slot, back.Message.Payload.SlotNumber)
	})

	// A segmentation that disagrees with what the builder committed to is no longer
	// detectable here. Under the detached-signature scheme the publisher could verify
	// locally, because the signature covered the group id; with the commitment in the block
	// there is nothing local to compare against until ExecutionPayloadBid carries it, so a
	// disagreement surfaces as peers rejecting the group. Recorded rather than dropped: it
	// is a real check that the interim scheme loses.
	t.Run("a different segmentation still produces a well-formed group", func(t *testing.T) {
		other := Params{SegmentSize: descriptor.SegmentSize / 2, HashID: auth.HashID}
		msgs, err := SegmentMessagesForEnvelope(signed, other)
		require.NoError(t, err)
		require.Equal(t, false, string(msgs[0].Descriptor.GroupID(hasher)) == string(groupID),
			"a different segment size must yield a different group id")
	})

	t.Run("unknown hash id rejected", func(t *testing.T) {
		bad := Params{SegmentSize: auth.SegmentSize, HashID: 200}
		_, err := SegmentMessagesForEnvelope(signed, bad)
		require.ErrorIs(t, err, segments.ErrUnknownHashID)
	})

	t.Run("invalid segment size rejected", func(t *testing.T) {
		for _, size := range []uint32{0, segments.MaxSegmentSize + 1} {
			bad := Params{SegmentSize: size, HashID: auth.HashID}
			_, err := SegmentMessagesForEnvelope(signed, bad)
			require.ErrorIs(t, err, segments.ErrSegmentSize)
		}
	})

	t.Run("nil envelope rejected", func(t *testing.T) {
		_, err := SegmentMessagesForEnvelope(nil, auth)
		require.ErrorIs(t, err, ErrEnvelopeMalformed)
	})

	t.Run("default params segment the envelope", func(t *testing.T) {
		segs, err := SegmentMessagesForEnvelope(signed, DefaultParams())
		require.NoError(t, err)
		want, _, err := segments.Commit(encoded, DefaultSegmentSize, hasher)
		require.NoError(t, err)
		require.Equal(t, int(want.Count), len(segs))
		require.Equal(t, true, want.Count > descriptor.Count, "the default cuts more segments than twice the default")
	})

}
