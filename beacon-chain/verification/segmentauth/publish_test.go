package segmentauth

import (
	"testing"

	"github.com/OffchainLabs/prysm/v7/consensus-types/primitives"
	"github.com/OffchainLabs/prysm/v7/container/segments"
	enginev1 "github.com/OffchainLabs/prysm/v7/proto/engine/v1"
	ethpb "github.com/OffchainLabs/prysm/v7/proto/prysm/v1alpha1"
	"github.com/OffchainLabs/prysm/v7/testing/require"
	"github.com/golang/snappy"
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

	t.Run("unknown encoding rejected", func(t *testing.T) {
		bad := Params{SegmentSize: auth.SegmentSize, HashID: auth.HashID, Encoding: segments.EncodingSnappy + 1}
		_, err := SegmentMessagesForEnvelope(signed, bad)
		require.ErrorIs(t, err, segments.ErrEncoding)
	})

	// The default: compressed first, coded at rate one half, the data segment count set by
	// the envelope's SSZ size at DefaultSegmentSize. A receiver completes at any half of the
	// group and decompresses what it recovers.
	t.Run("default params publish a coded, compress-first group", func(t *testing.T) {
		segs, err := SegmentMessagesForEnvelope(signed, DefaultParams())
		require.NoError(t, err)
		d := segs[0].Descriptor
		k := (len(encoded) + DefaultSegmentSize - 1) / DefaultSegmentSize
		require.Equal(t, segments.VersionCoded, d.Version)
		require.Equal(t, segments.EncodingSnappy, d.Encoding)
		require.Equal(t, uint32(k), d.Required(), "data segments follow the envelope's SSZ size")
		require.Equal(t, uint32(2*k), d.Count, "as many parity segments as data segments")
		require.Equal(t, 2*k, len(segs))
		compressed := snappy.Encode(nil, encoded)
		require.Equal(t, uint64(len(compressed)), d.TotalLength, "the committed bytes are the compressed envelope")
		require.Equal(t, true, d.SegmentSize < DefaultSegmentSize, "compressed bytes cut into K are smaller segments")

		// A receiver fed the second half of the group only: parity for every data segment.
		r, err := segments.NewReassembler(segments.ReassemblerConfig{
			Auth: New(func(g []byte) bool { return string(g) == string(d.GroupID(hasher)) }),
		})
		require.NoError(t, err)
		var out []byte
		for _, built := range segs[k:] {
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
		require.DeepEqual(t, compressed, out)
		back, err := snappy.Decode(nil, out)
		require.NoError(t, err)
		require.DeepEqual(t, encoded, back)
	})

	t.Run("an envelope the code cannot hold goes plain, still compressed", func(t *testing.T) {
		// 2K segments must fit the field's 256 points, so at 16 KiB an envelope above 2 MiB
		// of SSZ is published as a plain compressed group rather than not at all.
		require.Equal(t, 2<<20, MaxCodedPayload(DefaultSegmentSize))
		big := testEnvelope(t, slot, builder, 600)
		bigEncoded, err := big.MarshalSSZ()
		require.NoError(t, err)
		require.Equal(t, true, len(bigEncoded) > MaxCodedPayload(DefaultSegmentSize))
		segs, err := SegmentMessagesForEnvelope(big, DefaultParams())
		require.NoError(t, err)
		d := segs[0].Descriptor
		require.Equal(t, segments.Version, d.Version)
		require.Equal(t, segments.EncodingSnappy, d.Encoding)
		require.Equal(t, uint32(DefaultSegmentSize), d.SegmentSize)
		require.Equal(t, d.Count, d.Required())
	})
}
