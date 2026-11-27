package segmentgossip_test

import (
	"bytes"
	"encoding/binary"
	"reflect"
	"testing"

	ethpb "github.com/OffchainLabs/prysm/v7/proto/prysm/v1alpha1"
	"github.com/OffchainLabs/prysm/v7/testing/require"
	core "github.com/cskiraly/seacast-go"
	"github.com/golang/snappy"
)

// TestCoreEncodingIsTheSegment checks the core's encoding against this port's types. The body of
// the payload's chunk (kind 0x06, seacast-bindings.md §1.2) is the ExecutionPayloadSegment's SSZ,
// both ways, and a cell's header is a PartialDataColumnHeader's SSZ.
func TestCoreEncodingIsTheSegment(t *testing.T) {
	seg := testSegment()
	ssz, err := seg.MarshalSSZ()
	require.NoError(t, err)
	desc, err := seg.SegmentDescriptor.MarshalSSZ()
	require.NoError(t, err)
	var key [32]byte
	copy(key[:], seg.Group)
	c := core.Chunk{
		Group:   core.NewGroupID(core.TypePayload, key, 0),
		Index:   seg.Index,
		Context: append(binary.LittleEndian.AppendUint64(nil, seg.Slot), seg.BeaconBlockRoot...),
		Header:  desc,
		Proof:   bytes.Join(seg.Proof, nil),
		Data:    seg.Data,
	}
	enc, err := core.Encode(c)
	require.NoError(t, err)
	require.Equal(t, byte(0x06), enc[0])
	body, err := snappy.Decode(nil, enc[1:])
	require.NoError(t, err)
	require.Equal(t, true, bytes.Equal(body, ssz), "the core's body is not the segment's SSZ")
	m, err := core.Decode(append([]byte{0x06}, snappy.Encode(nil, ssz)...))
	require.NoError(t, err)
	require.Equal(t, true, reflect.DeepEqual(m, c), "the segment's SSZ decodes to another chunk")

	h := &ethpb.PartialDataColumnHeader{
		KzgCommitments: [][]byte{bytes.Repeat([]byte{1}, 48), bytes.Repeat([]byte{2}, 48)},
		SignedBlockHeader: &ethpb.SignedBeaconBlockHeader{
			Header: &ethpb.BeaconBlockHeader{Slot: 9, ProposerIndex: 3, ParentRoot: bytes.Repeat([]byte{3}, 32),
				StateRoot: bytes.Repeat([]byte{4}, 32), BodyRoot: bytes.Repeat([]byte{5}, 32)},
			Signature: bytes.Repeat([]byte{6}, 96),
		},
		KzgCommitmentsInclusionProof: [][]byte{bytes.Repeat([]byte{7}, 32), bytes.Repeat([]byte{8}, 32),
			bytes.Repeat([]byte{9}, 32), bytes.Repeat([]byte{10}, 32)},
	}
	hb, err := h.MarshalSSZ()
	require.NoError(t, err)
	require.Equal(t, core.HeaderFixedLen+2*core.KZGCommitmentLen, len(hb))
	cell := core.Chunk{Group: core.NewGroupID(core.TypeColumn, key, 3), Index: 1,
		Data: bytes.Repeat([]byte{0x5a}, core.BytesPerCell), Proof: bytes.Repeat([]byte{0x6b}, core.KZGProofLen), Header: hb}
	enc, err = core.Encode(cell)
	require.NoError(t, err)
	m, err = core.Decode(enc)
	require.NoError(t, err)
	require.Equal(t, true, reflect.DeepEqual(m, cell), "a cell with a real header does not come back")
}
