package eth_test

import (
	"os"
	"slices"
	"testing"

	"github.com/OffchainLabs/go-bitfield"
	fieldparams "github.com/OffchainLabs/prysm/v7/config/fieldparams"
	eth "github.com/OffchainLabs/prysm/v7/proto/prysm/v1alpha1"
	"github.com/OffchainLabs/prysm/v7/testing/require"
)

// Local copies of the ssz_max/ssz_size symbols the row container is bound by, so this test
// pins the generated code against the values in proto/ssz_proto_library.bzl without pulling
// the cgo KZG bindings into the proto test binary.
const (
	rowBytesPerCell            = 2048 // bytes_per_cell.size
	rowInclusionProofDepth     = 4    // kzg_commitments_inclusion_proof_depth.size
	rowCellsPerExtendedBlob    = fieldparams.NumberOfColumns
	rowKzgCommitmentByteLength = 48
)

// TestPartialDataRowsProtoGoPackageVersion mirrors the partial-column guard: a stale
// go_package would make the next codegen run emit the wrong import path.
func TestPartialDataRowsProtoGoPackageVersion(t *testing.T) {
	content, err := os.ReadFile("partial_data_rows.proto")
	require.NoError(t, err, "failed to read proto file")

	want := `go_package = "github.com/OffchainLabs/prysm/v7/proto/prysm/v1alpha1;eth"`
	require.StringContains(t, want, string(content), "partial_data_rows.proto has wrong go_package")
}

func filled(length int, fill byte) []byte {
	out := make([]byte, length)
	for i := range out {
		out[i] = fill
	}

	return out
}

// sparseRow builds a row sidecar carrying the cells at the given column indices, packed in
// ascending index order as the bitmap requires.
func sparseRow(t *testing.T, rowIndex uint64, columnIndices ...uint64) *eth.PartialDataRowSidecar {
	t.Helper()

	bitmap := bitfield.NewBitlist(rowCellsPerExtendedBlob)
	for _, columnIndex := range columnIndices {
		bitmap.SetBitAt(columnIndex, true)
	}

	sidecar := &eth.PartialDataRowSidecar{RowIndex: rowIndex}
	for columnIndex := range uint64(rowCellsPerExtendedBlob) {
		if !bitmap.BitAt(columnIndex) {
			continue
		}
		sidecar.PartialRow = append(sidecar.PartialRow, filled(rowBytesPerCell, byte(columnIndex)))
		sidecar.KzgProofs = append(sidecar.KzgProofs, filled(rowKzgCommitmentByteLength, byte(columnIndex)))
	}
	sidecar.CellsPresentBitmap = bitmap

	return sidecar
}

func requireCellsEqual(t *testing.T, want, got [][]byte, label string) {
	t.Helper()

	require.Equal(t, len(want), len(got), label+": length differs")
	for i := range want {
		require.Equal(t, true, slices.Equal(want[i], got[i]), label+": element differs")
	}
}

func requireRowRoundTrip(t *testing.T, in *eth.PartialDataRowSidecar) {
	t.Helper()

	encoded, err := in.MarshalSSZ()
	require.NoError(t, err)
	require.Equal(t, in.SizeSSZ(), len(encoded), "SizeSSZ disagrees with MarshalSSZ")

	out := &eth.PartialDataRowSidecar{}
	require.NoError(t, out.UnmarshalSSZ(encoded))

	require.Equal(t, in.RowIndex, out.RowIndex)
	require.Equal(t, true, slices.Equal(in.CellsPresentBitmap, out.CellsPresentBitmap), "bitmap differs")
	requireCellsEqual(t, in.PartialRow, out.PartialRow, "cells")
	requireCellsEqual(t, in.KzgProofs, out.KzgProofs, "proofs")
	require.Equal(t, len(in.Header), len(out.Header))

	// Re-encoding must be byte identical, which catches a decode that silently drops or
	// reorders the packed cells while still producing the right count.
	reencoded, err := out.MarshalSSZ()
	require.NoError(t, err)
	require.Equal(t, true, slices.Equal(encoded, reencoded), "re-encoding is not byte identical")

	inRoot, err := in.HashTreeRoot()
	require.NoError(t, err)
	outRoot, err := out.HashTreeRoot()
	require.NoError(t, err)
	require.Equal(t, inRoot, outRoot)
}

func TestPartialDataRowSidecar_RoundTripSparse(t *testing.T) {
	requireRowRoundTrip(t, sparseRow(t, 3, 0))
	requireRowRoundTrip(t, sparseRow(t, 0, 7, 42, 127))
}

func TestPartialDataRowSidecar_RoundTripFullRow(t *testing.T) {
	// A complete row: every column of one blob. This is what a reconstructor holds, and the
	// largest cell payload the container can carry.
	columnIndices := make([]uint64, 0, rowCellsPerExtendedBlob)
	for columnIndex := range uint64(rowCellsPerExtendedBlob) {
		columnIndices = append(columnIndices, columnIndex)
	}
	full := sparseRow(t, 11, columnIndices...)
	require.Equal(t, rowCellsPerExtendedBlob, len(full.PartialRow))

	requireRowRoundTrip(t, full)
}

func TestPartialDataRowSidecar_RoundTripHeaderOnly(t *testing.T) {
	// Rows have no full-message form, so a header with no cells is a legitimate message: it
	// is what an eager push carries before any cell has been requested.
	inclusionProof := make([][]byte, rowInclusionProofDepth)
	for i := range inclusionProof {
		inclusionProof[i] = filled(32, byte(i))
	}

	headerOnly := &eth.PartialDataRowSidecar{
		RowIndex:           1,
		CellsPresentBitmap: bitfield.NewBitlist(rowCellsPerExtendedBlob),
		Header: []*eth.PartialDataColumnHeader{{
			KzgCommitments: [][]byte{
				filled(rowKzgCommitmentByteLength, 1),
				filled(rowKzgCommitmentByteLength, 2),
			},
			SignedBlockHeader: &eth.SignedBeaconBlockHeader{
				Header: &eth.BeaconBlockHeader{
					Slot:          7,
					ProposerIndex: 3,
					ParentRoot:    filled(32, 4),
					StateRoot:     filled(32, 5),
					BodyRoot:      filled(32, 6),
				},
				Signature: filled(96, 7),
			},
			KzgCommitmentsInclusionProof: inclusionProof,
		}},
	}

	requireRowRoundTrip(t, headerOnly)
}

func TestPartialDataRowSidecar_WrongCellSizeIsRejected(t *testing.T) {
	// Cells are fixed at BYTES_PER_CELL. A short cell must not encode, or the packing of
	// the cell list against the bitmap could be desynchronised.
	sidecar := sparseRow(t, 0, 5)
	sidecar.PartialRow[0] = filled(rowBytesPerCell-1, 1)

	_, err := sidecar.MarshalSSZ()
	require.NotNil(t, err)
}

func TestPartialDataRowSidecar_OverlongBitmapIsRejectedOnDecode(t *testing.T) {
	// The bitmap is bounded by the number of columns, not by the blob count: unlike a
	// column, a row has the same number of parts whatever the block holds. The bound has to
	// hold on decode, because that is the direction a peer controls.
	//
	// A 129-bit bitlist is not testable here: it serialises to the same 17 bytes as a
	// 128-bit one and differs only in where the sentinel bit sits, so only a length clearly
	// over the bound is distinguishable at the SSZ layer.
	sidecar := &eth.PartialDataRowSidecar{
		RowIndex:           0,
		CellsPresentBitmap: bitfield.NewBitlist(rowCellsPerExtendedBlob * 2),
	}

	encoded, err := sidecar.MarshalSSZ()
	require.NoError(t, err, "marshalling does not enforce the bound; decoding must")

	out := &eth.PartialDataRowSidecar{}
	require.NotNil(t, out.UnmarshalSSZ(encoded), "a bitmap longer than NUMBER_OF_COLUMNS must not decode")
}
