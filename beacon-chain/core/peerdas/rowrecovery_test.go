package peerdas_test

import (
	"fmt"
	"testing"

	"github.com/OffchainLabs/prysm/v7/beacon-chain/blockchain/kzg"
	"github.com/OffchainLabs/prysm/v7/beacon-chain/core/peerdas"
	fieldparams "github.com/OffchainLabs/prysm/v7/config/fieldparams"
	"github.com/OffchainLabs/prysm/v7/consensus-types/blocks"
	ethpb "github.com/OffchainLabs/prysm/v7/proto/prysm/v1alpha1"
	"github.com/OffchainLabs/prysm/v7/testing/require"
	"github.com/OffchainLabs/prysm/v7/testing/util"
)

// rowFixture is one blob's worth of real cells and proofs, plus an empty PartialDataRow to
// fill from them.
type rowFixture struct {
	cells  []kzg.Cell
	proofs []kzg.Proof
	row    blocks.PartialDataRow
}

func newRowFixture(t *testing.T, blobCount, rowIndex int) *rowFixture {
	t.Helper()
	require.NoError(t, kzg.Start())

	_, roBlobSidecars := util.GenerateTestElectraBlockWithSidecar(t, [32]byte{}, 42, blobCount)
	blobs := make([]kzg.Blob, blobCount)
	for i := range blobCount {
		copy(blobs[i][:], roBlobSidecars[i].Blob)
	}
	// cellsPerBlob is indexed [blob][column], so one entry of it is exactly a row.
	cellsPerBlob, proofsPerBlob := util.GenerateCellsAndProofs(t, blobs)
	require.Equal(t, fieldparams.NumberOfColumns, len(cellsPerBlob[rowIndex]))

	commitments := make([][]byte, blobCount)
	for i := range blobCount {
		commitment, err := kzg.BlobToKZGCommitment(&blobs[i])
		require.NoError(t, err)
		commitments[i] = commitment[:]
	}

	header := &ethpb.SignedBeaconBlockHeader{
		Header: &ethpb.BeaconBlockHeader{
			Slot:       42,
			ParentRoot: make([]byte, fieldparams.RootLength),
			StateRoot:  make([]byte, fieldparams.RootLength),
			BodyRoot:   make([]byte, fieldparams.RootLength),
		},
		Signature: make([]byte, 96),
	}
	// The header container cannot encode a proof of any other length, and NewPartialDataRow
	// refuses one; recovery does not look at it.
	inclusionProof := make([][]byte, 4)
	for i := range inclusionProof {
		inclusionProof[i] = make([]byte, fieldparams.RootLength)
	}
	row, err := blocks.NewPartialDataRow([fieldparams.RootLength]byte{1}, header, uint64(rowIndex), commitments, inclusionProof)
	require.NoError(t, err)

	return &rowFixture{
		cells:  cellsPerBlob[rowIndex],
		proofs: proofsPerBlob[rowIndex],
		row:    row,
	}
}

// fill adds the cells at the given column indices to the row.
func (f *rowFixture) fill(t *testing.T, columnIndices ...uint64) {
	t.Helper()

	for _, columnIndex := range columnIndices {
		cell := f.cells[columnIndex]
		proof := f.proofs[columnIndex]
		require.Equal(t, true, f.row.ExtendFromVerifiedCell(columnIndex, cell[:], proof[:]))
	}
}

func columnRange(from, to uint64) []uint64 {
	indices := make([]uint64, 0, to-from)
	for i := from; i < to; i++ {
		indices = append(indices, i)
	}

	return indices
}

// TestVerifyCellsKZGProofsOnTheRowAxis checks the claim that the row axis needs no
// verification function of its own: a row's cells share one commitment and vary in cell index,
// which the same batch verifier handles.
func TestVerifyCellsKZGProofsOnTheRowAxis(t *testing.T) {
	fixture := newRowFixture(t, 3, 1)

	// A message carrying half the row, as a peer would send it.
	present := columnRange(0, 64)
	message := &ethpb.PartialDataRowSidecar{
		RowIndex:           1,
		CellsPresentBitmap: rowBitmap(present...),
	}
	for _, columnIndex := range present {
		cell := fixture.cells[columnIndex]
		proof := fixture.proofs[columnIndex]
		message.PartialRow = append(message.PartialRow, cell[:])
		message.KzgProofs = append(message.KzgProofs, proof[:])
	}

	indices, bundles, err := fixture.row.CellsToVerifyFromPartialMessage(message)
	require.NoError(t, err)
	require.Equal(t, len(present), len(indices))
	require.NoError(t, peerdas.VerifyCellsKZGProofs(bundles))

	// Corrupting one cell must fail the batch.
	bundles[7].Cell = append([]byte(nil), bundles[7].Cell...)
	bundles[7].Cell[0] ^= 0xff
	require.NotNil(t, peerdas.VerifyCellsKZGProofs(bundles))
}

// TestVerifyCellsKZGProofsRejectsAWrongCommitment guards against the row-axis mistake that
// column code cannot make: using the wrong blob's commitment for the whole row.
func TestVerifyCellsKZGProofsRejectsAWrongCommitment(t *testing.T) {
	fixture := newRowFixture(t, 3, 1)
	fixture.fill(t, 0)

	other := newRowFixture(t, 3, 2)
	bundles := []blocks.CellProofBundle{{
		ColumnIndex: 0,
		Commitment:  other.row.Commitment(),
		Cell:        fixture.cells[0][:],
		Proof:       fixture.proofs[0][:],
	}}
	require.NotNil(t, peerdas.VerifyCellsKZGProofs(bundles))
}

func TestRecoverRow(t *testing.T) {
	t.Run("recovers from exactly the threshold", func(t *testing.T) {
		fixture := newRowFixture(t, 2, 0)
		// The interesting case for RowDAS: the lower half of the columns, which is what a
		// node custodying columns 0..63 would hold.
		fixture.fill(t, columnRange(0, 64)...)
		require.Equal(t, true, fixture.row.ReconstructionThresholdMet())
		require.Equal(t, false, fixture.row.IsComplete())

		require.NoError(t, peerdas.RecoverRow(&fixture.row))
		require.Equal(t, true, fixture.row.IsComplete())

		// Every recovered cell and proof must match what the original encoding produced.
		for columnIndex := range uint64(fieldparams.NumberOfColumns) {
			cell := fixture.cells[columnIndex]
			proof := fixture.proofs[columnIndex]
			require.DeepEqual(t, cell[:], fixture.row.Cells[columnIndex], fmt.Sprintf("cell %d", columnIndex))
			require.DeepEqual(t, proof[:], fixture.row.Proofs[columnIndex], fmt.Sprintf("proof %d", columnIndex))
		}
	})

	t.Run("recovers from scattered indices", func(t *testing.T) {
		// A pooled-custody row: cells arriving from many peers holding disjoint columns, so
		// the present indices are spread rather than contiguous.
		fixture := newRowFixture(t, 2, 1)
		scattered := make([]uint64, 0, 64)
		for i := range uint64(64) {
			scattered = append(scattered, i*2)
		}
		fixture.fill(t, scattered...)

		require.NoError(t, peerdas.RecoverRow(&fixture.row))
		require.Equal(t, true, fixture.row.IsComplete())
		for columnIndex := range uint64(fieldparams.NumberOfColumns) {
			cell := fixture.cells[columnIndex]
			require.DeepEqual(t, cell[:], fixture.row.Cells[columnIndex])
		}
	})

	t.Run("below the threshold", func(t *testing.T) {
		fixture := newRowFixture(t, 2, 0)
		fixture.fill(t, columnRange(0, 63)...)
		require.ErrorIs(t, peerdas.RecoverRow(&fixture.row), peerdas.ErrRowBelowReconstructionThreshold)
		require.Equal(t, uint64(63), fixture.row.Included.Count(), "a failed recovery must not mutate the row")
	})

	t.Run("complete row is a no-op", func(t *testing.T) {
		fixture := newRowFixture(t, 2, 0)
		fixture.fill(t, columnRange(0, fieldparams.NumberOfColumns)...)
		require.NoError(t, peerdas.RecoverRow(&fixture.row))
		require.Equal(t, true, fixture.row.IsComplete())
	})

	t.Run("recovered row verifies against its commitment", func(t *testing.T) {
		// The point of checking this rather than trusting the bindings: a recovered row is
		// re-served to other peers, so a bad recovery would be indistinguishable from
		// malice at the receiving end.
		fixture := newRowFixture(t, 2, 1)
		fixture.fill(t, columnRange(64, fieldparams.NumberOfColumns)...)
		require.NoError(t, peerdas.RecoverRow(&fixture.row))

		bundles := make([]blocks.CellProofBundle, 0, fieldparams.NumberOfColumns)
		for columnIndex := range uint64(fieldparams.NumberOfColumns) {
			bundles = append(bundles, blocks.CellProofBundle{
				ColumnIndex: columnIndex,
				Commitment:  fixture.row.Commitment(),
				Cell:        fixture.row.Cells[columnIndex],
				Proof:       fixture.row.Proofs[columnIndex],
			})
		}
		require.NoError(t, peerdas.VerifyCellsKZGProofs(bundles))
	})
}
