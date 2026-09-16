package kzg

// What does recovering a row actually cost, and how much of it is the proofs?
//
// RowDAS prices its reconstruction duty in these numbers (notes/rowdas/TODO.md D5), and the first
// pricing used RecoverCellsAndKZGProofs throughout without checking how much of that is the proof
// computation rather than the erasure decode. These two benchmarks are the split.

import (
	"testing"

	"github.com/OffchainLabs/prysm/v7/crypto/random"
	"github.com/OffchainLabs/prysm/v7/testing/require"
)

// halfCells returns 64 of a random blob's 128 cells, which is exactly the reconstruction
// threshold and so the worst case for recovery.
func halfCells(tb testing.TB) ([]uint64, []Cell) {
	require.NoError(tb, Start())

	randBlob := random.GetRandBlob(123)
	var blob Blob
	copy(blob[:], randBlob[:])
	cells, err := ComputeCells(&blob)
	require.NoError(tb, err)

	indices := make([]uint64, 64)
	partial := make([]Cell, 64)
	for i := range 64 {
		indices[i] = uint64(i)
		partial[i] = cells[i]
	}

	return indices, partial
}

// BenchmarkRecoverCells is the erasure decode alone: enough to hold the data, not enough to serve
// it, since a peer verifies a cell against its proof.
func BenchmarkRecoverCells(b *testing.B) {
	indices, partial := halfCells(b)

	for b.Loop() {
		if _, err := RecoverCells(indices, partial); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkRecoverCellsAndKZGProofs is what RecoverRow calls, and what a node that intends to
// serve the recovered cells needs.
func BenchmarkRecoverCellsAndKZGProofs(b *testing.B) {
	indices, partial := halfCells(b)

	for b.Loop() {
		if _, _, err := RecoverCellsAndKZGProofs(indices, partial); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkComputeCellsAndKZGProofsFromBlob is the second half of a split recovery: having
// recovered the cells, the first 64 of them *are* the blob (the encoding is systematic), so the
// proofs can be computed later from that -- only if the node still needs to serve.
//
// The question this answers is whether splitting costs anything. If RecoverCells plus this is about
// the same as RecoverCellsAndKZGProofs, then a node can pay 17 ms up front and defer the other
// 145 ms until it knows nobody else has served the row.
func BenchmarkComputeCellsAndKZGProofsFromBlob(b *testing.B) {
	require.NoError(b, Start())
	randBlob := random.GetRandBlob(123)
	var blob Blob
	copy(blob[:], randBlob[:])

	for b.Loop() {
		if _, _, err := ComputeCellsAndKZGProofs(&blob); err != nil {
			b.Fatal(err)
		}
	}
}

// TestRecoveredCellsGiveBackTheBlob is what the split above rests on: that the first half of the
// 128 cells is the original blob, so a node that recovered cells only can compute the proofs later
// without keeping anything else. If the encoding were not systematic, deferring the proofs would
// mean re-deriving the blob and the split would not be free.
func TestRecoveredCellsGiveBackTheBlob(t *testing.T) {
	require.NoError(t, Start())

	randBlob := random.GetRandBlob(123)
	var blob Blob
	copy(blob[:], randBlob[:])

	indices, partial := halfCells(t)
	recovered, err := RecoverCells(indices, partial)
	require.NoError(t, err)
	require.Equal(t, 128, len(recovered))

	// The first 64 cells, concatenated, should be the blob.
	var rebuilt Blob
	offset := 0
	for i := range 64 {
		copy(rebuilt[offset:], recovered[i][:])
		offset += len(recovered[i])
	}
	require.Equal(t, len(blob), offset, "64 cells should tile the blob exactly")
	require.DeepEqual(t, blob[:], rebuilt[:], "the cell encoding should be systematic")

	// And the proofs computed from the rebuilt blob must be the real ones.
	cells, proofs, err := ComputeCellsAndKZGProofs(&rebuilt)
	require.NoError(t, err)
	require.Equal(t, 128, len(cells))
	require.Equal(t, 128, len(proofs))
	for i := range 128 {
		require.DeepEqual(t, recovered[i][:], cells[i][:],
			"cells from the rebuilt blob should match the recovered ones")
	}
}

// The row axis can verify a way the column axis cannot, and these benchmarks price the difference.
//
// A column holds one cell of each blob, so it can only ever verify cell by cell, against a cell
// proof. A *row* holds many cells of one blob -- so once it holds 64 it can reconstruct the blob and
// check `BlobToKZGCommitment(blob) == commitment`, using the commitment the block header already
// carries. One commitment computation instead of 128 cell-proof verifications, and no cell proofs
// required at all.

// BenchmarkBlobToKZGCommitment is the row-axis alternative: verify a recovered row by recomputing
// its commitment.
func BenchmarkBlobToKZGCommitment(b *testing.B) {
	require.NoError(b, Start())
	randBlob := random.GetRandBlob(123)
	var blob Blob
	copy(blob[:], randBlob[:])

	for b.Loop() {
		if _, err := BlobToKZGCommitment(&blob); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkVerifyCellKZGProofBatch128 is what the row path does today: batch-verify all 128 cells
// of a row against the single commitment, one proof each.
func BenchmarkVerifyCellKZGProofBatch128(b *testing.B) {
	require.NoError(b, Start())
	randBlob := random.GetRandBlob(123)
	var blob Blob
	copy(blob[:], randBlob[:])

	cells, proofs, err := ComputeCellsAndKZGProofs(&blob)
	require.NoError(b, err)
	commitment, err := BlobToKZGCommitment(&blob)
	require.NoError(b, err)

	commitments := make([]Bytes48, 128)
	indices := make([]uint64, 128)
	cellSlice := make([]Cell, 128)
	proofSlice := make([]Bytes48, 128)
	for i := range 128 {
		commitments[i] = Bytes48(commitment)
		indices[i] = uint64(i)
		cellSlice[i] = cells[i]
		proofSlice[i] = Bytes48(proofs[i])
	}

	for b.Loop() {
		ok, err := VerifyCellKZGProofBatch(commitments, indices, cellSlice, proofSlice)
		if err != nil || !ok {
			b.Fatalf("verify failed: ok=%v err=%v", ok, err)
		}
	}
}

// TestRecoveredRowVerifiesByCommitment pins the claim the benchmarks are about: 64 cells are enough
// to rebuild the blob, and the commitment it yields is the one the block header carries. So a row
// can be verified without any cell proof.
func TestRecoveredRowVerifiesByCommitment(t *testing.T) {
	require.NoError(t, Start())

	randBlob := random.GetRandBlob(123)
	var blob Blob
	copy(blob[:], randBlob[:])
	expected, err := BlobToKZGCommitment(&blob)
	require.NoError(t, err)

	indices, partial := halfCells(t)
	recovered, err := RecoverCells(indices, partial)
	require.NoError(t, err)

	var rebuilt Blob
	offset := 0
	for i := range 64 {
		copy(rebuilt[offset:], recovered[i][:])
		offset += len(recovered[i])
	}
	got, err := BlobToKZGCommitment(&rebuilt)
	require.NoError(t, err)
	require.DeepEqual(t, expected[:], got[:],
		"a row recovered from 64 cells should reproduce the header's commitment, with no cell proofs involved")
}

// BenchmarkComputeCells is the cost of extending a blob to its 128 cells *without* proofs, which is
// what a node needs when it already has the proofs -- the case where the blob transaction is in the
// local execution client, since EIP-7594 has that wrapper carry CELLS_PER_EXT_BLOB cell proofs per
// blob.
func BenchmarkComputeCells(b *testing.B) {
	require.NoError(b, Start())
	randBlob := random.GetRandBlob(123)
	var blob Blob
	copy(blob[:], randBlob[:])

	for b.Loop() {
		if _, err := ComputeCells(&blob); err != nil {
			b.Fatal(err)
		}
	}
}
