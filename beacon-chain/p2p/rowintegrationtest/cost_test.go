package rowintegrationtest

// What the harness actually costs, so a fidelity decision rests on numbers.
//
// The intuition that real KZG makes a network experiment slow turns out to be wrong here, and
// this test is what says so. Verification is microseconds per batch; the fixed costs are the
// trusted setup and gossipsub's mesh formation, and faking the crypto touches neither.

import (
	"testing"
	"time"

	"github.com/OffchainLabs/prysm/v7/beacon-chain/blockchain/kzg"
	"github.com/OffchainLabs/prysm/v7/beacon-chain/core/peerdas"
	"github.com/OffchainLabs/prysm/v7/consensus-types/blocks"
	"github.com/OffchainLabs/prysm/v7/testing/require"
	"github.com/OffchainLabs/prysm/v7/testing/util"
)

func TestHarnessCostBreakdown(t *testing.T) {
	start := time.Now()
	require.NoError(t, kzg.Start())
	setup := time.Since(start)

	start = time.Now()
	_, roBlobSidecars := util.GenerateTestElectraBlockWithSidecar(t, [32]byte{}, harnessSlot, 4)
	blobs := make([]kzg.Blob, 4)
	for i := range 4 {
		copy(blobs[i][:], roBlobSidecars[i].Blob)
	}
	blockGen := time.Since(start)

	start = time.Now()
	_, _ = util.GenerateCellsAndProofs(t, blobs)
	cellGen := time.Since(start)

	m := newMatrix(t, 1)

	// One batch as a node receives it: cells verified against the row's single commitment at
	// that many different cell indices, which is the row axis.
	rowBatch := make([]blocks.CellProofBundle, 0, 60)
	for column := range uint64(60) {
		cell := m.cells[0][column]
		proof := m.proofs[0][column]
		rowBatch = append(rowBatch, blocks.CellProofBundle{
			ColumnIndex: column,
			Commitment:  m.commitments[0],
			Cell:        cell[:],
			Proof:       proof[:],
		})
	}

	start = time.Now()
	require.NoError(t, peerdas.VerifyCellsKZGProofs(rowBatch))
	verifyLarge := time.Since(start)

	start = time.Now()
	require.NoError(t, peerdas.VerifyCellsKZGProofs(rowBatch[:4]))
	verifySmall := time.Since(start)

	columns := make([]uint64, 0, 64)
	for i := range uint64(64) {
		columns = append(columns, i)
	}
	row := m.row(t, 0, columns)
	start = time.Now()
	require.NoError(t, peerdas.RecoverRow(&row))
	recovery := time.Since(start)

	t.Log("harness cost breakdown:")
	t.Logf("  kzg.Start, trusted setup           %8v   once per process", setup.Round(time.Millisecond))
	t.Logf("  block and blob sidecar generation  %8v   once per process (BLS setup dominates)", blockGen.Round(time.Millisecond))
	t.Logf("  cells and proofs, 4 blobs          %8v   %v per blob, once per matrix",
		cellGen.Round(time.Millisecond), (cellGen / 4).Round(time.Millisecond))
	t.Logf("  verify a 60-cell batch             %8v   per received batch", verifyLarge.Round(time.Microsecond))
	t.Logf("  verify a 4-cell batch              %8v   per received batch", verifySmall.Round(time.Microsecond))
	t.Logf("  recover one row from 64 cells      %8v   per recovery", recovery.Round(time.Millisecond))
	t.Logf("  gossipsub mesh formation           %8v   per network built", gossipsimMeshFormation)

	// The conclusion this test exists to pin: verification is not the cost. A 16-node run
	// verifies of the order of 250 batches, so faking it would save a fraction of one mesh
	// formation.
	impliedVerification := 250 * verifySmall
	t.Logf("  implied verification for a 16-node run: %v, against %v of mesh formation",
		impliedVerification.Round(time.Millisecond), gossipsimMeshFormation)
	require.Equal(t, true, impliedVerification < gossipsimMeshFormation/4,
		"if verification ever approaches mesh formation, revisit the fidelity decision")
}
