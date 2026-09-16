package rowintegrationtest

// R4(b): does pooled custody actually reconstruct, over a real network?
//
// R4(a) established that the custody derivation gives enough distinct columns once a row subnet
// has ~27 custody-minimum members. Coverage is necessary but not sufficient: the cells have to
// actually reach one node, which is a question about the pull dynamics rather than about the
// custody maths.
//
// Prediction, written before running: reconstruction succeeds but *late*, gated by the pull
// round trips, and every node that pulls should reach the threshold rather than just one --
// the exchange is symmetric, so there is no reason for one node to win.

import (
	"context"
	"testing"
	"time"

	"github.com/OffchainLabs/prysm/v7/beacon-chain/core/peerdas"
	fieldparams "github.com/OffchainLabs/prysm/v7/config/fieldparams"
	"github.com/OffchainLabs/prysm/v7/consensus-types/blocks"
	"github.com/OffchainLabs/prysm/v7/testing/require"
)

// TestR4bPooledCustodyReconstructs is the end-to-end row exchange: sixteen nodes, each holding
// a disjoint quarter-slice of the columns, pool their cells on one row subnet until they can
// recover the row.
//
// Columns are assigned rather than derived so the cover is exact: node i holds columns
// 4i..4i+3, so the sixteen of them hold columns 0..63 -- exactly the reconstruction threshold,
// with nothing to spare. That makes the test sensitive: lose one cell anywhere and no node can
// recover.
func TestR4bPooledCustodyReconstructs(t *testing.T) {
	const nodes = 16
	const columnsPerNode = 4
	threshold := blocks.ReconstructionThreshold()
	require.Equal(t, uint64(nodes*columnsPerNode), threshold, "the cover should be exactly the threshold")

	// One blob, so the matrix is cheap and the row is unambiguous.
	m := newMatrix(t, 1)
	const rowIndex = uint64(0)
	subnet, err := peerdas.RowSubnetForBlob(rowIndex, harnessSlot)
	require.NoError(t, err)

	nw, stop := newRowNetwork(t, nodes, subnet, func(i int) []uint64 {
		columns := make([]uint64, 0, columnsPerNode)
		for k := range uint64(columnsPerNode) {
			columns = append(columns, uint64(i)*columnsPerNode+k)
		}
		return columns
	})
	defer stop()

	t.Logf("R4(b) %d nodes, %d columns each, threshold %d, row subnet %d",
		nodes, columnsPerNode, threshold, subnet)

	// Every node offers what its custody gives it. This is the row-topic equivalent of a node
	// publishing the cells it holds: it announces its bitmap, and peers pull what they lack.
	start := time.Now()
	for _, node := range nw.nodes {
		row := m.row(t, rowIndex, node.columns)
		require.NoError(t, node.broadcaster.PublishRow(context.Background(), nw.topic, row))
	}

	// Wait for the pull exchange to converge, then read who reached the threshold.
	deadline := time.Now().Add(30 * time.Second)
	var reached int
	for time.Now().Before(deadline) {
		reached = 0
		for _, node := range nw.nodes {
			if len(node.callbacks.recoverableSnapshot()) > 0 {
				reached++
			}
		}
		if reached == nodes {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	elapsed := time.Since(start)

	// Per-node accounting.
	var totalCells, totalRecv, totalSent int
	for i, node := range nw.nodes {
		_, cellCalls, cellsSeen, complete := node.callbacks.counts()
		recv, sent, recvRPCs, sentRPCs := rowBytes(nw.tracers[i])
		totalCells += cellsSeen
		totalRecv += recv
		totalSent += sent
		if i < 4 {
			t.Logf("  node %2d: verified %3d cells in %2d batches, partial rx %7dB/%3d rpcs tx %7dB/%3d rpcs, complete %d",
				i, cellsSeen, cellCalls, recv, recvRPCs, sent, sentRPCs, complete)
		}
	}
	t.Logf("  %d/%d nodes reached the threshold in %v", reached, nodes, elapsed.Round(time.Millisecond))
	t.Logf("  network total: %d cells verified, %.1f KiB partial rx, %.1f KiB partial tx",
		totalCells, float64(totalRecv)/1024, float64(totalSent)/1024)

	require.Equal(t, nodes, reached, "every node should reach the threshold: the exchange is symmetric")

	// The claim under test is not just "reached the threshold" but "can therefore recover the
	// row". Take one node's state and recover from it for real.
	snapshot, err := nw.nodes[0].broadcaster.RowSnapshot(context.Background(), nw.topic, m.rowGroupID(t, rowIndex))
	require.NoError(t, err)
	require.NotNil(t, snapshot)
	require.Equal(t, true, snapshot.ReconstructionThresholdMet())
	require.Equal(t, threshold, snapshot.Included.Count(), "exactly the cover, nothing spare")

	require.NoError(t, peerdas.RecoverRow(snapshot))
	require.Equal(t, true, snapshot.IsComplete())

	// Every recovered cell must equal what the original encoding produced, including the ones
	// no node on this subnet ever held.
	for column := range uint64(fieldparams.NumberOfColumns) {
		cell := m.cells[rowIndex][column]
		require.DeepEqual(t, cell[:], snapshot.Cells[column], "recovered cell differs")
	}
	t.Logf("  recovered all %d cells of row %d from the pooled %d", fieldparams.NumberOfColumns, rowIndex, threshold)
}

// TestR4bBelowCoverCannotRecover is the negative control. One node short of the cover, the
// exchange must converge to 60 cells and no node may claim it can recover -- otherwise the
// positive result above says nothing.
func TestR4bBelowCoverCannotRecover(t *testing.T) {
	const nodes = 15
	const columnsPerNode = 4
	threshold := blocks.ReconstructionThreshold()

	m := newMatrix(t, 1)
	const rowIndex = uint64(0)
	subnet, err := peerdas.RowSubnetForBlob(rowIndex, harnessSlot)
	require.NoError(t, err)

	nw, stop := newRowNetwork(t, nodes, subnet, func(i int) []uint64 {
		columns := make([]uint64, 0, columnsPerNode)
		for k := range uint64(columnsPerNode) {
			columns = append(columns, uint64(i)*columnsPerNode+k)
		}
		return columns
	})
	defer stop()

	for _, node := range nw.nodes {
		row := m.row(t, rowIndex, node.columns)
		require.NoError(t, node.broadcaster.PublishRow(context.Background(), nw.topic, row))
	}

	// Wait for the exchange to stop rather than for a guessed interval: a short wait here
	// would produce a false pass, since "nobody recoverable yet" is not "nobody can be".
	settled := nw.waitUntilQuiet(t, 30*time.Second)

	for _, node := range nw.nodes {
		require.Equal(t, 0, len(node.callbacks.recoverableSnapshot()),
			"no node can be recoverable below the cover")
	}

	snapshot, err := nw.nodes[0].broadcaster.RowSnapshot(context.Background(), nw.topic, m.rowGroupID(t, rowIndex))
	require.NoError(t, err)
	require.NotNil(t, snapshot)
	require.Equal(t, uint64(nodes*columnsPerNode), snapshot.Included.Count(),
		"the exchange should still pool every cell that exists")
	require.Equal(t, false, snapshot.ReconstructionThresholdMet())
	require.ErrorIs(t, peerdas.RecoverRow(snapshot), peerdas.ErrRowBelowReconstructionThreshold)

	t.Logf("R4(b) negative control: %d nodes pooled %d of %d cells in %v, nobody recoverable",
		nodes, snapshot.Included.Count(), threshold, settled.Round(time.Millisecond))
}
