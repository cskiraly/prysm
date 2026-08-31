package rowintegrationtest

// R12 — can the row axis substitute for a node's own dead column path?
//
// R9 and R11 model loss at the source: a column's cells are never published, so nobody has them and
// the row subnet has to reconstruct. That leaves a boundary on the resilience claim, because the
// more common failure is the other one — the data is published and served, and *this node's* route
// to it is broken: an eclipsed column topic, a mesh that never grafted, a subnet it cannot reach.
//
// Here the proposer publishes every column, so the data is fully available network-wide. A subset of
// nodes custody columns and create state for them, exactly as a node does on block arrival, but
// never subscribe to those topics. Nothing can reach them by the column path. The question is what
// the row axis delivers instead.
//
// The answer is a ratio rather than a yes, and that is the point of sweeping the blob count: a
// node's row subscription carries the cells of the rows it is on, so it can cover its own columns
// only for those rows. One row subnet and one blob is full cover; one row subnet and four blobs is a
// quarter. That is the same `rows_subscribed / blob_count` that bounds R2's latency gain, showing up
// here as the bound on substitution.

import (
	"context"
	"testing"
	"time"

	"github.com/OffchainLabs/go-bitfield"
	"github.com/OffchainLabs/prysm/v7/beacon-chain/core/peerdas"
	fieldparams "github.com/OffchainLabs/prysm/v7/config/fieldparams"
	"github.com/OffchainLabs/prysm/v7/consensus-types/blocks"
	"github.com/OffchainLabs/prysm/v7/testing/require"
)

const (
	r12Nodes          = 16
	r12ColumnsPerNode = 8
	// The first half of the network keeps a working column path; the second half's is dead.
	r12FirstVictim = 8
)

func r12Custody(i int) []uint64 {
	columns := make([]uint64, 0, r12ColumnsPerNode)
	for k := range uint64(r12ColumnsPerNode) {
		columns = append(columns, uint64(i)*r12ColumnsPerNode+k)
	}

	return columns
}

func r12IsVictim(i int) bool { return i >= r12FirstVictim }

// r12Result is one blob count: what the victims completed against what the healthy nodes did.
type r12Result struct {
	blobs            int
	rowsSubscribed   int
	victimComplete   int
	victimExpected   int
	healthyComplete  int
	healthyExpected  int
	victimRowsPooled int
	// recovered is how many healthy nodes reconstructed the row, which is the only route by which
	// a victim's columns can be reached at all.
	recovered int
}

// TestR12RowAxisSubstitutesForADeadColumnPath is R12.
func TestR12RowAxisSubstitutesForADeadColumnPath(t *testing.T) {
	results := []r12Result{
		runR12Arm(t, 1),
		runR12Arm(t, 4),
	}

	t.Log("R12 the row axis as a substitute for a dead column path")
	t.Logf("  %6s %16s %11s %22s %22s", "blobs", "rows subscribed", "recovered", "victim columns done", "healthy columns done")
	for _, r := range results {
		t.Logf("  %6d %16d %11d %14d /%-7d %14d /%-7d",
			r.blobs, r.rowsSubscribed, r.recovered,
			r.victimComplete, r.victimExpected, r.healthyComplete, r.healthyExpected)
	}
	t.Logf("  victim row cells pooled: %d at 1 blob, %d at 4 blobs (128 is the whole row)",
		results[0].victimRowsPooled/(r12Nodes-r12FirstVictim), results[1].victimRowsPooled/(r12Nodes-r12FirstVictim))

	one, four := results[0], results[1]

	// The healthy half is the control: its column path works, so it completes regardless. Without
	// this the victim numbers cannot be read at all.
	require.Equal(t, one.healthyExpected, one.healthyComplete,
		"the healthy half should complete its columns at 1 blob")
	require.Equal(t, four.healthyExpected, four.healthyComplete,
		"the healthy half should complete its columns at 4 blobs")

	// One blob, one row subnet: the row a node is on carries every cell of every column, so the
	// row axis is a *complete* substitute for the dead column path.
	require.Equal(t, one.victimExpected, one.victimComplete,
		"at 1 blob the row axis should cover a dead column path entirely")

	// Four blobs, one row subnet: the row axis carries one of the four cells each column needs, so
	// no column completes. The substitution is bounded by rows_subscribed / blob_count, and this is
	// what that bound looks like when it bites.
	require.Equal(t, 0, four.victimComplete,
		"at 4 blobs one row subnet cannot complete a column on its own")

	// And at 4 blobs the victims are not cut off — they receive the *whole* recovered row, all 128
	// cells, which is one of the four cells each of their columns needs. So the substitution is 25%
	// at the cell level and 0% at the column level, and the reason is the ratio rather than any
	// failure of the mechanism.
	victims := r12Nodes - r12FirstVictim
	require.Equal(t, fieldparams.NumberOfColumns, four.victimRowsPooled/victims,
		"each victim should hold the whole recovered row, one cell for each of its columns")
}

// runR12Arm publishes every column, kills the victims' column path, and reports what completed.
func runR12Arm(t *testing.T, blobs int) r12Result {
	t.Helper()

	m := newMatrix(t, blobs)
	const rowIndex = uint64(0)
	subnet, err := peerdas.RowSubnetForBlob(rowIndex, harnessSlot)
	require.NoError(t, err)

	nw, stop := newRowColumnNetwork(t, networkOpts{
		n:              r12Nodes,
		rowSubnet:      subnet,
		columnsFor:     r12Custody,
		columnCount:    uint64(fieldparams.NumberOfColumns),
		columnPathDead: func(node int, _ uint64) bool { return r12IsVictim(node) },
		graphDegree:    r12Nodes - 1,
	})
	defer stop()

	t.Logf("%d blobs: %d nodes, %d columns each, column path dead on nodes %d..%d",
		blobs, r12Nodes, r12ColumnsPerNode, r12FirstVictim, r12Nodes-1)

	// Every node creates state for its custodied columns and asks for everything, which is what a
	// real node does on block arrival. The victims do this too -- their state exists, only their
	// route to the cells is gone.
	for _, node := range nw.nodes {
		require.NoError(t, node.broadcaster.Publish(context.Background(), func(yield func(string, blocks.PartialDataColumn) bool) {
			for _, columnIndex := range node.columns {
				column := m.column(t, columnIndex, nil)
				requests := bitfield.NewBitlist(uint64(m.blobCount)).Not()
				require.NoError(t, column.SetPartsRequests(requests))
				if !yield(columnTopic(t, columnIndex), column) {
					return
				}
			}
		}))
	}

	// The proposer publishes every column in full, so the data is available everywhere the column
	// path works. Nothing is withheld: this experiment is about the receiver's path, not the source.
	allRows := make([]uint64, 0, blobs)
	for blobIndex := range uint64(blobs) {
		allRows = append(allRows, blobIndex)
	}
	proposer := nw.nodes[0]
	require.NoError(t, proposer.broadcaster.Publish(context.Background(), func(yield func(string, blocks.PartialDataColumn) bool) {
		for columnIndex := range uint64(fieldparams.NumberOfColumns) {
			if !yield(columnTopic(t, columnIndex), m.column(t, columnIndex, allRows)) {
				return
			}
		}
	}))

	// The row axis, started the same way R2 starts it: an empty row from every node, so the
	// exchange is driven by what cross-fill from the column path supplies.
	for _, node := range nw.nodes {
		empty, err := blocks.NewPartialDataRow(m.root, m.header, rowIndex, m.commitments, m.inclusionProof)
		require.NoError(t, err)
		require.NoError(t, node.broadcaster.PublishRow(context.Background(), nw.topic, empty))
	}

	// Wait on the quantity that matters, not on quiet. `waitUntilQuiet` returned before the row
	// exchange had started for most nodes here -- at one blob there is little traffic, so two equal
	// polls arrive early and read as "finished" -- and the arm reported that the healthy half could
	// not reach the threshold. `awaitRecoverable` requires observed progress before it accepts a
	// stall, which is the difference.
	reached := nw.awaitRecoverable(t, 45*time.Second)
	t.Logf("    %d of %d nodes reached the reconstruction threshold before recovery", reached, r12Nodes)

	// Now the step this experiment turns on, and which a first version of it left out: the row has
	// to be *recovered* before it can carry anything the victims need.
	//
	// The healthy half custodies columns 0-63, so pooling gives its row state exactly those 64
	// cells -- the threshold, nothing spare. The cells the victims need are for columns 64-127, and
	// nobody holds those in row state at all: their only custodians are the victims, whose column
	// path is dead. So the row axis can substitute here only through reconstruction, and without
	// this step the arm measured zero and would have read as "the row axis cannot do it".
	//
	// Driven from the test, as in R9: this is about where cells can go, not when the phase timers
	// fire.
	groupID := m.rowGroupID(t, rowIndex)

	// Diagnostics before the recovery step, because the first version of this arm measured zero and
	// the question was whether the row axis had anything to work with at all.
	for _, node := range nw.nodes {
		row, err := node.broadcaster.RowSnapshot(context.Background(), nw.topic, groupID)
		require.NoError(t, err)
		held := 0
		if row != nil {
			held = int(row.Included.Count())
		}
		t.Logf("    node %2d victim=%-5v row cells %3d, columns complete %d of %d",
			node.index, r12IsVictim(node.index), held,
			len(node.callbacks.columnsCompleteSnapshot()), r12ColumnsPerNode)
	}

	recovered := 0
	for _, node := range nw.nodes {
		if r12IsVictim(node.index) {
			continue
		}
		row, err := node.broadcaster.RowSnapshot(context.Background(), nw.topic, groupID)
		require.NoError(t, err)
		if row == nil || !row.ReconstructionThresholdMet() {
			continue
		}
		recovered++
		require.NoError(t, peerdas.RecoverRow(row))
		require.NoError(t, node.broadcaster.PublishRow(context.Background(), nw.topic, *row))
	}
	require.Equal(t, true, recovered > 0,
		"the healthy half should be able to recover the row, or the arm cannot test substitution")

	// And the same discipline for the rescue: poll the victims' completions and accept a stall only
	// after something has actually moved.
	nw.awaitVictimColumns(t, r12IsVictim, 45*time.Second)

	result := r12Result{blobs: blobs, rowsSubscribed: 1, recovered: recovered}
	for _, node := range nw.nodes {
		done := len(node.callbacks.columnsCompleteSnapshot())
		if r12IsVictim(node.index) {
			result.victimComplete += done
			result.victimExpected += r12ColumnsPerNode
			row, err := node.broadcaster.RowSnapshot(context.Background(), nw.topic, groupID)
			require.NoError(t, err)
			if row != nil {
				result.victimRowsPooled += int(row.Included.Count())
			}
			continue
		}
		result.healthyComplete += done
		result.healthyExpected += r12ColumnsPerNode
	}

	return result
}
