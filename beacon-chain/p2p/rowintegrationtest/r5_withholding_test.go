package rowintegrationtest

// R5 — withholding: is futile recovery work actually reduced?
//
// The threat EIP-8371 inherits from PeerDAS: a proposer releases 64 columns' worth of cells for
// every row except one, and keeps that row below the reconstruction threshold by withholding as
// little as a single cell. Under PeerDAS every node with enough custody attempts the recovery,
// discovers it cannot, and the attempt is futile. The claim is that under RowDAS the work is one
// reconstructor's.
//
// The honest metric here is attempts and their scheduling, not CPU: a node at 63 of 64 cells
// cannot start a recovery, so the futile "work" is bookkeeping. What is *not* bookkeeping is the
// traffic — the row axis pools cells for a row that can never be recovered, and that is a cost
// RowDAS adds under this attack rather than removes. So this measures both.

import (
	"context"
	"testing"
	"time"

	"github.com/OffchainLabs/prysm/v7/beacon-chain/core/peerdas"
	"github.com/OffchainLabs/prysm/v7/consensus-types/blocks"
	"github.com/OffchainLabs/prysm/v7/testing/require"
)

const (
	r5Nodes           = 16
	r5ColumnsPerNode  = 4
	r5HealthyRowIndex = uint64(0)
)

// r5Result is one arm: what the network reached, what it scheduled, and what it spent.
type r5Result struct {
	name string
	// cover is how many distinct cells of the row exist anywhere in the network.
	cover int
	// reachedThreshold counts nodes that could recover the row.
	reachedThreshold int
	// heldMin/heldMax bracket what each node ended up holding, which is what a bitmap observer
	// would see.
	heldMin, heldMax int
	rowBytesRecv     int
	rowBytesSent     int
}

// TestR5WithholdingBelowThreshold is R5. Two arms over the same setup, differing by one cell:
// a row covered exactly to the threshold, and the same row one cell short.
func TestR5WithholdingBelowThreshold(t *testing.T) {
	require.Equal(t, uint64(r5Nodes*r5ColumnsPerNode), blocks.ReconstructionThreshold(),
		"the cover should be exactly the threshold, so withholding one cell is decisive")

	healthy := runR5Arm(t, "covered", false)
	withheld := runR5Arm(t, "one cell short", true)

	t.Log("R5 withholding a single cell of one row")
	t.Logf("  %-16s %6s %10s %8s %8s %12s %12s", "arm", "cover", "threshold", "min held", "max held", "row rx", "row tx")
	for _, r := range []r5Result{healthy, withheld} {
		t.Logf("  %-16s %6d %10d %8d %8d %12d %12d",
			r.name, r.cover, r.reachedThreshold, r.heldMin, r.heldMax, r.rowBytesRecv, r.rowBytesSent)
	}

	// The claim, in the form the row axis can actually deliver: the recovery trigger is cells in
	// hand, not custody, so a row one cell short schedules **no** recovery anywhere. Under PeerDAS
	// the trigger is custody -- every node holding half the columns attempts every incomplete blob
	// -- so the futile attempt happens once per such node per slot.
	require.Equal(t, r5Nodes, healthy.reachedThreshold,
		"the covered row should be recoverable everywhere, or the arms are not comparable")
	require.Equal(t, 0, withheld.reachedThreshold,
		"a row one cell short must not schedule a recovery anywhere")

	// And the part that is a cost rather than a saving. Every node pools the 63 cells that do
	// exist, because below the threshold a node asks for everything it lacks -- it cannot know
	// which cells its peers hold, and one of them is the one that would let it recover. So the
	// row axis spends bytes on a row that can never be recovered.
	require.Equal(t, true, withheld.rowBytesRecv > 0,
		"the row axis does pool a sub-threshold row, which is the cost this arm exists to price")
	t.Logf("  futile row traffic: %d bytes received across %d nodes, %.1f KiB each",
		withheld.rowBytesRecv, r5Nodes, float64(withheld.rowBytesRecv)/float64(r5Nodes)/1024)

	// The information needed to stop is present: every node ends up seeing the same 63, and the
	// availability bitmap carries exactly that. Nothing uses it, which is the implementation gap
	// R5's falsification condition names.
	require.Equal(t, withheld.cover, withheld.heldMin,
		"every node should converge on the full cover, so a bitmap observer sees the shortfall")
	require.Equal(t, withheld.cover, withheld.heldMax)
	require.Equal(t, int(blocks.ReconstructionThreshold())-1, withheld.cover,
		"the withheld arm should sit exactly one cell short")
}

// runR5Arm brings up a subnet whose pooled custody covers the row exactly, optionally withholding
// one cell of it, and reports what the network reached and spent.
func runR5Arm(t *testing.T, name string, withhold bool) r5Result {
	t.Helper()

	m := newMatrix(t, 1)
	subnet, err := peerdas.RowSubnetForBlob(r5HealthyRowIndex, harnessSlot)
	require.NoError(t, err)

	columnsFor := func(i int) []uint64 {
		columns := make([]uint64, 0, r5ColumnsPerNode)
		for k := range uint64(r5ColumnsPerNode) {
			columns = append(columns, uint64(i)*r5ColumnsPerNode+k)
		}

		return columns
	}

	nw, stop := newRowNetwork(t, r5Nodes, subnet, columnsFor)
	defer stop()

	// Each node publishes its own custody's cells of the row, which is what cross-fill from its
	// column subscriptions would have produced. The withheld arm drops exactly one cell: node 0's
	// first column. That is the whole attack -- one cell, from one column, of one row.
	cover := 0
	for _, node := range nw.nodes {
		columns := node.columns
		if withhold && node.index == 0 {
			columns = columns[1:]
		}
		cover += len(columns)
		row := m.row(t, r5HealthyRowIndex, columns)
		require.NoError(t, node.broadcaster.PublishRow(context.Background(), nw.topic, row))
	}

	// Let the exchange run to a stop rather than to a deadline: what is being measured is where
	// it settles, and a sub-threshold row settles below the threshold.
	nw.waitUntilQuiet(t, 30*time.Second)

	groupID := m.rowGroupID(t, r5HealthyRowIndex)
	result := r5Result{name: name, cover: cover, heldMin: -1}
	for _, node := range nw.nodes {
		row, err := node.broadcaster.RowSnapshot(context.Background(), nw.topic, groupID)
		require.NoError(t, err)
		require.NotNil(t, row)

		held := int(row.Included.Count())
		if result.heldMin < 0 || held < result.heldMin {
			result.heldMin = held
		}
		if held > result.heldMax {
			result.heldMax = held
		}
		if row.ReconstructionThresholdMet() {
			result.reachedThreshold++
		}

		recv, sent, _, _ := rowBytes(nw.tracers[node.index])
		result.rowBytesRecv += recv
		result.rowBytesSent += sent
	}

	return result
}
