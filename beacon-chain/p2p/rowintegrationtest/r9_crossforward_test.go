package rowintegrationtest

// R9: cross-forwarding arms -- where does the relief come from?
//
// The claim (EIP-8371): reconstructed cells pushed into non-custodied column subnets are what
// turn one node's reconstruction into network-wide relief. The pull direction is listed as MAY.
//
// The setup that makes the claim testable took some getting to, and the shape of it is the
// result:
//
//   - The row subnet must be a *subset* of the network. With every node on it, every node
//     reconstructs and fills its own columns, and cross-forwarding has nothing left to do --
//     R4(b) established that the row exchange is symmetric, so this is not a hypothetical. The
//     push matters precisely for the nodes that need a cell and are not on the row subnet that
//     recovered it, which is the mainnet case: 128 row subnets, one per node.
//   - The proposer must withhold. If every column subnet gets its cells, nothing needs relief.
//     Here the proposer publishes the first half of the columns, which is also the largest
//     withholding a row can survive: the published half is exactly the reconstruction threshold.
//   - Connectivity is a complete graph, deliberately. A push reaches only the pusher's own
//     peers, because fanout is drawn from the peers that announced a subscription and
//     announcements travel one hop. At sixteen nodes with one subscriber per column subnet, a
//     degree-10 graph would leave a third of the subscribers unreachable from any given pusher
//     and the arm would read as "the push does not work" when the truth is "the pusher was not
//     connected". Complete is safe here because no topic has more subscribers than Dhi, so
//     nothing overshoots the band and no prune backoff is paid.
//
// Predictions, written before running:
//
//	R9(a) push. Arm `rows` completes nothing beyond what the proposer published: the row-subnet
//	      members' own columns are already complete, and the withheld columns' subscribers are
//	      not on the row subnet, so no cell reaches them. Arm `rows+push` completes all of them.
//	      A near-total difference, not a marginal one -- if it is marginal, the mechanism is not
//	      doing what the EIP says it does.
//	R9(b) pull. With the row subnet one cell short of the threshold, arm `rows+push` reconstructs
//	      nothing at all, and `rows+push+pull` reconstructs and then completes the withheld
//	      columns. So pull should be decisive *in this configuration* -- which is narrower than
//	      it sounds, and the interesting part of the result is how narrow: it requires the
//	      missing cell to be in a column subnet the row members do not custody, and the row
//	      subnet to be short by an amount smaller than what the pull can fetch.

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
	// r9Nodes is split in half: the first half is the row subnet and holds the published
	// columns, the second half holds the withheld ones and is not on the row subnet at all.
	r9Nodes          = 16
	r9RowMembers     = 8
	r9ColumnsPerNode = 8
	// r9Columns is every column, so custody covers the whole space and each column subnet has
	// exactly one subscriber. One is enough for the mechanism under test -- a push reaches a
	// subnet through fanout, which needs a subscriber, not a mesh -- but it does mean nothing
	// here speaks to column-subnet mesh health.
	r9Columns = uint64(fieldparams.NumberOfColumns)
)

// r9Custody gives node i the columns 8i..8i+7, so nodes 0..7 hold columns 0..63 and nodes 8..15
// hold 64..127. The split is what makes the withholding line up with the row-subnet membership.
func r9Custody(i int) []uint64 {
	columns := make([]uint64, 0, r9ColumnsPerNode)
	for k := range uint64(r9ColumnsPerNode) {
		columns = append(columns, uint64(i)*r9ColumnsPerNode+k)
	}

	return columns
}

func r9IsRowMember(i int) bool { return i < r9RowMembers }

// r9Arm is one cross-forwarding configuration.
type r9Arm struct {
	name string
	push bool
	pull bool
}

// TestR9CrossForwardingArms is R9(a): does the push turn one node's reconstruction into relief for
// nodes that are not on its row subnet?
func TestR9CrossForwardingArms(t *testing.T) {
	arms := []r9Arm{
		{name: "rows", push: false, pull: false},
		{name: "rows+push", push: true, pull: false},
		{name: "rows+push+pull", push: true, pull: true},
	}

	type result struct {
		arm               string
		reconstructors    int
		withheldComplete  int
		publishedComplete int
		firstWithheld     time.Duration
		lastWithheld      time.Duration
		rowRecv, rowSent  int
		colCellsSeen      int
	}
	results := make([]result, 0, len(arms))

	for _, arm := range arms {
		t.Run(arm.name, func(t *testing.T) {
			r := runR9Arm(t, arm, r9RowMembers*r9ColumnsPerNode)
			results = append(results, result{
				arm: arm.name, reconstructors: r.reconstructors,
				withheldComplete: r.withheldComplete, publishedComplete: r.publishedComplete,
				firstWithheld: r.firstWithheld, lastWithheld: r.lastWithheld,
				rowRecv: r.rowRecv, rowSent: r.rowSent, colCellsSeen: r.colCellsSeen,
			})
		})
	}

	t.Log("R9(a) cross-forwarding arms")
	t.Logf("  %-16s %10s %10s %10s %9s %9s %11s %10s", "arm", "reconstr", "withheld", "published", "first", "last", "partial B", "col cells")
	for _, r := range results {
		t.Logf("  %-16s %10d %10d %10d %9v %9v %11d %10d",
			r.arm, r.reconstructors, r.withheldComplete, r.publishedComplete,
			r.firstWithheld.Round(time.Millisecond), r.lastWithheld.Round(time.Millisecond),
			r.rowRecv+r.rowSent, r.colCellsSeen)
	}

	require.Equal(t, len(arms), len(results), "every arm should have produced a result")
}

type r9Result struct {
	reconstructors    int
	withheldComplete  int
	withheldTotal     int
	publishedComplete int
	firstWithheld     time.Duration
	lastWithheld      time.Duration
	rowRecv, rowSent  int
	colCellCalls      int
	colCellsSeen      int
}

// runR9Arm brings up one arm and returns what it completed.
//
// publishedColumns is how many columns from 0 the proposer publishes; alsoPublished names further
// columns outside that prefix, which R9(b) needs to put a cell somewhere the row members can only
// reach by pulling.
func runR9Arm(t *testing.T, arm r9Arm, publishedColumns int, alsoPublished ...uint64) r9Result {
	t.Helper()

	extra := make(map[uint64]bool, len(alsoPublished))
	for _, columnIndex := range alsoPublished {
		extra[columnIndex] = true
	}
	isPublished := func(columnIndex uint64) bool {
		return columnIndex < uint64(publishedColumns) || extra[columnIndex]
	}

	// One blob, so a column is complete once it holds its single cell and the row is unambiguous.
	m := newMatrix(t, 1)
	const rowIndex = uint64(0)
	subnet, err := peerdas.RowSubnetForBlob(rowIndex, harnessSlot)
	require.NoError(t, err)

	nw, stop := newRowColumnNetwork(t, networkOpts{
		n:           r9Nodes,
		rowSubnet:   subnet,
		columnsFor:  r9Custody,
		rowMembers:  r9IsRowMember,
		columnCount: r9Columns,
		graphDegree: r9Nodes - 1,
	})
	defer stop()

	t.Logf("%s: %d nodes, %d on the row subnet, %d columns published of %d, %d columns each",
		arm.name, r9Nodes, r9RowMembers, publishedColumns, r9Columns, r9ColumnsPerNode)

	start := time.Now()

	// Every node publishes an empty column requesting every cell, for each column it custodies.
	// This is what a real node does on block arrival (emptyPartialColumnsRequestingAll in
	// beacon-chain/sync) and it is not optional detail: the partial protocol answers requests,
	// and a node that has only received a header never republishes -- both the republish and the
	// heartbeat-gossip paths are gated on having published. Omitting it made the first run of
	// this experiment report zero deliveries and blame the push.
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

	// The proposer publishes the first publishedColumns columns and withholds the rest. Node 0
	// is on the row subnet, which is what a real proposer would be; it publishes on every one of
	// these topics whether or not it custodies them, as a proposer does.
	proposer := nw.nodes[0]
	require.NoError(t, proposer.broadcaster.Publish(context.Background(), func(yield func(string, blocks.PartialDataColumn) bool) {
		for columnIndex := range r9Columns {
			if !isPublished(columnIndex) {
				continue
			}
			column := m.column(t, columnIndex, []uint64{rowIndex})
			if !yield(columnTopic(t, columnIndex), column) {
				return
			}
		}
	}))

	// Every row-subnet member offers what it actually holds: the cells of the published columns
	// it custodies. A member whose columns were all withheld offers an empty row, which is how
	// it asks for everything.
	for _, node := range nw.nodes {
		if !r9IsRowMember(node.index) {
			continue
		}
		held := make([]uint64, 0, len(node.columns))
		for _, columnIndex := range node.columns {
			if isPublished(columnIndex) {
				held = append(held, columnIndex)
			}
		}
		row := m.row(t, rowIndex, held)
		require.NoError(t, node.broadcaster.PublishRow(context.Background(), nw.topic, row))
	}

	// Recover, then cross-forward according to the arm. Reconstruction is driven from the test
	// rather than by the sync service's phase timers: this experiment is about where the cells
	// go, and the phases are R7's subject.
	nw.awaitRecoverable(t, 30*time.Second)
	groupID := m.rowGroupID(t, rowIndex)
	reconstructors := 0
	for _, node := range nw.nodes {
		if !r9IsRowMember(node.index) {
			continue
		}
		row, err := node.broadcaster.RowSnapshot(context.Background(), nw.topic, groupID)
		require.NoError(t, err)
		if row == nil {
			continue
		}
		// The pull comes before the threshold check, not after: its whole purpose is a row that
		// is short, and asking once the row can already be recovered is the case where it has
		// nothing to do.
		if arm.pull && !row.ReconstructionThresholdMet() {
			_, err := node.broadcaster.PullRowFromColumns(context.Background(), nw.topic, row, nonCustodied(node.columns))
			require.NoError(t, err)
			row = nw.awaitRowThreshold(t, node, groupID, 15*time.Second)
		}
		if row == nil || !row.ReconstructionThresholdMet() {
			continue
		}
		reconstructors++
		// The relay gate, exactly as production applies it (rows_crossforward.go): forward only
		// the columns whose cells recovery had to supply. Under R11's withholding this selects
		// precisely the lost columns -- their cells arrive row-wise from nobody -- which is the
		// gate working, and what makes R11 the gate's resilience verification.
		heldBefore := bitfield.Bitlist(append([]byte(nil), row.Included...))
		require.NoError(t, peerdas.RecoverRow(row))
		require.NoError(t, node.broadcaster.PublishRow(context.Background(), nw.topic, *row))
		if arm.push {
			columns := nonCustodied(node.columns)
			gated := columns[:0]
			for _, columnIndex := range columns {
				if columnIndex < heldBefore.Len() && heldBefore.BitAt(columnIndex) {
					continue
				}
				gated = append(gated, columnIndex)
			}
			if len(gated) == 0 {
				continue
			}
			_, err := node.broadcaster.CrossForwardRow(context.Background(), nw.topic, row, gated)
			require.NoError(t, err)
		}
	}

	nw.waitUntilQuiet(t, 30*time.Second)

	// Count completions as distinct (node, column) pairs. HandleColumn can fire more than once
	// for the same column -- several paths call verifier.Complete() -- so a raw event count
	// overstates it, and did: an early run reported 256 completions out of 64 possible columns.
	//
	// A withheld column completing is the whole of the claim: its subscriber is not on the row
	// subnet and had no other way to get the cell.
	type nodeColumn struct {
		node   int
		column uint64
	}
	seen := make(map[nodeColumn]bool)

	var res r9Result
	res.reconstructors = reconstructors
	res.firstWithheld, res.lastWithheld = -1, -1
	for i, node := range nw.nodes {
		custodied := make(map[uint64]bool, len(node.columns))
		for _, held := range node.columns {
			custodied[held] = true
		}
		nodeWithheld, nodePublished := 0, 0
		for _, event := range node.callbacks.columnsCompleteSnapshot() {
			// Only a *custodying* node completing the column counts. A node that pushed or
			// pulled on a topic holds transient state for it and will complete that state from
			// its own recovered row -- which says nothing about whether the column reached the
			// node responsible for it. An early run counted those and reported 256 completions
			// out of 64 possible columns, all of them at the four nodes that had pulled.
			if !custodied[event.columnIndex] {
				continue
			}
			key := nodeColumn{node: i, column: event.columnIndex}
			if seen[key] {
				continue
			}
			seen[key] = true
			if isPublished(event.columnIndex) {
				res.publishedComplete++
				nodePublished++
				continue
			}
			res.withheldComplete++
			nodeWithheld++
			elapsed := event.at.Sub(start)
			if res.firstWithheld < 0 || elapsed < res.firstWithheld {
				res.firstWithheld = elapsed
			}
			if elapsed > res.lastWithheld {
				res.lastWithheld = elapsed
			}
		}
		recv, sent, _, _ := rowBytes(nw.tracers[i])
		res.rowRecv += recv
		res.rowSent += sent
		cellCalls, cellsSeen := node.callbacks.columnCounts()
		res.colCellCalls += cellCalls
		res.colCellsSeen += cellsSeen
		t.Logf("    node %2d rowMember=%-5v custody %3d..%3d: column cells seen %3d, complete pub %d withheld %d",
			i, r9IsRowMember(i), node.columns[0], node.columns[len(node.columns)-1],
			cellsSeen, nodePublished, nodeWithheld)
	}

	withheldTotal := 0
	for columnIndex := range r9Columns {
		if !isPublished(columnIndex) {
			withheldTotal++
		}
	}
	res.withheldTotal = withheldTotal
	t.Logf("  reconstructors %d, withheld columns complete %d/%d, published complete %d, column cells validated %d",
		res.reconstructors, res.withheldComplete, withheldTotal, res.publishedComplete, res.colCellsSeen)

	return res
}

// nonCustodied is the columns a node does not custody, which is the policy the node's own
// beacon-chain/sync applies. Duplicated here rather than imported because that function reaches
// into the p2p service for the node id.
func nonCustodied(columns []uint64) []uint64 {
	held := make(map[uint64]bool, len(columns))
	for _, columnIndex := range columns {
		held[columnIndex] = true
	}
	out := make([]uint64, 0, r9Columns)
	for columnIndex := range r9Columns {
		if !held[columnIndex] {
			out = append(out, columnIndex)
		}
	}

	return out
}

// TestR9PullArm is R9(b): is the pull direction ever decisive, and how narrow is the case?
//
// The row subnet is put one cell short. The proposer publishes 63 of the columns the row members
// custody, so pooling their custody reaches 63 of the 64 cells the threshold needs, and publishes
// one further column that only a node *outside* the row subnet custodies. That last cell is
// reachable by exactly one route: asking a column subnet the row members are not on.
//
// Prediction, written before running: `rows+push` reconstructs nothing at all -- 63 is 63 -- and
// `rows+push+pull` reconstructs and then completes the withheld columns. So pull is decisive here.
// The interesting part of the result is not that it works but how contrived getting here was: it
// needs the shortfall to be smaller than what a pull can fetch AND the missing cell to sit in a
// subnet the row members do not custody AND a node outside the row subnet to hold it. R9(a) is the
// common case, and there the pull cost 2.4x the column-cell validations for no extra completion.
func TestR9PullArm(t *testing.T) {
	// 63 published columns from the row members' half, plus column 64 -- custodied by node 8,
	// which is not on the row subnet.
	const publishedFromRowHalf = 63
	const outsideColumn = uint64(64)

	arms := []r9Arm{
		{name: "rows+push", push: true, pull: false},
		{name: "rows+push+pull", push: true, pull: true},
	}

	type armRow struct {
		arm              string
		reconstructors   int
		withheldComplete int
		withheldTotal    int
		colCellsSeen     int
	}
	summary := make([]armRow, 0, len(arms))
	for _, arm := range arms {
		t.Run(arm.name, func(t *testing.T) {
			r := runR9Arm(t, arm, publishedFromRowHalf, outsideColumn)
			summary = append(summary, armRow{
				arm: arm.name, reconstructors: r.reconstructors,
				withheldComplete: r.withheldComplete, withheldTotal: r.withheldTotal,
				colCellsSeen: r.colCellsSeen,
			})
		})
	}

	t.Log("R9(b) the pull arm, with the row subnet one cell short")
	t.Logf("  %-16s %10s %12s %10s", "arm", "reconstr", "withheld", "col cells")
	for _, r := range summary {
		t.Logf("  %-16s %10d %6d/%-5d %10d", r.arm, r.reconstructors, r.withheldComplete, r.withheldTotal, r.colCellsSeen)
	}
}
