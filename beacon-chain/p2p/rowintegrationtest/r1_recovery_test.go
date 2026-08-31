package rowintegrationtest

// R1(b): does the phase-1 jitter actually turn k reconstructors into one recovery?
//
// R1(a) settled the arithmetic: where a row subnet holds a reconstructor, its duty is
// `blobCount / ROW_SUBNET_COUNT` rather than `blobCount`. But that is a statement about *duty*, and
// duty is not work. Every reconstructor on a subnet has phase-1 duty for every row that subnet
// carries -- the EIP says a row reconstructor MUST recover the rows mapped to its own subnet, not
// "one of you must". So k reconstructors on a subnet are k candidate recoveries of the same row,
// and what reduces them to one is not the mapping but two mechanisms:
//
//	jitter        each node waits a random fraction of the phase-1 window before starting;
//	cancellation  a node whose row completed in the meantime does not start at all.
//
// Which makes the whole saving hinge on a race: does a recovered row propagate to the other k-1
// members before their timers fire? That is a network question, and it is the one R1(a) could not
// touch. If propagation is slower than the jitter window, RowDAS has relocated the duplication
// rather than removed it -- exactly the falsification condition R1 states.
//
// Prediction, written before running: the window has to exceed the row-completion propagation time,
// which R3 measured at 250-400 ms for this topology. So a zero window should give all 16 nodes
// recovering, and the count should fall towards 1 as the window passes ~400 ms. The EIP's phase-1
// bound of a few percent of a slot (600 ms at 12 s) should land just above the knee -- comfortable
// but not generous, and the margin should shrink as the network grows.

import (
	"context"
	"fmt"
	"math/rand"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/OffchainLabs/prysm/v7/beacon-chain/core/peerdas"
	"github.com/OffchainLabs/prysm/v7/consensus-types/blocks"
	"github.com/OffchainLabs/prysm/v7/testing/gossipsim"
	"github.com/OffchainLabs/prysm/v7/testing/require"
)

// r1bPhase1WindowBPS mirrors rowPhase1MaxJitterBPS in beacon-chain/sync. Duplicated rather than
// imported because that constant is package-private to sync; the test asserts they agree in
// spirit by putting the shipped value in the sweep.
const r1bPhase1WindowBPS = 500

// TestR1bPhase1JitterPricesDuplicateRecoveries sweeps the phase-1 jitter window and counts how many
// of the subnet's reconstructors actually perform the recovery.
func TestR1bPhase1JitterPricesDuplicateRecoveries(t *testing.T) {
	// Both R1(b) assertions describe the default request policy. ROWDAS_SCOPED_REQUESTS=0 exists
	// to measure that policy's cost, not to run the suite in, and under it these findings invert --
	// so say so rather than failing in a way that reads as a regression.
	if !scopedRowRequests() {
		t.Skip("R1(b) asserts the default row request policy; unset ROWDAS_SCOPED_REQUESTS to run it")
	}

	// The shipped window at a 12-second slot, alongside the values that bracket the knee.
	shipped := time.Duration(12000*r1bPhase1WindowBPS/10000) * time.Millisecond

	windows := []time.Duration{
		0,
		100 * time.Millisecond,
		300 * time.Millisecond,
		shipped,
		2 * shipped,
	}

	results := make([]cell1b, 0, len(windows))

	for _, window := range windows {
		results = append(results, runR1bWindow(t, window))
	}

	t.Logf("R1(b) phase-1 jitter versus duplicate recoveries, %d reconstructors on one row subnet", results[0].nodes)
	t.Logf("  PeerDAS baseline: all %d would recover the row", results[0].nodes)
	t.Logf("  %-10s %12s %12s %12s %12s", "window", "recoveries", "wasted", "first", "last")
	for _, r := range results {
		t.Logf("  %-10v %12d %12d %12v %12v",
			r.window, r.recoveries, r.recoveries-1,
			r.firstAt.Round(time.Millisecond), r.lastAt.Round(time.Millisecond))
	}

	// With no jitter every node starts at once and nothing can cancel anything, which is the
	// measurement's own control: if this does not duplicate, the sweep is not sensitive.
	require.Equal(t, results[0].nodes, results[0].recoveries,
		"a zero jitter window must duplicate across every reconstructor")

	// The original finding was that the shipped window collapses essentially nothing, because
	// cancellation waited for the row to be *complete* locally and that took longer than the whole
	// window -- 14, 15 or 16 of 16 still duplicating. That trigger is now known to be unreachable
	// for a pooling node rather than merely slow, and this sweep uses the availability observation
	// instead. See TestR1bRecoveryPropagationPricesTheCancellation for both timings.

	// Why the trigger had to change. A pooling node stops asking for a row's cells once it holds
	// enough to recover it, instead of downloading all 128 -- which is the point of RowDAS and
	// worth 32% of the row axis's bytes (TODO.md D11). But local completion was the only
	// cancellation signal the shipped code had, and a node that stops fetching at the threshold
	// never reaches it. Measured: 0 of 15 peers complete, against 15 of 15 before the scoping.
	//
	// So the signal is the peer's availability bitmap instead, which lands at ~188 ms against a
	// 600 ms window. TODO.md D5.
	// **What the signal is worth.** With the availability observation as the trigger, the shipped
	// window collapses most of the duplication rather than none of it -- the observation lands at
	// ~188 ms against a 600 ms window. It does not collapse all of it, and the reason is a
	// property of the scoping rather than of the trigger: only the recovering node holds the whole
	// row, so the claim propagates exactly one mesh hop. Nodes further out never see it.
	require.Equal(t, true, 2*results[3].recoveries < results[3].nodes,
		"with the availability observation as the trigger, most reconstructors should stand down")
}

// runR1bWindow brings up a fresh network, gets the row to the reconstruction threshold everywhere,
// then races every node's phase-1 timer against the propagation of the first recovery.
func runR1bWindow(t *testing.T, window time.Duration) cell1b {
	t.Helper()

	const nodes = 16
	const columnsPerNode = 4
	require.Equal(t, uint64(nodes*columnsPerNode), blocks.ReconstructionThreshold(),
		"the cover should be exactly the threshold, as in R4(b)")

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

	// Pool custody on the row topic until every node can recover, which is the state phase 1
	// starts from.
	for _, node := range nw.nodes {
		row := m.row(t, rowIndex, node.columns)
		require.NoError(t, node.broadcaster.PublishRow(context.Background(), nw.topic, row))
	}
	reached := nw.awaitRecoverable(t, 30*time.Second)
	require.Equal(t, nodes, reached, "every node should reach the threshold before the race starts")
	nw.waitUntilQuiet(t, 20*time.Second)

	// The race. Each node draws its own delay from [0, window), and on firing does what
	// reconstructRow does: re-read the row, skip if it is already complete, otherwise recover and
	// publish. Seeded, so a reported figure is reproducible.
	groupID := m.rowGroupID(t, rowIndex)
	rng := rand.New(rand.NewSource(int64(0x8371_3000 + window.Milliseconds())))
	delays := make([]time.Duration, nodes)
	for i := range delays {
		if window > 0 {
			delays[i] = time.Duration(rng.Int63n(int64(window)))
		}
	}

	var recoveries atomic.Int64
	var firstAt, lastAt atomic.Int64
	firstAt.Store(int64(time.Hour))
	start := time.Now()
	var wg sync.WaitGroup
	for i, node := range nw.nodes {
		wg.Add(1)
		go func(node *rowNode, delay time.Duration) {
			defer wg.Done()
			time.Sleep(delay)

			row, err := node.broadcaster.RowSnapshot(context.Background(), nw.topic, groupID)
			if err != nil || row == nil {
				return
			}
			// The cancellation, which is the whole mechanism. Two triggers, and the sweep exists
			// to show that they are not equivalent:
			//
			//   - the row completed locally while we waited. This is what the node ships, and
			//     since the request scoping a pooling node never reaches it -- it stops fetching
			//     at the threshold, so the other 64 cells never arrive.
			//   - a peer holds the whole row and we want nothing further from it. Measured at
			//     ~188 ms against a 600 ms window, where the first arrives never.
			//
			// Both are checked here so the sweep prices the second rather than assuming it.
			if row.IsComplete() {
				return
			}
			if !node.callbacks.servedElsewhereObservedAt().IsZero() {
				return
			}
			if !row.ReconstructionThresholdMet() {
				return
			}
			if err := peerdas.RecoverRow(row); err != nil {
				return
			}
			recoveries.Add(1)
			elapsed := int64(time.Since(start))
			for {
				current := firstAt.Load()
				if elapsed >= current || firstAt.CompareAndSwap(current, elapsed) {
					break
				}
			}
			for {
				current := lastAt.Load()
				if elapsed <= current || lastAt.CompareAndSwap(current, elapsed) {
					break
				}
			}
			// Serve it back, which is what gives the others something to cancel on.
			if err := node.broadcaster.PublishRow(context.Background(), nw.topic, *row); err != nil {
				t.Logf("node %d could not publish its recovered row: %v", node.index, err)
			}
		}(node, delays[i])
	}
	wg.Wait()
	nw.waitUntilQuiet(t, 20*time.Second)

	first := time.Duration(firstAt.Load())
	if recoveries.Load() == 0 {
		first = 0
	}

	return cell1b{
		window:     window,
		recoveries: int(recoveries.Load()),
		nodes:      nodes,
		firstAt:    first,
		lastAt:     time.Duration(lastAt.Load()),
	}
}

type cell1b struct {
	window     time.Duration
	recoveries int
	nodes      int
	firstAt    time.Duration
	lastAt     time.Duration
}

// TestR1bRecoveryPropagationPricesTheCancellation prices the thing the phase-1 window is racing
// against, which the sweep above shows it loses to.
//
// One designated node recovers the row; nobody else has a timer. The question is when the other
// fifteen learn about it, and by which route. Two routes exist and they are an order of magnitude
// apart:
//
//	the bitmap   the recovering node's parts metadata says "I have all 128", 24 bytes, one round
//	             trip. This is what a peer could cancel on.
//	the cells    the other nodes hold exactly the 64 cells the pooled cover gave them, so becoming
//	             *complete* means pulling 64 more -- 128 KiB each, and R3 showed the row exchange
//	             duplicates that by roughly one copy per mesh peer. This is what the shipped code
//	             cancels on.
//
// Prediction, written before running: cell propagation lands in the hundreds of milliseconds and
// bitmap propagation near one round trip (50 ms here), so the shipped trigger has to wait for
// something roughly an order of magnitude slower than the signal that was already available. If
// that holds, the fix for R1(b)'s falsification is to cancel on a peer announcing full
// availability rather than on local completion.
func TestR1bRecoveryPropagationPricesTheCancellation(t *testing.T) {
	// Both R1(b) assertions describe the default request policy. ROWDAS_SCOPED_REQUESTS=0 exists
	// to measure that policy's cost, not to run the suite in, and under it these findings invert --
	// so say so rather than failing in a way that reads as a regression.
	if !scopedRowRequests() {
		t.Skip("R1(b) asserts the default row request policy; unset ROWDAS_SCOPED_REQUESTS to run it")
	}

	const nodes = 16
	const columnsPerNode = 4

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
	require.Equal(t, nodes, nw.awaitRecoverable(t, 30*time.Second))
	nw.waitUntilQuiet(t, 20*time.Second)

	// Exactly one recovery, so every completion elsewhere is attributable to it.
	groupID := m.rowGroupID(t, rowIndex)
	recoverer := nw.nodes[0]
	row, err := recoverer.broadcaster.RowSnapshot(context.Background(), nw.topic, groupID)
	require.NoError(t, err)
	require.NotNil(t, row)
	require.Equal(t, false, row.IsComplete(), "the pooled cover should leave the row incomplete")

	// Cell-validation counts before the recovery, so the delta attributes anything that arrives
	// afterwards to the recovery rather than to the pooling exchange.
	before := make([]int, nodes)
	for i, node := range nw.nodes {
		_, _, before[i], _ = node.callbacks.counts()
	}

	recoveredAt := time.Now()
	require.NoError(t, peerdas.RecoverRow(row))
	require.NoError(t, recoverer.broadcaster.PublishRow(context.Background(), nw.topic, *row))

	// Poll rather than wait for quiet: "quiet" cannot distinguish a transfer that finished from
	// one that never started, and that distinction is the whole question here.
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		grown := 0
		for _, node := range nw.nodes {
			if node.index == recoverer.index {
				continue
			}
			snapshot, err := node.broadcaster.RowSnapshot(context.Background(), nw.topic, groupID)
			require.NoError(t, err)
			if snapshot != nil && snapshot.Included.Count() > 64 {
				grown++
			}
		}
		if grown == nodes-1 {
			break
		}
		time.Sleep(250 * time.Millisecond)
	}

	// How many cells does each node actually hold now? A completion count of zero could mean
	// slow propagation or no propagation, and those call for different conclusions.
	for _, node := range nw.nodes {
		snapshot, err := node.broadcaster.RowSnapshot(context.Background(), nw.topic, groupID)
		require.NoError(t, err)
		held := uint64(0)
		if snapshot != nil {
			held = snapshot.Included.Count()
		}
		_, _, cellsSeen, _ := node.callbacks.counts()
		recv, sent, _, _ := rowBytes(nw.tracers[node.index])
		t.Logf("    node %2d holds %3d cells, cells offered for verification since recovery %3d, rx %7d tx %7d",
			node.index, held, cellsSeen-before[node.index], recv, sent)
	}

	// When did each of the others become complete?
	var delays []time.Duration
	notComplete := 0
	for _, node := range nw.nodes {
		if node.index == recoverer.index {
			continue
		}
		at := node.callbacks.completedAt()
		if at.IsZero() {
			notComplete++
			continue
		}
		delays = append(delays, at.Sub(recoveredAt))
	}
	slices.Sort(delays)

	t.Log("R1(b) what the phase-1 window is racing against")
	if len(delays) > 0 {
		t.Logf("  cell propagation to %d peers: min %v median %v max %v",
			len(delays), delays[0].Round(time.Millisecond),
			delays[len(delays)/2].Round(time.Millisecond),
			delays[len(delays)-1].Round(time.Millisecond))
	}
	t.Logf("  peers never complete: %d of %d", notComplete, nodes-1)
	t.Logf("  one-way link latency %v, so a bitmap announcement is ~%v away",
		gossipsim.DefaultLatency, 2*gossipsim.DefaultLatency)
	t.Logf("  shipped phase-1 window: %v", time.Duration(12000*r1bPhase1WindowBPS/10000)*time.Millisecond)

	// And the route that does arrive: the recovering node's availability bitmap. This is the
	// signal the cancellation should be using, so measure when it lands rather than asserting it
	// exists.
	var observed []time.Duration
	neverObserved := 0
	for _, node := range nw.nodes {
		if node.index == recoverer.index {
			continue
		}
		at := node.callbacks.servedElsewhereObservedAt()
		if at.IsZero() {
			neverObserved++
			continue
		}
		observed = append(observed, at.Sub(recoveredAt))
	}
	slices.Sort(observed)
	if len(observed) > 0 {
		t.Logf("  whole-row availability observed by %d peers: min %v median %v max %v",
			len(observed), observed[0].Round(time.Millisecond),
			observed[len(observed)/2].Round(time.Millisecond),
			observed[len(observed)-1].Round(time.Millisecond))
	}
	t.Logf("  peers never observing it: %d of %d", neverObserved, nodes-1)

	// The fix this prices, asserted: the observation reaches most peers, and it reaches them
	// inside the phase-1 window that local completion cannot meet.
	require.Equal(t, true, len(observed) > (nodes-1)/2,
		"most peers should observe the recovering node's whole-row availability")
	shipped := time.Duration(12000*r1bPhase1WindowBPS/10000) * time.Millisecond
	require.Equal(t, true, observed[len(observed)/2] < shipped,
		"the observation must arrive inside the phase-1 window, or it is no better than completion")

	// The finding, in its post-D11 form: **no peer completes at all.** Before the row request
	// scoping, a peer completed from a recovery in a median 783 ms -- slower than the whole 600 ms
	// phase-1 window, which was the original point. Now a pooling node stops asking once it holds
	// the reconstruction threshold, so it never assembles the other 64 cells from the network and
	// the trigger the shipped code waits for can never fire.
	//
	// Both readings say the same thing about the mechanism and the second says it more strongly: a
	// cancellation that waits for local completion is waiting for the wrong event. The right one is
	// a peer's availability bitmap, ~50 ms away, which is on the wire and unused. TODO.md D5.
	if len(delays) > 0 {
		median := delays[len(delays)/2]
		require.Equal(t, true, median > 4*gossipsim.DefaultLatency,
			"cell propagation should be many round trips, or the cancellation trigger is not the problem")
	}
	require.Equal(t, nodes-1, notComplete+len(delays),
		"every peer should be accounted for as either completed or not")
	require.Equal(t, true, notComplete > (nodes-1)/2,
		"most peers should never complete from a recovery, which is what makes the trigger unusable")

	// And the sharper form: for the peers that do complete, completion takes longer than the
	// entire phase-1 window -- which is why the sweep above sees no cancellation even where the
	// signal exists at all.
	if len(delays) > 0 {
		shippedWindow := time.Duration(12000*r1bPhase1WindowBPS/10000) * time.Millisecond
		require.Equal(t, true, delays[len(delays)/2] > shippedWindow,
			"median completion should exceed the phase-1 window -- that is why cancellation never fires")
	}
}

// TestRecoveredRowCellsVerify is the check R1(b) needed and nothing had made: do the cells and
// proofs RecoverRow produces actually pass the verification a peer will run on them?
//
// R1(b) measured a recovering node sending 1.3 MB of recovered cells that no peer accepted, and
// this is the first place to look. If recovered proofs do not verify, every cross-forward and every
// row republish of a recovered row is rejected on arrival -- the reconstruction is correct locally
// and useless to anyone else.
func TestRecoveredRowCellsVerify(t *testing.T) {
	m := newMatrix(t, 1)
	const rowIndex = uint64(0)

	// Half the columns, which is exactly the reconstruction threshold.
	held := make([]uint64, 0, 64)
	for columnIndex := range uint64(64) {
		held = append(held, columnIndex)
	}
	row := m.row(t, rowIndex, held)
	require.Equal(t, true, row.ReconstructionThresholdMet())
	require.NoError(t, peerdas.RecoverRow(&row))
	require.Equal(t, true, row.IsComplete())

	// Verify every cell the way a receiving peer does: one commitment, varying cell index.
	commitment := row.Commitment()
	bundles := make([]blocks.CellProofBundle, 0, 128)
	for _, columnIndex := range row.PresentColumnIndices() {
		bundles = append(bundles, blocks.CellProofBundle{
			ColumnIndex: columnIndex,
			Commitment:  commitment,
			Cell:        row.Cells[columnIndex],
			Proof:       row.Proofs[columnIndex],
		})
	}
	require.Equal(t, 128, len(bundles))
	require.NoError(t, peerdas.VerifyCellsKZGProofs(bundles),
		"recovered cells must verify, or a recovered row is useless to every peer")

	// And the recovered halves must match what the proposer would have published, so the
	// verification passing is not an artefact of a self-consistent but wrong recovery.
	for columnIndex := range uint64(128) {
		expected := m.cells[rowIndex][columnIndex]
		require.DeepEqual(t, expected[:], row.Cells[columnIndex],
			fmt.Sprintf("recovered cell at column %d differs from the original", columnIndex))
	}
}

// BenchmarkRecoverRow prices one row recovery, which is what D5's duplication is measured in.
func BenchmarkRecoverRow(b *testing.B) {
	t := &testing.T{}
	m := newMatrix(t, 1)
	held := make([]uint64, 0, 64)
	for columnIndex := range uint64(64) {
		held = append(held, columnIndex)
	}

	for b.Loop() {
		row := m.row(t, 0, held)
		if err := peerdas.RecoverRow(&row); err != nil {
			b.Fatal(err)
		}
	}
}
