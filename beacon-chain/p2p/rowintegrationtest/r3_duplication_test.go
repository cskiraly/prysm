package rowintegrationtest

// R3: does the row exchange's signalling and duplication stay bounded?
//
// EIP-8371 flags one risk here -- "sending an update on every received cell can lead to
// quadratic message complexity" -- and says implementations SHOULD debounce. R4(b) turned up a
// second and larger one that the EIP does not mention: **duplicate cells**. Nothing coordinates
// which mesh peer serves which missing cell, so several peers answer the same gap at once.
//
// This experiment measures both against mesh degree, which is the parameter that sets how many
// peers can answer a gap simultaneously.
//
// Prediction, written before running: duplicate cell bytes grow roughly linearly in degree,
// because each additional mesh peer is one more node that may independently decide to serve the
// same cell. Metadata bytes should be a small fraction of the total, since a bitmap is 24 bytes
// against a 2 KiB cell -- so the EIP's stated concern should turn out to be the smaller of the
// two.

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/OffchainLabs/prysm/v7/beacon-chain/core/peerdas"
	"github.com/OffchainLabs/prysm/v7/consensus-types/blocks"
	"github.com/OffchainLabs/prysm/v7/testing/require"
)

// bytesPerCell is the wire size of one cell, which sets the scale everything else is judged
// against: a parts-metadata bitmap for 128 columns is 24 bytes, two orders of magnitude less.
const bytesPerCell = 2048

// TestR3DuplicationVersusMeshDegree runs the same exact-cover pooling exchange at several mesh
// degrees and reports how much of the traffic was useful.
func TestR3DuplicationVersusMeshDegree(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-degree network sweep")
	}

	const nodes = 16
	const columnsPerNode = 4
	threshold := blocks.ReconstructionThreshold()

	m := newMatrix(t, 1)
	const rowIndex = uint64(0)
	subnet, err := peerdas.RowSubnetForBlob(rowIndex, harnessSlot)
	require.NoError(t, err)

	// minUsefulCells is the information-theoretic minimum: every node must receive the cells
	// it does not custody, and no fewer.
	minUsefulCells := nodes * (int(threshold) - columnsPerNode)

	type cell struct {
		degree         int
		realizedMesh   float64
		graphDegree    int
		submittedCells int
		recvBytes      int
		sentBytes      int
		recvRPCs       int
		amplification  float64
		verifyWaste    float64
		converged      time.Duration
	}
	var results []cell

	for _, degree := range []int{4, 8, 12} {
		func() {
			nw, stop := newRowNetworkWithDegree(t, nodes, degree, subnet, func(i int) []uint64 {
				columns := make([]uint64, 0, columnsPerNode)
				for k := range uint64(columnsPerNode) {
					columns = append(columns, uint64(i)*columnsPerNode+k)
				}
				return columns
			})
			defer stop()

			start := time.Now()
			for _, node := range nw.nodes {
				row := m.row(t, rowIndex, node.columns)
				require.NoError(t, node.broadcaster.PublishRow(context.Background(), nw.topic, row))
			}

			deadline := time.Now().Add(30 * time.Second)
			converged := time.Duration(0)
			for time.Now().Before(deadline) {
				reached := 0
				for _, node := range nw.nodes {
					if len(node.callbacks.recoverableSnapshot()) > 0 {
						reached++
					}
				}
				if reached == nodes {
					converged = time.Since(start)
					break
				}
				time.Sleep(50 * time.Millisecond)
			}
			require.Equal(t, true, converged > 0, fmt.Sprintf("degree %d did not converge", degree))
			// Read the counters once the exchange has stopped, not after a guessed sleep: a
			// short read under-reports duplication, which is the quantity under test.
			quiet := nw.waitUntilQuiet(t, 20*time.Second)

			var submittedCells, recvBytes, sentBytes, recvRPCs int
			for i, node := range nw.nodes {
				_, _, cellsSeen, _ := node.callbacks.counts()
				recv, sent, rpcs, _ := rowBytes(nw.tracers[i])
				submittedCells += cellsSeen
				recvBytes += recv
				sentBytes += sent
				recvRPCs += rpcs
			}
			t.Logf("  D=%d traffic settled %v after convergence", degree, quiet.Round(time.Millisecond))
			results = append(results, cell{
				degree:         degree,
				realizedMesh:   nw.meanMesh(),
				graphDegree:    nw.graphDegree,
				submittedCells: submittedCells,
				recvBytes:      recvBytes,
				sentBytes:      sentBytes,
				recvRPCs:       recvRPCs,
				amplification:  float64(recvBytes) / float64(minUsefulCells*bytesPerCell),
				verifyWaste:    float64(submittedCells) / float64(minUsefulCells),
				converged:      converged,
			})
		}()
	}

	minUsefulBytes := minUsefulCells * bytesPerCell
	t.Logf("R3 duplication against mesh degree, %d nodes x %d columns, exact cover of %d", nodes, columnsPerNode, threshold)
	t.Logf("  minimum useful traffic is %d cells (%.1f KiB): every node needs the %d it does not custody",
		minUsefulCells, float64(minUsefulBytes)/1024, int(threshold)-columnsPerNode)
	for _, r := range results {
		t.Logf("  D=%-3d (graph %2d, mesh formed %4.1f)  converged %6v  rx %8.1f KiB over %4d rpcs  amplification %5.2fx  per mesh peer %4.2fx  cells verified %4d (%.2fx minimum)",
			r.degree, r.graphDegree, r.realizedMesh, r.converged.Round(time.Millisecond),
			float64(r.recvBytes)/1024, r.recvRPCs, r.amplification, r.amplification/r.realizedMesh,
			r.submittedCells, r.verifyWaste)
	}

	// The mesh that formed must actually differ across arms, or the sweep is not a degree sweep
	// at all -- which is what an earlier version of this test got wrong.
	require.Equal(t, true, results[len(results)-1].realizedMesh > results[0].realizedMesh*1.5,
		fmt.Sprintf("realized mesh barely moved across arms: %v", results))

	// The headline finding: the exchange amplifies, and it amplifies with degree. Nothing
	// coordinates which mesh peer answers a gap, so each extra peer is one more independent
	// server for the same missing cell. Asserted so a future coordination fix shows up here as
	// a failure rather than passing unnoticed.
	require.Equal(t, true, results[0].amplification > 1.0,
		fmt.Sprintf("expected duplication, got %.2fx at D=%d", results[0].amplification, results[0].degree))
	require.Equal(t, true, results[len(results)-1].amplification > results[0].amplification,
		fmt.Sprintf("amplification should grow with degree: %v", results))

	// A second, smaller finding. Cells submitted for verification exceed the minimum, because
	// the "do I already have this" filter runs when a batch is dequeued, not when its
	// verification finishes -- so two batches carrying the same missing cell can both pass it.
	// That is wasted KZG work on top of the wasted bytes.
	for _, r := range results {
		require.Equal(t, true, r.submittedCells >= minUsefulCells,
			fmt.Sprintf("fewer cells verified than needed at D=%d: %d < %d", r.degree, r.submittedCells, minUsefulCells))
		require.Equal(t, true, r.verifyWaste < 1.5,
			fmt.Sprintf("verification waste at D=%d is %.2fx, far above the bytes it takes to notice", r.degree, r.verifyWaste))
	}
}

// TestR3FanoutTradesBytesForTail prices the one knob request de-confliction leaves open.
//
// Assignment at fanout 1 is byte-optimal: each missing cell is asked of exactly one peer that holds
// it, so received bytes approach the information-theoretic minimum. It is also the shape with the
// least redundancy, and its cost has a name -- "ask k peers, take the first: trades bytes for tail
// latency" -- so the parameter exists to be measured rather than argued about.
//
// The case that motivated this: R1(b) measured recovered-row propagation getting *slower* under
// assignment, 783 ms to 939 ms. A recovered row has exactly one source, so every peer's assignment
// names that one node and waits behind its uplink, where before they would take the cells from
// whichever peer happened to have them. That is the tail this sweep is about.
//
// Prediction, written before running: fanout 2 buys back most of the convergence time and costs
// roughly a second copy of the gap -- so somewhere near 2x the bytes of fanout 1, still far below
// the ~10x of no assignment at all. If it costs much more than 2x, the claim bookkeeping is
// re-asking rather than hedging.
//
// Result: N=2 costs 1.50x against N=1's 1.12x and converges no faster, so the byte half was about
// right and the latency half was wrong. **But this network is the wrong place to price N**, and
// saying so is the honest reading rather than "the knob does not pay": N is redundancy, and
// redundancy only earns its keep against loss, churn or withholding. Here every peer answers, every
// link is clean and nobody leaves, so a second request can do nothing but duplicate. The regime
// where N > 1 should pay is R5 (withholding proposer) and R6 (suppressed row subnet), and this
// sweep does not visit it. What it does establish is the *floor*: on a healthy network N=1 is
// strictly better, so the hedge is a cost to be justified by an adversarial arm, not a default.
func TestR3FanoutTradesBytesForTail(t *testing.T) {
	const nodes = 16
	const columnsPerNode = 4

	fanouts := []int{1, 2, 3}

	type cell struct {
		fanout    int
		mesh      float64
		converged time.Duration
		recvKiB   float64
		amplify   float64
		cellsSeen int
	}
	var results []cell

	for _, fanout := range fanouts {
		func() {
			previous := blocks.RequestN
			blocks.RequestN = fanout
			defer func() { blocks.RequestN = previous }()

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

			start := time.Now()
			for _, node := range nw.nodes {
				row := m.row(t, rowIndex, node.columns)
				require.NoError(t, node.broadcaster.PublishRow(context.Background(), nw.topic, row))
			}
			reached := nw.awaitRecoverable(t, 60*time.Second)
			require.Equal(t, nodes, reached, "every node should reach the threshold")

			// Convergence is the last node to reach the threshold, which is the tail this is about.
			converged := time.Duration(0)
			for _, node := range nw.nodes {
				for _, event := range node.callbacks.recoverableSnapshot() {
					if d := event.at.Sub(start); d > converged {
						converged = d
					}
				}
			}
			nw.waitUntilQuiet(t, 30*time.Second)

			recv, cellsSeen := 0, 0
			for i, node := range nw.nodes {
				r, _, _, _ := rowBytes(nw.tracers[i])
				recv += r
				_, _, seen, _ := node.callbacks.counts()
				cellsSeen += seen
			}

			// The same basis TestR3DuplicationVersusMeshDegree uses, so the figures are
			// comparable: every node needs enough cells to *reach the threshold*, which is 64
			// minus its own 4, not all 124 it lacks. Getting this wrong once made the
			// amplification read below 1.0, which is impossible and was the giveaway.
			needed := int(blocks.ReconstructionThreshold()) - columnsPerNode
			minimum := float64(nodes*needed) * bytesPerCell
			results = append(results, cell{
				fanout:    fanout,
				mesh:      nw.meanMesh(),
				converged: converged,
				recvKiB:   float64(recv) / 1024,
				amplify:   float64(recv) / minimum,
				cellsSeen: cellsSeen,
			})
		}()
	}

	t.Logf("R3 fanout sweep, %d nodes each custodying %d columns (exact cover)", nodes, columnsPerNode)
	t.Logf("  %-8s %8s %12s %14s %14s %12s", "fanout", "mesh", "converged", "rx KiB", "amplification", "cells")
	for _, r := range results {
		t.Logf("  %-8d %8.1f %12v %14.1f %13.2fx %12d",
			r.fanout, r.mesh, r.converged.Round(time.Millisecond), r.recvKiB, r.amplify, r.cellsSeen)
	}

	require.Equal(t, len(fanouts), len(results))

	// Fanout 1 must be the cheapest, or assignment is not doing what it claims.
	require.Equal(t, true, results[0].amplify < results[1].amplify,
		"fanout 1 should cost fewer bytes than fanout 2")

	// And every arm must stay far below the ~10x of no assignment at all, or the hedge has
	// undone the fix.
	for _, r := range results {
		require.Equal(t, true, r.amplify < 5.0,
			"fanout %d amplification %.2fx is approaching the un-de-conflicted 9.6x")
	}
}
