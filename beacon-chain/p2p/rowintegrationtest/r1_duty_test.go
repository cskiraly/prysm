package rowintegrationtest

// R1(a): how much reconstruction work does RowDAS actually remove from a reconstructor?
//
// EIP-8371's §Security claim is "strictly reducing total work": under PeerDAS every node with
// enough custody reconstructs every missing row, under RowDAS each row is reconstructed once by
// the node whose row subnet carries it. The size of the saving is a property of two derivations --
// node-to-subnet and blob-to-subnet -- so it needs no network, which makes it the cheap half of
// R1 and the half that decides what the networked half has to look at.
//
// The arithmetic is not the interesting part; `blobCount / ROW_SUBNET_COUNT` is not a measurement.
// Two things are:
//
//	occupancy   A row whose subnet holds no reconstructor has no designated reconstructor at all,
//	            and falls through to phase 2 -- where *every* reconstructor is eligible and the
//	            saving erodes towards the PeerDAS baseline. So the real question is not the mean
//	            duty but P(every row has someone).
//	spread      Duty is `|BlobsForRowSubnet|`, which is not flat: the per-slot shuffle hands some
//	            subnets more rows than others. A claim about per-node CPU has to quote the worst
//	            case, not the mean.
//
// Prediction, written before running: occupancy is the binding constraint, not duty. At mainnet
// scale (thousands of nodes, ROW_SUBNET_COUNT = 128) every subnet holds many reconstructors and
// occupancy is 1, so the saving is the full factor of 128 -- but it should fall off sharply once
// nodes-per-subnet approaches the reconstructor share, which for a network of a few hundred nodes
// at 128 subnets it does. Duty spread should be mild: with blobCount >= ROW_SUBNET_COUNT the
// shuffle gives floor or ceil, and below it the duty is 0 or 1 and the spread question dissolves.

import (
	"fmt"
	"math"
	"math/rand"
	"testing"

	"github.com/OffchainLabs/prysm/v7/beacon-chain/core/peerdas"
	"github.com/OffchainLabs/prysm/v7/config/params"
	"github.com/OffchainLabs/prysm/v7/testing/require"
	"github.com/ethereum/go-ethereum/p2p/enode"
)

// r1Trials is how many independent draws of a network each cell averages over. Occupancy is a
// probability that runs from 0 to 1 across the sweep, so a few hundred draws resolve the
// transition well enough to read a threshold off it.
const r1Trials = 200

// r1Node is one drawn node: its row subnet and whether it holds enough custody to recover a row
// alone.
type r1Node struct {
	rowSubnet     uint64
	reconstructor bool
}

// drawNetwork draws n nodes with the real derivations. reconstructorShare is the fraction given
// supernode custody; the rest get the custody minimum, which is not enough to recover alone.
func drawNetwork(t *testing.T, rng *rand.Rand, n int, reconstructorShare float64) []r1Node {
	t.Helper()

	cfg := params.BeaconConfig()
	nodes := make([]r1Node, 0, n)
	for i := range n {
		var id enode.ID
		_, _ = rng.Read(id[:])

		subnet, err := peerdas.RowSubnetForNode(id)
		require.NoError(t, err)

		// Custody is drawn by position rather than at random so the share is exact: a
		// probability over a probability would need many more trials to say anything.
		groupCount := cfg.CustodyRequirement
		if float64(i) < float64(n)*reconstructorShare {
			groupCount = cfg.NumberOfCustodyGroups
		}
		groups, err := peerdas.CustodyGroups(id, groupCount)
		require.NoError(t, err)
		columns, err := peerdas.CustodyColumns(groups)
		require.NoError(t, err)

		nodes = append(nodes, r1Node{
			rowSubnet:     subnet,
			reconstructor: peerdas.IsRowReconstructor(uint64(len(columns))),
		})
	}

	return nodes
}

// r1Outcome is what one drawn network gives for one blob count.
type r1Outcome struct {
	// rowsWithReconstructor is how many of the block's rows have at least one reconstructor on
	// the subnet that carries them.
	rowsWithReconstructor int
	rows                  int
	// maxDuty is the largest phase-1 duty any single reconstructor carries, and meanDuty the
	// average over reconstructors that have any.
	maxDuty  int
	sumDuty  int
	withDuty int
}

func evaluate(t *testing.T, nodes []r1Node, blobCount uint64) r1Outcome {
	t.Helper()

	// Which subnets hold a reconstructor, and how many rows each subnet carries.
	reconstructorsOn := make(map[uint64]int)
	for _, node := range nodes {
		if node.reconstructor {
			reconstructorsOn[node.rowSubnet]++
		}
	}

	out := r1Outcome{rows: int(blobCount)}
	rowSubnetCount := params.BeaconConfig().RowSubnetCount
	dutyOf := make(map[uint64]int, rowSubnetCount)
	for subnet := range rowSubnetCount {
		blobIndices, err := peerdas.BlobsForRowSubnet(subnet, harnessSlot, blobCount)
		require.NoError(t, err)
		dutyOf[subnet] = len(blobIndices)
		if len(blobIndices) > 0 && reconstructorsOn[subnet] > 0 {
			out.rowsWithReconstructor += len(blobIndices)
		}
	}

	for _, node := range nodes {
		if !node.reconstructor {
			continue
		}
		duty := dutyOf[node.rowSubnet]
		if duty == 0 {
			continue
		}
		out.withDuty++
		out.sumDuty += duty
		out.maxDuty = max(out.maxDuty, duty)
	}

	return out
}

// TestR1aReconstructionDutyAndOccupancy sweeps network size and ROW_SUBNET_COUNT, and reports the
// two quantities that decide whether the saving is real: what each reconstructor is asked to do,
// and whether every row has somebody to do it.
func TestR1aReconstructionDutyAndOccupancy(t *testing.T) {
	params.SetupTestConfigCleanup(t)

	const blobCount = uint64(32)
	const reconstructorShare = 0.10 // supernode share; swept separately below

	networkSizes := []int{50, 100, 200, 500, 1000, 5000, 20000}
	subnetCounts := []uint64{8, 32, 128}

	type cell struct {
		n            int
		subnetCount  uint64
		occupancyPct float64
		coveredPct   float64
		meanDuty     float64
		maxDuty      int
	}
	var results []cell

	for _, subnetCount := range subnetCounts {
		cfg := params.BeaconConfig().Copy()
		cfg.RowSubnetCount = subnetCount
		params.OverrideBeaconConfig(cfg)

		for _, n := range networkSizes {
			rng := rand.New(rand.NewSource(int64(0x8371_1000 + n*100 + int(subnetCount))))
			allCovered, coveredRows, totalRows := 0, 0, 0
			sumDuty, withDuty, maxDuty := 0, 0, 0
			for range r1Trials {
				out := evaluate(t, drawNetwork(t, rng, n, reconstructorShare), blobCount)
				if out.rowsWithReconstructor == out.rows {
					allCovered++
				}
				coveredRows += out.rowsWithReconstructor
				totalRows += out.rows
				sumDuty += out.sumDuty
				withDuty += out.withDuty
				maxDuty = max(maxDuty, out.maxDuty)
			}
			meanDuty := 0.0
			if withDuty > 0 {
				meanDuty = float64(sumDuty) / float64(withDuty)
			}
			results = append(results, cell{
				n: n, subnetCount: subnetCount,
				occupancyPct: 100 * float64(allCovered) / float64(r1Trials),
				coveredPct:   100 * float64(coveredRows) / float64(totalRows),
				meanDuty:     meanDuty,
				maxDuty:      maxDuty,
			})
		}
	}

	t.Logf("R1(a) reconstruction duty and occupancy, %d blobs, %.0f%% reconstructor share, %d trials per cell",
		blobCount, 100*reconstructorShare, r1Trials)
	t.Logf("  PeerDAS baseline: every reconstructor recovers all %d rows", blobCount)
	t.Logf("  %-6s %-6s %14s %12s %10s %8s %10s", "N", "n", "P(all rows)", "rows covered", "mean duty", "max duty", "reduction")
	for _, r := range results {
		reduction := 0.0
		if r.meanDuty > 0 {
			reduction = float64(blobCount) / r.meanDuty
		}
		t.Logf("  %-6d %-6d %13.1f%% %11.1f%% %10.2f %8d %9.1fx",
			r.subnetCount, r.n, r.occupancyPct, r.coveredPct, r.meanDuty, r.maxDuty, reduction)
	}

	find := func(subnetCount uint64, n int) cell {
		for _, r := range results {
			if r.subnetCount == subnetCount && r.n == n {
				return r
			}
		}
		t.Fatalf("no cell for N=%d n=%d", subnetCount, n)

		return cell{}
	}

	// Duty is the full division wherever a reconstructor exists. That half of the claim holds
	// everywhere in the sweep, which is why it is not the interesting half.
	for _, r := range results {
		if r.meanDuty == 0 {
			continue
		}
		want := float64(blobCount) / float64(r.subnetCount)
		if want < 1 {
			want = 1
		}
		require.Equal(t, true, r.meanDuty <= want+1e-9,
			fmt.Sprintf("N=%d n=%d: duty %.2f exceeds %d blobs over %d subnets",
				r.subnetCount, r.n, r.meanDuty, blobCount, r.subnetCount))
	}

	// Occupancy is the binding constraint, and it is where the prediction was too loose. It said
	// occupancy would be 1 "at mainnet scale" without saying what that means; the sweep says the
	// boundary sits between 5 000 and 20 000 nodes at a 10 percent reconstructor share, which is
	// squarely inside the range mainnet occupies. Both ends are asserted so the boundary cannot
	// move unnoticed.
	require.Equal(t, true, find(128, 5000).occupancyPct < 80.0,
		fmt.Sprintf("N=128 n=5000: expected occupancy well short of certain, got %.1f pct",
			find(128, 5000).occupancyPct))
	require.Equal(t, 100.0, find(128, 20000).occupancyPct,
		"N=128 n=20000: expected every row to have a designated reconstructor")

	// Fewer subnets buy occupancy at the cost of duty, and the trade should be visible.
	eight := find(8, 1000)
	require.Equal(t, true, eight.occupancyPct > 90.0,
		fmt.Sprintf("N=8 n=1000: expected near-certain occupancy, got %.1f pct", eight.occupancyPct))
	require.Equal(t, true, eight.meanDuty > find(128, 1000).meanDuty,
		"fewer subnets must mean more rows per reconstructor")
}

// TestR1aOccupancyIsPoissonInReconstructorsPerSubnet restates R1(a)'s finding in the form that
// does not depend on the network size, and checks the derivation against the closed form.
//
// If `RowSubnetForNode` spreads nodes uniformly, the number of reconstructors on a given subnet is
// Binomial(R, 1/N) ~ Poisson(lambda) with lambda = R/N, so a row is unattended with probability
// e^-lambda and all `b` rows are attended with probability roughly (1 - e^-lambda)^b. Measuring
// against that does two things at once: it says how many reconstructors a network needs per row
// subnet, and it tests that the subnet derivation is actually uniform -- a skewed hash would show
// up as measured occupancy below the closed form.
func TestR1aOccupancyIsPoissonInReconstructorsPerSubnet(t *testing.T) {
	params.SetupTestConfigCleanup(t)

	const blobCount = uint64(32)
	const subnetCount = uint64(128)
	cfg := params.BeaconConfig().Copy()
	cfg.RowSubnetCount = subnetCount
	params.OverrideBeaconConfig(cfg)

	// Reconstructors per subnet, which is the only variable that matters.
	lambdas := []float64{0.5, 1, 2, 3, 5, 8, 12}

	t.Logf("R1(a) occupancy against reconstructors per row subnet, N=%d, %d blobs, %d trials",
		subnetCount, blobCount, r1Trials)
	t.Logf("  %8s %12s %14s %16s %14s", "lambda", "reconstr", "P(all rows)", "P(all) closed", "rows covered")

	for _, lambda := range lambdas {
		reconstructors := int(lambda * float64(subnetCount))
		// Every drawn node is a reconstructor, so the count is exact and the sweep is over
		// lambda alone rather than over lambda and a share at once.
		rng := rand.New(rand.NewSource(int64(0x8371_2000 + reconstructors)))
		allCovered, coveredRows, totalRows := 0, 0, 0
		for range r1Trials {
			out := evaluate(t, drawNetwork(t, rng, reconstructors, 1.0), blobCount)
			if out.rowsWithReconstructor == out.rows {
				allCovered++
			}
			coveredRows += out.rowsWithReconstructor
			totalRows += out.rows
		}
		attended := 1 - math.Exp(-lambda)
		closed := math.Pow(attended, float64(blobCount))
		t.Logf("  %8.1f %12d %13.1f%% %15.1f%% %13.1f%%",
			lambda, reconstructors,
			100*float64(allCovered)/float64(r1Trials), 100*closed,
			100*float64(coveredRows)/float64(totalRows))

		// The derivation must be uniform: measured per-row coverage should track 1 - e^-lambda.
		// A skewed subnet hash would concentrate reconstructors and show up here.
		measured := float64(coveredRows) / float64(totalRows)
		require.Equal(t, true, math.Abs(measured-attended) < 0.05,
			fmt.Sprintf("lambda=%.1f: measured coverage %.3f vs closed form %.3f -- subnet derivation may not be uniform",
				lambda, measured, attended))

		// The threshold, asserted at both ends so it cannot drift: two reconstructors per subnet
		// is nowhere near enough, eight is enough.
		allRows := float64(allCovered) / float64(r1Trials)
		if lambda == 2 {
			require.Equal(t, true, allRows < 0.10,
				fmt.Sprintf("lambda=2 should almost never attend every row, got %.3f", allRows))
		}
		if lambda == 8 {
			require.Equal(t, true, allRows > 0.95,
				fmt.Sprintf("lambda=8 should almost always attend every row, got %.3f", allRows))
		}
	}
}
