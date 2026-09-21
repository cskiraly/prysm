package rowintegrationtest

// R4(a): can custody-minimum nodes on a row subnet collectively cover enough columns to
// reconstruct a row?
//
// EIP-8371's rationale for putting non-reconstructors on row topics rests on this claim:
// "~94 custody-minimum nodes per subnet collectively cover 64+ columns probabilistically".
// It is a property of the custody derivation alone, so it needs no network -- which makes it
// the cheapest of the R-questions and the one that decides whether R4(b), the network run, is
// worth building.
//
// Prediction, written before running (R4a): coverage saturates fast. The coupon-collector shape
// means m = 25 members at k = 4 columns each should already cover 64 distinct columns with high
// probability, so m = 94 is comfortable. The interesting
// regime is therefore *small* m -- a sparse row subnet -- not a mainnet-sized one, and the
// threshold should be sharp in m and much weaker in k.

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/OffchainLabs/prysm/v7/beacon-chain/core/peerdas"
	fieldparams "github.com/OffchainLabs/prysm/v7/config/fieldparams"
	"github.com/OffchainLabs/prysm/v7/config/params"
	"github.com/OffchainLabs/prysm/v7/testing/require"
	"github.com/ethereum/go-ethereum/p2p/enode"
)

// coverageTrials is how many independent draws of a subnet's membership each cell averages
// over. The quantity being estimated is a probability near 0 or 1 over most of the range, so
// a few hundred draws resolve the transition well enough to read a threshold off it.
const coverageTrials = 400

// drawNodeIDs returns count node IDs from a seeded source, so a reported figure can be
// reproduced from its seed.
func drawNodeIDs(rng *rand.Rand, count int) []enode.ID {
	ids := make([]enode.ID, count)
	for i := range ids {
		_, _ = rng.Read(ids[i][:])
	}

	return ids
}

// distinctColumns is how many distinct columns a set of nodes collectively custodies.
func distinctColumns(t *testing.T, ids []enode.ID, custodyGroupCount uint64) int {
	t.Helper()

	covered := make(map[uint64]bool, len(ids)*int(custodyGroupCount))
	for _, id := range ids {
		groups, err := peerdas.CustodyGroups(id, custodyGroupCount)
		require.NoError(t, err)
		columns, err := peerdas.CustodyColumns(groups)
		require.NoError(t, err)
		for column := range columns {
			covered[column] = true
		}
	}

	return len(covered)
}

// TestR4aPooledCustodyCoverage measures P(distinct columns >= reconstruction threshold) over
// the number of members on a row subnet, for each plausible custody requirement.
func TestR4aPooledCustodyCoverage(t *testing.T) {
	threshold := int(peerdas.MinimumColumnCountToReconstruct())
	cfg := params.BeaconConfig()

	// k = CUSTODY_REQUIREMENT is a node with no validators attached; k =
	// VALIDATOR_CUSTODY_REQUIREMENT is the floor for one that has some.
	custodyCounts := []uint64{cfg.CustodyRequirement, cfg.ValidatorCustodyRequirement}
	memberCounts := []int{4, 8, 12, 16, 20, 25, 30, 40, 64, 94}

	type cell struct {
		members     int
		custody     uint64
		reachedPct  float64
		meanCovered float64
		minCovered  int
	}
	var results []cell

	for _, custody := range custodyCounts {
		for _, members := range memberCounts {
			rng := rand.New(rand.NewSource(int64(0x8371_0000 + members*100 + int(custody))))
			reached, totalCovered, minCovered := 0, 0, fieldparams.NumberOfColumns+1
			for range coverageTrials {
				covered := distinctColumns(t, drawNodeIDs(rng, members), custody)
				totalCovered += covered
				minCovered = min(minCovered, covered)
				if covered >= threshold {
					reached++
				}
			}
			results = append(results, cell{
				members:     members,
				custody:     custody,
				reachedPct:  100 * float64(reached) / float64(coverageTrials),
				meanCovered: float64(totalCovered) / float64(coverageTrials),
				minCovered:  minCovered,
			})
		}
	}

	t.Logf("R4(a) pooled custody coverage, %d trials per cell, threshold %d of %d columns",
		coverageTrials, threshold, fieldparams.NumberOfColumns)
	for _, r := range results {
		t.Logf("  k=%-2d m=%-3d  P(cover>=%d) %6.2f%%  mean covered %6.1f  worst %3d",
			r.custody, r.members, threshold, r.reachedPct, r.meanCovered, r.minCovered)
	}

	// The claims the EIP actually makes, asserted rather than eyeballed.
	find := func(custody uint64, members int) cell {
		for _, r := range results {
			if r.custody == custody && r.members == members {
				return r
			}
		}
		t.Fatalf("no cell for k=%d m=%d", custody, members)

		return cell{}
	}

	// At the EIP's stated mainnet figure, coverage must be a certainty rather than a
	// probability, or the "virtual reconstruction by pooled custody" fallback is not a
	// fallback.
	mainnet := find(cfg.CustodyRequirement, 94)
	require.Equal(t, 100.0, mainnet.reachedPct,
		fmt.Sprintf("94 custody-minimum nodes should always cover %d columns", threshold))

	// And a sparse subnet must fail, or the measurement is not sensitive to anything.
	sparse := find(cfg.CustodyRequirement, 8)
	require.Equal(t, 0.0, sparse.reachedPct,
		fmt.Sprintf("8 nodes holding %d columns each cannot cover %d", cfg.CustodyRequirement, threshold))
}

// TestR4aCoverageThreshold locates the member count at which coverage becomes near-certain,
// which is the number that matters for a devnet or a partially-adopted network: below it the
// row topic cannot reconstruct at all, however many cells are exchanged.
func TestR4aCoverageThreshold(t *testing.T) {
	threshold := int(peerdas.MinimumColumnCountToReconstruct())
	custody := params.BeaconConfig().CustodyRequirement

	firstCertain, firstPossible := 0, 0
	for members := 1; members <= 64; members++ {
		rng := rand.New(rand.NewSource(int64(0x4a_0000 + members)))
		reached := 0
		for range coverageTrials {
			if distinctColumns(t, drawNodeIDs(rng, members), custody) >= threshold {
				reached++
			}
		}
		if reached > 0 && firstPossible == 0 {
			firstPossible = members
		}
		if reached == coverageTrials && firstCertain == 0 {
			firstCertain = members
			break
		}
	}

	t.Logf("R4(a) threshold at k=%d: first m with any coverage %d, first m with certain coverage %d (%d trials)",
		custody, firstPossible, firstCertain, coverageTrials)

	require.Equal(t, true, firstPossible > 0, "coverage never reached at any member count up to 64")
	require.Equal(t, true, firstCertain > 0, "coverage never certain at any member count up to 64")
	// A hard floor: k members hold at most k*custody columns, so nothing below
	// threshold/custody can ever work. The measured onset must respect it.
	require.Equal(t, true, firstPossible >= threshold/int(custody),
		"coverage claimed below the information-theoretic floor")
}

// TestR4aConcentratedCustodyCoversBetter records a falsified prediction.
//
// The prediction was that at equal total custodied column slots, many nodes holding few columns
// would cover at least as well as few nodes holding many -- the coupon-collector reading, where
// independent draws overlap less than a concentrated one.
//
// It is the other way round, and by a clear margin: at 96 total slots, 3 nodes holding 32
// columns each cover the threshold every time while 24 nodes holding 4 each manage 94%.
//
// The reason is in the custody derivation rather than in probability. CustodyGroups keeps
// drawing until it has the requested number of *distinct* groups
// (beacon-chain/core/peerdas/das_core.go), so one node's k columns never collide with each
// other -- only across nodes. Concentrating custody therefore wastes no slots, and spreading it
// wastes them at exactly the birthday rate.
//
// What this means for EIP-8371 is mildly reassuring rather than awkward: custody-minimum nodes
// are the many-small shape, which is the *worst* case for a given total, so the ~94-member
// figure the rationale quotes is the conservative reading. A subnet whose members hold more
// than the minimum needs fewer of them than proportionally.
func TestR4aConcentratedCustodyCoversBetter(t *testing.T) {
	threshold := int(peerdas.MinimumColumnCountToReconstruct())

	// Hold the total number of custodied column slots constant and vary how they are
	// distributed: many nodes holding few columns against few nodes holding many.
	//
	// The total has to sit *near the transition* to show anything. A first attempt used 256
	// slots and every shape came out at 100%, which measured only that 256 is far above 64 --
	// the design was wrong, not the claim. 96 slots is just above where k=4 becomes certain
	// (m=27, measured above), so overlap losses are visible here.
	type shape struct {
		members int
		custody uint64
	}
	shapes := []shape{{24, 4}, {12, 8}, {6, 16}, {3, 32}}

	t.Logf("R4(a) same total column slots (96), distributed differently, %d trials per cell", coverageTrials)
	var pcts []float64
	for _, s := range shapes {
		rng := rand.New(rand.NewSource(int64(0x4a_1000 + s.members)))
		reached := 0
		for range coverageTrials {
			if distinctColumns(t, drawNodeIDs(rng, s.members), s.custody) >= threshold {
				reached++
			}
		}
		pct := 100 * float64(reached) / float64(coverageTrials)
		pcts = append(pcts, pct)
		t.Logf("  m=%-3d k=%-3d  P(cover>=%d) %6.2f%%", s.members, s.custody, threshold, pct)
	}

	// Coverage improves monotonically as custody concentrates. Asserted in the measured
	// direction, which is the opposite of the prediction above.
	for i := 1; i < len(pcts); i++ {
		require.Equal(t, true, pcts[i] >= pcts[i-1]-0.01,
			fmt.Sprintf("coverage should not fall as custody concentrates: %v", pcts))
	}
	require.Equal(t, true, pcts[len(pcts)-1] > pcts[0],
		fmt.Sprintf("concentrated custody should cover strictly better at equal total slots: %v", pcts))
	// The sweep must not be saturated, or it is measuring nothing. The first attempt used 256
	// total slots and every shape came out at 100%.
	require.Equal(t, true, pcts[0] < 100.0,
		fmt.Sprintf("every shape covers: the total is too far above the threshold to be informative %v", pcts))
}
