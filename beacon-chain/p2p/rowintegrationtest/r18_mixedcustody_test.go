package rowintegrationtest

// R18 — mixed custody: who does the row axis cost, and who does it pay?
//
// The uniform-custody shapes bracket the EIP's claims without containing them: no supernode to
// relieve, no serving hotspot for D8's assignment to concentrate on, no custody-heterogeneous row
// subnets to pool -- and the D17 clause-1 question (does any request-competition penalty survive
// the announce-policy repairs?) has its signal, if it exists, on custody-minimum nodes, which a
// uniform shape dilutes. Predictions in notes/rowdas/experiments.md R18, written first.
//
// Gated on ROWDAS_R18=1 and driven entirely by the env the runner sets: the custody mix
// (ROWDAS_R2_CUSTODY_MIX), the node count matching it, seeds, and the clock domain
// (ROWDAS_SYNCTEST for counts, wall clock for the latency anchor).

import (
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/OffchainLabs/prysm/v7/beacon-chain/p2p/partialdatacolumnbroadcaster"
)

func init() {
	// Cells choose the deferral per arm (deferMS); the production default (1 s, R19) would
	// otherwise silently apply to every pre-deferral experiment in this package, changing what
	// they measure. Zero here is "the knob is the arm's to set", not a claim about production.
	partialdatacolumnbroadcaster.RowRequestDeferral = 0
}

// TestR18MixedCustodyPaired runs base and rows on the mixed-custody shape and reports per-class
// paired deltas. It asserts nothing: the estimand is the table, read against R18's predictions.
func TestR18MixedCustodyPaired(t *testing.T) {
	if os.Getenv("ROWDAS_R18") != "1" {
		t.Skip("set ROWDAS_R18=1 (with ROWDAS_R2_CUSTODY_MIX and a matching node count) to run the mixed-custody comparison")
	}
	if os.Getenv("ROWDAS_R2_CUSTODY_MIX") == "" {
		t.Fatal("R18 needs ROWDAS_R2_CUSTODY_MIX; a uniform shape cannot answer its question")
	}

	arms := []r2Arm{{name: "base", rows: false}, {name: "rows", rows: true}}
	// ROWDAS_R18_DEFER_MS sweeps D17 clause 1 on this shape: a comma-separated list of request
	// deferrals in milliseconds, each becoming a rows arm. 0 is the undeferred control.
	if list := os.Getenv("ROWDAS_R18_DEFER_MS"); list != "" {
		arms = arms[:1]
		for part := range strings.SplitSeq(list, ",") {
			ms, err := strconv.Atoi(strings.TrimSpace(part))
			if err != nil || ms < 0 {
				t.Fatalf("bad ROWDAS_R18_DEFER_MS entry %q", part)
			}
			arms = append(arms, r2Arm{name: fmt.Sprintf("rows-d%d", ms), rows: true, deferMS: ms})
		}
	}
	seeds := r2Seeds()
	results := make(map[string][]r2Result, len(arms))
	// Seed outer, arm inner: an interrupted run keeps whole paired samples.
	for _, seed := range seeds {
		for _, arm := range arms {
			t.Run(fmt.Sprintf("%s/seed-%x", arm.name, seed), func(t *testing.T) {
				cell := func(t *testing.T) {
					results[arm.name] = append(results[arm.name], runR2Arm(t, arm, seed))
				}
				if rowSynctest() {
					synctest.Test(t, cell)
				} else {
					cell(t)
				}
			})
		}
	}

	base := results["base"]
	if len(base) == 0 {
		t.Fatal("no base results")
	}

	classes := make([]int, 0, 4)
	for class := range base[0].classP50 {
		classes = append(classes, class)
	}
	slices.Sort(classes)

	clock := "wall clock (latency anchor)"
	if rowSynctest() {
		clock = "synctest (counts sharp, latencies indicative)"
	}
	t.Logf("R18 mixed custody, %d paired seeds, %s, mix %s", len(base), clock, os.Getenv("ROWDAS_R2_CUSTODY_MIX"))
	t.Logf("  %-14s %-10s %14s %14s %16s %14s %16s", "arm", "class", "base p50", "arm p50", "paired dp50", "arm-favoured", "class actions/seed")
	for _, arm := range arms[1:] {
		rows := results[arm.name]
		if len(rows) != len(base) {
			t.Fatalf("unpaired results: %d base, %d %s", len(base), len(rows), arm.name)
		}
		for _, class := range classes {
			var deltaSum time.Duration
			favoured := 0
			var basep50s, rowsp50s []time.Duration
			actions := 0
			for k := range base {
				b, r := base[k].classP50[class], rows[k].classP50[class]
				deltaSum += r - b
				if r < b {
					favoured++
				}
				basep50s = append(basep50s, b)
				rowsp50s = append(rowsp50s, r)
				actions += rows[k].classActions[class]
			}
			slices.Sort(basep50s)
			slices.Sort(rowsp50s)
			t.Logf("  %-14s custody %3d %14v %14v %16v %10d of %d %16d",
				arm.name, class,
				basep50s[len(basep50s)/2].Round(time.Millisecond),
				rowsp50s[len(rowsp50s)/2].Round(time.Millisecond),
				(deltaSum / time.Duration(len(base))).Round(time.Millisecond),
				favoured, len(base),
				actions/len(base))
		}
	}
}
