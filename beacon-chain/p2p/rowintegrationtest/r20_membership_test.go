package rowintegrationtest

// R20 — the membership scaling curve, re-measured on the repaired axis.
//
// The storm's most important property was that it grew with row-subnet membership: 8 -> 64 -> 128
// members took row-topic RPCs 1.5k -> 169k -> 570k (storm-era, plan-repair.md section 1), roughly
// quadratically -- exactly EIP-8371's "quadratic message complexity" warning. Every announce-policy
// repair since (per-peer leading-edge holding, the 200 ms window, bounded retry, gated
// advertisement cross-forwarding, D17 clause 1) changed the mechanisms behind that curve, and none
// of them has been measured against membership. A mainnet row subnet holds ~78 members, so the
// dense end is the realistic one.
//
// Gated on ROWDAS_R20=1. ROWDAS_R20_MODS is the sweep (comma-separated member mods; every kth node
// is a member), default "16,8,4,2,1" -- 8 to 128 members at 128 nodes. Both arms are re-run per
// mod: membership shapes the graph for base too, per the shared-graph discipline. Counts want
// synctest; a wall pass anchors latency.

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/OffchainLabs/prysm/v7/testing/require"
)

// TestR20MembershipScaling is R20. It asserts nothing: the estimand is the curve.
func TestR20MembershipScaling(t *testing.T) {
	if os.Getenv("ROWDAS_R20") != "1" {
		t.Skip("set ROWDAS_R20=1 to run the membership scaling sweep")
	}

	modsSpec := os.Getenv("ROWDAS_R20_MODS")
	if modsSpec == "" {
		modsSpec = "16,8,4,2,1"
	}
	mods := make([]int, 0, 8)
	for part := range strings.SplitSeq(modsSpec, ",") {
		mod, err := strconv.Atoi(strings.TrimSpace(part))
		if err != nil || mod < 1 {
			t.Fatalf("bad ROWDAS_R20_MODS entry %q", part)
		}
		mods = append(mods, mod)
	}

	prevMod, hadMod := os.LookupEnv("ROWDAS_R2_ROW_MEMBER_MOD")
	t.Cleanup(func() {
		if hadMod {
			_ = os.Setenv("ROWDAS_R2_ROW_MEMBER_MOD", prevMod)
		} else {
			_ = os.Unsetenv("ROWDAS_R2_ROW_MEMBER_MOD")
		}
	})

	// The curve is measured at the adopted request deferral (R19's 1 s) unless the sweep says
	// otherwise -- the package init zeroes the production default so pre-deferral experiments
	// keep measuring what they measured, which makes the arm's own setting the only truth here.
	deferMS := envInt("ROWDAS_R20_DEFER_MS", 1000)

	type point struct {
		mod  int
		arm  string
		seed uint64
		res  r2Result
	}
	var points []point
	seeds := r2Seeds()
	// Seed outer, then mod, then arm: an interrupted run keeps whole paired samples per point.
	for _, seed := range seeds {
		for _, mod := range mods {
			require.NoError(t, os.Setenv("ROWDAS_R2_ROW_MEMBER_MOD", strconv.Itoa(mod)))
			for _, arm := range []r2Arm{{name: "base"}, {name: "rows", rows: true, deferMS: deferMS}} {
				t.Run(fmt.Sprintf("mod-%d/%s/seed-%x", mod, arm.name, seed), func(t *testing.T) {
					cell := func(t *testing.T) {
						points = append(points, point{mod: mod, arm: arm.name, seed: seed, res: runR2Arm(t, arm, seed)})
					}
					if rowSynctest() {
						synctest.Test(t, cell)
					} else {
						cell(t)
					}
				})
			}
		}
	}

	clock := "wall clock"
	if rowSynctest() {
		clock = "synctest"
	}
	custody := os.Getenv("ROWDAS_R2_CUSTODY_MIX")
	if custody == "" {
		custody = strconv.Itoa(r2ColumnsPerNode())
	}
	t.Logf("R20 membership scaling, %d seeds, %s, %d nodes / %d blobs / custody %s",
		len(seeds), clock, r2Nodes(), r2Blobs(), custody)
	t.Logf("  %-8s %10s %14s %14s %14s %16s %14s", "members", "row RPCs", "row actions", "avail-add", "row bytes", "paired dp50", "rows-favoured")
	for _, mod := range mods {
		var rpcs, actions, avail, rowB, n int
		var deltaSum time.Duration
		favoured, pairs := 0, 0
		for _, p := range points {
			if p.mod != mod || p.arm != "rows" {
				continue
			}
			rpcs += p.res.rowRPCs
			actions += p.res.rowActions
			avail += p.res.availAdd
			rowB += p.res.rowB
			n++
			for _, q := range points {
				if q.mod == mod && q.arm == "base" && q.seed == p.seed {
					deltaSum += p.res.p50 - q.res.p50
					if p.res.p50 < q.res.p50 {
						favoured++
					}
					pairs++
				}
			}
		}
		if n == 0 {
			continue
		}
		t.Logf("  %-8d %10d %14d %14d %14d %16v %10d of %d",
			r2Nodes()/mod, rpcs/n, actions/n, avail/n, rowB/n,
			(deltaSum / time.Duration(max(pairs, 1))).Round(time.Millisecond), favoured, pairs)
	}
}
