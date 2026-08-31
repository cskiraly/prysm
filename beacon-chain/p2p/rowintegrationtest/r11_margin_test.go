package rowintegrationtest

// R11 — how much column-path loss does the row axis absorb?
//
// The erasure-coding margin already exists under PeerDAS: any 64 of 128 columns reconstruct the
// blob. But only a node that *holds* 64 columns can use it, so for everyone else the margin is
// unusable. RowDAS lets nodes holding a handful of columns each pool over the row topic until they
// collectively reach the threshold, reconstruct, and push the recovered cells back into the column
// subnets that lost them.
//
// If that is right, RowDAS does not add a margin — it converts an existing, unusable one into a
// usable one. This sweep is what would falsify that, in both directions: a partial rescue above the
// threshold would mean the push does not reach every lost column, and *any* rescue below it would
// mean the harness is feeding the row subnet data by a route the experiment does not intend.
//
// Loss is modelled at the source, because the harness has no packet-loss model and a column subnet
// that delivers nothing is indistinguishable from one whose cells were never published. So this
// measures loss of a column's data, whatever the cause — withholding, an eclipsed topic, a mesh
// that never formed.

import (
	"testing"
	"time"

	"github.com/OffchainLabs/prysm/v7/consensus-types/blocks"
	"github.com/OffchainLabs/prysm/v7/testing/require"
)

// r11Point is one dose: how many column subnets survived, and what the row axis rescued.
type r11Point struct {
	surviving int
	lost      int
	rescued   int
	// reconstructors is how many row-subnet members reached the threshold, which is the
	// mechanism behind any rescue at all.
	reconstructors int
	firstAt        time.Duration
	lastAt         time.Duration
}

// TestR11ColumnLossMargin is R11.
func TestR11ColumnLossMargin(t *testing.T) {
	threshold := int(blocks.ReconstructionThreshold())
	require.Equal(t, 64, threshold, "the sweep is built around the 64-cell threshold")

	// Around the threshold, not merely across it: 65 and 63 are the two points that decide whether
	// the boundary is where the coding says it should be.
	surviving := []int{96, 65, 64, 63, 32}

	points := make([]r11Point, 0, len(surviving))
	for _, s := range surviving {
		t.Run(fmtSurviving(s), func(t *testing.T) {
			got := runR9Arm(t, r9Arm{name: "rows+push", push: true}, s)
			points = append(points, r11Point{
				surviving:      s,
				lost:           int(r9Columns) - s,
				rescued:        got.withheldComplete,
				reconstructors: got.reconstructors,
				firstAt:        got.firstWithheld,
				lastAt:         got.lastWithheld,
			})
		})
	}

	t.Log("R11 column-path loss absorbed by the row axis")
	t.Logf("  %10s %8s %10s %16s %10s %10s", "surviving", "lost", "rescued", "reconstructors", "first", "last")
	for _, p := range points {
		t.Logf("  %10d %8d %10d %16d %10v %10v",
			p.surviving, p.lost, p.rescued, p.reconstructors,
			p.firstAt.Round(time.Millisecond), p.lastAt.Round(time.Millisecond))
	}

	for _, p := range points {
		if p.surviving >= threshold {
			// Above the threshold the pooled subnet reaches 64 cells and every lost column comes
			// back. A partial rescue here would be a mechanism problem in the push.
			require.Equal(t, p.lost, p.rescued,
				"with "+fmtSurviving(p.surviving)+" the row axis should rescue every lost column")
			continue
		}
		// Below it the data is genuinely gone: fewer than 64 cells of the row exist anywhere, so no
		// amount of pooling can recover it. Any rescue here would mean the setup leaks.
		require.Equal(t, 0, p.rescued,
			"below the coding threshold nothing can be rescued, so "+fmtSurviving(p.surviving)+" must rescue none")
		require.Equal(t, 0, p.reconstructors,
			"and nothing should even reach the threshold")
	}

	// The comparison that makes this a resilience result rather than a curiosity: no node in this
	// setup holds more than 8 of the 128 columns, so under PeerDAS *nothing* is rescued at any point
	// on the sweep. The margin exists in the coding either way; pooling is what makes it reachable.
	require.Equal(t, 8, len(r9Custody(0)), "each node holds 8 columns, far below the threshold")
}

func fmtSurviving(n int) string {
	switch n {
	case 96:
		return "96-surviving"
	case 65:
		return "65-surviving"
	case 64:
		return "64-surviving"
	case 63:
		return "63-surviving"
	default:
		return "32-surviving"
	}
}
