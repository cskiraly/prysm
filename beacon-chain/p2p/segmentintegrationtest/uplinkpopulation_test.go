package segmentintegrationtest

// The heterogeneity population's own known-answer checks.
//
// A cell cannot verify these. Identical configurations differ by several per cent run to run at
// these sizes, so "sigma=0 reproduces the old scenario" is not a claim byte counts can settle;
// it has to be settled on the generator itself, where the answer is exact.

import (
	"math"
	"testing"

	"github.com/OffchainLabs/prysm/v7/testing/require"
	simlibp2p "github.com/libp2p/go-libp2p/x/simlibp2p"
)

const testBase = 50 * simlibp2p.OneMbps

func TestSigmaUplinks(t *testing.T) {
	t.Run("off unless asked", func(t *testing.T) {
		require.IsNil(t, sigmaUplinks(t, 500, testBase))
		t.Setenv("SEGMENT_UPLINK_SIGMA", "0")
		require.IsNil(t, sigmaUplinks(t, 500, testBase))
	})

	t.Run("holds the arithmetic mean across sigma", func(t *testing.T) {
		for _, sigma := range []string{"0.35", "0.7", "1.1"} {
			t.Setenv("SEGMENT_UPLINK_SIGMA", sigma)
			up := sigmaUplinks(t, 500, testBase)
			require.Equal(t, 500, len(up))
			var sum float64
			for _, x := range up[1:] {
				sum += float64(x)
			}
			am := sum / float64(len(up)-1)
			// Within a per cent of the target: the renormalisation stops at a tenth of that,
			// and the integer truncation costs less again.
			require.Equal(t, true, math.Abs(am-testBase)/testBase < 0.01,
				"sigma %s: arithmetic mean %.0f against target %d", sigma, am, testBase)
		}
	})

	t.Run("holds the harmonic mean when asked", func(t *testing.T) {
		t.Setenv("SEGMENT_UPLINK_SIGMA", "0.7")
		t.Setenv("SEGMENT_UPLINK_MEAN", "harmonic")
		up := sigmaUplinks(t, 500, testBase)
		var inv float64
		for _, x := range up[1:] {
			inv += 1 / float64(x)
		}
		hm := float64(len(up)-1) / inv
		require.Equal(t, true, math.Abs(hm-testBase)/testBase < 0.01,
			"harmonic mean %.0f against target %d", hm, testBase)
	})

	t.Run("the spread grows with sigma", func(t *testing.T) {
		var last float64
		for _, sigma := range []string{"0.2", "0.5", "0.9"} {
			t.Setenv("SEGMENT_UPLINK_SIGMA", sigma)
			up := sigmaUplinks(t, 500, testBase)
			var sum, sq float64
			for _, x := range up[1:] {
				sum += float64(x)
				sq += float64(x) * float64(x)
			}
			n := float64(len(up) - 1)
			sd := math.Sqrt(sq/n - (sum/n)*(sum/n))
			require.Equal(t, true, sd > last, "sigma %s: sd %.0f did not exceed %.0f", sigma, sd, last)
			last = sd
		}
	})

	t.Run("same seed, same population", func(t *testing.T) {
		// Without this a paired comparison across arms measures the draw, not the arm.
		t.Setenv("SEGMENT_UPLINK_SIGMA", "0.7")
		t.Setenv("SEGMENT_SEED", "11")
		a := sigmaUplinks(t, 300, testBase)
		b := sigmaUplinks(t, 300, testBase)
		require.DeepEqual(t, a, b)
		t.Setenv("SEGMENT_SEED", "12")
		c := sigmaUplinks(t, 300, testBase)
		require.Equal(t, false, equalInts(a, c), "a different seed drew the same population")
	})

	t.Run("classes are scale-invariant", func(t *testing.T) {
		// populations() reads the classes back without knowing the rate the driver passed to
		// meshLinks. That is only sound if the draw scales, so it is asserted rather than
		// assumed -- a per-class table that does not describe the cell looks like a result.
		t.Setenv("SEGMENT_UPLINK_SIGMA", "0.7")
		slowA, dcA := sigmaClasses(sigmaUplinks(t, 300, testBase))
		slowB, dcB := sigmaClasses(sigmaUplinks(t, 300, 20*testBase))
		require.DeepEqual(t, slowA, slowB)
		require.DeepEqual(t, dcA, dcB)
		slowC, dcC := sigmaNodeClasses(t, 300)
		require.DeepEqual(t, slowA, slowC)
		require.DeepEqual(t, dcA, dcC)
	})

	t.Run("the clamp bounds the draw", func(t *testing.T) {
		t.Setenv("SEGMENT_UPLINK_SIGMA", "1.5")
		t.Setenv("SEGMENT_UPLINK_CLAMP", "8")
		up := sigmaUplinks(t, 500, testBase)
		for i, x := range up[1:] {
			require.Equal(t, true, x >= testBase/8 && x <= testBase*8,
				"node %d at %d outside the clamp", i+1, x)
		}
	})

	t.Run("node 0 is left to meshLinks", func(t *testing.T) {
		// The builder must not be drawn into the population: mixing the first hop into the
		// spread confounds it with the relay path (Q33).
		t.Setenv("SEGMENT_UPLINK_SIGMA", "0.7")
		up := sigmaUplinks(t, 500, testBase)
		require.Equal(t, testBase, up[0])
	})
}

func equalInts(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
