package rowintegrationtest

// A feasibility probe: can the row cells run under testing/synctest at all, and what does the
// bubble's bookkeeping cost per cell?
//
// Why one would want to: the wall clock is this harness's largest noise source -- scheduler
// overshoot feeds timing, timing feeds claim lapses, lapses feed packets, which is why identical
// code at 128 nodes varies +-22% on counts. Under a virtual clock timers fire at exact instants,
// so the count-type headlines (the storm's RPC and action totals, the attribution shares) get an
// independent check with most of that noise removed. What synctest can NOT check: anything CPU
// (KZG work is invisible to the virtual clock) and any wall-clock-comparable latency.
//
// The probe compares within one tree at one seed, where mustEdges is deterministic, so the two
// clock modes see the same graph -- the cross-merge unpairing the segment study found (its Q53)
// does not apply.
//
// Gated on ROWDAS_SYNCTEST=1 because the harness comment in gossipsim/network.go records why
// wall clock is the default: bubble cost is roughly (timer events) x (goroutines), and a cell
// that outgrows it stalls rather than failing. Run with a real -timeout.

import (
	"os"
	"testing"
	"testing/synctest"
	"time"
)

func rowSynctest() bool { return os.Getenv("ROWDAS_SYNCTEST") == "1" }

// TestR2SynctestProbe runs one base and one rows cell inside a synctest bubble and reports the
// wall-time cost of simulating them. Counts logged by runR2Arm are the payload; the PASS line's
// real elapsed time is the feasibility answer.
func TestR2SynctestProbe(t *testing.T) {
	if !rowSynctest() {
		t.Skip("set ROWDAS_SYNCTEST=1 to run the synctest feasibility probe")
	}
	const seed = 0x83710001

	for _, arm := range []r2Arm{
		{name: "base", rows: false},
		{name: "rows", rows: true},
		{name: "rows+eager", rows: true, push: true, pushEager: true},
		{name: "rows+ads", rows: true, push: true},
		{name: "rows+gated", rows: true, push: true, pushGated: true},
	} {
		t.Run(arm.name, func(t *testing.T) {
			wallStart := time.Now()
			synctest.Test(t, func(t *testing.T) {
				virtualStart := time.Now()
				runR2Arm(t, arm, seed)
				t.Logf("  virtual time simulated: %v", time.Since(virtualStart).Round(time.Millisecond))
			})
			t.Logf("  wall time spent: %v", time.Since(wallStart).Round(time.Millisecond))
		})
	}
}
