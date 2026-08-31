package rowintegrationtest

// R3(b) — the announce-policy windows, swept where the instrument is sharp.
//
// The availability window (100 ms) and the cancellation delay (500 ms) are placeholders, the
// leading edge was chosen by design argument, and R15's clock-sensitivity finding has an untested
// mechanism (synchronized flush deadlines). This sweep prices all four on counts, which under
// synctest repeat to ~1% (R15) -- so it is meant to run with ROWDAS_SYNCTEST=1, and the winning
// values get a wall-clock latency anchor by running the same test without it. Predictions are in
// notes/rowdas/experiments.md R3(b), written before the first run.
//
// Gated on ROWDAS_R3=1: it is a sweep, not a regression test, and it asserts nothing -- the output
// is the table.

import (
	"fmt"
	"os"
	"testing"
	"testing/synctest"
	"time"

	"github.com/OffchainLabs/prysm/v7/consensus-types/blocks"
)

type r3Arm struct {
	name    string
	avail   time.Duration
	cancel  time.Duration
	leading bool
	jitter  float64
}

// setPolicy applies one arm's announce policy globally and returns the restore. Global vars are
// safe here because cells run strictly sequentially and each cell's broadcasters are stopped
// before the next arm is applied.
func setPolicy(arm r3Arm) func() {
	prevAvail := blocks.RequestAvailabilityMaxDelay
	prevCancel := blocks.RequestCancellationMaxDelay
	prevLeading := blocks.AvailabilityLeadingEdge
	prevJitter := blocks.AvailabilityFlushJitterFraction

	blocks.RequestAvailabilityMaxDelay = arm.avail
	blocks.RequestCancellationMaxDelay = arm.cancel
	blocks.AvailabilityLeadingEdge = arm.leading
	blocks.AvailabilityFlushJitterFraction = arm.jitter

	return func() {
		blocks.RequestAvailabilityMaxDelay = prevAvail
		blocks.RequestCancellationMaxDelay = prevCancel
		blocks.AvailabilityLeadingEdge = prevLeading
		blocks.AvailabilityFlushJitterFraction = prevJitter
	}
}

// TestR3AnnouncePolicySweep is R3(b).
func TestR3AnnouncePolicySweep(t *testing.T) {
	if os.Getenv("ROWDAS_R3") != "1" {
		t.Skip("set ROWDAS_R3=1 (and normally ROWDAS_SYNCTEST=1, see R15) to run the announce-policy sweep")
	}

	arms := []r3Arm{
		// The availability window, leading edge. 0 is holding switched off entirely.
		{name: "avail-0", avail: 0, cancel: 500 * time.Millisecond, leading: true},
		{name: "avail-25", avail: 25 * time.Millisecond, cancel: 500 * time.Millisecond, leading: true},
		{name: "avail-50", avail: 50 * time.Millisecond, cancel: 500 * time.Millisecond, leading: true},
		{name: "avail-100", avail: 100 * time.Millisecond, cancel: 500 * time.Millisecond, leading: true},
		{name: "avail-200", avail: 200 * time.Millisecond, cancel: 500 * time.Millisecond, leading: true},
		{name: "avail-400", avail: 400 * time.Millisecond, cancel: 500 * time.Millisecond, leading: true},
		// The edge question, at the default window.
		{name: "trailing-100", avail: 100 * time.Millisecond, cancel: 500 * time.Millisecond, leading: false},
		// R15's synchronization hypothesis: a deterministic per-peer +-25% window spread.
		{name: "jitter-100", avail: 100 * time.Millisecond, cancel: 500 * time.Millisecond, leading: true, jitter: 0.25},
		// The cancellation delay, at the default window. 500 ms is the avail-100 arm.
		{name: "cancel-125", avail: 100 * time.Millisecond, cancel: 125 * time.Millisecond, leading: true},
		{name: "cancel-2000", avail: 100 * time.Millisecond, cancel: 2000 * time.Millisecond, leading: true},
	}

	// Seed outer, arm inner: an interrupted run still yields whole paired samples across arms,
	// the same ordering R2 uses and for the same reason.
	seeds := r2Seeds()
	results := make(map[string][]r2Result, len(arms))
	for _, seed := range seeds {
		for _, arm := range arms {
			t.Run(fmt.Sprintf("%s/seed-%x", arm.name, seed), func(t *testing.T) {
				restore := setPolicy(arm)
				defer restore()
				cell := func(t *testing.T) {
					results[arm.name] = append(results[arm.name], runR2Arm(t, r2Arm{name: "rows", rows: true}, seed))
				}
				if rowSynctest() {
					synctest.Test(t, cell)
				} else {
					cell(t)
				}
			})
		}
	}

	clock := "wall clock (latency anchor; counts are noisy here, see R15)"
	if rowSynctest() {
		clock = "synctest (counts sharp, latencies indicative only)"
	}
	t.Logf("R3(b) announce-policy sweep, %d seeds, %s", len(seeds), clock)
	t.Logf("  %-14s %12s %12s %10s %9s %9s %10s", "arm", "row actions", "row RPCs", "avail-add", "req-add", "churn", "p50 (ind.)")
	for _, arm := range arms {
		rs := results[arm.name]
		if len(rs) == 0 {
			continue
		}
		var actions, rpcs, avail, add, churn int
		var p50 time.Duration
		for _, r := range rs {
			actions += r.rowActions
			rpcs += r.rowRPCs
			avail += r.availAdd
			add += r.reqAdd
			churn += r.reqChurn
			p50 += r.p50
		}
		n := len(rs)
		t.Logf("  %-14s %12d %12d %10d %9d %9d %10v",
			arm.name, actions/n, rpcs/n, avail/n, add/n, churn/n, (p50 / time.Duration(n)).Round(time.Millisecond))
	}
}
