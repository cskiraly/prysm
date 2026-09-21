package segmentintegrationtest

// The in-process harness's answer for the Shadow known-answer cell: one whole message over one
// link, the cell's payload and links, so the Shadow run has an exact number to agree with rather
// than an arithmetic estimate. SHADOW_KNOWN_ANSWER=1 runs it; the SEGMENT_* variables select the
// payload size and the uplink as in every other cell.

import (
	"os"
	"testing"
	"testing/synctest"
	"time"

	"github.com/OffchainLabs/prysm/v7/config/params"
)

func TestShadowKnownAnswer(t *testing.T) {
	if os.Getenv("SHADOW_KNOWN_ANSWER") == "" {
		t.Skip("SHADOW_KNOWN_ANSWER=1 prints the harness's one-link whole-message completion")
	}
	params.SetupTestConfigCleanup(t)
	cell := q6CellFromEnv(t)
	rate := envMbps("SEGMENT_UP_MBPS", defaultRate)
	for _, oneWay := range []time.Duration{defaultLatency} {
		synctest.Test(t, func(t *testing.T) {
			params.SetupTestConfigCleanup(t)
			complete, firstFwd, drops := runLineAtLatency(t, 1, cell.whole, nil, oneWay, false)
			wire := time.Duration(float64(cell.whole.total*8) / float64(rate) * float64(time.Second))
			t.Logf("known answer: whole %d B over one link at %d Mbps, %v one-way: complete %v (wire %v + latency %v = %v), first forward %v, drops %d",
				cell.whole.total, rate/1_000_000, oneWay, complete.Round(time.Microsecond),
				wire.Round(time.Microsecond), oneWay, (wire + oneWay).Round(time.Microsecond), firstFwd, drops)
		})
	}
}
