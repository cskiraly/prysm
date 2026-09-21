package segmentintegrationtest

// The in-process harness on a star: one publisher, n-1 leaves, every leaf in the publisher's mesh.
// It isolates how the publisher's uplink is shared across its parallel mesh pushes -- the leaves'
// completion times are all near D x (message / rate) when the flows share fairly, and spread from
// one message time upwards when one flow at a time wins -- which is the first place the Shadow
// network and simnet can differ on a whole message. SHADOW_STAR=1 runs it; SHADOW_EDGES=star runs
// the same cell under Shadow.

import (
	"context"
	"os"
	"sort"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/OffchainLabs/prysm/v7/config/params"
	"github.com/OffchainLabs/prysm/v7/testing/require"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/marcopolo/simnet"
)

func TestShadowStar(t *testing.T) {
	if os.Getenv("SHADOW_STAR") == "" {
		t.Skip("SHADOW_STAR=1 runs the whole-message star cell in the in-process harness")
	}
	params.SetupTestConfigCleanup(t)
	cell := q6CellFromEnv(t)
	require.Equal(t, 1, len(cell.arms), "one arm")
	n := shadowN(t)
	v := cell.variant(t, cell.arms[0], envMbps("SEGMENT_UP_MBPS", defaultRate), defaultLatency, n)
	synctest.Test(t, func(t *testing.T) {
		params.SetupTestConfigCleanup(t)
		tracers := make([]*recordingTracer, n)
		for i := range tracers {
			tracers[i] = newRecordingTracer()
		}
		nw, stop := newSimNetwork(t, networkConfig{
			links:   meshLinks(t, n, defaultRate),
			edges:   star(n),
			latency: simnet.StaticLatency(defaultLatency),
			perNodeOpts: func(i int) []pubsub.Option {
				opts := []pubsub.Option{pubsub.WithRawTracer(tracers[i])}
				opts = append(opts, structuredIDOpts()...)
				return append(opts, v.pubsubOpts(t, i)...)
			},
		})
		defer stop()
		topics, subs, cancelSubs := joinAndSubscribeAll(t, nw, false)
		defer cancelSubs()
		// A leaf's mesh is the publisher alone, below Dlo, so the mesh cannot be awaited; the
		// join's formation sleep has run, and the publisher's mesh is what the cell is about.
		t.Logf("publisher mesh %d of %d leaves", tracers[0].MeshPeers(), n-1)
		ctx, cancel := context.WithTimeout(context.Background(), networkOpTimeout)
		defer cancel()
		var wg sync.WaitGroup
		times := make([]time.Duration, n)
		start := time.Now()
		for i := 1; i < n; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				if v.watchNode(ctx, idx, nw.Pubsubs[idx], subs[idx]) {
					times[idx] = time.Since(start)
				}
			}(i)
		}
		require.NoError(t, v.publishFrom(ctx, topics[0], nw.Pubsubs[0], failPublishOmit(t, len(v.arm.msgs))))
		wg.Wait()
		sorted := append([]time.Duration(nil), times[1:]...)
		sort.Slice(sorted, func(a, b int) bool { return sorted[a] < sorted[b] })
		var text string
		for _, d := range sorted {
			text += " " + d.Round(time.Millisecond).String()
		}
		_, _, _, tx := tracers[0].Counts()
		t.Logf("star n=%d %s: leaf completion times%s; publisher tx %d B", n, v.arm.name, text, tx)
	})
}
