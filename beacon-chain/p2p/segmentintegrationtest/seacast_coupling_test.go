package segmentintegrationtest

import (
	"context"
	"math/rand/v2"
	"slices"
	"testing"
	"testing/synctest"
	"time"

	"github.com/OffchainLabs/prysm/v7/beacon-chain/verification/segmentauth"
	"github.com/OffchainLabs/prysm/v7/config/params"
	"github.com/OffchainLabs/prysm/v7/testing/require"
)

// couplingBlockBytes is the block's size: an incompressible 100 KB, about a full beacon block's.
const couplingBlockBytes = 100_000

// TestSeacastBlockBesidePayload reports what the payload's path does to the block topic beside it,
// on TestSeacastPayloadPath's network. The source publishes a block alone, or a payload's segments
// and then a block at the same instant, with the path's options or on stock gossipsub. For each
// arm it logs every node's block arrival and the arrival of its K-th segment, from the publish.
// It requires only that every node gets the block and, with a payload, at least K of its segments;
// the times are a reading, not a bound.
func TestSeacastBlockBesidePayload(t *testing.T) {
	for _, arm := range []struct {
		name    string
		path    bool
		payload bool
	}{
		{name: "path, block alone", path: true},
		{name: "path, block with the payload", path: true, payload: true},
		{name: "stock, block with the payload", payload: true},
	} {
		t.Run(arm.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				params.SetupTestConfigCleanup(t)
				nodes, topic, stop := pathNetwork(t, pathConfig{path: arm.path, block: true})
				defer stop()

				env := pathEnvelope(t, 128, 4096)
				segs, err := segmentauth.SegmentMessagesForEnvelope(env, segmentauth.DefaultParams())
				require.NoError(t, err)
				k := int(segs[0].Descriptor.Required()) // lint:ignore uintcast -- bounded by segments.MaxSegments.
				block := make([]byte, couplingBlockBytes)
				r := rand.New(rand.NewPCG(7, 11))
				for i := range block {
					block[i] = byte(r.Uint32())
				}

				start := time.Now()
				if arm.payload {
					publishGroup(t, nodes[0], topic, segs)
				}
				require.NoError(t, nodes[0].blockTopic.Publish(context.Background(), block))
				time.Sleep(pathSettle)
				synctest.Wait()

				var blockMs, kthMs []float64
				for i, n := range nodes[1:] {
					at, ok := n.firstBlockAt()
					require.Equal(t, true, ok, "node %d never got the block", i+1)
					b := float64(at.Sub(start).Microseconds()) / 1000
					blockMs = append(blockMs, b)
					if !arm.payload {
						t.Logf("node %d: block at %.1f ms", i+1, b)
						continue
					}
					kat, ok := n.kthSegmentAt(k)
					require.Equal(t, true, ok, "node %d got fewer than K = %d segments", i+1, k)
					s := float64(kat.Sub(start).Microseconds()) / 1000
					kthMs = append(kthMs, s)
					t.Logf("node %d: block at %.1f ms, K-th segment at %.1f ms", i+1, b, s)
				}
				t.Logf("%s: block median %.1f ms, max %.1f ms", arm.name, median(blockMs), slices.Max(blockMs))
				if arm.payload {
					t.Logf("%s: K-th segment median %.1f ms, max %.1f ms (K %d of %d)",
						arm.name, median(kthMs), slices.Max(kthMs), k, len(segs))
				}
			})
		})
	}
}

func median(xs []float64) float64 {
	s := slices.Clone(xs)
	slices.Sort(s)
	if len(s)%2 == 1 {
		return s[len(s)/2]
	}
	return (s[len(s)/2-1] + s[len(s)/2]) / 2
}
