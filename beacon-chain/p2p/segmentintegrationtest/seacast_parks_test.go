package segmentintegrationtest

import (
	"bytes"
	"os"
	"testing"
	"testing/synctest"
	"time"

	"github.com/OffchainLabs/prysm/v7/beacon-chain/verification/segmentauth"
	"github.com/OffchainLabs/prysm/v7/config/params"
	"github.com/OffchainLabs/prysm/v7/testing/require"
)

// TestSeacastPromiseAtWrite: with the node's router options, an ask's promise
// starts when the ask is written, as the spec says: "A promise starts at the write, not at the
// decision to ask, so that no peer is parked for an ask it never received" (seacast-lite.md §6.1). Four
// sources publish a group each at once on the path, every node honest; every node completes every
// group, and the routers opened their promises at the write. Started at the decision instead, an ask
// waiting behind the window broke its promise and parked an honest peer: on this path, over ten runs,
// 55 parks, some in every run, against 3 in three runs from the write. Before the fork stopped write
// reports from reopening a promise after its ask had closed, the write clock left 10 parks in five
// runs of another ten (TestPromiseAtWriteAfterClose isolates that mechanism; the runs differ, and the
// id cache's lock changed between them). What delays the answers behind the 3 left isn't read, so the
// parks are logged, not held to zero.
func TestSeacastPromiseAtWrite(t *testing.T) {
	cfg := pathConfig{path: os.Getenv("SEACAST_TEST_WITHOUT_PATH_OPTIONS") == ""}
	const sources = 4
	synctest.Test(t, func(t *testing.T) {
		params.SetupTestConfigCleanup(t)
		nodes, topic, stop := pathNetwork(t, cfg)
		defer stop()

		type group struct {
			root  string
			count int
			want  []byte
		}
		groups := make([]group, sources)
		for i := range groups {
			env := pathEnvelope(t, 128+i, 4096) // a distinct envelope per source
			want, err := env.MarshalSSZ()
			require.NoError(t, err)
			segs, err := segmentauth.SegmentMessagesForEnvelope(env, segmentauth.DefaultParams())
			require.NoError(t, err)
			groups[i] = group{string(segs[0].Group), int(segs[0].Descriptor.Count), want} // lint:ignore uintcast -- bounded by segments.MaxSegments.
			publishGroup(t, nodes[i], topic, segs)
		}
		time.Sleep(pathSettle)
		synctest.Wait()

		var parks, breaks, atWrite, lastResort, parkedAsked, unpushed, owedSent int64
		for i, n := range nodes {
			parks += n.m.Parks.Load()
			breaks += n.m.ParkBreaks.Load()
			atWrite += n.m.V05.PromisesAtWrite.Load()
			lastResort += n.m.V05.LastResortAsks.Load()
			parkedAsked += n.m.V05.ParkedAsked.Load()
			unpushed += n.m.V05.Unpushed.Load()
			owedSent += n.m.V05.OwedSent.Load()
			for j, g := range groups {
				if j == i {
					continue
				}
				got, held := heldEnvelope(t, n, g.root, g.count)
				require.Equal(t, g.count, held, "node %d holds %d of group %d's %d", i, held, j, g.count)
				require.Equal(t, true, bytes.Equal(g.want, got), "node %d's envelope %d is not the source's", i, j)
			}
		}
		t.Logf("%d groups at once on %d nodes: %d promises opened at the write, %d broken, %d parks; last-resort attempts %d, taken to a parked peer %d; unpushed (projected or refused) %d, owed offer indices attached %d",
			sources, len(nodes), atWrite, breaks, parks, lastResort, parkedAsked, unpushed, owedSent)
		require.Equal(t, true, atWrite > 0, "no promise opened at the write: the router starts them at the decision to ask")
	})
}
