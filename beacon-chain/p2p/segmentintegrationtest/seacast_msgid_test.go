package segmentintegrationtest

import (
	"os"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/OffchainLabs/prysm/v7/beacon-chain/p2p"
	"github.com/OffchainLabs/prysm/v7/beacon-chain/verification/segmentauth"
	"github.com/OffchainLabs/prysm/v7/config/params"
	"github.com/OffchainLabs/prysm/v7/testing/require"
	pubsubpb "github.com/libp2p/go-libp2p-pubsub/pb"
)

// idCounter wraps the node's message-id function and counts its calls per message object. It keeps
// every message it saw alive, which a test can afford.
type idCounter struct {
	mu    sync.Mutex
	calls map[*pubsubpb.Message]int
}

func newIDCounter() *idCounter { return &idCounter{calls: map[*pubsubpb.Message]int{}} }

func (c *idCounter) id(m *pubsubpb.Message) string {
	c.mu.Lock()
	c.calls[m]++
	c.mu.Unlock()
	return p2p.MsgID(m)
}

// most returns the most calls any one message had, the messages, and the calls in all.
func (c *idCounter) most() (int, int, int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	most, all := 0, 0
	for _, n := range c.calls {
		most, all = max(most, n), all+n
	}
	return most, len(c.calls), all
}

// TestSeacastMsgIDOncePerMessage: on the payload path with the node's router
// options, the id function runs at most once for each message object a node holds, received or
// published. Prysm's segment id snappy-decodes and hashes the segment, and the router's uncached
// RawID (the outbound queue's HAVE index, the IDONTWANT filters, the queue's order) called it again
// for every use, which held 50 to 66 % of a node's allocations on a devnet.
func TestSeacastMsgIDOncePerMessage(t *testing.T) {
	ids := newIDCounter()
	cfg := pathConfig{path: os.Getenv("SEACAST_TEST_WITHOUT_PATH_OPTIONS") == "", ids: ids}
	synctest.Test(t, func(t *testing.T) {
		params.SetupTestConfigCleanup(t)
		nodes, topic, stop := pathNetwork(t, cfg)
		defer stop()

		env := pathEnvelope(t, 128, 4096)
		segs, err := segmentauth.SegmentMessagesForEnvelope(env, segmentauth.DefaultParams())
		require.NoError(t, err)
		group, count := string(segs[0].Group), int(segs[0].Descriptor.Count) // lint:ignore uintcast -- bounded by segments.MaxSegments.

		publishGroup(t, nodes[0], topic, segs)
		time.Sleep(pathSettle)
		synctest.Wait()

		for i, n := range nodes[1:] {
			_, held := heldEnvelope(t, n, group, count)
			require.Equal(t, count, held, "node %d holds %d of %d", i+1, held, count)
		}
		most, msgs, all := ids.most()
		t.Logf("the id function ran %d times on %d message objects over %d nodes; at most %d times on one",
			all, msgs, len(nodes), most)
		require.Equal(t, true, msgs > 0, "the id function never ran")
		require.Equal(t, 1, most, "a message's id was computed %d times", most)
	})
}
