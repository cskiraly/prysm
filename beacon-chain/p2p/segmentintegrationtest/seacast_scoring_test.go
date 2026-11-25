package segmentintegrationtest

import (
	"math"
	"os"
	"testing"
	"testing/synctest"
	"time"

	"github.com/OffchainLabs/prysm/v7/beacon-chain/verification/segmentauth"
	"github.com/OffchainLabs/prysm/v7/config/params"
	"github.com/OffchainLabs/prysm/v7/testing/require"
	"github.com/libp2p/go-libp2p/core/peer"
)

// TestSeacastPayloadPathScored is TestSeacastPayloadPath's path with the node's peer scoring. From the meshes' forming to the heartbeat after the IWANT follow-up time of
// the last ask, no node scores any peer below zero, no node charges a broken promise, and every
// receiver still completes the group. One slot's score can't show broken promises: prysm's behaviour
// penalty starts above 6, and a charge decays to 0.01 over ten epochs, so one broken promise a slot
// from a peer settles near 70 and graylists it. SEACAST_TEST_REJECT_ONE_INDEX=1 makes node 5 reject
// one index from every sender, which must drive a sender's score below zero: the control that the
// test sees a negative score.
func TestSeacastPayloadPathScored(t *testing.T) {
	cfg := pathConfig{path: true, scored: true}
	if os.Getenv("SEACAST_TEST_REJECT_ONE_INDEX") != "" {
		cfg.rejectAt, cfg.rejectIndex = 5, 0
	}
	synctest.Test(t, func(t *testing.T) {
		params.SetupTestConfigCleanup(t)
		nodes, topic, stop := pathNetwork(t, cfg)
		defer stop()
		name := make(map[peer.ID]int, len(nodes))
		for i, n := range nodes {
			name[n.self] = i
		}

		segs, err := segmentauth.SegmentMessagesForEnvelope(pathEnvelope(t, 128, 4096), segmentauth.DefaultParams())
		require.NoError(t, err)
		publishGroup(t, nodes[0], topic, segs)
		// A broken promise is charged at the first heartbeat after the ask's follow-up time.
		time.Sleep(pathSettle + pathGossipParams().IWantFollowupTime + 2*pathHeartbeat)
		synctest.Wait()

		for i, n := range nodes {
			charged, outstanding := n.ps.BrokenPromises()
			n.scoreMu.Lock()
			lowest, by := math.Inf(1), -1
			for pid, lo := range n.lowest {
				if lo < lowest {
					lowest, by = lo, name[pid]
				}
			}
			var behaviour, invalid float64
			for _, s := range n.last {
				behaviour = max(behaviour, s.BehaviourPenalty)
				if ts, ok := s.Topics[topic]; ok {
					invalid = max(invalid, ts.InvalidMessageDeliveries)
				}
			}
			scored := len(n.lowest)
			n.scoreMu.Unlock()
			var brokenCharged, brokenOutstanding int
			for _, c := range charged {
				brokenCharged += c
			}
			for _, c := range outstanding {
				brokenOutstanding += c
			}
			t.Logf("node %d: %d peers scored, lowest score %.2f (of node %d); highest behaviour penalty %.2f, invalid deliveries %.2f; broken promises charged %d, outstanding %d; completed %d",
				i, scored, lowest, by, behaviour, invalid, brokenCharged, brokenOutstanding, n.m.Seacast.GroupsCompleted.Load())
			require.Equal(t, true, scored > 0, "node %d scored no peer", i)
			require.Equal(t, true, lowest >= 0, "node %d scored node %d at %.2f", i, by, lowest)
			require.Equal(t, 0, brokenCharged, "node %d charged broken promises", i)
		}
		for i, n := range nodes[1:] {
			require.Equal(t, int64(1), n.m.Seacast.GroupsCompleted.Load(), "node %d did not complete the group", i+1)
		}
	})
}
