package p2p

import (
	"testing"

	"github.com/OffchainLabs/prysm/v7/beacon-chain/p2p/internal/segmentgossip"
	"github.com/OffchainLabs/prysm/v7/config/features"
	"github.com/OffchainLabs/prysm/v7/testing/require"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
)

func TestSegmentGossipParams(t *testing.T) {
	stock := pubsubGossipParam()
	require.Equal(t, pubsub.DefaultGossipSubParams().MaxIHaveMessages, stock.MaxIHaveMessages)

	reset := features.InitWithReset(&features.Flags{EnableSegmentedPayloadGossip: true})
	defer reset()
	on := pubsubGossipParam()
	require.Equal(t, segmentgossip.MaxIHaveMessages, on.MaxIHaveMessages)
}

// TestSegmentGroupCompleteFeedsGate checks the seam between the sync layer and the router:
// a completed root reported to the service is what the segment topic's pull gate declines,
// and with the feature off the call is inert.
func TestSegmentGroupCompleteFeedsGate(t *testing.T) {
	var root [32]byte
	root[5] = 0xaa
	id := make([]byte, 0, segmentgossip.MessageIDLen)
	id = append(id, root[:]...)
	id = append(id, 7, 0, 0, 0)
	id = append(id, make([]byte, 20)...)
	mid := string(id)

	t.Run("marks the gate", func(t *testing.T) {
		gate := segmentgossip.NewPullGate()
		s := &Service{segmentPullGate: gate}
		require.Equal(t, true, gate.Allow("", "", mid))
		s.SegmentGroupComplete(root)
		require.Equal(t, false, gate.Allow("", "", mid))
	})

	t.Run("no gate, no effect", func(t *testing.T) {
		s := &Service{}
		s.SegmentGroupComplete(root)
	})

	t.Run("the feature installs the gate", func(t *testing.T) {
		reset := features.InitWithReset(&features.Flags{EnableSegmentedPayloadGossip: true})
		defer reset()
		s := &Service{cfg: &Config{}}
		_ = s.pubsubOptions()
		require.NotNil(t, s.segmentPullGate)
	})
}
