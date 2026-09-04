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
