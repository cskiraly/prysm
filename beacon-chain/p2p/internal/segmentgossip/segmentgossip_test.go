package segmentgossip_test

import (
	"context"
	"testing"

	"github.com/OffchainLabs/prysm/v7/beacon-chain/p2p/internal/segmentgossip"
	p2ptest "github.com/OffchainLabs/prysm/v7/beacon-chain/p2p/testing"
	"github.com/OffchainLabs/prysm/v7/testing/require"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
)

// TestOptionsApply checks the options are accepted by a gossipsub router in the order given:
// the park and the offer table refuse to install without the discipline.
func TestOptionsApply(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p := p2ptest.NewTestP2P(t)
	ps, err := pubsub.NewGossipSub(ctx, p.BHost, segmentgossip.Options("execution_payload_segment")...)
	require.NoError(t, err)
	require.NotNil(t, ps)
}
