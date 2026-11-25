package sync

import (
	"fmt"
	"sync"
	"testing"
	"time"

	mock "github.com/OffchainLabs/prysm/v7/beacon-chain/blockchain/testing"
	"github.com/OffchainLabs/prysm/v7/beacon-chain/p2p"
	"github.com/OffchainLabs/prysm/v7/beacon-chain/p2p/peers"
	p2ptest "github.com/OffchainLabs/prysm/v7/beacon-chain/p2p/testing"
	p2ptypes "github.com/OffchainLabs/prysm/v7/beacon-chain/p2p/types"
	lruwrpr "github.com/OffchainLabs/prysm/v7/cache/lru"
	"github.com/OffchainLabs/prysm/v7/config/params"
	ethpb "github.com/OffchainLabs/prysm/v7/proto/prysm/v1alpha1"
	"github.com/OffchainLabs/prysm/v7/testing/assert"
	"github.com/OffchainLabs/prysm/v7/testing/require"
	"github.com/OffchainLabs/prysm/v7/testing/util"
	"github.com/libp2p/go-libp2p/core/network"
)

// TestEnvelopesFetchedByRootCounted: an envelope received by root over req/resp is counted, so a
// reading can tell the envelopes the node fetched from those that reached it by gossip or segments.
func TestEnvelopesFetchedByRootCounted(t *testing.T) {
	params.SetupTestConfigCleanup(t)
	cfg := params.BeaconConfig()
	cfg.FuluForkEpoch = 0
	cfg.GloasForkEpoch = 0
	params.OverrideBeaconConfig(cfg)
	params.BeaconConfig().InitializeForkSchedule()

	p1, p2 := p2ptest.NewTestP2P(t), p2ptest.NewTestP2P(t)
	p1.Connect(p2)
	r := newEnvelopeFetchService(t, p1)
	root := [32]byte{0xAB}
	env := makeSignedEnvelope(root, 1)

	protocolID := fmt.Sprintf("%s/ssz_snappy", p2p.RPCExecutionPayloadEnvelopesByRootTopicV1)
	var wg sync.WaitGroup
	wg.Add(1)
	p2.SetStreamHandler(protocolID, func(stream network.Stream) {
		defer wg.Done()
		req := new(p2ptypes.ExecutionPayloadEnvelopesByRootReq)
		assert.NoError(t, p2.Encoding().DecodeWithMaxLength(stream, req))
		assert.NoError(t, WriteExecutionPayloadEnvelopeChunk(stream, p2.Encoding(), env))
		assert.NoError(t, stream.CloseWrite())
	})

	before := counterValue(t, envelopesFetchedByRootCounter)
	r.fetchAndQueuePayloadEnvelopesForRoots(t.Context(), p2.PeerID(), p2ptypes.BeaconBlockByRootsReq{root})
	if util.WaitTimeout(&wg, time.Second) {
		t.Fatal("Did not receive envelope-by-root request within 1 sec")
	}
	require.Equal(t, before+1, counterValue(t, envelopesFetchedByRootCounter), "the pending blocks' fetch")

	// The late-head and child-block recovery (fetchPayloadEnvelope) asks a best peer and receives
	// the envelope straight into the chain; it is counted too.
	chain, ok := r.cfg.chain.(*mock.ChainService)
	require.Equal(t, true, ok)
	chain.FinalizedCheckPoint = &ethpb.Checkpoint{}
	p1.Peers().SetConnectionState(p2.PeerID(), peers.Connected)
	p1.Peers().SetChainState(p2.PeerID(), &ethpb.StatusV2{})
	r.ctx = t.Context()
	r.badPayloadCache = lruwrpr.New(10)
	wg.Add(1)
	r.fetchPayloadEnvelope(root)
	if util.WaitTimeout(&wg, time.Second) {
		t.Fatal("Did not receive envelope-by-root request within 1 sec")
	}
	require.Equal(t, true, chain.ReceivePayloadEnvelopeCtxHadDeadline, "the envelope reached the chain")
	require.Equal(t, before+2, counterValue(t, envelopesFetchedByRootCounter), "the late-head recovery")
}
