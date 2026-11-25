package sync

import (
	"fmt"
	"testing"

	p2ptest "github.com/OffchainLabs/prysm/v7/beacon-chain/p2p/testing"
	"github.com/OffchainLabs/prysm/v7/config/features"
	fieldparams "github.com/OffchainLabs/prysm/v7/config/fieldparams"
	"github.com/OffchainLabs/prysm/v7/consensus-types/blocks"
	"github.com/OffchainLabs/prysm/v7/consensus-types/interfaces"
	enginev1 "github.com/OffchainLabs/prysm/v7/proto/engine/v1"
	ethpb "github.com/OffchainLabs/prysm/v7/proto/prysm/v1alpha1"
	"github.com/OffchainLabs/prysm/v7/testing/require"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"
)

// TestPendingEnvelopeRebroadcast: the pending queue rebroadcasts an envelope whole once it imports
// it, and with --seacast-payload-only it does not, since the whole envelope is what the switch
// keeps off the network.
func TestPendingEnvelopeRebroadcast(t *testing.T) {
	notSeen := errors.New("block not seen")
	for _, only := range []bool{false, true} {
		t.Run(fmt.Sprintf("payload-only=%v", only), func(t *testing.T) {
			reset := features.InitWithReset(&features.Flags{EnableSegmentedPayloadGossip: true, SeacastPayloadOnly: only})
			defer reset()
			n := newSegmentNode(t, 1, mockExecutionPayloadEnvelopeVerifier{errBlockRootSeen: notSeen})
			env, root := n.block(t, 1)
			n.chain.ForkchoiceRoots = map[[32]byte]bool{}
			n.deliver(t, env)
			require.Equal(t, true, proto.Equal(env, n.queued(env)), "not queued")
			tp, ok := n.p.(*p2ptest.TestP2P)
			require.Equal(t, true, ok)
			tp.BroadcastCalled.Store(false)
			n.s.processPendingPayloadEnvelope(t.Context(), root)
			require.Equal(t, 1, n.imported(env))
			require.Equal(t, !only, tp.BroadcastCalled.Load(), "whole envelope rebroadcast")
		})
	}
}

// pqEnvelopeOf is env in the PQ fork's type: the same payload, block and builder, the PQ fork's
// execution requests and a leanXMSS signature.
func pqEnvelopeOf(env *ethpb.SignedExecutionPayloadEnvelope) *ethpb.SignedExecutionPayloadEnvelopePQ {
	return &ethpb.SignedExecutionPayloadEnvelopePQ{
		Message: &ethpb.ExecutionPayloadEnvelopePQ{
			Payload:               env.Message.Payload,
			ExecutionRequests:     &enginev1.ExecutionRequestsPQ{},
			BuilderIndex:          env.Message.BuilderIndex,
			BeaconBlockRoot:       env.Message.BeaconBlockRoot,
			ParentBeaconBlockRoot: env.Message.ParentBeaconBlockRoot,
		},
		Signature: make([]byte, fieldparams.XMSSSignatureLength),
	}
}

// TestPendingPQEnvelopeRebroadcast: the segment path carries PQ envelopes as it carries Gloas ones,
// so the pending queue treats them alike: it rebroadcasts a PQ envelope whole once it imports it,
// and with --seacast-payload-only it does not.
func TestPendingPQEnvelopeRebroadcast(t *testing.T) {
	for _, only := range []bool{false, true} {
		t.Run(fmt.Sprintf("payload-only=%v", only), func(t *testing.T) {
			reset := features.InitWithReset(&features.Flags{EnableSegmentedPayloadGossip: true, SeacastPayloadOnly: only})
			defer reset()
			n := newSegmentNode(t, 1, mockExecutionPayloadEnvelopeVerifier{})
			env, root := n.block(t, 1)
			e, err := blocks.WrappedROSignedExecutionPayloadEnvelopePQ(pqEnvelopeOf(env))
			require.NoError(t, err)
			n.s.pendingEnvelopeLock.Lock()
			n.s.pendingPayloadEnvelopes[root] = map[uint64]interfaces.ROSignedExecutionPayloadEnvelope{uint64(env.Message.BuilderIndex): e}
			n.s.pendingEnvelopeLock.Unlock()
			tp, ok := n.p.(*p2ptest.TestP2P)
			require.Equal(t, true, ok)
			tp.BroadcastCalled.Store(false)
			n.s.processPendingPayloadEnvelope(t.Context(), root)
			require.Equal(t, 1, len(n.chain.PayloadEnvelopesReceived), "the PQ envelope was not imported")
			require.Equal(t, !only, tp.BroadcastCalled.Load(), "whole PQ envelope rebroadcast")
		})
	}
}
