package validator

import (
	"context"
	"testing"

	mockp2p "github.com/OffchainLabs/prysm/v7/beacon-chain/p2p/testing"
	"github.com/OffchainLabs/prysm/v7/beacon-chain/verification/segmentauth"
	"github.com/OffchainLabs/prysm/v7/config/features"
	"github.com/OffchainLabs/prysm/v7/consensus-types/primitives"
	enginev1 "github.com/OffchainLabs/prysm/v7/proto/engine/v1"
	ethpb "github.com/OffchainLabs/prysm/v7/proto/prysm/v1alpha1"
	"github.com/OffchainLabs/prysm/v7/testing/require"
	"github.com/sirupsen/logrus"
)

// segmentTestEnvelope builds a signed envelope big enough to need several segments.
func segmentTestEnvelope() *ethpb.SignedExecutionPayloadEnvelope {
	txs := make([][]byte, 40)
	for i := range txs {
		txs[i] = make([]byte, 4096)
		for j := range txs[i] {
			txs[i][j] = byte(i + j)
		}
	}
	return &ethpb.SignedExecutionPayloadEnvelope{
		Message: &ethpb.ExecutionPayloadEnvelope{
			Payload: &enginev1.ExecutionPayloadGloas{
				SlotNumber:    primitives.Slot(4096),
				Transactions:  txs,
				ParentHash:    make([]byte, 32),
				FeeRecipient:  make([]byte, 20),
				StateRoot:     make([]byte, 32),
				ReceiptsRoot:  make([]byte, 32),
				LogsBloom:     make([]byte, 256),
				PrevRandao:    make([]byte, 32),
				ExtraData:     []byte{},
				BaseFeePerGas: make([]byte, 32),
				BlockHash:     make([]byte, 32),
			},
			ExecutionRequests:     &enginev1.ExecutionRequestsGloas{},
			BuilderIndex:          primitives.BuilderIndex(11),
			BeaconBlockRoot:       make([]byte, 32),
			ParentBeaconBlockRoot: make([]byte, 32),
		},
		Signature: make([]byte, 96),
	}
}

func TestPublishEnvelopeSegments(t *testing.T) {
	signed := segmentTestEnvelope()
	log := logrus.NewEntry(logrus.New())

	t.Run("feature off publishes nothing", func(t *testing.T) {
		p := mockp2p.NewTestP2P(t)
		vs := &Server{P2P: p}
		vs.publishEnvelopeSegments(context.Background(), log, signed)
		require.Equal(t, false, p.BroadcastCalled.Load())
	})

	t.Run("feature on broadcasts every segment of the default segmentation", func(t *testing.T) {
		reset := features.InitWithReset(&features.Flags{EnableSegmentedPayloadGossip: true})
		defer reset()
		p := mockp2p.NewTestP2P(t)
		vs := &Server{P2P: p}
		vs.publishEnvelopeSegments(context.Background(), log, signed)
		require.Equal(t, true, p.BroadcastCalled.Load())
		want, err := segmentauth.SegmentMessagesForEnvelope(signed, segmentauth.DefaultParams())
		require.NoError(t, err)
		require.Equal(t, true, len(want) > 1, "test envelope should need several segments")
		require.Equal(t, len(want), len(p.BroadcastedSegments()))
	})
}
