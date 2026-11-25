//go:build minimal

package validator

import (
	"fmt"
	"testing"
	"time"

	"github.com/OffchainLabs/prysm/v7/beacon-chain/p2p"
	mockp2p "github.com/OffchainLabs/prysm/v7/beacon-chain/p2p/testing"
	"github.com/OffchainLabs/prysm/v7/beacon-chain/verification/segmentauth"
	"github.com/OffchainLabs/prysm/v7/config/features"
	fieldparams "github.com/OffchainLabs/prysm/v7/config/fieldparams"
	"github.com/OffchainLabs/prysm/v7/config/params"
	"github.com/OffchainLabs/prysm/v7/encoding/bytesutil"
	enginev1 "github.com/OffchainLabs/prysm/v7/proto/engine/v1"
	ethpb "github.com/OffchainLabs/prysm/v7/proto/prysm/v1alpha1"
	"github.com/OffchainLabs/prysm/v7/testing/require"
	"github.com/pkg/errors"
	dto "github.com/prometheus/client_model/go"
	"github.com/sirupsen/logrus"
	logTest "github.com/sirupsen/logrus/hooks/test"
)

func fallbackCount(t *testing.T, reason string) float64 {
	t.Helper()
	m := &dto.Metric{}
	require.NoError(t, envelopeFallbackCounter.WithLabelValues(reason).Write(m))
	return m.GetCounter().GetValue()
}

// TestPublishPQEnvelopeSegments: the PQ publish call sends a PQ envelope as the Gloas call sends a
// Gloas one. With --seacast-payload-only it sends the segments and not the whole envelope, and falls
// back to the whole envelope, counted under the reason, when the segments cannot go out. Without
// payload-only it sends the whole envelope and the segments beside it.
func TestPublishPQEnvelopeSegments(t *testing.T) {
	params.SetupTestConfigCleanup(t)
	cfg := params.BeaconConfig().Copy()
	cfg.GloasForkEpoch = 0
	cfg.PQForkEpoch = 0
	params.OverrideBeaconConfig(cfg)
	envelope := func() *ethpb.SignedExecutionPayloadEnvelopePQ {
		g := segmentTestEnvelope(t)
		return &ethpb.SignedExecutionPayloadEnvelopePQ{
			Message: &ethpb.ExecutionPayloadEnvelopePQ{
				Payload:               g.Message.Payload,
				ExecutionRequests:     &enginev1.ExecutionRequestsPQ{},
				BuilderIndex:          g.Message.BuilderIndex,
				BeaconBlockRoot:       g.Message.BeaconBlockRoot,
				ParentBeaconBlockRoot: g.Message.ParentBeaconBlockRoot,
			},
			Signature: make([]byte, fieldparams.XMSSSignatureLength),
		}
	}
	publish := func(t *testing.T, b *mockp2p.MockBroadcaster, signed *ethpb.SignedExecutionPayloadEnvelopePQ) {
		receiver := &mockExecutionPayloadEnvelopeReceiver{done: make(chan struct{})}
		vs := &Server{Ctx: t.Context(), P2P: b, ExecutionPayloadEnvelopeReceiver: receiver}
		require.NoError(t, vs.PublishExecutionPayloadEnvelopeContentsPQ(t.Context(), signed, nil, nil))
		waitForEnvelopeImport(t, receiver)
	}

	t.Run("payload-only: segments, not the whole envelope", func(t *testing.T) {
		reset := features.InitWithReset(&features.Flags{EnableSegmentedPayloadGossip: true, SeacastPayloadOnly: true})
		defer reset()
		b := &mockp2p.MockBroadcaster{}
		signed := envelope()
		publish(t, b, signed)
		want, err := segmentauth.SegmentMessagesForEnvelope(signed, segmentauth.DefaultParams())
		require.NoError(t, err)
		require.Equal(t, 1, b.NumSegmentBroadcasts())
		require.Equal(t, len(want), len(b.BroadcastedSegments[0]))
		require.Equal(t, 0, b.NumMessages(), "whole envelope broadcast")
	})

	t.Run("payload-only: falls back to the whole envelope", func(t *testing.T) {
		reset := features.InitWithReset(&features.Flags{EnableSegmentedPayloadGossip: true, SeacastPayloadOnly: true})
		defer reset()
		before := fallbackCount(t, fallbackNoPeers)
		b := &mockp2p.MockBroadcaster{SegmentsErr: p2p.ErrSegmentsNoPeers}
		publish(t, b, envelope())
		require.Equal(t, 1, b.NumMessages(), "whole envelope broadcasts")
		require.Equal(t, before+1, fallbackCount(t, fallbackNoPeers))
	})

	t.Run("without payload-only: the whole envelope, and the segments beside it", func(t *testing.T) {
		reset := features.InitWithReset(&features.Flags{EnableSegmentedPayloadGossip: true})
		defer reset()
		b := &mockp2p.MockBroadcaster{}
		publish(t, b, envelope())
		require.Equal(t, 1, b.NumMessages(), "whole envelope broadcasts")
		// The segments go out off the call's path.
		deadline := time.Now().Add(5 * time.Second)
		for b.NumSegmentBroadcasts() == 0 && time.Now().Before(deadline) {
			time.Sleep(10 * time.Millisecond)
		}
		require.Equal(t, 1, b.NumSegmentBroadcasts())
	})
}

// TestPublishPayloadOnly: with --seacast-payload-only the proposer sends the envelope's segments
// and not the whole envelope. Each reason the segments cannot go out sends the whole envelope,
// moves the counter under the reason and logs the slot, the block root and the reason; a whole
// envelope that also fails fails the call and moves the counter under fallback-failed.
func TestPublishPayloadOnly(t *testing.T) {
	params.SetupTestConfigCleanup(t)
	cfg := params.BeaconConfig().Copy()
	cfg.GloasForkEpoch = 0
	params.OverrideBeaconConfig(cfg)
	reset := features.InitWithReset(&features.Flags{EnableSegmentedPayloadGossip: true, SeacastPayloadOnly: true})
	defer reset()

	root := bytesutil.PadTo([]byte{0xab, 0xcd, 0xef}, 32)
	envelope := func(t *testing.T) *ethpb.SignedExecutionPayloadEnvelope {
		signed := segmentTestEnvelope(t)
		signed.Message.BeaconBlockRoot = root
		return signed
	}
	slot := envelope(t).Message.Payload.SlotNumber
	publish := func(t *testing.T, b *mockp2p.MockBroadcaster, signed *ethpb.SignedExecutionPayloadEnvelope) (*mockExecutionPayloadEnvelopeReceiver, error) {
		receiver := &mockExecutionPayloadEnvelopeReceiver{done: make(chan struct{})}
		vs := &Server{Ctx: t.Context(), P2P: b, ExecutionPayloadEnvelopeReceiver: receiver}
		_, err := vs.PublishExecutionPayloadEnvelopeV2(t.Context(), &ethpb.GenericSignedExecutionPayloadEnvelope{
			Envelope: &ethpb.GenericSignedExecutionPayloadEnvelope_Contents{
				Contents: &ethpb.SignedExecutionPayloadEnvelopeContents{SignedExecutionPayloadEnvelope: signed},
			},
		})
		return receiver, err
	}
	reasons := []string{fallbackSegmentError, fallbackNoPeers, fallbackNoEnqueue, fallbackPublishError, fallbackFailed}
	counts := func(t *testing.T) map[string]float64 {
		c := make(map[string]float64)
		for _, r := range reasons {
			c[r] = fallbackCount(t, r)
		}
		return c
	}
	// fallbackLogged requires one warning naming the slot, the block root and the reason.
	fallbackLogged := func(t *testing.T, hook *logTest.Hook, reason string) {
		t.Helper()
		var found int
		for _, e := range hook.AllEntries() {
			if e.Data["reason"] != reason {
				continue
			}
			found++
			require.Equal(t, logrus.WarnLevel, e.Level)
			require.Equal(t, slot, e.Data["slot"])
			require.Equal(t, fmt.Sprintf("%#x", root), e.Data["blockRoot"])
		}
		require.Equal(t, 1, found, "fallback log lines with reason %s", reason)
	}

	t.Run("segments, not the whole envelope", func(t *testing.T) {
		before := counts(t)
		b := &mockp2p.MockBroadcaster{}
		signed := envelope(t)
		receiver, err := publish(t, b, signed)
		require.NoError(t, err)
		want, err := segmentauth.SegmentMessagesForEnvelope(signed, segmentauth.DefaultParams())
		require.NoError(t, err)
		require.Equal(t, 1, b.NumSegmentBroadcasts())
		require.Equal(t, len(want), len(b.BroadcastedSegments[0]))
		require.Equal(t, 0, b.NumMessages(), "whole envelope broadcast")
		waitForEnvelopeImport(t, receiver)
		require.DeepEqual(t, before, counts(t))
	})

	for _, tc := range []struct {
		reason      string
		segmentsErr error
		malformed   bool
	}{
		{reason: fallbackNoPeers, segmentsErr: p2p.ErrSegmentsNoPeers},
		{reason: fallbackNoEnqueue, segmentsErr: errors.Wrap(p2p.ErrSegmentsNotEnqueued, "context deadline exceeded")},
		{reason: fallbackPublishError, segmentsErr: errors.New("could not publish payload segment batch: closed")},
		{reason: fallbackSegmentError, malformed: true},
	} {
		t.Run("falls back on "+tc.reason, func(t *testing.T) {
			hook := logTest.NewGlobal()
			before := counts(t)
			b := &mockp2p.MockBroadcaster{SegmentsErr: tc.segmentsErr}
			signed := envelope(t)
			if tc.malformed {
				// Past the envelope's wrapper, which does not check the payload's fields, but not
				// past its SSZ encoding.
				signed.Message.Payload.LogsBloom = make([]byte, 255)
			}
			receiver, err := publish(t, b, signed)
			require.NoError(t, err)
			require.Equal(t, 1, b.NumMessages(), "whole envelope broadcasts")
			if tc.malformed {
				require.Equal(t, 0, b.NumSegmentBroadcasts())
			}
			waitForEnvelopeImport(t, receiver)
			after := counts(t)
			for _, r := range reasons {
				want := before[r]
				if r == tc.reason {
					want++
				}
				require.Equal(t, want, after[r], "counter under %s", r)
			}
			fallbackLogged(t, hook, tc.reason)
		})
	}

	t.Run("a failed fallback fails the call", func(t *testing.T) {
		hook := logTest.NewGlobal()
		before := counts(t)
		b := &mockp2p.MockBroadcaster{SegmentsErr: p2p.ErrSegmentsNoPeers, BroadcastErr: errors.New("topic closed")}
		receiver, err := publish(t, b, envelope(t))
		require.ErrorContains(t, "topic closed", err)
		require.Equal(t, before[fallbackNoPeers]+1, fallbackCount(t, fallbackNoPeers))
		require.Equal(t, before[fallbackFailed]+1, fallbackCount(t, fallbackFailed))
		require.Equal(t, int32(0), receiver.calls.Load(), "imported")
		fallbackLogged(t, hook, fallbackNoPeers)
	})
}
