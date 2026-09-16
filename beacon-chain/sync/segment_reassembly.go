package sync

import (
	"github.com/OffchainLabs/prysm/v7/beacon-chain/p2p/segmentbroadcaster"
	"github.com/OffchainLabs/prysm/v7/beacon-chain/verification/segmentauth"
	"github.com/OffchainLabs/prysm/v7/config/features"
	leakybucket "github.com/OffchainLabs/prysm/v7/container/leaky-bucket"
	"github.com/OffchainLabs/prysm/v7/container/segments"
	ethpb "github.com/OffchainLabs/prysm/v7/proto/prysm/v1alpha1"
	"github.com/pkg/errors"
)

// initSegmentReassembly prepares whichever segmented-gossip variant is selected.
//
// The two variants receive segments through different substrates and so own their reassembly
// separately: variant A's arrives through a topic validator here, variant B's arrives through
// the partial-messages extension in the p2p layer. What they share is the authenticator,
// because a descriptor is authenticated against the builder registry either way and only this
// service can reach the head state.
func (s *Service) initSegmentReassembly() error {
	switch features.Get().SegmentedPayloadGossip {
	case features.SegmentedPayloadMessages:
		return s.initSegmentGossipMessages()
	case features.SegmentedPayloadPartial:
		return s.initSegmentPartialMessages()
	default:
		// A nil segmentReassembler is what the validator checks to decide the feature is off,
		// so nothing is allocated and no topic behaviour changes.
		return nil
	}
}

// initSegmentGossipMessages sets up variant A: segments as ordinary gossip messages.
func (s *Service) initSegmentGossipMessages() error {
	r, err := segments.NewReassembler(segments.ReassemblerConfig{Auth: s.segmentAuthenticator()})
	if err != nil {
		return err
	}
	s.segmentReassembler = r
	// Bounds the only expensive step a peer can trigger: opening a group costs a signature
	// verification, and a peer that never opens one is never charged. deleteEmptyBuckets is
	// set so a churn of short-lived peers does not accumulate buckets.
	s.segmentAuthLimiter = leakybucket.NewCollector(
		authAttemptRefillPerSecond, authAttemptsPerPeer, authAttemptPrunePeriod, true)
	return nil
}

// initSegmentPartialMessages sets up variant B: segments as partial-message parts.
//
// The broadcaster was built in the p2p layer, because that is where the pubsub options are
// assembled; it does no work until it is given an authenticator and somewhere to deliver a
// completed envelope, which is what happens here.
func (s *Service) initSegmentPartialMessages() error {
	b := s.cfg.p2p.SegmentBroadcaster()
	if b == nil {
		return errors.New("segmented payload gossip is in partial mode but p2p installed no broadcaster")
	}
	callbacks := segmentbroadcaster.Callbacks{
		Auth:       s.segmentAuthenticator(),
		OnEnvelope: s.onReassembledEnvelope,
	}
	// Start blocks: it is the broadcaster's loop.
	s.spawn(func() {
		if err := b.Start(callbacks); err != nil {
			log.WithError(err).Error("Segment broadcaster stopped")
		}
	})
	return nil
}

// segmentAuthenticator builds the descriptor authenticator both variants use.
//
// Interim: until ExecutionPayloadBid carries the segment commitment there is no commitment set
// to test against, so the first group offered in a slot is the one that slot admits. The key is
// this node's own clock, so a peer cannot choose it or flood it. It bounds groups and
// establishes nothing about who published them -- see segmentauth.FirstSeenAuthenticator for
// the full statement and why it must not ship as the real scheme.
func (s *Service) segmentAuthenticator() segments.Authenticator {
	return segmentauth.NewFirstSeen(s.cfg.clock.CurrentSlot)
}

// onReassembledEnvelope hands a variant B reassembly to the same queue variant A uses.
//
// Deliberately the same destination. Reassembly proves the bytes match the group pinned for
// the anchored block; it does not run the envelope gossip ladder (block availability, slot, builder
// index, payload hash, execution requests root), so treating it as validated would bypass
// those checks. The pending queue revalidates fully and only processes once the block is in
// forkchoice.
func (s *Service) onReassembledEnvelope(encoded []byte) error {
	envelope := &ethpb.SignedExecutionPayloadEnvelope{}
	if err := envelope.UnmarshalSSZ(encoded); err != nil {
		// The bytes match the group pinned for this root, so a decode failure is the
		// publisher's doing, not the relay's that delivered the last segment.
		return errors.Wrap(err, "reassembled bytes are not an execution payload envelope")
	}
	segmentReassembledCounter.Inc()
	s.queuePendingPayloadEnvelopeFromRootRequest(envelope)
	return nil
}
