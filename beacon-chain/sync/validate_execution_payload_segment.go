package sync

import (
	"context"
	"errors"
	"time"

	"github.com/OffchainLabs/prysm/v7/beacon-chain/p2p"
	"github.com/OffchainLabs/prysm/v7/beacon-chain/verification/segmentauth"
	"github.com/OffchainLabs/prysm/v7/container/segments"
	"github.com/OffchainLabs/prysm/v7/monitoring/tracing"
	"github.com/OffchainLabs/prysm/v7/monitoring/tracing/trace"
	ethpb "github.com/OffchainLabs/prysm/v7/proto/prysm/v1alpha1"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
	"google.golang.org/protobuf/proto"
)

// Budget for descriptor authentication, which is the only expensive step a peer can trigger.
//
// Verifying a segment is hashing and costs almost nothing, but opening a group costs a BLS
// verification. A peer can fabricate a descriptor over its own Merkle tree so the cheap
// checks pass, attach a bogus signature, and never create a group -- so no group cap is ever
// reached and the signature work is unbounded. These bound it per peer instead.
const (
	// authAttemptsPerPeer is the burst a peer may spend on opening groups.
	authAttemptsPerPeer = 8
	// authAttemptRefillPerSecond is the sustained rate, chosen so a peer can open a couple
	// of groups per slot -- more than honest traffic needs, since a proposal produces one
	// group per slot.
	authAttemptRefillPerSecond = 1.0
	// authAttemptPrunePeriod is how often idle peer buckets are dropped.
	authAttemptPrunePeriod = 10 * time.Minute
)

var errSegmentedGossipDisabled = errors.New("segmented payload gossip is disabled")

// validateExecutionPayloadSegment validates one segment of a segmented execution payload.
//
// Ordering is deliberate and runs cheapest-first: decode, then the Merkle proof and length
// checks, then the per-peer authentication budget, and only then the signature. Anything a
// peer can trigger without a builder key is therefore cheap to refuse.
func (s *Service) validateExecutionPayloadSegment(ctx context.Context, pid peer.ID, msg *pubsub.Message) (pubsub.ValidationResult, error) {
	if pid == s.cfg.p2p.PeerID() {
		return pubsub.ValidationAccept, nil
	}
	if s.segmentReassembler == nil {
		return pubsub.ValidationIgnore, errSegmentedGossipDisabled
	}

	_, span := trace.StartSpan(ctx, "sync.validateExecutionPayloadSegment")
	defer span.End()

	if msg.Topic == nil {
		return pubsub.ValidationReject, p2p.ErrInvalidTopic
	}
	m, err := s.decodePubsubMessage(msg)
	if err != nil {
		tracing.AnnotateError(span, err)
		return pubsub.ValidationReject, err
	}
	pb, ok := m.(*ethpb.ExecutionPayloadSegment)
	if !ok {
		return pubsub.ValidationReject, errWrongMessage
	}

	// Malformed bytes are the peer's fault and provably invalid, so reject rather than
	// ignore: this is what feeds gossipsub's invalid-message scoring.
	seg, hasher, err := segments.UnmarshalSegmentMessage(pb.Segment)
	if err != nil {
		tracing.AnnotateError(span, err)
		return pubsub.ValidationReject, err
	}
	if err := seg.Verify(hasher); err != nil {
		tracing.AnnotateError(span, err)
		return pubsub.ValidationReject, err
	}

	// Only an unopened group costs a signature verification, so only charge for those.
	// A segment joining an open group proves itself against the already-authenticated root.
	groupID := seg.Descriptor.GroupID(hasher)
	if !s.segmentReassembler.Has(groupID) {
		if !s.spendSegmentAuthBudget(pid) {
			segmentAuthThrottledCounter.Inc()
			return pubsub.ValidationIgnore, nil
		}
	}

	complete, err := s.segmentReassembler.Add(hasher, seg)
	if err != nil {
		tracing.AnnotateError(span, err)
		return segmentAddResult(err)
	}
	if complete == nil {
		// Buffered a new segment. Accept so it propagates: it is verified and belongs to an
		// authenticated descriptor, which is the whole precondition for forwarding.
		msg.ValidatorData = pb
		return pubsub.ValidationAccept, nil
	}

	envelope := &ethpb.SignedExecutionPayloadEnvelope{}
	if err := envelope.UnmarshalSSZ(complete); err != nil {
		// The bytes hashed to an authenticated root, so a decode failure here means the
		// builder committed to something that is not an envelope. Reject: it is attributable.
		tracing.AnnotateError(span, err)
		return pubsub.ValidationReject, err
	}
	segmentReassembledCounter.Inc()
	msg.ValidatorData = envelope
	return pubsub.ValidationAccept, nil
}

// segmentAddResult maps a reassembler error onto a gossip validation result.
//
// The split is between "this peer sent something wrong" and "we cannot judge it yet or do
// not have room", because only the former should cost the peer reputation.
func segmentAddResult(err error) (pubsub.ValidationResult, error) {
	switch {
	// Our own limits, not the peer's fault.
	case errors.Is(err, segments.ErrTooManyGroups), errors.Is(err, segments.ErrBufferFull):
		return pubsub.ValidationIgnore, err
	// Transient: the commitment arrives with a block, so a segment that outruns its block is
	// not the sender's fault. Nothing in this package buffers it -- the segment is dropped and
	// must arrive again, which is why the pending-segment store is still an open item rather
	// than a claim.
	case errors.Is(err, segmentauth.ErrNotCommitted):
		return pubsub.ValidationIgnore, err
	// Under the interim first-seen authority a conflict proves only that two peers disagree.
	// If an attacker won the race it is the honest peer that conflicts, so this must never
	// cost reputation. Only a commitment-backed refusal may be attributable.
	case errors.Is(err, segmentauth.ErrFirstSeenConflict):
		return pubsub.ValidationIgnore, err
	// Everything else -- a malformed descriptor, a proof that does not verify, a descriptor
	// conflicting with the one pinned for its group -- is attributable to whoever sent it.
	default:
		return pubsub.ValidationReject, err
	}
}

// spendSegmentAuthBudget reports whether pid may spend one authentication attempt.
func (s *Service) spendSegmentAuthBudget(pid peer.ID) bool {
	if s.segmentAuthLimiter == nil {
		return true
	}
	key := pid.String()
	if s.segmentAuthLimiter.Remaining(key) <= 0 {
		return false
	}
	s.segmentAuthLimiter.Add(key, 1)
	return true
}

// executionPayloadSegmentSubscriber handles the output of segment validation.
//
// Partial segments arrive here as the segment message itself and need no work: the
// reassembler already holds them. Only a completed envelope has anything to do.
//
// A reassembled envelope is handed to the same queue that serves envelopes fetched over
// req/resp, deliberately. Reassembly proves the bytes match a builder-signed commitment; it
// does not run the envelope gossip ladder (block availability, slot, builder index, payload
// hash, execution requests root), so treating it as validated would bypass those checks.
// The pending queue revalidates fully, only processes once the block is in forkchoice, and
// re-broadcasts on the envelope topic afterwards -- which is also how peers that do not
// speak segments end up with the payload.
func (s *Service) executionPayloadSegmentSubscriber(_ context.Context, msg proto.Message) error {
	switch m := msg.(type) {
	case *ethpb.ExecutionPayloadSegment:
		return nil
	case *ethpb.SignedExecutionPayloadEnvelope:
		s.queuePendingPayloadEnvelopeFromRootRequest(m)
		return nil
	default:
		return errWrongMessage
	}
}
