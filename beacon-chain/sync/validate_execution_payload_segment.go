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

// Budget on opening groups, the one step a peer can trigger that costs more than a hash.
//
// Verifying a segment is hashing and costs almost nothing. Opening a group asks the
// authenticator whether the descriptor is legitimate and, if it is, takes a slot in the
// reassembler; under the interim first-seen authority the question is a table lookup, and
// once the bid carries the commitment it is a lookup against the committed set. What the
// budget bounds is how many groups a peer can try to open, so a peer fabricating descriptors
// over its own Merkle trees cannot spend the reassembler's group cap or the authenticator's
// attention faster than this.
const (
	// authAttemptsPerPeer is the burst a peer may spend on opening groups.
	authAttemptsPerPeer = 8
	// authAttemptRefillPerSecond is the sustained rate, chosen so a peer can open a couple
	// of groups per slot -- more than honest traffic needs, since a proposal produces one
	// group per slot.
	authAttemptRefillPerSecond = 1.0
	// authAttemptPeriod is the period the refill rate is expressed in. The collector leaks
	// rate tokens per period, so this must be one second for the rate above to mean per
	// second; it also sets how often the collector drops idle peer buckets.
	authAttemptPeriod = time.Second
)

var errSegmentedGossipDisabled = errors.New("segmented payload gossip is disabled")

// validateExecutionPayloadSegment validates one segment of a segmented execution payload.
//
// Ordering is deliberate and runs cheapest-first: decode, then the per-peer budget when the
// segment would open a group, then the Merkle proof, the length checks and the descriptor
// authentication, all inside the reassembler's Add so a segment is verified exactly once.
// While the node is still syncing every segment is ignored, as envelopes are: nothing here
// could be judged against a chain the node does not have yet.
func (s *Service) validateExecutionPayloadSegment(ctx context.Context, pid peer.ID, msg *pubsub.Message) (pubsub.ValidationResult, error) {
	if pid == s.cfg.p2p.PeerID() {
		return pubsub.ValidationAccept, nil
	}
	if s.segmentReassembler == nil {
		return pubsub.ValidationIgnore, errSegmentedGossipDisabled
	}
	if s.cfg.initialSync.Syncing() {
		return pubsub.ValidationIgnore, nil
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

	// Only a segment that would open a group is charged. A segment joining an open group
	// proves itself against the already-authenticated root inside Add and costs nothing here.
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
	// One step: Add reports how much fitted under the cap, so a full bucket admits nothing
	// and two concurrent validators cannot both pass a check-then-charge.
	return s.segmentAuthLimiter.Add(pid.String(), 1) == 1
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
