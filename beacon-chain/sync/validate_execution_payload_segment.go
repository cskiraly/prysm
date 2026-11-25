package sync

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"time"

	"github.com/OffchainLabs/prysm/v7/beacon-chain/p2p"
	"github.com/OffchainLabs/prysm/v7/beacon-chain/p2p/encoder"
	"github.com/OffchainLabs/prysm/v7/beacon-chain/verification/segmentauth"
	"github.com/OffchainLabs/prysm/v7/config/features"
	"github.com/OffchainLabs/prysm/v7/config/params"
	"github.com/OffchainLabs/prysm/v7/consensus-types/blocks"
	"github.com/OffchainLabs/prysm/v7/consensus-types/interfaces"
	"github.com/OffchainLabs/prysm/v7/container/segments"
	"github.com/OffchainLabs/prysm/v7/encoding/bytesutil"
	"github.com/OffchainLabs/prysm/v7/monitoring/tracing"
	"github.com/OffchainLabs/prysm/v7/monitoring/tracing/trace"
	ethpb "github.com/OffchainLabs/prysm/v7/proto/prysm/v1alpha1"
	prysmTime "github.com/OffchainLabs/prysm/v7/time"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/sirupsen/logrus"
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

// segmentVerdict is the payload chunk's accept path's verdict, the same whichever binding carried the
// chunk. Accept: the chunk is taken and passed on. Ignore: neither, and the sender is not to blame; the
// node's own limits and a group it can't judge yet (the specification's DEFER, which this path doesn't
// buffer) are ignored. Reject: the sender's fault (the specification's REJECT, which penalises the sender,
// seacast.md §4.2's on_validated).
type segmentVerdict int

const (
	segmentAccept segmentVerdict = iota
	segmentIgnore
	segmentReject
)

// gossip is the verdict as gossipsub's validation result.
func (v segmentVerdict) gossip() pubsub.ValidationResult {
	switch v {
	case segmentAccept:
		return pubsub.ValidationAccept
	case segmentReject:
		return pubsub.ValidationReject
	default:
		return pubsub.ValidationIgnore
	}
}

// segmentPathOpen is the node's side of the accept path's first check: whether it takes payload chunks at
// all. While the node is still syncing every chunk is ignored, as envelopes are: nothing on the path could
// be judged against a chain the node does not have yet.
func (s *Service) segmentPathOpen() (segmentVerdict, error) {
	if s.segmentReassembler == nil {
		return segmentIgnore, errSegmentedGossipDisabled
	}
	if s.cfg.initialSync.Syncing() {
		return segmentIgnore, nil
	}
	return segmentAccept, nil
}

// validateExecutionPayloadSegment is the segment topic's gossip validator: the gossipsub binding's side of
// a payload chunk's accept path (acceptSegment). It accepts the node's own segments at once, and checks
// that the node takes chunks before it reads one from its gossip wire form. Then it hands the chunk, the
// sender and the topic to the accept path, and returns its verdict as gossipsub's. An accepted segment
// carries its decoded chunk to the subscriber.
func (s *Service) validateExecutionPayloadSegment(ctx context.Context, pid peer.ID, msg *pubsub.Message) (pubsub.ValidationResult, error) {
	if pid == s.cfg.p2p.PeerID() {
		return pubsub.ValidationAccept, nil
	}
	if v, err := s.segmentPathOpen(); v != segmentAccept {
		return v.gossip(), err
	}

	ctx, span := trace.StartSpan(ctx, "sync.validateExecutionPayloadSegment")
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
	digest, err := p2p.ExtractGossipDigest(*msg.Topic)
	if err != nil {
		// The router runs this validator on the segment topic only, whose name carries a digest.
		tracing.AnnotateError(span, err)
		return pubsub.ValidationIgnore, err
	}
	// The gate was checked above, once, as it was before the accept path was lifted out of this
	// validator: a resync between two checks would ignore a segment the first one let through.
	v, err := s.acceptOpenSegment(ctx, segmentArrival{from: pid, place: *msg.Topic, digest: digest, chunk: pb})
	if err != nil {
		tracing.AnnotateError(span, err)
	}
	if v == segmentAccept {
		msg.ValidatorData = pb
	}
	return v.gossip(), err
}

// segmentArrival is a payload chunk as a binding hands it to the accept path.
type segmentArrival struct {
	// from is the peer that sent the chunk.
	from peer.ID
	// place is where the chunk travelled, in its binding's terms, handed back to the binding's path as is
	// (SegmentGroupSeen). On gossipsub it is the segment topic, which keys the message ids of the
	// segments the router rebuilds.
	place string
	// digest is the fork digest the chunk travelled under, which names the envelope type the group decodes
	// as.
	digest [4]byte
	chunk  *ethpb.ExecutionPayloadSegment
}

// acceptSegment is a payload chunk's accept path, whatever binding carried it. It runs from the chunk to a
// verdict and, for the chunk that completes its group, to the envelope's admission and import.
//
// The order is deliberate and runs cheapest-first:
//   - the chunk's group key and slot;
//   - the per-peer budget, when the chunk would open a group;
//   - the Merkle proof, the length checks and the descriptor's authentication, all inside the
//     reassembler's Add, so that a chunk is verified exactly once.
//
// The chunk that completes a group is followed by the group's recovery, the payload's decompression
// when the descriptor says it was compressed, the envelope's decode, and the envelope topic's checks.
// The binding has read the chunk from its wire form by then. The path tells the binding's path what it
// learns of the group: its shape (SegmentGroupSeen) and, once recovered and its completing chunk
// accepted, its bytes (SegmentGroupCommitted).
//
// It checks first that the node takes chunks (segmentPathOpen). A binding that checks that itself before
// it reads the chunk from its wire form, as the gossipsub validator does, calls acceptOpenSegment.
func (s *Service) acceptSegment(ctx context.Context, a segmentArrival) (segmentVerdict, error) {
	if v, err := s.segmentPathOpen(); v != segmentAccept {
		return v, err
	}
	return s.acceptOpenSegment(ctx, a)
}

// acceptOpenSegment is acceptSegment once the binding has checked that the node takes chunks.
func (s *Service) acceptOpenSegment(ctx context.Context, a segmentArrival) (segmentVerdict, error) {
	// A message the wire type carries but the descriptor cannot hold is the peer's fault and
	// provably invalid, so reject rather than ignore: this feeds the binding's invalid-message scoring.
	seg, hasher, err := segments.FromProto(a.chunk)
	if err != nil {
		return segmentReject, err
	}
	// seacast-lite.md §3's level 1, as far as it goes without the bid's commitment. The chunk's group
	// key must be its descriptor's group id at its slot: the key names the group in every message id,
	// offer and ask, and it binds the descriptor and the slot. And the slot must be its block's,
	// where the node has the block. A chunk whose block the node lacks is taken, not deferred as the
	// specification would: the interim authority below does not need the block.
	if err := seg.VerifyGroupKey(hasher); err != nil {
		return segmentReject, err
	}
	if blockSlot, err := s.cfg.chain.RecentBlockSlot(bytesutil.ToBytes32(seg.BeaconBlockRoot)); err == nil && uint64(blockSlot) != seg.Slot {
		return segmentReject, fmt.Errorf("segment names slot %d for a block at slot %d", seg.Slot, blockSlot)
	}

	// Only a segment that would open a group is charged. A segment joining an open group
	// proves itself against the already-authenticated root inside Add and costs nothing here.
	groupID := seg.Descriptor.GroupID(hasher)
	if !s.segmentReassembler.Has(groupID) {
		if !s.spendSegmentAuthBudget(a.from) {
			segmentAuthThrottledCounter.Inc()
			return segmentIgnore, nil
		}
	}

	complete, err := s.segmentReassembler.Add(hasher, seg)
	if err != nil {
		if errors.Is(err, segmentauth.ErrFirstSeenConflict) {
			// The interim authority's assumption is that at most one earlier slot's group is still
			// in flight when a slot's own group starts; a refusal here means it broke.
			segmentGroupRefusedCounter.WithLabelValues(segmentRefusedFirstSeenFull).Inc()
			log.WithError(err).WithField("slot", s.cfg.clock.CurrentSlot()).Debug("Payload segment group refused: both of the slot's places are taken")
		}
		return segmentAddResult(err)
	}
	// The segment is verified against an authenticated descriptor, so its group's shape is known:
	// the payload's path learns it here, before the router counts the segment.
	count, threshold := int(seg.Descriptor.Count), int(seg.Descriptor.Required()) // lint:ignore uintcast -- bounded by segments.MaxSegments.
	s.cfg.p2p.SegmentGroupSeen(bytesutil.ToBytes32(seg.Group), a.place, count, threshold)
	if complete == nil {
		// Buffered a new segment, or one more of a group already delivered. Accept so it
		// propagates: it is verified and belongs to an authenticated descriptor, which is
		// the whole precondition for forwarding.
		return segmentAccept, nil
	}

	// This node holds what the root committed to. It asks for no more of the group: the router's
	// group machine stops the group's asks once its decoder completes it (stop-pull).

	payload, err := decodeSegmentedPayload(seg.Descriptor.Encoding, complete)
	if err != nil {
		// The bytes hashed to an authenticated root, so a failure here means the builder
		// committed to something that does not decode as it said. Reject: it is attributable.
		return segmentReject, err
	}
	envelope, err := envelopeOfDigest(a.digest)
	if err != nil {
		// The node takes chunks only at digests its schedule has.
		return segmentIgnore, err
	}
	if err := envelope.UnmarshalSSZ(payload); err != nil {
		// Likewise: the builder committed to something that is not its fork's envelope.
		return segmentReject, err
	}
	segmentReassembledCounter.Inc()
	// The group's key binds its slot, so a payload of another slot was published under a false one.
	// The node does not import it from this group.
	if slot, ok := envelopeSlot(envelope); !ok || slot != seg.Slot {
		segmentGroupRefusedCounter.WithLabelValues(segmentRefusedSlot).Inc()
		log.WithFields(logrus.Fields{"groupSlot": seg.Slot, "payloadSlot": slot}).Debug("Reassembled payload envelope is not of its group's slot")
		return segmentIgnore, nil
	}
	// From here on the segment is accepted, so it reaches the binding's path. That path's decode of the
	// group cuts it again for its missing segments, and takes these bytes rather than recover them a
	// second time. They are handed over only now: a segment refused above never reaches the path, which
	// then may never decode the group, and the bytes would wait for the window to pass it. Nothing below
	// writes to them.
	s.cfg.p2p.SegmentGroupCommitted(bytesutil.ToBytes32(seg.Group), complete)

	// The envelope gets the envelope topic's checks, branch by branch. Their verdict is the
	// envelope's, not this segment's: the segment verified against its root, and the peer that sent
	// the group's last segment is not answerable for what the builder committed to. (The admission is
	// the envelope topic's, so its verdict is gossipsub's type.)
	res := pubsub.ValidationIgnore
	ro, err := wrapSignedEnvelope(envelope)
	if err == nil {
		s.validateEnvelopeLock.Lock()
		res, err = s.admitExecutionPayloadEnvelope(ctx, ro, prysmTime.Now(), envelopeFromSegments)
		s.validateEnvelopeLock.Unlock()
	}
	if res != pubsub.ValidationAccept {
		// Queued for its block, or refused.
		log.WithError(err).WithFields(envelopeFields(ro)).Debug("Reassembled payload envelope not imported on completion")
		return segmentAccept, nil
	}
	// The envelope passed and is marked seen, so this node takes no other copy of it: the import
	// starts here rather than at the delivery of this segment, which the router may still drop
	// (a full subscription buffer, its own admission check after validation).
	s.segmentWork.Go(func() { s.importReassembledEnvelope(envelope, envelopeFields(ro)) })
	if !features.Get().SeacastPayloadOnly {
		// Without payload-only, peers without segments still take the payload on the envelope
		// topic. The seen mark keeps this node from forwarding a whole copy it receives later, so
		// it relays the envelope once now, as develop forwards one it accepts.
		s.segmentWork.Go(func() {
			if err := s.cfg.p2p.Broadcast(s.ctx, envelope); err != nil {
				log.WithError(err).Debug("Could not relay a reassembled payload envelope")
			}
		})
	}
	return segmentAccept, nil
}

// importReassembledEnvelope imports an envelope its group completed, as the envelope topic's
// subscriber imports one: under the same deadline, so an import that waits for data columns that
// never come gives the block root back; with a failure logged and counted under the envelope topic;
// and with a panic recovered.
func (s *Service) importReassembledEnvelope(envelope proto.Message, fields logrus.Fields) {
	ctx, cancel := context.WithTimeout(s.ctx, pubsubMessageTimeout)
	defer cancel()
	topic := fmt.Sprintf(p2p.ExecutionPayloadEnvelopeTopicFormat, s.currentForkDigest()) + s.cfg.p2p.Encoding().ProtocolSuffix()
	defer func() {
		if r := recover(); r != nil {
			log.WithField("error", r).WithFields(fields).WithField("stack", string(debug.Stack())).
				Error("Panic occurred while importing a reassembled payload envelope")
		}
	}()
	if err := s.executionPayloadEnvelopeSubscriber(ctx, envelope); err != nil {
		log.WithError(err).WithFields(fields).Error("Could not import a reassembled payload envelope")
		messageFailedProcessingCounter.WithLabelValues(topic).Inc()
	}
}

// signedEnvelopeMessage is a signed execution payload envelope of a fork the segment path carries,
// as a completed group decodes into it.
type signedEnvelopeMessage interface {
	proto.Message
	UnmarshalSSZ([]byte) error
}

// envelopeOfDigest is an empty signed envelope of the fork a chunk travelled under (the segment topic's
// digest on gossipsub), as the envelope topic maps it at that fork's epoch: the PQ fork's envelope from
// the PQ fork on, Gloas's before it. The type follows the chunk's digest, not the node's clock, so a
// group published just before a fork decodes as its own fork's envelope.
func envelopeOfDigest(digest [4]byte) (signedEnvelopeMessage, error) {
	_, epoch, err := params.ForkDataFromDigest(digest)
	if err != nil {
		return nil, err
	}
	m, ok := p2p.GossipTopicMappings(p2p.ExecutionPayloadEnvelopeTopicFormat, epoch).(signedEnvelopeMessage)
	if !ok {
		return nil, fmt.Errorf("no signed envelope type at epoch %d", epoch)
	}
	return m, nil
}

// wrapSignedEnvelope wraps a decoded envelope in the read-only interface, as the envelope topic's
// validator does.
func wrapSignedEnvelope(m proto.Message) (interfaces.ROSignedExecutionPayloadEnvelope, error) {
	switch t := m.(type) {
	case *ethpb.SignedExecutionPayloadEnvelope:
		return blocks.WrappedROSignedExecutionPayloadEnvelope(t)
	case *ethpb.SignedExecutionPayloadEnvelopePQ:
		return blocks.WrappedROSignedExecutionPayloadEnvelopePQ(t)
	default:
		return nil, errWrongMessage
	}
}

// envelopeFields are a reassembled envelope's slot and block root for the log, none when it could
// not be wrapped.
func envelopeFields(ro interfaces.ROSignedExecutionPayloadEnvelope) logrus.Fields {
	if ro == nil {
		return logrus.Fields{}
	}
	env, err := ro.Envelope()
	if err != nil {
		return logrus.Fields{}
	}
	root := env.BeaconBlockRoot()
	return logrus.Fields{"slot": env.Slot(), "root": fmt.Sprintf("%#x", root[:])}
}

// decodeSegmentedPayload undoes what the publisher did to the envelope's bytes before
// committing to them, as the descriptor names it. The decompressed size is bounded by the
// gossip payload maximum, the same bound every gossip message decodes under.
func decodeSegmentedPayload(enc segments.Encoding, committed []byte) ([]byte, error) {
	switch enc {
	case segments.EncodingRaw:
		return committed, nil
	case segments.EncodingSnappy:
		return encoder.DecodeSnappy(committed, params.BeaconConfig().MaxPayloadSize)
	default:
		// Unreachable: the descriptor validated inside Add, and Validate bounds the encoding.
		return nil, fmt.Errorf("%w: %d", segments.ErrEncoding, enc)
	}
}

// segmentAddResult maps a reassembler error onto the accept path's verdict.
//
// The split is between "this peer sent something wrong" and "we cannot judge it yet or do
// not have room", because only the former should cost the peer reputation.
func segmentAddResult(err error) (segmentVerdict, error) {
	switch {
	// Our own limits, not the peer's fault.
	case errors.Is(err, segments.ErrTooManyGroups), errors.Is(err, segments.ErrBufferFull):
		return segmentIgnore, err
	// Transient: the commitment arrives with a block, so a segment that outruns its block is
	// not the sender's fault. Nothing in this package buffers it -- the segment is dropped and
	// must arrive again, which is why the pending-segment store is still an open item rather
	// than a claim.
	case errors.Is(err, segmentauth.ErrNotCommitted):
		return segmentIgnore, err
	// Under the interim first-seen authority a conflict proves only that two peers disagree.
	// If an attacker won the race it is the honest peer that conflicts, so this must never
	// cost reputation. Only a commitment-backed refusal may be attributable.
	case errors.Is(err, segmentauth.ErrFirstSeenConflict):
		return segmentIgnore, err
	// Everything else -- a malformed descriptor, a proof that does not verify, a descriptor
	// conflicting with the one pinned for its group -- is attributable to whoever sent it.
	default:
		return segmentReject, err
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

// executionPayloadSegmentSubscriber handles the output of segment validation. Segments need no
// work here: the reassembler already holds them, and the validator itself hands a completed
// group's envelope to the import or the pending queue.
func (s *Service) executionPayloadSegmentSubscriber(_ context.Context, msg proto.Message) error {
	if _, ok := msg.(*ethpb.ExecutionPayloadSegment); !ok {
		return errWrongMessage
	}
	return nil
}

// envelopeSlot is a decoded envelope's slot, its payload's slot number, as the envelope handlers read
// it; false for a type the segment path does not carry.
func envelopeSlot(m proto.Message) (uint64, bool) {
	switch e := m.(type) {
	case *ethpb.SignedExecutionPayloadEnvelope:
		if e.GetMessage().GetPayload() == nil {
			return 0, false
		}
		return uint64(e.Message.Payload.SlotNumber), true
	case *ethpb.SignedExecutionPayloadEnvelopePQ:
		if e.GetMessage().GetPayload() == nil {
			return 0, false
		}
		return uint64(e.Message.Payload.SlotNumber), true
	default:
		return 0, false
	}
}
