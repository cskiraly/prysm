package segmentauth

import (
	"fmt"

	"github.com/OffchainLabs/prysm/v7/container/segments"
	"github.com/OffchainLabs/prysm/v7/math"
	ethpb "github.com/OffchainLabs/prysm/v7/proto/prysm/v1alpha1"
)

// ErrEnvelopeMalformed is returned when the envelope or its segmentation parameters are unusable.
var ErrEnvelopeMalformed = fmt.Errorf("segment publish input malformed")

// SegmentMessagesForEnvelope derives the segmentation of a signed envelope.
//
// The segments carry no authority field: a receiver authenticates them by checking the derived
// group id against the commitments it holds from accepted bids. So this function's only job is
// to segment the bytes the way the builder committed to.
//
// Note what that does NOT give the publisher: with the commitment in the block rather than in a
// signature, there is nothing local to check the derived segmentation against, so a caller that
// supplies a segment size or hash the builder did not commit to produces a well-formed group
// that every peer refuses. Restoring that local check needs the bid field, at which point this
// function can compare its derived group id against the block's commitment before broadcasting.
//
// The result is the segmentation itself, not a wire form. Which wire form it takes -- a gossip
// message per segment, or parts of one partial message -- is the variant choice, and it is
// made in the p2p layer.
//
// auth carries the segmentation parameters only. Its Slot and Signature fields are not
// consulted: the signature belonged to a scheme this package replaced, and the slot to an
// anchor that is no longer on the wire. Both are vestigial in the proto, tracked in
// notes/TODO.md.
func SegmentMessagesForEnvelope(
	signed *ethpb.SignedExecutionPayloadEnvelope,
	auth *ethpb.PayloadSegmentAuth,
) ([]*segments.SegmentMessage, error) {
	if signed == nil || signed.Message == nil {
		return nil, fmt.Errorf("%w: nil envelope", ErrEnvelopeMalformed)
	}
	if auth == nil {
		return nil, fmt.Errorf("%w: nil segmentation parameters", ErrEnvelopeMalformed)
	}
	encoded, err := signed.MarshalSSZ()
	if err != nil {
		return nil, fmt.Errorf("marshal signed envelope: %w", err)
	}
	hasher, err := segments.HasherByID(segments.HashID(auth.HashId))
	if err != nil {
		return nil, err
	}
	segmentSize, err := segmentSizeAsInt(auth.SegmentSize)
	if err != nil {
		return nil, err
	}

	descriptor, segs, err := segments.Commit(encoded, segmentSize, hasher)
	if err != nil {
		return nil, fmt.Errorf("commit to envelope segments: %w", err)
	}
	tree, err := segments.BuildTree(hasher, segs)
	if err != nil {
		return nil, fmt.Errorf("build segment tree: %w", err)
	}
	out := make([]*segments.SegmentMessage, len(segs))
	for i := range segs {
		proof, err := tree.Proof(i)
		if err != nil {
			return nil, fmt.Errorf("segment proof %d: %w", i, err)
		}
		out[i] = &segments.SegmentMessage{
			Descriptor: descriptor,
			Index:      uint32(i),
			Proof:      proof,
			Data:       segs[i],
		}
	}
	return out, nil
}

// SegmentsForEnvelope returns the variant A wire protos for a committed segmentation.
//
// Kept as a distinct entry point because the SSZ frame is variant A's wire format, not a
// property of segmenting: variant B puts the same codec bytes in a partial message instead.
func SegmentsForEnvelope(
	signed *ethpb.SignedExecutionPayloadEnvelope,
	auth *ethpb.PayloadSegmentAuth,
) ([]*ethpb.ExecutionPayloadSegment, error) {
	msgs, err := SegmentMessagesForEnvelope(signed, auth)
	if err != nil {
		return nil, err
	}
	out := make([]*ethpb.ExecutionPayloadSegment, len(msgs))
	for i, m := range msgs {
		enc, err := m.Marshal()
		if err != nil {
			return nil, fmt.Errorf("marshal segment %d: %w", i, err)
		}
		out[i] = &ethpb.ExecutionPayloadSegment{Segment: enc}
	}
	return out, nil
}

// segmentSizeAsInt converts a wire segment size, rejecting values the codec would refuse.
func segmentSizeAsInt(size uint32) (int, error) {
	if size == 0 || size > segments.MaxSegmentSize {
		return 0, fmt.Errorf("%w: %d", segments.ErrSegmentSize, size)
	}
	// Bounded above first, then a checked conversion: int is 32-bit on some platforms and
	// this value is attacker-influenced, so a raw cast could wrap.
	n, err := math.Int(uint64(size))
	if err != nil {
		return 0, fmt.Errorf("%w: %d: %w", segments.ErrSegmentSize, size, err)
	}
	return n, nil
}
