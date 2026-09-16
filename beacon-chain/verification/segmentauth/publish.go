package segmentauth

import (
	"fmt"

	"github.com/OffchainLabs/prysm/v7/container/segments"
	"github.com/OffchainLabs/prysm/v7/math"
	ethpb "github.com/OffchainLabs/prysm/v7/proto/prysm/v1alpha1"
)

// ErrEnvelopeMalformed is returned when the envelope or its segmentation parameters are unusable.
var ErrEnvelopeMalformed = fmt.Errorf("segment publish input malformed")

// Params selects a segmentation of an envelope: the segment size and the hash the tree is
// built with. Both are part of the group id, so a receiver only admits segments cut with the
// parameters the chain committed to.
type Params struct {
	SegmentSize uint32
	HashID      segments.HashID
}

// DefaultSegmentSize is the segment size a publisher cuts at until the commitment in the bid
// names one. 16 KiB is where the measured completion floor is: on a 500-node simulated mesh,
// across payloads from 128 KiB to 2 MiB, 16 KiB takes about a tenth off the median and the
// p99 against the codec's 32 KiB, 8 KiB adds nothing and doubles the control traffic, and
// 64 KiB costs 15 to 40 percent.
const DefaultSegmentSize = 16 << 10

// DefaultParams is the segmentation a publisher uses until the commitment in the bid names
// one: DefaultSegmentSize over SHA-256.
func DefaultParams() Params {
	return Params{SegmentSize: DefaultSegmentSize, HashID: segments.HashSHA256}
}

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
// The result is the segmentation itself; the p2p broadcaster frames it for gossip.
func SegmentMessagesForEnvelope(
	signed *ethpb.SignedExecutionPayloadEnvelope,
	p Params,
) ([]*segments.SegmentMessage, error) {
	if signed == nil || signed.Message == nil {
		return nil, fmt.Errorf("%w: nil envelope", ErrEnvelopeMalformed)
	}
	encoded, err := signed.MarshalSSZ()
	if err != nil {
		return nil, fmt.Errorf("marshal signed envelope: %w", err)
	}
	hasher, err := segments.HasherByID(p.HashID)
	if err != nil {
		return nil, err
	}
	segmentSize, err := segmentSizeAsInt(p.SegmentSize)
	if err != nil {
		return nil, err
	}

	out, err := segments.BuildSegmentMessages(encoded, segmentSize, hasher)
	if err != nil {
		return nil, fmt.Errorf("segment envelope: %w", err)
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
