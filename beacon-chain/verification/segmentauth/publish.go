package segmentauth

import (
	"fmt"

	"github.com/OffchainLabs/prysm/v7/container/segments"
	"github.com/OffchainLabs/prysm/v7/math"
	ethpb "github.com/OffchainLabs/prysm/v7/proto/prysm/v1alpha1"
	"github.com/golang/snappy"
)

// ErrEnvelopeMalformed is returned when the envelope or its segmentation parameters are unusable.
var ErrEnvelopeMalformed = fmt.Errorf("segment publish input malformed")

// Params selects a segmentation of an envelope. Every field is part of the group id, so a
// receiver only admits segments cut with the parameters the chain committed to.
type Params struct {
	// SegmentSize is the size the envelope's SSZ bytes are cut at; it sets the segment count.
	SegmentSize uint32
	// HashID names the hash the tree is built with.
	HashID segments.HashID
	// Encoding is what is done to the envelope's bytes before they are committed to.
	// EncodingSnappy compresses first, so every segment is dense on the wire and the group
	// carries fewer of them; the receiver decompresses what it reassembles.
	Encoding segments.Encoding
	// Coded adds as many parity segments as there are data segments, so any half of the
	// group completes a receiver. A group the code cannot hold, above MaxCodedPayload at the
	// segment size, is published plain instead.
	Coded bool
}

// DefaultSegmentSize is the segment size a publisher cuts at until the commitment in the bid
// names one. 16 KiB is where the measured completion floor is: on a 500-node simulated mesh,
// across payloads from 128 KiB to 2 MiB, 16 KiB takes about a tenth off the median and the
// p99 against the codec's 32 KiB, 8 KiB adds nothing and doubles the control traffic, and
// 64 KiB costs 15 to 40 percent.
const DefaultSegmentSize = 16 << 10

// DefaultParams is the segmentation a publisher uses until the commitment in the bid names
// one: the envelope compressed first, cut into as many segments as DefaultSegmentSize gives
// its SSZ bytes, with as many parity segments again, over SHA-256. This is the third tier of
// the study's recommendation; measured against the plain group at the same segment size it
// completes sooner at every payload size and asks for fewer bytes, because a receiver is done
// at the first half of the group to reach it, whichever half that is.
func DefaultParams() Params {
	return Params{
		SegmentSize: DefaultSegmentSize,
		HashID:      segments.HashSHA256,
		Encoding:    segments.EncodingSnappy,
		Coded:       true,
	}
}

// SegmentMessagesForEnvelope derives the segmentation of a signed envelope.
//
// The segments carry no authority field: a receiver authenticates them by checking the derived
// group id against the commitments it holds from accepted bids. So this function's only job is
// to segment the bytes the way the builder committed to.
//
// Note what that does NOT give the publisher: with the commitment in the block rather than in a
// signature, there is nothing local to check the derived segmentation against, so a caller that
// supplies parameters the builder did not commit to produces a well-formed group that every
// peer refuses. Restoring that local check needs the bid field, at which point this function
// can compare its derived group id against the block's commitment before broadcasting.
//
// Shape of a coded group: the segment count K follows the envelope's SSZ size at SegmentSize,
// the segments themselves are the committed (compressed) bytes cut into K, and K parity
// segments follow. So a 1 MiB envelope is 64 data segments of about 12 KiB once compressed,
// plus 64 parity, which is the shape the study measured. A plain group cuts the committed
// bytes at SegmentSize directly.
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

	committed := encoded
	switch p.Encoding {
	case segments.EncodingRaw:
	case segments.EncodingSnappy:
		committed = snappy.Encode(nil, encoded)
	default:
		return nil, fmt.Errorf("%w: encoding %d", segments.ErrEncoding, p.Encoding)
	}

	layout := segments.Layout{SegmentSize: segmentSize, Hasher: hasher, Encoding: p.Encoding}
	if p.Coded {
		k := (len(encoded) + segmentSize - 1) / segmentSize
		if 2*k <= segments.MaxCodedSegments {
			layout.SegmentSize = (len(committed) + k - 1) / k
			layout.Parity = k
		}
	}
	out, err := segments.Build(committed, layout)
	if err != nil {
		return nil, fmt.Errorf("segment envelope: %w", err)
	}
	return out, nil
}

// MaxCodedPayload is the largest envelope, by SSZ size, that a coded group at segmentSize can
// hold: the code has room for 2K segments, K of them data.
func MaxCodedPayload(segmentSize int) int {
	return segments.MaxCodedSegments / 2 * segmentSize
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
