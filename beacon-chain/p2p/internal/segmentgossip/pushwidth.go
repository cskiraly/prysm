package segmentgossip

import (
	"encoding/binary"

	"github.com/OffchainLabs/prysm/v7/beacon-chain/p2p/encoder"
	"github.com/OffchainLabs/prysm/v7/config/params"
	ethpb "github.com/OffchainLabs/prysm/v7/proto/prysm/v1alpha1"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
)

// Push width by payload size.
//
// Phase forwarding pushes a segment to a few mesh peers and announces it to the rest, and how
// many is the one knob that trades bytes for time. At 16 KiB segments the trade depends on the
// payload: a small payload is a few dozen segments whose duplicates are cheap, and pushing
// wide gets every node its copy sooner; a large payload is where the duplicates are the bytes
// and where the pull path already keeps completion. Measured on the study's 500-node mesh over
// home links, the width below was the best of the fixed widths at every size it names, so a
// node reads the payload's length from the segment and pushes accordingly. The length is the
// descriptor's total_length, committed with the rest of the descriptor, so a peer cannot make
// its segments travel wider than the payload warrants without failing every proof.
const (
	// pushWidthSmall is the width for payloads up to pushWidthSmallLimit.
	pushWidthSmall      = 4
	pushWidthSmallLimit = 256 << 10
	// pushWidthMedium is the width for payloads up to pushWidthMediumLimit.
	pushWidthMedium      = 3
	pushWidthMediumLimit = 1 << 20
	// pushWidthLarge is the width above that, and for a message whose length cannot be read.
	pushWidthLarge = 2
)

// sszTotalLengthAt is where the descriptor's total_length sits in the SSZ body; see the layout
// note above sszRootAt.
const sszTotalLengthAt = 16

// PushWidth returns how many mesh peers a segment of a payload totalLength bytes long is
// pushed to before the rest are announced.
func PushWidth(totalLength uint64) int {
	switch {
	case totalLength <= pushWidthSmallLimit:
		return pushWidthSmall
	case totalLength <= pushWidthMediumLimit:
		return pushWidthMedium
	default:
		return pushWidthLarge
	}
}

// PushWidthOf is PushWidth for a gossip message about to be forwarded. The payload length
// comes from the segment the validator decoded when it left one on the message, and
// otherwise from the SSZ body at its fixed offset, which is the case for a node's own
// segments: they skip validation. A body that is not a segment container is pushed at the
// large-payload width; such a message never validates, so this is a floor, not a path.
func PushWidthOf(msg *pubsub.Message) int {
	if msg == nil {
		return pushWidthLarge
	}
	if seg, ok := msg.ValidatorData.(*ethpb.ExecutionPayloadSegment); ok && seg.SegmentDescriptor != nil {
		return PushWidth(seg.SegmentDescriptor.TotalLength)
	}
	decoded, err := encoder.DecodeSnappy(msg.GetData(), params.BeaconConfig().MaxPayloadSize)
	if err != nil || !isSegmentBody(decoded) {
		return pushWidthLarge
	}
	return PushWidth(binary.LittleEndian.Uint64(decoded[sszTotalLengthAt : sszTotalLengthAt+8]))
}
