// Package segmentgossip holds what the execution payload segment topic runs under, kept
// apart from the p2p service so an integration test can install it on a bare gossipsub
// router without the service exporting it: the message id its segments travel by, and the
// push/pull policy.
package segmentgossip

import (
	"encoding/binary"
	"strings"
)

// TopicMatcher reports whether a fully formatted topic string is the topic named topicName.
// Matched with both separators: "execution_payload" is a prefix of both the bid and the
// segment topic names, so a bare substring test would be ambiguous.
func TopicMatcher(topicName string) func(topic string) bool {
	needle := "/" + topicName + "/"
	return func(topic string) bool { return strings.Contains(topic, needle) }
}

// Structured message ids on the segment topic.
//
// Gossipsub announces a message by its id, so an id that is only a content hash tells a
// receiver nothing about what is being offered: not which group the segment belongs to, not
// which index it fills. Both decisions a receiver wants to make at announcement time need
// exactly that -- declining a segment of a group it already holds enough of, or of a group no
// block it knows has committed to -- so on the segment topic the id carries the claim in the
// clear:
//
//	root (32) || index (4) || content id (20) = 56 bytes
//
// The claim is read straight from the SSZ container at fixed offsets, so every node derives
// the same id from the wire without unmarshalling the segment. The content half is Prysm's
// ordinary 20-byte id over the same message. It stays because gossipsub deduplicates by id: a
// bare structural id would let one bad first arrival occupy (root, index) for every later
// honest copy, while the hybrid gives each distinct body its own id and leaves the claim
// readable. A message on the topic that is not a well-formed container falls back to the
// content id alone, so a claim is never invented for it; a consumer tells the two apart by
// length.
//
// The claim is not authenticated by the id. It is checked when the body arrives, by the
// segment's proof against the root and the root against the commitment. Until then it is
// what the announcer says the message is, which is all an announcement ever was.

// Byte layout of the SSZ-encoded ExecutionPayloadSegment, pinned by TestMessageIDOffsets
// against a real encode: the 60-byte SegmentDescriptor (version, hash_id, count, segment_size
// as uint32, total_length as uint64, root, encoding as uint32), then index, then the offset
// words of the two variable-length fields, proof and data.
const (
	sszRootAt    = 24
	sszIndexAt   = 60
	sszProofAt   = 64
	sszFixedLen  = 72
	contentIDLen = 20
)

// ClaimLen is the width of the structural half of a segment message id.
const ClaimLen = 32 + 4

// MessageIDLen is the width of a structured segment message id.
const MessageIDLen = ClaimLen + contentIDLen

// Claim is what a segment message id says the message is: a segment of the group committed
// to by Root, filling Index.
type Claim struct {
	Root  [32]byte
	Index uint32
}

// MessageID returns the structured id of a segment message whose snappy-decoded body is
// decoded and whose content id is contentID, or contentID alone when the body is not a
// well-formed ExecutionPayloadSegment.
func MessageID(decoded []byte, contentID string) string {
	if !isSegmentBody(decoded) || len(contentID) != contentIDLen {
		return contentID
	}
	id := make([]byte, 0, MessageIDLen)
	id = append(id, decoded[sszRootAt:sszRootAt+32]...)
	id = append(id, decoded[sszIndexAt:sszIndexAt+4]...)
	id = append(id, contentID...)
	return string(id)
}

// isSegmentBody reports whether an SSZ body has the shape of an ExecutionPayloadSegment: long
// enough for the fixed part, with the first variable-length field's offset word pointing just
// past it. Anything else is not this container, whatever its length.
func isSegmentBody(decoded []byte) bool {
	return len(decoded) >= sszFixedLen &&
		binary.LittleEndian.Uint32(decoded[sszProofAt:sszProofAt+4]) == sszFixedLen
}

// ParseMessageID recovers the claim from a structured segment message id. The second result
// is false for an id that carries none, which is what every other topic's id and a segment
// topic fallback look like.
func ParseMessageID(mid string) (Claim, bool) {
	if len(mid) != MessageIDLen {
		return Claim{}, false
	}
	var c Claim
	copy(c.Root[:], mid[:32])
	c.Index = binary.LittleEndian.Uint32([]byte(mid[32:ClaimLen]))
	return c, true
}
