package features

import (
	"fmt"
	"strings"
)

// SegmentedPayloadMode selects how, if at all, a node participates in segmented execution
// payload gossip.
//
// This is a switch between design variants rather than a simple on/off, because the two
// variants make opposite trade-offs and the branch exists to compare them. They differ in
// what a segment *is* on the wire, and everything else follows from that:
//
//   - Variant A treats each segment as an ordinary gossip message on its own topic. Simple,
//     and it needs nothing from libp2p that is not already there — but every mesh peer
//     eager-pushes every segment, so a node receives the payload several times over. That
//     duplicate reception, not the publisher's upload, is what bounds completion.
//   - Variant B treats segments as parts of one message, carried by gossipsub's
//     partial-message extension on the envelope topic. A peer says what it holds and what it
//     wants, so a segment can be sent once rather than D times, and the representation is
//     chosen per link: peers that speak segments get segments, peers that do not still get
//     the whole envelope from the same topic.
type SegmentedPayloadMode uint8

const (
	// SegmentedPayloadOff leaves segmented gossip disabled. Nothing is allocated and no
	// topic behaviour changes.
	SegmentedPayloadOff SegmentedPayloadMode = iota
	// SegmentedPayloadMessages is variant A: segments as ordinary gossip messages on a
	// dedicated topic.
	SegmentedPayloadMessages
	// SegmentedPayloadPartial is variant B: segments as partial-message parts on the
	// envelope topic, negotiated per link.
	SegmentedPayloadPartial
)

// segmentedPayloadModeNames is the mapping the flag accepts, and the source for String.
var segmentedPayloadModeNames = map[SegmentedPayloadMode]string{
	SegmentedPayloadOff:      "off",
	SegmentedPayloadMessages: "messages",
	SegmentedPayloadPartial:  "partial",
}

// String returns the flag spelling of a mode.
func (m SegmentedPayloadMode) String() string {
	if s, ok := segmentedPayloadModeNames[m]; ok {
		return s
	}
	return fmt.Sprintf("unknown(%d)", uint8(m))
}

// Enabled reports whether segmented gossip runs at all, in either variant.
//
// Used where the two variants share machinery — reassembly, authentication, the segment
// budget — so that adding a third variant does not mean revisiting every call site.
func (m SegmentedPayloadMode) Enabled() bool {
	return m != SegmentedPayloadOff
}

// ParseSegmentedPayloadMode resolves a flag value to a mode.
//
// An unrecognised value is an error rather than a silent fall back to off: a typo in the
// mode name would otherwise disable the feature being measured without saying so.
func ParseSegmentedPayloadMode(s string) (SegmentedPayloadMode, error) {
	want := strings.ToLower(strings.TrimSpace(s))
	for mode, name := range segmentedPayloadModeNames {
		if name == want {
			return mode, nil
		}
	}
	return SegmentedPayloadOff, fmt.Errorf(
		"unknown segmented payload gossip mode %q, want one of off, messages, partial", s)
}
