package scheduleddiffusion

import (
	"time"

	"github.com/OffchainLabs/prysm/v7/container/segments"
)

// PeerState is what we remember about one peer for one segment group.
//
// What we hold lives in the reassembler. Here is what we believe the peer holds, split by
// how we came to believe it, and what it still wants. The forwarding decision is their
// conjunction, so nothing else about a peer has to be remembered -- no claim, no timer per
// request, no outstanding ask.
//
// The extension owns the map these live in and hands it to us only inside a callback, so
// every field here is only ever touched on the pubsub goroutine. Nothing in this type may
// be shared with the broadcaster's own loop.
type PeerState struct {
	// Sent is the last metadata that was *admitted* to this peer's send queue. Kept so a
	// redundant metadata send can be suppressed, which the extension asks for. Written in
	// OnSent, never optimistically: a dropped update we recorded as sent would never be
	// said again.
	Sent *segments.PartsMetadata
	// Theirs is evidence: the union of the Available bitmaps the peer has announced and the
	// segments it has sent us. Monotone, because what a peer once held it still holds.
	Theirs *segments.Bitmap
	// Pushed is belief: the segments we have had admitted to the peer's queue and that
	// Theirs does not yet confirm. Kept apart from Theirs because it can be wrong -- a frame
	// lost on the wire or dropped at the receiver leaves a bit here that nothing else would
	// clear -- and a peer that still asks for one of these long after we pushed it is the
	// evidence that clears it (see PushedAt and Config.PushGrace).
	Pushed *segments.Bitmap
	// Wanted is what the peer still wants. Nil means everything -- a want bitmap is a
	// standing order, and a peer we have heard nothing from is assumed to want the lot.
	// That default is what removes the round trip: nobody has to ask to be served.
	Wanted *segments.Bitmap
	// SentAt is when Sent was admitted, for the announce batching window.
	SentAt time.Time
	// PushedAt is when a push to this peer was last admitted. A want that contradicts a
	// push older than the grace is read as a loss.
	PushedAt time.Time
}

// Clone returns a copy safe to mutate without disturbing the stored state.
//
// The extension's contract is that the peer state must not be installed when the action
// failed. Cloning before an OnSent mutation is what makes a failed action leave no trace.
func (s PeerState) Clone() PeerState {
	return PeerState{
		Sent:     s.Sent.Clone(),
		Theirs:   s.Theirs.Clone(),
		Pushed:   s.Pushed.Clone(),
		Wanted:   s.Wanted.Clone(),
		SentAt:   s.SentAt,
		PushedAt: s.PushedAt,
	}
}

// holds reports whether we believe the peer holds index, by evidence or by our own push.
func (s PeerState) holds(index uint32) bool {
	return s.Theirs.Has(index) || s.Pushed.Has(index)
}

// wants reports whether the peer still wants index.
//
// A nil Wanted is everything, not nothing: until a peer tells us what it wants, the
// standing order is the whole group. The opposite reading would make a silent peer
// unserveable, which is exactly the node that most needs serving.
func (s PeerState) wants(index uint32) bool {
	if s.Wanted == nil {
		return true
	}
	return s.Wanted.Has(index)
}

// sendable is held ∧ wanted ∧ ¬(theirs ∪ pushed) for this peer, in ascending index order.
//
// The whole forwarding decision. Ascending because selection policy is a later stage; this
// one only has to be deterministic.
func (s PeerState) sendable(held *segments.Bitmap) []uint32 {
	var out []uint32
	for _, idx := range held.AndNot(s.Theirs) {
		if !s.Pushed.Has(idx) && s.wants(idx) {
			out = append(out, idx)
		}
	}
	return out
}
