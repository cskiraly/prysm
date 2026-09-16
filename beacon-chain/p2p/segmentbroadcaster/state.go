package segmentbroadcaster

import (
	"time"

	"github.com/OffchainLabs/prysm/v7/container/segments"
)

// PeerState is what we remember about one peer for one segment group.
//
// The extension owns the map these live in and hands it to us only inside a callback, so
// every field here is only ever touched on the pubsub goroutine. Nothing in this type may
// be shared with the broadcaster's own loop.
type PeerState struct {
	// Sent is the metadata we last sent this peer: our view of what it believes we hold.
	// Kept so a redundant metadata send can be suppressed, which the extension asks for.
	Sent *segments.PartsMetadata
	// Recvd is the metadata this peer last sent us: what it holds and what it wants.
	Recvd *segments.PartsMetadata
	// Pushed is the set of segments we have actually transmitted to this peer. Distinct
	// from Sent.Available, which is only a claim about what we hold.
	Pushed *segments.Bitmap
	// MetaSentAt is when Sent last went on the wire, for the announce batching window.
	MetaSentAt time.Time
}

// Clone returns a copy safe to mutate without disturbing the stored state.
//
// The extension's contract is that a PublishActionsFn returns the *next* state, and that
// state must not be installed when the action failed. Cloning first is what makes a failed
// action leave no trace.
func (s PeerState) Clone() PeerState {
	return PeerState{
		Sent:       s.Sent.Clone(),
		Recvd:      s.Recvd.Clone(),
		Pushed:     s.Pushed.Clone(),
		MetaSentAt: s.MetaSentAt,
	}
}

// holds reports whether the peer has told us it holds index.
//
// A peer we have heard nothing from holds nothing, which is the safe reading: it means we
// may send to it, never that we may skip it.
func (s PeerState) holds(index uint32) bool {
	if s.Recvd == nil {
		return false
	}
	return s.Recvd.Available.Has(index)
}

// wants reports whether the peer has explicitly asked for index.
func (s PeerState) wants(index uint32) bool {
	if s.Recvd == nil {
		return false
	}
	return s.Recvd.Requests.Has(index)
}

// pushed reports whether we have already transmitted index to this peer.
func (s PeerState) pushed(index uint32) bool {
	return s.Pushed.Has(index)
}

// heardFrom reports whether this peer has ever told us anything about the group.
//
// This is the eager-push trigger: with no information about a peer there is nothing to
// deduplicate against, so the only choice is to push or to stay silent.
func (s PeerState) heardFrom() bool {
	return s.Recvd != nil
}
