package segmentgossip

import (
	"sync"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
)

// Stop-pull: the receiver's side of a coded group.
//
// A coded group completes a node at Required of its segments, so every segment of the group
// offered after that is bytes the node does not need. A push it cannot refuse; a pull it
// can. The request gate sees every announced id before an IWANT leaves, and a segment's id
// names its group's root, so the gate declines the segments of every group this node has
// completed. The completing node stops asking, the announcers stop serving it, and the
// bytes a coded group would otherwise cost above a plain one -- the parity nobody needed --
// are mostly never sent. Measured on the study's mesh the coded group with stop-pull asked
// for fewer bytes than the plain group at every payload size.
//
// The gate declines; it does not forget. A declined announcement is not replayed, because
// nothing about this node's position changes: it has the payload. Pushes are untouched, and
// a group's segments still forward to peers that are not done.

// PullGate is the request gate installed on the segment topic. It vetoes requests for
// segments of groups the node has completed and has no opinion about anything else.
type PullGate struct {
	mu       sync.Mutex
	now      func() time.Time
	complete map[[32]byte]time.Time
}

// completeRetention is how long a completed group's root is remembered. It matches the
// reassembler's retention of a delivered group; past it, no honest peer still announces the
// group, since gossipsub's cache serves a message for a few seconds only.
const completeRetention = 30 * time.Second

// NewPullGate returns a gate that declines nothing yet.
func NewPullGate() *PullGate {
	return &PullGate{now: time.Now, complete: make(map[[32]byte]time.Time)}
}

// MarkComplete records that this node has reassembled the group committed to by root, so
// its remaining segments are no longer requested.
func (g *PullGate) MarkComplete(root [32]byte) {
	now := g.now()
	g.mu.Lock()
	defer g.mu.Unlock()
	for r, at := range g.complete {
		if now.Sub(at) >= completeRetention {
			delete(g.complete, r)
		}
	}
	g.complete[root] = now
}

// Allow implements pubsub.RequestGate: false for a segment of a completed group, true for
// every other id, including ids that carry no claim.
func (g *PullGate) Allow(_ peer.ID, _ string, mid string) bool {
	claim, ok := ParseMessageID(mid)
	if !ok {
		return true
	}
	g.mu.Lock()
	at, done := g.complete[claim.Root]
	g.mu.Unlock()
	return !done || g.now().Sub(at) >= completeRetention
}

// Committed implements pubsub.RequestGate. The gate charges no budget, so there is nothing
// to record.
func (g *PullGate) Committed(peer.ID, []string) {}

// Failed implements pubsub.RequestGate; see Committed.
func (g *PullGate) Failed(peer.ID, []string) {}

var _ pubsub.RequestGate = (*PullGate)(nil)
