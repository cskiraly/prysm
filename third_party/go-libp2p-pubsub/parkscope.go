package pubsub

import (
	"fmt"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

// --- The park scoped to a group ---
//
// The commitment park charges a peer for a promise it broke and, at k breaks, makes all its
// announcements ineligible for pulls for the TTL, whatever message they name. With a message
// every slot and a TTL of 30 s, an honest relay that was slow under one message's load stays
// parked through the next ones (the multi-message cells: warm messages 20-40 percent slower),
// and a parked peer's IHAVE is dropped before its offer and holder evidence are recorded, so
// the asker forgets it holds the frames even after the park ends.
//
// Scoped, a break is a fact about one group: breaks are counted per (peer, group) and a park
// makes the peer ineligible for that group's pulls only. A withholder is still caught within
// the group it withholds; a relay slow on one message is asked normally on the next. Announces
// from a parked peer are recorded as offers and holder evidence and filtered at ask time, as
// every other park check already does. Ids outside any group (no HAVE root) keep one scope per
// peer, the unscoped park's behaviour.

type parkEntry struct {
	breaks int
	until  time.Time
	last   time.Time
}

// WithIHaveParkPerGroup scopes the commitment park to the group an id belongs to. scope, when
// set, names it (an application that encodes the group in its message ids); otherwise the group
// is the HAVE root, through WithHaveBitmaps' index function or a want-by-index id. Requires
// WithIHaveCommitmentPark first.
func WithIHaveParkPerGroup(scope func(mid string) (string, bool)) Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok {
			return fmt.Errorf("group-scoped park requires a gossipsub router")
		}
		if gs.parkK == 0 {
			return fmt.Errorf("group-scoped park requires WithIHaveCommitmentPark first")
		}
		gs.parkGroups = make(map[peer.ID]map[string]*parkEntry)
		gs.parkScope = scope
		return nil
	}
}

// parkScopeOf is the group mid belongs to; "" for an id outside any group.
func (gs *GossipSubRouter) parkScopeOf(mid string) string {
	if gs.parkScope != nil {
		if g, ok := gs.parkScope(mid); ok {
			return g
		}
	}
	if gs.have != nil {
		if root, _, ok := gs.have.index(mid); ok {
			return root
		}
	}
	if root, _, ok := decodeWant(mid); ok {
		return root
	}
	return ""
}

// noteGroupBreak charges p one break in mid's group, parking it there at parkK.
func (gs *GossipSubRouter) noteGroupBreak(mid string, p peer.ID) {
	g := gs.parkScopeOf(mid)
	byGroup := gs.parkGroups[p]
	if byGroup == nil {
		byGroup = make(map[string]*parkEntry)
		gs.parkGroups[p] = byGroup
	}
	e := byGroup[g]
	if e == nil {
		e = &parkEntry{}
		byGroup[g] = e
	}
	now := time.Now()
	e.breaks++
	e.last = now
	if e.breaks >= gs.parkK {
		if !now.Before(e.until) && gs.parksCtr != nil {
			gs.parksCtr.Add(1)
		}
		e.until = now.Add(gs.parkTTL)
	}
}

// peerParkedFor reports whether p's announcement of mid is ineligible for pull selection.
func (gs *GossipSubRouter) peerParkedFor(p peer.ID, mid string) bool {
	if gs.parkGroups == nil {
		return gs.peerParked(p)
	}
	e := gs.parkGroups[p][gs.parkScopeOf(mid)]
	return e != nil && time.Now().Before(e.until)
}

// sweepGroupParks forgets a group's breaks once they are a TTL old and no park runs; called
// from the heartbeat.
func (gs *GossipSubRouter) sweepGroupParks() {
	if gs.parkGroups == nil {
		return
	}
	now := time.Now()
	for p, byGroup := range gs.parkGroups {
		for g, e := range byGroup {
			if !now.Before(e.until) && now.Sub(e.last) >= gs.parkTTL {
				delete(byGroup, g)
			}
		}
		if len(byGroup) == 0 {
			delete(gs.parkGroups, p)
		}
	}
}

// --- The promise re-checked at its deadline ---
//
// A promise is cleared when its id arrives, from anyone. On a coded group the asker usually
// stops needing the id without it arriving: it decodes, holds every index, and its decode HAVE
// cancels the answer still queued at the asked peer. At the deadline the promise is open and the
// peer that obeyed our own claim is charged. The deadline is a timer standing in for "was this
// ask served?"; the question waiting can answer is "do we hold what we asked for?", so that is
// what is asked when it fires.

// WithPromiseRecheck absolves, at its deadline, a promise for an index we already hold.
// Requires WithIHaveCommitmentPark first.
func WithPromiseRecheck() Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok {
			return fmt.Errorf("promise recheck requires a gossipsub router")
		}
		if gs.iwantPromised == nil {
			return fmt.Errorf("promise recheck requires WithIHaveCommitmentPark first")
		}
		gs.promiseRecheck = true
		return nil
	}
}

// haveHolds reports whether we hold mid's index of its group, received or decoded.
func (gs *GossipSubRouter) haveHolds(mid string) bool {
	h := gs.have
	if h == nil {
		return false
	}
	root, index, ok := h.index(mid)
	if !ok {
		if root, index, ok = decodeWant(mid); !ok { // an ask by index names no message id
			return false
		}
	}
	e := h.mine[root]
	return e != nil && e.bits.Has(index)
}

// absolvePromise closes p's promise for mid without a charge, as a delivery would.
func (gs *GossipSubRouter) absolvePromise(mid string, p peer.ID) {
	if peers := gs.iwantPromised[mid]; peers != nil {
		delete(peers, p)
		if len(peers) == 0 {
			delete(gs.iwantPromised, mid)
		}
	}
}

// PromisesHeldAtDeadline is how many promises fell due with their index already held.
func (p *PubSub) PromisesHeldAtDeadline() int64 {
	gs, ok := p.rt.(*GossipSubRouter)
	if !ok {
		return 0
	}
	return gs.promiseHeld.Load()
}
