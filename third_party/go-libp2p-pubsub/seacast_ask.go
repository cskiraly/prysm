package pubsub

// No second ask of a peer at once (seacast.md §4.3's second-ask rule, seacast-lite.md §6.1's
// outstanding). An ask of a chunk to a peer is outstanding while it is
// queued to the peer (from its dispatch to the writer's report, which opens the promise under
// v0.5's E) or promised by the peer. The offer handler passes over such a peer's offer of the chunk
// (the offer is still recorded), and the retry picks among the announcers not outstanding, looking
// again later when none is free instead of asking the same peer twice. The ask closes with the
// chunk's delivery, the promise's deadline, a tie, the group's expiry, the connection's close, or
// when the writer drops it.

import (
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

// seacastAskQueued records an ask of mid dispatched to p, until the writer reports it (promised)
// or drops it. Router goroutine, from noteIWantSent.
func (gs *GossipSubRouter) seacastAskQueued(p peer.ID, mids []string) {
	sc := gs.seacast
	if sc == nil {
		return
	}
	if sc.askQueued == nil {
		sc.askQueued = map[string]map[peer.ID]time.Time{}
	}
	now := time.Now()
	for _, mid := range mids {
		peers := sc.askQueued[mid]
		if peers == nil {
			peers = map[peer.ID]time.Time{}
			sc.askQueued[mid] = peers
		}
		peers[p] = now
	}
}

// seacastAskWritten takes the writer's report that an ask of mid to p was written at at: it closes
// the queued ask and reports true when that ask was dispatched by then. A report for an ask that
// closed before it (delivered, tied, its offer dropped, completed, expired) finds no record, or the
// record of a newer ask of the same chunk dispatched after the write, which it leaves to that ask's
// own report: false. Router goroutine.
func (gs *GossipSubRouter) seacastAskWritten(p peer.ID, mid string, at time.Time) bool {
	sc := gs.seacast
	if sc == nil {
		return false
	}
	dispatched, queued := sc.askQueued[mid][p]
	if !queued || dispatched.After(at) {
		return false
	}
	gs.seacastAskClosed(p, mid)
	return true
}

// seacastAskClosed forgets p's queued ask of mid: written (the promise is open from here), dropped
// at the queue, answered with a copy that waits, or closed with the group or the connection.
func (gs *GossipSubRouter) seacastAskClosed(p peer.ID, mid string) {
	sc := gs.seacast
	if sc == nil || sc.askQueued == nil {
		return
	}
	if peers := sc.askQueued[mid]; peers != nil {
		delete(peers, p)
		if len(peers) == 0 {
			delete(sc.askQueued, mid)
		}
	}
}

// seacastAsksClosed forgets every queued ask of mid (delivered, or its group expired).
func (gs *GossipSubRouter) seacastAsksClosed(mid string) {
	if sc := gs.seacast; sc != nil && sc.askQueued != nil {
		delete(sc.askQueued, mid)
	}
}

// seacastAskIsQueued says whether p's ask of mid is still queued: dispatched, and neither reported
// written nor closed since. Router goroutine.
func (gs *GossipSubRouter) seacastAskIsQueued(p peer.ID, mid string) bool {
	sc := gs.seacast
	if sc == nil {
		return false
	}
	_, queued := sc.askQueued[mid][p]
	return queued
}

// seacastGroupStopped says whether mid's group stopped pulling at its completion, when its promises
// closed (seacastClosePromises). Router goroutine.
func (gs *GossipSubRouter) seacastGroupStopped(mid string) bool {
	sc := gs.seacast
	if sc == nil || !sc.machine {
		return false
	}
	root := gs.seacastAskRoot(mid)
	if root == "" {
		return false
	}
	g := sc.groups[root]
	return g != nil && g.stopPull
}

// seacastAskOutstanding says whether an ask of mid to p is open: queued to p, or promised by p.
func (gs *GossipSubRouter) seacastAskOutstanding(p peer.ID, mid string) bool {
	sc := gs.seacast
	if sc == nil {
		return false
	}
	if _, open := gs.iwantPromised[mid][p]; open {
		return true
	}
	_, queued := sc.askQueued[mid][p]
	return queued
}

// seacastRetryLater arms the retry of mid once more after the ask window: every announcer is
// outstanding now (seacast-lite.md's retry_offer: "look again later"). Router goroutine.
func (gs *GossipSubRouter) seacastRetryLater(mid string) {
	if gs.seacast == nil {
		return
	}
	gs.seacast.ctr.RetryDeferred.Add(1)
	time.AfterFunc(gs.iwantWindow+5*time.Millisecond, func() {
		select {
		case gs.p.eval <- func() { gs.retryOfferedPull(mid) }:
		case <-gs.p.ctx.Done():
		}
	})
}
