package pubsub

// An ask goes with its announcer's offers (seacast.md §4.2's bullet, seacast-lite.md §6.1's
// drop_offer, v0.8 of 2026-10-28). Where a peer's offer of a chunk is
// dropped, with its tie (seacast_tie.go's drop) or by the offer table's sweep thirty seconds after
// it was made, an ask of the chunk still queued to the peer is removed at the writer and the
// peer's promise for the chunk closes without a charge, so the retry asks another announcer. Kept,
// the ask would leave the peer outstanding: its next offer passed over, a parked announcer's offer
// drawing no last resort while this peer is unparked, and once its promise fell due nothing asking
// again, a stall.

import "github.com/libp2p/go-libp2p/core/peer"

// seacastOfferDropped is the path's part of dropping p's offer of mid: a queued ask of mid to p is
// marked for the writer to drop, an open promise closes without a charge; true when either was
// open, so the caller runs the retry without p. Router goroutine.
func (gs *GossipSubRouter) seacastOfferDropped(p peer.ID, mid string) bool {
	sc := gs.seacast
	if sc == nil {
		return false
	}
	// The offer is gone from every record, the per-group bits of v0.5's announcers too, so the
	// retry does not find p again through them.
	if gs.v05 != nil && gs.v05.offered != nil {
		if root, index, ok := gs.groupIndex(mid); ok {
			if g := gs.v05.offered[p][root]; g != nil {
				g.bits.Clear(index)
			}
		}
	}
	open := false
	if _, queued := sc.askQueued[mid][p]; queued {
		sc.dropMu.Lock()
		if sc.askDropped == nil {
			sc.askDropped = map[peer.ID]map[string]struct{}{}
		}
		if sc.askDropped[p] == nil {
			sc.askDropped[p] = map[string]struct{}{}
		}
		sc.askDropped[p][mid] = struct{}{}
		sc.dropMu.Unlock()
		gs.seacastAskClosed(p, mid)
		sc.ctr.F13AskDropped.Add(1)
		open = true
	}
	if _, promised := gs.iwantPromised[mid][p]; promised {
		gs.absolvePromise(mid, p)
		sc.ctr.F13Closed.Add(1)
		open = true
	}
	return open
}

// seacastAskDropped reports, once, whether the ask of mid queued to p was dropped with p's offer:
// the writer leaves it out of the RPC (dropHeldAsks). Writer goroutine.
func (gs *GossipSubRouter) seacastAskDropped(p peer.ID, mid string) bool {
	sc := gs.seacast
	if sc == nil {
		return false
	}
	sc.dropMu.Lock()
	defer sc.dropMu.Unlock()
	mids := sc.askDropped[p]
	if _, ok := mids[mid]; !ok {
		return false
	}
	delete(mids, mid)
	if len(mids) == 0 {
		delete(sc.askDropped, p)
	}
	return true
}

// seacastPeerDropsForgotten forgets the marks of a connection that closed. Router goroutine.
func (gs *GossipSubRouter) seacastPeerDropsForgotten(p peer.ID) {
	sc := gs.seacast
	if sc == nil {
		return
	}
	sc.dropMu.Lock()
	delete(sc.askDropped, p)
	sc.dropMu.Unlock()
}
