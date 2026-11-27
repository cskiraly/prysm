package pubsub

// An ask goes with its announcer's offers (seacast.md §4.2's bullet, seacast-lite.md §6.1's
// drop_offer, v0.8 of 2026-10-28). Where a peer's offer of a chunk is
// dropped, with its tie (seacast_tie.go's drop) or by the offer table's sweep thirty seconds after
// it was made, an ask of the chunk still queued to the peer is removed from the peer's queue and the
// peer's promise for the chunk closes without a charge, so the retry asks another announcer. Kept,
// the ask would leave the peer outstanding: its next offer passed over, a parked announcer's offer
// drawing no last resort while this peer is unparked, and once its promise fell due nothing asking
// again, a stall.

import "github.com/libp2p/go-libp2p/core/peer"

// seacastOfferDropped is the path's part of dropping p's offer of mid: a queued ask of mid to p is
// dropped from p's queue, an open promise closes without a charge; true when either was
// open, so the caller runs the retry without p. Router goroutine.
func (gs *GossipSubRouter) seacastOfferDropped(p peer.ID, mid string) bool {
	if root, g, i, ok := gs.pullChunk(mid); ok {
		return gs.pullOfferDropped(p, root, g, i) // a chunk: drop_offer in the core (seacast_pull.go)
	}
	sc := gs.seacast
	if sc == nil {
		return false
	}
	open := false
	if _, queued := sc.askQueued[mid][p]; queued {
		gs.seacastDropQueuedAsk(p, mid)
		open = true
	}
	if _, promised := gs.iwantPromised[mid][p]; promised {
		gs.absolvePromise(mid, p)
		sc.ctr.F13Closed.Add(1)
		open = true
	}
	return open
}

// seacastDropQueuedAsk drops p's queued ask of mid with p's offer: the ask leaves p's queue, unless the
// writer has popped it already, and its record closes, so a write reported later opens no promise.
// The score's promise pending its write goes too, as for an ask the queue refused, so neither the
// park nor the score holds p to it. Nothing is left behind for the writer to read. Router goroutine.
func (gs *GossipSubRouter) seacastDropQueuedAsk(p peer.ID, mid string) {
	if gs.p != nil {
		if q, ok := gs.p.peers[p]; ok {
			q.stripAsk(mid)
		}
	}
	gs.seacastAskClosed(p, mid)
	gs.gossipTracer.asksRefused(p, []string{mid})
	gs.seacast.ctr.F13AskDropped.Add(1)
}
