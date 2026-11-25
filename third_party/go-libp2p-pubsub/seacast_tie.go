package pubsub

// Ties (seacast.md §4.2; seacast-lite.md §4.2, §6.1 and §8.1). A copy
// that cannot be checked yet, kept in the pending store or refused at its bound, and another copy
// of a pending id, tie their sender to the root they name for the index: the sender answered, with
// a copy that cannot be checked, so its promise for the index closes without a charge, and it is
// not asked for the index again while its copy waits (the offer handler, the retry, the last resort
// and the re-drive pass it over). The header's validation releases the root's ties with its copies;
// a tie whose copy was refused at the bound becomes an offer of the index, asked for with the rest
// if still wanted; a pending copy the header does not admit (an index beyond the type's count) is a
// forgery, dropped with its tie, and its sender's offers of the index go with it (F13's queued ask,
// seacast_f13.go), the recovery going on without it; a tie whose slot leaves the window goes with its
// group. A peer's pending copies and ties go with its connection, its reservations released, so a
// reconnected peer's copies are evaluated afresh.

import "github.com/libp2p/go-libp2p/core/peer"

// seacastTie ties from to root for index; from's promise for mid closes without a charge.
func (gs *GossipSubRouter) seacastTie(from peer.ID, root string, index uint32, mid string) {
	sc := gs.seacast
	if sc.tied == nil {
		sc.tied = map[string]map[peer.ID]map[uint32]string{}
	}
	byPeer := sc.tied[root]
	if byPeer == nil {
		byPeer = map[peer.ID]map[uint32]string{}
		sc.tied[root] = byPeer
	}
	ids := byPeer[from]
	if ids == nil {
		ids = map[uint32]string{}
		byPeer[from] = ids
		gs.seacastNoteTieSlot(from, root, mid) // the slot this first copy named (tie_slot)
	}
	if _, tied := ids[index]; !tied {
		ids[index] = mid
		sc.ctr.Tied.Add(1)
	}
	gs.absolvePromise(mid, from)
	gs.seacastAskClosed(from, mid) // it answered: nothing of ours is open to it for the index
}

// seacastTiedTo says whether p's copy of mid waits for its root: p is not asked for it meanwhile.
func (gs *GossipSubRouter) seacastTiedTo(p peer.ID, mid string) bool {
	sc := gs.seacast
	if sc == nil || sc.tied == nil {
		return false
	}
	root, index, ok := gs.groupIndex(mid)
	if !ok {
		return false
	}
	_, tied := sc.tied[root][p][index]
	return tied
}

// seacastDropOffer drops p's offers of mid (seacast-lite.md's drop_tie, F13): p leaves the offer
// table's entry and its ordered list, its per-group offer bit for the index is cleared, and its
// promise for mid closes without a charge. The caller runs the retry without p.
func (gs *GossipSubRouter) seacastDropOffer(p peer.ID, mid, root string, index uint32) {
	if peers := gs.offerTable[mid]; peers != nil {
		if _, ok := peers[p]; ok {
			delete(peers, p)
			if gs.offerByPeer[p] > 0 {
				gs.offerByPeer[p]--
			}
			if gs.v05LastResort() {
				gs.dropOrder(mid, p)
			}
			if len(peers) == 0 {
				gs.clearOffers(mid)
			}
		}
	}
	if gs.v05 != nil && gs.v05.offered != nil {
		if g := gs.v05.offered[p][root]; g != nil {
			g.bits.Clear(index)
		}
	}
	gs.seacastOfferDropped(p, mid) // F13: its queued ask and promise go with the offer
	gs.absolvePromise(mid, p)
}

// seacastReleaseTies ends the root's ties at the header's validation: a tie whose copy was refused
// at the bound becomes an offer of the index (v0.3's on_block_installed), and the retry asks for it
// with the rest if still wanted. The pending copies themselves resume in seacastRelease.
func (gs *GossipSubRouter) seacastReleaseTies(root string, kept map[peer.ID]map[uint32]bool) {
	sc := gs.seacast
	byPeer := sc.tied[root]
	if byPeer == nil {
		return
	}
	delete(sc.tied, root)
	T := gs.seacastRootType(root)
	topic := sc.rootTopic[root]
	for p, ids := range byPeer {
		if _, connected := gs.peers[p]; !connected {
			continue
		}
		for index, mid := range ids {
			if kept[p][index] {
				continue // its copy resumes level 1 now
			}
			if T != nil && int(index) >= T.IndexCount(root) {
				continue // a forgery's tie: nothing to ask for
			}
			if gs.haveSeen(mid) {
				continue
			}
			gs.recordOffer(mid, topic, p)
			if gs.v05LastResort() {
				gs.noteOffered(p, topic, mid)
			}
			sc.ctr.TiesToOffers.Add(1)
			gs.retryOfferedPull(mid)
		}
	}
}

// seacastPeerGone drops a connection's pending copies, ties and admission registrations
// (per-connection state goes with the connection, v0.3 §8.1). Router goroutine, from RemovePeer.
func (gs *GossipSubRouter) seacastPeerGone(p peer.ID) {
	sc := gs.seacast
	if sc == nil {
		return
	}
	for mid, e := range sc.pending {
		if e.from == p {
			delete(sc.pending, mid)
			sc.ctr.PendingGone.Add(1)
		}
	}
	delete(sc.pendingOf, p)
	gs.seacastPeerDropsForgotten(p)
	gs.seacastProbeGone(p)
	for mid, peers := range sc.askQueued {
		delete(peers, p)
		if len(peers) == 0 {
			delete(sc.askQueued, mid)
		}
	}
	for root, byPeer := range sc.tied {
		if _, ok := byPeer[p]; ok {
			delete(byPeer, p)
			if len(byPeer) == 0 {
				delete(sc.tied, root)
			}
		}
	}
	for root, g := range sc.asked {
		for index, peers := range g {
			delete(peers, p)
			if len(peers) == 0 {
				delete(g, index)
			}
		}
		if len(g) == 0 {
			delete(sc.asked, root)
		}
	}
}

// SeacastTies is the (root, peer) ties held, for the fixtures. Any goroutine.
func (p *PubSub) SeacastTies() int {
	n := 0
	p.seacastRead(func(gs *GossipSubRouter) {
		for _, byPeer := range gs.seacast.tied {
			for _, ids := range byPeer {
				if len(ids) > 0 {
					n++
				}
			}
		}
	})
	return n
}

// seacastNoteTieSlot records the slot from's first copy under root names, where the type reads it
// (seacast-lite.md §4.2's tie_slot). A new first tie replaces what an earlier, released one left.
func (gs *GossipSubRouter) seacastNoteTieSlot(from peer.ID, root, mid string) {
	sc := gs.seacast
	cs, ok := gs.seacastRootType(root).(ChunkSlotter)
	if !ok {
		return
	}
	s, ok := cs.ChunkSlot(mid)
	if !ok {
		return
	}
	if sc.tieSlot == nil {
		sc.tieSlot = map[string]map[peer.ID]uint64{}
	}
	if sc.tieSlot[root] == nil {
		sc.tieSlot[root] = map[peer.ID]uint64{}
	}
	sc.tieSlot[root][from] = s
}
