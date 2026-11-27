package pubsub

// Ties, the deferred store and the gate's opening (seacast.md §4.2; seacast-lite.md §4.2, §6.1, §8.1
// and §8.2), through the core's Ties, Pending, OpenGate and DropStaleWaits. A copy that cannot be
// checked yet, kept in the deferred store or refused at its bound, and another copy of a kept id, tie
// their sender to the root they name for the index: the sender answered, with a copy that cannot be
// checked, so its promise for the index closes without a charge, and it is not asked for the index
// again while its copy waits (the offer handler, the retry, the last resort and the re-drive pass it
// over). The gate's opening (the application's SeacastHeaderValidated) resumes the kept copies first,
// then releases the ties: each sender's park for the group lifts, its last-resort count for the index
// clears, and the index is its offer again where the node doesn't hold it; then the indices of the
// group offered and not held are retried (the ledger). A copy or a tie whose slot leaves the live
// window, or whose group expires, goes at the heartbeat (drop_stale_waits), a tie through drop_tie:
// its sender's offers of the indices go, and the recovery goes on without it where the group can be
// checked. A peer's ties go with its connection; its kept copies stay, by root. The copy's message
// is the binding's (waitMsg).

import (
	"maps"
	"math"
	"slices"

	core "github.com/cskiraly/seacast-go"
	"github.com/libp2p/go-libp2p/core/peer"
)

// noSlot is the slot of a copy whose type names none (no ChunkSlotter): it never leaves the live
// window, and goes with its group.
const noSlot = math.MaxUint64

// rootKey is root as the core's tie key: the 32 bytes the group's root is.
func rootKey(root string) (k [32]byte) {
	copy(k[:], root)
	return k
}

// seacastChunk is the copy mid of index of root as the core's chunk; Ref carries mid, which the
// type reads the slot from.
func seacastChunk(root string, index uint32, mid string) core.Chunk {
	return core.Chunk{Group: seacastGID(root), Index: index, Ref: mid}
}

// waitEnv is the core's TieEnv, DeferEnv, ReleaseEnv, StaleEnv and GateEnv over the router. A copy's
// key is the root its id names, and its group that root's. mid is the copy a tie is made for, whose
// promise and asks it closes; root is the root whose gate opens, for Commits. Router goroutine.
type waitEnv struct {
	gs   *GossipSubRouter
	mid  string
	root string
}

func (e waitEnv) owner(g core.GroupID) (string, ObjectType) {
	root := seacastGIDRoot(g)
	return root, e.gs.seacastRootOwner(root)
}

func (e waitEnv) Root(c core.Chunk) [32]byte { return rootKey(seacastGIDRoot(c.Group)) }

// Slot is the slot the copy names, where its type reads it, else noSlot.
func (e waitEnv) Slot(c core.Chunk) uint64 {
	_, T := e.owner(c.Group)
	mid, _ := c.Ref.(string)
	if cs, ok := T.(ChunkSlotter); ok && mid != "" {
		if s, ok := cs.ChunkSlot(mid); ok {
			return s
		}
	}
	return noSlot
}

// Expired says whether g's group is past its lifetime, without registering its root.
func (e waitEnv) Expired(g core.GroupID) bool {
	root, _ := e.owner(g)
	return e.gs.seacastWaitExpired(root)
}

// Live says whether slot is in the node's live window (seacast-lite.md §8.1's live): not past it, and
// not a later slot than the node's. Always before the first advance, and for noSlot.
func (e waitEnv) Live(slot uint64) bool {
	sc := e.gs.seacast
	return slot == noSlot || !sc.slotKnown || (slot <= sc.slot && slot+sc.limits.LiveWindow > sc.slot)
}

// Admitted is the ties' room for another group of p's (MAX_PENDING_GROUPS): p is tied for g already,
// or tied for fewer groups than the limit.
func (e waitEnv) Admitted(p peer.ID, g core.GroupID) bool {
	sc := e.gs.seacast
	return sc.ties.TieRoom(p, rootKey(seacastGIDRoot(g)), g, 0, 0, false, sc.limits.UnknownGroupsPerPeer)
}

func (e waitEnv) IndexCount(g core.GroupID) (uint32, bool) {
	root, T := e.owner(g)
	if T == nil || !T.HeaderKnown(root) {
		return 0, false
	}
	n := T.IndexCount(root)
	if n <= 0 {
		return 0, false
	}
	return uint32(n), true // lint:ignore uintcast -- n > 0, an index count.
}

// ClosePromiseOf closes p's promise for the tie's copy without a charge, and nothing of ours is open
// to p for it any more: it answered. An ask of the chunk still queued to p goes too
// (seacastDropTiedAsks).
func (e waitEnv) ClosePromiseOf(_ core.GroupID, _ uint32, p peer.ID) {
	e.gs.absolvePromise(e.mid, p)
	e.gs.seacastAskClosed(p, e.mid)
	e.gs.seacastDropTiedAsks(p, e.mid)
}

// Verify is level 1's gate, again: the type's header known, or the copy defers.
func (e waitEnv) Verify(c core.Chunk) core.Verdict {
	if root, T := e.owner(c.Group); T != nil && T.HeaderKnown(root) {
		return core.VerdictAccept
	}
	return core.VerdictDefer
}

// DropOffer drops p's offers of chunk i (drop_offer): a chunk's in the core, else under each of its
// ids, with their queued asks and promises. Without the chunk pull an offer is kept under the id it
// named, which may be one chunkForms doesn't know (another copy's id for the index, the tied copy's
// own among them): every id the offer table holds for the index goes too.
func (e waitEnv) DropOffer(p peer.ID, g core.GroupID, i uint32) {
	root := seacastGIDRoot(g)
	if e.gs.pullOn() {
		e.gs.seacastDropOffer(p, e.gs.chunkAskID(root, i), root, i)
		return
	}
	mids := map[string]struct{}{}
	for _, mid := range e.gs.chunkForms(root, i) {
		mids[mid] = struct{}{}
	}
	for mid := range e.gs.offerTable {
		if r, j, ok := e.gs.groupIndex(mid); ok && r == root && j == i {
			mids[mid] = struct{}{}
		}
	}
	for _, mid := range slices.Sorted(maps.Keys(mids)) {
		e.gs.seacastDropOffer(p, mid, root, i)
	}
}

func (e waitEnv) Authenticable(g core.GroupID) bool {
	root, T := e.owner(g)
	return T != nil && T.HeaderKnown(root)
}

// RetryOffer is retry_offer for chunk i: the core's for a chunk, else the offer table's under its id.
func (e waitEnv) RetryOffer(g core.GroupID, i uint32) {
	root := seacastGIDRoot(g)
	if e.gs.pullOn() {
		e.gs.pullRetry(g, i)
		return
	}
	if mid := e.gs.chunkAskID(root, i); e.gs.offerTable != nil && !e.gs.haveSeen(mid) {
		e.gs.retryOfferedPull(mid)
	}
}

// Commits says whether the root whose gate opens commits g: a tie is keyed by its group's own root.
func (e waitEnv) Commits(g core.GroupID) bool { return seacastGIDRoot(g) == e.root }

// LiftPark lifts p's park for g, the core's; a router whose chunk pull is off keeps its parks.
func (e waitEnv) LiftPark(p peer.ID, g core.GroupID) {
	if e.gs.pullOn() {
		e.gs.pull.promises.Unpark(p, g)
	}
}

func (e waitEnv) ClearAskedParked(g core.GroupID, i uint32, p peer.ID) { e.gs.pull.lr.Discard(g, i, p) }

// Joined says whether p may offer the group: connected, and joined to the root's topic where the
// router placed it.
func (e waitEnv) Joined(p peer.ID, g core.GroupID) bool {
	if _, connected := e.gs.peers[p]; !connected {
		return false
	}
	topic, placed := e.gs.seacast.rootTopic[seacastGIDRoot(g)]
	if !placed {
		return true
	}
	_, joined := e.gs.p.topics[topic][p]
	return joined
}

func (e waitEnv) Holds(g core.GroupID, i uint32) bool { return e.gs.chunkHeld(seacastGIDRoot(g), i) }

// AddOffer records p's offer of chunk i again, under the id an ask of it carries.
func (e waitEnv) AddOffer(p peer.ID, g core.GroupID, i uint32) {
	root := seacastGIDRoot(g)
	e.gs.recordOffer(e.gs.chunkAskID(root, i), e.gs.seacast.rootTopic[root], p)
	e.gs.seacast.ctr.TiesToOffers.Add(1)
	if !e.gs.pullOn() {
		e.RetryOffer(g, i) // no ledger without the chunk pull: the offer's retry at once, as before
	}
}

// Resume pushes a kept copy into the receive path: validated as the arrival it is and, at ACCEPT,
// filled (on_validated). A copy whose group expired while it waited, or whose own slot left the live
// window, goes, as on_validated drops it. A copy naming an index the header does not admit (a
// forgery) resumes level 1 too, which fails it: the failure is its sender's.
func (e waitEnv) Resume(w core.Waiting[peer.ID]) {
	gs, sc := e.gs, e.gs.seacast
	msg := sc.waitMsg[w.ID]
	delete(sc.waitMsg, w.ID)
	if msg == nil {
		return
	}
	root := seacastGIDRoot(w.Chunk.Group)
	if gs.seacastRootExpired(root) || !e.Live(w.Slot) {
		sc.ctr.Expired.Add(1)
		return
	}
	sc.ctr.Released.Add(1)
	// The receive path's two steps, as handleIncomingRPC runs them for an admitted message.
	if gs.p.shouldPush(msg) {
		gs.Preprocess(w.Peer, []*Message{msg})
		gs.p.pushMsg(msg)
	}
}

func (e waitEnv) DropOutside(core.GroupID) {} // seacast-lite.md has no asks from before the gate

// Ledger is the indices of g offered by a connected peer and not held; none without the chunk pull,
// whose offers AddOffer retries at once.
func (e waitEnv) Ledger(g core.GroupID) []uint32 {
	if !e.gs.pullOn() {
		return nil
	}
	peers := make([]peer.ID, 0, len(e.gs.peers))
	for p := range e.gs.peers {
		peers = append(peers, p)
	}
	slices.Sort(peers)
	root := seacastGIDRoot(g)
	return e.gs.pull.offers.Ledger(g, peers, func(i uint32) bool { return e.gs.chunkHeld(root, i) })
}

// seacastWaitExpired says whether root's group is past its lifetime, without registering the root:
// a tombstone, or its owner's slot out of the window. It reads the owner, never the types' TopicOf (as
// gs.expired does through seacastRootType): the heartbeat must not take up again a root the window
// forgot.
func (gs *GossipSubRouter) seacastWaitExpired(root string) bool {
	sc := gs.seacast
	if _, dead := sc.expired[root]; dead {
		return true
	}
	T := gs.seacastRootOwner(root)
	if T == nil || !sc.slotKnown {
		return false
	}
	s, ok := T.SlotOf(root)
	return ok && s+sc.limits.LiveWindow <= sc.slot
}

// seacastTie ties from to root for index (Ties.Tie): from's promise for mid closes without a charge.
func (gs *GossipSubRouter) seacastTie(from peer.ID, root string, index uint32, mid string) {
	sc := gs.seacast
	g := seacastGID(root)
	was := sc.ties.Tied(from, g, index)
	if sc.ties.Tie(waitEnv{gs: gs, mid: mid}, from, seacastChunk(root, index, mid), sc.limits.UnknownGroupsPerPeer) && !was {
		sc.ctr.Tied.Add(1)
	}
}

// seacastTiedTo says whether p's copy of mid waits for its root: p is not asked for it meanwhile.
func (gs *GossipSubRouter) seacastTiedTo(p peer.ID, mid string) bool {
	if gs.seacast == nil {
		return false
	}
	root, index, ok := gs.groupIndex(mid)
	if !ok {
		return false
	}
	return gs.seacastTiedIndex(p, root, index)
}

// seacastDropTiedAsks takes out of p's queue, in either lane, its asks of mid's chunk in any form that
// names the chunk's root and index (seacastAskCell): p's copy of it waits for its root, and the spec
// drops an ask to such a peer before it is written (seacast-lite.md §7.2's filter_entry,
// tied_to_root). A probe's ask goes too: seacast.md's filter_entry tests tied_to_root after the
// probe's exemptions, and a probe never written gets no verdict (seacastProbeVerdict). Each closes as
// an ask dropped at the queue, the score's pending promise with it (noteIWantDropped's). Asks of the
// chunk made after the tie are refused where they are made (seacastTiedTo, env.Tied, seacastAsk).
// Router goroutine.
func (gs *GossipSubRouter) seacastDropTiedAsks(p peer.ID, mid string) {
	root, index, ok := gs.groupIndex(mid)
	if !ok || gs.p == nil || gs.seacast == nil {
		return
	}
	q, ok := gs.p.peers[p]
	if !ok {
		return
	}
	dropped := q.discardAsks(func(id string) bool {
		r, i, ok := gs.seacastAskCell(id)
		return ok && r == root && i == index
	})
	for _, id := range dropped {
		gs.seacastAskClosed(p, id)
	}
	if len(dropped) > 0 {
		gs.gossipTracer.asksRefused(p, dropped)
	}
}

// seacastTiedIndex says whether p's copy of index of root waits for its root.
func (gs *GossipSubRouter) seacastTiedIndex(p peer.ID, root string, index uint32) bool {
	sc := gs.seacast
	return sc != nil && sc.ties.Tied(p, seacastGID(root), index)
}

// seacastDropOffer drops p's offers of mid (seacast-lite.md's drop_tie, F13): p leaves the offer
// table's entry and its ordered list, its per-group offer bit for the index is cleared, and its
// promise for mid closes without a charge. The caller runs the retry without p.
func (gs *GossipSubRouter) seacastDropOffer(p peer.ID, mid, root string, index uint32) {
	if _, g, i, ok := gs.pullChunk(mid); ok {
		gs.pullOfferDropped(p, root, g, i) // a chunk: drop_offer in the core (seacast_pull.go)
		return
	}
	if peers := gs.offerTable[mid]; peers != nil {
		if _, ok := peers[p]; ok {
			delete(peers, p)
			if gs.offerByPeer[p] > 0 {
				gs.offerByPeer[p]--
			}
			if len(peers) == 0 {
				gs.clearOffers(mid)
			}
		}
	}
	gs.seacastOfferDropped(p, mid) // F13: its queued ask and promise go with the offer
	gs.absolvePromise(mid, p)
}

// noteGateMet records that something met root's closed gate: its opening has work to do.
func (sc *seacast) noteGateMet(root string) {
	if sc.gateMet == nil {
		sc.gateMet = map[string]struct{}{}
	}
	sc.gateMet[root] = struct{}{}
}

// seacastOpenGate is the gate's opening for root (core.OpenGate): its kept copies resume, its ties
// release, and its group's ledger is retried. Only a gate something met closed opens: a type whose
// gate never closes (HeaderKnown true from the start, the descriptor in each chunk) tells the path
// the count, not an opening, and nothing was offered meanwhile; its ledger is not retried, so an ask
// the limits held back waits for the heartbeat as before. Router goroutine, after drop_beyond.
func (gs *GossipSubRouter) seacastOpenGate(root string) {
	sc := gs.seacast
	if _, met := sc.gateMet[root]; !met {
		return
	}
	delete(sc.gateMet, root)
	g := seacastGID(root)
	core.OpenGate[peer.ID](waitEnv{gs: gs, root: root}, &sc.waits, &sc.ties, rootKey(root),
		func(h core.GroupID) bool { return h == g }, []core.GroupID{g})
}

// seacastDropStaleWaits is drop_stale_waits (core.DropStaleWaits), at the heartbeat: a kept copy, or
// a sender's ties to a root, whose group expired or whose slot left the live window go, a tie
// through drop_tie. Router goroutine.
func (gs *GossipSubRouter) seacastDropStaleWaits() {
	sc := gs.seacast
	if sc == nil || (sc.waits.Len() == 0 && sc.ties.Pairs() == 0) {
		return
	}
	pairs := sc.ties.Pairs()
	gone := core.DropStaleWaits[peer.ID](waitEnv{gs: gs}, &sc.waits, &sc.ties)
	for _, w := range gone {
		delete(sc.waitMsg, w.ID)
	}
	sc.ctr.StaleWaits.Add(int64(len(gone) + pairs - sc.ties.Pairs()))
}

// seacastForgetWaits drops root's ties and kept copies at once: its group expired, was cleared or
// left the window (Ties.ForgetRoot, Pending.ForgetRoot). Router goroutine.
func (gs *GossipSubRouter) seacastForgetWaits(root string) {
	sc := gs.seacast
	k := rootKey(root)
	delete(sc.gateMet, root)
	sc.ties.ForgetRoot(k)
	for _, w := range sc.waits.ForgetRoot(k) {
		delete(sc.waitMsg, w.ID)
		sc.ctr.Expired.Add(1)
	}
}

// seacastDropFilledWait takes a kept copy of mid out of the deferred store: the chunk was filled under
// mid another way (the node's own publication), so the copy waits for nothing, and its sender's
// reservation goes back. Router goroutine, at a fill.
func (gs *GossipSubRouter) seacastDropFilledWait(root, mid string) {
	sc := gs.seacast
	if _, ok := sc.waits.Drop(rootKey(root), mid); ok {
		delete(sc.waitMsg, mid)
	}
}

// seacastPeerGone drops a connection's ties, its IHAVE counts, its probe state, its queued asks'
// records and its admission registrations (per-connection state goes with the connection, v0.3
// §8.1). Its kept copies stay, by root (seacast-lite.md §8.1's table). Router goroutine, from
// RemovePeer.
func (gs *GossipSubRouter) seacastPeerGone(p peer.ID) {
	sc := gs.seacast
	if sc == nil {
		return
	}
	sc.ties.Forget(p)
	sc.ihave.Forget(p) // the IHAVE limits' counts are the connection's (seacast-lite.md §8.1)
	gs.seacastProbeGone(p)
	for mid, peers := range sc.askQueued {
		delete(peers, p)
		if len(peers) == 0 {
			delete(sc.askQueued, mid)
			gs.pullAskFormGone(mid)
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
	p.seacastRead(func(gs *GossipSubRouter) { n = gs.seacast.ties.Pairs() })
	return n
}
