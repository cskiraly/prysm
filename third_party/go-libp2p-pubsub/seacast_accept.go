package pubsub

// The accepted-chunk path (seacast.md §4.2): on the path's topics a chunk's effects wait for
// its verdict. Today Preprocess, before
// validation, notes the arrival in the holdings, closes the ask it was made under and queues the
// IDONTWANT holding to the mesh; the validation marks the id seen before the user validators; an
// accepted message reaches the router through publishMessage; a seen id's later copy is
// acknowledged whatever its verdict. Here, for an owned topic:
//
//   - At admission, before the seen check and validation, a chunk of a group whose gate is closed
//     (HeaderKnown false) is kept in a pending store, per peer and bounded, and enters nothing: not
//     seen, not validated; a second copy of a pending id is dropped. Preprocess does none of the
//     effects above for the path's messages.
//   - The verdict is seen through the validation's hook: REJECT marks the id rejected, so its later
//     copies are dropped unacknowledged, and counts the penalty (the fork runs no scoring).
//   - SeacastHeaderValidated(roots) pushes the pending copies of those groups into the receive
//     path as the fresh arrivals they are, so they are validated and, at ACCEPT, filled.
//   - At ACCEPT, in Publish (the relay path): the fill. The holding noted and the id learned, the
//     ask closed, the holding sent as v4 to the suppression scope (the topic's mesh, the relation on
//     v0.3's wire) and to the sender as its acknowledgement; no relay HAVE.
//   - SeacastExpire(roots) drops the pending copies of the groups named instead.
//   - shouldPush's duplicate acknowledgement runs only for an id the path accepted.

import (
	"fmt"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
)

// SeacastLimits bounds the path's state (X6: limits before state).
type SeacastLimits struct {
	// PendingPerPeer caps the chunks kept per peer while their group's gate is closed.
	PendingPerPeer int
	// LiveWindow is the live window in slots (v0.3 §8.1, seacast_window.go): a group whose slot is
	// this many behind the node's current slot has left it. More slots than a group needs to reach
	// every honest node from its slot's start; the harness's groups complete within 2 s of a 12 s
	// slot, so 2.
	LiveWindow uint64
	// UnknownGroupsPerPeer is MAX_PENDING_GROUPS (seacast-lite.md §8.2; spec f6c19db): the groups whose
	// header is not yet validated that a peer's deferred copies may name. A copy of a further one is
	// ignored, neither kept nor tied (seacast.md §4.2's defer, admitted). Zero: no limit, the
	// toy's default ("the client's").
	UnknownGroupsPerPeer int
}

// DefaultSeacastLimits is the toy's: a closed gate holds a block's worth of a column per peer; the
// window two slots.
var DefaultSeacastLimits = SeacastLimits{PendingPerPeer: 64, LiveWindow: 2}

// WithSeacastLimits sets the path's limits (after WithSeacast): the harness's cells set the live
// window from their configuration.
func WithSeacastLimits(limits SeacastLimits) Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok || gs.seacast == nil {
			return fmt.Errorf("seacast limits need WithSeacast before them")
		}
		if limits.PendingPerPeer <= 0 || limits.LiveWindow == 0 {
			return fmt.Errorf("seacast limits: pending per peer %d and live window %d must be positive", limits.PendingPerPeer, limits.LiveWindow)
		}
		gs.seacast.limits = limits
		return nil
	}
}

// seacastPending is one copy kept while its gate is closed.
type seacastPending struct {
	from peer.ID
	msg  *Message
	root string
	// slot is the slot the copy names, where the type reads it (slotOK): drop_stale_waits drops the copy
	// when it leaves the live window.
	slot   uint64
	slotOK bool
}

// seacastPreprocess is the path's part of Preprocess for one message: true when the message is
// the path's and Preprocess is to do nothing else with it (its effects wait for the verdict).
func (gs *GossipSubRouter) seacastPreprocess(from peer.ID, msg *Message) bool {
	if gs.seacastType(msg.GetTopic()) == nil {
		return false
	}
	mid := gs.p.idGen.ID(msg)
	gs.seacastNoteRoot(msg.GetTopic(), mid)
	if msg.Local || from == gs.p.host.ID() {
		gs.seacast.ctr.Own.Add(1) // the node's own publication passing Preprocess (§4.7), not an arrival
		gs.seacastNoteOwn(mid)    // its header the node's own (seacast_order.go)
	} else {
		gs.seacast.ctr.Arrived.Add(1)
	}
	return true
}

// seacastAdmit is the path's gate at admission (the receive goroutine, before the seen check and
// validation): false keeps the message out, in the pending store while its group's header is not
// validated, or dropped as a second copy of a pending id or over the per-peer bound. Off the
// path's topics, true.
func (gs *GossipSubRouter) seacastAdmit(msg *Message) bool {
	T := gs.seacastType(msg.GetTopic())
	if T == nil || gs.have == nil {
		return true
	}
	if msg.Local || msg.ReceivedFrom == gs.p.host.ID() {
		return true // our own data: stored and seen without reception (§4.7), never pending
	}
	mid := gs.p.idGen.ID(msg)
	gs.seacastNoteRoot(msg.GetTopic(), mid)
	root, index, ok := gs.have.index(mid)
	if !ok || len(root) != haveRootLen {
		return true
	}
	sc := gs.seacast
	if expired, future := gs.seacastRootState(root); expired {
		sc.ctr.ExpiredDropped.Add(1) // after the expiry, or in validation at it; or out of the window: no fill
		return false
	} else if future {
		sc.ctr.Future.Add(1) // a chunk of a later slot: ignored, as a block from a future slot is
		return false
	}
	if _, dup := sc.pending[mid]; dup {
		sc.ctr.PendingDup.Add(1)
		if gs.seacastAdmitted(msg.ReceivedFrom, root) {
			gs.seacastTie(msg.ReceivedFrom, root, index, mid) // another copy of one that waits: its sender too
		}
		return false
	}
	if T.HeaderKnown(root) {
		return true // the gate is open: validated now, filled at ACCEPT
	}
	from := msg.ReceivedFrom
	if !gs.seacastAdmitted(from, root) {
		sc.ctr.UnknownRefused.Add(1) // a group beyond the sender's unknown groups: neither kept nor tied
		return false
	}
	if sc.pending == nil {
		sc.pending = map[string]*seacastPending{}
		sc.pendingOf = map[peer.ID]int{}
	}
	if sc.pendingOf[from] >= sc.limits.PendingPerPeer {
		sc.ctr.PendingRefused.Add(1)
		gs.seacastTie(from, root, index, mid) // refused, not kept: its sender tied all the same
		return false
	}
	e := &seacastPending{from: from, msg: msg, root: root}
	if cs, ok := T.(ChunkSlotter); ok {
		e.slot, e.slotOK = cs.ChunkSlot(mid)
	}
	sc.pending[mid] = e
	sc.pendingOf[from]++
	sc.ctr.Pending.Add(1)
	gs.seacastTie(from, root, index, mid) // kept: its sender tied to the root it names
	return false
}

// seacastAdmitted is seacast.md §4.2's admitted (seacast-lite.md §8.2's MAX_PENDING_GROUPS; spec f6c19db):
// whether p's deferred copy of root's group may be kept or tie p. p's unknown groups are the roots it is
// tied to, whose headers are not yet validated (a root's ties go at its header's validation). Router side
// of admission; the receive goroutine reads the tie table as seacastAdmit's other checks do.
func (gs *GossipSubRouter) seacastAdmitted(p peer.ID, root string) bool {
	sc := gs.seacast
	lim := sc.limits.UnknownGroupsPerPeer
	if lim <= 0 {
		return true
	}
	if _, tied := sc.tied[root][p]; tied {
		return true
	}
	n := 0
	for r, byPeer := range sc.tied {
		if _, ok := byPeer[p]; ok && r != root {
			n++
		}
	}
	return n < lim
}

// seacastVerdict is the validation's hook: the verdict of a message on the path's topics.
// Router goroutine (through eval); a REJECT marks the id so its copies are never acknowledged.
func (gs *GossipSubRouter) seacastVerdict(src peer.ID, msg *Message, result ValidationResult) {
	if gs.seacastType(msg.GetTopic()) == nil {
		return
	}
	mid := gs.p.idGen.ID(msg)
	sc := gs.seacast
	switch result {
	case ValidationReject:
		if sc.rejected == nil {
			sc.rejected = map[string]struct{}{}
		}
		sc.rejected[mid] = struct{}{}
		sc.ctr.Rejected.Add(1)
		sc.dropPending(mid)
	case ValidationIgnore:
		sc.ctr.Ignored.Add(1)
	}
}

// seacastAccepted is the fill, from Publish at ACCEPT of a message on the path's topics: the
// holding and the id, the ask it closes, the holding to the suppression scope and the sender.
func (gs *GossipSubRouter) seacastAccepted(msg *Message) {
	from := msg.ReceivedFrom
	mid := gs.p.idGen.ID(msg)
	sc := gs.seacast
	sc.dropPending(mid)
	gs.noteArrival(mid, from)
	gs.iwantDelivered(mid)
	gs.haveNoteArrival(mid)
	gs.haveArrived(mid, from)
	sc.ctr.Accepted.Add(1)
	h := gs.have
	if h == nil {
		return
	}
	root, index, ok := h.index(mid)
	if !ok || len(root) != haveRootLen {
		return
	}
	gs.seacastProbeCopy(root, index, from) // (R5) the claimant's own copy passes its probe
	T := gs.seacastType(msg.GetTopic())
	if g := sc.groups[root]; g != nil && g.state == seacastInvalid {
		T = nil // an INVALID group is not taken up again (§4.2's fill, seacast.md §4.1)
	}
	if T != nil {
		gs.seacastOrderFill(T, root, index)              // (i) the gate before the fill (seacast_order.go)
		gs.seacastStorePut(T, root, index, msg)          // the fill's store half: served after mcache lets go
		gs.seacastServeStanding(T, root, index, msg)     // §4.10: the chunk a standing request waited for is here
		gs.seacastServeWaitingProbe(T, root, index, msg) // (R5) a probe that reached us while we decoded
		gs.seacastCheckCompletion(T, root)               // under the machine: the holding below carries its claims
	}
	// [c7] the holding to whoever may push this group to us, on v0.3's wire the topic's mesh;
	// [c14] and to the sender, as its acknowledgement. Both leave exactly as the arrival HAVE
	// does from Preprocess (the same lane, the same coalescing); the holding's encoding is v4 on the
	// path's roots (haveIDontWantIDs). The stock IDONTWANT's size threshold does not apply: the
	// draft's holding has no size rule, and a group's holding is one bitmap whatever the chunk's
	// size.
	topic := msg.GetTopic()
	gs.idontwantMesh(topic, []string{mid}, from)
	if from != gs.p.host.ID() && gs.feature(GossipSubFeatureIdontwant, gs.peers[from]) {
		// The sender's acknowledgement, the holding ([c14]: the sender always hears it).
		ids := gs.haveIDontWantIDs([]string{mid})
		if len(ids) > 0 {
			gs.sendRPC(from, rpcWithControl(nil, nil, nil, nil, nil, []*pb.ControlIDontWant{{MessageIDs: ids}}), true)
		}
	}
	sc.ctr.HoldingsSent.Add(1)
}

// seacastAckDuplicate acknowledges a later copy of a seen id ([c14]: duplicates are acknowledged
// too, draft §4.2 and §4.10): off the path the hose's acknowledgement as today; on the path, for an
// id the path accepted (seacastAckAllowed), the hose's stamped acknowledgement where the hose runs
// and the plain v4 holding otherwise, as the first acceptance sends it (seacastAccepted), so the
// sender always hears it. DupAcked counts at the send. processLoop goroutine, from shouldPush.
func (gs *GossipSubRouter) seacastAckDuplicate(mid string, from peer.ID) {
	if !gs.seacastAckAllowed(mid) {
		return
	}
	if gs.seacast != nil && gs.have != nil {
		if root, index, ok := gs.have.index(mid); ok && len(root) == haveRootLen {
			gs.seacastProbeCopy(root, index, from) // (R5) a duplicate from the claimant passes its probe too
		}
	}
	onPath := false
	if gs.seacast != nil && gs.have != nil {
		if root, _, ok := gs.have.index(mid); ok && len(root) == haveRootLen && gs.seacastRootType(root) != nil {
			onPath = true
		}
	}
	if !onPath || gs.hoseAck {
		if gs.hoseAckDuplicate(mid, from) && onPath {
			gs.seacast.ctr.DupAcked.Add(1)
		}
		return
	}
	if from == gs.p.host.ID() || !gs.feature(GossipSubFeatureIdontwant, gs.peers[from]) {
		return
	}
	ids := gs.haveIDontWantIDs([]string{mid})
	if len(ids) == 0 {
		return
	}
	gs.sendRPC(from, rpcWithControl(nil, nil, nil, nil, nil, []*pb.ControlIDontWant{{MessageIDs: ids}}), true)
	gs.seacast.ctr.DupAcked.Add(1)
}

// seacastAckAllowed says whether a seen id's later copy may be acknowledged: off the path always;
// on the path only for an id the path accepted, never for a pending or rejected one.
func (gs *GossipSubRouter) seacastAckAllowed(mid string) bool {
	sc := gs.seacast
	if sc == nil {
		return true
	}
	if _, pending := sc.pending[mid]; pending {
		sc.ctr.DupUnacked.Add(1)
		return false
	}
	if _, rejected := sc.rejected[mid]; rejected {
		sc.ctr.DupUnacked.Add(1)
		return false
	}
	if gs.have != nil {
		if root, index, ok := gs.have.index(mid); ok && len(root) == haveRootLen {
			if gs.seacastRootType(root) != nil {
				if m := gs.have.mine[root]; m == nil || !m.bits.Has(index) {
					sc.ctr.DupUnacked.Add(1)
					return false
				}
			}
		}
	}
	return true
}

// dropPending forgets a pending copy.
func (sc *seacast) dropPending(mid string) {
	e, ok := sc.pending[mid]
	if !ok {
		return
	}
	delete(sc.pending, mid)
	sc.pendingOf[e.from]--
}

// seacastRelease pushes the pending copies of the given roots into the receive path: the gate
// opened. Router goroutine (the receive loop's); a copy was never seen, so it is validated as the
// arrival it is and, at ACCEPT, filled by seacastAccepted.
func (gs *GossipSubRouter) seacastRelease(roots []string) {
	sc := gs.seacast
	if sc == nil {
		return
	}
	if len(sc.pending) == 0 {
		for _, r := range roots {
			gs.seacastReleaseTies(r, nil) // ties of refused copies only
		}
		return
	}
	want := make(map[string]bool, len(roots))
	for _, r := range roots {
		want[r] = true
	}
	kept := map[string]map[peer.ID]map[uint32]bool{} // per root, the copies that resume, by sender and index
	for mid, e := range sc.pending {
		if !want[e.root] {
			continue
		}
		delete(sc.pending, mid)
		sc.pendingOf[e.from]--
		if gs.seacastRootExpired(e.root) {
			sc.ctr.Expired.Add(1) // the group expired or left the window while it waited: dropped
			continue
		}
		_, index, _ := gs.have.index(mid)
		if T := gs.seacastRootType(e.root); T != nil && int(index) >= T.IndexCount(e.root) {
			// A copy naming an index its root's header does not admit: a forgery, dropped with its
			// sender's offers of the index (seacast-lite.md's drop_tie); the recovery goes on without it.
			sc.ctr.Forged.Add(1)
			gs.seacastDropOffer(e.from, mid, e.root, index)
			if gs.offerTable != nil && !gs.haveSeen(mid) {
				gs.retryOfferedPull(mid) // the recovery goes on without the forger
			}
			continue
		}
		if kept[e.root] == nil {
			kept[e.root] = map[peer.ID]map[uint32]bool{}
		}
		if kept[e.root][e.from] == nil {
			kept[e.root][e.from] = map[uint32]bool{}
		}
		kept[e.root][e.from][index] = true
		sc.ctr.Released.Add(1)
		// The receive path's two steps, as handleIncomingRPC runs them for an admitted message.
		if gs.p.shouldPush(e.msg) {
			gs.Preprocess(e.from, []*Message{e.msg})
			gs.p.pushMsg(e.msg)
		}
	}
	for _, r := range roots {
		gs.seacastReleaseTies(r, kept[r])
	}
}

// seacastExpire ends the given groups' active lifetime (the application's word): each leaves a
// tombstone until the window passes its slot, and its pending copies, store counts, admission
// registrations and pull state go (seacast_window.go).
func (gs *GossipSubRouter) seacastExpire(roots []string) {
	sc := gs.seacast
	if sc == nil {
		return
	}
	if sc.expired == nil {
		sc.expired = map[string]struct{}{}
	}
	for _, r := range roots {
		sc.expired[r] = struct{}{}
		gs.seacastDropGroup(r)
	}
}

// SeacastHeaderValidated tells the path that the application validated the header of the groups
// named: their pending copies are validated now (on_header_validated). Any goroutine.
func (p *PubSub) SeacastHeaderValidated(roots []string) {
	gs, ok := p.rt.(*GossipSubRouter)
	if !ok || gs.seacast == nil {
		return
	}
	select {
	case p.eval <- func() { gs.seacastDropBeyond(roots); gs.seacastRelease(roots) }:
	case <-p.ctx.Done():
	}
}

// seacastDropBeyond is seacast-lite.md §4.2's drop_beyond (spec f6c19db; seacast.md's fill carries it) at
// the moment the fork learns a group's count, its header's validation (seacastCountKnown): what named an
// index the group has not, while the count was unknown, goes: the offers of it with their ordered list,
// per-group offer bits and queued asks, its promises, tries, ask record and last-resort counts. Ties
// beyond the count go at their release (seacastReleaseTies). Router goroutine, before the release.
func (gs *GossipSubRouter) seacastDropBeyond(roots []string) {
	sc := gs.seacast
	for _, root := range roots {
		T := gs.seacastRootType(root)
		if T == nil {
			continue
		}
		n := T.IndexCount(root)
		if n <= 0 {
			continue
		}
		mids := map[string]uint32{}
		note := func(mid string) {
			if r, i, ok := gs.groupIndex(mid); ok && r == root && int64(i) >= int64(n) {
				mids[mid] = i
			}
		}
		for mid := range gs.offerTable {
			note(mid)
		}
		for mid := range gs.offerTried {
			note(mid)
		}
		for mid := range gs.iwantAsked {
			note(mid)
		}
		for mid := range gs.iwantPromised {
			note(mid)
		}
		if gs.v05 != nil {
			for mid := range gs.v05.offerOrder {
				note(mid)
			}
			for _, byRoot := range gs.v05.offered {
				g := byRoot[root]
				if g == nil {
					continue
				}
				for i := n; i < len(g.bits.b)*8; i++ {
					if g.bits.Has(uint32(i)) { // lint:ignore uintcast -- bounded by haveMaxBytes*8.
						g.bits.Clear(uint32(i))           // lint:ignore uintcast -- as above.
						note(encodeWant(root, uint32(i))) // lint:ignore uintcast -- as above.
					}
				}
			}
		}
		for mid, i := range mids {
			var peers []peer.ID
			for p := range gs.offerTable[mid] {
				peers = append(peers, p)
			}
			for p := range gs.iwantPromised[mid] {
				peers = append(peers, p)
			}
			for _, p := range peers {
				gs.seacastDropOffer(p, mid, root, i) // the offer, its bit, its queued ask, its promise
			}
			gs.clearOffers(mid) // the rest of the offer table's entry: tries, topic, ordered list
			delete(gs.iwantAsked, mid)
			if gs.v05 != nil {
				k := gs.chunkKey(mid)
				delete(gs.v05.askedParked, k)
				delete(gs.v05.lrArmedKeys, k)
			}
			sc.ctr.DroppedBeyond.Add(1)
		}
	}
}

// SeacastExpire tells the path that the groups named expired: their pending copies are dropped.
func (p *PubSub) SeacastExpire(roots []string) {
	gs, ok := p.rt.(*GossipSubRouter)
	if !ok || gs.seacast == nil {
		return
	}
	select {
	case p.eval <- func() { gs.seacastExpire(roots) }:
	case <-p.ctx.Done():
	}
}

// SeacastPending is the pending copies held, for the harness's lines and the fixtures.
func (p *PubSub) SeacastPending() int {
	gs, ok := p.rt.(*GossipSubRouter)
	if !ok || gs.seacast == nil {
		return 0
	}
	done := make(chan int, 1)
	select {
	case p.eval <- func() { done <- len(gs.seacast.pending) }:
	case <-p.ctx.Done():
		return 0
	}
	select {
	case n := <-done:
		return n
	case <-p.ctx.Done():
		return 0
	}
}

func (sc *seacast) limitsString() string {
	return fmt.Sprintf("pending per peer %d, live window %d slots", sc.limits.PendingPerPeer, sc.limits.LiveWindow)
}
