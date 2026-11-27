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

	core "github.com/cskiraly/seacast-go"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
)

// SeacastLimits bounds the path's state (X6: limits before state): the core's Limits.
type SeacastLimits = core.Limits

// DefaultSeacastLimits is the toy's (core.DefaultLimits).
var DefaultSeacastLimits = core.DefaultLimits

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

// seacastAdmit is the path's gate at admission (handleIncomingRPC, before the seen check and
// validation; and again once validation finishes, validated): false keeps the message out, in the
// deferred store while its group's header is not validated (core.Pending.Defer), or dropped as a
// second copy of a kept id or over the per-peer bound, its sender tied either way. Off the path's
// topics, true.
func (gs *GossipSubRouter) seacastAdmit(msg *Message, validated bool) bool {
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
	env := waitEnv{gs: gs, mid: mid}
	if s := env.Slot(seacastChunk(root, index, mid)); !env.Live(s) {
		// The slot the copy names, where its type reads one (ChunkSlotter), out of the live window:
		// on_chunk's and on_validated's not live(f.slot), at admission and once validated.
		if s > sc.slot {
			sc.ctr.Future.Add(1)
		} else {
			sc.ctr.ExpiredDropped.Add(1)
		}
		return false
	}
	if sc.waits.Has(mid) {
		sc.ctr.PendingDup.Add(1)
		gs.seacastTie(msg.ReceivedFrom, root, index, mid) // another copy of one that waits: its sender too, if admitted
		return false
	}
	if T.HeaderKnown(root) {
		return true // the gate is open: validated now, filled at ACCEPT
	}
	if !validated && gs.p.seenMessage(mid) {
		// on_chunk's seen comes first: a copy of an id the node has seen (its own publication filled
		// it while a copy waited, and the copy went) is a duplicate, never kept.
		sc.ctr.SeenAtGate.Add(1)
		return true
	}
	sc.noteGateMet(root)
	from, g := msg.ReceivedFrom, seacastGID(root)
	if !env.Admitted(from, g) {
		sc.ctr.UnknownRefused.Add(1) // a group beyond the sender's unknown groups: neither kept nor tied
		return false
	}
	kept, tied := sc.waits.Of(from), sc.ties.Tied(from, g, index)
	if _, again := sc.waits.Defer(env, &sc.ties, from, seacastChunk(root, index, mid), mid, sc.limits); again {
		return true // the gate opened meanwhile: validated now
	}
	if !tied && sc.ties.Tied(from, g, index) {
		sc.ctr.Tied.Add(1)
	}
	if sc.waits.Of(from) > kept {
		if sc.waitMsg == nil {
			sc.waitMsg = map[string]*Message{}
		}
		sc.waitMsg[mid] = msg
		sc.ctr.Pending.Add(1) // kept: its sender tied to the root it names
	} else {
		sc.ctr.PendingRefused.Add(1) // refused, not kept: its sender tied all the same
	}
	return false
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
	case ValidationIgnore:
		sc.ctr.Ignored.Add(1)
	}
}

// seacastAccepted is the fill, from Publish at ACCEPT of a message on the path's topics: the
// holding and the id, the ask it closes, the holding to the suppression scope on a claim and to
// the sender.
func (gs *GossipSubRouter) seacastAccepted(msg *Message) {
	from := msg.ReceivedFrom
	mid := gs.p.idGen.ID(msg)
	sc := gs.seacast
	gs.noteArrival(mid, from)
	gs.iwantDelivered(mid)
	claim := gs.haveNoteArrival(mid)
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
	gs.seacastDropFilledWait(root, mid)
	gs.seacastProbeCopy(root, index, from) // (R5) the claimant's own copy passes its probe
	T := gs.seacastType(msg.GetTopic())
	if g := sc.group(root); g != nil && g.State == seacastInvalid {
		T = nil // an INVALID group is not taken up again (§4.2's fill, seacast.md §4.1)
	}
	if T != nil {
		gs.seacastPlaceAccepted(T, root)                 // the group on its type's topic, from its first accepted chunk
		gs.seacastOrderFill(T, root, index)              // (i) the gate before the fill (seacast_order.go)
		gs.seacastStorePut(T, root, index, msg)          // the fill's store half: served after mcache lets go
		gs.seacastServeStanding(T, root, index, msg)     // §4.10: the chunk a standing request waited for is here
		gs.seacastServeWaitingProbe(T, root, index, msg) // (R5) a probe that reached us while we decoded
		gs.seacastCheckCompletion(T, root)               // under the machine: the holding below carries its claims
	}
	// [c7] the holding to whoever may push this group to us, on v0.3's wire the topic's mesh,
	// when the chunk is a claim; [c14] and to the sender always, as its acknowledgement
	// (seacast-lite.md §4.2's on_validated; the core's HaveTargets). A copy of an index the node
	// holds already, as one its decode rebuilt, changes nothing the scope has heard, so it goes
	// to the sender alone. Both leave exactly as the arrival HAVE does from Preprocess (the same
	// lane, the same coalescing); the holding's encoding is v4 on the path's roots
	// (haveIDontWantIDs). The stock IDONTWANT's size threshold does not apply: the draft's
	// holding has no size rule, and a group's holding is one bitmap whatever the chunk's size.
	topic := msg.GetTopic()
	if claim {
		gs.idontwantMesh(topic, []string{mid}, from)
	}
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
	if sc.waits.Has(mid) {
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
				if m := gs.have.mine[seacastGID(root)]; m == nil || !m.Bits.Has(index) {
					sc.ctr.DupUnacked.Add(1)
					return false
				}
			}
		}
	}
	return true
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
// named: drop_beyond, then the gate's opening for each (on_header_validated): its kept copies are
// validated now, its ties release and its ledger is retried. Any goroutine.
func (p *PubSub) SeacastHeaderValidated(roots []string) {
	gs, ok := p.rt.(*GossipSubRouter)
	if !ok || gs.seacast == nil {
		return
	}
	select {
	case p.eval <- func() {
		gs.seacastDropBeyond(roots)
		for _, r := range roots {
			gs.seacastOpenGate(r)
		}
	}:
	case <-p.ctx.Done():
	}
}

// seacastDropBeyond is seacast-lite.md §4.2's drop_beyond (spec f6c19db; seacast.md's fill carries it) at
// the moment the fork learns a group's count, its header's validation (seacastCountKnown): what named an
// index the group has not, while the count was unknown, goes: the offers of it with their ordered list,
// per-group offer bits and queued asks, its promises, tries, ask record and last-resort counts, and the
// ties of indices beyond it (Ties.DropBeyond). Router goroutine, before the gate's opening.
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
		if gs.have != nil {
			// The source's acknowledgements: an index acknowledged before the count was known goes,
			// or its holding would name a bit its receivers refuse.
			gs.have.originAcks.dropBeyond(root, uint32(n)) // lint:ignore uintcast -- n > 0, an index count.
		}
		if gs.pullOn() {
			// A chunk's pull is the core's (seacast_pull.go), the ties with it.
			sc.ctr.DroppedBeyond.Add(int64(gs.pullDropBeyond(root, seacastGID(root), uint32(n)))) // lint:ignore uintcast -- n > 0, an index count.
			continue
		}
		sc.ties.DropBeyond(seacastGID(root), uint32(n)) // lint:ignore uintcast -- n > 0, an index count.
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
		for mid := range sc.askQueued {
			note(mid)
		}
		for mid, i := range mids {
			// Every peer something of ours names for it: an offer, a promise, or an ask still queued,
			// which the offer table may not hold (its allowance refused the offer).
			peers := map[peer.ID]struct{}{}
			for p := range gs.offerTable[mid] {
				peers[p] = struct{}{}
			}
			for p := range gs.iwantPromised[mid] {
				peers[p] = struct{}{}
			}
			for p := range sc.askQueued[mid] {
				peers[p] = struct{}{}
			}
			for p := range peers {
				gs.seacastDropOffer(p, mid, root, i) // the offer, its bit, its queued ask, its promise
			}
			gs.clearOffers(mid) // the rest of the offer table's entry: tries, topic
			delete(gs.iwantAsked, mid)
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
	case p.eval <- func() { done <- gs.seacast.waits.Len() }:
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
