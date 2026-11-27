package pubsub

// The live window (seacast.md §4.2 and v0.3 §8.1). A group is a
// descriptor committed at a slot, named by its key, so its slot is fixed; a node's current slot
// only moves forward (SeacastAdvanceSlot), and a group's slot is live from its start until it is
// LiveWindow slots behind the current one. A chunk, offer or ask of a group whose slot has left the
// window is ignored before level 1, as is a chunk of a later slot (a block from a future slot). A
// group's active lifetime ends when its slot leaves the window if it has not ended before; the
// tombstone an earlier expiry leaves (sc.expired: the application's SeacastExpire, or the ledger's
// aging of a path root) is kept until the window has passed the slot, and then goes: from then on
// the window keeps the group out. Nodes need not agree on the window or the slot. The application
// supplies the slots: the type's SlotOf for a root it can place, and the node's current slot.

// seacastRootState is root's place in the node's window: expired (a tombstone, or its slot has left
// the window) or future (its slot is ahead of the current one). A root the path does not own, or
// whose slot the application cannot place, or before the first advance, is neither.
func (gs *GossipSubRouter) seacastRootState(root string) (expired, future bool) {
	sc := gs.seacast
	if sc == nil {
		return false, false
	}
	if _, dead := sc.expired[root]; dead {
		return true, false
	}
	if !sc.slotKnown {
		return false, false
	}
	s, ok := gs.seacastSlotOf(root)
	if !ok {
		return false, false
	}
	if s > sc.slot {
		return false, true
	}
	return s+sc.limits.LiveWindow <= sc.slot, false
}

// seacastRootExpired is seacastRootState's first answer, for the callers that drop both ways.
func (gs *GossipSubRouter) seacastRootExpired(root string) bool {
	e, _ := gs.seacastRootState(root)
	return e
}

// seacastSlotOf asks the root's owning type for its slot.
func (gs *GossipSubRouter) seacastSlotOf(root string) (uint64, bool) {
	T := gs.seacastRootType(root)
	if T == nil {
		return 0, false
	}
	return T.SlotOf(root)
}

// seacastForgetRoot takes a root out of the path's registry once the window has passed its slot: the
// type's slot for a root it places, else the slot the path registered the root at (one registered by
// a chunk that never validates, or only named in an offer, or registered again after the type dropped
// its record). While the type still places the root by its slot, a later chunk, offer or ask registers
// it again and is dropped as expired; the next advance forgets it again. Its chunks held back go too, a
// live group's and a tombstone's: the heartbeat's retry would look the root up again, and a type may
// place a root it forgot as new, where the spec's retry finds the group out of the window. Router
// goroutine.
func (gs *GossipSubRouter) seacastForgetRoot(root string) {
	sc := gs.seacast
	gs.seacastForgetHeldBack(root)
	gs.pull.held.ForgetGroup(seacastGID(root))
	gs.seacastForgetWaits(root) // its ties and kept copies: the heartbeat must not look the root up again
	delete(sc.roots, root)
	delete(sc.rootTopic, root)
	delete(sc.knownAt, root)
	delete(sc.regSlot, root)
}

// seacastRegistrySlot is the slot the window keeps a registered root by: its type's, or failing that
// the one the path registered it at.
func (gs *GossipSubRouter) seacastRegistrySlot(root string) (uint64, bool) {
	if s, ok := gs.seacastSlotOf(root); ok {
		return s, true
	}
	s, ok := gs.seacast.regSlot[root]
	return s, ok
}

// seacastAdvanceSlot moves the node's current slot forward: the tombstones the window has passed
// go, and the live groups whose slot has left the window expire with their pull state. A lower slot
// is ignored and counted (the window only moves forward). Router goroutine.
func (gs *GossipSubRouter) seacastAdvanceSlot(slot uint64) {
	sc := gs.seacast
	if sc == nil {
		return
	}
	if sc.slotKnown && slot < sc.slot {
		sc.ctr.SlotRegress.Add(1)
		return
	}
	sc.slot, sc.slotKnown = slot, true
	for root := range sc.roots { // the roots registered before the node had a slot take this one
		if _, ok := sc.regSlot[root]; !ok {
			sc.noteRegSlot(root)
		}
	}
	passed := map[string]bool{} // the tombstones the window passed: already dropped, not expired again
	for root := range sc.expired {
		if s, ok := gs.seacastRegistrySlot(root); ok && s+sc.limits.LiveWindow <= slot {
			delete(sc.expired, root)
			passed[root] = true
			sc.ctr.TombstonesDropped.Add(1)
		}
	}
	for root := range sc.roots {
		if _, dead := sc.expired[root]; dead || passed[root] {
			continue
		}
		// One lookup decides: the type may place the root on another goroutine at any moment, and
		// a second lookup could see a placement the first did not.
		s, placed := gs.seacastSlotOf(root)
		if !placed {
			var ok bool
			if s, ok = sc.regSlot[root]; !ok {
				continue
			}
		}
		if s+sc.limits.LiveWindow > slot {
			continue
		}
		sc.ctr.WindowExpired.Add(1)
		gs.seacastClearGroup(root)
		if gs.have != nil {
			// A root the type placed leaves a holdings tombstone, as before; the window keeps it out
			// from here on, so it gets no path tombstone. A root no type placed leaves none, and
			// keeps one an earlier expiry left: the type may place it as a live group after this
			// lookup, and a new holdings tombstone would keep its chunks from being recorded as
			// held, and its asks from leaving the writers, until the tombstone aged out.
			if placed {
				gs.markGroupExpired(root)
			}
			gs.discardGroupPull(root)
		}
		gs.seacastForgetRoot(root)
	}
	for root := range passed {
		gs.seacastForgetRoot(root)
	}
}

// seacastDropGroup ends a group's active lifetime on the path: its pending copies, its store's
// served counts and its admission registrations go, and its pull state with them (offers, tried
// asks, asks in flight, promises, what is owed, and its queued frames, offers and asks: v0.5's D′,
// expireGroup). Router goroutine.
func (gs *GossipSubRouter) seacastDropGroup(root string) {
	gs.seacastClearGroup(root)
	if gs.have != nil {
		gs.expireGroup(root)
	}
}

// seacastClearGroup is seacastDropGroup without the holdings' side: the path's own state of the
// group goes, and nothing is tombstoned. Router goroutine.
func (gs *GossipSubRouter) seacastClearGroup(root string) {
	sc := gs.seacast
	sc.storeForget([]string{root})
	sc.askedForget([]string{root})
	sc.standingForget(root)
	sc.forgetGroup(root)        // its state (seacast_complete.go)
	sc.orderForget(root)        // its ordering state (seacast_order.go), before its rebuilt indices go
	delete(sc.rebuilt, root)    // its rebuilt indices (seacast_decode.go)
	gs.seacastForgetWaits(root) // its ties and kept copies go with it (seacast-lite.md §8.1)
	gs.seacastOriginForget(root)
}

// seacastGroupAged is the ledger's word that a path root's holdings aged out (haveHeartbeat, GROUP_TTL
// as holdTTL): the group expired before its slot left the window, so it leaves a tombstone until the
// window passes it. A root the path no longer has registered was forgotten by the window, which keeps
// it out: no tombstone. Router goroutine, from expireGroup.
func (gs *GossipSubRouter) seacastGroupAged(root string) {
	sc := gs.seacast
	if sc == nil {
		return
	}
	gs.seacastForgetHeldBack(root)
	if gs.seacastRootOwner(root) == nil {
		return
	}
	if sc.slotKnown {
		if s, ok := gs.seacastRegistrySlot(root); ok && s+sc.limits.LiveWindow <= sc.slot {
			return // its slot has left the window: the window keeps it out, no tombstone
		}
	}
	if sc.expired == nil {
		sc.expired = map[string]struct{}{}
	}
	if _, dead := sc.expired[root]; dead {
		return
	}
	sc.expired[root] = struct{}{}
	sc.storeForget([]string{root})
	sc.askedForget([]string{root})
	sc.standingForget(root)
	sc.forgetGroup(root)     // its state (seacast_complete.go)
	sc.orderForget(root)     // its ordering state (seacast_order.go), before its rebuilt indices go
	delete(sc.rebuilt, root) // its rebuilt indices (seacast_decode.go)
	gs.seacastForgetWaits(root)
	gs.seacastOriginForget(root)
}

// SeacastAdvanceSlot tells the path the node's current slot (the harness: the block's number as
// block m starts on every router). Any goroutine.
func (p *PubSub) SeacastAdvanceSlot(slot uint64) {
	gs, ok := p.rt.(*GossipSubRouter)
	if !ok || gs.seacast == nil {
		return
	}
	select {
	case p.eval <- func() { gs.seacastAdvanceSlot(slot) }:
	case <-p.ctx.Done():
	}
}

// SeacastAdvanceSlotWith tells the path the node's current slot, as SeacastAdvanceSlot does, then
// runs then(slot) on the router's goroutine in the same router event, and returns once both are done
// or the router stopped. An object type's own advance goes in then: the router's expiry at this slot
// reads the type's records before the type moves, so it never misses one the type then drops, and
// every later router event sees both moved. What the type does on other goroutines between the two
// still sees the type's earlier slot. then must not wait on the router. Once the router has taken the
// event, then runs even if the router's context ends meanwhile, and the call may return before then
// does; without a path, then runs at once, on the caller's goroutine. Any goroutine.
func (p *PubSub) SeacastAdvanceSlotWith(slot uint64, then func(slot uint64)) {
	gs, ok := p.rt.(*GossipSubRouter)
	if !ok || gs.seacast == nil {
		then(slot)
		return
	}
	done := make(chan struct{})
	select {
	case p.eval <- func() {
		defer close(done)
		gs.seacastAdvanceSlot(slot)
		then(slot)
	}:
	case <-p.ctx.Done():
		return
	}
	select {
	case <-done:
	case <-p.ctx.Done():
	}
}

// SeacastSlot is the node's current slot and whether one was set; SeacastTombstones the groups
// expired before their slot left the window, still kept out by name. For the harness's lines and
// the fixtures. Any goroutine.
func (p *PubSub) SeacastSlot() (uint64, bool) {
	var slot uint64
	var known bool
	p.seacastRead(func(gs *GossipSubRouter) { slot, known = gs.seacast.slot, gs.seacast.slotKnown })
	return slot, known
}

func (p *PubSub) SeacastTombstones() int {
	n := 0
	p.seacastRead(func(gs *GossipSubRouter) { n = len(gs.seacast.expired) })
	return n
}

// seacastRead runs f on the router goroutine with the path installed, and waits for it.
func (p *PubSub) seacastRead(f func(gs *GossipSubRouter)) {
	gs, ok := p.rt.(*GossipSubRouter)
	if !ok || gs.seacast == nil {
		return
	}
	done := make(chan struct{})
	select {
	case p.eval <- func() { f(gs); close(done) }:
	case <-p.ctx.Done():
		return
	}
	select {
	case <-done:
	case <-p.ctx.Done():
	}
}
