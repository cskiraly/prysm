package pubsub

import (
	"math/rand/v2"
	"time"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
)

// --- The views, the claims and the probe (seacast.md §4.6, R5; part 4) ---
//
// known_in_scope is what the node and its suppression scope (the topic's mesh on v0.3's wire) hold
// of a group by their holdings, a claimant's holding counting for nothing until... never: the draft
// keeps every claimant out of the view. A peer's claim_all (a v4 holding's flag) is a claim; on a group whose later duty
// is armed and still short it defers the duty once, verified by one ask of one claimant, the probe:
// an ordinary-lane WANT past stop_pull and the ask window, whose promise is DEFER_WINDOW and whose
// break penalises and parks the claimant. A verified claim ends the duty. (known_across, R4's wait
// and owed_missing, the member check, are the two-axis types'; this fork has none.)

// seacastProbe is one group's deferral: the index probed, the claimant asked, the ask's id.
type seacastProbe struct {
	index uint32
	asked peer.ID
	mid   string
	at    time.Time // when it was asked: its promise's identity, with the peer and the id
	// cancelled: the claimant's connection closed while the promise ran; nothing is charged, and a
	// later connection of the same peer is not charged for it (the review of part 4's gate, finding 4).
	// judged: the promise ran its course (answered or broken); a later disconnection cancels nothing.
	cancelled, judged bool
	// own: the claimant's own copy of the probed cell came, first or a duplicate, in either axis's
	// form (probe_answer); verdict: the verdict was taken at reconsider (probe_ok set).
	own, verdict bool
	// sent: its WANT was written (probe_sent): its promise and the verdict's wait started then. noVerdict: it was
	// refused by a full queue or still unwritten at the reconsider, so it got no verdict (probe_ok None).
	sent, noVerdict bool
	// dutyAt: the duty's own time when the claim deferred it (duty_at); a probe with no verdict returns to it.
	dutyAt time.Time
}

// seacastKnownInScope is known_in_scope(g). Router goroutine.
func (gs *GossipSubRouter) seacastKnownInScope(T ObjectType, root string) *haveBits {
	known := &haveBits{}
	if m := gs.have.mine[seacastGID(root)]; m != nil {
		known.Or(m.Bits.Bytes())
	}
	topic, ok := T.TopicOf(root)
	if !ok {
		return known
	}
	for q := range gs.mesh[topic] {
		e := gs.have.peers.Of(q, seacastGID(root))
		if e == nil || e.ClaimAll {
			continue // (R5) a claim counts for nothing here
		}
		known.Or(e.Bits.Bytes())
	}
	return known
}

// seacastReceived reports whether the node holds index of root from the network (or its own
// publication), not by its own rebuild: the probe's verdict reads this (§4.6, received[g]).
func (gs *GossipSubRouter) seacastReceived(root string, index uint32) bool {
	if b := gs.seacast.rebuilt[root]; b != nil && b.Has(index) {
		return false
	}
	return gs.seacastHeld(root, index)
}

// seacastHeld reports whether the node holds index of root (received or rebuilt). Router goroutine.
func (gs *GossipSubRouter) seacastHeld(root string, index uint32) bool {
	m := gs.have.mine[seacastGID(root)]
	return m != nil && m.Bits.Has(index)
}

// seacastClaimants is the group's claims set: the peers whose holding claims every index. Router
// goroutine.
func (gs *GossipSubRouter) seacastClaimants(root string) []peer.ID {
	return gs.have.peers.Claimants(seacastGID(root))
}

// seacastNoteClaim is note_claim(p, g), from every v4 holding that claims (on_holding): on a group whose
// later duty is armed and still short, the duty is deferred once by a probe. Router goroutine.
func (gs *GossipSubRouter) seacastNoteClaim(p peer.ID, root string) {
	sc := gs.seacast
	if sc == nil || !sc.machine {
		return
	}
	sc.ctr.ClaimsNoted.Add(1)
	g := sc.group(root)
	T := gs.seacastRootType(root)
	if g == nil || T == nil || g.ReconsiderAt.IsZero() || g.State != seacastComplete || g.Ext != nil {
		return
	}
	if m := gs.have.mine[seacastGID(root)]; m != nil && m.Bits.Count() >= T.IndexCount(root) {
		return
	}
	gs.seacastDeferOnClaim(T, root)
}

// seacastDeferOnClaim is defer_on_claim(g): one index the node lacks, asked of one claimant with
// serving allowance left (the asker's own count against the type's SERVE_LIMIT), the ask's promise
// DEFER_WINDOW, the duty re-armed to the window's end at the earliest. No eligible claimant: no
// probe, and the duty goes on as armed. Router goroutine.
func (gs *GossipSubRouter) seacastDeferOnClaim(T ObjectType, root string) {
	sc := gs.seacast
	g := sc.groupOf(root)
	// (R5) the probed cell is drawn at random among those we lack: the claimant cannot tell which.
	var lack []uint32
	for i := range T.IndexCount(root) {
		if idx := uint32(i); !gs.seacastHeld(root, idx) { // lint:ignore uintcast -- an index below the group's count.
			lack = append(lack, idx)
		}
	}
	found := len(lack) > 0
	var index uint32
	if found {
		index = lack[rand.IntN(len(lack))]
	}
	limit := T.Params().ServeLimit
	var cands []peer.ID
	for _, q := range gs.seacastClaimants(root) {
		if _, connected := gs.peers[q]; !connected {
			continue
		}
		if limit > 0 && sc.askedOf[root][q] >= limit {
			continue // its allowance is spent: another claimant, or none (§4.6)
		}
		cands = append(cands, q)
	}
	if !found || len(cands) == 0 {
		sc.ctr.ProbesNone.Add(1)
		return
	}
	q := cands[rand.IntN(len(cands))]
	mid := encodeWantV2Probe(root, index) // (R5) marked: answered whatever we hold
	asked := time.Now()
	dutyAt := g.ReconsiderAt // the duty's own time (duty_at): a probe with no verdict returns to it
	gs.haveQueue(q)          // the queue's hooks, the write report's among them, before the probe is queued
	g.Ext = &seacastProbe{index: index, asked: q, mid: mid, at: asked, dutyAt: dutyAt}
	if sc.probeAsks == nil {
		sc.probeAsks = map[string]struct{}{}
	}
	sc.probeAsks[mid] = struct{}{}
	// (R5) probe_asks by (group, index): any ask of the probed cell passes stop_pull until the group
	// expires, whatever id it carries.
	if sc.probeCells == nil {
		sc.probeCells = map[string]*haveBits{}
	}
	if sc.probeCells[root] == nil {
		sc.probeCells[root] = &haveBits{}
	}
	sc.probeCells[root].Set(index)
	gs.seacastProbeWait(q, mid, true) // (R5) our writer keeps it though another copy comes first
	sc.registerAsk(root, index, q)
	gs.seacastOrderProbe(q, root) // (iv) at the probe's ask (seacast_order.go)
	written := gs.sendRPC(q, rpcWithControl(nil, nil, []*pb.ControlIWant{{MessageIDs: []string{mid}}}, nil, nil, nil), false)
	sc.ctr.ProbesAsked.Add(1)
	sc.event(SeacastEvProbeAsked, root)
	if !written {
		// (R5, spec 43fe953) refused by a full queue, never written: no verdict, and our own repair is
		// reconsidered at the duty's own time, at once if that has passed.
		g.Ext.verdict, g.Ext.noVerdict = true, true
		sc.ctr.ProbesRefused.Add(1)
		sc.ctr.ProbesNoVerdict.Add(1)
		gs.seacastProbeWait(q, mid, false)
		at := dutyAt
		if now := time.Now(); at.Before(now) {
			at = now
		}
		gs.seacastArm(root, at)
		return
	}
	// Its promise starts at its write (seacastProbeWritten), not here; not written by the reconsider, it
	// gets no verdict (seacastProbeVerdict).
	window := T.Params().DeferWindow
	if window <= 0 {
		window = time.Second
	}
	at := asked.Add(window)
	if g.ReconsiderAt.After(at) {
		at = g.ReconsiderAt
	}
	gs.seacastArm(root, at)
}

// seacastProbeDue is the probe's promise_due at DEFER_WINDOW. The answered and broken counts read
// the cell from anyone (the member's duty reads the same); the promise is the claimant's, kept open
// by another peer's copy (probe_promise, R5), so without the claimant's own copy it is charged as a
// broken promise (parked under the park's rule). The verdict is reconsider's (seacastProbeVerdict).
// Router goroutine.
func (gs *GossipSubRouter) seacastProbeDue(root, mid string, q peer.ID, index uint32, asked time.Time) {
	sc := gs.seacast
	delete(sc.probeAsks, mid)
	if expired, _ := gs.seacastRootState(root); expired || sc.group(root) == nil {
		return // the group passed its lifetime or was dropped: no promise is owed any more
	}
	g := sc.group(root)
	if g.Ext == nil || g.Ext.mid != mid || g.Ext.asked != q || !g.Ext.at.Equal(asked) || g.Ext.cancelled {
		return // not this probe's promise any more, or its connection closed meanwhile (seacastProbeGone)
	}
	if _, connected := gs.peers[q]; !connected {
		return // the connection closed: its promise goes with it
	}
	g.Ext.judged = true
	if gs.seacastReceived(root, index) {
		sc.ctr.ProbesAnswered.Add(1)
		sc.event(SeacastEvProbeAnswered, root)
	} else {
		sc.ctr.ProbesBroken.Add(1)
		sc.event(SeacastEvProbeBroken, root)
	}
	if !g.Ext.own {
		gs.notePromiseBreak(mid, q) // (R5) only the claimant's own copy keeps its promise
	}
}

// seacastReconsiderVerdict is reconsider's middle (§4.6), after the stale-timer check and with the
// group short in the node's holdings: true when the duty ends here (a whole view, a verified claim,
// a deferral started), false when the node decodes. Router goroutine.
func (gs *GossipSubRouter) seacastReconsiderVerdict(T ObjectType, root string, g *seacastGroup) bool {
	sc := gs.seacast
	n := T.IndexCount(root)
	if gs.seacastKnownInScope(T, root).Count() >= n {
		sc.ctr.DutiesCancelled.Add(1)
		return true // whole in the subnet's view: nothing to rebuild
	}
	if g.Ext != nil && gs.seacastReceived(root, g.Ext.index) {
		sc.ctr.ClaimsVerified.Add(1)
		sc.event(SeacastEvVerified, root)
		return true // (R5) the claim held and its repair reached us
	}
	if g.Ext == nil && len(gs.seacastClaimants(root)) > 0 {
		gs.seacastDeferOnClaim(T, root)
		if g.Ext != nil {
			return true // (R5) a claim: the one deferral
		}
	}
	return false // (R5) the claimant's verdict was taken first, in seacastReconsider (seacastProbeVerdict)
}

// probeAsksForget drops the probe's ask exemptions (by id and by cell) of root's cells, at
// the group's expiry: a later ask of them is an ordinary one (the review of part 4's build, finding
// 4). Router goroutine, from expireGroup.
func (sc *seacast) probeAsksForget(root string) {
	for mid := range sc.probeAsks {
		if r, _, _, _, ok, _ := decodeWantV2(mid); ok && r == root {
			delete(sc.probeAsks, mid)
		}
	}
	delete(sc.probeCells, root) // (R5) the probe's exemption by cell goes with the group
	for _, groups := range sc.probeServed {
		delete(groups, root) // and, as a claimant, the cap on its marked asks
	}
}

// seacastProbeGone is claims_close(p) (seacast.md §4.6, spec 43fe953) with the probe's promise: the
// probes asked of p, whose connection closed, lose their promises if not yet due, so neither this
// connection nor a later one of the same peer is charged for them, and the exemption goes too; a probe
// not yet judged at reconsider, and not passed by p's own copy, gets no verdict, and the duty returns
// to its own time (duty_at), at once if that has passed. Router goroutine, from seacastPeerGone.
func (gs *GossipSubRouter) seacastProbeGone(p peer.ID) {
	sc := gs.seacast
	delete(sc.probeServed, p) // as a claimant, p's caps go with its connection
	for _, w := range sc.probeWaiting {
		delete(w, p) // and its probes kept while we decoded
	}
	if sc.gm == nil {
		return
	}
	now := time.Now()
	for gid, g := range sc.gm.All() {
		root := seacastGIDRoot(gid)
		pr := g.Ext
		if pr == nil || pr.asked != p {
			continue
		}
		if !pr.cancelled && !pr.judged {
			pr.cancelled = true
			delete(sc.probeAsks, pr.mid)
			if b := sc.probeCells[root]; b != nil {
				b.Clear(pr.index) // the exemption goes with the probe (this package's close rule)
			}
			sc.ctr.ProbesCancelled.Add(1)
		}
		if pr.verdict || pr.own {
			continue // judged at reconsider already, or passed by the claimant's own copy (probe_answer)
		}
		pr.verdict, pr.noVerdict = true, true // (R5, claims_close) no verdict
		sc.ctr.ProbesNoVerdict.Add(1)
		at := pr.dutyAt // the duty at its own time, at once if that has passed
		if at.Before(now) {
			at = now
		}
		gs.seacastArm(root, at)
	}
}
