package pubsub

import (
	"fmt"
	"time"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
)

// --- A group's completion and its state (seacast.md §4.6's check_completion and arm) ---
//
// With WithSeacastMachine the router keeps each path group's state: COLLECTING until the node's
// interest in it is satisfied (an interest of every index of a group with a code at K chunks, any
// other interest when every wanted index is held), then COMPLETE, or DECODING where the type's
// DecodeAtK says the node decodes at once. At completion the group's asks stop (stop_pull, the
// request check below, a probe's ask exempt), its promises close, and the holding carries the two
// claims: complete for stop_pull, claim_all for a group being decoded or held whole. A later duty
// (DecodeLater) is armed from known_at and fires unless the group became whole meanwhile
// (reconsider). Without the option nothing here runs.

const (
	seacastCollecting uint8 = iota
	seacastComplete
	seacastDecoding
	seacastInvalid
)

// seacastGroup is one group's state. Router goroutine.
type seacastGroup struct {
	state    uint8
	stopPull bool
	// reconsiderAt is the armed later duty's instant; zero when none is armed.
	reconsiderAt time.Time
	// probe is the group's one deferral on a claim (seacast_claims.go).
	probe *seacastProbe
	// decodeOp names the group's decode in flight; a result for another is stale. decodeHeld is the
	// held count its snapshot took; decodedOK that a decode from K held installed (seacast_order.go).
	decodeOp   uint64
	decodeHeld int
	decodedOK  bool
}

// WithSeacastMachine runs part 4's group machine on the path (seacast_complete.go). Requires
// WithSeacast first.
func WithSeacastMachine() Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok || gs.seacast == nil {
			return fmt.Errorf("the group machine requires WithSeacast first")
		}
		gs.seacast.machine = true
		gs.seacast.groups = make(map[string]*seacastGroup)
		return nil
	}
}

// groupOf is root's state, made on first use. Router goroutine.
func (sc *seacast) groupOf(root string) *seacastGroup {
	g := sc.groups[root]
	if g == nil {
		g = &seacastGroup{}
		sc.groups[root] = g
	}
	return g
}

// seacastInterestSatisfied is §1.5's interest_satisfied: K distinct chunks for an interest of every
// index (a group with a code), else every wanted index held. Router goroutine.
func (gs *GossipSubRouter) seacastInterestSatisfied(T ObjectType, root string, want *haveBits) bool {
	var held *haveBits
	if m := gs.have.mine[root]; m != nil {
		held = &m.bits
	}
	if held == nil {
		return false
	}
	n := T.IndexCount(root)
	if want.Count() >= n {
		return held.Count() >= T.Threshold(root)
	}
	for i := range n {
		idx := uint32(i) // lint:ignore uintcast -- an index below the group's count.
		if want.Has(idx) && !held.Has(idx) {
			return false
		}
	}
	return true
}

// seacastCheckCompletion is check_completion(g) for a path group, after a chunk of it was accepted
// or filled: the group completes, stops its asks, closes its promises, and either decodes at once or
// arms its later duty. The holding the caller sends next carries the new claims. Router goroutine.
func (gs *GossipSubRouter) seacastCheckCompletion(T ObjectType, root string) {
	sc := gs.seacast
	if sc == nil || !sc.machine || gs.have == nil {
		return
	}
	g := sc.groupOf(root)
	if g.state != seacastCollecting {
		return
	}
	want := gs.seacastInterestOfPeer(gs.p.host.ID(), root)
	if want == nil || want.Count() == 0 || !gs.seacastInterestSatisfied(T, root, want) {
		return
	}
	g.stopPull = true
	sc.ctr.GroupsCompleted.Add(1)
	gs.seacastDropQueuedAsks(root)
	p := T.Params()
	if p.DecodeAtK != nil && p.DecodeAtK(root) {
		g.state = seacastDecoding
	} else {
		g.state = seacastComplete
		if p.DecodeLater != nil {
			if d, ok := p.DecodeLater(root); ok {
				at, known := sc.knownAt[root]
				if !known {
					at = time.Now()
				}
				gs.seacastArm(root, at.Add(d))
			}
		}
	}
	gs.seacastClosePromises(root)
	if g.state == seacastDecoding {
		gs.seacastStartDecode(T, root)
	}
}

// seacastArm is arm(g, at): a later duty that fires at at unless re-armed or the group was taken up
// meanwhile. The timer is made on the router's goroutine and comes back through eval.
func (gs *GossipSubRouter) seacastArm(root string, at time.Time) {
	g := gs.seacast.groupOf(root)
	g.reconsiderAt = at
	gs.seacast.ctr.DutiesArmed.Add(1)
	time.AfterFunc(max(0, time.Until(at)), func() {
		select {
		case gs.p.eval <- func() { gs.seacastReconsider(root, at) }:
		case <-gs.p.ctx.Done():
		}
	})
}

// seacastReconsider is reconsider(g, at): a stale or re-armed timer, or a group no longer COMPLETE,
// does nothing; a group whole in the node's holdings needs nothing rebuilt; then the views and the
// claims in the draft's order (seacastReconsiderVerdict); otherwise the late
// decode claim, as at K. Router goroutine.
func (gs *GossipSubRouter) seacastReconsider(root string, at time.Time) {
	sc := gs.seacast
	g := sc.groups[root]
	if g == nil || !g.reconsiderAt.Equal(at) || g.state != seacastComplete {
		return
	}
	g.reconsiderAt = time.Time{}
	T := gs.seacastRootType(root)
	if T == nil {
		return
	}
	gs.seacastProbeVerdict(root, g) // (R5) the verdict on the claimant, once, before anything returns
	if m := gs.have.mine[root]; m != nil && m.bits.Count() >= T.IndexCount(root) {
		sc.ctr.DutiesCancelled.Add(1)
		return
	}
	if gs.seacastReconsiderVerdict(T, root, g) {
		return // the views and the claims (seacast_claims.go)
	}
	if gs.seacastKnownInScope(T, root).Count() >= T.IndexCount(root) {
		sc.ord(ordDecodeWhole) // a late decode of a group whole in the subnet's view (seacast_order.go)
	}
	g.state = seacastDecoding
	sc.ctr.LateDecodes.Add(1)
	sc.event(SeacastEvLateDecode, root)
	gs.seacastSendHolding(T, root)
	gs.seacastStartDecode(T, root)
}

// seacastStartDecode starts decode_group (§4.6) for root, off the router's goroutine
// (seacast_decode.go). Router goroutine.
func (gs *GossipSubRouter) seacastStartDecode(T ObjectType, root string) {
	gs.seacast.ctr.DecodesStarted.Add(1)
	gs.seacastDecodeStart(T, root)
}

// seacastSendHolding sends root's holding, with its claims, to the group's suppression scope (the
// topic's mesh on v0.3's wire), as an acceptance's holding goes. Router goroutine.
func (gs *GossipSubRouter) seacastSendHolding(T ObjectType, root string) {
	topic, ok := T.TopicOf(root)
	if !ok || gs.have == nil {
		return
	}
	m := gs.have.mine[root]
	if m == nil {
		return
	}
	id := gs.seacastEncodeHave(root, &m.bits, false)
	for p := range gs.mesh[topic] {
		if gs.feature(GossipSubFeatureIdontwant, gs.peers[p]) {
			gs.sendRPC(p, rpcWithControl(nil, nil, nil, nil, nil, []*pb.ControlIDontWant{{MessageIDs: []string{id}}}), true)
		}
	}
	gs.seacast.ctr.HoldingsSent.Add(1)
}

// seacastClosePromises is close_promises_of_group(g): nobody owes us chunks of a completed group.
// Router goroutine.
func (gs *GossipSubRouter) seacastClosePromises(root string) {
	if gs.have == nil {
		return
	}
	if gs.gossipTracer != nil { // the score's promises of the group's asks close with them
		gs.gossipTracer.fulfillPromisesWhere(func(mid string) bool { return gs.seacastAskRoot(mid) == root })
	}
	if gs.iwantPromised == nil {
		return
	}
	for mid := range gs.iwantPromised {
		r, _, ok := gs.have.index(mid)
		if !ok {
			r, _, ok = decodeWant(mid)
		}
		if ok && r == root {
			delete(gs.iwantPromised, mid)
			gs.seacast.ctr.PromisesClosed.Add(1)
		}
	}
}

// seacastAskRoot is the group an ask's id names, in any of its forms (a v4 WANT, a chunk id, a v1
// WANT); "" for none. Router goroutine.
func (gs *GossipSubRouter) seacastAskRoot(mid string) string {
	if r, _, _, _, ok, malformed := decodeWantV2(mid); ok && !malformed {
		return r
	}
	if gs.have != nil {
		if r, _, ok := gs.have.index(mid); ok && len(r) == haveRootLen {
			return r
		}
	}
	if r, _, ok := decodeWant(mid); ok {
		return r
	}
	return ""
}

// seacastDropQueuedAsks is stop_pull at the writers (seacast-lite.md §7.2 at completion; the review of
// part 4's gate, finding 1): the group's asks still queued to every peer leave no more, the probe's
// excepted; each closed as an ask dropped at the queue. Router goroutine, at
// the group's completion.
func (gs *GossipSubRouter) seacastDropQueuedAsks(root string) {
	sc := gs.seacast
	if gs.p == nil {
		return
	}
	drop := func(id string) bool {
		if gs.seacastAskRoot(id) != root {
			return false
		}
		r, index, ok := gs.seacastAskCell(id)
		return !(ok && sc.seacastProbeExempt(id, r, index)) // the probe's asks stay
	}
	for p, q := range gs.p.peers {
		for _, id := range q.discardAsks(drop) {
			gs.seacastAskClosed(p, id)
			sc.ctr.StopPullDiscards.Add(1)
		}
	}
}

// seacastGated is gated(a) for an ask on the path under the machine: the group's asks stop at its
// completion (stop_pull), an ask of a probed cell excepted, whatever its id (seacast.md §4.6's
// probe_asks, R5), and the probe's own ask by its id.
func (gs *GossipSubRouter) seacastGated(mid string) bool {
	sc := gs.seacast
	if sc == nil || !sc.machine || gs.have == nil {
		return false
	}
	// A v4 WANT names its group first (the probe's, the sample's), whatever the id parser makes of
	// it, as parkScopeOf reads it (the review of part 4's build, finding 2: a permissive parser
	// read its bytes as another root, and the ask passed without the exemption).
	root, index, ok := gs.seacastAskCell(mid)
	if !ok {
		return false
	}
	g := sc.groups[root]
	if g == nil || !g.stopPull {
		return false
	}
	if sc.seacastProbeExempt(mid, root, index) {
		return false // (R5) the probed cell, whatever id the ask carries
	}
	sc.ctr.StopPullDeclined.Add(1)
	return true
}

// seacastStopPulled is the retry's own test of stop_pull (seacast-lite.md §6.1's retry_offer and ask_parked, spec
// 43fe953): the group's asks stopped at its completion, with no exemption. The probe's (probe_asks) passes gated,
// which on_offer and the writer read; a retry of the probed cell still stops. Router goroutine.
func (gs *GossipSubRouter) seacastStopPulled(mid string) bool {
	sc := gs.seacast
	if sc == nil || !sc.machine || gs.have == nil {
		return false
	}
	root, _, ok := gs.seacastAskCell(mid)
	if !ok {
		return false
	}
	g := sc.groups[root]
	return g != nil && g.stopPull
}

// requestAllowed is the request check every ask passes: the machine's stop_pull. Without the
// machine every ask passes.
func (gs *GossipSubRouter) requestAllowed(p peer.ID, topic, mid string) bool {
	return !gs.seacastGated(mid)
}
