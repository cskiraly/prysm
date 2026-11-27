package pubsub

import (
	"fmt"
	"time"

	core "github.com/cskiraly/seacast-go"
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
	seacastCollecting = core.Collecting
	seacastComplete   = core.Complete
	seacastDecoding   = core.Decoding
	seacastInvalid    = core.Invalid
)

// seacastGroup is one group's state in the core's machine (seacast/machine.go). Its Ext is the
// group's one deferral on a claim (seacast_claims.go). Router goroutine.
type seacastGroup = core.Group[*seacastProbe]

// WithSeacastMachine runs part 4's group machine on the path (seacast_complete.go). Requires
// WithSeacast first.
func WithSeacastMachine() Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok || gs.seacast == nil {
			return fmt.Errorf("the group machine requires WithSeacast first")
		}
		gs.seacast.machine = true
		gs.seacast.gm = core.NewMachine[*seacastProbe]()
		return nil
	}
}

// seacastGID is the machine's key for root: the payload group's GroupID (seacast_wire.go), or, for
// a root of another length, which the path never names, the root itself as an opaque key.
func seacastGID(root string) core.GroupID {
	if g, ok := seacastGroupID(root); ok {
		return g
	}
	return core.GroupID(root)
}

// seacastGIDRoot is the root a machine key names, seacastGID's inverse.
func seacastGIDRoot(g core.GroupID) string {
	if root, ok := seacastRootOf(g); ok {
		return root
	}
	return string(g)
}

// groupOf is root's state, made on first use. Router goroutine.
func (sc *seacast) groupOf(root string) *seacastGroup { return sc.gm.Group(seacastGID(root)) }

// group is root's state; nil when it has none or the machine is off. Router goroutine.
func (sc *seacast) group(root string) *seacastGroup {
	if sc.gm == nil {
		return nil
	}
	return sc.gm.Lookup(seacastGID(root))
}

// forgetGroup drops root's state. Router goroutine.
func (sc *seacast) forgetGroup(root string) {
	if sc.gm != nil {
		sc.gm.Forget(seacastGID(root))
	}
}

// seacastCheckCompletion is check_completion(g) for a path group (the core's
// Machine.CheckCompletion), after a chunk of it was accepted or filled. Router goroutine.
func (gs *GossipSubRouter) seacastCheckCompletion(T ObjectType, root string) {
	sc := gs.seacast
	if sc == nil || !sc.machine || gs.have == nil {
		return
	}
	sc.gm.CheckCompletion(gs.seacastEnv(T), seacastGID(root))
}

// seacastArm is arm(g, at) (the core's Machine.Arm): the timer comes back through eval as
// seacastReconsider. Router goroutine.
func (gs *GossipSubRouter) seacastArm(root string, at time.Time) {
	gs.seacast.gm.Arm(gs.seacastEnv(nil), seacastGID(root), at)
}

// seacastReconsider is reconsider(g, at) (the core's Machine.Reconsider). Router goroutine.
func (gs *GossipSubRouter) seacastReconsider(root string, at time.Time) {
	if gs.seacast.gm == nil {
		return
	}
	gs.seacast.gm.Reconsider(gs.seacastEnv(gs.seacastRootType(root)), seacastGID(root), at)
}

// seacastSendHolding sends root's holding, with its claims, to the group's suppression scope (the
// topic's mesh on v0.3's wire), as an acceptance's holding goes (seacastHoldingID). Router goroutine.
func (gs *GossipSubRouter) seacastSendHolding(T ObjectType, root string) {
	topic, ok := T.TopicOf(root)
	if !ok {
		return
	}
	id, ok := gs.seacastHoldingID(root)
	if !ok {
		return
	}
	for p := range gs.mesh[topic] {
		if gs.feature(GossipSubFeatureIdontwant, gs.peers[p]) {
			gs.sendRPC(p, rpcWithControl(nil, nil, nil, nil, nil, []*pb.ControlIDontWant{{MessageIDs: []string{id}}}), true)
		}
	}
	gs.seacast.ctr.HoldingsSent.Add(1)
}

// seacastHoldingID is the holding seacastSendHolding sends for a group the node holds: its bitmap is
// have_bits (haveBitsFor), so for a group the node publishes, the chunks acknowledged so far, not
// what it holds. ok is false when nothing goes: no group held, or none of it acknowledged.
func (gs *GossipSubRouter) seacastHoldingID(root string) (id string, ok bool) {
	if gs.have == nil || gs.have.mine[seacastGID(root)] == nil {
		return "", false
	}
	bits := gs.have.haveBitsFor(root)
	if bits == nil {
		return "", false
	}
	return gs.seacastEncodeHave(root, bits, false), true
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
	if gs.pullOn() {
		// A chunk's promises are the core's (seacast_pull.go).
		gs.seacast.ctr.PromisesClosed.Add(int64(gs.pull.promises.CloseGroup(seacastGID(root))))
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
	g := sc.group(root)
	if g == nil || !g.StopPull {
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
	return ok && gs.seacastStopPulledRoot(root)
}

// seacastStopPulledRoot says whether root's group stopped pulling at its completion. Router goroutine.
func (gs *GossipSubRouter) seacastStopPulledRoot(root string) bool {
	sc := gs.seacast
	if sc == nil || !sc.machine || gs.have == nil {
		return false
	}
	g := sc.group(root)
	return g != nil && g.StopPull
}

// seacastUnauthenticable says whether mid is a chunk of a registered group whose gate is closed: no
// installed block's bid commits it yet (seacast-lite.md §4.4's authenticable, §3, "the block first").
// Its offer is recorded and not asked; the gate's opening asks for it from the ledger
// (seacastOpenGate). With the chunk pull off there is no ledger, and the offer is asked as before.
// Router goroutine.
func (gs *GossipSubRouter) seacastUnauthenticable(mid string) bool {
	if gs.seacast == nil || !gs.pullOn() {
		return false
	}
	root, _, ok := gs.seacastAskCell(mid)
	if !ok {
		return false
	}
	T := gs.seacastRootOwner(root)
	if T == nil || T.HeaderKnown(root) {
		return false
	}
	gs.seacast.noteGateMet(root) // the gate's opening asks for it (seacastOpenGate)
	return true
}

// requestAllowed is the request check every ask passes: the machine's stop_pull. Without the
// machine every ask passes.
func (gs *GossipSubRouter) requestAllowed(p peer.ID, topic, mid string) bool {
	return !gs.seacastGated(mid)
}
