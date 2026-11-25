package pubsub

// Admission by outstanding WANT (seacast.md §4.10's sampler on v0.3's wire):
// a sampler subscribes to nothing, so the stock receive path drops its answers before any admission
// hook runs (the subscription condition at the head of handleIncomingRPC). A WANT the path sends
// registers (group, index, asked peer) with the receive path, which admits that chunk from that
// peer only; the registration lives until the group expires, apart from the application's interest
// (not a retry, not the first accepted copy, not the sample's verdict ends it), so a late duplicate
// from a peer asked earlier is admitted and acknowledged like any duplicate. Nothing else is
// admitted on an unsubscribed topic: no other index, no other peer. The standing and routed forms
// of the WANT (§4.5) come with part 2; this is the plain indexed ask and its admission.

import (
	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
)

// seacastAsk sends p a v4 WANT for (root, index) and registers the answer's admission. Router
// goroutine. A plain ask goes on the urgent lane as the phase path's asks do; a standing or
// routed WANT (a sampler's, a forwarder's) goes on the ordinary lane, the draft's
// send(..., urgent=False) (§4.10, §4.5), and starts no promise (seacast_standing.go).
func (gs *GossipSubRouter) seacastAsk(p peer.ID, root string, index uint32, standing bool, hops uint8) bool {
	sc := gs.seacast
	if sc == nil || len(root) != haveRootLen {
		return false
	}
	if _, connected := gs.peers[p]; !connected {
		return false
	}
	if !gs.seacastMayBeAsked(p, root, index, hops > 0) {
		sc.ctr.AskOutside.Add(1) // outside may_be_asked (§4.5, P7): refused at the sender
		return false
	}
	sc.registerAsk(root, index, p)
	want := encodeWantV2(root, index, standing, hops)
	gs.sendRPC(p, rpcWithControl(nil, nil, []*pb.ControlIWant{{MessageIDs: []string{want}}}, nil, nil, nil), !standing && hops == 0)
	sc.ctr.WantsSent.Add(1)
	return true
}

// registerAsk records that (root, index) was asked of p: the answer's admission on a topic the
// node has not subscribed to (seacastExpects). Router goroutine.
func (sc *seacast) registerAsk(root string, index uint32, p peer.ID) {
	if sc.asked == nil {
		sc.asked = map[string]map[uint32]map[peer.ID]struct{}{}
	}
	g := sc.asked[root]
	if g == nil {
		g = map[uint32]map[peer.ID]struct{}{}
		sc.asked[root] = g
	}
	peers := g[index]
	if peers == nil {
		peers = map[peer.ID]struct{}{}
		g[index] = peers
	}
	if _, dup := peers[p]; dup {
		return
	}
	peers[p] = struct{}{}
	if sc.askedOf == nil {
		sc.askedOf = map[string]map[peer.ID]int{}
	}
	if sc.askedOf[root] == nil {
		sc.askedOf[root] = map[peer.ID]int{}
	}
	sc.askedOf[root][p]++
}

// seacastNoteAsked registers the pull discipline's asks of p for chunks of the path's groups, by
// index or by id, as the path's own WANTs are registered (registerAsk): every ask the node makes
// is then known at the arrival. Router goroutine, from noteIWantSent, the one point every IWANT
// send passes.
func (gs *GossipSubRouter) seacastNoteAsked(p peer.ID, mids []string) {
	sc := gs.seacast
	if sc == nil || gs.have == nil {
		return
	}
	for _, mid := range mids {
		root, index, ok := decodeWant(mid)
		if !ok {
			root, index, ok = gs.have.index(mid)
		}
		if !ok || len(root) != haveRootLen || gs.seacastRootType(root) == nil {
			continue
		}
		sc.registerAsk(root, index, p)
	}
}

// SeacastWant sends p a v4 WANT for the chunk at index of the group root with its flags: standing,
// answered when the chunk arrives as well as from what p holds (§4.5, §4.10); hops, the routed
// request's steps left. It registers the answer's admission as SeacastAsk does.
// Any goroutine; false when p is not connected or the path is not installed.
func (p *PubSub) SeacastWant(to peer.ID, root string, index uint32, standing bool, hops uint8) bool {
	gs, ok := p.rt.(*GossipSubRouter)
	if !ok || gs.seacast == nil {
		return false
	}
	done := make(chan bool, 1)
	select {
	case p.eval <- func() { done <- gs.seacastAsk(to, root, index, standing, hops) }:
	case <-p.ctx.Done():
		return false
	}
	select {
	case sent := <-done:
		return sent
	case <-p.ctx.Done():
		return false
	}
}

// seacastExpects is the receive path's admission of a chunk on a topic we have not subscribed to:
// true when the chunk's (group, index) was asked of the sender and the group has not expired.
// processLoop goroutine, from handleIncomingRPC ahead of the seen check and validation.
func (gs *GossipSubRouter) seacastExpects(m *pb.Message, from peer.ID) bool {
	sc := gs.seacast
	if sc == nil || gs.have == nil || sc.asked == nil {
		return false
	}
	if gs.seacastType(m.GetTopic()) == nil {
		return false
	}
	mid := gs.p.idGen.ID(&Message{Message: m})
	root, index, ok := gs.have.index(mid)
	if !ok || len(root) != haveRootLen {
		return false
	}
	if _, asked := sc.asked[root][index][from]; !asked {
		return false
	}
	sc.ctr.AdmittedByWant.Add(1)
	return true
}

// askedForget drops the registrations of the groups named (expired).
func (sc *seacast) askedForget(roots []string) {
	for _, r := range roots {
		delete(sc.asked, r)
		delete(sc.askedOf, r)
	}
}

// SeacastAsk sends p a WANT for the chunk at index of the group root on the path, and registers
// the answer's admission: the sampler's ask (§4.10). Any goroutine; false when p is not connected
// or the path is not installed.
func (p *PubSub) SeacastAsk(to peer.ID, root string, index uint32) bool {
	gs, ok := p.rt.(*GossipSubRouter)
	if !ok || gs.seacast == nil {
		return false
	}
	done := make(chan bool, 1)
	select {
	case p.eval <- func() { done <- gs.seacastAsk(to, root, index, false, 0) }:
	case <-p.ctx.Done():
		return false
	}
	select {
	case sent := <-done:
		return sent
	case <-p.ctx.Done():
		return false
	}
}
