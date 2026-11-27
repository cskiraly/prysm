package pubsub

// The standing table and the serving limit (seacast.md §4.5: on_want's declared and standing
// branches and serve_standing; §4.10's standing request; §6.3's SERVE_LIMIT, [R6]).
// An ask the node cannot answer now is kept when the asker is DECLARED for the type or
// the WANT says standing, and answered when the chunk arrives (the fill) or is published (the
// node's own); a DECLARED asker's WANT is also its interest (want_p). SERVE_LIMIT bounds the
// chunks answered to one peer per group, the ask at the limit refused without penalty and a
// standing request not answered past it. A standing answer goes on the ordinary lane as any
// answer; one a full queue refuses is owed as an answer where v0.5 runs (oweRefused, through
// rpc.solicited). The table and the counts go with the group at its expiry.

import (
	core "github.com/cskiraly/seacast-go"
	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
)

// seacastDeclare records a DECLARED asker's WANT as its interest for the group (want_p[p][g],
// §4.5, §1.2) and reports whether p is DECLARED for the root's type: then the ask is standing
// whether or not the WANT says so (§2: a sampler's WANT always is). Router goroutine, from
// handleIWant, before anything refuses the ask.
func (gs *GossipSubRouter) seacastDeclare(p peer.ID, root string, index uint32) bool {
	sc := gs.seacast
	if sc == nil {
		return false
	}
	j, _, ok := gs.seacastJoinOf(p, root)
	if !ok || j.Interest != InterestDeclared {
		return false
	}
	if sc.wantP == nil {
		sc.wantP = map[peer.ID]map[string]*haveBits{}
	}
	g := sc.wantP[p]
	if g == nil {
		g = map[string]*haveBits{}
		sc.wantP[p] = g
	}
	b := g[root]
	if b == nil {
		b = &haveBits{}
		g[root] = b
	}
	b.Set(index)
	return true
}

// seacastServeLimited reports whether p is at the type's SERVE_LIMIT for root ([R6]): the ask
// is refused without penalty, counted. Router goroutine, from handleIWant.
func (gs *GossipSubRouter) seacastServeLimited(p peer.ID, root string) bool {
	sc := gs.seacast
	if sc == nil {
		return false
	}
	T := gs.seacastRootType(root)
	if T == nil {
		return false
	}
	limit := T.Params().ServeLimit
	if !sc.served.Limited(seacastGID(root), p, limit) {
		return false
	}
	sc.ctr.ServeLimited.Add(1)
	return true
}

// seacastServed counts an answer to p for the id's group (served[p][g]++). Router goroutine, from
// handleIWant at the answer.
func (gs *GossipSubRouter) seacastServed(p peer.ID, mid string) {
	sc := gs.seacast
	if sc == nil || gs.have == nil {
		return
	}
	root, _, ok := gs.have.index(mid)
	if !ok || len(root) != haveRootLen {
		return
	}
	gs.seacastNoteServed(p, root)
}

// seacastNoteServed counts an answer to p for root: served[p][g]. Router goroutine.
func (gs *GossipSubRouter) seacastNoteServed(p peer.ID, root string) {
	gs.seacast.noteServed(p, root)
}

func (sc *seacast) noteServed(p peer.ID, root string) {
	if sc.served == nil {
		sc.served = core.Served[peer.ID]{}
	}
	sc.served.Note(seacastGID(root), p)
}

// seacastStand keeps p's request for (root, index) until the chunk is here (§4.5:
// standing[g][i].add(p)). Counted once per (root, index, peer). Router goroutine, from
// handleIWant when the ask resolves to nothing held.
func (gs *GossipSubRouter) seacastStand(p peer.ID, root string, index uint32) {
	sc := gs.seacast
	if sc == nil {
		return
	}
	if sc.standing == nil {
		sc.standing = map[string]map[uint32]map[peer.ID]struct{}{}
	}
	g := sc.standing[root]
	if g == nil {
		g = map[uint32]map[peer.ID]struct{}{}
		sc.standing[root] = g
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
	sc.ctr.StandingKept.Add(1)
}

// seacastServeStanding is serve_standing(g, i) (§4.5): the chunk a standing request waited for is
// here, accepted or published by the node. Each waiting peer whose holding lacks the index and
// whose served count is under the type's limit gets it as a solicited answer on the ordinary
// lane, counted against the limit and noted as pushed so that no offer follows it (§4.4); a peer
// that holds it or is gone gets nothing; one at the limit is skipped and counted; a full queue
// refuses the answer, counted, and v0.5's writer owes it (rpc.solicited, oweRefused). Router
// goroutine, from seacastAccepted and seacastStoreOwn.
func (gs *GossipSubRouter) seacastServeStanding(T ObjectType, root string, index uint32, msg *Message) {
	sc := gs.seacast
	if sc == nil {
		return
	}
	peers := sc.standing[root][index]
	if len(peers) == 0 {
		return
	}
	if gs.seacastHoards() {
		if b := sc.rebuilt[root]; b != nil && b.Has(index) {
			delete(sc.standing[root], index) // a hoarder drops a standing request for a rebuilt chunk
			return
		}
	}
	delete(sc.standing[root], index)
	if len(sc.standing[root]) == 0 {
		delete(sc.standing, root)
	}
	limit := T.Params().ServeLimit
	mid := gs.p.idGen.ID(msg)
	for q := range peers {
		if gs.havePeerHolds(q, root, index) {
			continue // it has it: nothing to answer (§4.5)
		}
		if sc.served.Limited(seacastGID(root), q, limit) {
			sc.ctr.StandingLimited.Add(1)
			continue
		}
		if _, connected := gs.peers[q]; !connected {
			continue
		}
		gs.seacastNoteServed(q, root)
		gs.sendingSolicited = true
		sent := gs.sendRPC(q, rpcWithControl([]*pb.Message{msg.Message}, nil, nil, nil, nil, nil), false)
		gs.sendingSolicited = false
		if !sent {
			sc.ctr.StandingRefused.Add(1)
			continue
		}
		sc.ctr.StandingServed.Add(1)
		gs.haveNotePushed(q, mid)
	}
}

// standingForget drops the standing requests and the served counts of root (expired); the
// requests dropped are counted.
func (sc *seacast) standingForget(root string) {
	for _, peers := range sc.standing[root] {
		sc.ctr.StandingExpired.Add(int64(len(peers)))
	}
	delete(sc.standing, root)
	sc.served.Forget(seacastGID(root))
	delete(sc.probeWaiting, root) // (R5) the probes kept while we decoded go with the group
}
