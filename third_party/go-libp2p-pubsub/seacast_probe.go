package pubsub

import (
	"time"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
)

// --- The probe of a claim, marked, and its verdict on the claimant (seacast.md §4.6, R5) ---
//
// The probe asks one claimant for one cell the member lacks, drawn at random. Its WANT carries the
// probe flag. Any ask of the probed cell passes stop_pull, whatever id it carries (probe_asks by
// (group, index)), so another peer's offer of the cell is asked and its copy fills the cell for the
// member's duty. The verdict on the claimant reads only the claimant's own copy, first or a
// duplicate, in either axis's form; it is taken once, at reconsider, and a failed verdict
// penalises the claimant. As a claimant, the node answers the first marked ask per asker and group,
// of an index it claims, whatever the asker's holding says, and its writer lets that answer through
// the duplicate cancel once. As an asker, its writer keeps its waiting marked probe though it holds
// the cell by then.
//
// Three cases the text leaves open stay open here: a probe reaching a claimant still decoding (the
// ask of an unheld index is dropped), and a probe written late or not at all (its promise starts at
// the decision). The third, a claimant that has left, stays as this package had it: claimants are
// drawn among the connected ones, and a close cancels the probe, its promise and its verdict.

// seacastProbeOfClaim is probe_of_claim(p, gi) for an ask of root's index by p: we claim the index
// and no marked ask of the group from p was answered yet. Router goroutine.
func (gs *GossipSubRouter) seacastProbeOfClaim(p peer.ID, root string, index uint32) bool {
	sc := gs.seacast
	if sc == nil || !sc.machine || gs.have == nil {
		return false
	}
	if sc.probeServed[p][root] {
		return false // the cap: a later marked ask of the group is a plain one
	}
	return gs.seacastClaimsIndex(root, index)
}

// seacastClaimsIndex reports whether our holding of root claims index: held, or every index while
// we decode it or hold it whole (holding_of's claim_all). Router goroutine.
func (gs *GossipSubRouter) seacastClaimsIndex(root string, index uint32) bool {
	if gs.seacastHeld(root, index) {
		return true
	}
	g := gs.seacast.group(root)
	return g != nil && g.State == seacastDecoding
}

// seacastServeProbe is serve_probe(p, g, f): the cap for p's marked asks of root is taken, and our
// answer mid to p passes p's writer's duplicate cancel once (card 8). Router goroutine, as the
// answer is decided.
func (gs *GossipSubRouter) seacastServeProbe(p peer.ID, root, mid string) {
	sc := gs.seacast
	if sc.probeServed == nil {
		sc.probeServed = map[peer.ID]map[string]bool{}
	}
	if sc.probeServed[p] == nil {
		sc.probeServed[p] = map[string]bool{}
	}
	sc.probeServed[p][root] = true
	sc.ctr.ProbesServed.Add(1)
	r, index, ok := gs.have.index(mid)
	if !ok {
		return
	}
	q := gs.p.peers[p]
	if q == nil || q.have == nil {
		return
	}
	q.queueMu.Lock()
	defer q.queueMu.Unlock()
	if q.have.probeAnswers == nil {
		q.have.probeAnswers = map[string]*haveBits{}
	}
	if q.have.probeAnswers[r] == nil {
		q.have.probeAnswers[r] = &haveBits{}
	}
	q.have.probeAnswers[r].Set(index)
}

// seacastProbeWait marks (on) or unmarks our probe mid of p as waiting at p's writer (probe_waits).
// Router goroutine.
func (gs *GossipSubRouter) seacastProbeWait(p peer.ID, mid string, on bool) {
	if gs.p == nil {
		return
	}
	q := gs.p.peers[p]
	if q == nil || q.have == nil {
		return
	}
	q.queueMu.Lock()
	defer q.queueMu.Unlock()
	if !on {
		delete(q.have.probeWaits, mid)
		return
	}
	if q.have.probeWaits == nil {
		q.have.probeWaits = map[string]struct{}{}
	}
	q.have.probeWaits[mid] = struct{}{}
}

// seacastProbeCopy is probe_answer(g, i, p) for a copy of root's index from p, accepted or an
// accepted duplicate: the claimant's own copy passes the probe of that cell. Router goroutine.
func (gs *GossipSubRouter) seacastProbeCopy(root string, index uint32, from peer.ID) {
	sc := gs.seacast
	if sc == nil || !sc.machine || sc.gm == nil || sc.gm.Len() == 0 {
		return
	}
	gs.seacastProbeCopyOf(root, index, from)
}

func (gs *GossipSubRouter) seacastProbeCopyOf(root string, index uint32, from peer.ID) {
	g := gs.seacast.group(root)
	if g == nil || g.Ext == nil {
		return
	}
	pr := g.Ext
	if pr.index != index || pr.asked != from || pr.own || pr.verdict || pr.cancelled {
		return
	}
	pr.own = true
	gs.seacast.ctr.ProbesOwnCopy.Add(1)
	gs.seacastProbeWait(pr.asked, pr.mid, false)
}

// seacastProbeVerdict is reconsider's first step (seacast.md §4.6, R5): the verdict on the
// claimant, once, before anything else reconsider decides; only its own copy passes it. A probe
// cancelled by the claimant's close gets none. Router goroutine.
func (gs *GossipSubRouter) seacastProbeVerdict(root string, g *seacastGroup) {
	pr := g.Ext
	if pr == nil || pr.cancelled || pr.verdict {
		return
	}
	pr.verdict = true
	gs.seacastProbeWait(pr.asked, pr.mid, false)
	gs.seacastProbeSettle(pr.asked, pr.mid) // still queued, it is not written (probe_settled)
	sc := gs.seacast
	if !pr.sent && !pr.own {
		// (R5, spec 43fe953) not written by now and not answered by the claimant's own copy (probe_answer, which
		// passes it whenever it comes): no verdict, and our own repair goes on below.
		pr.noVerdict = true
		sc.ctr.ProbesNoVerdict.Add(1)
		return
	}
	if pr.own {
		sc.ctr.VerdictsPassed.Add(1)
		sc.event(SeacastEvVerdictPassed, root)
		return
	}
	sc.ctr.Penalties.Add(1)
	sc.event(SeacastEvPenalty, root)
}

// seacastAskCell resolves the cell an ask id names, whatever its form: a v4 WANT first (its group
// named first, as parkScopeOf reads it), then the id parser, then a v1 WANT.
func (gs *GossipSubRouter) seacastAskCell(mid string) (string, uint32, bool) {
	root, index, _, _, ok, malformed := decodeWantV2(mid)
	if ok && !malformed {
		return root, index, true
	}
	if gs.have != nil {
		if root, index, ok = gs.have.index(mid); ok {
			return root, index, true
		}
	}
	return decodeWant(mid)
}

// seacastProbeExempt reports whether an ask id passes stop_pull as a probe's: by its literal id
// (the probe's own ask), or by the cell it names (seacast.md's probe_asks).
// Router goroutine.
func (sc *seacast) seacastProbeExempt(mid, root string, index uint32) bool {
	if _, exempt := sc.probeAsks[mid]; exempt {
		return true
	}
	b := sc.probeCells[root]
	return b != nil && b.Has(index)
}

// seacastProbeQueuesForget drops the probe's state of root at every peer's writer, at the group's
// expiry (seacast.md §3: the probe's tables go with the group): the answers to a peer's probe not
// yet written, and our own waiting probes. Router goroutine, from expireGroup.
func (gs *GossipSubRouter) seacastProbeQueuesForget(root string) {
	if gs.p == nil {
		return
	}
	for _, q := range gs.p.peers {
		if q == nil || q.have == nil {
			continue
		}
		q.queueMu.Lock()
		delete(q.have.probeAnswers, root)
		for mid := range q.have.probeWaits {
			if r, _, _, _, ok, _ := decodeWantV2(mid); ok && r == root {
				delete(q.have.probeWaits, mid)
			}
		}
		q.queueMu.Unlock()
	}
}

// seacastProbeWritten is filter_entry's (R5) line at the write of our probe of p (seacast.md §4.6, spec
// 43fe953): probe_sent; its promise runs DEFER_WINDOW from now, and the verdict waits until the later of that
// and the duty's own time. Router goroutine, from the writer's report.
func (gs *GossipSubRouter) seacastProbeWritten(p peer.ID, mids []string) {
	sc := gs.seacast
	if sc == nil {
		return
	}
	for _, mid := range mids {
		root, _, ok := gs.seacastAskCell(mid)
		if !ok {
			continue
		}
		g := sc.group(root)
		if g == nil || g.Ext == nil {
			continue
		}
		pr := g.Ext
		if pr.mid != mid || pr.asked != p || pr.sent || pr.verdict || pr.cancelled {
			continue
		}
		pr.sent = true
		T := gs.seacastRootType(root)
		window := time.Second
		if T != nil && T.Params().DeferWindow > 0 {
			window = T.Params().DeferWindow
		}
		index, at := pr.index, pr.at
		time.AfterFunc(window, func() {
			select {
			case gs.p.eval <- func() { gs.seacastProbeDue(root, mid, p, index, at) }:
			case <-gs.p.ctx.Done():
			}
		})
		due := time.Now().Add(window)
		if g.ReconsiderAt.After(due) {
			due = g.ReconsiderAt
		}
		gs.seacastArm(root, due)
	}
}

// seacastProbeSettle marks our probe mid of p settled at p's writer: still queued, it is not written
// (probe_settled). Router goroutine.
func (gs *GossipSubRouter) seacastProbeSettle(p peer.ID, mid string) {
	if gs.p == nil {
		return
	}
	q := gs.p.peers[p]
	if q == nil || q.have == nil {
		return
	}
	q.queueMu.Lock()
	defer q.queueMu.Unlock()
	if q.have.probeSettled == nil {
		q.have.probeSettled = map[string]struct{}{}
	}
	q.have.probeSettled[mid] = struct{}{}
}

// seacastKeepProbe is on_want's probe_waiting (seacast.md §4.5, §4.6; spec 43fe953): p's marked ask of a
// claimed index we have not stored, while we decode, is kept, the first per asker and group, in place of p's
// standing WANT of that index. False when p has one kept for the group already. Router goroutine.
func (gs *GossipSubRouter) seacastKeepProbe(p peer.ID, root string, index uint32) bool {
	sc := gs.seacast
	if sc == nil {
		return false
	}
	if _, kept := sc.probeWaiting[root][p]; kept {
		return false
	}
	if sc.probeWaiting == nil {
		sc.probeWaiting = map[string]map[peer.ID]uint32{}
	}
	if sc.probeWaiting[root] == nil {
		sc.probeWaiting[root] = map[peer.ID]uint32{}
	}
	sc.probeWaiting[root][p] = index
	if peers := sc.standing[root][index]; peers != nil {
		delete(peers, p)
	}
	sc.ctr.ProbesKept.Add(1)
	return true
}

// seacastProbeKept says whether p's kept probe of root is of index: then p's standing WANT of the index is
// not kept as well.
func (gs *GossipSubRouter) seacastProbeKept(p peer.ID, root string, index uint32) bool {
	sc := gs.seacast
	if sc == nil {
		return false
	}
	j, kept := sc.probeWaiting[root][p]
	return kept && j == index
}

// seacastServeWaitingProbe is serve_waiting_probe(g, i): the cell a kept probe waited for is stored, received
// or rebuilt, and is answered as on_want answers a probe, once, with its exemption, within SERVE_LIMIT.
// Router goroutine, beside every seacastServeStanding.
func (gs *GossipSubRouter) seacastServeWaitingProbe(T ObjectType, root string, index uint32, msg *Message) {
	sc := gs.seacast
	if sc == nil || len(sc.probeWaiting[root]) == 0 {
		return
	}
	limit := T.Params().ServeLimit
	mid := gs.p.idGen.ID(msg)
	for q, j := range sc.probeWaiting[root] {
		if j != index {
			continue
		}
		delete(sc.probeWaiting[root], q)
		if sc.probeServed[q][root] || sc.served.Limited(seacastGID(root), q, limit) {
			continue
		}
		if _, connected := gs.peers[q]; !connected {
			continue
		}
		gs.seacastNoteServed(q, root)
		gs.seacastServeProbe(q, root, mid)
		gs.sendingSolicited = true
		sent := gs.sendRPC(q, rpcWithControl([]*pb.Message{msg.Message}, nil, nil, nil, nil, nil), false)
		gs.sendingSolicited = false
		if sent {
			gs.haveNotePushed(q, mid)
		}
	}
	if len(sc.probeWaiting[root]) == 0 {
		delete(sc.probeWaiting, root)
	}
}
