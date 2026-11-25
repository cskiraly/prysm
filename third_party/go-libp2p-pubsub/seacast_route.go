package pubsub

// may_be_asked (seacast.md §4.5, P7): the asker's rule. An ask may go to a peer that offered
// the index, whose interest holds it, that claims the whole group, or, for a routed request, whose
// duty is FORWARD; the path's own asks are refused at the sender outside it, the ordinary asks
// counted. The fork has no sampler and does not pass a routed request on (§4.5's
// route_want): no node samples the payload, so no conformant peer routes a request for one of its
// groups, and the receiver takes a WANT with hops as malformed (seacastWantIn).

import (
	"github.com/libp2p/go-libp2p/core/peer"
)

// seacastMayBeAsked is may_be_asked(p, g, i, routed) (§4.5, P7): p offered or holds the index,
// p's interest holds it, p claims the whole group, or the request is routed and p's duty is
// FORWARD. True off the path. Router goroutine.
func (gs *GossipSubRouter) seacastMayBeAsked(p peer.ID, root string, index uint32, routed bool) bool {
	sc := gs.seacast
	if sc == nil || sc.roles == nil {
		return true // without a role table the asker reads no interest: the rule is the table's
	}
	T := gs.seacastRootType(root)
	if T == nil {
		return true // not a group of the path
	}
	if gs.have != nil {
		if e := gs.have.peers[p][root]; e != nil && (e.claimAll || e.bits.Has(index)) {
			return true // offered us, or holds it by its own word
		}
	}
	if in := gs.seacastInterestOfPeer(p, root); in != nil && in.Has(index) {
		return true
	}
	if routed {
		if j, ok := sc.roles.JoinOf(p, T); ok && j.Duty == DutyForward {
			return true // [D12] a routed request may go to any forwarding node
		}
	}
	return false
}

// seacastNoteAskOutside counts an ordinary ask (v0.5's sendAsk: the retry, the last resort)
// written outside may_be_asked, for the gate's count; the ordinary asks go to announcers, so the
// count is expected at zero. Router goroutine.
func (gs *GossipSubRouter) seacastNoteAskOutside(p peer.ID, mid string) {
	sc := gs.seacast
	if sc == nil || gs.have == nil {
		return
	}
	root, index, ok := decodeWant(mid)
	if !ok {
		if root, index, ok = gs.have.index(mid); !ok {
			return
		}
	}
	if len(root) != haveRootLen || sc.roots[root] == nil {
		return
	}
	if !gs.seacastMayBeAsked(p, root, index, false) {
		sc.ctr.AskOutside.Add(1)
	}
}
