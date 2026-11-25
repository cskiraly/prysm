package pubsub

// Gossipsub's two IHAVE limits as written (seacast.md §4.3 and §4.8, seacast-lite.md §4.4 v0.7).
// With the path installed, MaxIHaveMessages counts the offer-bearing
// RPCs from a peer within a heartbeat and MaxIHaveLength the asks made of it in reply to its
// offers; over either, the peer's offers are still recorded and what the node would ask of it is
// held back to the next heartbeat, where the retry runs for it with its checks (handleIHave,
// askFrom, clearIHaveCounters). Only asks in reply to offers count: the retry's and the last
// resort's do not (sendAsk, retryOfferedPull). The limits delay asks and lose no offer.

import (
	"sort"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
)

// seacastHoldBack keeps the chunks whose ask a limit held back, with the peer that offered them
// (empty for a re-drive without one). Router goroutine.
func (gs *GossipSubRouter) seacastHoldBack(mids []string, p peer.ID) {
	sc := gs.seacast
	if sc == nil || len(mids) == 0 {
		return
	}
	if sc.heldBack == nil {
		sc.heldBack = map[string]map[peer.ID]struct{}{}
	}
	for _, mid := range mids {
		peers, held := sc.heldBack[mid]
		if !held {
			peers = map[peer.ID]struct{}{}
			sc.heldBack[mid] = peers
			sc.ctr.HeldBack.Add(1)
		}
		if p != "" {
			peers[p] = struct{}{}
		}
	}
}

// seacastForgetHeldBack drops the held-back asks of root's group, whose registration or lifetime
// ended. After a forgotten registration, the next heartbeat's retry could ask for them and register
// the root again, subject to its own checks; after an expiry, the retry's own check skips them.
// Router goroutine.
func (gs *GossipSubRouter) seacastForgetHeldBack(root string) {
	for mid := range gs.seacast.heldBack {
		if r, _, ok := gs.groupIndex(mid); ok && r == root {
			delete(gs.seacast.heldBack, mid)
		}
	}
}

// seacastRetryHeldBack runs the retry for every chunk a limit held back, once the limits started
// again at the heartbeat. Router goroutine, from clearIHaveCounters.
func (gs *GossipSubRouter) seacastRetryHeldBack() {
	sc := gs.seacast
	if sc == nil || len(sc.heldBack) == 0 {
		return
	}
	held := sc.heldBack
	sc.heldBack = nil
	for mid, peers := range held {
		if gs.haveSeen(mid) {
			continue
		}
		if gs.expiredID(mid) || gs.seacastStopPulled(mid) {
			// The retry's own checks, made here as well, since the ask below doesn't make them: the
			// group passed its lifetime, or pulls no more.
			continue
		}
		sc.ctr.HeldBackRetried.Add(1)
		if _, sent := gs.retryOfferedPull(mid); !sent && len(peers) > 0 {
			// The offer table, bounded per peer, kept no entry for it (offers can run past the
			// bound): one of the announcers kept here is asked, under the retry's checks.
			gs.seacastAskAnnouncer(mid, peers)
		}
	}
}

// seacastAskAnnouncer asks one of the announcers kept for mid, under the retry's checks (connected,
// not parked for the group, not tied to its root, no ask of the chunk outstanding to it, the request
// gate, the pull discipline's window). The announcers are ranked per chunk (phaseRank, as the pushes
// are), so a group's asks spread over its announcers instead of piling on one. The ask is a retry's,
// not counted against the IHAVE limits. Router goroutine, from seacastRetryHeldBack.
func (gs *GossipSubRouter) seacastAskAnnouncer(mid string, peers map[peer.ID]struct{}) {
	cands := make([]peer.ID, 0, len(peers))
	for p := range peers {
		if _, connected := gs.peers[p]; !connected {
			continue
		}
		if gs.peerParkedFor(p, mid) || gs.seacastTiedTo(p, mid) || gs.seacastAskOutstanding(p, mid) {
			continue
		}
		if !gs.requestAllowed(p, gs.offerTopic[mid], mid) {
			continue
		}
		cands = append(cands, p)
	}
	if len(cands) == 0 {
		return
	}
	sort.Slice(cands, func(i, j int) bool {
		ri, rj := phaseRank(mid, cands[i]), phaseRank(mid, cands[j])
		if ri != rj {
			return ri > rj
		}
		return cands[i] < cands[j]
	})
	pick := cands[0]
	if !gs.iwantAllowed(mid, pick) {
		return
	}
	gs.gossipTracer.AddPromise(pick, []string{mid})
	gs.noteIWantSent(pick, []string{mid})
	gs.sendRPC(pick, rpcWithControl(nil, nil, []*pb.ControlIWant{{MessageIDs: []string{mid}}}, nil, nil, nil), false)
}
