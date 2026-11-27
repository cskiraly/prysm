package pubsub

// Gossipsub's two IHAVE limits as written (seacast.md §4.3 and §4.8, seacast-lite.md §4.4 v0.7).
// With the path installed, MaxIHaveMessages counts the offer-bearing RPCs from a peer within a
// heartbeat and MaxIHaveLength the asks made of it in reply to its offers, in the core's counts
// (core.IHaveLimits), which a connection's close drops. Each candidate an offer makes, in a drawn
// order, is held back to the next heartbeat while the peer is over either limit, and is otherwise
// asked if the request policies allow it; only the asks made count (seacastAskFrom). The heartbeat
// starts the limits again and then retries what they held back, a chunk's in the order held
// (core.HeartbeatLimits). The retry's and the last resort's asks don't count. The limits delay asks
// and lose no offer.

import (
	"sort"

	core "github.com/cskiraly/seacast-go"
	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
)

// seacastAskFrom is on_offer's asks over p's candidates (seacast-lite.md §4.4). The candidates are
// drawn once. The ids outside the core's pull (seacastAskStock) go first, as they went before the
// path took the limits. Then, in the drawn order, a chunk is held back while p is over an IHAVE limit,
// and otherwise asked if the request policies allow it, and counted. A chunk a policy refuses is
// neither asked nor counted, so the next one takes its place. Router goroutine, from askFrom.
func (gs *GossipSubRouter) seacastAskFrom(p peer.ID, candidates []string, topicOf map[string]string) []string {
	sc := gs.seacast
	shuffleStrings(candidates)
	var chunks, stock []string
	for _, mid := range candidates {
		if _, _, _, ok := gs.pullChunk(mid); ok {
			chunks = append(chunks, mid)
		} else {
			stock = append(stock, mid)
		}
	}
	out := gs.seacastAskStock(p, stock, topicOf)
	for k := range chunks {
		if sc.ihave.Over(p, gs.params.MaxIHaveMessages, gs.params.MaxIHaveLength) {
			gs.seacastHoldBack(chunks[k:k+1], p)
			continue
		}
		if asked := gs.selectIWants(chunks[k:k+1], topicOf, p, 1); len(asked) > 0 {
			out = append(out, asked...)
			sc.ihave.Ask(p)
		}
	}
	return out
}

// seacastAskStock is askFrom's rule for p's drawn candidates outside the core's pull, as a router
// without the path applies it: the allowance left of MaxIHaveLength is decided before the request
// policies, the candidates past it are held back, the policies select among the rest, and the ids
// asked are counted. Router goroutine, from seacastAskFrom.
func (gs *GossipSubRouter) seacastAskStock(p peer.ID, mids []string, topicOf map[string]string) []string {
	if len(mids) == 0 {
		return nil
	}
	sc := gs.seacast
	_, asked := sc.ihave.Counts(p)
	allowance := gs.params.MaxIHaveLength - asked
	if allowance <= 0 {
		gs.seacastHoldBack(mids, p)
		return nil
	}
	if len(mids) > allowance {
		gs.seacastHoldBack(mids[allowance:], p) // over the ask limit: held back, not dropped
		mids = mids[:allowance]
	}
	out := gs.selectIWants(mids, topicOf, p, allowance)
	for range out {
		sc.ihave.Ask(p)
	}
	return out
}

// seacastHoldBack keeps the asks a limit held back: a chunk's in the pull's held_back, and any other
// id's with the peer that offered it (empty for a re-drive without one). Router goroutine.
func (gs *GossipSubRouter) seacastHoldBack(mids []string, p peer.ID) {
	sc := gs.seacast
	if sc == nil || len(mids) == 0 {
		return
	}
	for _, mid := range mids {
		if _, g, i, ok := gs.pullChunk(mid); ok {
			if !gs.pull.held.Holds(g, i) {
				gs.pull.held.Hold(g, i)
				sc.ctr.HeldBack.Add(1)
			}
			continue
		}
		if sc.heldBack == nil {
			sc.heldBack = map[string]map[peer.ID]struct{}{}
		}
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

// seacastForgetHeldBack drops the held-back asks of root's group outside the core's pull, whose
// registration or lifetime ended. After a forgotten registration, the next heartbeat's retry could
// ask for them and register the root again, subject to its own checks; after an expiry, the retry's
// own check skips them. A chunk's held_back stays to the heartbeat, whose retry checks the expiry
// (seacast-lite.md §8.1). Router goroutine.
func (gs *GossipSubRouter) seacastForgetHeldBack(root string) {
	for mid := range gs.seacast.heldBack {
		if r, _, ok := gs.groupIndex(mid); ok && r == root {
			delete(gs.seacast.heldBack, mid)
		}
	}
}

// seacastHeartbeatLimits is on_heartbeat's first lines (seacast-lite.md §6.4): the IHAVE limits start
// again, and the retry runs for what they held back, a chunk's in the order held, then any other id's.
// Router goroutine, from clearIHaveCounters.
func (gs *GossipSubRouter) seacastHeartbeatLimits() {
	sc := gs.seacast
	if sc == nil {
		return
	}
	core.HeartbeatLimits(&sc.ihave, &gs.pull.held, func(g core.GroupID, i uint32) {
		sc.ctr.HeldBackRetried.Add(1)
		gs.pullRetry(g, i) // the retry's own checks: held, stop-pull, expiry (seacast_pull.go)
	})
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
// not counted against the IHAVE limits. Router goroutine, from seacastHeartbeatLimits.
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
