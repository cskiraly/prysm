package pubsub

// Typed gossip (seacast.md §4.8 on_heartbeat and send_gossip, §4.4's announce under
// ANNOUNCE_EXTRA). The draft selects the gossip peers once per
// heartbeat and announces to each the eligible groups of the types whose ANNOUNCE_EXTRA is SAMPLE,
// the payload's (§6.1) and, in this fork, every type's. The stock heartbeat gossips per topic, one paced job per selected peer and topic,
// which over the path's many topics (128 columns) would exhaust the pace budget and expire the
// later jobs; here the path's topics leave the per-topic loops and get one job per selected
// peer carrying every SAMPLE topic's ids the peer is joined to, released as several IHAVE
// entries in one RPC and filtered per topic at release (pacedgossip.go). The peers are the joined
// peers of the path's SAMPLE topics outside that topic's mesh (the mesh was pushed), direct peers
// and peers below the gossip threshold left out, max(D_lazy, GOSSIP_FACTOR × eligible) of them.

import (
	core "github.com/cskiraly/seacast-go"
	"github.com/libp2p/go-libp2p/core/peer"
)

// seacastEmitGossip is the heartbeat's gossip over the path's topics, once: the SAMPLE types'
// eligible ids per topic, the peers selected once over those topics, and one typed job per
// selected peer. Router goroutine, from the heartbeat after the per-topic loops.
func (gs *GossipSubRouter) seacastEmitGossip(topics []string) {
	sc := gs.seacast
	if sc == nil || len(topics) == 0 {
		return
	}
	perTopic := map[string][]string{}
	for _, topic := range topics {
		if sc.types[topic] == nil {
			continue
		}
		if gs.seacastDutyNone(topic) {
			continue // a node of duty NONE (a sampler) offers nothing (§1.2); its fanout for the topic is inert
		}
		if mids := gs.mcache.GetGossipIDs(topic); len(mids) > 0 {
			perTopic[topic] = mids
		}
	}
	if len(perTopic) == 0 {
		return
	}
	// The peers, selected once over the SAMPLE topics: joined to the topic, outside its mesh (the
	// mesh was pushed), not direct, mesh-capable and above the gossip threshold, as emitGossip
	// selects them per topic.
	cands := map[peer.ID][]string{}
	for topic := range perTopic {
		// The mesh's peers were pushed, the fanout's too (a topic the node publishes on without
		// joining it): both are the stock loops' exclude sets.
		mesh, fanout := gs.mesh[topic], gs.fanout[topic]
		for p := range gs.p.topics[topic] {
			if _, inMesh := mesh[p]; inMesh {
				continue
			}
			if _, inFanout := fanout[p]; inFanout {
				continue
			}
			if _, direct := gs.direct[p]; direct {
				continue
			}
			if !gs.feature(GossipSubFeatureMesh, gs.peers[p]) || gs.score.Score(p) < gs.gossipThreshold {
				continue
			}
			cands[p] = append(cands[p], topic)
		}
	}
	if len(cands) == 0 {
		return
	}
	peers := make([]peer.ID, 0, len(cands))
	for p := range cands {
		peers = append(peers, p)
	}
	if target := core.GossipCount(len(peers), gs.params.Dlazy, gs.params.GossipFactor); target < len(peers) {
		shufflePeers(peers)
		peers = peers[:target]
	}
	for _, p := range peers {
		var job []pacedTopicIDs
		n := 0
		for _, topic := range cands[p] {
			mids := gs.seacastGossipCap(perTopic[topic])
			job = append(job, pacedTopicIDs{topic: topic, mids: mids})
			n += len(mids)
		}
		gs.gossipToTyped(p, job)
		sc.ctr.GossipJobs.Add(1)
		sc.ctr.GossipIdsOffered.Add(int64(n))
	}
}

// seacastGossipCap is a typed job's ids of one topic, cut to MaxIHaveLength as the stock gossip cuts
// a topic's: the ids drawn, and a copy returned, since the job keeps it past the heartbeat. With HAVE
// bitmaps announced, a group's ids go out as one bitmap, whose size the group's count bounds and which
// the spec sends whole (seacast-lite.md §4.4), so only the ids that go out as ids count and are cut. An
// index of MAX_PLAIN_CHUNKS or more is no chunk of a group (seacast-lite.md §1): no bitmap carries it,
// and a receiver's offer guard drops it as an id, so it is not offered. Router goroutine.
func (gs *GossipSubRouter) seacastGossipCap(mids []string) []string {
	var grouped, plain []string
	if h := gs.have; h != nil && h.ihaveOn {
		for _, mid := range mids {
			if root, index, ok := h.index(mid); ok && len(root) == haveRootLen {
				if index >= core.MaxPlainChunks {
					continue
				}
				grouped = append(grouped, mid)
			} else {
				plain = append(plain, mid)
			}
		}
	} else {
		plain = append([]string(nil), mids...)
	}
	if len(plain) > gs.params.MaxIHaveLength {
		shuffleStrings(plain)
		plain = plain[:gs.params.MaxIHaveLength]
	}
	return append(grouped, plain...)
}
