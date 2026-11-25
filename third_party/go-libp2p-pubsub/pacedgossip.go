package pubsub

import (
	"fmt"
	"sync/atomic"
	"time"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
)

// --- Paced gossip ---
//
// Heartbeat gossip is a burst: every heartbeat a node sends each of max(Dlazy, GossipFactor of
// its non-mesh peers) an IHAVE of every id in the gossip window at once; the batched IWANTs come
// back one round trip later and the answers queue behind one another on the uplink. For a message
// of 128 frames diffusing in half a second, a heartbeat that lands mid-diffusion offers the whole
// set to peers that are still missing most of it, all of which ask the same node. Paced, the
// heartbeat still selects the peers and the ids, but the IHAVEs leave one per tick, spread over
// the next heartbeat; ids younger than minAge wait, since the eager path (push and the immediate
// announce) is still carrying them. A job not released within a heartbeat is dropped: the next
// heartbeat's selection supersedes it. The id list is encoded at release (as a HAVE bitmap when
// bitmaps are on), so it offers what the node holds then.

type pacedJob struct {
	p     peer.ID
	topic string
	mids  []string
	at    time.Time
	// topics is a typed job's ids per topic (seacast_gossip.go, §4.8: one job per peer over the
	// path's types); topic and mids are empty then, and the release sends one IHAVE entry per
	// topic in one RPC, each filtered at release as a single topic's is.
	topics []pacedTopicIDs
}

// pacedTopicIDs is one topic's ids within a typed job.
type pacedTopicIDs struct {
	topic string
	mids  []string
}

type pacedGossip struct {
	tick, minAge time.Duration
	queue        []pacedJob
	born         map[string]time.Time
	started      bool

	released, dropped, deferred, emptied atomic.Int64
}

// WithPacedGossip releases heartbeat gossip one IHAVE per tick instead of all at the heartbeat,
// offering only ids at least minAge old.
func WithPacedGossip(tick, minAge time.Duration) Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok {
			return fmt.Errorf("paced gossip requires a gossipsub router")
		}
		if tick <= 0 || minAge < 0 {
			return fmt.Errorf("paced gossip needs a positive tick and a non-negative minimum age")
		}
		gs.paced = &pacedGossip{tick: tick, minAge: minAge, born: make(map[string]time.Time)}
		return nil
	}
}

// gossipTo hands one peer's heartbeat IHAVE to the pacer, or queues it for the heartbeat's
// flush as before.
func (gs *GossipSubRouter) gossipTo(p peer.ID, topic string, mids []string) {
	if gs.paced == nil {
		gs.enqueueGossip(p, &pb.ControlIHave{TopicID: &topic, MessageIDs: gs.haveIHaveIDs(p, mids)})
		return
	}
	gs.paced.queue = append(gs.paced.queue, pacedJob{p: p, topic: topic, mids: append([]string(nil), mids...), at: time.Now()})
	gs.startPacer()
}

// gossipToTyped hands one peer's typed heartbeat job, several topics' ids at once, to the pacer, or
// queues one IHAVE per topic for the heartbeat's flush. Router goroutine.
func (gs *GossipSubRouter) gossipToTyped(p peer.ID, topics []pacedTopicIDs) {
	if gs.paced == nil {
		for _, t := range topics {
			topic := t.topic
			gs.enqueueGossip(p, &pb.ControlIHave{TopicID: &topic, MessageIDs: gs.haveIHaveIDs(p, t.mids)})
		}
		return
	}
	gs.paced.queue = append(gs.paced.queue, pacedJob{p: p, topics: topics, at: time.Now()})
	gs.startPacer()
}

// pacedBorn stamps an id entering the message cache. Router goroutine.
func (gs *GossipSubRouter) pacedBorn(mid string) {
	if gs.paced != nil {
		if _, ok := gs.paced.born[mid]; !ok {
			gs.paced.born[mid] = time.Now()
		}
	}
}

func (gs *GossipSubRouter) startPacer() {
	pg := gs.paced
	if pg.started {
		return
	}
	pg.started = true
	go func() {
		t := time.NewTicker(pg.tick)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				select {
				case gs.p.eval <- gs.releaseGossip:
				case <-gs.p.ctx.Done():
					return
				}
			case <-gs.p.ctx.Done():
				return
			}
		}
	}()
}

// releaseGossip sends the oldest queued IHAVE whose ids are old enough. Router goroutine.
func (gs *GossipSubRouter) releaseGossip() {
	j, mids, ok := gs.nextPaced(time.Now())
	if !ok {
		return
	}
	if len(j.topics) > 0 {
		// A typed job: one IHAVE entry per topic, in one RPC.
		var ihaves []*pb.ControlIHave
		for _, t := range j.topics {
			ids := gs.haveIHaveIDs(j.p, t.mids)
			if len(ids) == 0 {
				continue
			}
			topic := t.topic
			ihaves = append(ihaves, &pb.ControlIHave{TopicID: &topic, MessageIDs: ids})
		}
		if len(ihaves) == 0 {
			gs.paced.emptied.Add(1)
			return
		}
		gs.paced.released.Add(1)
		gs.sendRPC(j.p, rpcWithControl(nil, ihaves, nil, nil, nil, nil), false)
		return
	}
	ids := gs.haveIHaveIDs(j.p, mids)
	if len(ids) == 0 {
		gs.paced.emptied.Add(1)
		return
	}
	gs.paced.released.Add(1)
	topic := j.topic
	gs.sendRPC(j.p, rpcWithControl(nil, []*pb.ControlIHave{{TopicID: &topic, MessageIDs: ids}}, nil, nil, nil, nil), false)
}

// nextPaced takes the next job to release and the ids it may offer now: jobs older than a
// heartbeat or to a peer gone are dropped, ids the peer holds are left out, and a job with
// nothing old enough yet goes to the back and ends the tick.
func (gs *GossipSubRouter) nextPaced(now time.Time) (pacedJob, []string, bool) {
	pg := gs.paced
	for len(pg.queue) > 0 {
		j := pg.queue[0]
		pg.queue = pg.queue[1:]
		if now.Sub(j.at) >= gs.params.HeartbeatInterval {
			pg.dropped.Add(1)
			continue
		}
		if _, ok := gs.peers[j.p]; !ok {
			pg.dropped.Add(1)
			continue
		}
		if len(j.topics) > 0 {
			// A typed job: each topic's ids filtered as a single topic's are; a topic left with
			// nothing is dropped from the job, a job left with nothing old enough waits at the back.
			var kept []pacedTopicIDs
			young := false
			for _, t := range j.topics {
				mids := make([]string, 0, len(t.mids))
				for _, mid := range t.mids {
					if gs.haveSeenBy(j.p, mid) {
						continue
					}
					if b, ok := pg.born[mid]; ok && now.Sub(b) < pg.minAge {
						young = true
						continue
					}
					mids = append(mids, mid)
				}
				if len(mids) > 0 {
					kept = append(kept, pacedTopicIDs{topic: t.topic, mids: mids})
				}
			}
			if len(kept) > 0 {
				j.topics = kept
				return j, nil, true
			}
			if young {
				pg.deferred.Add(1)
				pg.queue = append(pg.queue, j)
				return pacedJob{}, nil, false
			}
			pg.emptied.Add(1)
			continue
		}
		mids := make([]string, 0, len(j.mids))
		young := false
		for _, mid := range j.mids {
			if gs.haveSeenBy(j.p, mid) {
				continue
			}
			if b, ok := pg.born[mid]; ok && now.Sub(b) < pg.minAge {
				young = true
				continue
			}
			mids = append(mids, mid)
		}
		if len(mids) > 0 {
			return j, mids, true
		}
		if young {
			pg.deferred.Add(1)
			pg.queue = append(pg.queue, j)
			return pacedJob{}, nil, false
		}
		pg.emptied.Add(1)
	}
	return pacedJob{}, nil, false
}

// haveSeenBy reports whether p has said it holds mid (IDONTWANT, or a HAVE marking it).
func (gs *GossipSubRouter) haveSeenBy(p peer.ID, mid string) bool {
	_, held := gs.unwanted[p][computeChecksum(mid)]
	return held
}

// sweepPaced forgets birth stamps older than the message cache keeps ids. Heartbeat.
func (gs *GossipSubRouter) sweepPaced() {
	if gs.paced == nil {
		return
	}
	keep := time.Duration(gs.params.HistoryLength+1) * gs.params.HeartbeatInterval
	now := time.Now()
	for mid, b := range gs.paced.born {
		if now.Sub(b) > keep {
			delete(gs.paced.born, mid)
		}
	}
}

// PacedGossipStat is one node's paced-gossip counters.
type PacedGossipStat struct {
	Released, Dropped, Deferred, Emptied int64
}

// PacedGossipStats reads the counters; ok is false when gossip is not paced.
func (p *PubSub) PacedGossipStats() (PacedGossipStat, bool) {
	gs, ok := p.rt.(*GossipSubRouter)
	if !ok || gs.paced == nil {
		return PacedGossipStat{}, false
	}
	pg := gs.paced
	return PacedGossipStat{Released: pg.released.Load(), Dropped: pg.dropped.Load(), Deferred: pg.deferred.Load(), Emptied: pg.emptied.Load()}, true
}
