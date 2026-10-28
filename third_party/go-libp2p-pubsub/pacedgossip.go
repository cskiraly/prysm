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
}

type pacedGossip struct {
	tick, minAge time.Duration
	queue        []pacedJob
	born         map[string]time.Time
	started      bool
	// hose: release only while the node's hose window has a free data slot (WithPacedGossipHose).
	hose bool

	released, dropped, deferred, emptied, held atomic.Int64
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

// WithPacedGossipHose releases a paced IHAVE only while the node's hose window has a free data
// slot: HRC's offer throttle on the lazy channel alone, since the asks an IHAVE draws are served
// through that window. A tick that finds the window full releases nothing; the job keeps its
// place and its one-heartbeat lifetime. Without a hose window (or with a passive one) it never
// holds. Requires WithPacedGossip first.
func WithPacedGossipHose() Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok || gs.paced == nil {
			return fmt.Errorf("paced gossip under the hose requires WithPacedGossip first")
		}
		gs.paced.hose = true
		return nil
	}
}

// WithHeartbeatInitialDelay sets when the first heartbeat fires, which fixes the node's heartbeat
// phase: a harness starting every node at one instant otherwise runs every heartbeat in lockstep.
func WithHeartbeatInitialDelay(d time.Duration) Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok {
			return fmt.Errorf("heartbeat delay requires a gossipsub router")
		}
		if d < 0 {
			return fmt.Errorf("heartbeat delay must not be negative")
		}
		gs.params.HeartbeatInitialDelay = d
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
	if gs.pacedHeld() {
		return
	}
	j, mids, ok := gs.nextPaced(time.Now())
	if !ok {
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

// pacedHeld reports whether the hose window withholds this tick's release.
func (gs *GossipSubRouter) pacedHeld() bool {
	pg := gs.paced
	if !pg.hose || gs.p.hose == nil || len(pg.queue) == 0 || gs.p.hose.free() >= 1 {
		return false
	}
	pg.held.Add(1)
	return true
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
	Released, Dropped, Deferred, Emptied, Held int64
}

// PacedGossipStats reads the counters; ok is false when gossip is not paced.
func (p *PubSub) PacedGossipStats() (PacedGossipStat, bool) {
	gs, ok := p.rt.(*GossipSubRouter)
	if !ok || gs.paced == nil {
		return PacedGossipStat{}, false
	}
	pg := gs.paced
	return PacedGossipStat{Released: pg.released.Load(), Dropped: pg.dropped.Load(), Deferred: pg.deferred.Load(), Emptied: pg.emptied.Load(), Held: pg.held.Load()}, true
}
