package pubsub

import (
	"fmt"
	"math"
	"sync/atomic"
	"time"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
)

// --- Offers under the hose window (HRC's offer/select, Birke, Kiraly et al.) ---
//
// Phase forwarding announces every frame a node receives to each mesh peer it did not push to,
// at once, so every frame invites pulls from several directions whatever the node's uplink can
// serve. In Hose Rate Control the offer is the throttle: a peer offers only while it has a free
// trading thread. Here a phase announce is an offer that waits in a node-wide FIFO and leaves only
// while the hose window has room; an offer stays out until the peer asks for the frame (its
// answer then takes a slot at the writer), the peer says it holds the frame, or a timeout. Data
// pops waiting for a slot are served first; offers take what is left.
//
// Not every offer turns into a frame: each is made to several peers and most are covered by a
// delivery from elsewhere. Charging an offer a whole slot filled the window with offers and
// throttled the pulls that carry most of the payload (the first cells). So, as HRC's clipping on
// the positive-select ratio does, the offers out may reach the free data slots divided by pa, the
// share of closed offers that were answered.

type offerItem struct {
	p     peer.ID
	topic string
	mid   string
}

type hoseOffers struct {
	timeout time.Duration
	queue   []offerItem
	out     map[peer.ID]map[string]time.Time
	nOut    int
	kicking atomic.Bool

	queued, sent, answered, covered, expired, stale atomic.Int64
}

// WithHoseOffers routes phase announces through the hose window as offers. Requires
// WithHoseWindow and phase forwarding.
func WithHoseOffers(timeout time.Duration) Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok {
			return fmt.Errorf("hose offers require a gossipsub router")
		}
		if ps.hose == nil || gs.phaseForward == nil {
			return fmt.Errorf("hose offers require WithHoseWindow and WithPhaseForwarding first")
		}
		if timeout <= 0 {
			return fmt.Errorf("hose offers need a positive timeout")
		}
		gs.hoseOffers = &hoseOffers{timeout: timeout, out: make(map[peer.ID]map[string]time.Time)}
		ps.hose.onFree = gs.kickOffers
		return nil
	}
}

// offerLater queues an announce of mid to p. Router goroutine.
func (gs *GossipSubRouter) offerLater(p peer.ID, topic, mid string) {
	o := gs.hoseOffers
	o.queue = append(o.queue, offerItem{p: p, topic: topic, mid: mid})
	o.queued.Add(1)
	gs.kickOffers()
}

// kickOffers schedules one pump on the router goroutine; callable from any goroutine, including
// under the hose window's lock, since it only starts a goroutine.
func (gs *GossipSubRouter) kickOffers() {
	o := gs.hoseOffers
	if o == nil || !o.kicking.CompareAndSwap(false, true) {
		return
	}
	go func() {
		select {
		case gs.p.eval <- gs.pumpOffers:
		case <-gs.p.ctx.Done():
		}
	}()
}

// pumpOffers sends queued offers while the window has room. Router goroutine.
func (gs *GossipSubRouter) pumpOffers() {
	o := gs.hoseOffers
	o.kicking.Store(false)
	h := gs.p.hose
	for len(o.queue) > 0 {
		it := o.queue[0]
		if gs.offerStale(it) {
			o.queue = o.queue[1:]
			o.stale.Add(1)
			continue
		}
		if float64(o.nOut) >= h.free()/o.pa() {
			return
		}
		o.queue = o.queue[1:]
		ids := gs.haveIHaveIDs(it.p, []string{it.mid})
		if len(ids) == 0 {
			o.stale.Add(1)
			continue
		}
		ihave := rpcWithControl(nil, []*pb.ControlIHave{{TopicID: &it.topic, MessageIDs: ids}}, nil, nil, nil, nil)
		ihave.urgent = gs.haveAnnounceUrgent()
		byMid := o.out[it.p]
		if byMid == nil {
			byMid = make(map[string]time.Time)
			o.out[it.p] = byMid
		}
		at := time.Now()
		byMid[it.mid] = at
		o.nOut++
		o.sent.Add(1)
		gs.sendRPC(it.p, ihave, ihave.urgent)
		gs.haveOriginOffered(it.mid)
		p, mid := it.p, it.mid
		time.AfterFunc(o.timeout, func() {
			select {
			case gs.p.eval <- func() { gs.offerDone(p, mid, at, &o.expired) }:
			case <-gs.p.ctx.Done():
			}
		})
	}
}

// offerStale reports whether an offer no longer needs to go: the peer holds the frame, or has
// gone.
func (gs *GossipSubRouter) offerStale(it offerItem) bool {
	if _, ok := gs.peers[it.p]; !ok {
		return true
	}
	if _, ok := gs.unwanted[it.p][computeChecksum(it.mid)]; ok {
		return true
	}
	return gs.announcedHolder(it.mid, it.p)
}

// offerDone closes an outstanding offer and kicks the pump; at, when set, must match the offer
// (a timeout for an offer already closed and re-sent does nothing). Router goroutine.
func (gs *GossipSubRouter) offerDone(p peer.ID, mid string, at time.Time, count *atomic.Int64) {
	o := gs.hoseOffers
	if o == nil {
		return
	}
	t, ok := o.out[p][mid]
	if !ok || (!at.IsZero() && !t.Equal(at)) {
		return
	}
	delete(o.out[p], mid)
	o.nOut--
	count.Add(1)
	gs.kickOffers()
}

// pa is the share of closed offers the peer answered, floored so offers never stop entirely;
// 0.25 until enough have closed to say.
func (o *hoseOffers) pa() float64 {
	answered := float64(o.answered.Load())
	closed := answered + float64(o.covered.Load()+o.expired.Load())
	if closed < 32 {
		return 0.25
	}
	return math.Max(0.05, answered/closed)
}

// offerAnswered and offerCovered close an offer on the peer's ask and on its HAVE.
func (gs *GossipSubRouter) offerAnswered(p peer.ID, mid string) {
	if gs.hoseOffers != nil {
		gs.offerDone(p, mid, time.Time{}, &gs.hoseOffers.answered)
	}
}

func (gs *GossipSubRouter) offerCovered(p peer.ID, mid string) {
	if gs.hoseOffers != nil {
		gs.offerDone(p, mid, time.Time{}, &gs.hoseOffers.covered)
	}
}

// OfferStat is one node's offer counters under the hose window.
type OfferStat struct {
	Queued, Sent, Answered, Covered, Expired, Stale int64
}

// HoseOfferStats reads the node's offer counters; ok is false when offers are not gated.
func (p *PubSub) HoseOfferStats() (OfferStat, bool) {
	gs, ok := p.rt.(*GossipSubRouter)
	if !ok || gs.hoseOffers == nil {
		return OfferStat{}, false
	}
	o := gs.hoseOffers
	return OfferStat{Queued: o.queued.Load(), Sent: o.sent.Load(), Answered: o.answered.Load(),
		Covered: o.covered.Load(), Expired: o.expired.Load(), Stale: o.stale.Load()}, true
}
