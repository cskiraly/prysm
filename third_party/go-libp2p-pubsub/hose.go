package pubsub

import (
	"context"
	"fmt"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
)

// --- Hose rate control: a node-wide window over frames in flight ---
//
// A node's uplink is one hose shared by every peer it feeds, and the transport divides it by fair
// share: a node that commits a frame to many peers at once serves each of them slowly and all of
// them late. Hose Rate Control (Birke, Kiraly et al., P2P 2011; Computer Networks 2014) bounds the
// frames a node has in flight -- popped to a peer and not yet acknowledged -- by a window W driven
// by the queueing delay those frames see: on every acknowledgement W ← W − Kp·(D̂ − D0), where
// D̂ = ack time − pop time − the peer's base round trip, and D0 is the target. A fast uplink keeps
// D̂ under D0 and W grows; a slow or saturated one pushes D̂ over it and W shrinks, with no
// configured rate and no class label. Pushes and pull answers take the same window.
//
// The acknowledgement is the peer's HAVE bitmap covering the frame. Gossipsub never tells the
// sender of a message that it arrived, so WithHoseAck makes a receiver send its bitmap to the
// pusher as well as to its mesh. A timeout releases a frame whose acknowledgement never comes.
// Requires WithHaveBitmaps and WithHaveDispatch, whose per-peer state maps frames to indices.

// HoseConfig parameterises the window. Passive records the delay samples and the window it would
// have set without ever holding a pop back.
type HoseConfig struct {
	Target  time.Duration
	Kp      float64 // window frames per millisecond of delay error
	Init    float64
	Min     float64
	Max     float64
	Timeout time.Duration
	Passive bool
	// FrameSlots queues a batched answer one frame per RPC, so it takes a slot per frame rather
	// than one slot for the batch (Q132: at W = 2 a publisher had 1114 frames in flight).
	FrameSlots bool
	// Estimate, when set, sets the window from a capacity estimate instead of stepping it on each
	// acknowledgement (hoseestimate.go); the acknowledgements are still sampled and reported.
	Estimate *HoseEstimate
	// BaseRTT is the peer's base round trip (the transport's minimum RTT); zero or nil falls back
	// on the smallest pop-to-ack time seen from that peer.
	BaseRTT func(peer.ID) time.Duration
	// PeerCap, when positive, bounds the frames in flight to any one peer. The node's window
	// bounds its own uplink; this bounds what it commits to one receiver's downlink, which a
	// fast sender fills long before its own window binds (Q144).
	PeerCap int
}

type hoseWindow struct {
	cfg HoseConfig
	est *hoseEstimator // the capacity estimate, when cfg.Estimate is set
	// tickAcks and tickQ are the frames acknowledged and their queueing delays since the
	// estimate's last tick, under mu.
	tickAcks int
	tickQ    []time.Duration
	fwd      []fwdSample  // stamped acknowledgements since the forward source's last tick, under mu
	written  atomic.Int64 // bytes this node has handed its streams, every peer and lane

	mu       sync.Mutex
	w        float64
	inflight int
	waiters  []chan struct{}
	// waitCls is each waiter's priority class (WithFrameClassGrants), aligned with waiters; a
	// freed slot goes to the lowest class, first come within it. All zero: plain FIFO.
	waitCls []int

	// Observation, under mu.
	wLo, wHi         float64
	samples          []time.Duration // queueing-delay samples, capped
	ansSamples       []time.Duration // the same for pull answers alone
	acks, timeouts   int64
	lateAcks         int64 // acknowledgements after the frame's timeout, sampled without a slot
	waitTime         time.Duration
	acquires, gateOn int64
	peakInflight     int

	// onFree, when set, is told that a slot is free after every waiting pop was served: the
	// offer pump's kick (hoseoffer.go). Called under mu; it must only start work elsewhere.
	onFree func()
}

const hoseSampleCap = 8192

var errHoseEstimate = fmt.Errorf("hose estimate needs MeanRTT, a frame size and a prior")

// WithHoseWindow installs the node-wide hose window. Every peer queue shares it.
func WithHoseWindow(cfg HoseConfig) Option {
	return func(ps *PubSub) error {
		if cfg.Min < 1 || cfg.Max < cfg.Min || cfg.Init < cfg.Min || cfg.Init > cfg.Max {
			return fmt.Errorf("hose window needs 1 <= min <= init <= max")
		}
		if cfg.Timeout <= 0 || cfg.Kp <= 0 {
			return fmt.Errorf("hose window needs a positive timeout and gain")
		}
		ps.hose = &hoseWindow{cfg: cfg, w: cfg.Init, wLo: cfg.Init, wHi: cfg.Init}
		if cfg.Estimate != nil {
			e, err := newHoseEstimator(*cfg.Estimate)
			if err != nil {
				return err
			}
			ps.hose.est = e
		}
		return nil
	}
}

// WithHoseAck makes a receiver send its HAVE bitmap for a received group to the peer that sent
// the frame, not only to its other mesh peers: the acknowledgement the hose window clocks on.
func WithHoseAck() Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok {
			return fmt.Errorf("hose ack requires a gossipsub router")
		}
		gs.hoseAck = true
		return nil
	}
}

// hoseAckDuplicate acknowledges a frame that arrived as a duplicate, which never reaches
// Preprocess: without it the sender's slot waits out the full timeout and yields no sample, so
// every answer overtaken by another copy read as a loss (Q131, the collapse at σ = 0.7).
func (gs *GossipSubRouter) hoseAckDuplicate(mid string, from peer.ID) {
	if !gs.hoseAck || gs.have == nil || from == gs.p.host.ID() || !gs.feature(GossipSubFeatureIdontwant, gs.peers[from]) {
		return
	}
	ids := append(gs.hoseAckIDs([]string{mid}), gs.haveOriginSentAck(from, mid)...)
	if len(ids) == 0 {
		return // our own frame under OriginHaveNone: no HAVE, so no acknowledgement
	}
	ack := rpcWithControl(nil, nil, nil, nil, nil, []*pb.ControlIDontWant{{MessageIDs: ids}})
	gs.sendRPC(from, ack, true)
}

// limit is the window as a frame count.
func (h *hoseWindow) limit() int {
	return int(math.Max(1, math.Floor(h.w)))
}

// acquire blocks until a frame may be popped, an urgent RPC reaches the caller's queue, or ctx
// ends; granted is false when it returned for an urgent arrival.
func (h *hoseWindow) acquire(ctx context.Context, urgent <-chan struct{}) (granted bool, err error) {
	return h.acquireClass(ctx, urgent, 0)
}

// acquireClass is acquire for a writer whose best queued frame has priority class cls.
func (h *hoseWindow) acquireClass(ctx context.Context, urgent <-chan struct{}, cls int) (granted bool, err error) {
	h.mu.Lock()
	if h.cfg.Passive || (h.inflight < h.limit() && len(h.waiters) == 0) {
		h.takeLocked()
		h.mu.Unlock()
		return true, nil
	}
	ch := make(chan struct{})
	h.waiters = append(h.waiters, ch)
	h.waitCls = append(h.waitCls, cls)
	h.gateOn++
	h.mu.Unlock()
	t0 := time.Now()
	select {
	case <-ch:
		h.mu.Lock()
		h.waitTime += time.Since(t0)
		h.mu.Unlock()
		return true, nil
	case <-urgent:
		h.withdraw(ch)
		return false, nil
	case <-ctx.Done():
		h.withdraw(ch)
		return false, ctx.Err()
	}
}

func (h *hoseWindow) takeLocked() {
	h.inflight++
	h.acquires++
	if h.inflight > h.peakInflight {
		h.peakInflight = h.inflight
	}
}

// withdraw takes a waiter out of the line; a slot granted to it meanwhile is given back.
func (h *hoseWindow) withdraw(ch chan struct{}) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for i, w := range h.waiters {
		if w == ch {
			h.waiters = append(h.waiters[:i], h.waiters[i+1:]...)
			h.waitCls = append(h.waitCls[:i], h.waitCls[i+1:]...)
			return
		}
	}
	h.inflight--
	h.wakeLocked()
}

// add counts n more frames in flight past the one a pop took (a multi-frame RPC overshoots).
func (h *hoseWindow) add(n int) {
	if n <= 0 {
		return
	}
	h.mu.Lock()
	h.inflight += n
	if h.inflight > h.peakInflight {
		h.peakInflight = h.inflight
	}
	h.mu.Unlock()
}

// release returns one frame's slot; with a sample, the controller steps on its queueing delay.
func (h *hoseWindow) release(qd time.Duration, sampled, answer bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.inflight--
	if sampled {
		h.acks++
		h.stepLocked(qd, answer)
	} else {
		h.timeouts++
	}
	h.wakeLocked()
}

// lateSample steps the controller on an acknowledgement that came after the frame's timeout had
// already returned its slot. Dropping it, as the first form did, censored exactly the slow
// frames: the controller saw only the fast ones and opened while the queue it built went unseen.
func (h *hoseWindow) lateSample(qd time.Duration, answer bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.lateAcks++
	h.stepLocked(qd, answer)
	h.wakeLocked()
}

func (h *hoseWindow) stepLocked(qd time.Duration, answer bool) {
	if len(h.samples) < hoseSampleCap {
		h.samples = append(h.samples, qd)
	}
	if answer && len(h.ansSamples) < hoseSampleCap {
		h.ansSamples = append(h.ansSamples, qd)
	}
	if h.est != nil {
		// The estimate sets the window; an acknowledgement is a delivered frame and a queueing sample.
		h.tickAcks++
		h.tickQ = append(h.tickQ, qd)
		return
	}
	errMs := float64(qd-h.cfg.Target) / float64(time.Millisecond)
	h.w = math.Min(h.cfg.Max, math.Max(h.cfg.Min, h.w-h.cfg.Kp*errMs))
	h.wLo, h.wHi = math.Min(h.wLo, h.w), math.Max(h.wHi, h.w)
}

// unused returns a slot taken for a pop that sent no frame.
func (h *hoseWindow) unused() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.inflight--
	h.acquires--
	h.wakeLocked()
}

func (h *hoseWindow) wakeLocked() {
	for len(h.waiters) > 0 && (h.cfg.Passive || h.inflight < h.limit()) {
		best := 0
		for i := 1; i < len(h.waitCls); i++ {
			if h.waitCls[i] < h.waitCls[best] {
				best = i
			}
		}
		ch := h.waiters[best]
		h.waiters = append(h.waiters[:best], h.waiters[best+1:]...)
		h.waitCls = append(h.waitCls[:best], h.waitCls[best+1:]...)
		h.takeLocked()
		close(ch)
	}
	if h.onFree != nil && len(h.waiters) == 0 && (h.cfg.Passive || h.inflight < h.limit()) {
		h.onFree()
	}
}

// free is the data slots open now: none while a pop waits; unbounded when passive.
func (h *hoseWindow) free() float64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.cfg.Passive {
		return math.Inf(1)
	}
	if len(h.waiters) > 0 {
		return 0
	}
	return float64(h.limit() - h.inflight)
}

// ResetHoseStats starts a new observation window: samples, counters and the window's range.
// The window itself and the frames in flight are kept, so the controller does not notice.
func (p *PubSub) ResetHoseStats() {
	h := p.hose
	if h == nil {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.samples, h.ansSamples = nil, nil
	h.acks, h.timeouts, h.lateAcks, h.acquires, h.gateOn, h.waitTime = 0, 0, 0, 0, 0, 0
	h.wLo, h.wHi, h.peakInflight = h.w, h.w, h.inflight
}

// HoseStat is one node's hose window, observed.
type HoseStat struct {
	W, WLo, WHi         float64
	Acks, Timeouts      int64
	LateAcks            int64 // acknowledgements after a timeout, sampled (not in Acks)
	Acquires, Gated     int64
	WaitTime            time.Duration
	PeakInflight        int
	QueueP50, QueueP90  time.Duration
	QueueMean, QueueMax time.Duration
	// Answers is how many of the acks were pull answers, AnswerP50/P90 their queueing.
	Answers              int
	AnswerP50, AnswerP90 time.Duration
}

// HoseStats reads the node's hose window; ok is false when none is installed.
func (p *PubSub) HoseStats() (HoseStat, bool) {
	h := p.hose
	if h == nil {
		return HoseStat{}, false
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	s := HoseStat{W: h.w, WLo: h.wLo, WHi: h.wHi, Acks: h.acks, Timeouts: h.timeouts, LateAcks: h.lateAcks,
		Acquires: h.acquires, Gated: h.gateOn, WaitTime: h.waitTime, PeakInflight: h.peakInflight}
	if n := len(h.samples); n > 0 {
		sorted := append([]time.Duration(nil), h.samples...)
		sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
		var sum time.Duration
		for _, d := range sorted {
			sum += d
		}
		s.QueueP50, s.QueueP90, s.QueueMax = sorted[n/2], sorted[n*9/10], sorted[n-1]
		s.QueueMean = sum / time.Duration(n)
	}
	if n := len(h.ansSamples); n > 0 {
		sorted := append([]time.Duration(nil), h.ansSamples...)
		sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
		s.Answers, s.AnswerP50, s.AnswerP90 = n, sorted[n/2], sorted[n*9/10]
	}
	return s, true
}

// --- The per-peer side, under the queue's mutex ---

// sentFrame is a frame in flight: when it was popped, and whether it answered an ask.
type sentFrame struct {
	at      time.Time
	wrote   int64 // the node's written bytes when it was popped, the forward source's byte clock
	bound   bool  // pops were waiting on the window when it went: a rate under it is a floor
	ans     bool
	expired bool // its timeout returned the slot; a late acknowledgement still samples
}

// hoseLateHorizon is how long, in timeouts, an expired frame waits for a late acknowledgement.
const hoseLateHorizon = 10

// hoseSent is one peer's frames in flight: pop time per group root and index.
type hoseSent struct {
	at   map[string]map[uint32]sentFrame
	minD time.Duration // smallest pop-to-ack seen, the fallback base round trip
	// minOWD is the smallest stamped one-way delay seen, the forward source's base (hosefwd.go).
	minOWD  time.Duration
	haveOWD bool
}

// hasDataLocked reports whether the normal lane holds a data RPC.
func (q *rpcQueue) hasDataLocked() bool {
	for _, e := range q.queue.normal {
		if len(e.rpc.Publish) > 0 {
			return true
		}
	}
	return false
}

// hoseAfterPopLocked accounts a normal-lane pop that held a slot: the frames it sends go in
// flight with their pop time and a timeout; a pop that sends none gives the slot back.
func (q *rpcQueue) hoseAfterPopLocked(out *RPC) {
	if !q.hoseHolding {
		return
	}
	q.hoseHolding = false
	var frames []*pb.Message
	if out != nil && q.have != nil {
		for _, m := range out.Publish {
			if _, _, ok := q.have.index(m); ok {
				frames = append(frames, m)
			}
		}
	}
	if len(frames) == 0 {
		q.hose.unused()
		return
	}
	q.hose.add(len(frames) - 1)
	if q.hoseSent == nil {
		q.hoseSent = &hoseSent{at: make(map[string]map[uint32]sentFrame)}
	}
	now := time.Now()
	q.hose.mu.Lock()
	bound := len(q.hose.waiters) > 0
	q.hose.mu.Unlock()
	for _, m := range frames {
		root, idx, _ := q.have.index(m)
		byIdx := q.hoseSent.at[root]
		if byIdx == nil {
			byIdx = make(map[uint32]sentFrame)
			q.hoseSent.at[root] = byIdx
		}
		if e, dup := byIdx[idx]; dup && !e.expired {
			// A second copy of one frame to one peer: one acknowledgement will cover both.
			q.hose.unused()
			continue
		}
		byIdx[idx] = sentFrame{at: now, ans: out.solicited, wrote: q.hose.written.Load(), bound: bound}
		q.hoseToPeer++
		time.AfterFunc(q.hose.cfg.Timeout, func() { q.hoseExpire(root, idx, now) })
	}
}

// hoseExpire releases a frame whose acknowledgement never came.
func (q *rpcQueue) hoseExpire(root string, idx uint32, at time.Time) {
	q.queueMu.Lock()
	defer q.queueMu.Unlock()
	if q.hoseSent == nil {
		return
	}
	e, ok := q.hoseSent.at[root][idx]
	if !ok || !e.at.Equal(at) {
		return
	}
	if e.expired {
		delete(q.hoseSent.at[root], idx) // no acknowledgement within the late horizon
		return
	}
	e.expired = true
	q.hoseSent.at[root][idx] = e
	q.hosePeerReleasedLocked()
	q.hose.release(0, false, false)
	time.AfterFunc(time.Duration(hoseLateHorizon-1)*q.hose.cfg.Timeout, func() { q.hoseExpire(root, idx, at) })
}

// hoseAckedLocked clocks the window on the indices the peer has just announced holding; stamp is
// the peer's receive time when the acknowledgement carried one, for the indices in stamped only
// (the bitmap also covers frames the peer got earlier, from others).
func (q *rpcQueue) hoseAckedLocked(root string, idxs []uint32, stamp time.Time, stamped map[uint32]bool) {
	if q.hose == nil || q.hoseSent == nil {
		return
	}
	byIdx := q.hoseSent.at[root]
	if len(byIdx) == 0 {
		return
	}
	now := time.Now()
	for _, idx := range idxs {
		e, ok := byIdx[idx]
		if !ok {
			continue
		}
		delete(byIdx, idx)
		if !e.expired {
			q.hosePeerReleasedLocked()
		}
		if !stamp.IsZero() && stamped[idx] {
			q.hose.noteForward(&q.hoseSent.minOWD, &q.hoseSent.haveOWD, e.at, stamp, e.wrote, e.bound)
		}
		d := now.Sub(e.at)
		if q.hoseSent.minD == 0 || d < q.hoseSent.minD {
			q.hoseSent.minD = d
		}
		base := q.hoseSent.minD
		if q.hose.cfg.BaseRTT != nil {
			if b := q.hose.cfg.BaseRTT(q.peer); b > 0 {
				base = b
			}
		}
		qd := d - base
		if qd < 0 {
			qd = 0
		}
		if e.expired {
			q.hose.lateSample(qd, e.ans)
			continue
		}
		q.hose.release(qd, true, e.ans)
	}
}

// hoseCloseLocked returns every slot the queue's frames still hold.
func (q *rpcQueue) hoseCloseLocked() {
	if q.hoseHolding {
		q.hoseHolding = false
		q.hose.unused()
	}
	if q.hoseSent == nil {
		return
	}
	for root, byIdx := range q.hoseSent.at {
		for _, e := range byIdx {
			if !e.expired {
				q.hose.release(0, false, false)
			}
		}
		delete(q.hoseSent.at, root)
	}
	q.hoseToPeer = 0
}

// hosePeerReleasedLocked counts one of this peer's frames out of flight and wakes a writer waiting
// on the peer cap. Caller holds the queue lock.
func (q *rpcQueue) hosePeerReleasedLocked() {
	if q.hoseToPeer > 0 {
		q.hoseToPeer--
	}
	select {
	case q.peerFree <- struct{}{}:
	default:
	}
}
