package pubsub

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

var (
	ErrQueueCancelled    = errors.New("rpc queue operation cancelled")
	ErrQueueClosed       = errors.New("rpc queue closed")
	ErrQueueFull         = errors.New("rpc queue full")
	ErrQueuePushOnClosed = errors.New("push on closed rpc queue")
)

// rankedRPC is a normal-lane entry with its drain rank. Rank 0 is the neutral default, so a
// queue where nothing sets ranks drains in exact FIFO order (first minimal wins the scan).
type rankedRPC struct {
	rpc  *RPC
	rank int
}

type priorityQueue struct {
	normal   []rankedRPC
	priority []*RPC
}

func (q *priorityQueue) Len() int {
	return len(q.normal) + len(q.priority)
}

func (q *priorityQueue) NormalPush(rpc *RPC) {
	q.RankedPush(rpc, 0)
}

func (q *priorityQueue) RankedPush(rpc *RPC, rank int) {
	q.normal = append(q.normal, rankedRPC{rpc: rpc, rank: rank})
}

func (q *priorityQueue) PriorityPush(rpc *RPC) {
	q.priority = append(q.priority, rpc)
}

// PopAt removes and returns the normal-lane entry at i. Under the queue's mutex.
func (q *priorityQueue) PopAt(i int) *RPC {
	rpc := q.normal[i].rpc
	q.normal[i] = rankedRPC{}
	q.normal = append(q.normal[:i], q.normal[i+1:]...)
	return rpc
}

func (q *priorityQueue) Pop() *RPC {
	var rpc *RPC

	if len(q.priority) > 0 {
		rpc = q.priority[0]
		q.priority[0] = nil
		q.priority = q.priority[1:]
	} else if len(q.normal) > 0 {
		// Lowest rank first; FIFO within a rank (the scan keeps the first minimum). With
		// every rank 0 this is plain FIFO, so an unranked queue is byte-identical to the
		// old behaviour, and reordering can only ever happen where messages were already
		// waiting — the uncontended path is untouched by construction. Linear scan: the
		// queue is bounded (default 600) and normally holds a handful.
		best := 0
		for i := 1; i < len(q.normal); i++ {
			if q.normal[i].rank < q.normal[best].rank {
				best = i
			}
		}
		rpc = q.normal[best].rpc
		q.normal[best] = rankedRPC{}
		q.normal = append(q.normal[:best], q.normal[best+1:]...)
	}

	return rpc
}

type rpcQueue struct {
	dataAvailable  sync.Cond
	spaceAvailable sync.Cond
	// Mutex used to access queue
	queueMu sync.Mutex
	queue   priorityQueue

	closed  bool
	maxSize int

	// Passive outbound observation (measurement only): bytes pushed and not yet popped by the
	// writer and their peak, under queueMu; the writer's time blocked in the stream write and
	// its write count, written by the writer goroutine alone.
	queuedBytes  int64
	peakQueued   int64
	writeBlocked atomic.Int64 // nanoseconds
	writes       atomic.Int64
	maxWrite     atomic.Int64 // nanoseconds

	// have, when set, is the HAVE state the writer reads at dispatch (havebitmap.go); cancelled
	// counts messages dropped there, attached the RPCs that carried a pending HAVE.
	have      *queueHave
	cancelled atomic.Int64
	attached  atomic.Int64
	reordered atomic.Int64
	projected atomic.Int64

	// credits, when set, is the node-wide slot pool a normal-lane pop must hold a slot from
	// (WithDispatchCredits); holding says this queue's writer has one. urgentCh wakes a writer
	// waiting for a slot when an urgent RPC arrives, so the urgent lane never waits for one.
	credits        *dispatchCredits
	holding        bool
	urgentCh       chan struct{}
	creditWait     atomic.Int64 // nanoseconds spent waiting for a slot
	creditAcquires atomic.Int64

	// hose, when set, is the node-wide window over frames in flight (WithHoseWindow); hoseHolding
	// says this writer holds a slot for its next data pop, hoseSent is its frames in flight, and
	// peer is who the queue feeds, for the base round trip.
	hose        *hoseWindow
	hoseHolding bool
	hoseSent    *hoseSent
	peer        peer.ID
	// hoseToPeer is this peer's frames in flight, and peerFree wakes a writer waiting on
	// HoseConfig.PeerCap when one of them is acknowledged or expires.
	hoseToPeer int
	peerFree   chan struct{}
}

func newRpcQueue(maxSize int) *rpcQueue {
	q := &rpcQueue{maxSize: maxSize, urgentCh: make(chan struct{}, 1), peerFree: make(chan struct{}, 1)}
	q.dataAvailable.L = &q.queueMu
	q.spaceAvailable.L = &q.queueMu
	return q
}

// --- Dispatch credits: a node feeds at most A peers at a time ---
//
// A node that holds a frame commits it to every mesh peer at once, and the transport divides
// its uplink among those flows by fair share: every peer fed at a small fraction, every peer
// completing late and together, and most frames a sender turns to already held by the receiver
// from someone else feeding it at the same fraction (Q116, Q117). The credits make the division
// a decision: a writer takes one of A node-wide slots before it pops from the normal lane, and
// on the write's return keeps it while its queue still has data (sticky) or hands it to the
// longest-waiting writer (rotate). The urgent lane -- the IDONTWANT bitmap -- never takes a slot
// and never waits for one. A node's inactive connections carry only control, so its have-signal
// to the peers it is not feeding leaves at once. With A at the mesh degree nothing changes.

type dispatchCredits struct {
	mu      sync.Mutex
	free    int
	sticky  bool
	waiters []chan struct{} // FIFO; a returned slot is handed straight to the first
}

// acquire blocks until a slot is granted, an urgent RPC reaches the caller's queue, or ctx
// ends. granted is false when it returned for an urgent arrival; the caller re-reads its lanes.
func (c *dispatchCredits) acquire(ctx context.Context, urgent <-chan struct{}) (granted bool, err error) {
	c.mu.Lock()
	if c.free > 0 && len(c.waiters) == 0 {
		c.free--
		c.mu.Unlock()
		return true, nil
	}
	ch := make(chan struct{})
	c.waiters = append(c.waiters, ch)
	c.mu.Unlock()
	select {
	case <-ch:
		return true, nil
	case <-urgent:
		c.withdraw(ch)
		return false, nil
	case <-ctx.Done():
		c.withdraw(ch)
		return false, ctx.Err()
	}
}

// withdraw takes a waiter out of the line; if the slot was granted to it meanwhile, the slot is
// passed on so that it is never lost.
func (c *dispatchCredits) withdraw(ch chan struct{}) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i, w := range c.waiters {
		if w == ch {
			c.waiters = append(c.waiters[:i], c.waiters[i+1:]...)
			return
		}
	}
	c.releaseLocked()
}

func (c *dispatchCredits) release() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.releaseLocked()
}

func (c *dispatchCredits) releaseLocked() {
	if len(c.waiters) > 0 {
		ch := c.waiters[0]
		c.waiters = c.waiters[1:]
		close(ch)
		return
	}
	c.free++
}

// WithDispatchCredits bounds the peers a node feeds at once to a slots; sticky keeps a slot
// while the peer's queue still has data, otherwise it rotates to the longest-waiting peer.
func WithDispatchCredits(a int, sticky bool) Option {
	return func(ps *PubSub) error {
		if a < 1 {
			return fmt.Errorf("dispatch credits need at least one slot")
		}
		ps.dispatchCredits = &dispatchCredits{free: a, sticky: sticky}
		return nil
	}
}

// releaseCreditLocked returns a held slot. Under queueMu; credits.mu is taken inside, never
// the other way round.
func (q *rpcQueue) releaseCreditLocked() {
	if q.holding {
		q.holding = false
		q.credits.release()
	}
}

// afterWrite is the writer's completion event: under rotate the slot goes back; under sticky it
// is kept while the normal lane still has data, so one peer is served until it drains.
func (q *rpcQueue) afterWrite() {
	if q.credits == nil {
		return
	}
	q.queueMu.Lock()
	defer q.queueMu.Unlock()
	if !(q.holding && q.credits.sticky && len(q.queue.normal) > 0) {
		q.releaseCreditLocked()
	}
}

// dispatch applies the dispatch-time decisions to a popped RPC: drop what the peer has since
// announced, attach what it has not been told, record the pop. nil when nothing is left to send.
func (q *rpcQueue) dispatch(rpc *RPC) *RPC {
	if q.have == nil {
		return rpc
	}
	kept, cancelled, projected := q.have.filter(rpc)
	if cancelled > 0 {
		q.cancelled.Add(int64(cancelled))
	}
	if projected > 0 {
		q.projected.Add(int64(projected))
	}
	if kept == nil {
		return nil
	}
	if out := q.have.attach(kept); out != nil {
		q.attached.Add(1)
		kept = out
	}
	if out := q.have.attachSent(kept); out != nil {
		kept = out
	}
	for _, m := range kept.Publish {
		q.have.notePopped(m)
		if q.have.onDispatch != nil {
			q.have.onDispatch(m)
		}
		if q.have.onOriginPop != nil {
			q.have.onOriginPop(m)
		}
	}
	return kept
}

func (q *rpcQueue) Push(rpc *RPC, block bool) error {
	return q.push(rpc, false, block, 0)
}

// RankedPush enqueues on the normal lane with a drain rank; lower ranks drain first.
func (q *rpcQueue) RankedPush(rpc *RPC, rank int, block bool) error {
	return q.push(rpc, false, block, rank)
}

func (q *rpcQueue) UrgentPush(rpc *RPC, block bool) error {
	return q.push(rpc, true, block, 0)
}

func (q *rpcQueue) push(rpc *RPC, urgent bool, block bool, rank int) error {
	q.queueMu.Lock()
	defer q.queueMu.Unlock()

	if q.closed {
		panic(ErrQueuePushOnClosed)
	}

	for q.queue.Len() == q.maxSize {
		if block {
			q.spaceAvailable.Wait()
			// It can receive a signal because the queue is closed.
			if q.closed {
				panic(ErrQueuePushOnClosed)
			}
		} else {
			return ErrQueueFull
		}
	}
	if urgent {
		q.queue.PriorityPush(rpc)
		// A writer waiting for a slot must come back for this.
		select {
		case q.urgentCh <- struct{}{}:
		default:
		}
	} else {
		q.queue.RankedPush(rpc, rank)
	}
	q.queuedBytes += int64(rpc.size)
	if q.queuedBytes > q.peakQueued {
		q.peakQueued = q.queuedBytes
	}

	q.dataAvailable.Signal()
	return nil
}

// Note that, when the queue is empty and there are two blocked Pop calls, it
// doesn't mean that the first Pop will get the item from the next Push. The
// second Pop will probably get it instead.
func (q *rpcQueue) Pop(ctx context.Context) (*RPC, error) {
	q.queueMu.Lock()
	defer q.queueMu.Unlock()

	if q.closed {
		return nil, ErrQueueClosed
	}

	unregisterAfterFunc := context.AfterFunc(ctx, func() {
		q.dataAvailable.Broadcast()
	})
	// Note: unregisterAfterFunc is called on every path out of this function, so a Pop that
	// returned can no longer be woken by the context.
	defer unregisterAfterFunc()

	for {
		for q.queue.Len() == 0 {
			// Nothing to send: a held slot goes back so another peer's writer can use it.
			q.releaseCreditLocked()
			select {
			case <-ctx.Done():
				return nil, ErrQueueCancelled
			default:
			}
			q.dataAvailable.Wait()
			if q.closed {
				return nil, ErrQueueClosed
			}
		}
		if len(q.queue.priority) > 0 {
			// The urgent lane neither takes nor waits for a slot.
			rpc := q.queue.Pop()
			q.queuedBytes -= int64(rpc.size)
			q.spaceAvailable.Signal()
			if out := q.dispatch(rpc); out != nil {
				return out, nil
			}
			continue
		}
		if q.credits != nil && !q.holding {
			// The normal lane needs a slot. Wait for one outside the queue's lock; an urgent
			// arrival or the context brings the writer back to re-read its lanes.
			select {
			case <-q.urgentCh: // a stale wake from an urgent RPC already popped
			default:
			}
			q.queueMu.Unlock()
			t0 := time.Now()
			granted, err := q.credits.acquire(ctx, q.urgentCh)
			q.queueMu.Lock()
			if granted {
				q.holding = true
				q.creditAcquires.Add(1)
				q.creditWait.Add(int64(time.Since(t0)))
			}
			if err != nil {
				q.releaseCreditLocked()
				return nil, ErrQueueCancelled
			}
			if q.closed {
				q.releaseCreditLocked()
				return nil, ErrQueueClosed
			}
			continue
		}
		if q.hose != nil && q.have != nil && !q.hoseHolding && q.hose.cfg.PeerCap > 0 &&
			q.hoseToPeer >= q.hose.cfg.PeerCap && q.hasDataLocked() {
			// This peer already has its cap of frames in flight: wait for one to be acknowledged
			// or expire, so the backlog for a peer whose link is full stays here, where cancel and
			// the decode HAVE still reach it (hose.go, PeerCap).
			select {
			case <-q.urgentCh:
			default:
			}
			q.queueMu.Unlock()
			select {
			case <-q.peerFree:
			case <-q.urgentCh:
			case <-ctx.Done():
			}
			q.queueMu.Lock()
			if ctx.Err() != nil {
				return nil, ErrQueueCancelled
			}
			if q.closed {
				return nil, ErrQueueClosed
			}
			continue
		}
		if q.hose != nil && q.have != nil && !q.hoseHolding && q.hasDataLocked() {
			// A data pop needs a slot in the node's hose window; same wait as for a credit.
			select {
			case <-q.urgentCh:
			default:
			}
			q.queueMu.Unlock()
			granted, err := q.hose.acquireClass(ctx, q.urgentCh, q.grantClassLocked())
			q.queueMu.Lock()
			if granted {
				q.hoseHolding = true
			}
			if err != nil {
				q.hoseCloseLocked()
				return nil, ErrQueueCancelled
			}
			if q.closed {
				q.hoseCloseLocked()
				return nil, ErrQueueClosed
			}
			continue
		}
		var rpc *RPC
		if q.have != nil {
			// Dispatch-time selection: the rarest queued frame ahead of the next control RPC.
			if i := q.have.rarestIndex(q.queue.normal); i > 0 {
				rpc = q.queue.PopAt(i)
				q.reordered.Add(1)
			}
		}
		if rpc == nil {
			rpc = q.queue.Pop()
		}
		q.queuedBytes -= int64(rpc.size)
		q.spaceAvailable.Signal()
		// Dispatch-time decisions, under the lock the router writes the peer's state under:
		// drop what the peer has since announced, attach what it has not been told.
		out := q.dispatch(rpc)
		if q.hose != nil {
			q.hoseAfterPopLocked(out)
		}
		if out != nil {
			return out, nil
		}
	}
}

func (q *rpcQueue) Close() {
	q.queueMu.Lock()
	defer q.queueMu.Unlock()

	q.closed = true
	q.releaseCreditLocked()
	if q.hose != nil {
		q.hoseCloseLocked()
	}
	q.dataAvailable.Broadcast()
	q.spaceAvailable.Broadcast()
}

// noteWrite accounts one stream write by the writer goroutine: d is the time it blocked.
func (q *rpcQueue) noteWrite(d time.Duration) {
	q.writes.Add(1)
	if d <= 0 {
		return
	}
	q.writeBlocked.Add(int64(d))
	if int64(d) > q.maxWrite.Load() {
		q.maxWrite.Store(int64(d))
	}
}

// outboundStat reads the queue's observation counters.
func (q *rpcQueue) outboundStat() OutboundStat {
	q.queueMu.Lock()
	queued, peak := q.queuedBytes, q.peakQueued
	q.queueMu.Unlock()
	return OutboundStat{QueuedBytes: queued, PeakQueuedBytes: peak, WriteBlocked: time.Duration(q.writeBlocked.Load()),
		Writes: q.writes.Load(), MaxWrite: time.Duration(q.maxWrite.Load()),
		Cancelled: q.cancelled.Load(), Attached: q.attached.Load(), Reordered: q.reordered.Load(), Projected: q.projected.Load(),
		CreditWait: time.Duration(q.creditWait.Load()), CreditAcquires: q.creditAcquires.Load()}
}

// QueuedBytes is the bytes pushed and not yet popped by the writer: the load an announce to
// this peer carries (havebitmap.go).
func (q *rpcQueue) QueuedBytes() int64 {
	q.queueMu.Lock()
	defer q.queueMu.Unlock()
	return q.queuedBytes
}

// Occupancy returns the current queue length and its capacity, for the adaptive hedge's
// local congestion signal (measurement-plan section 15, Job 2).
func (q *rpcQueue) Occupancy() (length, capacity int) {
	q.queueMu.Lock()
	defer q.queueMu.Unlock()
	return q.queue.Len(), q.maxSize
}
