package pubsub

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
	"google.golang.org/protobuf/proto"
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

	// The writer's side of the v0.5 rules (specv05.go), installed as the queue is made. onAsks
	// hears the ask ids of each RPC the writer pops, and when (E: the promise starts at the write);
	// askLoss, when set, is the IWANT class loss drawn there under E. onSkipped hears the pushes
	// projection skipped (A′). owed is the offers owed to the peer, per group, which ride on the next
	// RPC out or leave alone; owedAnswers the answers a full queue refused, one per chunk, requeued
	// as soon as there is room. owedFilter, when set, is the class loss the owed offers must pass;
	// owedSent counts the indices they carried. Under queueMu.
	onAsks      func(mids []string, at time.Time)
	askLoss     func(ids []string) (kept, lost []string)
	onSkipped   func(msgs []*pb.Message)
	owed        map[string]*owedGroup
	owedAnswers []*pb.Message
	owedFilter  func(*RPC) *RPC
	owedSent    *atomic.Int64
}

// owedGroup is one group's offers owed to the peer.
type owedGroup struct {
	topic string
	bits  haveBits
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
		if q.onSkipped != nil {
			// A′: the skipped pushes go back to the router, which no longer counts them and owes
			// a mesh peer their offer. Read before notePopped below moves the projection.
			var skipped []*pb.Message
			for _, m := range rpc.Publish {
				if !(q.have.cancelOn && q.have.held(m)) && q.have.projectedComplete(m) {
					skipped = append(skipped, m)
				}
			}
			q.onSkipped(skipped)
		}
	}
	if kept == nil {
		return nil
	}
	if q.have.ownHeld != nil && kept.Control != nil && len(kept.Control.Iwant) > 0 {
		// Asks for frames we hold by now leave no more (the spec's §7.2).
		out, dropped := dropHeldAsks(kept, q.have.ownHeld)
		if dropped > 0 {
			StaleAsksDropped.Add(int64(dropped))
			if out == nil {
				return nil
			}
			kept = out
		}
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
		q.requeueOwedAnswersLocked()
		for q.queue.Len() == 0 && len(q.owed) == 0 {
			// Nothing to send: a held slot goes back so another peer's writer can use it.
			q.releaseCreditLocked()
			q.releaseHoseGrantLocked()
			select {
			case <-ctx.Done():
				return nil, ErrQueueCancelled
			default:
			}
			q.dataAvailable.Wait()
			if q.closed {
				return nil, ErrQueueClosed
			}
			q.requeueOwedAnswersLocked()
		}
		if q.queue.Len() == 0 {
			// Only owed offers wait: they leave alone (A′), and take no window slot.
			q.releaseHoseGrantLocked()
			if out := q.withOwedLocked(nil); out != nil {
				return out, nil
			}
			continue
		}
		if len(q.queue.priority) > 0 {
			// The urgent lane neither takes nor waits for a slot.
			rpc := q.queue.Pop()
			q.queuedBytes -= int64(rpc.size)
			q.spaceAvailable.Signal()
			if out := q.dispatch(rpc); out != nil {
				if out = q.finishPopLocked(out); out != nil {
					return out, nil
				}
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
			if out = q.finishPopLocked(out); out != nil {
				return out, nil
			}
		}
	}
}

// releaseHoseGrantLocked returns a window slot this writer was granted and has not used: the frame
// it waited for was discarded meanwhile (a group past its lifetime, v0.5's D′). Off the v0.5 paths a
// writer never holds a grant with nothing queued, so this does nothing there. Under queueMu.
func (q *rpcQueue) releaseHoseGrantLocked() {
	if q.hoseHolding {
		q.hoseHolding = false
		q.hose.unused()
	}
}

// finishPopLocked is the last step before an RPC leaves the queue: what is owed rides on it (A′),
// and its asks are reported as written (E), the ones an IWANT class loss takes included, since that
// loss stands for the wire. nil when the loss took all it carried. Under queueMu.
func (q *rpcQueue) finishPopLocked(out *RPC) *RPC {
	if len(q.owed) > 0 {
		if with := q.withOwedLocked(out); with != nil {
			out = with
		}
	}
	if q.onAsks != nil {
		var mids []string
		for _, w := range out.GetControl().GetIwant() {
			mids = append(mids, w.GetMessageIDs()...)
		}
		if len(mids) > 0 {
			if q.askLoss != nil {
				if kept, lost := q.askLoss(mids); len(lost) > 0 {
					if out = withAsks(out, kept); out == nil {
						// All lost on the wire: written all the same, at the write (after any send
						// delay), and then nothing goes out.
						return &RPC{asks: mids, lostOnly: true}
					}
				}
			}
			// The asks are reported when the writer writes the RPC (noteWrite), after any send delay;
			// an RPC carrying asks is per peer, but it is copied before it is marked.
			out = shallowRPCCopy(out)
			out.asks = mids
		}
	}
	return out
}

// noteAsksWritten reports rpc's asks as written now (E). The writer goroutine, at the write.
func (q *rpcQueue) noteAsksWritten(rpc *RPC) {
	if q.onAsks != nil && len(rpc.asks) > 0 {
		q.onAsks(rpc.asks, time.Now())
	}
}

// withAsks returns rpc with its asks replaced by ids (none when ids is empty); nil when nothing
// else is left to write. Never mutates rpc.
func withAsks(rpc *RPC, ids []string) *RPC {
	c := rpc.Control
	ctl := &pb.ControlMessage{Ihave: c.Ihave, Graft: c.Graft, Prune: c.Prune, Idontwant: c.Idontwant, Extensions: c.Extensions}
	if len(ids) > 0 {
		ctl.Iwant = []*pb.ControlIWant{{MessageIDs: ids}}
	}
	out := shallowRPCCopy(rpc)
	out.urgent = rpc.urgent
	out.Control = ctl
	if len(out.Publish) == 0 && rpcOtherwiseEmpty(out) {
		return nil
	}
	return out
}

// owe adds bits of root to what is owed to the peer as offers, and wakes the writer. Router
// goroutine (specv05.go).
func (q *rpcQueue) owe(topic, root string, bits *haveBits) {
	q.queueMu.Lock()
	defer q.queueMu.Unlock()
	if q.closed {
		return
	}
	if q.owed == nil {
		q.owed = make(map[string]*owedGroup)
	}
	g := q.owed[root]
	if g == nil {
		g = &owedGroup{topic: topic}
		q.owed[root] = g
	}
	g.bits.Or(bits.Bytes())
	q.dataAvailable.Signal()
}

// unowe drops index of root from what is owed: we pushed it to the peer since. Router goroutine.
func (q *rpcQueue) unowe(root string, index uint32) {
	q.queueMu.Lock()
	defer q.queueMu.Unlock()
	if g := q.owed[root]; g != nil {
		g.bits.Clear(index)
	}
}

// oweAnswer keeps an answer a full queue refused, to be queued once there is room. Router
// goroutine.
func (q *rpcQueue) oweAnswer(m *pb.Message) {
	q.queueMu.Lock()
	defer q.queueMu.Unlock()
	if q.closed {
		return
	}
	for _, o := range q.owedAnswers {
		if o == m || q.sameChunk(o, m) {
			return // one owed answer per chunk, as the spec's owed_answers bitmap holds
		}
	}
	// (Without a group and index, distinct messages stay distinct, even with equal payloads.)
	q.owedAnswers = append(q.owedAnswers, m)
	// The writer may have drained the queue and gone to sleep since the push was refused.
	q.dataAvailable.Signal()
}

// sameChunk reports whether a and b are the same chunk: the same group and index, when the queue
// knows the index function and both have one. Otherwise only the same message object is.
func (q *rpcQueue) sameChunk(a, b *pb.Message) bool {
	if q.have != nil {
		ra, ia, oka := q.have.index(a)
		rb, ib, okb := q.have.index(b)
		if oka && okb {
			return ra == rb && ia == ib
		}
	}
	return false
}

// requeueOwedAnswersLocked queues the owed answers while the queue has room: from there each is an
// ordinary answer, which the dispatch-time cancel drops if the peer has claimed it meanwhile.
// Under queueMu.
func (q *rpcQueue) requeueOwedAnswersLocked() {
	for len(q.owedAnswers) > 0 && q.queue.Len() < q.maxSize {
		m := q.owedAnswers[0]
		q.owedAnswers[0] = nil
		q.owedAnswers = q.owedAnswers[1:]
		rpc := &RPC{RPC: pb.RPC{Publish: []*pb.Message{m}}, solicited: true}
		rpc.size = proto.Size(&rpc.RPC)
		q.queue.NormalPush(rpc)
		q.queuedBytes += int64(rpc.size)
		if q.queuedBytes > q.peakQueued {
			q.peakQueued = q.queuedBytes
		}
	}
}

// discardGroup drops a group past its lifetime from the queue (v0.5's D′): what is owed of it, and
// its frames, offers and asks in every queued RPC; its HAVEs stay. ofMsg and ofID tell a frame and
// an id of the group. Returns how many frames and ids it dropped. Queued RPCs are shared across
// peers, so a changed one is replaced by a copy. Router goroutine.
func (q *rpcQueue) discardGroup(root string, ofMsg func(*pb.Message) bool, ofID func(string) bool) int {
	q.queueMu.Lock()
	defer q.queueMu.Unlock()
	dropped := 0
	delete(q.owed, root)
	answers := q.owedAnswers[:0]
	for _, m := range q.owedAnswers {
		if ofMsg(m) {
			dropped++
			continue
		}
		answers = append(answers, m)
	}
	q.owedAnswers = answers
	filter := func(rpc *RPC) *RPC {
		n := 0
		var publish []*pb.Message
		for _, m := range rpc.Publish {
			if ofMsg(m) {
				n++
				continue
			}
			publish = append(publish, m)
		}
		ctl := rpc.GetControl()
		var ihave []*pb.ControlIHave
		var iwant []*pb.ControlIWant
		for _, ih := range ctl.GetIhave() {
			var ids []string
			for _, id := range ih.GetMessageIDs() {
				if ofID(id) {
					n++
					continue
				}
				ids = append(ids, id)
			}
			if len(ids) > 0 {
				ihave = append(ihave, &pb.ControlIHave{TopicID: ih.TopicID, MessageIDs: ids})
			}
		}
		for _, iw := range ctl.GetIwant() {
			var ids []string
			for _, id := range iw.GetMessageIDs() {
				if ofID(id) {
					n++
					continue
				}
				ids = append(ids, id)
			}
			if len(ids) > 0 {
				iwant = append(iwant, &pb.ControlIWant{MessageIDs: ids})
			}
		}
		if n == 0 {
			return rpc
		}
		dropped += n
		out := shallowRPCCopy(rpc)
		out.urgent = rpc.urgent
		out.Publish = publish
		if ctl != nil {
			out.Control = &pb.ControlMessage{Ihave: ihave, Iwant: iwant, Graft: ctl.Graft, Prune: ctl.Prune, Idontwant: ctl.Idontwant, Extensions: ctl.Extensions}
		}
		if len(out.Publish) == 0 && rpcOtherwiseEmpty(out) {
			return nil
		}
		out.size = proto.Size(&out.RPC)
		return out
	}
	priority := q.queue.priority[:0]
	for _, rpc := range q.queue.priority {
		out := filter(rpc)
		q.queuedBytes -= int64(rpc.size)
		if out != nil {
			q.queuedBytes += int64(out.size)
			priority = append(priority, out)
		}
	}
	q.queue.priority = priority
	normal := q.queue.normal[:0]
	for _, e := range q.queue.normal {
		out := filter(e.rpc)
		q.queuedBytes -= int64(e.rpc.size)
		if out != nil {
			q.queuedBytes += int64(out.size)
			normal = append(normal, rankedRPC{rpc: out, rank: e.rank})
		}
	}
	q.queue.normal = normal
	q.spaceAvailable.Signal()
	return dropped
}

// withOwedLocked returns out carrying what is owed, one IHAVE bitmap per group, less what the peer
// has told us it holds and the frame out carries; an RPC of the owed IHAVEs alone when out is nil.
// The owed bits are spent either way. nil when nothing is left to send. Under queueMu.
func (q *rpcQueue) withOwedLocked(out *RPC) *RPC {
	if len(q.owed) == 0 {
		return nil
	}
	roots := make([]string, 0, len(q.owed))
	for root := range q.owed {
		roots = append(roots, root)
	}
	sort.Strings(roots)
	var ihaves []*pb.ControlIHave
	for _, root := range roots {
		g := q.owed[root]
		delete(q.owed, root)
		b := &g.bits
		if h := q.have; h != nil {
			if t := h.theirs[root]; t != nil {
				b = b.AndNot(t)
			}
			if c := h.claim[root]; c != nil {
				b = b.AndNot(c)
			}
			if out != nil {
				for _, m := range out.Publish {
					if r, i, ok := h.index(m); ok && r == root {
						b.Clear(i)
					}
				}
			}
		}
		n := b.Count()
		if n == 0 {
			continue
		}
		topic := g.topic
		ihaves = append(ihaves, &pb.ControlIHave{TopicID: &topic, MessageIDs: []string{encodeHave(root, b)}})
		if q.owedSent != nil {
			q.owedSent.Add(int64(n))
		}
	}
	if len(ihaves) == 0 {
		return nil
	}
	extra := &RPC{RPC: pb.RPC{Control: &pb.ControlMessage{Ihave: ihaves}}}
	if q.owedFilter != nil {
		if extra = q.owedFilter(extra); extra == nil {
			return nil
		}
	}
	if out == nil {
		return extra
	}
	with := shallowRPCCopy(out)
	ctl := &pb.ControlMessage{}
	if c := out.GetControl(); c != nil {
		ctl = &pb.ControlMessage{Ihave: c.Ihave, Iwant: c.Iwant, Graft: c.Graft, Prune: c.Prune, Idontwant: c.Idontwant, Extensions: c.Extensions}
	}
	ctl.Ihave = append(append([]*pb.ControlIHave(nil), ctl.Ihave...), extra.GetControl().GetIhave()...)
	with.Control = ctl
	return with
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
