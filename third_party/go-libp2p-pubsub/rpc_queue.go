package pubsub

import (
	"context"
	"errors"
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
	// onHoseExpire, when set, hears a frame's first window timeout (hoseExpire): v0.9's source takes it as
	// the chunk's acknowledgement when no HAVE came first (seacast_origin.go). Called under queueMu.
	onHoseExpire func(root string, idx uint32)
	// onProbeWritten, when set, hears our waiting probes of the peer as they are written (probe_sent,
	// seacast_probe.go). Called from the writer, off queueMu.
	onProbeWritten func(mids []string)
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
	// repairFrames and repairBytes count the rebuilt chunks written (RPC.repair) and their data bytes.
	repairFrames atomic.Int64
	repairBytes  atomic.Int64

	// have, when set, is the HAVE state the writer reads at dispatch (havebitmap.go); cancelled
	// counts messages dropped there, attached the RPCs that carried a pending HAVE.
	have      *queueHave
	cancelled atomic.Int64
	attached  atomic.Int64
	reordered atomic.Int64
	projected atomic.Int64

	// urgentCh wakes a writer waiting for a hose slot when an urgent RPC arrives, so the urgent
	// lane never waits for one.
	urgentCh chan struct{}

	// hose, when set, is the node-wide window over frames in flight (WithHoseWindow); hoseHolding
	// says this writer holds a slot for its next data pop, hoseSent is its frames in flight, and
	// peer is who the queue feeds (v0.5's writer hooks read it).
	hose        *hoseWindow
	hoseHolding bool
	hoseSent    *hoseSent
	peer        peer.ID
	// ownPicks counts the own choices that went ahead of the head (choose.go).
	ownPicks atomic.Int64

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
	q := &rpcQueue{maxSize: maxSize, urgentCh: make(chan struct{}, 1)}
	q.dataAvailable.L = &q.queueMu
	q.spaceAvailable.L = &q.queueMu
	return q
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
		held := q.have.ownHeld
		if len(q.have.probeWaits) > 0 {
			waits := q.have.probeWaits
			held = func(mid string) bool {
				if _, wait := waits[mid]; wait {
					return false // (R5) our waiting probe of the peer is written though we hold the cell
				}
				return q.have.ownHeld(mid)
			}
		}
		out, dropped := dropHeldAsks(kept, held)
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

	for q.fullLocked() {
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
			// Nothing to send: a held hose slot goes back so another peer's writer can use it.
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
		if q.hose != nil && q.have != nil && !q.hoseHolding && q.hasDataLocked() {
			// A data pop needs a slot in the node's hose window; same wait as for a credit.
			select {
			case <-q.urgentCh:
			default:
			}
			q.queueMu.Unlock()
			granted, err := q.hose.acquire(ctx, q.urgentCh)
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
			// Dispatch-time selection (choose.go): a source's own chunks and answers first when
			// those options are on, else the rarest queued frame ahead of the next control RPC.
			if i := q.have.chooseIndex(q.queue.normal, &q.ownPicks); i > 0 {
				rpc = q.queue.PopAt(i)
				q.reordered.Add(1)
			} else if i == 0 {
				rpc = q.queue.PopAt(0)
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
// and its asks are marked to be reported at the write (E, noteAsksWritten), the ones an IWANT class
// loss takes included, since that loss stands for the wire; when the loss took all it carried, a
// lostOnly marker carries its asks to the write, and nothing is written. Under queueMu.
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
	if rpc.Control == nil || len(rpc.Control.Iwant) == 0 {
		return
	}
	// The router installs the HAVE state and the hook under the lock, at its first send to the peer, while
	// this writer may already run: both are read under it.
	var hit []string
	q.queueMu.Lock()
	written := q.onProbeWritten
	if written != nil && q.have != nil {
		for _, w := range rpc.Control.Iwant {
			for _, id := range w.MessageIDs {
				if _, waits := q.have.probeWaits[id]; waits {
					hit = append(hit, id)
				}
			}
		}
	}
	q.queueMu.Unlock()
	if len(hit) > 0 {
		written(hit) // (R5) our probe, written now: its promise starts (seacastProbeWritten)
	}
}

// dropSettledProbes takes our settled probes out of rpc's asks at the write (probe_settled): judged or given
// no verdict while queued, a probe is not written later. nil when nothing else is left to write.
func (q *rpcQueue) dropSettledProbes(rpc *RPC) *RPC {
	if rpc.Control == nil || len(rpc.Control.Iwant) == 0 {
		return rpc
	}
	q.queueMu.Lock() // the router installs q.have under it (enableHave), maybe after this writer started
	if q.have == nil {
		q.queueMu.Unlock()
		return rpc
	}
	settled := q.have.probeSettled
	var ids []string
	dropped := 0
	for _, w := range rpc.Control.Iwant {
		for _, id := range w.MessageIDs {
			if _, s := settled[id]; s {
				dropped++
				continue
			}
			ids = append(ids, id)
		}
	}
	q.queueMu.Unlock()
	if dropped == 0 {
		return rpc
	}
	return withAsks(rpc, ids)
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
	for _, e := range q.queue.normal {
		if e.rpc.replay && len(e.rpc.Publish) == 1 && (e.rpc.Publish[0] == m || q.sameChunk(e.rpc.Publish[0], m)) {
			return // moved into the queue and not yet sent: it keeps its place
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
// ordinary answer, which the dispatch-time cancel drops if the peer has claimed it meanwhile, in its
// place in line (seacast-lite.md §7.2: "queued as soon as the queue has room, and is then an ordinary
// answer"). Marked replay, it takes no room from a later push (fullLocked), no entry ahead of it
// that isn't a payload frame waits for the hose on its account (hasDataLocked), and the
// dispatch-time selection never moves it ahead of such an entry (replayLead). Under queueMu.
func (q *rpcQueue) requeueOwedAnswersLocked() {
	for len(q.owedAnswers) > 0 && q.queue.Len() < q.maxSize {
		m := q.owedAnswers[0]
		q.owedAnswers[0] = nil
		q.owedAnswers = q.owedAnswers[1:]
		rpc := &RPC{RPC: pb.RPC{Publish: []*pb.Message{m}}, solicited: true, replay: true}
		rpc.size = proto.Size(&rpc.RPC)
		q.queue.NormalPush(rpc)
		q.queuedBytes += int64(rpc.size)
		if q.queuedBytes > q.peakQueued {
			q.peakQueued = q.queuedBytes
		}
	}
}

// fullLocked reports whether the queue refuses a push: it holds maxSize entries besides the owed
// answers moved into it (A′, requeueOwedAnswersLocked). An owed answer is one the queue without A′
// would have dropped, so it takes room from no push, whoever pushes (a stock topic's answer or
// subscription on a router the topics share, an ask): every push the queue without A′ would admit
// is admitted. The queue then holds at most maxSize other entries and up to maxSize owed answers
// (requeueOwedAnswersLocked moves one in only below maxSize entries in all). Under queueMu.
func (q *rpcQueue) fullLocked() bool {
	n := q.queue.Len()
	if n < q.maxSize {
		return false
	}
	for _, e := range q.queue.normal {
		if e.rpc.replay {
			n--
		}
	}
	return n >= q.maxSize
}

// replayLead is the index of the normal lane's first entry that is neither an owed answer nor a
// payload frame (payloadFrame). An owed answer ahead of it is an ordinary answer among the payload's
// frames and competes with them as the spec orders them (seacast-lite.md §7.2, its
// rarest). One past it waits for it, whatever the dispatch-time selection would choose
// (choose.go, rarestIndex), so an owed answer never moves ahead of a stock topic's entry or a
// control RPC that the queue without A′ would have sent before it. Under queueMu.
func replayLead(normal []rankedRPC, index func(*pb.Message) (string, uint32, bool)) int {
	for i, e := range normal {
		if !e.rpc.replay && !payloadFrame(e.rpc, index) {
			return i
		}
	}
	return len(normal)
}

// payloadFrame reports whether rpc carries frames and each of them has a group index: a chunk of
// the payload, whatever control rides with it (the router piggybacks control on any RPC). A stock
// topic's frame has no index; without an index function nothing is a payload frame.
func payloadFrame(rpc *RPC, index func(*pb.Message) (string, uint32, bool)) bool {
	if index == nil || len(rpc.Publish) == 0 {
		return false
	}
	for _, m := range rpc.Publish {
		if _, _, ok := index(m); !ok {
			return false
		}
	}
	return true
}

// discardGroup drops a group past its lifetime from the queue (v0.5's D′): what is owed of it, and
// its frames, offers and asks in every queued RPC; its HAVEs stay. ofMsg and ofID tell a frame and
// an id of the group. Returns how many frames and ids it dropped. Queued RPCs are shared across
// peers, so a changed one is replaced by a copy. Router goroutine.
// discardAsks drops the queued IWANT ids drop accepts and returns them: a group's ordinary asks at
// its completion (stop_pull; seacast_complete.go), the rest of the queue untouched. Router goroutine,
// under the queue's lock.
func (q *rpcQueue) discardAsks(drop func(string) bool) []string {
	q.queueMu.Lock()
	defer q.queueMu.Unlock()
	var dropped []string
	filter := func(rpc *RPC) *RPC {
		ctl := rpc.GetControl()
		if ctl == nil || len(ctl.GetIwant()) == 0 {
			return rpc
		}
		n := 0
		var iwant []*pb.ControlIWant
		for _, iw := range ctl.GetIwant() {
			var ids []string
			for _, id := range iw.GetMessageIDs() {
				if drop(id) {
					dropped = append(dropped, id)
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
		out := shallowRPCCopy(rpc)
		out.Control = &pb.ControlMessage{Ihave: ctl.Ihave, Iwant: iwant, Graft: ctl.Graft, Prune: ctl.Prune, Idontwant: ctl.Idontwant, Extensions: ctl.Extensions}
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
	if len(dropped) > 0 {
		q.spaceAvailable.Signal()
	}
	return dropped
}

func (q *rpcQueue) discardGroup(root string, ofMsg func(*pb.Message) bool, ofID func(string) bool) int {
	q.queueMu.Lock()
	defer q.queueMu.Unlock()
	dropped := 0
	delete(q.owed, root)
	if q.have != nil {
		// What was popped of the group goes with it: its frames leave the queue here, so no later
		// pop records it again, unless the group is taken up again and new frames of it are queued.
		// Otherwise only the peer's HAVE aging out or the connection's close drops it, and a peer
		// that sends no HAVE for the group never does.
		delete(q.have.popped, root)
	}
	answers := q.owedAnswers[:0]
	for _, m := range q.owedAnswers {
		if ofMsg(m) {
			dropped++
			continue
		}
		answers = append(answers, m)
	}
	clear(q.owedAnswers[len(answers):]) // the dropped answers' messages go with them, not kept by the array
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
	if q.hose != nil {
		q.hoseCloseLocked()
	}
	q.dataAvailable.Broadcast()
	q.spaceAvailable.Broadcast()
}

// noteWrite accounts one stream write by the writer goroutine: d is the time it blocked.
// noteRepairWritten counts the rebuilt chunks a written RPC carried (RPC.repair), after any dispatch-time
// cancel. Writer goroutine.
func (q *rpcQueue) noteRepairWritten(rpc *RPC) {
	if !rpc.repair {
		return
	}
	for _, m := range rpc.Publish {
		q.repairFrames.Add(1)
		q.repairBytes.Add(int64(len(m.Data)))
	}
}

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
		OwnFirst:     q.ownPicks.Load(),
		RepairFrames: q.repairFrames.Load(), RepairBytes: q.repairBytes.Load()}
}

// QueuedBytes is the bytes pushed and not yet popped by the writer: the load an announce to
// this peer carries (havebitmap.go).
func (q *rpcQueue) QueuedBytes() int64 {
	q.queueMu.Lock()
	defer q.queueMu.Unlock()
	return q.queuedBytes
}

// Occupancy returns the current queue length and its capacity, for the adaptive hedge's
// local congestion signal.
func (q *rpcQueue) Occupancy() (length, capacity int) {
	q.queueMu.Lock()
	defer q.queueMu.Unlock()
	return q.queue.Len(), q.maxSize
}
