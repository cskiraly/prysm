package segmentintegrationtest

// Bounded deferred admission -- measurement-plan section 21, stage 5.
//
// The receiver gate governs what a node *asks for*. It cannot govern what arrives: a peer may push
// an unsolicited segment on a subscribed topic, and the bytes and the initial parse happen whatever
// the receiver thinks. So the gate is only half the rule. The other half is the half that costs
// something, and until now the harness did not implement it at all:
//
//	a segment whose authorizing block is not installed must be buffered, NOT relayed.
//
// Every phase-arm number measured before this benefited from blockless nodes relaying segments they
// could not verify -- behaviour the design forbids -- which is why "gated pull wants eager push" had
// to be retired. This is the missing half.
//
// Three mechanics make it work, and each is load-bearing:
//
//   - **IGNORE, not REJECT.** An early arrival is nobody's fault. Prysm's segment validator already
//     distinguishes the three verdicts; a REJECT here would downscore honest forwarders.
//   - **Re-injection by local publish.** gossipsub marks a message seen *before* application
//     validation, so an IGNOREd id sits in the seen cache and network duplicates are not
//     redelivered. The fork's validation path re-validates a *local* publish of a message that was
//     seen but never delivered, precisely so it can still be forwarded -- so republishing the
//     buffered bytes once authority arrives both delivers and relays them.
//   - **Bounds with a per-peer reservation.** A global queue one peer can fill recreates
//     first-candidate exclusion, which is why the data-column sidecar rule makes the reservation a
//     SHOULD. Caps are global bytes, per-root bytes, and a reserved slot per (peer, index) class.

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/OffchainLabs/prysm/v7/testing/require"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pubsubpb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
)

// admissionStats aggregates admission decisions across a cell.
type admissionStats struct {
	accepted   atomic.Int64 // authorized on arrival
	deferred   atomic.Int64 // buffered pending authority
	drained    atomic.Int64 // republished once authority arrived
	dropped    atomic.Int64 // refused for want of queue space
	reserved   atomic.Int64 // admitted only because of a per-peer reservation
	peakBytes  atomic.Int64 // highest queue occupancy observed, any node
	calls      atomic.Int64 // validator invocations, to tell "not called" from "called and accepted"
	unclaimed  atomic.Int64 // invocations whose id carried no structural claim
	lost       atomic.Int64 // refused arrivals that are now UNRECOVERABLE without req/resp
	expired    atomic.Int64 // entries evicted by TTL before authority ever arrived
	evicted    atomic.Int64 // entries displaced to make room for a fairer candidate
	rejected   atomic.Int64 // groups the chain does not commit to, refused before the queue
	selfOrigin atomic.Int64 // this node's own publishes, which it never defers
	// lostHonest counts refusals and evictions whose victim was an honest sender, which is the
	// quantity the fairness rule turns on: a bounded queue that stays bounded by discarding honest
	// segments has not defended anything. Attribution is by immediate sender rather than by
	// originator, which is sound here precisely because the rule never relays what it defers or
	// refuses -- an adversary's candidate reaches only its own mesh neighbours, so `from` is the
	// adversary itself. If that ever stops holding, this counter starts lying.
	lostHonest atomic.Int64
	// Which bound refused, so "the queue held" can say what held it. The caps are not
	// interchangeable: a byte cap refuses the biggest attacker, a root-count cap refuses the most
	// *diverse* one, and tightening the wrong one buys nothing.
	refusedEntries atomic.Int64
	refusedRoots   atomic.Int64
	refusedHard    atomic.Int64
	refusedReserve atomic.Int64
}

// observePeak is a compare-and-swap maximum. A load-then-store across nodes lets a later, smaller
// occupancy overwrite a larger peak, which silently understates exactly the number the queue is
// sized from.
func (s *admissionStats) observePeak(v int64) {
	for {
		cur := s.peakBytes.Load()
		if v <= cur || s.peakBytes.CompareAndSwap(cur, v) {
			return
		}
	}
}

func (s *admissionStats) line() string {
	return fmt.Sprintf("admission: calls %d (unclaimed %d), accepted %d (self %d), deferred %d, drained %d, dropped %d, LOST %d, expired %d, evicted %d (reservation saved %d), rejected %d, honest losses %d, refused by [entries %d, roots %d, hard %d, reserve %d], peak queue %.0fKiB",
		s.calls.Load(), s.unclaimed.Load(), s.accepted.Load(), s.selfOrigin.Load(), s.deferred.Load(), s.drained.Load(),
		s.dropped.Load(), s.lost.Load(), s.expired.Load(), s.evicted.Load(),
		s.reserved.Load(), s.rejected.Load(), s.lostHonest.Load(),
		s.refusedEntries.Load(), s.refusedRoots.Load(), s.refusedHard.Load(), s.refusedReserve.Load(),
		float64(s.peakBytes.Load())/1024)
}

// admissionCaps bounds one node's queue.
//
// Two byte limits rather than one, and the second exists because the first one did not hold. The
// reservation admits a candidate that does *not* fit the soft caps, so that one peer filling the
// queue cannot exclude every other peer's copy -- but a reservation per (peer, index) class means up
// to degree x K reservations, which at degree 70 and K=32 is 2,240 slots. Measured: a node reached
// 744 KiB held against a 512 KiB per-root cap, because reservations walked straight past it. So the
// reservation is a soft-cap override, and hardBytes is the ceiling nothing overrides. This is the
// concrete form of "per-peer and per-root caps do not amount to a global budget".
type admissionCaps struct {
	globalBytes  int           // soft cap: an ordinary admission must fit under this
	perRootBytes int           // soft cap per authorizing block root
	hardBytes    int           // absolute ceiling; a reservation may exceed the soft caps but never this
	reservePeer  int           // reserved entries per *peer*, so one peer cannot exclude another
	maxEntries   int           // entry count bound: bytes alone do not bound map and per-entry overhead
	maxRoots     int           // distinct roots held: an attacker rotating unknown roots is otherwise free
	ttl          time.Duration // how long an entry is held before it is given up on
}

// The sizing constraint this makes explicit, which is a design fact rather than a tuning choice:
// a hard global ceiling and a guaranteed slot for every peer are in tension, and both can hold only
// if hardBytes >= globalBytes + (peers that can push) x (max segment size). Reservations keyed on
// anything finer than the peer do not give the guarantee at all -- keyed on (peer, index), one peer
// with K indices claims K reservations and walks past the soft cap by itself, which is what a live
// cell did when it reached 744 KiB against a 512 KiB per-root cap.

func defaultAdmissionCaps(t *testing.T) admissionCaps {
	// Sized from the measurement rather than guessed: the first two-publisher cells saw peak bytes
	// held per node of ~116 KiB (3-4 segments of 32 KiB). 1 MiB is an order of magnitude of
	// headroom and still bounded; the point of a cap is that it exists and is small.
	// The hard ceiling has to cover the soft cap plus one reserved slot per peer that can push, at
	// the maximum *encoded* candidate size -- at degree 70 and ~33 KiB segments that is about
	// 3.3 MiB, so the earlier 2 MiB did not actually support the guarantee it claimed. Entry and
	// root counts bound what bytes do not: map overhead, and an attacker rotating unknown roots
	// with tiny candidates. The TTL bounds a node that never gains authority at all.
	c := admissionCaps{
		globalBytes:  1 << 20,
		perRootBytes: 512 << 10,
		hardBytes:    4 << 20,
		reservePeer:  1,
		maxEntries:   256,
		maxRoots:     8,
		ttl:          8 * time.Second,
	}
	if v := os.Getenv("SEGMENT_ADMIT_BYTES"); v != "" {
		k, err := strconv.Atoi(v)
		if err != nil || k < 0 {
			t.Fatalf("bad SEGMENT_ADMIT_BYTES %q", v)
		}
		c.globalBytes = k
		c.hardBytes = 4 * k
	}
	if v := os.Getenv("SEGMENT_ADMIT_HARD_BYTES"); v != "" {
		k, err := strconv.Atoi(v)
		if err != nil || k < 0 {
			t.Fatalf("bad SEGMENT_ADMIT_HARD_BYTES %q", v)
		}
		c.hardBytes = k
	}
	if v := os.Getenv("SEGMENT_ADMIT_ROOT_BYTES"); v != "" {
		k, err := strconv.Atoi(v)
		if err != nil || k < 0 {
			t.Fatalf("bad SEGMENT_ADMIT_ROOT_BYTES %q", v)
		}
		c.perRootBytes = k
	}
	if v := os.Getenv("SEGMENT_ADMIT_RESERVE"); v != "" {
		k, err := strconv.Atoi(v)
		if err != nil || k < 0 {
			t.Fatalf("bad SEGMENT_ADMIT_RESERVE %q", v)
		}
		c.reservePeer = k
	}
	for name, dst := range map[string]*int{
		"SEGMENT_ADMIT_MAX_ENTRIES": &c.maxEntries,
		"SEGMENT_ADMIT_MAX_ROOTS":   &c.maxRoots,
	} {
		if v := os.Getenv(name); v != "" {
			k, err := strconv.Atoi(v)
			if err != nil || k < 1 {
				t.Fatalf("bad %s %q", name, v)
			}
			*dst = k
		}
	}
	if v := os.Getenv("SEGMENT_ADMIT_TTL_MS"); v != "" {
		k, err := strconv.Atoi(v)
		if err != nil || k < 0 {
			t.Fatalf("bad SEGMENT_ADMIT_TTL_MS %q", v)
		}
		c.ttl = time.Duration(k) * time.Millisecond
	}
	return c
}

// pendingSegment is one buffered arrival, with its provenance kept for attribution.
type pendingSegment struct {
	id  string
	key segKey
	// data is the encoded wire payload, kept because the drain republishes exactly these bytes.
	data []byte
	from peer.ID
	at   time.Time
	// reserved records whether THIS entry consumed its peer's reservation. Without it the drain
	// decremented a peer's reservation for any drained entry, so an ordinary entry under root A
	// released a reservation actually held under root B and the peer immediately obtained another.
	reserved bool
	class    classKey
}

// classKey identifies which (peer, index) slot a buffered candidate fills. It is provenance, kept
// for attribution and for releasing reservations on drain -- the *reservation* itself is per peer,
// because that is the only key at which "no peer can exclude another" is actually true.
type classKey struct {
	peer  peer.ID
	index uint32
}

// segmentAdmission is one node's bounded deferred-admission queue.
type segmentAdmission struct {
	mu        sync.Mutex
	caps      admissionCaps
	gate      *segmentGate
	byRoot    map[[32]byte][]pendingSegment
	rootBytes map[[32]byte]int
	classHave map[classKey]int
	reserved  map[peer.ID]int     // reserved entries currently held, per peer
	lostIDs   map[string]struct{} // ids refused and therefore unrecoverable without req/resp
	entries   int
	bytes     int
	stats     *admissionStats
	republish func(context.Context, []byte) error
	shadow    bool
	node      int
	self      peer.ID
	onDefer   func(node, size, held int)
	trace     func(string, ...any)
	// adversary reports whether a sender is one of the cell's attackers, so a loss can be
	// attributed. nil in every honest cell, where every loss is by definition honest.
	adversary func(peer.ID) bool

	// Per-node exposure and outcome, which is what the aggregate counters cannot express.
	//
	// A fabricated candidate is never relayed, so a node is pressured only by adversaries that
	// push to it directly. That makes each node's dose its own number, and a cell therefore
	// contains the whole dose-response curve at once -- nodes that drew no adversarial mesh peer
	// alongside nodes that drew two. Aggregating across nodes averages that curve away and reports
	// the mean of a distribution whose tail is the entire question.
	//
	// advFrom is the dose, measured rather than inferred from the graph: the distinct attackers
	// actually observed pushing to this node.
	advFrom     map[peer.ID]struct{}
	nDeferred   int
	nDropped    int
	nLostHonest int
	nPeak       int
}

// noteSender records an attacker that reached this node, which is this node's exposure.
func (a *segmentAdmission) noteSender(from peer.ID) {
	if a.adversary == nil || !a.adversary(from) {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.advFrom == nil {
		a.advFrom = make(map[peer.ID]struct{})
	}
	a.advFrom[from] = struct{}{}
}

// exposure reports how many distinct attackers pushed to this node.
func (a *segmentAdmission) exposure() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.advFrom)
}

// outcome reports this node's response to that exposure.
func (a *segmentAdmission) outcome() (deferred, dropped, lostHonest, peak int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.nDeferred, a.nDropped, a.nLostHonest, a.nPeak
}

// noteLoss records a refusal or eviction against its sender. Called with the lock held.
func (a *segmentAdmission) noteLoss(from peer.ID) {
	if a.adversary == nil || !a.adversary(from) {
		a.stats.lostHonest.Add(1)
		a.nLostHonest++
	}
}

func newSegmentAdmission(caps admissionCaps, gate *segmentGate, stats *admissionStats, shadow bool) *segmentAdmission {
	return &segmentAdmission{
		caps:      caps,
		gate:      gate,
		byRoot:    make(map[[32]byte][]pendingSegment),
		rootBytes: make(map[[32]byte]int),
		classHave: make(map[classKey]int),
		reserved:  make(map[peer.ID]int),
		lostIDs:   make(map[string]struct{}),
		stats:     stats,
		shadow:    shadow,
	}
}

// validate is the topic validator: the point where a node decides whether it may relay.
//
// Returning ValidationIgnore is what stops the relay -- gossipsub forwards only accepted messages --
// and it is the correct verdict rather than ValidationReject because an early arrival is nobody's
// fault. In shadow mode the arrival is recorded and then accepted anyway, so a control cell can
// price the rule without changing the race.
func (a *segmentAdmission) validate(_ context.Context, from peer.ID, msg *pubsub.Message) pubsub.ValidationResult {
	n := a.stats.calls.Add(1)
	if a.trace != nil && n <= 60 {
		a.trace("admit call %d node %d from %s local %v", n, a.node, from.String()[:6], msg.Local)
	}
	k, ok := segIDKey(msg.ID)
	if !ok {
		// No structural claim: nothing to authorize against, so this is not our business.
		a.stats.unclaimed.Add(1)
		return pubsub.ValidationAccept
	}
	// Never defer what we originated. Two paths reach here from this node itself: the builder's
	// own publish of the payload, and the drain's republish. Neither may be buffered -- the first
	// would mean the payload never leaves the builder at all, and the second would make the drain
	// fight itself.
	//
	// msg.Local is not sufficient: it is not set on messages published through PublishBatch, which
	// is how the builder publishes a whole group. Identity is, so compare the sender against our
	// own peer id.
	a.noteSender(from)
	if msg.Local || from == a.self {
		// Counted apart from accepted. A publisher validates its own messages, so folding these in
		// makes `accepted` scale with whatever the busiest publisher sent -- and under a flood the
		// adversary is the busiest by orders of magnitude, so the honest arrival count disappears
		// into its own attacker's self-accepts.
		a.stats.selfOrigin.Add(1)
		return pubsub.ValidationAccept
	}
	if a.gate.authorized(k.authorityKey()) {
		a.stats.accepted.Add(1)
		return pubsub.ValidationAccept
	}
	// The design's own rule, reachable once a cell models the committed set: authenticity is
	// membership of the group id in what the chain committed for the claimed slot. Checked after
	// the root path so a cell that models no commitments behaves exactly as it did before.
	if a.gate.committedGroup(k.slot, k.group) {
		a.stats.accepted.Add(1)
		return pubsub.ValidationAccept
	}
	// Holding the slot's commitments turns "I cannot verify this yet" into "this is not something
	// the chain committed to", and the two must not share a verdict. Buffering the second is
	// exactly what an attacker rotating fabricated groups would spend our memory on, so it is
	// refused before the queue rather than admitted into it.
	//
	// REJECT, unlike the early-arrival IGNORE above, because this *is* attributable: the sender
	// offered a group no block commits to. The deterrent that makes rejection cheap is peer
	// scoring, which the measurement builder does not install -- so in these cells rejection is
	// free to the sender, and any claim about its deterrent value is argued, not measured.
	if a.gate.slotCommitted(k.slot) {
		a.stats.rejected.Add(1)
		if a.shadow {
			return pubsub.ValidationAccept
		}
		return pubsub.ValidationReject
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.expireLocked()
	if !a.enqueueLocked(k, msg, from) {
		a.stats.dropped.Add(1)
		a.nDropped++
		// A refusal is not a neutral outcome, and treating it as one hid a real defect. gossipsub
		// marks a message seen *before* application validation, so an id we IGNORE and do not
		// retain sits in the seen cache while being in neither the queue nor the mcache: no network
		// duplicate will be redelivered, and no IWANT response can arrive either. The only routes
		// back are a local republish, which needs the bytes we just refused, or a req/resp fetch,
		// which this harness does not have. So a refusal is an UNRECOVERABLE loss until req/resp
		// exists, and it is counted separately and loudly rather than folded into a drop count.
		a.stats.lost.Add(1)
		a.noteLoss(from)
		a.lostIDs[msg.ID] = struct{}{}
		if a.shadow {
			return pubsub.ValidationAccept
		}
		return pubsub.ValidationIgnore
	}
	a.stats.deferred.Add(1)
	a.nDeferred++
	// Under enforcement a deferred segment is never delivered to the subscription until the drain,
	// so the arrival-time metrics cannot be observed there: measured at the subscription, "inverted"
	// is structurally zero the moment the rule is switched on. The validator is the only place that
	// sees the early arrival, so report it from here. Not called in shadow mode, where the
	// subscription still sees the arrival and would double-count it.
	if a.onDefer != nil && !a.shadow {
		a.onDefer(a.node, len(msg.GetData()), a.bytes)
	}
	if a.trace != nil {
		a.trace("DEFER node %d index %d from %s", a.node, k.index, from.String()[:6])
	}
	if a.shadow {
		return pubsub.ValidationAccept
	}
	return pubsub.ValidationIgnore
}

// enqueueLocked buffers one arrival, or reports that it does not fit.
//
// Admission is two-tier. A candidate that fits under the plain byte caps is taken. One that does not
// is still taken if its (peer, index) class has no entry yet and the reservation allows it, because
// a queue that one peer can fill excludes every other peer's copy of the same segment -- which is
// exactly the first-candidate exclusion the reservation exists to prevent.
func (a *segmentAdmission) enqueueLocked(k segKey, msg *pubsub.Message, from peer.ID) bool {
	size := len(msg.GetData())
	root := k.authorityKey()
	class := classKey{peer: from, index: k.index}
	// Count bounds first: bytes alone bound neither map overhead nor an attacker rotating unknown
	// roots with tiny candidates.
	// Zero means unbounded, consistently with hardBytes and ttl, so a caps literal that omits a
	// bound is permissive rather than accidentally refusing everything.
	if a.caps.maxEntries > 0 && a.entries >= a.caps.maxEntries {
		a.stats.refusedEntries.Add(1)
		return false
	}
	if a.caps.maxRoots > 0 {
		if _, known := a.byRoot[root]; !known && len(a.byRoot) >= a.caps.maxRoots {
			// Note this precedes the per-peer reservation below, so the reservation does not
			// protect a peer whose root is not already held: a table full of fabricated roots
			// refuses an honest one that arrives later, reservation or not.
			a.stats.refusedRoots.Add(1)
			return false
		}
	}
	// The hard ceiling overrides everything, reservations included. Without it the reservation is an
	// unbounded escape hatch.
	if a.caps.hardBytes > 0 && a.bytes+size > a.caps.hardBytes {
		a.stats.refusedHard.Add(1)
		return false
	}
	fits := a.bytes+size <= a.caps.globalBytes && a.rootBytes[root]+size <= a.caps.perRootBytes
	reservedHere := false
	if !fits {
		// Past the soft caps, only a peer not already holding a reservation may enter, and only from
		// the region between the soft cap and the hard ceiling. Per *peer* rather than per candidate:
		// a flooder gets one slot, not K.
		if a.reserved[from] >= a.caps.reservePeer {
			a.stats.refusedReserve.Add(1)
			return false
		}
		// Prefer displacing an over-quota peer to refusing this one. A refusal is unrecoverable
		// (see validate), so when the queue is full the loss should fall on whoever is over their
		// share rather than on whichever honest candidate happens to arrive last.
		for a.bytes+size > a.caps.hardBytes || a.rootBytes[root]+size > a.caps.perRootBytes {
			if !a.evictWorstLocked(from) {
				return false
			}
		}
		reservedHere = true
		a.stats.reserved.Add(1)
	}
	a.byRoot[root] = append(a.byRoot[root], pendingSegment{
		id: msg.ID, key: k, data: msg.GetData(), from: from, at: time.Now(),
		reserved: reservedHere, class: class,
	})
	a.rootBytes[root] += size
	a.bytes += size
	a.entries++
	a.classHave[class]++
	if reservedHere {
		a.reserved[from]++
	}
	a.stats.observePeak(int64(a.bytes))
	a.nPeak = max(a.nPeak, a.bytes)
	return true
}

// drain republishes everything held for root, once that block is installed.
//
// Republishing locally is the re-injection path: gossipsub marked these ids seen before validation,
// so no network duplicate will arrive again, but a local publish of a seen-but-never-delivered
// message is re-validated and can then be forwarded. Without this the buffered bytes would be held
// and then silently discarded.
func (a *segmentAdmission) drain(ctx context.Context, root [32]byte) {
	a.mu.Lock()
	pending := a.byRoot[root]
	republish := a.republish
	a.mu.Unlock()

	if republish == nil || len(pending) == 0 {
		return
	}
	// Republish first, release second. Deleting the entries up front and returning on the first
	// error lost the failed entry and every entry behind it, with nothing retaining the bytes and
	// the deferral ledger already treating them as seen. Retaining until a candidate is actually
	// published means a failure leaves the queue exactly as it was, still drainable.
	done := 0
	for _, p := range pending {
		if err := republish(ctx, p.data); err != nil {
			break
		}
		done++
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, p := range pending[:done] {
		a.releaseLocked(root, p)
		a.stats.drained.Add(1)
	}
	if rest := a.byRoot[root]; len(rest) == 0 {
		delete(a.byRoot, root)
		delete(a.rootBytes, root)
	}
}

// releaseLocked removes one entry's accounting. Caller holds the lock.
func (a *segmentAdmission) releaseLocked(root [32]byte, p pendingSegment) {
	// A fresh slice, NOT byRoot[root][:0]. Filtering in place reuses the backing array that a
	// caller's snapshot still points at, so releasing the first entry silently rewrote the pending
	// list being iterated -- the drain then released the wrong entries and leaked reservations.
	cur := a.byRoot[root]
	kept := make([]pendingSegment, 0, len(cur))
	for _, q := range cur {
		if q.id != p.id {
			kept = append(kept, q)
		}
	}
	a.byRoot[root] = kept
	a.rootBytes[root] -= len(p.data)
	a.bytes -= len(p.data)
	a.entries--
	a.classHave[p.class]--
	if a.classHave[p.class] <= 0 {
		delete(a.classHave, p.class)
	}
	// Only an entry that consumed a reservation returns one.
	if p.reserved && a.reserved[p.from] > 0 {
		a.reserved[p.from]--
		if a.reserved[p.from] == 0 {
			delete(a.reserved, p.from)
		}
	}
}

// expireLocked gives up on entries older than the TTL. Caller holds the lock.
//
// Without it a node that never gains authority pins its queue for the life of the process, which is
// the ordinary outcome of a partition or a withheld block rather than an exotic case.
func (a *segmentAdmission) expireLocked() {
	if a.caps.ttl <= 0 {
		return
	}
	for root := range a.byRoot {
		// Snapshot: releaseLocked rewrites byRoot[root].
		pending := append([]pendingSegment(nil), a.byRoot[root]...)
		for _, p := range pending {
			if time.Since(p.at) >= a.caps.ttl {
				a.releaseLocked(root, p)
				a.stats.expired.Add(1)
				// An expired entry is as unrecoverable as a refused one, for the same reason.
				a.stats.lost.Add(1)
				a.lostIDs[p.id] = struct{}{}
			}
		}
		if len(a.byRoot[root]) == 0 {
			delete(a.byRoot, root)
			delete(a.rootBytes, root)
		}
	}
}

// evictWorstLocked displaces one entry from the peer holding the most bytes, provided that is not
// the peer we are making room for. Returns false when there is nothing fair to evict.
func (a *segmentAdmission) evictWorstLocked(exclude peer.ID) bool {
	held := make(map[peer.ID]int)
	for _, pending := range a.byRoot {
		for _, p := range pending {
			held[p.from] += len(p.data)
		}
	}
	worst, worstBytes := peer.ID(""), 0
	for pid, b := range held {
		if pid == exclude {
			continue
		}
		if b > worstBytes {
			worst, worstBytes = pid, b
		}
	}
	if worstBytes == 0 {
		return false
	}
	for root := range a.byRoot {
		pending := append([]pendingSegment(nil), a.byRoot[root]...)
		for i := len(pending) - 1; i >= 0; i-- {
			if pending[i].from != worst {
				continue
			}
			p := pending[i]
			a.releaseLocked(root, p)
			a.stats.evicted.Add(1)
			a.stats.lost.Add(1)
			a.noteLoss(p.from)
			a.lostIDs[p.id] = struct{}{}
			return true
		}
	}
	return false
}

// drainAll releases every root. Tests only: in a live cell installing one block must not authorize
// candidates claiming every other root -- that would admit competing forks and attacker-chosen roots
// on the strength of one unrelated block -- so the driver calls drain(root) instead.
func (a *segmentAdmission) drainAll(ctx context.Context) {
	a.mu.Lock()
	roots := make([][32]byte, 0, len(a.byRoot))
	for root := range a.byRoot {
		roots = append(roots, root)
	}
	a.mu.Unlock()
	for _, root := range roots {
		a.drain(ctx, root)
	}
}

// held reports the current queue occupancy in bytes, for tests.
func (a *segmentAdmission) held() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.bytes
}

// maxIndexedSegmentTopics covers variant C's topic-per-index fan-out, whose largest tested value is
// 64. Registering past what a cell joins is harmless; registering short of it is a silent bypass.
const maxIndexedSegmentTopics = 128

// installAdmission registers each node's admission queue as the segment topic's validator and binds
// its drain to that node's own topic handle.
//
// The validator is where "may I relay this" is decided, so this is the only place the rule can go:
// gossipsub forwards accepted messages and nothing else. Republishing through the node's own topic
// handle makes the drained message a *local* publish, which is what gets it past the seen cache the
// original arrival already populated.
func installAdmission(t *testing.T, nw *simNetwork, gates *segmentGates, topics []*pubsub.Topic) {
	t.Helper()
	if gates == nil {
		return
	}
	topicStr := segmentTopic()
	for i, ps := range nw.Pubsubs {
		a := gates.admits[i]
		if a == nil {
			continue
		}
		topic := topics[i]
		a.mu.Lock()
		a.republish = func(ctx context.Context, data []byte) error { return topic.Publish(ctx, data) }
		a.node = i
		a.self = nw.Hosts[i].ID()
		if os.Getenv("SEGMENT_ADMIT_TRACE") != "" {
			a.trace = t.Logf
		}
		a.mu.Unlock()
		// Inline validation, so the verdict is reached before gossipsub decides whether to forward
		// on this delivery rather than concurrently with it. The ordering is the whole point: an
		// asynchronous validator would let a segment be relayed while its authority check is still
		// in flight, which is the property that has to be *established* rather than assumed.
		// The base topic and every indexed variant. Variant C publishes each segment index on
		// `<segment topic>/<index>`, and a validator registered only on the base string would let
		// every one of those topics bypass admission entirely.
		require.NoError(t, ps.RegisterTopicValidator(topicStr, a.validate, pubsub.WithValidatorInline(true)))
		for idx := range maxIndexedSegmentTopics {
			indexed := segmentTopicIndexed(idx)
			if err := ps.RegisterTopicValidator(indexed, a.validate, pubsub.WithValidatorInline(true)); err != nil {
				require.NoError(t, err)
			}
		}
	}
}

// TestSegmentAdmission covers the rule's own logic without a network.
func TestSegmentAdmission(t *testing.T) {
	mkKey := func(index uint32, rootByte byte) segKey {
		k := segKey{slot: 7, index: index}
		k.blockRoot[0] = rootByte
		k.group[0] = 0xcd
		return k
	}
	// pubsub.Message embeds *pb.Message, so the payload lives on the inner message.
	mkMsg := func(k segKey, tag byte, size int) *pubsub.Message {
		id := append(k.encode(), make([]byte, segIDDigestLen)...)
		id[len(id)-1] = tag
		return &pubsub.Message{
			ID:      string(id),
			Message: &pubsubpb.Message{Data: make([]byte, size)},
		}
	}

	newAdm := func(caps admissionCaps) (*segmentAdmission, *segmentGate) {
		g := newSegmentGate(8, &segmentGateStats{}, false)
		return newSegmentAdmission(caps, g, &admissionStats{}, false), g
	}

	// The modelled commitment set (segmentGate.commit) is what separates "cannot verify yet" from
	// "not a thing the chain committed to". Without it every unknown candidate defers, which is
	// the pre-bid behaviour and measures our unfinished work rather than the design.
	t.Run("committed set", func(t *testing.T) {
		committedGroup := func(k segKey) [32]byte { return k.group }

		t.Run("a group outside the slot's committed set is rejected, not buffered", func(t *testing.T) {
			a, g := newAdm(admissionCaps{globalBytes: 1 << 20, perRootBytes: 1 << 20, hardBytes: 4 << 20, reservePeer: 1})
			honest := mkKey(0, 1)
			g.commit(honest.slot, time.Now(), committedGroup(honest))

			// Same slot, a group the chain never committed to, under a root of the attacker's own
			// choosing -- the shape a flood takes, and the one a root-keyed check can only defer.
			bogus := mkKey(0, 9)
			bogus.group[0] = 0xff
			got := a.validate(context.Background(), peer.ID("adv"), mkMsg(bogus, 2, 4096))
			if got != pubsub.ValidationReject {
				t.Fatalf("verdict %v, want ValidationReject -- the slot's commitments are held and this group is not among them", got)
			}
			if a.held() != 0 {
				t.Fatalf("held %d bytes, want the flood refused before the queue", a.held())
			}
		})

		t.Run("a committed group is accepted without its root being opened", func(t *testing.T) {
			a, g := newAdm(admissionCaps{globalBytes: 1 << 20, perRootBytes: 1 << 20, hardBytes: 4 << 20, reservePeer: 1})
			k := mkKey(1, 2)
			g.commit(k.slot, time.Now(), committedGroup(k))
			// Authentication is membership of the group id, so nothing here opens k's block root.
			if got := a.validate(context.Background(), peer.ID("p1"), mkMsg(k, 3, 2048)); got != pubsub.ValidationAccept {
				t.Fatalf("verdict %v, want ValidationAccept -- the group is committed", got)
			}
			if a.held() != 0 {
				t.Fatalf("held %d bytes, want an authenticated segment passed straight through", a.held())
			}
		})

		t.Run("before the commitments install, the same flood only defers", func(t *testing.T) {
			a, _ := newAdm(admissionCaps{globalBytes: 1 << 20, perRootBytes: 1 << 20, hardBytes: 4 << 20, reservePeer: 1})
			// No commit call: this node does not hold the slot's set, so it cannot tell a
			// fabricated group from an honest segment that outran its block. This is the whole
			// attack surface, and its width is delta_install.
			bogus := mkKey(0, 9)
			bogus.group[0] = 0xff
			if got := a.validate(context.Background(), peer.ID("adv"), mkMsg(bogus, 2, 4096)); got != pubsub.ValidationIgnore {
				t.Fatalf("verdict %v, want ValidationIgnore -- nothing distinguishes this from an early arrival", got)
			}
			if a.held() != 4096 {
				t.Fatalf("held %d bytes, want the candidate buffered", a.held())
			}
		})

		t.Run("an empty set means the slot is not held, not that nothing is committed", func(t *testing.T) {
			a, g := newAdm(admissionCaps{globalBytes: 1 << 20, perRootBytes: 1 << 20, hardBytes: 4 << 20, reservePeer: 1})
			g.commit(7, time.Now()) // installed, no groups
			// Reading an empty set as complete would refuse every honest segment in any cell that
			// never installed commitments -- the failure this default exists to prevent.
			if got := a.validate(context.Background(), peer.ID("p1"), mkMsg(mkKey(0, 1), 1, 1024)); got != pubsub.ValidationIgnore {
				t.Fatalf("verdict %v, want ValidationIgnore -- an unheld slot cannot refuse anything", got)
			}
		})

		t.Run("commitments that have not arrived yet do not authenticate", func(t *testing.T) {
			a, g := newAdm(admissionCaps{globalBytes: 1 << 20, perRootBytes: 1 << 20, hardBytes: 4 << 20, reservePeer: 1})
			k := mkKey(0, 1)
			// Dated forward, the way the synthetic schedule dates authority: held later, not now.
			g.commit(k.slot, time.Now().Add(time.Hour), committedGroup(k))
			if got := a.validate(context.Background(), peer.ID("p1"), mkMsg(k, 1, 1024)); got != pubsub.ValidationIgnore {
				t.Fatalf("verdict %v, want ValidationIgnore -- the set is dated in the future", got)
			}
			if a.held() != 1024 {
				t.Fatalf("held %d bytes, want the segment buffered until its commitments arrive", a.held())
			}
		})
	})

	// Root-table exhaustion, and the limit of the per-peer guarantee.
	//
	// maxRoots bounds what bytes do not: an attacker rotating unknown roots with tiny candidates.
	// It is checked *before* the reservation, so the reservation -- which exists precisely so one
	// peer cannot exclude another -- does not cover this case. An attacker that fills the table
	// with fabricated roots therefore refuses an honest root that arrives later, and a refusal is
	// unrecoverable without req/resp.
	//
	// The exposure is real but bounded by the same window as everything else in this design: once
	// the slot's commitments install, a fabricated root is rejected before it can occupy a slot,
	// so the width of this hole is delta_install. Recorded rather than fixed, because the obvious
	// fix -- bounding roots per sender rather than globally -- is a design change, not a tuning
	// one. See notes/adversarial-cell-design.md.
	t.Run("a root table filled by one peer refuses an honest root despite its reservation", func(t *testing.T) {
		caps := admissionCaps{globalBytes: 1 << 20, perRootBytes: 1 << 20, hardBytes: 4 << 20,
			reservePeer: 1, maxRoots: 4}
		a, _ := newAdm(caps)
		for i := range 4 {
			k := mkKey(uint32(i), byte(0x40+i)) // lint:ignore uintcast -- loop bound 4.
			if got := a.validate(context.Background(), peer.ID("adv"), mkMsg(k, byte(i), 512)); got != pubsub.ValidationIgnore {
				t.Fatalf("filling root %d: verdict %v, want ValidationIgnore", i, got)
			}
		}
		// An honest peer, holding no reservation, offering a root the table has no room for.
		honest := mkKey(0, 0xee)
		got := a.validate(context.Background(), peer.ID("honest"), mkMsg(honest, 9, 512))
		if got != pubsub.ValidationIgnore {
			t.Fatalf("verdict %v, want ValidationIgnore", got)
		}
		if a.stats.refusedRoots.Load() != 1 {
			t.Fatalf("refusedRoots %d, want 1 -- the root count is what refused it", a.stats.refusedRoots.Load())
		}
		if a.stats.lostHonest.Load() != 1 {
			t.Fatalf("lostHonest %d, want 1 -- an honest sender lost a segment unrecoverably", a.stats.lostHonest.Load())
		}
	})

	t.Run("an unauthorized arrival is ignored, not rejected", func(t *testing.T) {
		a, _ := newAdm(admissionCaps{globalBytes: 1 << 20, perRootBytes: 1 << 20, hardBytes: 4 << 20, reservePeer: 1})
		got := a.validate(context.Background(), peer.ID("p1"), mkMsg(mkKey(0, 1), 1, 1024))
		if got != pubsub.ValidationIgnore {
			t.Fatalf("verdict %v, want ValidationIgnore -- an early arrival is nobody's fault", got)
		}
		if a.held() != 1024 {
			t.Fatalf("held %d bytes, want the segment buffered", a.held())
		}
	})

	t.Run("an authorized arrival is accepted and not buffered", func(t *testing.T) {
		a, g := newAdm(admissionCaps{globalBytes: 1 << 20, perRootBytes: 1 << 20, hardBytes: 4 << 20, reservePeer: 1})
		g.open(mkKey(0, 1).authorityKey(), time.Now())
		if got := a.validate(context.Background(), peer.ID("p1"), mkMsg(mkKey(0, 1), 1, 1024)); got != pubsub.ValidationAccept {
			t.Fatalf("verdict %v, want ValidationAccept", got)
		}
		if a.held() != 0 {
			t.Fatalf("held %d bytes for an authorized arrival", a.held())
		}
	})

	t.Run("a message with no structural claim is none of its business", func(t *testing.T) {
		a, _ := newAdm(admissionCaps{globalBytes: 1 << 20, perRootBytes: 1 << 20, hardBytes: 4 << 20, reservePeer: 1})
		m := &pubsub.Message{ID: "twenty-byte-id-here!", Message: &pubsubpb.Message{Data: make([]byte, 16)}}
		if got := a.validate(context.Background(), peer.ID("p1"), m); got != pubsub.ValidationAccept {
			t.Fatalf("verdict %v, want ValidationAccept for an unclaimed id", got)
		}
	})

	t.Run("the byte cap bounds the queue", func(t *testing.T) {
		a, _ := newAdm(admissionCaps{globalBytes: 2048, perRootBytes: 1 << 20, hardBytes: 4096, reservePeer: 0})
		for i := range 4 {
			a.validate(context.Background(), peer.ID("p1"), mkMsg(mkKey(uint32(i), 1), byte(i), 1024)) // lint:ignore uintcast -- test index.
		}
		if a.held() > 2048 {
			t.Fatalf("held %d bytes past a 2048-byte cap", a.held())
		}
		if a.stats.dropped.Load() == 0 {
			t.Fatal("cap never bit, so the test proves nothing")
		}
	})

	t.Run("one peer cannot exclude another once the queue is full", func(t *testing.T) {
		// Flooder fills the queue; the honest peer's copy must still get in, on its reservation.
		// Sized per the constraint above: the soft cap plus one reserved slot per pushing peer.
		a, _ := newAdm(admissionCaps{globalBytes: 2048, perRootBytes: 1 << 20, hardBytes: 4096, reservePeer: 1})
		for i := range 8 {
			a.validate(context.Background(), peer.ID("flood"), mkMsg(mkKey(uint32(i), 1), byte(i), 1024)) // lint:ignore uintcast -- test index.
		}
		if a.reserved[peer.ID("flood")] > 1 {
			t.Fatalf("one peer took %d reservations", a.reserved[peer.ID("flood")])
		}
		before := a.stats.reserved.Load()
		got := a.validate(context.Background(), peer.ID("honest"), mkMsg(mkKey(99, 1), 9, 1024))
		if got != pubsub.ValidationIgnore {
			t.Fatalf("verdict %v, want the honest copy buffered", got)
		}
		if a.stats.reserved.Load() != before+1 {
			t.Fatal("the honest copy was not admitted on its reservation -- one peer can exclude another")
		}
	})

	t.Run("reservations cannot walk past the hard ceiling", func(t *testing.T) {
		// The failure this pins: a reservation per (peer, index) class is degree x K slots, so
		// distinct classes could each override the soft cap and the queue grew without bound. A
		// live cell reached 744 KiB against a 512 KiB per-root cap this way.
		a, _ := newAdm(admissionCaps{globalBytes: 2048, perRootBytes: 2048, hardBytes: 4096, reservePeer: 1})
		for pi := range 20 {
			for idx := range 20 {
				a.validate(context.Background(),
					peer.ID(fmt.Sprintf("peer-%d", pi)),
					mkMsg(mkKey(uint32(idx), 1), byte(idx), 1024)) // lint:ignore uintcast -- test index.
			}
		}
		if a.held() > 4096 {
			t.Fatalf("held %d bytes past a 4096-byte hard ceiling across %d classes",
				a.held(), len(a.classHave))
		}
		if a.stats.dropped.Load() == 0 {
			t.Fatal("the ceiling never bit, so the test proves nothing")
		}
	})

	t.Run("the per-root cap is separate from the global one", func(t *testing.T) {
		a, _ := newAdm(admissionCaps{globalBytes: 1 << 20, perRootBytes: 2048, hardBytes: 4 << 20, reservePeer: 0})
		for i := range 4 {
			a.validate(context.Background(), peer.ID("p1"), mkMsg(mkKey(uint32(i), 7), byte(i), 1024)) // lint:ignore uintcast -- test index.
		}
		// A different root has its own budget.
		if got := a.validate(context.Background(), peer.ID("p1"), mkMsg(mkKey(0, 8), 20, 1024)); got != pubsub.ValidationIgnore {
			t.Fatalf("verdict %v: a second root was refused its own budget", got)
		}
	})

	t.Run("draining republishes what was held and empties the queue", func(t *testing.T) {
		a, _ := newAdm(admissionCaps{globalBytes: 1 << 20, perRootBytes: 1 << 20, hardBytes: 4 << 20, reservePeer: 1})
		var published int
		a.republish = func(context.Context, []byte) error { published++; return nil }
		for i := range 3 {
			a.validate(context.Background(), peer.ID("p1"), mkMsg(mkKey(uint32(i), 1), byte(i), 1024)) // lint:ignore uintcast -- test index.
		}
		root := mkKey(0, 1).authorityKey()
		a.drain(context.Background(), root)
		if published != 3 {
			t.Fatalf("republished %d of 3 buffered segments", published)
		}
		if a.held() != 0 {
			t.Fatalf("held %d bytes after draining", a.held())
		}
		t.Run("and the reservation slots are returned", func(t *testing.T) {
			if len(a.classHave) != 0 {
				t.Fatalf("%d class reservations still held after drain", len(a.classHave))
			}
		})
	})

	t.Run("a local republish is never re-deferred", func(t *testing.T) {
		a, _ := newAdm(admissionCaps{globalBytes: 1 << 20, perRootBytes: 1 << 20, hardBytes: 4 << 20, reservePeer: 1})
		m := mkMsg(mkKey(0, 1), 1, 1024)
		m.Local = true
		if got := a.validate(context.Background(), peer.ID(""), m); got != pubsub.ValidationAccept {
			t.Fatalf("verdict %v: the drain path would deadlock against itself", got)
		}
		if a.held() != 0 {
			t.Fatal("a local republish was buffered instead of delivered")
		}
	})

	t.Run("a refusal is recorded as an unrecoverable loss", func(t *testing.T) {
		// The defect this pins: gossipsub marks a message seen before application validation, so an
		// id we IGNORE and do not retain is in the seen cache but in neither the queue nor the
		// mcache. No network duplicate is redelivered and no IWANT response can arrive, so the only
		// routes back are a local republish of bytes we just refused, or a req/resp fetch that does
		// not exist yet. Silence here read as success across 90 cells.
		a, _ := newAdm(admissionCaps{globalBytes: 1024, perRootBytes: 1024, hardBytes: 1024, reservePeer: 0})
		a.validate(context.Background(), peer.ID("p1"), mkMsg(mkKey(0, 1), 1, 1024))
		a.validate(context.Background(), peer.ID("p1"), mkMsg(mkKey(1, 1), 2, 1024))
		if a.stats.lost.Load() != 1 {
			t.Fatalf("lost counted %d times, want 1", a.stats.lost.Load())
		}
		if len(a.lostIDs) != 1 {
			t.Fatalf("%d ids recorded as lost, want 1", len(a.lostIDs))
		}
	})

	t.Run("the TTL gives up on a node that never gains authority", func(t *testing.T) {
		a, _ := newAdm(admissionCaps{globalBytes: 1 << 20, perRootBytes: 1 << 20, hardBytes: 4 << 20,
			reservePeer: 1, ttl: 30 * time.Millisecond})
		a.validate(context.Background(), peer.ID("p1"), mkMsg(mkKey(0, 1), 1, 1024))
		if a.held() == 0 {
			t.Fatal("nothing buffered, so the expiry test proves nothing")
		}
		time.Sleep(40 * time.Millisecond)
		// Expiry runs on the next admission attempt, which is when the bound actually matters.
		a.validate(context.Background(), peer.ID("p2"), mkMsg(mkKey(1, 1), 2, 1024))
		if a.stats.expired.Load() != 1 {
			t.Fatalf("expired %d entries, want 1 -- a node that never gains authority pins its queue",
				a.stats.expired.Load())
		}
	})

	t.Run("opening one root leaves another held", func(t *testing.T) {
		// Installing any block must not release candidates claiming every root: that would
		// authorize competing forks and arbitrary attacker-chosen roots on one unrelated block.
		a, g := newAdm(admissionCaps{globalBytes: 1 << 20, perRootBytes: 1 << 20, hardBytes: 4 << 20, reservePeer: 1})
		var published int
		a.republish = func(context.Context, []byte) error { published++; return nil }
		a.validate(context.Background(), peer.ID("p1"), mkMsg(mkKey(0, 0xAA), 1, 1024))
		a.validate(context.Background(), peer.ID("p1"), mkMsg(mkKey(0, 0xBB), 2, 1024))
		rootA := mkKey(0, 0xAA).authorityKey()
		g.open(rootA, time.Now())
		a.drain(context.Background(), rootA)
		if published != 1 {
			t.Fatalf("republished %d entries, want exactly root A's one", published)
		}
		if a.held() != 1024 {
			t.Fatalf("held %d bytes, want root B's 1024 still held", a.held())
		}
	})

	t.Run("a failed republish leaves the queue drainable", func(t *testing.T) {
		// Deleting entries before publishing and returning on the first error lost the failed entry
		// and everything behind it, with the deferral ledger already treating them as seen.
		a, _ := newAdm(admissionCaps{globalBytes: 1 << 20, perRootBytes: 1 << 20, hardBytes: 4 << 20, reservePeer: 1})
		fail := true
		a.republish = func(context.Context, []byte) error {
			if fail {
				return context.Canceled
			}
			return nil
		}
		for i := range 3 {
			a.validate(context.Background(), peer.ID("p1"), mkMsg(mkKey(uint32(i), 1), byte(i), 1024)) // lint:ignore uintcast -- test index.
		}
		root := mkKey(0, 1).authorityKey()
		a.drain(context.Background(), root)
		if a.held() != 3*1024 {
			t.Fatalf("held %d bytes after a failed drain, want all 3072 retained", a.held())
		}
		fail = false
		a.drain(context.Background(), root)
		if a.held() != 0 {
			t.Fatalf("held %d bytes after a successful retry", a.held())
		}
	})

	t.Run("shadow mode records and accepts", func(t *testing.T) {
		g := newSegmentGate(8, &segmentGateStats{}, false)
		a := newSegmentAdmission(admissionCaps{globalBytes: 1 << 20, perRootBytes: 1 << 20, hardBytes: 4 << 20, reservePeer: 1},
			g, &admissionStats{}, true)
		if got := a.validate(context.Background(), peer.ID("p1"), mkMsg(mkKey(0, 1), 1, 1024)); got != pubsub.ValidationAccept {
			t.Fatalf("verdict %v, want ValidationAccept in shadow mode", got)
		}
		if a.stats.deferred.Load() != 1 {
			t.Fatal("shadow mode did not record the deferral it would have made")
		}
	})
}
