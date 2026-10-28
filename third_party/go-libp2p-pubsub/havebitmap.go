package pubsub

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"math/bits"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
)

// --- HAVE bitmaps for structured-id groups (D1) ---
//
// A segmented payload is K messages whose ids share a group root and carry an index. IDONTWANT
// names each one by its full id -- 56 bytes in the series, 101 in the harness shim -- once per
// message per mesh peer. A HAVE bitmap says the same for the whole group in one pseudo id,
// magic ‖ version ‖ root ‖ bitmap, sent exactly where IDONTWANT is sent (on receipt, to mesh
// peers, in the urgent lane) and carried inside the IDONTWANT control message, so a peer that
// does not know the encoding sees an id it will never match and nothing else changes for it.
//
// Everything that suppresses a send keeps reading the per-id unwanted map. A HAVE is expanded
// into it lazily: for the ids we already know when the HAVE arrives, and for the rest when the
// message arrives (Preprocess). The forward path, the batch publisher's filter and the IWANT
// handler are untouched. The bitmap is idempotent state, so control coalescing replaces the
// older HAVE for a root instead of appending to it -- which is where the bytes go.

const (
	haveMagic   = "HAVE"
	haveVersion = 1
	// haveVersionClaim is the claim kind (encodeHaveClaim); haveVersionLoad = 2 lives with the
	// urgent announce below.
	haveVersionClaim = 3
	haveRootLen      = 32
	haveHeaderLen    = len(haveMagic) + 1 + haveRootLen
	// haveMaxBytes bounds a bitmap a peer may send: 8192 indices.
	haveMaxBytes = 1024
)

// haveBits is a growable bitset over segment indices, little-endian within a byte.
type haveBits struct{ b []byte }

// Set marks index i. Indices past the bound are ignored rather than grown into.
func (h *haveBits) Set(i uint32) {
	if i >= haveMaxBytes*8 {
		return
	}
	at := int(i / 8)
	for len(h.b) <= at {
		h.b = append(h.b, 0)
	}
	h.b[at] |= 1 << (i % 8)
}

// Clear unmarks index i; indices past the stored bytes are already clear.
func (h *haveBits) Clear(i uint32) {
	at := int(i / 8)
	if at < len(h.b) {
		h.b[at] &^= 1 << (i % 8)
	}
}

// Has reports whether index i is set.
func (h *haveBits) Has(i uint32) bool {
	at := int(i / 8)
	return at < len(h.b) && h.b[at]&(1<<(i%8)) != 0
}

// Or folds an encoded bitmap in and returns the indices that were clear before.
func (h *haveBits) Or(enc []byte) []uint32 {
	var newly []uint32
	for len(h.b) < len(enc) {
		h.b = append(h.b, 0)
	}
	for i, x := range enc {
		if flipped := x &^ h.b[i]; flipped != 0 {
			for bit := uint32(0); bit < 8; bit++ {
				if flipped&(1<<bit) != 0 {
					newly = append(newly, uint32(i)*8+bit) // lint:ignore uintcast -- i < haveMaxBytes.
				}
			}
		}
		h.b[i] |= x
	}
	return newly
}

// AndNot returns a copy of h with other's bits cleared.
func (h *haveBits) AndNot(other *haveBits) *haveBits {
	out := &haveBits{b: append([]byte(nil), h.b...)}
	if other != nil {
		for i := range out.b {
			if i < len(other.b) {
				out.b[i] &^= other.b[i]
			}
		}
	}
	return out
}

// Bytes is the wire form: the bitmap with trailing zero bytes trimmed.
func (h *haveBits) Bytes() []byte {
	n := len(h.b)
	for n > 0 && h.b[n-1] == 0 {
		n--
	}
	return h.b[:n]
}

// Count is the number of indices set.
func (h *haveBits) Count() int {
	n := 0
	for _, x := range h.b {
		n += bits.OnesCount8(x)
	}
	return n
}

// encodeHave builds the pseudo id for root's bitmap.
func encodeHave(root string, bits *haveBits) string {
	out := make([]byte, 0, haveHeaderLen+len(bits.b))
	out = append(out, haveMagic...)
	out = append(out, haveVersion)
	out = append(out, root...)
	out = append(out, bits.Bytes()...)
	return string(out)
}

// decodeHave reads a pseudo id of any version, or reports that mid is an ordinary id.
func decodeHave(mid string) (root string, bitmap []byte, ok bool) {
	root, _, bitmap, ok = decodeHaveLoad(mid)
	return root, bitmap, ok
}

// encodeHaveClaim builds the claim pseudo id for root's bitmap: "stop sending me these" without
// "I hold these". A pusher stops pushing the claimed indices but keeps announcing to the peer,
// counts no holder for them, and lets the peer's later asks by index through.
func encodeHaveClaim(root string, bits *haveBits) string {
	out := make([]byte, 0, haveHeaderLen+len(bits.b))
	out = append(out, haveMagic...)
	out = append(out, haveVersionClaim)
	out = append(out, root...)
	out = append(out, bits.Bytes()...)
	return string(out)
}

// isHaveClaim reports whether mid is a claim pseudo id.
func isHaveClaim(mid string) bool {
	return len(mid) >= haveHeaderLen && mid[:len(haveMagic)] == haveMagic && mid[len(haveMagic)] == haveVersionClaim
}

// haveEntry is one bitmap with the IDONTWANT TTL, in heartbeats. claim is what the peer asked
// us to stop sending without holding it (encodeHaveClaim), kept apart from bits so a real HAVE
// after a claim still registers as new.
type haveEntry struct {
	bits  haveBits
	claim haveBits
	ttl   int
	// In pushed only: when we first pushed the peer a frame of the root, and whether the proof
	// guard has demoted the pair (WithPhaseSelectFewestGuard).
	first   time.Time
	demoted bool
}

// haveIDs is the ids known at each index of one root -- held or only announced -- with the
// IDONTWANT TTL, in heartbeats, since the last one was learned.
type haveIDs struct {
	ids map[uint32]string
	ttl int
}

// haveState is the router's HAVE bookkeeping: what we hold per root, which id sits at each
// index we know of, and what each peer has told us it holds.
type haveState struct {
	index func(mid string) (root string, index uint32, ok bool)
	mine  map[string]*haveEntry
	mids  map[string]*haveIDs
	peers map[peer.ID]map[string]*haveEntry
	// pushed is what we have pushed to each peer per root. The phase announce names a message
	// only to the peers it did not push it to; the per-peer bitmap keeps that by leaving out
	// what we pushed to the peer and what the peer has told us it holds.
	pushed map[peer.ID]map[string]*haveEntry
	// Dispatch-time decisions (WithHaveDispatch): msgIndex is index over a queued message.
	msgIndex func(m *pb.Message) (root string, index uint32, ok bool)
	cancel   bool
	attach   bool
	// Which side is a bitmap: IDONTWANT (on by WithHaveBitmaps) and IHAVE (WithHaveAnnounceBitmaps).
	idontwantOn bool
	ihaveOn     bool
	// D1c: the IHAVE bitmap in the urgent lane, carrying the announcer's load to the peer, and
	// the asker's conversion of that load back into a delay at a nominal rate.
	announceUrgent bool
	announceLoad   bool
	askRate        int64
	askCap         time.Duration
	// holdTTL ages mine and mids: what we hold and know the id of stands in for the seen
	// cache and mcache, so it lives as long as they do, in heartbeats. pushTTL ages pushed, for
	// as long as a message stays in the gossip window. Peers' HAVEs keep IDONTWANT's TTL.
	holdTTL int
	pushTTL int
	// D3: rarest-first at dispatch, from the holder counts the HAVEs build.
	rarest  bool
	holders *holderCounts
	// D7 idea 6: per-(frame, self, peer) order at dispatch; 0 off, havePairTie, havePairFirst.
	pairOrder int
	// class, when set, is each frame's priority class at dispatch (WithFrameClass): lower first.
	class         func(m *pb.Message) int
	classGrants   bool
	classPullOnly bool
	releaseMargin int
	releaseClass  int
	// Step 4b: the projection at dispatch, installed on every peer's queue by haveQueue.
	projectNeed  uint32
	projectFanIn int
	// ledger, when kept, records per (peer, root, index) when we popped the frame to the peer
	// and when the peer's HAVE covering it reached us (WithHaveLedger; measurement only).
	ledger *haveLedger
	// originHave sends our HAVE for each group we publish in a batch (WithOriginHave).
	originHave bool
	// originPaced holds our HAVE for each group we publish to its OriginHaveMode (WithOriginHaveMode).
	originPaced *originPaced
	// decodeClears fulfils the gossip tracer's promises for a group on its decode claim (WithDecodeClearsPromises).
	decodeClears bool
	// disclose limits what each peer is told of a group until we complete it (WithLinkDisclosure).
	disclose *linkDisclosure
	// rebuilt is what Rebuilt handed the router, and its counters; nil until it first runs.
	rebuilt atomic.Pointer[rebuiltState]
}

func newHaveState(index func(mid string) (string, uint32, bool)) *haveState {
	return &haveState{
		index: index,
		mine:  make(map[string]*haveEntry),
		mids:  make(map[string]*haveIDs),
		peers: make(map[peer.ID]map[string]*haveEntry),

		pushed:      make(map[peer.ID]map[string]*haveEntry),
		idontwantOn: true,
	}
}

// WithHaveBitmaps replaces IDONTWANT with one HAVE bitmap per group for messages whose id index
// recognises: index returns the 32-byte group root and the message's position in it.
func WithHaveBitmaps(index func(mid string) (root string, index uint32, ok bool)) Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok {
			return fmt.Errorf("HAVE bitmaps require a gossipsub router")
		}
		if index == nil {
			return fmt.Errorf("HAVE bitmaps require an index function")
		}
		gs.have = newHaveState(index)
		hb := gs.params.HeartbeatInterval
		if hb <= 0 {
			hb = GossipSubHeartbeatInterval
		}
		gs.have.holdTTL = max(int(ps.seenMsgTTL/hb)+1, gs.params.HistoryLength+1)
		gs.have.pushTTL = gs.params.HistoryLength
		return nil
	}
}

// markUnwanted records that p does not want the message with checksum csum, as an IDONTWANT
// naming it would.
func (gs *GossipSubRouter) markUnwanted(p peer.ID, csum checksum) {
	u := gs.unwanted[p]
	if u == nil {
		u = make(map[checksum]int)
		gs.unwanted[p] = u
	}
	u[csum] = gs.params.IDontWantMessageTTL
}

// haveLearnID records an id at its index, held or merely announced, and expands every peer
// HAVE that already covers that index into that peer's unwanted set and holder evidence.
//
// A HAVE names an index, not an id, so until the id is known the holder it evidences cannot be
// recorded, and a node's pulls for the message fall back on whoever announced it -- measured
// as the publisher's bytes rising 39% under A tuned before announced ids were learned here.
// Router goroutine, from Preprocess (arrivals) and handleIHave (announces).
func (gs *GossipSubRouter) haveLearnID(mid string) {
	h := gs.have
	if h == nil {
		return
	}
	root, index, ok := h.index(mid)
	if !ok || len(root) != haveRootLen {
		return
	}
	m := h.mids[root]
	if m == nil {
		m = &haveIDs{ids: make(map[uint32]string)}
		h.mids[root] = m
	}
	m.ttl = h.holdTTL
	if _, known := m.ids[index]; known {
		return
	}
	m.ids[index] = mid
	csum := computeChecksum(mid)
	for p, roots := range h.peers {
		if pe := roots[root]; pe != nil && pe.bits.Has(index) {
			gs.markUnwanted(p, csum)
			if gs.groupPush != nil {
				gs.groupPush.note(p, mid)
			}
			gs.noteRarityHolder(mid, p)
		}
	}
}

// haveNoteArrival records a received (or own) message in our bitmap and learns its id.
// Router goroutine, from Preprocess -- before any forwarding decision for the message -- and
// from the publish paths.
func (gs *GossipSubRouter) haveNoteArrival(mid string) {
	h := gs.have
	if h == nil {
		return
	}
	root, index, ok := h.index(mid)
	if !ok || len(root) != haveRootLen {
		return
	}
	e := h.mine[root]
	if e == nil {
		e = &haveEntry{}
		h.mine[root] = e
	}
	e.bits.Set(index)
	e.ttl = h.holdTTL
	gs.haveLearnID(mid)
}

// haveIDontWantIDs returns mids with every structured id replaced by one HAVE per group root,
// carrying our whole bitmap for it (haveBitsFor, none while it is empty); other ids pass through
// in place. mids is not mutated.
func (gs *GossipSubRouter) haveIDontWantIDs(mids []string) []string {
	h := gs.have
	if h == nil || !h.idontwantOn {
		return mids
	}
	var out []string
	var seen map[string]bool
	for i, mid := range mids {
		root, _, ok := h.index(mid)
		if !ok || len(root) != haveRootLen || h.mine[root] == nil {
			if out != nil {
				out = append(out, mid)
			}
			continue
		}
		if out == nil {
			out = append(make([]string, 0, len(mids)), mids[:i]...)
			seen = make(map[string]bool)
		}
		if seen[root] {
			continue
		}
		seen[root] = true
		if bits := h.haveBitsFor(root); bits != nil {
			out = append(out, encodeHave(root, bits))
		}
	}
	if out == nil {
		return mids
	}
	return out
}

// haveAbsorb reads a HAVE pseudo id from p: it records the bitmap and expands the indices whose
// message we already know into p's unwanted set. Reports whether mid was a HAVE at all.
func (gs *GossipSubRouter) haveAbsorb(p peer.ID, mid string) bool {
	h := gs.have
	if h == nil {
		return false
	}
	root, enc, ok := decodeHave(mid)
	if !ok {
		return false
	}
	roots := h.peers[p]
	if roots == nil {
		roots = make(map[string]*haveEntry)
		h.peers[p] = roots
	}
	e := roots[root]
	if e == nil {
		e = &haveEntry{}
		roots[root] = e
	}
	claim := isHaveClaim(mid)
	var newly []uint32
	e.ttl = gs.params.IDontWantMessageTTL
	if claim {
		// A claim is not evidence of holding: no holder count, no ledger HAVE, and the writer
		// keeps it apart from what the peer announced (cancel yes, projection no).
		newly = e.claim.Or(enc)
		if h.cancel {
			if q := gs.haveQueue(p); q != nil {
				q.haveClaim(root, enc)
			}
		}
	} else {
		newly = e.bits.Or(enc)
		if h.holders != nil {
			h.holders.add(root, newly)
		}
		if h.ledger != nil {
			h.ledger.have(p, root, newly, time.Now())
		}
		if h.cancel {
			// The writer's copy, for frames already queued to this peer.
			if q := gs.haveQueue(p); q != nil {
				q.haveTheirsAt(root, enc, gs.hoseAckAt, gs.hoseAckIdx[root])
			}
		}
	}
	known := h.mids[root]
	for _, index := range newly {
		if known == nil {
			break
		}
		kmid, have := known.ids[index]
		if !have {
			continue
		}
		gs.markUnwanted(p, computeChecksum(kmid))
		gs.offerCovered(p, kmid)
		if gs.groupPush != nil {
			gs.groupPush.note(p, kmid)
		}
		gs.noteRarityHolder(kmid, p)
	}
	return true
}

// haveHeartbeat ages the bitmaps out on the IDONTWANT TTL. With clearIDontWantCounters.
func (gs *GossipSubRouter) haveHeartbeat() {
	h := gs.have
	if h == nil {
		return
	}
	for root, e := range h.mine {
		if e.ttl--; e.ttl <= 0 {
			delete(h.mine, root)
			if h.holders != nil {
				h.holders.forget(root)
			}
			if h.originPaced != nil {
				h.originPaced.forget(root)
			}
		}
	}
	for root, m := range h.mids {
		if m.ttl--; m.ttl <= 0 {
			delete(h.mids, root)
		}
	}
	for p, roots := range h.pushed {
		for root, e := range roots {
			if e.ttl--; e.ttl <= 0 {
				delete(roots, root)
			}
		}
		if len(roots) == 0 {
			delete(h.pushed, p)
		}
	}
	for p, roots := range h.peers {
		for root, e := range roots {
			if e.ttl--; e.ttl <= 0 {
				delete(roots, root)
				if gs.p != nil {
					if q, ok := gs.p.peers[p]; ok {
						q.haveForgetRoot(root)
					}
				}
			}
		}
		if len(roots) == 0 {
			delete(h.peers, p)
		}
	}
	gs.underReportHeartbeat()
	gs.discloseHeartbeat()
}

// haveForget drops what a departed peer told us, and what we pushed to it.
func (gs *GossipSubRouter) haveForget(p peer.ID) {
	if gs.have != nil {
		delete(gs.have.peers, p)
		delete(gs.have.pushed, p)
		if gs.have.disclose != nil {
			delete(gs.have.disclose.links, p)
		}
	}
	if gs.under != nil {
		delete(gs.under.from, p)
	}
}

// --- D2: dispatch-time cancel and attach, on the per-peer queue ---
//
// The router decides what to send when it enqueues; the writer sends it later, and between the
// two a HAVE can arrive that makes the frame a duplicate. The per-peer rpcQueue is the one object
// the router and the writer already share under one mutex, so the peer's HAVE state lives there
// and the writer reads it at Pop: a queued frame the peer now holds is dropped instead of
// written, and a HAVE we have not yet told the peer rides whatever RPC leaves next -- an urgent
// control RPC, or the data frame about to be written, so it is fresh at the instant it leaves
// rather than stale by the depth of the queue (Q110 finding 1: behind data it is late).

// queueHave is one peer's HAVE state as the writer sees it. Every field is under queueMu.
type queueHave struct {
	// index maps a queued message to its group root and index; ok is false for other ids.
	index func(msg *pb.Message) (root string, index uint32, ok bool)
	// theirs is what the peer has told us it holds, per root; claim is what it asked us to stop
	// sending without holding it (D7 idea 2) — cancelled at pop like theirs, but not counted by
	// the projection, and a later ask by index revokes it.
	theirs map[string]*haveBits
	claim  map[string]*haveBits
	// pending is what we have not told the peer yet, per root, to attach to the next RPC out.
	pending  map[string]*haveBits
	cancelOn bool
	attachOn bool
	// Projection at dispatch (WithHaveDispatchProjection): popped is what
	// the writer has sent the peer per root; a frame is dropped at pop when what the peer said it
	// holds plus projectFanIn times what we sent it since would already complete it.
	projectNeed  uint32
	projectFanIn int
	popped       map[string]*haveBits
	// holders, when set, ranks queued data at Pop: fewest announced holders first.
	holders func(m *pb.Message) (int, bool)
	// pairKey, when set, orders queued data at Pop by a hash of (frame, self, peer), so two
	// pushers feeding the same receiver send in independent orders: the
	// tie-break within equal holder counts, or with pairFirst the primary key.
	pairKey   func(m *pb.Message) (uint64, bool)
	pairFirst bool
	// class, when set, leads the order at Pop: a lower class pops before any higher one.
	class func(m *pb.Message) int
	// classGrants makes the writer wait for a hose slot with its best queued frame's class;
	// classPullOnly applies the class to pull answers alone.
	classGrants   bool
	classPullOnly bool
	// releaseMargin > 0 demotes a frame to releaseClass once the projection takes the peer to
	// within that many frames of completing its group (WithFrameClassRelease).
	releaseMargin int
	releaseClass  int
	// onDispatch, when set, sees every data frame the writer pops for this peer (the ledger).
	onDispatch func(m *pb.Message)
	// onOriginPop, when set, sees them too, for the paced origin HAVE (WithOriginHavePaced).
	onOriginPop func(m *pb.Message)
	// originSent, when set, names the frames of groups we publish (OriginHaveSent): each pop of
	// one to a mesh peer of its topic (originMesh, as the router last saw it) carries our HAVE
	// naming shared[root], the frames of the group this peer and we exchanged.
	originSent func(m *pb.Message) (root string, index uint32, ok bool)
	shared     map[string]*haveBits
	originMesh map[string]bool
}

// WithHaveDispatch moves two decisions to dispatch time, per peer: cancel drops a queued frame
// the peer has since announced, attach carries our pending HAVE on the next RPC out instead of
// sending it alone when something is queued. Requires WithHaveBitmaps.
func WithHaveDispatch(cancel, attach bool) Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok {
			return fmt.Errorf("HAVE dispatch requires a gossipsub router")
		}
		if gs.have == nil {
			return fmt.Errorf("HAVE dispatch requires WithHaveBitmaps first")
		}
		h := gs.have
		h.cancel, h.attach = cancel, attach
		h.msgIndex = func(m *pb.Message) (string, uint32, bool) { return h.index(ps.idGen.RawID(m)) }
		return nil
	}
}

// haveQueue returns p's queue with HAVE dispatch enabled on it, or nil when p has no queue or
// dispatch is off. Router goroutine.
func (gs *GossipSubRouter) haveQueue(p peer.ID) *rpcQueue {
	h := gs.have
	if h == nil || (!h.cancel && !h.attach && h.projectNeed == 0 && h.originPaced == nil) || gs.p == nil {
		return nil
	}
	q, ok := gs.p.peers[p]
	if !ok {
		return nil
	}
	q.enableHave(h.msgIndex, h.cancel, h.attach)
	if h.rarest {
		q.enableRarest(h.haveHolders)
	}
	if h.class != nil {
		q.enableClass(h.class, h.classGrants, h.releaseMargin, h.releaseClass)
		q.queueMu.Lock()
		if q.have != nil {
			q.have.classPullOnly = h.classPullOnly
		}
		q.queueMu.Unlock()
	}
	if h.pairOrder != 0 {
		q.enablePairOrder(h.havePairKey(gs.p.host.ID(), p), h.pairOrder == havePairFirst || h.pairOrder == haveNodeFirst)
	}
	if h.projectNeed > 0 {
		q.enableProjection(h.projectNeed, h.projectFanIn)
	}
	if h.ledger != nil {
		led, index := h.ledger, h.msgIndex
		q.enableDispatchHook(func(m *pb.Message) {
			if root, i, ok := index(m); ok {
				led.pop(p, root, i, sha256.Sum256(m.GetData()), time.Now())
			}
		})
	}
	if o := h.originPaced; o != nil {
		switch o.mode {
		case OriginHavePaced:
			q.enableOriginPop(gs.haveOriginPopped)
		case OriginHaveSent:
			q.enableOriginSent(h.originOwn)
		}
	}
	return q
}

// havePendAll parks our bitmaps for the roots in ids -- all HAVE pseudo ids, or nothing is
// parked -- on p's queue, to ride the next RPC out. Reports false when they must go alone: the
// queue is empty, dispatch is off, or an id is an ordinary one.
func (gs *GossipSubRouter) havePendAll(p peer.ID, ids []string) bool {
	h := gs.have
	if h == nil || !h.attach || gs.controlMute {
		return false // a mute node's HAVE goes to sendRPC, where the mute strips it
	}
	roots := make([]string, 0, len(ids))
	bits := make([]*haveBits, 0, len(ids))
	cut := 0
	for _, id := range ids {
		root, _, ok := decodeHave(id)
		if !ok {
			return false
		}
		b := h.haveBitsFor(root)
		if b == nil {
			return false
		}
		if gs.under != nil {
			var n int
			b, n = gs.underReportBits(p, root, b)
			cut += n
		}
		if h.disclose != nil {
			if b = gs.discloseBits(p, root, b); b.Count() == 0 {
				return false // nothing p may be told: the lone send drops it
			}
		}
		roots = append(roots, root)
		bits = append(bits, b)
	}
	q := gs.haveQueue(p)
	if q == nil || !q.havePend(roots, bits) {
		return false
	}
	gs.underReportCount(cut)
	return true
}

// enableHave installs the writer-side HAVE state once.
func (q *rpcQueue) enableHave(index func(*pb.Message) (string, uint32, bool), cancel, attach bool) {
	q.queueMu.Lock()
	defer q.queueMu.Unlock()
	if q.have == nil {
		q.have = &queueHave{index: index, theirs: make(map[string]*haveBits), pending: make(map[string]*haveBits), popped: make(map[string]*haveBits), cancelOn: cancel, attachOn: attach}
	}
}

// enableProjection installs the dispatch-time projection once (WithHaveDispatchProjection).
func (q *rpcQueue) enableProjection(need uint32, fanIn int) {
	q.queueMu.Lock()
	defer q.queueMu.Unlock()
	if q.have != nil && q.have.projectNeed == 0 {
		q.have.projectNeed, q.have.projectFanIn = need, fanIn
	}
}

// WithHaveDispatchProjection drops a queued frame at the writer's pop when the peer is
// projected to complete its group without it: the indices the peer has announced, plus fanIn
// times the frames this node has popped to it beyond those (the other fanIn-1 pushers taken to
// have delivered as much unacknowledged as this one), reach need. What is dropped was
// forwarded, so the peer was not announced it by this node; a peer whose projection was
// optimistic pulls the rest by index from the peers that announced. Requires WithHaveBitmaps.
func WithHaveDispatchProjection(need uint32, fanIn int) Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok || gs.have == nil {
			return fmt.Errorf("dispatch projection requires WithHaveBitmaps first")
		}
		if need == 0 || fanIn < 1 {
			return fmt.Errorf("dispatch projection requires need > 0 and fan-in >= 1")
		}
		h := gs.have
		h.projectNeed, h.projectFanIn = need, fanIn
		if h.msgIndex == nil {
			h.msgIndex = func(m *pb.Message) (string, uint32, bool) { return h.index(ps.idGen.RawID(m)) }
		}
		return nil
	}
}

// enableRarest installs the writer's rarity signal once.
func (q *rpcQueue) enableRarest(holders func(*pb.Message) (int, bool)) {
	q.queueMu.Lock()
	defer q.queueMu.Unlock()
	if q.have != nil && q.have.holders == nil {
		q.have.holders = holders
	}
}

// enablePairOrder installs the writer's per-(frame, self, peer) order key once.
func (q *rpcQueue) enablePairOrder(key func(*pb.Message) (uint64, bool), first bool) {
	q.queueMu.Lock()
	defer q.queueMu.Unlock()
	if q.have != nil && q.have.pairKey == nil {
		q.have.pairKey = key
		q.have.pairFirst = first
	}
}

// enableClass installs the writer's priority class once.
func (q *rpcQueue) enableClass(class func(*pb.Message) int, grants bool, releaseMargin, releaseClass int) {
	q.queueMu.Lock()
	defer q.queueMu.Unlock()
	if q.have != nil && q.have.class == nil {
		q.have.class, q.have.classGrants = class, grants
		q.have.releaseMargin, q.have.releaseClass = releaseMargin, releaseClass
	}
}

// grantClassLocked is the class the writer waits for a hose slot with: its best queued data
// frame's, ahead of the next control RPC; 0 without class grants. A lower-class frame queued
// while the writer waits does not re-rank it. Under queueMu.
func (q *rpcQueue) grantClassLocked() int {
	if q.have == nil || !q.have.classGrants {
		return 0
	}
	best, found := 0, false
	for _, e := range q.queue.normal {
		if len(e.rpc.Publish) == 0 {
			break
		}
		if c := q.have.frameClass(e.rpc); !found || c < best {
			best, found = c, true
		}
	}
	return best
}

// enableDispatchHook installs the writer's dispatch observer once.
func (q *rpcQueue) enableDispatchHook(fn func(*pb.Message)) {
	q.queueMu.Lock()
	defer q.queueMu.Unlock()
	if q.have != nil && q.have.onDispatch == nil {
		q.have.onDispatch = fn
	}
}

// enableOriginPop installs the paced origin HAVE's pop observer once.
func (q *rpcQueue) enableOriginPop(fn func(*pb.Message)) {
	q.queueMu.Lock()
	defer q.queueMu.Unlock()
	if q.have != nil && q.have.onOriginPop == nil {
		q.have.onOriginPop = fn
	}
}

// enableOriginSent installs OriginHaveSent's frame test once.
func (q *rpcQueue) enableOriginSent(own func(*pb.Message) (string, uint32, bool)) {
	q.queueMu.Lock()
	defer q.queueMu.Unlock()
	if q.have != nil && q.have.originSent == nil {
		q.have.originSent = own
		q.have.shared = make(map[string]*haveBits)
		q.have.originMesh = make(map[string]bool)
	}
}

// haveOriginMesh records, for OriginHaveSent, whether p is in the mesh of the topics out sends
// frames of, as it enqueues: a HAVE to a peer outside the mesh, such as an asker we answer, is
// one no relay sends. Router goroutine, from sendRPC.
func (gs *GossipSubRouter) haveOriginMesh(p peer.ID, q *rpcQueue, out *RPC) {
	q.queueMu.Lock()
	defer q.queueMu.Unlock()
	if q.have == nil || q.have.originMesh == nil {
		return
	}
	for _, m := range out.Publish {
		_, in := gs.mesh[m.GetTopic()][p]
		q.have.originMesh[m.GetTopic()] = in
	}
}

// shareLocked adds index to what this peer and we exchanged of root and returns our HAVE for it.
// Under queueMu.
func (h *queueHave) shareLocked(root string, index uint32) string {
	b := h.shared[root]
	if b == nil {
		b = &haveBits{}
		h.shared[root] = b
	}
	b.Set(index)
	return encodeHave(root, b)
}

// attachSent returns rpc carrying our HAVE for each group we publish that it sends frames of to
// a mesh peer, naming the frames sent this peer so far; nil when it sends none (OriginHaveSent).
// Under queueMu.
func (h *queueHave) attachSent(rpc *RPC) *RPC {
	if h.originSent == nil || len(rpc.Publish) == 0 {
		return nil
	}
	var ids []string
	idx := make(map[string]int)
	for _, m := range rpc.Publish {
		if !h.originMesh[m.GetTopic()] {
			continue
		}
		root, index, ok := h.originSent(m)
		if !ok {
			continue
		}
		id := h.shareLocked(root, index)
		if at, seen := idx[root]; seen {
			ids[at] = id
			continue
		}
		idx[root] = len(ids)
		ids = append(ids, id)
	}
	if len(ids) == 0 {
		return nil
	}
	out := mergedControl(rpc, &RPC{RPC: pb.RPC{Control: &pb.ControlMessage{Idontwant: []*pb.ControlIDontWant{{MessageIDs: ids}}}}})
	out.solicited = rpc.solicited
	return out
}

// haveOriginSentAck is, under OriginHaveSent, our HAVE for a frame of a group we publish that p
// pushed back to us: the frame joins what p and we exchanged, as a relay's acknowledgement names
// what it received. Empty otherwise. Router goroutine.
func (gs *GossipSubRouter) haveOriginSentAck(p peer.ID, mid string) []string {
	h := gs.have
	if h == nil || h.originPaced == nil || h.originPaced.mode != OriginHaveSent {
		return nil
	}
	root, index, ok := h.index(mid)
	if !ok {
		return nil
	}
	o := h.originPaced
	o.mu.Lock()
	own := o.roots[root] != nil
	o.mu.Unlock()
	if !own {
		return nil
	}
	q := gs.haveQueue(p)
	if q == nil {
		return nil
	}
	q.queueMu.Lock()
	defer q.queueMu.Unlock()
	if q.have == nil || q.have.shared == nil {
		return nil
	}
	return []string{q.have.shareLocked(root, index)}
}

// rarestIndex picks, among the data RPCs ahead of the next control-only RPC in the normal lane,
// the one whose messages the fewest peers have announced; -1 when the choice is the head (FIFO,
// or nothing to choose among). Under queueMu.
func (h *queueHave) rarestIndex(normal []rankedRPC) int {
	if h.holders == nil || len(normal) < 2 || len(normal[0].rpc.Publish) == 0 {
		return -1
	}
	best, bestHolders, bestKey, bestClass := -1, 0, uint64(0), 0
	for i := range normal {
		rpc := normal[i].rpc
		if len(rpc.Publish) == 0 {
			break // control keeps its place: nothing behind it may overtake it
		}
		holders, known := 0, false
		for _, m := range rpc.Publish {
			c, ok := h.holders(m)
			if !ok {
				continue
			}
			if !known || c < holders {
				holders, known = c, true
			}
		}
		if !known {
			continue
		}
		cls := 0
		if h.class != nil {
			cls = h.frameClass(rpc)
			if best >= 0 && cls != bestClass {
				if cls < bestClass {
					best, bestHolders, bestKey, bestClass = i, holders, 0, cls
					if h.pairKey != nil {
						bestKey, _ = h.pairKey(rpc.Publish[0])
					}
				}
				continue
			}
		}
		key, keyed := uint64(0), false
		if h.pairKey != nil {
			key, keyed = h.pairKey(rpc.Publish[0])
		}
		if best < 0 || h.popsBefore(holders, key, keyed, bestHolders, bestKey) {
			best, bestHolders, bestKey, bestClass = i, holders, key, cls
		}
	}
	if best <= 0 {
		return -1
	}
	return best
}

// popsBefore says whether a frame with (holders, key) is popped ahead of the current best.
// Without a pair key it is the D3 rule: strictly fewer holders, FIFO within a tie. With one,
// the key breaks the tie, or (pairFirst) leads and the holder count breaks its ties.
func (h *queueHave) popsBefore(holders int, key uint64, keyed bool, bestHolders int, bestKey uint64) bool {
	if !keyed {
		return holders < bestHolders
	}
	if h.pairFirst {
		return key < bestKey || (key == bestKey && holders < bestHolders)
	}
	return holders < bestHolders || (holders == bestHolders && key < bestKey)
}

// haveRevoke clears one index of what the peer claimed: it asked for that frame by index, so
// whatever bitmap said it held the frame is superseded for it. Router goroutine.
func (q *rpcQueue) haveRevoke(root string, index uint32) {
	q.queueMu.Lock()
	defer q.queueMu.Unlock()
	if q.have == nil {
		return
	}
	if b := q.have.theirs[root]; b != nil {
		b.Clear(index)
	}
	if c := q.have.claim[root]; c != nil {
		c.Clear(index)
	}
}

// haveClaim folds the peer's claim bitmap in. Router goroutine.
func (q *rpcQueue) haveClaim(root string, enc []byte) {
	q.queueMu.Lock()
	defer q.queueMu.Unlock()
	if q.have == nil {
		return
	}
	if q.have.claim == nil {
		q.have.claim = make(map[string]*haveBits)
	}
	c := q.have.claim[root]
	if c == nil {
		c = &haveBits{}
		q.have.claim[root] = c
	}
	c.Or(enc)
}

// haveTheirs folds the peer's announced bitmap in. Router goroutine.
func (q *rpcQueue) haveTheirs(root string, enc []byte) { q.haveTheirsAt(root, enc, time.Time{}, nil) }

// haveTheirsAt is haveTheirs with the acknowledgement's receive stamp and the indices it covers,
// zero and nil when it had none.
func (q *rpcQueue) haveTheirsAt(root string, enc []byte, stamp time.Time, stamped map[uint32]bool) {
	q.queueMu.Lock()
	defer q.queueMu.Unlock()
	if q.have == nil {
		return
	}
	b := q.have.theirs[root]
	if b == nil {
		b = &haveBits{}
		q.have.theirs[root] = b
	}
	q.hoseAckedLocked(root, b.Or(enc), stamp, stamped)
}

// haveForgetRoot drops an aged-out root on both sides.
func (q *rpcQueue) haveForgetRoot(root string) {
	q.queueMu.Lock()
	defer q.queueMu.Unlock()
	if q.have != nil {
		delete(q.have.theirs, root)
		delete(q.have.pending, root)
		delete(q.have.popped, root)
		delete(q.have.claim, root)
		delete(q.have.shared, root)
	}
}

// havePend parks bitmaps to ride the next RPC out, all or none, under the one lock that also
// guards the check that something is queued to carry them.
func (q *rpcQueue) havePend(roots []string, bits []*haveBits) bool {
	q.queueMu.Lock()
	defer q.queueMu.Unlock()
	if q.have == nil || !q.have.attachOn || q.queue.Len() == 0 {
		return false
	}
	for i, root := range roots {
		q.have.pending[root] = &haveBits{b: append([]byte(nil), bits[i].Bytes()...)}
	}
	return true
}

// filter returns rpc without the messages the peer has announced it holds, and how many it
// dropped; nil when nothing worth writing is left. Never mutates rpc: a data RPC is one shared
// object across every recipient.
func (h *queueHave) filter(rpc *RPC) (out *RPC, cancelled, projected int) {
	if len(rpc.Publish) == 0 || (!h.cancelOn && h.projectNeed == 0) {
		return rpc, 0, 0
	}
	// A solicited frame (an IWANT answer) is cancelled like any other when the peer has since
	// announced it — the answer is stale — but never projected away: the ask is the peer's own
	// word that it lacks the frame, whatever the projection computes. Projecting answers made a
	// peer re-ask for what it had asked for and never got (Q131). After a claim (D7 idea 2) the
	// ask also revoked the claimed bit (haveRevoke), so the claim cannot cancel the answer
	// while a real later HAVE still does.
	drop := func(m *pb.Message) bool {
		if h.cancelOn && h.held(m) {
			return true
		}
		if rpc.solicited {
			return false
		}
		return h.projectedComplete(m)
	}
	for _, m := range rpc.Publish {
		if h.cancelOn && h.held(m) {
			cancelled++
		} else if drop(m) {
			projected++
		}
	}
	if cancelled+projected == 0 {
		return rpc, 0, 0
	}
	kept := make([]*pb.Message, 0, len(rpc.Publish)-cancelled-projected)
	for _, m := range rpc.Publish {
		if drop(m) {
			continue
		}
		kept = append(kept, m)
	}
	if len(kept) == 0 && rpcOtherwiseEmpty(rpc) {
		return nil, cancelled, projected
	}
	out = shallowRPCCopy(rpc)
	out.Publish = kept
	return out, cancelled, projected
}

// held reports whether the peer has announced the message's index.
func (h *queueHave) held(m *pb.Message) bool {
	root, index, ok := h.index(m)
	if !ok {
		return false
	}
	if b := h.theirs[root]; b != nil && b.Has(index) {
		return true
	}
	c := h.claim[root]
	return c != nil && c.Has(index)
}

// projectedComplete reports whether the peer is projected to complete m's group without m:
// what it said it holds plus fan-in times what we have sent it beyond that reaches need.
func (h *queueHave) projectedComplete(m *pb.Message) bool {
	if h.projectNeed == 0 {
		return false
	}
	n, ok := h.projectedCount(m)
	return ok && uint32(n) >= h.projectNeed // lint:ignore uintcast -- a popcount over at most haveMaxBytes*8 bits.
}

// projectedCount is what the projection takes the peer to hold of m's group: what it announced,
// plus projectFanIn times what we popped to it beyond that.
func (h *queueHave) projectedCount(m *pb.Message) (int, bool) {
	root, _, ok := h.index(m)
	if !ok {
		return 0, false
	}
	theirs, ours := h.theirs[root], h.popped[root]
	n := 0
	if theirs != nil {
		n = theirs.Count()
	}
	if ours != nil {
		unacked := ours
		if theirs != nil {
			unacked = ours.AndNot(theirs)
		}
		n += h.projectFanIn * unacked.Count()
	}
	return n, true
}

// frameClass is rpc's priority class toward this peer, read off its first message: the class,
// or releaseClass once the projection takes the peer to within releaseMargin frames of completing
// the group; 0 for a push under classPullOnly.
func (h *queueHave) frameClass(rpc *RPC) int {
	if h.classPullOnly && !rpc.solicited {
		return 0
	}
	m := rpc.Publish[0]
	c := h.class(m)
	if h.releaseMargin > 0 && c < h.releaseClass {
		if n, ok := h.projectedCount(m); ok && uint32(n+h.releaseMargin) >= h.projectNeed { // lint:ignore uintcast -- a popcount plus a small margin.
			return h.releaseClass
		}
	}
	return c
}

// notePopped records that the writer sent m to the peer, for the projection.
func (h *queueHave) notePopped(m *pb.Message) {
	if h.projectNeed == 0 {
		return
	}
	root, index, ok := h.index(m)
	if !ok {
		return
	}
	b := h.popped[root]
	if b == nil {
		b = &haveBits{}
		h.popped[root] = b
	}
	b.Set(index)
}

// rpcOtherwiseEmpty reports whether rpc carries nothing but its published messages.
func rpcOtherwiseEmpty(rpc *RPC) bool {
	c := rpc.Control
	return len(rpc.Subscriptions) == 0 && rpc.Partial == nil && rpc.TestExtension == nil &&
		(c == nil || (len(c.Ihave) == 0 && len(c.Iwant) == 0 && len(c.Graft) == 0 && len(c.Prune) == 0 && len(c.Idontwant) == 0))
}

// attach returns rpc carrying the pending HAVEs as well, and clears them; nil when none wait.
func (h *queueHave) attach(rpc *RPC) *RPC {
	if !h.attachOn || len(h.pending) == 0 {
		return nil
	}
	roots := make([]string, 0, len(h.pending))
	for root := range h.pending {
		roots = append(roots, root)
	}
	sort.Strings(roots)
	ids := make([]string, 0, len(roots))
	for _, root := range roots {
		ids = append(ids, encodeHave(root, h.pending[root]))
	}
	clear(h.pending)
	return mergedControl(rpc, &RPC{RPC: pb.RPC{Control: &pb.ControlMessage{Idontwant: []*pb.ControlIDontWant{{MessageIDs: ids}}}}})
}

// --- D1b: the IHAVE bitmap ---
//
// IDONTWANT and IHAVE both say "I have X" and mean different things, so each has its own bitmap
// and each bitmap inherits its message's semantics, emission point and lane. The IHAVE bitmap's
// bit is true once the message is validated and in mcache -- servable -- and it is emitted where
// IHAVE is: the phase announce and the heartbeat gossip, in the normal lane (because
// its FIFO wait behind our data is the scheme's only per-link load signal). A receiver may ask
// for an index it learns of this way; when it does not yet know the id, it asks by index with a
// WANT pseudo id, and the holder resolves that through its ids map.

const (
	wantMagic = "WANT"
	wantLen   = len(wantMagic) + 1 + haveRootLen + 4
)

// encodeWant is the pseudo id of an ask by index: magic ‖ version ‖ root ‖ index, big-endian.
func encodeWant(root string, index uint32) string {
	out := make([]byte, 0, wantLen)
	out = append(out, wantMagic...)
	out = append(out, haveVersion)
	out = append(out, root...)
	out = append(out, byte(index>>24), byte(index>>16), byte(index>>8), byte(index))
	return string(out)
}

// decodeWant reads an ask by index, or reports that mid is an ordinary id.
func decodeWant(mid string) (root string, index uint32, ok bool) {
	if len(mid) != wantLen || mid[:len(wantMagic)] != wantMagic || mid[len(wantMagic)] != haveVersion {
		return "", 0, false
	}
	root = mid[len(wantMagic)+1 : len(wantMagic)+1+haveRootLen]
	b := []byte(mid[wantLen-4:])
	return root, uint32(b[0])<<24 | uint32(b[1])<<16 | uint32(b[2])<<8 | uint32(b[3]), true
}

// WithHaveAnnounceBitmaps turns the IHAVE side on: one servable bitmap per group where IHAVE
// would name ids, and asks by index for indices whose id is not yet known. Requires
// WithHaveBitmaps.
func WithHaveAnnounceBitmaps() Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok {
			return fmt.Errorf("HAVE announce bitmaps require a gossipsub router")
		}
		if gs.have == nil {
			return fmt.Errorf("HAVE announce bitmaps require WithHaveBitmaps first")
		}
		gs.have.ihaveOn = true
		return nil
	}
}

// WithHaveIDontWantPerID keeps IDONTWANT as one id per message while the rest of the HAVE state
// (the ids map, and the IHAVE side if on) stays in place: the pairing that prices the two
// bitmaps apart. Requires WithHaveBitmaps.
func WithHaveIDontWantPerID() Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok {
			return fmt.Errorf("per-id IDONTWANT requires a gossipsub router")
		}
		if gs.have == nil {
			return fmt.Errorf("per-id IDONTWANT requires WithHaveBitmaps first")
		}
		gs.have.idontwantOn = false
		return nil
	}
}

// WithOriginHave makes the publisher send its HAVE for each group it batch-publishes, as a receiver's goes. Requires WithHaveBitmaps.
func WithOriginHave() Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok {
			return fmt.Errorf("origin HAVE requires a gossipsub router")
		}
		if gs.have == nil {
			return fmt.Errorf("origin HAVE requires WithHaveBitmaps first")
		}
		if gs.have.originPaced != nil {
			return fmt.Errorf("origin HAVE and an origin HAVE mode are exclusive")
		}
		gs.have.originHave = true
		return nil
	}
}

// OriginHaveMode is what our HAVE says of a group we publish, in place of every frame at publish,
// which marks the source to its mesh before any frame has left (privacy card P4 and its variants).
type OriginHaveMode int

const (
	// OriginHavePaced grows the HAVE as our writers pop the group's frames, as a relay's grows as
	// frames arrive (WithOriginHavePaced).
	OriginHavePaced OriginHaveMode = iota + 1
	// OriginHaveNone sends no HAVE for the group at all, acknowledgements of pushed-back frames
	// included: mesh peers learn what we hold only from our pushes and announces.
	OriginHaveNone
	// OriginHaveSent tells each peer, on the RPC carrying each frame we send it, a HAVE naming the
	// frames we have sent it and it has pushed back: a relay that holds exactly what it shared.
	OriginHaveSent
)

// WithOriginHavePaced makes our HAVE for a group we publish grow as our writers pop its frames, as
// a relay's grows as frames arrive, instead of naming every frame at publish: a HAVE that full
// before any frame has left marks the source to its mesh (privacy card P4). Each pop that adds a
// frame tells the topic's mesh the frames popped so far, through the path a receiver's HAVE takes,
// and a peer we push a frame gets our HAVE naming it just ahead of the first one; until then,
// publishing sends none. Requires WithHaveBitmaps; exclusive with WithOriginHave.
func WithOriginHavePaced() Option { return WithOriginHaveMode(OriginHavePaced) }

// WithOriginHaveMode sets what our HAVE says of a group we publish (OriginHaveMode). Requires
// WithHaveBitmaps; exclusive with WithOriginHave.
func WithOriginHaveMode(mode OriginHaveMode) Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok {
			return fmt.Errorf("origin HAVE mode requires a gossipsub router")
		}
		if gs.have == nil {
			return fmt.Errorf("origin HAVE mode requires WithHaveBitmaps first")
		}
		if mode < OriginHavePaced || mode > OriginHaveSent {
			return fmt.Errorf("unknown origin HAVE mode %d", mode)
		}
		h := gs.have
		if h.originHave {
			return fmt.Errorf("origin HAVE and an origin HAVE mode are exclusive")
		}
		h.originPaced = &originPaced{mode: mode, roots: make(map[string]*originPacedRoot)}
		if h.msgIndex == nil {
			h.msgIndex = func(m *pb.Message) (string, uint32, bool) { return h.index(ps.idGen.RawID(m)) }
		}
		return nil
	}
}

// originPaced is WithOriginHaveMode's state, shared by the router and the writers under mu: per
// group we publish, the frames our writers have popped and those the mesh has been told of.
type originPaced struct {
	mode  OriginHaveMode
	mu    sync.Mutex
	roots map[string]*originPacedRoot
	// queued says a tell is on its way to the router; pops meanwhile ride it.
	queued bool
}

// originOwn reports the root and index of m when it is a frame of a group we publish.
func (h *haveState) originOwn(m *pb.Message) (string, uint32, bool) {
	o := h.originPaced
	if !o.publishing() {
		return "", 0, false // a relay: no id to compute
	}
	root, index, ok := h.msgIndex(m)
	if !ok {
		return "", 0, false
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	return root, index, o.roots[root] != nil
}

type originPacedRoot struct {
	topic  string
	popped haveBits
	told   haveBits
	// led is the peers we have pushed a frame of the group to, each told our HAVE ahead of it.
	led map[peer.ID]bool
}

// publishing reports whether we have published a group whose HAVE follows the mode.
func (o *originPaced) publishing() bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	return len(o.roots) > 0
}

// haveOriginOwn records mid, a message we publish on topic, as one whose group's HAVE follows the mode;
// per-id IDONTWANT (WithHaveIDontWantPerID) has no bitmap to pace. Router goroutine, from Preprocess.
func (gs *GossipSubRouter) haveOriginOwn(mid, topic string) {
	h := gs.have
	if h == nil || h.originPaced == nil || !h.idontwantOn {
		return
	}
	root, _, ok := h.index(mid)
	if !ok || len(root) != haveRootLen {
		return
	}
	o := h.originPaced
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.roots[root] == nil {
		o.roots[root] = &originPacedRoot{topic: topic}
		gs.discloseOriginStart(topic, root)
	}
}

// haveBitsFor is the bitmap our HAVE carries for root: what we hold, or for a group we publish
// a copy of what our writers have popped under OriginHavePaced, until link disclosure counts it
// complete, and nothing under the per-peer or silent modes; nil when there is nothing.
func (h *haveState) haveBitsFor(root string) *haveBits {
	if o := h.originPaced; o != nil && (h.disclose == nil || !h.disclose.isDone(root)) {
		o.mu.Lock()
		r := o.roots[root]
		var popped []byte
		if r != nil && o.mode == OriginHavePaced {
			popped = append(popped, r.popped.Bytes()...)
		}
		o.mu.Unlock()
		if r != nil {
			if len(popped) == 0 {
				return nil
			}
			return &haveBits{b: popped}
		}
	}
	if e := h.mine[root]; e != nil {
		return &e.bits
	}
	return nil
}

// haveOriginPopped notes that a writer popped m: a frame of a group we publish joins our HAVE,
// and the first such pop since the last tell queues one on the router. Writer goroutine.
func (gs *GossipSubRouter) haveOriginPopped(m *pb.Message) {
	h := gs.have
	o := h.originPaced
	root, index, ok := h.originOwn(m)
	if !ok {
		return
	}
	o.mu.Lock()
	r := o.roots[root]
	if r == nil || r.popped.Has(index) {
		o.mu.Unlock()
		return
	}
	r.popped.Set(index)
	wake := !o.queued
	o.queued = true
	o.mu.Unlock()
	if wake {
		// Never block a writer on the router, which may be waiting on this queue's lock.
		go func() {
			select {
			case gs.p.eval <- gs.haveOriginTell:
			case <-gs.p.ctx.Done():
			}
		}()
	}
}

// haveOriginOffered notes, under a source push quota, that we offered mid, a frame of a group we
// publish: it joins our paced HAVE as a popped frame does, since a relay only offers what it holds
// and names it to its whole mesh. Router goroutine, from the offer paths.
func (gs *GossipSubRouter) haveOriginOffered(mid string) {
	h := gs.have
	if h == nil || h.originPaced == nil || h.originPaced.mode != OriginHavePaced || gs.phaseForward == nil || gs.phaseForward.srcPushQuota == 0 {
		return
	}
	root, index, ok := h.index(mid)
	if !ok {
		return
	}
	o := h.originPaced
	o.mu.Lock()
	r := o.roots[root]
	if r == nil || r.popped.Has(index) {
		o.mu.Unlock()
		return
	}
	r.popped.Set(index)
	wake := !o.queued
	o.queued = true
	o.mu.Unlock()
	if wake {
		go func() {
			select {
			case gs.p.eval <- gs.haveOriginTell:
			case <-gs.p.ctx.Done():
			}
		}()
	}
}

// haveOriginTell sends the mesh our HAVE for each group we publish whose writers popped frames
// since the last tell, naming the new frames' ids as Preprocess names a receiver's. Router goroutine.
func (gs *GossipSubRouter) haveOriginTell() {
	h := gs.have
	o := h.originPaced
	type tell struct {
		topic, root string
		mids        []string
	}
	var tells []tell
	o.mu.Lock()
	o.queued = false
	roots := make([]string, 0, len(o.roots))
	for root := range o.roots {
		roots = append(roots, root)
	}
	sort.Strings(roots)
	for _, root := range roots {
		r := o.roots[root]
		fresh := r.popped.AndNot(&r.told)
		if len(fresh.Bytes()) == 0 {
			continue
		}
		r.told.Or(fresh.Bytes())
		known := h.mids[root]
		if known == nil {
			continue
		}
		var mids []string
		for i := uint32(0); i < uint32(len(fresh.b))*8; i++ {
			if mid, ok := known.ids[i]; ok && fresh.Has(i) {
				mids = append(mids, mid)
			}
		}
		if len(mids) > 0 {
			tells = append(tells, tell{topic: r.topic, root: root, mids: mids})
		}
	}
	o.mu.Unlock()
	for _, t := range tells {
		if h.disclose != nil {
			if gs.discloseTells(t.root) {
				gs.idontwantMesh(t.topic, t.mids, "")
			}
			gs.discloseAnnounce(t.mids)
			continue
		}
		gs.idontwantMesh(t.topic, t.mids, "")
	}
}

// haveOriginLead sends p, ahead of the first frame of a group we publish that rpc pushes it, our
// HAVE naming what our writers have popped and that frame, alone in the urgent lane: a relay's
// HAVE reaches its push target before the frame does, and a source whose first frame led its
// first HAVE would stand out. Router goroutine, from the publish paths.
func (gs *GossipSubRouter) haveOriginLead(p peer.ID, rpc *RPC) {
	h := gs.have
	if h == nil || h.originPaced == nil || h.originPaced.mode != OriginHavePaced || len(rpc.Publish) == 0 || !h.originPaced.publishing() {
		return
	}
	o := h.originPaced
	for _, m := range rpc.Publish {
		root, index, ok := h.msgIndex(m)
		if !ok {
			continue
		}
		o.mu.Lock()
		r := o.roots[root]
		if r == nil || r.led[p] {
			o.mu.Unlock()
			continue
		}
		if r.led == nil {
			r.led = make(map[peer.ID]bool)
		}
		r.led[p] = true
		bits := &haveBits{b: append([]byte(nil), r.popped.Bytes()...)}
		o.mu.Unlock()
		bits.Set(index)
		gs.discloseLead(p, root, index)
		if gs.feature(GossipSubFeatureIdontwant, gs.peers[p]) {
			idontwant := []*pb.ControlIDontWant{{MessageIDs: []string{encodeHave(root, bits)}}}
			gs.sendRPC(p, rpcWithControl(nil, nil, nil, nil, nil, idontwant), true)
		}
	}
}

// forget drops an aged-out group.
func (o *originPaced) forget(root string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	delete(o.roots, root)
}

// WithDecodeClearsPromises makes a decode claim (HaveAll) fulfil every gossip-tracer promise for the group's ids, so no answerer is charged for an ask decode made moot. Requires WithHaveBitmaps.
func WithDecodeClearsPromises() Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok {
			return fmt.Errorf("decode-cleared promises require a gossipsub router")
		}
		if gs.have == nil {
			return fmt.Errorf("decode-cleared promises require WithHaveBitmaps first")
		}
		gs.have.decodeClears = true
		return nil
	}
}

// haveInRoot reports whether mid, an ask by index or a message id, names a frame of root.
func (h *haveState) haveInRoot(mid, root string) bool {
	if r, _, ok := decodeWant(mid); ok {
		return r == root
	}
	r, _, ok := h.index(mid)
	return ok && r == root
}

// haveNoteServable records a validated message we forward, or one of our own, as held: what
// we announce is what we forward, and what we can serve is what we hold. Router goroutine,
// from rpcs().
func (gs *GossipSubRouter) haveNoteServable(mid string) {
	gs.haveNoteArrival(mid)
}

// haveNotePushed records that we pushed mid to p, so p's announce bitmap leaves it out as the
// per-id announce would. Router goroutine, from rpcs().
func (gs *GossipSubRouter) haveNotePushed(p peer.ID, mid string) {
	h := gs.have
	if h == nil {
		return
	}
	if h.ledger != nil {
		// The ledger must see the first pop to p, which can precede p's first HAVE.
		gs.haveQueue(p)
	}
	if !h.ihaveOn {
		return
	}
	root, index, ok := h.index(mid)
	if !ok || len(root) != haveRootLen {
		return
	}
	roots := h.pushed[p]
	if roots == nil {
		roots = make(map[string]*haveEntry)
		h.pushed[p] = roots
	}
	e := roots[root]
	if e == nil {
		e = &haveEntry{first: time.Now()}
		roots[root] = e
	}
	e.bits.Set(index)
	e.ttl = h.pushTTL
}

// haveIHaveIDs returns mids with the structured ids among them replaced by one HAVE per group
// root carrying exactly those ids' indices -- the bitmap is an encoding of the announce's id
// list and nothing more, so a phase announce still names one message and a heartbeat gossip its
// window -- less what we pushed to p and what p has told us it holds. Other ids pass through;
// mids is not mutated. Identity when the IHAVE side is off; an empty bitmap is not sent.
//
// The first build announced the whole servable set instead. At n=500 that brought a peer's
// first announce with dozens of indices, the asker batched them onto that peer, and the 400 ms
// promise window broke under the serve load: 54k broken promises, 849 peers parked, copies up
// by half. The per-id announce never does that, so neither may its encoding.
func (gs *GossipSubRouter) haveIHaveIDs(p peer.ID, mids []string) []string {
	h := gs.have
	if h == nil || !h.ihaveOn {
		return mids
	}
	var out []string
	var roots []string
	var bits map[string]*haveBits
	for i, mid := range mids {
		root, index, ok := h.index(mid)
		if !ok || len(root) != haveRootLen {
			if out != nil {
				out = append(out, mid)
			}
			continue
		}
		if out == nil {
			out = append(make([]string, 0, len(mids)), mids[:i]...)
			bits = make(map[string]*haveBits)
		}
		b := bits[root]
		if b == nil {
			b = &haveBits{}
			bits[root] = b
			roots = append(roots, root)
		}
		b.Set(index)
	}
	if out == nil {
		return mids
	}
	for _, root := range roots {
		b := bits[root]
		if pe := h.pushed[p]; pe != nil && pe[root] != nil {
			b = b.AndNot(&pe[root].bits)
		}
		if pe := h.peers[p]; pe != nil && pe[root] != nil {
			b = b.AndNot(&pe[root].bits)
		}
		if len(b.Bytes()) == 0 {
			continue
		}
		if h.announceLoad {
			out = append(out, encodeHaveLoad(root, gs.haveAnnounceLoad(p), b))
		} else {
			out = append(out, encodeHave(root, b))
		}
	}
	return out
}

// haveSeen is seenMessage for ids that may be asks by index: an index we hold is seen.
func (gs *GossipSubRouter) haveSeen(mid string) bool {
	if gs.p.seenMessage(mid) {
		return true
	}
	h := gs.have
	if h == nil {
		return false
	}
	if root, index, ok := decodeWant(mid); ok {
		e := h.mine[root]
		return e != nil && e.bits.Has(index)
	}
	return false
}

// haveExpandIHave turns an announced bitmap into the ids to treat as announced: the real id
// where we know it, an ask-by-index id where we do not, and nothing for indices we hold.
func (gs *GossipSubRouter) haveExpandIHave(root string, enc []byte) []string {
	h := gs.have
	if h == nil || !h.ihaveOn || len(root) != haveRootLen {
		return nil
	}
	var theirs haveBits
	theirs.Or(enc)
	mine := h.mine[root]
	known := h.mids[root]
	var out []string
	for i := 0; i < len(theirs.b)*8; i++ {
		index := uint32(i) // lint:ignore uintcast -- bounded by haveMaxBytes*8.
		if !theirs.Has(index) || (mine != nil && mine.bits.Has(index)) {
			continue
		}
		if known != nil {
			if mid, ok := known.ids[index]; ok {
				out = append(out, mid)
				continue
			}
		}
		out = append(out, encodeWant(root, index))
	}
	return out
}

// haveResolveWant maps an ask by index to the id we hold at it. skip is true when the ask cannot
// be served: the index is unknown or not held. An ordinary id comes back as itself.
func (gs *GossipSubRouter) haveResolveWant(mid string) (real string, skip bool) {
	root, index, ok := decodeWant(mid)
	if !ok {
		return mid, false
	}
	h := gs.have
	if h == nil {
		return "", true
	}
	if m := h.mine[root]; m == nil || !m.bits.Has(index) {
		return "", true
	}
	known := h.mids[root]
	if known == nil {
		return "", true
	}
	real, ok = known.ids[index]
	return real, !ok
}

// haveArrived closes the ask an arriving message was made under when it was made by index:
// the budget, the promise, the outstanding pull, the offers, the request ledger and the gossip
// tracer's promise all live under the ask's id. Router goroutine, from Preprocess.
func (gs *GossipSubRouter) haveArrived(mid string, from peer.ID) {
	h := gs.have
	if h == nil || !h.ihaveOn {
		return
	}
	root, index, ok := h.index(mid)
	if !ok || len(root) != haveRootLen {
		return
	}
	want := encodeWant(root, index)
	gs.clearIWantBudgetFor(want)
	gs.noteArrival(want, from)
	if gs.gossipTracer != nil {
		gs.gossipTracer.fulfillPromiseID(want)
	}
}

// dedupeHaveIHave removes from pending the HAVE pseudo ids whose root next re-announces: a
// bitmap is idempotent, so the newer one is the whole truth. Pending is router-owned until it
// is piggybacked, when it leaves the map, so nothing the writer holds is touched.
func dedupeHaveIHave(pending []*pb.ControlIHave, next *pb.ControlIHave) {
	roots := make(map[string]bool)
	for _, mid := range next.GetMessageIDs() {
		if root, _, ok := decodeHave(mid); ok {
			roots[root] = true
		}
	}
	if len(roots) == 0 {
		return
	}
	for _, ih := range pending {
		kept := ih.MessageIDs[:0:0]
		changed := false
		for _, mid := range ih.GetMessageIDs() {
			if root, _, ok := decodeHave(mid); ok && roots[root] {
				changed = true
				continue
			}
			kept = append(kept, mid)
		}
		if changed {
			ih.MessageIDs = kept
		}
	}
}

// --- D1c: a fast announce that carries its own load ---
//
// IHAVE stays in the normal lane because its FIFO wait behind our data to a peer
// is the scheme's only per-link load signal. That signal can travel explicitly: the announce
// bitmap carries one byte, the announcer's queued bytes to this peer in log buckets, and may
// then take the urgent lane. The asker converts the byte back into the delay the normal lane
// would have imposed, at a nominal rate, so the two are the same information at different
// speeds -- and the asker, seeing every announcer's load at once, can also choose. The pair
// that prices this is slow-IHAVE against fast-IHAVE-with-load, with fast-and-no-delay as the
// predicted failure.

const haveVersionLoad = 2

// loadBucket maps queued bytes to one byte: 0 for nothing queued, else 1 + log2(KiB), capped.
func loadBucket(queued int64) byte {
	if queued <= 0 {
		return 0
	}
	b := byte(1)
	for kib := queued >> 10; kib > 0 && b < 255; kib >>= 1 {
		b++
	}
	return b
}

// loadBytes is loadBucket's inverse, at the bucket's floor.
func loadBytes(bucket byte) int64 {
	if bucket <= 1 {
		return 0
	}
	return int64(1) << (10 + int(bucket) - 2)
}

// encodeHaveLoad builds a v2 pseudo id: magic ‖ 2 ‖ root ‖ load ‖ bitmap.
func encodeHaveLoad(root string, load byte, bits *haveBits) string {
	out := make([]byte, 0, haveHeaderLen+1+len(bits.b))
	out = append(out, haveMagic...)
	out = append(out, haveVersionLoad)
	out = append(out, root...)
	out = append(out, load)
	out = append(out, bits.Bytes()...)
	return string(out)
}

// decodeHaveLoad reads a HAVE of either version; a v1 id reports load 0.
func decodeHaveLoad(mid string) (root string, load byte, bitmap []byte, ok bool) {
	if len(mid) < haveHeaderLen || len(mid) > haveHeaderLen+1+haveMaxBytes || mid[:len(haveMagic)] != haveMagic {
		return "", 0, nil, false
	}
	root = mid[len(haveMagic)+1 : haveHeaderLen]
	switch mid[len(haveMagic)] {
	case haveVersion, haveVersionClaim:
		if len(mid) > haveHeaderLen+haveMaxBytes {
			return "", 0, nil, false
		}
		return root, 0, []byte(mid[haveHeaderLen:]), true
	case haveVersionLoad:
		if len(mid) < haveHeaderLen+1 {
			return "", 0, nil, false
		}
		return root, mid[haveHeaderLen], []byte(mid[haveHeaderLen+1:]), true
	}
	return "", 0, nil, false
}

// WithHaveAnnounceUrgent puts the IHAVE bitmap in the urgent lane; with loadByte it carries the
// announcer's queued bytes to the peer, so the asker can restore the lane's delay itself.
func WithHaveAnnounceUrgent(loadByte bool) Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok {
			return fmt.Errorf("urgent announces require a gossipsub router")
		}
		if gs.have == nil || !gs.have.ihaveOn {
			return fmt.Errorf("urgent announces require WithHaveAnnounceBitmaps first")
		}
		gs.have.announceUrgent = true
		gs.have.announceLoad = loadByte
		return nil
	}
}

// WithHaveAskDelay makes the asker hold an announce that carries load for the time that load
// would have taken to drain at bitsPerSecond, at most maxDelay, before considering it.
func WithHaveAskDelay(bitsPerSecond int64, maxDelay time.Duration) Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok {
			return fmt.Errorf("ask delay requires a gossipsub router")
		}
		if gs.have == nil || !gs.have.ihaveOn {
			return fmt.Errorf("ask delay requires WithHaveAnnounceBitmaps first")
		}
		if bitsPerSecond <= 0 || maxDelay <= 0 {
			return fmt.Errorf("ask delay requires a positive rate and cap")
		}
		gs.have.askRate = bitsPerSecond
		gs.have.askCap = maxDelay
		return nil
	}
}

// haveAnnounceUrgent reports the lane the IHAVE bitmap takes.
func (gs *GossipSubRouter) haveAnnounceUrgent() bool {
	return gs.have != nil && gs.have.announceUrgent
}

// haveAnnounceLoad is the load byte for an announce: our queued bytes across every peer,
// bucketed. Across every peer, not to p alone: the normal lane's delay came from the writer
// blocking on the one uplink all queues share, and a publisher announcing to a peer it has
// pushed nothing to has an empty queue to that peer and a full uplink. Measured with the
// per-peer figure at σ=0.7: the publisher's bytes 2.7x, a herd at the source,
// with or without the asker's delay.
func (gs *GossipSubRouter) haveAnnounceLoad(_ peer.ID) byte {
	if gs.have == nil || !gs.have.announceLoad || gs.p == nil {
		return 0
	}
	var queued int64
	for _, q := range gs.p.peers {
		queued += q.QueuedBytes()
	}
	return loadBucket(queued)
}

// haveAskDelay is how long the asker holds an announce carrying load before acting on it.
func (gs *GossipSubRouter) haveAskDelay(load byte) time.Duration {
	h := gs.have
	if h == nil || h.askRate <= 0 || load == 0 {
		return 0
	}
	d := time.Duration(loadBytes(load) * 8 * int64(time.Second) / h.askRate)
	if d > h.askCap {
		d = h.askCap
	}
	return d
}

// deferAnnounce re-considers an announced bitmap from p after delay, on the router goroutine,
// and sends the ask itself. The lane's FIFO wait, applied by the asker instead.
func (gs *GossipSubRouter) deferAnnounce(p peer.ID, topic, root string, enc []byte, delay time.Duration) {
	bits := append([]byte(nil), enc...)
	time.AfterFunc(delay, func() {
		select {
		case gs.p.eval <- func() {
			if _, ok := gs.mesh[topic]; !ok || !gs.p.peerFilter(p, topic) {
				return
			}
			iwant := make(map[string]string)
			var candidates []string
			for _, mid := range gs.haveExpandIHave(root, bits) {
				gs.considerAnnounced(p, topic, mid, iwant, &candidates)
			}
			if asks := gs.askFrom(p, candidates, iwant); len(asks) > 0 {
				gs.sendRPC(p, rpcWithControl(nil, nil, asks, nil, nil, nil), false)
			}
		}:
		case <-gs.p.ctx.Done():
		}
	})
}

// --- D3, first form: rarest-first at dispatch ---
//
// The rarity-ordered drain ranks a frame when the router enqueues it and admits the rank goes
// stale while queued; Q63 and Q64 found it null, and the plan's reading is that nothing was
// scarce where it was measured. With the HAVE bitmaps the writer can make the same choice at
// dispatch from live evidence: how many peers have announced each index. The counts live in one
// small table the router writes on every HAVE and the writer reads at Pop, under its own lock,
// never the router's. Control RPCs keep their FIFO position -- a frame is chosen only among the
// data ahead of the next control RPC -- so IHAVE's queueing delay is
// untouched by construction.

// holderCounts is peers known to hold each index of each root, from their HAVE bitmaps.
type holderCounts struct {
	mu     sync.Mutex
	counts map[string][]int32
}

func (c *holderCounts) add(root string, indices []uint32) {
	if len(indices) == 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.counts == nil {
		c.counts = make(map[string][]int32)
	}
	arr := c.counts[root]
	for _, i := range indices {
		for int(i) >= len(arr) {
			arr = append(arr, 0)
		}
		arr[i]++
	}
	c.counts[root] = arr
}

func (c *holderCounts) get(root string, index uint32) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	arr := c.counts[root]
	if int(index) >= len(arr) {
		return 0
	}
	return int(arr[index])
}

func (c *holderCounts) forget(root string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.counts, root)
}

// WithHaveRarestDispatch makes each peer's writer pop, among the queued data frames ahead of
// the next control RPC, the one whose index the fewest peers have announced; FIFO within a tie.
// Requires WithHaveBitmaps and WithHaveDispatch (the queue-side state it rides on).
func WithHaveRarestDispatch() Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok {
			return fmt.Errorf("rarest dispatch requires a gossipsub router")
		}
		if gs.have == nil || (!gs.have.cancel && !gs.have.attach) {
			return fmt.Errorf("rarest dispatch requires WithHaveBitmaps and WithHaveDispatch first")
		}
		gs.have.rarest = true
		gs.have.holders = &holderCounts{}
		return nil
	}
}

// WithFrameClass gives each queued frame a priority class at dispatch, from its topic, group root
// and index: every peer's writer pops the lowest class first, and rarest-first (with any pair
// order) orders within a class. Frames already granted a slot in the hose window are not
// preempted. Requires WithHaveRarestDispatch.
func WithFrameClass(class func(topic, root string, index uint32) int) Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok {
			return fmt.Errorf("frame class requires a gossipsub router")
		}
		if gs.have == nil || !gs.have.rarest || gs.have.msgIndex == nil {
			return fmt.Errorf("frame class requires WithHaveRarestDispatch first")
		}
		h := gs.have
		h.class = func(m *pb.Message) int {
			root, index, ok := h.msgIndex(m)
			if !ok {
				return 0
			}
			return class(m.GetTopic(), root, index)
		}
		return nil
	}
}

// WithFrameClassPullOnly applies the class to pull answers alone: a frame sent because the peer
// asked for it by IWANT pops by class, a pushed frame keeps class 0 and pops by rarity with the
// first class. Requires WithFrameClass.
func WithFrameClassPullOnly() Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok || gs.have == nil || gs.have.class == nil {
			return fmt.Errorf("pull-only frame class requires WithFrameClass first")
		}
		gs.have.classPullOnly = true
		return nil
	}
}

// WithFrameClassGrants makes the hose window grant a freed slot to the waiting writer whose best
// queued frame has the lowest class, first come within a class, so a writer holding only
// low-priority frames does not take a slot ahead of one holding high-priority frames. Requires
// WithFrameClass.
func WithFrameClassGrants() Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok || gs.have == nil || gs.have.class == nil {
			return fmt.Errorf("frame class grants require WithFrameClass first")
		}
		gs.have.classGrants = true
		return nil
	}
}

// WithFrameClassRelease makes the class completion-aware per peer: a frame whose class is below
// class loses its priority toward a peer, and pops as class, once the dispatch projection takes
// that peer to within margin frames of completing the frame's group. A strict class otherwise
// keeps sending a group's frames ahead of everything else to a peer that no longer needs them.
// Requires WithFrameClass and WithHaveDispatchProjection.
func WithFrameClassRelease(margin, class int) Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok || gs.have == nil || gs.have.class == nil || gs.have.projectNeed == 0 {
			return fmt.Errorf("frame class release requires WithFrameClass and WithHaveDispatchProjection first")
		}
		if margin < 1 {
			return fmt.Errorf("frame class release requires margin >= 1")
		}
		gs.have.releaseMargin, gs.have.releaseClass = margin, class
		return nil
	}
}

const (
	havePairTie   = 1 // rarest-first, ties broken by the (frame, self, peer) hash
	havePairFirst = 2 // the (frame, self, peer) hash leads, holder count breaks its ties
	haveNodeTie   = 3 // as havePairTie with a (frame, self) hash: one order toward every peer
	haveNodeFirst = 4 // as havePairFirst with the (frame, self) hash
)

// WithHavePairOrder orders each peer's queued frames at Pop by a hash of (root, index, self,
// peer), so two pushers feeding the same receiver send in independent orders instead of the
// shared arrival order that makes them collide on every frame. first=false
// keeps rarest-first and breaks its ties by the hash; first=true makes the hash the primary key
// and the holder count the tie-break. perNode drops the peer from the hash: a node then sends
// the same order to all its peers, so its r copies of a frame still leave together (the source's
// two roots of every frame start at once) while different nodes' orders stay independent.
// Requires WithHaveRarestDispatch.
func WithHavePairOrder(first, perNode bool) Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok {
			return fmt.Errorf("pair order requires a gossipsub router")
		}
		if gs.have == nil || !gs.have.rarest {
			return fmt.Errorf("pair order requires WithHaveRarestDispatch first")
		}
		switch {
		case perNode && first:
			gs.have.pairOrder = haveNodeFirst
		case perNode:
			gs.have.pairOrder = haveNodeTie
		case first:
			gs.have.pairOrder = havePairFirst
		default:
			gs.have.pairOrder = havePairTie
		}
		return nil
	}
}

// havePairKey is the writer's order key for one peer: the pair hash of a queued frame (the
// node hash under the per-node modes), or false for a message outside any structured group.
func (h *haveState) havePairKey(local, remote peer.ID) func(*pb.Message) (uint64, bool) {
	if h.pairOrder == haveNodeTie || h.pairOrder == haveNodeFirst {
		remote = ""
	}
	return func(m *pb.Message) (uint64, bool) {
		if h.msgIndex == nil {
			return 0, false
		}
		root, index, ok := h.msgIndex(m)
		if !ok {
			return 0, false
		}
		return pairRank(root, index, local, remote), true
	}
}

// pairRank hashes (root, index, local, remote) with the same finalizer as phaseRank, so the
// order a pusher uses toward one receiver is independent of every other (pusher, receiver) pair.
func pairRank(root string, index uint32, local, remote peer.ID) uint64 {
	var ib [4]byte
	binary.BigEndian.PutUint32(ib[:], index)
	hh := fnv.New64a()
	_, _ = hh.Write([]byte(root))
	_, _ = hh.Write(ib[:])
	_, _ = hh.Write([]byte{0})
	_, _ = hh.Write([]byte(local))
	_, _ = hh.Write([]byte{0})
	_, _ = hh.Write([]byte(remote))
	x := hh.Sum64()
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return x
}

// haveHolders reports how many peers have announced the index a queued message sits at, and
// whether the message is one of a structured group at all. Read by the writer.
func (h *haveState) haveHolders(m *pb.Message) (int, bool) {
	if h.holders == nil || h.msgIndex == nil {
		return 0, false
	}
	root, index, ok := h.msgIndex(m)
	if !ok {
		return 0, false
	}
	return h.holders.get(root, index), true
}

// --- The dispatch ledger: what a cancel could have reached ---
//
// A duplicate's offset behind the first copy cannot say whether its sender could have cancelled
// it: the pop time and the instant the sender learned the peer held the frame are both on the
// sender. The ledger keeps them per (peer, root, index), so a harness that sees the duplicate
// land can class it -- popped after the HAVE arrived (a cancel that failed), popped before the
// receiver had its first copy (blind: no signal could have helped), or popped in between (the
// have-signal's latency window, what a faster signal would have bought). Measurement only:
// nothing in the router reads it, and it is not aged, so it holds one cell's roots.

// HaveLedgerEntry is one (peer, root, index) we dispatched to the peer or learned it holds.
type HaveLedgerEntry struct {
	Peer  peer.ID
	Root  string
	Index uint32
	// Digest is sha256 of the popped message's data, so arrivals keyed by data digest join
	// without an id map; zero when the frame was never popped to this peer.
	Digest [32]byte
	PopAt  time.Time // first pop of the frame to this peer; zero if never
	HaveAt time.Time // first HAVE from this peer covering the index; zero if never
}

type haveLedger struct {
	mu      sync.Mutex
	entries map[peer.ID]map[string]map[uint32]*HaveLedgerEntry
}

func (l *haveLedger) entry(p peer.ID, root string, index uint32) *HaveLedgerEntry {
	roots := l.entries[p]
	if roots == nil {
		roots = make(map[string]map[uint32]*HaveLedgerEntry)
		l.entries[p] = roots
	}
	byIndex := roots[root]
	if byIndex == nil {
		byIndex = make(map[uint32]*HaveLedgerEntry)
		roots[root] = byIndex
	}
	e := byIndex[index]
	if e == nil {
		e = &HaveLedgerEntry{Peer: p, Root: root, Index: index}
		byIndex[index] = e
	}
	return e
}

// pop records the first dispatch of (root, index) to p. Writer goroutine.
func (l *haveLedger) pop(p peer.ID, root string, index uint32, digest [32]byte, at time.Time) {
	l.mu.Lock()
	defer l.mu.Unlock()
	e := l.entry(p, root, index)
	if e.PopAt.IsZero() {
		e.PopAt, e.Digest = at, digest
	}
}

// have records when p's HAVE first covered each of indices. Router goroutine.
func (l *haveLedger) have(p peer.ID, root string, indices []uint32, at time.Time) {
	if len(indices) == 0 {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, i := range indices {
		e := l.entry(p, root, i)
		if e.HaveAt.IsZero() {
			e.HaveAt = at
		}
	}
}

func (l *haveLedger) snapshot() []HaveLedgerEntry {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []HaveLedgerEntry
	for _, roots := range l.entries {
		for _, byIndex := range roots {
			for _, e := range byIndex {
				out = append(out, *e)
			}
		}
	}
	return out
}

// WithHaveLedger keeps the dispatch ledger. Requires WithHaveBitmaps and WithHaveDispatch.
func WithHaveLedger() Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok {
			return fmt.Errorf("have ledger requires a gossipsub router")
		}
		if gs.have == nil || (!gs.have.cancel && !gs.have.attach) {
			return fmt.Errorf("have ledger requires WithHaveBitmaps and WithHaveDispatch first")
		}
		gs.have.ledger = &haveLedger{entries: make(map[peer.ID]map[string]map[uint32]*HaveLedgerEntry)}
		return nil
	}
}

// HaveLedger is a copy of the dispatch ledger, or nil when it is not kept.
func (p *PubSub) HaveLedger() []HaveLedgerEntry {
	gs, ok := p.rt.(*GossipSubRouter)
	if !ok || gs.have == nil || gs.have.ledger == nil {
		return nil
	}
	return gs.have.ledger.snapshot()
}

// DecodeHaveWant reports whether mid is a WANT pseudo id — an ask by index that the IHAVE
// bitmap produces in place of a message id — and which root and index it names. Request gates
// that key on structured ids need it: a WANT is segment traffic too, though no id parser sees it.
func DecodeHaveWant(mid string) (root string, index uint32, ok bool) {
	return decodeWant(mid)
}

// fullHaveBits is the bitmap with every index below count set.
func fullHaveBits(count uint32) *haveBits {
	var b haveBits
	for i := uint32(0); i < count; i++ {
		b.Set(i)
	}
	return &b
}

// HaveAll records that we hold every index of root below count and tells topic's mesh peers
// so, in the urgent lane, as an IDONTWANT goes. A node that decoded a coded group holds every
// frame of it, the ones it never received included, since it can re-encode them; saying so
// cancels what its pushers still have queued for it at their next pop (WithHaveDispatch) and
// counts it as a holder for the frames they have not forwarded yet.
// Safe from any goroutine; a no-op without WithHaveBitmaps.
func (p *PubSub) HaveAll(topic, root string, count uint32) {
	gs, ok := p.rt.(*GossipSubRouter)
	if !ok || gs.have == nil {
		return
	}
	select {
	case p.eval <- func() { gs.haveAll(topic, root, count) }:
	case <-p.ctx.Done():
	}
}

// HaveSuppress tells every mesh peer of topic to stop sending the group's frames — the full
// IDONTWANT bitmap — without claiming them: the node's own bits are untouched, so it keeps
// pulling, answers only what it holds, and treats arrivals as new. The
// receiver sends it when the frames already in flight will complete it, one flight before
// decode; a shortfall is its own to pull.
func (p *PubSub) HaveSuppress(topic, root string, count uint32) {
	gs, ok := p.rt.(*GossipSubRouter)
	if !ok || gs.have == nil {
		return
	}
	select {
	case p.eval <- func() { gs.haveSuppress(topic, root, count) }:
	case <-p.ctx.Done():
	}
}

// haveSuppress is HaveSuppress on the router goroutine. It bypasses the pending-HAVE attach
// path on purpose: that path would carry the node's real bits, not the full bitmap.
func (gs *GossipSubRouter) haveSuppress(topic, root string, count uint32) {
	h := gs.have
	if h == nil || len(root) != haveRootLen || !h.idontwantOn {
		return
	}
	mid := encodeHaveClaim(root, fullHaveBits(count))
	for pid := range gs.mesh[topic] {
		if !gs.feature(GossipSubFeatureIdontwant, gs.peers[pid]) {
			continue
		}
		idontwant := []*pb.ControlIDontWant{{MessageIDs: []string{mid}}}
		gs.sendRPC(pid, rpcWithControl(nil, nil, nil, nil, nil, idontwant), true)
	}
}

// HaveSuppressTo is HaveSuppress toward one peer: the claim for root, sent to p alone, so a
// receiver can time it per pusher by that pusher's one-way delay (D7 idea 2's remaining form).
func (p *PubSub) HaveSuppressTo(topic, root string, count uint32, to peer.ID) {
	gs, ok := p.rt.(*GossipSubRouter)
	if !ok || gs.have == nil {
		return
	}
	select {
	case p.eval <- func() { gs.haveSuppressTo(root, count, to) }:
	case <-p.ctx.Done():
	}
}

// haveSuppressTo is HaveSuppressTo on the router goroutine.
func (gs *GossipSubRouter) haveSuppressTo(root string, count uint32, to peer.ID) {
	h := gs.have
	if h == nil || len(root) != haveRootLen || !h.idontwantOn {
		return
	}
	if _, ok := gs.peers[to]; !ok || !gs.feature(GossipSubFeatureIdontwant, gs.peers[to]) {
		return
	}
	idontwant := []*pb.ControlIDontWant{{MessageIDs: []string{encodeHaveClaim(root, fullHaveBits(count))}}}
	gs.sendRPC(to, rpcWithControl(nil, nil, nil, nil, nil, idontwant), true)
}

// haveClaimedPeer reports whether p sent a claim covering mid's index: pushes stop, announces
// do not. Router goroutine.
func (gs *GossipSubRouter) haveClaimedPeer(p peer.ID, mid string) bool {
	h := gs.have
	if h == nil {
		return false
	}
	root, index, ok := h.index(mid)
	if !ok {
		return false
	}
	e := h.peers[p][root]
	return e != nil && e.claim.Has(index)
}

// haveOriginPushed notes that a batch pushed mid to p, keyed by mid's root; nil pushed means origin HAVE is off.
func (gs *GossipSubRouter) haveOriginPushed(pushed map[string]map[peer.ID]struct{}, mid string, p peer.ID) {
	if pushed == nil {
		return
	}
	root, _, ok := gs.have.index(mid)
	if !ok || len(root) != haveRootLen {
		return
	}
	if pushed[root] == nil {
		pushed[root] = make(map[peer.ID]struct{})
	}
	pushed[root][p] = struct{}{}
}

// haveOrigin sends our HAVE for each group in a published batch, once, to the topic's mesh and the peers the batch pushed it to.
func (gs *GossipSubRouter) haveOrigin(msgs []*Message, pushed map[string]map[peer.ID]struct{}) {
	h := gs.have
	if h == nil || !h.originHave || !h.idontwantOn {
		return
	}
	sent := make(map[string]bool)
	for _, msg := range msgs {
		root, _, ok := h.index(gs.p.idGen.ID(msg))
		if !ok || len(root) != haveRootLen || sent[root] || h.mine[root] == nil {
			continue
		}
		sent[root] = true
		mid := encodeHave(root, &h.mine[root].bits)
		to := make([]peer.ID, 0, len(gs.mesh[msg.GetTopic()])+len(pushed[root]))
		for p := range gs.mesh[msg.GetTopic()] {
			to = append(to, p)
		}
		for p := range pushed[root] {
			if _, inMesh := gs.mesh[msg.GetTopic()][p]; !inMesh {
				to = append(to, p)
			}
		}
		sort.Slice(to, func(i, j int) bool { return to[i] < to[j] })
		for _, pid := range to {
			if !gs.feature(GossipSubFeatureIdontwant, gs.peers[pid]) {
				continue
			}
			if gs.havePendAll(pid, []string{mid}) {
				continue
			}
			idontwant := []*pb.ControlIDontWant{{MessageIDs: []string{mid}}}
			gs.sendRPC(pid, rpcWithControl(nil, nil, nil, nil, nil, idontwant), true)
		}
	}
}

// haveAll is HaveAll on the router goroutine.
func (gs *GossipSubRouter) haveAll(topic, root string, count uint32) {
	h := gs.have
	if h == nil || len(root) != haveRootLen {
		return
	}
	e := h.mine[root]
	if e == nil {
		e = &haveEntry{}
		h.mine[root] = e
	}
	e.bits.Or(fullHaveBits(count).Bytes())
	e.ttl = h.holdTTL
	if h.decodeClears && gs.gossipTracer != nil {
		gs.gossipTracer.fulfillPromisesWhere(func(mid string) bool { return h.haveInRoot(mid, root) })
	}
	if !h.idontwantOn {
		return
	}
	mid := encodeHave(root, &e.bits)
	for pid := range gs.mesh[topic] {
		if !gs.feature(GossipSubFeatureIdontwant, gs.peers[pid]) {
			continue
		}
		if gs.havePendAll(pid, []string{mid}) {
			continue
		}
		idontwant := []*pb.ControlIDontWant{{MessageIDs: []string{mid}}}
		gs.sendRPC(pid, rpcWithControl(nil, nil, nil, nil, nil, idontwant), true)
	}
}

// RebuiltMode is what a decoded node does with the frames it rebuilt, once its check passed
// (the decoding note's section 5): hold them, announce them, or push them.
type RebuiltMode int

const (
	// RebuiltHold keeps the frames servable by index and announces none of them: the
	// specified behaviour, and in effect what the runs without rebuilt frames measured.
	RebuiltHold RebuiltMode = iota
	// RebuiltAnnounce also offers them as received frames are offered: one IHAVE bitmap to each
	// mesh peer, less what it holds and what we pushed it, and heartbeat gossip.
	RebuiltAnnounce
	// RebuiltPush also pushes them to the mesh peers whose HAVEs show them short of the group,
	// each a random choice among the rebuilt frames it lacks, up to its shortfall.
	RebuiltPush
)

// RebuiltConfig configures Rebuilt.
type RebuiltConfig struct {
	Mode RebuiltMode
	// Need is the number of distinct frames that complete a peer: K.
	Need int
	// PushMax caps the frames pushed to one peer under RebuiltPush; 0 is its whole shortfall.
	PushMax int
}

// RebuiltStat counts what Rebuilt did on one node, for the run's report.
type RebuiltStat struct {
	Held, AnnouncedTo, Announced, Pushed, Served int64
}

type rebuiltState struct {
	ids                                          map[string]struct{}
	held, announcedTo, announced, pushed, served atomic.Int64
}

// Rebuilt hands the router the frames of a group that this node rebuilt after decoding and did
// not receive, each as the bytes its source published on topic. They become servable: the
// message cache holds them, out of heartbeat gossip unless announced, and an ask by index
// resolves to them. A frame that arrived since is skipped. Safe from any goroutine; a no-op
// without WithHaveBitmaps.
func (p *PubSub) Rebuilt(topic string, data [][]byte, cfg RebuiltConfig) {
	gs, ok := p.rt.(*GossipSubRouter)
	if !ok || gs.have == nil {
		return
	}
	select {
	case p.eval <- func() { gs.rebuilt(topic, data, cfg) }:
	case <-p.ctx.Done():
	}
}

// RebuiltStats reports what Rebuilt did on this node; false when it never ran.
func (p *PubSub) RebuiltStats() (RebuiltStat, bool) {
	gs, ok := p.rt.(*GossipSubRouter)
	if !ok || gs.have == nil {
		return RebuiltStat{}, false
	}
	r := gs.have.rebuilt.Load()
	if r == nil {
		return RebuiltStat{}, false
	}
	return RebuiltStat{Held: r.held.Load(), AnnouncedTo: r.announcedTo.Load(), Announced: r.announced.Load(),
		Pushed: r.pushed.Load(), Served: r.served.Load()}, true
}

// rebuiltServed counts an answer that served a rebuilt frame. Router goroutine, from handleIWant.
func (gs *GossipSubRouter) rebuiltServed(mid string) {
	if gs.have == nil {
		return
	}
	if r := gs.have.rebuilt.Load(); r != nil {
		if _, ok := r.ids[mid]; ok {
			r.served.Add(1)
		}
	}
}

// rebuiltArrived drops a rebuilt frame from the rebuilt set once a copy of it arrives: from then
// on it is relayed and announced as a received frame. Router goroutine, from rpcs().
func (gs *GossipSubRouter) rebuiltArrived(mid string) {
	if gs.have == nil {
		return
	}
	if r := gs.have.rebuilt.Load(); r != nil {
		delete(r.ids, mid)
	}
}

// rebuilt is Rebuilt on the router goroutine.
func (gs *GossipSubRouter) rebuilt(topic string, data [][]byte, cfg RebuiltConfig) {
	h := gs.have
	r := h.rebuilt.Load()
	if r == nil {
		r = &rebuiltState{ids: make(map[string]struct{})}
		h.rebuilt.Store(r)
	}
	self := gs.p.host.ID()
	var mids []string
	msgs := make(map[string]*Message, len(data))
	root := ""
	for _, d := range data {
		t := topic
		m := &Message{Message: &pb.Message{Data: d, Topic: &t}, ReceivedFrom: self}
		mid := gs.p.idGen.ID(m)
		rt, _, ok := h.index(mid)
		if !ok || len(rt) != haveRootLen {
			continue
		}
		if _, held := gs.mcache.Get(mid); held {
			continue // it arrived after the decode claim
		}
		if cfg.Mode == RebuiltAnnounce {
			gs.mcache.Put(m)
		} else {
			gs.mcache.PutQuiet(m)
		}
		gs.haveLearnID(mid)
		r.ids[mid] = struct{}{}
		mids = append(mids, mid)
		msgs[mid] = m
		root = rt
	}
	r.held.Add(int64(len(mids)))
	if len(mids) == 0 {
		return
	}
	switch cfg.Mode {
	case RebuiltAnnounce:
		for _, pid := range gs.pushOrder(gs.mesh[topic]) {
			if gs.hoseOffers != nil {
				for _, mid := range mids {
					gs.offerLater(pid, topic, mid)
				}
				r.announcedTo.Add(1)
				continue
			}
			ids := gs.haveIHaveIDs(pid, mids)
			if len(ids) == 0 {
				continue
			}
			ihave := rpcWithControl(nil, []*pb.ControlIHave{{TopicID: &topic, MessageIDs: ids}}, nil, nil, nil, nil)
			ihave.urgent = gs.haveAnnounceUrgent()
			gs.sendRPC(pid, ihave, ihave.urgent)
			r.announcedTo.Add(1)
			for _, id := range ids {
				if _, b, ok := decodeHave(id); ok {
					hb := haveBits{b: []byte(b)}
					r.announced.Add(int64(hb.Count()))
				}
			}
		}
	case RebuiltPush:
		if gs.relaySilent {
			// A silent relay sends nothing on to others; frames it rebuilt are no exception.
			if gs.relaySkipped != nil {
				gs.relaySkipped.Add(int64(len(mids)))
			}
			return
		}
		if gs.rebuiltRefuse {
			// A withholder that will not repair keeps what it rebuilt.
			if gs.rebuiltRefused != nil {
				gs.rebuiltRefused.Add(int64(len(mids)))
			}
			return
		}
		for _, pid := range gs.pushOrder(gs.mesh[topic]) {
			var known haveBits
			if e := h.peers[pid][root]; e != nil {
				known.Or(e.bits.Bytes())
				known.Or(e.claim.Bytes())
			}
			if e := h.pushed[pid][root]; e != nil {
				known.Or(e.bits.Bytes())
			}
			short := cfg.Need - known.Count()
			if short <= 0 {
				continue
			}
			var pool []string
			for _, mid := range mids {
				if _, index, ok := h.index(mid); ok && !known.Has(index) {
					pool = append(pool, mid)
				}
			}
			gs.shuffleStrings(pool)
			n := short
			if cfg.PushMax > 0 && n > cfg.PushMax {
				n = cfg.PushMax
			}
			if n > len(pool) {
				n = len(pool)
			}
			for _, mid := range pool[:n] {
				gs.haveNotePushed(pid, mid)
				gs.sendRPC(pid, rpcWithMessages(msgs[mid].Message), false)
			}
			r.pushed.Add(int64(n))
		}
	}
}
