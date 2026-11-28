package pubsub

import (
	"crypto/sha256"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	core "github.com/cskiraly/seacast-go"
	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
)

// --- HAVE bitmaps for structured-id groups ---
//
// A segmented payload is K messages whose ids share a group root and carry an index. IDONTWANT
// names each one by its full id -- 56 bytes for prysm's segment ids -- once per
// message per mesh peer. A HAVE bitmap says the same for the whole group in one pseudo id,
// magic ‖ version ‖ root ‖ bitmap, sent exactly where IDONTWANT is sent (on receipt, to mesh
// peers, in the urgent lane) and carried inside the IDONTWANT control message, so a peer that
// does not know the encoding sees an id it will never match and nothing else changes for it.
//
// Everything that suppresses a send keeps reading the per-id unwanted map. A HAVE is expanded
// into it lazily: for the ids we already know when the HAVE arrives, and for the rest when the
// message arrives (Preprocess). The forward path, the batch publisher's filter and the IWANT
// handler are untouched. The bitmap is idempotent state, so where two HAVEs for a root meet in one
// RPC (the attach's mergedControl, the pending IHAVEs' dedupeHaveIHave) the newer replaces the
// older instead of being appended -- which is where the bytes go.

const (
	haveMagic     = "HAVE"
	haveVersion   = 1
	haveRootLen   = 32
	haveHeaderLen = len(haveMagic) + 1 + haveRootLen
	// haveMaxBytes bounds a bitmap a peer may send: 8192 indices.
	haveMaxBytes = 1024
)

// haveBits is a growable bitset over segment indices, little-endian within a byte: the core's Bits
// (seacast/bits.go).
type haveBits = core.Bits

// newHaveBits is a bitset over b, which it keeps.
func newHaveBits(b []byte) *haveBits {
	h := haveBits(b)
	return &h
}

// encodeHave builds the pseudo id for root's bitmap.
func encodeHave(root string, bits *haveBits) string {
	out := make([]byte, 0, haveHeaderLen+len(*bits))
	out = append(out, haveMagic...)
	out = append(out, haveVersion)
	out = append(out, root...)
	out = append(out, bits.Bytes()...)
	return string(out)
}

// decodeHave reads a holding's pseudo id of version 1, or reports that mid is an ordinary id.
func decodeHave(mid string) (root string, bitmap []byte, ok bool) {
	if len(mid) < haveHeaderLen || len(mid) > haveHeaderLen+haveMaxBytes || mid[:len(haveMagic)] != haveMagic {
		return "", nil, false
	}
	if mid[len(haveMagic)] != haveVersion {
		return "", nil, false
	}
	return mid[len(haveMagic)+1 : haveHeaderLen], []byte(mid[haveHeaderLen:]), true
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
	// originAcks is the acknowledged indices of the groups the node publishes on the path
	// (seacast_origin.go): their holding names those only.
	originAcks *originAcks
	mine       core.Holdings // by seacastGID(root)
	// tomb is the groups past their active lifetime under v0.5's D′, by heartbeats left: their
	// mine aged out GROUP_TTL after the last chunk, and only the id is kept, for another holdTTL.
	tomb map[string]int
	// own mirrors mine for the writers (ownView).
	own   ownView
	mids  map[string]*haveIDs
	peers core.PeerHoldings[peer.ID] // by seacastGID(root)
	// pushed is what we have pushed to each peer per root. The phase announce names a message
	// only to the peers it did not push it to; the per-peer bitmap keeps that by leaving out
	// what we pushed to the peer and what the peer has told us it holds.
	pushed core.Pushes[peer.ID] // by seacastGID(root)
	// Dispatch-time decisions (WithHaveDispatch): msgIndex is index over a queued message.
	msgIndex func(m *pb.Message) (root string, index uint32, ok bool)
	cancel   bool
	attach   bool
	ihaveOn  bool
	// holdTTL ages mine and mids: what we hold and know the id of stands in for the seen
	// cache and mcache, so it lives as long as they do, in heartbeats. pushTTL ages pushed, for
	// as long as a message stays in the gossip window. Peers' HAVEs keep IDONTWANT's TTL.
	holdTTL int
	pushTTL int
	// Rarest-first at dispatch, from the holder counts the HAVEs build.
	rarest  bool
	holders *holderCounts
	// §4.12's choose (choose.go): our own chunks first, and the picks over every queue.
	ownFirst bool
	ownPicks atomic.Int64
	// The projection at dispatch, installed on every peer's queue by haveQueue.
	projectNeed  uint32
	projectFanIn int
	// ledger, when kept, records per (peer, root, index) when we popped the frame to the peer
	// and when the peer's HAVE covering it reached us (WithHaveLedger; measurement only).
	ledger *haveLedger
	// rebuilt is what Rebuilt handed the router, and its counters; nil until it first runs.
	rebuilt atomic.Pointer[rebuiltState]
}

func newHaveState(index func(mid string) (string, uint32, bool)) *haveState {
	return &haveState{
		index: index,
		mine:  core.Holdings{},
		tomb:  make(map[string]int),
		mids:  make(map[string]*haveIDs),
		peers: core.PeerHoldings[peer.ID]{},

		pushed: core.Pushes[peer.ID]{},
		own:    ownView{bits: make(map[string]*haveBits)},
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

// haveLearnID records an id at its index, held or, off the path, merely announced, and expands
// every peer HAVE that already covers that index into that peer's unwanted set and holder evidence.
// The first id recorded at an index stays.
//
// A HAVE names an index, not an id, so until the id is known the holder it evidences cannot be
// recorded, and a node's pulls for the message fall back on whoever announced it -- measured
// as the publisher's bytes rising 39% under A tuned before announced ids were learned here.
// Router goroutine, from the arrivals (Preprocess, and on the path the accept), what we forward
// or publish (haveNoteServable), the rebuild, and off the path handleIHave (announces:
// considerAnnounced).
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
	gid := seacastGID(root)
	for p, roots := range h.peers {
		if pe := roots[gid]; pe != nil && pe.Bits.Has(index) {
			gs.markUnwanted(p, csum)
		}
	}
}

// haveNoteArrival records a received (or own) message in our bitmap and learns its id, and
// reports whether the index is new to the bitmap: a claim (seacast-lite.md §4.2's on_validated,
// claim = not mine[g].has(i)). Router goroutine, from Preprocess -- before any forwarding
// decision for the message -- and from the publish paths.
func (gs *GossipSubRouter) haveNoteArrival(mid string) (claim bool) {
	h := gs.have
	if h == nil {
		return false
	}
	root, index, ok := h.index(mid)
	if !ok || len(root) != haveRootLen {
		return false
	}
	if h.tomb[root] > 0 {
		return false // past its active lifetime: only the tombstone is left (v0.5's D′)
	}
	e := h.mine[seacastGID(root)]
	if e == nil {
		e = &core.Holding{}
		h.mine[seacastGID(root)] = e
	}
	claim = !e.Bits.Has(index)
	e.Bits.Set(index)
	h.own.set(root, index)
	e.TTL = h.holdTTL
	gs.haveLearnID(mid)
	return claim
}

// haveIDontWantIDs returns mids with every structured id replaced by one HAVE per group root,
// carrying our whole bitmap for it (haveBitsFor, none while it is empty); other ids pass through
// in place. mids is not mutated.
func (gs *GossipSubRouter) haveIDontWantIDs(mids []string) []string {
	h := gs.have
	if h == nil {
		return mids
	}
	var out []string
	var seen map[string]bool
	for i, mid := range mids {
		root, _, ok := h.index(mid)
		if !ok || len(root) != haveRootLen || h.mine[seacastGID(root)] == nil {
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
			out = append(out, gs.seacastEncodeHave(root, bits, false))
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
	if gs.seacast != nil && len(mid) > len(haveMagic) && mid[:len(haveMagic)] == haveMagic && mid[len(haveMagic)] == seacastVersion {
		return gs.seacastAbsorbV2(p, mid) // the length test first: an id shorter than the magic is an ordinary one
	}
	if seacastPseudoUnknown(mid) {
		gs.seacastCounters().Malformed.Add(1)
		return true // an unknown version of the pseudo id: ignored, never an ordinary id
	}
	h := gs.have
	if h == nil {
		return false
	}
	root, enc, ok := decodeHave(mid)
	if !ok {
		return false
	}
	if gs.seacastBitsBeyond(root, enc) {
		gs.seacastCounters().Malformed.Add(1)
		return true // a bit at or beyond the type's IndexCount on a path root: malformed, ignored, counted
	}
	if gs.expired(root) {
		return true // a HAVE for a group past its lifetime is ignored, without penalty (v0.5's D′)
	}
	gs.haveTake(p, root, enc, false, false)
	return true
}

// haveTake absorbs a holding of root from p that passed the intake's checks, v1's or v4's (claimAll
// and complete are v4's claims): into p's entry with its lifetime refreshed, the holder counts, the
// ledger and the writer's copy, and the indices whose message we already know into p's unwanted set.
// The bitmap's trailing zero bytes go first (the wire drops them, core.Bits): a padded holding grows
// neither the entry nor the writer's copy past its highest index.
func (gs *GossipSubRouter) haveTake(p peer.ID, root string, enc []byte, claimAll, complete bool) {
	if gs.seacast != nil {
		if t, ok := gs.seacastRootOwner(root).(AttachedObject); ok {
			t.NoteHolding(root, p)
		}
	}
	enc = core.Bits(enc).Trim()
	h := gs.have
	newly := h.peers.Absorb(p, seacastGID(root), enc, claimAll, complete, gs.params.IDontWantMessageTTL)
	gs.seacastOriginAckIndices(root, newly) // v0.9: a chunk we publish is acknowledged (seacast_origin.go)
	if h.holders != nil {
		h.holders.add(root, newly)
	}
	if h.ledger != nil {
		h.ledger.have(p, root, newly, time.Now())
	}
	if h.cancel || h.projectNeed > 0 || gs.seacast != nil {
		// The writer's copy, for frames already queued to this peer: the cancel reads it, and so
		// does a projection, router-wide or per type (seacast_project.go), which counts what the
		// peer announced against the threshold.
		if q := gs.haveQueue(p); q != nil {
			q.haveTheirs(root, enc)
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
	}
}

// haveHeartbeat ages the bitmaps out on the IDONTWANT TTL. With clearIDontWantCounters.
func (gs *GossipSubRouter) haveHeartbeat() {
	h := gs.have
	if h == nil {
		return
	}
	for root, n := range h.tomb {
		if n--; n <= 0 {
			delete(h.tomb, root)
			h.own.revive(root)
		} else {
			h.tomb[root] = n
		}
	}
	for gid, e := range h.mine {
		root := seacastGIDRoot(gid)
		if e.TTL--; e.TTL <= 0 {
			delete(h.mine, gid)
			h.own.forget(root)
			if gs.seacastRootOwner(root) != nil {
				gs.expireGroup(root) // D′ on the path: a tombstone until the window passes it
			}
			// The holder counts stay: they are the peers' live holdings, which go at their own TTL.
		}
	}
	for root, m := range h.mids {
		if m.ttl--; m.ttl <= 0 {
			delete(h.mids, root)
		}
	}
	h.pushed.Age()
	h.peers.Age(func(p peer.ID, g core.GroupID, held core.Bits) {
		root := seacastGIDRoot(g)
		h.holders.remove(root, held.Indices()) // p is no longer a live holder of what it held
		if gs.p != nil {
			if q, ok := gs.p.peers[p]; ok {
				q.haveForgetRoot(root)
			}
		}
	})
}

// haveForget drops what a departed peer told us, and what we pushed to it: it leaves the holders of
// what it held.
func (gs *GossipSubRouter) haveForget(p peer.ID) {
	if gs.have != nil {
		for g, e := range gs.have.peers[p] {
			gs.have.holders.remove(seacastGIDRoot(g), e.Bits.Indices())
		}
		gs.have.peers.Forget(p)
		gs.have.pushed.Forget(p)
	}
}

// --- Dispatch-time cancel and attach, on the per-peer queue ---
//
// The router decides what to send when it enqueues; the writer sends it later, and between the
// two a HAVE can arrive that makes the frame a duplicate. The per-peer rpcQueue is the one object
// the router and the writer already share under one mutex, so the peer's HAVE state lives there
// and the writer reads it at Pop: a queued frame the peer now holds is dropped instead of
// written, and a HAVE we have not yet told the peer rides whatever RPC leaves next -- an urgent
// control RPC, or the data frame about to be written, so it is fresh at the instant it leaves
// rather than stale by the depth of the queue (behind data it is late).

// queueHave is one peer's HAVE state as the writer sees it. Every field is under queueMu.
// pendHave is a holding waiting to ride the next RPC out: the bitmap and its encoding, v4 with
// the flags on SEACAST's roots and v1 otherwise, decided on the router goroutine at the pend
// (seacastHaveFlags), since the queue's goroutine may not read the router's registry.
type pendHave struct {
	bits  *haveBits
	v4    bool
	flags byte
}

type queueHave struct {
	// index maps a queued message to its group root and index; ok is false for other ids.
	index func(msg *pb.Message) (root string, index uint32, ok bool)
	// theirs is what the peer has told us it holds, per root.
	theirs map[string]*haveBits
	// pending is what we have not told the peer yet, per root, to attach to the next RPC out.
	pending  map[string]*pendHave
	cancelOn bool
	attachOn bool
	// probeAnswers is our answers to the peer's probes (seacast.md §4.6, R5) not yet written, per
	// the chunk's own group: card 8 lets each through once, whatever the peer's holding says, and the
	// answer's write spends it. probeWaits is our own waiting probes of the peer, by their WANT id:
	// the writer keeps them though we hold the cell by now (probe_waits). Both under queueMu, set by
	// the router.
	probeAnswers map[string]*haveBits
	probeWaits   map[string]struct{}
	// probeSettled is our probes of the peer judged or given no verdict: one still queued is not written
	// (probe_settled). Under queueMu, set by the router.
	probeSettled map[string]struct{}
	// Projection at dispatch (WithHaveDispatchProjection): popped is what
	// the writer has sent the peer per root; a frame is dropped at pop when what the peer said it
	// holds plus projectFanIn times what we sent it since would already complete it.
	projectNeed  uint32
	projectFanIn int
	popped       map[string]*haveBits
	// projectOf, when set, is the projection per object type (seacast_project.go, §4.9): for a
	// queued message of the path it gives the type's threshold and fan-in (0: nothing projected,
	// the type having no code) and ok; off the path, ok false and the router-wide values apply.
	projectOf func(m *pb.Message) (need uint32, fanIn int, ok bool)
	// holders, when set, ranks queued data at Pop: fewest announced holders first.
	holders func(m *pb.Message) (int, bool)
	// ownFirst is §4.12's choose (choose.go); ownPickCtr the router's counter of its picks.
	ownFirst   bool
	ownPickCtr *atomic.Int64
	// onDispatch, when set, sees every data frame the writer pops for this peer (the ledger).
	onDispatch func(m *pb.Message)
	// ownHeld, when set, reports whether we hold the frame an ask id names: dispatch drops such
	// asks (enableStaleAsks).
	ownHeld func(mid string) bool
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
	if h == nil || (!h.cancel && !h.attach && h.projectNeed == 0 && gs.seacast == nil) || gs.p == nil {
		return nil
	}
	q, ok := gs.p.peers[p]
	if !ok {
		return nil
	}
	q.enableHave(h.msgIndex, h.cancel, h.attach)
	if gs.seacast != nil {
		q.queueMu.Lock()
		if q.onProbeWritten == nil && gs.seacast.machine {
			q.onProbeWritten = func(mids []string) { // off the writer, to the router
				go func() {
					select {
					case gs.p.eval <- func() { gs.seacastProbeWritten(p, mids) }:
					case <-gs.p.ctx.Done():
					}
				}()
			}
		}
		if q.onHoseExpire == nil {
			q.onHoseExpire = func(root string, idx uint32) { // off the queue's lock, to the router
				go func() {
					select {
					case gs.p.eval <- func() { gs.seacastOriginAck(root, idx) }:
					case <-gs.p.ctx.Done():
					}
				}()
			}
		}
		q.queueMu.Unlock()
	}
	// Asks for frames held by now leave no more (§7.2).
	q.enableStaleAsks(h.ownHeldID)
	if h.rarest {
		q.enableRarest(h.haveHolders)
	}
	if h.ownFirst {
		q.enableChoose(h.ownFirst, &h.ownPicks)
	}
	if h.projectNeed > 0 {
		q.enableProjection(h.projectNeed, h.projectFanIn)
	}
	if fn := gs.seacastProjection(); fn != nil {
		q.enableTypeProjection(fn) // the projection per object type on the path's roots (§4.9)
	}
	if h.ledger != nil {
		led, index := h.ledger, h.msgIndex
		q.enableDispatchHook(func(m *pb.Message) {
			if root, i, ok := index(m); ok {
				led.pop(p, root, i, sha256.Sum256(m.GetData()), time.Now())
			}
		})
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
	pends := make([]*pendHave, 0, len(ids))
	for _, id := range ids {
		root, ok := seacastHaveRoot(id) // a v1 or a v4 holding
		if !ok {
			return false
		}
		b := h.haveBitsFor(root)
		if b == nil {
			return false
		}
		roots = append(roots, root)
		// The encoding is decided here, on the router goroutine: v4 with the flags on the path's
		// roots (a claim the id carried is kept), v1 otherwise; the queue only writes it out.
		claimAll := false
		if _, ca, _, _, isV4, _ := decodeHaveV2(id); isV4 {
			claimAll = ca
		}
		v4, flags := gs.seacastHaveFlags(root, b, claimAll)
		pends = append(pends, &pendHave{bits: newHaveBits(append([]byte(nil), b.Bytes()...)), v4: v4, flags: flags})
	}
	q := gs.haveQueue(p)
	if q == nil || !q.havePend(roots, pends) {
		return false
	}
	if gs.seacast != nil { // the parked holdings' ordering checks (seacast_order.go)
		for i, root := range roots {
			gs.seacastOrderHolding(root, pends[i].v4 && pends[i].flags&seacastFlagClaimAll != 0)
		}
	}
	return true
}

// enableHave installs the writer-side HAVE state once.
func (q *rpcQueue) enableHave(index func(*pb.Message) (string, uint32, bool), cancel, attach bool) {
	q.queueMu.Lock()
	defer q.queueMu.Unlock()
	if q.have == nil {
		q.have = &queueHave{index: index, theirs: make(map[string]*haveBits), pending: make(map[string]*pendHave), popped: make(map[string]*haveBits), cancelOn: cancel, attachOn: attach}
	}
}

// enableProjection installs the dispatch-time projection once (WithHaveDispatchProjection).
// enableStaleAsks installs the dispatch-time drop of asks for frames we hold by now (the spec's
// §7.2): an ask queued behind data while its frame arrived from someone else would otherwise leave
// after our HAVE for the frame, which overtakes it on the urgent lane.
func (q *rpcQueue) enableStaleAsks(held func(mid string) bool) {
	q.queueMu.Lock()
	defer q.queueMu.Unlock()
	if q.have != nil && q.have.ownHeld == nil {
		q.have.ownHeld = held
	}
}

func (q *rpcQueue) enableProjection(need uint32, fanIn int) {
	q.queueMu.Lock()
	defer q.queueMu.Unlock()
	if q.have != nil && q.have.projectNeed == 0 {
		q.have.projectNeed, q.have.projectFanIn = need, fanIn
	}
}

// enableTypeProjection installs the projection per object type once (seacast_project.go).
func (q *rpcQueue) enableTypeProjection(projectOf func(m *pb.Message) (uint32, int, bool)) {
	q.queueMu.Lock()
	defer q.queueMu.Unlock()
	if q.have != nil && q.have.projectOf == nil {
		q.have.projectOf = projectOf
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

// enableDispatchHook installs the writer's dispatch observer once.
func (q *rpcQueue) enableDispatchHook(fn func(*pb.Message)) {
	q.queueMu.Lock()
	defer q.queueMu.Unlock()
	if q.have != nil && q.have.onDispatch == nil {
		q.have.onDispatch = fn
	}
}

// rarestIndex picks, among the data RPCs ahead of the next control-only RPC in the normal lane,
// the one whose messages the fewest peers have announced (core.Rarest); -1 when the choice is the
// head (FIFO, or nothing to choose among). An entry counts its fewest holders, since it can carry
// several messages; one whose holders are all unknown (a stock message) is not a candidate. Under
// queueMu.
func (h *queueHave) rarestIndex(normal []*RPC) int {
	if h.holders == nil || len(normal) < 2 || len(normal[0].Publish) == 0 {
		return -1
	}
	lead := replayLead(normal, h.index)
	best := core.Rarest(len(normal), func(i int) (int, core.EntryKind) {
		rpc := normal[i]
		if len(rpc.Publish) == 0 {
			return 0, core.Stop // control keeps its place: nothing behind it may overtake it
		}
		if rpc.replay && i >= lead {
			return 0, core.Skip // an owed answer behind a stock entry or control waits for it (replayLead)
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
			return 0, core.Skip
		}
		return holders, core.Candidate
	})
	if best <= 0 {
		return -1
	}
	return best
}

// AsksForHeldDropped counts asks by index dropped because the asker's own HAVE named the index (the
// ask was stale), and StaleAsksDropped the ask ids a writer dropped at dispatch because we hold the
// frame by now. Process-wide, for a harness.
var AsksForHeldDropped, StaleAsksDropped atomic.Int64

// havePeerHolds reports whether p's HAVE for root names index. Router goroutine.
func (gs *GossipSubRouter) havePeerHolds(p peer.ID, root string, index uint32) bool {
	h := gs.have
	if h == nil {
		return false
	}
	e := h.peers.Of(p, seacastGID(root))
	return e != nil && e.Bits.Has(index)
}

// ownView mirrors what we hold (haveState.mine) for the writers, which drop at dispatch the asks
// for frames we have received since they were queued (the spec's §7.2) and run off the router's
// goroutine, where mine lives.
type ownView struct {
	mu   sync.RWMutex
	bits map[string]*haveBits
	// dead is the groups past their active lifetime (v0.5's D′): every index reads as held, so a
	// writer drops the asks for them.
	dead map[string]bool
}

// kill marks root past its lifetime; revive forgets its tombstone.
func (o *ownView) kill(root string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	delete(o.bits, root)
	if o.dead == nil {
		o.dead = make(map[string]bool)
	}
	o.dead[root] = true
}

func (o *ownView) revive(root string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	delete(o.dead, root)
}

func (o *ownView) set(root string, index uint32) {
	o.mu.Lock()
	defer o.mu.Unlock()
	b := o.bits[root]
	if b == nil {
		b = &haveBits{}
		o.bits[root] = b
	}
	b.Set(index)
}

func (o *ownView) forget(root string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	delete(o.bits, root)
}

func (o *ownView) has(root string, index uint32) bool {
	o.mu.RLock()
	defer o.mu.RUnlock()
	if o.dead[root] {
		return true
	}
	b := o.bits[root]
	return b != nil && b.Has(index)
}

// ownHeldID reports whether we hold the frame an ask id names: an ask by index (v1 or v4), or a
// frame's id. Any goroutine.
func (h *haveState) ownHeldID(mid string) bool {
	root, index, _, _, ok, malformed := decodeWantV2(mid) // a v4 WANT names its cell as v1's does
	if malformed {
		return false
	}
	if !ok {
		root, index, ok = decodeWant(mid)
	}
	if !ok {
		if root, index, ok = h.index(mid); !ok || len(root) != haveRootLen {
			return false
		}
	}
	return h.own.has(root, index)
}

// probeAnswer reports whether m is an unwritten answer to the peer's probe (seacast.md §4.6, R5).
// Under queueMu.
func (h *queueHave) probeAnswer(m *pb.Message) bool {
	if h.index == nil {
		return false
	}
	root, index, ok := h.index(m)
	if !ok {
		return false
	}
	b := h.probeAnswers[root]
	return b != nil && b.Has(index)
}

// probeAnswerSpent clears m's probe-answer exemption once m is written. Under queueMu.
func (h *queueHave) probeAnswerSpent(m *pb.Message) {
	root, index, ok := h.index(m)
	if !ok {
		return
	}
	if b := h.probeAnswers[root]; b != nil {
		b.Clear(index)
		if b.Count() == 0 {
			delete(h.probeAnswers, root)
		}
	}
}

// dropHeldAsks returns rpc without the ask ids held reports, and how many it dropped; nil when
// nothing worth writing is left. held is asked once per id. Never mutates rpc.
func dropHeldAsks(rpc *RPC, held func(string) bool) (*RPC, int) {
	n := 0
	var iwants []*pb.ControlIWant
	for _, w := range rpc.Control.GetIwant() {
		var kept []string
		for _, mid := range w.GetMessageIDs() {
			if held(mid) {
				n++
				continue
			}
			kept = append(kept, mid)
		}
		if len(kept) > 0 {
			iwants = append(iwants, &pb.ControlIWant{MessageIDs: kept})
		}
	}
	if n == 0 {
		return rpc, 0
	}
	c := rpc.Control
	ctl := &pb.ControlMessage{Ihave: c.Ihave, Iwant: iwants, Graft: c.Graft, Prune: c.Prune, Idontwant: c.Idontwant, Extensions: c.Extensions}
	out := shallowRPCCopy(rpc)
	out.Control = ctl
	if len(out.Publish) == 0 && rpcOtherwiseEmpty(out) {
		return nil, n
	}
	return out, n
}

// haveTheirs folds the peer's announced bitmap in. Router goroutine.
func (q *rpcQueue) haveTheirs(root string, enc []byte) {
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
	q.hoseAckedLocked(root, b.Or(enc))
}

// haveForgetRoot drops an aged-out root on both sides.
func (q *rpcQueue) haveForgetRoot(root string) {
	q.queueMu.Lock()
	defer q.queueMu.Unlock()
	if q.have != nil {
		delete(q.have.theirs, root)
		delete(q.have.pending, root)
		delete(q.have.popped, root)
	}
}

// havePend parks bitmaps to ride the next RPC out, all or none, under the one lock that also
// guards the check that something is queued to carry them.
func (q *rpcQueue) havePend(roots []string, pends []*pendHave) bool {
	q.queueMu.Lock()
	defer q.queueMu.Unlock()
	if q.have == nil || !q.have.attachOn || q.queue.Len() == 0 {
		return false
	}
	for i, root := range roots {
		q.have.pending[root] = pends[i]
	}
	return true
}

// filter returns rpc without the messages the peer has announced it holds, and how many it
// dropped; nil when nothing worth writing is left. Never mutates rpc: a data RPC is one shared
// object across every recipient.
func (h *queueHave) filter(rpc *RPC) (out *RPC, cancelled, projected int) {
	if len(rpc.Publish) == 0 || (!h.cancelOn && h.projectNeed == 0 && h.projectOf == nil) {
		return rpc, 0, 0
	}
	// A solicited frame (an IWANT answer) is cancelled like any other when the peer has since
	// announced it — the answer is stale — but never projected away: the ask is the peer's own
	// word that it lacks the frame, whatever the projection computes. Projecting answers made a
	// peer re-ask for what it had asked for and never got.
	// (R5) an answer to the peer's probe passes the cancel once (probeAnswer); a push of the same
	// chunk does not.
	exempt := make([]bool, len(rpc.Publish))
	if rpc.solicited && len(h.probeAnswers) > 0 {
		for k, m := range rpc.Publish {
			exempt[k] = h.probeAnswer(m)
		}
	}
	cancel := func(k int, m *pb.Message) bool { return h.cancelOn && !exempt[k] && h.held(m) }
	drop := func(k int, m *pb.Message) bool {
		if cancel(k, m) {
			return true
		}
		if rpc.solicited {
			return false
		}
		return h.projectedComplete(m)
	}
	for k, m := range rpc.Publish {
		if cancel(k, m) {
			cancelled++
		} else if drop(k, m) {
			projected++
		}
	}
	for k, m := range rpc.Publish {
		if exempt[k] {
			h.probeAnswerSpent(m) // written: the exemption is spent, once
		}
	}
	if cancelled+projected == 0 {
		return rpc, 0, 0
	}
	kept := make([]*pb.Message, 0, len(rpc.Publish)-cancelled-projected)
	for k, m := range rpc.Publish {
		if drop(k, m) {
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
	b := h.theirs[root]
	return b != nil && b.Has(index)
}

// projectedComplete reports whether the peer is projected to complete m's group without m
// (core.Projected): what it said it holds plus fan-in times what we have sent it beyond that
// reaches need.
func (h *queueHave) projectedComplete(m *pb.Message) bool {
	need, fanIn := h.projection(m)
	if need == 0 {
		return false
	}
	root, _, ok := h.index(m)
	if !ok {
		return false
	}
	var theirs, ours haveBits
	if b := h.theirs[root]; b != nil {
		theirs = *b
	}
	if b := h.popped[root]; b != nil {
		ours = *b
	}
	return core.Projected(theirs, ours, int(need), fanIn)
}

// projection is the threshold and fan-in that apply to m: its type's where the path projects per
// type (projectOf), the router-wide values otherwise.
func (h *queueHave) projection(m *pb.Message) (need uint32, fanIn int) {
	if h.projectOf != nil {
		if n, f, ok := h.projectOf(m); ok {
			return n, f
		}
	}
	return h.projectNeed, h.projectFanIn
}

// notePopped records that the writer sent m to the peer, for the projection.
func (h *queueHave) notePopped(m *pb.Message) {
	if need, _ := h.projection(m); need == 0 {
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
	// A control extension is content too: an RPC left with one is sent (the second check of part
	// 4's gate, finding 21: a filtered RPC with an extension was dropped whole).
	return len(rpc.Subscriptions) == 0 && rpc.Partial == nil && rpc.TestExtension == nil &&
		(c == nil || (len(c.Ihave) == 0 && len(c.Iwant) == 0 && len(c.Graft) == 0 && len(c.Prune) == 0 && len(c.Idontwant) == 0 && c.Extensions == nil))
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
		e := h.pending[root]
		if e.v4 {
			ids = append(ids, encodeHaveV2(root, e.bits, e.flags&seacastFlagClaimAll != 0, e.flags&seacastFlagComplete != 0))
		} else {
			ids = append(ids, encodeHave(root, e.bits))
		}
	}
	clear(h.pending)
	return mergedControl(rpc, &RPC{RPC: pb.RPC{Control: &pb.ControlMessage{Idontwant: []*pb.ControlIDontWant{{MessageIDs: ids}}}}})
}

// --- The IHAVE bitmap: two bitmaps, not one ---
//
// IDONTWANT and IHAVE both say "I have X" and mean different things, so each has its own bitmap
// and each bitmap inherits its message's semantics, emission point and lane. The IHAVE bitmap's
// bit is true once the message is validated and in mcache -- servable -- and it is emitted where
// IHAVE is: the phase announce and the heartbeat gossip, in the normal lane,
// whose FIFO wait behind our data is the scheme's only per-link load signal. A receiver may ask
// for an index it learns of this way; when it does not yet know the id, it asks by index with a
// WANT pseudo id, and the holder resolves that through its ids map.

const (
	wantMagic = "WANT"
	wantLen   = len(wantMagic) + 1 + haveRootLen + 4
)

// encodeWant is the pseudo id of an ask by index: magic ‖ version ‖ root ‖ index, little-endian,
// as seacast-lite.md §2's WantIndex.
func encodeWant(root string, index uint32) string {
	out := make([]byte, 0, wantLen)
	out = append(out, wantMagic...)
	out = append(out, haveVersion)
	out = append(out, root...)
	out = append(out, byte(index), byte(index>>8), byte(index>>16), byte(index>>24))
	return string(out)
}

// decodeWant reads an ask by index, or reports that mid is an ordinary id.
func decodeWant(mid string) (root string, index uint32, ok bool) {
	if len(mid) != wantLen || mid[:len(wantMagic)] != wantMagic || mid[len(wantMagic)] != haveVersion {
		return "", 0, false
	}
	root = mid[len(wantMagic)+1 : len(wantMagic)+1+haveRootLen]
	b := []byte(mid[wantLen-4:])
	return root, uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24, true
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

// haveBitsFor is the bitmap our HAVE carries for root: for a group we publish on the path the
// chunks acknowledged so far, else what we hold; nil when there is nothing.
func (h *haveState) haveBitsFor(root string) *haveBits {
	if b, ok := h.originAcks.bits(root); ok {
		return b // a group we publish on the path: the chunks acknowledged so far (v0.9)
	}
	if e := h.mine[seacastGID(root)]; e != nil {
		return &e.Bits
	}
	return nil
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
	e, made := h.pushed.Entry(p, seacastGID(root))
	if made {
		e.First = time.Now()
	} else if gs.v05OfferSkipped() && e.Bits.Count() == 0 {
		// Every push counted to p was skipped or refused (v0.5's A′): the guard's clock starts
		// again at this one, which p can receive.
		e.First = time.Now()
	}
	e.Bits.Set(index)
	e.TTL = h.pushTTL
	if gs.v05OfferSkipped() {
		if q, ok := gs.p.peers[p]; ok {
			q.unowe(root, index) // pushed now: no longer owed as an offer
		}
	}
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
		if gs.expired(root) {
			// A group past its lifetime, or dropped INVALID, is offered to nobody (v0.5's D′,
			// seacast-lite.md's drop_group): the gossip history and paced jobs may still name it (the
			// review of part 4's gate, finding 5).
			if gs.seacast != nil {
				gs.seacast.ctr.ExpiredOffersSkipped.Add(1)
			}
			continue
		}
		b := bits[root]
		if pe := h.pushed.Of(p, seacastGID(root)); pe != nil {
			b = b.AndNot(&pe.Bits)
		}
		if pe := h.peers.Of(p, seacastGID(root)); pe != nil {
			b = b.AndNot(&pe.Bits)
		}
		if len(b.Bytes()) == 0 {
			continue
		}
		out = append(out, encodeHave(root, b))
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
		e := h.mine[seacastGID(root)]
		return e != nil && e.Bits.Has(index)
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
	mine := h.mine[seacastGID(root)]
	known := h.mids[root]
	var out []string
	for i := 0; i < len(theirs)*8; i++ {
		index := uint32(i) // lint:ignore uintcast -- bounded by haveMaxBytes*8.
		if !theirs.Has(index) || (mine != nil && mine.Bits.Has(index)) {
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
// be served: the index is unknown or not held. An ordinary id comes back as itself, except a
// chunk's id on the path (haveResolveID).
func (gs *GossipSubRouter) haveResolveWant(mid string) (real string, skip bool) {
	root, index, ok := decodeWant(mid)
	if !ok {
		return gs.haveResolveID(mid), false
	}
	h := gs.have
	if h == nil {
		return "", true
	}
	if m := h.mine[seacastGID(root)]; m == nil || !m.Bits.Has(index) {
		return "", true
	}
	known := h.mids[root]
	if known == nil {
		return "", true
	}
	real, ok = known.ids[index]
	return real, !ok
}

// haveResolveID is an ask by a chunk's id on the path, resolved as an ask by its index is: to the id
// we hold at that index, when we hold it (seacast-lite.md §4.5's resolve, [c7]: our chunk at that
// index, received or rebuilt). Two ids can name one index when an id carries the content's hash, so
// an ask under another id of the index, as a forged offer could have taught the asker, is answered
// with our chunk. An id whose index we don't hold, any id off the path, and an id of no group come
// back as themselves.
func (gs *GossipSubRouter) haveResolveID(mid string) string {
	h := gs.have
	if h == nil || gs.seacast == nil {
		return mid
	}
	root, index, ok := h.index(mid)
	if !ok || len(root) != haveRootLen {
		return mid
	}
	if m := h.mine[seacastGID(root)]; m == nil || !m.Bits.Has(index) {
		return mid
	}
	if known := h.mids[root]; known != nil {
		if id, ok := known.ids[index]; ok {
			return id
		}
	}
	return mid
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
	gs.iwantDelivered(want)
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

// --- Rarest-first at dispatch ---
//
// The rarity-ordered drain ranks a frame when the router enqueues it and admits the rank goes
// stale while queued; measured, it changed nothing, most likely because nothing was
// scarce where it was measured. With the HAVE bitmaps the writer can make the same choice at
// dispatch from live evidence: how many peers have announced each index. The counts live in one
// small table the router writes on every HAVE and the writer reads at Pop, under its own lock,
// never the router's. Control RPCs keep their FIFO position -- a frame is chosen only among the
// data ahead of the next control RPC -- so IHAVE's queueing delay is
// untouched by construction.

// holderCounts is peers known to hold each index of each root, from their HAVE bitmaps: the live
// holders (seacast-lite.md §4.3's holders). Each index a peer newly holds counts once (haveAbsorb),
// and a peer leaves the counts when its holding of the group ages out or its connection closes
// (haveHeartbeat, haveForget), so a root's counts are always its group's live holdings; the root
// goes with its last holder. The router writes it, and the writer reads it at Pop under its own lock,
// one count at a time.
type holderCounts struct {
	mu     sync.Mutex
	counts map[string][]int32
}

// remove takes one holder out of each of indices of root, and the root once it has none left.
func (c *holderCounts) remove(root string, indices []uint32) {
	if c == nil || len(indices) == 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	arr := c.counts[root]
	for _, i := range indices {
		if int(i) < len(arr) && arr[i] > 0 {
			arr[i]--
		}
	}
	for _, n := range arr {
		if n > 0 {
			return
		}
	}
	delete(c.counts, root)
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
// nothing in the router reads it, and it is not aged; a harness empties it per message
// (ResetHaveLedger).

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

// ResetHaveLedger empties the dispatch ledger, when it is kept. A harness that publishes several
// messages calls it as each one starts, once the previous message's readers are done.
func (p *PubSub) ResetHaveLedger() {
	gs, ok := p.rt.(*GossipSubRouter)
	if !ok || gs.have == nil || gs.have.ledger == nil {
		return
	}
	l := gs.have.ledger
	l.mu.Lock()
	defer l.mu.Unlock()
	l.entries = make(map[peer.ID]map[string]map[uint32]*HaveLedgerEntry)
}

// fullHaveBits is the bitmap with every index below count set.
func fullHaveBits(count uint32) *haveBits {
	var b haveBits
	for i := uint32(0); i < count; i++ {
		b.Set(i)
	}
	return &b
}

// haveOrigin sends our HAVE for each group in a publish, once, to the topic's mesh: the HAVE a
// source sends before its data, so no relay pushes its frames back. Preprocess no longer sends one
// per message as each is added to a batch, N growing HAVEs before the data. A group we publish on
// the path is left out: its holding grows at each acknowledgement (seacast_origin.go). Router
// goroutine.
func (gs *GossipSubRouter) haveOrigin(msgs []*Message) {
	h := gs.have
	if h == nil {
		return
	}
	sent := make(map[string]bool)
	for _, msg := range msgs {
		if len(msg.GetData()) < gs.params.IDontWantMessageThreshold {
			continue // only where stock IDONTWANT would have gone, above its threshold
		}
		root, _, ok := h.index(gs.p.idGen.ID(msg))
		if !ok || len(root) != haveRootLen || sent[root] || h.mine[seacastGID(root)] == nil {
			continue
		}
		if _, own := h.originAcks.bits(root); own {
			continue // the source's holding grows at each acknowledgement instead (seacast_origin.go)
		}
		sent[root] = true
		mid := gs.seacastEncodeHave(root, &h.mine[seacastGID(root)].Bits, false) // v4 on the path's roots, v1 otherwise
		to := make([]peer.ID, 0, len(gs.mesh[msg.GetTopic()]))
		for p := range gs.mesh[msg.GetTopic()] {
			to = append(to, p)
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
		if gs.expired(rt) {
			continue // the group passed its lifetime while it decoded (v0.5's D′)
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
			ids := gs.haveIHaveIDs(pid, mids)
			if len(ids) == 0 {
				continue
			}
			ihave := rpcWithControl(nil, []*pb.ControlIHave{{TopicID: &topic, MessageIDs: ids}}, nil, nil, nil, nil)
			gs.sendRPC(pid, ihave, false)
			r.announcedTo.Add(1)
			for _, id := range ids {
				if _, b, ok := decodeHave(id); ok {
					hb := haveBits([]byte(b))
					r.announced.Add(int64(hb.Count()))
				}
			}
		}
	case RebuiltPush:
		for _, pid := range gs.pushOrder(gs.mesh[topic]) {
			var known haveBits
			if e := h.peers.Of(pid, seacastGID(root)); e != nil {
				known.Or(e.Bits.Bytes())
			}
			if e := h.pushed.Of(pid, seacastGID(root)); e != nil {
				known.Or(e.Bits.Bytes())
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
			shuffleStrings(pool)
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
