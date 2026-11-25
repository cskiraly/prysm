package pubsub

import (
	cryptorand "crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
)

// --- Version 0.5 of the protocol text: the rules its model check added ---
//
// The specification's v0.5 (seacast-spec 5a729ff, from a model check of v0.4) added
// rules this implementation predated; its §9 lists them. Each is a switch here, so they can be taken
// one at a time, and with every switch off the router behaves as before them:
//
//   - PromiseAtWrite (E, §6.1, §7.2): the promise behind the park starts when the writer writes the
//     ask, not when the ask is decided. An ask that waits behind the window, or that a full queue
//     refuses, opens no promise, so no peer is parked for an ask it never received.
//   - OfferSkipped (A′, §5.2, §7.2): a push that does not happen -- skipped by projection at the
//     writer, or refused by a full queue -- is no longer counted as pushed, and the chunk is offered
//     to the peer instead if it is in the mesh. An offer a full queue refuses is owed, as is an
//     answer: the next write to the peer carries what is owed. The guard's clock restarts once
//     every push counted to the peer has been skipped or refused.
//   - LastResort (B′, §6.1): every offer is kept, per connection and group; the retry tries the
//     announcers in announcement order, the ordered list first; and when every announcer of a chunk
//     is parked, each is asked once more, after PROMISE × 2ⁿ for the n-th such ask of the chunk.
//   - CloseWithConn (§6.1, §8.1): a peer's promises, offers, tried asks and last-resort counts go
//     with its connection; the retry moves on to the other announcers.
//
// D′'s expiry (§6.1, §7.2, §8.1) runs on SEACAST's path for the path's roots (expireGroup below,
// seacast_window.go): a group whose holdings aged out leaves a tombstone, and its pull state and
// its queued frames, offers and asks go at once. Its switch for every group, and C′'s ledger, are
// not in this fork.

// SpecV05 selects the v0.5 rules; the zero value is the implementation as measured before them.
type SpecV05 struct {
	PromiseAtWrite bool
	OfferSkipped   bool
	LastResort     bool
	CloseWithConn  bool
}

// SpecV05All is every v0.5 rule this package implements.
var SpecV05All = SpecV05{PromiseAtWrite: true, OfferSkipped: true, LastResort: true, CloseWithConn: true}

// SpecV05Counters counts what the v0.5 rules did; an application may share one across nodes.
type SpecV05Counters struct {
	// PromisesAtWrite is the promises opened by a written ask; AsksLost the asks a class-loss
	// injection dropped after the write, which open theirs too.
	PromisesAtWrite, AsksLost atomic.Int64
	// Unpushed is the pushes no longer counted (projection skips and full-queue refusals);
	// OffersOwed the indices owed to mesh peers as offers; OwedSent the owed indices the writer
	// attached to an RPC it popped (before its write); AnswersOwed the answers of chunks a full
	// queue refused, owed to be requeued.
	Unpushed, OffersOwed, OwedSent, AnswersOwed atomic.Int64
	// LastResortsArmed is the last-resort timers armed; LastResortsDeferred the times a timer
	// found an ask of the chunk outstanding at every candidate and looked again later;
	// LastResortAsks the asks they tried (counted before the queue takes them), of which
	// ParkedAsked went to a parked announcer and the queue took; BeyondList the asks (retries and
	// last resorts) whose announcer was known only from the per-group offers, past the ordered list.
	LastResortsArmed, LastResortsDeferred, LastResortAsks, ParkedAsked, BeyondList atomic.Int64
	// ClosedPromises is the promises closed with their connection.
	ClosedPromises atomic.Int64
	// ScorePromisesAtWrite is the score's promises opened by a written ask under E (the gossip
	// tracer's, which the heartbeat charges as the behaviour penalty); ClosedScorePromises those
	// of grouped asks closed with their connection under CloseWithConn.
	ScorePromisesAtWrite, ClosedScorePromises atomic.Int64
	// Expired is the groups that passed their lifetime under D′; ExpiredDiscards the queued
	// frames, offers and asks of such groups dropped from the queues.
	Expired, ExpiredDiscards atomic.Int64
}

type specV05 struct {
	SpecV05
	ctr *SpecV05Counters
	// offered is every offer, per connection and group (B′): the bitmap of indices offered, the
	// topic, and when the announcer last offered the group. Router goroutine.
	offered map[peer.ID]map[string]*offeredGroup
	// offerOrder is each id's ordered announcer list, the first offerPerID in announcement order.
	offerOrder map[string][]peer.ID
	// lrArmedKeys and askedParked are B′'s per-chunk state: whether the last resort's timer is
	// armed, and the parked announcers it has asked, and when.
	lrArmedKeys map[string]bool
	askedParked map[string]map[peer.ID]time.Time
	// orderKey is the secret of the node's own order of the announcers beyond a chunk's ordered list
	// (seacast-lite.md §6.1's own_order, §8.1; spec f6c19db), drawn at random at its first use and never
	// sent. Router goroutine.
	orderKey    [32]byte
	orderKeySet bool
}

// ownRank is the reference rank of p as an announcer of (root, index): the first eight bytes of
// SHA-256 over the node's secret, the group, the index and the peer id. Larger is tried first.
func (v *specV05) ownRank(root string, index uint32, p peer.ID) uint64 {
	if !v.orderKeySet {
		if _, err := cryptorand.Read(v.orderKey[:]); err != nil {
			panic(fmt.Sprintf("own order: drawing the secret: %v", err))
		}
		v.orderKeySet = true
	}
	h := sha256.New()
	h.Write(v.orderKey[:])
	h.Write([]byte(root))
	var ix [4]byte
	binary.BigEndian.PutUint32(ix[:], index)
	h.Write(ix[:])
	h.Write([]byte(p))
	return binary.BigEndian.Uint64(h.Sum(nil)[:8])
}

type offeredGroup struct {
	bits  haveBits
	topic string
	last  time.Time
}

// offeredRootsPerPeer bounds the groups whose offers are kept per connection: the live groups and
// those not yet tied to a block (the spec's MAX_PENDING_GROUPS) fit with room to spare.
const offeredRootsPerPeer = 64

// WithSpecV05 turns on the chosen v0.5 rules. Each needs what it changes: PromiseAtWrite and
// LastResort the commitment park (and LastResort the offer table and the park per group);
// OfferSkipped the HAVE bitmaps with the IHAVE side. Must follow those options. ctr may be nil.
func WithSpecV05(rules SpecV05, ctr *SpecV05Counters) Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok {
			return fmt.Errorf("spec v0.5 rules require a gossipsub router")
		}
		if (rules.PromiseAtWrite || rules.LastResort) && gs.iwantPromised == nil {
			return fmt.Errorf("spec v0.5 E and B′ require WithIHaveCommitmentPark first")
		}
		if rules.LastResort && (gs.offerTable == nil || gs.parkGroups == nil) {
			return fmt.Errorf("spec v0.5 B′ requires WithOfferTable and WithIHaveParkPerGroup first")
		}
		if rules.OfferSkipped && (gs.have == nil || !gs.have.ihaveOn) {
			return fmt.Errorf("spec v0.5 A′ requires WithHaveBitmaps and WithHaveAnnounceBitmaps first")
		}
		if ctr == nil {
			ctr = &SpecV05Counters{}
		}
		v := &specV05{SpecV05: rules, ctr: ctr}
		if rules.LastResort {
			v.offered = make(map[peer.ID]map[string]*offeredGroup)
			v.offerOrder = make(map[string][]peer.ID)
			v.lrArmedKeys = make(map[string]bool)
			v.askedParked = make(map[string]map[peer.ID]time.Time)
		}
		gs.v05 = v
		if rules.PromiseAtWrite || rules.OfferSkipped {
			ps.queueHooks = gs.v05QueueHooks
		}
		return nil
	}
}

func (gs *GossipSubRouter) v05PromiseAtWrite() bool { return gs.v05 != nil && gs.v05.PromiseAtWrite }
func (gs *GossipSubRouter) v05OfferSkipped() bool   { return gs.v05 != nil && gs.v05.OfferSkipped }
func (gs *GossipSubRouter) v05LastResort() bool     { return gs.v05 != nil && gs.v05.LastResort }

// v05LastResortFor reports whether B′ takes mid: a chunk, which names a group and an index. An id
// outside any group (a message of a stock topic, on a router the topics share) keeps the ordinary
// retry. The last resort is a rule for chunks (seacast-lite.md §6.1); on such an id, whose park is shared
// by every ungrouped id of the peer, a broken last-resort ask renewed the park of a peer that the
// ordinary retry leaves alone until it ends.
func (gs *GossipSubRouter) v05LastResortFor(mid string) bool {
	if !gs.v05LastResort() {
		return false
	}
	_, _, ok := gs.groupIndex(mid)
	return ok
}

// expired reports whether root is past its active lifetime: on SEACAST's path, the path's
// tombstone or a slot that left the live window (seacast_window.go).
func (gs *GossipSubRouter) expired(root string) bool {
	return gs.seacastRootExpired(root)
}

// expiredID reports whether the group mid names is past its lifetime.
func (gs *GossipSubRouter) expiredID(mid string) bool {
	if gs.seacast == nil {
		return false
	}
	root, _, ok := gs.groupIndex(mid)
	return ok && gs.expired(root)
}

// v05QueueHooks installs the writer's side of E and A′ on a new peer queue. pubsub's goroutine,
// as the queue is made.
func (gs *GossipSubRouter) v05QueueHooks(q *rpcQueue) {
	p := q.peer
	// A writer's report reaches the router later, on its event loop; by then the connection may have
	// closed (or been replaced by a new queue). A report from a queue no longer the peer's is dropped,
	// so nothing is recreated for a closed connection (its promises closed with it).
	current := func() bool { cur, ok := gs.p.peers[p]; return ok && cur == q }
	if gs.v05.PromiseAtWrite {
		q.onAsks = func(mids []string, at time.Time) {
			// Never block a writer on the router, which may be waiting on this queue's lock.
			go func() {
				select {
				case gs.p.eval <- func() {
					if current() {
						// The score's promise first (gossip_tracer.go), then the park's.
						gs.v05.ctr.ScorePromisesAtWrite.Add(int64(gs.gossipTracer.promisesWritten(p, mids, at)))
						gs.promisesFromWrite(p, mids, at)
					}
				}:
				case <-gs.p.ctx.Done():
				}
			}()
		}
		if gs.lossClass == lossClassIWant {
			// Under E an ask lost on the wire was written: the loss is drawn at the writer, after the
			// window and the queue, and the lost ask opens its promise as a delivered one does.
			q.askLoss = func(ids []string) (kept, lost []string) {
				for _, id := range ids {
					if gs.lossDecide(id, p) {
						lost = append(lost, id)
					} else {
						kept = append(kept, id)
					}
				}
				if n := int64(len(lost)); n > 0 {
					gs.v05.ctr.AsksLost.Add(n)
					if gs.lossDropped != nil {
						gs.lossDropped.Add(n)
					}
				}
				return kept, lost
			}
		}
	}
	if gs.v05.OfferSkipped {
		q.onSkipped = func(msgs []*pb.Message) {
			go func() {
				select {
				case gs.p.eval <- func() {
					if !current() {
						return
					}
					for _, m := range msgs {
						gs.unpush(p, m.GetTopic(), gs.p.idGen.RawID(m))
					}
				}:
				case <-gs.p.ctx.Done():
				}
			}()
		}
		q.owedSent = &gs.v05.ctr.OwedSent
		if gs.lossClass == lossClassIHave {
			// An owed offer leaves from the writer, past doSendRPC: the class loss must reach it.
			q.owedFilter = func(rpc *RPC) *RPC { return gs.applyClassLoss(rpc, p) }
		}
	}
}

// --- E: the promise starts at the write ---

// promisesFromWrite opens the park's promise for each ask the writer wrote to p at the time at, the
// write (after any send delay); its deadline is PROMISE after that. An ask re-written while its promise is open keeps
// the earlier deadline, as a re-ask did. Router goroutine.
func (gs *GossipSubRouter) promisesFromWrite(p peer.ID, mids []string, at time.Time) {
	if gs.iwantPromised == nil {
		return
	}
	wait := max(0, gs.parkPromise-time.Since(at))
	for _, mid := range mids {
		// Written: the promise is the outstanding ask from here. On the path, only the ask this write
		// carried: one dispatched before it and still queued.
		queued := gs.seacastAskWritten(p, mid, at)
		if _, _, _, _, v4, _ := decodeWantV2(mid); v4 {
			// A v4 WANT is a direct ask (a sampler's, a forwarder's, the probe's, the repair
			// sample's): it opens no offer promise (seacast.md §4.10, §3.4 item 10); the probe keeps
			// its own of DEFER_WINDOW (seacast_claims.go).
			continue
		}
		if gs.seacast != nil && !queued {
			// On the path, the report comes after the ask closed: delivered, answered with a tie,
			// declined or its offer dropped (F13), absolved, or its group expired. Nothing is owed
			// for it; a newer ask of the chunk, dispatched after this write, has its own report.
			continue
		}
		if gs.haveSeen(mid) || gs.haveHolds(mid) || gs.seacastGroupStopped(mid) || gs.expiredID(mid) {
			// Arrived, rebuilt by a decode (held, its id not marked seen), its group completed (its
			// promises closed then, and a written ask stays queued until its report), or its group
			// died, between the write and this event: nothing is owed.
			continue
		}
		peers := gs.iwantPromised[mid]
		if peers == nil {
			peers = make(map[peer.ID]time.Time)
			gs.iwantPromised[mid] = peers
		}
		prev, open := peers[p]
		if open && !at.Before(prev) {
			continue // the earlier promise keeps its deadline (reports may arrive out of order)
		}
		peers[p] = at // an earlier report moves the deadline back; the later timer finds nothing
		if !open {
			gs.v05.ctr.PromisesAtWrite.Add(1)
		}
		m, f, decided := mid, p, gs.iwantAsked[mid]
		time.AfterFunc(wait, func() {
			select {
			case gs.p.eval <- func() { gs.promiseDue(m, f, at, decided) }:
			case <-gs.p.ctx.Done():
			}
		})
	}
}

// --- A′: a push that does not happen is offered instead ---

// unpush forgets a push to p of mid that will not be made, and owes p the offer when it is a
// mesh peer of the topic or, at the source of mid's group, a peer of the pool the push was planned
// from (seacast.md §4.7's unpush and source_pool, spec 43fe953: with no mesh peer, the topic's
// links). Any other push outside the mesh (the source's wide first hop beside a mesh) is forgotten
// but not offered. Router goroutine.
func (gs *GossipSubRouter) unpush(p peer.ID, topic, mid string) {
	h := gs.have
	root, index, ok := h.index(mid)
	if !ok || len(root) != haveRootLen || gs.expired(root) {
		return
	}
	if pe := h.pushed[p]; pe != nil {
		if e := pe[root]; e != nil && e.bits.Has(index) {
			e.bits.Clear(index)
		}
	}
	gs.v05.ctr.Unpushed.Add(1)
	if _, inMesh := gs.mesh[topic][p]; !inMesh && !gs.seacastSourceOwes(p, topic, root) {
		return
	}
	var b haveBits
	b.Set(index)
	gs.owe(p, topic, root, &b)
}

// owe records bits of root as owed to p: the writer's next write to p carries them as an IHAVE.
// Router goroutine.
func (gs *GossipSubRouter) owe(p peer.ID, topic, root string, bits *haveBits) {
	if gs.expired(root) {
		return // a group past its lifetime is offered to nobody (D′)
	}
	q, ok := gs.p.peers[p]
	if !ok {
		return
	}
	gs.v05.ctr.OffersOwed.Add(int64(bits.Count()))
	q.owe(topic, root, bits)
}

// oweRefused handles what a full queue refused to p: a push is unpushed (and owed as an offer to a
// mesh peer), an answer of a chunk is owed, and an IHAVE bitmap is owed. Asks are not: under E, an
// ask that was never written opens no promise. Router goroutine, from doSendRPC, before doDropRPC
// strips the control.
//
// Only an answer of a chunk of a live group is owed, as the spec's owed_answers holds it: a bit per
// connection, group and index, gone with the group (seacast-lite.md §4.1, §8.1). An answer of any other
// message (a block or an attestation on a topic off the path) is dropped, as stock gossipsub drops
// it: kept, it would stay for as long as the connection, past the message cache, one for each
// distinct message a peer that doesn't read asked for.
func (gs *GossipSubRouter) oweRefused(p peer.ID, q *rpcQueue, rpc *RPC) {
	for _, m := range rpc.Publish {
		if rpc.solicited {
			if root, _, ok := gs.have.index(gs.p.idGen.RawID(m)); ok && len(root) == haveRootLen && !gs.expired(root) {
				gs.v05.ctr.AnswersOwed.Add(1)
				q.oweAnswer(m)
			}
			continue
		}
		gs.unpush(p, m.GetTopic(), gs.p.idGen.RawID(m))
	}
	for _, ih := range rpc.GetControl().GetIhave() {
		topic := ih.GetTopicID()
		for _, id := range ih.GetMessageIDs() {
			var b haveBits
			root := ""
			if r, enc, ok := decodeHave(id); ok {
				root = r
				b.Or(enc)
			} else if r, i, ok := gs.have.index(id); ok && len(r) == haveRootLen {
				root = r
				b.Set(i)
			}
			if root != "" && b.Count() > 0 {
				gs.owe(p, topic, root, &b)
			}
		}
	}
}

// --- B′: every offer kept, the announcement order, the last resort ---

// chunkKey names the chunk mid is, whichever form names it: its group and index, or the id itself
// outside any group.
func (gs *GossipSubRouter) chunkKey(mid string) string {
	if root, index, ok := gs.groupIndex(mid); ok {
		var b [4]byte
		binary.BigEndian.PutUint32(b[:], index)
		return root + string(b[:])
	}
	return mid
}

// groupIndex is the group and index mid names: a structured id or an ask by index.
func (gs *GossipSubRouter) groupIndex(mid string) (string, uint32, bool) {
	if gs.have != nil {
		if root, index, ok := gs.have.index(mid); ok && len(root) == haveRootLen {
			return root, index, true
		}
	}
	return decodeWant(mid)
}

// noteOffered keeps p's offer of mid in its per-group bitmap, past the ordered lists' bounds.
// Router goroutine, from considerAnnounced.
func (gs *GossipSubRouter) noteOffered(p peer.ID, topic, mid string) {
	root, index, ok := gs.groupIndex(mid)
	if !ok {
		return
	}
	byRoot := gs.v05.offered[p]
	if byRoot == nil {
		byRoot = make(map[string]*offeredGroup)
		gs.v05.offered[p] = byRoot
	}
	g := byRoot[root]
	if g == nil {
		if len(byRoot) >= offeredRootsPerPeer {
			return // the peer's further groups are ignored until one ages out
		}
		g = &offeredGroup{topic: topic}
		byRoot[root] = g
	}
	g.bits.Set(index)
	g.last = time.Now()
}

// noteOrder appends p to mid's ordered announcer list, which keeps announcement order.
func (gs *GossipSubRouter) noteOrder(mid string, p peer.ID) {
	gs.v05.offerOrder[mid] = append(gs.v05.offerOrder[mid], p)
}

// dropOrder removes p from mid's ordered list.
func (gs *GossipSubRouter) dropOrder(mid string, p peer.ID) {
	l := gs.v05.offerOrder[mid]
	for i, q := range l {
		if q == p {
			gs.v05.offerOrder[mid] = append(l[:i:i], l[i+1:]...)
			break
		}
	}
	if len(gs.v05.offerOrder[mid]) == 0 {
		delete(gs.v05.offerOrder, mid)
	}
}

// announcers is everyone connected who offered mid: the ordered list, then every other peer whose
// per-group offers name mid's index, in peer-id order. Router goroutine.
func (gs *GossipSubRouter) announcers(mid string) (out []peer.ID, listed int) {
	in := make(map[peer.ID]bool)
	for _, p := range gs.v05.offerOrder[mid] {
		if _, connected := gs.peers[p]; connected && !in[p] {
			in[p] = true
			out = append(out, p)
		}
	}
	listed = len(out)
	root, index, ok := gs.groupIndex(mid)
	if !ok {
		return out, listed
	}
	var more []peer.ID
	for p, byRoot := range gs.v05.offered {
		if in[p] {
			continue
		}
		if _, connected := gs.peers[p]; !connected {
			continue
		}
		if g := byRoot[root]; g != nil && g.bits.Has(index) {
			more = append(more, p)
		}
	}
	// The node's own order (seacast-lite.md §6.1's own_order, spec f6c19db): by the keyed rank, larger
	// first, then the larger peer id, as §5.3's tie-break. The same over a chunk's retries.
	rank := make(map[peer.ID]uint64, len(more))
	for _, p := range more {
		rank[p] = gs.v05.ownRank(root, index, p)
	}
	sort.Slice(more, func(i, j int) bool {
		if rank[more[i]] != rank[more[j]] {
			return rank[more[i]] > rank[more[j]]
		}
		return more[i] > more[j]
	})
	return append(out, more...), listed
}

// offerTopicOf is the topic mid was offered under.
func (gs *GossipSubRouter) offerTopicOf(mid string, p peer.ID) string {
	if t, ok := gs.offerTopic[mid]; ok {
		return t
	}
	if root, _, ok := gs.groupIndex(mid); ok {
		if g := gs.v05.offered[p][root]; g != nil {
			return g.topic
		}
	}
	return ""
}

// pickAnnouncer is the earliest announcer in cands not yet asked for mid, else the least recently
// asked; cands keeps announcement order.
func (gs *GossipSubRouter) pickAnnouncer(mid string, cands []peer.ID) (peer.ID, bool) {
	tried := gs.offerTried[mid]
	pick, found := peer.ID(""), false
	var oldest time.Time
	for _, p := range cands {
		t, was := tried[p]
		if !was {
			return p, true
		}
		if !found || t.Before(oldest) {
			pick, found, oldest = p, true, t
		}
	}
	return pick, found
}

// retryV05 is retryOfferedPull under B′: every announcer, in announcement order, and the last
// resort when every one of them is parked. It returns the announcer asked and whether the queue
// took the ask; nothing when it asked nobody now (the last resort's ask comes later). Router
// goroutine.
func (gs *GossipSubRouter) retryV05(mid string) (peer.ID, bool) {
	if gs.expiredID(mid) {
		return "", false
	}
	anns, listed := gs.announcers(mid)
	var cands []peer.ID
	for _, p := range anns {
		if gs.peerParkedFor(p, mid) || gs.seacastTiedTo(p, mid) {
			continue
		}
		if !gs.requestAllowed(p, gs.offerTopicOf(mid, p), mid) {
			continue
		}
		cands = append(cands, p)
	}
	if len(cands) == 0 {
		gs.lastResort(mid)
		return "", false
	}
	free := cands[:0:0]
	for _, p := range cands {
		if !gs.seacastAskOutstanding(p, mid) {
			free = append(free, p)
		}
	}
	if len(free) == 0 {
		gs.seacastRetryLater(mid) // every announcer not yet answered: look again later (seacast-lite.md §6.1)
		return "", false
	}
	pick, found := gs.pickAnnouncer(mid, free)
	if !found || !gs.iwantAllowed(mid, pick) {
		return "", false
	}
	if gs.offerRetryCtr != nil {
		gs.offerRetryCtr.Add(1)
	}
	return pick, gs.sendAsk(pick, mid, anns, listed)
}

// sendAsk sends one ask of mid to p, recorded as every ask is; reports whether the queue took it.
func (gs *GossipSubRouter) sendAsk(p peer.ID, mid string, anns []peer.ID, listed int) bool {
	gs.seacastNoteAskOutside(p, mid) // §4.5's may_be_asked on the ordinary asks: counted, not refused
	for i, q := range anns {
		if q == p && i >= listed {
			gs.v05.ctr.BeyondList.Add(1)
			break
		}
	}
	if gs.seacast == nil {
		gs.iasked[p]++ // with the path, only asks in reply to offers count (seacast.md §4.3)
	}
	gs.gossipTracer.AddPromise(p, []string{mid})
	gs.noteIWantSent(p, []string{mid})
	return gs.sendRPC(p, rpcWithControl(nil, nil, []*pb.ControlIWant{{MessageIDs: []string{mid}}}, nil, nil, nil), false)
}

// lastResort arms, once per chunk, the ask of a parked announcer, when every announcer of mid is
// parked and one has not yet been asked this way. The wait doubles with each such ask of the
// chunk: PROMISE, 2 × PROMISE, 4 × PROMISE, ... Chunks only (v05LastResortFor). Router goroutine.
func (gs *GossipSubRouter) lastResort(mid string) {
	if !gs.v05LastResortFor(mid) {
		return
	}
	k := gs.chunkKey(mid)
	if gs.v05.lrArmedKeys[k] || gs.expiredID(mid) {
		return
	}
	anns, _ := gs.announcers(mid)
	if len(anns) == 0 {
		return
	}
	done := gs.v05.askedParked[k]
	allDone := true
	for _, p := range anns {
		if gs.seacastTiedTo(p, mid) {
			continue // a tied announcer is passed over (seacast-lite.md's last_resort)
		}
		if !gs.peerParkedFor(p, mid) {
			return // someone can be asked the ordinary way
		}
		if _, asked := done[p]; !asked {
			allDone = false
		}
	}
	if allDone {
		return
	}
	gs.v05.lrArmedKeys[k] = true
	gs.v05.ctr.LastResortsArmed.Add(1)
	wait := gs.parkPromise << min(len(done), 16)
	time.AfterFunc(wait, func() {
		select {
		case gs.p.eval <- func() { gs.askParked(mid) }:
		case <-gs.p.ctx.Done():
		}
	})
}

// askParked is the last resort's ask: an announcer unparked meanwhile if there is one, else a
// parked one not yet asked this way; of those, one without an ask of the chunk outstanding,
// looking again after the ask window when every one has one, as the retry does (seacast-lite.md's
// ask_parked); counted against the parked announcer once the queue has taken the ask. Router
// goroutine.
func (gs *GossipSubRouter) askParked(mid string) {
	k := gs.chunkKey(mid)
	delete(gs.v05.lrArmedKeys, k)
	if gs.haveSeen(mid) || gs.expiredID(mid) || gs.seacastStopPulled(mid) {
		return // ask_parked returns at stop_pull, the probed cell's included (seacast_complete.go)
	}
	if at, ok := gs.iwantAsked[mid]; ok && time.Since(at) < gs.iwantWindow {
		return // someone was asked meanwhile
	}
	anns, listed := gs.announcers(mid)
	untied := anns[:0:0]
	for _, p := range anns {
		if !gs.requestAllowed(p, gs.offerTopicOf(mid, p), mid) {
			continue // stop-pull, or a gate that cannot authenticate the group yet
		}
		if gs.seacastTiedTo(p, mid) {
			continue // its copy waits for its root
		}
		untied = append(untied, p)
	}
	var cands []peer.ID
	for _, p := range untied {
		if !gs.peerParkedFor(p, mid) {
			cands = append(cands, p)
		}
	}
	if len(cands) == 0 {
		for _, p := range untied {
			if _, asked := gs.v05.askedParked[k][p]; !asked {
				cands = append(cands, p)
			}
		}
	}
	if len(cands) == 0 {
		return
	}
	free := cands[:0:0]
	for _, p := range cands {
		if !gs.seacastAskOutstanding(p, mid) {
			free = append(free, p)
		}
	}
	if len(free) == 0 {
		// None is asked again while an ask of it is not answered. The last resort waits as the
		// retry does, and stays armed meanwhile: returning here instead left the chunk to nobody
		// when that ask was then written and not answered, since its broken promise arms nothing.
		gs.v05.lrArmedKeys[k] = true
		gs.v05.ctr.LastResortsDeferred.Add(1)
		time.AfterFunc(gs.iwantWindow+5*time.Millisecond, func() {
			select {
			case gs.p.eval <- func() { gs.askParked(mid) }:
			case <-gs.p.ctx.Done():
			}
		})
		return
	}
	pick, found := gs.pickAnnouncer(mid, free)
	if !found || !gs.iwantAllowed(mid, pick) {
		return
	}
	gs.v05.ctr.LastResortAsks.Add(1)
	if gs.sendAsk(pick, mid, anns, listed) && gs.peerParkedFor(pick, mid) {
		if gs.v05.askedParked[k] == nil {
			gs.v05.askedParked[k] = make(map[peer.ID]time.Time)
		}
		gs.v05.askedParked[k][pick] = time.Now()
		gs.v05.ctr.ParkedAsked.Add(1)
	}
}

// v05ClearList drops mid's ordered announcer list, with the rest of the offer table's entry for it.
func (gs *GossipSubRouter) v05ClearList(mid string) {
	if gs.v05LastResort() {
		delete(gs.v05.offerOrder, mid)
	}
}

// v05ChunkDelivered drops B′'s per-chunk state once a copy of the chunk arrived, whichever form named
// it. The per-chunk count of last-resort asks lives on otherwise, as `tried` does: a list of one form
// expiring, or a connection closing, does not reset it (each entry ages by its own time, and a
// closed connection takes only its own).
func (gs *GossipSubRouter) v05ChunkDelivered(mid string) {
	if !gs.v05LastResort() {
		return
	}
	k := gs.chunkKey(mid)
	delete(gs.v05.askedParked, k)
	delete(gs.v05.lrArmedKeys, k)
}

// sweepV05 ages B′'s offers out 30 s after the announcer's last offer of the group, and the
// last-resort counts as tried ages. Router goroutine, from the heartbeat's sweep.
func (gs *GossipSubRouter) sweepV05() {
	if !gs.v05LastResort() {
		return
	}
	for p, byRoot := range gs.v05.offered {
		for root, g := range byRoot {
			if time.Since(g.last) >= offerTTL {
				delete(byRoot, root)
			}
		}
		if len(byRoot) == 0 {
			delete(gs.v05.offered, p)
		}
	}
	for k, peers := range gs.v05.askedParked {
		for p, at := range peers {
			if time.Since(at) >= offerTTL {
				delete(peers, p)
			}
		}
		if len(peers) == 0 {
			delete(gs.v05.askedParked, k)
		}
	}
}

// --- The connection: promises and pull tables close with it ---

// v05PeerGone closes p's promises and drops its offers, tried asks and last-resort counts, of the
// ids of a group: an id outside any group (a stock topic's, on a router the topics share) keeps
// what the router without the rule keeps, its promise falling due as before and its offer and
// tried ask left for the ordinary retry once the peer is back. Router goroutine, from
// OnClosedOutboundStream.
func (gs *GossipSubRouter) v05PeerGone(p peer.ID) {
	if gs.v05 == nil {
		return
	}
	if gs.v05.LastResort {
		delete(gs.v05.offered, p)
		for k, peers := range gs.v05.askedParked {
			delete(peers, p)
			if len(peers) == 0 {
				delete(gs.v05.askedParked, k)
			}
		}
	}
	grouped := func(mid string) bool { return gs.parkScopeOf(mid) != "" }
	// The score's promises: under E the unwritten ones go with the queue; under CloseWithConn the
	// open ones of the grouped asks close, as the park's below; a stock ask's stays, as upstream's.
	var closeOpen func(string) bool
	if gs.v05.CloseWithConn {
		closeOpen = grouped
	}
	gs.v05.ctr.ClosedScorePromises.Add(int64(gs.gossipTracer.peerGone(p, closeOpen)))
	if !gs.v05.CloseWithConn {
		return
	}
	for mid, peers := range gs.iwantPromised {
		if _, open := peers[p]; open && grouped(mid) {
			delete(peers, p)
			gs.v05.ctr.ClosedPromises.Add(1)
			if len(peers) == 0 {
				delete(gs.iwantPromised, mid)
			}
		}
	}
	for mid, peers := range gs.offerTable {
		if _, ok := peers[p]; ok && grouped(mid) {
			delete(peers, p)
			if gs.offerByPeer[p] > 0 {
				gs.offerByPeer[p]--
			}
			if gs.v05LastResort() {
				gs.dropOrder(mid, p)
			}
			if len(peers) == 0 {
				gs.clearOffers(mid)
			}
		}
	}
	if gs.offerByPeer[p] == 0 {
		delete(gs.offerByPeer, p)
	}
	for mid, tried := range gs.offerTried {
		if grouped(mid) {
			delete(tried, p)
		}
	}
}

// --- D′: expiry ---

// expireGroup leaves root's tombstone and drops its pull state and its queued sends. Router
// goroutine, from the heartbeat, as the group's holdings age out.
func (gs *GossipSubRouter) expireGroup(root string) {
	gs.markGroupExpired(root)
	gs.seacastGroupAged(root)
	gs.discardGroupPull(root)
}

// markGroupExpired leaves root's holdings tombstone, for holdTTL heartbeats, and marks it dead in
// the writers' view. Router goroutine.
func (gs *GossipSubRouter) markGroupExpired(root string) {
	h := gs.have
	h.tomb[root] = h.holdTTL
	h.own.kill(root)
	if gs.v05 != nil {
		gs.v05.ctr.Expired.Add(1)
	}
}

// discardGroupPull drops root's pull state (offers, tried asks, asks in flight, promises, the score's
// promises) and its queued sends. Router goroutine.
func (gs *GossipSubRouter) discardGroupPull(root string) {
	inGroup := func(mid string) bool {
		r, _, ok := gs.groupIndex(mid)
		return ok && r == root
	}
	if gs.gossipTracer != nil {
		// The score's promises of the group's asks close with it, as at completion: an ask still
		// queued is discarded below and never sent, and after an expiry an answer that arrives
		// is dropped at admission, so no peer asked could keep the promise. After the sweep of a
		// root no type placed, which forgets the root rather than expiring it, a later answer can
		// still be admitted; its promise closes all the same. A promise whose deadline passed but
		// that the heartbeat hasn't charged yet closes too: a peer that left an ask of the group
		// unanswered past its deadline goes uncharged when the group ends before the next heartbeat.
		gs.gossipTracer.fulfillPromisesWhere(func(mid string) bool { return gs.seacastAskRoot(mid) == root })
	}
	for mid := range gs.offerTable {
		if inGroup(mid) {
			gs.clearOffers(mid)
		}
	}
	for mid := range gs.offerTried {
		if inGroup(mid) {
			delete(gs.offerTried, mid)
		}
	}
	for mid := range gs.iwantAsked {
		if inGroup(mid) {
			delete(gs.iwantAsked, mid)
		}
	}
	for mid := range gs.iwantPromised {
		if inGroup(mid) {
			delete(gs.iwantPromised, mid) // nobody is parked for a group that died
		}
	}
	if gs.seacast != nil {
		for mid := range gs.seacast.askQueued {
			if inGroup(mid) {
				gs.seacastAsksClosed(mid)
			}
		}
		gs.seacast.probeAsksForget(root)  // the probe's exemptions go with it
		gs.seacastProbeQueuesForget(root) // and the probe's state at the writers (seacast_probe.go)
	}
	if gs.v05 != nil && gs.v05.LastResort {
		for _, byRoot := range gs.v05.offered {
			delete(byRoot, root)
		}
		for k := range gs.v05.lrArmedKeys {
			if strings.HasPrefix(k, root) {
				delete(gs.v05.lrArmedKeys, k)
			}
		}
		for k := range gs.v05.askedParked {
			if strings.HasPrefix(k, root) {
				delete(gs.v05.askedParked, k)
			}
		}
	}
	if gs.p == nil {
		return
	}
	ofMsg := func(m *pb.Message) bool { return inGroup(gs.p.idGen.RawID(m)) }
	// A control id of any form names its group: a v1 holding, a v4 holding (its flags), a v4 WANT
	// (the probe's, or a standing one; seacast.md §4.5), and the
	// ids groupIndex reads (a chunk's, a v1 WANT). The v4 forms counted apart.
	v4 := 0
	ofID := func(id string) bool {
		if r, _, ok := decodeHave(id); ok {
			return r == root
		}
		if r, ok := seacastHaveRoot(id); ok {
			if r == root {
				v4++
			}
			return r == root
		}
		if r, _, _, _, ok, malformed := decodeWantV2(id); ok && !malformed {
			if r == root {
				v4++
			}
			return r == root
		}
		return inGroup(id)
	}
	for _, q := range gs.p.peers {
		n := q.discardGroup(root, ofMsg, ofID)
		if gs.v05 != nil {
			gs.v05.ctr.ExpiredDiscards.Add(int64(n))
		}
	}
	if gs.seacast != nil {
		gs.seacast.ctr.ExpiredV4Discards.Add(int64(v4))
	}
}

// errIsQueueFull reports whether a push failed because the queue was full.
func errIsQueueFull(err error) bool { return errors.Is(err, ErrQueueFull) }
