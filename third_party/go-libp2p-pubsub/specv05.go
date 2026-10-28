package pubsub

import (
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
// The specification's v0.5 (segmented-broadcast-spec 5a729ff, from a model check of v0.4) added
// rules this implementation predated; its §9 lists them. Each is a switch here, so an arm can take
// them one at a time and the measured arms stay reproducible with every switch off:
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
//   - Expiry (D′, §6.1, §7.2, §8.1): a group whose holdings aged out (GROUP_TTL after its last
//     chunk, holdTTL here) leaves a tombstone, its id, for another holdTTL. Its pull state goes at
//     once: offers, tried asks, asks in flight, promises, last-resort state, what is owed of it,
//     and its queued frames, offers and asks in every queue (its queued HAVEs stay). The retry, the
//     last resort and the announcement handler ignore it; the writer drops asks for it; a late copy
//     does not bring it back; a rebuild that returns after it does nothing.
//   - LedgerFromOffers (C′, §3, §4.2, §4.4): what the request gate declines, because no installed
//     block authenticates the group yet, stays in the offer table, which is the ledger: when the
//     application replays (the block installed), every offered index not held is asked for as a
//     retry is, in place of the separate deferral ledger (4 announcers per id, a cap on ids).

// SpecV05 selects the v0.5 rules; the zero value is the implementation as measured before them.
type SpecV05 struct {
	PromiseAtWrite   bool
	OfferSkipped     bool
	LastResort       bool
	CloseWithConn    bool
	LedgerFromOffers bool
	Expiry           bool
}

// SpecV05All is every v0.5 rule this package implements.
var SpecV05All = SpecV05{PromiseAtWrite: true, OfferSkipped: true, LastResort: true, CloseWithConn: true, LedgerFromOffers: true, Expiry: true}

// SpecV05Counters counts what the v0.5 rules did; an application may share one across nodes.
type SpecV05Counters struct {
	// PromisesAtWrite is the promises opened by a written ask; AsksLost the asks a class-loss
	// injection dropped after the write, which open theirs too.
	PromisesAtWrite, AsksLost atomic.Int64
	// Unpushed is the pushes no longer counted (projection skips and full-queue refusals);
	// OffersOwed the indices owed to mesh peers as offers; OwedSent the indices that left in
	// owed IHAVEs; AnswersOwed the answers a full queue refused and the writer requeued.
	Unpushed, OffersOwed, OwedSent, AnswersOwed atomic.Int64
	// LastResortsArmed is the last-resort timers armed; LastResortAsks the asks they sent, of
	// which ParkedAsked went to a parked announcer; BeyondList the asks (retries and last
	// resorts) whose announcer was known only from the per-group offers, past the ordered list.
	LastResortsArmed, LastResortAsks, ParkedAsked, BeyondList atomic.Int64
	// ClosedPromises is the promises closed with their connection.
	ClosedPromises atomic.Int64
	// LedgerAsks is the ids a replay found in the offer table and handed to the retry (C′).
	LedgerAsks atomic.Int64
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
		if rules.LedgerFromOffers && !rules.LastResort {
			return fmt.Errorf("spec v0.5 C′ reads B′'s offers: it requires LastResort too")
		}
		if rules.Expiry && gs.have == nil {
			return fmt.Errorf("spec v0.5 D′ ages groups by their HAVE state: it requires WithHaveBitmaps first")
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
		if rules.Expiry {
			// A received chunk of a group past its lifetime is ignored before anything else sees it:
			// no Preprocess (no IDONTWANT), no validation, no delivery, no forwarding (§4.2).
			ps.admitMsg = func(m *Message) bool { return !gs.expiredID(ps.idGen.ID(m)) }
		}
		return nil
	}
}

func (gs *GossipSubRouter) v05PromiseAtWrite() bool { return gs.v05 != nil && gs.v05.PromiseAtWrite }
func (gs *GossipSubRouter) v05OfferSkipped() bool   { return gs.v05 != nil && gs.v05.OfferSkipped }
func (gs *GossipSubRouter) v05LastResort() bool     { return gs.v05 != nil && gs.v05.LastResort }
func (gs *GossipSubRouter) v05Expiry() bool         { return gs.v05 != nil && gs.v05.Expiry }

// expired reports whether root is past its active lifetime (D′): only its tombstone is left.
func (gs *GossipSubRouter) expired(root string) bool {
	return gs.v05Expiry() && gs.have != nil && gs.have.tomb[root] > 0
}

// expiredID reports whether the group mid names is past its lifetime.
func (gs *GossipSubRouter) expiredID(mid string) bool {
	if !gs.v05Expiry() {
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
// writer's pop; its deadline is PROMISE after that. An ask re-written while its promise is open keeps
// the earlier deadline, as a re-ask did. Router goroutine.
func (gs *GossipSubRouter) promisesFromWrite(p peer.ID, mids []string, at time.Time) {
	if gs.iwantPromised == nil {
		return
	}
	wait := max(0, gs.parkPromise-time.Since(at))
	for _, mid := range mids {
		if gs.haveSeen(mid) || gs.expiredID(mid) {
			continue // arrived, or its group died, between the write and this event: nothing is owed
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
// mesh peer of the topic. A push to a connection outside the mesh (the source's wide first hop)
// is forgotten but not offered. Router goroutine.
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
	if _, inMesh := gs.mesh[topic][p]; !inMesh {
		return
	}
	var b haveBits
	b.Set(index)
	gs.owe(p, topic, root, &b)
}

// owe records bits of root as owed to p: the writer's next write to p carries them as an IHAVE.
// Router goroutine.
func (gs *GossipSubRouter) owe(p peer.ID, topic, root string, bits *haveBits) {
	if gs.have.disclose != nil {
		return // link disclosure decides what p may be told, and owes nothing past it
	}
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
// mesh peer), an answer is owed, and an IHAVE bitmap is owed. Asks are not: under E, an ask that
// was never written opens no promise. Router goroutine, from doSendRPC, before doDropRPC strips
// the control.
func (gs *GossipSubRouter) oweRefused(p peer.ID, q *rpcQueue, rpc *RPC) {
	for _, m := range rpc.Publish {
		if rpc.solicited {
			gs.v05.ctr.AnswersOwed.Add(1)
			q.oweAnswer(m)
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
	sort.Slice(more, func(i, j int) bool { return more[i] < more[j] })
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
// resort when every one of them is parked. Router goroutine.
func (gs *GossipSubRouter) retryV05(mid string) {
	if gs.expiredID(mid) {
		return
	}
	anns, listed := gs.announcers(mid)
	var cands []peer.ID
	for _, p := range anns {
		if gs.peerParkedFor(p, mid) {
			continue
		}
		if gs.requestGate != nil && !gs.requestGate.Allow(p, gs.offerTopicOf(mid, p), mid) {
			continue
		}
		cands = append(cands, p)
	}
	if len(cands) == 0 {
		gs.lastResort(mid)
		return
	}
	pick, found := gs.pickAnnouncer(mid, cands)
	if !found || !gs.iwantAllowed(mid, pick) {
		return
	}
	if gs.offerRetryCtr != nil {
		gs.offerRetryCtr.Add(1)
	}
	gs.sendAsk(pick, mid, anns, listed)
}

// sendAsk sends one ask of mid to p, recorded as every ask is; reports whether the queue took it.
func (gs *GossipSubRouter) sendAsk(p peer.ID, mid string, anns []peer.ID, listed int) bool {
	for i, q := range anns {
		if q == p && i >= listed {
			gs.v05.ctr.BeyondList.Add(1)
			break
		}
	}
	gs.iasked[p]++
	gs.gossipTracer.AddPromise(p, []string{mid})
	gs.noteIWantSent(p, []string{mid})
	if gs.requestGate != nil {
		gs.requestGate.Committed(p, []string{mid})
	}
	return gs.sendRPC(p, rpcWithControl(nil, nil, []*pb.ControlIWant{{MessageIDs: []string{mid}}}, nil, nil, nil), false)
}

// lastResort arms, once per chunk, the ask of a parked announcer, when every announcer of mid is
// parked and one has not yet been asked this way. The wait doubles with each such ask of the
// chunk: PROMISE, 2 × PROMISE, 4 × PROMISE, ... Router goroutine.
func (gs *GossipSubRouter) lastResort(mid string) {
	if !gs.v05LastResort() {
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
// parked one not yet asked this way; counted against the parked announcer once the queue has
// taken the ask. Router goroutine.
func (gs *GossipSubRouter) askParked(mid string) {
	k := gs.chunkKey(mid)
	delete(gs.v05.lrArmedKeys, k)
	if gs.haveSeen(mid) || gs.expiredID(mid) {
		return
	}
	if at, ok := gs.iwantAsked[mid]; ok && time.Since(at) < gs.iwantWindow {
		return // someone was asked meanwhile
	}
	anns, listed := gs.announcers(mid)
	allowed := anns[:0:0]
	for _, p := range anns {
		if gs.requestGate != nil && !gs.requestGate.Allow(p, gs.offerTopicOf(mid, p), mid) {
			continue // stop-pull, or a gate that cannot authenticate the group yet
		}
		allowed = append(allowed, p)
	}
	var cands []peer.ID
	for _, p := range allowed {
		if !gs.peerParkedFor(p, mid) {
			cands = append(cands, p)
		}
	}
	if len(cands) == 0 {
		for _, p := range allowed {
			if _, asked := gs.v05.askedParked[k][p]; !asked {
				cands = append(cands, p)
			}
		}
	}
	if len(cands) == 0 {
		return
	}
	pick, found := gs.pickAnnouncer(mid, cands)
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

// v05PeerGone closes p's promises and drops its offers, tried asks and last-resort counts.
// Router goroutine, from OnClosedOutboundStream.
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
	if !gs.v05.CloseWithConn {
		return
	}
	for mid, peers := range gs.iwantPromised {
		if _, open := peers[p]; open {
			delete(peers, p)
			gs.v05.ctr.ClosedPromises.Add(1)
			if len(peers) == 0 {
				delete(gs.iwantPromised, mid)
			}
		}
	}
	for mid, peers := range gs.offerTable {
		if _, ok := peers[p]; ok {
			delete(peers, p)
			if gs.v05LastResort() {
				gs.dropOrder(mid, p)
			}
			if len(peers) == 0 {
				gs.clearOffers(mid)
			}
		}
	}
	delete(gs.offerByPeer, p)
	for _, tried := range gs.offerTried {
		delete(tried, p)
	}
}

// --- C′: the offer table is the ledger ---

// replayLedger asks, as a retry would, for every index offered on a live connection and not held:
// the announcements the gate declined while it could not authenticate their group. Ids outside any
// group are in the offer table alone. The gate decides again per announcer, so a group still
// without its block stays where it is. Router goroutine, from the application's Replay.
func (gs *GossipSubRouter) replayLedger() {
	seen := make(map[string]bool)
	var mids []string
	add := func(mid string) {
		if !seen[mid] && !gs.haveSeen(mid) {
			seen[mid] = true
			mids = append(mids, mid)
		}
	}
	for p, byRoot := range gs.v05.offered {
		if _, connected := gs.peers[p]; !connected {
			continue
		}
		for root, g := range byRoot {
			for i, b := range g.bits.b {
				for bit := uint32(0); bit < 8; bit++ {
					if b&(1<<bit) == 0 {
						continue
					}
					index := uint32(i)*8 + bit // lint:ignore uintcast -- i < haveMaxBytes.
					if known := gs.have.mids[root]; known != nil && known.ids[index] != "" {
						add(known.ids[index])
					} else {
						add(encodeWant(root, index))
					}
				}
			}
		}
	}
	for mid := range gs.offerTable {
		if _, _, grouped := gs.groupIndex(mid); !grouped {
			add(mid)
		}
	}
	sort.Strings(mids)
	gs.shuffleStrings(mids)
	for _, mid := range mids {
		if at, ok := gs.iwantAsked[mid]; ok && time.Since(at) < gs.iwantWindow {
			continue // asked meanwhile
		}
		gs.v05.ctr.LedgerAsks.Add(1)
		gs.retryV05(mid)
	}
}

// --- D′: expiry ---

// expireGroup leaves root's tombstone and drops its pull state and its queued sends. Router
// goroutine, from the heartbeat, as the group's holdings age out.
func (gs *GossipSubRouter) expireGroup(root string) {
	h := gs.have
	h.tomb[root] = h.holdTTL
	h.own.kill(root)
	gs.v05.ctr.Expired.Add(1)
	inGroup := func(mid string) bool {
		r, _, ok := gs.groupIndex(mid)
		return ok && r == root
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
			if gs.iwantCount != nil {
				delete(gs.iwantCount, mid)
			}
		}
	}
	for mid := range gs.iwantPromised {
		if inGroup(mid) {
			delete(gs.iwantPromised, mid) // nobody is parked for a group that died
		}
	}
	if gs.v05.LastResort {
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
	ofID := func(id string) bool {
		if r, _, ok := decodeHave(id); ok {
			return r == root
		}
		return inGroup(id)
	}
	for _, q := range gs.p.peers {
		gs.v05.ctr.ExpiredDiscards.Add(int64(q.discardGroup(root, ofMsg, ofID)))
	}
}

// errIsQueueFull reports whether a push failed because the queue was full.
func errIsQueueFull(err error) bool { return errors.Is(err, ErrQueueFull) }
