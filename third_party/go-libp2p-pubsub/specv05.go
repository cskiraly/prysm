package pubsub

import (
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	core "github.com/cskiraly/seacast-go"
	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
)

// --- Version 0.5 of the protocol text: the rules its model check added ---
//
// The specification's v0.5 (seacast-spec 5a729ff, from a model check of v0.4) added
// rules this implementation predated; its §9 lists them. Two of them are switches here, so they
// can be taken one at a time, and with both off the router behaves as before them:
//
//   - PromiseAtWrite (E, §6.1, §7.2): the promise behind the park starts when the writer writes the
//     ask, not when the ask is decided. An ask that waits behind the window, or that a full queue
//     refuses, opens no promise, so no peer is parked for an ask it never received. A chunk's
//     promise starts at the write whatever the switch (seacast_pull.go); the switch takes the ids
//     outside any group and the score's promises.
//   - OfferSkipped (A′, §5.2, §7.2): a push that does not happen -- skipped by projection at the
//     writer, or refused by a full queue -- is no longer counted as pushed, and the chunk is offered
//     to the peer instead if it is in the mesh. An offer a full queue refuses is owed, as is an
//     answer: the next write to the peer carries what is owed. The guard's clock restarts once
//     every push counted to the peer has been skipped or refused.
//
// The other two are a chunk's pull in the core (seacast_pull.go), with no switch: every offer kept,
// per connection and group, the retry in announcement order and the last resort when every announcer
// is parked (B′, §6.1); and a peer's promises, offers, tries and last-resort counts going with its
// connection (§6.1, §8.1).
//
// D′'s expiry (§6.1, §7.2, §8.1) runs on SEACAST's path for the path's roots (expireGroup below,
// seacast_window.go): a group whose holdings aged out leaves a tombstone, and its pull state and
// its queued frames, offers and asks go at once. Its switch for every group, and C′'s ledger, are
// not in this fork.

// SpecV05 selects the v0.5 rules; the zero value is the implementation as measured before them.
type SpecV05 struct {
	PromiseAtWrite bool
	OfferSkipped   bool
}

// SpecV05All is every v0.5 rule this package switches.
var SpecV05All = SpecV05{PromiseAtWrite: true, OfferSkipped: true}

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
	// The chunk pull counts them (seacast_pull.go).
	LastResortsArmed, LastResortsDeferred, LastResortAsks, ParkedAsked, BeyondList atomic.Int64
	// ClosedPromises is a chunk's promises closed with their connection.
	ClosedPromises atomic.Int64
	// ScorePromisesAtWrite is the score's promises opened by a written ask under E (the gossip
	// tracer's, which the heartbeat charges as the behaviour penalty); ClosedScorePromises those
	// of grouped asks closed with their connection, under the chunk pull.
	ScorePromisesAtWrite, ClosedScorePromises atomic.Int64
	// Expired is the groups that passed their lifetime under D′; ExpiredDiscards the queued
	// frames, offers and asks of such groups dropped from the queues.
	Expired, ExpiredDiscards atomic.Int64
}

type specV05 struct {
	SpecV05
	ctr *SpecV05Counters
}

// WithSpecV05 turns on the chosen v0.5 rules. Each needs what it changes: PromiseAtWrite the
// commitment park; OfferSkipped the HAVE bitmaps with the IHAVE side. Must follow those options. ctr
// may be nil.
func WithSpecV05(rules SpecV05, ctr *SpecV05Counters) Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok {
			return fmt.Errorf("spec v0.5 rules require a gossipsub router")
		}
		if rules.PromiseAtWrite && gs.iwantPromised == nil {
			return fmt.Errorf("spec v0.5 E requires WithIHaveCommitmentPark first")
		}
		if rules.OfferSkipped && (gs.have == nil || !gs.have.ihaveOn) {
			return fmt.Errorf("spec v0.5 A′ requires WithHaveBitmaps and WithHaveAnnounceBitmaps first")
		}
		if ctr == nil {
			ctr = &SpecV05Counters{}
		}
		gs.v05 = &specV05{SpecV05: rules, ctr: ctr}
		if rules.PromiseAtWrite || rules.OfferSkipped {
			ps.queueHooks = gs.v05QueueHooks
		}
		return nil
	}
}

func (gs *GossipSubRouter) v05PromiseAtWrite() bool { return gs.v05 != nil && gs.v05.PromiseAtWrite }
func (gs *GossipSubRouter) v05OfferSkipped() bool   { return gs.v05 != nil && gs.v05.OfferSkipped }

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

// v05QueueHooks installs the writer's side of E and A′ on a new peer queue, and the report of written
// asks a chunk's promise opens at (seacast_pull.go). pubsub's goroutine, as the queue is made.
func (gs *GossipSubRouter) v05QueueHooks(q *rpcQueue) {
	p := q.peer
	// A writer's report reaches the router later, on its event loop; by then the connection may have
	// closed (or been replaced by a new queue). A report from a queue no longer the peer's is dropped,
	// so nothing is recreated for a closed connection (its promises closed with it).
	current := func() bool { cur, ok := gs.p.peers[p]; return ok && cur == q }
	if gs.v05PromiseAtWrite() || gs.pullOn() {
		q.onAsks = func(mids []string, popped uint64, at time.Time) {
			// Never block a writer on the router, which may be waiting on this queue's lock.
			go func() {
				select {
				case gs.p.eval <- func() {
					if current() {
						// The score's promise first (gossip_tracer.go), then the park's.
						gs.v05Ctr().ScorePromisesAtWrite.Add(int64(gs.gossipTracer.promisesWritten(p, mids, popped, at)))
						gs.promisesFromWrite(p, mids, popped, at)
					}
				}:
				case <-gs.p.ctx.Done():
				}
			}()
		}
	}
	if gs.v05PromiseAtWrite() {
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
					gs.v05Ctr().AsksLost.Add(n)
					if gs.lossDropped != nil {
						gs.lossDropped.Add(n)
					}
				}
				return kept, lost
			}
		}
	}
	if gs.v05OfferSkipped() {
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

// promisesFromWrite opens the park's promise for each ask the writer popped (popped, in askSeq's order)
// and wrote to p at the time at, the write (after any send delay); its deadline is PROMISE after that. An ask re-written while its promise is open keeps
// the earlier deadline, as a re-ask did. A chunk's promise is the core's (seacast_pull.go), whatever E; an
// id outside any group opens one here only under E. Router goroutine.
func (gs *GossipSubRouter) promisesFromWrite(p peer.ID, mids []string, popped uint64, at time.Time) {
	if gs.iwantPromised == nil {
		return
	}
	wait := max(0, gs.parkPromise-time.Since(at))
	for _, mid := range mids {
		_, g, i, chunk := gs.pullChunk(mid)
		if !chunk && !gs.v05PromiseAtWrite() {
			continue // without E, the report is a chunk's only: what else was asked keeps its record as before
		}
		// Written: the promise is the outstanding ask from here. On the path, only the ask this write
		// carried: one dispatched before the writer popped it, and still queued.
		queued := gs.seacastAskWritten(p, mid, popped)
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
		if chunk {
			gs.pullPromiseWritten(p, g, i, at)
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
	if e := h.pushed.Of(p, seacastGID(root)); e != nil && e.Bits.Has(index) {
		e.Bits.Clear(index)
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
// What p is recorded as pushed is not owed: a push takes its index out of what is owed (unowe), and
// a bitmap refused after a push of one of its indices (PublishBatch plans its RPCs before it serves
// standing answers) owes the others alone, as the spec's with_owed takes pushed out at the write.
// Router goroutine.
func (gs *GossipSubRouter) owe(p peer.ID, topic, root string, bits *haveBits) {
	if gs.expired(root) {
		return // a group past its lifetime is offered to nobody (D′)
	}
	q, ok := gs.p.peers[p]
	if !ok {
		return
	}
	if h := gs.have; h != nil {
		if e := h.pushed.Of(p, seacastGID(root)); e != nil {
			bits = bits.AndNot(&e.Bits)
		}
	}
	n := bits.Count()
	if n == 0 {
		return
	}
	gs.v05.ctr.OffersOwed.Add(int64(n))
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

// --- The ids of a group ---

// groupIndex is the group and index mid names: a structured id or an ask by index.
func (gs *GossipSubRouter) groupIndex(mid string) (string, uint32, bool) {
	if gs.have != nil {
		if root, index, ok := gs.have.index(mid); ok && len(root) == haveRootLen {
			return root, index, true
		}
	}
	return decodeWant(mid)
}

// pickAnnouncer is the stock retry's pick for mid: the earliest announcer in cands not yet asked for
// mid, else the least recently asked (core.Pick: untried, else least_recently_tried).
func (gs *GossipSubRouter) pickAnnouncer(mid string, cands []peer.ID) (peer.ID, bool) {
	tried := gs.offerTried[mid]
	return core.Pick(cands, func(p peer.ID) (time.Time, bool) {
		t, was := tried[p]
		return t, was
	})
}

// --- The connection: promises and pull tables close with it ---

// v05PeerGone closes p's chunk pull (seacast_pull.go): its promises close, and its offers, tries and
// last-resort counts go. An id outside any group (a stock topic's, on a router the topics share) keeps
// what the router keeps for it, its promise falling due as before and its offer and tried ask left for
// the ordinary retry once the peer is back. The score's promises: under E the unwritten ones go with the
// queue, and with the chunk pull the open ones of the grouped asks close; a stock ask's stays, as
// upstream's. Router goroutine, from RemovePeer.
func (gs *GossipSubRouter) v05PeerGone(p peer.ID) {
	if gs.v05 != nil || gs.pullOn() {
		var closeOpen func(string) bool
		if gs.pullOn() {
			closeOpen = func(mid string) bool { return gs.parkScopeOf(mid) != "" }
		}
		gs.v05Ctr().ClosedScorePromises.Add(int64(gs.gossipTracer.peerGone(p, closeOpen)))
	}
	if gs.pullOn() {
		gs.v05Ctr().ClosedPromises.Add(int64(gs.pullPeerGone(p)))
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
	if gs.pullOn() {
		gs.pullForgetGroup(seacastGID(root)) // a chunk's pull state (seacast_pull.go)
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
