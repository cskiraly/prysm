package pubsub

// A chunk's pull through the core (seacast-lite.md §6.1, as seacast.md §4.3 carries it): the offers, the
// asks, the promises and the park, the retry and the last resort of every id that names a chunk, kept in
// the core's tables by group and index. An ask by index and a chunk's own id name one chunk, so they share
// one offer list, one time asked, one promise per peer and one last-resort count. The router holds the
// tables and implements their envs; the core decides. Ids outside any group (stock topics' messages) keep
// gossipsub's IWANT discipline, the id-keyed offer table, its retry and its park (phaseforward.go,
// parkscope.go), and so does every id on a router without the pull's four options.
//
// Every table is the router goroutine's: the receive path, the heartbeat, the timers through gs.p.eval and
// the writer's reports through it. The writer reads none of them.

import (
	cryptorand "crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"slices"
	"time"

	core "github.com/cskiraly/seacast-go"
	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
)

// retrySlack is RETRY_SLACK: the retry looks again this long after the ask window has run.
const retrySlack = 5 * time.Millisecond

// offeredRootsPerPeer bounds the groups whose offers are kept per connection: the live groups and those
// not yet tied to a block (the spec's MAX_PENDING_GROUPS) fit with room to spare. A further group's offers
// are not kept until one of the peer's groups ages out.
const offeredRootsPerPeer = 64

// chunkPull is the core's pull state of chunks. The zero value is ready to use. Router goroutine.
type chunkPull struct {
	offers   core.Offers[peer.ID]
	asks     core.Asks[peer.ID]
	promises core.Promises[peer.ID]
	lr       core.LastResorts[peer.ID]
	// held is held_back: the chunks whose ask an IHAVE limit held back, in the order held, retried at
	// the next heartbeat (seacast_ihave.go).
	held core.HeldBack
	// lastOffer is each announcer's last offer of a group: the announcer's offers of the group go
	// offerTTL after it (seacast-lite.md §8.1).
	lastOffer map[peer.ID]map[core.GroupID]time.Time
	// retryDue is, per chunk, the instants its armed retries fall due. A retry armed within RETRY_SLACK of
	// one already armed is that retry, so one event or one sweep that arms it twice starts one timer.
	retryDue map[pullKey][]time.Time
	// askForms is, per chunk, the ids an ask still queued carries (seacastAskQueued, until its record
	// closes): a queued ask is found by the id it carries, which the chunk's learned id, aged out
	// meanwhile (haveAge), may no longer name.
	askForms map[pullKey][]string
	// orderKey is the secret of the node's own order of a chunk's announcers beyond its ordered list
	// (seacast-lite.md §6.1's own_order), drawn at random at its first use and never sent.
	orderKey    [32]byte
	orderKeySet bool
}

// pullOn says whether chunks are pulled through the core: the ask discipline, the commitment park, the
// park per group and the offer table are installed, over the HAVE bitmaps that name a chunk's group and
// index.
func (gs *GossipSubRouter) pullOn() bool {
	return gs.iwantPromised != nil && gs.parkGroups != nil && gs.offerTable != nil && gs.have != nil
}

// pullChunk names the chunk mid is, when the core pulls it: its group's root and key, and its index. mid
// is a chunk's id or an ask by index.
func (gs *GossipSubRouter) pullChunk(mid string) (root string, g core.GroupID, i uint32, ok bool) {
	if !gs.pullOn() {
		return "", "", 0, false
	}
	root, i, ok = gs.groupIndex(mid)
	if !ok || len(root) != haveRootLen {
		return "", "", 0, false
	}
	return root, seacastGID(root), i, true
}

// pullParkGroup is the group whose park an ask of mid reads, and that a broken promise of mid parks
// its peer for: a chunk's, or a v4 WANT's (a direct ask, such as the probe's), when the core pulls.
func (gs *GossipSubRouter) pullParkGroup(mid string) (core.GroupID, bool) {
	if !gs.pullOn() {
		return "", false
	}
	if root, _, _, _, ok, malformed := decodeWantV2(mid); ok && !malformed && len(root) == haveRootLen {
		return seacastGID(root), true
	}
	if root, _, ok := gs.groupIndex(mid); ok && len(root) == haveRootLen {
		return seacastGID(root), true
	}
	return "", false
}

// chunkForms is the ids an ask of chunk i of root can go by: the ask by index, the chunk's id once
// known, and the ids its asks still queued carry (askForms).
func (gs *GossipSubRouter) chunkForms(root string, i uint32) []string {
	forms := []string{encodeWant(root, i)}
	if m := gs.have.mids[root]; m != nil {
		if id, ok := m.ids[i]; ok {
			forms = append(forms, id)
		}
	}
	for _, id := range gs.pull.askForms[pullKey{seacastGID(root), i}] {
		if !slices.Contains(forms, id) {
			forms = append(forms, id)
		}
	}
	return forms
}

// pullAskFormGone forgets mid as a form its chunk was asked under: no ask under it is queued any more.
func (gs *GossipSubRouter) pullAskFormGone(mid string) {
	_, g, i, ok := gs.pullChunk(mid)
	if !ok {
		return
	}
	k := pullKey{g, i}
	forms := slices.DeleteFunc(gs.pull.askForms[k], func(f string) bool { return f == mid })
	if len(forms) == 0 {
		delete(gs.pull.askForms, k)
	} else {
		gs.pull.askForms[k] = forms
	}
}

// pullAskForm records that chunk i of g was asked under mid, for chunkForms.
func (gs *GossipSubRouter) pullAskForm(g core.GroupID, i uint32, mid string) {
	k := pullKey{g, i}
	if slices.Contains(gs.pull.askForms[k], mid) {
		return
	}
	if gs.pull.askForms == nil {
		gs.pull.askForms = map[pullKey][]string{}
	}
	gs.pull.askForms[k] = append(gs.pull.askForms[k], mid)
}

// chunkAskID is the id an ask of chunk i of root carries: the chunk's id once known, else the ask by
// index, as haveExpandIHave names an offered index.
func (gs *GossipSubRouter) chunkAskID(root string, i uint32) string {
	if m := gs.have.mids[root]; m != nil {
		if id, ok := m.ids[i]; ok {
			return id
		}
	}
	return encodeWant(root, i)
}

// chunkHeld says whether the node holds chunk i of root: received or rebuilt, or its id seen and the
// copy in validation.
func (gs *GossipSubRouter) chunkHeld(root string, i uint32) bool {
	if e := gs.have.mine[seacastGID(root)]; e != nil && e.Bits.Has(i) {
		return true
	}
	if m := gs.have.mids[root]; m != nil {
		if id, ok := m.ids[i]; ok {
			return gs.p.seenMessage(id)
		}
	}
	return false
}

// chunkExpired says whether root's group is past its lifetime, on SEACAST's path.
func (gs *GossipSubRouter) chunkExpired(root string) bool {
	return gs.seacast != nil && gs.expired(root)
}

// pullOutstanding is outstanding(p, a): p's promise of chunk i of g is open, or an ask of it to p is
// still queued, under either id.
func (gs *GossipSubRouter) pullOutstanding(p peer.ID, root string, g core.GroupID, i uint32) bool {
	if gs.pull.promises.Promised(p, g, i) {
		return true
	}
	for _, mid := range gs.chunkForms(root, i) {
		if gs.seacastAskIsQueued(p, mid) {
			return true
		}
	}
	return false
}

// pullAddOffer is add_offer: p offers chunk i of g. It is kept while p's offers name at most
// offeredRootsPerPeer groups, and renews p's offers of g for offerTTL.
func (gs *GossipSubRouter) pullAddOffer(p peer.ID, g core.GroupID, i uint32) {
	if i >= core.MaxPlainChunks {
		return // no group has it (valid_id): neither kept nor a renewal of p's offers of g
	}
	byGroup := gs.pull.lastOffer[p]
	if _, known := byGroup[g]; !known && len(byGroup) >= offeredRootsPerPeer {
		return
	}
	if gs.pull.lastOffer == nil {
		gs.pull.lastOffer = map[peer.ID]map[core.GroupID]time.Time{}
	}
	if byGroup == nil {
		byGroup = map[core.GroupID]time.Time{}
		gs.pull.lastOffer[p] = byGroup
	}
	byGroup[g] = time.Now()
	gs.pull.offers.Add(p, g, i)
}

// pullOfferKept says whether p's offer of chunk i of g is kept. An offer the core did not keep, of a
// group past the groups kept per peer or of an index no group can have, is ignored: not asked, so
// no ask is left that the offer's aging or drop_beyond cannot reach, and its id not learned.
func (gs *GossipSubRouter) pullOfferKept(p peer.ID, g core.GroupID, i uint32) bool {
	return gs.pull.offers.Offered(p, g, i)
}

// pullMayAsk is may_ask for chunk i of g from p, the ask an offer makes (selectIWants, the held-back
// asks): recorded, and the retry armed for when the window has run, if the core allows it.
func (gs *GossipSubRouter) pullMayAsk(p peer.ID, g core.GroupID, i uint32) bool {
	if !gs.pull.asks.MayAsk(g, i, p, time.Now(), gs.iwantWindow) {
		return false
	}
	gs.pullArmRetry(g, i)
	return true
}

// pullKey is a chunk: its group and index.
type pullKey struct {
	g core.GroupID
	i uint32
}

// pullArmRetry arms retry_offer for chunk i of g, IWANT_WINDOW + RETRY_SLACK from now, unless a retry of
// the chunk falls due within RETRY_SLACK of that already. The one armed falls due at most RETRY_SLACK
// early, so still after the window of an ask made now.
func (gs *GossipSubRouter) pullArmRetry(g core.GroupID, i uint32) {
	k, due := pullKey{g, i}, time.Now().Add(gs.iwantWindow+retrySlack)
	for _, d := range gs.pull.retryDue[k] {
		if diff := d.Sub(due); diff > -retrySlack && diff < retrySlack {
			return
		}
	}
	if gs.pull.retryDue == nil {
		gs.pull.retryDue = map[pullKey][]time.Time{}
	}
	gs.pull.retryDue[k] = append(gs.pull.retryDue[k], due)
	time.AfterFunc(gs.iwantWindow+retrySlack, func() {
		select {
		case gs.p.eval <- func() {
			at := slices.IndexFunc(gs.pull.retryDue[k], func(d time.Time) bool { return d.Equal(due) })
			if at < 0 {
				return // its group was forgotten since (pullForgetGroup): a group taken up again arms its own
			}
			dues := slices.Delete(gs.pull.retryDue[k], at, at+1)
			if len(dues) == 0 {
				delete(gs.pull.retryDue, k)
			} else {
				gs.pull.retryDue[k] = dues
			}
			gs.pullRetry(g, i)
		}:
		case <-gs.p.ctx.Done():
		}
	})
}

// pullRetry is retry_offer for chunk i of g. It returns the announcer asked, and whether the queue took
// the ask. Router goroutine.
func (gs *GossipSubRouter) pullRetry(g core.GroupID, i uint32) (peer.ID, bool) {
	env := gs.pullEnvOf(g, false)
	pick, ok := core.RetryOffer[peer.ID](env, &gs.pull.asks, g, i, time.Now(), gs.iwantWindow)
	if !ok {
		return "", false
	}
	return pick, env.sent
}

// pullLastResort is last_resort for chunk i of g. Router goroutine.
func (gs *GossipSubRouter) pullLastResort(g core.GroupID, i uint32) {
	if gs.pull.lr.Arm(gs.pullEnvOf(g, false), g, i, gs.parkPromise) {
		gs.v05Ctr().LastResortsArmed.Add(1)
	}
}

// pullAskParked is ask_parked for chunk i of g, its timer having fired. Router goroutine.
func (gs *GossipSubRouter) pullAskParked(g core.GroupID, i uint32) {
	gs.pull.lr.AskParked(gs.pullEnvOf(g, true), &gs.pull.asks, g, i, time.Now(), gs.iwantWindow, retrySlack)
}

// pullSendAsk sends one ask of chunk i of g to p, under the id chunkAskID names, recorded as every ask
// is, and arms the retry for it; it reports whether the queue took it.
func (gs *GossipSubRouter) pullSendAsk(p peer.ID, root string, g core.GroupID, i uint32) bool {
	mid := gs.chunkAskID(root, i)
	gs.seacastNoteAskOutside(p, mid) // §4.5's may_be_asked on the ordinary asks: counted, not refused
	if !slices.Contains(gs.pull.offers.List(g, i), p) {
		gs.v05Ctr().BeyondList.Add(1)
	}
	if gs.seacast == nil {
		gs.iasked[p]++ // with the path, only asks in reply to offers count (seacast.md §4.3)
	}
	gs.gossipTracer.AddPromise(p, []string{mid})
	gs.noteIWantSent(p, []string{mid})
	gs.pullArmRetry(g, i)
	return gs.sendRPC(p, rpcWithControl(nil, nil, []*pb.ControlIWant{{MessageIDs: []string{mid}}}, nil, nil, nil), false)
}

// pullPromiseWritten opens p's promise of chunk i of g for an ask the writer wrote at the time at, and
// arms promise_due PROMISE after the write. An ask re-written while its promise is open keeps the open
// promise. The caller made the write's checks (promisesFromWrite). Router goroutine.
func (gs *GossipSubRouter) pullPromiseWritten(p peer.ID, g core.GroupID, i uint32, at time.Time) {
	if !gs.pull.promises.Open(g, i, p, at) {
		return
	}
	gs.v05Ctr().PromisesAtWrite.Add(1)
	time.AfterFunc(max(0, gs.parkPromise-time.Since(at)), func() {
		select {
		case gs.p.eval <- func() { gs.pullPromiseDue(p, g, i, at) }:
		case <-gs.p.ctx.Done():
		}
	})
}

// pullPromiseDue is promise_due: p's promise of chunk i of g, opened at at, falls due. If it is still
// that promise, p is parked for g for PARK_TTL. Router goroutine.
func (gs *GossipSubRouter) pullPromiseDue(p peer.ID, g core.GroupID, i uint32, at time.Time) {
	now := time.Now()
	was := gs.pull.promises.Parked(p, g, now)
	if gs.pull.promises.Due(g, i, p, at, now, gs.parkTTL) {
		gs.pullCountPark(was)
	}
}

// pullPark parks p for g for PARK_TTL, for a broken promise the router keeps itself (the probe's,
// seacast.md §4.6). Router goroutine.
func (gs *GossipSubRouter) pullPark(p peer.ID, g core.GroupID) {
	now := time.Now()
	was := gs.pull.promises.Parked(p, g, now)
	gs.pull.promises.Park(p, g, now.Add(gs.parkTTL))
	gs.pullCountPark(was)
}

// pullCountPark counts a broken promise, and a park when the peer was not parked for the group already.
func (gs *GossipSubRouter) pullCountPark(wasParked bool) {
	if gs.parkBreaksCtr != nil {
		gs.parkBreaksCtr.Add(1)
	}
	if !wasParked && gs.parksCtr != nil {
		gs.parksCtr.Add(1)
	}
}

// pullClose is close_promises(g, i): chunk i of g arrived, or was rebuilt. Its promises close, its offers
// and ordered list go, what was kept to ask for it and its last-resort count go, and no ask of it stays
// queued as outstanding. Router goroutine.
func (gs *GossipSubRouter) pullClose(root string, g core.GroupID, i uint32) {
	gs.pull.promises.Close(g, i)
	gs.pull.offers.Close(g, i)
	gs.pull.asks.Forget(g, i)
	gs.pull.lr.Forget(g, i)
	for _, mid := range gs.chunkForms(root, i) {
		gs.seacastAsksClosed(mid)
	}
}

// pullOfferDropped is drop_offer(g, i, p): p's offer of chunk i of g goes, an ask of it still queued to p
// is marked for the writer to drop, under either id, and p's promise of it closes without a charge. It
// reports whether an ask or a promise was open, so the caller runs the retry without p (F13). Router
// goroutine.
func (gs *GossipSubRouter) pullOfferDropped(p peer.ID, root string, g core.GroupID, i uint32) bool {
	gs.pull.offers.Drop(p, g, i)
	open := false
	if sc := gs.seacast; sc != nil {
		for _, mid := range gs.chunkForms(root, i) {
			if _, queued := sc.askQueued[mid][p]; queued {
				gs.seacastDropQueuedAsk(p, mid)
				open = true
			}
		}
	}
	if gs.pull.promises.Promised(p, g, i) {
		gs.pull.promises.CloseFrom(g, i, p)
		if gs.seacast != nil {
			gs.seacast.ctr.F13Closed.Add(1)
		}
		open = true
	}
	return open
}

// pullAbsolve closes p's promise of chunk i of g without a charge, as a delivery would, and nothing of
// the node's is queued to p for it any more: p answered with a copy that waits (a tie). Router goroutine.
func (gs *GossipSubRouter) pullAbsolve(p peer.ID, root string, g core.GroupID, i uint32) {
	for _, mid := range gs.chunkForms(root, i) {
		gs.seacastAskClosed(p, mid)
	}
	gs.pull.promises.CloseFrom(g, i, p)
}

// pullDropBeyond is drop_beyond's pull for g, whose count n is now known: what named an index of g at or
// past n goes, its queued asks at every peer, its time asked and tries, its promises, its offers and its
// last-resort count. Router goroutine.
func (gs *GossipSubRouter) pullDropBeyond(root string, g core.GroupID, n uint32) int {
	env := &pullBeyondEnv{gs: gs, root: root}
	core.DropBeyond[peer.ID](env, &gs.pull.offers, &gs.pull.asks, &gs.seacast.ties, g, n)
	return env.n
}

// pullBeyondEnv is DropBeyond's env over the router.
type pullBeyondEnv struct {
	gs   *GossipSubRouter
	root string
	n    int
}

// Unqueue marks every ask of chunk i still queued, under either id, for its writer to drop.
func (e *pullBeyondEnv) Unqueue(_ core.GroupID, i uint32) {
	e.n++
	sc := e.gs.seacast
	if sc == nil {
		return
	}
	for _, mid := range e.gs.chunkForms(e.root, i) {
		for p := range sc.askQueued[mid] {
			e.gs.seacastDropQueuedAsk(p, mid)
		}
	}
}

// ClosePromises is close_promises(g, i) but for the asks, which DropBeyond forgets itself.
func (e *pullBeyondEnv) ClosePromises(g core.GroupID, i uint32) {
	e.gs.pull.promises.Close(g, i)
	e.gs.pull.offers.Close(g, i)
	e.gs.pull.lr.Forget(g, i)
}

// pullForgetGroup drops g's pull state: g expired or was dropped (seacast-lite.md §8.1). Router
// goroutine.
func (gs *GossipSubRouter) pullForgetGroup(g core.GroupID) {
	gs.pull.offers.ForgetGroup(g)
	gs.pull.asks.ForgetGroup(g)
	gs.pull.promises.ForgetGroup(g)
	gs.pull.lr.ForgetGroup(g)
	for k := range gs.pull.askForms {
		if k.g == g {
			delete(gs.pull.askForms, k)
		}
	}
	for k := range gs.pull.retryDue {
		if k.g == g {
			delete(gs.pull.retryDue, k) // their timers, firing, find no due of theirs and do nothing
		}
	}
	for p, byGroup := range gs.pull.lastOffer {
		delete(byGroup, g)
		if len(byGroup) == 0 {
			delete(gs.pull.lastOffer, p)
		}
	}
}

// pullPeerGone drops p's pull state: its connection closed (seacast-lite.md §8.1). Its offers and ordered
// places, its tries and last-resort counts go, and its promises close; its parks stay. It returns the
// promises closed. Router goroutine, from RemovePeer.
func (gs *GossipSubRouter) pullPeerGone(p peer.ID) int {
	gs.pull.offers.Forget(p)
	gs.pull.asks.ForgetPeer(p)
	gs.pull.lr.ForgetPeer(p)
	delete(gs.pull.lastOffer, p)
	return gs.pull.promises.ForgetPeer(p)
}

// pullSweep ages the pull state at the heartbeat (seacast-lite.md §8.1): an announcer's offers of a group
// go offerTTL after its last offer of it, each as drop_offer, and the retry runs without it for each
// chunk whose ask or promise went with them; tries and last-resort counts go offerTTL after they were
// made, a chunk's time asked once its window has run and no try is left, and parks once they ended.
// Router goroutine.
func (gs *GossipSubRouter) pullSweep() {
	if !gs.pullOn() {
		return
	}
	now := time.Now()
	type chunk struct {
		root string
		g    core.GroupID
		i    uint32
	}
	var retry []chunk
	for p, byGroup := range gs.pull.lastOffer {
		for g, at := range byGroup {
			if now.Sub(at) < offerTTL {
				continue
			}
			delete(byGroup, g)
			root := seacastGIDRoot(g)
			for _, i := range gs.pull.offers.Indices(p, g) {
				if gs.pullOfferDropped(p, root, g, i) {
					retry = append(retry, chunk{root, g, i})
				}
			}
		}
		if len(byGroup) == 0 {
			delete(gs.pull.lastOffer, p)
		}
	}
	gs.pull.asks.Sweep(now, gs.iwantWindow, offerTTL)
	gs.pull.lr.Sweep(now, offerTTL)
	gs.pull.promises.Sweep(now)
	for _, c := range retry {
		if !gs.chunkHeld(c.root, c.i) {
			gs.pullRetry(c.g, c.i)
		}
	}
}

// ownRank is the reference rank of p as an announcer of chunk i of root: the first eight bytes of
// SHA-256 over the node's secret, the group, the index and the peer id. Larger is tried first.
func (c *chunkPull) ownRank(root string, i uint32, p peer.ID) uint64 {
	if !c.orderKeySet {
		if _, err := cryptorand.Read(c.orderKey[:]); err != nil {
			panic(fmt.Sprintf("own order: drawing the secret: %v", err))
		}
		c.orderKeySet = true
	}
	h := sha256.New()
	h.Write(c.orderKey[:])
	h.Write([]byte(root))
	var ix [4]byte
	binary.BigEndian.PutUint32(ix[:], i)
	h.Write(ix[:])
	h.Write([]byte(p))
	return binary.BigEndian.Uint64(h.Sum(nil)[:8])
}

// pullEnv is the core's RetryEnv and LastResortEnv over the router, for one chunk's retry or last resort.
type pullEnv struct {
	gs   *GossipSubRouter
	root string
	// parkedAsk is set for ask_parked: a wait it arms again is a deferral.
	parkedAsk bool
	// sent says whether the queue took the ask made.
	sent bool
}

// pullEnvOf is the env for a chunk of g; parkedAsk for ask_parked's.
func (gs *GossipSubRouter) pullEnvOf(g core.GroupID, parkedAsk bool) *pullEnv {
	return &pullEnv{gs: gs, root: seacastGIDRoot(g), parkedAsk: parkedAsk}
}

func (e *pullEnv) Held(_ core.GroupID, i uint32) bool { return e.gs.chunkHeld(e.root, i) }
func (e *pullEnv) StopPull(core.GroupID) bool {
	return e.gs.seacastStopPulledRoot(e.root)
}
func (e *pullEnv) Expired(core.GroupID) bool { return e.gs.chunkExpired(e.root) }

// Announcers is announcers(a): the chunk's ordered list, then every other connected peer that offered
// it, in the node's own order. A listed peer the router has no stream to (its offer came on an inbound
// stream before the outbound one, or while it was replaced) is passed over, as before the chunk pull.
// So is one whose offers of the group have lived their offerTTL (seacast-lite.md §8.1), which the
// heartbeat's sweep may not have dropped yet: the held-back retry runs before it.
func (e *pullEnv) Announcers(g core.GroupID, i uint32) []peer.ID {
	peers := make([]peer.ID, 0, len(e.gs.peers))
	for p := range e.gs.peers {
		peers = append(peers, p)
	}
	ranks := map[peer.ID]uint64{}
	anns := e.gs.pull.offers.Announcers(g, i, peers, func(p peer.ID) uint64 {
		r, ok := ranks[p]
		if !ok {
			r = e.gs.pull.ownRank(e.root, i, p)
			ranks[p] = r
		}
		return r
	})
	now := time.Now()
	return slices.DeleteFunc(anns, func(p peer.ID) bool {
		if _, connected := e.gs.peers[p]; !connected {
			return true
		}
		at, ok := e.gs.pull.lastOffer[p][g]
		return ok && now.Sub(at) >= offerTTL
	})
}

func (e *pullEnv) Parked(p peer.ID, g core.GroupID) bool {
	return e.gs.pull.promises.Parked(p, g, time.Now())
}
func (e *pullEnv) Tied(p peer.ID, _ core.GroupID, i uint32) bool {
	return e.gs.seacastTiedIndex(p, e.root, i)
}
func (e *pullEnv) Outstanding(p peer.ID, g core.GroupID, i uint32) bool {
	return e.gs.pullOutstanding(p, e.root, g, i)
}
func (e *pullEnv) LastResort(g core.GroupID, i uint32) { e.gs.pullLastResort(g, i) }

// RetryLater arms the retry again after the window: every announcer left has an ask outstanding.
func (e *pullEnv) RetryLater(g core.GroupID, i uint32) {
	if e.gs.seacast != nil {
		e.gs.seacast.ctr.RetryDeferred.Add(1)
	}
	e.gs.pullArmRetry(g, i)
}

// Ask is the retry's ask.
func (e *pullEnv) Ask(p peer.ID, g core.GroupID, i uint32) {
	if e.gs.offerRetryCtr != nil {
		e.gs.offerRetryCtr.Add(1)
	}
	e.sent = e.gs.pullSendAsk(p, e.root, g, i)
}

// AskParkedAfter arms ask_parked d from now. A timer whose arm was forgotten meanwhile, with its group
// (pullForgetGroup: the window can take a forgotten root up again), does nothing: its token is no
// longer the chunk's.
func (e *pullEnv) AskParkedAfter(g core.GroupID, i uint32, d time.Duration) {
	if e.parkedAsk {
		e.gs.v05Ctr().LastResortsDeferred.Add(1)
	}
	gs := e.gs
	tok := gs.pull.lr.Armed(g, i)
	time.AfterFunc(d, func() {
		select {
		case gs.p.eval <- func() {
			if gs.pull.lr.Armed(g, i) == tok {
				gs.pullAskParked(g, i)
			}
		}:
		case <-gs.p.ctx.Done():
		}
	})
}

// Enqueue is the last resort's ask; it reports whether the queue took it.
func (e *pullEnv) Enqueue(p peer.ID, g core.GroupID, i uint32) bool {
	ctr := e.gs.v05Ctr()
	ctr.LastResortAsks.Add(1)
	e.sent = e.gs.pullSendAsk(p, e.root, g, i)
	if e.sent && e.Parked(p, g) {
		ctr.ParkedAsked.Add(1)
	}
	return e.sent
}

// noV05Counters takes the counts of a router without WithSpecV05's counters.
var noV05Counters SpecV05Counters

// v05Ctr is the v0.5 rules' counters, which the chunk pull counts into too.
func (gs *GossipSubRouter) v05Ctr() *SpecV05Counters {
	if gs.v05 != nil && gs.v05.ctr != nil {
		return gs.v05.ctr
	}
	return &noV05Counters
}
