package pubsub

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
)

// gossipTracer is an internal tracer that tracks IWANT requests in order to penalize
// peers who don't follow up on IWANT requests after an IHAVE advertisement.
// The tracking of promises is probabilistic to avoid using too much memory.
type gossipTracer struct {
	sync.Mutex

	idGen *msgIDGenerator

	followUpTime time.Duration

	// promises for messages by message ID; for each message tracked, we track the promise
	// expiration time for each peer.
	promises map[string]map[peer.ID]time.Time
	// promises for each peer; for each peer, we track the promised message IDs.
	// this index allows us to quickly void promises when a peer is throttled.
	peerPromises map[peer.ID]map[string]struct{}

	// atWrite is v0.5's E (PromiseAtWrite, specv05.go) for the score's promise, as for the park's:
	// a promise starts when the writer writes the ask. AddPromise picks the id as before and keeps
	// it pending, with the time of its dispatch, until the writer reports the write
	// (promisesWritten). An ask the queue refuses, or that leaves with a closed connection, was
	// never written and breaks no promise; one that waits in the queue has its follow-up time from
	// the write.
	atWrite     bool
	pending     map[string]map[peer.ID]time.Time
	peerPending map[peer.ID]map[string]struct{}

	// wantOf maps a message's id to the id of an ask by index for it (havebitmap.go's encodeWant),
	// where the router reads a group and an index from ids. Upstream fulfils a promise when a copy
	// of its message begins validation, is rejected for a reason other than its signature, or is
	// delivered; an ask by index is fulfilled at the same points, so a reply that validation then
	// ignores, which the receiver must not hold against its sender, breaks no promise; and by a
	// duplicate, which answers an ask by index for a chunk already seen (DuplicateMessage). Set at
	// Start from the HAVE state's index function, which reads only the id: nil without HAVE bitmaps.
	wantOf func(mid string) (string, bool)
}

// promisePendingTTL bounds a pending promise whose write is never reported (an ask a filter
// dropped in the queue, a report for a queue no longer the peer's): the heartbeat forgets it,
// uncharged, past this age.
const promisePendingTTL = 30 * time.Second

func newGossipTracer() *gossipTracer {
	return &gossipTracer{
		idGen:        newMsgIdGenerator(),
		promises:     make(map[string]map[peer.ID]time.Time),
		peerPromises: make(map[peer.ID]map[string]struct{}),
		pending:      make(map[string]map[peer.ID]time.Time),
		peerPending:  make(map[peer.ID]map[string]struct{}),
	}
}

func (gt *gossipTracer) Start(gs *GossipSubRouter) {
	if gt == nil {
		return
	}

	gt.idGen = gs.p.idGen
	gt.followUpTime = gs.params.IWantFollowupTime
	// Under E the writer reports each ask it writes (v05QueueHooks' onAsks), and the score's
	// promise waits for that report.
	gt.atWrite = gs.v05PromiseAtWrite()
	if gs.have != nil && gs.have.index != nil {
		index := gs.have.index
		gt.wantOf = func(mid string) (string, bool) {
			root, i, ok := index(mid)
			if !ok || len(root) != haveRootLen {
				return "", false
			}
			return encodeWant(root, i), true
		}
	}
}

// track a promise to deliver a message from a list of msgIDs we are requesting
func (gt *gossipTracer) AddPromise(p peer.ID, msgIDs []string) {
	if gt == nil {
		return
	}

	idx := rand.Intn(len(msgIDs))
	mid := msgIDs[idx]

	gt.Lock()
	defer gt.Unlock()

	if gt.atWrite {
		if _, open := gt.promises[mid][p]; !open {
			gt.pendLocked(mid, p, time.Now())
		}
		return
	}

	promises, ok := gt.promises[mid]
	if !ok {
		promises = make(map[peer.ID]time.Time)
		gt.promises[mid] = promises
	}

	_, ok = promises[p]
	if !ok {
		promises[p] = time.Now().Add(gt.followUpTime)
		peerPromises, ok := gt.peerPromises[p]
		if !ok {
			peerPromises = make(map[string]struct{})
			gt.peerPromises[p] = peerPromises
		}
		peerPromises[mid] = struct{}{}
	}
}

// pendLocked keeps mid's promise from p pending its write (E), from the dispatch at at; an earlier
// pending dispatch keeps its time. Under the lock.
func (gt *gossipTracer) pendLocked(mid string, p peer.ID, at time.Time) {
	peers := gt.pending[mid]
	if peers == nil {
		peers = make(map[peer.ID]time.Time)
		gt.pending[mid] = peers
	}
	if _, ok := peers[p]; ok {
		return
	}
	peers[p] = at
	mids := gt.peerPending[p]
	if mids == nil {
		mids = make(map[string]struct{})
		gt.peerPending[p] = mids
	}
	mids[mid] = struct{}{}
}

// unpendLocked forgets mid's pending promise from p, and reports whether there was one. Under the
// lock.
func (gt *gossipTracer) unpendLocked(mid string, p peer.ID) bool {
	peers, ok := gt.pending[mid]
	if !ok {
		return false
	}
	if _, ok := peers[p]; !ok {
		return false
	}
	delete(peers, p)
	if len(peers) == 0 {
		delete(gt.pending, mid)
	}
	if mids := gt.peerPending[p]; mids != nil {
		delete(mids, mid)
		if len(mids) == 0 {
			delete(gt.peerPending, p)
		}
	}
	return true
}

// promisesWritten opens, for each of mids the writer wrote to p at the time at, the promise its
// dispatch left pending (E): due the follow-up time after the write. An id with nothing pending
// was not picked, or closed since (delivered, refused, its group ended, the peer gone or
// throttled), and opens nothing; an open promise keeps its deadline. Returns how many it opened.
// Router goroutine.
func (gt *gossipTracer) promisesWritten(p peer.ID, mids []string, at time.Time) int {
	if gt == nil || !gt.atWrite {
		return 0
	}
	gt.Lock()
	defer gt.Unlock()
	opened := 0
	for _, mid := range mids {
		if !gt.unpendLocked(mid, p) {
			continue
		}
		promises := gt.promises[mid]
		if promises == nil {
			promises = make(map[peer.ID]time.Time)
			gt.promises[mid] = promises
		}
		if _, open := promises[p]; open {
			continue
		}
		promises[p] = at.Add(gt.followUpTime)
		peerPromises := gt.peerPromises[p]
		if peerPromises == nil {
			peerPromises = make(map[string]struct{})
			gt.peerPromises[p] = peerPromises
		}
		peerPromises[mid] = struct{}{}
		opened++
	}
	return opened
}

// asksRefused forgets the pending promises of the asks of mids to p that were dropped before
// leaving: a full queue refused them, p had no queue, or their RPC was oversized (E). Without E a
// refused ask keeps the promise from its dispatch, as upstream's does. Router goroutine, from
// doDropRPC (noteIWantDropped).
func (gt *gossipTracer) asksRefused(p peer.ID, mids []string) {
	if gt == nil || !gt.atWrite {
		return
	}
	gt.Lock()
	defer gt.Unlock()
	for _, mid := range mids {
		gt.unpendLocked(mid, p)
	}
}

// peerGone forgets p's pending promises when its connection closes (E): its queue goes with the
// connection, so they were never written. Then, when closeOpen is given (CloseWithConn), the open
// promises of the asks closeOpen names close too, as the park's do (specv05.go, v05PeerGone); the
// others stay, as upstream keeps a departed peer's. Returns how many open promises closed. Router
// goroutine.
func (gt *gossipTracer) peerGone(p peer.ID, closeOpen func(mid string) bool) int {
	if gt == nil {
		return 0
	}
	gt.Lock()
	defer gt.Unlock()
	if gt.atWrite {
		for mid := range gt.peerPending[p] {
			gt.unpendLocked(mid, p)
		}
	}
	if closeOpen == nil {
		return 0
	}
	closed := 0
	for mid := range gt.peerPromises[p] {
		if !closeOpen(mid) {
			continue
		}
		promises := gt.promises[mid]
		delete(promises, p)
		if len(promises) == 0 {
			delete(gt.promises, mid)
		}
		delete(gt.peerPromises[p], mid)
		closed++
	}
	if len(gt.peerPromises[p]) == 0 {
		delete(gt.peerPromises, p)
	}
	return closed
}

// returns the number of broken promises for each peer who didn't follow up
// on an IWANT request.
func (gt *gossipTracer) GetBrokenPromises() map[peer.ID]int {
	if gt == nil {
		return nil
	}

	gt.Lock()
	defer gt.Unlock()

	var res map[peer.ID]int
	now := time.Now()

	// Pending promises whose write was never reported go uncharged past their age limit (E).
	for mid, peers := range gt.pending {
		for p, at := range peers {
			if now.Sub(at) > promisePendingTTL {
				gt.unpendLocked(mid, p)
			}
		}
	}

	// find broken promises from peers
	for mid, promises := range gt.promises {
		for p, expire := range promises {
			if expire.Before(now) {
				if res == nil {
					res = make(map[peer.ID]int)
				}
				res[p]++

				delete(promises, p)

				peerPromises := gt.peerPromises[p]
				delete(peerPromises, mid)
				if len(peerPromises) == 0 {
					delete(gt.peerPromises, p)
				}
			}
		}

		if len(promises) == 0 {
			delete(gt.promises, mid)
		}
	}

	return res
}

// WithPromiseTracking installs the gossip tracer without peer scoring, so broken IWANT promises are counted though never penalised.
func WithPromiseTracking() Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok {
			return fmt.Errorf("promise tracking requires a gossipsub router")
		}
		if gs.gossipTracer != nil {
			return nil
		}
		gs.gossipTracer = newGossipTracer()
		if ps.tracer != nil {
			ps.tracer.raw = append(ps.tracer.raw, gs.gossipTracer)
		} else {
			ps.tracer = &pubsubTracer{raw: []RawTracer{gs.gossipTracer}, pid: ps.host.ID(), idGen: ps.idGen}
		}
		return nil
	}
}

// brokenPromiseCount accumulates the broken promises charged per peer, under its own lock.
type brokenPromiseCount struct {
	mu      sync.Mutex
	perPeer map[peer.ID]int
}

func (c *brokenPromiseCount) add(p peer.ID, n int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.perPeer == nil {
		c.perPeer = make(map[peer.ID]int)
	}
	c.perPeer[p] += n
}

// forget drops a peer that left, with its count: a node meeting peers for long keeps none for
// those gone.
func (c *brokenPromiseCount) forget(p peer.ID) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.perPeer, p)
}

// BrokenPromises is the IWANT promises charged since ResetBrokenPromises, per peer the router had an
// outbound gossip stream to when the heartbeat charged them and has kept since (a peer's count goes
// when its stream closes), and those still outstanding per peer, which can include a peer that has
// left, until its promises expire; nil without a gossip tracer. Under E an ask still pending its
// write has no promise yet, so it is not outstanding.
func (p *PubSub) BrokenPromises() (charged, outstanding map[peer.ID]int) {
	gs, ok := p.rt.(*GossipSubRouter)
	if !ok || gs.gossipTracer == nil {
		return nil, nil
	}
	gs.brokenPromises.mu.Lock()
	charged = make(map[peer.ID]int, len(gs.brokenPromises.perPeer))
	for pid, n := range gs.brokenPromises.perPeer {
		charged[pid] = n
	}
	gs.brokenPromises.mu.Unlock()
	gt := gs.gossipTracer
	gt.Lock()
	defer gt.Unlock()
	outstanding = make(map[peer.ID]int, len(gt.peerPromises))
	for pid, mids := range gt.peerPromises {
		outstanding[pid] = len(mids)
	}
	return charged, outstanding
}

// ResetBrokenPromises starts the charged count afresh (per message of a multi-message cell).
func (p *PubSub) ResetBrokenPromises() {
	gs, ok := p.rt.(*GossipSubRouter)
	if !ok {
		return
	}
	gs.brokenPromises.mu.Lock()
	defer gs.brokenPromises.mu.Unlock()
	gs.brokenPromises.perPeer = nil
}

var _ RawTracer = (*gossipTracer)(nil)

func (gt *gossipTracer) fulfillPromise(msg *Message) {
	mid := gt.idGen.ID(msg)
	gt.Lock()
	defer gt.Unlock()
	gt.fulfillLocked(mid)
	if gt.wantOf != nil {
		if want, ok := gt.wantOf(mid); ok {
			gt.fulfillLocked(want) // the same chunk asked by index (wantOf)
		}
	}
}

// fulfillPromiseID fulfils the promises made under mid, for an ask made by index whose message
// arrives under its own id (havebitmap.go).
func (gt *gossipTracer) fulfillPromiseID(mid string) {
	gt.Lock()
	defer gt.Unlock()
	gt.fulfillLocked(mid)
}

// fulfillPromisesWhere fulfils every promise whose id match accepts, and returns how many ids it cleared; match runs under the lock.
func (gt *gossipTracer) fulfillPromisesWhere(match func(mid string) bool) int {
	gt.Lock()
	defer gt.Unlock()
	n := 0
	for mid := range gt.promises {
		if match(mid) {
			gt.fulfillLocked(mid)
			n++
		}
	}
	for mid := range gt.pending { // pending their write (E): they close with the group too
		if match(mid) {
			gt.fulfillLocked(mid)
			n++
		}
	}
	return n
}

// fulfillLocked is fulfillPromiseID with the lock held. Promises of mid still pending their write
// (E) close too: the write would open nothing owed.
func (gt *gossipTracer) fulfillLocked(mid string) {
	if peers, ok := gt.pending[mid]; ok {
		for p := range peers {
			gt.unpendLocked(mid, p)
		}
	}
	promises, ok := gt.promises[mid]
	if !ok {
		return
	}
	delete(gt.promises, mid)

	// delete the promise for all peers that promised it, as they have no way to fulfill it.
	for p := range promises {
		peerPromises, ok := gt.peerPromises[p]
		if ok {
			delete(peerPromises, mid)
			if len(peerPromises) == 0 {
				delete(gt.peerPromises, p)
			}
		}
	}
}

func (gt *gossipTracer) DeliverMessage(msg *Message) {
	// someone delivered a message, fulfill promises for it
	gt.fulfillPromise(msg)
}

func (gt *gossipTracer) RejectMessage(msg *Message, reason string) {
	// A message got rejected, so we can fulfill promises and let the score penalty apply
	// from invalid message delivery.
	// We do take exception and apply promise penalty regardless in the following cases, where
	// the peer delivered an obviously invalid message.
	switch reason {
	case RejectMissingSignature:
		return
	case RejectInvalidSignature:
		return
	}

	gt.fulfillPromise(msg)
}

func (gt *gossipTracer) ValidateMessage(msg *Message) {
	// we consider the promise fulfilled as soon as the message begins validation
	// if it was a case of signature issue it would have been rejected immediately
	// without triggering the Validate trace
	gt.fulfillPromise(msg)
}

func (gt *gossipTracer) OnNewOutboundStream(p peer.ID, proto protocol.ID) {}
func (gt *gossipTracer) OnClosedOutboundStream(p peer.ID)                 {}
func (gt *gossipTracer) Join(topic string)                                {}
func (gt *gossipTracer) Leave(topic string)                               {}
func (gt *gossipTracer) Graft(p peer.ID, topic string)                    {}
func (gt *gossipTracer) Prune(p peer.ID, topic string)                    {}
func (gt *gossipTracer) RecvRPC(rpc *RPC)                                 {}
func (gt *gossipTracer) SendRPC(rpc *RPC, p peer.ID)                      {}
func (gt *gossipTracer) SendRPCCompleted(*RPC, peer.ID, time.Duration)    {}
func (gt *gossipTracer) DropRPC(rpc *RPC, p peer.ID)                      {}
func (gt *gossipTracer) UndeliverableMessage(msg *Message)                {}

func (gt *gossipTracer) DuplicateMessage(msg *Message) {
	// An ask by index can be answered with a copy already seen: B′'s last resort asks a parked
	// announcer again for a chunk whose earlier copy validation ignored, and the answer is then a
	// duplicate. It is a delivery of the chunk, so it fulfils the ask by index (wantOf). Upstream
	// never asks by id for an id already seen, so a promise by id is left as upstream leaves it.
	if gt == nil || gt.wantOf == nil {
		return
	}
	if want, ok := gt.wantOf(gt.idGen.ID(msg)); ok {
		gt.fulfillPromiseID(want)
	}
}

func (gt *gossipTracer) ThrottlePeer(p peer.ID) {
	gt.Lock()
	defer gt.Unlock()

	// Pending promises (E) are voided with the open ones.
	for mid := range gt.peerPending[p] {
		gt.unpendLocked(mid, p)
	}

	peerPromises, ok := gt.peerPromises[p]
	if !ok {
		return
	}

	for mid := range peerPromises {
		promises := gt.promises[mid]
		delete(promises, p)
		if len(promises) == 0 {
			delete(gt.promises, mid)
		}
	}

	delete(gt.peerPromises, p)
}
