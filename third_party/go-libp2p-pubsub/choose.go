package pubsub

import (
	"fmt"
	"sync/atomic"

	"github.com/libp2p/go-libp2p/core/peer"
)

// --- Which entry goes next (seacast.md §4.12; experimental) ---
//
// choose, after the head's control-only entry: a source's own chunks first, over the whole normal
// lane, by pick (the fewest holders, the earliest on a tie); then v0.3's rarest frame ahead of the
// first control-only entry (rarestIndex). ANSWERS_FIRST, an option off in every setting, is not
// in this fork.
//
// An entry is own when every frame in it is a push of a chunk this node published and did not
// rebuild: rpc.own, set as the router enqueues it (doSendRPC, from Publish of a message received from
// ourselves or from PublishBatch, never for a message published WithRebuiltPublication, never for an
// answer). That reads the draft's own_chunk at enqueue: a node never records a chunk it published as
// received (a copy back is a duplicate, refused before the fill), and a rebuilt chunk is marked at its
// publication, so the mark does not change while the entry waits.

// WithOwnChunksFirst makes the writer send this node's own chunks first (§4.12's choose). Requires
// WithHaveBitmaps.
func WithOwnChunksFirst() Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok || gs.have == nil {
			return fmt.Errorf("own chunks first requires WithHaveBitmaps first")
		}
		gs.have.ownFirst = true
		return nil
	}
}

// ownPublication reports whether Publish's message is our own chunk: published here, not rebuilt.
func ownPublication(msg *Message, self peer.ID) bool {
	return msg.ReceivedFrom == self && !msg.rebuilt
}

// enableChoose installs the writer's selection rule once; own, when set, is the router's counter
// of the picks (ChooseStats), beside the queue's own.
func (q *rpcQueue) enableChoose(ownFirst bool, own *atomic.Int64) {
	q.queueMu.Lock()
	defer q.queueMu.Unlock()
	if q.have != nil {
		q.have.ownFirst = ownFirst
		q.have.ownPickCtr = own
	}
}

// ChooseStats is the node's own choices that went ahead of a queue's head, over every peer since
// the start (WithOwnChunksFirst). An atomic counter: safe from any goroutine, and outside a
// synctest bubble too.
func (p *PubSub) ChooseStats() (own int64) {
	gs, ok := p.rt.(*GossipSubRouter)
	if !ok || gs.have == nil {
		return 0
	}
	return gs.have.ownPicks.Load()
}

// chooseIndex is the normal-lane index the writer pops next under §4.12, or -1 for the queue's own
// order (the head, or the lowest rank). Without the option it is rarestIndex. ownPicks counts the
// own choices that went ahead of the head. Under queueMu.
func (h *queueHave) chooseIndex(normal []rankedRPC, ownPicks *atomic.Int64) int {
	if len(normal) == 0 || len(normal[0].rpc.Publish) == 0 {
		return -1 // control at the head goes at once
	}
	if h.ownFirst {
		if k := h.pick(normal, func(r *RPC) bool { return r.own && !r.solicited }); k >= 0 {
			if k > 0 {
				ownPicks.Add(1)
				if h.ownPickCtr != nil {
					h.ownPickCtr.Add(1)
				}
			}
			return k
		}
	}
	return h.rarestIndex(normal)
}

// pick is §4.12's pick: of the data entries that pass in, the one with the fewest holders, the
// earliest on a tie; without holder counts (rarest dispatch off), the earliest. A frame whose holders
// are unknown counts none. -1 when no entry passes. Under queueMu.
func (h *queueHave) pick(normal []rankedRPC, in func(*RPC) bool) int {
	best, bestHolders := -1, 0
	lead := replayLead(normal, h.index)
	for i := range normal {
		rpc := normal[i].rpc
		if len(rpc.Publish) == 0 || !in(rpc) || (rpc.replay && i >= lead) {
			continue // an owed answer behind a stock entry or control is not taken past it (replayLead)
		}
		if h.holders == nil {
			return i
		}
		holders := -1
		for _, m := range rpc.Publish {
			c, ok := h.holders(m)
			if !ok {
				c = 0
			}
			if holders < 0 || c < holders {
				holders = c
			}
		}
		if best < 0 || holders < bestHolders {
			best, bestHolders = i, holders
		}
	}
	return best
}
