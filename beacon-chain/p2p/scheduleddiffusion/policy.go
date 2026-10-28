package scheduleddiffusion

import (
	"iter"
	"sort"
	"time"

	"github.com/OffchainLabs/prysm/v7/container/segments"
	"github.com/libp2p/go-libp2p-pubsub/partialmessages"
	"github.com/libp2p/go-libp2p/core/peer"
)

// publishInput is everything the publish pass needs that does not come from the extension.
// Gathered on the broadcaster's loop and passed in, so the decision itself is a pure
// function of its inputs and can be tested without a network.
type publishInput struct {
	// Fields are ordered widest-first: nogo's maligned check rejects the natural grouping.

	// groupID is the group being served. Carried for logging and for symmetry with the
	// extension's own argument; the decision does not depend on it, because there is no
	// hash-based assignment left to seed.
	groupID []byte
	// held is the set of segments we can serve. Never nil in a real pass.
	held *segments.Bitmap
	// segment returns the wire message for one held index.
	segment func(index uint32) (*segments.SegmentMessage, bool)
	// bump applies a change to the counters. Optional.
	bump func(func(*Counters))
	// deferAnnounce is called when an update was held back, so the group is re-passed once
	// the window closes. A held-back update with no wake-up would otherwise be lost until
	// unrelated traffic re-runs the pass.
	deferAnnounce func()
	// meshPeers, when set, bounds the forwarding set to the peers it yields: the router's
	// mesh for the topic. Read at pass start, on the router goroutine, which is the only
	// place it may be read. Nil forwards to every negotiated peer in the state map -- which
	// grows to the whole connectivity, since every peer that ever sent an RPC for the group
	// is in it. That is the meshless arm, and without dispatch-time decisions it is a flood.
	meshPeers func() iter.Seq[peer.ID]
	// now is the pass's clock reading, for the announce batching window.
	now time.Time
	// announceWindow batches updates per peer. Zero sends every change immediately.
	announceWindow time.Duration
	// required is how many segments reassemble the group: the bitmap width for a plain
	// group, fewer for a coded one. Zero is read as the bitmap width.
	required uint32
	// complete is the reassembler's verdict on this group. A complete node asks for
	// nothing, which is what its peers read as "stop sending me this".
	complete bool
	// urgentUpdates puts metadata updates in the urgent lane.
	urgentUpdates bool
	// compress snappy-compresses the segment in the part. See Config.CompressSegments.
	compress bool
}

// peerPlan is one peer's share of a pass, computed from the pass-start snapshot.
//
// The plan is fixed before the first action is yielded, and deliberately so. OnSent runs
// before the next action is built, so recomputing the sendable set between rounds would
// read a state that the current pass has already half-written; the round-k segment would
// then move or vanish depending on whether round k-1 was admitted. Planning from the
// snapshot and latching incrementally keeps the two independent: a round that is not
// admitted leaves its own index unlatched and nothing else.
type peerPlan struct {
	id peer.ID
	// sendable is held ∧ wanted ∧ ¬(theirs ∪ pushed) at pass start, ascending. One index per
	// data round.
	sendable []uint32
	// meta is the metadata this peer has not been told yet, or nil when its Sent is
	// current or the announce window deferred it.
	meta *segments.PartsMetadata
	err  error
}

// publishActions is the extension's PublishActionsFn: for each peer in the group, what to
// send and -- once the send is admitted -- what the peer's next state becomes.
//
// Every update travels alone and first, then one segment per RPC round-robin across peers,
// so the first segment of every peer leaves before the second of any. A frame is read whole
// at every hop, so a bundle would hold a peer's first segment until its last had crossed the
// uplink.
func publishActions(in publishInput) partialmessages.PublishActionsFn[PeerState] {
	return func(peerStates map[peer.ID]PeerState, peerRequestsPartial func(peer.ID) bool) iter.Seq2[peer.ID, partialmessages.PublishAction] {
		return func(yield func(peer.ID, partialmessages.PublishAction) bool) {
			// A node with nothing has nothing to serve, and the everything-default already
			// states its want, so there is nothing to announce either.
			if in.held == nil || in.held.Len() == 0 {
				return
			}
			plans := planPass(in, peerStates, peerRequestsPartial)
			if len(plans) == 0 {
				return
			}
			runPass(in, peerStates, plans, yield)
		}
	}
}

// planPass computes every peer's share of the pass from the pass-start snapshot.
func planPass(in publishInput, peerStates map[peer.ID]PeerState, peerRequestsPartial func(peer.ID) bool) []peerPlan {
	peers := forwardingSet(in, peerStates, peerRequestsPartial)
	if len(peers) == 0 {
		return nil
	}
	// The same for every peer: there are no per-peer requests to make. A want bitmap is a
	// standing order, so it is broadcast rather than aimed.
	meta, err := groupMetadata(in)
	if err != nil {
		return []peerPlan{{id: peers[0], err: err}}
	}
	plans := make([]peerPlan, 0, len(peers))
	for _, id := range peers {
		state := peerStates[id]
		plan := peerPlan{id: id, sendable: state.sendable(in.held)}
		if !meta.Equal(state.Sent) {
			plan.meta = meta
		}
		if holdBack(in, state, plan) {
			if in.bump != nil {
				in.bump(func(c *Counters) { c.MetadataHeldBack++ })
			}
			if in.deferAnnounce != nil {
				in.deferAnnounce()
			}
			plan.meta = nil
		}
		plans = append(plans, plan)
	}
	return plans
}

// forwardingSet is the peers this pass may send to, in a stable order: the negotiated peers
// in the state map, cut down to the mesh when the pass has one.
//
// A peer that did not negotiate partial messages is skipped entirely: gossipsub delivers it
// the whole envelope on the same topic, so a bitmap would tell it nothing it can act on.
func forwardingSet(in publishInput, peerStates map[peer.ID]PeerState, peerRequestsPartial func(peer.ID) bool) []peer.ID {
	var mesh map[peer.ID]bool
	if in.meshPeers != nil {
		mesh = make(map[peer.ID]bool)
		for p := range in.meshPeers() {
			mesh[p] = true
		}
	}
	out := make([]peer.ID, 0, len(peerStates))
	for id := range peerStates {
		if !peerRequestsPartial(id) {
			continue
		}
		if mesh != nil && !mesh[id] {
			continue
		}
		out = append(out, id)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// holdBack reports whether the announce window defers this peer's update.
//
// A completion update -- we want nothing further -- never waits, because it is the message
// that stops peers sending us duplicates, which is the whole saving the variant exists for.
// The first contact goes immediately: a zero SentAt is nothing to measure a window from.
// Data is never held back; only the update waits, and the frames it would have accompanied
// still carry their own evidence of what we hold.
func holdBack(in publishInput, state PeerState, plan peerPlan) bool {
	if plan.meta == nil || in.announceWindow <= 0 {
		return false
	}
	if state.SentAt.IsZero() || in.now.Sub(state.SentAt) >= in.announceWindow {
		return false
	}
	return plan.meta.Requests.Len() > 0
}

// runPass yields the planned actions: every peer's update first, alone, then the data
// round-robin, one segment per RPC.
//
// Updates go first and alone because the have bitmap is what stops peers sending us what we
// now hold, and riding a data frame it arrives behind the very frames it should have
// suppressed: the first n=30 cell, with updates piggybacked, read sixteen copies per node.
// Alone, an update can take the urgent lane, as IDONTWANT does on the message path.
func runPass(in publishInput, peerStates map[peer.ID]PeerState, plans []peerPlan,
	yield func(peer.ID, partialmessages.PublishAction) bool) {
	for i := range plans {
		plan := &plans[i]
		if plan.err != nil {
			if !yield(plan.id, partialmessages.PublishAction{Err: plan.err}) {
				return
			}
			continue
		}
		if plan.meta == nil {
			continue
		}
		action, ok := metaAction(in, peerStates, plan)
		if !ok {
			continue
		}
		if in.bump != nil {
			in.bump(func(c *Counters) { c.PeerActions++ })
		}
		if !yield(plan.id, action) {
			return
		}
	}
	for round := 0; ; round++ {
		more := false
		for i := range plans {
			plan := &plans[i]
			if plan.err != nil || round >= len(plan.sendable) {
				continue
			}
			if len(plan.sendable) > round+1 {
				more = true
			}
			action, ok := dataAction(in, peerStates, plan, round)
			if !ok {
				continue
			}
			if in.bump != nil {
				in.bump(func(c *Counters) { c.PeerActions++ })
			}
			if !yield(plan.id, action) {
				return
			}
		}
		if !more {
			return
		}
	}
}

// metaAction is one peer's update, travelling alone.
func metaAction(in publishInput, peerStates map[peer.ID]PeerState, plan *peerPlan) (partialmessages.PublishAction, bool) {
	encoded, err := plan.meta.Marshal()
	if err != nil {
		return partialmessages.PublishAction{Err: err}, true
	}
	id, meta, now := plan.id, plan.meta, in.now
	return partialmessages.PublishAction{
		EncodedPartsMetadata: encoded,
		Urgent:               in.urgentUpdates,
		OnSent: func(admitted bool) {
			if !admitted {
				if in.bump != nil {
					in.bump(func(c *Counters) { c.SendsNotAdmitted++ })
				}
				return
			}
			// The stored state, not a plan-time clone: an earlier action of this pass may
			// have latched into it, and writing back a copy would erase that.
			next := peerStates[id].Clone()
			next.Sent = meta.Clone()
			next.SentAt = now
			peerStates[id] = next
			if in.bump != nil {
				in.bump(func(c *Counters) { c.MetadataSent++ })
			}
		},
	}, true
}

// dataAction is one segment to one peer, or nothing when the segment is gone.
func dataAction(in publishInput, peerStates map[peer.ID]PeerState, plan *peerPlan, round int) (partialmessages.PublishAction, bool) {
	index := plan.sendable[round]
	m, ok := in.segment(index)
	if !ok {
		// Raced with eviction. Drop the segment rather than fail the action: the peer still
		// wants it and the next pass offers it again.
		return partialmessages.PublishAction{}, false
	}
	encoded, err := encodeParts([]*segments.SegmentMessage{m}, in.compress)
	if err != nil {
		return partialmessages.PublishAction{Err: err}, true
	}
	id, now, count := plan.id, in.now, in.held.Count()
	return partialmessages.PublishAction{
		EncodedPartialMessage: encoded,
		// A data frame in the urgent lane would overtake queued data, which is the pacing
		// this stage still leaves to the queue.
		OnSent: func(admitted bool) {
			if !admitted {
				if in.bump != nil {
					in.bump(func(c *Counters) { c.SendsNotAdmitted++ })
				}
				return
			}
			next := peerStates[id].Clone()
			if next.Pushed == nil {
				b, err := segments.NewBitmap(count)
				if err != nil {
					return
				}
				next.Pushed = b
			}
			// A segment admitted to their queue is one we believe they will hold, so it
			// stops being sendable. Out of range only if the peer announced a different
			// width, in which case there is nothing correct to record.
			if err := next.Pushed.Set(index); err != nil {
				return
			}
			next.PushedAt = now
			peerStates[id] = next
			if in.bump != nil {
				in.bump(func(c *Counters) { c.SegmentsSent++ })
			}
		},
	}, true
}

// groupMetadata is what we say about the group: what we hold, and what we still want.
func groupMetadata(in publishInput) (*segments.PartsMetadata, error) {
	count := in.held.Count()
	meta, err := segments.NewPartsMetadata(count)
	if err != nil {
		return nil, err
	}
	// NewPartsMetadata already reports every segment as required, which is right for a
	// plain group. Narrowing it is the coded case, kept only so the wire matches variant B.
	if in.required > 0 && in.required < count {
		meta.Required = in.required
	}
	for _, idx := range in.held.Indices() {
		if err := meta.Available.Set(idx); err != nil {
			return nil, err
		}
	}
	if !in.complete {
		for _, idx := range in.held.Missing() {
			if err := meta.Requests.Set(idx); err != nil {
				return nil, err
			}
		}
	}
	return meta, nil
}

// encodeParts marshals a segment into one partial message, compressed or not.
func encodeParts(msgs []*segments.SegmentMessage, compress bool) ([]byte, error) {
	if compress {
		return segments.MarshalPartialMessageCompressed(msgs)
	}
	return segments.MarshalPartialMessage(msgs)
}
