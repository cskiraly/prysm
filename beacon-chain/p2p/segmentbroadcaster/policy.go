package segmentbroadcaster

import (
	"encoding/binary"
	"hash/fnv"
	"iter"
	"sort"
	"time"

	"github.com/OffchainLabs/prysm/v7/container/segments"
	"github.com/libp2p/go-libp2p-pubsub/partialmessages"
	"github.com/libp2p/go-libp2p/core/peer"
)

// Policy decides, for one peer, which segments to push without being asked.
//
// This is the dimension variant B exists to explore. Everything else about the variant --
// one topic, per-link representation, a bitmap exchange -- is fixed; what changes between
// arms is how much a node volunteers before anyone requests anything. The three arms bracket
// the trade-off:
//
//	PushAll    every segment to every partial peer. Reproduces variant A's duplication over
//	           the new substrate, and is the control: if PushAll and variant A do not agree,
//	           the substrate itself is costing something and no other arm can be trusted.
//	PushNone   nothing but metadata. Pure announce-then-pull: the duplicate-free bound, at
//	           one extra round trip per hop. This is the floor on bytes and the ceiling on
//	           latency.
//	PushSplit  a disjoint slice of the segments to each peer, plus metadata for the rest.
//	           One copy leaves the node, and a peer's first RTT is spent receiving rather
//	           than asking. This is the arm the design predicts wins.
type Policy uint8

const (
	// PushSplit assigns each segment to one peer. The default: it is the only arm that
	// neither duplicates nor pays a round trip before anything moves.
	PushSplit Policy = iota
	// PushAll pushes every held segment to every peer that lacks it.
	PushAll
	// PushNone pushes nothing; peers must request what they want.
	PushNone
	// PushPhase pushes eagerly early and falls back to announce late, using the number of
	// peers known to hold a segment as the local estimate of how far it has diffused. This
	// is PPPT's push-pull phase transition with the bitmap state standing in for a hop
	// counter: a segment is volunteered to its top
	// max(0, replication - knownHolders) rendezvous peers, so the push degree decays to
	// zero exactly where duplicates concentrate -- late in the diffusion.
	PushPhase
)

// policyNames is the mapping used by String and by the harness when labelling arms.
var policyNames = map[Policy]string{
	PushSplit: "split",
	PushAll:   "all",
	PushNone:  "none",
	PushPhase: "phase",
}

// String returns the arm's short name.
func (p Policy) String() string {
	if s, ok := policyNames[p]; ok {
		return s
	}
	return "unknown"
}

// publishInput is everything the publish decision needs that does not come from the
// extension. Gathered on the broadcaster's loop and passed in, so the decision itself is a
// pure function of its inputs and can be tested without a network.
type publishInput struct {
	// Fields are ordered widest-first: nogo's maligned check rejects the natural grouping,
	// which padded the two single-byte fields into their own words.

	// groupID seeds the assignment hashes, so two groups in flight at once do not give every
	// peer the same share of both.
	groupID []byte
	// self is our own peer id. Needed because a coordinated push decision has to depend on the
	// *pair*: a rule that ranks only receivers makes every sender pick the same one.
	self peer.ID
	// held is the set of segments we can serve. Never nil.
	held *segments.Bitmap
	// wanted is the set of segments we are missing and would like sent to us. Nil once the
	// group is complete.
	wanted *segments.Bitmap
	// segment returns the wire message for one held index.
	segment func(index uint32) (*segments.SegmentMessage, bool)
	// requestFrom decides, for a segment we are missing, whether to ask this particular
	// peer for it. Asking every peer that advertises a segment would reintroduce exactly
	// the duplication the variant removes. perPeer, when non-nil, is this peer's remaining
	// fresh-claim allowance for the pass; requestFrom decrements it.
	requestFrom func(index uint32, p peer.ID, perPeer *int) bool
	// onSend reports what went to a peer: segments volunteered, segments the peer had asked
	// for, and whether metadata went with them. Called for every peer in the pass, including
	// peers that got nothing, so a pass over an empty peer set is distinguishable from a pass
	// with nothing to say. Optional.
	onSend func(p peer.ID, pushed, served int, metadata bool)
	// onWithheld reports how many want-driven sends F1 withholding skipped, so injected
	// exposure is verified, not assumed. Paired with withholdServes below.
	onWithheld func(int)
	// replication is how many peers each segment is volunteered to under PushSplit. Zero is
	// read as one, so a zero-value publishInput still pushes.
	replication int
	// claimCap bounds fresh claims per peer per pass. See Config.ClaimPerPeer. Zero is
	// unlimited.
	claimCap int
	// pushChunk is Config.PushChunk: segments per push RPC, round-robin across peers; 0 = one bundle per peer.
	pushChunk int
	// now is the pass's clock reading, for the announce batching window.
	now time.Time
	// announceWindow batches announce-only metadata per peer. See Config.AnnounceWindow.
	// Zero sends every change immediately.
	announceWindow time.Duration
	// deferAnnounce is called when an announce-only update was held back, so the group is
	// re-passed once the window closes. A held-back announce with no wake-up would otherwise
	// be lost until unrelated traffic re-runs the pass.
	deferAnnounce func()
	// pushDivisor turns on the coordinated push predicate, and sets its density: a sender
	// pushes a segment to a peer with probability 1/pushDivisor, so a receiver with in-degree
	// P expects P/pushDivisor copies. Zero keeps the uncoordinated rendezvous rule.
	pushDivisor int
	// required is how many segments reassemble the group: the bitmap width for a plain
	// group, fewer for a coded one. Zero is read as the bitmap width. Carried into outgoing
	// metadata so announce-only receivers can budget their requests.
	required uint32
	// policy is the arm in force.
	policy Policy
	// pushGrace is true while the group is young enough that pushes may still be in flight. It
	// gates the request suppression, so a push that never arrives cannot strand a segment.
	pushGrace bool
	// withholdServes is the F1 injection: skip want-driven sends, keep pushes and announces.
	withholdServes bool
	// compress snappy-compresses each segment in the part. See Config.CompressSegments.
	compress bool
}

// publishActions is the extension's PublishActionsFn: for each peer in the group, what to
// send and what the peer's next state becomes.
//
// Two things are always sent when they are not empty, and they are independent: the segments
// this peer should receive, and the metadata describing what we hold and want. A peer that
// has not negotiated partial messages gets metadata only -- gossipsub delivers it the whole
// envelope on the same topic, which is what makes the representation a per-link choice
// rather than a network-wide one.
func publishActions(in publishInput) partialmessages.PublishActionsFn[PeerState] {
	return func(peerStates map[peer.ID]PeerState, peerRequestsPartial func(peer.ID) bool) iter.Seq2[peer.ID, partialmessages.PublishAction] {
		return func(yield func(peer.ID, partialmessages.PublishAction) bool) {
			// Assignment under PushSplit has to be stable across calls and identical for
			// every segment, so it is computed once against a sorted peer list rather than
			// from map iteration order.
			partialPeers := sortedPartialPeers(peerStates, peerRequestsPartial)
			// Which segments *someone* is about to push us. Computed once over all peers,
			// because the question is per segment and not per peer -- see incomingPushes.
			incoming := incomingPushes(in, peerStates)
			// The order requests are claimed in, computed once per pass.
			wants := orderedWants(in)
			// How many peers are known to hold each held segment: PushPhase's diffusion
			// estimate. Computed once per pass, since it ranges over all peers.
			holders := knownHolders(in, peerStates)

			if in.pushChunk > 0 {
				chunkedActions(in, peerStates, peerRequestsPartial, partialPeers, incoming, wants, holders, yield)
				return
			}
			for id, state := range peerStates {
				next, action := actionForPeer(in, id, state, peerRequestsPartial(id), partialPeers, incoming, wants, holders)
				if action.Err == nil {
					peerStates[id] = next
				}
				if !yield(id, action) {
					return
				}
			}
		}
	}
}

// sortedPartialPeers returns the peers that speak partial messages, in a stable order.
func sortedPartialPeers(peerStates map[peer.ID]PeerState, peerRequestsPartial func(peer.ID) bool) []peer.ID {
	out := make([]peer.ID, 0, len(peerStates))
	for id := range peerStates {
		if peerRequestsPartial(id) {
			out = append(out, id)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// actionForPeer builds one peer's publish action and its next state.
func actionForPeer(
	in publishInput,
	id peer.ID,
	state PeerState,
	requestsPartial bool,
	partialPeers []peer.ID,
	incoming map[uint32]bool,
	wants []uint32,
	holders map[uint32]int,
) (PeerState, partialmessages.PublishAction) {
	next, action, _ := actionForPeerMsgs(in, id, state, requestsPartial, partialPeers, incoming, wants, holders, false)
	return next, action
}

// actionForPeerMsgs is actionForPeer with the encoding of the pushed segments optionally left
// to the caller: with deferEncode the action carries no partial message and the segments come
// back unencoded, so chunkedActions can send them in small RPCs round-robin across peers
// (PushChunk) instead of one bundle per peer. The bookkeeping is the same either way.
func actionForPeerMsgs(
	in publishInput,
	id peer.ID,
	state PeerState,
	requestsPartial bool,
	partialPeers []peer.ID,
	incoming map[uint32]bool,
	wants []uint32,
	holders map[uint32]int,
	deferEncode bool,
) (PeerState, partialmessages.PublishAction, []*segments.SegmentMessage) {
	next := state.Clone()
	count := in.held.Count()
	if next.Pushed == nil {
		b, err := segments.NewBitmap(count)
		if err != nil {
			return state, partialmessages.PublishAction{Err: err}, nil
		}
		next.Pushed = b
	}

	var encodedMsg []byte
	var pushed, served int
	var msgs []*segments.SegmentMessage
	if requestsPartial {
		indices := segmentsForPeer(in, id, next, partialPeers, holders)
		msgs = make([]*segments.SegmentMessage, 0, len(indices))
		withheld := 0
		for _, idx := range indices {
			m, ok := in.segment(idx)
			if !ok {
				// Raced with eviction. Skip it rather than fail the whole action: the peer
				// will ask again from its metadata.
				continue
			}
			if in.withholdServes && next.wants(idx) {
				withheld++
				continue
			}
			if next.wants(idx) {
				served++
			} else {
				pushed++
			}
			msgs = append(msgs, m)
		}
		if withheld > 0 && in.onWithheld != nil {
			in.onWithheld(withheld)
		}
		if len(msgs) > 0 {
			if !deferEncode {
				enc, err := encodeParts(msgs, in.compress)
				if err != nil {
					return state, partialmessages.PublishAction{Err: err}, nil
				}
				encodedMsg = enc
			}
			for _, m := range msgs {
				if err := next.Pushed.Set(m.Index); err != nil {
					return state, partialmessages.PublishAction{Err: err}, nil
				}
				// Served, so stop counting it as outstanding.
				if next.Recvd != nil {
					if err := next.Recvd.Requests.Clear(m.Index); err != nil {
						return state, partialmessages.PublishAction{Err: err}, nil
					}
				}
				// A segment we just sent is a segment they hold, so do not send it again
				// on the next round for want of an acknowledgement.
				if next.Recvd != nil {
					if err := next.Recvd.Available.Set(m.Index); err != nil {
						return state, partialmessages.PublishAction{Err: err}, nil
					}
				}
			}
		}
	}

	meta, err := metadataForPeer(in, id, next, count, incoming, wants)
	if err != nil {
		return state, partialmessages.PublishAction{Err: err}, nil
	}
	var encodedMeta []byte
	if !meta.Equal(next.Sent) {
		// Announce batching: an update that carries no segments and changes no requests is
		// pure announce, and may wait out the per-peer window so a burst of arrivals folds
		// into one send. Requests must never wait -- the claim ledger has already been
		// charged for them, so a delayed request is a guaranteed lapse. The first contact
		// (leading edge) always goes immediately.
		holdBack := in.announceWindow > 0 && len(encodedMsg) == 0 &&
			!next.MetaSentAt.IsZero() && in.now.Sub(next.MetaSentAt) < in.announceWindow &&
			bitmapEqual(meta.Requests, next.Sent.Requests)
		if holdBack {
			if in.deferAnnounce != nil {
				in.deferAnnounce()
			}
		} else {
			encodedMeta, err = meta.Marshal()
			if err != nil {
				return state, partialmessages.PublishAction{Err: err}, nil
			}
			next.Sent = meta
			next.MetaSentAt = in.now
		}
	}

	if in.onSend != nil {
		in.onSend(id, pushed, served, len(encodedMeta) > 0)
	}
	return next, partialmessages.PublishAction{
		EncodedPartialMessage: encodedMsg,
		EncodedPartsMetadata:  encodedMeta,
	}, msgs
}

// encodeParts marshals pushed segments into one partial message, compressed or not.
func encodeParts(msgs []*segments.SegmentMessage, compress bool) ([]byte, error) {
	if compress {
		return segments.MarshalPartialMessageCompressed(msgs)
	}
	return segments.MarshalPartialMessage(msgs)
}

// chunkedActions is publishActions' PushChunk mode. Every peer's action is built first, then
// round 0 yields each peer's metadata with its first chunk of segments and later rounds one
// chunk per peer, round-robin in a stable peer order, so the first segment of every peer
// leaves before the second of any -- A's batch-publishing order on B's substrate.
func chunkedActions(in publishInput, peerStates map[peer.ID]PeerState, peerRequestsPartial func(peer.ID) bool,
	partialPeers []peer.ID, incoming map[uint32]bool, wants []uint32, holders map[uint32]int,
	yield func(peer.ID, partialmessages.PublishAction) bool) {
	ids := make([]peer.ID, 0, len(peerStates))
	for id := range peerStates {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	type pending struct {
		id     peer.ID
		action partialmessages.PublishAction
		msgs   []*segments.SegmentMessage
	}
	order := make([]pending, 0, len(ids))
	for _, id := range ids {
		next, action, msgs := actionForPeerMsgs(in, id, peerStates[id], peerRequestsPartial(id), partialPeers, incoming, wants, holders, true)
		if action.Err == nil {
			peerStates[id] = next
		}
		order = append(order, pending{id: id, action: action, msgs: msgs})
	}
	// A chunk of 1000 or more is the metadata-first screen: round 0 carries each peer's
	// metadata alone and round 1 the whole bundle, so early claims can be told apart from
	// small frames (both arrive together under PushChunk = 1).
	chunk, firstChunkRound := in.pushChunk, 0
	if chunk >= 1000 {
		chunk, firstChunkRound = 1<<30, 1
	}
	for round := 0; ; round++ {
		more := false
		for _, p := range order {
			act := partialmessages.PublishAction{}
			if round == 0 {
				act.EncodedPartsMetadata = p.action.EncodedPartsMetadata
				act.Err = p.action.Err
			}
			lo := -1
			if round >= firstChunkRound {
				lo = (round - firstChunkRound) * chunk
			}
			if act.Err == nil && lo >= 0 && lo < len(p.msgs) {
				hi := min(lo+chunk, len(p.msgs))
				enc, err := encodeParts(p.msgs[lo:hi], in.compress)
				if err != nil {
					act.Err = err
				} else {
					act.EncodedPartialMessage = enc
					more = more || hi < len(p.msgs)
				}
			}
			if round == 0 || len(act.EncodedPartialMessage) > 0 || act.Err != nil {
				if !yield(p.id, act) {
					return
				}
			}
		}
		if !more && round >= firstChunkRound {
			return
		}
	}
}

// segmentsForPeer is the push decision: which held segments this peer gets unasked.
//
// Explicit requests are always honoured regardless of policy -- a request means the peer
// has already decided it needs the segment and has waited a round trip for it, so refusing
// would strand it. The policy only governs what goes out on top of that.
func segmentsForPeer(in publishInput, id peer.ID, state PeerState, partialPeers []peer.ID, holders map[uint32]int) []uint32 {
	var out []uint32
	for _, idx := range in.held.Indices() {
		if state.pushed(idx) {
			continue
		}
		if state.wants(idx) {
			out = append(out, idx)
			continue
		}
		if state.holds(idx) {
			continue
		}
		switch in.policy {
		case PushAll:
			out = append(out, idx)
		case PushSplit:
			if in.pushDivisor > 0 {
				// Coordinated: a pairwise predicate, so the receiver can run it too.
				if willPush(in.groupID, idx, in.self, id, in.pushDivisor) {
					out = append(out, idx)
				}
			} else if assignedTo(in.groupID, idx, id, partialPeers, in.replication) {
				out = append(out, idx)
			}
		case PushNone:
			// Requests only.
		case PushPhase:
			// PPPT: push degree max(0, replication - knownHolders), so a segment nobody is
			// known to hold goes out replication-wide and a well-diffused one goes out not
			// at all. The peers already counted as holders are skipped above anyway, so the
			// decayed degree ranks only peers that still lack the segment.
			r := in.replication
			if r <= 0 {
				r = 1
			}
			r -= holders[idx]
			if r > 0 && assignedTo(in.groupID, idx, id, partialPeers, r) {
				out = append(out, idx)
			}
		}
	}
	return out
}

// knownHolders counts, per held segment, the peers whose metadata says they hold it.
//
// This is PushPhase's diffusion estimate, and it is deliberately local and unauthenticated:
// a bitmap is a claim, but the only thing a false claim changes is who we volunteer a copy
// to, and any peer can still request anything. Nil for every other policy, so the map costs
// nothing where it decides nothing.
func knownHolders(in publishInput, peerStates map[peer.ID]PeerState) map[uint32]int {
	if in.policy != PushPhase {
		return nil
	}
	out := make(map[uint32]int)
	for _, idx := range in.held.Indices() {
		for _, state := range peerStates {
			if state.holds(idx) {
				out[idx]++
			}
		}
	}
	return out
}

// assignedTo reports whether this peer is one of the owners of segment idx.
//
// Rendezvous hashing (highest random weight): score every candidate peer against
// (groupID, idx), and this peer owns the segment if it is in the top `replication`. Two
// properties matter, and the modulo this replaces had neither.
//
// **Stable as peers appear.** The previous rule was `idx % len(peers)` over a sorted list, so
// every arrival shifted every assignment: a segment already pushed to one peer was reassigned
// to another and pushed again. Since `Pushed` is per peer, nothing suppressed the second copy.
// That was measured to be the *whole* of PushSplit's duplication -- at a 3 s request timeout it
// reissued no requests at all and still received 2.37 copies per node. Under rendezvous, adding
// a peer only moves the segments the newcomer outscores; everything else stays put.
//
// **Independent of list order.** Only membership feeds the score, so map-iteration order cannot
// reach the result. The sort in sortedPartialPeers is now belt and braces.
//
// `replication` is the r of the design space's dimension 9: r=1 sends one copy and leaves the
// rest to be pulled, r >= len(peers) is PushAll by another name, and the interesting values are
// in between. Cost is O(peers) hashes per (segment, peer), which at degree 20 and K=32 is a few
// thousand FNV rounds per publish pass -- immaterial beside a single segment's transmission.
func assignedTo(groupID []byte, idx uint32, id peer.ID, partialPeers []peer.ID, replication int) bool {
	if len(partialPeers) == 0 {
		return false
	}
	// An unset replication means one copy, not none. Zero would otherwise mean "volunteer to
	// nobody", which is PushNone by a different route -- and a policy that silently stops
	// pushing because a field was left at its zero value is the kind of failure that reads as
	// the design not working.
	if replication <= 0 {
		replication = 1
	}
	if replication >= len(partialPeers) {
		return true
	}
	mine := rendezvousScore(groupID, idx, id)
	better := 0
	for _, p := range partialPeers {
		if p == id {
			continue
		}
		score := rendezvousScore(groupID, idx, p)
		// Ties broken by peer id, so a collision cannot make two peers each believe the other
		// ranks above it -- which would let a segment go to more peers than replication allows.
		if score > mine || (score == mine && p > id) {
			better++
			if better >= replication {
				return false
			}
		}
	}
	return true
}

// willPush reports whether `from` volunteers segment idx to `to`, unasked.
//
// A pairwise predicate over (group, segment, sender, receiver) rather than a ranking over the
// sender's peer set, and the difference is the point: **the receiver can evaluate it too**. A
// node that knows a peer holds a segment can work out whether that peer is about to send it,
// and skip asking. That is what closes the push/pull race.
//
// Why the race mattered more than the collisions it replaced. Measured at n=500 r=1: pushes
// totalled **0.658 copies per (node, segment)** -- under one copy, so pushes could not have
// been duplicating among themselves -- while pulls ran at 0.955. Only **20% of pushes displaced
// a pull**; the other 80% landed on a node that had already requested the segment, because it
// asked the moment it heard of the group and the push had not arrived yet. The duplication was
// push-against-pull, not sender-against-sender.
//
// It also fixes something the rendezvous rule got wrong. That score was a function of the
// receiver alone, so every sender ranked its peers identically -- a receiver that scored high
// for a segment was chosen by *all* of its neighbours at once. Collisions were positively
// correlated rather than independent. Including the sender decorrelates them.
//
// `divisor` sets the density: each sender pushes with probability 1/divisor, so a receiver with
// in-degree P expects P/divisor copies and divisor = P gives one. Both ends must use the same
// value or the receiver's prediction is wrong, so it is a constant rather than something derived
// from a peer count that the two ends see differently.
func willPush(groupID []byte, idx uint32, from, to peer.ID, divisor int) bool {
	if divisor <= 0 {
		return false
	}
	h := fnv.New64a()
	_, _ = h.Write(groupID)
	var b [4]byte
	binary.LittleEndian.PutUint32(b[:], idx)
	_, _ = h.Write(b[:])
	_, _ = h.Write([]byte(from))
	// A separator, so (from="ab", to="c") and (from="a", to="bc") do not collide.
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(to))
	return mix64(h.Sum64())%uint64(divisor) == 0
}

// incomingPushes is the set of segments some peer is predicted to volunteer to us.
//
// Per segment, deliberately. The first version asked this per peer -- "is *this* peer going to
// push me segment i, and if so skip it" -- which let the request fall through to the next peer in
// the pass, collecting the predicted push *and* a reply from someone else. Measured at n=500 that
// doubled completion, 1.768 s to 3.485 s, and left duplication unchanged at 1.62 copies per node.
// Coordination that is not global over the segment is worse than not coordinating at all.
//
// Nil when coordination is off or the grace window has passed, which makes both a no-op at the
// call site rather than a branch there.
func incomingPushes(in publishInput, peerStates map[peer.ID]PeerState) map[uint32]bool {
	if in.pushDivisor <= 0 || !in.pushGrace || in.wanted == nil {
		return nil
	}
	out := make(map[uint32]bool)
	for _, idx := range in.wanted.Indices() {
		for p, state := range peerStates {
			// Only a peer that says it holds the segment can be about to send it.
			if !state.holds(idx) {
				continue
			}
			if willPush(in.groupID, idx, p, in.self, in.pushDivisor) {
				out[idx] = true
				break
			}
		}
	}
	return out
}

// orderedWants is the order in which missing segments are offered to requestFrom.
//
// Ascending order would do for a plain group, where the budget covers every missing index
// anyway. For a coded group it would not: the budget stops the pass partway through the
// list, so every node asking in ascending order concentrates the network's requests on the
// lowest indices and leaves the rest of the codeword uncirculated. Ranking by a hash of
// (group, index, self) gives each node its own preference order, which is exactly the
// diversity the code exists to exploit.
//
// A claim cap makes the hash order mandatory for plain groups too, and for the same reason:
// the cap stops the pass partway through the list, so a shared ascending order would point
// the whole network's first claims at the lowest indices.
func orderedWants(in publishInput) []uint32 {
	idxs := in.wanted.Indices()
	if len(idxs) <= 1 {
		return idxs
	}
	if in.claimCap <= 0 && (in.required == 0 || in.required >= in.wanted.Count()) {
		return idxs
	}
	sort.Slice(idxs, func(a, b int) bool {
		return rendezvousScore(in.groupID, idxs[a], in.self) < rendezvousScore(in.groupID, idxs[b], in.self)
	})
	return idxs
}

// bitmapEqual reports whether two bitmaps carry the same set.
//
// Same cardinality plus an empty one-way difference is equality; the segments package has
// no Equal on Bitmap and this stays O(count) without touching its internals.
func bitmapEqual(a, b *segments.Bitmap) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	if a.Count() != b.Count() || a.Len() != b.Len() {
		return false
	}
	return len(a.AndNot(b)) == 0
}

// mix64 avalanches a hash before it is reduced modulo a small number.
//
// Necessary, not decorative. FNV-1a's low bits carry almost no mixing: modulo 4 its recurrence
// collapses to `h <- -(h ^ byte)` over two bits, a state machine on the low bits of the input
// alone. Two byte strings that are permutations of each other -- which "alice\0bob" and
// "bob\0alice" are -- then land on the same residue. Every link's two directions agreed on
// every segment, so pushes were either mirrored or absent per link rather than independent.
// Caught by TestWillPushIsSymmetricallyComputable's direction subtest.
//
// This is the splitmix64 finalizer.
func mix64(x uint64) uint64 {
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return x
}

// rendezvousScore is the weight of one (group, segment, peer) triple.
//
// FNV-1a rather than a cryptographic hash: nothing here is adversary-facing. A peer cannot
// profit from predicting the assignment, because the only thing it decides is which of our
// peers we volunteer a segment to, and every peer can ask for any segment regardless.
func rendezvousScore(groupID []byte, idx uint32, p peer.ID) uint64 {
	h := fnv.New64a()
	_, _ = h.Write(groupID)
	var b [4]byte
	binary.LittleEndian.PutUint32(b[:], idx)
	_, _ = h.Write(b[:])
	_, _ = h.Write([]byte(p))
	return mix64(h.Sum64())
}

// metadataForPeer builds the metadata to send this peer: what we hold, and what we want
// from this peer specifically.
//
// The requests bitmap is per peer on purpose. Broadcasting the same want to every peer is
// what makes announce-then-pull duplicate as badly as eager push; asking one peer per
// segment is what makes it not.
func metadataForPeer(in publishInput, id peer.ID, state PeerState, count uint32, incoming map[uint32]bool, wants []uint32) (*segments.PartsMetadata, error) {
	meta, err := segments.NewPartsMetadata(count)
	if err != nil {
		return nil, err
	}
	if in.required > 0 && in.required < count {
		meta.Required = in.required
	}
	for _, idx := range in.held.Indices() {
		if err := meta.Available.Set(idx); err != nil {
			return nil, err
		}
	}
	if in.wanted != nil && in.requestFrom != nil {
		var perPeer *int
		if in.claimCap > 0 {
			allowance := in.claimCap
			perPeer = &allowance
		}
		for _, idx := range wants {
			// Only ask a peer that has said it holds the segment, and only if this is the
			// peer chosen for it.
			if !state.holds(idx) {
				continue
			}
			// Do not ask *anyone* for what is already on its way. Asking buys a second copy and
			// nothing else, and that overlap was 80% of this variant's duplication.
			//
			// Keyed on the segment, not on this peer -- see incomingPushes for why that
			// distinction cost a factor of two.
			//
			// Bounded by pushGrace on purpose. The predicate is deterministic, so without a
			// deadline a push that is lost or never sent would be waited on forever.
			if incoming[idx] {
				continue
			}
			if !in.requestFrom(idx, id, perPeer) {
				continue
			}
			if err := meta.Requests.Set(idx); err != nil {
				return nil, err
			}
		}
	}
	return meta, nil
}
