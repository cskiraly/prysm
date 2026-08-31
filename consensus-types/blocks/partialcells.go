package blocks

import (
	"bytes"
	"hash/fnv"
	"iter"
	"slices"
	"strings"
	"time"

	"github.com/OffchainLabs/go-bitfield"
	ethpb "github.com/OffchainLabs/prysm/v7/proto/prysm/v1alpha1"
	"github.com/libp2p/go-libp2p-pubsub/partialmessages"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/pkg/errors"
)

// partialParts is the domain-specific half of the partial-message publish state machine. A
// partial data column and a RowDAS partial data row differ only in what a "part" is -- a
// column's parts are indexed by blob, a row's by column -- so the state machine below, which
// decides what to send a peer and how to track what it has, is shared.
//
// The peer-state and parts-metadata types are shared too. The gossipsub partial-messages
// extension is instantiated once per host with a single PeerState type, so rows and columns
// have no choice but to agree on it; the shape (available/requests bitmaps over parts) is
// domain-agnostic anyway, whatever the column-flavoured names suggest.
type partialParts interface {
	// partsCount is the number of parts in a complete message: the blob count for a
	// column, the column count for a row.
	partsCount() uint64
	// newPartsMetadata describes what we have and what we want. requests is the subset of the
	// parts we lack that this peer has been assigned; a nil bitlist means "everything we lack",
	// which is the behaviour before request de-confliction and what a caller with no peer
	// context gets.
	newPartsMetadata(requests bitfield.Bitlist) (*ethpb.PartialDataColumnPartsMetadata, error)
	// missingParts is what we lack and are willing to ask for, which is the input to the
	// assignment. It already accounts for any partsRequests override.
	missingParts() bitfield.Bitlist
	// claims is the per-part request ledger, which lives on the message so it outlives a single
	// publish. See requestclaims.go.
	claims() *requestClaims
	// cellsToSendToPeer encodes the cells this peer wants and does not have, returning the
	// bitmap of what was actually included. It returns (nil, nil, nil) when there is
	// nothing to send.
	cellsToSendToPeer(peerMeta *ethpb.PartialDataColumnPartsMetadata) ([]byte, bitfield.Bitlist, error)
	// headerMessage encodes a message carrying the block header and no cells. It returns
	// nil when this domain or fork has no header to exchange, in which case an eager push
	// carries parts metadata only.
	headerMessage() ([]byte, error)
	// defersAnnouncements says whether news that is not on the recipient's critical path -- a
	// request *removal*, and availability growth -- may wait for a packet going there anyway
	// rather than buying its own. True on the row axis; false on the column axis, which keeps the
	// shipped announce-everything-now behaviour until it gets the mechanism in a commit of its own
	// (notes/rowdas/plan-repair.md section 5.4).
	defersAnnouncements() bool
	// availabilityIsUrgent says the current availability must not be held: it is news other
	// nodes act on at once. A complete row is the case -- its announcement is the cancellation
	// signal EIP-8371's reconstruction phases stand down on (RowServedElsewhere), and holding it
	// for a ride let more reconstructors duplicate the recovery before they heard.
	availabilityIsUrgent() bool
}

// RequestAvailabilityMaxDelay is the per-peer announcement window for availability growth. A
// peer that has been told nothing for a whole window hears the first growth at once; growth
// inside the window waits to ride a packet going to that peer anyway, and is flushed when the
// window -- measured from the last thing the peer was told -- ends. So: at most one availability
// announcement per window per peer, never a late first one. Availability is an *offer*:
// delaying it delays other peers pulling from us, and in a subnet of many members someone else
// likely holds the same cell -- so it can wait longer than a request addition, which is on our
// own critical path and never waits. It is also monotone, so holding it is lossless: the
// announcement that eventually goes out subsumes every one it replaced.
//
// This replaces the earlier per-group publish throttle (a leading-edge window on *when a
// publish ran*), which held cell delivery back with the announcements: a peer's request arriving
// inside the window waited for the window's end. Holding per peer holds only the announcement.
//
// A var so R3 can sweep it. 200 ms is R3(b)'s choice (2026-08-31): the window/count trade is
// smooth with no knee, so the latency anchor decided -- at 64/4 on the wall clock, 100 -> 200 ms
// saves 36% of row RPCs with p50 no worse (724 -> 698 ms), while 400 ms's further gain is inside
// the noise and its staleness cost is unpriced (a full-membership cell cannot see a peer whose
// pull or phase-2 pool waits on our availability). Holding at all is worth 5.3x on RPCs and
// ~280 ms of p50 against no holding.
var RequestAvailabilityMaxDelay = 200 * time.Millisecond

func availabilityMaxDelay() time.Duration { return RequestAvailabilityMaxDelay }

// AvailabilityLeadingEdge selects which edge of the per-peer window carries the first
// announcement. Leading (true, the default and the author's specification): a peer told nothing
// for a whole window hears the first growth at once, later growth waits. Trailing (false): even
// the first growth waits out the window from when it was noted. Trailing exists as an R3(b) sweep
// arm -- the design argument against it is that its saving lands on first-contact latency -- and
// is not a production configuration.
var AvailabilityLeadingEdge = true

// AvailabilityFlushJitterFraction spreads each peer's availability window by a deterministic
// per-peer factor in [-f, +f]. Zero (the default) means every peer uses the window as configured.
// R15 found the announce policy clock-sensitive and hypothesized synchronized flush deadlines as
// the mechanism; this knob is that hypothesis made testable (R3(b)), and if it holds it becomes a
// protocol recommendation. Deterministic from the peer ID rather than drawn per flush, so the due
// check and the wake-up deadline can never disagree about the same peer.
var AvailabilityFlushJitterFraction float64

// availabilityDelayFor is the effective window for one peer: the configured window, spread by the
// peer's deterministic jitter. The hash is FNV-1a with a splitmix64 finalizer -- the raw low bits
// of FNV over similar inputs collapse under a modulus, and the top 53 bits after mixing are
// uniform enough for a jitter fraction.
func availabilityDelayFor(p peer.ID) time.Duration {
	d := availabilityMaxDelay()
	f := AvailabilityFlushJitterFraction
	if f <= 0 || d <= 0 {
		return d
	}
	h := fnv.New64a()
	_, _ = h.Write([]byte(p))
	x := h.Sum64()
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	u := float64(x>>11) / float64(1<<53) // uniform in [0, 1)

	return d + time.Duration((2*u-1)*f*float64(d))
}

// RequestCancellationMaxDelay bounds how long a request removal waits to piggyback on another
// packet to the same peer before a packet is spent on it. The effective delay is this divided by
// RequestN: at N=1 the peer being told is the one already sending, so the cancellation is worth
// little and can wait; at N>1 the other claim-holders are worth telling, because their
// transmission may not have been dispatched. The packet arithmetic is in plan-repair.md section
// 5.3 -- 42 B of metadata against a 2 KB cell means a piggybacked cancellation costs nothing and a
// dedicated one pays only if the peer probably has not dispatched.
//
// A var so R3 can sweep it; 500 ms is a starting point, not a measurement.
var RequestCancellationMaxDelay = 500 * time.Millisecond

func cancellationMaxDelay() time.Duration {
	return RequestCancellationMaxDelay / time.Duration(max(RequestN, 1))
}

// partialClock is the publish path's clock, a seam so tests can move it past a deadline instead
// of sleeping.
var partialClock = time.Now

// ActionReason labels *why* a publish action carried what it carried. It is a bitmask because one
// action can carry several kinds of news at once -- cells and metadata travel in the same message
// -- and single-labelling would mis-attribute the traffic.
//
// It exists because a metadata storm was diagnosed three times over from aggregate counters and got
// the mechanism wrong each time: the driving term could not be separated from the amplifying one
// without knowing which comparison in partialForPeer fired. See notes/rowdas/plan-repair.md.
type ActionReason uint8

const (
	// ReasonEagerPush is first contact with a peer whose state we do not know yet.
	ReasonEagerPush ActionReason = 1 << iota
	// ReasonCells means the action carries cell payload.
	ReasonCells
	// ReasonFirstMetadata is the first metadata we have ever sent this peer.
	ReasonFirstMetadata
	// ReasonAvailabilityAdd means we hold something we had not told this peer about. Monotone.
	ReasonAvailabilityAdd
	// ReasonRequestAdd means we want something from this peer we had not asked it for.
	ReasonRequestAdd
	// ReasonRequestChurn means our request set for this peer changed *without* gaining anything --
	// a part dropped, or a claim moved elsewhere. This is the term suspected of driving the storm,
	// and separating it from ReasonRequestAdd is the point of the whole label set.
	ReasonRequestChurn
)

// String renders the set for logs and metric labels, most significant news first.
func (r ActionReason) String() string {
	if r == 0 {
		return "none"
	}
	names := make([]string, 0, 6)
	for _, pair := range []struct {
		bit  ActionReason
		name string
	}{
		{ReasonEagerPush, "eager"},
		{ReasonCells, "cells"},
		{ReasonFirstMetadata, "first-meta"},
		{ReasonAvailabilityAdd, "avail-add"},
		{ReasonRequestAdd, "req-add"},
		{ReasonRequestChurn, "req-churn"},
	} {
		if r&pair.bit != 0 {
			names = append(names, pair.name)
		}
	}

	return strings.Join(names, "+")
}

// Has reports whether every bit in want is set.
func (r ActionReason) Has(want ActionReason) bool { return r&want == want }

func isEagerPush(requestedMessage bool, peerState PartialDataColumnPeerState) bool {
	return requestedMessage && peerState.Recvd == nil
}

// recordHeaderSent marks in headerSentCache that the header was sent to peerID. It only
// writes when a header was actually included, so a nil cache -- a domain with no header to
// exchange -- is never written.
func recordHeaderSent(peerID peer.ID, includeHeader bool, headerSentCache map[peer.ID]bool) {
	if !includeHeader || headerSentCache == nil {
		return
	}
	headerSentCache[peerID] = true
}

// partialPublishActions yields the publish action for each peer in peerStates. On a
// successful action it updates peerStates with the next state and records sent headers in
// headerSentCache.
func partialPublishActions(
	parts partialParts,
	peerStates map[peer.ID]PartialDataColumnPeerState,
	peerRequestsPartial func(peer.ID) bool,
	headerSentCache map[peer.ID]bool,
	onEagerPush func(peer.ID),
	onAction func(peer.ID, ActionReason),
) iter.Seq2[peer.ID, partialmessages.PublishAction] {
	return func(yield func(peer.ID, partialmessages.PublishAction) bool) {
		// Work out who to ask for what, once, before building any peer's action. Doing it per
		// peer is what produced the duplication: each peer independently saw the whole gap.
		now := partialClock()
		assignment := assignRequests(parts, peerStates, peerRequestsPartial, now)
		parts.claims().retainPending(peerStates)

		for peerID, peerState := range peerStates {
			requested := peerRequestsPartial(peerID)
			nextState, action, includeHeader, reason := partialForPeer(parts, peerID, requested, peerState, !headerSentCache[peerID], assignment[peerID], now)
			// Only update state if there was no error.
			if action.Err == nil {
				// Report only actions that will actually go on the wire. The extension drops
				// an action carrying neither cells nor metadata, so counting those would
				// inflate every attribution.
				if onAction != nil && reason != 0 {
					onAction(peerID, reason)
				}
				if onEagerPush != nil && isEagerPush(requested, peerState) {
					onEagerPush(peerID)
				}

				// Everything below is recorded *on confirmation*, not optimistically.
				//
				// This state is a memory of what we told a peer, and it is what suppresses saying
				// it again: partialForPeer compares against `Sent`, and the claim ledger parks a
				// part for a TTL. Gossipsub meanwhile drops on queue pressure and expects the
				// receiver to recover through the next heartbeat's IHAVE -- which works because
				// IHAVE is *regenerated*. Ours is remembered, so a silent drop meant we never
				// announced that part to that peer again, and the claim sat parked with nobody
				// actually asked. Deferring into OnSent records only what was admitted; a dropped
				// action leaves the state untouched, so the next publish rebuilds and re-sends it,
				// exactly as IHAVE would be.
				//
				// OnSent runs on the publishing goroutine before the next peer's action is built,
				// so mutating peerStates here is safe. It is not called when nothing goes on the
				// wire, which is what we want: there is then nothing to remember.
				peerID, nextState, assigned := peerID, nextState, assignment[peerID]
				includeHeader, hasMetadata := includeHeader, len(action.EncodedPartsMetadata) > 0
				// Asked without a packet: the assignment gave this peer parts its last-sent
				// request set already carries, so there is nothing to tell it -- but it *is*
				// being asked, and the claim must exist for the TTL, the backoff and the
				// exclusion to apply. Otherwise a silent peer re-selected this way would be
				// re-selected forever, with no lapse ever charged to it.
				if !hasMetadata && peerState.Sent != nil && assigned != nil {
					if alreadyAsked, err := assigned.And(peerState.Sent.Requests); err == nil && alreadyAsked.Count() > 0 {
						parts.claims().commit(peerID, alreadyAsked, now)
					}
				}
				action.OnSent = func(admitted bool) {
					if !admitted {
						return
					}
					recordHeaderSent(peerID, includeHeader, headerSentCache)
					peerStates[peerID] = nextState
					// Claim the assigned parts only now that this peer's request has actually gone
					// out. Claiming at assignment time would park a part for a whole TTL with
					// nobody asked -- the phantom-ledger-entry problem the nqg series calls out at
					// its own record site.
					if hasMetadata {
						parts.claims().commit(peerID, assigned, now)
						// Whatever was waiting for a ride has had one, and the next window
						// starts now.
						parts.claims().clearPending(peerID, now)
					}
				}
			}
			if !yield(peerID, action) {
				return
			}
		}
	}
}

// assignRequests partitions the parts this node lacks across the peers that hold them, so each
// peer is asked for a disjoint share rather than for the whole gap. A peer that does not request
// partial messages is not a candidate: it will never answer.
//
// The peer's own `Recvd.Available` is what says which parts it holds. That is a complete statement
// rather than a sampled announcement, which is why assignment is possible here and the gossipsub
// IWANT path could only rate-limit -- see requestclaims.go.
func assignRequests(
	parts partialParts,
	peerStates map[peer.ID]PartialDataColumnPeerState,
	peerRequestsPartial func(peer.ID) bool,
	now time.Time,
) map[peer.ID]bitfield.Bitlist {
	candidates := make([]peer.ID, 0, len(peerStates))
	for peerID := range peerStates {
		if peerRequestsPartial(peerID) {
			candidates = append(candidates, peerID)
		}
	}
	// Deterministic order, so a run is reproducible; the load balancing inside assign is what
	// actually spreads the requests.
	slices.SortFunc(candidates, func(a, b peer.ID) int { return strings.Compare(string(a), string(b)) })

	holds := func(p peer.ID) bitfield.Bitlist {
		recvd := peerStates[p].Recvd
		if recvd == nil {
			return nil
		}

		return recvd.Available
	}

	return parts.claims().assign(parts.missingParts(), candidates, holds, RequestN, now)
}

// partialForPeer returns the next peer state, the publish action for this peer, and whether
// the action carries the block header.
//
// assigned is the subset of the parts we lack that this peer is being asked for. A nil bitlist
// falls back to asking for everything, which is what a caller with no peer context gets.
//
// What earns a metadata packet, and what does not (plan-repair.md section 5.2):
//
//	first contact, first metadata     always
//	request addition                  always -- it unblocks our own pull; nobody else asks for us
//	availability grew                 not on its own. Held per peer; rides the next packet going
//	                                  there anyway, else flushed after RequestAvailabilityMaxDelay.
//	request removal only              not on its own. Same, with RequestCancellationMaxDelay / N.
//
// Cells never wait: a cells-only action goes at once and carries whatever news was waiting for
// that peer -- metadata generation is forced when something is held. The held cases are the
// author's piggybacking proposal, and they are safe to hold because neither can hurt the peer:
// stale availability means the peer asks a little later, and a stale request set means it can
// only over-serve.
func partialForPeer(
	parts partialParts,
	peerID peer.ID,
	requestedMessage bool,
	peerState PartialDataColumnPeerState,
	includeHeader bool,
	assigned bitfield.Bitlist,
	now time.Time,
) (PartialDataColumnPeerState, partialmessages.PublishAction, bool, ActionReason) {
	peerState = peerState.Clone()
	var reason ActionReason

	// Eager push - we don't know what the peer has and message has been requested.
	// Set RecvdState so subsequent calls skip the eager push path.
	if isEagerPush(requestedMessage, peerState) {
		var encoded []byte
		if includeHeader {
			var err error
			encoded, err = parts.headerMessage()
			if err != nil {
				return peerState, partialmessages.PublishAction{Err: err}, false, 0
			}
			// A domain or fork with no header to exchange never claims to have sent one.
			includeHeader = encoded != nil
		}
		myPartsMeta, err := parts.newPartsMetadata(assigned)
		if err != nil {
			return peerState, partialmessages.PublishAction{Err: err}, false, 0
		}
		peerState.Recvd = NewPartsMetaWithNoAvailableAndNoRequests(parts.partsCount())
		// We're sending our parts metadata so update the sent state i.e. the peer's view of what we have.
		peerState.Sent = myPartsMeta
		encodedMeta, err := marshalPartsMetadata(myPartsMeta)
		if err != nil {
			return peerState, partialmessages.PublishAction{Err: err}, false, 0
		}
		return peerState, partialmessages.PublishAction{
			EncodedPartialMessage: encoded,
			EncodedPartsMetadata:  encodedMeta,
		}, includeHeader, ReasonEagerPush
	}

	var cellsSent bitfield.Bitlist
	sentMeta := peerState.Sent
	recvdMeta := peerState.Recvd
	var encodedMsg []byte

	//  Normal - message requested and we have RecvdState.
	if requestedMessage && recvdMeta != nil {
		var err error
		encodedMsg, cellsSent, err = parts.cellsToSendToPeer(recvdMeta)
		if err != nil {
			return peerState, partialmessages.PublishAction{Err: err}, false, 0
		}
		if cellsSent != nil && cellsSent.Count() != 0 {
			newRecvd, err := MergeAvailableIntoPartsMetadata(recvdMeta, cellsSent)
			if err != nil {
				return peerState, partialmessages.PublishAction{Err: err}, false, 0
			}
			peerState.Recvd = newRecvd
			reason |= ReasonCells
		}
	}

	//  Check if we need to send partsMetadata.
	var partsMetadataToSend partialmessages.PartsMetadata
	myPartsMeta, err := parts.newPartsMetadata(assigned)
	if err != nil {
		return peerState, partialmessages.PublishAction{Err: err}, false, 0
	}
	var shouldSendPartsMetadata bool

	if sentMeta != nil {
		// Both fields are judged independently, then the packet decision is made once: a packet
		// goes if anything on the critical path needs it, and whatever is merely *held* for this
		// peer rides along. Holding applies only where the domain defers and the peer requests
		// partial messages -- the extension may drop actions to a peer that does not, so a
		// deadline noted for it could never be cleared.
		holds := parts.defersAnnouncements() && requestedMessage

		availContained, err := sentMeta.Available.Contains(myPartsMeta.Available)
		if err != nil {
			return peerState, partialmessages.PublishAction{Err: errors.Wrap(err, "check available parts metadata containment")}, false, 0
		}
		availGrew := !availContained
		availDue := availGrew && (!holds || parts.availabilityIsUrgent())
		if availGrew && holds && !availDue {
			// Leading edge, per peer: a peer told nothing for a whole window hears the first
			// growth at once; within the window the growth waits for a ride or the window's
			// end. The window runs from when the peer was last told anything, so this is at
			// most one availability announcement per window per peer -- and never a late first
			// one, which the trailing-edge version of this got wrong. The trailing arm is kept
			// behind AvailabilityLeadingEdge for R3(b): there the window runs from when the
			// growth was first noted, and the first growth waits too.
			lastTold, availSince := parts.claims().noteAvailPending(peerID, now)
			window := availabilityDelayFor(peerID)
			if AvailabilityLeadingEdge {
				availDue = lastTold.IsZero() || !now.Before(lastTold.Add(window))
			} else {
				availDue = !now.Before(availSince.Add(window))
			}
		}

		var critical, removal, removalDue bool
		if bytes.Equal(sentMeta.Requests, myPartsMeta.Requests) {
			if holds {
				// The peer's view matches ours again -- typically a claim that moved away and
				// came back before its removal was ever sent. Nothing to cancel any more, and
				// leaving the deferral would keep its deadline firing for nothing.
				parts.claims().clearCancelPending(peerID)
			}
		} else {
			// Which *kind* of request change: did we gain a want from this peer, or merely stop
			// wanting something? Containment failing means we now want something this peer has
			// not been asked for.
			onlyRemoval, containErr := sentMeta.Requests.Contains(myPartsMeta.Requests)
			switch {
			case containErr != nil:
				// Length mismatch should not happen for one group; send, and label it churn
				// rather than guessing, so the attribution never silently over-credits additions.
				critical = true
				reason |= ReasonRequestChurn
			case !onlyRemoval:
				critical = true
				reason |= ReasonRequestAdd
			default:
				removal = true
				removalDue = !holds
				if holds {
					since := parts.claims().noteCancelPending(peerID, now)
					removalDue = !now.Before(since.Add(cancellationMaxDelay()))
				}
			}
		}

		if critical || reason.Has(ReasonCells) || availDue || removalDue {
			shouldSendPartsMetadata = true
			if availGrew {
				reason |= ReasonAvailabilityAdd
			}
			if removal {
				reason |= ReasonRequestChurn
			}
		}
	}

	if sentMeta == nil || shouldSendPartsMetadata {
		var err error
		partsMetadataToSend, err = marshalPartsMetadata(myPartsMeta)
		if err != nil {
			return peerState, partialmessages.PublishAction{Err: err}, false, 0
		}
		if sentMeta == nil {
			reason |= ReasonFirstMetadata
		}
		if sentMeta == nil {
			peerState.Sent = myPartsMeta
		} else {
			sentMeta, err = MergeAvailableIntoPartsMetadata(sentMeta, myPartsMeta.Available)
			if err != nil {
				return peerState, partialmessages.PublishAction{Err: err}, false, 0
			}
			sentMeta.Requests = myPartsMeta.Requests
			peerState.Sent = sentMeta
		}
	}

	return peerState, partialmessages.PublishAction{
		EncodedPartialMessage: encodedMsg,
		EncodedPartsMetadata:  partsMetadataToSend,
	}, false, reason
}
