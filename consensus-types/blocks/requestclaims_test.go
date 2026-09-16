package blocks

import (
	"testing"
	"time"

	"github.com/OffchainLabs/go-bitfield"
	"github.com/OffchainLabs/prysm/v7/testing/require"
	"github.com/libp2p/go-libp2p/core/peer"
)

// holdersOf builds a `holds` function from a map of peer to the parts that peer has.
func holdersOf(length uint64, byPeer map[peer.ID][]uint64) func(peer.ID) bitfield.Bitlist {
	bitmaps := make(map[peer.ID]bitfield.Bitlist, len(byPeer))
	for p, parts := range byPeer {
		bl := bitfield.NewBitlist(length)
		for _, part := range parts {
			bl.SetBitAt(part, true)
		}
		bitmaps[p] = bl
	}

	return func(p peer.ID) bitfield.Bitlist { return bitmaps[p] }
}

func missingAll(length uint64) bitfield.Bitlist {
	return bitfield.NewBitlist(length).Not()
}

// commitAll is what partialPublishActions does once each peer's request has actually gone out.
// assign only plans; nothing enters the ledger until commit.
func commitAll(claims *requestClaims, assignment map[peer.ID]bitfield.Bitlist, now time.Time) {
	for p, asked := range assignment {
		claims.commit(p, asked, now)
	}
}

// TestAssignAsksOnePeerPerPart is the property the whole file exists for: before this, a node's
// request bitmap was the whole gap and went to every peer, so every peer served it.
// TestRequestClaimsCommitDoesNotExtendLiveClaims pins that a claim ages from the first ask.
//
// The metadata we send carries the whole current request bitmap, not a delta, so a message sent for
// an unrelated reason -- our availability grew -- restates every outstanding request. If that
// restatement refreshed the deadline, a peer that never answers would hold its claims for as long as
// we kept talking to it and the part would never be offered elsewhere. An adversary would need to do
// nothing but stay silent.
func TestRequestClaimsCommitDoesNotExtendLiveClaims(t *testing.T) {
	claims := newRequestClaims()
	const peerA = peer.ID("a")
	asked := bitfield.NewBitlist(8)
	asked.SetBitAt(3, true)

	start := time.Unix(1700000000, 0)
	claims.commit(peerA, asked, start)
	first, ok := claims.earliestDeadline()
	require.Equal(t, true, ok)

	// A later commit for the same part, as an unrelated announcement would produce.
	claims.commit(peerA, asked, start.Add(200*time.Millisecond))
	again, ok := claims.earliestDeadline()
	require.Equal(t, true, ok)
	require.Equal(t, true, again.Equal(first), "a live claim's deadline must not be extended")

	// Once it has lapsed, committing again is a fresh ask and does set a new deadline.
	claims.commit(peerA, asked, first.Add(time.Millisecond))
	renewed, ok := claims.earliestDeadline()
	require.Equal(t, true, ok)
	require.Equal(t, true, renewed.After(first), "a lapsed claim re-asked gets a new deadline")
}

func TestAssignAsksOnePeerPerPart(t *testing.T) {
	const length = 8
	a, b, c := peer.ID("a"), peer.ID("b"), peer.ID("c")
	holds := holdersOf(length, map[peer.ID][]uint64{
		a: {0, 1, 2, 3, 4, 5, 6, 7},
		b: {0, 1, 2, 3, 4, 5, 6, 7},
		c: {0, 1, 2, 3, 4, 5, 6, 7},
	})

	claims := newRequestClaims()
	assignment := claims.assign(missingAll(length), []peer.ID{a, b, c}, holds, 1, time.Now())

	for part := range uint64(length) {
		askedFrom := 0
		for _, p := range []peer.ID{a, b, c} {
			if assignment[p].BitAt(part) {
				askedFrom++
			}
		}
		require.Equal(t, 1, askedFrom, "each part should be asked of exactly one peer")
	}

	// And the load should be spread rather than dumped on whoever sorts first.
	for _, p := range []peer.ID{a, b, c} {
		count := assignment[p].Count()
		require.Equal(t, true, count > 0 && count <= 4,
			"expected a spread share, got %d of 8 for %s")
	}
}

// TestAssignFanoutTwo covers the hedge: the parameter is there so the bytes-versus-tail trade can
// be swept rather than argued.
func TestAssignFanoutTwo(t *testing.T) {
	const length = 4
	a, b, c := peer.ID("a"), peer.ID("b"), peer.ID("c")
	holds := holdersOf(length, map[peer.ID][]uint64{a: {0, 1, 2, 3}, b: {0, 1, 2, 3}, c: {0, 1, 2, 3}})

	claims := newRequestClaims()
	assignment := claims.assign(missingAll(length), []peer.ID{a, b, c}, holds, 2, time.Now())

	for part := range uint64(length) {
		askedFrom := 0
		for _, p := range []peer.ID{a, b, c} {
			if assignment[p].BitAt(part) {
				askedFrom++
			}
		}
		require.Equal(t, 2, askedFrom, "fanout 2 should ask two peers per part")
	}
}

// TestAssignSkipsPartsNobodyHas: asking for a cell no peer advertises is pure cost, and it is what
// the undifferentiated bitmap did on every publish.
func TestAssignSkipsPartsNobodyHas(t *testing.T) {
	const length = 4
	a := peer.ID("a")
	holds := holdersOf(length, map[peer.ID][]uint64{a: {0, 2}})

	claims := newRequestClaims()
	assignment := claims.assign(missingAll(length), []peer.ID{a}, holds, 1, time.Now())

	require.Equal(t, true, assignment[a].BitAt(0))
	require.Equal(t, false, assignment[a].BitAt(1), "peer a does not hold part 1")
	require.Equal(t, true, assignment[a].BitAt(2))
	require.Equal(t, false, assignment[a].BitAt(3), "peer a does not hold part 3")
}

// TestAssignKeepsLiveClaims is the de-confliction across publishes. A metadata update goes out on
// every incoming RPC, so without this the second publish would re-ask everyone and the fix would
// buy nothing.
func TestAssignKeepsLiveClaims(t *testing.T) {
	const length = 4
	a, b := peer.ID("a"), peer.ID("b")
	holds := holdersOf(length, map[peer.ID][]uint64{a: {0, 1, 2, 3}, b: {0, 1, 2, 3}})

	claims := newRequestClaims()
	now := time.Now()
	first := claims.assign(missingAll(length), []peer.ID{a, b}, holds, 1, now)
	commitAll(claims, first, now)
	second := claims.assign(missingAll(length), []peer.ID{a, b}, holds, 1, now.Add(time.Millisecond))

	for part := range uint64(length) {
		require.Equal(t, first[a].BitAt(part), second[a].BitAt(part),
			"a live claim should not move between publishes")
		require.Equal(t, first[b].BitAt(part), second[b].BitAt(part),
			"a live claim should not move between publishes")
	}
}

// TestAssignReassignsAfterLapseAndPrefersAFreshPeer covers the rule notes/design-space.md section 10
// lists as *unbuilt* for variant B: "a lapsed claim can currently go straight back to the peer that
// did not answer".
func TestAssignReassignsAfterLapseAndPrefersAFreshPeer(t *testing.T) {
	const length = 1
	a, b := peer.ID("a"), peer.ID("b")
	holds := holdersOf(length, map[peer.ID][]uint64{a: {0}, b: {0}})

	claims := newRequestClaims()
	now := time.Now()
	first := claims.assign(missingAll(length), []peer.ID{a, b}, holds, 1, now)
	commitAll(claims, first, now)

	var firstAsked, other peer.ID
	if first[a].BitAt(0) {
		firstAsked, other = a, b
	} else {
		firstAsked, other = b, a
	}

	// Nothing changes while the claim is live.
	mid := claims.assign(missingAll(length), []peer.ID{a, b}, holds, 1, now.Add(10*time.Millisecond))
	commitAll(claims, mid, now.Add(10*time.Millisecond))
	require.Equal(t, true, mid[firstAsked].BitAt(0))
	require.Equal(t, false, mid[other].BitAt(0))

	// Past the claim, the request moves to the peer that has not been asked.
	after := claims.assign(missingAll(length), []peer.ID{a, b}, holds, 1, now.Add(requestClaimCeiling+time.Second))
	require.Equal(t, true, after[other].BitAt(0), "a lapsed claim should prefer a peer not already asked")
	require.Equal(t, false, after[firstAsked].BitAt(0))
}

// TestAssignFallsBackToAnAskedPeerABoundedNumberOfTimes is the liveness half of the rule above,
// with the bound the repair plan added: preferring a fresh peer must not mean giving up on the only
// holder the first time it fails -- but a holder that keeps failing is not asked forever either.
// Before the bound the fallback was unconditional, which a withholding sole holder could exploit by
// doing nothing: it was re-asked every TTL for the life of the group.
func TestAssignFallsBackToAnAskedPeerABoundedNumberOfTimes(t *testing.T) {
	const length = 1
	a := peer.ID("a")
	holds := holdersOf(length, map[peer.ID][]uint64{a: {0}})

	claims := newRequestClaims()
	now := time.Now()
	asks := 0
	for round := range MaxClaimLapses + 2 {
		at := now.Add(time.Duration(round) * (requestClaimCeiling + time.Second))
		assignment := claims.assign(missingAll(length), []peer.ID{a}, holds, 1, at)
		if assignment[a].BitAt(0) {
			asks++
			commitAll(claims, assignment, at)
		}
	}
	require.Equal(t, MaxClaimLapses, asks,
		"the only holder is re-asked after each lapse until it has lapsed MaxClaimLapses times, then left alone")

	// Excluded for the group's lifetime, not just this round.
	later := claims.assign(missingAll(length), []peer.ID{a}, holds, 1, now.Add(time.Hour))
	require.Equal(t, false, later[a].BitAt(0))

	// A new holder appearing is asked at once: the exclusion is per peer, not per part.
	b := peer.ID("b")
	both := holdersOf(length, map[peer.ID][]uint64{a: {0}, b: {0}})
	withB := claims.assign(missingAll(length), []peer.ID{a, b}, both, 1, now.Add(time.Hour))
	require.Equal(t, true, withB[b].BitAt(0))
	require.Equal(t, false, withB[a].BitAt(0))
}

// TestRetryBackoffLengthensTheClaim: a re-ask after a lapse waits twice as long as the ask before
// it, so a slow-but-honest peer gets the time it evidently needs and a silent one costs less each
// round.
func TestRetryBackoffLengthensTheClaim(t *testing.T) {
	const length = 1
	a := peer.ID("a")
	holds := holdersOf(length, map[peer.ID][]uint64{a: {0}})

	claims := newRequestClaims()
	now := time.Now()
	first := claims.assign(missingAll(length), []peer.ID{a}, holds, 1, now)
	commitAll(claims, first, now)
	firstDeadline, ok := claims.earliestDeadline()
	require.Equal(t, true, ok)
	base := firstDeadline.Sub(now)
	require.Equal(t, claimTTL(1), base)

	// Lapse, re-ask.
	at := now.Add(base + time.Millisecond)
	second := claims.assign(missingAll(length), []peer.ID{a}, holds, 1, at)
	require.Equal(t, true, second[a].BitAt(0))
	commitAll(claims, second, at)
	secondDeadline, ok := claims.earliestDeadline()
	require.Equal(t, true, ok)
	require.Equal(t, 2*base, secondDeadline.Sub(at), "the second claim should wait twice as long")

	// The backoff never exceeds the ceiling.
	require.Equal(t, requestClaimCeiling, claims.backoff(0, a, requestClaimCeiling))
}

// TestSettleClearsLapses: a late answer is still an answer. Once the part arrives nothing about
// this peer's history on it is kept -- and since the ledger is per part, its record on other parts
// is untouched either way.
func TestSettleClearsLapses(t *testing.T) {
	claims := newRequestClaims()
	a := peer.ID("a")
	for range MaxClaimLapses {
		claims.noteLapse(0, a)
	}
	claims.noteLapse(1, a)
	require.Equal(t, true, claims.exhausted(0, a))
	require.Equal(t, false, claims.exhausted(1, a))

	claims.settle(0)
	require.Equal(t, false, claims.exhausted(0, a))
	require.Equal(t, 1, claims.lapses[1][a])
}

// TestClaimTTLScalesWithWhatWasAsked pins why the timeout is not a bare constant: a peer asked for
// sixty cells needs longer than one asked for two, and notes/design-space.md section 10 calls the
// fixed version "current, and known wrong".
func TestClaimTTLScalesWithWhatWasAsked(t *testing.T) {
	require.Equal(t, requestClaimBase+requestClaimPerPart, claimTTL(1))
	require.Equal(t, true, claimTTL(60) > claimTTL(1), "more parts asked should mean a longer claim")
	require.Equal(t, requestClaimCeiling, claimTTL(1_000_000), "and it must be clamped")
}

// TestSettleClearsAClaim: cells arriving means the part is no longer wanted, so its ledger entry
// must go too or the map grows with the group.
func TestSettleClearsAClaim(t *testing.T) {
	const length = 2
	a := peer.ID("a")
	holds := holdersOf(length, map[peer.ID][]uint64{a: {0, 1}})

	claims := newRequestClaims()
	now := time.Now()
	commitAll(claims, claims.assign(missingAll(length), []peer.ID{a}, holds, 1, now), now)
	require.Equal(t, 1, claims.liveCount(0, now))

	claims.settle(0)
	require.Equal(t, 0, claims.liveCount(0, now))
	require.Equal(t, 1, claims.liveCount(1, now), "settling one part must not clear another")
}

// TestNewPartsMetadataNarrowsToTheAssignment is the join between the ledger and the wire: the
// requests bitmap a peer receives must be its share, not the whole gap.
func TestNewPartsMetadataNarrowsToTheAssignment(t *testing.T) {
	// Four parts, none held: the whole gap is wanted.
	column := mustNewPartialColumn(t, 4)

	// Lacking everything, but assigned only part 2.
	assigned := bitfield.NewBitlist(4)
	assigned.SetBitAt(2, true)

	meta, err := column.newPartsMetadata(assigned)
	require.NoError(t, err)
	require.Equal(t, uint64(1), meta.Requests.Count(), "requests should be the assignment, not the gap")
	require.Equal(t, true, meta.Requests.BitAt(2))

	// A nil assignment is the no-peer-context case and asks for everything.
	whole, err := column.newPartsMetadata(nil)
	require.NoError(t, err)
	require.Equal(t, uint64(4), whole.Requests.Count())
}

// TestCommitIsSeparateFromAssign is the phantom-ledger-entry guard, ported from the nqg series --
// "record the asks only for the messages we are actually sending, so a request dropped by
// truncation leaves no phantom entry in the ledger". Here the equivalent is a peer whose action
// failed to build: claiming at plan time would park the part for a whole TTL with nobody asked.
func TestCommitIsSeparateFromAssign(t *testing.T) {
	const length = 2
	a := peer.ID("a")
	holds := holdersOf(length, map[peer.ID][]uint64{a: {0, 1}})

	claims := newRequestClaims()
	now := time.Now()
	assignment := claims.assign(missingAll(length), []peer.ID{a}, holds, 1, now)
	require.Equal(t, uint64(2), assignment[a].Count(), "both parts should be planned")

	// Planning alone must leave the ledger empty, so an unsent request is retried at once rather
	// than after a timeout.
	require.Equal(t, 0, claims.liveCount(0, now), "assign must not claim")
	require.Equal(t, 0, claims.liveCount(1, now), "assign must not claim")

	// A replan immediately after therefore still offers the parts.
	replan := claims.assign(missingAll(length), []peer.ID{a}, holds, 1, now)
	require.Equal(t, uint64(2), replan[a].Count(), "an unsent plan must not block the next one")

	// Only commit claims them.
	claims.commit(a, assignment[a], now)
	require.Equal(t, 1, claims.liveCount(0, now))
	require.Equal(t, 1, claims.liveCount(1, now))
}

// TestExtendFromVerifiedCellSettlesTheClaim wires the ledger to arrival, which is nqg's
// fulfillIWant. Without it the entry sits until the group is evicted.
func TestExtendFromVerifiedCellSettlesTheClaim(t *testing.T) {
	column := mustNewPartialColumn(t, 4)
	a := peer.ID("a")
	holds := holdersOf(4, map[peer.ID][]uint64{a: {0, 1, 2, 3}})

	now := time.Now()
	assignment := column.claims().assign(column.missingParts(), []peer.ID{a}, holds, 1, now)
	column.claims().commit(a, assignment[a], now)
	require.Equal(t, 1, column.claims().liveCount(2, now))

	require.Equal(t, true, column.ExtendFromVerifiedCell(2, make([]byte, 2048), make([]byte, 48)))
	require.Equal(t, 0, column.claims().liveCount(2, now), "an arrived cell should settle its claim")
	require.Equal(t, 1, column.claims().liveCount(3, now), "and leave the others alone")
}
