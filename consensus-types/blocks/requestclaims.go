package blocks

// Request de-confliction for partial messages.
//
// The problem, measured (notes/rowdas/experiments.md R3 and R2): a node's `requests` bitmap is
// everything it lacks, the same bitmap goes to every mesh peer, and each peer independently serves
// whatever it holds of that set. Nothing de-conflicts, so received bytes are linear in mesh degree
// with a coefficient of one -- 9.7x the information-theoretic minimum on the row axis at a median
// mesh of 9, against 1.8x on the column axis, which mostly escapes it because the proposer is a
// single source and pooled custody is not.
//
// This is the same failure gossipsub's IWANT path had before the discipline in
// `third_party/go-libp2p-pubsub/phaseforward.go`, but it cannot reuse that fix: partial messages
// ride `rpc.Partial` and never emit IWANT, and the extension is parts-agnostic, so there is no
// request identity at the router layer to hang a discipline on. It has to live here.
//
// What this implements, and deliberately not more. `notes/design-space.md` section 10 scopes the
// (N, q, g) kernel down to the *inner per-ID launch scheduler* and lists four things a deployable
// request path needs around it -- a candidate policy, admission limits, attempt/deadline semantics,
// and a censoring-aware estimator feeding q. That document also records which levers measured
// badly: a per-peer response-time quantile needs a censoring-aware hierarchical estimator to mean
// anything (variant B's `srtt+4·rttvar` is explicitly "a rough precedent, not an implementation"),
// and the coordinated push-grace sweep *doubled* completion time. So:
//
//	built here    "one peer per missing part" -- which section 10 lists as current-and-right for
//	              variant B -- with the fanout as a parameter, a timeout that scales with what was
//	              asked rather than a bare constant, and a replacement that prefers a peer not
//	              already asked. That last one is listed as *unbuilt* for variant B, so it is
//	              cheaper to get right here than to retrofit there.
//	not built     the per-peer quantile estimator, the push grace, and node-wide admission limits.
//	              The first two are cautioned against by measurement; the third is unnecessary here
//	              in a way it is not in general, because the request set is bounded by the part
//	              count of one group -- 128 cells -- rather than by an unbounded stream of ids.
//
// The scheduling mechanism, stated plainly because it is much less than (N, q, g) and the difference
// matters:
//
//	dispatch      There is no timer. The assignment is recomputed synchronously inside
//	              partialPublishActions, so a request goes out whenever the broadcaster publishes on
//	              that (topic, group) -- on our own publish, on an incoming RPC that warrants a
//	              republish, and on gossipsub's heartbeat gossip emission. So `g`, the dispatch
//	              grace, is zero: the first request goes out on the first publish.
//	expiry        A claim lapses by wall clock, `claimTTL(parts asked of that peer)`, doubled for
//	              each time that peer already let a claim on the part lapse. The lapse is *noticed*
//	              at the next publish or at the claim wake-up timer, whichever is first. There is no
//	              `q`: the TTL is base + per-part, clamped, not a quantile of a per-peer
//	              response-time distribution.
//	bound         A peer whose claim on a part lapsed MaxClaimLapses times is not asked for that
//	              part again for the group's lifetime, even if it is the only holder. The part then
//	              waits for a new holder to appear or for the column path -- which EIP-8371 line 171
//	              names as the authoritative path the row axis may fall back to. Before this bound
//	              the fallback was unconditional, which was live but exploitable: a silent peer that
//	              was the sole holder was re-asked every TTL for as long as the group lived.
//	granularity   A peer is asked for *every* part assigned to it in one metadata message -- the
//	              requests bitmap -- not one part at a time. With 96 missing parts over 9 peers,
//	              each peer is asked for about ten. What is one-at-a-time is the other axis: at
//	              N = 1, each *part* is asked of exactly one peer until its claim lapses.
//
// On reassignment waiting for the next publish: this is not a shortcut, it is the same choice the
// nqg series made, and for the same reason. Its `canRequestIWant` says so directly -- "the deferred
// pull happens on the next IHAVE once the grace has elapsed (IHAVEs re-gossip each heartbeat), not
// from a timer". Neither implementation has the per-peer timer callback that the idealized loop in
// design-space.md section 10 describes; both re-aim on the next event, which for us is the next
// publish and for gossipsub is the next IHAVE, and both are therefore bounded by the heartbeat.
//
// What nqg does have and this does not: a *per-peer* expiry horizon, adaptive from observed response
// times (`iwantHorizon`, `IWantAdaptiveHorizon`, fed by `fulfillIWant`). That is the q of (N, q, g),
// and section 10 is explicit that it needs a censoring-aware hierarchical estimator to mean
// anything. claimTTL here is base + per-part, clamped -- the same class of thing as variant B's
// `srtt+4·rttvar`, which that document calls "a rough precedent, not an implementation". Recorded as
// the follow-up in notes/rowdas/TODO.md D8.
//
// One thing that looked like a requirement and is not: a tie-break keyed on node identity, so that
// different nodes do not all pick the same peer for the same part. Two nodes that both lack part i
// and share a peer must both be served it -- they are different requesters with a genuine need, so
// that is not duplication. The duplication this fixes is one node asking several peers. Spreading
// load across *my own* peers is the real requirement, and it is local.

import (
	"sync"
	"time"

	"github.com/OffchainLabs/go-bitfield"
	"github.com/libp2p/go-libp2p/core/peer"
)

// RequestN is N from notes/design-space.md section 10: "target plausible parallelism **per needed
// ID**". Here the needed ID is a part -- one cell of one group -- so RequestN is the maximum number
// of peers that may hold a live claim on the same part at the same time. Named for N rather than
// coining a second word for it, because this repository already carries a lot of reasoning in those
// terms.
//
// One is byte-optimal and is the liveness-riskiest: a part asked only of a withholder stalls until
// the claim lapses, and a part whose only holder is saturated waits behind that peer's uplink rather
// than being served opportunistically by whoever is free. Two is the "ask k peers, take the first"
// hedge that section 10 lists as trading bytes for tail latency; measured at 1.49x the bytes for no
// convergence gain on the pooling exchange (TODO.md D8), so one is the setting.
//
// Note what N is *not*, per the same section: it does not bound node load. With K missing parts it
// admits up to K x N plausible requests. That is safe here in a way it is not in general, because K
// is bounded by the part count of a single group -- 128 cells -- rather than by an open-ended stream
// of message ids, so no separate admission layer is needed.
//
// A var rather than a const so the experiments can sweep it; production takes the default.
var RequestN = 1

// MaxClaimLapses bounds how many times a peer is asked for one part after failing to serve it.
// Each ask after a lapse waits twice as long as the last, so with the defaults a silent sole
// holder is asked at 0, ~0.3 s and ~0.9 s and then left alone: about 2.1 s of waiting, inside the
// phase windows the reconstruction duties run on.
//
// The bound exists because re-asking a peer that did not answer is right only when the request
// never reached it -- and that case is now visible at its source, since a claim is committed only
// after queue admission. A peer that received the request and stayed silent is withholding or
// unable, and asking it again is waste that it can induce for free. See notes/rowdas/plan-repair.md
// section 4.
//
// A var so experiments can sweep it: the R5 withholding arm is where it earns or loses its keep.
var MaxClaimLapses = 3

// Request-claim tuning. These are clamps, not an estimator; the estimator is the follow-up recorded
// in notes/rowdas/TODO.md D8.
const (
	// requestClaimBase must cover a round trip plus the peer's queueing delay. Below that, a
	// lapsed claim is re-aimed at a different peer while the first is still in flight and both
	// send -- which is the duplication this file exists to remove, reintroduced by a too-short
	// timeout. notes/design-space.md section 10 calls the fixed-timeout version "current, and
	// known wrong" for exactly this reason.
	requestClaimBase = 300 * time.Millisecond

	// requestClaimPerPart scales the claim with what was actually asked of that peer, which is
	// the part a constant cannot track: a peer asked for sixty cells needs longer than one asked
	// for two.
	requestClaimPerPart = 2 * time.Millisecond

	// requestClaimCeiling bounds the whole thing, so a pathological assignment cannot park a part
	// on a silent peer for a slot.
	requestClaimCeiling = 2 * time.Second
)

// requestClaims is the per-part request ledger: for each part this node lacks, which peers hold a
// live claim on serving it and which have ever been asked.
//
// It lives on the partial message rather than in the broadcaster because the message *is* the
// per-(topic, group) state, and a claim has to outlive a single publish: the whole point is that
// the next publish does not re-ask everyone.
type requestClaims struct {
	// mu guards the maps below.
	//
	// The ledger is entered from two goroutines. `assign` and `commit` run while the
	// partial-messages extension builds publish actions, which happens on pubsub's goroutine;
	// `earliestDeadline` is read by the broadcaster's event loop when it arms the claim wake-up.
	// Before that wake-up existed nothing outside the publish path touched this, so the race is
	// new -- and it announced itself immediately as "concurrent map iteration and map write" in the
	// row integration suite.
	//
	// Only the three entry points -- assign, commit, earliestDeadline -- take the lock. Everything
	// else here is called from inside them and assumes it is held, which is why those helpers are
	// named plainly rather than being exported into a second locking layer.
	mu sync.Mutex

	live map[uint64]map[peer.ID]time.Time
	// asked is every peer ever asked for a part, so a replacement can prefer a fresh one. It is
	// bounded by parts x peers for one group and cleared with the group.
	asked map[uint64]map[peer.ID]bool
	// lapses counts, per part and peer, the claims that expired unserved. It drives the backoff
	// and the exclusion, and settle clears it: a late answer is still an answer.
	lapses map[uint64]map[peer.ID]int
	// pending records, per peer, news that is being held back rather than sent -- the
	// piggyback-or-flush policy in partialcells.go: when a request *removal* to that peer was
	// first deferred, and when an *availability* growth was. It lives here rather than in the
	// peer state because the peer state is recorded only when an action is admitted, and the
	// whole point of a deferral is that nothing is sent. Cleared when metadata to the peer goes
	// out, since that metadata carries everything current; pruned to the peers a publish still
	// sees, so a departed peer cannot hold a deadline open.
	pending map[peer.ID]pendingNews
}

// pendingNews is what a peer has not been told yet, by when it started waiting. A zero time means
// nothing of that kind is waiting.
type pendingNews struct {
	cancelSince time.Time
	// availPending is whether availability grew since the peer was last told. Its deadline is
	// not its own start but lastTold plus the window: at most one availability announcement per
	// window per peer, and the first one after a quiet window goes at once. Nagle's shape --
	// send when nothing is in flight, hold while something is.
	availPending bool
	// lastTold is when metadata to this peer was last admitted, for any reason. Zero means
	// never, which counts as quiet.
	lastTold time.Time
}

func (n pendingNews) empty() bool { return n.cancelSince.IsZero() && !n.availPending }

func newRequestClaims() *requestClaims {
	return &requestClaims{
		live:    make(map[uint64]map[peer.ID]time.Time),
		asked:   make(map[uint64]map[peer.ID]bool),
		lapses:  make(map[uint64]map[peer.ID]int),
		pending: make(map[peer.ID]pendingNews),
	}
}

// liveCount drops lapsed claims on a part, charging each lapse to the peer that let it, and
// returns how many remain.
func (c *requestClaims) liveCount(part uint64, now time.Time) int {
	holders, ok := c.live[part]
	if !ok {
		return 0
	}
	for holder, expiry := range holders {
		if now.After(expiry) {
			delete(holders, holder)
			c.noteLapse(part, holder)
		}
	}
	if len(holders) == 0 {
		delete(c.live, part)
	}

	return len(holders)
}

// holdsClaim reports whether this peer already has a live claim on the part.
func (c *requestClaims) holdsClaim(part uint64, p peer.ID, now time.Time) bool {
	expiry, ok := c.live[part][p]

	return ok && !now.After(expiry)
}

// record awards a claim on a part to a peer.
func (c *requestClaims) record(part uint64, p peer.ID, expiry time.Time) {
	holders, ok := c.live[part]
	if !ok {
		holders = make(map[peer.ID]time.Time)
		c.live[part] = holders
	}
	holders[p] = expiry

	everAsked, ok := c.asked[part]
	if !ok {
		everAsked = make(map[peer.ID]bool)
		c.asked[part] = everAsked
	}
	everAsked[p] = true
}

// noteLapse charges one expired claim on part to p.
func (c *requestClaims) noteLapse(part uint64, p peer.ID) {
	byPeer, ok := c.lapses[part]
	if !ok {
		byPeer = make(map[peer.ID]int)
		c.lapses[part] = byPeer
	}
	byPeer[p]++
}

// exhausted reports whether p has let enough claims on part lapse to be excluded from it.
func (c *requestClaims) exhausted(part uint64, p peer.ID) bool {
	return c.lapses[part][p] >= MaxClaimLapses
}

// backoff scales a base claim TTL by the peer's prior lapses on the part: doubled per lapse, capped.
func (c *requestClaims) backoff(part uint64, p peer.ID, base time.Duration) time.Duration {
	ttl := base
	for range c.lapses[part][p] {
		ttl *= 2
		if ttl >= requestClaimCeiling {
			return requestClaimCeiling
		}
	}

	return ttl
}

// settle drops all claims on a part, which is what arriving cells mean.
// settle is called when a part arrives, from whichever goroutine verified the cell -- so it takes
// the lock like the other entry points. It was the writer still racing earliestDeadline after the
// first fix, because it is reached from ExtendFromVerifiedCell rather than from the publish path.
func (c *requestClaims) settle(part uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	delete(c.live, part)
	delete(c.asked, part)
	delete(c.lapses, part)
}

// assign works out, for one publish, which parts to ask which peer for. It *plans* only: nothing
// enters the ledger until commit is called for a peer whose request actually went out.
//
// missing is what this node lacks and is willing to ask for. holds(p) is what peer p has told us it
// holds -- a *complete* statement, not a sampled IHAVE, which is why assignment is available here
// where the IWANT path could only rate-limit. The result maps each peer to the subset of missing
// parts it is being asked for; a peer with nothing assigned gets an empty bitlist, which is what
// stops it re-serving everything.
func (c *requestClaims) assign(
	missing bitfield.Bitlist,
	peers []peer.ID,
	holds func(peer.ID) bitfield.Bitlist,
	fanout int,
	now time.Time,
) map[peer.ID]bitfield.Bitlist {
	c.mu.Lock()
	defer c.mu.Unlock()

	length := missing.Len()
	assignment := make(map[peer.ID]bitfield.Bitlist, len(peers))
	for _, p := range peers {
		assignment[p] = bitfield.NewBitlist(length)
	}
	if len(peers) == 0 {
		return assignment
	}
	if fanout < 1 {
		fanout = RequestN
	}

	// Load per peer for this round, so a node does not dump every request on one peer. This is
	// the spreading that actually matters; cross-node coordination neither is possible nor is
	// needed -- see the note at the top of the file.
	load := make(map[peer.ID]int, len(peers))

	for part := range length {
		if !missing.BitAt(part) {
			continue
		}
		// Claims that have not lapsed stay put: re-asking a peer that is still plausibly
		// answering is the duplication being removed.
		liveHolders := c.liveCount(part, now)
		for _, p := range peers {
			if c.holdsClaim(part, p, now) {
				assignment[p].SetBitAt(part, true)
				load[p]++
			}
		}
		if liveHolders >= fanout {
			continue
		}

		// Candidates: peers that say they hold this part and do not already hold a claim on it.
		// Preferring one never asked before is the "prefer a peer not already asked" rule that
		// notes/design-space.md section 10 lists as unbuilt for variant B.
		var fresh, retry []peer.ID
		for _, p := range peers {
			if c.holdsClaim(part, p, now) {
				continue
			}
			available := holds(p)
			if available == nil || part >= available.Len() || !available.BitAt(part) {
				continue
			}
			if c.exhausted(part, p) {
				continue
			}
			if c.asked[part][p] {
				retry = append(retry, p)
				continue
			}
			fresh = append(fresh, p)
		}

		for range fanout - liveHolders {
			chosen, ok := leastLoaded(fresh, load)
			if !ok {
				// Nobody fresh. Falling back to a peer already asked keeps liveness when only
				// the peer that failed to answer has the part -- bounded by MaxClaimLapses and
				// backed off in commit, so a silent holder is not asked forever.
				chosen, ok = leastLoaded(retry, load)
				if !ok {
					break
				}
				retry = remove(retry, chosen)
			} else {
				fresh = remove(fresh, chosen)
			}
			assignment[chosen].SetBitAt(part, true)
			load[chosen]++
		}
	}

	return assignment
}

// commit records the claims for a peer whose request actually went out.
//
// Separate from assign, and deliberately so: the vendored fork's nqg series learned this the same
// way, and says why at its own call site -- "record the asks only for the messages we are actually
// sending, so a request dropped by truncation leaves no phantom entry in the ledger". A claim
// recorded for a request that was never sent parks the part for a whole TTL with nobody asked.
//
// The timeout scales with what this peer was asked for, which is why it is computed here rather
// than per part.
// commit records a claim for each part this peer was just asked for.
//
// **A live claim's deadline is not extended.** The deadline measures "how long ago we first asked
// this peer and it has not delivered", not "how long ago we last mentioned it" -- and those diverge
// badly. The metadata we send carries our *whole* current request bitmap, not a delta, so a message
// sent for an unrelated reason (our availability grew) also restates every outstanding request. If
// that restatement refreshed the deadlines, a peer that simply never answers would hold its claims
// for as long as we kept talking to it, and the part would never be offered to another holder. An
// adversary needs to do nothing at all to exploit that: stay silent and stay claimed.
//
// So a claim ages from its first ask. Reassignment then happens on schedule, whatever else is being
// announced in the meantime.
func (c *requestClaims) commit(p peer.ID, asked bitfield.Bitlist, now time.Time) {
	if asked == nil || asked.Count() == 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	base := claimTTL(asked.Count())
	for part := range asked.Len() {
		if !asked.BitAt(part) {
			continue
		}
		if holders, ok := c.live[part]; ok {
			if deadline, held := holders[p]; held && now.Before(deadline) {
				// Already claimed by this peer and not yet lapsed: leave the original deadline.
				continue
			}
		}
		// A re-ask after a lapse waits longer, so a slow-but-honest peer gets the time it
		// evidently needs and a silent one costs less each round.
		c.record(part, p, now.Add(c.backoff(part, p, base)))
	}
}

// noteCancelPending records that a removal to p is being deferred, and returns when the deferral
// began -- now, if this is the first time.
func (c *requestClaims) noteCancelPending(p peer.ID, now time.Time) time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()

	n := c.pending[p]
	if n.cancelSince.IsZero() {
		n.cancelSince = now
		c.pending[p] = n
	}

	return n.cancelSince
}

// noteAvailPending records that availability grew for p since it was last told, and returns when
// it was last told -- the hold's deadline is that plus the window. A zero return means never told,
// which the caller treats as quiet.
func (c *requestClaims) noteAvailPending(p peer.ID) time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()

	n := c.pending[p]
	n.availPending = true
	c.pending[p] = n

	return n.lastTold
}

// lastTold reports when metadata to p was last admitted; zero if never.
func (c *requestClaims) lastTold(p peer.ID) time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.pending[p].lastTold
}

// clearCancelPending forgets a deferred removal to p -- the request set matched again without
// anything sent, so there is nothing left to cancel. Availability that is waiting keeps waiting,
// and the last-told record stays.
func (c *requestClaims) clearCancelPending(p peer.ID) {
	c.mu.Lock()
	defer c.mu.Unlock()

	n := c.pending[p]
	n.cancelSince = time.Time{}
	c.pending[p] = n
}

// clearPending forgets everything waiting for p, because metadata carrying the current state has
// gone out to it at now -- which is also the moment the next window starts from.
func (c *requestClaims) clearPending(p peer.ID, now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.pending[p] = pendingNews{lastTold: now}
}

// retainPending drops held news for peers no longer in keep. Without it a peer that left would
// hold its deadline open for the group's lifetime, and the wake-up would fire for it every time.
func (c *requestClaims) retainPending(keep map[peer.ID]PartialDataColumnPeerState) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for p := range c.pending {
		if _, ok := keep[p]; !ok {
			delete(c.pending, p)
		}
	}
}

// earliestDeadline reports the soonest moment something in this ledger needs a publish: a live
// claim lapsing, or held news -- a deferred cancellation, held availability -- reaching its
// maximum delay. A caller wakes up then and
// publishes rather than waiting for whatever traffic happens to arrive next. Lapsed entries are not
// reaped here -- that is liveCount's job during assignment -- so a deadline already in the past is
// returned as-is and the caller wakes immediately.
func (c *requestClaims) earliestDeadline() (time.Time, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	var earliest time.Time
	consider := func(deadline time.Time) {
		if earliest.IsZero() || deadline.Before(earliest) {
			earliest = deadline
		}
	}
	for _, holders := range c.live {
		for _, deadline := range holders {
			consider(deadline)
		}
	}
	cancelDelay, availDelay := cancellationMaxDelay(), availabilityMaxDelay()
	for _, n := range c.pending {
		if !n.cancelSince.IsZero() {
			consider(n.cancelSince.Add(cancelDelay))
		}
		if n.availPending {
			consider(n.lastTold.Add(availDelay))
		}
	}

	return earliest, !earliest.IsZero()
}

// claimTTL is the clamped, size-scaled timeout. Not an estimator: see the file header.
func claimTTL(parts uint64) time.Duration {
	ttl := requestClaimBase + time.Duration(parts)*requestClaimPerPart
	if ttl > requestClaimCeiling {
		return requestClaimCeiling
	}

	return ttl
}

func leastLoaded(candidates []peer.ID, load map[peer.ID]int) (peer.ID, bool) {
	var best peer.ID
	found := false
	for _, p := range candidates {
		if !found || load[p] < load[best] {
			best, found = p, true
		}
	}

	return best, found
}

func remove(candidates []peer.ID, p peer.ID) []peer.ID {
	for i, candidate := range candidates {
		if candidate == p {
			return append(candidates[:i], candidates[i+1:]...)
		}
	}

	return candidates
}
