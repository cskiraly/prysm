package segmentbroadcaster

import (
	"time"

	"github.com/OffchainLabs/prysm/v7/container/segments"
	"github.com/libp2p/go-libp2p/core/peer"
)

// verifyQueueSize bounds how many segment batches may wait for verification.
//
// Overflow drops the batch rather than blocking, because the alternative is blocking
// pubsub's goroutine. A dropped batch is recoverable: the segments are still advertised by
// the peer that sent them, so the next publish pass requests them again.
const verifyQueueSize = 512

// loop is the broadcaster's single worker. Everything that publishes happens here.
func (b *Broadcaster) loop() {
	retry := time.NewTicker(b.cfg.RetryInterval)
	defer retry.Stop()

	for {
		select {
		case <-b.ctx.Done():
			return
		case work := <-b.verifyQueue():
			b.handleSegments(work)
		case <-b.wake:
			b.drainPending()
		case <-retry.C:
			// A group that is still incomplete may be waiting on a request that was lost or
			// on a peer that stopped answering. Re-running the pass reassigns it.
			b.retryIncomplete()
		}
	}
}

// handleSegments verifies a batch, buffers it, and schedules a publish pass.
func (b *Broadcaster) handleSegments(work verifyWork) {
	b.mu.Lock()
	r := b.reassembler
	b.mu.Unlock()
	if r == nil {
		return
	}

	// Opening a group costs a signature verification, so charge the peer for it. A segment
	// joining an already-open group proves itself against the pinned root and is free.
	if !r.Has(work.groupID) && !b.spendAuthBudget(work.from) {
		b.bump(func(c *Counters) { c.AuthThrottled++ })
		return
	}

	var completed []byte
	for i, m := range work.msgs {
		done, err := r.Add(work.hashers[i], m)
		if err != nil {
			b.bump(func(c *Counters) { c.SegmentsRejected++ })
			b.logger.WithError(err).WithField("peer", work.from.String()).
				Debug("Rejected payload segment")
			continue
		}
		if done != nil {
			completed = done
		}
		b.bump(func(c *Counters) { c.SegmentsReceived++ })
		b.sampleClaim(work.topic, work.groupID, m.Index, work.from)
	}

	// Publish before delivering: peers waiting on the segments we just took in should not
	// wait behind envelope processing.
	b.markPending(work.topic, work.groupID)

	if completed != nil {
		b.bump(func(c *Counters) { c.GroupsCompleted++ })
		if err := b.callbacks.OnEnvelope(completed); err != nil {
			b.logger.WithError(err).Debug("Could not deliver reassembled envelope")
		}
	}
}

// drainPending runs the publish pass for every group that was marked.
func (b *Broadcaster) drainPending() {
	b.mu.Lock()
	keys := make([]groupKey, 0, len(b.pending))
	for k := range b.pending {
		keys = append(keys, k)
	}
	b.pending = make(map[groupKey]struct{})
	b.mu.Unlock()

	for _, k := range keys {
		if err := b.publishGroup(k); err != nil {
			b.logger.WithError(err).WithField("topic", k.topic).Debug("Could not publish segment group")
		}
	}
}

// retryIncomplete re-runs the publish pass for groups we have not finished.
//
// This is the only thing that recovers a lost request. A request is aimed at one peer and
// nothing acknowledges it, so if it or the reply is dropped the segment simply never arrives;
// the claim lapses after RequestTimeout and this pass hands it to someone else.
//
// A group we hold *nothing* of is the case that most needs retrying and is easiest to get
// wrong: the reassembler has never heard of it, so asking the reassembler whether it is
// complete cannot be the test. An earlier version treated "reassembler does not know it" as
// "forget it", which silently abandoned exactly the groups that were stalled from the first
// request onwards.
func (b *Broadcaster) retryIncomplete() {
	b.mu.Lock()
	r := b.reassembler
	keys := make([]groupKey, 0, len(b.ledgers))
	ages := make([]time.Duration, 0, len(b.ledgers))
	now := b.now()
	for k, l := range b.ledgers {
		keys = append(keys, k)
		ages = append(ages, now.Sub(l.created))
	}
	b.mu.Unlock()
	if r == nil {
		return
	}

	for i, k := range keys {
		groupID := []byte(k.group)
		if r.Complete(groupID) {
			// Complete -- which for a coded group happens at Required segments, not at a
			// full bitmap. The group itself lingers to serve peers until its own TTL; the
			// request ledger has nothing left to track.
			b.forgetLedger(k)
			continue
		}
		// Bound the announce-only case. A group whose segments never arrive would otherwise
		// be retried for the lifetime of the process, since nothing else ever retires it.
		if ages[i] > b.cfg.GroupTTL {
			b.forgetLedger(k)
			b.forgetAnnounced(k)
			continue
		}
		if err := b.publishGroup(k); err != nil {
			b.logger.WithError(err).Debug("Could not retry segment group")
		}
	}
}

// publishGroup builds the publish input for one group and hands it to the extension.
//
// The state read here -- what we hold, what we want -- is a snapshot. The decision the
// extension then makes with it is per peer, and the peer half of the state lives in the
// extension's map, so this is the only place the two views meet.
func (b *Broadcaster) publishGroup(k groupKey) error {
	b.mu.Lock()
	r, publish := b.reassembler, b.publishPartial
	b.mu.Unlock()
	if r == nil || publish == nil {
		return ErrNotStarted
	}

	groupID := []byte(k.group)
	held, ok := r.Held(groupID)
	var required uint32
	if ok {
		// The reassembler is now the authority on this group's shape.
		b.forgetAnnounced(k)
		if d, _, dok := r.Descriptor(groupID); dok {
			required = d.Required()
		}
	} else {
		// We hold nothing, so the only thing we know is the shape a peer announced. That is
		// enough to ask: an empty holdings bitmap of the right width makes every index a want.
		count, req, known := b.announcedCount(k)
		if !known {
			return nil
		}
		empty, err := segments.NewBitmap(count)
		if err != nil {
			return err
		}
		held = empty
		required = req
	}
	if required == 0 || required > held.Count() {
		required = held.Count()
	}

	var wanted *segments.Bitmap
	if !r.Complete(groupID) && uint32(held.Len()) < required {
		w, err := b.bitmapOfMissing(held)
		if err != nil {
			return err
		}
		wanted = w
	}

	ledger := b.ledgerFor(k, held.Count())
	budget := b.requestBudget(ledger, held, required)
	if required < held.Count() && b.now().Sub(ledger.created) < b.cfg.RequestDefer {
		// A coded group inside its deferral window issues no new requests: pushes still in
		// flight count toward K whatever their index, so asking now buys duplicates. Plain
		// groups skip this on purpose -- for them deferral trades the same duplicates
		// against stalling on exactly the indices the pushes do not cover.
		budget = 0
	}
	b.mu.Lock()
	self, grace := b.self, b.now().Sub(ledger.created) < b.cfg.PushGrace
	b.mu.Unlock()
	in := publishInput{
		held:        held,
		policy:      b.cfg.Policy,
		wanted:      wanted,
		groupID:     groupID,
		replication: b.cfg.Replication,
		required:    required,
		self:        self,
		pushDivisor: b.cfg.PushDivisor,
		pushGrace:   grace,
		segment: func(index uint32) (*segments.SegmentMessage, bool) {
			return r.Segment(groupID, index)
		},
		claimCap:       b.cfg.ClaimPerPeer,
		pushChunk:      b.cfg.PushChunk,
		now:            b.now(),
		announceWindow: b.cfg.AnnounceWindow,
		deferAnnounce:  func() { b.armAnnounceFlush(k) },
		requestFrom: func(index uint32, p peer.ID, perPeer *int) bool {
			return b.claimRequest(ledger, index, p, &budget, perPeer)
		},
		onSend:         b.recordSend,
		withholdServes: b.cfg.WithholdServes,
		onWithheld: func(k int) {
			b.bump(func(c *Counters) { c.ServesWithheld += k })
		},
		compress: b.cfg.CompressSegments,
	}
	b.bump(func(c *Counters) { c.PublishPasses++ })
	return publish(k.topic, groupID, publishActions(in))
}

// recordSend counts what a publish pass actually put on the wire.
//
// Split by whether the peer asked: a served request is the cost of announce-then-pull, an
// unrequested push is the cost of volunteering. Keeping them apart is what lets the arms be
// compared on bytes rather than on intent.
func (b *Broadcaster) recordSend(_ peer.ID, pushed, served int, metadata bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.counters.PeerActions++
	b.counters.SegmentsPushed += pushed
	b.counters.SegmentsServed += served
	if metadata {
		b.counters.MetadataSent++
	}
}

// bitmapOfMissing inverts a holdings bitmap into a wants bitmap.
func (b *Broadcaster) bitmapOfMissing(held *segments.Bitmap) (*segments.Bitmap, error) {
	out, err := segments.NewBitmap(held.Count())
	if err != nil {
		return nil, err
	}
	for _, idx := range held.Missing() {
		if err := out.Set(idx); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// ledgerFor returns the request ledger for a group, creating it on first use.
func (b *Broadcaster) ledgerFor(k groupKey, count uint32) *requestLedger {
	b.mu.Lock()
	defer b.mu.Unlock()
	l, ok := b.ledgers[k]
	if !ok {
		l = &requestLedger{
			from:     make([]peer.ID, count),
			at:       make([]time.Time, count),
			reissued: make([]bool, count),
			struck:   make([]bool, count),
			strikes:  make(map[peer.ID]int),
			strikeAt: make(map[peer.ID]time.Time),
			created:  b.now(),
		}
		b.ledgers[k] = l
	}
	return l
}

// forgetLedger drops a group's request ledger.
func (b *Broadcaster) forgetLedger(k groupKey) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.ledgers, k)
}

// sampleClaim feeds the adaptive timeout: a verified arrival that satisfies our own
// outstanding claim on that peer measures claim-to-arrival, queueing and transmission
// included. Unsolicited pushes carry no claim and produce no sample.
func (b *Broadcaster) sampleClaim(topic string, groupID []byte, index uint32, from peer.ID) {
	if !b.cfg.AdaptiveRequestTimeout {
		return
	}
	k := groupKey{topic: topic, group: string(groupID)}
	b.mu.Lock()
	l, ok := b.ledgers[k]
	var sample time.Duration
	if ok && index < uint32(len(l.from)) && l.from[index] == from && !l.at[index].IsZero() && !l.reissued[index] {
		sample = b.now().Sub(l.at[index])
	}
	b.mu.Unlock()
	if sample > 0 {
		b.observeRTT(from, sample)
	}
}

// requestBudget is how many *new* request claims the next pass may make.
//
// K-of-K it is exactly the unclaimed missing indices, which is today's behaviour by another
// route. For a coded group it is what separates "ask for what completes me" from "ask for
// everything the group contains": needing K of N with H held and O claims outstanding, only
// K-H-O more requests do anything, and every claim beyond that buys a segment that arrives
// after completion.
//
// The count is a snapshot: the pass that spends it runs later, on pubsub's goroutine, and a
// second pass may be planned before the first has spent anything. The overshoot is bounded
// by one pass and self-corrects, since every pass recounts from the ledger.
func (b *Broadcaster) requestBudget(l *requestLedger, held *segments.Bitmap, required uint32) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := b.now()
	outstanding := 0
	for _, idx := range held.Missing() {
		if idx < uint32(len(l.from)) && l.from[idx] != "" && now.Sub(l.at[idx]) < b.claimTimeoutLocked(l.from[idx]) {
			outstanding++
		}
	}
	committed := uint32(held.Len() + outstanding)
	target := required
	if b.cfg.RequestSurplus > 0 && required < held.Count() {
		target = min(required+uint32(b.cfg.RequestSurplus), held.Count())
	}
	if committed >= target {
		return 0
	}
	return int(target - committed)
}

// claimRequest decides whether to ask this peer for this segment.
//
// One peer per segment at a time. The first peer offered the segment in a pass takes it and
// keeps it for RequestTimeout; after that the claim lapses and the next pass may hand it to
// someone else. That is what turns "everyone who has it" into "one who has it", and it is the
// difference between announce-then-pull costing one copy and costing D.
//
// A fresh claim additionally spends from the pass's budget and, when perPeer is non-nil,
// from this peer's per-pass allowance; re-affirming an unexpired claim is free, so a peer
// keeps being asked for what it was already asked for.
func (b *Broadcaster) claimRequest(l *requestLedger, index uint32, p peer.ID, budget, perPeer *int) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if index >= uint32(len(l.from)) {
		return false
	}
	now := b.now()
	if l.from[index] != "" && now.Sub(l.at[index]) < b.claimTimeoutLocked(l.from[index]) {
		return l.from[index] == p
	}
	// The old claim, if any, lapsed unserved: that is the one observable a withholder
	// produces, so it is what the strike memory records. Arrival never reaches here -- a
	// held index is not a want -- so a slow serve that got covered by someone else is not
	// blamed either. One lapse is one strike: the pass evaluates every candidate for the
	// index before someone claims it, and charging the lapsed peer once per candidate made
	// a cap of two behave as a cap of one on any group with more than one holder.
	if b.cfg.StrikeCap > 0 && l.from[index] != "" && !l.struck[index] {
		l.strikes[l.from[index]]++
		l.strikeAt[l.from[index]] = now
		l.struck[index] = true
	}
	if b.cfg.StrikeCap > 0 && l.strikes[p] >= b.cfg.StrikeCap {
		ttl := b.cfg.StrikeTTL
		if ttl <= 0 {
			ttl = 2 * time.Second
		}
		// A struck peer is still asked for an index nobody else has served: if the lapsed
		// claim is a full extra timeout old and no other peer has claimed the index since,
		// there is no alternative to move on to, and a park that strands the index would be
		// the mechanism deciding the cell. The strike stays on the peer's record.
		stranded := l.from[index] != "" && now.Sub(l.at[index]) >= 2*b.claimTimeoutLocked(p)
		if now.Sub(l.strikeAt[p]) < ttl && !stranded {
			return false
		}
		if now.Sub(l.strikeAt[p]) >= ttl {
			// Forgiveness: the TTL expiring clears the count, so a group whose every
			// announcer is struck degrades to one burned expiry per TTL rather than a
			// permanent stall.
			delete(l.strikes, p)
			delete(l.strikeAt, p)
		}
	}
	if *budget <= 0 {
		return false
	}
	if perPeer != nil {
		if *perPeer <= 0 {
			return false
		}
		*perPeer--
	}
	*budget--
	// Distinguish a first ask from a re-ask. A re-ask means the previous claim lapsed without
	// the segment arriving, so the two counts together are the retry pressure on the group --
	// folding them into one number reported eight requests for a four-segment group that was
	// served exactly once, which reads as duplication that did not happen.
	if l.from[index] == "" {
		b.counters.SegmentsRequested++
	} else {
		b.counters.RequestsReissued++
		// Karn's rule: a reissued claim's eventual arrival is ambiguous between the first
		// and second peer, so it must never feed the RTT estimator (adaptive v1 lacked
		// this, and ambiguous samples helped clamp the estimate low).
		l.reissued[index] = true
	}
	l.from[index] = p
	l.at[index] = now
	l.struck[index] = false
	return true
}

// spendAuthBudget reports whether a peer may spend one descriptor authentication.
func (b *Broadcaster) spendAuthBudget(p peer.ID) bool {
	b.mu.Lock()
	limiter := b.authLimiter
	b.mu.Unlock()
	if limiter == nil {
		return true
	}
	key := p.String()
	if limiter.Remaining(key) <= 0 {
		return false
	}
	limiter.Add(key, 1)
	return true
}
