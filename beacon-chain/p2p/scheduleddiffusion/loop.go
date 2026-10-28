package scheduleddiffusion

import (
	"time"

	"github.com/OffchainLabs/prysm/v7/container/segments"
	"github.com/libp2p/go-libp2p/core/peer"
)

// verifyQueueSize bounds how many segment batches may wait for verification.
//
// Overflow drops the batch rather than blocking, because the alternative is blocking
// pubsub's goroutine. The sender that pushed a dropped batch will not offer it again -- its
// latch says we hold it -- so recovery is from another holder only. In virtual time the loop
// keeps up and the queue never fills.
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
			b.retryGroups()
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

	// The reassembler accepts a duplicate idempotently and reports it the same way as an
	// addition, so what was already held has to be read before the add to tell them apart.
	held, known := r.Held(work.groupID)
	var completed []byte
	for i, m := range work.msgs {
		if !known {
			if bm, err := segments.NewBitmap(m.Descriptor.Count); err == nil {
				held, known = bm, true
			}
		}
		duplicate := held.Has(m.Index)
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
		if duplicate {
			b.bump(func(c *Counters) { c.SegmentsDuplicate++ })
			continue
		}
		b.bump(func(c *Counters) { c.SegmentsReceived++ })
		if err := held.Set(m.Index); err != nil {
			b.logger.WithError(err).Debug("Could not record held segment")
		}
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

// retryGroups re-runs the publish pass for every group we still keep.
//
// The only thing that recovers a refused RPC. Nothing acknowledges a send, and an action
// the peer's queue refused left no state behind, so the segment is simply still sendable --
// this pass offers it again. Complete groups tick too, until their TTL: a publisher is
// complete from the first pass, and a refused send to a peer that holds nothing has no other
// trigger -- that peer says nothing, and its standing want is already everything. A pass
// with nothing new yields no actions, so the ticking is cheap.
func (b *Broadcaster) retryGroups() {
	b.mu.Lock()
	r := b.reassembler
	keys := make([]groupKey, 0, len(b.groups))
	ages := make([]time.Duration, 0, len(b.groups))
	now := b.now()
	for k, created := range b.groups {
		keys = append(keys, k)
		ages = append(ages, now.Sub(created))
	}
	b.mu.Unlock()
	if r == nil {
		return
	}

	for i, k := range keys {
		// Bound the announce-only case. A group whose segments never arrive would otherwise
		// be retried for the lifetime of the process.
		if ages[i] > b.cfg.GroupTTL {
			b.forgetGroup(k)
			continue
		}
		if err := b.publishGroup(k); err != nil {
			b.logger.WithError(err).Debug("Could not retry segment group")
		}
	}
}

// forgetGroup drops a group from the retry set.
func (b *Broadcaster) forgetGroup(k groupKey) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.groups, k)
}

// publishGroup builds the publish input for one group and hands it to the extension.
//
// The state read here -- what we hold -- is a snapshot. The decision the extension then
// makes with it is per peer, and the peer half of the state lives in the extension's map,
// so this is the only place the two views meet.
func (b *Broadcaster) publishGroup(k groupKey) error {
	b.mu.Lock()
	r, publish := b.reassembler, b.publishPartial
	b.mu.Unlock()
	if r == nil || publish == nil {
		return ErrNotStarted
	}

	groupID := []byte(k.group)
	held, ok := r.Held(groupID)
	if !ok || held.Len() == 0 {
		// Nothing to serve and nothing to say. A peer's want for what we do not have is
		// already standing, so silence costs it nothing.
		return nil
	}
	var required uint32
	if d, _, dok := r.Descriptor(groupID); dok {
		required = d.Required()
	}

	in := publishInput{
		groupID:  groupID,
		held:     held,
		required: required,
		complete: r.Complete(groupID),
		segment: func(index uint32) (*segments.SegmentMessage, bool) {
			return r.Segment(groupID, index)
		},
		bump:           b.bump,
		deferAnnounce:  func() { b.armAnnounceFlush(k) },
		meshPeers:      b.meshPeers(k.topic),
		now:            b.now(),
		announceWindow: b.cfg.AnnounceWindow,
		urgentUpdates:  !b.cfg.NormalLaneUpdates,
		compress:       b.cfg.CompressSegments,
	}
	b.bump(func(c *Counters) { c.PublishPasses++ })
	return publish(k.topic, groupID, publishActions(in))
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
