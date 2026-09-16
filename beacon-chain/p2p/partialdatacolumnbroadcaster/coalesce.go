package partialdatacolumnbroadcaster

import (
	"time"

	"github.com/OffchainLabs/prysm/v7/consensus-types/blocks"
)

// Row-side timers: the claim / held-news wake-up.
//
// This file once also held a per-group publish throttle -- a leading-edge window on *when a row
// publish ran*, which folded rapid availability growth into one announcement per window. It was
// the first thing to take the metadata storm apart (68,671 -> 12,467 generated actions), and it
// was the wrong shape: it throttled the publish, so a peer's request arriving inside the window
// waited for the window's end to be answered with cells. Announcements are now held *per peer*
// in the publish decision itself (consensus-types/blocks/partialcells.go): a publish is never
// delayed, it simply produces no packet for a peer whose only news is being held, and the held
// news rides the next packet going there or is flushed at its deadline. The deadline reaches this
// file through EarliestClaimDeadline, so one timer per group serves claims and held news alike.

// coalesceKey identifies a row group on a topic.
type coalesceKey struct {
	topic   string
	groupID string
}

// Waking up when a request claim lapses.
//
// A claim parks a part with one peer for a TTL so nobody else is asked for it. The lapse was only
// ever *noticed* at the next publish, so on a quiet mesh a part asked of a peer that never answers
// could stall indefinitely, and even on a busy one the reassignment granularity was whatever traffic
// happened to arrive. This arms a timer at the earliest live deadline and publishes then: the publish
// recomputes the assignment, which reaps the lapsed claim and offers the part to another holder.
//
// Self-limiting: if the part arrived meanwhile, or the reassignment changes nothing this peer needs
// to hear, `partialForPeer` produces no metadata and the extension emits no packet. The cost of a
// spurious wake is one event-loop iteration.

// minClaimWake is the shortest interval between two wake-ups for one group.
const minClaimWake = 10 * time.Millisecond

// armRowClaimWake ensures a wake-up is armed at the row's earliest claim deadline. Runs on the event
// loop.
func (p *PartialColumnBroadcaster) armRowClaimWake(topic string, groupID []byte, row *blocks.PartialDataRow) {
	if row == nil {
		return
	}
	deadline, ok := row.EarliestClaimDeadline()
	if !ok {
		return
	}

	key := coalesceKey{topic: topic, groupID: string(groupID)}
	if _, armed := p.rowClaimWake[key]; armed {
		// One timer per group. Re-arming on every publish would mean a timer per publish, and the
		// armed one fires no later than needed: deadlines are `now + ttl`, so a claim committed
		// later cannot lapse before one committed earlier unless their TTLs differ sharply -- and
		// waking early is harmless, because the publish is self-limiting.
		return
	}

	wait := deadline.Sub(p.now())
	if wait < minClaimWake {
		// A floor, not a delay: a deadline that is already due wakes at once, but a deadline that
		// somehow never clears -- a dropped flush to a peer that keeps dropping -- must not turn
		// into a zero-interval loop on the event loop.
		wait = minClaimWake
	}
	p.rowClaimWake[key] = p.armCoalesce(wait, func() {
		select {
		case p.claimWake <- key:
		case <-p.ctx.Done():
		}
	})
}

// handleClaimWake runs on the event loop when a claim deadline has passed.
func (p *PartialColumnBroadcaster) handleClaimWake(key coalesceKey) {
	delete(p.rowClaimWake, key)

	entry := p.getRowEntry(key.topic, []byte(key.groupID))
	if entry == nil || !entry.row.Published {
		return
	}
	// The publish path arms the next wake-up itself, from whatever the recomputed assignment leaves
	// outstanding, so there is nothing to re-arm here.
	if err := p.publishPartialRow(key.topic, entry.row.GroupID(), entry.row); err != nil {
		p.logger.WithError(err).WithField("topic", key.topic).
			Debug("Failed to publish on request-claim wake-up")
	}
}

// dropRowClaimWake cancels and forgets a group's wake-up, for eviction.
func (p *PartialColumnBroadcaster) dropRowClaimWake(groupID []byte) {
	for key, timer := range p.rowClaimWake {
		if key.groupID != string(groupID) {
			continue
		}
		if timer != nil {
			timer.Stop()
		}
		delete(p.rowClaimWake, key)
	}
}
