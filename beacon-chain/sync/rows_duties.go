package sync

// Which block root a slot's RowDAS reconstruction duties attach to, and when this node first
// obtained it. Two EIP-8371 rules need this and neither can be answered from a group id alone.
//
// D10, the equivocation cap (eip-8371.md line 102):
//
//	"Reconstruction obligations attach to at most one block root per slot: the block root on the
//	node's current head branch, or the first valid block root seen. For any additional
//	(equivocating or competing) block roots of the same slot, reconstruction is OPTIONAL, so that
//	equivocation cannot amplify reconstruction work."
//
// Without this, duties are keyed by group id -- i.e. by block root -- so a proposer publishing k
// blocks for one slot multiplies every affected reconstructor's work by k, for the price of k
// blocks. At 161.9 ms a recovery that is a cheap amplification vector against exactly the CPU
// that is already duplicated lambda-fold.
//
// D9, the reference instant:
//
//	"All delays are measured from the moment the node first obtains the block root of a valid
//	block for the slot."
//
// Not from the moment the row becomes recoverable, which is where the timers used to start. The
// difference matters because it is what makes phase 2 a *shared* fallback: under the EIP's
// reference every node's phase 2 fires at the same offset from the block, so it covers for phase
// 1 having failed. Under per-node recoverability a node slow to reach the threshold delays its
// own phase 2 too, and the fallback drifts with the thing it is supposed to back up.
//
// It also cuts the other way, which is the honest half of the fix: the time spent reaching the
// threshold is now consumed from the phase window rather than added to it. R3 puts convergence at
// ~250 ms against phase 1's 600 ms, so the real jitter room is nearer 350 ms --
// rowReconstructionSchedulingLag measures exactly this, and it is the input R7 needs.
//
// The two clauses of the cap are evaluated at different moments, and that is deliberate. Roots
// are recorded as they are obtained, where the only applicable clause is "first valid block root
// seen": gossip validation runs *before* block processing, so a root is not in forkchoice yet and
// asking whether it is on the head branch there would always answer no. The head-branch clause is
// therefore evaluated lazily, when a duty for a competing root would actually be declined -- by
// then the block has had its chance to be processed, the answer is meaningful, and the lookup is
// rate-limited per root rather than paid per message.

import (
	"fmt"
	"sync"
	"time"

	"github.com/OffchainLabs/prysm/v7/beacon-chain/p2p/partialdatacolumnbroadcaster"
	"github.com/OffchainLabs/prysm/v7/consensus-types/primitives"
	"github.com/sirupsen/logrus"
)

// rowDutyRetentionSlots is how far back the ledger remembers. It has to outlive the broadcaster's
// group TTL, or a row still alive in the broadcaster could lose the acquisition record its phase
// delays are measured from.
const rowDutyRetentionSlots = primitives.Slot(partialdatacolumnbroadcaster.TTLInSlots + 1)

// rowDutyHeadBranchRecheck is how often the head-branch verdict for one competing root may be
// re-asked. Short enough that a root becoming canonical is noticed within a fraction of the
// phase-1 window, long enough that k equivocating blocks times 128 rows cannot turn into a
// forkchoice query per row.
const rowDutyHeadBranchRecheck = 200 * time.Millisecond

// rowDutyRoot is one block root as this node saw it.
type rowDutyRoot struct {
	// acquired is the first time this node obtained this root from a path that had established
	// the block was valid. Never moved afterwards: it is the reference instant, and every later
	// path reporting the same root -- the column header, the row header, the block itself --
	// would otherwise push it forward.
	acquired time.Time
	// ruledAt is when the head-branch lookup was last paid for this root. It rate-limits the
	// lookup so an equivocating proposer cannot turn 128 rows into 128 forkchoice queries,
	// without making the verdict permanent -- a root can become canonical after the first row
	// of it was declined, and if the answer were cached forever the canonical block of that slot
	// would never be reconstructed at all.
	ruledAt time.Time
}

// rowDutySlot is one slot's attachment: the single root duties attach to, and every root seen for
// the slot. Competing roots are kept rather than dropped so that the scheduler can tell "declined"
// from "never heard of", which are different bugs.
type rowDutySlot struct {
	attached [32]byte
	roots    map[[32]byte]*rowDutyRoot
}

// rowDutyLedger records the attachment for the recent slots.
type rowDutyLedger struct {
	mu      sync.Mutex
	bySlot  map[primitives.Slot]*rowDutySlot
	slotOf  map[[32]byte]primitives.Slot
	highest primitives.Slot
}

func newRowDutyLedger() *rowDutyLedger {
	return &rowDutyLedger{
		bySlot: make(map[primitives.Slot]*rowDutySlot),
		slotOf: make(map[[32]byte]primitives.Slot),
	}
}

// rowDutyRef is what a root's reconstruction duties are measured against.
type rowDutyRef struct {
	slot     primitives.Slot
	acquired time.Time
	// attached reports whether this slot's duties attach to this root. False means the root is
	// known but competing, and reconstruction for it is OPTIONAL -- which this node declines.
	attached bool
}

// attach records that this node has obtained root as the root of a valid block for slot, and
// reports whether the slot's duties attach to it.
//
// First valid root seen wins. The head-branch clause is not evaluated here; see the file comment
// for why it cannot be, and promoteIfHeadBranch for where it is.
func (l *rowDutyLedger) attach(slot primitives.Slot, root [32]byte, now time.Time) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	if slot > l.highest {
		l.highest = slot
		l.prune()
	}

	entry, ok := l.bySlot[slot]
	if !ok {
		l.bySlot[slot] = &rowDutySlot{
			attached: root,
			roots:    map[[32]byte]*rowDutyRoot{root: {acquired: now}},
		}
		l.slotOf[root] = slot

		return true
	}
	if _, seen := entry.roots[root]; !seen {
		entry.roots[root] = &rowDutyRoot{acquired: now}
		l.slotOf[root] = slot
	}

	return entry.attached == root
}

// reference reports what a root's duties are measured against. ok=false means the root is
// unknown to this node -- no path has reported a valid block for it -- which is not a state the
// node path should reach, because row state is only built from a validated header.
func (l *rowDutyLedger) reference(root [32]byte) (rowDutyRef, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()

	return l.refLocked(root)
}

func (l *rowDutyLedger) refLocked(root [32]byte) (rowDutyRef, bool) {
	slot, ok := l.slotOf[root]
	if !ok {
		return rowDutyRef{}, false
	}
	entry, ok := l.bySlot[slot]
	if !ok {
		return rowDutyRef{}, false
	}
	record, ok := entry.roots[root]
	if !ok {
		return rowDutyRef{}, false
	}

	return rowDutyRef{
		slot:     slot,
		acquired: record.acquired,
		attached: entry.attached == root,
	}, true
}

// promoteIfHeadBranch is the head-branch clause: a competing root that the chain has actually
// selected takes the attachment from the first-seen one. Called when a duty for a competing root
// is about to be declined, which is both the moment the answer matters and late enough for
// forkchoice to have one.
//
// The lookup is rate-limited per root rather than cached, because a root can become canonical
// after the first row of it was declined. Two-phase on purpose: onHeadBranch reaches into
// forkchoice, and holding this ledger's mutex across another subsystem is how deadlocks are
// built.
func (l *rowDutyLedger) promoteIfHeadBranch(root [32]byte, now time.Time, onHeadBranch func([32]byte) bool) (rowDutyRef, bool) {
	l.mu.Lock()
	ref, ok := l.refLocked(root)
	if !ok || ref.attached {
		l.mu.Unlock()
		return ref, ok
	}
	record := l.bySlot[ref.slot].roots[root]
	if !record.ruledAt.IsZero() && now.Sub(record.ruledAt) < rowDutyHeadBranchRecheck {
		l.mu.Unlock()
		return ref, true
	}
	record.ruledAt = now
	l.mu.Unlock()

	if onHeadBranch == nil || !onHeadBranch(root) {
		return ref, true
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	// Re-read: the slot may have been pruned while we were asking.
	if entry, ok := l.bySlot[ref.slot]; ok {
		if _, known := entry.roots[root]; known {
			entry.attached = root
		}
	}

	return l.refLocked(root)
}

// prune drops slots too old to carry a live row. Called with the lock held, and only when the
// highest slot moves, so it costs nothing per message.
func (l *rowDutyLedger) prune() {
	if l.highest < rowDutyRetentionSlots {
		return
	}
	horizon := l.highest - rowDutyRetentionSlots
	for slot, entry := range l.bySlot {
		if slot > horizon {
			continue
		}
		for root := range entry.roots {
			delete(l.slotOf, root)
		}
		delete(l.bySlot, slot)
	}
}

// noteRowDutyRoot records a block root this node has obtained for a slot, from any path that has
// established the block is valid: a row header, a column header, or block gossip. A no-op unless
// RowDAS is on, so the shipped paths that call it are unaffected.
func (s *Service) noteRowDutyRoot(slot primitives.Slot, root [32]byte) {
	if s.rowDuties == nil {
		return
	}

	if s.rowDuties.attach(slot, root, time.Now()) {
		return
	}

	// A second root for a slot that already has one. Under EIP-8371 reconstruction for it is
	// OPTIONAL, and unless forkchoice later prefers it this node declines -- which is the whole
	// point: the counter is the measure of how much work equivocation would otherwise have
	// bought.
	rowDutyRootsUnattachedTotal.Inc()
	log.WithFields(logrus.Fields{
		"slot": slot,
		"root": fmt.Sprintf("%#x", root),
	}).Debug("Row reconstruction duties already attached to another root for this slot")
}

// rootOnHeadBranch reports whether a root is on the node's current head branch, which is
// EIP-8371's first-choice rule for which root a slot's duties attach to. A root at slot N is on
// the head branch exactly when forkchoice calls it canonical.
func (s *Service) rootOnHeadBranch(root [32]byte) bool {
	if s.cfg == nil || s.cfg.chain == nil {
		return false
	}
	canonical, err := s.cfg.chain.IsCanonical(s.ctx, root)
	if err != nil {
		log.WithError(err).Debug("Could not determine whether a competing root is on the head branch")
		return false
	}

	return canonical
}
