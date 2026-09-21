package sync

// RowDAS (EIP-8371) reconstruction duties.
//
// The whole point of RowDAS is that a row is recovered once, by a node whose row subnet carries
// it, instead of by every reconstructor independently. What makes that work is not the topic --
// it is the delays. Without them every reconstructor on a subnet starts the same recovery at the
// same instant and the duplication RowDAS exists to remove is simply relocated.
//
// Three phases, all timed from the moment this node first obtained the block root of a valid
// block for the slot -- not from the moment the row became recoverable -- and all cancelled if
// the row completes by other means. The reference instant is what makes phase 2 a shared
// fallback rather than a per-node drift; see rows_duties.go, which also holds the EIP quote:
//
//	phase 1  the row mapped to *my* row subnet. A row reconstructor MUST do this, and can from
//	         its own custody alone. Short jitter, only to desynchronise peers.
//	phase 2  any other row that has become recoverable. Supernodes SHOULD, other reconstructors
//	         MAY. Longer, so that cells from getBlobs, columns and rows have time to arrive and
//	         make the work unnecessary.
//	phase 3  anything still incomplete. Any row-subnet subscriber SHOULD. Longest; expected to
//	         fire almost never, which is what makes it free.
//
// Every delay is bounded. EIP-8371 requires that and leaves the bound TBD; the values below are
// fractions of a slot rather than absolute durations, so they survive a slot-time change.

import (
	"sync"
	"time"

	"github.com/OffchainLabs/prysm/v7/beacon-chain/core/peerdas"
	"github.com/OffchainLabs/prysm/v7/config/params"
	"github.com/OffchainLabs/prysm/v7/consensus-types/blocks"
	"github.com/sirupsen/logrus"
)

// Phase delays as basis points of a slot, so they scale with SECONDS_PER_SLOT.
//
// Phase 1's jitter has to exceed one network round trip or it does not desynchronise anything;
// phase 2 has to exceed the time for a phase-1 result to propagate as a bitmap update, or it
// duplicates phase 1 wholesale rather than covering for it. These are first estimates, and R7 is
// the experiment that should replace them with measured values.
const (
	rowPhase1MaxJitterBPS = 500  // 5% of a slot
	rowPhase2DelayBPS     = 2500 // 25%
	rowPhase3DelayBPS     = 5000 // 50%

	// rowPullDelayBPS is when the optional pull arm asks non-custodied column subnets for a row's
	// missing cells. Between phase 2 and phase 3: late enough that the row's own subnet has had
	// its chance -- pulling before that competes with the cheap source -- and early enough that
	// the cells can still arrive before phase 3 would recover the row anyway.
	rowPullDelayBPS = 3000 // 30%
)

// rowReconstructionKey identifies a pending recovery. A row is (block, blob index); the topic is
// not part of the identity, because the same row reached through a different topic is the same
// work.
type rowReconstructionKey struct {
	groupID  string
	rowIndex uint64
}

// rowReconstructionScheduler tracks pending row recoveries so they can be cancelled when the row
// completes by other means -- which is the mechanism that turns three phases into
// at-most-one-recovery rather than three.
// pendingRecovery is an armed recovery: the timer, the phase whose duty it is, and the topic it
// will publish back to. The phase is kept because an observation that the row is served elsewhere
// demotes an urgent recovery to the opportunistic phase rather than dropping it.
type pendingRecovery struct {
	timer *time.Timer
	phase int
	topic string
}

type rowReconstructionScheduler struct {
	mu      sync.Mutex
	pending map[rowReconstructionKey]*pendingRecovery
	// done records rows that have been recovered or completed, so a later phase does not
	// re-schedule work that is finished. Bounded by the group TTL: the broadcaster calls
	// RowGroupEvicted when it drops a group, and evictGroup clears this along with the rest.
	//
	// That hook did not exist when this comment first claimed the bound, and the map grew for the
	// process's lifetime. Keep the claim and the mechanism together.
	done map[rowReconstructionKey]bool
	// servedElsewhere records rows a peer has claimed in full while we wanted nothing further.
	// Kept rather than acted on once, so a row that becomes recoverable *after* the observation
	// is armed at the opportunistic phase straight away.
	servedElsewhere map[rowReconstructionKey]bool
	// arm creates the phase timer. A seam rather than a call to time.AfterFunc directly, because
	// the delay is otherwise unobservable -- a *time.Timer does not report it -- and a delay
	// computed from the wrong instant is precisely the bug the block-root reference fixed. A
	// test that cannot see the delay cannot tell the fix from its absence.
	arm func(time.Duration, func()) *time.Timer
}

func newRowReconstructionScheduler() *rowReconstructionScheduler {
	return &rowReconstructionScheduler{
		pending:         make(map[rowReconstructionKey]*pendingRecovery),
		done:            make(map[rowReconstructionKey]bool),
		servedElsewhere: make(map[rowReconstructionKey]bool),
		arm:             time.AfterFunc,
	}
}

// slotFraction converts basis points of a slot into a duration.
func slotFraction(bps uint64) time.Duration {
	return time.Duration(uint64(params.BeaconConfig().SlotDuration()) * bps / 10000)
}

// scheduleRowReconstruction arms the recovery for a row that has just become recoverable.
//
// Which phase applies depends on whether this is the row our own subnet carries and on how much
// custody we hold. A node that is not a row reconstructor still schedules, at phase 3: it got
// here by pooling cells over the row topic, which is exactly the case EIP-8371 keeps in reserve
// for when the designated reconstructor is absent.
//
// *When* it fires is measured from block-root acquisition rather than from now, and rows whose
// root is not the one this slot's duties attach to are not armed at all. Both are EIP-8371 rules
// and both need the duty ledger; see rows_duties.go.
func (s *Service) scheduleRowReconstruction(topic string, groupID []byte, rowIndex uint64) {
	if s.rowReconstruction == nil || s.rowDuties == nil {
		return
	}

	key := rowReconstructionKey{groupID: string(groupID), rowIndex: rowIndex}

	// Cheap rejections first. Deciding the phase costs a custody lookup, and a row that is
	// already done or already armed does not need one.
	if s.rowReconstructionSettled(key) {
		return
	}

	ref, root, ok := s.rowDutyReference(groupID)
	if !ok {
		// No acquisition record for this root, so there is no instant to measure delays from and
		// no slot to test the attachment against. Row state is only ever built from a validated
		// header, and every path that validates one records the root, so this should not happen
		// -- the counter is there to say so if it does, rather than silently not reconstructing.
		rowReconstructionsSkippedTotal.WithLabelValues("unknown_root").Inc()
		log.WithField("rowIndex", rowIndex).Debug("No block-root acquisition record for a recoverable row")
		return
	}
	if !ref.attached {
		// Duties attach to at most one block root per slot, and this is not the root they attach
		// to -- unless the chain has since selected it, which is the one question worth a
		// forkchoice lookup here and could not be answered when the root was recorded.
		ref, ok = s.rowDuties.promoteIfHeadBranch(root, time.Now(), s.rootOnHeadBranch)
		if !ok || !ref.attached {
			// Reconstruction for a competing root is OPTIONAL and we decline. Otherwise an
			// equivocating proposer multiplies every reconstructor's work by the number of
			// blocks it published, for the price of publishing them.
			rowReconstructionsSkippedTotal.WithLabelValues("not_attached").Inc()
			return
		}
	}

	// Deliberately outside the lock: choosing a phase reaches into peerdas and the p2p service,
	// and holding the scheduler's mutex across another subsystem is how deadlocks are built.
	phase, offset := s.rowReconstructionPhase(rowIndex)
	if s.rowServedElsewhereSeen(key) {
		// A peer already claimed this row in full while we wanted nothing further. Demote to the
		// opportunistic phase rather than skipping: the claim is unverified, and phase 3 is what
		// makes a false one cost delay instead of the row.
		phase, offset = 3, slotFraction(rowPhase3DelayBPS)
	}

	delay, lag := phaseWait(offset, ref.acquired, time.Now())
	rowReconstructionSchedulingLag.WithLabelValues(phaseLabel(phase)).Observe(lag.Seconds())

	s.rowReconstruction.mu.Lock()
	defer s.rowReconstruction.mu.Unlock()
	// Re-check under the lock: another phase may have armed this row while we were deciding.
	if s.rowReconstruction.done[key] {
		return
	}
	if _, already := s.rowReconstruction.pending[key]; already {
		// An earlier phase already has this row. Never replace it: the earliest phase that
		// applies is the one whose duty it is, and re-arming would only push the work later.
		return
	}

	log.WithFields(logrus.Fields{
		"rowIndex": rowIndex,
		"slot":     ref.slot,
		"phase":    phase,
		"offset":   offset,
		"lag":      lag,
		"delay":    delay,
		"topic":    topic,
	}).Debug("Scheduling row reconstruction")

	s.rowReconstruction.pending[key] = &pendingRecovery{
		timer: s.rowReconstruction.arm(delay, func() {
			s.reconstructRow(topic, groupID, rowIndex, phase)
		}),
		phase: phase,
		topic: topic,
	}
}

// rowServedElsewhereSeen reports whether a peer has already been observed holding this whole row.
func (s *Service) rowServedElsewhereSeen(key rowReconstructionKey) bool {
	s.rowReconstruction.mu.Lock()
	defer s.rowReconstruction.mu.Unlock()

	return s.rowReconstruction.servedElsewhere[key]
}

// rowServedElsewhere is the cancellation signal the phases were missing: a peer holds the whole
// row and we want nothing further from it.
//
// It demotes an armed phase-1 or phase-2 recovery to phase 3 rather than cancelling it. That is
// the whole design, and the reason is that the claim is unverified: cancelling outright would let
// one peer in the mesh of every duty holder stand all of them down and lose the row. Demoting
// costs a liar only the delay from its phase to phase 3, and moves the honest case off the part of
// the slot where reconstruction competes with attestation work.
//
// What it does *not* yet do is decide at phase 3 whether the work is still needed. The signal for
// that is on the wire too -- peers still requesting cells of this row -- and it is the next step.
func (s *Service) rowServedElsewhere(groupID []byte, rowIndex uint64) {
	if s.rowReconstruction == nil || s.rowDuties == nil {
		return
	}

	key := rowReconstructionKey{groupID: string(groupID), rowIndex: rowIndex}
	ref, _, ok := s.rowDutyReference(groupID)

	s.rowReconstruction.mu.Lock()
	defer s.rowReconstruction.mu.Unlock()

	s.rowReconstruction.servedElsewhere[key] = true
	if s.rowReconstruction.done[key] {
		return
	}
	armed, pending := s.rowReconstruction.pending[key]
	if !pending || armed.phase >= 3 {
		return
	}
	if !ok {
		// No reference instant to measure phase 3 from. Leave the armed recovery alone rather
		// than guessing: doing the work early is a cost, doing it never is a loss.
		return
	}

	armed.timer.Stop()
	delay, _ := phaseWait(slotFraction(rowPhase3DelayBPS), ref.acquired, time.Now())
	log.WithFields(logrus.Fields{
		"rowIndex":  rowIndex,
		"fromPhase": armed.phase,
		"delay":     delay,
	}).Debug("A peer holds this row; demoting the recovery to phase 3")
	rowReconstructionsDemotedTotal.WithLabelValues(phaseLabel(armed.phase)).Inc()

	topic := armed.topic
	s.rowReconstruction.pending[key] = &pendingRecovery{
		timer: s.rowReconstruction.arm(delay, func() {
			s.reconstructRow(topic, groupID, rowIndex, 3)
		}),
		phase: 3,
		topic: topic,
	}
}

// phaseWait turns a phase offset -- which EIP-8371 measures from block-root acquisition -- into
// a wait from now, and reports how much of the window reaching recoverability already spent.
//
// A row that took longer than its phase offset to become recoverable waits zero, which is
// correct rather than a clamp against something impossible: the window it was waiting out has
// passed. That is also the case worth watching, because a phase 1 that always fires immediately
// has lost the jitter that desynchronises peers on the same row subnet.
func phaseWait(offset time.Duration, acquired, now time.Time) (wait, lag time.Duration) {
	lag = now.Sub(acquired)
	if lag < 0 {
		// Acquired in the future: a clock that moved, not a row that arrived early.
		lag = 0
	}
	wait = offset - lag
	if wait < 0 {
		wait = 0
	}

	return wait, lag
}

// rowDutyReference looks up what a row's phase delays are measured against, from the group id the
// broadcaster identifies the row by, and returns the root it read out of it. Row group ids are
// the Fulu form, 0x00 || block_root, which is the only form that carries a root the ledger can be
// keyed by.
func (s *Service) rowDutyReference(groupID []byte) (rowDutyRef, [32]byte, bool) {
	isGloas, _, root, err := blocks.ParsePartialColumnGroupID(groupID)
	if err != nil || isGloas {
		return rowDutyRef{}, root, false
	}
	ref, ok := s.rowDuties.reference(root)

	return ref, root, ok
}

// rowReconstructionSettled reports whether a row is already done or already armed.
func (s *Service) rowReconstructionSettled(key rowReconstructionKey) bool {
	s.rowReconstruction.mu.Lock()
	defer s.rowReconstruction.mu.Unlock()
	if s.rowReconstruction.done[key] {
		return true
	}
	_, pending := s.rowReconstruction.pending[key]

	return pending
}

// rowReconstructionPhase decides which duty applies to a row, and the offset from block-root
// acquisition at which it should fire. The caller turns that offset into a wait.
func (s *Service) rowReconstructionPhase(rowIndex uint64) (phase int, offset time.Duration) {
	custodyColumns, err := s.custodiedColumnCount()
	if err != nil {
		log.WithError(err).Debug("Could not determine custody for row reconstruction phase")
		return 3, slotFraction(rowPhase3DelayBPS)
	}

	if !peerdas.IsRowReconstructor(custodyColumns) {
		// Not enough custody to recover anything alone. We are here only because the row topic
		// pooled enough cells, which is phase 3's case.
		return 3, slotFraction(rowPhase3DelayBPS)
	}

	if s.isOwnRowSubnetRow(rowIndex) {
		// Phase 1: our duty. Jitter only, so peers on the same subnet do not all start at once.
		jitter := time.Duration(s.reconstructionRandGen.Int63n(int64(slotFraction(rowPhase1MaxJitterBPS)) + 1))
		return 1, jitter
	}

	return 2, slotFraction(rowPhase2DelayBPS)
}

// isOwnRowSubnetRow reports whether a row is the one this node's row subnet carries.
func (s *Service) isOwnRowSubnetRow(rowIndex uint64) bool {
	subnet, err := peerdas.RowSubnetForNode(s.cfg.p2p.NodeID())
	if err != nil {
		return false
	}
	rowSubnet, err := peerdas.RowSubnetForBlob(rowIndex, s.cfg.clock.CurrentSlot())
	if err != nil {
		return false
	}

	return subnet == rowSubnet
}

// custodiedColumnCount is how many columns this node custodies, which is what decides whether it
// is a row reconstructor.
func (s *Service) custodiedColumnCount() (uint64, error) {
	samplingSize, err := s.samplingSize()
	if err != nil {
		return 0, err
	}
	info, _, err := peerdas.Info(s.cfg.p2p.NodeID(), samplingSize)
	if err != nil {
		return 0, err
	}

	return uint64(len(info.CustodyColumns)), nil
}

// cancelRowReconstruction drops a pending recovery, because the row completed by other means.
// This is what makes the phases cheap: a later phase that would have duplicated an earlier one's
// work never runs.
func (s *Service) cancelRowReconstruction(groupID []byte, rowIndex uint64) {
	if s.rowReconstruction == nil {
		return
	}

	key := rowReconstructionKey{groupID: string(groupID), rowIndex: rowIndex}

	s.rowReconstruction.mu.Lock()
	defer s.rowReconstruction.mu.Unlock()
	s.rowReconstruction.done[key] = true
	if armed, ok := s.rowReconstruction.pending[key]; ok {
		armed.timer.Stop()
		delete(s.rowReconstruction.pending, key)
		rowReconstructionsCancelledTotal.Inc()
	}
}

// reconstructRow performs the recovery, having waited out its phase delay.
func (s *Service) reconstructRow(topic string, groupID []byte, rowIndex uint64, phase int) {
	key := rowReconstructionKey{groupID: string(groupID), rowIndex: rowIndex}

	s.rowReconstruction.mu.Lock()
	delete(s.rowReconstruction.pending, key)
	alreadyDone := s.rowReconstruction.done[key]
	s.rowReconstruction.mu.Unlock()
	if alreadyDone {
		return
	}

	broadcaster := s.cfg.p2p.PartialColumnBroadcaster()
	if broadcaster == nil {
		return
	}

	// Take the row as it stands now, not as it stood when the phase was armed. Waiting out a
	// delay is only useful if the state is re-read afterwards -- cells that arrived in the
	// meantime are the whole reason for waiting.
	row, err := broadcaster.RowSnapshot(s.ctx, topic, groupID)
	if err != nil {
		log.WithError(err).Debug("Could not snapshot row for reconstruction")
		return
	}
	if row == nil {
		return
	}
	if row.IsComplete() {
		s.markRowDone(key)
		return
	}
	if !row.ReconstructionThresholdMet() {
		// The row went backwards relative to when it was armed, which can only mean the group
		// was evicted and rebuilt. Nothing to do.
		return
	}

	start := time.Now()
	if err := peerdas.RecoverRow(row); err != nil {
		log.WithError(err).WithField("rowIndex", rowIndex).Error("Failed to recover row")
		return
	}
	s.markRowDone(key)

	rowReconstructionsTotal.WithLabelValues(phaseLabel(phase)).Inc()
	rowReconstructionDuration.Observe(time.Since(start).Seconds())
	log.WithFields(logrus.Fields{
		"rowIndex": rowIndex,
		"phase":    phase,
		"took":     time.Since(start),
	}).Debug("Recovered row")

	// Offer the recovered row back to the topic. Every cell of it is now available, which is
	// what lets the rest of the subnet stop asking -- and what the cross-fill bridge turns into
	// cells for the columns we custody.
	if err := broadcaster.PublishRow(s.ctx, topic, *row); err != nil {
		log.WithError(err).Debug("Could not publish recovered row")
	}

	// Then push it out to the columns we do *not* custody. Serving the row helps the row subnet
	// and our own columns; this is the step EIP-8371 expects to turn one node's reconstruction
	// into relief for the other ~124 column subnets.
	s.crossForwardRecoveredRow(topic, row)
}

// evictGroup drops every trace of a group, called when the broadcaster evicts it on TTL.
//
// All three maps are keyed by (groupID, rowIndex), so eviction is a scan of each for the group's
// prefix rather than a single delete. The maps hold one entry per row of one block, which is at
// most the blob count -- a scan is cheaper than maintaining a second index to avoid it.
//
// Armed timers are stopped, not left to fire. A recovery for an evicted group would find no row to
// snapshot and return, so firing is harmless, but stopping it frees the timer immediately and keeps
// "pending is what is armed" true.
func (s *rowReconstructionScheduler) evictGroup(groupID []byte) {
	group := string(groupID)

	s.mu.Lock()
	defer s.mu.Unlock()

	for key, recovery := range s.pending {
		if key.groupID == group {
			recovery.timer.Stop()
			delete(s.pending, key)
		}
	}
	for key := range s.done {
		if key.groupID == group {
			delete(s.done, key)
		}
	}
	for key := range s.servedElsewhere {
		if key.groupID == group {
			delete(s.servedElsewhere, key)
		}
	}
}

func (s *Service) markRowDone(key rowReconstructionKey) {
	s.rowReconstruction.mu.Lock()
	s.rowReconstruction.done[key] = true
	s.rowReconstruction.mu.Unlock()
}

func phaseLabel(phase int) string {
	switch phase {
	case 1:
		return "phase1"
	case 2:
		return "phase2"
	default:
		return "phase3"
	}
}
