package sync

import (
	"context"
	"testing"
	"time"

	p2ptest "github.com/OffchainLabs/prysm/v7/beacon-chain/p2p/testing"
	"github.com/OffchainLabs/prysm/v7/config/params"
	"github.com/OffchainLabs/prysm/v7/consensus-types/blocks"
	"github.com/OffchainLabs/prysm/v7/testing/require"
)

// TestSlotFractionScalesWithSlotDuration pins the reason the phase delays are basis points
// rather than milliseconds: EIP-8371 requires a bound, and a bound expressed in absolute time
// silently becomes a different fraction of the slot when the slot time changes.
func TestSlotFractionScalesWithSlotDuration(t *testing.T) {
	params.SetupTestConfigCleanup(t)

	cfg := params.BeaconConfig().Copy()
	cfg.SecondsPerSlot = 12
	cfg.SlotDurationMilliseconds = 12000
	params.OverrideBeaconConfig(cfg)

	require.Equal(t, 600*time.Millisecond, slotFraction(rowPhase1MaxJitterBPS))
	require.Equal(t, 3*time.Second, slotFraction(rowPhase2DelayBPS))
	require.Equal(t, 6*time.Second, slotFraction(rowPhase3DelayBPS))

	cfg = params.BeaconConfig().Copy()
	cfg.SecondsPerSlot = 6
	cfg.SlotDurationMilliseconds = 6000
	params.OverrideBeaconConfig(cfg)

	require.Equal(t, 300*time.Millisecond, slotFraction(rowPhase1MaxJitterBPS))
	require.Equal(t, 1500*time.Millisecond, slotFraction(rowPhase2DelayBPS))
	require.Equal(t, 3*time.Second, slotFraction(rowPhase3DelayBPS))
}

// TestPhaseOrderingIsStrict is the property the phases exist for. Phase 1 has to be able to
// finish and have its result propagate before phase 2 starts, or phase 2 duplicates it wholesale
// rather than covering for its absence -- which would relocate the duplication RowDAS removes
// instead of removing it.
func TestPhaseOrderingIsStrict(t *testing.T) {
	params.SetupTestConfigCleanup(t)

	require.Equal(t, true, rowPhase1MaxJitterBPS < rowPhase2DelayBPS,
		"phase 1 must be able to finish before phase 2 starts")
	require.Equal(t, true, rowPhase2DelayBPS < rowPhase3DelayBPS,
		"phase 2 must be able to finish before phase 3 starts")
	require.Equal(t, true, rowPhase3DelayBPS < 10000,
		"every phase must fire within the slot it belongs to")
}

// TestRowReconstructionSchedulerCancels covers the mechanism that turns three phases into
// at-most-one recovery: a row that completes by other means must drop its pending timer.
func TestRowReconstructionSchedulerCancels(t *testing.T) {
	scheduler := newRowReconstructionScheduler()
	key := rowReconstructionKey{groupID: "g", rowIndex: 3}

	// A timer long enough that it cannot fire during the test.
	scheduler.pending[key] = &pendingRecovery{timer: time.AfterFunc(time.Hour, func() {}), phase: 1}

	s := &Service{rowReconstruction: scheduler}
	s.cancelRowReconstruction([]byte("g"), 3)

	require.Equal(t, 0, len(scheduler.pending), "the pending timer should be dropped")
	require.Equal(t, true, scheduler.done[key], "the row should be marked done so a later phase does not re-arm it")
}

// TestRowReconstructionSchedulerIgnoresDoneRows covers the other half: once a row is done, a
// later phase must not schedule it again.
func TestRowReconstructionSchedulerIgnoresDoneRows(t *testing.T) {
	scheduler := newRowReconstructionScheduler()
	scheduler.done[rowReconstructionKey{groupID: "g", rowIndex: 3}] = true

	// A service with no p2p config would panic if scheduling proceeded past the done check,
	// which is what makes this test sensitive rather than vacuous.
	s := &Service{rowReconstruction: scheduler}
	s.scheduleRowReconstruction("/eth2/aaaa/data_row_1/ssz_snappy", []byte("g"), 3)

	require.Equal(t, 0, len(scheduler.pending))
}

// TestEvictGroupClearsSchedulerState covers the leak the repair review found. `done` and
// `servedElsewhere` were documented as "cleared with the group's TTL by the broadcaster", but no
// eviction hook existed, so both grew for the lifetime of the process -- one entry per row of every
// block the node ever saw.
//
// Only the named group goes. A scheduler holds rows of several blocks at once during a
// reorg or a slow slot, and an eviction that took the neighbours with it would re-arm recoveries
// that had already run.
func TestEvictGroupClearsSchedulerState(t *testing.T) {
	scheduler := newRowReconstructionScheduler()

	fired := make(chan struct{}, 1)
	// Long enough that it cannot fire on its own during the test: if the channel ever receives,
	// the timer was not stopped.
	scheduler.pending[rowReconstructionKey{groupID: "gone", rowIndex: 1}] = &pendingRecovery{
		timer: time.AfterFunc(time.Hour, func() { fired <- struct{}{} }),
		phase: 1,
	}
	scheduler.done[rowReconstructionKey{groupID: "gone", rowIndex: 2}] = true
	scheduler.servedElsewhere[rowReconstructionKey{groupID: "gone", rowIndex: 3}] = true

	// A second group, which must survive.
	keptPending := rowReconstructionKey{groupID: "kept", rowIndex: 1}
	keptDone := rowReconstructionKey{groupID: "kept", rowIndex: 2}
	keptServed := rowReconstructionKey{groupID: "kept", rowIndex: 3}
	scheduler.pending[keptPending] = &pendingRecovery{timer: time.AfterFunc(time.Hour, func() {}), phase: 1}
	scheduler.done[keptDone] = true
	scheduler.servedElsewhere[keptServed] = true

	scheduler.evictGroup([]byte("gone"))

	require.Equal(t, 1, len(scheduler.pending))
	require.Equal(t, 1, len(scheduler.done))
	require.Equal(t, 1, len(scheduler.servedElsewhere))
	require.NotNil(t, scheduler.pending[keptPending])
	require.Equal(t, true, scheduler.done[keptDone])
	require.Equal(t, true, scheduler.servedElsewhere[keptServed])

	select {
	case <-fired:
		t.Fatal("the evicted group's timer fired; evictGroup must stop it")
	default:
	}
}

// TestEvictGroupOnAnUnknownGroupIsANoOp: the broadcaster evicts every group it holds, including
// ones this node never scheduled a recovery for.
func TestEvictGroupOnAnUnknownGroupIsANoOp(t *testing.T) {
	scheduler := newRowReconstructionScheduler()
	scheduler.done[rowReconstructionKey{groupID: "g", rowIndex: 1}] = true

	scheduler.evictGroup([]byte("other"))

	require.Equal(t, 1, len(scheduler.done))
}

// TestRowGroupEvictedWithoutSchedulerDoesNotPanic: the callback is reachable on a node with
// --row-das off, where the scheduler is nil.
func TestRowGroupEvictedReachesTheScheduler(t *testing.T) {
	scheduler := newRowReconstructionScheduler()
	scheduler.done[rowReconstructionKey{groupID: "g", rowIndex: 1}] = true

	c := &rowCallbacks{service: &Service{rowReconstruction: scheduler}}
	c.RowGroupEvicted([]byte("g"))

	require.Equal(t, 0, len(scheduler.done))
}

// TestScheduleRowReconstructionWithoutSchedulerIsANoOp covers the disabled case: a node without
// --row-das has a nil scheduler, and the callbacks must not be reachable in a way that panics.
func TestScheduleRowReconstructionWithoutSchedulerIsANoOp(t *testing.T) {
	s := &Service{}
	s.scheduleRowReconstruction("/eth2/aaaa/data_row_1/ssz_snappy", []byte("g"), 3)
	s.cancelRowReconstruction([]byte("g"), 3)
}

// TestPhaseWaitMeasuresFromAcquisition is the D9 rule. EIP-8371 measures every phase delay from
// the moment the node first obtained the block root of a valid block for the slot, so the time a
// node spent reaching recoverability is spent *out of* the phase window, not added to it.
func TestPhaseWaitMeasuresFromAcquisition(t *testing.T) {
	acquired := time.Unix(1700000000, 0)
	offset := 3 * time.Second

	// Recoverable the instant the root arrived: the whole window is left.
	wait, lag := phaseWait(offset, acquired, acquired)
	require.Equal(t, offset, wait)
	require.Equal(t, time.Duration(0), lag)

	// Recoverable a second in: the window is a second shorter, which is the point. Under the old
	// per-node reference this case waited the full 3s and phase 2 drifted with phase 1.
	wait, lag = phaseWait(offset, acquired, acquired.Add(time.Second))
	require.Equal(t, 2*time.Second, wait)
	require.Equal(t, time.Second, lag)

	// Recoverable after the window has passed: fire now. Not a clamp against the impossible --
	// this is the case where phase 1's jitter has stopped desynchronising anything, and the lag
	// is reported so the metric can say so.
	wait, lag = phaseWait(offset, acquired, acquired.Add(10*time.Second))
	require.Equal(t, time.Duration(0), wait)
	require.Equal(t, 10*time.Second, lag)

	// A clock that moved backwards is not a row that arrived before its block.
	wait, lag = phaseWait(offset, acquired, acquired.Add(-time.Second))
	require.Equal(t, offset, wait)
	require.Equal(t, time.Duration(0), lag)
}

// rowGroupID is the Fulu partial-message group id, 0x00 || block_root, which is the form
// PartialDataRow.GroupID() produces. Checked against the production parser rather than assumed,
// because the scheduler's whole duty lookup hangs off being able to read a root back out of it.
func rowGroupID(t *testing.T, root [32]byte) []byte {
	id := append([]byte{0x00}, root[:]...)
	isGloas, _, parsed, err := blocks.ParsePartialColumnGroupID(id)
	require.NoError(t, err)
	require.Equal(t, false, isGloas)
	require.Equal(t, root, parsed)

	return id
}

// TestScheduleRowReconstructionAttachesToOneRootPerSlot is D10, EIP-8371's equivocation cap:
// duties attach to at most one block root per slot, and reconstruction for competing roots is
// OPTIONAL. Without it a proposer publishing k blocks for a slot multiplies every affected
// reconstructor's work by k, because the duty key is the group id and the group id is the root.
func TestScheduleRowReconstructionAttachesToOneRootPerSlot(t *testing.T) {
	params.SetupTestConfigCleanup(t)

	p := p2ptest.NewTestP2P(t)
	s := &Service{
		ctx:               context.Background(),
		cfg:               &config{p2p: p},
		rowReconstruction: newRowReconstructionScheduler(),
		rowDuties:         newRowDutyLedger(),
	}

	attached, competing, unknown := rootOf(1), rootOf(2), rootOf(3)
	require.Equal(t, true, s.rowDuties.attach(64, attached, time.Now()))
	require.Equal(t, false, s.rowDuties.attach(64, competing, time.Now()))

	topic := "/eth2/aaaaaaaa/data_row_1/ssz_snappy"

	// The positive control. Without it the two declines below would pass even if the gate
	// rejected everything -- which is the failure mode that would silently stop RowDAS working.
	// A test node custodies too little to be a row reconstructor, so this is phase 3: a 50%
	// offset against a fresh acquisition, far too long to fire during the test.
	s.scheduleRowReconstruction(topic, rowGroupID(t, attached), 3)
	require.Equal(t, 1, len(s.rowReconstruction.pending), "the attached root should be armed")

	s.scheduleRowReconstruction(topic, rowGroupID(t, competing), 3)
	require.Equal(t, 1, len(s.rowReconstruction.pending), "a competing root for the same slot must not be armed")

	s.scheduleRowReconstruction(topic, rowGroupID(t, unknown), 3)
	require.Equal(t, 1, len(s.rowReconstruction.pending), "a root with no acquisition record must not be armed")

	for _, armed := range s.rowReconstruction.pending {
		armed.timer.Stop()
	}
}

// TestScheduleRowReconstructionPromotesTheHeadBranchRoot is the other half of the cap. Declining
// a competing root is right only while the chain has not selected it; once forkchoice calls it
// canonical, it is the root duties attach to and the work must happen. Without this, an
// equivocating block that merely arrived first would stop the real block of that slot from ever
// being reconstructed.
func TestScheduleRowReconstructionPromotesTheHeadBranchRoot(t *testing.T) {
	params.SetupTestConfigCleanup(t)

	p := p2ptest.NewTestP2P(t)
	s := &Service{
		ctx:               context.Background(),
		cfg:               &config{p2p: p},
		rowReconstruction: newRowReconstructionScheduler(),
		rowDuties:         newRowDutyLedger(),
	}

	firstSeen, selected := rootOf(1), rootOf(2)
	require.Equal(t, true, s.rowDuties.attach(64, firstSeen, time.Now()))
	require.Equal(t, false, s.rowDuties.attach(64, selected, time.Now()))

	// s.cfg.chain is nil, so rootOnHeadBranch answers no and the competing root is declined.
	topic := "/eth2/aaaaaaaa/data_row_1/ssz_snappy"
	s.scheduleRowReconstruction(topic, rowGroupID(t, selected), 3)
	require.Equal(t, 0, len(s.rowReconstruction.pending))

	// Now let forkchoice select it, the way it would once the block is processed. Advance past
	// the recheck interval, because the decline above already paid for one lookup.
	calls := 0
	ref, ok := s.rowDuties.promoteIfHeadBranch(selected, time.Now().Add(2*rowDutyHeadBranchRecheck), alwaysCanonical(&calls))
	require.Equal(t, true, ok)
	require.Equal(t, true, ref.attached)

	s.scheduleRowReconstruction(topic, rowGroupID(t, selected), 3)
	require.Equal(t, 1, len(s.rowReconstruction.pending), "the head-branch root's duties must be armed")

	for _, armed := range s.rowReconstruction.pending {
		armed.timer.Stop()
	}
}

// TestScheduleRowReconstructionArmsFromAcquisition is the D9 fix at the scheduler rather than in
// the arithmetic: the phase offset has to be measured from the block-root acquisition instant the
// ledger holds. Tested through the arm seam because a *time.Timer will not say what it was given,
// and a pure-function test of phaseWait passes just as happily when nothing calls it with the
// right instant -- which is the trap that left settle() dead outside its own tests earlier on
// this branch.
func TestScheduleRowReconstructionArmsFromAcquisition(t *testing.T) {
	params.SetupTestConfigCleanup(t)

	p := p2ptest.NewTestP2P(t)
	s := &Service{
		ctx:               context.Background(),
		cfg:               &config{p2p: p},
		rowReconstruction: newRowReconstructionScheduler(),
		rowDuties:         newRowDutyLedger(),
	}

	var armed []time.Duration
	s.rowReconstruction.arm = func(d time.Duration, _ func()) *time.Timer {
		armed = append(armed, d)
		return time.AfterFunc(time.Hour, func() {})
	}

	// A test node custodies too little to be a row reconstructor, so every row here is phase 3.
	offset := slotFraction(rowPhase3DelayBPS)
	spent := 2 * time.Second

	fresh, stale := rootOf(1), rootOf(2)
	require.Equal(t, true, s.rowDuties.attach(64, fresh, time.Now()))
	require.Equal(t, true, s.rowDuties.attach(65, stale, time.Now().Add(-spent)))

	topic := "/eth2/aaaaaaaa/data_row_1/ssz_snappy"
	s.scheduleRowReconstruction(topic, rowGroupID(t, fresh), 3)
	s.scheduleRowReconstruction(topic, rowGroupID(t, stale), 3)
	require.Equal(t, 2, len(armed))

	// Acquired now: the whole window.
	require.Equal(t, true, armed[0] > offset-time.Second && armed[0] <= offset,
		"a row recoverable at acquisition should wait the full phase offset")

	// Acquired two seconds ago: two seconds less. Measuring from now instead -- the pre-D9
	// behaviour -- would arm this at the full offset and make phase 2 drift per node.
	want := offset - spent
	require.Equal(t, true, armed[1] > want-time.Second && armed[1] <= want,
		"a row that spent time reaching recoverability should spend it out of the phase window")
}

// TestRowServedElsewhereDemotesRatherThanCancels is the whole design of the cancellation signal.
//
// Cancelling outright on a peer's unverified claim would let one peer in the mesh of every duty
// holder stand all of them down and lose the row. Demoting to phase 3 costs a liar the delay from
// its phase to phase 3, and moves the honest case off the part of the slot where reconstruction
// competes with attestation work.
func TestRowServedElsewhereDemotesRatherThanCancels(t *testing.T) {
	params.SetupTestConfigCleanup(t)

	p := p2ptest.NewTestP2P(t)
	s := &Service{
		ctx:               context.Background(),
		cfg:               &config{p2p: p},
		rowReconstruction: newRowReconstructionScheduler(),
		rowDuties:         newRowDutyLedger(),
	}

	var armed []time.Duration
	s.rowReconstruction.arm = func(d time.Duration, _ func()) *time.Timer {
		armed = append(armed, d)
		return time.AfterFunc(time.Hour, func() {})
	}

	root := rootOf(1)
	require.Equal(t, true, s.rowDuties.attach(64, root, time.Now()))
	groupID := rowGroupID(t, root)
	topic := "/eth2/aaaaaaaa/data_row_1/ssz_snappy"

	// A test node custodies too little to be a row reconstructor, so this arms at phase 3 already.
	// Force an urgent phase instead, which is the case the demotion exists for.
	key := rowReconstructionKey{groupID: string(groupID), rowIndex: 3}
	s.rowReconstruction.pending[key] = &pendingRecovery{
		timer: time.AfterFunc(time.Hour, func() {}),
		phase: 1,
		topic: topic,
	}

	s.rowServedElsewhere(groupID, 3)

	require.Equal(t, 1, len(s.rowReconstruction.pending), "the duty must not be dropped")
	require.Equal(t, 3, s.rowReconstruction.pending[key].phase, "it should be the opportunistic phase now")
	require.Equal(t, false, s.rowReconstruction.done[key], "demotion is not completion")
	require.Equal(t, 1, len(armed), "a new timer should have been armed")
	require.Equal(t, true, armed[0] > slotFraction(rowPhase2DelayBPS),
		"the re-armed delay should be the phase-3 offset, not the phase-1 one")

	for _, a := range s.rowReconstruction.pending {
		a.timer.Stop()
	}
}

// TestRowServedElsewhereArmsPhase3ForALaterRow: the observation can arrive before the row is even
// recoverable, so it is remembered rather than acted on once.
func TestRowServedElsewhereArmsPhase3ForALaterRow(t *testing.T) {
	params.SetupTestConfigCleanup(t)

	p := p2ptest.NewTestP2P(t)
	s := &Service{
		ctx:               context.Background(),
		cfg:               &config{p2p: p},
		rowReconstruction: newRowReconstructionScheduler(),
		rowDuties:         newRowDutyLedger(),
	}

	root := rootOf(2)
	require.Equal(t, true, s.rowDuties.attach(64, root, time.Now()))
	groupID := rowGroupID(t, root)

	// Nothing armed yet: the observation is recorded.
	s.rowServedElsewhere(groupID, 5)
	require.Equal(t, 0, len(s.rowReconstruction.pending))

	s.scheduleRowReconstruction("/eth2/aaaaaaaa/data_row_1/ssz_snappy", groupID, 5)
	key := rowReconstructionKey{groupID: string(groupID), rowIndex: 5}
	armed, ok := s.rowReconstruction.pending[key]
	require.Equal(t, true, ok)
	require.Equal(t, 3, armed.phase)
	armed.timer.Stop()
}

// TestRowServedElsewhereLeavesADoneRowAlone: a row already recovered or completed has no pending
// work to demote, and marking it must not resurrect one.
func TestRowServedElsewhereLeavesADoneRowAlone(t *testing.T) {
	params.SetupTestConfigCleanup(t)

	s := &Service{
		ctx:               context.Background(),
		cfg:               &config{p2p: p2ptest.NewTestP2P(t)},
		rowReconstruction: newRowReconstructionScheduler(),
		rowDuties:         newRowDutyLedger(),
	}
	root := rootOf(3)
	require.Equal(t, true, s.rowDuties.attach(64, root, time.Now()))
	groupID := rowGroupID(t, root)
	key := rowReconstructionKey{groupID: string(groupID), rowIndex: 1}
	s.rowReconstruction.done[key] = true

	s.rowServedElsewhere(groupID, 1)
	require.Equal(t, 0, len(s.rowReconstruction.pending))
}

// TestScheduleRowReconstructionWithoutDutyLedgerIsANoOp: the ledger and the scheduler are
// allocated together, so a scheduler without a ledger is a bug -- and it must not be one that
// reconstructs from an unknown reference instant.
func TestScheduleRowReconstructionWithoutDutyLedgerIsANoOp(t *testing.T) {
	s := &Service{rowReconstruction: newRowReconstructionScheduler()}
	s.scheduleRowReconstruction("/eth2/aaaa/data_row_1/ssz_snappy", []byte("g"), 3)

	require.Equal(t, 0, len(s.rowReconstruction.pending))
}
