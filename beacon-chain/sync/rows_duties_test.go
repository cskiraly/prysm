package sync

import (
	"testing"
	"time"

	mock "github.com/OffchainLabs/prysm/v7/beacon-chain/blockchain/testing"
	dbtest "github.com/OffchainLabs/prysm/v7/beacon-chain/db/testing"
	"github.com/OffchainLabs/prysm/v7/beacon-chain/verification"
	"github.com/OffchainLabs/prysm/v7/consensus-types/primitives"
	ethpb "github.com/OffchainLabs/prysm/v7/proto/prysm/v1alpha1"
	"github.com/OffchainLabs/prysm/v7/testing/require"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/pkg/errors"
)

func rootOf(b byte) [32]byte {
	var root [32]byte
	root[0] = b
	return root
}

// neverCanonical and alwaysCanonical stand in for the forkchoice lookup. Counting calls is part
// of the point: an equivocating proposer must not be able to turn 128 rows into 128 queries.
func neverCanonical(calls *int) func([32]byte) bool {
	return func([32]byte) bool {
		*calls++
		return false
	}
}

func alwaysCanonical(calls *int) func([32]byte) bool {
	return func([32]byte) bool {
		*calls++
		return true
	}
}

// TestRowDutyLedgerAttachesFirstRootSeen is EIP-8371's fallback rule, and the only one that can
// apply where roots are recorded: gossip validation runs before block processing, so the root is
// not in forkchoice yet. The first valid root seen for a slot is the one duties attach to, and
// every competing root for that slot is declined.
func TestRowDutyLedgerAttachesFirstRootSeen(t *testing.T) {
	l := newRowDutyLedger()
	now := time.Unix(1700000000, 0)

	require.Equal(t, true, l.attach(10, rootOf(1), now))
	require.Equal(t, false, l.attach(10, rootOf(2), now.Add(time.Millisecond)))
	require.Equal(t, false, l.attach(10, rootOf(3), now.Add(2*time.Millisecond)))

	ref, ok := l.reference(rootOf(1))
	require.Equal(t, true, ok)
	require.Equal(t, true, ref.attached)
	require.Equal(t, primitives.Slot(10), ref.slot)
	require.Equal(t, now, ref.acquired)

	// The competitors are known -- so the scheduler can tell "declined" from "never seen" -- but
	// not attached, and each keeps its own acquisition instant in case forkchoice later prefers
	// it.
	for i, b := range []byte{2, 3} {
		ref, ok := l.reference(rootOf(b))
		require.Equal(t, true, ok)
		require.Equal(t, false, ref.attached)
		require.Equal(t, now.Add(time.Duration(i+1)*time.Millisecond), ref.acquired)
	}

	// A root at a slot we have never heard of is unknown, which is a different answer again.
	_, ok = l.reference(rootOf(9))
	require.Equal(t, false, ok)
}

// TestRowDutyLedgerDoesNotMoveTheReferenceInstant is the D9 property. The acquisition instant is
// the *first* time the root was obtained, so the row header, the column header and block gossip
// all reporting the same root must leave it where it was -- otherwise the phase offsets are
// measured from whichever path reported last, which is exactly the drift D9 removes.
func TestRowDutyLedgerDoesNotMoveTheReferenceInstant(t *testing.T) {
	l := newRowDutyLedger()
	first := time.Unix(1700000000, 0)

	require.Equal(t, true, l.attach(10, rootOf(1), first))
	require.Equal(t, true, l.attach(10, rootOf(1), first.Add(400*time.Millisecond)))
	require.Equal(t, true, l.attach(10, rootOf(1), first.Add(2*time.Second)))

	ref, ok := l.reference(rootOf(1))
	require.Equal(t, true, ok)
	require.Equal(t, first, ref.acquired)

	// The same must hold for a competing root, whose instant is what its delays would be
	// measured from if forkchoice later selected it.
	competing := first.Add(100 * time.Millisecond)
	require.Equal(t, false, l.attach(10, rootOf(2), competing))
	require.Equal(t, false, l.attach(10, rootOf(2), competing.Add(time.Second)))
	ref, ok = l.reference(rootOf(2))
	require.Equal(t, true, ok)
	require.Equal(t, competing, ref.acquired)
}

// TestRowDutyLedgerPromotesTheHeadBranchRoot covers the EIP's first clause, evaluated lazily
// because forkchoice cannot answer it any earlier: a competing root the chain has actually
// selected takes the attachment from the first-seen one.
func TestRowDutyLedgerPromotesTheHeadBranchRoot(t *testing.T) {
	l := newRowDutyLedger()
	now := time.Unix(1700000000, 0)
	calls := 0

	require.Equal(t, true, l.attach(10, rootOf(1), now))
	competing := now.Add(500 * time.Millisecond)
	require.Equal(t, false, l.attach(10, rootOf(2), competing))
	require.Equal(t, 0, calls, "recording a root must never cost a forkchoice lookup")

	ref, ok := l.promoteIfHeadBranch(rootOf(2), competing, alwaysCanonical(&calls))
	require.Equal(t, true, ok)
	require.Equal(t, true, ref.attached)
	require.Equal(t, 1, calls)
	// The reference instant is when we first obtained *this* root, not when it was promoted:
	// its delays belong to its own block.
	require.Equal(t, competing, ref.acquired)

	ref, ok = l.reference(rootOf(1))
	require.Equal(t, true, ok)
	require.Equal(t, false, ref.attached, "the displaced root's duties are now optional")

	// Promoting the already-attached root is free.
	_, ok = l.promoteIfHeadBranch(rootOf(2), competing.Add(time.Second), alwaysCanonical(&calls))
	require.Equal(t, true, ok)
	require.Equal(t, 1, calls)
}

// TestRowDutyLedgerRulesOnACompetitorAtMostPeriodically bounds the cost of declining without
// making the verdict permanent. A group has 128 rows, and every one of them reaching
// recoverability must not be a forkchoice query -- but a root that becomes canonical after the
// first row of it was declined must still be able to win, or an equivocating block that merely
// arrived first would stop the real block of that slot from ever being reconstructed.
func TestRowDutyLedgerRulesOnACompetitorAtMostPeriodically(t *testing.T) {
	l := newRowDutyLedger()
	now := time.Unix(1700000000, 0)
	calls := 0

	require.Equal(t, true, l.attach(10, rootOf(1), now))
	require.Equal(t, false, l.attach(10, rootOf(2), now))

	// 128 rows of the competing root, all within one recheck interval: one lookup.
	for i := 0; i < 128; i++ {
		ref, ok := l.promoteIfHeadBranch(rootOf(2), now.Add(time.Duration(i)*time.Millisecond), neverCanonical(&calls))
		require.Equal(t, true, ok)
		require.Equal(t, false, ref.attached)
	}
	require.Equal(t, 1, calls)

	// A recheck interval later, forkchoice has selected it and the ledger notices.
	later := now.Add(rowDutyHeadBranchRecheck + time.Millisecond)
	ref, ok := l.promoteIfHeadBranch(rootOf(2), later, alwaysCanonical(&calls))
	require.Equal(t, true, ok)
	require.Equal(t, true, ref.attached)
	require.Equal(t, 2, calls)
	require.Equal(t, now, ref.acquired, "promotion must not move the reference instant")
}

// TestRowDutyLedgerPromoteIgnoresUnknownRoots: a root we have no record of cannot be promoted,
// because there is no slot to take the attachment from and no instant to measure delays against.
func TestRowDutyLedgerPromoteIgnoresUnknownRoots(t *testing.T) {
	l := newRowDutyLedger()
	calls := 0

	_, ok := l.promoteIfHeadBranch(rootOf(7), time.Unix(1700000000, 0), alwaysCanonical(&calls))
	require.Equal(t, false, ok)
	require.Equal(t, 0, calls)
}

// TestRowDutyLedgerPrunes bounds the ledger. It has to outlive the broadcaster's group TTL, and
// it must not outlive it by much: this is per-slot state fed from a hot path.
func TestRowDutyLedgerPrunes(t *testing.T) {
	l := newRowDutyLedger()
	now := time.Unix(1700000000, 0)

	l.attach(10, rootOf(1), now)
	l.attach(10, rootOf(2), now)

	// Still within retention: with a group TTL of 3 slots, a row from slot 10 can still be alive
	// in the broadcaster at slot 13, so its acquisition record has to be too.
	l.attach(10+rowDutyRetentionSlots-1, rootOf(3), now)
	_, ok := l.reference(rootOf(1))
	require.Equal(t, true, ok)

	// One slot further and the group is gone from the broadcaster, so the record can go too.
	l.attach(10+rowDutyRetentionSlots, rootOf(4), now)
	_, ok = l.reference(rootOf(1))
	require.Equal(t, false, ok, "the attached root of a slot past retention should be forgotten")
	_, ok = l.reference(rootOf(2))
	require.Equal(t, false, ok, "and so should its competitors, or slotOf leaks")

	require.Equal(t, 2, len(l.bySlot))
	require.Equal(t, 2, len(l.slotOf))
}

// TestNoteRowDutyRootWithoutLedgerIsANoOp covers the disabled case: the two shipped paths that
// record acquisitions -- column header validation and block gossip -- call this on every node,
// including nodes without --row-das.
func TestNoteRowDutyRootWithoutLedgerIsANoOp(t *testing.T) {
	s := &Service{}
	s.noteRowDutyRoot(10, rootOf(1))
	require.Equal(t, false, s.rootOnHeadBranch(rootOf(1)))
}

// rowDutyService builds the smallest service that can validate a partial header: the mock column
// verifier accepts, and the mock chain has seen the zero parent root buildPartialColumn uses.
func rowDutyService(t *testing.T, verifier verification.MockDataColumnsVerifier) *Service {
	t.Helper()

	return &Service{
		newColumnsVerifier: testNewColumnsVerifier(verifier),
		cfg: &config{chain: &mock.ChainService{
			// The mock only consults InitSyncBlockRoots when it has a DB to miss in first.
			DB:                 dbtest.SetupDB(t),
			InitSyncBlockRoots: map[[32]byte]bool{{}: true},
		}},
		rowDuties: newRowDutyLedger(),
	}
}

// TestColumnHeaderValidationRecordsTheAcquisition is the wiring for D9's reference instant on the
// column axis. The same header serves both DAS axes, and on a supernode it usually arrives on a
// column topic first -- so if this path did not record the root, the phase delays of a row would
// be measured from whenever the row axis happened to catch up.
func TestColumnHeaderValidationRecordsTheAcquisition(t *testing.T) {
	s := rowDutyService(t, verification.MockDataColumnsVerifier{})
	callbacks := &partialColumnCallbacks{service: s}

	col := buildPartialColumn(t, 1, nil)
	_, result, err := callbacks.PartialVerifierFromHeader(col)
	require.NoError(t, err)
	require.Equal(t, pubsub.ValidationAccept, result)

	ref, ok := s.rowDuties.reference(col.BlockRoot())
	require.Equal(t, true, ok, "an accepted header must record the block root it carries")
	require.Equal(t, true, ref.attached)
	require.Equal(t, col.Slot(), ref.slot)
}

// TestColumnHeaderValidationRecordsNothingOnRejection: a root only counts as obtained once the
// block behind it is known to be valid. Recording a rejected header would let anyone set the
// reference instant, and worse, claim a slot's attachment with a root that is not a block.
func TestColumnHeaderValidationRecordsNothingOnRejection(t *testing.T) {
	invalid := errors.Wrap(verification.ErrInvalid, "invalid verification")
	s := rowDutyService(t, verification.MockDataColumnsVerifier{ErrValidProposerSignature: invalid})
	callbacks := &partialColumnCallbacks{service: s}

	col := buildPartialColumn(t, 1, nil)
	_, result, err := callbacks.PartialVerifierFromHeader(col)
	require.NotNil(t, err)
	require.Equal(t, pubsub.ValidationReject, result)

	_, ok := s.rowDuties.reference(col.BlockRoot())
	require.Equal(t, false, ok)
}

// TestRowHeaderValidationRecordsTheAcquisition is the same wiring on the row axis, which is the
// path that cannot be skipped: row state is only ever built from a validated header, so this is
// what makes "unknown_root" at the scheduler a bug rather than a routine case.
func TestRowHeaderValidationRecordsTheAcquisition(t *testing.T) {
	s := rowDutyService(t, verification.MockDataColumnsVerifier{})
	callbacks := &rowCallbacks{service: s}

	col := buildPartialColumn(t, 1, nil)
	sbh, err := col.SignedBlockHeader()
	require.NoError(t, err)
	commitments, err := col.KzgCommitments()
	require.NoError(t, err)
	proof, err := col.KzgCommitmentsInclusionProof()
	require.NoError(t, err)

	header := &ethpb.PartialDataColumnHeader{
		KzgCommitments:               commitments,
		SignedBlockHeader:            sbh,
		KzgCommitmentsInclusionProof: proof,
	}

	result, err := callbacks.ValidateRowHeader(header)
	require.NoError(t, err)
	require.Equal(t, pubsub.ValidationAccept, result)

	root, err := sbh.Header.HashTreeRoot()
	require.NoError(t, err)
	ref, ok := s.rowDuties.reference(root)
	require.Equal(t, true, ok, "an accepted row header must record the block root it carries")
	require.Equal(t, true, ref.attached)
	require.Equal(t, sbh.Header.Slot, ref.slot)
}
