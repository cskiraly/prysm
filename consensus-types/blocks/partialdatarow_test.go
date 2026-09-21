package blocks

import (
	"iter"
	"slices"
	"testing"
	"time"

	"github.com/OffchainLabs/go-bitfield"
	fieldparams "github.com/OffchainLabs/prysm/v7/config/fieldparams"
	"github.com/OffchainLabs/prysm/v7/consensus-types/primitives"
	ethpb "github.com/OffchainLabs/prysm/v7/proto/prysm/v1alpha1"
	"github.com/OffchainLabs/prysm/v7/testing/require"
	"github.com/libp2p/go-libp2p-pubsub/partialmessages"
	"github.com/libp2p/go-libp2p/core/peer"
)

const testBlobCount = 4

func testRowRoot() [fieldparams.RootLength]byte {
	var root [fieldparams.RootLength]byte
	for i := range root {
		root[i] = byte(i)
	}

	return root
}

// testRow builds an empty partial row for rowIndex of a block with testBlobCount blobs.
func testRow(t *testing.T, rowIndex uint64) PartialDataRow {
	t.Helper()

	header := testSignedHeader(true, 96)
	header.Header.Slot = 42
	row, err := NewPartialDataRow(
		testRowRoot(),
		header,
		rowIndex,
		sizedSlices(testBlobCount, 48, 0x10),
		sizedSlices(kzgCommitmentsInclusionProofDepth, 32, 0x20),
	)
	require.NoError(t, err)

	return row
}

func testCell(fill byte) []byte {
	cell := make([]byte, 2048)
	for i := range cell {
		cell[i] = fill
	}

	return cell
}

func testProof(fill byte) []byte {
	proof := make([]byte, 48)
	for i := range proof {
		proof[i] = fill
	}

	return proof
}

func TestNewPartialDataRow(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		row := testRow(t, 2)
		require.Equal(t, uint64(2), row.RowIndex())
		require.Equal(t, testRowRoot(), row.BlockRoot())
		require.Equal(t, primitives.Slot(42), row.Slot())
		require.Equal(t, uint64(fieldparams.NumberOfColumns), row.Included.Len())
		require.Equal(t, uint64(0), row.Included.Count())
		require.Equal(t, fieldparams.NumberOfColumns, len(row.Cells))
		require.Equal(t, fieldparams.NumberOfColumns, len(row.Proofs))
	})

	t.Run("nil header", func(t *testing.T) {
		_, err := NewPartialDataRow(testRowRoot(), nil, 0, sizedSlices(1, 48, 0), nil)
		require.NotNil(t, err)
	})

	t.Run("no commitments", func(t *testing.T) {
		_, err := NewPartialDataRow(testRowRoot(), testSignedHeader(true, 96), 0, nil, nil)
		require.NotNil(t, err)
	})

	t.Run("row index beyond blob count", func(t *testing.T) {
		_, err := NewPartialDataRow(testRowRoot(), testSignedHeader(true, 96), 4, sizedSlices(4, 48, 0), nil)
		require.ErrorIs(t, err, errRowIndexOutOfRange)
	})

	t.Run("wrong inclusion proof length", func(t *testing.T) {
		// A row always has a header to send, and the header cannot encode a proof of the
		// wrong depth, so this is caught at construction rather than at publish time.
		_, err := NewPartialDataRow(testRowRoot(), testSignedHeader(true, 96), 0, sizedSlices(testBlobCount, 48, 0), nil)
		require.ErrorIs(t, err, errInclusionProofLength)

		_, err = NewPartialDataRow(testRowRoot(), testSignedHeader(true, 96), 0,
			sizedSlices(testBlobCount, 48, 0), sizedSlices(kzgCommitmentsInclusionProofDepth+1, 32, 0))
		require.ErrorIs(t, err, errInclusionProofLength)
	})
}

// TestPartialDataRowGroupIDMatchesColumn pins the property that lets one validated-header
// cache serve both domains: a row and a column of the same block share a group id.
func TestPartialDataRowGroupIDMatchesColumn(t *testing.T) {
	root := testRowRoot()
	row := testRow(t, 1)

	column, err := NewPartialDataColumn(root, testSignedHeader(true, 96), 7, sizedSlices(testBlobCount, 48, 0x10), sizedSlices(kzgCommitmentsInclusionProofDepth, 32, 0x20))
	require.NoError(t, err)

	require.DeepEqual(t, column.GroupID(), row.GroupID())
}

// TestPartialDataRowCommitmentIsTheRowsOwn is the axis on which row verification differs from
// column verification: one commitment for the whole row, rather than one per cell.
func TestPartialDataRowCommitmentIsTheRowsOwn(t *testing.T) {
	commitments := sizedSlices(testBlobCount, 48, 0x10)
	for rowIndex := range uint64(testBlobCount) {
		row := testRow(t, rowIndex)
		require.DeepEqual(t, commitments[rowIndex], row.Commitment())
	}
}

func TestPartialDataRowExtendFromVerifiedCell(t *testing.T) {
	row := testRow(t, 0)

	require.Equal(t, true, row.ExtendFromVerifiedCell(5, testCell(1), testProof(1)))
	require.Equal(t, true, row.Included.BitAt(5))
	require.Equal(t, uint64(1), row.Included.Count())

	// Already present.
	require.Equal(t, false, row.ExtendFromVerifiedCell(5, testCell(2), testProof(2)))
	require.DeepEqual(t, testCell(1), row.Cells[5])

	// Out of range: a row always has NUMBER_OF_COLUMNS parts, so this is not a blob-count
	// question.
	require.Equal(t, false, row.ExtendFromVerifiedCell(fieldparams.NumberOfColumns, testCell(3), testProof(3)))
	require.Equal(t, uint64(1), row.Included.Count())
}

func TestPartialDataRowCompletionThresholds(t *testing.T) {
	threshold := ReconstructionThreshold()
	require.Equal(t, uint64(64), threshold, "expected half of 128 columns, rounded up")

	row := testRow(t, 0)
	for columnIndex := range threshold - 1 {
		require.Equal(t, true, row.ExtendFromVerifiedCell(columnIndex, testCell(byte(columnIndex)), testProof(byte(columnIndex))))
	}
	require.Equal(t, false, row.ReconstructionThresholdMet(), "63 cells is not enough")
	require.Equal(t, false, row.IsComplete())

	require.Equal(t, true, row.ExtendFromVerifiedCell(threshold-1, testCell(0), testProof(0)))
	require.Equal(t, true, row.ReconstructionThresholdMet(), "64 cells is enough to recover the rest")
	require.Equal(t, false, row.IsComplete(), "recoverable is not the same as complete")

	for columnIndex := threshold; columnIndex < uint64(fieldparams.NumberOfColumns); columnIndex++ {
		require.Equal(t, true, row.ExtendFromVerifiedCell(columnIndex, testCell(0), testProof(0)))
	}
	require.Equal(t, true, row.IsComplete())
}

func TestPartialDataRowPresentColumnIndicesAreAscending(t *testing.T) {
	// The KZG recovery functions require ascending cell indices, so this ordering is load
	// bearing rather than cosmetic.
	row := testRow(t, 0)
	for _, columnIndex := range []uint64{100, 3, 57, 0} {
		require.Equal(t, true, row.ExtendFromVerifiedCell(columnIndex, testCell(1), testProof(1)))
	}

	indices := row.PresentColumnIndices()
	require.DeepEqual(t, []uint64{0, 3, 57, 100}, indices)
	require.Equal(t, true, slices.IsSorted(indices))
}

func TestPartialDataRowPartsMetadata(t *testing.T) {
	row := testRow(t, 0)
	require.Equal(t, true, row.ExtendFromVerifiedCell(1, testCell(1), testProof(1)))
	require.Equal(t, true, row.ExtendFromVerifiedCell(9, testCell(9), testProof(9)))

	meta, err := row.newPartsMetadata(nil)
	require.NoError(t, err)
	require.Equal(t, uint64(fieldparams.NumberOfColumns), meta.Available.Len())
	require.Equal(t, uint64(2), meta.Available.Count())
	require.Equal(t, true, meta.Available.BitAt(1))
	require.Equal(t, true, meta.Available.BitAt(9))
	// By default we request everything we are missing.
	require.Equal(t, uint64(fieldparams.NumberOfColumns-2), meta.Requests.Count())
	require.Equal(t, false, meta.Requests.BitAt(1))

	// An override narrows the requests, and is intersected with what is missing so it can
	// never ask for a cell we already hold.
	require.NoError(t, row.SetPartsRequests(testBitlist(fieldparams.NumberOfColumns, 1, 2, 3)))
	meta, err = row.newPartsMetadata(nil)
	require.NoError(t, err)
	require.Equal(t, uint64(2), meta.Requests.Count())
	require.Equal(t, false, meta.Requests.BitAt(1), "already held, so not requested")
	require.Equal(t, true, meta.Requests.BitAt(2))
	require.Equal(t, true, meta.Requests.BitAt(3))

	requests, ok := row.PartsRequests()
	require.Equal(t, true, ok)
	require.Equal(t, uint64(3), requests.Count())

	row.ClearPartsRequests()
	_, ok = row.PartsRequests()
	require.Equal(t, false, ok)

	// A wrong-length override is refused rather than silently truncated.
	require.NotNil(t, row.SetPartsRequests(testBitlist(testBlobCount)))
}

func TestPartialDataRowCellsToSendToPeer(t *testing.T) {
	row := testRow(t, 2)
	for _, columnIndex := range []uint64{1, 4, 7} {
		require.Equal(t, true, row.ExtendFromVerifiedCell(columnIndex, testCell(byte(columnIndex)), testProof(byte(columnIndex))))
	}

	t.Run("sends only what is requested, held, and not already there", func(t *testing.T) {
		// Peer wants 1, 4, 9; already has 4; we do not have 9.
		peerMeta := testPeerMeta(fieldparams.NumberOfColumns, []uint64{4}, []uint64{1, 4, 9})
		encoded, sent, err := row.cellsToSendToPeer(peerMeta)
		require.NoError(t, err)
		require.Equal(t, uint64(1), sent.Count())
		require.Equal(t, true, sent.BitAt(1))

		decoded, err := DecodePartialRowSidecar(encoded)
		require.NoError(t, err)
		require.Equal(t, uint64(2), decoded.RowIndex, "the row index travels with the cells")
		require.Equal(t, 1, len(decoded.PartialRow))
		require.DeepEqual(t, testCell(1), decoded.PartialRow[0])
		require.DeepEqual(t, testProof(1), decoded.KzgProofs[0])
	})

	t.Run("cells are packed in ascending column order", func(t *testing.T) {
		peerMeta := testPeerMeta(fieldparams.NumberOfColumns, nil, []uint64{7, 4, 1})
		encoded, sent, err := row.cellsToSendToPeer(peerMeta)
		require.NoError(t, err)
		require.Equal(t, uint64(3), sent.Count())

		decoded, err := DecodePartialRowSidecar(encoded)
		require.NoError(t, err)
		require.DeepEqual(t, testCell(1), decoded.PartialRow[0])
		require.DeepEqual(t, testCell(4), decoded.PartialRow[1])
		require.DeepEqual(t, testCell(7), decoded.PartialRow[2])
	})

	t.Run("nothing to send", func(t *testing.T) {
		peerMeta := testPeerMeta(fieldparams.NumberOfColumns, []uint64{1, 4, 7}, []uint64{1, 4, 7})
		encoded, sent, err := row.cellsToSendToPeer(peerMeta)
		require.NoError(t, err)
		require.IsNil(t, encoded)
		require.IsNil(t, sent)
	})

	t.Run("bitmap length mismatch is an error", func(t *testing.T) {
		_, _, err := row.cellsToSendToPeer(testPeerMeta(testBlobCount, nil, []uint64{1}))
		require.NotNil(t, err)
	})
}

func TestPartialDataRowHeaderMessage(t *testing.T) {
	// Rows have no full-message form, so the header is the only way a row-topic-only peer
	// learns the commitments. Unlike Gloas columns, a row always has one to send.
	row := testRow(t, 1)

	encoded, err := row.headerMessage()
	require.NoError(t, err)

	decoded, err := DecodePartialRowSidecar(encoded)
	require.NoError(t, err)
	require.Equal(t, uint64(1), decoded.RowIndex)
	require.Equal(t, 0, len(decoded.PartialRow), "a header message carries no cells")
	require.Equal(t, 1, len(decoded.Header))
	require.Equal(t, testBlobCount, len(decoded.Header[0].KzgCommitments))
	require.Equal(t, primitives.Slot(42), decoded.Header[0].SignedBlockHeader.Header.Slot)
}

func TestPartialDataRowCellsToVerifyFromPartialMessage(t *testing.T) {
	row := testRow(t, 3)
	require.Equal(t, true, row.ExtendFromVerifiedCell(2, testCell(2), testProof(2)))

	t.Run("filters cells we already have", func(t *testing.T) {
		message := &ethpb.PartialDataRowSidecar{
			RowIndex:           3,
			CellsPresentBitmap: testBitlist(fieldparams.NumberOfColumns, 2, 5),
			PartialRow:         [][]byte{testCell(2), testCell(5)},
			KzgProofs:          [][]byte{testProof(2), testProof(5)},
		}

		indices, bundles, err := row.CellsToVerifyFromPartialMessage(message)
		require.NoError(t, err)
		require.DeepEqual(t, []uint64{5}, indices)
		require.Equal(t, 1, len(bundles))
		require.Equal(t, uint64(5), bundles[0].ColumnIndex, "the cell index of a row cell is its column index")
		require.DeepEqual(t, row.Commitment(), bundles[0].Commitment, "every cell of a row shares one commitment")
		require.DeepEqual(t, testCell(5), bundles[0].Cell)
	})

	t.Run("rejects a message for another row", func(t *testing.T) {
		message := &ethpb.PartialDataRowSidecar{
			RowIndex:           1,
			CellsPresentBitmap: testBitlist(fieldparams.NumberOfColumns, 5),
			PartialRow:         [][]byte{testCell(5)},
			KzgProofs:          [][]byte{testProof(5)},
		}
		_, _, err := row.CellsToVerifyFromPartialMessage(message)
		require.NotNil(t, err)
	})

	t.Run("rejects a wrong bitmap length", func(t *testing.T) {
		message := &ethpb.PartialDataRowSidecar{
			RowIndex:           3,
			CellsPresentBitmap: testBitlist(testBlobCount, 1),
			PartialRow:         [][]byte{testCell(1)},
			KzgProofs:          [][]byte{testProof(1)},
		}
		_, _, err := row.CellsToVerifyFromPartialMessage(message)
		require.NotNil(t, err)
	})

	t.Run("rejects counts that disagree with the bitmap", func(t *testing.T) {
		message := &ethpb.PartialDataRowSidecar{
			RowIndex:           3,
			CellsPresentBitmap: testBitlist(fieldparams.NumberOfColumns, 5, 6),
			PartialRow:         [][]byte{testCell(5)},
			KzgProofs:          [][]byte{testProof(5)},
		}
		_, _, err := row.CellsToVerifyFromPartialMessage(message)
		require.NotNil(t, err)

		message.PartialRow = [][]byte{testCell(5), testCell(6)}
		message.KzgProofs = [][]byte{testProof(5)}
		_, _, err = row.CellsToVerifyFromPartialMessage(message)
		require.NotNil(t, err)
	})

	t.Run("empty bitmap yields nothing", func(t *testing.T) {
		indices, bundles, err := row.CellsToVerifyFromPartialMessage(&ethpb.PartialDataRowSidecar{RowIndex: 3})
		require.NoError(t, err)
		require.Equal(t, 0, len(indices))
		require.Equal(t, 0, len(bundles))
	})
}

func TestPartialDataRowPublishActions(t *testing.T) {
	row := testRow(t, 0)
	require.Equal(t, true, row.ExtendFromVerifiedCell(3, testCell(3), testProof(3)))

	remote := peer.ID("peer-a")
	headerSent := make(map[peer.ID]bool)
	var eagerPushed []peer.ID
	requestsPartial := func(peer.ID) bool { return true }

	// First pass: we know nothing about the peer, so this is an eager push carrying the
	// header and our metadata, but no cells.
	peerStates := map[peer.ID]PartialDataColumnPeerState{remote: {}}
	actions := row.PublishActionsFn(headerSent, func(p peer.ID) { eagerPushed = append(eagerPushed, p) }, nil)
	for id, action := range actions(peerStates, requestsPartial) {
		require.Equal(t, remote, id)
		require.NoError(t, action.Err)
		// The extension records state only on admission; a direct consumer must say so.
		action.OnSent(true)
		require.Equal(t, true, len(action.EncodedPartsMetadata) > 0)

		decoded, err := DecodePartialRowSidecar(action.EncodedPartialMessage)
		require.NoError(t, err)
		require.Equal(t, 1, len(decoded.Header))
		require.Equal(t, 0, len(decoded.PartialRow))
	}
	require.DeepEqual(t, []peer.ID{remote}, eagerPushed)
	require.Equal(t, true, headerSent[remote])
	require.NotNil(t, peerStates[remote].Recvd)

	// Second pass, now that the peer has told us it wants cell 3: send the cell, and no
	// second copy of the header.
	peerStates[remote] = PartialDataColumnPeerState{
		Sent:  peerStates[remote].Sent,
		Recvd: testPeerMeta(fieldparams.NumberOfColumns, nil, []uint64{3}),
	}
	for _, action := range actions(peerStates, requestsPartial) {
		action.OnSent(true)
		require.NoError(t, action.Err)

		decoded, err := DecodePartialRowSidecar(action.EncodedPartialMessage)
		require.NoError(t, err)
		require.Equal(t, 0, len(decoded.Header), "the header is sent once")
		require.Equal(t, 1, len(decoded.PartialRow))
		require.DeepEqual(t, testCell(3), decoded.PartialRow[0])
	}
	// Having sent the cell, we now believe the peer has it.
	require.Equal(t, true, peerStates[remote].Recvd.Available.BitAt(3))
}

// TestPartialDataRowUnadmittedSendIsRetried pins the reason state is recorded on confirmation
// rather than optimistically.
//
// Gossipsub drops an RPC on queue pressure and expects the receiver to recover through the next
// heartbeat's IHAVE -- which works because IHAVE is *regenerated* every heartbeat. Our announce
// state is *remembered*: `partialForPeer` compares against `Sent`, so once we believe a peer was
// told, we never say it again. Before this, a dropped announcement was therefore permanent, and the
// claim ledger parked the part for a full TTL with nobody actually asked.
//
// So: an action reported as not admitted must leave no trace, and the next publish must rebuild the
// same announcement.
func TestPartialDataRowUnadmittedSendIsRetried(t *testing.T) {
	row := testRow(t, 0)
	row.Published = true
	const remote = peer.ID("remote")
	requestsPartial := func(peer.ID) bool { return true }

	peerStates := map[peer.ID]PartialDataColumnPeerState{remote: {}}
	headerSent := map[peer.ID]bool{}
	actions := row.PublishActionsFn(headerSent, nil, nil)

	// First publish, reported as dropped by the transport.
	var firstMetadata []byte
	for _, action := range actions(peerStates, requestsPartial) {
		require.NoError(t, action.Err)
		require.Equal(t, true, len(action.EncodedPartsMetadata) > 0)
		firstMetadata = action.EncodedPartsMetadata
		action.OnSent(false)
	}
	require.Equal(t, false, headerSent[remote], "a dropped send must not record the header")
	require.Equal(t, true, peerStates[remote].Sent == nil, "a dropped send must not record Sent")

	// Second publish: because nothing was recorded, the same announcement is rebuilt. Had the
	// state latched optimistically, this would produce no metadata at all and the peer would never
	// learn what we hold.
	var resent bool
	for _, action := range actions(peerStates, requestsPartial) {
		require.NoError(t, action.Err)
		require.Equal(t, true, len(action.EncodedPartsMetadata) > 0, "the dropped announcement must be re-sent")
		require.DeepEqual(t, firstMetadata, action.EncodedPartsMetadata)
		resent = true
		action.OnSent(true)
	}
	require.Equal(t, true, resent)
	require.NotNil(t, peerStates[remote].Sent, "an admitted send records Sent")

	// And now that it is recorded, an unchanged state announces nothing -- the suppression that
	// made the dropped case permanent is still doing its job when the send actually happened.
	for _, action := range actions(peerStates, requestsPartial) {
		require.Equal(t, 0, len(action.EncodedPartsMetadata), "no re-announcement when nothing changed")
	}
}

// cancellationFixture sets up the one situation where a request *removal* arises without any other
// news: two peers both hold part 0, the claim goes to one of them, lapses, and moves to the other.
// The first peer's request set then shrinks and nothing else about it changes. It returns the row,
// the peer states, the action iterator, which peer lost the claim, and a clock the test moves.
func cancellationFixture(t *testing.T) (row *PartialDataRow, peerStates map[peer.ID]PartialDataColumnPeerState, actions partialmessages.PublishActionsFn[PartialDataColumnPeerState], loser peer.ID, clock *time.Time) {
	t.Helper()

	r := testRow(t, 0)
	r.Published = true
	row = &r
	a, b := peer.ID("a"), peer.ID("b")
	holdsZero := func() *ethpb.PartialDataColumnPartsMetadata {
		m := NewPartsMetaWithNoAvailableAndNoRequests(numberOfColumns)
		m.Available.SetBitAt(0, true)
		return m
	}
	// Recvd set, so neither peer takes the eager-push path and the test sees the ordinary decision.
	peerStates = map[peer.ID]PartialDataColumnPeerState{a: {Recvd: holdsZero()}, b: {Recvd: holdsZero()}}
	// Only part 0 is missing and wanted; the fixture's peers hold nothing else.
	require.NoError(t, r.SetPartsRequests(bitlistWith(numberOfColumns, 0)))
	requestsPartial := func(peer.ID) bool { return true }
	actions = func(states map[peer.ID]PartialDataColumnPeerState, rp func(peer.ID) bool) iter.Seq2[peer.ID, partialmessages.PublishAction] {
		return row.PublishActionsFn(map[peer.ID]bool{}, nil, nil)(states, rp)
	}

	start := time.Unix(1_700_000_000, 0)
	now := start
	clock = &now
	prev := partialClock
	partialClock = func() time.Time { return *clock }
	t.Cleanup(func() { partialClock = prev })

	// First publish: both get first metadata; exactly one is asked for part 0.
	var asked []peer.ID
	for id, action := range actions(peerStates, requestsPartial) {
		require.NoError(t, action.Err)
		require.Equal(t, true, len(action.EncodedPartsMetadata) > 0)
		meta := &ethpb.PartialDataColumnPartsMetadata{}
		require.NoError(t, meta.UnmarshalSSZ(action.EncodedPartsMetadata))
		if meta.Requests.BitAt(0) {
			asked = append(asked, id)
		}
		action.OnSent(true)
	}
	require.Equal(t, 1, len(asked), "RequestN=1: one peer asked")
	loser = asked[0]

	// Let the claim lapse; the request moves to the fresh peer. The loser sees a removal only.
	// The winner's claim is committed at this instant, so its deadline is this plus claimTTL(1).
	*clock = start.Add(requestClaimCeiling + time.Second)
	winner := a
	if loser == a {
		winner = b
	}
	for id, action := range actions(peerStates, requestsPartial) {
		require.NoError(t, action.Err)
		switch id {
		case winner:
			require.Equal(t, true, len(action.EncodedPartsMetadata) > 0, "the addition to the new peer is sent")
			action.OnSent(true)
		case loser:
			require.Equal(t, 0, len(action.EncodedPartsMetadata), "a removal on its own buys no packet")
			require.Equal(t, 0, len(action.EncodedPartialMessage))
		}
	}
	require.Equal(t, true, peerStates[loser].Sent.Requests.BitAt(0), "the loser still believes it is asked")

	return row, peerStates, actions, loser, clock
}

func bitlistWith(length uint64, bits ...uint64) bitfield.Bitlist {
	l := bitfield.NewBitlist(length)
	for _, b := range bits {
		l.SetBitAt(b, true)
	}
	return l
}

// TestRowCancellationRidesTheNextPacket is the piggyback: when a cells-only action to the peer
// comes up, the deferred removal is forced onto it -- metadata generation is no longer independent
// of cell generation, which was listed as the thing that would otherwise be implemented wrongly.
func TestRowCancellationRidesTheNextPacket(t *testing.T) {
	row, peerStates, actions, loser, clock := cancellationFixture(t)
	requestsPartial := func(peer.ID) bool { return true }
	lapseAt := *clock

	// Give the loser a reason to receive a packet that is *not* metadata news: it asks for a cell
	// we hold, and we hold it since before our last metadata to it -- so no availability growth.
	// Plant the cell, then re-sync Sent.Available so the fixture's history stays consistent.
	require.Equal(t, true, row.ExtendFromVerifiedCell(7, make([]byte, 2048), make([]byte, 48)))
	state := peerStates[loser]
	merged, err := MergeAvailableIntoPartsMetadata(state.Sent, bitlistWith(numberOfColumns, 7))
	require.NoError(t, err)
	state.Sent = merged
	state.Recvd.Requests.SetBitAt(7, true)
	peerStates[loser] = state

	*clock = clock.Add(10 * time.Millisecond) // well inside the maximum delay
	for id, action := range actions(peerStates, requestsPartial) {
		require.NoError(t, action.Err)
		if id != loser {
			continue
		}
		require.Equal(t, true, len(action.EncodedPartialMessage) > 0, "the cell goes")
		require.Equal(t, true, len(action.EncodedPartsMetadata) > 0, "and the removal rides with it")
		action.OnSent(true)
	}
	require.Equal(t, false, peerStates[loser].Sent.Requests.BitAt(0), "the peer now knows")

	// Nothing left pending for the loser. (The winner now has *availability* held -- the cell
	// planted above grew our bitmap for it too -- so the row's earliest deadline is that hold,
	// not the winner's claim; the cancellation is what this test is about.)
	require.Equal(t, true, row.claims().pending[loser].cancelSince.IsZero(), "no cancellation deadline remains")
	_ = lapseAt
}

// TestRowCancellationIsFlushedAtTheDeadline is the other half: a removal that found no packet to
// ride within the maximum delay gets one of its own, and the row exposes that deadline so the
// broadcaster's wake-up fires the publish that carries it.
func TestRowCancellationIsFlushedAtTheDeadline(t *testing.T) {
	// A maximum delay shorter than the winner's claim, so the flush is observed while the claim
	// is live. Were the claim to lapse first, the part would rotate back to the loser, whose view
	// already matches ours, and the removal would become moot -- a different case, covered below.
	prevDelay := RequestCancellationMaxDelay
	RequestCancellationMaxDelay = claimTTL(1) / 2
	t.Cleanup(func() { RequestCancellationMaxDelay = prevDelay })

	row, peerStates, actions, loser, clock := cancellationFixture(t)
	requestsPartial := func(peer.ID) bool { return true }
	deferredAt := *clock

	// The row's next deadline is the cancellation's, provided it is sooner than the live claim.
	deadline, ok := row.EarliestClaimDeadline()
	require.Equal(t, true, ok)
	require.Equal(t, false, deadline.After(deferredAt.Add(cancellationMaxDelay())),
		"the deferred removal's deadline is exposed for the wake-up")

	// Before the deadline: still nothing.
	*clock = deferredAt.Add(cancellationMaxDelay() / 2)
	for id, action := range actions(peerStates, requestsPartial) {
		require.NoError(t, action.Err)
		if id == loser {
			require.Equal(t, 0, len(action.EncodedPartsMetadata))
		}
	}

	// At the deadline: a dedicated metadata packet, no cells.
	*clock = deferredAt.Add(cancellationMaxDelay())
	var flushed bool
	for id, action := range actions(peerStates, requestsPartial) {
		require.NoError(t, action.Err)
		if id != loser {
			continue
		}
		require.Equal(t, true, len(action.EncodedPartsMetadata) > 0, "flushed at the maximum delay")
		require.Equal(t, 0, len(action.EncodedPartialMessage))
		flushed = true
		action.OnSent(true)
	}
	require.Equal(t, true, flushed)
	require.Equal(t, false, peerStates[loser].Sent.Requests.BitAt(0))
}

// availabilityFixture: one peer that has been told our state once, then we learn a cell it did
// not know we hold. Returns the row, the peer state map, the iterator and a movable clock.
func availabilityFixture(t *testing.T) (row *PartialDataRow, peerStates map[peer.ID]PartialDataColumnPeerState, actions func() iter.Seq2[peer.ID, partialmessages.PublishAction], clock *time.Time) {
	t.Helper()

	r := testRow(t, 0)
	r.Published = true
	row = &r
	a := peer.ID("a")
	peerStates = map[peer.ID]PartialDataColumnPeerState{a: {Recvd: NewPartsMetaWithNoAvailableAndNoRequests(numberOfColumns)}}
	// Nothing to request: the fixture is about what we *offer*.
	require.NoError(t, r.SetPartsRequests(bitfield.NewBitlist(numberOfColumns)))
	requestsPartial := func(peer.ID) bool { return true }
	actions = func() iter.Seq2[peer.ID, partialmessages.PublishAction] {
		return row.PublishActionsFn(map[peer.ID]bool{}, nil, nil)(peerStates, requestsPartial)
	}

	start := time.Unix(1_700_000_000, 0)
	now := start
	clock = &now
	prev := partialClock
	partialClock = func() time.Time { return *clock }
	t.Cleanup(func() { partialClock = prev })

	// First metadata goes at once.
	for _, action := range actions() {
		require.NoError(t, action.Err)
		require.Equal(t, true, len(action.EncodedPartsMetadata) > 0)
		action.OnSent(true)
	}
	require.NotNil(t, peerStates[a].Sent)

	// Now we learn a cell.
	require.Equal(t, true, row.ExtendFromVerifiedCell(3, make([]byte, 2048), make([]byte, 48)))

	return row, peerStates, actions, clock
}

// TestRowAvailabilityGrowthIsHeldPerPeer: learning a cell does not, by itself, buy a packet to a
// peer. The announcement waits for a ride, and is flushed at its deadline if none comes. This is
// what replaced the per-group publish throttle, which held the cells that answer a request along
// with the announcements.
func TestRowAvailabilityGrowthIsHeldPerPeer(t *testing.T) {
	row, peerStates, actions, clock := availabilityFixture(t)
	a := peer.ID("a")
	learnedAt := *clock

	packets := func() int {
		n := 0
		for _, action := range actions() {
			require.NoError(t, action.Err)
			if len(action.EncodedPartsMetadata) > 0 || len(action.EncodedPartialMessage) > 0 {
				n++
				action.OnSent(true)
			}
		}
		return n
	}

	require.Equal(t, 0, packets(), "availability growth alone buys no packet")
	require.Equal(t, false, peerStates[a].Sent.Available.BitAt(3), "the peer has not been told")

	// The row exposes the hold's deadline for the wake-up.
	deadline, ok := row.EarliestClaimDeadline()
	require.Equal(t, true, ok)
	require.Equal(t, true, deadline.Equal(learnedAt.Add(availabilityMaxDelay())))

	// Still nothing before the deadline, however many publishes run.
	*clock = learnedAt.Add(availabilityMaxDelay() / 2)
	require.Equal(t, 0, packets())
	require.Equal(t, 0, packets())

	// At the deadline, one packet, and the peer knows.
	*clock = learnedAt.Add(availabilityMaxDelay())
	require.Equal(t, 1, packets(), "flushed at the maximum delay")
	require.Equal(t, true, peerStates[a].Sent.Available.BitAt(3))
	_, ok = row.EarliestClaimDeadline()
	require.Equal(t, false, ok, "nothing is held any more")
}

// TestRowAvailabilityRidesACellPacket: when the peer asks for a cell we hold, the cell goes at once
// and the held availability rides with it -- before its own deadline.
func TestRowAvailabilityRidesACellPacket(t *testing.T) {
	row, peerStates, actions, clock := availabilityFixture(t)
	a := peer.ID("a")

	// Peer asks for cell 3 -- the one it does not know we hold, which is fine: a request is the
	// peer's business. Any cell we hold that it wants would do.
	state := peerStates[a]
	state.Recvd.Requests.SetBitAt(3, true)
	peerStates[a] = state

	*clock = clock.Add(time.Millisecond)
	var carried bool
	for _, action := range actions() {
		require.NoError(t, action.Err)
		require.Equal(t, true, len(action.EncodedPartialMessage) > 0, "the cell goes at once")
		require.Equal(t, true, len(action.EncodedPartsMetadata) > 0, "and the held availability rides with it")
		carried = true
		action.OnSent(true)
	}
	require.Equal(t, true, carried)
	require.Equal(t, true, peerStates[a].Sent.Available.BitAt(3))
	require.Equal(t, true, row.claims().pending[a].empty(), "nothing left held for the peer")
}

// TestRowAvailabilityIsLeadingEdgePerPeer is the shape the author specified and the trailing-edge
// version got wrong: after a quiet window the first growth goes at once; growth inside the window
// holds until the window -- from the last thing the peer was told -- ends, or a packet rides.
func TestRowAvailabilityIsLeadingEdgePerPeer(t *testing.T) {
	row, peerStates, actions, clock := availabilityFixture(t)
	a := peer.ID("a")
	window := availabilityMaxDelay()

	packets := func() int {
		n := 0
		for _, action := range actions() {
			require.NoError(t, action.Err)
			if len(action.EncodedPartsMetadata) > 0 || len(action.EncodedPartialMessage) > 0 {
				n++
				action.OnSent(true)
			}
		}
		return n
	}

	// The fixture's growth came right after the first metadata: inside the window, held.
	require.Equal(t, 0, packets())
	// Flushed when the window from the first metadata ends.
	*clock = clock.Add(window)
	require.Equal(t, 1, packets(), "flushed at the window's end")
	toldAt := *clock

	// Quiet for a whole window, then a cell: the first growth goes at once. Leading edge.
	*clock = toldAt.Add(window + time.Millisecond)
	require.Equal(t, true, row.ExtendFromVerifiedCell(4, make([]byte, 2048), make([]byte, 48)))
	require.Equal(t, 1, packets(), "the first growth after a quiet window is announced immediately")
	require.Equal(t, true, peerStates[a].Sent.Available.BitAt(4))
	toldAt = *clock

	// Another cell right behind it: inside the new window, held, with the window's end as its
	// deadline -- not a fresh 100 ms from the cell.
	*clock = toldAt.Add(10 * time.Millisecond)
	require.Equal(t, true, row.ExtendFromVerifiedCell(5, make([]byte, 2048), make([]byte, 48)))
	require.Equal(t, 0, packets(), "growth inside the window is held")
	deadline, ok := row.EarliestClaimDeadline()
	require.Equal(t, true, ok)
	require.Equal(t, true, deadline.Equal(toldAt.Add(window)), "the hold ends with the window, not later")

	*clock = toldAt.Add(window)
	require.Equal(t, 1, packets(), "and is flushed then")
	require.Equal(t, true, peerStates[a].Sent.Available.BitAt(5))
}

// TestRowCompletionIsNeverHeld: the announcement that completes a row is the whole-row claim other
// reconstructors stand down on (RowServedElsewhere), and it goes at once. Found by R1(b): held
// like ordinary growth, it let more reconstructors duplicate the recovery before they heard.
func TestRowCompletionIsNeverHeld(t *testing.T) {
	row, peerStates, actions, clock := availabilityFixture(t)
	a := peer.ID("a")

	// Ordinary growth is held.
	*clock = clock.Add(time.Millisecond)
	for _, action := range actions() {
		require.NoError(t, action.Err)
		require.Equal(t, 0, len(action.EncodedPartsMetadata), "one more cell is held for a ride")
	}

	// Complete the row.
	for column := range numberOfColumns {
		if !row.Included.BitAt(column) {
			require.Equal(t, true, row.ExtendFromVerifiedCell(column, make([]byte, 2048), make([]byte, 48)))
		}
	}
	require.Equal(t, true, row.IsComplete())

	*clock = clock.Add(time.Millisecond) // still far inside the hold's delay
	var announced bool
	for _, action := range actions() {
		require.NoError(t, action.Err)
		require.Equal(t, true, len(action.EncodedPartsMetadata) > 0, "completion is announced at once")
		announced = true
		action.OnSent(true)
	}
	require.Equal(t, true, announced)
	require.Equal(t, numberOfColumns, peerStates[a].Sent.Available.Count())
}

// TestRowRequestAdditionIsNeverHeld: a new want from a peer is on our own critical path and goes
// immediately, carrying whatever availability was waiting.
func TestRowRequestAdditionIsNeverHeld(t *testing.T) {
	row, peerStates, actions, clock := availabilityFixture(t)
	a := peer.ID("a")

	// The peer now holds cell 9, which we lack and want.
	state := peerStates[a]
	state.Recvd.Available.SetBitAt(9, true)
	peerStates[a] = state
	require.NoError(t, row.SetPartsRequests(bitlistWith(numberOfColumns, 9)))

	*clock = clock.Add(time.Millisecond)
	var sent bool
	for _, action := range actions() {
		require.NoError(t, action.Err)
		require.Equal(t, true, len(action.EncodedPartsMetadata) > 0, "a request addition goes at once")
		meta := &ethpb.PartialDataColumnPartsMetadata{}
		require.NoError(t, meta.UnmarshalSSZ(action.EncodedPartsMetadata))
		require.Equal(t, true, meta.Requests.BitAt(9))
		require.Equal(t, true, meta.Available.BitAt(3), "and the held availability rides with it")
		sent = true
		action.OnSent(true)
	}
	require.Equal(t, true, sent)
}

// TestColumnAvailabilityGrowthIsNotHeld: the column axis announces at once, as shipped.
func TestColumnAvailabilityGrowthIsNotHeld(t *testing.T) {
	column, err := NewPartialDataColumn(testRowRoot(), testSignedHeader(true, 96), 0, sizedSlices(testBlobCount, 48, 0x10), sizedSlices(kzgCommitmentsInclusionProofDepth, 32, 0x20))
	require.NoError(t, err)
	column.Published = true
	a := peer.ID("a")
	peerStates := map[peer.ID]PartialDataColumnPeerState{a: {Recvd: NewPartsMetaWithNoAvailableAndNoRequests(testBlobCount)}}
	requestsPartial := func(peer.ID) bool { return true }
	actions := column.PublishActionsFn(map[peer.ID]bool{}, nil, nil)

	for _, action := range actions(peerStates, requestsPartial) {
		require.NoError(t, action.Err)
		action.OnSent(true)
	}
	require.Equal(t, true, column.ExtendFromVerifiedCell(1, make([]byte, 2048), make([]byte, 48)))

	var announced bool
	for _, action := range actions(peerStates, requestsPartial) {
		require.NoError(t, action.Err)
		require.Equal(t, true, len(action.EncodedPartsMetadata) > 0, "a column announces growth immediately")
		announced = true
	}
	require.Equal(t, true, announced)
}

// TestRowCancellationDeadlineIsNDependent pins the author's point that the flush deadline should
// depend on the request parallelism: at N=1 the peer being told is the one already sending, so
// the cancellation can wait; at N>1 the other holders may not have dispatched yet.
func TestRowCancellationDeadlineIsNDependent(t *testing.T) {
	prev := RequestN
	t.Cleanup(func() { RequestN = prev })

	RequestN = 1
	one := cancellationMaxDelay()
	RequestN = 2
	two := cancellationMaxDelay()
	require.Equal(t, one/2, two)
}

// TestRowCancellationForgetsADepartedPeer: a deferred removal to a peer that is no longer in the
// peer set must not hold the wake deadline open for the group's lifetime.
func TestRowCancellationForgetsADepartedPeer(t *testing.T) {
	row, peerStates, actions, loser, clock := cancellationFixture(t)
	requestsPartial := func(peer.ID) bool { return true }

	lapseAt := *clock
	delete(peerStates, loser)
	*clock = clock.Add(time.Millisecond)
	for _, action := range actions(peerStates, requestsPartial) {
		require.NoError(t, action.Err)
	}
	deadline, ok := row.EarliestClaimDeadline()
	require.Equal(t, true, ok, "the winner's claim is still live")
	require.Equal(t, true, deadline.Equal(lapseAt.Add(claimTTL(1))), "no cancellation deadline survives the peer")
}

// TestRowCancellationBecomesMootWhenTheClaimComesBack: if the winner's claim lapses before the
// deferred removal is flushed, the part rotates back to the loser -- whose last-sent request set
// still carries it. Nothing is sent, because the peer's view is already right; the deferral is
// dropped, because there is nothing left to cancel; and the loser is charged a claim all the same,
// so the retry bound still applies to a peer that is asked this way and stays silent.
func TestRowCancellationBecomesMootWhenTheClaimComesBack(t *testing.T) {
	row, peerStates, actions, loser, clock := cancellationFixture(t)
	requestsPartial := func(peer.ID) bool { return true }
	lapseAt := *clock

	// Past the winner's claim, before the cancellation deadline.
	*clock = lapseAt.Add(claimTTL(1) + time.Millisecond)
	require.Equal(t, true, clock.Before(lapseAt.Add(cancellationMaxDelay())), "fixture premise")

	var packets int
	for _, action := range actions(peerStates, requestsPartial) {
		require.NoError(t, action.Err)
		if len(action.EncodedPartsMetadata) > 0 || len(action.EncodedPartialMessage) > 0 {
			packets++
			action.OnSent(true)
		}
	}
	// Whoever the part rotated to, it was a peer that already held that request in its view.
	require.Equal(t, 0, packets, "no packet: both peers' views already carry the request")

	// No cancellation deadline survives; the earliest deadline is a fresh claim on the re-asked
	// peer, charged without a packet.
	deadline, ok := row.EarliestClaimDeadline()
	require.Equal(t, true, ok)
	require.Equal(t, false, deadline.Before(*clock), "a claim was recorded for the re-ask")
	require.Equal(t, true, deadline.After(lapseAt.Add(cancellationMaxDelay())) || row.claims().pending[loser].cancelSince.IsZero(),
		"the deferred cancellation is gone")
	_ = loser
}

// TestColumnKeepsByteEqualityForRequests: the column axis is deliberately unchanged -- a request
// removal there still buys its own packet, until the mechanism reaches it in its own commit.
func TestColumnKeepsByteEqualityForRequests(t *testing.T) {
	column, err := NewPartialDataColumn(testRowRoot(), testSignedHeader(true, 96), 0, sizedSlices(testBlobCount, 48, 0x10), sizedSlices(kzgCommitmentsInclusionProofDepth, 32, 0x20))
	require.NoError(t, err)
	require.Equal(t, false, column.defersAnnouncements())
	row := testRow(t, 0)
	require.Equal(t, true, row.defersAnnouncements())
}

func TestPartialDataRowPeerStateIsSharedWithColumns(t *testing.T) {
	// Not a style choice: the gossipsub partial-messages extension carries one PeerState
	// type per host, so a single broadcaster serving both domains needs this to hold.
	row := testRow(t, 0)
	var _ = row.PublishActionsFn(nil, nil, nil)

	column, err := NewPartialDataColumn(testRowRoot(), testSignedHeader(true, 96), 0, sizedSlices(testBlobCount, 48, 0x10), sizedSlices(kzgCommitmentsInclusionProofDepth, 32, 0x20))
	require.NoError(t, err)

	rowFn := row.PublishActionsFn(nil, nil, nil)
	columnFn := column.PublishActionsFn(nil, nil, nil)
	require.Equal(t, true, rowFn != nil && columnFn != nil)

	// Both metadata forms are the same container, differing only in bitmap length.
	rowMeta, err := row.newPartsMetadata(nil)
	require.NoError(t, err)
	columnMeta, err := column.newPartsMetadata(nil)
	require.NoError(t, err)
	require.Equal(t, uint64(fieldparams.NumberOfColumns), rowMeta.Available.Len())
	require.Equal(t, uint64(testBlobCount), columnMeta.Available.Len())
}

func TestReconstructionThresholdIsHalfTheColumns(t *testing.T) {
	require.Equal(t, uint64(fieldparams.NumberOfColumns/2), ReconstructionThreshold())
	require.Equal(t, uint64(fieldparams.NumberOfColumns), numberOfColumns)
}

// TestPartialDataRow_WantedPartsUnderInterest is the rule that decides what the row axis costs.
//
// Below the reconstruction threshold a pooling node asks for everything missing, because any cell
// brings it closer and it cannot know which ones its peers hold. Once it can recover the row it
// asks only for the cells it keeps -- its own columns' -- because the rest it would neither store
// nor compute with, and recovery fills them for free.
//
// The first version of this asked for a *specific* subset up to the threshold and that was wrong:
// in R9's withholding shape it cut the nodes reaching the threshold from 8 to 1, because a blind
// subset asks for cells no peer holds while missing the ones they do.
func TestPartialDataRow_WantedPartsUnderInterest(t *testing.T) {
	row := testRow(t, 0)

	keep := testBitlist(fieldparams.NumberOfColumns, 0, 1, 2, 3)
	require.NoError(t, row.SetRequestInterest(keep, true))

	// Pooling, and far short of the threshold: ask for everything missing.
	wanted, err := row.wantedParts()
	require.NoError(t, err)
	require.Equal(t, uint64(fieldparams.NumberOfColumns), wanted.Count())

	// Carry it over the threshold. Now only the kept cells it still lacks are requested.
	for columnIndex := uint64(0); columnIndex < ReconstructionThreshold(); columnIndex++ {
		require.Equal(t, true, row.ExtendFromVerifiedCell(columnIndex, testCell(1), testProof(1)))
	}
	require.Equal(t, true, row.ReconstructionThresholdMet())
	wanted, err = row.wantedParts()
	require.NoError(t, err)
	require.Equal(t, uint64(0), wanted.Count(),
		"cells 0..3 are all held now, so a node that keeps only those asks for nothing")

	// A node that keeps a column it has not received still asks for that one, which is the path
	// diversity the row axis exists to provide.
	keep = testBitlist(fieldparams.NumberOfColumns, 0, uint64(fieldparams.NumberOfColumns-1))
	require.NoError(t, row.SetRequestInterest(keep, true))
	wanted, err = row.wantedParts()
	require.NoError(t, err)
	require.Equal(t, uint64(1), wanted.Count())
	require.Equal(t, true, wanted.BitAt(uint64(fieldparams.NumberOfColumns-1)))
}

// TestPartialDataRow_ReconstructorNeverPools: a node whose custody already covers the threshold
// asks only for its own columns' cells from the outset -- it is never below the threshold in the
// sense that matters, because it does not need foreign cells to recover. EIP-8371 says as much.
func TestPartialDataRow_ReconstructorNeverPools(t *testing.T) {
	row := testRow(t, 0)

	keep := testBitlist(fieldparams.NumberOfColumns, 7)
	require.NoError(t, row.SetRequestInterest(keep, false))

	// Empty row, so under the pooling rule this would be "everything missing".
	require.Equal(t, false, row.ReconstructionThresholdMet())
	wanted, err := row.wantedParts()
	require.NoError(t, err)
	require.Equal(t, uint64(1), wanted.Count(), "a non-pooling node asks only for what it keeps")
	require.Equal(t, true, wanted.BitAt(7))
}

// TestPartialDataRow_ExplicitRequestsBeatInterest: a caller that set its own request bitmap knows
// more about this row than the policy does -- the pull arm naming one cell, a recovered row asking
// for nothing.
func TestPartialDataRow_ExplicitRequestsBeatInterest(t *testing.T) {
	row := testRow(t, 0)

	require.NoError(t, row.SetRequestInterest(testBitlist(fieldparams.NumberOfColumns, 0, 1), true))
	require.NoError(t, row.SetPartsRequests(testBitlist(fieldparams.NumberOfColumns, 40)))

	wanted, err := row.wantedParts()
	require.NoError(t, err)
	require.Equal(t, uint64(1), wanted.Count())
	require.Equal(t, true, wanted.BitAt(40))
}

// TestPartialDataRow_SetRequestInterestLengthMismatch: a wrong-length interest is a caller bug,
// and it must be loud rather than silently narrowing to nothing.
func TestPartialDataRow_SetRequestInterestLengthMismatch(t *testing.T) {
	row := testRow(t, 0)
	require.ErrorContains(t, "request interest length mismatch",
		row.SetRequestInterest(testBitlist(testBlobCount), true))
}

// TestPartialDataRow_CloneCopiesInterest: the row is handed between goroutines by value, so the
// interest has to be deep-copied with the rest of it.
func TestPartialDataRow_CloneCopiesInterest(t *testing.T) {
	row := testRow(t, 0)
	require.NoError(t, row.SetRequestInterest(testBitlist(fieldparams.NumberOfColumns, 5), true))

	clone := row.Clone()
	keep, pooling, ok := clone.RequestInterest()
	require.Equal(t, true, ok)
	require.Equal(t, true, pooling)
	require.Equal(t, uint64(1), keep.Count())

	keep.SetBitAt(6, true)
	original, _, _ := clone.RequestInterest()
	require.Equal(t, uint64(1), original.Count(), "the returned bitlist must be a copy")
}
