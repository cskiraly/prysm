package partialdatacolumnbroadcaster

import (
	"fmt"
	"testing"

	"github.com/OffchainLabs/prysm/v7/beacon-chain/verification"
	fieldparams "github.com/OffchainLabs/prysm/v7/config/fieldparams"
	"github.com/OffchainLabs/prysm/v7/consensus-types/blocks"
	"github.com/OffchainLabs/prysm/v7/testing/require"
)

// seedColumn puts a column state for the same block as testRowHeader into the store, so the
// cross-fill has something to fill. The column's cells are indexed by blob, so it has
// testRowBlobCount of them.
func seedColumn(t *testing.T, h *rowHarness, columnIndex uint64, published bool) (topic string, verifier *verification.PartialColumnVerifier) {
	t.Helper()

	header, root := testRowHeader(t)
	column, err := blocks.NewPartialDataColumn(root, header.SignedBlockHeader, columnIndex, header.KzgCommitments, header.KzgCommitmentsInclusionProof)
	require.NoError(t, err)
	column.Published = published

	topic = fmt.Sprintf("/eth2/abcd1234/data_column_sidecar_%d/ssz_snappy", columnIndex)
	h.broadcaster.topics[topic] = nil
	h.broadcaster.subscribedTopics.Store(topic, struct{}{})

	v := newMockPartialVerifier(&column)
	store, ok := h.broadcaster.partialMsgStore[topic]
	if !ok {
		store = make(map[string]*verification.PartialColumnVerifier)
		h.broadcaster.partialMsgStore[topic] = store
	}
	store[string(column.GroupID())] = v

	return topic, v
}

func TestCrossFillColumnsFromRow(t *testing.T) {
	t.Run("a row cell fills the column we custody", func(t *testing.T) {
		h := newRowHarness(t)
		rowTopic, groupID, _ := seedRow(t, h, 2)
		_, verifier := seedColumn(t, h, 7, true)

		// One cell of row 2, at column 7.
		cells := rowCellsFor(rowTopic, groupID, 7, 8)
		require.NoError(t, h.broadcaster.handleRowCellsValidated(cells))

		// For a column, the cell index is the blob index, so row 2's cell lands at index 2.
		require.Equal(t, true, verifier.Column.Included.BitAt(2))
		require.Equal(t, uint64(1), verifier.Column.Included.Count())
	})

	t.Run("cells for columns we do not hold are skipped", func(t *testing.T) {
		h := newRowHarness(t)
		rowTopic, groupID, entry := seedRow(t, h, 1)
		_, verifier := seedColumn(t, h, 7, true)

		// Columns 0..5, none of which is 7.
		require.NoError(t, h.broadcaster.handleRowCellsValidated(rowCellsFor(rowTopic, groupID, 0, 6)))

		require.Equal(t, uint64(0), verifier.Column.Included.Count())
		require.Equal(t, uint64(6), entry.row.Included.Count(), "the row still took them")
	})

	t.Run("a row that completes a column completes it once", func(t *testing.T) {
		// A column of a 4-blob block needs 4 cells, i.e. four different rows. One row can
		// only ever contribute one of them, so completion here is via publishRowOnLoop
		// feeding the other three in.
		h := newRowHarness(t)
		_, verifier := seedColumn(t, h, 3, true)
		header, root := testRowHeader(t)

		for rowIndex := range uint64(testRowBlobCount) {
			rowTopic, subnet := rowTopicFor(t, rowIndex)
			h.subscribe(rowTopic)
			rpc := rowRPC(t, rowTopic, subnet, root, rowMessage(rowIndex, header))
			require.NoError(t, h.broadcaster.handleIncomingRowRPC(rpc))

			entry := h.broadcaster.getRowEntry(rowTopic, rpc.GroupID)
			require.NotNil(t, entry)
			entry.row.Published = true
			require.NoError(t, h.broadcaster.handleRowCellsValidated(rowCellsFor(rowTopic, rpc.GroupID, 3, 4)))
		}

		require.Equal(t, uint64(testRowBlobCount), verifier.Column.Included.Count())
		require.Equal(t, true, verifier.Column.IsComplete())
	})

	t.Run("re-offered cells do not double count", func(t *testing.T) {
		h := newRowHarness(t)
		rowTopic, groupID, _ := seedRow(t, h, 0)
		_, verifier := seedColumn(t, h, 5, true)

		require.NoError(t, h.broadcaster.handleRowCellsValidated(rowCellsFor(rowTopic, groupID, 5, 6)))
		require.Equal(t, uint64(1), verifier.Column.Included.Count())

		require.NoError(t, h.broadcaster.handleRowCellsValidated(rowCellsFor(rowTopic, groupID, 5, 6)))
		require.Equal(t, uint64(1), verifier.Column.Included.Count())
	})

	t.Run("no column state is not an error", func(t *testing.T) {
		h := newRowHarness(t)
		rowTopic, groupID, _ := seedRow(t, h, 0)
		require.NoError(t, h.broadcaster.handleRowCellsValidated(rowCellsFor(rowTopic, groupID, 0, 4)))
	})
}

func TestCrossFillRowFromColumn(t *testing.T) {
	t.Run("a column cell fills our row", func(t *testing.T) {
		h := newRowHarness(t)
		_, groupID, entry := seedRow(t, h, 2)
		columnTopic, _ := seedColumn(t, h, 11, true)

		// A column message carrying the cell of blob 2 -- our row -- and of blob 0, which is
		// not.
		cells := &cellsValidated{
			topic:       columnTopic,
			group:       groupID,
			cellIndices: []uint64{0, 2},
			cells: []blocks.CellProofBundle{
				{ColumnIndex: 11, Cell: rowCellBytes(1), Proof: make([]byte, 48)},
				{ColumnIndex: 11, Cell: rowCellBytes(2), Proof: make([]byte, 48)},
			},
		}
		require.NoError(t, h.broadcaster.handleCellsValidated(cells))

		require.Equal(t, true, entry.row.Included.BitAt(11), "the cell of our blob at column 11")
		require.Equal(t, uint64(1), entry.row.Included.Count(), "the cell of blob 0 is not ours")
		require.DeepEqual(t, rowCellBytes(2), entry.row.Cells[11])
	})

	t.Run("a column carrying nothing for our row leaves it alone", func(t *testing.T) {
		h := newRowHarness(t)
		_, groupID, entry := seedRow(t, h, 3)
		columnTopic, _ := seedColumn(t, h, 4, true)

		cells := &cellsValidated{
			topic:       columnTopic,
			group:       groupID,
			cellIndices: []uint64{0, 1},
			cells: []blocks.CellProofBundle{
				{ColumnIndex: 4, Cell: rowCellBytes(1), Proof: make([]byte, 48)},
				{ColumnIndex: 4, Cell: rowCellBytes(2), Proof: make([]byte, 48)},
			},
		}
		require.NoError(t, h.broadcaster.handleCellsValidated(cells))

		require.Equal(t, uint64(0), entry.row.Included.Count())
	})

	t.Run("with rows disabled nothing happens", func(t *testing.T) {
		h := newRowHarness(t)
		_, groupID, entry := seedRow(t, h, 3)
		columnTopic, _ := seedColumn(t, h, 4, true)
		h.broadcaster.rowCallbacks = nil

		cells := &cellsValidated{
			topic:       columnTopic,
			group:       groupID,
			cellIndices: []uint64{3},
			cells:       []blocks.CellProofBundle{{ColumnIndex: 4, Cell: rowCellBytes(9), Proof: make([]byte, 48)}},
		}
		require.NoError(t, h.broadcaster.handleCellsValidated(cells))

		require.Equal(t, uint64(0), entry.row.Included.Count())
	})
}

// TestCrossFillReachesTheReconstructionThreshold is the shape RowDAS depends on: a node
// custodying a handful of columns reaches the row threshold only by pooling cells over the row
// topic, and its own columns contribute what they can.
func TestCrossFillReachesTheReconstructionThreshold(t *testing.T) {
	h := newRowHarness(t)
	rowTopic, groupID, entry := seedRow(t, h, 2)

	// Our own custody contributes column 0 via the column axis.
	columnTopic, _ := seedColumn(t, h, 0, true)
	require.NoError(t, h.broadcaster.handleCellsValidated(&cellsValidated{
		topic:       columnTopic,
		group:       groupID,
		cellIndices: []uint64{2},
		cells:       []blocks.CellProofBundle{{ColumnIndex: 0, Cell: rowCellBytes(0), Proof: make([]byte, 48)}},
	}))
	require.Equal(t, uint64(1), entry.row.Included.Count())

	// The row topic supplies the rest, as pooled custody would.
	threshold := blocks.ReconstructionThreshold()
	h.callbacks.notifiedWait.Add(1)
	require.NoError(t, h.broadcaster.handleRowCellsValidated(rowCellsFor(rowTopic, groupID, 1, threshold)))
	h.callbacks.notifiedWait.Wait()

	require.Equal(t, threshold, entry.row.Included.Count())
	recoverable, complete := h.callbacks.snapshot()
	require.DeepEqual(t, []uint64{2}, recoverable)
	require.Equal(t, 0, len(complete))
}

func TestColumnStatesForGroupIndexesByColumnIndex(t *testing.T) {
	h := newRowHarness(t)
	_, first := seedColumn(t, h, 3, false)
	_, second := seedColumn(t, h, 9, false)

	states := h.broadcaster.columnStatesForGroup(first.Column.GroupID())
	require.Equal(t, 2, len(states))
	require.Equal(t, uint64(3), states[3].verifier.Column.Index())
	require.Equal(t, uint64(9), states[9].verifier.Column.Index())
	_ = second

	// A different group sees nothing.
	var other [fieldparams.RootLength]byte
	other[0] = 0x33
	require.Equal(t, 0, len(h.broadcaster.columnStatesForGroup(groupIDForRoot(other))))
}

// TestPublishedRowFillsOurColumns covers the case the incoming path cannot reach: a row this
// node recovered locally. Its cells were computed rather than received, so without a cross-fill
// at publish time the node would recover a row, serve it to peers, and still be missing the
// cells for the columns it custodies -- which is most of what reconstruction is for.
func TestPublishedRowFillsOurColumns(t *testing.T) {
	h := newRowHarness(t)
	header, root := testRowHeader(t)
	rowTopic, _ := rowTopicFor(t, 2)
	h.subscribe(rowTopic)

	_, held := seedColumn(t, h, 11, true)
	require.Equal(t, uint64(0), held.Column.Included.Count())

	// A fully recovered row 2, as RecoverRow leaves it -- never having passed through a message.
	recovered, err := blocks.NewPartialDataRow(root, header.SignedBlockHeader, 2,
		header.KzgCommitments, header.KzgCommitmentsInclusionProof)
	require.NoError(t, err)
	for columnIndex := range uint64(fieldparams.NumberOfColumns) {
		require.Equal(t, true, recovered.ExtendFromVerifiedCell(columnIndex, rowCellBytes(byte(columnIndex)), make([]byte, 48)))
	}

	require.NoError(t, h.broadcaster.publishRowOnLoop(rowTopic, recovered))

	// Row 2's cell of column 11 is now in the column, at cell index 2 -- the blob index.
	require.Equal(t, true, held.Column.Included.BitAt(2), "the recovered row should fill our column")
	require.Equal(t, uint64(1), held.Column.Included.Count(), "one row contributes exactly one of the column's cells")
}
