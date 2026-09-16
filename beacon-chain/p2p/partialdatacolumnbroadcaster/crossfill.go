package partialdatacolumnbroadcaster

import (
	stderrors "errors"

	"github.com/OffchainLabs/prysm/v7/beacon-chain/verification"
	"github.com/OffchainLabs/prysm/v7/consensus-types/blocks"
	"github.com/pkg/errors"
)

// The cross-fill bridge. A verified cell is a coordinate (row, column) in the block's cell
// matrix, and both axes want it:
//
//   - a cell arriving on a row topic fills that column for us, possibly completing a column we
//     custody without any column-topic traffic at all;
//   - a cell arriving on a column topic fills our row, moving it towards the half-way point at
//     which the row can be recovered;
//   - a recovered row yields all 128 cells at once, which is how one node's reconstruction work
//     relieves the rest of the network.
//
// This is the mechanism RowDAS exists for, and the reason both axes live in one broadcaster:
// inside a single event loop it is a function call, and across two broadcasters it would be
// cross-goroutine plumbing over shared state.
//
// All of it runs on the event loop, which owns every column and row state, so nothing here
// locks.

// crossFillColumnsFromRow offers cells learned on a row topic to every column state we hold
// for the same block. rowIndex is the blob the cells belong to; each bundle's ColumnIndex says
// which column it is, which is the column state that wants it.
//
// For a column, the cell index is the blob index -- the transpose of the row case -- so a cell
// at (rowIndex, columnIndex) lands at index rowIndex of column columnIndex.
func (p *PartialColumnBroadcaster) crossFillColumnsFromRow(groupID []byte, rowIndex uint64, cells []blocks.CellProofBundle) error {
	if len(cells) == 0 {
		return nil
	}

	columns := p.columnStatesForGroup(groupID)
	if len(columns) == 0 {
		return nil
	}

	var aggErr error
	for _, bundle := range cells {
		state, ok := columns[bundle.ColumnIndex]
		if !ok {
			// We do not hold this column: it is not ours to custody, or we have not seen a
			// header on its topic. Pushing it out to peers who do is a separate step.
			continue
		}
		if rowIndex >= state.verifier.Column.KzgCommitmentCount() {
			// Cannot happen for a well-formed block, since both come from the same header.
			aggErr = stderrors.Join(aggErr, errors.Errorf("row %d beyond column %d cell count", rowIndex, bundle.ColumnIndex))
			continue
		}
		if !state.verifier.ExtendFromVerifiedCell(rowIndex, bundle.Cell, bundle.Proof) {
			continue
		}
		crossFilledCellsTotal.WithLabelValues("row_to_column").Inc()
		if err := p.afterColumnExtended(state.topic, groupID, state.verifier); err != nil {
			aggErr = stderrors.Join(aggErr, err)
		}
	}

	return aggErr
}

// crossFillRowFromWholeColumn offers every cell a column holds to the row states we hold for the
// same block. It exists for the same reason crossFillColumnsFromWholeRow does, in the other
// direction: crossFillRowFromColumn is reachable only from the *incoming* path, so cells a node
// supplied itself -- a proposer publishing its columns, a column rebuilt locally, a column from the
// execution client -- never seeded its row state at all.
//
// Measured cost of the gap, R12: with the proposer also custodying columns, the row axis was short
// exactly the proposer's cells and every node stalled 8 short of the 64-cell threshold, so nothing
// could reconstruct. The node holding *everything* was the one node unable to serve the row axis.
func (p *PartialColumnBroadcaster) crossFillRowFromWholeColumn(groupID []byte, column *blocks.PartialDataColumn) error {
	if p.rowCallbacks == nil || column == nil {
		return nil
	}

	blobIndices := make([]uint64, 0, column.Included.Count())
	cells := make([]blocks.CellProofBundle, 0, column.Included.Count())
	for blobIndex := range column.Included.Len() {
		if !column.Included.BitAt(blobIndex) {
			continue
		}
		blobIndices = append(blobIndices, blobIndex)
		cells = append(cells, blocks.CellProofBundle{
			ColumnIndex: column.Index(),
			Cell:        column.Column()[blobIndex],
			Proof:       column.KzgProofs()[blobIndex],
		})
	}
	if len(cells) == 0 {
		return nil
	}

	return p.crossFillRowFromColumn(groupID, column.Index(), blobIndices, cells)
}

// crossFillRowFromHeldColumns seeds a row from the columns this node already holds.
//
// The other direction of D13, and the one that does not depend on ordering. A cross-fill at publish
// time only helps if row state already exists when the column arrives, and it usually does not: a
// node creates column state on block arrival and row state when a row header reaches it, in either
// order. So a row created *after* its columns starts empty even though the node holds cells for it.
//
// Called where row state is created, for the same reason applyRowRequestPolicy is: what a row asks
// its peers for is decided from what it already has.
func (p *PartialColumnBroadcaster) crossFillRowFromHeldColumns(groupID []byte, row *blocks.PartialDataRow) {
	if row == nil {
		return
	}
	columns := p.columnStatesForGroup(groupID)
	if len(columns) == 0 {
		return
	}

	rowIndex := row.RowIndex()
	for columnIndex, state := range columns {
		column := state.verifier.Column
		if rowIndex >= column.Included.Len() || !column.Included.BitAt(rowIndex) {
			continue
		}
		if row.ExtendFromVerifiedCell(columnIndex, column.Column()[rowIndex], column.KzgProofs()[rowIndex]) {
			crossFilledCellsTotal.WithLabelValues("column_to_row").Inc()
		}
	}
}

// crossFillRowFromColumn offers cells learned on a column topic to the row state we hold for
// the same block. columnIndex is the column the cells came from; each cell index is a blob
// index, which is the row that wants it.
func (p *PartialColumnBroadcaster) crossFillRowFromColumn(groupID []byte, columnIndex uint64, blobIndices []uint64, cells []blocks.CellProofBundle) error {
	if p.rowCallbacks == nil || len(cells) == 0 {
		return nil
	}

	topic, entry := p.rowStateForGroup(groupID)
	if entry == nil {
		return nil
	}

	rowIndex := entry.row.RowIndex()
	var extended bool
	for i, bundle := range cells {
		if blobIndices[i] != rowIndex {
			// A column carries one cell of every blob; only the one belonging to our row is
			// of interest here.
			continue
		}
		if entry.row.ExtendFromVerifiedCell(columnIndex, bundle.Cell, bundle.Proof) {
			extended = true
			crossFilledCellsTotal.WithLabelValues("column_to_row").Inc()
		}
	}
	if !extended {
		return nil
	}

	p.notifyRowProgress(topic, entry)
	if !entry.row.Published {
		return nil
	}

	// Same availability-growth path as handlePartialRowCells, and the one the review flagged as
	// able to publish once per extended column. Coalesced for the same reason.
	groupID, row := entry.row.GroupID(), entry.row

	return p.publishPartialRow(topic, groupID, row)
}

// crossFillColumnsFromWholeRow offers every cell a row holds to the column states for the same
// block.
//
// The incoming path cross-fills the cells of each message as it arrives, which covers everything
// that came off the wire. It does not cover a row this node *recovered*: those cells were
// computed locally and never passed through a message, so without this the node would recover a
// row, serve it to peers, and still be missing the cells for the columns it custodies -- which is
// most of what reconstruction is for.
func (p *PartialColumnBroadcaster) crossFillColumnsFromWholeRow(groupID []byte, row *blocks.PartialDataRow) error {
	if row.Included.Count() == 0 {
		return nil
	}

	commitment := row.Commitment()
	cells := make([]blocks.CellProofBundle, 0, row.Included.Count())
	for _, columnIndex := range row.PresentColumnIndices() {
		cells = append(cells, blocks.CellProofBundle{
			ColumnIndex: columnIndex,
			Commitment:  commitment,
			Cell:        row.Cells[columnIndex],
			Proof:       row.Proofs[columnIndex],
		})
	}

	return p.crossFillColumnsFromRow(groupID, row.RowIndex(), cells)
}

// columnState pairs a column verifier with the topic it was learned on, which is what the
// completion and republish paths need.
type columnState struct {
	topic    string
	verifier *verification.PartialColumnVerifier
}

// columnStatesForGroup indexes the column states we hold for a group by column index.
func (p *PartialColumnBroadcaster) columnStatesForGroup(groupID []byte) map[uint64]columnState {
	var states map[uint64]columnState
	for topic, store := range p.partialMsgStore {
		verifier := store[string(groupID)]
		if verifier == nil {
			continue
		}
		if states == nil {
			states = make(map[uint64]columnState, len(p.partialMsgStore))
		}
		states[verifier.Column.Index()] = columnState{topic: topic, verifier: verifier}
	}

	return states
}

// rowStateForGroup returns the row we hold for a group. A node subscribes to a single row
// subnet, so there is at most one live entry per group; a stale topic from a fork rotation is
// possible until its group expires, and the first match is as good as any.
func (p *PartialColumnBroadcaster) rowStateForGroup(groupID []byte) (string, *rowEntry) {
	for topic, store := range p.rowStore {
		if entry := store[string(groupID)]; entry != nil {
			return topic, entry
		}
	}

	return "", nil
}

// afterColumnExtended runs the completion and republish steps a column needs once it has
// gained a cell, whichever axis the cell came from.
func (p *PartialColumnBroadcaster) afterColumnExtended(topic string, groupID []byte, verifier *verification.PartialColumnVerifier) error {
	column, complete, err := verifier.Complete()
	if err != nil {
		return errors.Wrap(err, "complete partial column verifier")
	}
	if complete {
		go p.callbacks.HandleColumn(topic, column)
	}

	if !verifier.Column.Published {
		p.recordRepublishSkip(groupID, verifier.Column.Index())
		return nil
	}

	return p.publishPartialCol(topic, verifier.Column.GroupID(), verifier.Column)
}
