package sync

// What a node asks its row-topic peers for.
//
// "Everything I lack" is the column axis's answer and it is right there: a node subscribed to a
// column subnet stores every cell of every blob for that column, so every cell it lacks is a cell
// it will keep. A node does not store a row -- only the cells of its own columns. So this reports
// two things and `PartialDataRow.wantedParts` combines them:
//
//   - `keep`, the cells of the columns this node custodies. It stores those, and a second route to
//     them is the row axis's latency benefit (R2).
//   - `poolToThreshold`, whether it still needs foreign cells to be able to recover the row at
//     all. False for a node whose custody already covers the threshold, which EIP-8371 says needs
//     no foreign row information: "a row reconstructor can satisfy this phase from its own column
//     subscriptions, without foreign row information."
//
// A pooling node therefore asks widely until it can recover, and only for its own columns' cells
// after that. Asking for a *specific* subset up to the threshold was the first attempt and it is
// wrong: R9's withholding shape cut the nodes reaching the threshold from 8 to 1, because a blind
// subset asks for cells no peer holds while missing the ones they do.
//
// Deliberately not included here: delaying the request so the column path gets first refusal.
// That trades latency for bytes and needs the frontier measured rather than guessed; it is R7's
// business.

import (
	"github.com/OffchainLabs/go-bitfield"
	"github.com/OffchainLabs/prysm/v7/beacon-chain/core/peerdas"
	fieldparams "github.com/OffchainLabs/prysm/v7/config/fieldparams"
	"github.com/sirupsen/logrus"
)

// RowRequestInterest implements partialdatacolumnbroadcaster.RowCallbacks.
func (c *rowCallbacks) RowRequestInterest(rowIndex uint64) (bitfield.Bitlist, bool) {
	keep, poolToThreshold, err := c.service.rowRequestInterest()
	if err != nil {
		// Falling back to nil asks for everything, which is worse for bytes and better for
		// liveness. A custody lookup that fails is not a reason to stop fetching a row.
		log.WithError(err).WithField("rowIndex", rowIndex).
			Debug("Could not scope row cell requests; asking for everything missing")
		return nil, false
	}

	return keep, poolToThreshold
}

// rowRequestInterest reports the cells this node keeps -- those of its custodied columns -- and
// whether it still needs foreign cells to reach the reconstruction threshold.
func (s *Service) rowRequestInterest() (bitfield.Bitlist, bool, error) {
	samplingSize, err := s.samplingSize()
	if err != nil {
		return nil, false, err
	}
	info, _, err := peerdas.Info(s.cfg.p2p.NodeID(), samplingSize)
	if err != nil {
		return nil, false, err
	}

	keep := bitfield.NewBitlist(fieldparams.NumberOfColumns)
	for column := range info.CustodyColumns {
		if column < fieldparams.NumberOfColumns {
			keep.SetBitAt(column, true)
		}
	}

	held := uint64(len(info.CustodyColumns))
	// A node at or above the threshold from custody alone never pools: cross-fill from its own
	// columns carries the row over the line without a single row-topic request. This is the case
	// the EIP describes outright.
	poolToThreshold := !peerdas.IsRowReconstructor(held)

	log.WithFields(logrus.Fields{
		"custodied":       held,
		"poolToThreshold": poolToThreshold,
	}).Debug("Scoped row cell requests")

	return keep, poolToThreshold, nil
}
