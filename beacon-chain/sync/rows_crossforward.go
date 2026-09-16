package sync

// RowDAS (EIP-8371) cross-forwarding: the policy half.
//
// The broadcaster owns the mechanism -- build a single-cell partial column per target and push it
// on the column topic. What is left here is the part the EIP leaves to the implementation:
//
//	which columns   the ones this node does not custody. The ones it does custody are already
//	                filled by the cross-fill bridge and republished on their own subnets.
//	how many        EIP-8371 says a node MAY limit the push to a random subset. We push all of
//	                them by default; the cap exists so R9 can sweep it.
//	when            the EIP says a node MAY delay, to avoid competing with a subnet's own
//	                subscribers. We do not add a delay: the push happens only after a
//	                reconstruction, and a reconstruction has already waited out its phase delay,
//	                which is a longer wait than any push delay would be and is there for the
//	                same reason.
//
// Only a *reconstructed* row is pushed. A row that completed because its subnet delivered every
// cell needs no help from us: every other member of that subnet holds the same cells and would
// push the same columns, so pushing then would multiply the traffic by the subnet's size to no
// effect. Pushing only what we recovered makes the reconstructor the single source, which is the
// same argument that makes the reconstruction phases worth having.

import (
	"github.com/OffchainLabs/prysm/v7/beacon-chain/core/peerdas"
	fieldparams "github.com/OffchainLabs/prysm/v7/config/fieldparams"
	"github.com/OffchainLabs/prysm/v7/consensus-types/blocks"
	"github.com/sirupsen/logrus"
)

// rowCrossForwardMaxColumns caps how many non-custodied columns one recovered row is pushed into.
// Zero means no cap, which is the default: with 128 columns and a custody-minimum node the cap
// would have to bite at 4 to change anything, and the EIP's own reasoning is that the push is
// where the relief comes from. R9 is the experiment that should decide whether a cap is wanted.
const rowCrossForwardMaxColumns = 0

// crossForwardRecoveredRow pushes a row this node has just recovered into the column subnets it
// does not custody.
func (s *Service) crossForwardRecoveredRow(topic string, row *blocks.PartialDataRow) {
	broadcaster := s.cfg.p2p.PartialColumnBroadcaster()
	if broadcaster == nil {
		return
	}

	columns, err := s.nonCustodiedColumns()
	if err != nil {
		log.WithError(err).Debug("Could not determine non-custodied columns for cross-forwarding")
		return
	}
	if len(columns) == 0 {
		// A supernode custodies everything, so it has nothing to push: its own subnets already
		// carry every cell it recovered.
		return
	}

	pushed, err := broadcaster.CrossForwardRow(s.ctx, topic, row, columns)
	if err != nil {
		log.WithError(err).WithField("rowIndex", row.RowIndex()).Debug("Could not cross-forward row")
		return
	}

	log.WithFields(logrus.Fields{
		"rowIndex": row.RowIndex(),
		"columns":  pushed,
	}).Debug("Cross-forwarded a recovered row into non-custodied column subnets")
}

// pullRowFromColumns asks the column subnets this node does not custody for the cells its row is
// missing. EIP-8371's optional direction, off unless --row-das-pull is set.
//
// Only worth doing while the row is short of the reconstruction threshold: past it the row can be
// recovered locally, and pulling more cells buys nothing but traffic on subnets we do not serve.
// That is also why this is aimed at custody-minimum nodes -- a reconstructor holds a cell of every
// row from its own custody and has nothing to pull.
func (s *Service) pullRowFromColumns(topic string, groupID []byte, rowIndex uint64) {
	if !s.cfg.p2p.RowDASPullEnabled() {
		return
	}
	broadcaster := s.cfg.p2p.PartialColumnBroadcaster()
	if broadcaster == nil {
		return
	}

	row, err := broadcaster.RowSnapshot(s.ctx, topic, groupID)
	if err != nil || row == nil {
		return
	}
	if row.ReconstructionThresholdMet() {
		return
	}

	columns, err := s.nonCustodiedColumns()
	if err != nil {
		log.WithError(err).Debug("Could not determine non-custodied columns to pull from")
		return
	}
	if len(columns) == 0 {
		return
	}

	pulled, err := broadcaster.PullRowFromColumns(s.ctx, topic, row, columns)
	if err != nil {
		log.WithError(err).WithField("rowIndex", rowIndex).Debug("Could not pull row cells from column subnets")
		return
	}
	if pulled == 0 {
		return
	}

	log.WithFields(logrus.Fields{
		"rowIndex": rowIndex,
		"columns":  pulled,
		"have":     row.Included.Count(),
	}).Debug("Asked non-custodied column subnets for a row's missing cells")
}

// nonCustodiedColumns lists the columns this node does not custody, in ascending order, capped at
// rowCrossForwardMaxColumns.
//
// Ascending rather than shuffled: the cap is off by default, and a deterministic order makes the
// push reproducible under the harness's common-random-numbers discipline. If R9 turns the cap on,
// the choice of subset becomes load-bearing and wants shuffling by node identity -- so that
// different nodes cover different columns rather than all picking the same prefix.
func (s *Service) nonCustodiedColumns() ([]uint64, error) {
	samplingSize, err := s.samplingSize()
	if err != nil {
		return nil, err
	}
	info, _, err := peerdas.Info(s.cfg.p2p.NodeID(), samplingSize)
	if err != nil {
		return nil, err
	}

	columns := make([]uint64, 0, fieldparams.NumberOfColumns)
	for columnIndex := range uint64(fieldparams.NumberOfColumns) {
		if info.CustodyColumns[columnIndex] {
			continue
		}
		columns = append(columns, columnIndex)
		if rowCrossForwardMaxColumns > 0 && len(columns) == rowCrossForwardMaxColumns {
			break
		}
	}

	return columns, nil
}
