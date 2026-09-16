package sync

// The RowDAS (EIP-8371) side of the partial broadcaster's callbacks.
//
// Header validation is deliberately the *column* path unchanged. The header container is the
// same, its checks are the same, and the partial-columns spec says a header validated on any
// subnet may be used for all subnets -- so a row header goes through
// validatePartialDataColumnHeader, and the only thing this file adds is the throwaway column it
// needs as a carrier.

import (
	"time"

	"github.com/OffchainLabs/prysm/v7/beacon-chain/core/peerdas"
	"github.com/OffchainLabs/prysm/v7/consensus-types/blocks"
	ethpb "github.com/OffchainLabs/prysm/v7/proto/prysm/v1alpha1"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// rowCallbacks implements partialdatacolumnbroadcaster.RowCallbacks.
type rowCallbacks struct {
	service *Service
}

// ValidateRowHeader runs the same checks the column path runs, via a throwaway partial column
// built from the header.
//
// The column index it is built with is irrelevant: PartialColumnRequirements excludes
// RequireCorrectSubnet precisely because a partial-message header is not tied to the subnet it
// arrived on. Using index 0 is therefore not a shortcut -- there is no index that would be more
// correct, because the header does not mention one.
func (c *rowCallbacks) ValidateRowHeader(header *ethpb.PartialDataColumnHeader) (pubsub.ValidationResult, error) {
	if header == nil || header.SignedBlockHeader == nil || header.SignedBlockHeader.Header == nil {
		return pubsub.ValidationReject, errHeaderNil
	}
	if len(header.KzgCommitments) == 0 {
		return pubsub.ValidationReject, errHeaderEmptyCommitments
	}

	root, err := header.SignedBlockHeader.Header.HashTreeRoot()
	if err != nil {
		return pubsub.ValidationReject, errors.Wrap(err, "header hash tree root")
	}

	carrier, err := blocks.NewPartialDataColumn(root, header.SignedBlockHeader, 0,
		header.KzgCommitments, header.KzgCommitmentsInclusionProof)
	if err != nil {
		return pubsub.ValidationReject, errors.Wrap(err, "new partial data column from row header")
	}

	_, result, err := c.service.validatePartialDataColumnHeader(c.service.ctx, &carrier)
	if err == nil && result == pubsub.ValidationAccept {
		// A valid block header for the slot, which is what EIP-8371 measures the reconstruction
		// phase delays from and what decides which root the slot's duties attach to. On a row
		// topic this is usually the earliest such moment, because a row's cells cannot be used
		// before its header is validated.
		c.service.noteRowDutyRoot(header.SignedBlockHeader.Header.Slot, root)
	}

	return result, err
}

// ValidateRowCells batch-verifies the KZG proofs of row cells. Both DAS axes reduce to the same
// call: a row's bundles share one commitment and vary in cell index.
func (c *rowCallbacks) ValidateRowCells(cells []blocks.CellProofBundle) error {
	return peerdas.VerifyCellsKZGProofs(cells)
}

// RowStarted is where the pull arm decides. Deliberately delayed rather than immediate: the row's
// own subnet is the cheap source and should be given time to deliver, and by the time the delay
// expires the row may need nothing. pullRowFromColumns re-checks that before asking anyone.
func (c *rowCallbacks) RowStarted(topic string, groupID []byte, rowIndex uint64) {
	if !c.service.cfg.p2p.RowDASPullEnabled() {
		return
	}
	time.AfterFunc(slotFraction(rowPullDelayBPS), func() {
		c.service.pullRowFromColumns(topic, groupID, rowIndex)
	})
}

// RowRecoverable is the trigger for the reconstruction duties: this node now holds enough cells
// of the row to recover the rest. It does not recover here -- the phases decide whether and when,
// and cancel if the row completes by other means.
func (c *rowCallbacks) RowRecoverable(topic string, groupID []byte, rowIndex uint64) {
	c.service.scheduleRowReconstruction(topic, groupID, rowIndex)
}

// RowServedElsewhere means a peer holds this whole row and we want nothing further from it. The
// scheduler demotes an urgent recovery to the opportunistic phase rather than cancelling, because
// the claim is unverified; see rows_reconstruct.go.
func (c *rowCallbacks) RowServedElsewhere(topic string, groupID []byte, rowIndex uint64) {
	c.service.rowServedElsewhere(groupID, rowIndex)
	log.WithFields(logrus.Fields{
		"topic":    topic,
		"rowIndex": rowIndex,
	}).Debug("Row observed as served elsewhere")
}

// RowGroupEvicted drops the scheduler's per-group state along with the broadcaster's. Called on the
// broadcaster's event loop, so it does nothing but take the scheduler's own lock.
func (c *rowCallbacks) RowGroupEvicted(groupID []byte) {
	c.service.rowReconstruction.evictGroup(groupID)
}

// RowComplete means the row needed no reconstruction at all, or somebody else's reached us
// first. Either way any pending recovery for it is wasted work.
func (c *rowCallbacks) RowComplete(topic string, groupID []byte, rowIndex uint64) {
	c.service.cancelRowReconstruction(groupID, rowIndex)
	log.WithFields(logrus.Fields{
		"topic":    topic,
		"rowIndex": rowIndex,
	}).Debug("Row complete without local reconstruction")
}
