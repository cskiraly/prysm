package partialdatacolumnbroadcaster

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/OffchainLabs/go-bitfield"
	"github.com/OffchainLabs/prysm/v7/beacon-chain/core/peerdas"
	"github.com/OffchainLabs/prysm/v7/consensus-types/blocks"
	"github.com/OffchainLabs/prysm/v7/consensus-types/primitives"
	ethpb "github.com/OffchainLabs/prysm/v7/proto/prysm/v1alpha1"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pubsub_pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// RowCallbacks is what the broadcaster needs from the application to serve RowDAS (EIP-8371)
// row topics. A nil RowCallbacks turns row topics off: incoming row messages are ignored and
// no row state is allocated.
//
// There is deliberately no row equivalent of PartialVerifierFromHeader. The header container
// and its checks are identical to the column case, and the partial-columns spec says a header
// validated on any subnet may be used for all subnets, so ValidateRowHeader is expected to run
// the same checks the column path runs.
type RowCallbacks interface {
	// ValidateRowHeader validates a header first seen on a row topic.
	// Returns (result, err) where:
	//   - ValidationReject, err!=nil: peer should be penalized
	//   - ValidationIgnore, err!=nil: don't penalize, just ignore
	//   - ValidationAccept, err=nil: header is valid
	ValidateRowHeader(header *ethpb.PartialDataColumnHeader) (pubsub.ValidationResult, error)
	// ValidateRowCells validates the KZG proofs of the given cells. Every bundle carries the
	// row's single commitment and its own cell index, which is the cell's column.
	ValidateRowCells(cells []blocks.CellProofBundle) error
	// RowStarted is called once per row, when a validated header first identifies it -- before
	// any cell of it has necessarily arrived. It is the hook for deciding to pull the row's
	// missing cells from elsewhere, which is a decision worth making early or not at all.
	RowStarted(topic string, groupID []byte, rowIndex uint64)
	// RowRecoverable is called once per row, the first time it holds enough cells to recover
	// the rest. It is the trigger for the reconstruction duties; the callback decides whether
	// and when to act, and fetches the cells with RowSnapshot if it does.
	RowRecoverable(topic string, groupID []byte, rowIndex uint64)
	// RowComplete is called once per row, when every cell is present.
	RowComplete(topic string, groupID []byte, rowIndex uint64)
	// RowServedElsewhere is called once per row, the first time a peer's availability bitmap
	// claims every cell of it *and* this node has nothing further it wants from the row.
	//
	// This is the cancellation signal the reconstruction phases lack. EIP-8371's own rationale
	// for placing the stronger phase-2 duty on supernodes is that "only a node subscribed to all
	// column subnets can observe rows completing elsewhere and cancel the redundant work" -- this
	// is that observation, and it arrives in about one round trip where waiting for the row to
	// complete locally takes longer than the whole phase-1 window, or never.
	//
	// The claim is unverified, which is why both halves are required and why the callback should
	// treat it as a reason to push the work later rather than to drop it. See notes/rowdas/TODO.md
	// D5.
	RowServedElsewhere(topic string, groupID []byte, rowIndex uint64)
	// RowRequestInterest reports what this node can use of a row: `keep` is the cells it
	// stores, i.e. the cells of the columns it custodies, and `poolToThreshold` is whether it
	// still needs foreign cells to be able to recover the row at all.
	//
	// Policy, and therefore the caller's: the broadcaster knows neither what this node
	// custodies nor how much of it. It exists because "everything I lack" is the column axis's
	// answer -- right there, since a node stores every cell of a column it subscribes to, and
	// wrong on the row axis, where a node stores none of the row outside its own columns.
	//
	// A nil `keep` means no policy, and the row asks for everything it lacks.
	RowRequestInterest(rowIndex uint64) (keep bitfield.Bitlist, poolToThreshold bool)
	// RowGroupEvicted is called when the broadcaster drops every trace of a group, once its TTL
	// expires. It exists because the application keeps its own per-group state -- the
	// reconstruction scheduler's completed and served-elsewhere sets -- whose comments claimed to
	// be "cleared with the group's TTL by the broadcaster" while nothing actually cleared them.
	// Without this hook that state grows for the lifetime of the process.
	//
	// Called once per group, on the broadcaster's event loop and *synchronously*, unlike the
	// per-row notifications above. Synchronously because the alternative races: a late eviction
	// notice arriving after the same group was rebuilt would wipe the new group's state. An
	// implementation must therefore not call back into the broadcaster, and is expected to do no
	// more than drop its own map entries.
	RowGroupEvicted(groupID []byte)
}

// Bounding row state against equivocation.
//
// A row group is allocated for any block whose header validates, and a header validates for any
// block the slot's proposer signed -- including every equivocating one. The reconstruction duty
// ledger already declines *duties* for competing roots (EIP-8371 line 102), but row state -- cells,
// claims, timers, peer bitmaps, and the publishing they drive -- was allocated earlier, in
// handleIncomingRowRPC, for every one of them. TTL was the only bound.
//
// Two bounds, both on the store rather than on the source, so our own publishes are held to them
// too. Per slot: a legitimate slot has one block, so a second root is only ever a reorg's or an
// equivocator's; we keep one competitor in case the first-seen root loses, and refuse beyond that.
// Per topic: the belt for the per-slot bound, which depends on header validation confining slots to
// a window. A refused group is not downscored -- the peer relayed a valid header -- but it gets no
// state, no callbacks, and nothing is published for it; the column path still carries the block,
// which is what EIP-8371 line 171 says the row axis may fall back to.
//
// The extension's own PeerInitiatedGroupLimitPerTopic (255) and per-peer limit (8) bound its state
// independently; these are smaller because ours holds cells. Vars, not consts, so a test can lower
// them and R-experiments can sweep them.
var (
	MaxRowGroupsPerTopic = 32
	MaxRowGroupsPerSlot  = 2
)

// rowGroupAdmissible reports whether a new row group for a block at slot may be allocated on
// topic, and if not, which bound refused it. Runs on the event loop.
func (p *PartialColumnBroadcaster) rowGroupAdmissible(topic string, slot primitives.Slot) (bool, string) {
	topicStore := p.rowStore[topic]
	if len(topicStore) >= MaxRowGroupsPerTopic {
		return false, "topic"
	}
	atSlot := 0
	for _, entry := range topicStore {
		if entry.row.Slot() == slot {
			atSlot++
		}
	}
	if atSlot >= MaxRowGroupsPerSlot {
		return false, "slot"
	}

	return true, ""
}

// refuseRowGroup records a refusal. The group keeps a TTL entry so anything cached under its id
// before the refusal -- the validated header -- ages out with the groups that were admitted.
func (p *PartialColumnBroadcaster) refuseRowGroup(topic string, groupID []byte, slot primitives.Slot, bound string) {
	if _, ok := p.groupTTL[string(groupID)]; !ok {
		p.groupTTL[string(groupID)] = TTLInSlots
	}
	partialMessageRowGroupsRefusedTotal.WithLabelValues(bound).Inc()
	p.logger.WithFields(logrus.Fields{
		"topic": topic,
		"group": fmt.Sprintf("%#x", groupID),
		"slot":  slot,
		"bound": bound,
	}).Debug("Refusing row group: bound reached")
}

// rowEntry is the broadcaster's per-(topic, group) row state. The notification flags are
// bookkeeping rather than row data, which is why they live here and not on PartialDataRow.
type rowEntry struct {
	row                 *blocks.PartialDataRow
	notifiedRecoverable bool
	notifiedComplete    bool
	// peerHasWholeRow records that some peer's availability bitmap has claimed every cell of
	// this row. Kept on the entry rather than acted on immediately, because the other half of
	// the signal -- that we want nothing further -- becomes true at a different moment.
	peerHasWholeRow bool
	// notifiedServedElsewhere is the once-per-row latch for RowServedElsewhere.
	notifiedServedElsewhere bool
}

// rowPeerHasWhole says that a peer's parts metadata claimed every cell of a row. Carried to the
// event loop because the row state it applies to is loop-owned.
type rowPeerHasWhole struct {
	topic   string
	groupID []byte
	from    peer.ID
}

// incomingRowRPC is a row-topic partial message that has passed the cheap checks on the
// pubsub goroutine and is queued for the event loop.
type incomingRowRPC struct {
	*pubsub_pb.PartialMessagesExtension
	from      peer.ID
	message   *ethpb.PartialDataRowSidecar
	rowSubnet uint64
	root      [32]byte
}

func (r incomingRowRPC) logFields() logrus.Fields {
	return logrus.Fields{
		"from":      r.from,
		"topic":     r.GetTopicID(),
		"group":     fmt.Sprintf("%#x", r.GroupID),
		"rowSubnet": r.rowSubnet,
	}
}

// rowCellsValidated carries verified row cells back to the event loop.
type rowCellsValidated struct {
	validationTook time.Duration
	topic          string
	group          []byte
	columnIndices  []uint64
	cells          []blocks.CellProofBundle
}

func (c *rowCellsValidated) logFields() logrus.Fields {
	return logrus.Fields{
		"topic": c.topic,
		"group": fmt.Sprintf("%#x", c.group),
	}
}

// rowSnapshotRequest asks the event loop for a deep copy of a row. The loop owns all row
// state, so a caller on another goroutine cannot read it directly.
type rowSnapshotRequest struct {
	topic   string
	groupID []byte
	out     *blocks.PartialDataRow
}

var (
	errRowsDisabled       = errors.New("row callbacks not configured")
	errRowGroupIDNotFulu  = errors.New("row group ids use the fulu form only")
	errRowSubnetCarriesNo = errors.New("row subnet carries no blob at this slot")
	errRowSubnetMultiRow  = errors.New("row subnet carries more than one blob at this slot")
	errRowGroupCapReached = errors.New("row group cap reached")

	// errRowGloasTopicUnsupported is returned for a row topic on a gloas fork digest. Rows
	// still emit the Fulu group id form, and accepting it on a gloas topic would break
	// cross-fill silently rather than loudly.
	errRowGloasTopicUnsupported = errors.New("row topics are not supported on a gloas fork digest")
)

// onIncomingRowRPC is the row half of the pubsub-goroutine callback. It must stay fast and
// non-blocking: it does the cheap peer-attributable checks, updates the peer state the
// extension owns, and queues the rest for the event loop.
func (p *PartialColumnBroadcaster) onIncomingRowRPC(
	from peer.ID,
	peerStates map[peer.ID]blocks.PartialDataColumnPeerState,
	rpc *pubsub_pb.PartialMessagesExtension,
	rowSubnet uint64,
) error {
	if p.rowCallbacks == nil {
		return nil
	}

	topicID := rpc.GetTopicID()
	isGloas, _, root, err := blocks.ParsePartialColumnGroupID(rpc.GetGroupID())
	if err != nil {
		p.logger.WithError(err).WithFields(logrus.Fields{
			"peer":  from,
			"topic": topicID,
		}).Debug("Invalid row group ID")
		p.reportPeerFeedbackAsync(topicID, from, pubsub.PeerFeedbackInvalidMessage)
		return errors.Wrap(err, "parse row group id")
	}
	if isGloas {
		// Rows share the block's column group id, and the Gloas column form is not emitted
		// for rows yet. See notes/rowdas/design.md section 4.
		p.logger.WithFields(logrus.Fields{"peer": from, "topic": topicID}).Debug("Ignoring gloas-form row group id")
		return errRowGroupIDNotFulu
	}
	// Refuse rows on a Gloas-digest topic outright rather than accepting the Fulu group id form
	// there. Accepting it would store the row under 0x00 || root while the same block's columns
	// live under 0x01 || SSZ(root, slot), so cross-fill -- which matches group id bytes exactly
	// -- would silently find nothing. A loud refusal is the honest state of the code until rows
	// track the column group id per fork.
	topicIsGloas, err := topicForkIsGloas(topicID)
	if err != nil {
		return errors.Wrap(err, "topicForkIsGloas")
	}
	if topicIsGloas {
		p.logger.WithFields(logrus.Fields{"peer": from, "topic": topicID}).
			Debug("Row topics are not supported on a gloas fork digest yet")
		return errRowGloasTopicUnsupported
	}

	// Row topics carry no cells we did not ask for on topics we do not follow: unlike
	// columns, we never publish a row on a topic we are not subscribed to.
	if _, subscribed := p.subscribedTopics.Load(topicID); !subscribed {
		p.logIgnoreUnsubscribedTopic(from, topicID)
		return nil
	}

	nextPeerState, message, err := updateRowPeerStateFromIncomingRPC(peerStates[from], rpc)
	if err != nil {
		if errors.Is(err, errMalformedPartialMessage) {
			p.reportPeerFeedbackAsync(topicID, from, pubsub.PeerFeedbackInvalidMessage)
		}
		return errors.Wrap(err, "update row peer state from incoming rpc")
	}

	// A peer claiming the whole row is the cancellation signal the phases need, and this is
	// where the claim lands. Enqueued rather than acted on: the row state it applies to belongs
	// to the event loop.
	if nextPeerState.Recvd != nil && nextPeerState.Recvd.Available.Count() == numberOfColumns {
		if _, ok := p.tryEnqueue(requestKindRowPeerHasWholeRow, requestValues{
			rowPeerHasWhole: rowPeerHasWhole{topic: topicID, groupID: rpc.GetGroupID(), from: from},
		}); !ok {
			// Dropping this costs a cancellation, not correctness: the phase timer still fires.
			p.logger.WithFields(logrus.Fields{"peer": from, "topic": topicID}).
				Debug("Dropping a whole-row availability claim")
		}
	}

	if _, ok := p.tryEnqueue(requestKindHandleIncomingRowRPC, requestValues{
		incomingRowRPC: incomingRowRPC{rpc, from, message, rowSubnet, root},
	}); !ok {
		p.logger.WithFields(logrus.Fields{
			"peer":  from,
			"topic": topicID,
			"group": fmt.Sprintf("%#x", rpc.GetGroupID()),
		}).Warn("Dropping incoming partial row RPC")
		return errors.New("incomingReq channel is full, dropping row RPC")
	}
	peerStates[from] = nextPeerState

	return nil
}

func (p *PartialColumnBroadcaster) getRowEntry(topic string, groupID []byte) *rowEntry {
	topicStore, ok := p.rowStore[topic]
	if !ok {
		return nil
	}

	return topicStore[string(groupID)]
}

func (p *PartialColumnBroadcaster) putRowEntry(topic string, groupID []byte, entry *rowEntry) {
	topicStore, ok := p.rowStore[topic]
	if !ok {
		topicStore = make(map[string]*rowEntry)
		p.rowStore[topic] = topicStore
	}
	topicStore[string(groupID)] = entry
}

// rowIndexForSubnet resolves which blob a row subnet carries at a given slot.
func rowIndexForSubnet(rowSubnet uint64, slot primitives.Slot, blobCount uint64) (uint64, error) {
	blobIndices, err := peerdas.BlobsForRowSubnet(rowSubnet, slot, blobCount)
	if err != nil {
		return 0, errors.Wrap(err, "blobs for row subnet")
	}
	switch len(blobIndices) {
	case 0:
		// Derivable from public data, so a peer sending cells for a row that does not exist
		// on this subnet is at fault rather than unlucky.
		return 0, errRowSubnetCarriesNo
	case 1:
		return blobIndices[0], nil
	default:
		// Only reachable when the blob count exceeds ROW_SUBNET_COUNT, which the wire format
		// does not handle yet: one group id cannot carry two independent bitmaps. See
		// notes/rowdas/design.md section 6.1.
		return 0, errRowSubnetMultiRow
	}
}

// handleIncomingRowRPC runs on the event loop and owns all row state.
func (p *PartialColumnBroadcaster) handleIncomingRowRPC(rpc incomingRowRPC) error {
	if p.rowCallbacks == nil {
		return errRowsDisabled
	}
	if p.peerFeedback == nil || p.publishPartialRow == nil {
		return errors.New("pubsub not initialized")
	}

	topicID := rpc.GetTopicID()
	if _, subscribed := p.topics[topicID]; !subscribed {
		p.logIgnoreUnsubscribedTopic(rpc.from, topicID)
		return nil
	}

	groupID := rpc.GroupID
	message := rpc.message
	hasMessage := message != nil
	entry := p.getRowEntry(topicID, groupID)
	var shouldRepublish bool

	if entry == nil {
		if !hasMessage {
			// Metadata for a row we know nothing about. We cannot even say which blob it
			// refers to until a header arrives, so there is nothing to record.
			return nil
		}
		newEntry, err := p.rowEntryFromHeader(rpc)
		if err != nil {
			if errors.Is(err, errInvalidHeader) {
				return nil
			}
			return err
		}
		if newEntry == nil {
			return nil
		}
		// After validation, not before: the slot comes from the header, and a header that does
		// not validate is the peer's fault and is downscored above, whereas a refusal here is not.
		if ok, bound := p.rowGroupAdmissible(topicID, newEntry.row.Slot()); !ok {
			p.refuseRowGroup(topicID, groupID, newEntry.row.Slot(), bound)
			return nil
		}
		entry = newEntry
		p.putRowEntry(topicID, groupID, entry)
		p.groupTTL[string(groupID)] = TTLInSlots
		shouldRepublish = true
		go p.rowCallbacks.RowStarted(topicID, groupID, entry.row.RowIndex())
	}

	if hasMessage {
		if err := p.handlePartialRowCells(entry, message, rpc); err != nil {
			return errors.Wrap(err, "handle partial row cells")
		}
	}

	return p.republishRow(entry, rpc, shouldRepublish)
}

// rowEntryFromHeader validates the header for a row we have not seen yet and builds the row
// state it identifies. It returns (nil, nil) when the message carries no usable header, and
// errInvalidHeader when the header was rejected or ignored.
func (p *PartialColumnBroadcaster) rowEntryFromHeader(rpc incomingRowRPC) (*rowEntry, error) {
	topicID := rpc.GetTopicID()
	groupID := rpc.GroupID

	header, headerWasCached := p.cachedOrMessageHeader(groupID, rpc.message.Header)
	if header == nil {
		return nil, nil
	}
	// Everything below that a peer can get wrong is answered with errInvalidHeader, which the
	// caller swallows after the peer has been downscored. Returning a real error instead would
	// log at Error level for every malformed message a peer chooses to send.
	if header.SignedBlockHeader == nil || header.SignedBlockHeader.Header == nil {
		p.logger.WithFields(rpc.logFields()).Debug("Row header is missing signed block header")
		p.reportPeerFeedback(topicID, rpc.from, pubsub.PeerFeedbackInvalidMessage)
		return nil, errInvalidHeader
	}
	if len(header.KzgCommitments) == 0 {
		p.logger.WithFields(rpc.logFields()).Debug("Row header has no KZG commitments")
		p.reportPeerFeedback(topicID, rpc.from, pubsub.PeerFeedbackInvalidMessage)
		return nil, errInvalidHeader
	}

	root, err := header.SignedBlockHeader.Header.HashTreeRoot()
	if err != nil {
		p.logger.WithFields(rpc.logFields()).WithError(err).Debug("Failed to get root from row header")
		p.reportPeerFeedback(topicID, rpc.from, pubsub.PeerFeedbackInvalidMessage)
		return nil, errInvalidHeader
	}
	if root != rpc.root {
		p.logger.WithFields(rpc.logFields()).Debug("Row header root does not match group ID")
		p.reportPeerFeedback(topicID, rpc.from, pubsub.PeerFeedbackInvalidMessage)
		return nil, errInvalidHeader
	}

	// Which blob this subnet carries is a function of the slot and the blob count, both of
	// which come from the header. That is why a row message without a header cannot be acted
	// on even though its bitmap length is fixed.
	slot := header.SignedBlockHeader.Header.Slot
	rowIndex, err := rowIndexForSubnet(rpc.rowSubnet, slot, uint64(len(header.KzgCommitments)))
	if err != nil {
		switch {
		case errors.Is(err, errRowSubnetCarriesNo):
			p.logger.WithFields(rpc.logFields()).WithField("slot", slot).
				Debug("Row subnet carries no blob at this slot; downscoring")
			p.reportPeerFeedback(topicID, rpc.from, pubsub.PeerFeedbackInvalidMessage)
		case errors.Is(err, errRowSubnetMultiRow):
			p.logger.WithFields(rpc.logFields()).WithField("slot", slot).
				Warn("Row subnet carries more than one blob; unsupported, ignoring")
		}
		return nil, errInvalidHeader
	}

	if rpc.message.RowIndex != rowIndex {
		// The derived value is authoritative; the field is a cross-check, so a mismatch
		// means the sender computed the mapping differently and its cells cannot be trusted
		// to belong to this row.
		p.logger.WithFields(rpc.logFields()).WithFields(logrus.Fields{
			"claimed": rpc.message.RowIndex,
			"derived": rowIndex,
		}).Debug("Row index does not match the subnet mapping; downscoring")
		p.reportPeerFeedback(topicID, rpc.from, pubsub.PeerFeedbackInvalidMessage)
		return nil, errInvalidHeader
	}

	if !headerWasCached {
		result, err := p.rowCallbacks.ValidateRowHeader(header)
		if err != nil {
			p.logger.WithError(err).WithFields(rpc.logFields()).WithField("result", result).
				Debug("Row header validation failed")
			if result == pubsub.ValidationReject {
				p.reportPeerFeedback(topicID, rpc.from, pubsub.PeerFeedbackInvalidMessage)
			}
			return nil, errInvalidHeader
		}
		p.logger.WithFields(rpc.logFields()).Debug("Handling row header as it was previously not cached for this group")
		p.cacheAndHandleHeader(groupID, header, rpc.logFields())
	}

	row, err := blocks.NewPartialDataRow(root, header.SignedBlockHeader, rowIndex, header.KzgCommitments, header.KzgCommitmentsInclusionProof)
	if err != nil {
		return nil, errors.Wrap(err, "new partial data row")
	}
	if !bytes.Equal(row.GroupID(), groupID) {
		// The root already matched, so this can only mean the group id form disagrees, which
		// is a bug on our side rather than the peer's.
		p.logger.WithFields(rpc.logFields()).Error("Row group ID mismatch")
		return nil, errors.New("row group id mismatch")
	}

	p.applyRowRequestPolicy(&row)
	// Seed it from the columns we already hold, before the request policy's bitmap goes out: a row
	// created after its columns would otherwise start empty and ask for cells it is already
	// holding one axis over. See D13.
	p.crossFillRowFromHeldColumns(groupID, &row)

	return &rowEntry{row: &row}, nil
}

// applyRowRequestPolicy records what this node can use of a row. Applied where row state is
// created, because the request bitmap goes out with the first parts metadata and a narrowing
// applied later would already have leaked the wide request.
func (p *PartialColumnBroadcaster) applyRowRequestPolicy(row *blocks.PartialDataRow) {
	if p.rowCallbacks == nil {
		return
	}
	keep, poolToThreshold := p.rowCallbacks.RowRequestInterest(row.RowIndex())
	if keep == nil {
		// No policy: ask for everything missing, which is what the row did before this existed.
		return
	}
	if err := row.SetRequestInterest(keep, poolToThreshold); err != nil {
		// A length mismatch is a caller bug, and asking for everything is the behaviour that
		// preserves liveness rather than the one that preserves bytes.
		p.logger.WithError(err).WithField("rowIndex", row.RowIndex()).
			Error("Ignoring the row request policy: wrong bitmap length")
	}
}

// handlePartialRowCells hands the cells we do not already have to the validator, off the event
// loop. Verified cells come back as a requestKindRowCellsValidated event.
func (p *PartialColumnBroadcaster) handlePartialRowCells(entry *rowEntry, message *ethpb.PartialDataRowSidecar, rpc incomingRowRPC) error {
	topicID := rpc.GetTopicID()

	columnIndices, cellsToVerify, err := entry.row.CellsToVerifyFromPartialMessage(message)
	if err != nil {
		// A message whose row index or bitmap disagrees with the row it was sent on is the
		// peer's fault.
		p.logger.WithError(err).WithFields(rpc.logFields()).Debug("Unusable partial row message; downscoring")
		p.reportPeerFeedback(topicID, rpc.from, pubsub.PeerFeedbackInvalidMessage)
		return nil
	}
	if len(cellsToVerify) == 0 {
		return nil
	}

	rowIndexStr := strconv.FormatUint(entry.row.RowIndex(), 10)
	partialMessageRowCellsReceivedTotal.WithLabelValues(rowIndexStr).Add(float64(len(columnIndices)))

	select {
	case p.concurrentValidatorSemaphore <- struct{}{}:
		group := entry.row.GroupID()
		// Capture the callbacks rather than reading the field from the goroutine: it is set
		// once at Start, and a captured value cannot be read while something else writes it.
		callbacks := p.rowCallbacks
		go func() {
			defer func() { <-p.concurrentValidatorSemaphore }()
			start := time.Now()
			if err := callbacks.ValidateRowCells(cellsToVerify); err != nil {
				p.logger.WithError(err).WithFields(rpc.logFields()).Error("Failed to validate row cells")
				p.reportPeerFeedback(topicID, rpc.from, pubsub.PeerFeedbackInvalidMessage)
				return
			}
			p.reportPeerFeedback(topicID, rpc.from, pubsub.PeerFeedbackUsefulMessage)
			_, _ = p.enqueue(p.ctx, requestKindRowCellsValidated, requestValues{
				rowCellsValidated: &rowCellsValidated{
					validationTook: time.Since(start),
					topic:          topicID,
					group:          group,
					columnIndices:  columnIndices,
					cells:          cellsToVerify,
				},
			})
		}()
	default:
		partialMessageRowValidationsDroppedTotal.WithLabelValues(rowIndexStr).Add(float64(len(cellsToVerify)))
		p.logger.WithFields(rpc.logFields()).Warn("Validator semaphore saturated, dropping row cell validation")
	}

	return nil
}

// handleRowCellsValidated folds verified cells into the row and fires the duty notifications.
func (p *PartialColumnBroadcaster) handleRowCellsValidated(cells *rowCellsValidated) error {
	entry := p.getRowEntry(cells.topic, cells.group)
	if entry == nil {
		// The group can be evicted while validation is in flight.
		p.logger.WithFields(cells.logFields()).Debug("Row not found for verified cells")
		return nil
	}

	var extended bool
	for i, bundle := range cells.cells {
		if entry.row.ExtendFromVerifiedCell(cells.columnIndices[i], bundle.Cell, bundle.Proof) {
			extended = true
		}
	}
	if !extended {
		return nil
	}

	rowIndexStr := strconv.FormatUint(entry.row.RowIndex(), 10)
	partialMessageRowUsefulCellsTotal.WithLabelValues(rowIndexStr).Add(float64(len(cells.cells)))

	// A cell learned on the row topic is a cell of some column, and if that column is one we
	// custody this is the cheapest way it will ever arrive.
	if err := p.crossFillColumnsFromRow(cells.group, entry.row.RowIndex(), cells.cells); err != nil {
		p.logger.WithError(err).WithFields(cells.logFields()).Error("Failed to cross-fill columns from row cells")
	}

	p.notifyRowProgress(cells.topic, entry)

	if !entry.row.Published {
		return nil
	}

	// Availability grew, which was the 81.6% term of the storm. The publish itself is not
	// throttled: the per-peer decision holds the announcement for each peer until a packet goes
	// there anyway or its deadline passes (partialcells.go), so this produces packets only for
	// peers with something on their critical path.
	return p.publishPartialRow(cells.topic, entry.row.GroupID(), entry.row)
}

// handleRowPeerHasWholeRow records a peer's whole-row availability claim and re-evaluates
// whether the row can be declared served elsewhere.
func (p *PartialColumnBroadcaster) handleRowPeerHasWholeRow(claim rowPeerHasWhole) {
	entry := p.getRowEntry(claim.topic, claim.groupID)
	if entry == nil {
		// No state for this row: nothing to cancel, and creating state on a bare claim would let
		// a peer allocate it for any group id it likes.
		return
	}
	if entry.peerHasWholeRow {
		return
	}
	entry.peerHasWholeRow = true
	p.logger.WithFields(logrus.Fields{
		"peer":     claim.from,
		"topic":    claim.topic,
		"rowIndex": entry.row.RowIndex(),
	}).Debug("A peer claims the whole row")

	p.notifyRowProgress(claim.topic, entry)
}

// notifyRowProgress fires the recoverable and complete notifications, each at most once per
// row. They run off the event loop, and take only identifiers: the callback pulls a snapshot
// with RowSnapshot if and when it decides to act, so it never touches loop-owned state.
func (p *PartialColumnBroadcaster) notifyRowProgress(topic string, entry *rowEntry) {
	groupID := entry.row.GroupID()
	rowIndex := entry.row.RowIndex()

	if !entry.notifiedRecoverable && entry.row.ReconstructionThresholdMet() {
		entry.notifiedRecoverable = true
		partialMessageRowsRecoverableTotal.Inc()
		go p.rowCallbacks.RowRecoverable(topic, groupID, rowIndex)
	}
	if !entry.notifiedComplete && entry.row.IsComplete() {
		entry.notifiedComplete = true
		partialMessageRowsCompleteTotal.Inc()
		go p.rowCallbacks.RowComplete(topic, groupID, rowIndex)
	}
	// Served elsewhere needs both halves: a peer claiming the whole row, and nothing left that we
	// want from it. The second is what keeps an unverified claim from standing down a node that
	// still needs cells -- a peer that cannot serve what it claims cannot satisfy us either.
	if !entry.notifiedServedElsewhere && entry.peerHasWholeRow && entry.row.MissingWantedCount() == 0 {
		entry.notifiedServedElsewhere = true
		partialMessageRowsServedElsewhereTotal.Inc()
		go p.rowCallbacks.RowServedElsewhere(topic, groupID, rowIndex)
	}
}

// republishRow re-publishes our row state when it differs from what the peer told us, which is
// how a pull-based exchange makes progress.
func (p *PartialColumnBroadcaster) republishRow(entry *rowEntry, rpc incomingRowRPC, shouldRepublish bool) error {
	if !entry.row.Published {
		// Before we have published, we do not yet know what we hold, so announcing an empty
		// state would only tell peers to stop asking.
		return nil
	}

	if !shouldRepublish && len(rpc.PartsMetadata) > 0 {
		myMeta, err := entry.row.PartsMetadata()
		if err != nil {
			return errors.Wrap(err, "row parts metadata")
		}
		if !bytes.Equal(rpc.PartsMetadata, myMeta) {
			shouldRepublish = true
		}
	}
	if !shouldRepublish {
		return nil
	}

	// This fires on every inbound RPC carrying metadata, and it used to be throttled through the
	// availability window because each publish re-announced whatever availability had accumulated
	// to every peer. It is not throttled any more, and must not be: it is the path that answers a
	// peer's request with cells. The announcements it used to spray are now held per peer in the
	// publish decision, so an unthrottled publish here costs a packet only where a peer's critical
	// path needs one.
	return p.publishPartialRow(rpc.GetTopicID(), entry.row.GroupID(), entry.row)
}

// gossipRow answers a heartbeat gossip emission for a row group by re-announcing what we hold.
func (p *PartialColumnBroadcaster) gossipRow(topic string, groupID []byte) {
	entry := p.getRowEntry(topic, groupID)
	if entry == nil || !entry.row.Published || entry.row.Included.Count() == 0 {
		return
	}
	if err := p.publishPartialRow(topic, entry.row.GroupID(), entry.row); err != nil {
		p.logger.WithError(err).Warn("Failed to publish row gossip")
	}
}

// PublishRow hands a row to the broadcaster to serve on a row topic. It is the entry point
// both for a node offering the cells it holds from its own custody and for one offering a row
// it has just recovered.
func (p *PartialColumnBroadcaster) PublishRow(ctx context.Context, topic string, row blocks.PartialDataRow) error {
	if p.rowCallbacks == nil {
		return errRowsDisabled
	}
	if p.peerFeedback == nil || p.publishPartialRow == nil {
		return errors.New("pubsub not initialized")
	}
	req, err := p.enqueue(ctx, requestKindPublishRow, requestValues{
		publishRow: publishRow{topic: topic, row: row},
	})
	if err != nil {
		return err
	}

	return req.waitForResponse()
}

func (p *PartialColumnBroadcaster) publishRowOnLoop(topic string, incoming blocks.PartialDataRow) error {
	groupID := incoming.GroupID()
	entry := p.getRowEntry(topic, groupID)
	if entry == nil {
		// The bound is on the store, so our own publishes are held to it too. A caller that
		// wants to know -- the reconstruction scheduler -- gets the error rather than a silent
		// drop, because for it a refused publish means a recovered row goes nowhere on this axis.
		if ok, bound := p.rowGroupAdmissible(topic, incoming.Slot()); !ok {
			p.refuseRowGroup(topic, groupID, incoming.Slot(), bound)
			return errors.Wrapf(errRowGroupCapReached, "%s bound", bound)
		}
		// Clone rather than adopting the caller's value. PublishRow takes a PartialDataRow by
		// value, but its bitmap and cell slices still alias the caller's, so storing it
		// directly would let the caller write loop-owned state from another goroutine -- and a
		// reconstruction scheduler does exactly that, extending its own copy after publishing.
		adopted := incoming.Clone()
		// Only when the caller expressed no preference. A caller that set its own request
		// bitmap -- the pull arm naming one cell, a recovered row asking for nothing -- knows
		// more about this publish than the policy does.
		if _, ok := adopted.PartsRequests(); !ok {
			p.applyRowRequestPolicy(&adopted)
		}
		p.crossFillRowFromHeldColumns(groupID, &adopted)
		entry = &rowEntry{row: &adopted}
		p.putRowEntry(topic, groupID, entry)
	} else {
		if entry.row.RowIndex() != incoming.RowIndex() {
			return errors.Errorf("row index mismatch for group: have %d, got %d", entry.row.RowIndex(), incoming.RowIndex())
		}
		if requests, ok := incoming.PartsRequests(); ok {
			if err := entry.row.SetPartsRequests(requests); err != nil {
				return errors.Wrap(err, "set row parts requests")
			}
		} else {
			entry.row.ClearPartsRequests()
		}
		for columnIndex := range incoming.Included.Len() {
			if incoming.Included.BitAt(columnIndex) {
				entry.row.ExtendFromVerifiedCell(columnIndex, incoming.Cells[columnIndex], incoming.Proofs[columnIndex])
			}
		}
		p.notifyRowProgress(topic, entry)
	}

	p.groupTTL[string(groupID)] = TTLInSlots

	// Offer the row's cells to the columns we custody before serving it. A row published here
	// may be one this node just recovered, whose cells never arrived as a message and so never
	// passed through the incoming path's cross-fill.
	if err := p.crossFillColumnsFromWholeRow(groupID, entry.row); err != nil {
		p.logger.WithError(err).WithField("rowIndex", entry.row.RowIndex()).
			Error("Failed to cross-fill columns from a published row")
	}

	if err := p.publishPartialRow(topic, groupID, entry.row); err != nil {
		return errors.Wrap(err, "publish partial row")
	}
	entry.row.Published = true

	return nil
}

// RowSnapshot returns a deep copy of the row held for a (topic, group), or nil if there is
// none. It goes through the event loop because the loop owns all row state; callers on other
// goroutines must not read a row directly.
func (p *PartialColumnBroadcaster) RowSnapshot(ctx context.Context, topic string, groupID []byte) (*blocks.PartialDataRow, error) {
	snapshot := &rowSnapshotRequest{topic: topic, groupID: groupID}
	req, err := p.enqueue(ctx, requestKindRowSnapshot, requestValues{rowSnapshot: snapshot})
	if err != nil {
		return nil, err
	}
	if err := req.waitForResponse(); err != nil {
		return nil, err
	}

	return snapshot.out, nil
}

func (p *PartialColumnBroadcaster) rowSnapshotOnLoop(request *rowSnapshotRequest) error {
	entry := p.getRowEntry(request.topic, request.groupID)
	if entry == nil {
		return nil
	}
	clone := entry.row.Clone()
	request.out = &clone

	return nil
}
