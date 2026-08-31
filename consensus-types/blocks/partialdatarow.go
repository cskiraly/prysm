package blocks

import (
	"iter"
	"slices"

	"github.com/OffchainLabs/go-bitfield"
	fieldparams "github.com/OffchainLabs/prysm/v7/config/fieldparams"
	"github.com/OffchainLabs/prysm/v7/consensus-types/primitives"
	ethpb "github.com/OffchainLabs/prysm/v7/proto/prysm/v1alpha1"
	"github.com/libp2p/go-libp2p-pubsub/partialmessages"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"time"
)

// numberOfColumns is the number of parts in a row: a row is an extended blob, and an extended
// blob has one cell per column.
const numberOfColumns = uint64(fieldparams.NumberOfColumns)

// kzgCommitmentsInclusionProofDepth mirrors kzg_commitments_inclusion_proof_depth.size in
// proto/ssz_proto_library.bzl. There is no Go constant for it.
const kzgCommitmentsInclusionProofDepth = 4

var (
	errRowIndexOutOfRange   = errors.New("row index beyond the block's blob count")
	errInclusionProofLength = errors.New("wrong kzg commitments inclusion proof length")
)

// PartialDataRow is a partially populated row -- the cells of one blob across all columns --
// used for exchanging cells with peers on the RowDAS (EIP-8371) data_row topics.
//
// It is the transpose of PartialDataColumn. Where a column holds one cell of every blob and
// verifies each cell against a different commitment at a fixed cell index, a row holds every
// cell of one blob and verifies all of them against a single commitment at varying cell
// indices.
//
// Unlike a column there is no completed sidecar type to embed: a row has no full-message form
// on the wire, and a completed row is a blob rather than a sidecar. What it does share with a
// column is the header, so a header validated on any subnet -- column or row -- serves both.
type PartialDataRow struct {
	root     [fieldparams.RootLength]byte
	rowIndex uint64
	groupID  []byte

	// The block header, kept whole rather than reduced to commitments[rowIndex], because the
	// inclusion proof is over the entire commitment list and eager pushes carry the header.
	signedBlockHeader            *ethpb.SignedBeaconBlockHeader
	kzgCommitments               [][]byte
	kzgCommitmentsInclusionProof [][]byte

	// Cells and Proofs are indexed by column index and are always NUMBER_OF_COLUMNS long;
	// Included says which entries are populated.
	Cells    [][]byte
	Proofs   [][]byte
	Included bitfield.Bitlist

	// partsRequests overrides the request bitmap in parts metadata, for when we know which
	// cells to ask for before we have anything to offer.
	partsRequests bitfield.Bitlist

	// requestsAfter, when non-zero, holds the default request policy silent until the instant
	// passes: D17 clause 1, the row axis yielding right-of-way to the column path. R18 measured
	// what asking immediately costs at the realistic mixed-custody shape: about half a second of
	// column-completion p50, in every custody class, because both axes fight for the same links
	// while the column path is still delivering. Only the *requests* wait -- availability is
	// still announced on time, so peers can see what this node holds -- and an explicit
	// partsRequests override bypasses the hold entirely: the pull arm naming one cell and a
	// recovered row asking for nothing know more about this row than the policy does.
	requestsAfter time.Time

	// requestClaimLedger records which peer each part we lack has been asked from, so a
	// later publish does not re-ask everyone. Allocated on first use; see
	// requestclaims.go.
	requestClaimLedger *requestClaims

	// keep is the cells this node stores: the cells of the columns it custodies. Nil means no
	// policy has been set and the row asks for everything it lacks, which is what the column
	// axis does.
	keep bitfield.Bitlist
	// poolToThreshold is whether this node still needs foreign cells to be able to recover the
	// row. False for a node that holds enough columns to recover from its own custody, which
	// EIP-8371 says needs no foreign row information at all.
	poolToThreshold bool

	// Published is set once this node has published the row itself. As for columns, we only
	// republish in response to an incoming RPC after publishing, because that is the point
	// at which we know what we have and what we are missing.
	//
	// Last, beside the other bool, so the two share a word: Bazel's nogo runs the maligned
	// check and refuses a struct whose field order wastes padding.
	Published bool
}

var _ partialParts = (*PartialDataRow)(nil)

// NewPartialDataRow creates an empty partial row for one blob of a block. It does not
// validate the header or the inclusion proof; the caller is responsible for that.
//
// The group id is deliberately the same one the column topics use for this block. Row and
// column topics are distinct, so there is no collision, and sharing it lets a single validated
// header cache serve both domains.
func NewPartialDataRow(
	root [fieldparams.RootLength]byte,
	signedBlockHeader *ethpb.SignedBeaconBlockHeader,
	rowIndex uint64,
	kzgCommitments [][]byte,
	kzgCommitmentsInclusionProof [][]byte,
) (PartialDataRow, error) {
	if signedBlockHeader == nil {
		return PartialDataRow{}, errors.New("signedBlockHeader is nil")
	}
	if len(kzgCommitments) == 0 {
		return PartialDataRow{}, errors.New("kzgCommitments is empty")
	}
	if rowIndex >= uint64(len(kzgCommitments)) {
		return PartialDataRow{}, errors.Wrapf(errRowIndexOutOfRange, "row %d, blob count %d", rowIndex, len(kzgCommitments))
	}
	// The header container cannot encode a proof of any other length, and a row always has a
	// header to send -- so refuse here rather than at publish time, where the failure reads as
	// a broadcaster fault rather than a caller one.
	if len(kzgCommitmentsInclusionProof) != kzgCommitmentsInclusionProofDepth {
		return PartialDataRow{}, errors.Wrapf(errInclusionProofLength, "got %d, want %d",
			len(kzgCommitmentsInclusionProof), kzgCommitmentsInclusionProofDepth)
	}

	return PartialDataRow{
		root:                         root,
		rowIndex:                     rowIndex,
		groupID:                      groupIDFromRoot(root),
		signedBlockHeader:            signedBlockHeader,
		kzgCommitments:               kzgCommitments,
		kzgCommitmentsInclusionProof: kzgCommitmentsInclusionProof,
		Cells:                        make([][]byte, numberOfColumns),
		Proofs:                       make([][]byte, numberOfColumns),
		Included:                     bitfield.NewBitlist(numberOfColumns),
	}, nil
}

// GroupID returns the libp2p partial-messages group identifier, as a copy so callers cannot
// mutate the internal one.
func (p *PartialDataRow) GroupID() []byte {
	return slices.Clone(p.groupID)
}

// RowIndex is the blob index this row carries.
func (p *PartialDataRow) RowIndex() uint64 {
	return p.rowIndex
}

// BlockRoot is the root of the block this row belongs to.
func (p *PartialDataRow) BlockRoot() [fieldparams.RootLength]byte {
	return p.root
}

// Slot is the slot of the block this row belongs to.
func (p *PartialDataRow) Slot() primitives.Slot {
	return p.signedBlockHeader.Header.Slot
}

// Commitment is the KZG commitment of this row's blob. Every cell in the row verifies against
// it, which is the axis on which row verification differs from column verification.
func (p *PartialDataRow) Commitment() []byte {
	return p.kzgCommitments[p.rowIndex]
}

// KzgCommitments returns all of the block's commitments, needed to validate the header's
// inclusion proof.
func (p *PartialDataRow) KzgCommitments() [][]byte {
	return p.kzgCommitments
}

// SignedBlockHeader returns the block header this row was built from.
func (p *PartialDataRow) SignedBlockHeader() *ethpb.SignedBeaconBlockHeader {
	return p.signedBlockHeader
}

// KzgCommitmentsInclusionProof returns the proof that the commitment list is the block's.
func (p *PartialDataRow) KzgCommitmentsInclusionProof() [][]byte {
	return p.kzgCommitmentsInclusionProof
}

// partsCount implements partialParts. A row's parts are its cells, one per column, and the
// count does not depend on the blob count.
// defersAnnouncements is true on the row axis: a request removal and availability growth ride
// the next packet to the peer rather than buying their own. See partialForPeer.
func (p *PartialDataRow) defersAnnouncements() bool { return true }

// availabilityIsUrgent: a complete row announces at once. Its bitmap is the whole-row claim other
// reconstructors stand down on, and the value of that signal is in its promptness.
func (p *PartialDataRow) availabilityIsUrgent() bool { return p.IsComplete() }

// DeferRequestsUntil holds the default request policy silent until t (D17 clause 1). Zero clears
// the hold. Explicit SetPartsRequests overrides are unaffected.
func (p *PartialDataRow) DeferRequestsUntil(t time.Time) { p.requestsAfter = t }

// RequestsDeferredUntil reports the hold, zero when none is set. The publish wake-up uses it: a
// deferral that expires must trigger a publish, or the requests appear only when unrelated
// traffic happens to run one.
func (p *PartialDataRow) RequestsDeferredUntil() time.Time { return p.requestsAfter }

func (p *PartialDataRow) partsCount() uint64 {
	return numberOfColumns
}

func (p *PartialDataRow) newPartsMetadata(assigned bitfield.Bitlist) (*ethpb.PartialDataColumnPartsMetadata, error) {
	available := slices.Clone(p.Included)
	requests, err := p.wantedParts()
	if err != nil {
		return nil, err
	}
	// Narrow to what this peer was assigned. A nil bitlist means the caller has no peer context
	// and asks for everything, which is the pre-de-confliction behaviour.
	if assigned != nil {
		requests, err = requests.And(assigned)
		if err != nil {
			return nil, errors.Wrap(err, "intersect parts requests with the peer's assignment")
		}
	}

	return &ethpb.PartialDataColumnPartsMetadata{
		Available: available,
		Requests:  requests,
	}, nil
}

// wantedParts is what we lack and are willing to ask for.
//
// An explicit partsRequests override wins outright: a caller that set one -- the pull arm naming a
// single cell, a recovered row asking for nothing -- knows more about this row than any policy.
//
// Otherwise the answer depends on whether this node can still use foreign cells, which is what
// separates the row axis from the column axis. A node subscribed to a column stores every cell it
// asks for; a node does not store a row. So:
//
//   - while it is short of the reconstruction threshold it asks for everything missing, because
//     any cell brings it closer and it cannot know which ones its peers actually hold. Asking for
//     a specific subset is how a node ends up requesting cells that do not exist while missing the
//     ones that do -- measured, in R9's withholding shape: it cut the nodes reaching the threshold
//     from 8 to 1.
//   - once it can recover the row it asks only for the cells it keeps, i.e. the cells of its own
//     custodied columns. A second route to those is the row axis's latency benefit; the rest it
//     would neither store nor compute with, and recovery fills them for free.
//
// A node that holds enough columns to recover from custody alone therefore asks for nothing beyond
// its own columns from the outset, since it is never below the threshold once cross-fill has run.
func (p *PartialDataRow) wantedParts() (bitfield.Bitlist, error) {
	missing := p.Included.Not()
	if p.partsRequests != nil {
		requests, err := p.partsRequests.And(missing)
		if err != nil {
			return nil, errors.Wrap(err, "intersect parts requests with missing cells")
		}

		return requests, nil
	}
	if !p.requestsAfter.IsZero() && partialClock().Before(p.requestsAfter) {
		// Deferred (D17 clause 1): ask for nothing yet. No claims are made either, so nothing
		// ages toward a lapse while the column path works.
		return bitfield.NewBitlist(p.Included.Len()), nil
	}
	if p.keep == nil {
		return missing, nil
	}
	if p.poolToThreshold && !p.ReconstructionThresholdMet() {
		return missing, nil
	}
	wanted, err := p.keep.And(missing)
	if err != nil {
		return nil, errors.Wrap(err, "intersect kept cells with missing cells")
	}

	return wanted, nil
}

// SetRequestInterest records what this node can use of a row: the cells it keeps, and whether it
// still needs foreign cells to reach the reconstruction threshold. Policy belongs to the caller --
// the row knows neither what this node custodies nor how much of it.
func (p *PartialDataRow) SetRequestInterest(keep bitfield.Bitlist, poolToThreshold bool) error {
	if keep.Len() != p.Included.Len() {
		return errors.Errorf("request interest length mismatch: got %d, want %d", keep.Len(), p.Included.Len())
	}
	p.keep = slices.Clone(keep)
	p.poolToThreshold = poolToThreshold

	return nil
}

// RequestInterest returns the interest set on this row, if any.
func (p *PartialDataRow) RequestInterest() (keep bitfield.Bitlist, poolToThreshold, ok bool) {
	if p.keep == nil {
		return nil, false, false
	}

	return slices.Clone(p.keep), p.poolToThreshold, true
}

// missingWantedCount is how many cells this row still wants, which is not the same as how many
// it lacks: under a request interest a node stops wanting cells it will neither store nor need.
// Zero means a peer serving the whole row can tell us nothing further.
func (p *PartialDataRow) MissingWantedCount() uint64 {
	return p.missingParts().Count()
}

// missingParts implements partialParts.
func (p *PartialDataRow) missingParts() bitfield.Bitlist {
	wanted, err := p.wantedParts()
	if err != nil {
		// A length mismatch between the override and the message can only come from a caller
		// bug, and asking for nothing is the safe reading of it.
		return bitfield.NewBitlist(p.partsCount())
	}

	return wanted
}

// claims implements partialParts, allocating on first use so a zero value is usable.
func (p *PartialDataRow) claims() *requestClaims {
	if p.requestClaimLedger == nil {
		p.requestClaimLedger = newRequestClaims()
	}

	return p.requestClaimLedger
}

// SetPartsRequests overrides the request bitmap emitted in parts metadata.
func (p *PartialDataRow) SetPartsRequests(requests bitfield.Bitlist) error {
	if requests.Len() != p.Included.Len() {
		return errors.Errorf("parts requests length mismatch: got %d, want %d", requests.Len(), p.Included.Len())
	}
	p.partsRequests = slices.Clone(requests)

	return nil
}

// ClearPartsRequests removes any request bitmap override.
func (p *PartialDataRow) ClearPartsRequests() {
	p.partsRequests = nil
}

// PartsRequests returns a cloned request bitmap override, if one is set.
func (p *PartialDataRow) PartsRequests() (bitfield.Bitlist, bool) {
	if p.partsRequests == nil {
		return nil, false
	}

	return slices.Clone(p.partsRequests), true
}

// PartsMetadata returns the SSZ-encoded parts metadata for this row.
func (p *PartialDataRow) PartsMetadata() (partialmessages.PartsMetadata, error) {
	// No peer context here, so this describes the whole gap. It is the shape a caller
	// outside the publish path sees; the per-peer narrowing happens in partialForPeer.
	meta, err := p.newPartsMetadata(nil)
	if err != nil {
		return nil, errors.Wrap(err, "new parts metadata")
	}

	return marshalPartsMetadata(meta)
}

// cellsToSendToPeer implements partialParts. A cell is sent only if the peer requested it, we
// have it, and the peer does not already have it. The peer is allowed to request cells it
// already has; we filter those out to save bandwidth.
func (p *PartialDataRow) cellsToSendToPeer(peerMeta *ethpb.PartialDataColumnPartsMetadata) (encodedMsg []byte, cellsSent bitfield.Bitlist, err error) {
	meetsRequests, err := peerMeta.Requests.And(p.Included)
	if err != nil {
		return nil, nil, errors.Wrap(err, "peer metadata bitmap length mismatch - requests")
	}
	meetsNeeds, err := meetsRequests.And(peerMeta.Available.Not())
	if err != nil {
		return nil, nil, errors.Wrap(err, "peer metadata bitmap length mismatch - available")
	}

	nCells := meetsNeeds.Count()
	if nCells == 0 {
		return nil, nil, nil
	}

	out := &ethpb.PartialDataRowSidecar{
		RowIndex:           p.rowIndex,
		CellsPresentBitmap: meetsNeeds,
		PartialRow:         make([][]byte, 0, nCells),
		KzgProofs:          make([][]byte, 0, nCells),
	}
	for columnIndex := range meetsNeeds.Len() {
		if !meetsNeeds.BitAt(columnIndex) {
			continue
		}
		out.PartialRow = append(out.PartialRow, p.Cells[columnIndex])
		out.KzgProofs = append(out.KzgProofs, p.Proofs[columnIndex])
	}

	encoded, err := out.MarshalSSZ()
	if err != nil {
		return nil, nil, errors.Wrap(err, "marshal partial data row sidecar")
	}

	return encoded, meetsNeeds, nil
}

// headerMessage implements partialParts. Rows always have a header to exchange: with no
// full-message form, a peer whose only source is the row topic has nowhere else to get the
// commitments from.
func (p *PartialDataRow) headerMessage() ([]byte, error) {
	out := &ethpb.PartialDataRowSidecar{
		RowIndex:           p.rowIndex,
		CellsPresentBitmap: bitfield.NewBitlist(numberOfColumns),
		Header: []*ethpb.PartialDataColumnHeader{{
			KzgCommitments:               p.kzgCommitments,
			SignedBlockHeader:            p.signedBlockHeader,
			KzgCommitmentsInclusionProof: p.kzgCommitmentsInclusionProof,
		}},
	}
	encoded, err := out.MarshalSSZ()
	if err != nil {
		return nil, errors.Wrap(err, "marshal partial row header")
	}

	return encoded, nil
}

// EarliestClaimDeadline reports when this row next needs a publish on its own account: its
// soonest outstanding request claim lapsing, or a deferred cancellation reaching its maximum delay.
//
// It exists so the broadcaster can wake up at that moment and publish, rather than noticing the
// event whenever the next publish happens to occur. Without it, reassignment granularity is bounded
// by whatever traffic arrives next -- gossipsub's heartbeat in the best case, and nothing at all on
// a quiet mesh, which is how a part asked of a silent peer could stall indefinitely.
func (p *PartialDataRow) EarliestClaimDeadline() (time.Time, bool) {
	return p.claims().earliestDeadline()
}

// PublishActionsFn returns the publish-action iterator for this row. headerSentCache tracks
// which peers have already been sent the header so it is only sent once; onEagerPush, if
// non-nil, is called for each peer that was eager pushed to.
func (p *PartialDataRow) PublishActionsFn(headerSentCache map[peer.ID]bool, onEagerPush func(peer.ID), onAction func(peer.ID, ActionReason)) partialmessages.PublishActionsFn[PartialDataColumnPeerState] {
	return func(peerStates map[peer.ID]PartialDataColumnPeerState, peerRequestsPartial func(peer.ID) bool) iter.Seq2[peer.ID, partialmessages.PublishAction] {
		return partialPublishActions(p, peerStates, peerRequestsPartial, headerSentCache, onEagerPush, onAction)
	}
}

// DecodePartialRowSidecar SSZ-decodes an incoming partial-message body.
func DecodePartialRowSidecar(raw []byte) (*ethpb.PartialDataRowSidecar, error) {
	sidecar := &ethpb.PartialDataRowSidecar{}
	if err := sidecar.UnmarshalSSZ(raw); err != nil {
		return nil, errors.Wrap(err, "unmarshal partial data row sidecar")
	}

	return sidecar, nil
}

// CellsToVerifyFromPartialMessage returns the cells in the message that we do not already
// have, along with their column indices. Every bundle carries this row's single commitment;
// what varies is the cell index, which is the column index.
func (p *PartialDataRow) CellsToVerifyFromPartialMessage(message *ethpb.PartialDataRowSidecar) ([]uint64, []CellProofBundle, error) {
	if message.RowIndex != p.rowIndex {
		return nil, nil, errors.Errorf("message is for row %d, not row %d", message.RowIndex, p.rowIndex)
	}

	included := message.CellsPresentBitmap
	if included.Len() == 0 {
		return nil, nil, nil
	}
	if included.Len() != p.Included.Len() {
		return nil, nil, errors.Errorf("invalid message: bitmap length %d, want %d", included.Len(), p.Included.Len())
	}

	includedCells := included.Count()
	if uint64(len(message.KzgProofs)) != includedCells {
		return nil, nil, errors.New("invalid message. Missing KZG proofs")
	}
	if uint64(len(message.PartialRow)) != includedCells {
		return nil, nil, errors.New("invalid message. Missing cells")
	}

	commitment := p.Commitment()
	cellIndices := make([]uint64, 0, includedCells)
	cellsToVerify := make([]CellProofBundle, 0, includedCells)
	// j tracks the position in the packed PartialRow/KzgProofs arrays.
	var j int
	for columnIndex := range included.Len() {
		if !included.BitAt(columnIndex) {
			continue
		}
		if !p.Included.BitAt(columnIndex) {
			cellIndices = append(cellIndices, columnIndex)
			cellsToVerify = append(cellsToVerify, CellProofBundle{
				ColumnIndex: columnIndex,
				Cell:        message.PartialRow[j],
				Proof:       message.KzgProofs[j],
				Commitment:  commitment,
			})
		}
		j++
	}

	return cellIndices, cellsToVerify, nil
}

// ExtendFromVerifiedCell adds one verified cell at the given column index. It returns false
// without modifying the row if the cell is already present or the index is out of range.
func (p *PartialDataRow) ExtendFromVerifiedCell(columnIndex uint64, cell, proof []byte) bool {
	if columnIndex >= numberOfColumns {
		log.WithFields(logrus.Fields{
			"rowIndex":    p.rowIndex,
			"columnIndex": columnIndex,
			"columnCount": numberOfColumns,
		}).Error("Column index out of range for partial data row")

		return false
	}
	if p.Included.BitAt(columnIndex) {
		return false
	}
	// The cell arrived, so any request claim on it is settled. This is the analogue of nqg's
	// fulfillIWant, and it is also where a response-time estimator would attach if one is built
	// (notes/rowdas/TODO.md D8).
	if p.requestClaimLedger != nil {
		p.requestClaimLedger.settle(columnIndex)
	}

	p.Included.SetBitAt(columnIndex, true)
	p.Cells[columnIndex] = cell
	p.Proofs[columnIndex] = proof

	return true
}

// IsComplete reports whether every cell of the row is present.
func (p *PartialDataRow) IsComplete() bool {
	return p.Included.Count() == numberOfColumns
}

// ReconstructionThreshold is the number of cells needed to recover a row. It has no column
// analogue: a column is either complete or it is not, whereas a row can be recovered from
// half of its cells.
func ReconstructionThreshold() uint64 {
	return (numberOfColumns + 1) / 2
}

// ReconstructionThresholdMet reports whether enough cells are present to recover the rest of
// the row.
func (p *PartialDataRow) ReconstructionThresholdMet() bool {
	return p.Included.Count() >= ReconstructionThreshold()
}

// PresentColumnIndices returns the column indices of the cells present, in ascending order.
// The KZG recovery functions require ascending indices, so this is the form they want.
func (p *PartialDataRow) PresentColumnIndices() []uint64 {
	indices := make([]uint64, 0, p.Included.Count())
	for columnIndex := range p.Included.Len() {
		if p.Included.BitAt(columnIndex) {
			indices = append(indices, columnIndex)
		}
	}

	return indices
}

// Clone returns a deep copy of the row, safe to hand to another goroutine. The header fields
// are shared: they are treated as immutable once validated.
func (p *PartialDataRow) Clone() PartialDataRow {
	clone := *p
	clone.groupID = slices.Clone(p.groupID)
	clone.Included = slices.Clone(p.Included)
	clone.partsRequests = slices.Clone(p.partsRequests)
	clone.keep = slices.Clone(p.keep)
	clone.Cells = make([][]byte, len(p.Cells))
	clone.Proofs = make([][]byte, len(p.Proofs))
	for i := range p.Cells {
		clone.Cells[i] = slices.Clone(p.Cells[i])
		clone.Proofs[i] = slices.Clone(p.Proofs[i])
	}

	return clone
}
