package partialdatacolumnbroadcaster

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"testing"

	"github.com/OffchainLabs/go-bitfield"
	"github.com/OffchainLabs/prysm/v7/beacon-chain/core/peerdas"
	"github.com/OffchainLabs/prysm/v7/beacon-chain/verification"
	fieldparams "github.com/OffchainLabs/prysm/v7/config/fieldparams"
	"github.com/OffchainLabs/prysm/v7/consensus-types/blocks"
	"github.com/OffchainLabs/prysm/v7/consensus-types/primitives"
	ethpb "github.com/OffchainLabs/prysm/v7/proto/prysm/v1alpha1"
	"github.com/OffchainLabs/prysm/v7/testing/require"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pubsub_pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

const (
	// kzgCommitmentsInclusionProofDepth mirrors kzg_commitments_inclusion_proof_depth.size in
	// proto/ssz_proto_library.bzl.
	kzgCommitmentsInclusionProofDepth = 4

	testRowSlot      = primitives.Slot(64)
	testRowBlobCount = 4
	testRowPeer      = peer.ID("row-peer")
)

// rowCallbackRecorder records what the broadcaster asks of the application.
type rowCallbackRecorder struct {
	mu sync.Mutex

	headerResult pubsub.ValidationResult
	headerErr    error
	cellsErr     error

	headerCalls  int
	cellsCalls   int
	started      []uint64
	recoverable  []uint64
	complete     []uint64
	notifiedWait sync.WaitGroup
	// requestKeep, when non-nil, is returned from RowRequestInterest along with requestPool.
	requestKeep bitfield.Bitlist
	requestPool bool

	servedElsewhere []uint64
	servedWait      sync.WaitGroup

	// evicted records the group IDs the broadcaster reported as evicted, so a test can assert the
	// application is told to drop its own per-group state.
	evicted []string
}

func (r *rowCallbackRecorder) servedElsewhereSnapshot() []uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()

	return slices.Clone(r.servedElsewhere)
}

func newRowCallbackRecorder() *rowCallbackRecorder {
	return &rowCallbackRecorder{headerResult: pubsub.ValidationAccept}
}

func (r *rowCallbackRecorder) ValidateRowHeader(*ethpb.PartialDataColumnHeader) (pubsub.ValidationResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.headerCalls++

	return r.headerResult, r.headerErr
}

func (r *rowCallbackRecorder) ValidateRowCells([]blocks.CellProofBundle) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.cellsCalls++

	return r.cellsErr
}

func (r *rowCallbackRecorder) RowStarted(_ string, _ []byte, rowIndex uint64) {
	r.mu.Lock()
	r.started = append(r.started, rowIndex)
	r.mu.Unlock()
}

func (r *rowCallbackRecorder) RowRecoverable(_ string, _ []byte, rowIndex uint64) {
	r.mu.Lock()
	r.recoverable = append(r.recoverable, rowIndex)
	r.mu.Unlock()
	r.notifiedWait.Done()
}

func (r *rowCallbackRecorder) RowComplete(_ string, _ []byte, rowIndex uint64) {
	r.mu.Lock()
	r.complete = append(r.complete, rowIndex)
	r.mu.Unlock()
	r.notifiedWait.Done()
}

// RowRequestInterest returns no interest by default: these tests exercise the broadcaster's
// mechanism, and a policy that narrowed the requests would change what the mechanism is asked to
// do. The policy itself is tested where it lives, in beacon-chain/sync and in the blocks package.
//
// requestKeep, when set, overrides that -- the tests that check the broadcaster applies a policy
// at all set it.
// RowServedElsewhere records the cancellation signal so a test can assert it fired exactly once.
func (r *rowCallbackRecorder) RowServedElsewhere(_ string, _ []byte, rowIndex uint64) {
	r.mu.Lock()
	r.servedElsewhere = append(r.servedElsewhere, rowIndex)
	r.mu.Unlock()
	r.servedWait.Done()
}

// RowGroupEvicted records the eviction. Synchronous by contract, so no wait group -- the caller is
// the broadcaster's own loop and the test drives eviction directly.
func (r *rowCallbackRecorder) evictedSnapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]string(nil), r.evicted...)
}

func (r *rowCallbackRecorder) RowGroupEvicted(groupID []byte) {
	r.mu.Lock()
	r.evicted = append(r.evicted, string(groupID))
	r.mu.Unlock()
}

func (r *rowCallbackRecorder) RowRequestInterest(uint64) (bitfield.Bitlist, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.requestKeep, r.requestPool
}

func (r *rowCallbackRecorder) snapshot() (recoverable, complete []uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	return slices.Clone(r.recoverable), slices.Clone(r.complete)
}

// stubColumnCallbacks satisfies the shared parts of ColumnCallbacks that the row path reaches,
// which is only HandleHeader.
type stubColumnCallbacks struct{}

func (stubColumnCallbacks) PartialVerifierFromHeader(*blocks.PartialDataColumn) (*verification.PartialColumnVerifier, pubsub.ValidationResult, error) {
	return nil, pubsub.ValidationIgnore, errors.New("not used")
}

func (stubColumnCallbacks) PartialVerifierFromTrustedColumn(*blocks.PartialDataColumn) (*verification.PartialColumnVerifier, error) {
	return nil, errors.New("not used")
}

func (stubColumnCallbacks) ValidateColumn([]blocks.CellProofBundle) error { return nil }

func (stubColumnCallbacks) HandleColumn(string, blocks.VerifiedRODataColumn) {}

func (stubColumnCallbacks) HandleHeader(*ethpb.PartialDataColumnHeader, string) {}

func (stubColumnCallbacks) ValidateGloasGroupID(primitives.Slot, [32]byte) pubsub.ValidationResult {
	return pubsub.ValidationAccept
}

type publishedRow struct {
	topic string
	row   *blocks.PartialDataRow
}

// rowHarness drives the row paths directly, without the event loop, so the assertions are
// deterministic.
type rowHarness struct {
	t           *testing.T
	broadcaster *PartialColumnBroadcaster
	callbacks   *rowCallbackRecorder
	cancel      context.CancelFunc

	mu        sync.Mutex
	published []publishedRow
	feedback  []pubsub.PeerFeedbackKind
}

func newRowHarness(t *testing.T) *rowHarness {
	t.Helper()

	ctx, cancel := context.WithCancel(t.Context())
	h := &rowHarness{
		t:           t,
		broadcaster: NewBroadcaster(ctx, logrus.New()),
		callbacks:   newRowCallbackRecorder(),
		cancel:      cancel,
	}
	t.Cleanup(cancel)

	h.broadcaster.rowCallbacks = h.callbacks
	h.broadcaster.peerFeedback = func(_ string, _ peer.ID, kind pubsub.PeerFeedbackKind) error {
		h.mu.Lock()
		defer h.mu.Unlock()
		h.feedback = append(h.feedback, kind)
		return nil
	}
	h.broadcaster.publishPartialRow = func(topic string, _ []byte, row *blocks.PartialDataRow) error {
		h.mu.Lock()
		defer h.mu.Unlock()
		h.published = append(h.published, publishedRow{topic: topic, row: row})
		return nil
	}
	// A header first seen on a row topic is handed to the shared header hook, which lives on
	// the column callbacks, so the harness needs those too.
	h.broadcaster.callbacks = stubColumnCallbacks{}
	h.broadcaster.publishPartialCol = func(string, []byte, *blocks.PartialDataColumn) error { return nil }

	return h
}

func (h *rowHarness) subscribe(topic string) {
	h.broadcaster.topics[topic] = nil
	h.broadcaster.subscribedTopics.Store(topic, struct{}{})
}

func (h *rowHarness) feedbackSnapshot() []pubsub.PeerFeedbackKind {
	h.mu.Lock()
	defer h.mu.Unlock()

	return slices.Clone(h.feedback)
}

func (h *rowHarness) publishedSnapshot() []publishedRow {
	h.mu.Lock()
	defer h.mu.Unlock()

	return slices.Clone(h.published)
}

// testRowHeader builds a header for a block at testRowSlot with testRowBlobCount blobs.
func testRowHeader(t *testing.T) (*ethpb.PartialDataColumnHeader, [32]byte) {
	t.Helper()

	signed := &ethpb.SignedBeaconBlockHeader{
		Header: &ethpb.BeaconBlockHeader{
			Slot:       testRowSlot,
			ParentRoot: make([]byte, fieldparams.RootLength),
			StateRoot:  make([]byte, fieldparams.RootLength),
			BodyRoot:   make([]byte, fieldparams.RootLength),
		},
		Signature: make([]byte, 96),
	}
	root, err := signed.Header.HashTreeRoot()
	require.NoError(t, err)

	commitments := make([][]byte, testRowBlobCount)
	for i := range commitments {
		commitments[i] = make([]byte, 48)
		commitments[i][0] = byte(i + 1)
	}

	// The header container cannot encode a proof of any other length, and NewPartialDataRow
	// refuses one, so the placeholder has to be the right shape.
	inclusionProof := make([][]byte, kzgCommitmentsInclusionProofDepth)
	for i := range inclusionProof {
		inclusionProof[i] = make([]byte, fieldparams.RootLength)
	}

	return &ethpb.PartialDataColumnHeader{
		KzgCommitments:               commitments,
		SignedBlockHeader:            signed,
		KzgCommitmentsInclusionProof: inclusionProof,
	}, root
}

// rowTopicFor returns the row topic that carries the given blob at testRowSlot.
func rowTopicFor(t *testing.T, rowIndex uint64) (topic string, subnet uint64) {
	t.Helper()

	subnet, err := peerdas.RowSubnetForBlob(rowIndex, testRowSlot)
	require.NoError(t, err)

	return fmt.Sprintf("/eth2/abcd1234/data_row_%d/ssz_snappy", subnet), subnet
}

// idleRowSubnet returns a row subnet that carries no blob at testRowSlot.
func idleRowSubnet(t *testing.T) uint64 {
	t.Helper()

	for subnet := range uint64(fieldparams.NumberOfColumns) {
		blobIndices, err := peerdas.BlobsForRowSubnet(subnet, testRowSlot, testRowBlobCount)
		require.NoError(t, err)
		if len(blobIndices) == 0 {
			return subnet
		}
	}
	t.Fatal("every row subnet carries a blob")

	return 0
}

func rowCellBytes(fill byte) []byte {
	cell := make([]byte, 2048)
	for i := range cell {
		cell[i] = fill
	}

	return cell
}

// rowMessage builds a row message carrying the given columns, optionally with the header.
func rowMessage(rowIndex uint64, header *ethpb.PartialDataColumnHeader, columnIndices ...uint64) *ethpb.PartialDataRowSidecar {
	bitmap := bitfield.NewBitlist(uint64(fieldparams.NumberOfColumns))
	for _, columnIndex := range columnIndices {
		bitmap.SetBitAt(columnIndex, true)
	}

	message := &ethpb.PartialDataRowSidecar{RowIndex: rowIndex, CellsPresentBitmap: bitmap}
	for columnIndex := range uint64(fieldparams.NumberOfColumns) {
		if !bitmap.BitAt(columnIndex) {
			continue
		}
		message.PartialRow = append(message.PartialRow, rowCellBytes(byte(columnIndex)))
		message.KzgProofs = append(message.KzgProofs, make([]byte, 48))
	}
	if header != nil {
		message.Header = []*ethpb.PartialDataColumnHeader{header}
	}

	return message
}

func rowRPC(t *testing.T, topic string, subnet uint64, root [32]byte, message *ethpb.PartialDataRowSidecar) incomingRowRPC {
	t.Helper()

	groupID := groupIDForRoot(root)

	return incomingRowRPC{
		PartialMessagesExtension: &pubsub_pb.PartialMessagesExtension{
			TopicID: &topic,
			GroupID: groupID,
		},
		from:      testRowPeer,
		message:   message,
		rowSubnet: subnet,
		root:      root,
	}
}

// groupIDForRoot mirrors the Fulu partial-column group id, which rows share.
func groupIDForRoot(root [32]byte) []byte {
	groupID := make([]byte, 0, len(root)+1)
	groupID = append(groupID, 0x00)

	return append(groupID, root[:]...)
}

func TestRowIndexForSubnet(t *testing.T) {
	t.Run("the subnet carrying a blob resolves to it", func(t *testing.T) {
		for rowIndex := range uint64(testRowBlobCount) {
			_, subnet := rowTopicFor(t, rowIndex)
			got, err := rowIndexForSubnet(subnet, testRowSlot, testRowBlobCount)
			require.NoError(t, err)
			require.Equal(t, rowIndex, got)
		}
	})

	t.Run("an idle subnet is an error", func(t *testing.T) {
		_, err := rowIndexForSubnet(idleRowSubnet(t), testRowSlot, testRowBlobCount)
		require.ErrorIs(t, err, errRowSubnetCarriesNo)
	})

	t.Run("more blobs than subnets is unsupported", func(t *testing.T) {
		// Two rows on one subnet cannot be represented by one group id, so this is refused
		// rather than silently handling only the first. Blob 0 and blob ROW_SUBNET_COUNT
		// share a subnet, so that is the one to ask about.
		_, sharedSubnet := rowTopicFor(t, 0)
		_, err := rowIndexForSubnet(sharedSubnet, testRowSlot, uint64(fieldparams.NumberOfColumns)+1)
		require.ErrorIs(t, err, errRowSubnetMultiRow)
	})
}

func TestHandleIncomingRowRPC_CreatesRowFromHeader(t *testing.T) {
	h := newRowHarness(t)
	header, root := testRowHeader(t)
	topic, subnet := rowTopicFor(t, 2)
	h.subscribe(topic)

	rpc := rowRPC(t, topic, subnet, root, rowMessage(2, header, 5))
	require.NoError(t, h.broadcaster.handleIncomingRowRPC(rpc))

	entry := h.broadcaster.getRowEntry(topic, rpc.GroupID)
	require.NotNil(t, entry)
	require.Equal(t, uint64(2), entry.row.RowIndex())
	require.Equal(t, testRowSlot, entry.row.Slot())
	require.Equal(t, uint64(fieldparams.NumberOfColumns), entry.row.Included.Len())
	require.Equal(t, 1, h.callbacks.headerCalls)

	// The header is cached by group id, shared with the column path.
	require.NotNil(t, h.broadcaster.validHeaderCache[string(rpc.GroupID)])

	// Nothing is published before we have published our own state, because announcing an
	// empty row would only tell peers to stop asking.
	require.Equal(t, 0, len(h.publishedSnapshot()))
}

func TestHandleIncomingRowRPC_RejectsRowIndexNotOnThisSubnet(t *testing.T) {
	// The derived row index is authoritative; the field on the wire is a cross-check.
	h := newRowHarness(t)
	header, root := testRowHeader(t)
	topic, subnet := rowTopicFor(t, 2)
	h.subscribe(topic)

	rpc := rowRPC(t, topic, subnet, root, rowMessage(3, header, 5))
	require.NoError(t, h.broadcaster.handleIncomingRowRPC(rpc))

	require.IsNil(t, h.broadcaster.getRowEntry(topic, rpc.GroupID))
	require.DeepEqual(t, []pubsub.PeerFeedbackKind{pubsub.PeerFeedbackInvalidMessage}, h.feedbackSnapshot())
}

func TestHandleIncomingRowRPC_RejectsCellsOnAnIdleSubnet(t *testing.T) {
	// With 4 blobs, 124 of 128 row subnets carry nothing. Which ones is public knowledge, so
	// a peer sending cells there is at fault.
	h := newRowHarness(t)
	header, root := testRowHeader(t)
	subnet := idleRowSubnet(t)
	topic := fmt.Sprintf("/eth2/abcd1234/data_row_%d/ssz_snappy", subnet)
	h.subscribe(topic)

	rpc := rowRPC(t, topic, subnet, root, rowMessage(0, header, 1))
	require.NoError(t, h.broadcaster.handleIncomingRowRPC(rpc))

	require.IsNil(t, h.broadcaster.getRowEntry(topic, rpc.GroupID))
	require.DeepEqual(t, []pubsub.PeerFeedbackKind{pubsub.PeerFeedbackInvalidMessage}, h.feedbackSnapshot())
}

func TestHandleIncomingRowRPC_RejectsHeaderRootMismatch(t *testing.T) {
	h := newRowHarness(t)
	header, _ := testRowHeader(t)
	topic, subnet := rowTopicFor(t, 1)
	h.subscribe(topic)

	var wrongRoot [32]byte
	wrongRoot[0] = 0xff
	rpc := rowRPC(t, topic, subnet, wrongRoot, rowMessage(1, header, 3))
	require.NoError(t, h.broadcaster.handleIncomingRowRPC(rpc))

	require.IsNil(t, h.broadcaster.getRowEntry(topic, rpc.GroupID))
	require.DeepEqual(t, []pubsub.PeerFeedbackKind{pubsub.PeerFeedbackInvalidMessage}, h.feedbackSnapshot())
}

func TestHandleIncomingRowRPC_IgnoresRejectedHeader(t *testing.T) {
	h := newRowHarness(t)
	h.callbacks.headerResult = pubsub.ValidationReject
	h.callbacks.headerErr = errors.New("bad header")

	header, root := testRowHeader(t)
	topic, subnet := rowTopicFor(t, 0)
	h.subscribe(topic)

	rpc := rowRPC(t, topic, subnet, root, rowMessage(0, header, 1))
	require.NoError(t, h.broadcaster.handleIncomingRowRPC(rpc))

	require.IsNil(t, h.broadcaster.getRowEntry(topic, rpc.GroupID))
	require.DeepEqual(t, []pubsub.PeerFeedbackKind{pubsub.PeerFeedbackInvalidMessage}, h.feedbackSnapshot())
	require.IsNil(t, h.broadcaster.validHeaderCache[string(rpc.GroupID)])
}

func TestHandleIncomingRowRPC_CellsWithoutHeaderAreDropped(t *testing.T) {
	// Rows have no full-message form, so without a header there is no way to know which blob
	// the cells belong to, let alone verify them.
	h := newRowHarness(t)
	_, root := testRowHeader(t)
	topic, subnet := rowTopicFor(t, 1)
	h.subscribe(topic)

	rpc := rowRPC(t, topic, subnet, root, rowMessage(1, nil, 2))
	require.NoError(t, h.broadcaster.handleIncomingRowRPC(rpc))

	require.IsNil(t, h.broadcaster.getRowEntry(topic, rpc.GroupID))
	require.Equal(t, 0, len(h.feedbackSnapshot()), "an absent header is not the peer's fault")
	require.Equal(t, 0, h.callbacks.headerCalls)
}

func TestHandleIncomingRowRPC_MetadataForAnUnknownRowIsDropped(t *testing.T) {
	h := newRowHarness(t)
	_, root := testRowHeader(t)
	topic, subnet := rowTopicFor(t, 1)
	h.subscribe(topic)

	rpc := rowRPC(t, topic, subnet, root, nil)
	require.NoError(t, h.broadcaster.handleIncomingRowRPC(rpc))
	require.IsNil(t, h.broadcaster.getRowEntry(topic, rpc.GroupID))
}

func TestHandleIncomingRowRPC_IgnoresUnsubscribedTopic(t *testing.T) {
	h := newRowHarness(t)
	header, root := testRowHeader(t)
	topic, subnet := rowTopicFor(t, 1)
	// Deliberately not subscribed.

	rpc := rowRPC(t, topic, subnet, root, rowMessage(1, header, 2))
	require.NoError(t, h.broadcaster.handleIncomingRowRPC(rpc))
	require.IsNil(t, h.broadcaster.getRowEntry(topic, rpc.GroupID))
	require.Equal(t, 0, h.callbacks.headerCalls)
}

func TestHandleIncomingRowRPC_RowsDisabled(t *testing.T) {
	h := newRowHarness(t)
	h.broadcaster.rowCallbacks = nil

	header, root := testRowHeader(t)
	topic, subnet := rowTopicFor(t, 1)
	h.subscribe(topic)

	rpc := rowRPC(t, topic, subnet, root, rowMessage(1, header, 2))
	require.ErrorIs(t, h.broadcaster.handleIncomingRowRPC(rpc), errRowsDisabled)
	require.IsNil(t, h.broadcaster.getRowEntry(topic, rpc.GroupID))
}

// seedRow puts a row entry in the store, as if a header had arrived, and marks it published.
func seedRow(t *testing.T, h *rowHarness, rowIndex uint64) (topic string, groupID []byte, entry *rowEntry) {
	t.Helper()

	header, root := testRowHeader(t)
	topic, subnet := rowTopicFor(t, rowIndex)
	h.subscribe(topic)

	// Header only: a seeded row should not also kick off cell validation, which would run off
	// the loop and race the assertions.
	rpc := rowRPC(t, topic, subnet, root, rowMessage(rowIndex, header))
	require.NoError(t, h.broadcaster.handleIncomingRowRPC(rpc))

	entry = h.broadcaster.getRowEntry(topic, rpc.GroupID)
	require.NotNil(t, entry)
	entry.row.Published = true

	return topic, rpc.GroupID, entry
}

func TestHandleRowCellsValidated_ExtendsAndNotifiesOnce(t *testing.T) {
	h := newRowHarness(t)
	topic, groupID, entry := seedRow(t, h, 2)

	threshold := blocks.ReconstructionThreshold()

	// One cell short of the threshold: no notification yet.
	h.callbacks.notifiedWait.Add(1) // consumed by the recoverable notification below
	require.NoError(t, h.broadcaster.handleRowCellsValidated(rowCellsFor(topic, groupID, 0, threshold-1)))
	recoverable, complete := h.callbacks.snapshot()
	require.Equal(t, 0, len(recoverable))
	require.Equal(t, 0, len(complete))
	require.Equal(t, threshold-1, entry.row.Included.Count())

	// The threshold cell fires RowRecoverable exactly once.
	require.NoError(t, h.broadcaster.handleRowCellsValidated(rowCellsFor(topic, groupID, threshold-1, threshold)))
	h.callbacks.notifiedWait.Wait()
	recoverable, complete = h.callbacks.snapshot()
	require.DeepEqual(t, []uint64{2}, recoverable)
	require.Equal(t, 0, len(complete), "recoverable is not complete")

	// Filling the rest fires RowComplete once, and does not repeat RowRecoverable.
	h.callbacks.notifiedWait.Add(1)
	require.NoError(t, h.broadcaster.handleRowCellsValidated(rowCellsFor(topic, groupID, threshold, uint64(fieldparams.NumberOfColumns))))
	h.callbacks.notifiedWait.Wait()
	recoverable, complete = h.callbacks.snapshot()
	require.DeepEqual(t, []uint64{2}, recoverable)
	require.DeepEqual(t, []uint64{2}, complete)
	require.Equal(t, true, entry.row.IsComplete())

	// Every extension republishes, because our bitmap changed. That holds with coalescing disabled,
	// which is this harness's default; see the coalescing test for what the window does to it.
	require.Equal(t, true, len(h.publishedSnapshot()) >= 3)
}

// TestHandleRowCellsValidated_PublishesWithoutThrottling: the publish that follows availability
// growth is not delayed. The per-group window that once throttled it also delayed the cells that
// answer a peer's request; holding is now per peer, inside the publish decision, and tested there
// (consensus-types/blocks). Here, every batch of validated cells publishes at once.
func TestHandleRowCellsValidated_PublishesWithoutThrottling(t *testing.T) {
	h := newRowHarness(t)
	topic, groupID, _ := seedRow(t, h, 2)

	before := len(h.publishedSnapshot())
	for i := range uint64(3) {
		require.NoError(t, h.broadcaster.handleRowCellsValidated(rowCellsFor(topic, groupID, 10+i, 11+i)))
	}
	require.Equal(t, before+3, len(h.publishedSnapshot()), "each batch publishes; nothing waits for a window")
}

func TestHandleRowCellsValidated_DuplicateCellsDoNotRepublish(t *testing.T) {
	h := newRowHarness(t)
	topic, groupID, _ := seedRow(t, h, 1)

	require.NoError(t, h.broadcaster.handleRowCellsValidated(rowCellsFor(topic, groupID, 0, 4)))
	published := len(h.publishedSnapshot())

	require.NoError(t, h.broadcaster.handleRowCellsValidated(rowCellsFor(topic, groupID, 0, 4)))
	require.Equal(t, published, len(h.publishedSnapshot()), "re-offered cells add nothing, so nothing is republished")
}

func TestHandleRowCellsValidated_UnknownGroupIsDropped(t *testing.T) {
	// A group can be evicted while its cells are being validated off the loop.
	h := newRowHarness(t)
	topic, _ := rowTopicFor(t, 1)
	var root [32]byte
	require.NoError(t, h.broadcaster.handleRowCellsValidated(rowCellsFor(topic, groupIDForRoot(root), 0, 2)))
}

// rowCellsFor builds a validated-cells event for the columns in [from, to).
func rowCellsFor(topic string, groupID []byte, from, to uint64) *rowCellsValidated {
	out := &rowCellsValidated{topic: topic, group: groupID}
	for columnIndex := from; columnIndex < to; columnIndex++ {
		out.columnIndices = append(out.columnIndices, columnIndex)
		out.cells = append(out.cells, blocks.CellProofBundle{
			ColumnIndex: columnIndex,
			Cell:        rowCellBytes(byte(columnIndex)),
			Proof:       make([]byte, 48),
		})
	}

	return out
}

func TestPublishRowOnLoopAndSnapshot(t *testing.T) {
	h := newRowHarness(t)
	header, root := testRowHeader(t)
	topic, _ := rowTopicFor(t, 3)
	h.subscribe(topic)

	row, err := blocks.NewPartialDataRow(root, header.SignedBlockHeader, 3, header.KzgCommitments, header.KzgCommitmentsInclusionProof)
	require.NoError(t, err)
	require.Equal(t, true, row.ExtendFromVerifiedCell(9, rowCellBytes(9), make([]byte, 48)))

	require.NoError(t, h.broadcaster.publishRowOnLoop(topic, row))

	entry := h.broadcaster.getRowEntry(topic, row.GroupID())
	require.NotNil(t, entry)
	require.Equal(t, true, entry.row.Published)
	require.Equal(t, uint64(1), entry.row.Included.Count())
	require.Equal(t, 1, len(h.publishedSnapshot()))

	// Publishing again merges rather than replacing.
	second, err := blocks.NewPartialDataRow(root, header.SignedBlockHeader, 3, header.KzgCommitments, header.KzgCommitmentsInclusionProof)
	require.NoError(t, err)
	require.Equal(t, true, second.ExtendFromVerifiedCell(10, rowCellBytes(10), make([]byte, 48)))
	require.NoError(t, h.broadcaster.publishRowOnLoop(topic, second))
	require.Equal(t, uint64(2), entry.row.Included.Count())

	// The snapshot is a deep copy: mutating it must not touch loop-owned state.
	request := &rowSnapshotRequest{topic: topic, groupID: row.GroupID()}
	require.NoError(t, h.broadcaster.rowSnapshotOnLoop(request))
	require.NotNil(t, request.out)
	require.Equal(t, uint64(2), request.out.Included.Count())
	request.out.Cells[9][0] ^= 0xff
	require.Equal(t, false, request.out.Cells[9][0] == entry.row.Cells[9][0])

	// An unknown group snapshots to nil rather than erroring.
	var other [32]byte
	other[0] = 0x7f
	missing := &rowSnapshotRequest{topic: topic, groupID: groupIDForRoot(other)}
	require.NoError(t, h.broadcaster.rowSnapshotOnLoop(missing))
	require.IsNil(t, missing.out)
}

func TestPublishRowOnLoopRefusesARowIndexMismatch(t *testing.T) {
	h := newRowHarness(t)
	header, root := testRowHeader(t)
	topic, _ := rowTopicFor(t, 3)
	h.subscribe(topic)

	first, err := blocks.NewPartialDataRow(root, header.SignedBlockHeader, 3, header.KzgCommitments, header.KzgCommitmentsInclusionProof)
	require.NoError(t, err)
	require.NoError(t, h.broadcaster.publishRowOnLoop(topic, first))

	// Same block, same topic, different blob: the group id cannot address both.
	second, err := blocks.NewPartialDataRow(root, header.SignedBlockHeader, 2, header.KzgCommitments, header.KzgCommitmentsInclusionProof)
	require.NoError(t, err)
	require.NotNil(t, h.broadcaster.publishRowOnLoop(topic, second))
}

// TestRowRequestPolicyIsAppliedWhenStateIsCreated is the wiring for the request policy, and it has
// to be checked at creation rather than later: the request bitmap goes out with the first parts
// metadata, so a narrowing applied after the fact has already leaked the wide request.
//
// The interest here says "I keep cells 0 and 1, and I am not pooling", which is not a realistic
// policy -- it is chosen so the assertion cannot pass by accident on a row asking for everything.
func TestRowRequestPolicyIsAppliedWhenStateIsCreated(t *testing.T) {
	h := newRowHarness(t)
	header, root := testRowHeader(t)
	topic, subnet := rowTopicFor(t, 1)
	h.subscribe(topic)

	keep := bitfield.NewBitlist(uint64(fieldparams.NumberOfColumns))
	keep.SetBitAt(0, true)
	keep.SetBitAt(1, true)
	h.callbacks.requestKeep = keep
	h.callbacks.requestPool = false

	rpc := rowRPC(t, topic, subnet, root, rowMessage(1, header, 5))
	require.NoError(t, h.broadcaster.handleIncomingRowRPC(rpc))

	entry := h.broadcaster.getRowEntry(topic, rpc.GroupID)
	require.NotNil(t, entry)

	got, pooling, ok := entry.row.RequestInterest()
	require.Equal(t, true, ok, "the policy should have been recorded on the row")
	require.Equal(t, uint64(2), got.Count())
	require.Equal(t, false, pooling)

	// How the interest turns into a request bitmap is TestPartialDataRow_WantedParts's business,
	// in the blocks package where the rule lives. What this test owns is that the broadcaster
	// asked for a policy and applied it where state is created.
}

// TestRowRequestPolicyDefersToAnExplicitPublish: a caller that set its own request bitmap knows
// more about that publish than the policy does -- the pull arm naming one cell, a recovered row
// asking for nothing -- so the policy must not overwrite it.
func TestRowRequestPolicyDefersToAnExplicitPublish(t *testing.T) {
	h := newRowHarness(t)
	header, root := testRowHeader(t)
	topic, _ := rowTopicFor(t, 1)
	h.subscribe(topic)

	keep := bitfield.NewBitlist(uint64(fieldparams.NumberOfColumns))
	keep.SetBitAt(0, true)
	h.callbacks.requestKeep = keep

	row, err := blocks.NewPartialDataRow(root, header.SignedBlockHeader, 1,
		header.KzgCommitments, header.KzgCommitmentsInclusionProof)
	require.NoError(t, err)
	explicit := bitfield.NewBitlist(uint64(fieldparams.NumberOfColumns))
	explicit.SetBitAt(9, true)
	require.NoError(t, row.SetPartsRequests(explicit))

	require.NoError(t, h.broadcaster.publishRowOnLoop(topic, row))

	entry := h.broadcaster.getRowEntry(topic, row.GroupID())
	require.NotNil(t, entry)
	requests, ok := entry.row.PartsRequests()
	require.Equal(t, true, ok)
	require.Equal(t, uint64(1), requests.Count())
	require.Equal(t, true, requests.BitAt(9), "the caller's own request must survive")
}

// TestRowServedElsewhereNeedsBothHalves is the cancellation signal the reconstruction phases were
// missing, and the point of the test is that one half is not enough.
//
// A peer claiming the whole row is unverified. What makes it safe to act on is the second half --
// that we want nothing further from the row -- because a peer that cannot serve what it claims
// cannot have satisfied us either. So the claim alone must not fire the callback.
func TestRowServedElsewhereNeedsBothHalves(t *testing.T) {
	h := newRowHarness(t)
	header, root := testRowHeader(t)
	topic, subnet := rowTopicFor(t, 1)
	h.subscribe(topic)

	// This node keeps only cell 3 and does not pool, so "nothing further wanted" is reachable.
	keep := bitfield.NewBitlist(uint64(fieldparams.NumberOfColumns))
	keep.SetBitAt(3, true)
	h.callbacks.requestKeep = keep
	h.callbacks.requestPool = false

	rpc := rowRPC(t, topic, subnet, root, rowMessage(1, header, 9))
	require.NoError(t, h.broadcaster.handleIncomingRowRPC(rpc))
	entry := h.broadcaster.getRowEntry(topic, rpc.GroupID)
	require.NotNil(t, entry)
	require.Equal(t, uint64(1), entry.row.MissingWantedCount(), "cell 3 is still wanted")

	// The claim, with cell 3 still outstanding: recorded, but not acted on.
	h.broadcaster.handleRowPeerHasWholeRow(rowPeerHasWhole{topic: topic, groupID: rpc.GroupID, from: testRowPeer})
	require.Equal(t, true, entry.peerHasWholeRow)
	require.Equal(t, 0, len(h.callbacks.servedElsewhereSnapshot()),
		"a claim alone must not stand a node down while it still wants cells")

	// Cell 3 arrives, so there is nothing further to want and the signal fires.
	h.callbacks.servedWait.Add(1)
	require.Equal(t, true, entry.row.ExtendFromVerifiedCell(3, make([]byte, 2048), make([]byte, 48)))
	h.broadcaster.notifyRowProgress(topic, entry)
	h.callbacks.servedWait.Wait()
	require.DeepEqual(t, []uint64{1}, h.callbacks.servedElsewhereSnapshot())

	// And it is a latch: further progress does not re-fire it.
	h.broadcaster.notifyRowProgress(topic, entry)
	h.broadcaster.handleRowPeerHasWholeRow(rowPeerHasWhole{topic: topic, groupID: rpc.GroupID, from: testRowPeer})
	require.Equal(t, 1, len(h.callbacks.servedElsewhereSnapshot()))
}

// TestRowPeerHasWholeRowIgnoresUnknownGroups: the claim carries a peer-controlled group id, so it
// must not be able to allocate row state for a group we know nothing about.
func TestRowPeerHasWholeRowIgnoresUnknownGroups(t *testing.T) {
	h := newRowHarness(t)
	topic, _ := rowTopicFor(t, 1)
	h.subscribe(topic)

	h.broadcaster.handleRowPeerHasWholeRow(rowPeerHasWhole{topic: topic, groupID: []byte("nope"), from: testRowPeer})
	require.Equal(t, 0, len(h.callbacks.servedElsewhereSnapshot()))
	require.IsNil(t, h.broadcaster.getRowEntry(topic, []byte("nope")))
}

// testRowHeaderWithSalt is testRowHeader with a distinct block root: the salt goes into the parent
// root. It is how the equivocation tests make several blocks that all validate at one slot.
func testRowHeaderWithSalt(t *testing.T, salt byte) (*ethpb.PartialDataColumnHeader, [32]byte) {
	t.Helper()

	header, _ := testRowHeader(t)
	header.SignedBlockHeader.Header.ParentRoot[0] = salt
	root, err := header.SignedBlockHeader.Header.HashTreeRoot()
	require.NoError(t, err)

	return header, root
}

// feedRowHeader delivers a header-only row message for a salted block and reports whether the
// broadcaster allocated state for it. Unlike seedRow it does not require admission.
func feedRowHeader(t *testing.T, h *rowHarness, rowIndex uint64, salt byte) (groupID []byte, entry *rowEntry) {
	t.Helper()

	header, root := testRowHeaderWithSalt(t, salt)
	topic, subnet := rowTopicFor(t, rowIndex)
	h.subscribe(topic)

	rpc := rowRPC(t, topic, subnet, root, rowMessage(rowIndex, header))
	require.NoError(t, h.broadcaster.handleIncomingRowRPC(rpc))

	return rpc.GroupID, h.broadcaster.getRowEntry(topic, rpc.GroupID)
}

func setRowGroupCaps(t *testing.T, perTopic, perSlot int) {
	t.Helper()

	prevTopic, prevSlot := MaxRowGroupsPerTopic, MaxRowGroupsPerSlot
	MaxRowGroupsPerTopic, MaxRowGroupsPerSlot = perTopic, perSlot
	t.Cleanup(func() { MaxRowGroupsPerTopic, MaxRowGroupsPerSlot = prevTopic, prevSlot })
}

// TestRowGroupCapPerSlot is the equivocation bound. Every header here validates -- the harness
// accepts them all -- so without the bound each competing root would get row state, cross-fill,
// callbacks and a publish. EIP-8371 line 102 makes reconstruction optional for competing roots so
// equivocation cannot amplify work; the duty ledger already declines the *duty*, and this is the
// same rule applied to the state that was being allocated before the ledger was ever consulted.
func TestRowGroupCapPerSlot(t *testing.T) {
	setRowGroupCaps(t, 100, 2)
	h := newRowHarness(t)
	topic, _ := rowTopicFor(t, 2)

	_, first := feedRowHeader(t, h, 2, 1)
	_, second := feedRowHeader(t, h, 2, 2)
	refusedID, third := feedRowHeader(t, h, 2, 3)

	require.NotNil(t, first)
	require.NotNil(t, second)
	require.IsNil(t, third, "the third root at one slot must get no row state")
	require.Equal(t, 2, len(h.broadcaster.rowStore[topic]))

	// Not the peer's fault: it relayed a header that validates. No downscore.
	require.Equal(t, 0, len(h.feedbackSnapshot()))
	// But the header cached under the refused id must not outlive the admitted groups.
	_, hasTTL := h.broadcaster.groupTTL[string(refusedID)]
	require.Equal(t, true, hasTTL, "a refused group still ages out")

	// A refused group's cells are not merely unstored -- a later message for it is ignored
	// wholesale, so an equivocator cannot keep us busy validating cells for a root we refused.
	header, root := testRowHeaderWithSalt(t, 3)
	_, subnet := rowTopicFor(t, 2)
	rpc := rowRPC(t, topic, subnet, root, rowMessage(2, header, 5, 6))
	require.NoError(t, h.broadcaster.handleIncomingRowRPC(rpc))
	require.IsNil(t, h.broadcaster.getRowEntry(topic, refusedID))
	require.Equal(t, 2, len(h.broadcaster.rowStore[topic]))
}

// TestRowGroupCapPerTopic is the belt: the per-slot bound only bounds the total if header
// validation confines slots to a window, and that is the application's rule rather than ours.
func TestRowGroupCapPerTopic(t *testing.T) {
	setRowGroupCaps(t, 2, 100)
	h := newRowHarness(t)
	topic, _ := rowTopicFor(t, 2)

	for salt := byte(1); salt <= 2; salt++ {
		_, entry := feedRowHeader(t, h, 2, salt)
		require.NotNil(t, entry)
	}
	_, refused := feedRowHeader(t, h, 2, 3)
	require.IsNil(t, refused)
	require.Equal(t, 2, len(h.broadcaster.rowStore[topic]))
	require.Equal(t, 0, len(h.feedbackSnapshot()))
}

// TestRowGroupCapReleasesOnEviction: the bound is a bound on live state, not a ban list. Once the
// admitted groups age out, the refused root is admissible again -- which matters for a real
// reorg, where the root refused earlier may be the one the chain settles on.
func TestRowGroupCapReleasesOnEviction(t *testing.T) {
	setRowGroupCaps(t, 100, 1)
	h := newRowHarness(t)
	topic, _ := rowTopicFor(t, 2)

	_, first := feedRowHeader(t, h, 2, 1)
	require.NotNil(t, first)
	_, refused := feedRowHeader(t, h, 2, 2)
	require.IsNil(t, refused)

	for range TTLInSlots + 1 {
		h.broadcaster.evictExpiredGroups()
	}
	require.Equal(t, 0, len(h.broadcaster.rowStore[topic]))

	_, admitted := feedRowHeader(t, h, 2, 2)
	require.NotNil(t, admitted)
}

// TestRowGroupCapAppliesToOwnPublish: the bound is on the store, so a publish from this node --
// a recovered row, a pull request -- is held to it too, and is told, because for the reconstruction
// scheduler a silently dropped publish would mean a recovered row went nowhere on this axis.
func TestRowGroupCapAppliesToOwnPublish(t *testing.T) {
	setRowGroupCaps(t, 100, 1)
	h := newRowHarness(t)
	topic, _ := rowTopicFor(t, 3)

	_, first := feedRowHeader(t, h, 3, 1)
	require.NotNil(t, first)

	header, root := testRowHeaderWithSalt(t, 2)
	row, err := blocks.NewPartialDataRow(root, header.SignedBlockHeader, 3, header.KzgCommitments, header.KzgCommitmentsInclusionProof)
	require.NoError(t, err)

	err = h.broadcaster.publishRowOnLoop(topic, row)
	require.Equal(t, true, errors.Is(err, errRowGroupCapReached), "got %v", err)
	require.IsNil(t, h.broadcaster.getRowEntry(topic, row.GroupID()))
	require.Equal(t, 0, len(h.publishedSnapshot()))
}

// TestRowTimerEventsAfterEvictionAreInert covers the review's late-timer concern without a group
// generation: both timer handlers look the group up by key and do nothing when it is gone, and
// eviction stops and forgets the timers themselves. A fired-but-undelivered event that arrives
// after eviction -- or after the same id was re-created -- can at most cause one early flush.
func TestRowTimerEventsAfterEvictionAreInert(t *testing.T) {
	h := newRowHarness(t)
	topic, groupID, _ := seedRow(t, h, 2)
	key := coalesceKey{topic: topic, groupID: string(groupID)}

	for range TTLInSlots + 1 {
		h.broadcaster.evictExpiredGroups()
	}
	require.IsNil(t, h.broadcaster.getRowEntry(topic, groupID))

	before := len(h.publishedSnapshot())
	h.broadcaster.handleClaimWake(key)
	require.Equal(t, before, len(h.publishedSnapshot()), "a stale timer event must publish nothing")
	require.Equal(t, 0, len(h.broadcaster.rowClaimWake))
}

func TestRowStateIsEvictedWithItsGroup(t *testing.T) {
	h := newRowHarness(t)
	topic, groupID, _ := seedRow(t, h, 2)
	require.NotNil(t, h.broadcaster.getRowEntry(topic, groupID))

	// TTLInSlots ticks, then one more to cross zero.
	for range TTLInSlots + 1 {
		h.broadcaster.evictExpiredGroups()
	}

	require.IsNil(t, h.broadcaster.getRowEntry(topic, groupID))
	require.Equal(t, 0, len(h.broadcaster.rowStore))
}

// TestRowGroupEvictionNotifiesApplication pins the hook the application needs to bound its own
// per-group state.
//
// The reconstruction scheduler keeps `done` and `servedElsewhere` maps whose comments claimed to be
// "cleared with the group's TTL by the broadcaster" -- but no eviction hook existed, so nothing
// cleared them and they grew for the lifetime of the process. The claim is only true if the
// broadcaster says so, which is what this asserts.
func TestRowGroupEvictionNotifiesApplication(t *testing.T) {
	h := newRowHarness(t)
	topic, groupID, _ := seedRow(t, h, 2)
	require.NotNil(t, h.broadcaster.getRowEntry(topic, groupID))

	// Not before the TTL runs out: an early notice would clear state the group still needs.
	for range TTLInSlots {
		h.broadcaster.evictExpiredGroups()
	}
	require.Equal(t, 0, len(h.callbacks.evictedSnapshot()))

	h.broadcaster.evictExpiredGroups()

	evicted := h.callbacks.evictedSnapshot()
	require.Equal(t, 1, len(evicted))
	require.Equal(t, string(groupID), evicted[0])

	// Once, not once per tick. A repeated notice would be harmless for a map clear and wrong for
	// anything that counts.
	h.broadcaster.evictExpiredGroups()
	require.Equal(t, 1, len(h.callbacks.evictedSnapshot()))
}

// TestRowGroupEvictionWithoutRowCallbacks: RowDAS off must not panic on the new hook.
func TestRowGroupEvictionWithoutRowCallbacks(t *testing.T) {
	h := newRowHarness(t)
	seedRow(t, h, 2)
	h.broadcaster.rowCallbacks = nil

	for range TTLInSlots + 1 {
		h.broadcaster.evictExpiredGroups()
	}

	require.Equal(t, 0, len(h.broadcaster.groupTTL))
}

func TestUnsubscribeDropsRowState(t *testing.T) {
	h := newRowHarness(t)
	topic, groupID, _ := seedRow(t, h, 2)
	require.NotNil(t, h.broadcaster.getRowEntry(topic, groupID))

	require.NoError(t, h.broadcaster.unsubscribe(topic))
	require.IsNil(t, h.broadcaster.getRowEntry(topic, groupID))
}

// TestRowPartsMetadataChecksBothBitmaps covers the half-check a review found: only Available was
// length-checked, while Requests is the bitmap cellsToSendToPeer intersects with our own. A
// wrong-length Requests therefore poisoned that peer's state for the lifetime of the group --
// every later publish to it failed on a length mismatch.
func TestRowPartsMetadataChecksBothBitmaps(t *testing.T) {
	cases := []struct {
		name      string
		available uint64
		requests  uint64
		wantOK    bool
	}{
		{name: "both correct", available: fieldparams.NumberOfColumns, requests: fieldparams.NumberOfColumns, wantOK: true},
		{name: "available too long", available: fieldparams.NumberOfColumns * 2, requests: fieldparams.NumberOfColumns},
		{name: "requests too long", available: fieldparams.NumberOfColumns, requests: fieldparams.NumberOfColumns * 2},
		{name: "requests too short", available: fieldparams.NumberOfColumns, requests: 8},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			meta := &ethpb.PartialDataColumnPartsMetadata{
				Available: bitfield.NewBitlist(tc.available),
				Requests:  bitfield.NewBitlist(tc.requests),
			}
			encoded, err := meta.MarshalSSZ()
			require.NoError(t, err)

			topic := "/eth2/abcd1234/data_row_3/ssz_snappy"
			rpc := &pubsub_pb.PartialMessagesExtension{TopicID: &topic, PartsMetadata: encoded}
			_, _, err = updateRowPeerStateFromIncomingRPC(blocks.PartialDataColumnPeerState{}, rpc)
			if tc.wantOK {
				require.NoError(t, err)
				return
			}
			require.NotNil(t, err)
			// Malformed on a row topic is the peer's fault, so it must be downscorable.
			require.ErrorIs(t, err, errMalformedPartialMessage)
		})
	}
}

// TestRowMalformedMessagesAreDownscorable covers the second half of that finding: several
// peer-attributable errors returned raw errors, which onIncomingRowRPC does not penalise, so a
// peer could send undecodable SSZ indefinitely for free.
func TestRowMalformedMessagesAreDownscorable(t *testing.T) {
	topic := "/eth2/abcd1234/data_row_3/ssz_snappy"

	t.Run("undecodable parts metadata", func(t *testing.T) {
		rpc := &pubsub_pb.PartialMessagesExtension{TopicID: &topic, PartsMetadata: []byte{0xff, 0xff, 0xff}}
		_, _, err := updateRowPeerStateFromIncomingRPC(blocks.PartialDataColumnPeerState{}, rpc)
		require.ErrorIs(t, err, errMalformedPartialMessage)
	})

	t.Run("undecodable row sidecar", func(t *testing.T) {
		rpc := &pubsub_pb.PartialMessagesExtension{TopicID: &topic, PartialMessage: []byte{0xff, 0xff, 0xff}}
		_, _, err := updateRowPeerStateFromIncomingRPC(blocks.PartialDataColumnPeerState{}, rpc)
		require.ErrorIs(t, err, errMalformedPartialMessage)
	})

	t.Run("cell and proof counts disagreeing with the bitmap", func(t *testing.T) {
		message := rowMessage(3, nil, 1, 2)
		message.KzgProofs = message.KzgProofs[:1]
		encoded, err := message.MarshalSSZ()
		require.NoError(t, err)

		rpc := &pubsub_pb.PartialMessagesExtension{TopicID: &topic, PartialMessage: encoded}
		_, _, err = updateRowPeerStateFromIncomingRPC(blocks.PartialDataColumnPeerState{}, rpc)
		require.ErrorIs(t, err, errMalformedPartialMessage)
	})
}

// TestPublishRowDoesNotAdoptTheCallersSlices covers an aliasing bug a review found: PublishRow
// takes a PartialDataRow by value, but its bitmap and cell slices still alias the caller's, so
// storing it directly let the caller mutate loop-owned state from another goroutine. A
// reconstruction scheduler does exactly that -- publishes, then extends its own copy.
func TestPublishRowDoesNotAdoptTheCallersSlices(t *testing.T) {
	h := newRowHarness(t)
	header, root := testRowHeader(t)
	topic, _ := rowTopicFor(t, 3)
	h.subscribe(topic)

	mine, err := blocks.NewPartialDataRow(root, header.SignedBlockHeader, 3, header.KzgCommitments, header.KzgCommitmentsInclusionProof)
	require.NoError(t, err)
	require.Equal(t, true, mine.ExtendFromVerifiedCell(9, rowCellBytes(9), make([]byte, 48)))

	require.NoError(t, h.broadcaster.publishRowOnLoop(topic, mine))

	entry := h.broadcaster.getRowEntry(topic, mine.GroupID())
	require.NotNil(t, entry)
	require.Equal(t, uint64(1), entry.row.Included.Count())

	// The caller keeps working on its own copy, as a scheduler would.
	require.Equal(t, true, mine.ExtendFromVerifiedCell(10, rowCellBytes(10), make([]byte, 48)))
	mine.Cells[9][0] ^= 0xff

	require.Equal(t, uint64(1), entry.row.Included.Count(), "the caller must not be able to extend loop-owned state")
	require.Equal(t, false, entry.row.Included.BitAt(10))
	require.Equal(t, rowCellBytes(9)[0], entry.row.Cells[9][0], "the caller must not be able to rewrite a stored cell")
}
