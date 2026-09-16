package rowintegrationtest

// The RowDAS row-topic harness: N nodes on a shared gossipsub network, each running the real
// partial-message broadcaster over the real wire format, with real KZG verification.
//
// The substrate is testing/gossipsim. What is specific to RowDAS lives here: the cell matrix,
// the per-node custody assignment, the callbacks the broadcaster needs, and the accounting of
// who reached the reconstruction threshold and when.

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/OffchainLabs/go-bitfield"
	"github.com/OffchainLabs/prysm/v7/beacon-chain/blockchain/kzg"
	"github.com/OffchainLabs/prysm/v7/beacon-chain/core/peerdas"
	"github.com/OffchainLabs/prysm/v7/beacon-chain/p2p/partialdatacolumnbroadcaster"
	"github.com/OffchainLabs/prysm/v7/beacon-chain/verification"
	fieldparams "github.com/OffchainLabs/prysm/v7/config/fieldparams"
	"github.com/OffchainLabs/prysm/v7/config/params"
	"github.com/OffchainLabs/prysm/v7/consensus-types/blocks"
	"github.com/OffchainLabs/prysm/v7/consensus-types/primitives"
	ethpb "github.com/OffchainLabs/prysm/v7/proto/prysm/v1alpha1"
	"github.com/OffchainLabs/prysm/v7/testing/gossipsim"
	"github.com/OffchainLabs/prysm/v7/testing/require"
	"github.com/OffchainLabs/prysm/v7/testing/util"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/marcopolo/simnet"
	"github.com/sirupsen/logrus"
)

const (
	// harnessSlot is the slot every experiment publishes at. Fixed, because the
	// blob-to-subnet mapping depends on it and a figure has to be reproducible.
	harnessSlot = primitives.Slot(64)
)

// harnessDigest is a real fork digest, not a placeholder. The row topics are not registered in
// beacon-chain/p2p yet, so the harness builds the topic strings itself -- but the broadcaster
// resolves the digest to a fork to decide whether rows are supported there, so a made-up digest
// makes every incoming row RPC fail to classify. A fake digest is exactly the kind of shortcut
// that lets a test pass for the wrong reason.
func harnessDigest(t *testing.T) string {
	t.Helper()

	return fmt.Sprintf("%x", params.ForkDigest(0))
}

// gossipsimMeshFormation is how long the shared harness waits for gossipsub to graft. Named here
// so the cost breakdown can compare against it.
const gossipsimMeshFormation = gossipsim.MeshFormation

// rowTopic is the gossip topic for a row subnet.
func rowTopic(t *testing.T, subnet uint64) string {
	t.Helper()

	return fmt.Sprintf("/eth2/%s/%s%d/ssz_snappy", harnessDigest(t), partialdatacolumnbroadcaster.DataRowPrefix, subnet)
}

// columnTopic is the gossip topic for a column subnet. Built from the same digest as rowTopic, so
// it matches what the broadcaster's own columnTopicFromRowTopic derives when cross-forwarding.
func columnTopic(t *testing.T, columnIndex uint64) string {
	t.Helper()

	return fmt.Sprintf("/eth2/%s/data_column_sidecar_%d/ssz_snappy", harnessDigest(t), columnIndex)
}

// matrix is one block's cells and proofs, indexed [blob][column]. cellsPerBlob from the KZG
// helper is already row-major, so a row is one entry of it -- no transpose needed.
type matrix struct {
	blobCount   int
	cells       [][]kzg.Cell
	proofs      [][]kzg.Proof
	commitments [][]byte
	header      *ethpb.SignedBeaconBlockHeader
	// inclusionProof is a placeholder of the right shape. The harness's own header validation
	// accepts it: verifying a proof the harness built would only be checking the harness.
	inclusionProof [][]byte
	root           [fieldparams.RootLength]byte
}

// kzgCommitmentsInclusionProofDepth mirrors kzg_commitments_inclusion_proof_depth.size in
// proto/ssz_proto_library.bzl. The container cannot encode a proof of any other length.
const kzgCommitmentsInclusionProofDepth = 4

// newMatrix builds a real cell matrix with real proofs. Real KZG rather than random bytes:
// the whole point of the row axis is that cells verify against one commitment at varying cell
// indices, and random bytes would exercise none of it.
func newMatrix(t *testing.T, blobCount int) *matrix {
	t.Helper()
	require.NoError(t, kzg.Start())

	_, roBlobSidecars := util.GenerateTestElectraBlockWithSidecar(t, [32]byte{}, harnessSlot, blobCount)
	blobs := make([]kzg.Blob, blobCount)
	for i := range blobCount {
		copy(blobs[i][:], roBlobSidecars[i].Blob)
	}
	cells, proofs := util.GenerateCellsAndProofs(t, blobs)

	commitments := make([][]byte, blobCount)
	for i := range blobCount {
		commitment, err := kzg.BlobToKZGCommitment(&blobs[i])
		require.NoError(t, err)
		commitments[i] = commitment[:]
	}

	header := &ethpb.SignedBeaconBlockHeader{
		Header: &ethpb.BeaconBlockHeader{
			Slot:       harnessSlot,
			ParentRoot: make([]byte, fieldparams.RootLength),
			StateRoot:  make([]byte, fieldparams.RootLength),
			BodyRoot:   make([]byte, fieldparams.RootLength),
		},
		Signature: make([]byte, 96),
	}
	root, err := header.Header.HashTreeRoot()
	require.NoError(t, err)

	inclusionProof := make([][]byte, kzgCommitmentsInclusionProofDepth)
	for i := range inclusionProof {
		inclusionProof[i] = make([]byte, fieldparams.RootLength)
	}

	return &matrix{
		blobCount:      blobCount,
		cells:          cells,
		proofs:         proofs,
		commitments:    commitments,
		header:         header,
		inclusionProof: inclusionProof,
		root:           root,
	}
}

// row builds a PartialDataRow for one blob holding only the given columns, which is what a node
// custodying those columns would have.
func (m *matrix) row(t *testing.T, rowIndex uint64, columns []uint64) blocks.PartialDataRow {
	t.Helper()

	out, err := blocks.NewPartialDataRow(m.root, m.header, rowIndex, m.commitments, m.inclusionProof)
	require.NoError(t, err)
	for _, column := range columns {
		cell := m.cells[rowIndex][column]
		proof := m.proofs[rowIndex][column]
		require.Equal(t, true, out.ExtendFromVerifiedCell(column, cell[:], proof[:]))
	}

	return out
}

// column builds a PartialDataColumn for one column holding only the given rows' cells, which is
// what a proposer publishing an incomplete column would have. The transpose of row: here the cell
// index is the blob.
func (m *matrix) column(t *testing.T, columnIndex uint64, rows []uint64) blocks.PartialDataColumn {
	t.Helper()

	out, err := blocks.NewPartialDataColumn(m.root, m.header, columnIndex, m.commitments, m.inclusionProof)
	require.NoError(t, err)
	for _, rowIndex := range rows {
		cell := m.cells[rowIndex][columnIndex]
		proof := m.proofs[rowIndex][columnIndex]
		require.Equal(t, true, out.ExtendFromVerifiedCell(rowIndex, cell[:], proof[:]))
	}

	return out
}

// rowGroupID is the partial-message group id for this block, which rows share with the columns.
func (m *matrix) rowGroupID(t *testing.T, rowIndex uint64) []byte {
	t.Helper()

	row, err := blocks.NewPartialDataRow(m.root, m.header, rowIndex, m.commitments, m.inclusionProof)
	require.NoError(t, err)

	return row.GroupID()
}

// rowNode is one node in a row-topic experiment.
type rowNode struct {
	index       int
	columns     []uint64
	broadcaster *partialdatacolumnbroadcaster.PartialColumnBroadcaster
	callbacks   *harnessCallbacks

	cancel context.CancelFunc
}

// harnessCallbacks implements both callback interfaces the broadcaster needs. A row-only
// experiment reaches the column path only through the shared header hook; an experiment with the
// column axis (R9) exercises all of it, with real KZG on both axes.
type harnessCallbacks struct {
	t     *testing.T
	index int
	// columns is this node's custody, which the request policy needs: a row participant asks
	// only for cells it can use, and its own columns' cells are most of that.
	columns []uint64

	mu          sync.Mutex
	started     []recoverableEvent
	recoverable []recoverableEvent
	complete    int
	completeAt  time.Time
	// servedElsewhere counts whole-row availability claims observed while wanting nothing
	// further -- the cancellation signal, and what R1(b) prices.
	servedElsewhere   int
	servedElsewhereAt time.Time
	headerCalls       int
	cellCalls         int
	cellsSeen         int
	// Column-axis accounting, empty in a row-only experiment.
	columnsComplete []columnEvent
	columnCellCalls int
	columnCellsSeen int
}

// columnEvent records that a column became complete at this node, and when. The whole of R9 is a
// comparison of these across arms.
type columnEvent struct {
	at          time.Time
	columnIndex uint64
	topic       string
}

// recoverableEvent records that a node reached the reconstruction threshold, and when.
type recoverableEvent struct {
	at       time.Time
	rowIndex uint64
	groupID  []byte
	topic    string
}

func (c *harnessCallbacks) ValidateRowHeader(*ethpb.PartialDataColumnHeader) (pubsub.ValidationResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.headerCalls++

	// Header validation against chain state is the node's job, not the harness's: this
	// experiment is about cell exchange, and a header the harness itself constructed has
	// nothing to check that would not be checking the harness.
	return pubsub.ValidationAccept, nil
}

func (c *harnessCallbacks) ValidateRowCells(cells []blocks.CellProofBundle) error {
	c.mu.Lock()
	c.cellCalls++
	c.cellsSeen += len(cells)
	c.mu.Unlock()

	// Real verification, on the row axis: one commitment, many cell indices.
	return peerdas.VerifyCellsKZGProofs(cells)
}

// RowStarted records that a row was identified, which is where the pull arm would decide. The
// harness counts them so an arm that pulls can be told apart from one that does not.
func (c *harnessCallbacks) RowStarted(topic string, groupID []byte, rowIndex uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.started = append(c.started, recoverableEvent{
		at:       time.Now(),
		rowIndex: rowIndex,
		groupID:  groupID,
		topic:    topic,
	})
}

func (c *harnessCallbacks) RowRecoverable(topic string, groupID []byte, rowIndex uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.recoverable = append(c.recoverable, recoverableEvent{
		at:       time.Now(),
		rowIndex: rowIndex,
		groupID:  groupID,
		topic:    topic,
	})
}

func (c *harnessCallbacks) RowComplete(string, []byte, uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.complete++
	// The timestamp is what prices the cancellation trigger: a phase timer can only be
	// cancelled by something that has already arrived.
	if c.completeAt.IsZero() {
		c.completeAt = time.Now()
	}
}

// RowServedElsewhere counts the cancellation signal. The harness records it rather than acting on
// it: what a node does with the observation is the node's policy, and R1(b) is what prices it.
func (c *harnessCallbacks) RowServedElsewhere(string, []byte, uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.servedElsewhere++
	if c.servedElsewhereAt.IsZero() {
		c.servedElsewhereAt = time.Now()
	}
}

func (c *harnessCallbacks) RowGroupEvicted([]byte) {}

// RowRequestInterest mirrors the production policy in `beacon-chain/sync/rows_request_policy.go`:
// the cells of this node's own columns, and whether it still needs foreign cells to reach the
// reconstruction threshold.
//
// The harness has to mirror it rather than return nil, or the harness stops predicting the node.
// `ROWDAS_SCOPED_REQUESTS=0` returns to asking for everything missing, which is what every
// measurement before D11 did -- the point of keeping the switch is that the two can be compared
// at one seed in one run rather than across commits.
func (c *harnessCallbacks) RowRequestInterest(uint64) (bitfield.Bitlist, bool) {
	if !scopedRowRequests() {
		return nil, false
	}

	keep := bitfield.NewBitlist(uint64(fieldparams.NumberOfColumns))
	for _, column := range c.columns {
		keep.SetBitAt(column, true)
	}

	return keep, uint64(len(c.columns)) < blocks.ReconstructionThreshold()
}

// scopedRowRequests reports whether the row request policy is on. Default on, matching the node.
func scopedRowRequests() bool {
	if value, ok := os.LookupEnv("ROWDAS_SCOPED_REQUESTS"); ok {
		return value != "0"
	}

	return true
}

// servedElsewhereObservedAt is when this node first observed a peer holding the whole row while
// wanting nothing further -- the cancellation signal, zero if it never arrived.
func (c *harnessCallbacks) servedElsewhereObservedAt() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.servedElsewhereAt
}

// completedAt is when this node's row first became complete, zero if it never did.
func (c *harnessCallbacks) completedAt() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.completeAt
}

// Column-side callbacks. A row-only experiment reaches only HandleHeader; R9 reaches all of it.
//
// Header validation is accepted for the same reason the row side accepts it: the header was built
// by the harness, so checking it would be checking the harness. Cell verification is real, on
// both axes -- that is the part a fake would hollow out.
func (c *harnessCallbacks) PartialVerifierFromHeader(col *blocks.PartialDataColumn) (*verification.PartialColumnVerifier, pubsub.ValidationResult, error) {
	c.mu.Lock()
	c.headerCalls++
	c.mu.Unlock()

	return harnessColumnVerifier(col), pubsub.ValidationAccept, nil
}

func (c *harnessCallbacks) PartialVerifierFromTrustedColumn(col *blocks.PartialDataColumn) (*verification.PartialColumnVerifier, error) {
	return harnessColumnVerifier(col), nil
}

func (c *harnessCallbacks) ValidateColumn(cells []blocks.CellProofBundle) error {
	c.mu.Lock()
	c.columnCellCalls++
	c.columnCellsSeen += len(cells)
	c.mu.Unlock()

	return peerdas.VerifyCellsKZGProofs(cells)
}

func (c *harnessCallbacks) HandleColumn(topic string, col blocks.VerifiedRODataColumn) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.columnsComplete = append(c.columnsComplete, columnEvent{
		at:          time.Now(),
		columnIndex: col.Index(),
		topic:       topic,
	})
}

// harnessColumnVerifier wraps a partial column in a verifier whose header checks are already
// satisfied, which is what the broadcaster's own tests do. The KZG checks are not mocked -- those
// run in ValidateColumn above.
func harnessColumnVerifier(col *blocks.PartialDataColumn) *verification.PartialColumnVerifier {
	mock := &verification.MockDataColumnsVerifier{}
	mock.AppendRODataColumns(col.RODataColumn)

	return verification.NewPartialColumnVerifier(mock, col)
}

func (c *harnessCallbacks) HandleHeader(*ethpb.PartialDataColumnHeader, string) {}

func (c *harnessCallbacks) ValidateGloasGroupID(primitives.Slot, [32]byte) pubsub.ValidationResult {
	return pubsub.ValidationAccept
}

func (c *harnessCallbacks) recoverableSnapshot() []recoverableEvent {
	c.mu.Lock()
	defer c.mu.Unlock()

	return append([]recoverableEvent(nil), c.recoverable...)
}

func (c *harnessCallbacks) columnsCompleteSnapshot() []columnEvent {
	c.mu.Lock()
	defer c.mu.Unlock()

	return append([]columnEvent(nil), c.columnsComplete...)
}

func (c *harnessCallbacks) columnCounts() (cellCalls, cellsSeen int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.columnCellCalls, c.columnCellsSeen
}

func (c *harnessCallbacks) counts() (headerCalls, cellCalls, cellsSeen, complete int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.headerCalls, c.cellCalls, c.cellsSeen, c.complete
}

// rowNetwork is a running row-topic experiment.
type rowNetwork struct {
	sim     *gossipsim.Network
	nodes   []*rowNode
	tracers []*gossipsim.RecordingTracer
	topic   string
	// meshSizes is the per-node row-topic mesh that actually formed, gsp the parameters it
	// formed under, and graphDegree the connectivity it was drawn from.
	meshSizes   []int
	gsp         pubsub.GossipSubParams
	graphDegree int
	// columnCount is zero for a row-only experiment. handles is every joined topic per node,
	// which the cross-forwarding hooks need: pubsub.Join errors on a second handle, so a hook
	// that joined blindly would fight the pre-joins.
	columnCount uint64
	handles     []map[string]*pubsub.Topic
}

// columnSubscribers is the set of nodes subscribed to a column's subnet, which is what
// AwaitTopicMesh needs and what a completion count is read against.
func (nw *rowNetwork) columnSubscribers(columnIndex uint64) func(i int) bool {
	return func(i int) bool {
		for _, held := range nw.nodes[i].columns {
			if held == columnIndex {
				return true
			}
		}

		return false
	}
}

// meanMesh is the realized mean mesh size, which is the x-axis any per-mesh-peer claim needs.
func (nw *rowNetwork) meanMesh() float64 {
	if len(nw.meshSizes) == 0 {
		return 0
	}
	total := 0
	for _, size := range nw.meshSizes {
		total += size
	}

	return float64(total) / float64(len(nw.meshSizes))
}

// newRowNetwork brings up n nodes with the real broadcaster, all subscribed to one row topic.
//
// columnsFor assigns each node the columns it custodies. Assignment is a parameter rather than
// derived from a node ID because a correctness test wants a known cover; R4(a) measures what
// the real derivation gives.
func newRowNetwork(t *testing.T, n int, subnet uint64, columnsFor func(i int) []uint64) (*rowNetwork, func()) {
	t.Helper()

	return newRowNetworkWithDegree(t, n, 0, subnet, columnsFor)
}

// newRowNetworkWithDegree is newRowNetwork at an explicit mesh degree, which R3 sweeps. A
// degree of zero leaves production's D.
//
// The degree reaches gossipsub through Overrides.MeshDegree, not only the connectivity graph.
// An earlier version passed it to RandomRegular alone, so every arm actually ran at production
// D=8 and the sweep was over graph degree -- which caps the mesh from above but does not set
// it. The graph is built well above the mesh target for the same reason the segment study does
// it: at graph degree == mesh degree every peer is a mesh peer, so there are no non-mesh peers
// and the gossip path is not exercised at all.
func newRowNetworkWithDegree(t *testing.T, n, meshDegree int, subnet uint64, columnsFor func(i int) []uint64) (*rowNetwork, func()) {
	t.Helper()

	return newNetwork(t, networkOpts{n: n, meshDegree: meshDegree, rowSubnet: subnet, columnsFor: columnsFor})
}

// networkOpts is the full parameter set of the harness. It exists because R9 needs the column
// axis alongside the row axis and the two builders would otherwise be the same body twice.
type networkOpts struct {
	n          int
	meshDegree int
	rowSubnet  uint64
	// columnsFor assigns each node the columns it custodies.
	columnsFor func(i int) []uint64
	// rowMembers, when non-nil, restricts the row subnet to a subset of the network. Nil puts
	// every node on it.
	//
	// R9 needs the subset, and the reason is the whole point of the experiment: with every node
	// on the row subnet, every node reconstructs the row and fills its own columns, so
	// cross-forwarding has nothing left to do. The push matters precisely when the nodes that
	// need a cell are *not* on the row subnet that recovered it -- which is the mainnet case,
	// with 128 row subnets and one per node.
	rowMembers func(i int) bool
	// graphDegree overrides the connectivity degree. Zero derives it from the mesh degree.
	graphDegree int
	// graphSeed seeds the connectivity draw. Zero uses the default, so a single-run experiment
	// stays reproducible; a latency experiment sweeps it, since one graph is one sample.
	graphSeed uint64
	// subnetAware draws the connectivity graph so that every subnet's members are connected to
	// each other, then unions a global random graph over it -- what discovery approximates on a
	// real network, where a node peers with members of its own subnets.
	//
	// Off means a single global random regular graph, which is what every earlier experiment used
	// and which does *not* connect subnet members: the expected in-subnet degree is
	// globalDegree * (subnet fraction), independent of node count, so members end up with no
	// in-subnet neighbour and their topic mesh cannot form. Measured on the R2 custody shape, a
	// degree-10 global graph isolates 32 members at 24 nodes, 64 at 64 nodes, and 704 at 128 nodes
	// with realistic 8-column custody. See TestRandomRegularLeavesMembersIsolated.
	subnetAware bool
	// inSubnetDegree is how many in-subnet neighbours each member gets when subnetAware is set.
	// Zero means four.
	inSubnetDegree int
	// rowGraphMembers is the row-subnet membership used to *build* the connectivity graph, as
	// distinct from rowMembers, which decides who actually subscribes in this arm.
	//
	// They must be separable or the paired design breaks. `rowMembers` folds in "is the row axis
	// enabled in this arm", so a base arm reports no row members at all; if the graph is built from
	// that, the base and rows arms get *different physical graphs* and every paired comparison is
	// confounded. It is latent while every node is a member -- a subnet containing the whole network
	// needs no cover edges, and both arms measured 2048 edges at 128 nodes -- and it bites the moment
	// membership goes sparse. Found by an adversarial review of notes/rowdas/plan-repair.md before it
	// could corrupt a measurement.
	//
	// Nil falls back to rowMembers, which is right for every experiment where membership does not
	// vary by arm.
	rowGraphMembers func(i int) bool
	// publishers names the nodes that publish to subnets they may not be members of -- in
	// practice the proposer. Each is guaranteed a peer in every subnet, because gossipsub's
	// fanout can only deliver to a peer that is subscribed, and an empty fanout set drops the
	// whole topic silently. See gossipsim.EnsureSubnetReach for what that cost before it was
	// guaranteed.
	publishers []int
	// columnPathDead, when non-nil, names (node, column) pairs whose column subnet is dead *for
	// that node*: it custodies the column and creates state for it, exactly as a node does on
	// block arrival, but never subscribes to the topic -- so no cell of that column can reach it
	// by the column path while the rest of the network has it.
	//
	// This is the loss model R12 needs. R9 and R11 model loss at the source, where a column's
	// cells are never published at all; here they are published and served, and only this node's
	// route to them is broken. The two are different questions and only the second one asks
	// whether the row axis can substitute for the column path.
	columnPathDead func(node int, column uint64) bool
	// columnMeshFloor is the minimum per-topic mesh size each column subnet's subscribers must
	// reach before the experiment starts. Zero means one.
	//
	// It is not `subscribers - 1`, and that was a wrong turn worth recording. A subnet member can
	// only graft the subnet peers it is *connected* to, so on a degree-d graph over n nodes a
	// k-subscriber subnet settles at roughly (k-1)*d/(n-1) mesh peers -- 6 subscribers at degree
	// 10 over 24 nodes gives 2 to 4, measured. Demanding k-1 asserts a property of the
	// connectivity graph rather than of gossipsub, and the only way to satisfy it is a
	// near-complete graph, which is exactly what a latency experiment must not have. One mesh
	// peer per subscriber says the subnet can carry traffic at all, which is the real
	// precondition; that every column then completes is asserted by the experiment itself.
	columnMeshFloor int
	// columnCount, when non-zero, brings up the column axis: every node joins the column topics
	// 0..columnCount-1 and subscribes to the ones it custodies.
	//
	// Every node *joins* all of them, not only its own. Joining is local -- it announces
	// nothing and grafts nothing -- and it is what lets any node act as the proposer and what
	// the cross-forwarding hooks would do anyway. Subscription is what follows custody, and
	// subscription is what the claim under test is about.
	columnCount uint64
}

// newRowColumnNetwork brings up both DAS axes: one row subnet with every node on it, and
// columnCount column subnets with each node subscribed to the ones it custodies.
//
// This is the setup R9 needs, and the scale limit is worth stating plainly. The claim is about
// the columns a reconstructor does *not* custody, so the column set has to be large enough for
// "not custodied" to mean something, while the reconstruction threshold needs the union of the
// network's custody to reach 64 distinct columns. At sixteen nodes those two pull against each
// other: 64 columns and 8 per node gives a union of exactly 64 and only two subscribers per
// column subnet. Two is a working mesh but a degenerate one, so nothing here should be read as a
// statement about column-subnet mesh health -- only about whether a pushed cell arrives and
// completes a column that could not have completed otherwise.
func newRowColumnNetwork(t *testing.T, o networkOpts) (*rowNetwork, func()) {
	t.Helper()
	require.Equal(t, true, o.columnCount > 0, "newRowColumnNetwork needs a column count")

	return newNetwork(t, o)
}

func newNetwork(t *testing.T, o networkOpts) (*rowNetwork, func()) {
	t.Helper()
	n, meshDegree, subnet, columnsFor := o.n, o.meshDegree, o.rowSubnet, o.columnsFor

	overrides := gossipsim.NewOverrides()
	if meshDegree > 0 {
		overrides.MeshDegree = meshDegree
	}
	gsp := gossipsim.MeshParams(overrides.MeshDegree, overrides.IDontWantThreshold)
	// A little above D, so some peers sit outside the mesh, but well below Dhi. Drawing the
	// graph near-complete instead makes many nodes overshoot Dhi, and pruning them back costs a
	// full PruneBackoff (one minute) before AwaitMesh can settle -- 77 s per network against
	// 3 s. The cost of staying low is that few peers are outside the mesh, so the gossip and
	// announce paths are barely exercised here; that is a stated limitation of a 16-node
	// experiment, not something a degree choice can fix.
	graphDegree := min(gsp.D+2, n-1)
	if o.graphDegree > 0 {
		graphDegree = o.graphDegree
	}
	if graphDegree%2 == 1 && n%2 == 1 {
		graphDegree--
	}

	graphSeed := uint64(0x8371)
	if o.graphSeed != 0 {
		graphSeed = o.graphSeed
	}

	tracers := make([]*gossipsim.RecordingTracer, n)
	for i := range tracers {
		tracers[i] = gossipsim.NewRecordingTracer()
	}

	broadcasters := make([]*partialdatacolumnbroadcaster.PartialColumnBroadcaster, n)
	cancels := make([]context.CancelFunc, n)
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)
	for i := range n {
		ctx, cancel := context.WithCancel(context.Background())
		broadcasters[i] = partialdatacolumnbroadcaster.NewBroadcaster(ctx, logger)
		cancels[i] = cancel
	}

	// The link rate is a knob because a latency result has to be attributable. If raising it
	// tenfold does not move a penalty, that penalty is not bandwidth -- and the byte rates said as
	// much before this existed: the slowest arm measured ran at 1% of the link.
	rate := gossipsim.DefaultRate
	if mbps := envInt("ROWDAS_LINK_MBPS", 0); mbps > 0 {
		rate = mbps * 1_000_000
		t.Logf("  link rate: %d Mbps (default %d)", mbps, gossipsim.DefaultRate/1_000_000)
	}

	// One-way latency, knobbed for the same reason as the rate: it separates a penalty made of
	// sequential request/response rounds from one made of bandwidth or loss. If the row axis's cost
	// is round-dominated it scales with RTT; if it is loss-dominated it barely moves.
	latency := gossipsim.DefaultLatency
	if ms := envInt("ROWDAS_LATENCY_MS", 0); ms > 0 {
		latency = time.Duration(ms) * time.Millisecond
		t.Logf("  one-way latency: %v (default %v)", latency, gossipsim.DefaultLatency)
	}

	sim, stopSim := gossipsim.New(t, gossipsim.NetworkConfig{
		Links:     gossipsim.UniformLinks(n, rate),
		Latency:   simnet.StaticLatency(latency),
		Edges:     mustEdges(t, o, n, graphDegree, graphSeed),
		Overrides: overrides,
		WallClock: true,
		PerNodeOpts: func(i int) []pubsub.Option {
			return broadcasters[i].AppendPubSubOpts([]pubsub.Option{pubsub.WithRawTracer(tracers[i])})
		},
	})

	topic := rowTopic(t, subnet)
	nodes := make([]*rowNode, n)
	for i := range n {
		callbacks := &harnessCallbacks{t: t, index: i, columns: columnsFor(i)}
		go broadcasters[i].Start(callbacks, callbacks)
		nodes[i] = &rowNode{
			index:       i,
			columns:     columnsFor(i),
			broadcaster: broadcasters[i],
			callbacks:   callbacks,
			cancel:      cancels[i],
		}
	}

	// Join with partial messages requested, subscribe, and hand the topic to the broadcaster.
	// Subscribing matters even though row topics carry no full messages: without it the node
	// announces nothing and gossipsub never grafts, so no partial exchange happens either.
	handles := make([]map[string]*pubsub.Topic, n)
	subs := make([]*pubsub.Subscription, 0, n)
	for i, ps := range sim.Pubsubs {
		handles[i] = make(map[string]*pubsub.Topic)
		th, err := ps.Join(topic, pubsub.RequestPartialMessages())
		require.NoError(t, err)
		handles[i][topic] = th
		if o.rowMembers != nil && !o.rowMembers(i) {
			// Joined but not subscribed, so the node can still be reached and can still publish
			// -- it is simply not a member of this row subnet.
			continue
		}
		sub, err := th.Subscribe(pubsub.WithBufferSize(gossipsim.SubscriptionBuffer))
		require.NoError(t, err)
		subs = append(subs, sub)
		require.NoError(t, broadcasters[i].Subscribe(context.Background(), th))
	}

	// The column axis, when the experiment asks for it. Join every column topic on every node
	// (local only -- no announce, no graft) and subscribe to the custodied ones.
	if o.columnCount > 0 {
		for i, ps := range sim.Pubsubs {
			custodied := make(map[uint64]bool, len(nodes[i].columns))
			for _, held := range nodes[i].columns {
				custodied[held] = true
			}
			for columnIndex := range o.columnCount {
				colTopic := columnTopic(t, columnIndex)
				th, err := ps.Join(colTopic, pubsub.RequestPartialMessages())
				require.NoError(t, err)
				handles[i][colTopic] = th
				if !custodied[columnIndex] {
					continue
				}
				if o.columnPathDead != nil && o.columnPathDead(i, columnIndex) {
					// Joined but never subscribed: the node can still publish on the topic and
					// still create state for the column, and nothing can reach it there.
					continue
				}
				sub, err := th.Subscribe(pubsub.WithBufferSize(gossipsim.SubscriptionBuffer))
				require.NoError(t, err)
				subs = append(subs, sub)
				require.NoError(t, broadcasters[i].Subscribe(context.Background(), th))
			}
		}
	}

	// The cross-forwarding hooks. Join is a lookup rather than a join, because every topic the
	// experiment uses is already joined above and pubsub.Join errors on a second handle -- the
	// same reason the node wires these through p2p.Service rather than calling pubsub directly.
	for i := range n {
		nodeHandles := handles[i]
		ps := sim.Pubsubs[i]
		broadcasters[i].SetTopicPushHooks(partialdatacolumnbroadcaster.TopicPushHooks{
			Join: func(joinTopic string) error {
				if _, ok := nodeHandles[joinTopic]; ok {
					return nil
				}
				th, err := ps.Join(joinTopic, pubsub.RequestPartialMessages())
				if err != nil {
					return err
				}
				nodeHandles[joinTopic] = th

				return nil
			},
			// No Leave: the harness tears the whole network down, and leaving a topic mid-run
			// would drop the handle a later assertion reads.
			SetPartialInterest: func(interestTopic string, want bool) error {
				th, ok := nodeHandles[interestTopic]
				if !ok {
					return fmt.Errorf("no handle for %q", interestTopic)
				}

				return th.SetPartialInterest(context.Background(), want)
			},
		})
	}
	// Drain the subscriptions. A row topic should deliver no full message at all, but an
	// undrained subscription overflows and reads as loss rather than as the protocol working.
	drainCtx, stopDrain := context.WithCancel(context.Background())
	var drainWG sync.WaitGroup
	for _, sub := range subs {
		drainWG.Add(1)
		go func(s *pubsub.Subscription) {
			defer drainWG.Done()
			for {
				if _, err := s.Next(drainCtx); err != nil {
					return
				}
			}
		}(sub)
	}

	// Settle against the band gossipsub maintains, and keep the realized sizes: a claim about
	// mesh degree has to rest on the mesh that formed, not on the one that was requested.
	settleStart := time.Now()
	var meshSizes []int
	if o.rowMembers == nil {
		meshSizes = gossipsim.AwaitMesh(t, tracers, true, gsp)
		t.Logf("  mesh settled in %v: %s (graph degree %d, D %d, band %d..%d)",
			time.Since(settleStart).Round(time.Millisecond), gossipsim.SummariseMesh(meshSizes),
			graphDegree, gsp.D, gsp.Dlo, gsp.Dhi)
	} else {
		// A row subnet with fewer members than D settles at members-1, which gossipsub's own
		// band does not describe -- so the band comes from the membership.
		members := 0
		for i := range n {
			if o.rowMembers(i) {
				members++
			}
		}
		// The band is also capped by the *graph*: a mesh cannot exceed the number of in-subnet
		// neighbours the topology gave each member. With 8 subscribers and inSubnetDegree 4 the
		// meshes settle at 4-5, so a band floor of Dlo=6 can never be met and the arm times out
		// after three minutes -- which is what sparse membership hit the first time it was run.
		// Raising inSubnetDegree (Step 4 of plan-repair.md) is what makes a floor of 6 reachable;
		// this cap is what stops the harness demanding the unreachable in the meantime.
		lo := max(0, min(members-1, gsp.Dlo))
		// Only when membership is sparse. With every node a member, the global layer supplies
		// in-topic neighbours too and meshes reach 8-11, so capping by inSubnetDegree there would
		// weaken the check rather than make it satisfiable.
		if o.subnetAware && members < n {
			inSubnet := o.inSubnetDegree
			if inSubnet == 0 {
				inSubnet = 4
			}
			lo = max(0, min(lo, inSubnet))
		}
		meshSizes = gossipsim.AwaitTopicMesh(t, tracers, true, topic, o.rowMembers, lo, gsp.Dhi)
		// AwaitTopicMesh marks non-members -1; summarising those would report a minimum of -1.
		memberSizes := make([]int, 0, members)
		for _, size := range meshSizes {
			if size >= 0 {
				memberSizes = append(memberSizes, size)
			}
		}
		t.Logf("  row mesh settled in %v: %s over %d members (graph degree %d, band %d..%d)",
			time.Since(settleStart).Round(time.Millisecond), gossipsim.SummariseMesh(memberSizes),
			members, graphDegree, lo, gsp.Dhi)
	}

	stop := func() {
		stopDrain()
		for _, sub := range subs {
			sub.Cancel()
		}
		drainWG.Wait()
		for _, cancel := range cancels {
			cancel()
		}
		stopSim()
	}

	nw := &rowNetwork{sim: sim, nodes: nodes, tracers: tracers, topic: topic,
		meshSizes: meshSizes, gsp: gsp, graphDegree: graphDegree,
		columnCount: o.columnCount, handles: handles}

	// Settle each column subnet against its own band. A subnet of k subscribers settles at k-1
	// mesh peers, which gossipsub's Dlo..Dhi does not describe at all -- so the band here is
	// derived from the subscriber count rather than from D. Without this the experiment would
	// publish into column meshes that had not formed and report the result as the push failing.
	if o.columnCount > 0 {
		settleStart = time.Now()
		smallest := n
		for columnIndex := range o.columnCount {
			subscribed := nw.columnSubscribers(columnIndex)
			count := 0
			for i := range n {
				if subscribed(i) {
					count++
				}
			}
			if count == 0 {
				continue
			}
			smallest = min(smallest, count)
			floor := max(1, o.columnMeshFloor)
			want := min(count-1, floor)
			gossipsim.AwaitTopicMesh(t, tracers, true, columnTopic(t, columnIndex), subscribed, want, gsp.Dhi)
		}
		t.Logf("  %d column subnets settled in %v (smallest subnet %d subscribers)",
			o.columnCount, time.Since(settleStart).Round(time.Millisecond), smallest)
	}

	return nw, stop
}

// awaitVictimColumns waits for the columns of the nodes `isVictim` selects to stop completing, and
// returns how many completed. Same shape as awaitRecoverable and for the same reason: an arm where
// nothing completes is a legitimate result, so this stalls out rather than timing out, and it
// requires observed progress before accepting a stall.
func (nw *rowNetwork) awaitVictimColumns(t *testing.T, isVictim func(int) bool, timeout time.Duration) int {
	t.Helper()

	const poll = 100 * time.Millisecond
	const stallWindow = 3 * time.Second
	deadline := time.Now().Add(timeout)
	lastProgress := time.Now()
	previous := -1
	for time.Now().Before(deadline) {
		done := 0
		for _, node := range nw.nodes {
			if isVictim(node.index) {
				done += len(node.callbacks.columnsCompleteSnapshot())
			}
		}
		if done != previous {
			previous = done
			lastProgress = time.Now()
		} else if time.Since(lastProgress) > stallWindow {
			return done
		}
		time.Sleep(poll)
	}

	return previous
}

// awaitRecoverable waits until no further node reaches the reconstruction threshold and returns
// how many did. Not "until all of them": an arm where nobody can reconstruct is a legitimate
// result, and waiting for a count that will never arrive would report it as a timeout.
func (nw *rowNetwork) awaitRecoverable(t *testing.T, timeout time.Duration) int {
	t.Helper()

	// Return early on all-reached, or on a *stall* -- meaning no new node for stallWindow, not
	// merely one unchanged poll. One unchanged poll cannot tell "not started yet" from "done",
	// and this helper got that wrong: an exchange whose first threshold lands after 200 ms
	// returned zero and read as total failure. Same mistake waitUntilQuiet made in R1(b).
	const poll = 100 * time.Millisecond
	const stallWindow = 3 * time.Second
	deadline := time.Now().Add(timeout)
	lastProgress := time.Now()
	previous := 0
	for time.Now().Before(deadline) {
		reached := 0
		for _, node := range nw.nodes {
			if len(node.callbacks.recoverableSnapshot()) > 0 {
				reached++
			}
		}
		if reached == len(nw.nodes) {
			return reached
		}
		if reached != previous {
			previous = reached
			lastProgress = time.Now()
		} else if reached > 0 && time.Since(lastProgress) > stallWindow {
			// Progress made and then stopped: a legitimate partial result, which is what an arm
			// where not everybody can reconstruct looks like.
			return reached
		} else if reached == 0 && time.Since(lastProgress) > 2*stallWindow {
			// Nothing at all for twice the window. Give the caller the zero rather than hanging,
			// but say so, because a silent zero is what caused the misreading above.
			t.Logf("no node reached the reconstruction threshold within %v", 2*stallWindow)
			return 0
		}
		time.Sleep(poll)
	}
	t.Logf("recoverable count still moving after %v", timeout)

	return previous
}

// awaitRowThreshold polls one node's row until it can be recovered, and returns the snapshot it
// ends on -- which may still be short. Used after a pull, where the whole question is whether the
// cells asked for arrive.
func (nw *rowNetwork) awaitRowThreshold(t *testing.T, node *rowNode, groupID []byte, timeout time.Duration) *blocks.PartialDataRow {
	t.Helper()

	const poll = 100 * time.Millisecond
	deadline := time.Now().Add(timeout)
	var row *blocks.PartialDataRow
	for {
		snapshot, err := node.broadcaster.RowSnapshot(context.Background(), nw.topic, groupID)
		require.NoError(t, err)
		if snapshot != nil {
			row = snapshot
			if row.ReconstructionThresholdMet() {
				return row
			}
		}
		if time.Now().After(deadline) {
			return row
		}
		time.Sleep(poll)
	}
}

// mustEdges draws the connectivity graph, subnet-aware when the experiment asks for it.
//
// The subnet membership is derived from the same functions the subscriptions use, so the graph and
// the topics cannot disagree about who is in what -- which is the bug the old global-only draw
// hid rather than caused.
func mustEdges(t *testing.T, o networkOpts, n, degree int, seed uint64) []gossipsim.Edge {
	t.Helper()

	members := make([][]int, 0, int(o.columnCount)+1)
	// A row-only experiment leaves columnsFor nil, and the column loop below would panic on it.
	// The row subnet is still a subnet, so membership is built either way.
	columnCount := o.columnCount
	if o.columnsFor == nil {
		columnCount = 0
	}
	for columnIndex := range columnCount {
		var holders []int
		for i := range n {
			for _, held := range o.columnsFor(i) {
				if held == columnIndex {
					holders = append(holders, i)
					break
				}
			}
		}
		members = append(members, holders)
	}
	// The row subnet is a subnet too. Membership here is the *intended* membership, independent of
	// whether this arm enables the row axis, so that every arm is measured on the same graph.
	inRowSubnet := o.rowGraphMembers
	if inRowSubnet == nil {
		inRowSubnet = o.rowMembers
	}
	var rowMembers []int
	for i := range n {
		if inRowSubnet == nil || inRowSubnet(i) {
			rowMembers = append(rowMembers, i)
		}
	}
	members = append(members, rowMembers)

	var edges []gossipsim.Edge
	if o.subnetAware {
		inSubnet := o.inSubnetDegree
		if inSubnet == 0 {
			inSubnet = 4
		}
		var err error
		edges, err = gossipsim.SubnetAware(n, degree, inSubnet, members, seed)
		require.NoError(t, err)
		t.Logf("  subnet-aware graph: %d edges over %d nodes (global degree %d, in-subnet %d)",
			len(edges), n, degree, inSubnet)
	} else {
		edges = mustRandomRegular(t, n, degree, seed)
	}

	// Applied in *both* topologies, deliberately. A publisher with no peer in a subnet delivers
	// that topic to nobody, and the result reads as a protocol failure rather than as a graph that
	// could not carry the publish -- so it is not something to leave to the draw in an experiment
	// about something else. It is why ROWDAS_R2_SUBNET_AWARE=0 now reproduces the old topology's
	// *subnet layer* rather than the old runs exactly.
	if len(o.publishers) > 0 {
		var added int
		edges, added = gossipsim.EnsureSubnetReach(edges, members, o.publishers, seed)
		if added > 0 {
			t.Logf("  publisher reach: %d edges added so nodes %v have a peer in every subnet",
				added, o.publishers)
		}
	}

	return edges
}

func mustRandomRegular(t *testing.T, n, d int, seed uint64) []gossipsim.Edge {
	t.Helper()
	edges, err := gossipsim.RandomRegular(n, d, seed)
	require.NoError(t, err)

	return edges
}

// waitUntilQuiet returns once row traffic has stopped, so byte counters are read after the
// exchange has actually finished rather than after a guessed sleep. A fixed sleep is both slower
// than it needs to be and wrong in the other direction on a slow machine, where it would read
// the counters mid-flight and under-report duplication.
func (nw *rowNetwork) waitUntilQuiet(t *testing.T, timeout time.Duration) time.Duration {
	t.Helper()

	// Quiet is a *window* with no bytes, not one poll. The protocol now paces itself: an
	// availability announcement waits up to RequestAvailabilityMaxDelay for a packet to ride, a
	// cancellation up to RequestCancellationMaxDelay, and a re-ask of a silent peer backs off to
	// 1.2 s -- so a single empty 100 ms poll is an ordinary gap mid-exchange. The first version of
	// this helper returned on exactly that, and cut R5's arms off before any node reached the
	// threshold. The window has to exceed the longest legitimate pause.
	const (
		poll        = 100 * time.Millisecond
		quietWindow = 1500 * time.Millisecond
	)
	start := time.Now()
	deadline := start.Add(timeout)
	previous := -1
	lastChange := start
	for time.Now().Before(deadline) {
		total := 0
		for _, tr := range nw.tracers {
			recv, sent, _, _ := rowBytes(tr)
			total += recv + sent
		}
		if total != previous {
			previous = total
			lastChange = time.Now()
		} else if time.Since(lastChange) >= quietWindow {
			return time.Since(start)
		}
		time.Sleep(poll)
	}
	t.Logf("traffic still moving after %v; byte figures may be short", timeout)

	return time.Since(start)
}

// rowBytes sums the partial-message bytes a node sent and received. Row topics carry partial
// messages only, so this is the whole of the row traffic -- rpc.Publish is empty throughout.
func rowBytes(tr *gossipsim.RecordingTracer) (recv, sent, recvRPCs, sentRPCs int) {
	return tr.PartialCounts()
}
