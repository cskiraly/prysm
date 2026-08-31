package partialdatacolumnbroadcaster

import (
	"fmt"
	"slices"
	"testing"

	"github.com/OffchainLabs/prysm/v7/beacon-chain/verification"
	fieldparams "github.com/OffchainLabs/prysm/v7/config/fieldparams"
	"github.com/OffchainLabs/prysm/v7/consensus-types/blocks"
	"github.com/OffchainLabs/prysm/v7/testing/require"
)

// trustedColumnCallbacks is stubColumnCallbacks with the one method the push path needs: the
// pushed columns are built here from an already-validated row header, so they are trusted.
type trustedColumnCallbacks struct {
	stubColumnCallbacks
}

func (trustedColumnCallbacks) PartialVerifierFromTrustedColumn(col *blocks.PartialDataColumn) (*verification.PartialColumnVerifier, error) {
	return newMockPartialVerifier(col), nil
}

// pushHarness is a rowHarness with the cross-forward wiring recorded rather than executed.
type partialInterestCall struct {
	topic string
	want  bool
}

type pushHarness struct {
	*rowHarness
	joined    []string
	left      []string
	interest  []partialInterestCall
	published []*blocks.PartialDataColumn
	topics    []string
}

func newPushHarness(t *testing.T) *pushHarness {
	t.Helper()

	h := &pushHarness{rowHarness: newRowHarness(t)}
	h.broadcaster.callbacks = trustedColumnCallbacks{}
	h.broadcaster.SetTopicPushHooks(TopicPushHooks{
		Join: func(topic string) error {
			h.joined = append(h.joined, topic)
			return nil
		},
		Leave: func(topic string) error {
			h.left = append(h.left, topic)
			return nil
		},
		SetPartialInterest: func(topic string, want bool) error {
			h.interest = append(h.interest, partialInterestCall{topic: topic, want: want})
			return nil
		},
	})
	h.broadcaster.publishPartialCol = func(topic string, _ []byte, col *blocks.PartialDataColumn) error {
		h.topics = append(h.topics, topic)
		h.published = append(h.published, col)
		return nil
	}
	// The advertisement-only default registers instead of publishing. For these tests the
	// distinction is not under test -- what is pushed and where is -- so record registrations in
	// the same topics list the assertions already read. Registration stores the same column state
	// (the store bookkeeping is shared), it just spends no packet.
	h.broadcaster.registerPartialCol = func(topic string, _ []byte) error {
		h.topics = append(h.topics, topic)
		return nil
	}

	return h
}

// recoveredRow is a fully populated row, as RecoverRow leaves one.
func recoveredRow(t *testing.T, rowIndex uint64, columns ...uint64) *blocks.PartialDataRow {
	t.Helper()

	header, root := testRowHeader(t)
	row, err := blocks.NewPartialDataRow(root, header.SignedBlockHeader, rowIndex,
		header.KzgCommitments, header.KzgCommitmentsInclusionProof)
	require.NoError(t, err)
	if len(columns) == 0 {
		for columnIndex := range uint64(fieldparams.NumberOfColumns) {
			columns = append(columns, columnIndex)
		}
	}
	for _, columnIndex := range columns {
		require.Equal(t, true, row.ExtendFromVerifiedCell(columnIndex, rowCellBytes(byte(columnIndex)), make([]byte, 48)))
	}

	return &row
}

func TestColumnTopicFromRowTopic(t *testing.T) {
	t.Run("carries the fork digest and the encoding across", func(t *testing.T) {
		topic, err := columnTopicFromRowTopic("/eth2/abcd1234/data_row_17/ssz_snappy", 42)
		require.NoError(t, err)
		require.Equal(t, "/eth2/abcd1234/data_column_sidecar_42/ssz_snappy", topic)
	})

	t.Run("the result is a column topic classifyTopic accepts", func(t *testing.T) {
		// The point of deriving rather than formatting: whatever the row topic's digest is, the
		// column topic is the one this node's own column path would use.
		topic, err := columnTopicFromRowTopic("/eth2/deadbeef/data_row_3/ssz_snappy", 7)
		require.NoError(t, err)
		kind, index, err := classifyTopic(topic)
		require.NoError(t, err)
		require.Equal(t, topicKindColumn, kind)
		require.Equal(t, uint64(7), index)
	})

	t.Run("rejects a non-row topic", func(t *testing.T) {
		_, err := columnTopicFromRowTopic("/eth2/abcd1234/data_column_sidecar_1/ssz_snappy", 2)
		require.NotNil(t, err)
	})

	t.Run("rejects a column index out of range", func(t *testing.T) {
		_, err := columnTopicFromRowTopic("/eth2/abcd1234/data_row_1/ssz_snappy", fieldparams.NumberOfColumns)
		require.NotNil(t, err)
	})

	t.Run("rejects a row topic with no encoding suffix", func(t *testing.T) {
		_, err := columnTopicFromRowTopic("/eth2/abcd1234/data_row_1", 2)
		require.NotNil(t, err)
	})
}

// TestCrossForwardRowSkipsColumnsWeHold is the division of labour the push arm rests on: a column
// we custody is already filled by the cross-fill bridge and republished on its own subnet, so
// pushing it would duplicate that. Only the columns nobody here holds are pushed.
func TestCrossForwardRowSkipsColumnsWeHold(t *testing.T) {
	h := newPushHarness(t)
	rowTopic, _ := rowTopicFor(t, 2)
	seedColumn(t, h.rowHarness, 7, true)

	require.NoError(t, h.broadcaster.crossForwardRowOnLoop(crossForwardRow{
		rowTopic: rowTopic,
		row:      recoveredRow(t, 2),
		columns:  []uint64{5, 7, 9},
		pushed:   new(int),
	}))

	require.Equal(t, 2, len(h.topics), "column 7 is ours and should not be pushed")
	for _, columnIndex := range []uint64{5, 9} {
		want := fmt.Sprintf("/eth2/abcd1234/data_column_sidecar_%d/ssz_snappy", columnIndex)
		require.Equal(t, true, slices.Contains(h.topics, want), fmt.Sprintf("expected a push on %q", want))
		require.Equal(t, true, slices.Contains(h.joined, want), fmt.Sprintf("expected a join of %q", want))
	}
}

// TestCrossForwardRegistersWithoutPublishing pins the advertisement-only default (design.md
// section 6 item 16): a cross-forward stores servable column state -- with the pushed cell in it
// and an explicitly empty request bitmap -- registers it with the extension, and publishes
// nothing.
func TestCrossForwardRegistersWithoutPublishing(t *testing.T) {
	h := newPushHarness(t)
	rowTopic, _ := rowTopicFor(t, 1)

	require.NoError(t, h.broadcaster.crossForwardRowOnLoop(crossForwardRow{
		rowTopic: rowTopic,
		row:      recoveredRow(t, 1),
		columns:  []uint64{11},
		pushed:   new(int),
	}))

	require.Equal(t, 0, len(h.published), "the ads default must not publish")
	require.Equal(t, 1, len(h.topics), "the state must still be registered")

	verifier := h.broadcaster.getPartialVerifier(h.topics[0], recoveredRow(t, 1).GroupID())
	require.NotNil(t, verifier, "registered state must be stored and servable")
	requests, ok := verifier.Column.PartsRequests()
	require.Equal(t, true, ok, "registered state must carry an explicit request bitmap")
	require.Equal(t, uint64(0), requests.Count(), "registered state must request nothing")
	require.Equal(t, uint64(1), verifier.Column.Included.Count())
	require.Equal(t, true, verifier.Column.Included.BitAt(1))
}

// TestCrossForwardedColumnRequestsNothing guards the boundary between the two arms. A partial
// column left to its default asks for every cell it lacks; on a topic we do not custody that is
// the pull direction, which needs a framework change and is a separate arm. The push must not
// smuggle it in.
func TestCrossForwardedColumnRequestsNothing(t *testing.T) {
	h := newPushHarness(t)
	rowTopic, _ := rowTopicFor(t, 1)

	// The wire shape is the eager arm's to show; the ads default registers without building an
	// action. The registered state's shape is pinned by TestCrossForwardRegistersWithoutPublishing.
	prev := CrossForwardEagerPush
	CrossForwardEagerPush = true
	defer func() { CrossForwardEagerPush = prev }()

	require.NoError(t, h.broadcaster.crossForwardRowOnLoop(crossForwardRow{
		rowTopic: rowTopic,
		row:      recoveredRow(t, 1),
		columns:  []uint64{11},
		pushed:   new(int),
	}))

	require.Equal(t, 1, len(h.published))
	column := h.published[0]
	requests, ok := column.PartsRequests()
	require.Equal(t, true, ok, "a pushed column must carry an explicit request bitmap")
	require.Equal(t, uint64(0), requests.Count(), "a pushed column must request nothing")

	// And it does carry the one cell it is pushing: row 1's cell lands at cell index 1.
	require.Equal(t, uint64(1), column.Included.Count())
	require.Equal(t, true, column.Included.BitAt(1))
	require.DeepEqual(t, rowCellBytes(11), column.Column()[1])
}

// TestCrossForwardRowSkipsCellsItDoesNotHave covers the partial case: a row recovered from a
// group that was evicted mid-flight, or a row still filling. A column we have no cell for has
// nothing to push.
func TestCrossForwardRowSkipsCellsItDoesNotHave(t *testing.T) {
	h := newPushHarness(t)
	rowTopic, _ := rowTopicFor(t, 0)

	require.NoError(t, h.broadcaster.crossForwardRowOnLoop(crossForwardRow{
		rowTopic: rowTopic,
		row:      recoveredRow(t, 0, 4, 5),
		columns:  []uint64{4, 5, 6},
		pushed:   new(int),
	}))

	require.Equal(t, 2, len(h.topics), "column 6 has no cell in this row")
	require.Equal(t, 2, len(h.joined), "only the topics pushed into are joined")
}

// TestCrossForwardRowReportsHowManyItPushed is what the caller logs and what R9 counts.
func TestCrossForwardRowReportsHowManyItPushed(t *testing.T) {
	h := newPushHarness(t)
	rowTopic, _ := rowTopicFor(t, 3)

	var pushed int
	require.NoError(t, h.broadcaster.crossForwardRowOnLoop(crossForwardRow{
		rowTopic: rowTopic,
		row:      recoveredRow(t, 3),
		columns:  []uint64{1, 2, 3, 4},
		pushed:   &pushed,
	}))

	require.Equal(t, 4, pushed)
}

// TestCrossForwardRowNeedsAJoinFunction fails loudly rather than silently pushing into topics the
// host never joined -- where iSupportSendingPartial is false and every publish is a no-op.
func TestCrossForwardRowNeedsAJoinFunction(t *testing.T) {
	h := newRowHarness(t)
	rowTopic, _ := rowTopicFor(t, 2)

	err := h.broadcaster.crossForwardRowOnLoop(crossForwardRow{
		rowTopic: rowTopic,
		row:      recoveredRow(t, 2),
		columns:  []uint64{1},
		pushed:   new(int),
	})
	require.ErrorIs(t, err, errCrossForwardNotWired)
}

// TestPullRowRequestsOnlyItsOwnCell is the honesty condition of the pull arm. We are not a
// subscriber of these topics and have no business collecting a column we do not custody, so the
// request names exactly one cell: the one belonging to our row.
func TestPullRowRequestsOnlyItsOwnCell(t *testing.T) {
	h := newPushHarness(t)
	rowTopic, _ := rowTopicFor(t, 2)

	require.NoError(t, h.broadcaster.pullRowOnLoop(pullRow{
		rowTopic: rowTopic,
		row:      recoveredRow(t, 2, 0), // holds only column 0
		columns:  []uint64{9},
		pulled:   new(int),
	}))

	require.Equal(t, 1, len(h.published))
	column := h.published[0]
	requests, ok := column.PartsRequests()
	require.Equal(t, true, ok)
	require.Equal(t, uint64(1), requests.Count(), "exactly one cell requested")
	require.Equal(t, true, requests.BitAt(2), "the cell of our row, at the row's blob index")
	require.Equal(t, uint64(0), column.Included.Count(), "a pull carries no cells")
}

// TestPullRowDeclaresPartialInterest covers the framework capability the arm rests on. Without the
// announcement the peers we ask cannot see us as a valid recipient of partial messages on the
// topic, so their reply would go nowhere and the request would be pure cost.
func TestPullRowDeclaresPartialInterest(t *testing.T) {
	h := newPushHarness(t)
	rowTopic, _ := rowTopicFor(t, 1)

	require.NoError(t, h.broadcaster.pullRowOnLoop(pullRow{
		rowTopic: rowTopic,
		row:      recoveredRow(t, 1, 0),
		columns:  []uint64{4, 5},
		pulled:   new(int),
	}))

	require.Equal(t, 2, len(h.interest))
	for _, call := range h.interest {
		require.Equal(t, true, call.want, "interest is declared, not withdrawn")
	}
}

// TestPullRowSkipsWhatItDoesNotNeed: a cell we already hold, and a column whose own subnet we are
// on, are both cheaper to leave alone.
func TestPullRowSkipsWhatItDoesNotNeed(t *testing.T) {
	h := newPushHarness(t)
	rowTopic, _ := rowTopicFor(t, 0)
	seedColumn(t, h.rowHarness, 7, true)

	require.NoError(t, h.broadcaster.pullRowOnLoop(pullRow{
		rowTopic: rowTopic,
		row:      recoveredRow(t, 0, 4), // already holds column 4's cell
		columns:  []uint64{4, 7, 9},
		pulled:   new(int),
	}))

	require.Equal(t, 1, len(h.topics), "only column 9 is both missing and not ours")
	require.Equal(t, "/eth2/abcd1234/data_column_sidecar_9/ssz_snappy", h.topics[0])
}

// TestPullRowNeedsThePartialInterestHook fails loudly on a host whose pubsub cannot announce
// partial interest, rather than sending requests that can never be answered.
func TestPullRowNeedsThePartialInterestHook(t *testing.T) {
	h := newRowHarness(t)
	h.broadcaster.SetTopicPushHooks(TopicPushHooks{Join: func(string) error { return nil }})
	rowTopic, _ := rowTopicFor(t, 2)

	err := h.broadcaster.pullRowOnLoop(pullRow{
		rowTopic: rowTopic,
		row:      recoveredRow(t, 2, 0),
		columns:  []uint64{1},
		pulled:   new(int),
	})
	require.ErrorIs(t, err, errPullNotWired)
}

// TestCrossForwardTopicIsLeftWhenItsLastGroupIsEvicted is the lifecycle. These topics are joined,
// never subscribed, so the subscription path's own cleanup -- which walks subscribed topics --
// would never release them, and they would outlive their fork digest.
func TestCrossForwardTopicIsLeftWhenItsLastGroupIsEvicted(t *testing.T) {
	h := newPushHarness(t)
	rowTopic, _ := rowTopicFor(t, 3)

	require.NoError(t, h.broadcaster.crossForwardRowOnLoop(crossForwardRow{
		rowTopic: rowTopic,
		row:      recoveredRow(t, 3),
		columns:  []uint64{6},
		pushed:   new(int),
	}))
	pushed := "/eth2/abcd1234/data_column_sidecar_6/ssz_snappy"
	require.Equal(t, true, h.broadcaster.pushTopics[pushed])
	require.Equal(t, 0, len(h.left))

	// Age the group out. Eviction decrements once per call.
	for range TTLInSlots + 1 {
		h.broadcaster.evictExpiredGroups()
	}

	require.Equal(t, false, h.broadcaster.pushTopics[pushed], "the topic should no longer be tracked")
	require.DeepEqual(t, []string{pushed}, h.left)
	// And the interest declared for it is withdrawn, or a stale digest keeps drawing traffic.
	require.Equal(t, 1, len(h.interest))
	require.Equal(t, false, h.interest[0].want)
}

// TestSubscribedTopicIsNotTrackedAsAPushTopic: leaving a topic the subscription owns would take
// the subscription down with it.
func TestSubscribedTopicIsNotTrackedAsAPushTopic(t *testing.T) {
	h := newPushHarness(t)
	topic := "/eth2/abcd1234/data_column_sidecar_6/ssz_snappy"
	h.subscribe(topic)

	require.NoError(t, h.broadcaster.joinForPush(topic))

	require.Equal(t, false, h.broadcaster.pushTopics[topic])
	h.broadcaster.leavePushTopic(topic)
	require.Equal(t, 0, len(h.left))
}

// TestPushTopicSubscribedSinceJoinIsNotLeft covers the custody-change case. Withdrawing interest
// announces subscribe=false, which on a topic we now subscribe to would tell peers to stop sending
// us what we subscribed for.
func TestPushTopicSubscribedSinceJoinIsNotLeft(t *testing.T) {
	h := newPushHarness(t)
	topic := "/eth2/abcd1234/data_column_sidecar_6/ssz_snappy"

	require.NoError(t, h.broadcaster.joinForPush(topic))
	require.Equal(t, true, h.broadcaster.pushTopics[topic])

	h.subscribe(topic)
	h.broadcaster.leavePushTopic(topic)

	require.Equal(t, 0, len(h.left), "a subscribed topic must not be left")
	require.Equal(t, 0, len(h.interest), "and its interest must not be withdrawn")
}
