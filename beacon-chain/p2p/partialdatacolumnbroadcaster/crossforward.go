package partialdatacolumnbroadcaster

import (
	"context"
	"strconv"
	"strings"

	"github.com/OffchainLabs/go-bitfield"
	"github.com/OffchainLabs/prysm/v7/consensus-types/blocks"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// Cross-forwarding: the push direction.
//
// The cross-fill bridge in crossfill.go moves cells between the two axes *inside this node*. That
// helps this node and, through its own column subnets, the peers who share its custody. It does
// nothing for the other ~124 columns, and those are most of the network. EIP-8371's claim is that
// a reconstructor pushing its recovered cells into the column subnets it does *not* custody is
// what turns one node's work into relief for everyone -- R9 is the experiment that checks it.
//
// The push needs nothing from gossipsub that is not already there, which R9 confirmed the hard
// way. A partial message goes to the peers Router.MeshPeers yields, which falls back to fanout
// peers for a topic with no mesh, and the send predicate for the push direction is
// `iSupportSendingPartial(topic) && peerRequestsPartial(peer, topic)`: the first satisfied by
// joining the topic, the second by the peer's own subscription announcement. Joining does not
// announce, so pushing into a column subnet never claims custody of a column we do not hold.
//
// What is *not* obvious, and cost R9 a wrong turn, is that the push is request-driven at the far
// end. An eager push carries the block header and our parts metadata and no cells; the cells go
// out on the next publish, in answer to the peer's request. So the push delivers only to a peer
// that asks -- which a real node does, via emptyPartialColumnsRequestingAll on block arrival, and
// which a node that has merely received a header does not, because both the republish and the
// heartbeat-gossip paths are gated on having published. A first reading of R9's zero deliveries
// blamed addressability and made the push declare partial interest; a two-node probe
// (integrationtest/push_probe_test.go) showed the interest makes no difference and the receiver's
// request makes all of it.
//
// The pull direction is the one that genuinely needed a framework change: `requestsPartial` is
// carried only in a subscribe announcement, so a joined-but-unsubscribed topic could never be seen
// as requesting partials. `Topic.SetPartialInterest` in the vendored fork adds the missing signal.
//
// The two directions stay strictly separate: a *pushed* column explicitly requests nothing, so
// that the arms can be told apart in R9 and so that pushing never drags an unasked-for pull along
// with it.

// crossForwardRow is the event-loop request for a push.
type crossForwardRow struct {
	rowTopic string
	row      *blocks.PartialDataRow
	columns  []uint64
	// pushed is written by the loop: how many column topics the row was actually pushed into.
	pushed *int
}

// pullRow is the event-loop request for a pull.
type pullRow struct {
	rowTopic string
	row      *blocks.PartialDataRow
	columns  []uint64
	// pulled is written by the loop: how many column topics a request went out on.
	pulled *int
}

var (
	errCrossForwardNotWired = errors.New("cross-forwarding needs a topic-join function")
	errPullNotWired         = errors.New("pulling needs a partial-interest function")
)

// TopicPushHooks is how the broadcaster reaches column topics it does not subscribe to. All of it
// must go through whatever owns the process's topic handles: pubsub.Join returns "topic already
// exists" for a second handle, so a broadcaster joining on its own would take the handle the
// subscription path later needs and fail it.
type TopicPushHooks struct {
	// Join joins a topic for fanout push without subscribing to it.
	Join func(topic string) error
	// Leave releases a topic joined by Join. May be nil, at the cost of holding the handle for
	// the process lifetime.
	Leave func(topic string) error
	// SetPartialInterest announces that we request partial messages on a topic we have not
	// subscribed to, which is what the pull direction needs. Nil leaves the pull arm
	// unavailable.
	SetPartialInterest func(topic string, want bool) error
}

// SetTopicPushHooks installs the hooks above.
func (p *PartialColumnBroadcaster) SetTopicPushHooks(hooks TopicPushHooks) {
	p.joinTopicForPush = hooks.Join
	p.leaveTopicForPush = hooks.Leave
	p.setPartialInterest = hooks.SetPartialInterest
}

// joinForPush joins a column topic we do not subscribe to and remembers that we did, so it can be
// left again when its last group is evicted. A topic we are subscribed to is left alone: the
// subscription owns it.
func (p *PartialColumnBroadcaster) joinForPush(topic string) error {
	if err := p.joinTopicForPush(topic); err != nil {
		return err
	}
	if _, subscribed := p.topics[topic]; subscribed {
		return nil
	}
	p.pushTopics[topic] = true

	return nil
}

// leavePushTopic releases a topic joined for push or pull. Called when the topic's last group is
// evicted, which bounds these topics by the group TTL -- and cleans up after a fork rotation for
// free, since the subscription path's own cleanup walks only subscribed topics and would never
// see these.
func (p *PartialColumnBroadcaster) leavePushTopic(topic string) {
	if !p.pushTopics[topic] {
		return
	}
	delete(p.pushTopics, topic)

	if _, subscribed := p.topics[topic]; subscribed {
		// We took a subscription on it since joining -- a custody change, say. The subscription
		// owns the topic now, and withdrawing interest would announce subscribe=false on a topic
		// we are subscribed to, which tells peers to stop sending us what we subscribed for.
		return
	}

	if p.setPartialInterest != nil {
		if err := p.setPartialInterest(topic, false); err != nil {
			p.logger.WithError(err).WithField("topic", topic).Debug("Could not withdraw partial interest")
		}
	}
	if p.leaveTopicForPush == nil {
		return
	}
	if err := p.leaveTopicForPush(topic); err != nil {
		p.logger.WithError(err).WithField("topic", topic).Debug("Could not leave a cross-forwarding topic")
	}
}

// CrossForwardRow pushes the cells of a row into the column topics named by columns, one
// single-cell partial column per topic. It returns how many topics were pushed into.
//
// The caller chooses the columns, and with them the whole policy EIP-8371 leaves open: which
// columns (the ones it does not custody), how many of them ("MAY limit to random subsets") and
// when ("MAY delay to avoid competing with native subscribers"). This function is only the
// mechanism, which is what lets the node and the measurement harness share it.
func (p *PartialColumnBroadcaster) CrossForwardRow(ctx context.Context, rowTopic string, row *blocks.PartialDataRow, columns []uint64) (int, error) {
	if row == nil {
		return 0, errors.New("row is nil")
	}
	if len(columns) == 0 {
		return 0, nil
	}

	var pushed int
	// Clone on ingress, for the same reason PublishRow does: the loop takes ownership, and a
	// reconstruction scheduler holding the caller's copy would otherwise be writing loop state.
	adopted := row.Clone()
	req, err := p.enqueue(ctx, requestKindCrossForwardRow, requestValues{
		crossForwardRow: crossForwardRow{
			rowTopic: rowTopic,
			row:      &adopted,
			columns:  columns,
			pushed:   &pushed,
		},
	})
	if err != nil {
		return 0, err
	}
	if err := req.waitForResponse(); err != nil {
		return 0, err
	}

	return pushed, nil
}

// crossForwardRowOnLoop runs the push. It owns all column and row state.
func (p *PartialColumnBroadcaster) crossForwardRowOnLoop(req crossForwardRow) error {
	if p.joinTopicForPush == nil {
		return errCrossForwardNotWired
	}

	row := req.row
	groupID := row.GroupID()
	held := p.columnStatesForGroup(groupID)
	rowIndex := row.RowIndex()

	// Build the whole batch first, then publish it in one call, so the shipped publish path
	// owns verifier creation, the group TTL and the published-topics bookkeeping.
	topics := make([]string, 0, len(req.columns))
	pushColumns := make([]blocks.PartialDataColumn, 0, len(req.columns))
	for _, columnIndex := range req.columns {
		if _, ours := held[columnIndex]; ours {
			// We hold this column ourselves. The cross-fill bridge has already filled it and
			// its own subnet republishes it; pushing would duplicate that.
			continue
		}
		if columnIndex >= row.Included.Len() || !row.Included.BitAt(columnIndex) {
			continue
		}
		topic, err := columnTopicFromRowTopic(req.rowTopic, columnIndex)
		if err != nil {
			return errors.Wrap(err, "column topic from row topic")
		}
		if _, subscribed := p.topics[topic]; subscribed {
			// Subscribed but no state for this group yet -- the header has not reached us on
			// that topic. Still not ours to cross-forward: we are a member of that subnet and
			// its own traffic covers it.
			continue
		}
		column, err := p.pushColumnFromRow(row, columnIndex)
		if err != nil {
			return errors.Wrapf(err, "build push column %d", columnIndex)
		}
		if err := p.joinForPush(topic); err != nil {
			// A topic we cannot join is one we cannot push into, but the others are unaffected.
			p.logger.WithError(err).WithField("topic", topic).
				Debug("Could not join column topic for cross-forwarding")
			continue
		}
		topics = append(topics, topic)
		pushColumns = append(pushColumns, column)
	}

	if len(topics) == 0 {
		return nil
	}

	*req.pushed = len(topics)
	crossForwardedColumnsTotal.Add(float64(len(topics)))
	p.logger.WithFields(logrus.Fields{
		"rowIndex": rowIndex,
		"columns":  len(topics),
	}).Debug("Cross-forwarding a row into non-custodied column subnets")

	return p.publish(func(yield func(string, blocks.PartialDataColumn) bool) {
		for i, topic := range topics {
			if !yield(topic, pushColumns[i]) {
				return
			}
		}
	})
}

// pushColumnFromRow builds the partial column a row contributes to one column: a column holding
// exactly the cell at the row's blob index, and requesting nothing.
func (p *PartialColumnBroadcaster) pushColumnFromRow(row *blocks.PartialDataRow, columnIndex uint64) (blocks.PartialDataColumn, error) {
	column, err := blocks.NewPartialDataColumn(
		row.BlockRoot(),
		row.SignedBlockHeader(),
		columnIndex,
		row.KzgCommitments(),
		row.KzgCommitmentsInclusionProof(),
	)
	if err != nil {
		return blocks.PartialDataColumn{}, errors.Wrap(err, "new partial data column")
	}
	if !column.ExtendFromVerifiedCell(row.RowIndex(), row.Cells[columnIndex], row.Proofs[columnIndex]) {
		return blocks.PartialDataColumn{}, errors.Errorf("cell for row %d rejected by column %d", row.RowIndex(), columnIndex)
	}
	// Request nothing. A column left to its default asks for every cell it lacks, and asking a
	// subnet we do not custody for cells is the pull direction, which is a separate arm gated on
	// a framework change (notes/rowdas/TODO.md F1). Pushing must not smuggle it in.
	if err := column.SetPartsRequests(bitfield.NewBitlist(column.KzgCommitmentCount())); err != nil {
		return blocks.PartialDataColumn{}, errors.Wrap(err, "clear parts requests on push column")
	}

	return column, nil
}

// PullRowFromColumns asks the column subnets named by columns for the one cell each holds of this
// row, without subscribing to them. It returns how many topics a request went out on.
//
// This is EIP-8371's optional direction, and the one whose value is least obvious. A row
// reconstructor already holds a cell of every row from its own custody, so it has nothing to pull;
// the case this serves is a custody-minimum node whose row is short of the reconstruction
// threshold and whose row subnet has not made up the difference. R9 is the experiment that decides
// whether that case is worth the traffic.
//
// Requesting only the cell at this row's index -- rather than the whole column -- is what keeps it
// honest. We are not a subscriber of these topics and have no business collecting a column we do
// not custody.
func (p *PartialColumnBroadcaster) PullRowFromColumns(ctx context.Context, rowTopic string, row *blocks.PartialDataRow, columns []uint64) (int, error) {
	if row == nil {
		return 0, errors.New("row is nil")
	}
	if len(columns) == 0 {
		return 0, nil
	}

	var pulled int
	adopted := row.Clone()
	req, err := p.enqueue(ctx, requestKindPullRow, requestValues{
		pullRow: pullRow{
			rowTopic: rowTopic,
			row:      &adopted,
			columns:  columns,
			pulled:   &pulled,
		},
	})
	if err != nil {
		return 0, err
	}
	if err := req.waitForResponse(); err != nil {
		return 0, err
	}

	return pulled, nil
}

// pullRowOnLoop runs the pull. It owns all column and row state.
func (p *PartialColumnBroadcaster) pullRowOnLoop(req pullRow) error {
	if p.joinTopicForPush == nil {
		return errCrossForwardNotWired
	}
	if p.setPartialInterest == nil {
		return errPullNotWired
	}

	row := req.row
	groupID := row.GroupID()
	held := p.columnStatesForGroup(groupID)
	rowIndex := row.RowIndex()

	topics := make([]string, 0, len(req.columns))
	pullColumns := make([]blocks.PartialDataColumn, 0, len(req.columns))
	for _, columnIndex := range req.columns {
		if _, ours := held[columnIndex]; ours {
			// Our own subnet already asks for this column's cells.
			continue
		}
		if columnIndex < row.Included.Len() && row.Included.BitAt(columnIndex) {
			// Already have this one.
			continue
		}
		topic, err := columnTopicFromRowTopic(req.rowTopic, columnIndex)
		if err != nil {
			return errors.Wrap(err, "column topic from row topic")
		}
		if _, subscribed := p.topics[topic]; subscribed {
			// A subscription already asks this subnet for everything, and announcing partial
			// interest on a subscribed topic is refused outright -- it would carry
			// subscribe=false and tell peers to stop sending us what we subscribed for.
			continue
		}
		column, err := p.pullColumnForRow(row, columnIndex)
		if err != nil {
			return errors.Wrapf(err, "build pull column %d", columnIndex)
		}
		if err := p.joinForPush(topic); err != nil {
			p.logger.WithError(err).WithField("topic", topic).
				Debug("Could not join column topic to pull from")
			continue
		}
		// The announcement is what makes the request reachable: without it the peers we ask have
		// no way to see us as a valid recipient of partial messages on this topic, so their reply
		// would go nowhere.
		if err := p.setPartialInterest(topic, true); err != nil {
			p.logger.WithError(err).WithField("topic", topic).
				Debug("Could not declare partial interest in a column topic")
			continue
		}
		topics = append(topics, topic)
		pullColumns = append(pullColumns, column)
	}

	if len(topics) == 0 {
		return nil
	}

	*req.pulled = len(topics)
	pulledColumnsTotal.Add(float64(len(topics)))
	p.logger.WithFields(logrus.Fields{
		"rowIndex": rowIndex,
		"columns":  len(topics),
	}).Debug("Pulling a row's missing cells from non-custodied column subnets")

	return p.publish(func(yield func(string, blocks.PartialDataColumn) bool) {
		for i, topic := range topics {
			if !yield(topic, pullColumns[i]) {
				return
			}
		}
	})
}

// pullColumnForRow builds the partial column that carries a pull request: no cells, and a request
// for exactly the cell at this row's blob index.
func (p *PartialColumnBroadcaster) pullColumnForRow(row *blocks.PartialDataRow, columnIndex uint64) (blocks.PartialDataColumn, error) {
	column, err := blocks.NewPartialDataColumn(
		row.BlockRoot(),
		row.SignedBlockHeader(),
		columnIndex,
		row.KzgCommitments(),
		row.KzgCommitmentsInclusionProof(),
	)
	if err != nil {
		return blocks.PartialDataColumn{}, errors.Wrap(err, "new partial data column")
	}
	requests := bitfield.NewBitlist(column.KzgCommitmentCount())
	if row.RowIndex() >= requests.Len() {
		return blocks.PartialDataColumn{}, errors.Errorf("row %d beyond column cell count %d", row.RowIndex(), requests.Len())
	}
	requests.SetBitAt(row.RowIndex(), true)
	if err := column.SetPartsRequests(requests); err != nil {
		return blocks.PartialDataColumn{}, errors.Wrap(err, "set parts requests on pull column")
	}

	return column, nil
}

// columnTopicFromRowTopic rewrites a row topic into the column topic for one column, carrying the
// fork digest and encoding suffix across unchanged -- so neither has to be known here, and a
// digest rotation cannot leave the push on a stale topic while the row is on a fresh one.
func columnTopicFromRowTopic(rowTopic string, columnIndex uint64) (string, error) {
	if columnIndex >= numberOfColumns {
		return "", errors.Errorf("column index %d out of range", columnIndex)
	}
	rowAt := strings.Index(rowTopic, DataRowPrefix)
	if rowAt < 0 {
		return "", errors.Errorf("topic %q is not a data row topic", rowTopic)
	}
	suffix := rowTopic[rowAt+len(DataRowPrefix):]
	end := strings.Index(suffix, "/")
	if end < 0 {
		return "", errors.Errorf("data row topic %q has no encoding suffix", rowTopic)
	}

	return rowTopic[:rowAt] + dataColumnSidecarPrefix + strconv.FormatUint(columnIndex, 10) + suffix[end:], nil
}
