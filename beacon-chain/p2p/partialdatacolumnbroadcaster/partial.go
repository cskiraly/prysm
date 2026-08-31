package partialdatacolumnbroadcaster

import (
	"bytes"
	"context"
	stderrors "errors"
	"fmt"
	"github.com/OffchainLabs/prysm/v7/beacon-chain/verification"
	fieldparams "github.com/OffchainLabs/prysm/v7/config/fieldparams"
	"github.com/OffchainLabs/prysm/v7/config/params"
	"github.com/OffchainLabs/prysm/v7/consensus-types/blocks"
	"github.com/OffchainLabs/prysm/v7/consensus-types/primitives"
	"github.com/OffchainLabs/prysm/v7/container/slice"
	"github.com/OffchainLabs/prysm/v7/internal/logrusadapter"
	ethpb "github.com/OffchainLabs/prysm/v7/proto/prysm/v1alpha1"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p-pubsub/partialmessages"
	pubsub_pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"iter"
	"log/slog"
	"strconv"
	"sync"
	"time"
)

const TTLInSlots = 3

const logPackage = "beacon-chain/p2p/partialdatacolumnbroadcaster"

var errInvalidHeader = errors.New("invalid header")

var errMalformedPartialMessage = errors.New("malformed partial message")

// ColumnCallbacks is the interface that the broadcaster uses to validate and handle
// partial data column headers and cells.
type ColumnCallbacks interface {
	// PartialVerifierFromHeader builds and validates a partial column from a new header.
	// Returns (verifier, result, err) where:
	//   - ValidationReject, err!=nil: peer should be penalized
	//   - ValidationIgnore, err!=nil: don't penalize, just ignore
	//   - ValidationAccept, err=nil: valid verifier
	PartialVerifierFromHeader(col *blocks.PartialDataColumn) (verifier *verification.PartialColumnVerifier, result pubsub.ValidationResult, err error)
	// PartialVerifierFromTrustedColumn creates a verifier from a previously validated column.
	PartialVerifierFromTrustedColumn(col *blocks.PartialDataColumn) (*verification.PartialColumnVerifier, error)
	// ValidateColumn validates the KZG proofs of the given cells.
	ValidateColumn(cells []blocks.CellProofBundle) error
	// HandleColumn is called when a partial column has been fully reconstructed.
	HandleColumn(topic string, col blocks.VerifiedRODataColumn)
	// HandleHeader is called when a new partial data column header is first validated.
	HandleHeader(header *ethpb.PartialDataColumnHeader, groupID string)
	// ValidateGloasGroupID validates a Gloas partial-column group's slot and root against local block state:
	// [REJECT] when a seen block at the group's root has a different slot, [IGNORE] when no block for
	// the root has been seen, else [ACCEPT].
	ValidateGloasGroupID(slot primitives.Slot, root [32]byte) pubsub.ValidationResult
}

// Broadcaster is the behaviour of the partial data column broadcaster used by the rest of the node.
type Broadcaster interface {
	// Start runs the event loop. rowCallbacks may be nil, which leaves RowDAS row topics
	// inert: incoming row messages are ignored and no row state is allocated.
	Start(callbacks ColumnCallbacks, rowCallbacks RowCallbacks)
	Publish(ctx context.Context, topicsAndColumns iter.Seq2[string, blocks.PartialDataColumn]) error
	AppendPubSubOpts(opts []pubsub.Option) []pubsub.Option
	Subscribe(ctx context.Context, t *pubsub.Topic) error
	Unsubscribe(ctx context.Context, topic string) error
	// PublishRow offers a row on a row topic: the cells this node holds, or a row it has just
	// recovered. A no-op when row callbacks are not configured.
	PublishRow(ctx context.Context, topic string, row blocks.PartialDataRow) error
	// RowSnapshot returns a deep copy of the row held for a (topic, group), or nil. It goes
	// through the event loop because the loop owns all row state.
	RowSnapshot(ctx context.Context, topic string, groupID []byte) (*blocks.PartialDataRow, error)
	// CrossForwardRow pushes a row's cells into the given column topics, one single-cell
	// partial column each, and reports how many topics it pushed into. The caller picks the
	// columns, which is where EIP-8371 leaves the policy.
	CrossForwardRow(ctx context.Context, rowTopic string, row *blocks.PartialDataRow, columns []uint64) (int, error)
	// PullRowFromColumns asks the given column topics for the one cell each holds of this row,
	// without subscribing to them, and reports how many topics it asked. EIP-8371's optional
	// direction.
	PullRowFromColumns(ctx context.Context, rowTopic string, row *blocks.PartialDataRow, columns []uint64) (int, error)
}

var _ Broadcaster = (*PartialColumnBroadcaster)(nil)

type PartialColumnBroadcaster struct {
	logger *logrus.Entry

	ctx context.Context

	peerFeedback      func(topic string, peer peer.ID, kind pubsub.PeerFeedbackKind) error
	publishPartialCol func(topic string, groupID []byte, col *blocks.PartialDataColumn) error
	// registerPartialCol registers self-initiated partial state without contacting anyone: the
	// heartbeat's partial-message gossip announces it and requests against it are served, but no
	// first-contact burst is spent. The advertisement-only cross-forward (design.md section 6
	// item 16) publishes through this instead of publishPartialCol.
	registerPartialCol func(topic string, groupID []byte) error
	callbacks          ColumnCallbacks
	// map topic -> *pubsub.Topic
	topics map[string]*pubsub.Topic
	// subscribedTopics mirrors topics for lookups from the pubsub loop, which cannot touch the broadcaster-owned topics map.
	subscribedTopics                 sync.Map
	publishedTopics                  sync.Map
	peerFeedbackSemaphore            chan struct{}
	concurrentValidatorSemaphore     chan struct{}
	concurrentHeaderHandlerSemaphore chan struct{}
	// map topic -> map[groupID]PartialColumnVerifier
	partialMsgStore map[string]map[string]*verification.PartialColumnVerifier
	// rowCallbacks is nil when RowDAS is off.
	rowCallbacks RowCallbacks
	// rowStore is the row-topic twin of partialMsgStore: map topic -> map[groupID]rowEntry.
	rowStore          map[string]map[string]*rowEntry
	publishPartialRow func(topic string, groupID []byte, row *blocks.PartialDataRow) error
	// The cross-forwarding hooks, set by whatever owns the process's topic handles. A nil Join
	// leaves cross-forwarding unavailable; a nil setPartialInterest leaves the pull arm
	// unavailable. See TopicPushHooks.
	joinTopicForPush   func(topic string) error
	leaveTopicForPush  func(topic string) error
	setPartialInterest func(topic string, want bool) error
	// pushTopics are the topics joined for cross-forwarding rather than subscribed to, so they
	// can be left again when their last group is evicted.
	pushTopics map[string]bool
	groupTTL   map[string]int8
	// validHeaderCache caches validated headers by group ID (works across topics)
	validHeaderCache map[string]*ethpb.PartialDataColumnHeader
	// map groupID -> map[peer.ID]bool
	headerSentCache map[string]map[peer.ID]bool
	incomingReq     chan request
	eagerPushed     map[string]*eagerPushAgg
	// actionReasons attributes generated publish actions to the comparison that caused them. See
	// actionreasons.go for why aggregate counters could not settle the question.
	actionReasons    *actionReasonCounts
	republishSkipped map[string]map[uint64]bool
	// rowClaimWake wakes the loop when a request claim lapses or held news reaches its deadline,
	// so neither waits for whatever traffic arrives next. See coalesce.go.
	rowClaimWake map[coalesceKey]*time.Timer
	// claimWake carries due wake-ups from timer goroutines onto the event loop.
	claimWake chan coalesceKey
	// armCoalesce and now are seams: a test needs to drive a timer without sleeping, and the
	// D9 work learned that a timer with no seam is a timer no test can observe.
	armCoalesce func(time.Duration, func()) *time.Timer
	now         func() time.Time
}

type eagerPushAgg struct {
	indices map[uint64]bool
	peers   map[peer.ID]bool
}

type requestKind uint8

const (
	requestKindPublish requestKind = iota
	requestKindSubscribe
	requestKindUnsubscribe
	requestKindGossip
	requestKindHandleIncomingRPC
	requestKindCellsValidated
	requestKindHandleIncomingRowRPC
	requestKindRowCellsValidated
	requestKindPublishRow
	requestKindRowSnapshot
	requestKindGossipRow
	requestKindCrossForwardRow
	requestKindPullRow
	requestKindRowPeerHasWholeRow
)

func (k requestKind) String() string {
	switch k {
	case requestKindPublish:
		return "publish"
	case requestKindSubscribe:
		return "subscribe"
	case requestKindUnsubscribe:
		return "unsubscribe"
	case requestKindGossip:
		return "gossip"
	case requestKindHandleIncomingRPC:
		return "handle_incoming_rpc"
	case requestKindCellsValidated:
		return "cells_validated"
	case requestKindHandleIncomingRowRPC:
		return "handle_incoming_row_rpc"
	case requestKindRowCellsValidated:
		return "row_cells_validated"
	case requestKindPublishRow:
		return "publish_row"
	case requestKindRowSnapshot:
		return "row_snapshot"
	case requestKindGossipRow:
		return "gossip_row"
	case requestKindCrossForwardRow:
		return "cross_forward_row"
	case requestKindPullRow:
		return "pull_row"
	case requestKindRowPeerHasWholeRow:
		return "row_peer_has_whole_row"
	default:
		return "unknown"
	}
}

type requestValues struct {
	cellsValidated    *cellsValidated
	rowCellsValidated *rowCellsValidated
	rowSnapshot       *rowSnapshotRequest
	incomingRowRPC    incomingRowRPC
	publishRow        publishRow
	crossForwardRow   crossForwardRow
	pullRow           pullRow
	rowPeerHasWhole   rowPeerHasWhole
	unsub             unsubscribe
	incomingRPC       incomingPartialRPC
	sub               subscribe
	publish           publish
	gossip            gossip
}

type request struct {
	requestValues
	ctx      context.Context
	kind     requestKind
	response chan error
}

func newRequest(ctx context.Context, kind requestKind, v requestValues) request {
	return request{
		requestValues: v,
		ctx:           ctx,
		kind:          kind,
		response:      make(chan error, 1),
	}
}

// finish sends the result to the caller waiting on the response channel.
func (r request) finish(err error) {
	r.response <- err
}

// enqueue creates and enqueues a request, blocking until it is accepted.
// Returns an error if the broadcaster has stopped or the context has been cancelled.
// A nil ctx is permitted for fire-and-forget requests that have no cancellation.
func (p *PartialColumnBroadcaster) enqueue(ctx context.Context, kind requestKind, v requestValues) (request, error) {
	req := newRequest(ctx, kind, v)
	select {
	case p.incomingReq <- req:
		return req, nil
	case <-p.ctx.Done():
		return req, errPartialBroadcasterStopped
	case <-ctx.Done():
		return req, ctx.Err()
	}
}

// tryEnqueue creates and enqueues a request without blocking.
// Returns false if the request channel is full.
func (p *PartialColumnBroadcaster) tryEnqueue(kind requestKind, v requestValues) (request, bool) {
	req := newRequest(p.ctx, kind, v)
	select {
	case p.incomingReq <- req:
		return req, true
	default:
		return req, false
	}
}

// waitForResponse blocks until the request has been processed and returns the result.
// If the request's context is cancelled before a response arrives, it returns the context error.
func (r request) waitForResponse() error {
	select {
	case err := <-r.response:
		return err
	case <-r.ctx.Done():
		return r.ctx.Err()
	}
}

type publish struct {
	topicsAndColumns iter.Seq2[string, blocks.PartialDataColumn]
}

type subscribe struct {
	t *pubsub.Topic
}

type unsubscribe struct {
	topic string
}

type publishRow struct {
	topic string
	row   blocks.PartialDataRow
}

type incomingPartialRPC struct {
	*pubsub_pb.PartialMessagesExtension
	from    peer.ID
	message *ethpb.PartialDataColumnSidecar
	isGloas bool
	slot    primitives.Slot
	root    [32]byte
}

func (r incomingPartialRPC) logFields() logrus.Fields {
	return logrus.Fields{
		"from":  r.from,
		"topic": r.GetTopicID(),
		"group": fmt.Sprintf("%#x", r.GroupID),
	}
}

type cellsValidated struct {
	validationTook time.Duration
	topic          string
	group          []byte
	cellIndices    []uint64
	cells          []blocks.CellProofBundle
}

func (c *cellsValidated) logFields() logrus.Fields {
	return logrus.Fields{
		"topic": c.topic,
		"group": fmt.Sprintf("%#x", c.group),
	}
}

// gossip is used when we are republishing our PartialDataColumn to gossip peers.
type gossip struct {
	topic   string
	groupID []byte
}

func NewBroadcaster(ctx context.Context, logger *logrus.Logger) *PartialColumnBroadcaster {
	concurrency := params.BeaconConfig().DataColumnSidecarSubnetCount
	return &PartialColumnBroadcaster{
		ctx:              ctx,
		topics:           make(map[string]*pubsub.Topic),
		partialMsgStore:  make(map[string]map[string]*verification.PartialColumnVerifier),
		rowStore:         make(map[string]map[string]*rowEntry),
		pushTopics:       make(map[string]bool),
		groupTTL:         make(map[string]int8),
		validHeaderCache: make(map[string]*ethpb.PartialDataColumnHeader),
		headerSentCache:  make(map[string]map[peer.ID]bool),
		eagerPushed:      make(map[string]*eagerPushAgg),
		actionReasons:    newActionReasonCounts(),
		rowClaimWake:     make(map[coalesceKey]*time.Timer),
		claimWake:        make(chan coalesceKey, 64),
		armCoalesce:      time.AfterFunc,
		now:              time.Now,
		republishSkipped: make(map[string]map[uint64]bool),

		// GossipSub sends the messages to this channel. The buffer should be
		// big enough to avoid dropping messages. We don't want to block the gossipsub event loop for this.
		incomingReq: make(chan request, 128*16),
		logger:      logger.WithField("package", logPackage),

		peerFeedbackSemaphore:            make(chan struct{}, concurrency),
		concurrentValidatorSemaphore:     make(chan struct{}, concurrency),
		concurrentHeaderHandlerSemaphore: make(chan struct{}, concurrency),
	}
}

// onEmitGossip enqueues a gossip request for the broadcaster's event loop.
func (p *PartialColumnBroadcaster) onEmitGossip(topic string, groupID []byte, _ []peer.ID, _ map[peer.ID]blocks.PartialDataColumnPeerState) {
	kind := requestKindGossip
	if axis, _, err := classifyTopic(topic); err == nil && axis == topicKindRow {
		kind = requestKindGossipRow
	}
	// Drop gossip emission if we have too many pending requests.
	p.tryEnqueue(kind, requestValues{
		gossip: gossip{
			topic:   topic,
			groupID: groupID,
		},
	})
}

// onIncomingRPC processes an incoming partial message RPC by updating peer state
// and enqueuing the message for the broadcaster's event loop.
func (p *PartialColumnBroadcaster) onIncomingRPC(from peer.ID, peerStates map[peer.ID]blocks.PartialDataColumnPeerState, rpc *pubsub_pb.PartialMessagesExtension) error {
	if rpc == nil {
		return nil
	}

	// Which axis this message belongs to is decided by the topic, which is peer-controlled,
	// so an unrecognised or out-of-range topic is downscored rather than defaulted.
	kind, subnet, err := classifyTopic(rpc.GetTopicID())
	if err != nil {
		p.logger.WithError(err).WithFields(logrus.Fields{
			"peer":  from,
			"topic": rpc.GetTopicID(),
		}).Debug("Invalid topic ID")
		p.reportPeerFeedbackAsync(rpc.GetTopicID(), from, pubsub.PeerFeedbackInvalidMessage)
		return errors.Wrapf(err, "invalid topic ID %q", rpc.GetTopicID())
	}
	if kind == topicKindRow {
		return p.onIncomingRowRPC(from, peerStates, rpc, subnet)
	}

	// Parse the group ID to detect the fork (Fulu 0x00||root, 33B; Gloas 0x01||SSZ(groupID), 41B).
	// This validates the version byte, length, and (for Gloas) the SSZ encoding in one place.
	isGloas, slot, root, err := blocks.ParsePartialColumnGroupID(rpc.GetGroupID())
	if err != nil {
		p.logger.WithError(err).WithFields(logrus.Fields{
			"peer":  from,
			"topic": rpc.GetTopicID(),
			"got":   len(rpc.GetGroupID()),
		}).Debug("Invalid group ID")
		p.reportPeerFeedbackAsync(rpc.GetTopicID(), from, pubsub.PeerFeedbackInvalidMessage)
		return errors.Wrap(err, "parse partial column group id")
	}

	// Accept messages for subscribed topics and for topics we have published our own
	// column on (the proposer publishes on all topics, custody or not). The published
	// case is essential: this callback records the peer's parts-requests below, and
	// that request state is the only trigger for sending cells to a partial-requesting
	// peer — dropping these messages on published topics starves the network of the
	// proposer's cells.
	if _, subscribed := p.subscribedTopics.Load(rpc.GetTopicID()); !subscribed {
		if _, published := p.publishedTopics.Load(rpc.GetTopicID()); !published {
			p.logIgnoreUnsubscribedTopic(from, rpc.GetTopicID())
			return nil
		}
	}

	// Reject groups whose fork does not match the topic's fork digest, e.g. a Fulu group ID
	// on a Gloas-digest topic.
	topicIsGloas, err := topicForkIsGloas(rpc.GetTopicID())
	if err != nil {
		return errors.Wrap(err, "topicForkIsGloas")
	}
	if topicIsGloas != isGloas {
		p.logger.WithFields(logrus.Fields{
			"peer":       from,
			"topic":      rpc.GetTopicID(),
			"gloasGroup": isGloas,
		}).Debug("Group ID fork does not match topic fork")
		p.reportPeerFeedbackAsync(rpc.GetTopicID(), from, pubsub.PeerFeedbackInvalidMessage)
		return errors.Errorf("group ID fork (gloas=%t) does not match topic fork %q", isGloas, rpc.GetTopicID())
	}

	nextPeerState, message, err := updatePeerStateFromIncomingRPC(peerStates[from], rpc, isGloas)
	if err != nil {
		// A malformed message body is the peer's fault, so downscore it. Other errors
		// are dropped without penalty.
		if errors.Is(err, errMalformedPartialMessage) {
			p.reportPeerFeedbackAsync(rpc.GetTopicID(), from, pubsub.PeerFeedbackInvalidMessage)
		}
		return errors.Wrap(err, "update peer state from incoming rpc")
	}

	_, ok := p.tryEnqueue(requestKindHandleIncomingRPC, requestValues{
		incomingRPC: incomingPartialRPC{rpc, from, message, isGloas, slot, root},
	})
	if !ok {
		p.logger.WithFields(logrus.Fields{
			"peer":  from,
			"topic": rpc.GetTopicID(),
			"group": fmt.Sprintf("%#x", rpc.GetGroupID()),
		}).Warn("Dropping incoming partial RPC")
		return errors.New("incomingReq channel is full, dropping RPC")
	}
	peerStates[from] = nextPeerState
	return nil
}

func (p *PartialColumnBroadcaster) reportPeerFeedbackAsync(topic string, from peer.ID, kind pubsub.PeerFeedbackKind) {
	select {
	case p.peerFeedbackSemaphore <- struct{}{}:
		go func() {
			defer func() { <-p.peerFeedbackSemaphore }()
			// return early if the context is done (e.g. the broadcaster is shutting down) as gossipsub loop
			// might already be exiting
			if p.ctx.Err() != nil {
				return
			}
			p.reportPeerFeedback(topic, from, kind)
		}()
	default:
		p.logger.WithFields(logrus.Fields{
			"peer":  from,
			"topic": topic,
		}).Warn("Peer feedback semaphore saturated, dropping feedback")
	}
}

func (p *PartialColumnBroadcaster) reportPeerFeedback(topic string, from peer.ID, kind pubsub.PeerFeedbackKind) {
	if err := p.peerFeedback(topic, from, kind); err != nil {
		p.logger.WithFields(logrus.Fields{"peer": from, "topic": topic}).WithError(err).Debug("Failed to report peer feedback")
	}
}

func (p *PartialColumnBroadcaster) logIgnoreUnsubscribedTopic(from peer.ID, topic string) {
	p.logger.WithFields(logrus.Fields{"peer": from, "topic": topic}).Debug("Ignoring partial message for unsubscribed topic")
}

// AppendPubSubOpts adds the necessary pubsub options to enable partial messages.
func (p *PartialColumnBroadcaster) AppendPubSubOpts(opts []pubsub.Option) []pubsub.Option {
	slogger := slog.New(logrusadapter.Handler{Logger: p.logger.Logger}).With("package", logPackage)
	opts = append(opts,
		pubsub.WithPartialMessagesExtension(&partialmessages.PartialMessagesExtension[blocks.PartialDataColumnPeerState]{
			Logger:        slogger,
			OnEmitGossip:  p.onEmitGossip,
			OnIncomingRPC: p.onIncomingRPC,
		}),
		func(ps *pubsub.PubSub) error {
			p.peerFeedback = ps.PeerFeedback
			p.publishPartialCol = func(topic string, groupID []byte, col *blocks.PartialDataColumn) error {
				onEagerPush := func(remote peer.ID) {
					p.recordEagerPush(groupID, col.Index(), remote)
				}
				onAction := func(_ peer.ID, reason blocks.ActionReason) {
					p.actionReasons.record(false, reason)
				}
				return pubsub.PublishPartial(ps, topic, groupID, col.PublishActionsFn(p.headerSentCacheFor(groupID, col), onEagerPush, onAction))
			}
			p.registerPartialCol = func(topic string, groupID []byte) error {
				return pubsub.RegisterPartial[blocks.PartialDataColumnPeerState](ps, topic, groupID)
			}
			p.publishPartialRow = func(topic string, groupID []byte, row *blocks.PartialDataRow) error {
				onEagerPush := func(remote peer.ID) {
					p.recordEagerPush(groupID, row.RowIndex(), remote)
				}
				onAction := func(_ peer.ID, reason blocks.ActionReason) {
					p.actionReasons.record(true, reason)
				}
				err := pubsub.PublishPartial(ps, topic, groupID, row.PublishActionsFn(p.rowHeaderSentCacheFor(groupID), onEagerPush, onAction))
				// Claims are committed during that publish, so this is the point at which the
				// earliest deadline is known.
				p.armRowClaimWake(topic, groupID, row)

				return err
			}
			return nil
		},
	)
	return opts
}

// Start starts the event loop of the PartialColumnBroadcaster.
// It accepts the required validator and handler functions, returning an error if any is nil.
// Note: The event loop is blocking and so the broadcaster should be started in a goroutine.
func (p *PartialColumnBroadcaster) Start(callbacks ColumnCallbacks, rowCallbacks RowCallbacks) {
	p.callbacks = callbacks
	p.rowCallbacks = rowCallbacks
	p.loop()
}

var (
	errPartialBroadcasterStopped = errors.New("partial column broadcaster stopped")
	errUnknownRequestKind        = errors.New("unknown request kind")
)

func (p *PartialColumnBroadcaster) loop() {
	cleanup := time.NewTicker(params.BeaconConfig().SlotDuration())
	for {
		select {
		case key := <-p.claimWake:
			p.handleClaimWake(key)
		case req := <-p.incomingReq:
			// This check enables the requester to cancel the request by cancelling the given context.
			if req.ctx.Err() != nil {
				p.logger.WithError(req.ctx.Err()).WithField("kind", req.kind.String()).
					Debug("Context canceled for PartialColumnBroadcaster event.") // Debug log level to avoid log storm at node shutdown.
				req.finish(req.ctx.Err())
				continue
			}
			var err error
			switch req.kind {
			case requestKindPublish:
				err = p.publish(req.publish.topicsAndColumns)
			case requestKindSubscribe:
				err = p.subscribe(req.sub.t)
			case requestKindUnsubscribe:
				err = p.unsubscribe(req.unsub.topic)
			case requestKindGossip:
				p.gossip(req.gossip.topic, req.gossip.groupID)
			case requestKindHandleIncomingRPC:
				err = p.handleIncomingRPC(req.incomingRPC)
			case requestKindCellsValidated:
				err = p.handleCellsValidated(req.cellsValidated)
			case requestKindHandleIncomingRowRPC:
				err = p.handleIncomingRowRPC(req.incomingRowRPC)
			case requestKindRowCellsValidated:
				err = p.handleRowCellsValidated(req.rowCellsValidated)
			case requestKindPublishRow:
				err = p.publishRowOnLoop(req.publishRow.topic, req.publishRow.row)
			case requestKindRowSnapshot:
				err = p.rowSnapshotOnLoop(req.rowSnapshot)
			case requestKindGossipRow:
				p.gossipRow(req.gossip.topic, req.gossip.groupID)
			case requestKindCrossForwardRow:
				err = p.crossForwardRowOnLoop(req.crossForwardRow)
			case requestKindPullRow:
				err = p.pullRowOnLoop(req.pullRow)
			case requestKindRowPeerHasWholeRow:
				p.handleRowPeerHasWholeRow(req.rowPeerHasWhole)
			default:
				err = errUnknownRequestKind
			}
			if err != nil {
				p.logger.WithField("kind", req.kind.String()).WithError(err).
					Error("Failure handling PartialColumnBroadcaster event.")
				err = errors.Wrap(err, "partial column broadcaster "+req.kind.String()+" event")
			}
			req.finish(err)
		case <-p.ctx.Done():
			// Drain remaining requests before exiting the loop.
			for {
				select {
				case req := <-p.incomingReq:
					req.finish(errPartialBroadcasterStopped)
				default:
					return
				}
			}
		case <-cleanup.C:
			p.flushAggregatedLogs()
			p.evictExpiredGroups()
		}
	}
}

func (p *PartialColumnBroadcaster) headerSentCacheFor(groupID []byte, col *blocks.PartialDataColumn) map[peer.ID]bool {
	if col.IsGloas() {
		return nil
	}
	cache, ok := p.headerSentCache[string(groupID)]
	if !ok {
		cache = make(map[peer.ID]bool)
		p.headerSentCache[string(groupID)] = cache
	}
	return cache
}

// rowHeaderSentCacheFor returns the per-group set of peers already sent this block's header.
// It is the same cache the column path uses, on purpose: a peer needs the header once, not once
// per axis.
func (p *PartialColumnBroadcaster) rowHeaderSentCacheFor(groupID []byte) map[peer.ID]bool {
	cache, ok := p.headerSentCache[string(groupID)]
	if !ok {
		cache = make(map[peer.ID]bool)
		p.headerSentCache[string(groupID)] = cache
	}
	return cache
}

func (p *PartialColumnBroadcaster) recordEagerPush(groupID []byte, columnIndex uint64, remote peer.ID) {
	agg, ok := p.eagerPushed[string(groupID)]
	if !ok {
		agg = &eagerPushAgg{indices: make(map[uint64]bool), peers: make(map[peer.ID]bool)}
		p.eagerPushed[string(groupID)] = agg
	}
	agg.indices[columnIndex] = true
	agg.peers[remote] = true
}

func (p *PartialColumnBroadcaster) recordRepublishSkip(groupID []byte, columnIndex uint64) {
	indices, ok := p.republishSkipped[string(groupID)]
	if !ok {
		indices = make(map[uint64]bool)
		p.republishSkipped[string(groupID)] = indices
	}
	indices[columnIndex] = true
}

func (p *PartialColumnBroadcaster) flushAggregatedLogs() {
	for groupID, agg := range p.eagerPushed {
		p.logger.WithFields(logrus.Fields{
			"group":   fmt.Sprintf("%#x", groupID),
			"count":   len(agg.indices),
			"indices": slice.SortedPrettySliceFromMap(agg.indices),
			"peers":   len(agg.peers),
		}).Debug("Eager pushed partial data columns")
		delete(p.eagerPushed, groupID)
	}
	for groupID, indices := range p.republishSkipped {
		p.logger.WithFields(logrus.Fields{
			"group":   fmt.Sprintf("%#x", groupID),
			"count":   len(indices),
			"indices": slice.SortedPrettySliceFromMap(indices),
		}).Debug("Columns not published, skipping republish")
		delete(p.republishSkipped, groupID)
	}
}

func (p *PartialColumnBroadcaster) evictExpiredGroups() {
	for groupID, ttl := range p.groupTTL {
		if ttl > 0 {
			p.groupTTL[groupID] = ttl - 1
			continue
		}

		delete(p.groupTTL, groupID)
		delete(p.validHeaderCache, groupID)
		delete(p.headerSentCache, groupID)
		// Cancel any armed flush for this group, so a late timer cannot resurrect state for a
		// group that no longer exists -- the failure mode the review called out for per-group
		// timer maps.
		p.dropRowClaimWake([]byte(groupID))
		for topic, msgStore := range p.partialMsgStore {
			delete(msgStore, groupID)
			if len(msgStore) == 0 {
				delete(p.partialMsgStore, topic)
				p.publishedTopics.Delete(topic)
				// A topic we joined for cross-forwarding exists only for the groups we pushed or
				// pulled on it. The subscription path's cleanup walks subscribed topics only, so
				// without this these would outlive their fork digest.
				p.leavePushTopic(topic)
			}
		}
		for topic, rowStore := range p.rowStore {
			delete(rowStore, groupID)
			if len(rowStore) == 0 {
				delete(p.rowStore, topic)
			}
		}
		// Last, so the application drops its per-group state in step with ours. Synchronous by
		// contract -- see RowGroupEvicted -- because a notice that arrived after the group was
		// rebuilt would clear the wrong generation's state.
		if p.rowCallbacks != nil {
			p.rowCallbacks.RowGroupEvicted([]byte(groupID))
		}
	}
}

func (p *PartialColumnBroadcaster) getPartialVerifier(topic string, group []byte) *verification.PartialColumnVerifier {
	topicStore, ok := p.partialMsgStore[topic]
	if !ok {
		return nil
	}
	verifier, ok := topicStore[string(group)]
	if !ok {
		return nil
	}
	return verifier
}

func (p *PartialColumnBroadcaster) getDataColumn(topic string, group []byte) *blocks.PartialDataColumn {
	verifier := p.getPartialVerifier(topic, group)
	if verifier == nil {
		return nil
	}
	return verifier.Column
}

func (p *PartialColumnBroadcaster) handleIncomingRPC(rpc incomingPartialRPC) error {
	if p.peerFeedback == nil || p.publishPartialCol == nil {
		return errors.New("pubsub not initialized")
	}

	topicID := rpc.GetTopicID()
	// Only act on partial messages for topics we are currently subscribed to, OR for
	// groups we have published our own column for.
	if _, subscribed := p.topics[topicID]; !subscribed {
		if p.getPartialVerifier(topicID, rpc.GroupID) == nil {
			p.logIgnoreUnsubscribedTopic(rpc.from, topicID)
			return nil
		}
	}

	message := rpc.message
	hasMessage := message != nil

	groupID := rpc.GroupID
	ourVerifier := p.getPartialVerifier(topicID, groupID)
	var shouldRepublish bool

	// In Gloas, a nil verifier means we have not published this
	// column, so any cells the peer sends are unsolicited and dropped, never buffered.
	// [REJECT] downscore if a seen block at the group's root has a mismatched slot, or if the peer
	// pushed cells before we published; [IGNORE] otherwise.
	if ourVerifier == nil && rpc.isGloas {
		if p.callbacks.ValidateGloasGroupID(rpc.slot, rpc.root) == pubsub.ValidationReject {
			p.logger.WithFields(rpc.logFields()).Debug("Rejecting Gloas partial message: group slot does not match block slot")
			p.reportPeerFeedback(topicID, rpc.from, pubsub.PeerFeedbackInvalidMessage)
			return nil
		}
		if hasMessage && message.CellsPresentBitmap.Count() > 0 {
			p.logger.WithFields(rpc.logFields()).Debug("Peer pushed Gloas cells before we published our column; downscoring")
			p.reportPeerFeedback(topicID, rpc.from, pubsub.PeerFeedbackInvalidMessage)
		}
		return nil
	}

	// Metadata only, with no state for this topic yet, but the group's header already validated
	// on another topic.
	//
	// This is not a rare case, it is the normal one for a proposer's eager push. headerSentCache
	// is keyed by group rather than by topic -- deliberately, since a peer needs the header once
	// per block -- so the second and later eager pushes to the same peer in the same group carry
	// parts metadata and nothing else. Without the branch below the receiver has no state to
	// attach that metadata to and drops it, so a peer custodying k columns can receive partial
	// cells on exactly one of them per block. Measured: in R9's setup, with no full-message path
	// to cover for it, seven peers each completed 1 of their 8 published columns.
	//
	// Restricted to subscribed topics. A peer cannot use this to make us allocate state for
	// topics we do not follow; the bound is our own custody, as it is for the message path.
	if ourVerifier == nil && !hasMessage {
		if header := p.validHeaderCache[string(groupID)]; header != nil {
			if _, subscribed := p.topics[topicID]; subscribed {
				columnIndex, err := extractColumnIndexFromTopic(topicID)
				if err != nil {
					return errors.Wrap(err, "extract column index from topic")
				}
				verifier, err := p.makeVerifierFromHeader(rpc.root, header, columnIndex, true, rpc)
				if err != nil {
					if errors.Is(err, errInvalidHeader) {
						return nil
					}
					return errors.Wrap(err, "make verifier from cached header")
				}
				topicStore, ok := p.partialMsgStore[topicID]
				if !ok {
					topicStore = make(map[string]*verification.PartialColumnVerifier)
					p.partialMsgStore[topicID] = topicStore
				}
				topicStore[string(groupID)] = verifier
				p.groupTTL[string(groupID)] = TTLInSlots
				ourVerifier = verifier
				shouldRepublish = true
			}
		}
	}

	if ourVerifier == nil && hasMessage {
		header, headerWasCached := p.getHeader(groupID, message)
		if header == nil {
			return nil
		}

		// downscore peer if invalid header
		if header.SignedBlockHeader == nil || header.SignedBlockHeader.Header == nil {
			p.logger.WithFields(rpc.logFields()).Debug("Header is missing signed block header or header")
			p.reportPeerFeedback(topicID, rpc.from, pubsub.PeerFeedbackInvalidMessage)
			return errors.New("header is missing signed block header or header")
		}

		// downscore peer if invalid header
		root, err := header.SignedBlockHeader.Header.HashTreeRoot()
		if err != nil {
			p.logger.WithFields(rpc.logFields()).WithError(err).Debug("Failed to get root from header")
			p.reportPeerFeedback(topicID, rpc.from, pubsub.PeerFeedbackInvalidMessage)
			return errors.Wrap(err, "failed to get root from header")
		}

		columnIndex, err := extractColumnIndexFromTopic(topicID)
		if err != nil {
			return errors.Wrap(err, "extract column index from topic")
		}

		verifier, err := p.makeVerifierFromHeader(root, header, columnIndex, headerWasCached, rpc)
		if err != nil {
			if err == errInvalidHeader {
				return nil
			}
			return errors.Wrap(err, "make verifier from header")
		}

		if !headerWasCached {
			p.logger.WithFields(rpc.logFields()).Debug("Handling header as it was previously not cached for this group")
			p.handleHeader(rpc, header)
		}

		// Save to store
		topicStore, ok := p.partialMsgStore[topicID]
		if !ok {
			topicStore = make(map[string]*verification.PartialColumnVerifier)
			p.partialMsgStore[topicID] = topicStore
		}
		topicStore[string(groupID)] = verifier
		p.groupTTL[string(groupID)] = TTLInSlots

		ourVerifier = verifier
		shouldRepublish = true
	}

	if ourVerifier == nil {
		// We don't have a partial column for this. Can happen if we got cells
		// without a header.
		return nil
	}
	ourDataColumn := ourVerifier.Column

	if hasMessage {
		err := p.handlePartialCells(ourDataColumn, message, rpc)
		if err != nil {
			return errors.Wrap(err, "handle partial cells")
		}
	}

	return p.republishColumn(ourDataColumn, rpc, shouldRepublish)
}

func (p *PartialColumnBroadcaster) makeVerifierFromHeader(root [fieldparams.RootLength]byte, header *ethpb.PartialDataColumnHeader, columnIndex uint64,
	headerWasCached bool, rpc incomingPartialRPC) (*verification.PartialColumnVerifier, error) {
	topicID := rpc.GetTopicID()

	if len(header.KzgCommitments) == 0 {
		p.logger.WithFields(rpc.logFields()).Debug("Ignoring partial column header with no KZG commitments")
		return nil, errInvalidHeader
	}

	newColumn, err := blocks.NewPartialDataColumn(
		root,
		header.SignedBlockHeader,
		columnIndex,
		header.KzgCommitments,
		header.KzgCommitmentsInclusionProof,
	)
	if err != nil {
		p.logger.WithError(err).WithFields(logrus.Fields{
			"topic":          topicID,
			"columnIndex":    columnIndex,
			"numCommitments": len(header.KzgCommitments),
		}).Error("Failed to create partial data column from header")
		return nil, errors.Wrap(err, "new partial data column")
	}

	if !bytes.Equal(newColumn.GroupID(), rpc.GroupID) {
		p.logger.WithFields(rpc.logFields()).Error("Group ID mismatch")
		// REJECT case: penalize the peer
		p.reportPeerFeedback(topicID, rpc.from, pubsub.PeerFeedbackInvalidMessage)
		return nil, errors.New("group ID mismatch")
	}

	if headerWasCached {
		verifier, err := p.callbacks.PartialVerifierFromTrustedColumn(&newColumn)
		if err != nil {
			p.logger.WithError(err).WithFields(logrus.Fields{
				"topic":          topicID,
				"columnIndex":    columnIndex,
				"numCommitments": len(header.KzgCommitments),
			}).Error("Failed to create partial column verifier from header")
			return nil, errors.Wrap(err, "partial verifier from trusted column")
		}
		return verifier, nil
	}
	verifier, result, err := p.callbacks.PartialVerifierFromHeader(&newColumn)
	if err != nil {
		p.logger.WithError(err).WithFields(rpc.logFields()).WithField("result", result).Debug("Partial column header validation failed")
		if result == pubsub.ValidationReject {
			// REJECT case: penalize the peer
			p.reportPeerFeedback(topicID, rpc.from, pubsub.PeerFeedbackInvalidMessage)
		}
		// Both REJECT and IGNORE: don't process further
		return nil, errInvalidHeader
	}
	return verifier, nil
}

func (p *PartialColumnBroadcaster) getHeader(groupID []byte, message *ethpb.PartialDataColumnSidecar) (*ethpb.PartialDataColumnHeader, bool) {
	return p.cachedOrMessageHeader(groupID, message.Header)
}

// cachedOrMessageHeader returns the group's validated header if we have one, otherwise the
// header carried in the message. The second return says whether the header came from the
// cache, i.e. whether it has already been validated.
//
// The cache is keyed by group id alone, deliberately: the partial-columns spec says a header
// validated on any subnet may be used for all subnets, and RowDAS row topics share the block's
// column group id, so one cache serves both axes.
func (p *PartialColumnBroadcaster) cachedOrMessageHeader(groupID []byte, messageHeaders []*ethpb.PartialDataColumnHeader) (*ethpb.PartialDataColumnHeader, bool) {
	if cachedHeader, ok := p.validHeaderCache[string(groupID)]; ok {
		return cachedHeader, true
	}
	if len(messageHeaders) == 0 {
		p.logger.Debug("No partial column found and no header in message, ignoring")
		return nil, false
	}

	return messageHeaders[0], false
}

func (p *PartialColumnBroadcaster) republishColumn(ourDataColumn *blocks.PartialDataColumn, rpc incomingPartialRPC,
	shouldRepublish bool) error {
	if !ourDataColumn.Published {
		p.recordRepublishSkip(rpc.GroupID, ourDataColumn.Index())
		return nil
	}

	topicId := rpc.GetTopicID()

	peerMeta := rpc.PartsMetadata
	myMeta, err := ourDataColumn.PartsMetadata()
	if err != nil {
		return errors.Wrap(err, "parts metadata")
	}
	if !shouldRepublish && len(peerMeta) > 0 && !bytes.Equal(peerMeta, myMeta) {
		// Either we have something they don't or vice versa
		shouldRepublish = true
	}

	if shouldRepublish {
		err := p.publishPartialCol(topicId, ourDataColumn.GroupID(), ourDataColumn)
		if err != nil {
			return errors.Wrap(err, "publish partial column")
		}
	}
	return nil
}

func (p *PartialColumnBroadcaster) handlePartialCells(ourDataColumn *blocks.PartialDataColumn, message *ethpb.PartialDataColumnSidecar,
	rpc incomingPartialRPC) error {
	topicId := rpc.GetTopicID()

	cellIndices, cellsToVerify, err := ourDataColumn.CellsToVerifyFromPartialMessage(message)
	if err != nil {
		return errors.Wrap(err, "cells to verify from partial message")
	}
	// Track cells received via partial message
	if len(cellIndices) > 0 {
		columnIndexStr := strconv.FormatUint(ourDataColumn.Index(), 10)
		partialMessageCellsReceivedTotal.WithLabelValues(columnIndexStr).Add(float64(len(cellIndices)))
	}
	if len(cellsToVerify) > 0 {
		select {
		case p.concurrentValidatorSemaphore <- struct{}{}:
			go func() {
				defer func() {
					<-p.concurrentValidatorSemaphore
				}()
				start := time.Now()
				err := p.callbacks.ValidateColumn(cellsToVerify)
				if err != nil {
					p.logger.WithError(err).WithFields(rpc.logFields()).Error("Failed to validate cells")
					p.reportPeerFeedback(topicId, rpc.from, pubsub.PeerFeedbackInvalidMessage)
					return
				}
				p.reportPeerFeedback(topicId, rpc.from, pubsub.PeerFeedbackUsefulMessage)
				_, _ = p.enqueue(p.ctx, requestKindCellsValidated, requestValues{
					cellsValidated: &cellsValidated{
						validationTook: time.Since(start),
						topic:          topicId,
						group:          ourDataColumn.GroupID(),
						cells:          cellsToVerify,
						cellIndices:    cellIndices,
					},
				})
			}()
		default:
			columnIndexStr := strconv.FormatUint(ourDataColumn.Index(), 10)
			partialMessageValidationsDroppedTotal.WithLabelValues(columnIndexStr).Add(float64(len(cellsToVerify)))
			p.logger.WithFields(rpc.logFields()).Warn("Validator semaphore saturated, dropping cell validation")
		}
	}
	return nil
}

func (p *PartialColumnBroadcaster) handleHeader(rpc incomingPartialRPC, header *ethpb.PartialDataColumnHeader) {
	p.cacheAndHandleHeader(rpc.GroupID, header, rpc.logFields())
}

// cacheAndHandleHeader caches a newly validated header for its group and hands it to the
// application. It is called from either axis: whichever topic a header first arrives on, the
// cache and the downstream getBlobs path want it.
func (p *PartialColumnBroadcaster) cacheAndHandleHeader(groupID []byte, header *ethpb.PartialDataColumnHeader, logFields logrus.Fields) {
	p.validHeaderCache[string(groupID)] = header

	select {
	case p.concurrentHeaderHandlerSemaphore <- struct{}{}:
		go func() {
			p.callbacks.HandleHeader(header, string(groupID))
			<-p.concurrentHeaderHandlerSemaphore
		}()
	default:
		p.logger.WithFields(logFields).Warn("Dropping header handler, max concurrent header handlers reached")
	}
}

func (p *PartialColumnBroadcaster) handleCellsValidated(cells *cellsValidated) error {
	ourVerifier := p.getPartialVerifier(cells.topic, cells.group)
	if ourVerifier == nil {
		return errors.New("data column not found for verified cells")
	}
	ourDataColumn := ourVerifier.Column
	var extended bool
	for i, bundle := range cells.cells {
		if bundle.ColumnIndex != ourDataColumn.Index() {
			return errors.New("cell bundle has wrong column index")
		}
		if ourVerifier.ExtendFromVerifiedCell(cells.cellIndices[i], bundle.Cell, bundle.Proof) {
			extended = true
		}
	}

	if !extended {
		return nil
	}

	columnIndexStr := strconv.FormatUint(ourDataColumn.Index(), 10)
	// Track useful cells (cells that extended our data)
	partialMessageUsefulCellsTotal.WithLabelValues(columnIndexStr).Add(float64(len(cells.cells)))

	// Offer the same cells to our row before serving the column, since a cell that completes
	// our row is worth more than the order these two happen in.
	if err := p.crossFillRowFromColumn(cells.group, ourDataColumn.Index(), cells.cellIndices, cells.cells); err != nil {
		p.logger.WithError(err).WithFields(cells.logFields()).Error("Failed to cross-fill row from column cells")
	}

	if err := p.afterColumnExtended(cells.topic, cells.group, ourVerifier); err != nil {
		p.logger.WithError(err).WithFields(cells.logFields()).Error("Failed to handle extended partial column")
		return err
	}

	return nil
}

// Publish publishes partial columns for the given topics.
func (p *PartialColumnBroadcaster) Publish(ctx context.Context, topicsAndColumns iter.Seq2[string, blocks.PartialDataColumn]) error {
	if p.peerFeedback == nil || p.publishPartialCol == nil {
		return errors.New("pubsub not initialized")
	}
	req, err := p.enqueue(ctx, requestKindPublish, requestValues{
		publish: publish{
			topicsAndColumns: topicsAndColumns,
		},
	})
	if err != nil {
		return err
	}
	return req.waitForResponse()
}

func (p *PartialColumnBroadcaster) gossip(topic string, groupID []byte) {
	topicStore, ok := p.partialMsgStore[topic]
	if !ok {
		return
	}
	existing := topicStore[string(groupID)]
	if existing == nil {
		return
	}
	if existing.Column.Included.Count() == 0 {
		// Nothing useful here
		return
	}
	if !existing.Column.Published {
		return
	}
	err := p.publishPartialCol(topic, existing.Column.GroupID(), existing.Column)
	if err != nil {
		p.logger.WithFields(logrus.Fields{"err": err}).Warn("Failed to publish gossip")
	}
}

func (p *PartialColumnBroadcaster) publish(topicsAndColumns iter.Seq2[string, blocks.PartialDataColumn]) error {
	return p.publishColumns(topicsAndColumns, false)
}

// publishColumns is publish with a mode: registerOnly stores the state, arms the TTL and marks
// the topic published exactly as a publish would -- so incoming requests are served -- but
// registers with the extension instead of sending, leaving the announcement to heartbeat gossip.
// The advertisement-only cross-forward is its only registerOnly caller.
func (p *PartialColumnBroadcaster) publishColumns(topicsAndColumns iter.Seq2[string, blocks.PartialDataColumn], registerOnly bool) error {
	var aggErr error
	for topic, partialCol := range topicsAndColumns {
		if partialCol.KzgCommitmentCount() == 0 {
			p.logger.WithFields(logrus.Fields{
				"topic": topic,
			}).Debug("Skipping publish for column with no KZG commitments")
			continue
		}
		groupIDBytes := partialCol.GroupID()
		topicStore, ok := p.partialMsgStore[topic]
		if !ok {
			topicStore = make(map[string]*verification.PartialColumnVerifier)
			p.partialMsgStore[topic] = topicStore
		}
		verifier := p.getPartialVerifier(topic, groupIDBytes)
		if verifier == nil {
			var err error
			verifier, err = p.callbacks.PartialVerifierFromTrustedColumn(&partialCol)
			if err != nil {
				aggErr = stderrors.Join(aggErr, errors.Wrap(err, "partial verifier from trusted column"))
				continue
			}
			topicStore[string(groupIDBytes)] = verifier
		} else {
			if requests, ok := partialCol.PartsRequests(); ok {
				if err := verifier.Column.SetPartsRequests(requests); err != nil {
					aggErr = stderrors.Join(aggErr, errors.Wrap(err, "set parts requests"))
					continue
				}
			} else {
				verifier.Column.ClearPartsRequests()
			}
			var extended bool
			for i := range partialCol.Included.Len() {
				if partialCol.Included.BitAt(i) {
					if verifier.ExtendFromVerifiedCell(uint64(i), partialCol.Column()[i], partialCol.KzgProofs()[i]) {
						extended = true
					}
				}
			}
			if extended {
				// A column completed by this merge never reaches handleCellsValidated, so hand it to the callback here.
				col, ok, err := verifier.Complete()
				if err != nil {
					aggErr = stderrors.Join(aggErr, errors.Wrap(err, "complete partial column verifier"))
					continue
				}
				if ok {
					go p.callbacks.HandleColumn(topic, col)
				}
			}
		}
		ourColummn := verifier.Column

		// Seed our row states from this column before serving it. A cell we supplied ourselves
		// never passes through handleCellsValidated, so without this the row axis never sees the
		// columns a node holds by any route other than the column topic -- see D13.
		if err := p.crossFillRowFromWholeColumn(groupIDBytes, ourColummn); err != nil {
			aggErr = stderrors.Join(aggErr, errors.Wrap(err, "cross-fill rows from a published column"))
		}

		p.groupTTL[string(groupIDBytes)] = TTLInSlots
		// Mark the topic as locally published so incoming parts-requests on it are
		// accepted even without a subscription (see publishedTopics). Cleared when the
		// topic's last group is evicted.
		p.publishedTopics.Store(topic, struct{}{})
		if registerOnly {
			if p.registerPartialCol == nil {
				aggErr = stderrors.Join(aggErr, errors.New("register-only publish without a register hook wired"))
				continue
			}
			if err := p.registerPartialCol(topic, ourColummn.GroupID()); err != nil {
				aggErr = stderrors.Join(aggErr, errors.Wrap(err, "register partial column"))
				continue
			}
			// Published means "this state is ours to serve and announce from": the republish
			// path, the gossip handler and the cross-fill bridge all gate on it, and a registered
			// column is exactly that. Only the first wire contact differs -- heartbeat gossip
			// instead of an eager burst. Leaving it unset was R17's first dead end: the state
			// existed, and every path that could have announced or served it dropped it.
			ourColummn.Published = true
			continue
		}
		err := p.publishPartialCol(topic, ourColummn.GroupID(), ourColummn)
		if err == nil {
			ourColummn.Published = true
		} else {
			aggErr = stderrors.Join(aggErr, errors.Wrap(err, "publish partial column"))
		}
	}
	return aggErr
}

func (p *PartialColumnBroadcaster) Subscribe(ctx context.Context, t *pubsub.Topic) error {
	req, err := p.enqueue(ctx, requestKindSubscribe, requestValues{
		sub: subscribe{
			t: t,
		},
	})
	if err != nil {
		return err
	}
	return req.waitForResponse()
}

func (p *PartialColumnBroadcaster) subscribe(t *pubsub.Topic) error {
	topic := t.String()
	if _, ok := p.topics[topic]; ok {
		return errors.New("already subscribed")
	}

	p.topics[topic] = t
	p.subscribedTopics.Store(topic, struct{}{})
	return nil
}

func (p *PartialColumnBroadcaster) Unsubscribe(ctx context.Context, topic string) error {
	req, err := p.enqueue(ctx, requestKindUnsubscribe, requestValues{
		unsub: unsubscribe{
			topic: topic,
		},
	})
	if err != nil {
		return err
	}
	return req.waitForResponse()
}

func (p *PartialColumnBroadcaster) unsubscribe(topic string) error {
	if _, ok := p.topics[topic]; !ok {
		return errors.New("topic not found")
	}
	delete(p.topics, topic)
	p.subscribedTopics.Delete(topic)
	delete(p.partialMsgStore, topic)
	delete(p.rowStore, topic)
	p.publishedTopics.Delete(topic)
	return nil
}
