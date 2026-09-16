package segmentbroadcaster

// Variant B: execution payload segments carried as gossipsub partial-message parts.
//
// One topic, not two. A node joins the execution payload envelope topic with
// RequestPartialMessages, and gossipsub then chooses the representation per link: a peer that
// also requested partial messages is sent segments and is *skipped* by the ordinary
// full-message path, while a peer that did not is sent the whole envelope exactly as today.
// That is the property variant A could not have -- it needed a second topic, and a node
// subscribed to both received the payload twice.
//
// What travels per link is a bitmap and some segments. The bitmap says what we hold and what
// we want; the segments are whatever the policy decided to volunteer plus whatever was asked
// for. Because the peer's holdings are known, a segment can be sent once instead of once per
// mesh peer -- which is the whole point, since Q12 traced the binding constraint to duplicate
// reception rather than to the publisher's upload.
//
// Threading. The extension calls OnIncomingRPC and OnEmitGossip on pubsub's own goroutine,
// and PublishPartial has to reach that same goroutine through ps.eval -- so calling it from
// inside a callback would deadlock. Everything that publishes therefore happens on this
// package's loop goroutine, and the callbacks do only the work that must be synchronous:
// updating the peer-state map the extension lends them.

import (
	"bytes"
	"context"
	"sync"
	"time"

	leakybucket "github.com/OffchainLabs/prysm/v7/container/leaky-bucket"
	"github.com/OffchainLabs/prysm/v7/container/segments"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p-pubsub/partialmessages"
	pubsub_pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

const logPackage = "beacon-chain/p2p/segmentbroadcaster"

var (
	// ErrNotStarted is returned when a publish is attempted before Start.
	ErrNotStarted = errors.New("segment broadcaster is not started")
	// ErrGroupMismatch is returned when a peer files segments under a group id that is not
	// the one their descriptor derives.
	ErrGroupMismatch = errors.New("segment group id does not match its descriptor")
	// ErrNoTopic is returned for an RPC on a topic we do not serve segments for.
	ErrNoTopic = errors.New("segments are not served on this topic")
)

// Config bounds the broadcaster and selects the arm being measured.
type Config struct {
	// Policy is the push arm. See Policy.
	Policy Policy
	// AdaptiveRequestTimeout replaces the fixed per-claim expiry with a per-peer estimate:
	// SRTT + 4*RTTVAR over measured claim-to-arrival times (Jacobson/Karels with Karn's
	// exclusion of reissued claims), clamped to
	// [RequestTimeout/2, 4*RequestTimeout]. RequestTimeout remains the cold-start expiry for
	// a peer with no samples. The sample deliberately includes the peer's queueing and
	// transmission of whatever was asked of it, because that is exactly what the expiry has
	// to cover -- the fixed-timeout lesson (1.55 copies/node at 75 ms against 1.05 at
	// 400 ms) was that this is a correctness knob, and under heterogeneous latency no single
	// constant is correct for both an 8 ms neighbour and a 110 ms one.
	AdaptiveRequestTimeout bool
	// WithholdServes is the F1 failure injection (notes/measurement-plan.md section 15): the
	// node keeps announcing, receiving and pushing, but never answers a peer's wants. Test
	// harness only; a production configuration must never set it.
	WithholdServes bool
	// CompressSegments snappy-compresses each segment inside the partial message, as the
	// topic encoder does for an ordinary gossip message. The partial path never passes through
	// that encoder, so without this the same segment costs ~40% more on the wire here than on
	// the ordinary path (a systematic 32 KiB segment: ~33 KB raw, ~24 KB compressed; parity
	// shards do not compress). Receivers accept both encodings, so this is a per-sender choice.
	// Off keeps the raw wire the earlier measurements were taken on.
	CompressSegments bool
	// Replication is how many peers each segment is volunteered to under PushSplit -- the r of
	// design-space dimension 9. 1 sends one copy and leaves the rest to be pulled at a round
	// trip each; a value at or above the peer count is PushAll by another name. Defaults to 1.
	//
	// Ignored when PushDivisor is set, which replaces the ranking with a pairwise predicate.
	Replication int
	// PushDivisor turns on coordinated pushing and sets its density: a sender volunteers a
	// segment to a peer with probability 1/PushDivisor, so a receiver with in-degree P expects
	// P/PushDivisor copies and PushDivisor = P gives one.
	//
	// The point is not the density but that the rule is *pairwise*, so a receiver can evaluate
	// it for its peers and skip requesting what is already coming. Both ends must agree on the
	// value or that prediction is wrong, which is why it is a configured constant and not
	// derived from a peer count the two ends measure differently.
	//
	// Zero keeps the uncoordinated rendezvous ranking, so the two can be compared directly.
	PushDivisor int
	// PushGrace is how long after first hearing of a group a node will trust that a predicted
	// push is coming and hold off requesting it. Must exceed a round trip plus the sender's
	// transmission time, on the same reasoning as RequestTimeout. Past it, everything missing is
	// requested regardless, so a lost push cannot strand a segment.
	PushGrace time.Duration
	// RequestDefer is how long after first hearing of a *coded* group a node holds off
	// issuing new requests, so pushes already in flight can count toward its K before it
	// commits to asking. Zero -- the default -- requests immediately.
	//
	// The knob exists because the push/pull overlap is locked in by the first request
	// burst: a node asks for K - held the moment it hears of a group, and every push still
	// in flight then arrives as a duplicate. Under K-of-K deferring would trade that
	// duplication against stalling on exactly the indices the pushes do not cover; under
	// any-k-of-n the trade disappears, because whatever arrives during the deferral counts
	// and the budget refills the gap afterwards. Plain groups therefore ignore this knob.
	RequestDefer time.Duration
	// AnnounceWindow batches announce-only metadata per peer: after a metadata send, further
	// updates that carry no segments and change no requests wait until the window closes and
	// fold into one send. The first contact goes immediately (leading edge), and request
	// changes always go immediately -- their claims are already charged in the ledger.
	// Modelled on the RowDAS announce policy (their largest wire win). Zero sends every
	// change immediately.
	AnnounceWindow time.Duration
	// StrikeCap refuses fresh claims to a peer whose claims have lapsed unserved this many
	// times for the group, for StrikeTTL. The failure memory for the withholding row: an
	// announcing withholder is avoided after StrikeCap burned expiries instead of being
	// re-picked forever. Above one so a single innocent lapse (a slow honest serve) does not
	// exile a peer; time-bounded so a group whose every holder is struck can still finish.
	// Zero disables.
	StrikeCap int
	// StrikeTTL is how long a struck peer is avoided. Zero with StrikeCap set means 2 s.
	StrikeTTL time.Duration
	// ClaimPerPeer caps how many *fresh* claims one publish pass may aim at a single peer,
	// which pipelines the pull instead of mobbing the first holder: at hop one every
	// neighbour would otherwise claim the whole group from the publisher, committing
	// peers-times-payload into one uplink before anyone can re-aim. The cap also switches
	// want ordering to the per-node hash order for plain groups -- capped ascending order
	// would concentrate the whole network on the lowest indices. Zero keeps the old
	// claim-everything behaviour.
	ClaimPerPeer int
	// RequestSurplus lets a coded group keep this many claims outstanding beyond what
	// completes it, so the K-th arrival is the fastest of a surplus rather than the slowest
	// of an exact set; the extra segments arrive after completion and are the price. Zero
	// claims exactly what completes the node. Plain groups are unaffected: their target is
	// already every index.
	RequestSurplus int
	// PushChunk splits a pass's pushes into RPCs of at most this many segments, sent
	// round-robin across peers, instead of one bundle per peer. The extension delivers an
	// RPC whole, so a bundle holds a peer's first segment until its last has crossed the
	// uplink; at the publisher that is the whole burst. Zero keeps one bundle per peer.
	PushChunk int
	// RequestTimeout is how long a request for one segment is left outstanding with one
	// peer before it may be asked of another.
	//
	// It is the knob that decides how announce-then-pull degrades: too short and a slow peer
	// gets asked twice, reintroducing duplicates; too long and a lost request stalls the
	// group for that whole window. Sub-RTT values are always wrong.
	RequestTimeout time.Duration
	// RetryInterval is how often an incomplete group re-runs its publish decision, so a
	// dropped request or a peer that went quiet does not stall reassembly until the next
	// gossip heartbeat.
	RetryInterval time.Duration
	// AuthAttemptsPerPeer and AuthRefillPerSecond bound descriptor authentication, which is
	// the only expensive thing a peer can make us do. A peer can fabricate a descriptor over
	// its own Merkle tree so every cheap check passes and only the signature fails.
	AuthAttemptsPerPeer int64
	AuthRefillPerSecond float64
	// MaxGroups and MaxBytes bound reassembly buffers.
	MaxGroups int
	MaxBytes  int
	// GroupTTL is how long a group is kept, and so how long we go on serving its segments
	// to peers after we have completed it ourselves.
	GroupTTL time.Duration
}

// Callbacks is what the broadcaster needs from the node above it.
//
// Both are supplied by the sync service rather than resolved here, because both need chain
// state -- the builder registry for a pubkey, and the pending-envelope queue for delivery --
// and neither belongs in the p2p layer.
type Callbacks struct {
	// Auth authenticates a group's descriptor. Required.
	Auth segments.Authenticator
	// OnEnvelope receives the reassembled envelope bytes. Required.
	//
	// Reassembly proves the bytes match a builder-signed commitment; it does not run the
	// envelope gossip ladder, so the receiver must revalidate rather than treat this as
	// validated. That is the same contract variant A's subscriber has.
	OnEnvelope func(envelope []byte) error
}

// requestLedger records, per segment index, which peer we last asked and when.
//
// Its only job is to stop us asking every peer that advertises a segment for that segment.
// Doing so would be the natural implementation and would reintroduce, on the request path,
// exactly the D-fold duplication the variant removes on the send path.
type requestLedger struct {
	from []peer.ID
	at   []time.Time
	// reissued marks indices whose claim was ever re-aimed at a second peer. Karn's rule:
	// their eventual arrival is ambiguous and must not feed the RTT estimator.
	reissued []bool
	// struck marks indices whose current lapsed claim has already been charged as a strike,
	// so one lapse costs the peer one strike however many candidates the pass evaluates.
	struck []bool
	// strikes counts, per peer, claims that lapsed unserved, and strikeAt is the latest.
	// This is the failure memory the withholding row needs (Q73's lesson, ported): a
	// withholder announces truthfully and never serves, so without memory it keeps
	// attracting fresh claims and every victim burns a full expiry per encounter.
	strikes  map[peer.ID]int
	strikeAt map[peer.ID]time.Time
	// created bounds how long an announce-only group is tracked. Without it a group we never
	// receive a single segment of is retried forever, because the reassembler never learns of
	// it and so can never report it complete.
	created time.Time
}

// Broadcaster carries execution payload segments as partial messages.
type Broadcaster struct {
	cfg    Config
	logger *logrus.Entry
	now    func() time.Time

	ctx    context.Context
	cancel context.CancelFunc

	// reassembler is built at Start, once the authenticator is known.
	reassembler *segments.Reassembler
	authLimiter *leakybucket.Collector
	callbacks   Callbacks

	// publishPartial is installed by AppendPubSubOpts once the PubSub instance exists.
	publishPartial func(topic string, groupID []byte, fn partialmessages.PublishActionsFn[PeerState]) error

	mu sync.Mutex
	// pending is the set of (topic, group) pairs whose publish decision needs re-running.
	// A set rather than a queue: several triggers for one group collapse into one pass,
	// which is what keeps a burst of arriving segments from producing a burst of RPCs.
	pending map[groupKey]struct{}
	// ledgers tracks outstanding requests, keyed by topic and group.
	//
	// Keyed the same way as pending, deliberately. Keying it by group alone made the retry
	// pass republish a group onto every topic this node serves, which is invisible with one
	// topic and wrong the moment a fork transition brings up a second.
	ledgers map[groupKey]*requestLedger
	// topics is the set of topics we serve segments on.
	topics map[string]struct{}
	// announced is the segment count for groups a peer has told us about but that we hold
	// nothing of. Without it a node that has only ever seen an announce cannot size a bitmap
	// and so cannot ask for anything -- which makes announce-then-pull impossible.
	//
	// Two uint32 per group, and the extension already caps how many groups peers may open per
	// topic and per peer, so this is bounded by that rather than by the peer.
	announced map[groupKey]announcedShape
	// rtt is the per-peer claim-to-arrival estimator behind AdaptiveRequestTimeout.
	rtt map[peer.ID]*rttEstimate
	// announceFlush marks groups with a held-back announce whose window flush is already
	// armed, so a burst of deferrals arms one timer rather than one per suppressed send.
	announceFlush map[groupKey]bool

	// self is our own peer id, set by SetSelf once the host exists.
	self peer.ID

	// verify carries decoded-but-unverified batches from pubsub's goroutine to the loop.
	verify chan verifyWork

	wake     chan struct{}
	started  bool
	stopOnce sync.Once

	// Counters, read by tests and metrics.
	counters Counters
}

// groupKey identifies one group on one topic.
type groupKey struct {
	topic string
	group string
}

// Counters are observable totals. Exported so a measurement harness can read the quantities
// the design is judged on without instrumenting libp2p.
type Counters struct {
	SegmentsPushed int
	// SegmentsRequested counts indices asked for the first time; RequestsReissued counts
	// re-asks after a claim lapsed without the segment arriving. Together they are the retry
	// pressure on a group -- folded into one number they reported eight requests for a
	// four-segment group that was served exactly once, which reads as duplication that did
	// not happen.
	SegmentsRequested int
	RequestsReissued  int
	SegmentsServed    int
	ServesWithheld    int
	SegmentsReceived  int
	SegmentsDuplicate int
	SegmentsRejected  int
	MetadataSent      int
	MetadataReceived  int
	// PublishPasses counts publish decisions handed to the extension, and PeerActions the
	// per-peer decisions those produced. A pass with no actions means the group had no
	// negotiated peers to speak to, which is a very different failure from having nothing to
	// send, and is otherwise indistinguishable from silence.
	PublishPasses      int
	PeerActions        int
	GroupsCompleted    int
	AuthThrottled      int
	VerifyQueueDropped int
}

// New builds a Broadcaster. It does not run until Start. A nil logger uses the package one.
func New(ctx context.Context, logger *logrus.Logger, cfg Config) *Broadcaster {
	if cfg.Replication <= 0 {
		cfg.Replication = 1
	}
	if cfg.PushGrace <= 0 {
		cfg.PushGrace = 500 * time.Millisecond
	}
	if cfg.RequestTimeout <= 0 {
		cfg.RequestTimeout = 300 * time.Millisecond
	}
	if cfg.RetryInterval <= 0 {
		cfg.RetryInterval = 250 * time.Millisecond
	}
	if cfg.AuthAttemptsPerPeer <= 0 {
		cfg.AuthAttemptsPerPeer = 8
	}
	if cfg.AuthRefillPerSecond <= 0 {
		cfg.AuthRefillPerSecond = 1
	}
	if cfg.MaxGroups <= 0 {
		cfg.MaxGroups = 64
	}
	if cfg.MaxBytes <= 0 {
		cfg.MaxBytes = 64 << 20
	}
	if cfg.GroupTTL <= 0 {
		cfg.GroupTTL = 30 * time.Second
	}
	ctx, cancel := context.WithCancel(ctx)
	return &Broadcaster{
		cfg:           cfg,
		logger:        logrus.NewEntry(logger).WithField("package", logPackage),
		now:           time.Now,
		ctx:           ctx,
		cancel:        cancel,
		pending:       make(map[groupKey]struct{}),
		ledgers:       make(map[groupKey]*requestLedger),
		topics:        make(map[string]struct{}),
		announced:     make(map[groupKey]announcedShape),
		rtt:           make(map[peer.ID]*rttEstimate),
		announceFlush: make(map[groupKey]bool),
		wake:          make(chan struct{}, 1),
	}
}

// Start builds the reassembler and runs the loop. It blocks, so call it in a goroutine.
func (b *Broadcaster) Start(callbacks Callbacks) error {
	if callbacks.Auth == nil {
		return errors.New("callbacks.Auth is required")
	}
	if callbacks.OnEnvelope == nil {
		return errors.New("callbacks.OnEnvelope is required")
	}
	r, err := segments.NewReassembler(segments.ReassemblerConfig{
		Auth:      callbacks.Auth,
		MaxGroups: b.cfg.MaxGroups,
		MaxBytes:  b.cfg.MaxBytes,
		TTL:       b.cfg.GroupTTL,
		// Serving a peer a segment it asked for means still holding that segment, and its
		// proof, after we no longer need it ourselves.
		Retain: true,
	})
	if err != nil {
		return errors.Wrap(err, "could not build reassembler")
	}

	b.mu.Lock()
	b.reassembler = r
	b.callbacks = callbacks
	b.authLimiter = leakybucket.NewCollector(
		b.cfg.AuthRefillPerSecond, b.cfg.AuthAttemptsPerPeer, 10*time.Minute, true)
	b.started = true
	b.mu.Unlock()

	b.loop()
	return nil
}

// SetSelf records our own peer id, which the coordinated push predicate needs.
//
// Separate from New because the broadcaster is built before the libp2p host exists -- p2p
// assembles the pubsub options first and only then has an identity to report.
func (b *Broadcaster) SetSelf(id peer.ID) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.self = id
}

// Stop ends the loop and releases the rate limiter's pruning goroutine.
//
// Idempotent: the limiter's Free closes a channel, so a second call would panic.
func (b *Broadcaster) Stop() {
	b.stopOnce.Do(func() {
		b.cancel()
		b.mu.Lock()
		limiter := b.authLimiter
		b.authLimiter = nil
		b.mu.Unlock()
		if limiter != nil {
			limiter.Free()
		}
	})
}

// Counters returns a snapshot of the observable totals.
func (b *Broadcaster) Counters() Counters {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.counters
}

// ServeTopic marks a topic as one we exchange segments on.
//
// An RPC naming any other topic is refused outright. Without this the group-state surface is
// keyed by a peer-controlled topic id, which is the pre-admission problem recorded in
// notes/TODO.md; restricting it to topics we actually joined is the cheap half of the fix.
func (b *Broadcaster) ServeTopic(topic string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.topics[topic] = struct{}{}
}

// servesTopic reports whether we exchange segments on a topic.
func (b *Broadcaster) servesTopic(topic string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	_, ok := b.topics[topic]
	return ok
}

// AppendPubSubOpts installs the partial-messages extension and captures the PubSub handle.
func (b *Broadcaster) AppendPubSubOpts(opts []pubsub.Option) []pubsub.Option {
	return append(opts,
		pubsub.WithPartialMessagesExtension(&partialmessages.PartialMessagesExtension[PeerState]{
			Logger:        newSlogger(b.logger),
			OnEmitGossip:  b.onEmitGossip,
			OnIncomingRPC: b.onIncomingRPC,
		}),
		func(ps *pubsub.PubSub) error {
			b.mu.Lock()
			defer b.mu.Unlock()
			b.publishPartial = func(topic string, groupID []byte, fn partialmessages.PublishActionsFn[PeerState]) error {
				return pubsub.PublishPartial(ps, topic, groupID, fn)
			}
			return nil
		},
	)
}

// Publish makes a locally produced segmented envelope available and starts serving it.
//
// The segments are fed through the same reassembler a received group goes through, so the
// publisher holds its own group by exactly the same mechanism it will later serve from --
// there is no separate origin path to diverge.
func (b *Broadcaster) Publish(topic string, msgs []*segments.SegmentMessage) error {
	if len(msgs) == 0 {
		return nil
	}
	b.mu.Lock()
	r, started := b.reassembler, b.started
	b.mu.Unlock()
	if !started || r == nil {
		return ErrNotStarted
	}
	h, err := segments.HasherByID(msgs[0].Descriptor.HashID)
	if err != nil {
		return err
	}
	groupID := msgs[0].Descriptor.GroupID(h)
	for _, m := range msgs {
		if _, err := r.Add(h, m); err != nil {
			return errors.Wrap(err, "could not buffer own segment")
		}
	}
	b.ServeTopic(topic)
	b.markPending(topic, groupID)
	return nil
}

// onEmitGossip runs on pubsub's goroutine when peers outside the mesh should hear about a
// group. Metadata only: the announce that makes announce-then-pull possible at all.
//
// It cannot publish here -- that would re-enter pubsub's eval loop -- so it only marks the
// group and lets the loop do the work.
func (b *Broadcaster) onEmitGossip(topic string, groupID []byte, _ []peer.ID, _ map[peer.ID]PeerState) {
	if !b.servesTopic(topic) {
		return
	}
	b.markPending(topic, groupID)
}

// onIncomingRPC runs on pubsub's goroutine for every partial-message RPC.
//
// Only the peer-state update happens here, because the map is the extension's and is valid
// only for the duration of the call. Segment verification, which hashes and may verify a
// signature, is handed to the loop.
func (b *Broadcaster) onIncomingRPC(from peer.ID, peerStates map[peer.ID]PeerState, rpc *pubsub_pb.PartialMessagesExtension) error {
	if rpc == nil {
		return nil
	}
	topic := rpc.GetTopicID()
	if !b.servesTopic(topic) {
		// Not an error we attribute to the peer: it may simply be a topic we have since
		// unsubscribed from.
		return nil
	}

	state := peerStates[from]
	var wake bool
	if meta := rpc.GetPartsMetadata(); len(meta) > 0 {
		parsed, err := segments.UnmarshalPartsMetadata(meta)
		if err != nil {
			return errors.Wrap(err, "malformed parts metadata")
		}
		// A peer changing the group's segment count is contradicting itself. Refuse rather
		// than resize, so the count a group is opened with is the count it keeps.
		if state.Recvd != nil && state.Recvd.Count != parsed.Count {
			return segments.ErrCountMismatch
		}
		state.Recvd = parsed
		b.noteAnnounced(topic, rpc.GroupID, parsed.Count, parsed.Required)
		b.bump(func(c *Counters) { c.MetadataReceived++ })
		// Metadata is the announce half of announce-then-pull: the peer has either offered
		// something we may want or asked for something we may hold, and either way the group
		// needs a fresh publish decision. Omitting this wake is what makes a pure announce
		// arm sit silent forever.
		wake = true
	}

	if msg := rpc.GetPartialMessage(); len(msg) > 0 {
		// A peer that sent us segments demonstrably holds them, whatever its bitmap said.
		if err := b.enqueueSegments(from, topic, rpc.GroupID, msg, &state); err != nil {
			return err
		}
	}

	peerStates[from] = state
	if wake {
		b.markPending(topic, rpc.GroupID)
	}
	return nil
}

// rttEstimate is one peer's claim-to-arrival estimator, Jacobson/Karels style.
type rttEstimate struct {
	srtt   time.Duration
	rttvar time.Duration
}

// observeRTT feeds one measured claim-to-arrival sample for a peer.
func (b *Broadcaster) observeRTT(p peer.ID, sample time.Duration) {
	if sample <= 0 {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	e, ok := b.rtt[p]
	if !ok {
		b.rtt[p] = &rttEstimate{srtt: sample, rttvar: sample / 2}
		return
	}
	// Standard EWMA constants: alpha 1/8, beta 1/4.
	diff := e.srtt - sample
	if diff < 0 {
		diff = -diff
	}
	e.rttvar += (diff - e.rttvar) / 4
	e.srtt += (sample - e.srtt) / 8
}

// claimTimeoutLocked returns the expiry for a claim on peer p. Callers hold b.mu.
func (b *Broadcaster) claimTimeoutLocked(p peer.ID) time.Duration {
	if !b.cfg.AdaptiveRequestTimeout {
		return b.cfg.RequestTimeout
	}
	e, ok := b.rtt[p]
	if !ok {
		return b.cfg.RequestTimeout
	}
	rto := e.srtt + 4*e.rttvar
	// v2 floor: half the configured timeout rather than an eighth. v1's low floor let
	// fast-neighbour samples drag far-pair expiries below their service time, and the
	// reissue storm that followed was the measured failure (Q19).
	if lo := b.cfg.RequestTimeout / 2; rto < lo {
		rto = lo
	}
	if hi := 4 * b.cfg.RequestTimeout; rto > hi {
		rto = hi
	}
	return rto
}

// announcedShape is what an announce tells us about a group we hold nothing of: how many
// segments it has, and how many of them reassemble it.
type announcedShape struct {
	count    uint32
	required uint32
}

// noteAnnounced records the announced shape for a group we may hold nothing of.
//
// Recorded only while we hold nothing: once a segment arrives the reassembler is the
// authority on the shape, and it pins the descriptor rather than trusting a bitmap.
func (b *Broadcaster) noteAnnounced(topic string, groupID []byte, count, required uint32) {
	if count == 0 || count > segments.MaxSegments || required == 0 || required > count {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	k := groupKey{topic: topic, group: string(groupID)}
	if _, ok := b.announced[k]; !ok {
		b.announced[k] = announcedShape{count: count, required: required}
	}
}

// announcedCount returns the count and required count a peer told us a group has.
func (b *Broadcaster) announcedCount(k groupKey) (count, required uint32, ok bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	c, ok := b.announced[k]
	return c.count, c.required, ok
}

// forgetAnnounced drops the remembered count, once the reassembler knows better.
func (b *Broadcaster) forgetAnnounced(k groupKey) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.announced, k)
}

// enqueueSegments decodes a partial message, records the sender's holdings, and hands the
// segments to the loop.
//
// Decoding is bounded and cheap -- lengths are checked before allocation -- but verification
// is not done here: it hashes every segment and may verify a signature, and this call is on
// pubsub's critical path.
func (b *Broadcaster) enqueueSegments(from peer.ID, topic string, groupID, encoded []byte, state *PeerState) error {
	msgs, hashers, err := segments.UnmarshalPartialMessage(encoded)
	if err != nil {
		b.bump(func(c *Counters) { c.SegmentsRejected++ })
		return errors.Wrap(err, "malformed partial message")
	}
	for i, m := range msgs {
		// The group id is the peer's to choose, so it has to be checked against the
		// descriptor it claims to be for. Otherwise segments can be filed under any group.
		if !bytes.Equal(groupID, m.Descriptor.GroupID(hashers[i])) {
			b.bump(func(c *Counters) { c.SegmentsRejected++ })
			return ErrGroupMismatch
		}
		if state.Recvd == nil {
			meta, err := segments.NewPartsMetadata(m.Descriptor.Count)
			if err != nil {
				return err
			}
			state.Recvd = meta
		}
		if state.Recvd.Count != m.Descriptor.Count {
			return segments.ErrCountMismatch
		}
		if err := state.Recvd.Available.Set(m.Index); err != nil {
			return err
		}
		// The peer demonstrably holds it, so it cannot still be waiting on us for it.
		if err := state.Recvd.Requests.Clear(m.Index); err != nil {
			return err
		}
	}
	b.enqueueVerify(verifyWork{from: from, topic: topic, groupID: groupID, msgs: msgs, hashers: hashers})
	return nil
}

// verifyWork is a batch of decoded but unverified segments from one peer.
type verifyWork struct {
	from    peer.ID
	topic   string
	groupID []byte
	msgs    []*segments.SegmentMessage
	hashers []segments.Hasher
}

// armAnnounceFlush schedules one pass for a group once its announce window closes.
//
// The held-back announce owns this wake-up: without it a suppressed final update -- "I now
// hold everything" -- would wait on unrelated traffic that may never come.
func (b *Broadcaster) armAnnounceFlush(k groupKey) {
	b.mu.Lock()
	if b.announceFlush[k] {
		b.mu.Unlock()
		return
	}
	b.announceFlush[k] = true
	window := b.cfg.AnnounceWindow
	b.mu.Unlock()
	time.AfterFunc(window, func() {
		b.mu.Lock()
		delete(b.announceFlush, k)
		b.mu.Unlock()
		b.markPending(k.topic, []byte(k.group))
	})
}

// markPending schedules a publish pass for a group.
func (b *Broadcaster) markPending(topic string, groupID []byte) {
	b.mu.Lock()
	b.pending[groupKey{topic: topic, group: string(groupID)}] = struct{}{}
	b.mu.Unlock()
	b.signal()
}

// signal nudges the loop without blocking if it is already awake.
func (b *Broadcaster) signal() {
	select {
	case b.wake <- struct{}{}:
	default:
	}
}

// bump applies a change to the counters under the lock.
func (b *Broadcaster) bump(f func(*Counters)) {
	b.mu.Lock()
	f(&b.counters)
	b.mu.Unlock()
}
