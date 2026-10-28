package scheduleddiffusion

// Variant S, stage 1: execution payload segments diffused against standing wants.
//
// Variant B's machinery -- one topic, per-link representation, a bitmap exchange on
// gossipsub's partial-message extension -- with the request half removed. Per (topic, group,
// peer) a node keeps what it holds, what it believes the peer holds and what the peer still
// wants; the forwarding decision is `mine ∧ wanted ∧ ¬theirs` and nothing else. There is no
// request, no claim ledger, no timeout and no IWANT: a want bitmap is a standing order, and
// it defaults to everything until the peer's first metadata arrives.
//
// The other half of the stage is where state is written. B latches optimistically -- it
// records "I sent them index i" while building the action, so a silent drop on queue
// pressure suppresses the retry forever, because an application's memory of what it told a
// peer is remembered rather than regenerated. Here every action sets OnSent(admitted) and
// the peer state is written only in that callback, so an action that never left the process
// leaves no trace and the next pass re-yields the same segment.
//
// Threading is B's. The extension calls OnIncomingRPC and OnEmitGossip on pubsub's own
// goroutine, and PublishPartial has to reach that same goroutine through ps.eval -- so
// calling it from inside a callback would deadlock. Everything that publishes therefore
// happens on this package's loop goroutine, and the callbacks do only the work that must be
// synchronous: updating the peer-state map the extension lends them.

import (
	"bytes"
	"context"
	"iter"
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

const logPackage = "beacon-chain/p2p/scheduleddiffusion"

var (
	// ErrNotStarted is returned when a publish is attempted before Start.
	ErrNotStarted = errors.New("scheduled diffusion broadcaster is not started")
	// ErrGroupMismatch is returned when a peer files segments under a group id that is not
	// the one their descriptor derives.
	ErrGroupMismatch = errors.New("segment group id does not match its descriptor")
	// ErrNoTopic is returned for an RPC on a topic we do not serve segments for.
	ErrNoTopic = errors.New("segments are not served on this topic")
)

// Config bounds the broadcaster. There is no push policy here: the policy is the want
// bitmap, and what varies between stages is when a pass runs, not what it decides.
type Config struct {
	// AnnounceWindow batches standalone metadata per peer: after an update goes out, a
	// further update that carries no segments waits until the window closes and folds into
	// one send. Zero sends every change immediately. A completion update -- one that asks
	// for nothing -- never waits, because it is what stops peers sending us duplicates,
	// and piggybacked metadata never waits because it costs nothing to carry.
	AnnounceWindow time.Duration
	// RetryInterval is how often an incomplete group re-runs its publish pass. With state
	// latched on admission a pass with nothing new to say yields nothing, so this is cheap
	// enough to be the recovery path for a dropped RPC.
	RetryInterval time.Duration
	// GroupTTL is how long a group is kept, and so how long we go on serving its segments
	// to peers after we have completed it ourselves.
	GroupTTL time.Duration
	// PushGrace is how long a push is trusted against the peer's own word. A want that still
	// names a segment we pushed more than this long ago is read as a loss, and the push is
	// forgotten so the segment is sendable again. Must cover a one-way latency plus the
	// frame's wait in our queue, or a merely queued frame is sent twice. Default one second.
	PushGrace time.Duration
	// AuthAttemptsPerPeer and AuthRefillPerSecond bound descriptor authentication, which is
	// the only expensive thing a peer can make us do. A peer can fabricate a descriptor over
	// its own Merkle tree so every cheap check passes and only the signature fails.
	AuthAttemptsPerPeer int64
	AuthRefillPerSecond float64
	// MaxGroups and MaxBytes bound reassembly buffers.
	MaxGroups int
	MaxBytes  int
	// NormalLaneUpdates sends standalone metadata updates in the ordinary lane, behind queued
	// data, instead of the urgent lane IDONTWANT already uses. Off by default: an update's
	// whole value is timeliness. Piggybacked metadata rides its data frame's lane either way,
	// since the frame is the thing being paced. On exists to price the lane.
	NormalLaneUpdates bool
	// ForwardBeyondMesh sends to every negotiated peer in the group's state map instead of
	// the router's mesh. The map grows to the whole connectivity, so without dispatch-time
	// decisions this is a flood -- sixteen copies per node at degree twenty in the first
	// cell. Off is tier 1's forwarding set and the S1 control; on is the meshless arm.
	ForwardBeyondMesh bool
	// CompressSegments snappy-compresses each segment inside the partial message, as the
	// topic encoder does for an ordinary gossip message. The partial path never passes
	// through that encoder, so without this the same segment costs more on the wire here
	// than on the ordinary path. Receivers accept both encodings, so this is a per-sender
	// choice.
	CompressSegments bool
}

// Callbacks is what the broadcaster needs from the node above it.
type Callbacks struct {
	// Auth authenticates a group's descriptor. Required.
	Auth segments.Authenticator
	// OnEnvelope receives the reassembled envelope bytes. Required.
	//
	// Reassembly proves the bytes match a builder-signed commitment; it does not run the
	// envelope gossip ladder, so the receiver must revalidate rather than treat this as
	// validated.
	OnEnvelope func(envelope []byte) error
}

// Counters are observable totals. Exported so a measurement harness can read the quantities
// the design is judged on without instrumenting libp2p.
//
// Everything on the send side is counted on admission, not on intent, for the same reason
// the state is latched there: an RPC dropped on queue pressure never reaches the wire, and
// counting it would report bytes that were not sent.
type Counters struct {
	// PublishPasses counts publish decisions handed to the extension, and PeerActions the
	// RPCs those produced. A pass with no actions means the group had no negotiated peers
	// to speak to, which is a very different failure from having nothing to send.
	PublishPasses int
	PeerActions   int
	// SegmentsSent counts segments admitted to a peer's queue, one per RPC.
	SegmentsSent int
	// MetadataSent counts admitted updates; every update travels alone.
	MetadataSent int
	// MetadataHeldBack counts updates the announce window deferred.
	MetadataHeldBack int
	// PushesExpired counts pushes forgotten because the peer still asked for the segment
	// after PushGrace: the loss-recovery path, and a duplicate when the frame was only late.
	PushesExpired int
	// SendsNotAdmitted counts actions the peer's queue refused. Each one leaves the peer
	// state untouched, so the next pass offers the same thing again.
	SendsNotAdmitted int
	MetadataReceived int
	// SegmentsReceived counts segments verified and added; SegmentsDuplicate those already
	// held when they arrived. The reassembler reports a duplicate as a successful no-op, so
	// the two are told apart by reading Held before the add.
	SegmentsReceived  int
	SegmentsDuplicate int
	SegmentsRejected  int
	GroupsCompleted   int
	AuthThrottled     int
	// VerifyQueueDropped counts batches dropped because the verify queue was full.
	VerifyQueueDropped int
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
	// ext is the extension AppendPubSubOpts installed; a pass reads its mesh through it.
	ext *partialmessages.PartialMessagesExtension[PeerState]

	mu sync.Mutex
	// pending is the set of (topic, group) pairs whose publish pass needs re-running. A set
	// rather than a queue: several triggers for one group collapse into one pass, which is
	// what keeps a burst of arriving segments from producing a burst of RPCs.
	pending map[groupKey]struct{}
	// groups is when each group was first heard of, which is all the bookkeeping a
	// requestless variant needs: it bounds the retry ticker's work and ages groups out.
	groups map[groupKey]time.Time
	// topics is the set of topics we serve segments on.
	topics map[string]struct{}
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

// New builds a Broadcaster. It does not run until Start. A nil logger uses the package one.
func New(ctx context.Context, logger *logrus.Logger, cfg Config) *Broadcaster {
	// An incomplete group re-runs its pass this often. Shorter than B's 250 ms because a
	// pass costs nothing when nothing changed: with admission latching there is no claim to
	// re-affirm and no timeout to wait out, so the interval only bounds how long a dropped
	// RPC goes unnoticed.
	if cfg.RetryInterval <= 0 {
		cfg.RetryInterval = 100 * time.Millisecond
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
	if cfg.PushGrace <= 0 {
		cfg.PushGrace = time.Second
	}
	ctx, cancel := context.WithCancel(ctx)
	return &Broadcaster{
		cfg:           cfg,
		logger:        logrus.NewEntry(logger).WithField("package", logPackage),
		now:           time.Now,
		ctx:           ctx,
		cancel:        cancel,
		pending:       make(map[groupKey]struct{}),
		groups:        make(map[groupKey]time.Time),
		topics:        make(map[string]struct{}),
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
		// Serving a peer a segment it still wants means holding that segment, and its
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

// SetSelf records our own peer id.
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
// An RPC naming any other topic is refused outright: without this the group-state surface
// is keyed by a peer-controlled topic id.
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
	ext := &partialmessages.PartialMessagesExtension[PeerState]{
		Logger:        newSlogger(b.logger),
		OnEmitGossip:  b.onEmitGossip,
		OnIncomingRPC: b.onIncomingRPC,
	}
	b.mu.Lock()
	b.ext = ext
	b.mu.Unlock()
	return append(opts,
		pubsub.WithPartialMessagesExtension(ext),
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

// meshPeers is a pass's forwarding bound: the router's mesh for topic, read inside the pass
// on the router goroutine, which is the only place the extension may be asked. Nil when the
// configuration forwards beyond the mesh, or before the extension is installed.
func (b *Broadcaster) meshPeers(topic string) func() iter.Seq[peer.ID] {
	b.mu.Lock()
	ext := b.ext
	b.mu.Unlock()
	if b.cfg.ForwardBeyondMesh || ext == nil {
		return nil
	}
	return func() iter.Seq[peer.ID] { return ext.MeshPeers(topic) }
}

// Publish makes a locally produced segmented envelope available and starts serving it.
//
// The segments are fed through the same reassembler a received group goes through, so the
// publisher holds its own group by exactly the same mechanism it will later serve from.
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
// group.
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
		if state.Theirs != nil && state.Theirs.Count() != parsed.Count {
			return segments.ErrCountMismatch
		}
		if state.Theirs == nil {
			t, err := segments.NewBitmap(parsed.Count)
			if err != nil {
				return err
			}
			state.Theirs = t
		}
		// Union, not assignment: what a peer once held it still holds, and what we have
		// admitted to its queue is ours to remember. An announce that dropped bits would
		// otherwise make us resend.
		for _, idx := range parsed.Available.Indices() {
			if err := state.Theirs.Set(idx); err != nil {
				return err
			}
		}
		// The want bitmap, on the other hand, is replaced outright: it is a standing order,
		// and the peer's latest statement of it is the whole truth.
		state.Wanted = parsed.Requests.Clone()
		// A push the peer still asks for, long after we made it, was lost -- on the wire or
		// at their verify queue -- and nothing else would ever resend it. Within the grace it
		// is merely queued, and forgetting it would send it twice.
		if state.Pushed != nil && !state.PushedAt.IsZero() && b.now().Sub(state.PushedAt) > b.cfg.PushGrace {
			for _, idx := range parsed.Requests.Indices() {
				if state.Pushed.Has(idx) && state.Pushed.Clear(idx) == nil {
					b.bump(func(c *Counters) { c.PushesExpired++ })
				}
			}
		}
		b.bump(func(c *Counters) { c.MetadataReceived++ })
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
		if state.Theirs == nil {
			t, err := segments.NewBitmap(m.Descriptor.Count)
			if err != nil {
				return err
			}
			state.Theirs = t
		}
		if state.Theirs.Count() != m.Descriptor.Count {
			return segments.ErrCountMismatch
		}
		if err := state.Theirs.Set(m.Index); err != nil {
			return err
		}
		// A segment they sent us is one they cannot still be waiting for.
		if state.Wanted != nil {
			if err := state.Wanted.Clear(m.Index); err != nil {
				return err
			}
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
// The held-back announce owns this wake-up: without it a suppressed update would wait on
// unrelated traffic that may never come.
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
	k := groupKey{topic: topic, group: string(groupID)}
	b.mu.Lock()
	b.pending[k] = struct{}{}
	if _, ok := b.groups[k]; !ok {
		b.groups[k] = b.now()
	}
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
