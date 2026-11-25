package sync

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	mock "github.com/OffchainLabs/prysm/v7/beacon-chain/blockchain/testing"
	"github.com/OffchainLabs/prysm/v7/beacon-chain/core/feed"
	opfeed "github.com/OffchainLabs/prysm/v7/beacon-chain/core/feed/operation"
	"github.com/OffchainLabs/prysm/v7/beacon-chain/p2p"
	p2ptest "github.com/OffchainLabs/prysm/v7/beacon-chain/p2p/testing"
	"github.com/OffchainLabs/prysm/v7/beacon-chain/startup"
	"github.com/OffchainLabs/prysm/v7/beacon-chain/verification/segmentauth"
	lruwrpr "github.com/OffchainLabs/prysm/v7/cache/lru"
	"github.com/OffchainLabs/prysm/v7/config/features"
	fieldparams "github.com/OffchainLabs/prysm/v7/config/fieldparams"
	"github.com/OffchainLabs/prysm/v7/config/params"
	"github.com/OffchainLabs/prysm/v7/consensus-types/blocks"
	"github.com/OffchainLabs/prysm/v7/consensus-types/interfaces"
	"github.com/OffchainLabs/prysm/v7/consensus-types/primitives"
	"github.com/OffchainLabs/prysm/v7/container/segments"
	"github.com/OffchainLabs/prysm/v7/encoding/bytesutil"
	ethpb "github.com/OffchainLabs/prysm/v7/proto/prysm/v1alpha1"
	"github.com/OffchainLabs/prysm/v7/testing/require"
	"github.com/OffchainLabs/prysm/v7/testing/util"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"google.golang.org/protobuf/proto"
)

// segmentNode is a node on the segment path with the envelope tests' chain: a DB, a chain mock and
// a reassembler with the node's authenticator, whose clock stands in the given slot.
type segmentNode struct {
	s     *Service
	p     p2p.P2P
	chain *mock.ChainService
}

func newSegmentNode(t *testing.T, currentSlot primitives.Slot, v mockExecutionPayloadEnvelopeVerifier) *segmentNode {
	t.Helper()
	s, _, _, _ := newEnvelopeServiceForTest(t, 1, 1, true)
	s.ctx = t.Context()
	s.badBlockCache = lruwrpr.New(10)
	s.chainStarted = &atomic.Bool{}
	chain, ok := s.cfg.chain.(*mock.ChainService)
	require.Equal(t, true, ok)
	// Fork choice's slots, per root: block adds each block's, and a root not added is unknown.
	chain.BlockSlots = map[[32]byte]primitives.Slot{}
	slot := params.BeaconConfig().SlotDuration()
	genesis := time.Now().Add(-(time.Duration(currentSlot)*slot + slot/2))
	s.cfg.clock = startup.NewClock(genesis, chain.ValidatorsRoot)
	s.newExecutionPayloadEnvelopeVerifier = testNewExecutionPayloadEnvelopeVerifier(v)
	r, err := segments.NewReassembler(segments.ReassemblerConfig{Auth: s.segmentAuthenticator()})
	require.NoError(t, err)
	s.segmentReassembler = r
	return &segmentNode{s: s, p: s.cfg.p2p, chain: chain}
}

// block saves a block of slot to the node's DB, with its bid and its builder's key, and returns
// the block's envelope and root.
func (n *segmentNode) block(t *testing.T, slot primitives.Slot) (*ethpb.SignedExecutionPayloadEnvelope, [32]byte) {
	t.Helper()
	bid := util.GenerateTestSignedExecutionPayloadBid(slot)
	sb := util.NewBeaconBlockGloas()
	sb.Block.Slot = slot
	sb.Block.Body.SignedExecutionPayloadBid = bid
	signed, err := blocks.NewSignedBeaconBlock(sb)
	require.NoError(t, err)
	root, err := signed.Block().HashTreeRoot()
	require.NoError(t, err)
	require.NoError(t, n.s.cfg.beaconDB.SaveBlock(t.Context(), signed))
	n.chain.BuilderPubkeys[root] = &[fieldparams.BLSPubkeyLength]byte{0x01}
	n.chain.BlockSlots[root] = slot
	env := testSignedExecutionPayloadEnvelope(t, slot, primitives.BuilderIndex(bid.Message.BuilderIndex), root,
		bytesutil.ToBytes32(bid.Message.BlockHash))
	return env, root
}

// verdict is the segment validator's result for one segment.
type verdict struct {
	res pubsub.ValidationResult
	err error
}

// deliver hands the node every segment of env's group through the segment validator, waits for the
// work the validator starts, and returns the validator's verdicts in order. The subscriber is left
// out: the router may drop a validated message before it reaches the subscriber, so what reaches the
// chain must go by the validator alone.
func (n *segmentNode) deliver(t *testing.T, env proto.Message) []verdict {
	t.Helper()
	return n.deliverAt(t, env, n.s.currentForkDigest())
}

// deliverAt is deliver on the segment topic at the given fork digest.
func (n *segmentNode) deliverAt(t *testing.T, env proto.Message, digest [4]byte) []verdict {
	t.Helper()
	msgs, err := segmentauth.SegmentMessagesForEnvelope(env, segmentauth.DefaultParams())
	require.NoError(t, err)
	var out []verdict
	for _, m := range msgs {
		msg := segmentToPubsubAt(t, n.s, n.p, m, digest)
		res, err := n.s.validateExecutionPayloadSegment(t.Context(), peer.ID("sender"), msg)
		if res == pubsub.ValidationAccept {
			require.NoError(t, err)
		}
		out = append(out, verdict{res: res, err: err})
	}
	n.s.segmentWork.Wait()
	return out
}

// imported is how many times the chain imported env.
func (n *segmentNode) imported(env proto.Message) int {
	c := 0
	for _, got := range n.chain.PayloadEnvelopesReceived {
		if proto.Equal(got.Proto(), env) {
			c++
		}
	}
	return c
}

// queued is the pending queue's entry for env's root and builder, or nil.
func (n *segmentNode) queued(env *ethpb.SignedExecutionPayloadEnvelope) *ethpb.SignedExecutionPayloadEnvelope {
	n.s.pendingEnvelopeLock.RLock()
	defer n.s.pendingEnvelopeLock.RUnlock()
	e, ok := n.s.pendingPayloadEnvelopes[bytesutil.ToBytes32(env.Message.BeaconBlockRoot)][uint64(env.Message.BuilderIndex)]
	if !ok || e == nil {
		return nil
	}
	signed, _ := e.Proto().(*ethpb.SignedExecutionPayloadEnvelope)
	return signed
}

// roEnvelope wraps env in the read-only interface the pending queue holds. Two wrappings of one
// message are the same entry to the queue's marks, which compare the message, not the wrapper.
func roEnvelope(t *testing.T, env *ethpb.SignedExecutionPayloadEnvelope) interfaces.ROSignedExecutionPayloadEnvelope {
	t.Helper()
	e, err := blocks.WrappedROSignedExecutionPayloadEnvelope(env)
	require.NoError(t, err)
	return e
}

func histogramCount(t *testing.T, h prometheus.Histogram) uint64 {
	t.Helper()
	m := &dto.Metric{}
	require.NoError(t, h.Write(m))
	return m.GetHistogram().GetSampleCount()
}

func counterValue(t *testing.T, c prometheus.Counter) float64 {
	t.Helper()
	m := &dto.Metric{}
	require.NoError(t, c.Write(m))
	return m.GetCounter().GetValue()
}

// gossipEvents collects the operation feed's envelope gossip events.
func gossipEvents(t *testing.T, s *Service) func() []*opfeed.ExecutionPayloadGossipReceivedData {
	t.Helper()
	ch := make(chan *feed.Event, 8)
	sub := s.cfg.operationNotifier.OperationFeed().Subscribe(ch)
	t.Cleanup(sub.Unsubscribe)
	return func() []*opfeed.ExecutionPayloadGossipReceivedData {
		var got []*opfeed.ExecutionPayloadGossipReceivedData
		for {
			select {
			case e := <-ch:
				if d, ok := e.Data.(*opfeed.ExecutionPayloadGossipReceivedData); ok && e.Type == opfeed.ExecutionPayloadGossipReceived {
					got = append(got, d)
				}
			case <-time.After(50 * time.Millisecond):
				return got
			}
		}
	}
}

// TestSegmentEnvelopeAdmission: a reassembled envelope gets what develop gives a whole envelope that came by gossip, branch by branch, and is
// imported without waiting for the pending queue's sweep.
func TestSegmentEnvelopeAdmission(t *testing.T) {
	params.SetupTestConfigCleanup(t)
	notSeen := errors.New("block not seen")
	badSig := errors.New("bad signature")

	for _, tc := range []struct {
		name string
		// current is the node's slot; the envelope is of slot 1.
		current primitives.Slot
		known   bool
	}{
		{name: "block known: imported as the group completes", current: 1, known: true},
		{name: "block known, the group a slot late: imported as the group completes", current: 2, known: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			n := newSegmentNode(t, tc.current, mockExecutionPayloadEnvelopeVerifier{})
			env, root := n.block(t, 1)
			events := gossipEvents(t, n.s)
			arrivals := histogramCount(t, segmentEnvelopeArrivalDelaySeconds)
			n.deliver(t, env)
			require.Equal(t, 1, n.imported(env), "the envelope was not imported when its group completed")
			require.Equal(t, true, n.s.hasSeenPayloadEnvelope(root, env.Message.BuilderIndex))
			require.Equal(t, arrivals+1, histogramCount(t, segmentEnvelopeArrivalDelaySeconds), "arrival not recorded")
			got := events()
			require.Equal(t, 1, len(got), "gossip events")
			require.Equal(t, root, got[0].BlockRoot)
			require.Equal(t, primitives.Slot(1), got[0].Slot)
			require.Equal(t, true, n.queued(env) == nil, "the imported envelope is also queued")
		})
	}

	t.Run("block unknown: queued after its checks, imported when the block arrives", func(t *testing.T) {
		n := newSegmentNode(t, 1, mockExecutionPayloadEnvelopeVerifier{errBlockRootSeen: notSeen})
		env, root := n.block(t, 1)
		n.chain.ForkchoiceRoots = map[[32]byte]bool{}
		arrivals := histogramCount(t, segmentEnvelopeArrivalDelaySeconds)
		n.deliver(t, env)
		require.Equal(t, 0, n.imported(env))
		require.Equal(t, true, proto.Equal(env, n.queued(env)), "not queued")
		require.Equal(t, arrivals+1, histogramCount(t, segmentEnvelopeArrivalDelaySeconds), "arrival not recorded")
		// What the block's subscriber does when the block arrives.
		n.s.processPendingPayloadEnvelope(t.Context(), root)
		require.Equal(t, 1, n.imported(env))
	})

	t.Run("block in forkchoice by the time it is queued: imported at once", func(t *testing.T) {
		n := newSegmentNode(t, 1, mockExecutionPayloadEnvelopeVerifier{errBlockRootSeen: notSeen})
		env, root := n.block(t, 1)
		n.chain.ForkchoiceRoots = map[[32]byte]bool{root: true}
		done := make(chan struct{}, 1)
		n.chain.ReceivePayloadEnvelopeFn = func(interfaces.ROSignedExecutionPayloadEnvelope) error {
			done <- struct{}{}
			return nil
		}
		n.deliver(t, env)
		select {
		case <-done:
		case <-time.After(100 * time.Millisecond):
			t.Fatal("not imported within 100 ms of its group's completion")
		}
	})

	t.Run("a bad signature is refused with the block known", func(t *testing.T) {
		n := newSegmentNode(t, 1, mockExecutionPayloadEnvelopeVerifier{errSignature: badSig})
		env, root := n.block(t, 1)
		n.deliver(t, env)
		require.Equal(t, 0, n.imported(env))
		require.Equal(t, false, n.s.hasSeenPayloadEnvelope(root, env.Message.BuilderIndex))
		require.Equal(t, true, n.queued(env) == nil)
	})
	t.Run("a bad signature is refused with the block unknown, and not queued", func(t *testing.T) {
		n := newSegmentNode(t, 1, mockExecutionPayloadEnvelopeVerifier{errBlockRootSeen: notSeen, errSignature: badSig})
		env, _ := n.block(t, 1)
		n.chain.ForkchoiceRoots = map[[32]byte]bool{}
		n.deliver(t, env)
		require.Equal(t, 0, n.imported(env))
		require.Equal(t, true, n.queued(env) == nil, "queued with a bad signature")
	})

	t.Run("a past slot is refused with the block unknown", func(t *testing.T) {
		n := newSegmentNode(t, 2, mockExecutionPayloadEnvelopeVerifier{errBlockRootSeen: notSeen})
		env, _ := n.block(t, 1)
		n.chain.ForkchoiceRoots = map[[32]byte]bool{}
		n.deliver(t, env)
		require.Equal(t, 0, n.imported(env))
		require.Equal(t, true, n.queued(env) == nil, "a past slot's envelope queued with its block unknown")
	})

	// An envelope fetched by root is queued unverified; a verified one for the same root and builder
	// must not be declined behind it.
	invalid := func(env *ethpb.SignedExecutionPayloadEnvelope) *ethpb.SignedExecutionPayloadEnvelope {
		bad, ok := proto.Clone(env).(*ethpb.SignedExecutionPayloadEnvelope)
		require.Equal(t, true, ok)
		bad.Signature[0] ^= 0xff
		return bad
	}
	refuse := func(bad *ethpb.SignedExecutionPayloadEnvelope) func(interfaces.ROSignedExecutionPayloadEnvelope) error {
		return func(e interfaces.ROSignedExecutionPayloadEnvelope) error {
			if proto.Equal(e.Proto(), bad) {
				return badSig
			}
			return nil
		}
	}
	t.Run("behind an invalid fetched entry, the block arriving after", func(t *testing.T) {
		n := newSegmentNode(t, 1, mockExecutionPayloadEnvelopeVerifier{errBlockRootSeen: notSeen})
		env, root := n.block(t, 1)
		n.chain.ForkchoiceRoots = map[[32]byte]bool{}
		bad := invalid(env)
		n.chain.ReceivePayloadEnvelopeFn = refuse(bad)
		n.s.queuePendingPayloadEnvelopeFromRootRequest(roEnvelope(t, bad))
		n.deliver(t, env)
		require.Equal(t, true, proto.Equal(env, n.queued(env)), "the verified envelope did not replace the fetched one")
		n.s.processPendingPayloadEnvelope(t.Context(), root)
		require.Equal(t, 1, n.imported(env))
		require.Equal(t, 1, len(n.chain.PayloadEnvelopesReceived))
	})
	t.Run("behind an invalid fetched entry, the block arriving before", func(t *testing.T) {
		n := newSegmentNode(t, 1, mockExecutionPayloadEnvelopeVerifier{})
		env, root := n.block(t, 1)
		bad := invalid(env)
		n.chain.ReceivePayloadEnvelopeFn = refuse(bad)
		n.s.queuePendingPayloadEnvelopeFromRootRequest(roEnvelope(t, bad))
		// The block arrives: the fetched entry is tried and refused.
		n.s.processPendingPayloadEnvelope(t.Context(), root)
		require.Equal(t, 0, len(n.chain.PayloadEnvelopesReceived))
		n.deliver(t, env)
		require.Equal(t, 1, n.imported(env), "not imported when its group completed")
		require.Equal(t, 1, len(n.chain.PayloadEnvelopesReceived))
	})
	t.Run("behind a fetched entry of another slot", func(t *testing.T) {
		n := newSegmentNode(t, 1, mockExecutionPayloadEnvelopeVerifier{errBlockRootSeen: notSeen})
		env, _ := n.block(t, 1)
		n.chain.ForkchoiceRoots = map[[32]byte]bool{}
		other, ok := proto.Clone(env).(*ethpb.SignedExecutionPayloadEnvelope)
		require.Equal(t, true, ok)
		other.Message.Payload.SlotNumber = 0
		// Under another builder, so the verified envelope's own entry doesn't overwrite it.
		other.Message.BuilderIndex = env.Message.BuilderIndex + 1
		n.s.queuePendingPayloadEnvelopeFromRootRequest(roEnvelope(t, other))
		require.Equal(t, true, proto.Equal(other, n.queued(other)), "the fetched entry was not queued")
		n.deliver(t, env)
		require.Equal(t, true, proto.Equal(env, n.queued(env)), "the verified envelope was not queued")
		require.Equal(t, true, n.queued(other) == nil, "the other slot's entry stayed")
	})
	t.Run("behind full maps", func(t *testing.T) {
		n := newSegmentNode(t, 1, mockExecutionPayloadEnvelopeVerifier{errBlockRootSeen: notSeen})
		env, root := n.block(t, 1)
		n.chain.ForkchoiceRoots = map[[32]byte]bool{}
		// Two other builders' entries for the root, fetched by root unverified, and every other root
		// place taken. (A root full of checked entries takes no more: TestSegmentEnvelopeBesideVerifiedEntries.)
		n.s.pendingEnvelopeLock.Lock()
		inner := map[uint64]interfaces.ROSignedExecutionPayloadEnvelope{}
		for b := uint64(1); b <= maxPendingBuildersPerRoot; b++ {
			e, ok := proto.Clone(env).(*ethpb.SignedExecutionPayloadEnvelope)
			require.Equal(t, true, ok)
			e.Message.BuilderIndex = primitives.BuilderIndex(uint64(env.Message.BuilderIndex) + b)
			inner[uint64(e.Message.BuilderIndex)] = roEnvelope(t, e)
			n.s.markFetchedEnvelope(root, uint64(e.Message.BuilderIndex), roEnvelope(t, e))
		}
		n.s.pendingPayloadEnvelopes[root] = inner
		for i := 1; len(n.s.pendingPayloadEnvelopes) < maxPendingPayloadRoots; i++ {
			n.s.pendingPayloadEnvelopes[[32]byte{0xee, byte(i)}] = map[uint64]interfaces.ROSignedExecutionPayloadEnvelope{}
		}
		n.s.pendingEnvelopeLock.Unlock()
		n.deliver(t, env)
		require.Equal(t, true, proto.Equal(env, n.queued(env)), "the verified envelope was held back by the caps")
	})
	t.Run("an identical fetched copy as the block enters forkchoice: imported at once", func(t *testing.T) {
		n := newSegmentNode(t, 1, mockExecutionPayloadEnvelopeVerifier{errBlockRootSeen: notSeen})
		env, root := n.block(t, 1)
		n.chain.ForkchoiceRoots = map[[32]byte]bool{}
		fetched, ok := proto.Clone(env).(*ethpb.SignedExecutionPayloadEnvelope)
		require.Equal(t, true, ok)
		n.s.queuePendingPayloadEnvelopeFromRootRequest(roEnvelope(t, fetched))
		n.chain.ForkchoiceRoots = map[[32]byte]bool{root: true}
		n.deliver(t, env)
		require.Equal(t, 1, n.imported(env), "not imported when its group completed")
	})
	t.Run("block unknown: the sender of the last segment is not asked to answer for the block", func(t *testing.T) {
		n := newSegmentNode(t, 1, mockExecutionPayloadEnvelopeVerifier{errBlockRootSeen: notSeen})
		// A block this node has nowhere, so a request for it goes unserved.
		env := testSignedExecutionPayloadEnvelope(t, 1, 1, [32]byte{0x44}, [32]byte{0x45})
		n.chain.ForkchoiceRoots = map[[32]byte]bool{}
		tp, ok := n.p.(*p2ptest.TestP2P)
		require.Equal(t, true, ok)
		asked := blocksByRootProbe(t, tp, n.s.cfg.clock)
		n.deliver(t, env)
		require.Equal(t, true, proto.Equal(env, n.queued(env)), "not queued")
		select {
		case <-asked:
		case <-time.After(time.Second):
			t.Fatal("no best peer was asked for the block")
		}
		for range 10 {
			time.Sleep(50 * time.Millisecond)
			c, _ := n.p.Peers().Scorers().BadResponsesScorer().Count(peer.ID("sender"))
			require.Equal(t, true, c <= 0, "the sender was downscored for a block it was never sent for")
		}
	})
	for _, only := range []bool{false, true} {
		t.Run(fmt.Sprintf("block known, payload-only %v: the whole envelope relayed only without payload-only", only), func(t *testing.T) {
			reset := features.InitWithReset(&features.Flags{EnableSegmentedPayloadGossip: true, SeacastPayloadOnly: only})
			defer reset()
			n := newSegmentNode(t, 1, mockExecutionPayloadEnvelopeVerifier{})
			env, _ := n.block(t, 1)
			tp, ok := n.p.(*p2ptest.TestP2P)
			require.Equal(t, true, ok)
			rec := &broadcastRecorder{TestP2P: tp}
			n.s.cfg.p2p = rec
			n.deliver(t, env)
			require.Equal(t, 1, n.imported(env))
			got := rec.messages()
			if only {
				require.Equal(t, 0, len(got), "relayed with payload-only")
				return
			}
			require.Equal(t, 1, len(got), "not relayed once")
			require.Equal(t, true, proto.Equal(env, got[0]), "relayed something other than the envelope")
		})
	}
}

// broadcastRecorder records the messages the node broadcasts.
type broadcastRecorder struct {
	*p2ptest.TestP2P
	mu  sync.Mutex
	got []proto.Message
}

func (r *broadcastRecorder) Broadcast(ctx context.Context, m proto.Message) error {
	r.mu.Lock()
	r.got = append(r.got, m)
	r.mu.Unlock()
	return r.TestP2P.Broadcast(ctx, m)
}

func (r *broadcastRecorder) messages() []proto.Message {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]proto.Message(nil), r.got...)
}

// movableClock gives the node a clock that stands in a slot the test sets, from the node's current
// one, and a reassembler that reads it.
func (n *segmentNode) movableClock(t *testing.T) func(primitives.Slot) {
	t.Helper()
	genesis := n.s.cfg.clock.GenesisTime()
	slot := params.BeaconConfig().SlotDuration()
	var at atomic.Uint64
	at.Store(uint64(n.s.cfg.clock.CurrentSlot()))
	n.s.cfg.clock = startup.NewClock(genesis, n.chain.ValidatorsRoot, startup.WithNower(func() time.Time {
		return genesis.Add(time.Duration(at.Load())*slot + slot/2)
	}))
	r, err := segments.NewReassembler(segments.ReassemblerConfig{Auth: n.s.segmentAuthenticator()})
	require.NoError(t, err)
	n.s.segmentReassembler = r
	return func(s primitives.Slot) { at.Store(uint64(s)) }
}

// gossiped queues env as develop's gossip path does with the block unknown: in its own slot, with
// its signature checked.
func (n *segmentNode) gossiped(t *testing.T, env *ethpb.SignedExecutionPayloadEnvelope) {
	t.Helper()
	ro, err := blocks.WrappedROSignedExecutionPayloadEnvelope(env)
	require.NoError(t, err)
	e, err := ro.Envelope()
	require.NoError(t, err)
	res, err := n.s.queuePendingPayloadEnvelope(t.Context(), &mockExecutionPayloadEnvelopeVerifier{}, e, ro)
	require.NoError(t, err)
	require.Equal(t, pubsub.ValidationIgnore, res)
	require.Equal(t, true, proto.Equal(env, n.queued(env)), "the gossip entry was not queued")
}

// TestSegmentEnvelopeBesideVerifiedEntries: only an entry fetched unverified gives way to a
// reassembled envelope. A pending entry whose signature was checked on gossip stays: of the same
// builder, the first one stays, as on develop's gossip path; of another builder, the reassembled
// envelope is queued beside it. While the block is unknown, neither signature fixes the block's slot
// or builder, so when the block arrives, the chain imports the one the block names and refuses the
// other.
func TestSegmentEnvelopeBesideVerifiedEntries(t *testing.T) {
	params.SetupTestConfigCleanup(t)
	notSeen := errors.New("block not seen")
	badBid := errors.New("not the block's bid")
	// The node stands in slot 1, with the block of slot 1 unknown to it. right is the block's
	// envelope, queued from gossip.
	setup := func(t *testing.T) (*segmentNode, func(primitives.Slot), *ethpb.SignedExecutionPayloadEnvelope, [32]byte) {
		n := newSegmentNode(t, 1, mockExecutionPayloadEnvelopeVerifier{errBlockRootSeen: notSeen})
		move := n.movableClock(t)
		right, root := n.block(t, 1)
		n.chain.ForkchoiceRoots = map[[32]byte]bool{}
		delete(n.chain.BlockSlots, root) // unknown to fork choice, so its slot too
		n.gossiped(t, right)
		n.chain.ReceivePayloadEnvelopeFn = func(e interfaces.ROSignedExecutionPayloadEnvelope) error {
			if !proto.Equal(e.Proto(), right) {
				return badBid
			}
			return nil
		}
		return n, move, right, root
	}

	t.Run("another builder's, of the next slot: queued beside it, and the block's own imported", func(t *testing.T) {
		n, move, right, root := setup(t)
		move(2)
		wrong := testSignedExecutionPayloadEnvelope(t, 2, right.Message.BuilderIndex+1, root, [32]byte{0x77})
		n.deliver(t, wrong)
		require.Equal(t, true, proto.Equal(right, n.queued(right)), "the gossip entry of another slot gave way")
		require.Equal(t, true, proto.Equal(wrong, n.queued(wrong)), "the reassembled envelope was not queued beside it")
		n.s.processPendingPayloadEnvelope(t.Context(), root)
		require.Equal(t, 1, n.imported(right), "the block's own envelope was not imported")
		require.Equal(t, 0, n.imported(wrong))
	})
	t.Run("the same builder's, of the next slot: the first stays", func(t *testing.T) {
		n, move, right, root := setup(t)
		move(2)
		wrong := testSignedExecutionPayloadEnvelope(t, 2, right.Message.BuilderIndex, root, [32]byte{0x77})
		n.deliver(t, wrong)
		require.Equal(t, true, proto.Equal(right, n.queued(right)), "the gossip entry of another slot gave way")
		n.s.processPendingPayloadEnvelope(t.Context(), root)
		require.Equal(t, 1, n.imported(right), "the block's own envelope was not imported")
	})
	t.Run("a third builder's, beside two checked entries: not queued", func(t *testing.T) {
		n, move, right, root := setup(t)
		move(2)
		second := testSignedExecutionPayloadEnvelope(t, 2, right.Message.BuilderIndex+1, root, [32]byte{0x77})
		n.deliver(t, second)
		require.Equal(t, true, proto.Equal(second, n.queued(second)), "the second builder's envelope was not queued")
		move(3)
		third := testSignedExecutionPayloadEnvelope(t, 3, right.Message.BuilderIndex+2, root, [32]byte{0x78})
		n.deliver(t, third)
		require.Equal(t, true, n.queued(third) == nil, "a third entry queued beside two checked ones")
		require.Equal(t, true, proto.Equal(right, n.queued(right)), "a checked entry gave way")
		require.Equal(t, true, proto.Equal(second, n.queued(second)), "a checked entry gave way")
	})
	t.Run("a third builder's, beside a checked entry and a fetched one: the fetched one gives way", func(t *testing.T) {
		n, move, right, root := setup(t)
		move(2)
		fetched := testSignedExecutionPayloadEnvelope(t, 2, right.Message.BuilderIndex+1, root, [32]byte{0x77})
		n.s.queuePendingPayloadEnvelopeFromRootRequest(roEnvelope(t, fetched))
		require.Equal(t, true, proto.Equal(fetched, n.queued(fetched)), "the fetched entry was not queued")
		third := testSignedExecutionPayloadEnvelope(t, 2, right.Message.BuilderIndex+2, root, [32]byte{0x78})
		n.deliver(t, third)
		require.Equal(t, true, proto.Equal(third, n.queued(third)), "the reassembled envelope was not queued")
		require.Equal(t, true, n.queued(fetched) == nil, "the fetched entry stayed beside two others")
		require.Equal(t, true, proto.Equal(right, n.queued(right)), "the checked entry gave way")
	})
	t.Run("self-build bypasses the per-root cap", func(t *testing.T) {
		n, _, first, root := setup(t)
		second := testSignedExecutionPayloadEnvelope(t, 1, first.Message.BuilderIndex+1, root, [32]byte{0x77})
		n.gossiped(t, second)
		self := testSignedExecutionPayloadEnvelope(t, 1, params.BeaconConfig().BuilderIndexSelfBuild, root, [32]byte{0x78})
		n.deliver(t, self)
		require.Equal(t, true, proto.Equal(self, n.queued(self)), "the self-built envelope was refused")
		require.Equal(t, true, proto.Equal(first, n.queued(first)))
		require.Equal(t, true, proto.Equal(second, n.queued(second)))
	})
	t.Run("a refused builder leaves all fetched entries queued", func(t *testing.T) {
		n, _, first, root := setup(t)
		fetched := testSignedExecutionPayloadEnvelope(t, 1, first.Message.BuilderIndex+1, root, [32]byte{0x77})
		n.s.queuePendingPayloadEnvelopeFromRootRequest(roEnvelope(t, fetched))
		require.Equal(t, true, n.queued(fetched) == fetched)
		self := testSignedExecutionPayloadEnvelope(t, 1, params.BeaconConfig().BuilderIndexSelfBuild, root, [32]byte{0x78})
		n.gossiped(t, self)

		incoming := testSignedExecutionPayloadEnvelope(t, 1, first.Message.BuilderIndex+2, root, [32]byte{0x79})
		n.deliver(t, incoming)
		require.Equal(t, true, n.queued(incoming) == nil)
		n.s.pendingEnvelopeLock.RLock()
		got := n.s.pendingPayloadEnvelopes[root][uint64(fetched.Message.BuilderIndex)]
		marked := n.s.isFetchedEnvelope(root, uint64(fetched.Message.BuilderIndex), roEnvelope(t, fetched))
		n.s.pendingEnvelopeLock.RUnlock()
		require.Equal(t, true, got != nil && got.Proto() == proto.Message(fetched) && marked, "refusal changed the fetched entry or its mark")
	})
	t.Run("pruned at finality, each entry by its own slot", func(t *testing.T) {
		n, move, right, root := setup(t)
		later := primitives.Slot(2 * params.BeaconConfig().SlotsPerEpoch)
		move(later)
		second := testSignedExecutionPayloadEnvelope(t, later, right.Message.BuilderIndex+1, root, [32]byte{0x77})
		n.deliver(t, second)
		require.Equal(t, true, proto.Equal(second, n.queued(second)), "the second builder's envelope was not queued")
		n.chain.FinalizedCheckPoint = &ethpb.Checkpoint{Epoch: 1}
		n.s.prunePendingPayloadEnvelopes()
		require.Equal(t, true, n.queued(right) == nil, "the entry of epoch 0 stayed after epoch 1 was finalized")
		require.Equal(t, true, proto.Equal(second, n.queued(second)), "the entry of epoch 2 was pruned")
	})
	t.Run("the same builder's, of the same slot and different: the first stays", func(t *testing.T) {
		n, _, right, root := setup(t)
		wrong := testSignedExecutionPayloadEnvelope(t, 1, right.Message.BuilderIndex, root, [32]byte{0x77})
		n.deliver(t, wrong)
		require.Equal(t, true, proto.Equal(right, n.queued(right)), "the gossip entry gave way")
		n.s.processPendingPayloadEnvelope(t.Context(), root)
		require.Equal(t, 1, n.imported(right), "the block's own envelope was not imported")
	})
}

// TestFetchedEnvelopeMarksLeaveWithTheirEntries: the mark of an entry fetched by root leaves with the
// entry, whichever way the entry leaves the queue, so the marks never outgrow the queue.
func TestFetchedEnvelopeMarksLeaveWithTheirEntries(t *testing.T) {
	params.SetupTestConfigCleanup(t)
	notSeen := errors.New("block not seen")
	marked := func(n *segmentNode) int {
		n.s.pendingEnvelopeLock.RLock()
		defer n.s.pendingEnvelopeLock.RUnlock()
		return len(n.s.pendingFetchedEnvelopes)
	}
	fetch := func(t *testing.T, n *segmentNode, e *ethpb.SignedExecutionPayloadEnvelope) {
		n.s.queuePendingPayloadEnvelopeFromRootRequest(roEnvelope(t, e))
		require.Equal(t, true, proto.Equal(e, n.queued(e)), "the fetched entry was not queued")
		require.Equal(t, 1, marked(n), "the fetched entry was not marked")
	}
	variant := func(t *testing.T, env *ethpb.SignedExecutionPayloadEnvelope, change func(*ethpb.SignedExecutionPayloadEnvelope)) *ethpb.SignedExecutionPayloadEnvelope {
		e, ok := proto.Clone(env).(*ethpb.SignedExecutionPayloadEnvelope)
		require.Equal(t, true, ok)
		change(e)
		return e
	}

	t.Run("drained when the block arrives", func(t *testing.T) {
		n := newSegmentNode(t, 1, mockExecutionPayloadEnvelopeVerifier{errBlockRootSeen: notSeen})
		env, root := n.block(t, 1)
		fetch(t, n, env)
		n.s.processPendingPayloadEnvelope(t.Context(), root)
		require.Equal(t, 0, marked(n))
	})
	t.Run("pruned at finality", func(t *testing.T) {
		n := newSegmentNode(t, 1, mockExecutionPayloadEnvelopeVerifier{errBlockRootSeen: notSeen})
		env, _ := n.block(t, 1)
		fetch(t, n, env)
		n.chain.FinalizedCheckPoint = &ethpb.Checkpoint{Epoch: 1}
		n.s.prunePendingPayloadEnvelopes()
		require.Equal(t, true, n.queued(env) == nil, "not pruned")
		require.Equal(t, 0, marked(n))
	})
	t.Run("replaced by its builder's reassembled envelope", func(t *testing.T) {
		n := newSegmentNode(t, 1, mockExecutionPayloadEnvelopeVerifier{errBlockRootSeen: notSeen})
		env, _ := n.block(t, 1)
		n.chain.ForkchoiceRoots = map[[32]byte]bool{}
		fetch(t, n, variant(t, env, func(e *ethpb.SignedExecutionPayloadEnvelope) { e.Signature[0] ^= 0xff }))
		n.deliver(t, env)
		require.Equal(t, true, proto.Equal(env, n.queued(env)), "not replaced")
		require.Equal(t, 0, marked(n))
	})
	t.Run("giving way to a reassembled envelope of another slot", func(t *testing.T) {
		n := newSegmentNode(t, 1, mockExecutionPayloadEnvelopeVerifier{errBlockRootSeen: notSeen})
		env, _ := n.block(t, 1)
		n.chain.ForkchoiceRoots = map[[32]byte]bool{}
		other := variant(t, env, func(e *ethpb.SignedExecutionPayloadEnvelope) {
			e.Message.Payload.SlotNumber = 0
			e.Message.BuilderIndex++
		})
		fetch(t, n, other)
		n.deliver(t, env)
		require.Equal(t, true, n.queued(other) == nil, "the fetched entry of another slot stayed")
		require.Equal(t, 0, marked(n))
	})
}

func TestPendingEnvelopesWithoutSegments(t *testing.T) {
	params.SetupTestConfigCleanup(t)
	t.Run("gossip can accept when its first fetched entry matches", func(t *testing.T) {
		n := newSegmentNode(t, 1, mockExecutionPayloadEnvelopeVerifier{})
		move := n.movableClock(t)
		move(1)
		n.s.segmentReassembler = nil
		root := [32]byte{1}
		for i := range 2 {
			fetched := testSignedExecutionPayloadEnvelope(t, primitives.Slot(i), primitives.BuilderIndex(i), root, [32]byte{2})
			n.s.queuePendingPayloadEnvelopeFromRootRequest(roEnvelope(t, fetched))
			require.Equal(t, true, n.queued(fetched) == fetched)
		}
		self := testSignedExecutionPayloadEnvelope(t, 1, params.BeaconConfig().BuilderIndexSelfBuild, root, [32]byte{3})
		ro, err := blocks.WrappedROSignedExecutionPayloadEnvelope(self)
		require.NoError(t, err)
		env, err := ro.Envelope()
		require.NoError(t, err)
		v := &mockExecutionPayloadEnvelopeVerifier{}
		// Develop's map scan can visit either fetched entry first.
		for range 256 {
			clear(n.s.selfBuildSeenProposers)
			_, err := n.s.queuePendingPayloadEnvelope(t.Context(), v, env, roEnvelope(t, self))
			require.NoError(t, err)
			if n.queued(self) != nil {
				return
			}
		}
		t.Fatal("queued the self-built envelope 0 times in 256 attempts, want at least one")
	})

	t.Run("pruning keeps or removes a whole mixed-slot root", func(t *testing.T) {
		boundary := params.BeaconConfig().SlotsPerEpoch
		n := newSegmentNode(t, boundary, mockExecutionPayloadEnvelopeVerifier{})
		move := n.movableClock(t)
		move(boundary)
		n.s.segmentReassembler = nil
		root := [32]byte{1}
		for i, slot := range []primitives.Slot{boundary - 1, boundary} {
			fetched := testSignedExecutionPayloadEnvelope(t, slot, primitives.BuilderIndex(i), root, [32]byte{2})
			n.s.queuePendingPayloadEnvelopeFromRootRequest(roEnvelope(t, fetched))
			require.Equal(t, true, n.queued(fetched) == fetched)
		}
		n.chain.FinalizedCheckPoint = &ethpb.Checkpoint{Epoch: 1}
		n.s.prunePendingPayloadEnvelopes()
		entries := len(n.s.pendingPayloadEnvelopes[root])
		marks := len(n.s.pendingFetchedEnvelopes[root])
		if entries == 1 || marks != entries {
			t.Fatalf("pruned root has %d entries and %d fetched marks, want 0 and 0 or 2 and 2", entries, marks)
		}
	})
}

// TestSegmentGroupsAtTheBoundary: a group of slot S that completes in S+1 doesn't take S+1's own group's place. Both envelopes are imported; a third
// group in S+1 is refused, every segment of it ignored, and its segments counted under their reason.
func TestSegmentGroupsAtTheBoundary(t *testing.T) {
	params.SetupTestConfigCleanup(t)
	n := newSegmentNode(t, 2, mockExecutionPayloadEnvelopeVerifier{})
	late, _ := n.block(t, 1)
	own, _ := n.block(t, 2)
	third := testSignedExecutionPayloadEnvelope(t, 2, 1, [32]byte{0x33}, [32]byte{0x34})
	refused := counterValue(t, segmentGroupRefusedCounter.WithLabelValues(segmentRefusedFirstSeenFull))

	n.deliver(t, late)
	n.deliver(t, own)
	require.Equal(t, 1, n.imported(late), "slot S's late group was not imported")
	require.Equal(t, 1, n.imported(own), "slot S+1's own group was not imported behind the late one")

	// The third envelope's block is unknown, so its not being imported shows nothing; the verdicts do.
	// Each of the third group's segments tries to open it, until the sender's budget runs out.
	conflicts := 0
	for i, v := range n.deliver(t, third) {
		require.Equal(t, pubsub.ValidationIgnore, v.res, "the third group's segment %d: %v", i, v.err)
		if errors.Is(v.err, segmentauth.ErrFirstSeenConflict) {
			conflicts++
		}
	}
	require.Equal(t, true, conflicts > 0, "no segment of the third group was refused for its slot's places")
	require.Equal(t, float64(conflicts),
		counterValue(t, segmentGroupRefusedCounter.WithLabelValues(segmentRefusedFirstSeenFull))-refused,
		"the third group's refusals were not counted")
	require.Equal(t, 0, n.imported(third))
}

// TestSegmentImportAsTheSubscriber: the import the segment validator starts is bounded and accounted as
// the envelope topic's subscriber bounds and accounts one. It runs under the subscriber's deadline, so an
// import that waits for columns that never come gives the root back; a failure is counted under the
// envelope topic; and a panic in the import is recovered, not the node's end.
func TestSegmentImportAsTheSubscriber(t *testing.T) {
	params.SetupTestConfigCleanup(t)
	envelopeTopic := func(s *Service) string {
		return fmt.Sprintf(p2p.ExecutionPayloadEnvelopeTopicFormat, s.currentForkDigest()) + s.cfg.p2p.Encoding().ProtocolSuffix()
	}

	t.Run("a deadline", func(t *testing.T) {
		n := newSegmentNode(t, 1, mockExecutionPayloadEnvelopeVerifier{})
		env, _ := n.block(t, 1)
		n.deliver(t, env)
		require.Equal(t, 1, n.imported(env))
		require.Equal(t, true, n.chain.ReceivePayloadEnvelopeCtxHadDeadline, "the import had no deadline")
	})
	t.Run("a failure counted", func(t *testing.T) {
		n := newSegmentNode(t, 1, mockExecutionPayloadEnvelopeVerifier{})
		env, _ := n.block(t, 1)
		n.chain.ReceivePayloadEnvelopeErr = errors.New("import failed")
		failed := messageFailedProcessingCounter.WithLabelValues(envelopeTopic(n.s))
		before := counterValue(t, failed)
		n.deliver(t, env)
		require.Equal(t, before+1, counterValue(t, failed), "the failed import was not counted")
	})
	t.Run("a panic recovered", func(t *testing.T) {
		n := newSegmentNode(t, 1, mockExecutionPayloadEnvelopeVerifier{})
		env, _ := n.block(t, 1)
		n.chain.ReceivePayloadEnvelopeFn = func(interfaces.ROSignedExecutionPayloadEnvelope) error {
			panic("import panicked")
		}
		n.deliver(t, env)
		require.Equal(t, 0, n.imported(env))
	})
}
