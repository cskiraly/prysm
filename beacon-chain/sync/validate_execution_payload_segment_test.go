package sync

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	mock "github.com/OffchainLabs/prysm/v7/beacon-chain/blockchain/testing"
	"github.com/OffchainLabs/prysm/v7/beacon-chain/p2p"
	p2ptest "github.com/OffchainLabs/prysm/v7/beacon-chain/p2p/testing"
	"github.com/OffchainLabs/prysm/v7/beacon-chain/startup"
	mockSync "github.com/OffchainLabs/prysm/v7/beacon-chain/sync/initial-sync/testing"
	"github.com/OffchainLabs/prysm/v7/beacon-chain/verification/segmentauth"
	"github.com/OffchainLabs/prysm/v7/config/params"
	"github.com/OffchainLabs/prysm/v7/consensus-types/primitives"
	leakybucket "github.com/OffchainLabs/prysm/v7/container/leaky-bucket"
	"github.com/OffchainLabs/prysm/v7/container/segments"
	"github.com/OffchainLabs/prysm/v7/encoding/bytesutil"
	enginev1 "github.com/OffchainLabs/prysm/v7/proto/engine/v1"
	ethpb "github.com/OffchainLabs/prysm/v7/proto/prysm/v1alpha1"
	"github.com/OffchainLabs/prysm/v7/testing/require"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
)

// TestSegmentAddResult pins the split between "the peer is at fault" and "we cannot judge
// this yet". Only the former should cost the sender reputation, since Reject is what feeds
// the binding's invalid-message scoring (gossipsub's, as ValidationReject).
func TestSegmentAddResult(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want segmentVerdict
		why  string
	}{
		{
			name: "our group cap",
			err:  segments.ErrTooManyGroups,
			want: segmentIgnore,
			why:  "our own limit, the peer did nothing wrong",
		},
		{
			name: "our byte budget",
			err:  segments.ErrBufferFull,
			want: segmentIgnore,
			why:  "our own limit, the peer did nothing wrong",
		},
		{
			name: "group not committed by any known block yet",
			err:  segmentauth.ErrNotCommitted,
			want: segmentIgnore,
			why:  "transient: the commitment arrives with a block, so the sender is blameless",
		},
		{
			name: "descriptor conflicts with the group's pinned descriptor",
			err:  segments.ErrDescriptorConflict,
			want: segmentReject,
			why:  "attributable to the sender",
		},
		{
			name: "first-seen conflict under the interim authority",
			err:  segmentauth.ErrFirstSeenConflict,
			want: segmentIgnore,
			why:  "must never cost reputation: under first-seen a conflict proves only that two peers disagree",
		},
		{
			name: "unauthenticated wrapper alone",
			err:  segments.ErrUnauthenticatedDescriptor,
			want: segmentReject,
			why:  "no transient cause identified, so treat as the sender's fault",
		},
		{
			name: "coded group that is not a codeword",
			err:  segments.ErrCodewordMismatch,
			want: segmentReject,
			why:  "every segment proved against the root, so the builder committed an inconsistent group",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := segmentAddResult(tc.err)
			require.Equal(t, tc.want, got, tc.why)
			require.NotNil(t, err)
		})
	}

	t.Run("wrapped transient errors are still recognised", func(t *testing.T) {
		// The reassembler wraps the authenticator's error, so errors.Is has to see through
		// two layers or every auth failure would be punished as a Reject.
		wrapped := errors.Join(segments.ErrUnauthenticatedDescriptor, segmentauth.ErrNotCommitted)
		got, _ := segmentAddResult(wrapped)
		require.Equal(t, segmentIgnore, got)
	})
}

// TestSpendSegmentAuthBudget covers the CPU mitigation: a peer may only trigger a bounded
// number of signature verifications, and running without a limiter must not block traffic.
func TestSpendSegmentAuthBudget(t *testing.T) {
	pid := peer.ID("peer-one")
	other := peer.ID("peer-two")

	t.Run("no limiter allows everything", func(t *testing.T) {
		s := &Service{}
		for range 100 {
			require.Equal(t, true, s.spendSegmentAuthBudget(pid))
		}
	})

	t.Run("burst is capped", func(t *testing.T) {
		s := &Service{segmentAuthLimiter: leakybucket.NewCollector(
			authAttemptRefillPerSecond, authAttemptsPerPeer, authAttemptPeriod, true)}
		allowed := 0
		for range authAttemptsPerPeer * 4 {
			if s.spendSegmentAuthBudget(pid) {
				allowed++
			}
		}
		require.Equal(t, authAttemptsPerPeer, allowed)
	})

	t.Run("budget is per peer", func(t *testing.T) {
		// One noisy peer must not be able to starve authentication for everyone else.
		s := &Service{segmentAuthLimiter: leakybucket.NewCollector(
			authAttemptRefillPerSecond, authAttemptsPerPeer, authAttemptPeriod, true)}
		for range authAttemptsPerPeer * 2 {
			s.spendSegmentAuthBudget(pid)
		}
		require.Equal(t, false, s.spendSegmentAuthBudget(pid))
		require.Equal(t, true, s.spendSegmentAuthBudget(other))
	})

	t.Run("the budget refills at the sustained rate", func(t *testing.T) {
		// The collector leaks rate tokens per period, so one attempt must come back within a
		// period of exhausting the burst; a misread period once made that ten minutes.
		s := &Service{segmentAuthLimiter: leakybucket.NewCollector(
			authAttemptRefillPerSecond, authAttemptsPerPeer, authAttemptPeriod, true)}
		for range authAttemptsPerPeer {
			require.Equal(t, true, s.spendSegmentAuthBudget(pid))
		}
		require.Equal(t, false, s.spendSegmentAuthBudget(pid))
		time.Sleep(authAttemptPeriod + authAttemptPeriod/10)
		require.Equal(t, true, s.spendSegmentAuthBudget(pid), "one attempt should leak back within a period")
	})
}

// TestSegmentValidatorIgnoresWhileSyncing pins that a syncing node judges no segment, as it
// judges no envelope: without the chain it cannot tell a legitimate group from a stale one,
// and Ignore costs the sender nothing.
func TestSegmentValidatorIgnoresWhileSyncing(t *testing.T) {
	p := p2ptest.NewTestP2P(t)
	r, err := segments.NewReassembler(segments.ReassemblerConfig{
		Auth: segmentauth.NewFirstSeen(func() primitives.Slot { return 0 }),
	})
	require.NoError(t, err)
	s := &Service{
		cfg:                &config{p2p: p, initialSync: &mockSync.Sync{IsSyncing: true}},
		segmentReassembler: r,
	}
	result, err := s.validateExecutionPayloadSegment(context.Background(), peer.ID("other"), &pubsub.Message{Message: &pb.Message{}})
	require.NoError(t, err)
	require.Equal(t, pubsub.ValidationIgnore, result)
}

// TestSegmentValidatorDisabled checks the feature is inert when reassembly is not enabled.
func TestSegmentValidatorDisabled(t *testing.T) {
	s := &Service{}
	require.Equal(t, true, s.segmentReassembler == nil)
}

// segmentService builds a Service that validates segments on the current fork's segment
// topic, authenticating under the interim first-seen authority and with no per-peer budget.
func segmentService(t *testing.T) (*Service, *p2ptest.TestP2P) {
	t.Helper()
	p := p2ptest.NewTestP2P(t)
	r, err := segments.NewReassembler(segments.ReassemblerConfig{
		Auth: segmentauth.NewFirstSeen(func() primitives.Slot { return 0 }),
	})
	require.NoError(t, err)
	genesis := time.Unix(time.Now().Unix()-int64(params.BeaconConfig().SecondsPerSlot), 0)
	s := &Service{
		cfg: &config{
			p2p:         p,
			chain:       &mock.ChainService{BlockSlots: map[[32]byte]primitives.Slot{}}, // knows no block
			initialSync: &mockSync.Sync{},
			clock:       startup.NewClock(genesis, [32]byte{}),
		},
		segmentReassembler: r,
	}
	return s, p
}

// segmentToPubsub frames a segment as the gossip message a peer would send on the topic.
func segmentToPubsub(t *testing.T, s *Service, p p2p.P2P, m *segments.SegmentMessage) *pubsub.Message {
	t.Helper()
	return segmentToPubsubAt(t, s, p, m, s.currentForkDigest())
}

// segmentToPubsubAt is segmentToPubsub on the segment topic at the given fork digest.
func segmentToPubsubAt(t *testing.T, s *Service, p p2p.P2P, m *segments.SegmentMessage, digest [4]byte) *pubsub.Message {
	t.Helper()
	seg, err := m.ToProto()
	require.NoError(t, err)
	buf := new(bytes.Buffer)
	_, err = p.Encoding().EncodeGossip(buf, seg)
	require.NoError(t, err)
	topic := p2p.GossipTypeMapping[reflect.TypeFor[*ethpb.ExecutionPayloadSegment]()]
	topic = s.addDigestToTopic(topic, digest)
	return &pubsub.Message{Message: &pb.Message{Data: buf.Bytes(), Topic: &topic}}
}

// TestSegmentValidatorCompletesCodedGroup drives the default segmentation through the
// validator as a peer would deliver it: the parity half of a coded, compress-first group. The
// completing segment's validation imports the envelope, and the group's remaining segments are
// still accepted for forwarding without a second import.
func TestSegmentValidatorCompletesCodedGroup(t *testing.T) {
	params.SetupTestConfigCleanup(t)
	// The node's chain has the envelope's block, so the reassembled envelope passes the envelope
	// topic's checks and is handed on at completion.
	n := newSegmentNode(t, 1, mockExecutionPayloadEnvelopeVerifier{})
	s := n.s
	p, ok := s.cfg.p2p.(*p2ptest.TestP2P)
	require.Equal(t, true, ok)
	ctx := context.Background()
	from := peer.ID("other")

	env, _ := n.block(t, 1)
	txs := make([][]byte, 40)
	for i := range txs {
		txs[i] = bytes.Repeat([]byte{byte(i + 1)}, 4096)
	}
	txList, err := enginev1.NewProgressiveTransactionList(txs)
	require.NoError(t, err)
	env.Message.Payload.Transactions = txList
	msgs, err := segmentauth.SegmentMessagesForEnvelope(env, segmentauth.DefaultParams())
	require.NoError(t, err)
	d := msgs[0].Descriptor
	k := int(d.Required())
	require.Equal(t, segments.VersionCoded, d.Version)
	require.Equal(t, segments.EncodingSnappy, d.Encoding)
	require.Equal(t, true, k > 4, "the envelope should need several segments")
	group := bytesutil.ToBytes32(msgs[0].Group)

	for i, m := range msgs[k:] {
		msg := segmentToPubsub(t, s, p, m)
		res, err := s.validateExecutionPayloadSegment(ctx, from, msg)
		require.NoError(t, err)
		require.Equal(t, pubsub.ValidationAccept, res, "parity segment %d", i)
		_, ok := msg.ValidatorData.(*ethpb.ExecutionPayloadSegment)
		require.Equal(t, true, ok, "segment %d: validator data %T", i, msg.ValidatorData)
		s.segmentWork.Wait()
		if i < k-1 {
			require.Equal(t, 0, n.imported(env), "imported at segment %d of %d", i, k)
		} else {
			require.Equal(t, 1, n.imported(env), "not imported at the %dth segment", k)
		}
	}
	// Every accepted segment tells the payload path its group's shape and topic, before the router
	// counts it: the path knows no group's count until then.
	topic := segmentToPubsub(t, s, p, msgs[0]).GetTopic()
	seen := p.SeenSegmentGroups()
	require.Equal(t, k, len(seen))
	for _, g := range seen {
		require.DeepEqual(t, p2ptest.SeenSegmentGroup{Group: group, Topic: topic, Count: int(d.Count), Threshold: k}, g)
	}
	// The segment that completed the group handed its committed bytes (the systematic segments, in
	// order) to the payload path, so the router's decode does not recover them again.
	var want []byte
	for _, m := range msgs[:k] {
		want = append(want, m.Data...)
	}
	committed := p.CommittedSegmentGroups()
	require.Equal(t, 1, len(committed))
	require.Equal(t, group, committed[0].Group)
	require.Equal(t, true, bytes.Equal(want, committed[0].Committed), "the handed bytes are not the group's")

	t.Run("the rest of the group forwards without a second delivery", func(t *testing.T) {
		for _, m := range msgs[:k] {
			msg := segmentToPubsub(t, s, p, m)
			res, err := s.validateExecutionPayloadSegment(ctx, from, msg)
			require.NoError(t, err)
			require.Equal(t, pubsub.ValidationAccept, res)
			_, ok := msg.ValidatorData.(*ethpb.ExecutionPayloadSegment)
			require.Equal(t, true, ok)
		}
		require.Equal(t, 0, s.segmentReassembler.Bytes())
		require.Equal(t, 1, len(p.CommittedSegmentGroups()), "a delivered group's bytes handed over again")
	})
}

// TestSegmentAcceptPathOverAChunk: the accept path runs over a chunk with no gossipsub message around it,
// as another binding would hand one over. The place the chunk travelled is handed to the payload path as
// is, the group completes at its K-th chunk and imports, and a chunk whose group key is not its
// descriptor's is the sender's fault.
func TestSegmentAcceptPathOverAChunk(t *testing.T) {
	params.SetupTestConfigCleanup(t)
	n := newSegmentNode(t, 1, mockExecutionPayloadEnvelopeVerifier{})
	s := n.s
	p, ok := s.cfg.p2p.(*p2ptest.TestP2P)
	require.Equal(t, true, ok)
	ctx := t.Context()

	env, _ := n.block(t, 1)
	txs := make([][]byte, 40)
	for i := range txs {
		txs[i] = bytes.Repeat([]byte{byte(i + 1)}, 4096)
	}
	txList, err := enginev1.NewProgressiveTransactionList(txs)
	require.NoError(t, err)
	env.Message.Payload.Transactions = txList
	msgs, err := segmentauth.SegmentMessagesForEnvelope(env, segmentauth.DefaultParams())
	require.NoError(t, err)
	k := int(msgs[0].Descriptor.Required())
	require.Equal(t, segments.VersionCoded, msgs[0].Descriptor.Version)

	// A stream binding's protocol, say: not a gossip topic, and the path hands it back as is.
	const place = "/seacast/2.0.0"
	arrival := func(m *segments.SegmentMessage) segmentArrival {
		chunk, err := m.ToProto()
		require.NoError(t, err)
		return segmentArrival{from: peer.ID("other"), place: place, digest: s.currentForkDigest(), chunk: chunk}
	}

	t.Run("a chunk whose group key is not its descriptor's", func(t *testing.T) {
		a := arrival(msgs[0])
		a.chunk.Slot++ // the key binds the slot
		v, err := s.acceptSegment(ctx, a)
		require.NotNil(t, err)
		require.Equal(t, segmentReject, v)
	})

	for i, m := range msgs[k : 2*k] {
		v, err := s.acceptSegment(ctx, arrival(m))
		require.NoError(t, err)
		require.Equal(t, segmentAccept, v, "chunk %d", i)
		s.segmentWork.Wait()
		if i < k-1 {
			require.Equal(t, 0, n.imported(env), "imported at chunk %d of %d", i, k)
		}
	}
	require.Equal(t, 1, n.imported(env), "not imported at the %dth chunk", k)
	seen := p.SeenSegmentGroups()
	require.Equal(t, k, len(seen))
	for _, g := range seen {
		require.Equal(t, place, g.Topic, "the place not handed to the path as is")
	}
	require.Equal(t, 1, len(p.CommittedSegmentGroups()))
}

// TestSegmentValidatorTypeFollowsTheDigest: a completed group decodes as the signed envelope of the
// segment topic's fork. With every fork to Gloas at genesis and PQ at epoch 1, a node in epoch 1
// imports a PQ envelope that came on the PQ digest's topic, rejects a Gloas envelope on that topic
// (its bytes are not a PQ envelope), and still decodes a Gloas envelope on the Gloas digest's topic.
func TestSegmentValidatorTypeFollowsTheDigest(t *testing.T) {
	params.SetupTestConfigCleanup(t)
	cfg := params.BeaconConfig().Copy()
	cfg.AltairForkEpoch = 0
	cfg.BellatrixForkEpoch = 0
	cfg.CapellaForkEpoch = 0
	cfg.DenebForkEpoch = 0
	cfg.ElectraForkEpoch = 0
	cfg.FuluForkEpoch = 0
	cfg.GloasForkEpoch = 0
	cfg.PQForkEpoch = 1
	params.OverrideBeaconConfig(cfg)
	params.BeaconConfig().InitializeForkSchedule()
	slot := params.BeaconConfig().SlotsPerEpoch + 1
	gloasDigest, pqDigest := params.ForkDigest(0), params.ForkDigest(1)
	require.NotEqual(t, gloasDigest, pqDigest)
	// The segment that completes the group is the one the decode's verdict lands on; later ones
	// join a delivered group and are accepted, so the verdicts are counted, not read from the end.
	rejects := func(v []verdict) int {
		c := 0
		for _, x := range v {
			if x.res == pubsub.ValidationReject {
				require.NotNil(t, x.err)
				c++
			}
		}
		return c
	}

	t.Run("a PQ envelope on the PQ topic is imported", func(t *testing.T) {
		n := newSegmentNode(t, slot, mockExecutionPayloadEnvelopeVerifier{})
		require.Equal(t, pqDigest, n.s.currentForkDigest())
		env, _ := n.block(t, slot)
		pq := pqEnvelopeOf(env)
		for i, v := range n.deliverAt(t, pq, pqDigest) {
			require.Equal(t, pubsub.ValidationAccept, v.res, "segment %d: %v", i, v.err)
		}
		require.Equal(t, 1, n.imported(pq))
	})
	t.Run("a Gloas envelope on the PQ topic is rejected", func(t *testing.T) {
		n := newSegmentNode(t, slot, mockExecutionPayloadEnvelopeVerifier{})
		env, _ := n.block(t, slot)
		require.Equal(t, 1, rejects(n.deliverAt(t, env, pqDigest)))
		require.Equal(t, 0, n.imported(env))
	})
	t.Run("a Gloas envelope on the Gloas topic decodes", func(t *testing.T) {
		n := newSegmentNode(t, slot, mockExecutionPayloadEnvelopeVerifier{})
		env, _ := n.block(t, slot)
		require.Equal(t, 0, rejects(n.deliverAt(t, env, gloasDigest)))
		require.Equal(t, 1, n.imported(env))
	})
}

// TestSegmentValidatorRejectsUndecodableGroup pins the attribution of a group whose committed
// bytes do not decode as the descriptor says: the completing segment is rejected.
func TestSegmentValidatorRejectsUndecodableGroup(t *testing.T) {
	params.SetupTestConfigCleanup(t)
	s, p := segmentService(t)
	h, err := segments.HasherByID(segments.HashSHA256)
	require.NoError(t, err)
	// Bytes that are not snappy, committed as if they were.
	msgs, err := segments.Build(bytes.Repeat([]byte{0xff}, 3000), segments.Layout{SegmentSize: 1024, Hasher: h, Encoding: segments.EncodingSnappy})
	require.NoError(t, err)
	require.NoError(t, segments.WithContext(msgs, h, 0, make([]byte, 32)))
	for i, m := range msgs {
		res, err := s.validateExecutionPayloadSegment(context.Background(), peer.ID("other"), segmentToPubsub(t, s, p, m))
		if i < len(msgs)-1 {
			require.NoError(t, err)
			require.Equal(t, pubsub.ValidationAccept, res)
			continue
		}
		require.NotNil(t, err)
		require.Equal(t, pubsub.ValidationReject, res)
	}
}

// TestSegmentValidatorChecksTheChunkContext pins seacast-lite.md §3's level 1 as far as Prysm goes
// without the bid's commitment: a chunk whose group key is not its descriptor's at its slot is
// rejected, so is one that names a known block of another slot, and a group whose payload is not of
// the slot its chunks name is not imported from, its completing chunk ignored and counted.
func TestSegmentValidatorChecksTheChunkContext(t *testing.T) {
	params.SetupTestConfigCleanup(t)
	h, err := segments.HasherByID(segments.HashSHA256)
	require.NoError(t, err)
	// cut is env's segmentation, its chunks naming slot and blockRoot.
	cut := func(t *testing.T, env *ethpb.SignedExecutionPayloadEnvelope, slot uint64, blockRoot [32]byte) []*segments.SegmentMessage {
		msgs, err := segmentauth.SegmentMessagesForEnvelope(env, segmentauth.DefaultParams())
		require.NoError(t, err)
		require.NoError(t, segments.WithContext(msgs, h, slot, blockRoot[:]))
		return msgs
	}
	validate := func(n *segmentNode, m *segments.SegmentMessage) (pubsub.ValidationResult, error) {
		res, err := n.s.validateExecutionPayloadSegment(t.Context(), peer.ID("sender"), segmentToPubsub(t, n.s, n.p, m))
		n.s.segmentWork.Wait()
		return res, err
	}

	t.Run("a group key that is not the descriptor's at the slot", func(t *testing.T) {
		n := newSegmentNode(t, 1, mockExecutionPayloadEnvelopeVerifier{})
		env, root := n.block(t, 1)
		m := cut(t, env, 1, root)[0]
		forged := *m
		forged.Group = segments.GroupKey(m.Descriptor.GroupID(h), 2) // the key of another slot
		res, err := validate(n, &forged)
		require.ErrorIs(t, err, segments.ErrGroupKey)
		require.Equal(t, pubsub.ValidationReject, res)
	})
	t.Run("a known block of another slot", func(t *testing.T) {
		n := newSegmentNode(t, 2, mockExecutionPayloadEnvelopeVerifier{})
		env, root := n.block(t, 1)
		res, err := validate(n, cut(t, env, 2, root)[0]) // the block is of slot 1
		require.ErrorContains(t, "segment names slot 2 for a block at slot 1", err)
		require.Equal(t, pubsub.ValidationReject, res)
	})
	t.Run("an unknown block: taken, as the interim authority needs no block", func(t *testing.T) {
		n := newSegmentNode(t, 1, mockExecutionPayloadEnvelopeVerifier{})
		env, _ := n.block(t, 1)
		res, err := validate(n, cut(t, env, 1, [32]byte{0x99})[0])
		require.NoError(t, err)
		require.Equal(t, pubsub.ValidationAccept, res)
	})
	t.Run("a payload not of its chunks' slot", func(t *testing.T) {
		n := newSegmentNode(t, 2, mockExecutionPayloadEnvelopeVerifier{})
		env, _ := n.block(t, 1)
		_, other := n.block(t, 2)
		// Every chunk is consistent with the block it names, a block of slot 2, but the payload is of slot 1.
		msgs := cut(t, env, 2, other)
		refused := counterValue(t, segmentGroupRefusedCounter.WithLabelValues(segmentRefusedSlot))
		k := int(msgs[0].Descriptor.Required())
		for i, m := range msgs[:k] {
			res, err := validate(n, m)
			require.NoError(t, err)
			if i < k-1 {
				require.Equal(t, pubsub.ValidationAccept, res, "chunk %d", i)
			} else {
				require.Equal(t, pubsub.ValidationIgnore, res, "the completing chunk")
			}
		}
		require.Equal(t, float64(1), counterValue(t, segmentGroupRefusedCounter.WithLabelValues(segmentRefusedSlot))-refused)
		require.Equal(t, 0, n.imported(env))
		// The completing chunk never reaches the router, so its bytes are not handed over to wait there.
		tp, ok := n.p.(*p2ptest.TestP2P)
		require.Equal(t, true, ok)
		require.Equal(t, 0, len(tp.CommittedSegmentGroups()), "bytes handed over for a refused completing chunk")
	})
}

// flippingSync is a node that is synced at the first check and syncing at every later one: a resync
// that starts while a segment is being validated.
type flippingSync struct {
	mockSync.Sync
	checks int
}

func (f *flippingSync) Syncing() bool {
	f.checks++
	return f.checks > 1
}

// TestSegmentValidatorChecksSyncOnce pins that the gossip validator checks once whether the node takes
// chunks, before it reads the segment, as it did before the accept path was lifted out of it: a resync
// that starts during the validation does not turn a segment it let through into an ignored one.
func TestSegmentValidatorChecksSyncOnce(t *testing.T) {
	params.SetupTestConfigCleanup(t)
	n := newSegmentNode(t, 1, mockExecutionPayloadEnvelopeVerifier{})
	env, _ := n.block(t, 1)
	msgs, err := segmentauth.SegmentMessagesForEnvelope(env, segmentauth.DefaultParams())
	require.NoError(t, err)
	checker := &flippingSync{}
	n.s.cfg.initialSync = checker
	res, err := n.s.validateExecutionPayloadSegment(t.Context(), peer.ID("sender"), segmentToPubsub(t, n.s, n.p, msgs[0]))
	// The group completes at its one segment, and the import it starts reads the configuration that the
	// cleanup restores: it ends before the test does.
	n.s.segmentWork.Wait()
	require.NoError(t, err)
	require.Equal(t, pubsub.ValidationAccept, res)
	require.Equal(t, 1, checker.checks)
	require.Equal(t, 1, n.imported(env))
}
