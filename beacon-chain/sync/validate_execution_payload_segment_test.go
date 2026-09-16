package sync

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

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
	ethpb "github.com/OffchainLabs/prysm/v7/proto/prysm/v1alpha1"
	"github.com/OffchainLabs/prysm/v7/testing/require"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
)

// TestSegmentAddResult pins the split between "the peer is at fault" and "we cannot judge
// this yet". Only the former should cost the sender reputation, since Reject is what feeds
// gossipsub's invalid-message scoring.
func TestSegmentAddResult(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want pubsub.ValidationResult
		why  string
	}{
		{
			name: "our group cap",
			err:  segments.ErrTooManyGroups,
			want: pubsub.ValidationIgnore,
			why:  "our own limit, the peer did nothing wrong",
		},
		{
			name: "our byte budget",
			err:  segments.ErrBufferFull,
			want: pubsub.ValidationIgnore,
			why:  "our own limit, the peer did nothing wrong",
		},
		{
			name: "group not committed by any known block yet",
			err:  segmentauth.ErrNotCommitted,
			want: pubsub.ValidationIgnore,
			why:  "transient: the commitment arrives with a block, so the sender is blameless",
		},
		{
			name: "descriptor conflicts with the group's pinned descriptor",
			err:  segments.ErrDescriptorConflict,
			want: pubsub.ValidationReject,
			why:  "attributable to the sender",
		},
		{
			name: "first-seen conflict under the interim authority",
			err:  segmentauth.ErrFirstSeenConflict,
			want: pubsub.ValidationIgnore,
			why:  "must never cost reputation: under first-seen a conflict proves only that two peers disagree",
		},
		{
			name: "unauthenticated wrapper alone",
			err:  segments.ErrUnauthenticatedDescriptor,
			want: pubsub.ValidationReject,
			why:  "no transient cause identified, so treat as the sender's fault",
		},
		{
			name: "coded group that is not a codeword",
			err:  segments.ErrCodewordMismatch,
			want: pubsub.ValidationReject,
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
		require.Equal(t, pubsub.ValidationIgnore, got)
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
	seg, err := m.ToProto()
	require.NoError(t, err)
	buf := new(bytes.Buffer)
	_, err = p.Encoding().EncodeGossip(buf, seg)
	require.NoError(t, err)
	topic := p2p.GossipTypeMapping[reflect.TypeFor[*ethpb.ExecutionPayloadSegment]()]
	topic = s.addDigestToTopic(topic, s.currentForkDigest())
	return &pubsub.Message{Message: &pb.Message{Data: buf.Bytes(), Topic: &topic}}
}

// TestSegmentValidatorCompletesCodedGroup drives the default segmentation through the
// validator as a peer would deliver it: the parity half of a coded, compress-first group. The
// completing segment yields the envelope, the pull gate hears of the group once, and the
// group's remaining segments are still accepted for forwarding without a second delivery.
func TestSegmentValidatorCompletesCodedGroup(t *testing.T) {
	params.SetupTestConfigCleanup(t)
	s, p := segmentService(t)
	ctx := context.Background()
	from := peer.ID("other")

	env := testSignedExecutionPayloadEnvelope(t, 3, 7, [32]byte{}, [32]byte{})
	txs := make([][]byte, 40)
	for i := range txs {
		txs[i] = bytes.Repeat([]byte{byte(i + 1)}, 4096)
	}
	env.Message.Payload.Transactions = txs
	msgs, err := segmentauth.SegmentMessagesForEnvelope(env, segmentauth.DefaultParams())
	require.NoError(t, err)
	d := msgs[0].Descriptor
	k := int(d.Required())
	require.Equal(t, segments.VersionCoded, d.Version)
	require.Equal(t, segments.EncodingSnappy, d.Encoding)
	require.Equal(t, true, k > 4, "the envelope should need several segments")
	root := bytesutil.ToBytes32(d.Root)

	var delivered *ethpb.SignedExecutionPayloadEnvelope
	for i, m := range msgs[k:] {
		msg := segmentToPubsub(t, s, p, m)
		res, err := s.validateExecutionPayloadSegment(ctx, from, msg)
		require.NoError(t, err)
		require.Equal(t, pubsub.ValidationAccept, res, "parity segment %d", i)
		switch got := msg.ValidatorData.(type) {
		case *ethpb.ExecutionPayloadSegment:
			require.Equal(t, true, i < k-1, "delivered late: segment %d of %d still buffered", i, k)
		case *ethpb.SignedExecutionPayloadEnvelope:
			require.Equal(t, k-1, i, "delivered at segment %d, want the %dth", i, k)
			delivered = got
		default:
			t.Fatalf("segment %d: validator data %T", i, msg.ValidatorData)
		}
	}
	require.NotNil(t, delivered)
	require.DeepEqual(t, env, delivered)
	require.DeepEqual(t, [][32]byte{root}, p.CompletedSegmentGroups(), "the gate hears of the group once")

	t.Run("the rest of the group forwards without a second delivery", func(t *testing.T) {
		for _, m := range msgs[:k] {
			msg := segmentToPubsub(t, s, p, m)
			res, err := s.validateExecutionPayloadSegment(ctx, from, msg)
			require.NoError(t, err)
			require.Equal(t, pubsub.ValidationAccept, res)
			_, ok := msg.ValidatorData.(*ethpb.ExecutionPayloadSegment)
			require.Equal(t, true, ok)
		}
		require.Equal(t, 1, len(p.CompletedSegmentGroups()))
		require.Equal(t, 0, s.segmentReassembler.Bytes())
	})
}

// TestSegmentValidatorRejectsUndecodableGroup pins the attribution of a group whose committed
// bytes do not decode as the descriptor says: the gate still hears of it, since the node holds
// what the root committed to, and the completing segment is rejected.
func TestSegmentValidatorRejectsUndecodableGroup(t *testing.T) {
	params.SetupTestConfigCleanup(t)
	s, p := segmentService(t)
	h, err := segments.HasherByID(segments.HashSHA256)
	require.NoError(t, err)
	// Bytes that are not snappy, committed as if they were.
	msgs, err := segments.Build(bytes.Repeat([]byte{0xff}, 3000), segments.Layout{SegmentSize: 1024, Hasher: h, Encoding: segments.EncodingSnappy})
	require.NoError(t, err)
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
	require.Equal(t, 1, len(p.CompletedSegmentGroups()))
}
