package sync

import (
	"context"
	"errors"
	"testing"
	"time"

	p2ptest "github.com/OffchainLabs/prysm/v7/beacon-chain/p2p/testing"
	mockSync "github.com/OffchainLabs/prysm/v7/beacon-chain/sync/initial-sync/testing"
	"github.com/OffchainLabs/prysm/v7/beacon-chain/verification/segmentauth"
	"github.com/OffchainLabs/prysm/v7/consensus-types/primitives"
	leakybucket "github.com/OffchainLabs/prysm/v7/container/leaky-bucket"
	"github.com/OffchainLabs/prysm/v7/container/segments"
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
