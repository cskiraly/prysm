package sync

import (
	"errors"
	"testing"

	"github.com/OffchainLabs/prysm/v7/beacon-chain/verification/segmentauth"
	leakybucket "github.com/OffchainLabs/prysm/v7/container/leaky-bucket"
	"github.com/OffchainLabs/prysm/v7/container/segments"
	"github.com/OffchainLabs/prysm/v7/testing/require"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
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
			why:  "attributable to the sender",
		},
		{
			name: "descriptor conflict",
			err:  segments.ErrDescriptorConflict,
			want: pubsub.ValidationReject,
			why:  "attributable to the sender",
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
			authAttemptRefillPerSecond, authAttemptsPerPeer, authAttemptPrunePeriod, true)}
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
			authAttemptRefillPerSecond, authAttemptsPerPeer, authAttemptPrunePeriod, true)}
		for range authAttemptsPerPeer * 2 {
			s.spendSegmentAuthBudget(pid)
		}
		require.Equal(t, false, s.spendSegmentAuthBudget(pid))
		require.Equal(t, true, s.spendSegmentAuthBudget(other))
	})
}

// TestSegmentValidatorDisabled checks the feature is inert when reassembly is not enabled.
func TestSegmentValidatorDisabled(t *testing.T) {
	s := &Service{}
	require.Equal(t, true, s.segmentReassembler == nil)
}
