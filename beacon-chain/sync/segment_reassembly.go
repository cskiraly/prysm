package sync

import (
	"github.com/OffchainLabs/prysm/v7/beacon-chain/verification/segmentauth"
	"github.com/OffchainLabs/prysm/v7/config/features"
	leakybucket "github.com/OffchainLabs/prysm/v7/container/leaky-bucket"
	"github.com/OffchainLabs/prysm/v7/container/segments"
)

// initSegmentReassembly prepares segmented payload gossip when the feature is enabled.
//
// A nil segmentReassembler is what the validator and the subscriber check to decide the
// feature is off, so with the flag unset nothing is allocated and no topic behaviour changes.
func (s *Service) initSegmentReassembly() error {
	if !features.Get().EnableSegmentedPayloadGossip {
		return nil
	}
	r, err := segments.NewReassembler(segments.ReassemblerConfig{Auth: s.segmentAuthenticator()})
	if err != nil {
		return err
	}
	s.segmentReassembler = r
	// Bounds how many groups a peer can try to open: a peer that only joins open groups is
	// never charged. deleteEmptyBuckets is set so a churn of short-lived peers does not
	// accumulate buckets; the collector prunes them once per authAttemptPeriod.
	s.segmentAuthLimiter = leakybucket.NewCollector(
		authAttemptRefillPerSecond, authAttemptsPerPeer, authAttemptPeriod, true)
	return nil
}

// segmentAuthenticator builds the descriptor authenticator.
//
// Interim: until ExecutionPayloadBid carries the segment commitment there is no commitment set
// to test against, so the first group offered in a slot is the one that slot admits. The key is
// this node's own clock, so a peer cannot choose it or flood it. It bounds groups and
// establishes nothing about who published them; see segmentauth.FirstSeenAuthenticator for
// the full statement and why it must not ship as the real scheme.
func (s *Service) segmentAuthenticator() segments.Authenticator {
	return segmentauth.NewFirstSeen(s.cfg.clock.CurrentSlot)
}
