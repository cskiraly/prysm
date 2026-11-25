package sync

import (
	"context"

	"github.com/OffchainLabs/prysm/v7/async"
	"github.com/OffchainLabs/prysm/v7/config/features"
	"github.com/OffchainLabs/prysm/v7/config/params"
	"github.com/OffchainLabs/prysm/v7/consensus-types/interfaces"
	"github.com/OffchainLabs/prysm/v7/time/slots"
)

const (
	maxPendingPayloadRoots    = 128
	maxPendingBuildersPerRoot = 2
	maxSelfBuildSigFailures   = 3
)

// processPendingPayloadEnvelopeQueue sweeps the pending envelope map at
// mid-slot to recover envelopes orphaned by the race between
// queuePendingPayloadEnvelope and beaconBlockSubscriber.
func (s *Service) processPendingPayloadEnvelopeQueue() {
	async.RunEvery(s.ctx, slots.DivideSlotBy(2), func() {
		if !s.chainIsStarted() {
			return
		}
		s.processPendingPayloadEnvelopes(s.ctx)
	})
}

// processPendingPayloadEnvelope retrieves all queued payload envelopes for the
// given block root and calls ReceiveExecutionPayloadEnvelope on each.
func (s *Service) processPendingPayloadEnvelope(ctx context.Context, root [32]byte) {
	s.pendingEnvelopeLock.Lock()
	inner, ok := s.pendingPayloadEnvelopes[root]
	if !ok {
		s.pendingEnvelopeLock.Unlock()
		return
	}
	delete(s.pendingPayloadEnvelopes, root)
	delete(s.pendingFetchedEnvelopes, root)
	s.pendingEnvelopeLock.Unlock()

	for _, e := range inner {
		env, err := e.Envelope()
		if err != nil {
			log.WithError(err).Debug("Could not get pending execution payload envelope")
			continue
		}
		if s.hasSeenPayloadEnvelope(root, env.BuilderIndex()) {
			continue
		}
		if s.hasBadBlock(root) {
			s.setSeenPayloadEnvelope(root, env.BuilderIndex())
			continue
		}

		receiveCtx, cancel := context.WithTimeout(ctx, params.BeaconConfig().SlotDuration())
		err = s.cfg.chain.ReceiveExecutionPayloadEnvelope(receiveCtx, e)
		cancel()
		if err != nil {
			log.WithError(err).Debug("Could not process pending payload envelope")
			continue
		}
		s.setSeenPayloadEnvelope(root, env.BuilderIndex())
		if features.Get().SeacastPayloadOnly {
			// The segment path carries the payload, Gloas's and the PQ fork's alike, so the whole
			// envelope stays off the network.
			continue
		}
		if err := s.cfg.p2p.Broadcast(ctx, e.Proto()); err != nil {
			log.WithError(err).Warn("Could not broadcast pending payload envelope")
		}
	}
}

// processPendingPayloadEnvelopes iterates the pending envelope map and
// processes any entry whose beacon block is now in forkchoice.
func (s *Service) processPendingPayloadEnvelopes(ctx context.Context) {
	s.pendingEnvelopeLock.RLock()
	roots := make([][32]byte, 0, len(s.pendingPayloadEnvelopes))
	for root := range s.pendingPayloadEnvelopes {
		roots = append(roots, root)
	}
	s.pendingEnvelopeLock.RUnlock()

	for _, root := range roots {
		if !s.cfg.chain.InForkchoice(root) {
			continue
		}
		s.processPendingPayloadEnvelope(ctx, root)
	}
}

// prunePendingPayloadEnvelopes removes entries whose slot is before the finalized epoch's start
// slot, following the same pattern as validatePendingSlots. A root's entries can differ in slot: a
// request by root queues an entry of any slot up to the current one, and the segment path queues a
// reassembled envelope beside other builders' entries. With the segment path on, each entry is
// judged by its own slot, and a root goes once none of its entries is left. With it off, one entry
// decides for the whole root, as before the segment path.
func (s *Service) prunePendingPayloadEnvelopes() {
	s.pendingEnvelopeLock.Lock()
	defer s.pendingEnvelopeLock.Unlock()

	finalizedEpoch := s.cfg.chain.FinalizedCheckpt().Epoch
	segmentsEnabled := s.segmentReassembler != nil
	for root, inner := range s.pendingPayloadEnvelopes {
		for builder, signed := range inner {
			env, err := signed.Envelope()
			stale := err == nil && slots.ToEpoch(env.Slot()) < finalizedEpoch
			if !segmentsEnabled {
				if stale {
					delete(s.pendingPayloadEnvelopes, root)
					delete(s.pendingFetchedEnvelopes, root)
				}
				break
			}
			if stale {
				delete(inner, builder)
				s.forgetFetchedEnvelope(root, builder)
			}
		}
		if segmentsEnabled && len(inner) == 0 {
			delete(s.pendingPayloadEnvelopes, root)
		}
	}
}

// markFetchedEnvelope marks the pending entry a request by root queued for root and builder, whose
// signature nobody checked. The caller holds pendingEnvelopeLock.
func (s *Service) markFetchedEnvelope(root [32]byte, builder uint64, env interfaces.ROSignedExecutionPayloadEnvelope) {
	if s.pendingFetchedEnvelopes == nil {
		s.pendingFetchedEnvelopes = make(map[[32]byte]map[uint64]interfaces.ROSignedExecutionPayloadEnvelope)
	}
	marks := s.pendingFetchedEnvelopes[root]
	if marks == nil {
		marks = make(map[uint64]interfaces.ROSignedExecutionPayloadEnvelope)
		s.pendingFetchedEnvelopes[root] = marks
	}
	marks[builder] = env
}

// isFetchedEnvelope reports whether env, pending for root and builder, is the entry a request by
// root queued unverified: the same message, not an equal one. The caller holds pendingEnvelopeLock.
func (s *Service) isFetchedEnvelope(root [32]byte, builder uint64, env interfaces.ROSignedExecutionPayloadEnvelope) bool {
	marked, ok := s.pendingFetchedEnvelopes[root][builder]
	return ok && marked.Proto() == env.Proto()
}

// forgetFetchedEnvelope drops the mark of root and builder's pending entry, which has left the
// queue or been replaced. The caller holds pendingEnvelopeLock.
func (s *Service) forgetFetchedEnvelope(root [32]byte, builder uint64) {
	marks, ok := s.pendingFetchedEnvelopes[root]
	if !ok {
		return
	}
	delete(marks, builder)
	if len(marks) == 0 {
		delete(s.pendingFetchedEnvelopes, root)
	}
}
