package sync

import (
	"context"
	"time"

	"github.com/OffchainLabs/prysm/v7/beacon-chain/blockchain"
	"github.com/OffchainLabs/prysm/v7/beacon-chain/core/feed"
	"github.com/OffchainLabs/prysm/v7/beacon-chain/core/feed/operation"
	"github.com/OffchainLabs/prysm/v7/beacon-chain/core/helpers"
	"github.com/OffchainLabs/prysm/v7/beacon-chain/p2p"
	"github.com/OffchainLabs/prysm/v7/beacon-chain/verification"
	fieldparams "github.com/OffchainLabs/prysm/v7/config/fieldparams"
	"github.com/OffchainLabs/prysm/v7/config/params"
	"github.com/OffchainLabs/prysm/v7/consensus-types/blocks"
	"github.com/OffchainLabs/prysm/v7/consensus-types/interfaces"
	"github.com/OffchainLabs/prysm/v7/consensus-types/primitives"
	"github.com/OffchainLabs/prysm/v7/crypto/rand"
	"github.com/OffchainLabs/prysm/v7/encoding/bytesutil"
	"github.com/OffchainLabs/prysm/v7/monitoring/tracing"
	"github.com/OffchainLabs/prysm/v7/monitoring/tracing/trace"
	ethpb "github.com/OffchainLabs/prysm/v7/proto/prysm/v1alpha1"
	prysmTime "github.com/OffchainLabs/prysm/v7/time"
	"github.com/OffchainLabs/prysm/v7/time/slots"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"
)

func (s *Service) validateExecutionPayloadEnvelope(ctx context.Context, pid peer.ID, msg *pubsub.Message) (pubsub.ValidationResult, error) {
	if pid == s.cfg.p2p.PeerID() {
		return pubsub.ValidationAccept, nil
	}
	if s.cfg.initialSync.Syncing() {
		return pubsub.ValidationIgnore, nil
	}
	receivedTime := prysmTime.Now()

	ctx, span := trace.StartSpan(ctx, "sync.validateExecutionPayloadEnvelope")
	defer span.End()

	if msg.Topic == nil {
		return pubsub.ValidationReject, p2p.ErrInvalidTopic
	}

	s.validateEnvelopeLock.Lock()
	defer s.validateEnvelopeLock.Unlock()

	m, err := s.decodePubsubMessage(msg)
	if err != nil {
		tracing.AnnotateError(span, err)
		return pubsub.ValidationReject, err
	}

	var e interfaces.ROSignedExecutionPayloadEnvelope
	switch t := m.(type) {
	case *ethpb.SignedExecutionPayloadEnvelope:
		e, err = blocks.WrappedROSignedExecutionPayloadEnvelope(t)
	case *ethpb.SignedExecutionPayloadEnvelopePQ:
		e, err = blocks.WrappedROSignedExecutionPayloadEnvelopePQ(t)
	default:
		return pubsub.ValidationReject, errWrongMessage
	}
	if err != nil {
		log.WithError(err).Error("failed to create read only signed payload execution envelope")
		return pubsub.ValidationIgnore, err
	}
	res, err := s.admitExecutionPayloadEnvelope(ctx, e, receivedTime, envelopeFromGossip)
	if res == pubsub.ValidationAccept {
		msg.ValidatorData = e.Proto()
	}
	return res, err
}

// envelopeSource is the path an envelope came by.
type envelopeSource int

const (
	// envelopeFromGossip is a whole envelope from the envelope topic.
	envelopeFromGossip envelopeSource = iota
	// envelopeFromSegments is an envelope the node reassembled from a group of segments.
	envelopeFromSegments
)

// admitExecutionPayloadEnvelope runs the envelope topic's checks on an envelope, whichever path it
// came by, so that a reassembled envelope gets exactly what a whole one gets, branch by branch. With
// the block known it returns accept once the envelope passed, marked it seen, recorded its arrival
// and emitted the gossip event; the caller imports it. With the block unknown it queues the envelope
// for the block, and returns ignore. The caller holds validateEnvelopeLock.
func (s *Service) admitExecutionPayloadEnvelope(
	ctx context.Context,
	e interfaces.ROSignedExecutionPayloadEnvelope,
	receivedTime time.Time,
	source envelopeSource,
) (pubsub.ValidationResult, error) {
	v := s.newExecutionPayloadEnvelopeVerifier(e, verification.GossipExecutionPayloadEnvelopeRequirements)

	env, err := e.Envelope()
	if err != nil {
		return pubsub.ValidationIgnore, err
	}

	// [IGNORE] The envelope's block root envelope.block_root has been seen (via gossip or non-gossip sources)
	// (a client MAY queue payload for processing once the block is retrieved).
	if err := v.VerifyBlockRootSeen(func(root [32]byte) bool { return s.cfg.chain.HasBlock(ctx, root) }); err != nil {
		return s.queuePendingPayloadEnvelopeFrom(ctx, v, env, e, source, receivedTime)
	}
	root := env.BeaconBlockRoot()
	// [IGNORE] The node has not seen another valid SignedExecutionPayloadEnvelope for this block root from this builder.
	if s.hasSeenPayloadEnvelope(root, env.BuilderIndex()) || s.cfg.chain.HasFullNode(root) {
		return pubsub.ValidationIgnore, nil
	}
	finalized := s.cfg.chain.FinalizedCheckpt()
	if finalized == nil {
		return pubsub.ValidationIgnore, errors.New("nil finalized checkpoint")
	}
	// [IGNORE] The envelope is from a slot greater than or equal to the latest finalized slot --
	// i.e. validate that envelope.slot >= compute_start_slot_at_epoch(store.finalized_checkpoint.epoch).
	if err := v.VerifySlotAboveFinalized(finalized.Epoch); err != nil {
		return pubsub.ValidationIgnore, err
	}
	// [REJECT] block passes validation.
	if err := v.VerifyBlockRootValid(s.hasBadBlock); err != nil {
		return pubsub.ValidationReject, err
	}

	// Let block be the block with envelope.beacon_block_root.
	block, err := s.cfg.beaconDB.Block(ctx, root)
	if err != nil {
		return pubsub.ValidationIgnore, err
	}
	// A seen block may live in the init-sync cache but not yet in the DB, so Block can return (nil, nil).
	if err := blocks.BeaconBlockIsNil(block); err != nil {
		return pubsub.ValidationIgnore, nil
	}
	// [REJECT] block.slot equals envelope.slot.
	if err := v.VerifySlotMatchesBlock(block.Block().Slot()); err != nil {
		return pubsub.ValidationReject, err
	}

	// Let bid alias block.body.signed_execution_payload_bid.message
	// (notice that this can be obtained from the state.latest_execution_payload_bid).
	bid, err := block.Block().Body().ExecutionPayloadBid()
	if err != nil {
		return pubsub.ValidationIgnore, err
	}
	// [REJECT] envelope.builder_index == bid.builder_index.
	if err := v.VerifyBuilderValid(bid); err != nil {
		return pubsub.ValidationReject, err
	}
	// [REJECT] payload.block_hash == bid.block_hash.
	if err := v.VerifyPayloadHash(bid); err != nil {
		return pubsub.ValidationReject, err
	}
	// [REJECT] hash_tree_root(envelope.execution_requests) == bid.execution_requests_root.
	if err := v.VerifyExecutionRequestsRoot(bid); err != nil {
		return pubsub.ValidationReject, err
	}
	// [REJECT] The execution request counts are within their limits.
	if err := v.VerifyExecutionRequestsLimits(); err != nil {
		return pubsub.ValidationReject, err
	}
	// [REJECT] The number of withdrawals is within the limit.
	if err := v.VerifyWithdrawalsLimit(); err != nil {
		return pubsub.ValidationReject, err
	}

	var pubkey [fieldparams.BLSPubkeyLength]byte
	if env.BuilderIndex() == params.BeaconConfig().BuilderIndexSelfBuild {
		pubkey, err = s.cfg.chain.HeadValidatorIndexToPublicKey(ctx, block.Block().ProposerIndex())
		if err != nil {
			return pubsub.ValidationIgnore, err
		}
	} else {
		pk, err := s.cfg.chain.BuilderPubkey(root)
		if err != nil {
			return pubsub.ValidationIgnore, err
		}
		if pk == nil {
			return pubsub.ValidationIgnore, errors.New("unknown builder pubkey")
		}
		pubkey = *pk
	}
	if pubkey == [fieldparams.BLSPubkeyLength]byte{} {
		return pubsub.ValidationIgnore, errors.New("unknown pubkey")
	}

	// [REJECT] signed_execution_payload_envelope.signature is valid with respect to the builder's public key.
	if err := v.VerifySignatureWithPubkey(pubkey, s.cfg.clock.GenesisValidatorsRoot()); err != nil {
		return pubsub.ValidationReject, err
	}
	s.setSeenPayloadEnvelope(root, env.BuilderIndex())
	s.recordEnvelopeArrival(env, receivedTime, source)

	// execution_payload_gossip fires once the envelope passes gossip validation.
	if s.cfg.operationNotifier != nil {
		s.cfg.operationNotifier.OperationFeed().Send(&feed.Event{
			Type: operation.ExecutionPayloadGossipReceived,
			Data: &operation.ExecutionPayloadGossipReceivedData{
				Slot:         env.Slot(),
				BuilderIndex: env.BuilderIndex(),
				BlockHash:    env.BlockHash(),
				BlockRoot:    env.BeaconBlockRoot(),
			},
		})
	}

	return pubsub.ValidationAccept, nil
}

// recordEnvelopeArrival observes the time from the envelope's slot start to receivedTime, in the
// histogram of the path it came by.
func (s *Service) recordEnvelopeArrival(env interfaces.ROExecutionPayloadEnvelope, receivedTime time.Time, source envelopeSource) {
	startTime, err := slots.StartTime(s.cfg.clock.GenesisTime(), env.Slot())
	if err != nil {
		log.WithError(err).WithField("slot", env.Slot()).Debug("Could not compute execution payload envelope slot start time")
		return
	}
	h := syncExecutionPayloadEnvelopeArrivalDelaySeconds
	if source == envelopeFromSegments {
		h = segmentEnvelopeArrivalDelaySeconds
	}
	h.Observe(receivedTime.Sub(startTime).Seconds())
}

// queuePendingPayloadEnvelope verifies the builder signature and queues the
// envelope for processing once the corresponding block arrives.
func (s *Service) queuePendingPayloadEnvelope(
	ctx context.Context,
	v verification.ExecutionPayloadEnvelopeVerifier,
	env interfaces.ROExecutionPayloadEnvelope,
	signedEnvelope interfaces.ROSignedExecutionPayloadEnvelope,
) (pubsub.ValidationResult, error) {
	return s.queuePendingPayloadEnvelopeFrom(ctx, v, env, signedEnvelope, envelopeFromGossip, time.Time{})
}

// queuePendingPayloadEnvelopeFrom is queuePendingPayloadEnvelope for an envelope that came by source.
//
// An envelope reassembled from segments differs in three ways. It replaces the entries a request by
// root queued unverified, for its builder or of another slot, since such an entry could otherwise
// decline it, while the reassembler hands each envelope over once; makeRoomForReassembledEnvelope
// gives the rule. Its arrival is recorded once it is queued. And if its block reached forkchoice
// while it was being checked, it is processed at once rather than at the next sweep.
func (s *Service) queuePendingPayloadEnvelopeFrom(
	ctx context.Context,
	v verification.ExecutionPayloadEnvelopeVerifier,
	env interfaces.ROExecutionPayloadEnvelope,
	signedEnvelope interfaces.ROSignedExecutionPayloadEnvelope,
	source envelopeSource,
	receivedTime time.Time,
) (pubsub.ValidationResult, error) {
	currentSlot := s.cfg.clock.CurrentSlot()
	if env.Slot() != currentSlot {
		log.WithField("envelopeSlot", env.Slot()).WithField("currentSlot", currentSlot).Debug("Ignoring payload envelope not for current slot")
		return pubsub.ValidationIgnore, nil
	}
	st, err := s.cfg.chain.HeadStateReadOnly(ctx)
	if err != nil {
		return pubsub.ValidationIgnore, err
	}
	currentEpoch := slots.ToEpoch(currentSlot)
	stateEpoch := slots.ToEpoch(st.Slot())
	proposerInLookahead := (stateEpoch == currentEpoch || stateEpoch+1 == currentEpoch)
	builderIdx := uint64(env.BuilderIndex())
	isSelfBuild := builderIdx == uint64(params.BeaconConfig().BuilderIndexSelfBuild)
	root := env.BeaconBlockRoot()
	s.pendingEnvelopeLock.Lock()
	defer s.pendingEnvelopeLock.Unlock()
	inner, rootExists := s.pendingPayloadEnvelopes[root]
	fromSegments := source == envelopeFromSegments
	// The caps bound what unverified envelopes can add. A reassembled one adds no root when its root
	// is queued, and its root's entries cap it by the segment path's rule
	// (makeRoomForReassembledEnvelope).
	if !isSelfBuild && !(fromSegments && rootExists) && len(s.pendingPayloadEnvelopes) >= maxPendingPayloadRoots {
		log.Debug("Too many pending payload roots, ignoring new payload envelope")
		return pubsub.ValidationIgnore, nil
	}
	if !isSelfBuild && !fromSegments && len(inner) >= maxPendingBuildersPerRoot {
		log.Debug("Too many pending builders for root, ignoring new payload envelope")
		return pubsub.ValidationIgnore, nil
	}

	// Per-slot self-build state resets each slot.
	if isSelfBuild && s.selfBuildSlot != currentSlot {
		s.selfBuildSlot = currentSlot
		s.selfBuildSigFailures = 0
		s.selfBuildSeenProposers = make(map[primitives.ValidatorIndex]struct{})
	}
	if isSelfBuild && s.selfBuildSigFailures >= maxSelfBuildSigFailures {
		log.Debug("Ignoring self-built payload envelope because of too many signature failures")
		return pubsub.ValidationIgnore, nil
	}

	if !isSelfBuild || proposerInLookahead {
		if isSelfBuild {
			// Cap self-build at one queued payload per valid proposer per slot.
			proposerIdx, err := helpers.BeaconProposerIndexAtSlot(ctx, st, currentSlot)
			if err != nil {
				return pubsub.ValidationIgnore, err
			}
			if _, seen := s.selfBuildSeenProposers[proposerIdx]; seen {
				log.Debug("Already queued a self-built payload for this proposer this slot, ignoring")
				return pubsub.ValidationIgnore, nil
			}
			if err := v.VerifySignature(ctx, st); err != nil {
				s.selfBuildSigFailures++
				log.WithError(err).Debug("Ignoring self-built payload with invalid signature")
				return pubsub.ValidationIgnore, nil
			}
			// Record only after a valid signature so a bad envelope cannot reserve the proposer's slot.
			s.selfBuildSeenProposers[proposerIdx] = struct{}{}
		} else if err := v.VerifySignature(ctx, st); err != nil {
			// The envelope's block is unknown, so the head state used here may be on a different branch, do not penalize the peer.
			return pubsub.ValidationIgnore, err
		}
	} else {
		log.Debug("Ignoring payload envelope from self-build outside of the Lookahead window")
		return pubsub.ValidationIgnore, nil
	}
	if !rootExists {
		inner = make(map[uint64]interfaces.ROSignedExecutionPayloadEnvelope)
		s.pendingPayloadEnvelopes[root] = inner
	} else if !fromSegments {
		for existingBuilderIdx, existing := range inner {
			existingEnv, err := existing.Envelope()
			if err != nil {
				delete(inner, existingBuilderIdx)
				s.forgetFetchedEnvelope(root, existingBuilderIdx)
				log.Debug("Removed malformed pending payload envelope")
				continue
			}
			// A root's entries can differ in slot, after a request by root or a reassembly. With the
			// segment path on, every entry is checked; with it off, the first well-formed one
			// decides, as before the segment path.
			if existingEnv.Slot() != env.Slot() {
				log.Debug("Ignoring payload envelope with mismatched slot")
				return pubsub.ValidationIgnore, nil
			}
			if s.segmentReassembler == nil {
				break
			}
		}
	}
	if fromSegments {
		if !s.makeRoomForReassembledEnvelope(root, builderIdx, inner, signedEnvelope, env.Slot(), isSelfBuild) {
			return pubsub.ValidationIgnore, nil
		}
	} else if _, exists := inner[builderIdx]; exists {
		log.Debug("Already have a pending payload envelope for this builder and root, ignoring")
		return pubsub.ValidationIgnore, nil
	}
	inner[builderIdx] = signedEnvelope
	s.forgetFetchedEnvelope(root, builderIdx)

	s.pendingQueueLock.RLock()
	inPendingQueue := s.seenPendingBlocks[root]
	s.pendingQueueLock.RUnlock()
	inForkchoice := s.cfg.chain.InForkchoice(root)
	if !rootExists && !inPendingQueue && !inForkchoice && !s.cfg.chain.BlockBeingSynced(root) {
		if fromSegments {
			// The sender of the group's last segment relayed segments, which needs no block, so it
			// is not answerable for serving the block: the best peers are asked, as for any envelope.
			s.segmentWork.Go(func() {
				if err := s.sendBatchRootRequest(s.ctx, [][32]byte{root}, rand.NewGenerator()); err != nil {
					log.WithError(err).Debug("Could not request the block of a reassembled payload envelope")
				}
			})
		} else {
			go func() {
				if err := s.sendBatchRootRequest(s.ctx, [][32]byte{root}, rand.NewGenerator()); err != nil {
					log.WithError(err).Debug("Could not request beacon block for pending payload envelope")
				}
			}()
		}
	}
	if fromSegments {
		s.recordEnvelopeArrival(env, receivedTime, source)
		if inForkchoice {
			// The block reached forkchoice while the envelope was checked, after the block's
			// subscriber looked here for its envelopes.
			s.segmentWork.Go(func() { s.processPendingPayloadEnvelope(s.ctx, root) })
		}
	}
	return pubsub.ValidationIgnore, nil
}

// makeRoomForReassembledEnvelope applies the segment path's rule to root's pending entries, inner,
// before signedEnvelope, reassembled with its signature checked, is queued for builderIdx. It
// reports whether signedEnvelope may be queued, and removes entries only when it may. The caller
// holds pendingEnvelopeLock.
//
// Only an entry a request by root queued unverified gives way, and a malformed one. An entry whose
// signature was checked stays. If it is the builder's own and differs, the reassembled envelope is
// not queued: the builder's first stays, as on the gossip path. If it is another builder's, the
// reassembled envelope is queued beside it, whatever their slots: while the block is unknown, a
// checked signature fixes neither the block's slot nor its builder. When the block arrives, every
// entry is tried, and the chain imports only the one the block names.
//
// A root holds at most maxPendingBuildersPerRoot entries for a builder's envelope, as on the gossip
// path, and a self-built one is exempt, as there. When the root is full, one entry fetched
// unverified gives way if that makes room; otherwise the reassembled envelope is not queued, and
// nothing is removed.
func (s *Service) makeRoomForReassembledEnvelope(
	root [32]byte,
	builderIdx uint64,
	inner map[uint64]interfaces.ROSignedExecutionPayloadEnvelope,
	signedEnvelope interfaces.ROSignedExecutionPayloadEnvelope,
	slot primitives.Slot,
	isSelfBuild bool,
) bool {
	existing := inner[builderIdx]
	_, replaces := pendingEnvelopeOf(existing)
	if replaces && !proto.Equal(existing.Proto(), signedEnvelope.Proto()) && !s.isFetchedEnvelope(root, builderIdx, existing) {
		log.WithField("slot", slot).Debug("Reassembled payload envelope differs from its builder's verified pending one, ignoring")
		return false
	}

	var discard []uint64
	var fetchedBuilder uint64
	var canEvict bool
	for builder, entry := range inner {
		entryEnv, wellFormed := pendingEnvelopeOf(entry)
		fetched := wellFormed && s.isFetchedEnvelope(root, builder, entry)
		if !wellFormed || (fetched && entryEnv.Slot() != slot) {
			discard = append(discard, builder)
			if builder == builderIdx {
				replaces = false
			}
			continue
		}
		if fetched {
			fetchedBuilder, canEvict = builder, true
		}
	}
	remaining := len(inner) - len(discard)
	if !isSelfBuild && !replaces && remaining >= maxPendingBuildersPerRoot {
		if remaining > maxPendingBuildersPerRoot || !canEvict {
			log.WithField("slot", slot).Debug("Too many pending payload envelopes for root, ignoring reassembled one")
			return false
		}
		discard = append(discard, fetchedBuilder)
	}
	for _, builder := range discard {
		delete(inner, builder)
		s.forgetFetchedEnvelope(root, builder)
	}
	return true
}

// pendingEnvelopeOf returns a pending entry's envelope, and whether the entry is well formed: one
// whose envelope the read-only wrapper gives, as the gossip path's check of malformed entries asks.
func pendingEnvelopeOf(e interfaces.ROSignedExecutionPayloadEnvelope) (interfaces.ROExecutionPayloadEnvelope, bool) {
	if e == nil {
		return nil, false
	}
	env, err := e.Envelope()
	return env, err == nil
}

func (s *Service) executionPayloadEnvelopeSubscriber(ctx context.Context, msg proto.Message) error {
	var env interfaces.ROSignedExecutionPayloadEnvelope
	var err error
	switch t := msg.(type) {
	case *ethpb.SignedExecutionPayloadEnvelope:
		env, err = blocks.WrappedROSignedExecutionPayloadEnvelope(t)
	case *ethpb.SignedExecutionPayloadEnvelopePQ:
		env, err = blocks.WrappedROSignedExecutionPayloadEnvelopePQ(t)
	default:
		return errWrongMessage
	}
	if err != nil {
		return errors.Wrap(err, "could not wrap signed execution payload envelope")
	}
	if err := s.cfg.chain.ReceiveExecutionPayloadEnvelope(ctx, env); err != nil {
		if blockchain.IsInvalidBlock(err) {
			envelope, envErr := env.Envelope()
			if envErr == nil {
				s.setBadPayload(ctx, envelope.BeaconBlockRoot())
			} else {
				log.WithError(envErr).Error("failed to get envelope from signed execution payload envelope")
			}
		}
		return err
	}
	if s.chainIsStarted() {
		go func() {
			if err := s.processPendingBlocks(s.ctx); err != nil {
				log.WithError(err).Debug("Could not process pending blocks after envelope receipt")
			}
		}()
	}
	return nil
}

func (s *Service) hasSeenPayloadEnvelope(root [32]byte, builderIdx primitives.BuilderIndex) bool {
	if s.seenPayloadEnvelopeCache == nil {
		return false
	}

	b := append(bytesutil.Bytes32(uint64(builderIdx)), root[:]...)
	_, seen := s.seenPayloadEnvelopeCache.Get(string(b))
	return seen
}

func (s *Service) setSeenPayloadEnvelope(root [32]byte, builderIdx primitives.BuilderIndex) {
	if s.seenPayloadEnvelopeCache == nil {
		return
	}

	b := append(bytesutil.Bytes32(uint64(builderIdx)), root[:]...)
	s.seenPayloadEnvelopeCache.Add(string(b), true)
}
