package client

import (
	"context"

	"github.com/OffchainLabs/prysm/v7/beacon-chain/core/signing"
	fieldparams "github.com/OffchainLabs/prysm/v7/config/fieldparams"
	"github.com/OffchainLabs/prysm/v7/config/params"
	"github.com/OffchainLabs/prysm/v7/consensus-types/interfaces"
	"github.com/OffchainLabs/prysm/v7/consensus-types/primitives"
	"github.com/OffchainLabs/prysm/v7/container/segments"
	"github.com/OffchainLabs/prysm/v7/monitoring/tracing/trace"
	ethpb "github.com/OffchainLabs/prysm/v7/proto/prysm/v1alpha1"
	validatorpb "github.com/OffchainLabs/prysm/v7/proto/prysm/v1alpha1/validator-client"
	"github.com/OffchainLabs/prysm/v7/runtime/version"
	"github.com/OffchainLabs/prysm/v7/time/slots"
	"github.com/pkg/errors"
)

// signExecutionPayloadEnvelope signs the execution payload envelope using the
// proposer's key. The envelope is signed with DomainBeaconBuilder since it is
// a builder artifact — even in the self-build case where the proposer acts as
// their own builder.
func (v *validator) signExecutionPayloadEnvelope(
	ctx context.Context,
	pubKey [fieldparams.BLSPubkeyLength]byte,
	slot primitives.Slot,
	envelope *ethpb.ExecutionPayloadEnvelope,
) (*ethpb.SignedExecutionPayloadEnvelope, error) {
	ctx, span := trace.StartSpan(ctx, "validator.signExecutionPayloadEnvelope")
	defer span.End()

	epoch := slots.ToEpoch(slot)

	domain, err := v.domainData(ctx, epoch, params.BeaconConfig().DomainBeaconBuilder[:])
	if err != nil {
		return nil, errors.Wrap(err, "could not get domain data")
	}
	if domain == nil {
		return nil, errors.New("nil domain data")
	}

	signingRoot, err := signing.ComputeSigningRoot(envelope, domain.SignatureDomain)
	if err != nil {
		return nil, errors.Wrap(err, "could not compute signing root")
	}

	sig, err := v.km.Sign(ctx, &validatorpb.SignRequest{
		PublicKey:       pubKey[:],
		SigningRoot:     signingRoot[:],
		SignatureDomain: domain.SignatureDomain,
		Object: &validatorpb.SignRequest_ExecutionPayloadEnvelope{
			ExecutionPayloadEnvelope: envelope,
		},
		SigningSlot: slot,
	})
	if err != nil {
		return nil, errors.Wrap(err, "could not sign execution payload envelope")
	}

	return &ethpb.SignedExecutionPayloadEnvelope{
		Message:   envelope,
		Signature: sig.Marshal(),
	}, nil
}

func (v *validator) proposeSelfBuildEnvelope(
	ctx context.Context,
	slot primitives.Slot,
	pubKey [fieldparams.BLSPubkeyLength]byte,
	blk interfaces.SignedBeaconBlock,
) error {
	if blk.Version() < version.Gloas {
		return nil
	}

	bid, err := blk.Block().Body().SignedExecutionPayloadBid()
	if err != nil {
		return err
	}
	if bid == nil || bid.Message == nil {
		return errors.New("no execution payload bid found in block body")
	}
	if bid.Message.BuilderIndex != params.BeaconConfig().BuilderIndexSelfBuild {
		// only used for self build
		return nil
	}

	blockRoot, err := blk.Block().HashTreeRoot()
	if err != nil {
		return errors.Wrap(err, "could not compute beacon block root")
	}

	envelope, err := v.validatorClient.GetExecutionPayloadEnvelope(ctx, slot, blockRoot)
	if err != nil {
		validatorSelfBuildEnvelopeSubmissionTotal.WithLabelValues("failed").Inc()
		return errors.Wrap(err, "failed to get execution payload envelope for self-build")
	}

	signedEnvelope, err := v.signExecutionPayloadEnvelope(ctx, pubKey, slot, envelope)
	if err != nil {
		validatorSelfBuildEnvelopeSubmissionTotal.WithLabelValues("failed").Inc()
		return errors.Wrap(err, "could not sign execution payload envelope")
	}

	// Missing parameters are not fatal: the node then broadcasts the whole envelope as
	// before. Failing the proposal over an optional optimisation would be worse than not
	// segmenting, so this only logs.
	segmentAuth, err := v.segmentationParams(ctx, signedEnvelope, slot)
	if err != nil {
		log.WithError(err).WithField("slot", slot).
			Debug("Could not derive payload segmentation, publishing envelope unsegmented")
		segmentAuth = nil
	}

	if _, err := v.validatorClient.PublishExecutionPayloadEnvelope(ctx, signedEnvelope, segmentAuth); err != nil {
		validatorSelfBuildEnvelopeSubmissionTotal.WithLabelValues("failed").Inc()
		return errors.Wrap(err, "failed to publish execution payload envelope")
	}
	validatorSelfBuildEnvelopeSubmissionTotal.WithLabelValues("success").Inc()

	return nil
}

// segmentationParams tells the beacon node how to segment the signed envelope.
//
// It used to also carry a builder signature over the descriptor's group id. That signature
// is gone: it authenticated the signer but bounded nothing, since one builder key can sign
// any number of descriptors, so a receiver could be made to admit unboundedly many groups.
// Authority now comes from the block a segment anchors to, and the beacon node derives the
// anchor from the envelope's own slot and beacon_block_root -- neither of which needs the
// builder key, which is why this no longer signs anything.
//
// The segmentation still has to be decided here rather than in the beacon node, because
// segment size and hash choice must match what the builder committed to.
func (v *validator) segmentationParams(
	ctx context.Context,
	signed *ethpb.SignedExecutionPayloadEnvelope,
	slot primitives.Slot,
) (*ethpb.PayloadSegmentAuth, error) {
	_, span := trace.StartSpan(ctx, "validator.segmentationParams")
	defer span.End()

	encoded, err := signed.MarshalSSZ()
	if err != nil {
		return nil, errors.Wrap(err, "could not marshal signed envelope")
	}
	hasher, err := segments.HasherByID(segments.HashSHA256)
	if err != nil {
		return nil, errors.Wrap(err, "could not get segment hasher")
	}
	descriptor, _, err := segments.Commit(encoded, segments.DefaultSegmentSize, hasher)
	if err != nil {
		return nil, errors.Wrap(err, "could not commit to envelope segments")
	}
	return &ethpb.PayloadSegmentAuth{
		SegmentSize: descriptor.SegmentSize,
		HashId:      uint32(hasher.ID()),
		Slot:        slot,
	}, nil
}
