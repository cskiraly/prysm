package partialdatacolumnbroadcaster

import (
	"github.com/OffchainLabs/go-bitfield"
	"github.com/OffchainLabs/prysm/v7/consensus-types/blocks"
	ethpb "github.com/OffchainLabs/prysm/v7/proto/prysm/v1alpha1"
	pubsub_pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/pkg/errors"
)

// The two halves below are what a column and a row have in common when an RPC arrives: the
// peer told us what it has and wants (its parts metadata), and the cells it sent tell us the
// same thing implicitly. Only the message container differs between the axes, so only the
// decode step is per-axis.

func decodePartsMetadataFromPeerState(state *ethpb.PartialDataColumnPartsMetadata, expectedLength uint64) (*ethpb.PartialDataColumnPartsMetadata, error) {
	if state == nil {
		return blocks.NewPartsMetaWithNoAvailableAndNoRequests(expectedLength), nil
	}
	return state, nil
}

// mergeIncomingPartsMetadata folds a peer's parts metadata into what we believe it has. The
// requests bitmap is replaced -- a later metadata supersedes an earlier one -- while available
// is unioned, because a peer never loses a cell.
func mergeIncomingPartsMetadata(peerState blocks.PartialDataColumnPeerState, encoded []byte) (blocks.PartialDataColumnPeerState, error) {
	var incomingMeta ethpb.PartialDataColumnPartsMetadata
	if err := incomingMeta.UnmarshalSSZ(encoded); err != nil {
		return peerState, errors.Wrap(err, "failed to unmarshal incoming parts metadata")
	}
	if incomingMeta.Available.Len() == 0 {
		return peerState, errors.New("incoming parts metadata has 0 length availability")
	}

	if peerState.Recvd == nil {
		peerState.Recvd = &incomingMeta
		return peerState, nil
	}

	if peerState.Recvd.Requests.Len() != incomingMeta.Requests.Len() {
		return peerState, errors.New("failed to merge available cells into recvdState parts metadata. requests length mismatch")
	}
	peerState.Recvd.Requests = incomingMeta.Requests
	merged, err := peerState.Recvd.Available.Or(incomingMeta.Available)
	if err != nil {
		return peerState, errors.Wrap(err, "failed to merge available cells into recvdState parts metadata")
	}
	peerState.Recvd.Available = merged

	return peerState, nil
}

// mergePresentBitmapIntoPeerState folds the bitmap of cells a peer just sent us into both
// halves of the peer state: it clearly has them, and it now knows we have them too.
//
// The received half is skipped when the same RPC also carried explicit parts metadata, which
// is authoritative and has already been applied.
func mergePresentBitmapIntoPeerState(
	peerState blocks.PartialDataColumnPeerState,
	present bitfield.Bitlist,
	hadIncomingPartsMetadata bool,
) (blocks.PartialDataColumnPeerState, error) {
	partCount := present.Len()
	if partCount == 0 {
		return peerState, errors.New("length of cells present bitmap is 0")
	}

	if !hadIncomingPartsMetadata {
		receivedMeta, err := decodePartsMetadataFromPeerState(peerState.Recvd, partCount)
		if err != nil {
			return peerState, errors.Wrap(err, "received")
		}
		recvdState, err := blocks.MergeAvailableIntoPartsMetadata(receivedMeta, present)
		if err != nil {
			return peerState, errors.Wrap(err, "merge available cells into received parts metadata")
		}
		peerState.Recvd = recvdState
	}

	sentMeta, err := decodePartsMetadataFromPeerState(peerState.Sent, partCount)
	if err != nil {
		return peerState, errors.Wrap(err, "sent")
	}
	sentState, err := blocks.MergeAvailableIntoPartsMetadata(sentMeta, present)
	if err != nil {
		return peerState, errors.Wrap(err, "merge available cells into sent parts metadata")
	}
	peerState.Sent = sentState

	return peerState, nil
}

func updatePeerStateFromIncomingRPC(peerState blocks.PartialDataColumnPeerState, rpc *pubsub_pb.PartialMessagesExtension, isGloas bool) (blocks.PartialDataColumnPeerState,
	*ethpb.PartialDataColumnSidecar, error) {
	peerState = peerState.Clone()
	hasIncomingPartsMetadata := len(rpc.PartsMetadata) > 0
	hasMessage := len(rpc.PartialMessage) > 0

	if hasIncomingPartsMetadata {
		var err error
		peerState, err = mergeIncomingPartsMetadata(peerState, rpc.PartsMetadata)
		if err != nil {
			return peerState, nil, err
		}
	}

	// we've already handled the update to the peer state based on the incoming parts metadata,
	// so we can return early if there's no message to process.
	if !hasMessage {
		return peerState, nil, nil
	}

	message, err := blocks.DecodePartialColumnSidecar(rpc.PartialMessage, isGloas)
	if err != nil {
		return peerState, nil, errors.Wrap(err, "failed to unmarshal partial message data")
	}

	present := message.CellsPresentBitmap.Count()
	if uint64(len(message.PartialColumn)) != present || uint64(len(message.KzgProofs)) != present {
		return peerState, nil, errors.Wrap(errMalformedPartialMessage, "cells/proofs count does not match present bitmap")
	}
	if len(message.CellsPresentBitmap) == 0 {
		return peerState, message, nil
	}

	peerState, err = mergePresentBitmapIntoPeerState(peerState, message.CellsPresentBitmap, hasIncomingPartsMetadata)
	if err != nil {
		return peerState, nil, err
	}

	return peerState, message, nil
}

// updateRowPeerStateFromIncomingRPC is the row-axis twin of updatePeerStateFromIncomingRPC.
// The only differences are the message container and the bitmap length, which for a row is
// always NUMBER_OF_COLUMNS rather than the block's blob count.
func updateRowPeerStateFromIncomingRPC(peerState blocks.PartialDataColumnPeerState, rpc *pubsub_pb.PartialMessagesExtension) (blocks.PartialDataColumnPeerState,
	*ethpb.PartialDataRowSidecar, error) {
	peerState = peerState.Clone()
	hasIncomingPartsMetadata := len(rpc.PartsMetadata) > 0
	hasMessage := len(rpc.PartialMessage) > 0

	if hasIncomingPartsMetadata {
		var err error
		peerState, err = mergeIncomingPartsMetadata(peerState, rpc.PartsMetadata)
		if err != nil {
			// Anything the shared merge rejects is a malformed message, and on a row topic
			// that is the peer's fault. The shared helper returns a plain error because the
			// column path is shipped and its scoring is not this branch's decision to
			// change.
			return peerState, nil, errors.Wrap(errMalformedPartialMessage, err.Error())
		}
		// Both bitmaps, not just Available. Requests is the one cellsToSendToPeer intersects
		// with our own bitmap, so a wrong-length Requests poisons this peer's state for the
		// lifetime of the group: every later publish to it fails on a length mismatch.
		if peerState.Recvd.Available.Len() != numberOfColumns {
			return peerState, nil, errors.Wrapf(errMalformedPartialMessage,
				"row parts metadata available length %d, want %d", peerState.Recvd.Available.Len(), numberOfColumns)
		}
		if peerState.Recvd.Requests.Len() != numberOfColumns {
			return peerState, nil, errors.Wrapf(errMalformedPartialMessage,
				"row parts metadata requests length %d, want %d", peerState.Recvd.Requests.Len(), numberOfColumns)
		}
	}

	if !hasMessage {
		return peerState, nil, nil
	}

	message, err := blocks.DecodePartialRowSidecar(rpc.PartialMessage)
	if err != nil {
		return peerState, nil, errors.Wrap(errMalformedPartialMessage, "unmarshal partial row message data: "+err.Error())
	}

	present := message.CellsPresentBitmap.Count()
	if uint64(len(message.PartialRow)) != present || uint64(len(message.KzgProofs)) != present {
		return peerState, nil, errors.Wrap(errMalformedPartialMessage, "cells/proofs count does not match present bitmap")
	}
	// A header-only message carries an empty bitmap, which is legitimate: rows have no
	// full-message form, so the header has to travel on this topic.
	if len(message.CellsPresentBitmap) == 0 {
		return peerState, message, nil
	}
	if message.CellsPresentBitmap.Len() != numberOfColumns {
		return peerState, nil, errors.Wrapf(errMalformedPartialMessage,
			"row cells present bitmap length %d, want %d", message.CellsPresentBitmap.Len(), numberOfColumns)
	}

	peerState, err = mergePresentBitmapIntoPeerState(peerState, message.CellsPresentBitmap, hasIncomingPartsMetadata)
	if err != nil {
		return peerState, nil, errors.Wrap(errMalformedPartialMessage, err.Error())
	}

	return peerState, message, nil
}
