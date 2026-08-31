package peerdas

import (
	"encoding/binary"

	"github.com/OffchainLabs/prysm/v7/beacon-chain/blockchain/kzg"
	"github.com/OffchainLabs/prysm/v7/beacon-chain/core/helpers"
	fieldparams "github.com/OffchainLabs/prysm/v7/config/fieldparams"
	"github.com/OffchainLabs/prysm/v7/config/params"
	"github.com/OffchainLabs/prysm/v7/consensus-types/blocks"
	"github.com/OffchainLabs/prysm/v7/consensus-types/primitives"
	"github.com/OffchainLabs/prysm/v7/crypto/hash"
	"github.com/OffchainLabs/prysm/v7/encoding/bytesutil"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/holiman/uint256"
	"github.com/pkg/errors"
)

// rowSubnetDomain separates the per-slot blob-to-subnet shuffle from every other use of
// compute_shuffled_index.
var rowSubnetDomain = []byte("ROW_SUBNET")

var (
	// ErrRowSubnetCountZero is returned when ROW_SUBNET_COUNT is unset. RowDAS cannot be
	// used with zero subnets; a configuration that sets it to zero is disabling the
	// feature and callers must handle that rather than dividing by it.
	ErrRowSubnetCountZero = errors.New("ROW_SUBNET_COUNT is zero")
)

// hashNodeID returns hash(uint_to_bytes(uint256(node_id))), the hash both the custody-group
// and the row-subnet assignments are derived from.
//
// The spec's uint_to_bytes is little endian while an enode.ID is big endian, hence the
// reversal. Custody uses bytes [0:8] of this hash and rows use bytes [8:16], which is the
// whole of the domain separation between the two assignments — so they are deliberately
// computed in one place.
func hashNodeID(id *uint256.Int) [32]byte {
	bigEndian := id.Bytes32()
	return hash.Hash(bytesutil.ReverseByteOrder(bigEndian[:]))
}

// RowSubnetForNode computes the single row subnet a node participates in. Membership is a
// pure function of the node ID, so any peer's row subnet is computable from its ENR alone --
// no ENR entry and no MetaData field is needed.
//
// https://eips.ethereum.org/EIPS/eip-8371
//
//	def get_row_subnet(node_id: NodeID) -> RowSubnetIndex:
//	    return RowSubnetIndex(
//	        bytes_to_uint64(hash(uint_to_bytes(uint256(node_id)))[8:16])
//	        % ROW_SUBNET_COUNT
//	    )
func RowSubnetForNode(nodeID enode.ID) (uint64, error) {
	rowSubnetCount := params.BeaconConfig().RowSubnetCount
	if rowSubnetCount == 0 {
		return 0, ErrRowSubnetCountZero
	}

	hashed := hashNodeID(new(uint256.Int).SetBytes(nodeID.Bytes()))

	return binary.LittleEndian.Uint64(hashed[8:16]) % rowSubnetCount, nil
}

// rowSubnetSeed returns the per-slot seed for the blob-to-subnet permutation.
func rowSubnetSeed(slot primitives.Slot) [32]byte {
	input := make([]byte, 0, len(rowSubnetDomain)+8)
	input = append(input, rowSubnetDomain...)
	input = append(input, bytesutil.Uint64ToBytesLittleEndian(uint64(slot))...)

	return hash.Hash(input)
}

// RowSubnetForBlob computes the row subnet carrying a given blob of a given slot. The
// permutation is redrawn every slot so that a node's row duty is spread evenly over a short
// horizon; a fixed rotation would instead give each node bursts of work separated by idle
// stretches.
//
// While the blob count does not exceed ROW_SUBNET_COUNT the mapping is injective, so a row
// subnet carries at most one blob per slot and BlobsForRowSubnet is its exact inverse.
//
// https://eips.ethereum.org/EIPS/eip-8371
//
//	def get_blob_row_subnet(blob_index: BlobIndex, slot: Slot) -> RowSubnetIndex:
//	    seed = hash(b"ROW_SUBNET" + uint_to_bytes(uint64(slot)))
//	    return RowSubnetIndex(
//	        compute_shuffled_index(
//	            uint64(blob_index) % ROW_SUBNET_COUNT, uint64(ROW_SUBNET_COUNT), seed
//	        )
//	    )
func RowSubnetForBlob(blobIndex uint64, slot primitives.Slot) (uint64, error) {
	rowSubnetCount := params.BeaconConfig().RowSubnetCount
	if rowSubnetCount == 0 {
		return 0, ErrRowSubnetCountZero
	}

	shuffled, err := helpers.ShuffledIndex(
		primitives.ValidatorIndex(blobIndex%rowSubnetCount),
		rowSubnetCount,
		rowSubnetSeed(slot),
	)
	if err != nil {
		return 0, errors.Wrap(err, "shuffled index")
	}

	return uint64(shuffled), nil
}

// BlobsForRowSubnet returns the blob indices carried by a row subnet in a given slot, in
// ascending order. It is the exact inverse of RowSubnetForBlob.
//
// The result is empty when no blob maps to this subnet, which is the common case: with 6
// blobs and 128 subnets only 6 subnets carry anything, so most nodes have no row duty in most
// slots. Callers must treat an empty result as "nothing to do", not as an error.
//
// More than one index is returned only when the blob count exceeds ROW_SUBNET_COUNT. See
// notes/rowdas/design.md section 6.1: the wire format does not yet carry two independent
// bitmaps for one group, so that case is reported here but not handled downstream.
func BlobsForRowSubnet(subnet uint64, slot primitives.Slot, blobCount uint64) ([]uint64, error) {
	rowSubnetCount := params.BeaconConfig().RowSubnetCount
	if rowSubnetCount == 0 {
		return nil, ErrRowSubnetCountZero
	}
	if subnet >= rowSubnetCount {
		return nil, errors.Errorf("row subnet %d out of range for ROW_SUBNET_COUNT %d", subnet, rowSubnetCount)
	}

	// Invert the permutation to recover blobIndex % ROW_SUBNET_COUNT, then walk the
	// residue class.
	unshuffled, err := helpers.UnShuffledIndex(primitives.ValidatorIndex(subnet), rowSubnetCount, rowSubnetSeed(slot))
	if err != nil {
		return nil, errors.Wrap(err, "unshuffled index")
	}

	blobIndices := make([]uint64, 0, 1)
	for blobIndex := uint64(unshuffled); blobIndex < blobCount; blobIndex += rowSubnetCount {
		blobIndices = append(blobIndices, blobIndex)
	}

	return blobIndices, nil
}

// IsRowReconstructor reports whether a node custodying the given number of columns is a row
// reconstructor, meaning it holds enough cells of every row to reconstruct that row on its
// own. Every supernode is a row reconstructor.
func IsRowReconstructor(custodyColumnCount uint64) bool {
	return custodyColumnCount >= MinimumColumnCountToReconstruct()
}

// ErrRowBelowReconstructionThreshold is returned when a row does not hold enough cells to
// recover the rest.
var ErrRowBelowReconstructionThreshold = errors.New("row holds fewer cells than the reconstruction threshold")

// RecoverRow fills in a row's missing cells and proofs from the ones it already holds,
// extending the row in place. It is a no-op on a complete row.
//
// This is the per-blob half of PeerDAS reconstruction, addressed one row at a time: RowDAS
// exists so that one node recovers one row rather than every reconstructor recovering every
// row. The recovered proofs are correct by construction, so they are not re-verified -- the
// same assumption ReconstructDataColumnSidecars makes.
//
// The caller must have verified the cells already present; recovery over unverified cells
// produces garbage that then looks verified.
func RecoverRow(row *blocks.PartialDataRow) error {
	if row.IsComplete() {
		return nil
	}
	if !row.ReconstructionThresholdMet() {
		return ErrRowBelowReconstructionThreshold
	}

	// The KZG bindings require ascending cell indices, which is the order
	// PresentColumnIndices returns.
	columnIndices := row.PresentColumnIndices()
	partialCells := make([]kzg.Cell, 0, len(columnIndices))
	for _, columnIndex := range columnIndices {
		var cell kzg.Cell
		if len(row.Cells[columnIndex]) != len(cell) {
			return errors.Wrapf(ErrMismatchLength, "cell at column %d", columnIndex)
		}
		copy(cell[:], row.Cells[columnIndex])
		partialCells = append(partialCells, cell)
	}

	cells, proofs, err := kzg.RecoverCellsAndKZGProofs(columnIndices, partialCells)
	if err != nil {
		return errors.Wrap(err, "recover cells and kzg proofs")
	}
	if len(cells) != fieldparams.NumberOfColumns || len(proofs) != fieldparams.NumberOfColumns {
		return errors.Errorf("recovery returned %d cells and %d proofs, want %d of each",
			len(cells), len(proofs), fieldparams.NumberOfColumns)
	}

	for columnIndex := range uint64(fieldparams.NumberOfColumns) {
		if row.Included.BitAt(columnIndex) {
			continue
		}
		cell := cells[columnIndex]
		proof := proofs[columnIndex]
		if !row.ExtendFromVerifiedCell(columnIndex, cell[:], proof[:]) {
			return errors.Errorf("failed to extend row with recovered cell at column %d", columnIndex)
		}
	}

	return nil
}
