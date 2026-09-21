package peerdas_test

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math/rand"
	"testing"

	"github.com/OffchainLabs/go-bitfield"
	"github.com/OffchainLabs/prysm/v7/beacon-chain/core/peerdas"
	fieldparams "github.com/OffchainLabs/prysm/v7/config/fieldparams"
	"github.com/OffchainLabs/prysm/v7/config/params"
	"github.com/OffchainLabs/prysm/v7/consensus-types/primitives"
	"github.com/OffchainLabs/prysm/v7/testing/require"
	"github.com/ethereum/go-ethereum/p2p/enode"
)

// nodeIDs returns count deterministic node IDs.
func nodeIDs(count int) []enode.ID {
	rng := rand.New(rand.NewSource(0x8371))
	ids := make([]enode.ID, count)
	for i := range ids {
		_, _ = rng.Read(ids[i][:])
	}

	return ids
}

// referenceRowSubnetForNode re-derives get_row_subnet straight from the EIP-8371 pseudocode,
// deliberately without reusing any production helper, so the test fails if the derivation in
// rows.go drifts from the spec.
//
//	bytes_to_uint64(hash(uint_to_bytes(uint256(node_id)))[8:16]) % ROW_SUBNET_COUNT
func referenceRowSubnetForNode(t *testing.T, id enode.ID, rowSubnetCount uint64) uint64 {
	t.Helper()

	// uint_to_bytes is little endian; an enode.ID is big endian.
	littleEndian := make([]byte, len(id))
	for i := range id {
		littleEndian[i] = id[len(id)-1-i]
	}
	sum := sha256.Sum256(littleEndian)

	return binary.LittleEndian.Uint64(sum[8:16]) % rowSubnetCount
}

func TestRowSubnetForNode_MatchesSpecDerivation(t *testing.T) {
	rowSubnetCount := params.BeaconConfig().RowSubnetCount
	require.Equal(t, uint64(128), rowSubnetCount, "mainnet ROW_SUBNET_COUNT")

	for i, id := range nodeIDs(64) {
		got, err := peerdas.RowSubnetForNode(id)
		require.NoError(t, err)
		require.Equal(t, referenceRowSubnetForNode(t, id, rowSubnetCount), got, fmt.Sprintf("node %d", i))
	}
}

// TestRowSubnetForNode_IndependentOfCustody checks the property the EIP's choice of hash bytes
// [8:16] exists to provide: the row subnet is not a function of the custody assignment. If a
// future change derived rows from bytes [0:8] this test would fail, because the row subnet
// would then be determined by the first custody group.
func TestRowSubnetForNode_IndependentOfCustody(t *testing.T) {
	custodyRequirement := params.BeaconConfig().CustodyRequirement

	// Group nodes by their first custody group and check the row subnets within a group are
	// not all equal.
	rowSubnetsByFirstCustodyGroup := make(map[uint64]map[uint64]bool)
	for _, id := range nodeIDs(2048) {
		groups, err := peerdas.CustodyGroups(id, custodyRequirement)
		require.NoError(t, err)
		subnet, err := peerdas.RowSubnetForNode(id)
		require.NoError(t, err)

		if rowSubnetsByFirstCustodyGroup[groups[0]] == nil {
			rowSubnetsByFirstCustodyGroup[groups[0]] = make(map[uint64]bool)
		}
		rowSubnetsByFirstCustodyGroup[groups[0]][subnet] = true
	}

	var spread int
	for _, subnets := range rowSubnetsByFirstCustodyGroup {
		if len(subnets) > 1 {
			spread++
		}
	}
	require.Equal(t, true, spread > 0, "row subnets are constant within every custody group, so rows are not independent of custody")
}

func TestRowSubnetForNode_StableUnderUnrelatedConfigChanges(t *testing.T) {
	// A node's row subnet must survive forks and blob-parameter-only forks: the EIP relies on
	// it so that an upgrade does not force every row mesh to re-form at once. The only config
	// input allowed is ROW_SUBNET_COUNT itself.
	params.SetupTestConfigCleanup(t)

	ids := nodeIDs(32)
	before := make([]uint64, len(ids))
	for i, id := range ids {
		subnet, err := peerdas.RowSubnetForNode(id)
		require.NoError(t, err)
		before[i] = subnet
	}

	cfg := params.BeaconConfig().Copy()
	cfg.FuluForkEpoch = 999
	cfg.GloasForkEpoch = 1000
	cfg.BlobSchedule = nil
	cfg.NumberOfCustodyGroups = 64
	cfg.CustodyRequirement = 8
	cfg.DataColumnSidecarSubnetCount = 64
	params.OverrideBeaconConfig(cfg)

	for i, id := range ids {
		subnet, err := peerdas.RowSubnetForNode(id)
		require.NoError(t, err)
		require.Equal(t, before[i], subnet, fmt.Sprintf("node %d changed row subnet", i))
	}
}

func TestRowSubnetForNode_Distribution(t *testing.T) {
	rowSubnetCount := params.BeaconConfig().RowSubnetCount

	const nodes = 12800
	counts := make([]int, rowSubnetCount)
	for _, id := range nodeIDs(nodes) {
		subnet, err := peerdas.RowSubnetForNode(id)
		require.NoError(t, err)
		counts[subnet]++
	}

	// Expected 100 per subnet. A loose band: this is a smoke test for a broken derivation
	// (a constant, a truncated hash, a modulo on the wrong bytes), not a statistical test.
	expected := nodes / int(rowSubnetCount)
	for subnet, count := range counts {
		require.Equal(t, true, count > expected/3 && count < expected*3,
			fmt.Sprintf("subnet %d got %d nodes, expected around %d", subnet, count, expected))
	}
}

func TestRowSubnetForBlob_IsPermutation(t *testing.T) {
	rowSubnetCount := params.BeaconConfig().RowSubnetCount

	for _, slot := range []primitives.Slot{0, 1, 42, 1 << 20} {
		seen := make(map[uint64]uint64, rowSubnetCount)
		for blobIndex := range rowSubnetCount {
			subnet, err := peerdas.RowSubnetForBlob(blobIndex, slot)
			require.NoError(t, err)
			require.Equal(t, true, subnet < rowSubnetCount, "subnet out of range")

			previous, collided := seen[subnet]
			require.Equal(t, false, collided,
				fmt.Sprintf("slot %d: blobs %d and %d both map to subnet %d", slot, previous, blobIndex, subnet))
			seen[subnet] = blobIndex
		}
		require.Equal(t, int(rowSubnetCount), len(seen), "mapping is not onto")
	}
}

func TestRowSubnetForBlob_DependsOnSlot(t *testing.T) {
	// Guards against a constant or mis-serialised seed: the permutation must actually be
	// redrawn per slot, otherwise a node's row duty never rotates.
	var differing int
	for slot := primitives.Slot(0); slot < 32; slot++ {
		zeroSlot, err := peerdas.RowSubnetForBlob(0, 0)
		require.NoError(t, err)
		subnet, err := peerdas.RowSubnetForBlob(0, slot)
		require.NoError(t, err)
		if subnet != zeroSlot {
			differing++
		}
	}
	require.Equal(t, true, differing > 16, "blob 0's subnet barely moves across slots")
}

func TestBlobsForRowSubnet_InvertsRowSubnetForBlob(t *testing.T) {
	rowSubnetCount := params.BeaconConfig().RowSubnetCount

	for _, slot := range []primitives.Slot{0, 7, 12345} {
		// Forward then back.
		for blobIndex := range rowSubnetCount {
			subnet, err := peerdas.RowSubnetForBlob(blobIndex, slot)
			require.NoError(t, err)

			blobIndices, err := peerdas.BlobsForRowSubnet(subnet, slot, rowSubnetCount)
			require.NoError(t, err)
			require.Equal(t, 1, len(blobIndices), "exactly one blob per subnet when blobCount == ROW_SUBNET_COUNT")
			require.Equal(t, blobIndex, blobIndices[0])
		}

		// Back then forward.
		for subnet := range rowSubnetCount {
			blobIndices, err := peerdas.BlobsForRowSubnet(subnet, slot, rowSubnetCount)
			require.NoError(t, err)
			require.Equal(t, 1, len(blobIndices))

			got, err := peerdas.RowSubnetForBlob(blobIndices[0], slot)
			require.NoError(t, err)
			require.Equal(t, subnet, got)
		}
	}
}

func TestBlobsForRowSubnet_OccupancyBelowSubnetCount(t *testing.T) {
	// With fewer blobs than subnets, exactly blobCount subnets carry a row and each blob is
	// carried exactly once. This is the shape that matters in practice: at 6 blobs, 122 of 128
	// row subnets are idle and most nodes have no duty.
	rowSubnetCount := params.BeaconConfig().RowSubnetCount
	const slot = primitives.Slot(99)

	for _, blobCount := range []uint64{0, 1, 6, 48} {
		occupied := 0
		carried := make(map[uint64]int, blobCount)
		for subnet := range rowSubnetCount {
			blobIndices, err := peerdas.BlobsForRowSubnet(subnet, slot, blobCount)
			require.NoError(t, err)
			if len(blobIndices) == 0 {
				continue
			}
			occupied++
			require.Equal(t, 1, len(blobIndices))
			carried[blobIndices[0]]++
		}

		require.Equal(t, int(blobCount), occupied, fmt.Sprintf("blobCount %d", blobCount))
		require.Equal(t, int(blobCount), len(carried), fmt.Sprintf("blobCount %d: every blob carried exactly once", blobCount))
		for blobIndex, times := range carried {
			require.Equal(t, 1, times, fmt.Sprintf("blob %d carried %d times", blobIndex, times))
		}
	}
}

func TestBlobsForRowSubnet_AboveSubnetCount(t *testing.T) {
	// Above ROW_SUBNET_COUNT the mapping stops being injective: subnets carry several rows.
	// The wire format does not yet handle two rows in one group, but the mapping must still be
	// total and lossless.
	rowSubnetCount := params.BeaconConfig().RowSubnetCount
	blobCount := rowSubnetCount + rowSubnetCount/2
	const slot = primitives.Slot(5)

	carried := make(map[uint64]int, blobCount)
	multi := 0
	for subnet := range rowSubnetCount {
		blobIndices, err := peerdas.BlobsForRowSubnet(subnet, slot, blobCount)
		require.NoError(t, err)
		if len(blobIndices) > 1 {
			multi++
		}
		for _, blobIndex := range blobIndices {
			carried[blobIndex]++

			got, err := peerdas.RowSubnetForBlob(blobIndex, slot)
			require.NoError(t, err)
			require.Equal(t, subnet, got)
		}
	}

	require.Equal(t, int(blobCount), len(carried), "every blob carried")
	for blobIndex, times := range carried {
		require.Equal(t, 1, times, fmt.Sprintf("blob %d carried %d times", blobIndex, times))
	}
	require.Equal(t, int(rowSubnetCount/2), multi, "half the subnets should carry two rows")
}

func TestBlobsForRowSubnet_SubnetOutOfRange(t *testing.T) {
	rowSubnetCount := params.BeaconConfig().RowSubnetCount
	_, err := peerdas.BlobsForRowSubnet(rowSubnetCount, 0, 1)
	require.NotNil(t, err)
}

func TestRowSubnetCountZeroIsAnError(t *testing.T) {
	params.SetupTestConfigCleanup(t)
	cfg := params.BeaconConfig().Copy()
	cfg.RowSubnetCount = 0
	params.OverrideBeaconConfig(cfg)

	_, err := peerdas.RowSubnetForNode(enode.ID{})
	require.ErrorIs(t, err, peerdas.ErrRowSubnetCountZero)

	_, err = peerdas.RowSubnetForBlob(0, 0)
	require.ErrorIs(t, err, peerdas.ErrRowSubnetCountZero)

	_, err = peerdas.BlobsForRowSubnet(0, 0, 1)
	require.ErrorIs(t, err, peerdas.ErrRowSubnetCountZero)
}

func TestRowSubnetForBlob_ReducedSubnetCount(t *testing.T) {
	// Test and devnet configurations reduce ROW_SUBNET_COUNT so that a small network still
	// puts several nodes on each row subnet. The mapping must stay a permutation there too.
	params.SetupTestConfigCleanup(t)
	cfg := params.BeaconConfig().Copy()
	cfg.RowSubnetCount = 4
	params.OverrideBeaconConfig(cfg)

	seen := make(map[uint64]bool, 4)
	for blobIndex := range uint64(4) {
		subnet, err := peerdas.RowSubnetForBlob(blobIndex, 3)
		require.NoError(t, err)
		require.Equal(t, false, seen[subnet])
		seen[subnet] = true
	}
	require.Equal(t, 4, len(seen))

	// 6 blobs over 4 subnets: two subnets carry two rows.
	total := 0
	for subnet := range uint64(4) {
		blobIndices, err := peerdas.BlobsForRowSubnet(subnet, 3, 6)
		require.NoError(t, err)
		total += len(blobIndices)
	}
	require.Equal(t, 6, total)
}

func TestIsRowReconstructor(t *testing.T) {
	threshold := peerdas.MinimumColumnCountToReconstruct()
	require.Equal(t, uint64(64), threshold, "expected 64 of 128 columns")

	require.Equal(t, false, peerdas.IsRowReconstructor(0))
	require.Equal(t, false, peerdas.IsRowReconstructor(threshold-1))
	require.Equal(t, true, peerdas.IsRowReconstructor(threshold))
	require.Equal(t, true, peerdas.IsRowReconstructor(128))
}

// rowBitmap builds a NUMBER_OF_COLUMNS-long bitlist with the given column indices set.
func rowBitmap(columnIndices ...uint64) bitfield.Bitlist {
	bitmap := bitfield.NewBitlist(fieldparams.NumberOfColumns)
	for _, columnIndex := range columnIndices {
		bitmap.SetBitAt(columnIndex, true)
	}

	return bitmap
}
