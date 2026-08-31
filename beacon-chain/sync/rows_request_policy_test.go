package sync

import (
	"testing"

	"github.com/OffchainLabs/prysm/v7/beacon-chain/core/peerdas"
	p2ptest "github.com/OffchainLabs/prysm/v7/beacon-chain/p2p/testing"
	fieldparams "github.com/OffchainLabs/prysm/v7/config/fieldparams"
	"github.com/OffchainLabs/prysm/v7/config/params"
	"github.com/OffchainLabs/prysm/v7/testing/require"
)

// requestPolicyService builds a service whose custody is driven by the custody group count, which
// is what decides both the columns it holds and whether it counts as a row reconstructor.
func requestPolicyService(t *testing.T, custodyGroups uint64) *Service {
	t.Helper()

	p := p2ptest.NewTestP2P(t)
	_, _, err := p.UpdateCustodyInfo(0, custodyGroups)
	require.NoError(t, err)

	return &Service{cfg: &config{p2p: p}}
}

// TestRowInterestKeepsOwnColumnsAndPools is a pooling node: the cells it keeps are its own
// columns' -- a second route to those is the row axis's latency benefit -- and it still needs
// foreign cells to be able to recover the row, so it pools.
func TestRowInterestKeepsOwnColumnsAndPools(t *testing.T) {
	params.SetupTestConfigCleanup(t)

	s := requestPolicyService(t, 4)
	samplingSize, err := s.samplingSize()
	require.NoError(t, err)
	info, _, err := peerdas.Info(s.cfg.p2p.NodeID(), samplingSize)
	require.NoError(t, err)
	held := uint64(len(info.CustodyColumns))
	require.Equal(t, false, peerdas.IsRowReconstructor(held), "this test needs a node below the threshold")

	keep, poolToThreshold, err := s.rowRequestInterest()
	require.NoError(t, err)
	require.Equal(t, uint64(fieldparams.NumberOfColumns), keep.Len())
	require.Equal(t, true, poolToThreshold)

	require.Equal(t, held, keep.Count(), "keep is exactly this node's custody")
	for column := range info.CustodyColumns {
		require.Equal(t, true, keep.BitAt(column),
			"a cell of a column this node keeps must stay in the request")
	}
}

// TestRowReconstructorDoesNotPool is the second half, and the one EIP-8371 states outright: a node
// with enough columns to recover a row alone needs no foreign row information, so it never pools
// and its requests never reach beyond its own columns.
func TestRowReconstructorDoesNotPool(t *testing.T) {
	params.SetupTestConfigCleanup(t)

	s := requestPolicyService(t, params.BeaconConfig().NumberOfCustodyGroups)
	samplingSize, err := s.samplingSize()
	require.NoError(t, err)
	info, _, err := peerdas.Info(s.cfg.p2p.NodeID(), samplingSize)
	require.NoError(t, err)
	require.Equal(t, true, peerdas.IsRowReconstructor(uint64(len(info.CustodyColumns))),
		"this test needs a node at or above the threshold")

	_, poolToThreshold, err := s.rowRequestInterest()
	require.NoError(t, err)
	require.Equal(t, false, poolToThreshold)
}

// TestRowRequestInterestFallsBackToEverything: a custody lookup that fails must not stop a node
// fetching a row. Asking for everything is worse for bytes and better for liveness, and that is
// the right way round for a fallback.
func TestRowRequestInterestFallsBackToEverything(t *testing.T) {
	params.SetupTestConfigCleanup(t)

	// A sampling size larger than the number of custody groups cannot be satisfied, so the
	// custody lookup fails and the policy declines to narrow anything.
	cfg := params.BeaconConfig().Copy()
	cfg.NumberOfCustodyGroups = 4
	cfg.SamplesPerSlot = 200
	params.OverrideBeaconConfig(cfg)

	c := &rowCallbacks{service: &Service{cfg: &config{p2p: p2ptest.NewTestP2P(t)}}}
	_, _, err := c.service.rowRequestInterest()
	require.NotNil(t, err, "this test needs the custody lookup to fail")

	keep, poolToThreshold := c.RowRequestInterest(0)
	require.IsNil(t, keep)
	require.Equal(t, false, poolToThreshold)
}
