package partialdatacolumnbroadcaster

import (
	"testing"
	"time"

	"github.com/OffchainLabs/prysm/v7/consensus-types/blocks"
	"github.com/OffchainLabs/prysm/v7/testing/require"
)

func init() {
	// The production default (1 s, R19's sweep) would shift the timing of every row test in this
	// package that predates the deferral. Tests exercise the mechanism through
	// DeferRequestsUntil directly; the policy default is measured where it matters, in
	// rowintegrationtest's deferral arms.
	RowRequestDeferral = 0
}

// TestRequestDeferralArmsThePublishWake pins the liveness half of D17 clause 1: a row whose
// requests are held has a wake-up armed at the hold's expiry, so the requests reach the wire when
// the hold ends rather than whenever unrelated traffic happens to run a publish. Without this the
// deferral would be a request *drop* on a quiet mesh.
func TestRequestDeferralArmsThePublishWake(t *testing.T) {
	h := newRowHarness(t)
	const topic = "/eth2/abcd1234/data_row_2/ssz_snappy"

	start := time.Unix(1_700_000_000, 0)
	h.broadcaster.now = func() time.Time { return start }
	var armedAfter []time.Duration
	h.broadcaster.armCoalesce = func(d time.Duration, _ func()) *time.Timer {
		armedAfter = append(armedAfter, d)
		return time.NewTimer(time.Hour) // never fires in this test
	}

	row := newDeferralTestRow(t)
	row.DeferRequestsUntil(start.Add(1500 * time.Millisecond))

	h.broadcaster.armRowClaimWake(topic, row.GroupID(), &row)
	require.Equal(t, 1, len(armedAfter), "a deferred row must arm a wake-up")
	require.Equal(t, 1500*time.Millisecond, armedAfter[0],
		"the wake-up must fire when the hold expires")

	// A second arm for the same group is a no-op: one timer per group, as for claims.
	h.broadcaster.armRowClaimWake(topic, row.GroupID(), &row)
	require.Equal(t, 1, len(armedAfter))
}

// TestNoWakeWithoutDeferralOrClaims is the control: a row with no claims and no hold arms nothing.
func TestNoWakeWithoutDeferralOrClaims(t *testing.T) {
	h := newRowHarness(t)
	const topic = "/eth2/abcd1234/data_row_2/ssz_snappy"

	h.broadcaster.armCoalesce = func(time.Duration, func()) *time.Timer {
		t.Fatal("nothing is pending, nothing should be armed")
		return nil
	}

	row := newDeferralTestRow(t)
	h.broadcaster.armRowClaimWake(topic, row.GroupID(), &row)
}

func newDeferralTestRow(t *testing.T) blocks.PartialDataRow {
	t.Helper()
	header, root := testRowHeader(t)
	row, err := blocks.NewPartialDataRow(root, header.SignedBlockHeader, 2,
		header.KzgCommitments, header.KzgCommitmentsInclusionProof)
	require.NoError(t, err)

	return row
}
