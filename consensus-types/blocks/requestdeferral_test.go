package blocks

import (
	"testing"
	"time"

	"github.com/OffchainLabs/go-bitfield"
	"github.com/OffchainLabs/prysm/v7/testing/require"
)

// TestRowRequestsAreDeferredUntilTheInstant pins D17 clause 1's mechanism: with a deferral set,
// the default request policy asks for nothing -- and makes no claims -- until the instant passes;
// afterwards the same row asks as it always did. Availability is untouched throughout: only the
// requests wait.
func TestRowRequestsAreDeferredUntilTheInstant(t *testing.T) {
	row := testRow(t, 0)

	start := time.Unix(1_700_000_000, 0)
	now := start
	prev := partialClock
	partialClock = func() time.Time { return now }
	t.Cleanup(func() { partialClock = prev })

	row.DeferRequestsUntil(start.Add(2 * time.Second))

	held, err := row.newPartsMetadata(nil)
	require.NoError(t, err)
	require.Equal(t, uint64(0), bitfield.Bitlist(held.Requests).Count(),
		"a deferred row must request nothing")
	require.Equal(t, row.Included.Count(), bitfield.Bitlist(held.Available).Count(),
		"availability must be announced in full while requests are held")

	now = start.Add(2 * time.Second)
	released, err := row.newPartsMetadata(nil)
	require.NoError(t, err)
	require.Equal(t, numberOfColumns-row.Included.Count(), bitfield.Bitlist(released.Requests).Count(),
		"once the deferral passes the row asks for everything it lacks, as before")
}

// TestExplicitRequestsBypassTheDeferral pins the override rule: a caller that set its own request
// bitmap -- the pull arm naming one cell, a recovered row asking for nothing -- knows more about
// this row than the deferral policy, and is not held.
func TestExplicitRequestsBypassTheDeferral(t *testing.T) {
	row := testRow(t, 0)

	start := time.Unix(1_700_000_000, 0)
	prev := partialClock
	partialClock = func() time.Time { return start }
	t.Cleanup(func() { partialClock = prev })

	row.DeferRequestsUntil(start.Add(time.Hour))
	one := bitfield.NewBitlist(numberOfColumns)
	// A cell the fixture does not hold, so the missing-intersection keeps it.
	one.SetBitAt(numberOfColumns-1, true)
	require.NoError(t, row.SetPartsRequests(one))

	meta, err := row.newPartsMetadata(nil)
	require.NoError(t, err)
	require.Equal(t, uint64(1), bitfield.Bitlist(meta.Requests).Count(),
		"an explicit request bitmap must not be held by the deferral")
}
