package segmentbroadcaster

import (
	"context"
	"testing"
	"time"

	"github.com/OffchainLabs/prysm/v7/testing/require"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/sirupsen/logrus"
)

// newLedgerHarness returns a broadcaster with a settable clock and one four-segment ledger.
func newLedgerHarness(t *testing.T, strikeCap int, ttl time.Duration) (*Broadcaster, *requestLedger, *time.Time) {
	t.Helper()
	b := New(context.Background(), logrus.New(), Config{
		StrikeCap: strikeCap, StrikeTTL: ttl, RequestTimeout: 400 * time.Millisecond,
	})
	now := time.Unix(1_700_000_000, 0)
	b.now = func() time.Time { return now }
	l := b.ledgerFor(groupKey{topic: "t", group: "g"}, 4)
	return b, l, &now
}

func TestClaimRequestOneLapseOneStrike(t *testing.T) {
	b, l, now := newLedgerHarness(t, 2, 30*time.Second)
	p1, p2, p3 := peer.ID("p1"), peer.ID("p2"), peer.ID("p3")
	budget := 10
	require.Equal(t, true, b.claimRequest(l, 0, p1, &budget, nil))

	// The claim lapses. A pass then evaluates two more candidates for the index without
	// either taking it (no budget left): the lapsed peer must be charged once, not twice.
	*now = now.Add(500 * time.Millisecond)
	empty := 0
	require.Equal(t, false, b.claimRequest(l, 0, p2, &empty, nil))
	require.Equal(t, false, b.claimRequest(l, 0, p3, &empty, nil))
	require.Equal(t, 1, l.strikes[p1])

	// A fresh claim by another peer resets the marker, so the next lapse is charged again.
	require.Equal(t, true, b.claimRequest(l, 0, p2, &budget, nil))
	*now = now.Add(500 * time.Millisecond)
	require.Equal(t, true, b.claimRequest(l, 0, p3, &budget, nil))
	require.Equal(t, 1, l.strikes[p1])
	require.Equal(t, 1, l.strikes[p2])
}

func TestClaimRequestParkAtOneDoesNotStrandAnIndex(t *testing.T) {
	b, l, now := newLedgerHarness(t, 1, 30*time.Second)
	p1, p2 := peer.ID("p1"), peer.ID("p2")
	budget := 10
	require.Equal(t, true, b.claimRequest(l, 0, p1, &budget, nil))

	// p1 lapses; p2 takes the index and p1 is parked at the first strike.
	*now = now.Add(500 * time.Millisecond)
	require.Equal(t, true, b.claimRequest(l, 0, p2, &budget, nil))
	require.Equal(t, 1, l.strikes[p1])

	// p2 lapses too. p1 is the only other holder but is parked: refused while the index has
	// been unclaimed for less than a full extra timeout...
	*now = now.Add(500 * time.Millisecond)
	require.Equal(t, false, b.claimRequest(l, 0, p1, &budget, nil))
	require.Equal(t, 1, l.strikes[p2])
	// ...and asked again once nobody else has claimed it for a whole further timeout, with
	// the strike still on its record.
	*now = now.Add(400 * time.Millisecond)
	require.Equal(t, true, b.claimRequest(l, 0, p1, &budget, nil))
	require.Equal(t, 1, l.strikes[p1])
}
