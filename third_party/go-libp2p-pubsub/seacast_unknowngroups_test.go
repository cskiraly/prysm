package pubsub

import (
	"strings"
	"testing"
	"time"
)

// TestSeacastUnknownGroupsPerPeer is seacast.md §4.2's defer at spec f6c19db (seacast-lite.md §8.2's
// MAX_PENDING_GROUPS): a deferred copy of a group beyond its sender's limit of unknown groups is ignored,
// neither kept nor tied. A publishes one cell of each of two groups whose headers B has not validated.
// With a limit of one, B keeps and ties the first group's copy only; with no limit (the default) it keeps
// and ties both.
func TestSeacastUnknownGroupsPerPeer(t *testing.T) {
	for _, tc := range []struct {
		name         string
		limit        int
		pending, tie int
		refused      int64
	}{{"limit 1", 1, 1, 1, 1}, {"no limit (the default)", 0, 2, 2, 0}} {
		t.Run(tc.name, func(t *testing.T) {
			synctestTest(t, func(t *testing.T) {
				limits := DefaultSeacastLimits
				limits.UnknownGroupsPerPeer = tc.limit
				c := newSeacastTieCell(t, 2, limits)
				a, b := c.pubsubs[0], c.pubsubs[1]
				for k, r := range []string{"u", "v"} {
					if err := a.Publish("seg/col/0", seacastCell(strings.Repeat(r, haveRootLen), byte(3+k))); err != nil {
						t.Fatal(err)
					}
					time.Sleep(100 * time.Millisecond)
				}
				time.Sleep(time.Second)
				ctrB := c.gs(1).seacast.ctr
				if b.SeacastPending() != tc.pending || b.SeacastTies() != tc.tie || ctrB.UnknownRefused.Load() != tc.refused {
					t.Fatalf("pending %d ties %d refused %d; want %d %d %d", b.SeacastPending(), b.SeacastTies(), ctrB.UnknownRefused.Load(), tc.pending, tc.tie, tc.refused)
				}
			})
		})
	}
}
