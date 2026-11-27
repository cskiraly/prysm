package pubsub

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
)

// The harness's honest shortfall (H11b; the DAS pull trace of node 91, cell (31, 60)), rebuilt on
// the path. In the trace the node asked only its faulty offerers of the cell: each of its honest
// holders had broken a promise to it on another cell of the same column before offering this one,
// and after the faulty asks expired no ask followed. The harness's cells run the park as
// seacast-lite.md §6.1 writes it (k = 1, per group, a 400 ms promise, 30 s) but without v0.5's rules
// (SEGMENT_V05 unset, so no WithSpecV05): the last resort, §6.1's answer to "every announcer is
// parked", is off. This fixture shows the miss with the cells' options and its absence with B′.

// newSeacastParkMissCell is newSeacastAskCell with the v0.5 rules as given: nil for the harness's
// cells (no WithSpecV05), else WithSpecV05(*v05); extra options go before WithSeacast.
func newSeacastParkMissCell(t *testing.T, n int, v05 *SpecV05, ctr *SpecV05Counters, extra ...Option) *seacastAskCell {
	t.Helper()
	c := &seacastAskCell{}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	c.hosts = getDefaultHosts(t, n)
	params := DefaultGossipSubParams()
	params.IDontWantMessageThreshold = 1
	c.pubsubs = getGossipsubsOptFn(ctx, c.hosts, func(i int, h host.Host) []Option {
		T := &testObjectType{name: "col", params: TypeParams{RNode: 2}, topics: []string{"seg/col/0"}, store: NewMapCellStore()}
		opts := []Option{
			WithGossipSubParams(params),
			WithMessageIdFn(seacastTestID),
			WithHaveBitmaps(seacastTestIndex),
			WithPhaseForwarding(func(topic string) bool { return topic != "seg/col/0" }, 2),
			WithIWantDiscipline(200 * time.Millisecond),
			WithIHaveCommitmentPark(1, 400*time.Millisecond, 30*time.Second, &c.breaks, &c.parks),
			WithIHaveParkPerGroup(nil),
			WithOfferTable(nil),
			WithRequestObservation(),
		}
		if v05 != nil {
			opts = append(opts, WithSpecV05(*v05, ctr))
		}
		opts = append(opts, extra...)
		return append(opts, WithSeacast([]ObjectType{T}, nil))
	})
	for _, ps := range c.pubsubs {
		if _, err := ps.Subscribe("seg/col/0"); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < n-1; i++ {
		connect(t, c.hosts[i], c.hosts[n-1])
	}
	time.Sleep(2 * time.Second)
	return c
}

// TestSeacastParkedHoldersMiss: H1 and H2, honest, each break a promise to the asker B on another
// cell of the group (an offer B asks for that is not answered within the 400 ms promise), so k = 1
// parks both for the group. Then the cell: F1, faulty, offers it first and never answers; 1.2 s
// later F2, faulty, offers it and is asked; 50 ms after that H1 and H2 offer it while F2's ask is
// open. Every announcer of the cell is parked once F2's promise breaks. The last resort asks a parked
// announcer once more (seacast_pull.go), and one of H1, H2 is asked. Before the chunk pull, with the
// cells' options and no v0.5 rules, B asked F1 and F2 once each and never H1 or H2, and the cell was
// not asked for again.
func TestSeacastParkedHoldersMiss(t *testing.T) {
	type result struct{ f1, f2, h1, h2, breaks, parks, armed, lrAsks, parkedAsked int64 }
	run := func(t *testing.T, v05 *SpecV05) (r result) {
		ctr := &SpecV05Counters{}
		synctestTest(t, func(t *testing.T) {
			root := strings.Repeat("c", haveRootLen)
			c := newSeacastParkMissCell(t, 5, v05, ctr) // F1 = 0, F2 = 1, H1 = 2, H2 = 3, B = 4
			F1, F2, H1, H2 := c.hosts[0].ID(), c.hosts[1].ID(), c.hosts[2].ID(), c.hosts[3].ID()
			// The earlier cells of the group: one broken promise each parks H1 and H2 for the group.
			c.offer(2, root+string(byte(1)))
			c.offer(3, root+string(byte(2)))
			time.Sleep(600 * time.Millisecond)
			if c.parks.Load() != 2 {
				t.Fatalf("after the earlier cells: %d parks, want 2 (H1 and H2 parked for the group)", c.parks.Load())
			}
			mid := root + string(byte(9))
			c.offer(0, mid) // F1 first
			time.Sleep(1150 * time.Millisecond)
			c.offer(1, mid) // F2, 1.2 s later
			c.offer(2, mid) // H1 and H2 while F2's ask is open
			c.offer(3, mid)
			time.Sleep(6 * time.Second)
			r = result{
				f1: int64(c.asksTo(mid, F1)), f2: int64(c.asksTo(mid, F2)),
				h1: int64(c.asksTo(mid, H1)), h2: int64(c.asksTo(mid, H2)),
				breaks: c.breaks.Load(), parks: c.parks.Load(),
				armed: ctr.LastResortsArmed.Load(), lrAsks: ctr.LastResortAsks.Load(), parkedAsked: ctr.ParkedAsked.Load(),
			}
		})
		return r
	}
	r := run(t, &SpecV05{})
	t.Logf("asks of the cell to F1 %d, F2 %d, H1 %d, H2 %d; breaks %d, parks %d; last resorts armed %d, asks %d, to parked %d",
		r.f1, r.f2, r.h1, r.h2, r.breaks, r.parks, r.armed, r.lrAsks, r.parkedAsked)
	if r.parks != 4 {
		t.Fatalf("every announcer of the cell should be parked: %d parks, want 4", r.parks)
	}
	if r.h1+r.h2 == 0 || r.parkedAsked == 0 {
		t.Fatalf("the last resort should ask H1 or H2, got H1 %d H2 %d, parked asks %d", r.h1, r.h2, r.parkedAsked)
	}
}
