package pubsub

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
)

// TestSeacastTypeProjection is the window's projection per object type (§4.9)
// on three routers: B publishes cells that reach A, and C, under a router-wide
// projection that would drop a frame to a peer holding 16, pushes to A. A column cell (K = N) to
// A, which holds the other 31, is not dropped; of two row cells (K = 64 of 128) pushed to A, which
// holds 63, the second is dropped at the pop, A being projected to 64 with the first, counted on
// C's queue; and A's ask for the dropped cell is answered, an answer never projected away. The row
// type pushes in this fixture, to populate the push path a pull-only type never exercises.
func TestSeacastTypeProjection(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		const colTopic, rowTopic = "seg/col/0", "seg/row/0"
		colRoot, rowRoot := strings.Repeat("c", haveRootLen), strings.Repeat("r", haveRootLen)
		hosts := getDefaultHosts(t, 3)
		params := DefaultGossipSubParams()
		params.IDontWantMessageThreshold = 1
		psubs := getGossipsubsOptFn(ctx, hosts, func(i int, h host.Host) []Option {
			col := &testObjectType{name: "col", params: TypeParams{RNode: 3}, topics: []string{colTopic},
				store: NewMapCellStore(), threshold: 32, topicOf: func(r string) (string, bool) { return colTopic, r == colRoot }}
			row := &testObjectType{name: "row", params: TypeParams{RNode: 3, ProjectFanIn: 1}, topics: []string{rowTopic},
				store: NewMapCellStore(), indexCount: 128, threshold: 64, topicOf: func(r string) (string, bool) { return rowTopic, r == rowRoot }}
			opts := []Option{
				WithGossipSubParams(params),
				WithValidateQueueSize(1024),
				WithPeerOutboundQueueSize(1024),
				WithMessageSignaturePolicy(StrictNoSign),
				WithMessageIdFn(seacastTestID),
				WithHaveBitmaps(seacastTestIndex),
				WithPhaseForwarding(func(tp string) bool { return !strings.HasPrefix(tp, "seg/") }, 2),
			}
			if i == 2 {
				opts = append(opts, WithHaveDispatchProjection(16, 1)) // router-wide at C: a frame to a peer holding 16 would be dropped
			}
			return append(opts, WithSeacast([]ObjectType{col, row}, nil))
		})
		seacastOrdZeroAtEnd(t, psubs)
		for _, ps := range psubs {
			for _, topic := range []string{colTopic, rowTopic} {
				if _, err := ps.Subscribe(topic); err != nil {
					t.Fatal(err)
				}
			}
		}
		connect(t, hosts[0], hosts[1]) // A–B
		connect(t, hosts[0], hosts[2]) // A–C
		time.Sleep(2 * time.Second)
		a, b, c := psubs[0], psubs[1], psubs[2]
		gsA, gsC := a.rt.(*GossipSubRouter), c.rt.(*GossipSubRouter)
		cell := func(root string, i byte) []byte {
			return append([]byte(root+string(rune(i))), bytes.Repeat([]byte{'a' + i%26}, 64)...)
		}
		read := func(gs *GossipSubRouter, f func()) {
			done := make(chan struct{})
			gs.p.eval <- func() { f(); close(done) }
			<-done
		}
		holds := func(gs *GossipSubRouter, root string, index uint32) (held bool) {
			read(gs, func() {
				if m := gs.have.mine[root]; m != nil && m.bits.Has(index) {
					held = true
				}
			})
			return
		}
		projected := func() (n int64) {
			read(gsC, func() {
				if q, ok := gsC.p.peers[hosts[0].ID()]; ok {
					n = q.projected.Load()
				}
			})
			return
		}

		// 1. The column (K = N): B publishes 31 of its 32 cells, which reach A; C publishes the last
		// to A unprojected, where its router-wide projection alone would have dropped it.
		for i := range 31 {
			if err := b.Publish(colTopic, cell(colRoot, byte(i))); err != nil {
				t.Fatal(err)
			}
		}
		time.Sleep(time.Second)
		if err := c.Publish(colTopic, cell(colRoot, 31)); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Second)
		if !holds(gsA, colRoot, 31) {
			t.Fatal("A did not receive the column cell: projected away although a column has no code")
		}
		if n := projected(); n != 0 {
			t.Fatalf("C's queue to A projected %d frames on the column, want 0", n)
		}

		// 2. The row (K = 64 of 128): B publishes 63, which reach A; C publishes two more to A: the
		// first is popped, the second dropped at the pop, A being projected to 64 with the first.
		for i := range 63 {
			if err := b.Publish(rowTopic, cell(rowRoot, byte(i))); err != nil {
				t.Fatal(err)
			}
		}
		time.Sleep(time.Second)
		if err := c.Publish(rowTopic, cell(rowRoot, 100)); err != nil {
			t.Fatal(err)
		}
		if err := c.Publish(rowTopic, cell(rowRoot, 101)); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Second)
		got := 0
		for _, i := range []uint32{100, 101} {
			if holds(gsA, rowRoot, i) {
				got++
			}
		}
		if got != 1 {
			t.Fatalf("A holds %d of the two row cells, want 1 (the second projected away at K = 64)", got)
		}
		if n := projected(); n != 1 {
			t.Fatalf("C's queue to A projected %d frames on the row, want 1", n)
		}

		// 3. An answer is never projected away: A asks C for the cell it lacks and gets it.
		missing := uint32(100)
		if holds(gsA, rowRoot, 100) {
			missing = 101
		}
		if !a.SeacastWant(hosts[2].ID(), rowRoot, missing, false, 0) {
			t.Fatal("A's WANT not sent")
		}
		time.Sleep(time.Second)
		if !holds(gsA, rowRoot, missing) {
			t.Fatal("A did not receive the answer to its ask: projected away although solicited")
		}
		if n := projected(); n != 1 {
			t.Fatalf("C's queue to A projected %d frames after the answer, want 1 still", n)
		}
	})
}

// TestSeacastTypeProjectionFanIn is the type's fan-in at the writer (seacast-lite.md §7.2's projected,
// §8's FAN_IN): a push is skipped once what the peer announced plus FAN_IN times what was sent to it
// beyond that reaches K. A row type (K = 64 of 128) on three routers: B publishes 62 cells, which
// reach A, and A's holdings reach C; C then publishes three more, which it pushes to A. At fan-in 1
// C sends two and skips the third (62 + 2 × 1 = 64); at fan-in 2 it sends one and skips two
// (62 + 1 × 2 = 64). The links' 50 ms latency keeps A's holding of a sent cell from reaching C
// before C's next pop, so the count is the projection's alone.
func TestSeacastTypeProjectionFanIn(t *testing.T) {
	for _, tc := range []struct {
		fanIn, sent, skipped int
	}{{1, 2, 1}, {2, 1, 2}} {
		t.Run(fmt.Sprintf("fan-in %d", tc.fanIn), func(t *testing.T) {
			synctestTest(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				const rowTopic = "seg/row/0"
				rowRoot := strings.Repeat("f", haveRootLen)
				hosts := v05Hosts(t, 3, 100, 50*time.Millisecond)
				params := DefaultGossipSubParams()
				params.IDontWantMessageThreshold = 1
				psubs := getGossipsubsOptFn(ctx, hosts, func(i int, _ host.Host) []Option {
					// C's fan-in is the case's; B's stays 1, so that its 62 pushes to A, unconfirmed for
					// a round trip, are not projected away (at 2 it would stop at 32).
					fanIn := 1
					if i == 2 {
						fanIn = tc.fanIn
					}
					row := &testObjectType{name: "row", params: TypeParams{RNode: 3, ProjectFanIn: fanIn}, topics: []string{rowTopic},
						store: NewMapCellStore(), indexCount: 128, threshold: 64, topicOf: func(r string) (string, bool) { return rowTopic, r == rowRoot }}
					return []Option{
						WithGossipSubParams(params),
						WithValidateQueueSize(1024),
						WithPeerOutboundQueueSize(1024),
						WithMessageSignaturePolicy(StrictNoSign),
						WithMessageIdFn(seacastTestID),
						WithHaveBitmaps(seacastTestIndex),
						WithPhaseForwarding(func(tp string) bool { return !strings.HasPrefix(tp, "seg/") }, 2),
						WithSeacast([]ObjectType{row}, nil),
					}
				})
				seacastOrdZeroAtEnd(t, psubs)
				for _, ps := range psubs {
					if _, err := ps.Subscribe(rowTopic); err != nil {
						t.Fatal(err)
					}
				}
				connect(t, hosts[0], hosts[1]) // A–B
				connect(t, hosts[0], hosts[2]) // A–C
				time.Sleep(2 * time.Second)
				b, c := psubs[1], psubs[2]
				gsA, gsC := psubs[0].rt.(*GossipSubRouter), c.rt.(*GossipSubRouter)
				cell := func(i byte) []byte {
					return append([]byte(rowRoot+string(rune(i))), bytes.Repeat([]byte{'a' + i%26}, 64)...)
				}
				read := func(gs *GossipSubRouter, f func()) {
					done := make(chan struct{})
					gs.p.eval <- func() { f(); close(done) }
					<-done
				}

				for i := range 62 {
					if err := b.Publish(rowTopic, cell(byte(i))); err != nil {
						t.Fatal(err)
					}
				}
				time.Sleep(time.Second)
				for _, i := range []byte{100, 101, 102} {
					if err := c.Publish(rowTopic, cell(i)); err != nil {
						t.Fatal(err)
					}
				}
				time.Sleep(time.Second)
				got, skipped := 0, int64(0)
				read(gsA, func() {
					if m := gsA.have.mine[rowRoot]; m != nil {
						for _, i := range []uint32{100, 101, 102} {
							if m.bits.Has(i) {
								got++
							}
						}
					}
				})
				read(gsC, func() {
					if q, ok := gsC.p.peers[hosts[0].ID()]; ok {
						skipped = q.projected.Load()
					}
				})
				if got != tc.sent || skipped != int64(tc.skipped) {
					t.Fatalf("A holds %d of C's three cells and C's queue to A skipped %d; want %d and %d", got, skipped, tc.sent, tc.skipped)
				}
			})
		})
	}
}
