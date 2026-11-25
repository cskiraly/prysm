package pubsub

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
)

// seacastAskCell is n routers on one owned topic with the gate open, the pull discipline (a 200 ms
// window), promises and parks (k = 1, a 400 ms promise), the offer table, v0.5's retry and the
// request ledger, which counts the asks per peer. Every host is connected to the last one, the
// asker.
type seacastAskCell struct {
	pubsubs []*PubSub
	hosts   []host.Host
	breaks  atomic.Int64
	parks   atomic.Int64
}

func newSeacastAskCell(t *testing.T, n int) *seacastAskCell {
	t.Helper()
	c := &seacastAskCell{}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	c.hosts = getDefaultHosts(t, n)
	params := DefaultGossipSubParams()
	params.IDontWantMessageThreshold = 1
	c.pubsubs = getGossipsubsOptFn(ctx, c.hosts, func(i int, h host.Host) []Option {
		T := &testObjectType{name: "col", params: TypeParams{RNode: 2}, topics: []string{"seg/col/0"}, store: NewMapCellStore()}
		return []Option{
			WithGossipSubParams(params),
			WithMessageIdFn(seacastTestID),
			WithHaveBitmaps(seacastTestIndex),
			WithPhaseForwarding(func(topic string) bool { return topic != "seg/col/0" }, 2),
			WithIWantDiscipline(200 * time.Millisecond),
			WithIHaveCommitmentPark(1, 400*time.Millisecond, 30*time.Second, &c.breaks, &c.parks),
			WithIHaveParkPerGroup(nil),
			WithOfferTable(nil),
			WithRequestObservation(),
			WithSpecV05(SpecV05{LastResort: true, CloseWithConn: true}, nil),
			WithSeacast([]ObjectType{T}, nil),
		}
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

func (c *seacastAskCell) gs(i int) *GossipSubRouter { return c.pubsubs[i].rt.(*GossipSubRouter) }

// offer sends, from router i to the asker, an IHAVE naming mid.
func (c *seacastAskCell) offer(i int, mid string) { c.offerOn(i, "seg/col/0", mid) }

// offerOn sends, from router i to the asker, an IHAVE naming mid on topic.
func (c *seacastAskCell) offerOn(i int, topic, mid string) {
	to := c.hosts[len(c.hosts)-1].ID()
	gs := c.gs(i)
	done := make(chan struct{})
	gs.p.eval <- func() {
		gs.sendRPC(to, rpcWithControl(nil, []*pb.ControlIHave{{TopicID: &topic, MessageIDs: []string{mid}}}, nil, nil, nil, nil), false)
		close(done)
	}
	<-done
	time.Sleep(50 * time.Millisecond)
}

// asksTo counts the asker's asks of mid to peer p, from the request ledger.
func (c *seacastAskCell) asksTo(mid string, p peer.ID) int {
	gs := c.gs(len(c.hosts) - 1)
	n := 0
	done := make(chan struct{})
	gs.p.eval <- func() {
		if r := gs.reqLedger[mid]; r != nil {
			for _, a := range r.Asks {
				if a.Peer == p {
					n++
				}
			}
		}
		close(done)
	}
	<-done
	return n
}

// TestSeacastNoSecondAskOfAPeer: A offers a cell it never serves and B asks it once. A's second
// offer draws no second ask, and the retry after the 200 ms window, with A's promise still open,
// asks nobody (it looks again later) where the code before the rule asked A again. Once the
// promise breaks A is parked and the last resort asks it once more, a new ask after the first closed.
func TestSeacastNoSecondAskOfAPeer(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		root := strings.Repeat("a", haveRootLen)
		c := newSeacastAskCell(t, 2) // A = 0, B = 1
		mid := root + string(byte(9))
		A := c.hosts[0].ID()
		ctrB := c.gs(1).seacast.ctr
		c.offer(0, mid)
		if n := c.asksTo(mid, A); n != 1 {
			t.Fatalf("after A's offer: %d asks to A, want 1", n)
		}
		c.offer(0, mid) // A offers again while B's ask of it is open
		if n := c.asksTo(mid, A); n != 1 || ctrB.AskOutstanding.Load() != 1 {
			t.Fatalf("after A's second offer: %d asks to A, passed over %d; want 1 1", n, ctrB.AskOutstanding.Load())
		}
		time.Sleep(200 * time.Millisecond) // past the ask window (at about 300 ms), the promise (400 ms) still open
		if n := c.asksTo(mid, A); n != 1 || ctrB.RetryDeferred.Load() < 1 {
			t.Fatalf("after the window with A's promise open: %d asks to A, retries deferred %d; want 1 and >= 1", n, ctrB.RetryDeferred.Load())
		}
		time.Sleep(2 * time.Second) // the promise breaks, A is parked, the last resort asks it once more
		if n := c.asksTo(mid, A); n != 2 || c.breaks.Load() < 1 || c.parks.Load() != 1 {
			t.Fatalf("after the promise broke: %d asks to A, breaks %d, parks %d; want 2, >= 1, 1", n, c.breaks.Load(), c.parks.Load())
		}
	})
}

// eval runs f on router i's goroutine and waits for it.
func (c *seacastAskCell) eval(i int, f func(gs *GossipSubRouter)) {
	gs := c.gs(i)
	done := make(chan struct{})
	gs.p.eval <- func() { f(gs); close(done) }
	<-done
}

// TestSeacastLastResortWaitsForOutstanding (seacast-lite.md's ask_parked): the last resort takes an
// announcer unparked meanwhile first, and asks none while each such one has an ask outstanding. P,
// the only announcer, never answers and is parked (its promise breaks at about 400 ms); the last
// resort is armed for about 810 ms. At about 600 ms A offers too and is asked. When the last resort
// fires, A's ask is still open: it waits, where the code before the fix passed over A and asked
// parked P. Once A's promise has broken too, it asks P, the parked one asked longest ago.
func TestSeacastLastResortWaitsForOutstanding(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		root := strings.Repeat("w", haveRootLen)
		c := newSeacastAskCell(t, 3) // A = 0, P = 1, B = 2
		mid := root + string(byte(9))
		A, P := c.hosts[0].ID(), c.hosts[1].ID()
		c.offer(1, mid)                    // about 0 to 50 ms
		time.Sleep(550 * time.Millisecond) // P parked at about 400 ms; the last resort armed at about 410 ms
		c.offer(0, mid)                    // A offers at about 600 ms and is asked
		time.Sleep(250 * time.Millisecond) // about 900 ms: the last resort fired at about 810 ms
		if a, p := c.asksTo(mid, A), c.asksTo(mid, P); a != 1 || p != 1 {
			t.Fatalf("with A's ask open when the last resort fired: asks to A %d, to P %d; want 1 1", a, p)
		}
		time.Sleep(400 * time.Millisecond) // about 1300 ms: A parked at about 1000 ms, the last resort looked again
		if p := c.asksTo(mid, P); p != 2 {
			t.Fatalf("after A's promise broke: asks to P %d, want 2", p)
		}
	})
}

// TestSeacastLastResortLooksAgain (seacast-lite.md's ask_parked): when every candidate of the last
// resort has an ask of the chunk outstanding, it looks again after the ask window, as the retry
// does. P, the only announcer, never answers and is parked; the last resort is armed for about
// 810 ms. An ask of the chunk to P is set queued from 700 to 900 ms (the state of an ask that
// waits behind a slow queue); the code before the fix found no candidate at 810 ms and gave up,
// leaving the chunk to nobody, where the fix asks P at about 1015 ms.
func TestSeacastLastResortLooksAgain(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		root := strings.Repeat("v", haveRootLen)
		c := newSeacastAskCell(t, 2) // P = 0, B = 1
		mid := root + string(byte(9))
		P := c.hosts[0].ID()
		c.offer(0, mid)                    // about 0 to 50 ms
		time.Sleep(650 * time.Millisecond) // about 700 ms: P parked, the last resort armed
		c.eval(1, func(gs *GossipSubRouter) { gs.seacastAskQueued(P, []string{mid}) })
		time.Sleep(200 * time.Millisecond) // about 900 ms: the last resort fired at about 810 ms
		deferred := c.gs(1).v05.ctr.LastResortsDeferred.Load()
		c.eval(1, func(gs *GossipSubRouter) { gs.seacastAskClosed(P, mid) })
		time.Sleep(400 * time.Millisecond) // about 1300 ms
		if p := c.asksTo(mid, P); p != 2 || deferred != 1 {
			t.Fatalf("asks to P %d, last resorts deferred %d; want 2 1", p, deferred)
		}
	})
}

// TestSeacastLastResortChunksOnly: B′'s last resort is a rule for chunks (seacast-lite.md §6.1). On a
// stock topic of the same router, P offers B an id outside any group (shorter than a chunk's id
// here) and never answers; the promise breaks and parks P for its ungrouped ids, which share one
// park. The ordinary retry asks a parked peer nothing more, and the park ends PARK_TTL after the
// break. Before B′ was limited to chunks, the last resort asked P once more, at about 810 ms, and
// that ask's broken promise renewed P's park. (A chunk's last resort: TestSeacastNoSecondAskOfAPeer.)
func TestSeacastLastResortChunksOnly(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		c := newSeacastAskCell(t, 2) // P = 0, B = 1
		const stock = "blocks"
		for _, ps := range c.pubsubs {
			if _, err := ps.Subscribe(stock); err != nil {
				t.Fatal(err)
			}
		}
		time.Sleep(2 * time.Second)
		mid := "a-block-of-20-bytes!" // no group: a chunk's id is 33 bytes in this cell
		P := c.hosts[0].ID()
		start := time.Now()
		c.offerOn(0, stock, mid)
		time.Sleep(3 * time.Second)
		var until time.Time
		c.eval(1, func(gs *GossipSubRouter) {
			if e := gs.parkGroups[P][""]; e != nil {
				until = e.until
			}
		})
		armed := c.gs(1).v05.ctr.LastResortsArmed.Load()
		if n := c.asksTo(mid, P); n != 1 || c.breaks.Load() != 1 || armed != 0 {
			t.Errorf("an ungrouped id's only announcer, parked: %d asks, %d broken promises, %d last resorts armed; want 1 1 0", n, c.breaks.Load(), armed)
		}
		if ends := until.Sub(start); ends <= 30*time.Second || ends > 30*time.Second+600*time.Millisecond {
			t.Errorf("P's park ends %v after the offer, want PARK_TTL after the one break (30 s and the promise)", ends)
		}
	})
}

// TestSeacastStockRetryAsksFree: an id outside any group keeps the ordinary retry, which asks a
// free announcer. On a stock topic P offers B an ungrouped id and is asked; Q offers it inside the
// ask window and is not; the retry after the window passes over P, whose ask is open, and asks Q.
// (A retry that sent such an id to B′, whose lists now keep chunks only, would ask nobody.)
func TestSeacastStockRetryAsksFree(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		c := newSeacastAskCell(t, 3) // P = 0, Q = 1, B = 2
		const stock = "blocks"
		for _, ps := range c.pubsubs {
			if _, err := ps.Subscribe(stock); err != nil {
				t.Fatal(err)
			}
		}
		time.Sleep(2 * time.Second)
		mid := "another-block-20-byt" // no group: a chunk's id is 33 bytes in this cell
		P, Q := c.hosts[0].ID(), c.hosts[1].ID()
		c.offerOn(0, stock, mid) // about 0 to 50 ms
		c.offerOn(1, stock, mid) // about 50 to 100 ms, inside the window
		if p, q := c.asksTo(mid, P), c.asksTo(mid, Q); p != 1 || q != 0 {
			t.Fatalf("inside the window: asks to P %d, to Q %d; want 1 0", p, q)
		}
		time.Sleep(250 * time.Millisecond) // about 350 ms: the retry ran at about 205 ms
		if p, q := c.asksTo(mid, P), c.asksTo(mid, Q); p != 1 || q != 1 {
			t.Fatalf("after the window: asks to P %d, to Q %d; want 1 1", p, q)
		}
	})
}

// TestSeacastCloseWithConnChunksOnly: CloseWithConn closes the promises and drops the offers and
// tried asks of chunks; an id outside any group keeps the base's. P offers B a chunk and, on a stock
// topic, an ungrouped id, never answers, and closes the connection after both asks. B closes the
// chunk's promise and drops its offer; the stock id's offer stays for the ordinary retry once P is
// back, and its promise falls due as the router without the rule lets it. Before, both closed and
// both offers went.
func TestSeacastCloseWithConnChunksOnly(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		c := newSeacastAskCell(t, 2) // P = 0, B = 1
		const stock = "blocks"
		for _, ps := range c.pubsubs {
			if _, err := ps.Subscribe(stock); err != nil {
				t.Fatal(err)
			}
		}
		time.Sleep(2 * time.Second)
		chunk := strings.Repeat("c", haveRootLen) + string(byte(9))
		block := "a-third-block-20-byt" // no group
		P := c.hosts[0].ID()
		c.offer(0, chunk)
		c.offerOn(0, stock, block)
		if a, b := c.asksTo(chunk, P), c.asksTo(block, P); a != 1 || b != 1 {
			t.Fatalf("both offers should be asked: %d %d", a, b)
		}
		if err := c.hosts[0].Network().ClosePeer(c.hosts[1].ID()); err != nil {
			t.Fatal(err)
		}
		time.Sleep(100 * time.Millisecond)
		var chunkOffer, blockOffer, blockTried bool
		c.eval(1, func(gs *GossipSubRouter) {
			_, chunkOffer = gs.offerTable[chunk][P]
			_, blockOffer = gs.offerTable[block][P]
			_, blockTried = gs.offerTried[block][P]
		})
		closed := c.gs(1).v05.ctr.ClosedPromises.Load()
		if chunkOffer || !blockOffer || !blockTried || closed != 1 {
			t.Errorf("after the close: the chunk's offer kept %v, the stock id's offer kept %v and tried %v, promises closed %d; want false true true 1",
				chunkOffer, blockOffer, blockTried, closed)
		}
		time.Sleep(500 * time.Millisecond) // past the stock id's promise
		if b := c.breaks.Load(); b != 1 {
			t.Errorf("broken promises %d, want 1: the stock id's, as without the rule", b)
		}
	})
}

// TestSeacastRetryPicksFreeAnnouncer: with A's ask open, C's later offer is not asked for inside
// the window, and the retry after it asks C, not A.
func TestSeacastRetryPicksFreeAnnouncer(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		root := strings.Repeat("b", haveRootLen)
		c := newSeacastAskCell(t, 3) // A = 0, C = 1, B = 2
		mid := root + string(byte(9))
		A, C := c.hosts[0].ID(), c.hosts[1].ID()
		c.offer(0, mid)
		c.offer(1, mid)
		if a, cc := c.asksTo(mid, A), c.asksTo(mid, C); a != 1 || cc != 0 {
			t.Fatalf("inside the window: asks to A %d, to C %d; want 1 0", a, cc)
		}
		time.Sleep(200 * time.Millisecond)
		if a, cc := c.asksTo(mid, A), c.asksTo(mid, C); a != 1 || cc != 1 {
			t.Fatalf("after the window: asks to A %d, to C %d; want 1 1", a, cc)
		}
	})
}
