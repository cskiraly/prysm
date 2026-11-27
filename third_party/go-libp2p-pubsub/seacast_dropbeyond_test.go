package pubsub

import (
	"slices"
	"strings"
	"testing"
	"time"

	core "github.com/cskiraly/seacast-go"
	"github.com/libp2p/go-libp2p/core/peer"
)

// lrTakesEnv is a last resort's env over one parked announcer whose queue takes the ask, sending
// nothing: it gives a chunk a last-resort count without an ask on the wire.
type lrTakesEnv struct{ p peer.ID }

func (lrTakesEnv) Held(core.GroupID, uint32) bool                     { return false }
func (lrTakesEnv) StopPull(core.GroupID) bool                         { return false }
func (lrTakesEnv) Expired(core.GroupID) bool                          { return false }
func (e lrTakesEnv) Announcers(core.GroupID, uint32) []peer.ID        { return []peer.ID{e.p} }
func (lrTakesEnv) Parked(peer.ID, core.GroupID) bool                  { return true }
func (lrTakesEnv) Tied(peer.ID, core.GroupID, uint32) bool            { return false }
func (lrTakesEnv) Outstanding(peer.ID, core.GroupID, uint32) bool     { return false }
func (lrTakesEnv) AskParkedAfter(core.GroupID, uint32, time.Duration) {}
func (lrTakesEnv) Enqueue(peer.ID, core.GroupID, uint32) bool         { return true }

// TestSeacastDropBeyond is seacast-lite.md §4.2's drop_beyond at spec f6c19db, which the draft carries
// (seacast.md's fill): once a group's count is known, what named an index the group has not, while
// the count was unknown, goes: the offer, its ordered list, the per-group offer bit, a queued ask, a
// promise, a try and a last-resort count, in the core's tables (seacast_pull.go), where one bitmap per
// announcer and group holds the offer. In the fork a group's count is known once its header is
// validated (seacastCountKnown). A offers B the id of index 40, and of index 3 as the control, of a
// group whose header B has not validated (the test type counts 32); B records both. B holds, for index
// 40, an ask of A still queued (as a dispatch records it), A's promise (as a write opens it) and a
// last-resort count with a try. B then validates the header: the offer of index 40 and every trace of
// it go, the queued ask marked for A's writer to drop, and the offer of index 3 stays.
func TestSeacastDropBeyond(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		root := strings.Repeat("d", haveRootLen)
		c := newSeacastTieCell(t, 2)
		b := c.pubsubs[1]
		A := c.hosts[0].ID()
		mid40, mid3 := root+string(byte(40)), root+string(byte(3))
		g := seacastGID(root)
		c.offer(0, 1, mid40)
		c.offer(0, 1, mid3)
		var before40, before3, queuedBefore, promisedBefore, lrBefore, triedBefore bool
		c.read(1, func(gs *GossipSubRouter) {
			before40 = gs.pull.offers.Offered(A, g, 40)
			before3 = gs.pull.offers.Offered(A, g, 3)
			gs.seacastAskQueued(A, []string{mid40})
			gs.pull.promises.Open(g, 40, A, time.Now())
			// a window on: the offer's own ask of index 40 is in flight until then
			gs.pull.lr.AskParked(lrTakesEnv{A}, &gs.pull.asks, g, 40, time.Now().Add(gs.iwantWindow), gs.iwantWindow, retrySlack)
			queuedBefore = gs.seacastAskIsQueued(A, mid40)
			promisedBefore = gs.pull.promises.Promised(A, g, 40)
			lrBefore = gs.pull.lr.Asked(g, 40, A)
			_, triedBefore = gs.pull.asks.Tried(g, 40, A)
		})
		if !before40 || !before3 || !queuedBefore || !promisedBefore || !lrBefore || !triedBefore {
			t.Fatalf("before the header: offers of 40 %v and 3 %v, 40 queued %v promised %v last-resort %v tried %v; want all",
				before40, before3, queuedBefore, promisedBefore, lrBefore, triedBefore)
		}
		ctrB := c.gs(1).seacast.ctr
		droppedBefore := ctrB.F13AskDropped.Load()
		c.types[1].gateClosed = false // B's application validated the header
		b.SeacastHeaderValidated([]string{root})
		time.Sleep(100 * time.Millisecond)
		var offered, tried, asked, promised, listed, queued, lr, kept3 bool
		c.read(1, func(gs *GossipSubRouter) { // read on the router; judged outside it
			offered = gs.pull.offers.Offered(A, g, 40)
			_, tried = gs.pull.asks.Tried(g, 40, A)
			asked = slices.Contains(gs.pull.asks.Beyond(g, 40), 40)
			promised = gs.pull.promises.Promised(A, g, 40)
			listed = len(gs.pull.offers.List(g, 40)) > 0
			queued = gs.seacastAskIsQueued(A, mid40) || gs.seacastAskIsQueued(A, encodeWant(root, 40))
			lr = gs.pull.lr.Asked(g, 40, A)
			kept3 = gs.pull.offers.Offered(A, g, 3) || gs.haveSeen(mid3)
		})
		if offered || tried || asked || promised || listed || queued || lr {
			t.Fatalf("after the header: index 40 left offered %v tried %v asked %v promised %v listed %v queued %v last-resort %v; want none",
				offered, tried, asked, promised, listed, queued, lr)
		}
		if d := ctrB.F13AskDropped.Load() - droppedBefore; d != 1 {
			t.Fatalf("after the header: %d queued asks marked for the writer to drop; want 1", d)
		}
		if !kept3 {
			t.Fatal("after the header: the control, A's offer of index 3, went too")
		}
	})
}
