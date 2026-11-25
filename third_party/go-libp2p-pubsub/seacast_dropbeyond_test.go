package pubsub

import (
	"strings"
	"testing"
	"time"
)

// TestSeacastDropBeyond is seacast-lite.md §4.2's drop_beyond at spec f6c19db, which the draft carries
// (seacast.md's fill): once a group's count is known, what named an index the group has not, while
// the count was unknown, goes: the offer, its ordered list, the per-group offer bit, a queued ask, a
// promise, a try and a last-resort count. In the fork a group's count is known once its header is
// validated (seacastCountKnown). A offers B the id of index 40, and of index 3 as the control, of a
// group whose header B has not validated (the test type counts 32); B records both. B then validates
// the header: the offer of index 40 and every trace of it go, and the offer of index 3 stays.
func TestSeacastDropBeyond(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		root := strings.Repeat("d", haveRootLen)
		c := newSeacastTieCell(t, 2)
		b := c.pubsubs[1]
		A := c.hosts[0].ID()
		mid40, mid3 := root+string(byte(40)), root+string(byte(3))
		c.offer(0, 1, mid40)
		c.offer(0, 1, mid3)
		var before40, before3 bool
		c.read(1, func(gs *GossipSubRouter) {
			_, before40 = gs.offerTable[mid40][A]
			_, before3 = gs.offerTable[mid3][A]
		})
		if !before40 || !before3 {
			t.Fatalf("before the header: A's offers recorded, index 40 %v and 3 %v; want both", before40, before3)
		}
		c.types[1].gateClosed = false // B's application validated the header
		b.SeacastHeaderValidated([]string{root})
		time.Sleep(100 * time.Millisecond)
		var offered, tried, asked, promised, listed, bit, kept3 bool
		c.read(1, func(gs *GossipSubRouter) { // read on the router; judged outside it
			_, offered = gs.offerTable[mid40][A]
			_, tried = gs.offerTried[mid40]
			_, asked = gs.iwantAsked[mid40]
			_, promised = gs.iwantPromised[mid40]
			listed = len(gs.v05.offerOrder[mid40]) > 0
			if g := gs.v05.offered[A][root]; g != nil {
				bit = g.bits.Has(40)
				kept3 = g.bits.Has(3)
			}
			if _, ok := gs.offerTable[mid3][A]; ok || gs.haveSeen(mid3) {
				kept3 = true
			}
		})
		if offered || tried || asked || promised || listed || bit {
			t.Fatalf("after the header: index 40 left offered %v tried %v asked %v promised %v listed %v bit %v; want none",
				offered, tried, asked, promised, listed, bit)
		}
		if !kept3 {
			t.Fatal("after the header: the control, A's offer of index 3, went too")
		}
	})
}
