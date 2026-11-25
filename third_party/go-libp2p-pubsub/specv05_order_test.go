package pubsub

import (
	"strings"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
)

// newOrderRouter is a router with B′'s offer table and three connected peers that offered index 5
// of root, none of them in the chunk's ordered list: every one is an announcer beyond the list.
func newOrderRouter(root string, ps []peer.ID) *GossipSubRouter {
	gs := &GossipSubRouter{peers: map[peer.ID]protocol.ID{}}
	gs.v05 = &specV05{SpecV05: SpecV05{LastResort: true}, ctr: &SpecV05Counters{},
		offered: map[peer.ID]map[string]*offeredGroup{}, offerOrder: map[string][]peer.ID{}}
	for _, p := range ps {
		gs.peers[p] = "test"
		g := &offeredGroup{topic: "t", last: time.Now()}
		g.bits.Set(5)
		gs.v05.offered[p] = map[string]*offeredGroup{root: g}
	}
	return gs
}

// TestSpecV05OwnOrder is the node's own order of the announcers beyond a chunk's ordered list (seacast-lite.md
// §6.1's announcers and own_order, §8.1; spec f6c19db): a keyed hash of the chunk and the peer under a
// secret each node draws, which no peer can predict or arrange. Eight routers rank the same three
// announcers: under peer-id order (the fork before) all eight agree on the sorted order; under a secret
// each draws, all eight agree with probability (1/6)^8. Each router's order is the same over a chunk's
// retries, and the ordered list still comes first.
func TestSpecV05OwnOrder(t *testing.T) {
	root := strings.Repeat("r", haveRootLen)
	mid := encodeWant(root, 5)
	ps := []peer.ID{"peer-a", "peer-b", "peer-c"}
	differs := 0
	for i := 0; i < 8; i++ {
		gs := newOrderRouter(root, ps)
		first, listed := gs.announcers(mid)
		if listed != 0 || len(first) != 3 {
			t.Fatalf("router %d: %d announcers, %d listed; want 3 and 0", i, len(first), listed)
		}
		again, _ := gs.announcers(mid)
		for j := range first {
			if first[j] != again[j] {
				t.Fatalf("router %d: the order changed between two retries: %v then %v", i, first, again)
			}
		}
		for j := range first {
			if first[j] != ps[j] {
				differs++
				break
			}
		}
		// The ordered list comes first, whatever the rank.
		gs.v05.offerOrder[mid] = []peer.ID{"peer-c"}
		withList, listed := gs.announcers(mid)
		if listed != 1 || withList[0] != "peer-c" || len(withList) != 3 {
			t.Fatalf("router %d: with peer-c listed, got %v (%d listed); want peer-c first, 1 listed", i, withList, listed)
		}
	}
	if differs == 0 {
		t.Fatalf("all eight routers ranked the announcers beyond the list in peer-id order %v: not an order of the node's own", ps)
	}
}
