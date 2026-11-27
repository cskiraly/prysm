package pubsub

import (
	"fmt"
	"strings"
	"testing"

	core "github.com/cskiraly/seacast-go"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
)

// newOrderRouter is a router whose chunk pull has index 5 of root offered first by core.MaxAnnouncers
// connected announcers, which fill the chunk's ordered list, and then by the connected peers ps, beyond
// it. Its own order's secret is key. It returns the router's env for the chunk (seacast_pull.go).
func newOrderRouter(root string, ps []peer.ID, key byte) (*GossipSubRouter, *pullEnv) {
	gs := &GossipSubRouter{peers: map[peer.ID]protocol.ID{}}
	for b := range gs.pull.orderKey {
		gs.pull.orderKey[b] = key
	}
	gs.pull.orderKeySet = true
	g := seacastGID(root)
	for k := range core.MaxAnnouncers {
		p := peer.ID(fmt.Sprintf("listed-%02d", k))
		gs.peers[p] = "test"
		gs.pull.offers.Add(p, g, 5)
	}
	for _, p := range ps {
		gs.peers[p] = "test"
		gs.pull.offers.Add(p, g, 5)
	}
	return gs, gs.pullEnvOf(g, false)
}

// TestSpecV05OwnOrder is the node's own order of the announcers beyond a chunk's ordered list (seacast-lite.md
// §6.1's announcers and own_order, §8.1; spec f6c19db): a keyed hash of the chunk and the peer under a
// secret each node draws, which no peer can predict or arrange. Sixteen routers, each with a secret of
// its own, rank the same three announcers. The secrets must give different orders (under one order for
// every router, by peer id or any other, all sixteen agree), and each router's order is the same over
// a chunk's retries, the ordered list coming first, in announcement order. A listed announcer the
// router has no stream to is passed over.
func TestSpecV05OwnOrder(t *testing.T) {
	root := strings.Repeat("r", haveRootLen)
	g := seacastGID(root)
	ps := []peer.ID{"peer-a", "peer-b", "peer-c"}
	orders := map[string]bool{}
	for key := range byte(16) {
		gs, env := newOrderRouter(root, ps, key)
		anns := env.Announcers(g, 5)
		if len(anns) != core.MaxAnnouncers+3 {
			t.Fatalf("router %d: %d announcers; want %d", key, len(anns), core.MaxAnnouncers+3)
		}
		for k := range core.MaxAnnouncers {
			if want := peer.ID(fmt.Sprintf("listed-%02d", k)); anns[k] != want {
				t.Fatalf("router %d: announcer %d is %s; want the ordered list first, %s", key, k, anns[k], want)
			}
		}
		beyond := anns[core.MaxAnnouncers:]
		again := env.Announcers(g, 5)[core.MaxAnnouncers:]
		if fmt.Sprint(beyond) != fmt.Sprint(again) {
			t.Fatalf("router %d: the order changed between two retries: %v then %v", key, beyond, again)
		}
		orders[fmt.Sprint(beyond)] = true
		if key == 0 {
			delete(gs.peers, "listed-00") // its stream went: passed over, though listed
			if anns := env.Announcers(g, 5); len(anns) != core.MaxAnnouncers+2 || anns[0] != "listed-01" {
				t.Fatalf("router 0 without listed-00's stream: %v; want it passed over", anns)
			}
		}
	}
	if len(orders) < 2 {
		t.Fatalf("sixteen routers with sixteen secrets ranked the announcers beyond the list one way, %v: not an order of each node's own", orders)
	}
}
