package pubsub

import (
	"slices"

	"github.com/libp2p/go-libp2p/core/peer"
)

// The tests' reads of a chunk's pull state, which is in the core's tables (seacast_pull.go), by the id a
// test names the chunk by: its own id or an ask by index. Each is false for an id the core does not pull.
// Router goroutine.

// pullOffered says whether p's offer of mid's chunk is kept.
func (gs *GossipSubRouter) pullOffered(p peer.ID, mid string) bool {
	_, g, i, ok := gs.pullChunk(mid)
	return ok && gs.pull.offers.Offered(p, g, i)
}

// pullOfferedByAny says whether any peer's offer of mid's chunk is kept.
func (gs *GossipSubRouter) pullOfferedByAny(mid string) bool {
	_, g, i, ok := gs.pullChunk(mid)
	return ok && slices.Contains(gs.pull.offers.Beyond(g, i), i)
}

// pullPromised says whether p's promise of mid's chunk is open.
func (gs *GossipSubRouter) pullPromised(p peer.ID, mid string) bool {
	_, g, i, ok := gs.pullChunk(mid)
	return ok && gs.pull.promises.Promised(p, g, i)
}

// pullAsked says whether the time mid's chunk was last asked is kept.
func (gs *GossipSubRouter) pullAsked(mid string) bool {
	_, g, i, ok := gs.pullChunk(mid)
	return ok && slices.Contains(gs.pull.asks.Beyond(g, i), i)
}

// pullTried says whether p's try of mid's chunk is kept.
func (gs *GossipSubRouter) pullTried(p peer.ID, mid string) bool {
	_, g, i, ok := gs.pullChunk(mid)
	if !ok {
		return false
	}
	_, tried := gs.pull.asks.Tried(g, i, p)
	return tried
}
