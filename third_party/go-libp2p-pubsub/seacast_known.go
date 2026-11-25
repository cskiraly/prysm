package pubsub

// known_at (seacast.md §3's state): the earliest the node knew a group's id, by its first
// sight on the path (a chunk, an offer, a WANT naming it) or the application's word at block
// arrival (SeacastKnown), whichever came first; a later word never restarts it. A group's later
// decode is armed from it (§4.6's DECODE_LATER; seacast_complete.go).

import "time"

// noteKnown records the earliest knowledge of root (known_at[g]); a root known already keeps its
// instant. Router goroutine.
func (sc *seacast) noteKnown(root string) {
	if sc.knownAt == nil {
		sc.knownAt = map[string]time.Time{}
	}
	if _, known := sc.knownAt[root]; !known {
		sc.knownAt[root] = time.Now()
	}
}

// SeacastKnown tells the path that the application knows the groups' ids (the block arrived):
// their known_at where the path has not seen them yet; a root known already keeps its earlier
// instant. Any goroutine.
func (p *PubSub) SeacastKnown(roots []string) {
	gs, ok := p.rt.(*GossipSubRouter)
	if !ok || gs.seacast == nil {
		return
	}
	done := make(chan struct{})
	select {
	case p.eval <- func() {
		for _, root := range roots {
			if gs.seacastRootType(root) != nil {
				gs.seacast.noteKnown(root)
			}
		}
		close(done)
	}:
	case <-p.ctx.Done():
		return
	}
	select {
	case <-done:
	case <-p.ctx.Done():
	}
}
