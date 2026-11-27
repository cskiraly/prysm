package pubsub

import (
	"time"

	core "github.com/cskiraly/seacast-go"
	"github.com/libp2p/go-libp2p/core/peer"
)

// unproven reports whether the proof guard demotes p for root at now, as the push choice reads it
// (core.GuardFails under this forwarding's settings).
func (pf *phaseForwarding) unproven(gs *GossipSubRouter, p peer.ID, root string, now time.Time) bool {
	return core.GuardFails(gs.have.peers, gs.have.pushed, p, seacastGID(root), pf.pushChoice("", root, now))
}

// knownFrames is how many frames of root p is known to hold or to have in flight from us, as the
// push choice counts them: the union of its HAVE bitmap and what we pushed it (core.Known).
func (h *haveState) knownFrames(p peer.ID, root string) int {
	return core.Known(h.peers, h.pushed, p, seacastGID(root))
}
