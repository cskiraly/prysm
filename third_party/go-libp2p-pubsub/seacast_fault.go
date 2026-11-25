package pubsub

import (
	"fmt"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
)

// --- The toy's faults for the defences (seacast.md §4.6; the toy's node.py, --fault; part 4) ---
//
// On a node the harness draws as faulty (the toy's scenario_fail_select, the harness's set):
//   - claim: a liar. Its holding of a coded group it takes part in claims every index (claim_all,
//     complete) without a decode; with the harness's withholding it answers no ask.
//   - hoard: decodes and claims as the document says, but pushes nothing it rebuilt, drops a
//     standing request for a rebuilt chunk, and answers an ask for one only when it is the RPC's one
//     ask on that group, once per peer and group: what a probe looks like.

const (
	seacastFaultNone = iota
	seacastFaultClaim
	seacastFaultHoard
)

// WithSeacastFault makes this node one of the toy's faults ("claim", "hoard"). Requires
// WithSeacastMachine first.
func WithSeacastFault(kind string) Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok || gs.seacast == nil || !gs.seacast.machine {
			return fmt.Errorf("a fault requires WithSeacastMachine first")
		}
		switch kind {
		case "claim":
			gs.seacast.fault = seacastFaultClaim
		case "hoard":
			gs.seacast.fault = seacastFaultHoard
		default:
			return fmt.Errorf("unknown fault %q (want claim or hoard)", kind)
		}
		return nil
	}
}

// seacastHoards says whether this node hoards.
func (gs *GossipSubRouter) seacastHoards() bool {
	sc := gs.seacast
	return sc != nil && sc.fault == seacastFaultHoard
}

// seacastAsksPerGroup counts an RPC's asks per group, for the hoarder's probe test: a v4 WANT, a v1
// WANT and an ask by chunk id alike, as the toy's group_index classifies both forms (node.py:563–568,
// :1513; the review of part 4's build, finding 8: the chunk-id asks went uncounted, so a lone one
// was refused where the toy serves it once). Router goroutine.
func (gs *GossipSubRouter) seacastAsksPerGroup(ctl *pb.ControlMessage) map[string]int {
	out := map[string]int{}
	for _, iw := range ctl.GetIwant() {
		for _, mid := range iw.GetMessageIDs() {
			if root, _, _, _, ok, malformed := decodeWantV2(mid); ok && !malformed {
				out[root]++
			} else if root, _, ok := decodeWant(mid); ok {
				out[root]++
			} else if gs.have != nil {
				if root, _, ok := gs.have.index(mid); ok && len(root) == haveRootLen {
					out[root]++
				}
			}
		}
	}
	return out
}

// seacastHoardRefuses says whether the hoarder refuses p's ask for index of root: a rebuilt chunk,
// asked for other than as the RPC's one ask on its group or a second time by p for the group.
// Router goroutine.
func (gs *GossipSubRouter) seacastHoardRefuses(p peer.ID, root string, index uint32, perGroup map[string]int) bool {
	sc := gs.seacast
	if b := sc.rebuilt[root]; b == nil || !b.Has(index) {
		return false
	}
	key := string(p) + "\x00" + root
	if perGroup[root] != 1 || sc.hoardProbed[key] {
		sc.ctr.HoardRefused.Add(1)
		return true
	}
	if sc.hoardProbed == nil {
		sc.hoardProbed = map[string]bool{}
	}
	sc.hoardProbed[key] = true
	sc.ctr.HoardServed.Add(1)
	return false
}
