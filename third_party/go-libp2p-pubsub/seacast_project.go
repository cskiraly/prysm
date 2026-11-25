package pubsub

// The window's projection per object type (seacast.md §4.9, [c12]).
// Card 12 drops a queued chunk at the writer's pop when the peer is projected to
// complete the group without it: what it announced it holds plus FAN_IN times what we sent it
// beyond that reaches K. That premise holds for a type with a code, where any K chunks rebuild
// the group (the rows, K = 64 of 128), and fails for one without (the columns, K = N: a dropped
// cell is a cell the peer still needs, and in a thin family nobody else may be pushing it; the
// toy's custody setting found a custodian short of exactly the cells its pushers had projected
// away). So the threshold is the root's type's: K where K < N, nothing where K = N, and the
// router-wide value for a root off the path. Installed on every peer's queue beside the
// router-wide projection and independently of it (which is off without WithHaveDispatchProjection), and read by
// the writers from the queued message's topic and root alone: the types map is fixed at
// installation and the type's Threshold and IndexCount read no router-owned map. The solicited
// exemption stays the queue's: an answer is never projected away, and the ordinary cancellation
// of a stale answer stays.

import pb "github.com/libp2p/go-libp2p-pubsub/pb"

// seacastProjection is the per-type projection for the writers: nil without the path.
func (gs *GossipSubRouter) seacastProjection() func(m *pb.Message) (uint32, int, bool) {
	sc, h := gs.seacast, gs.have
	if sc == nil || h == nil || h.msgIndex == nil {
		return nil
	}
	types, index := sc.types, h.msgIndex
	return func(m *pb.Message) (uint32, int, bool) {
		T := types[m.GetTopic()]
		if T == nil {
			return 0, 0, false
		}
		root, _, ok := index(m)
		if !ok || len(root) != haveRootLen {
			return 0, 0, false
		}
		k, n := T.Threshold(root), T.IndexCount(root)
		if k <= 0 || k >= n {
			return 0, 0, true // K = N: no code across the group, nothing projected
		}
		fanIn := T.Params().ProjectFanIn
		if fanIn < 1 {
			fanIn = 1
		}
		return uint32(k), fanIn, true // lint:ignore uintcast -- a threshold below the group's count.
	}
}
