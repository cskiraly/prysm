package pubsub

// The role table (seacast.md §1.2, §2's Join, §4.1's interest_of_peer, wants_from_me and
// serves): on v0.3's wire JOIN is the subscription, which carries none of the
// draft's Join fields, so each peer's interest kind, custody count, duty and node id come from a
// table the application supplies, out of band and trusted (§3.5: this build does not test
// declaration negotiation or false declarations). join_recv[p][T] is then p's subscription to a
// topic of T plus the table's entry for (p, T); join_sent is our own entry the same way. The
// lookups here are what the later parts call: interest for pushes and offers ([I3]), service by
// interest, and the sampler's DECLARED interest from its standing WANTs (part 2).

import (
	"github.com/libp2p/go-libp2p/core/peer"
)

// InterestKind is what a node is to receive of a group (§1.2).
type InterestKind uint8

const (
	InterestAll      InterestKind = iota // the whole object
	InterestDerived                      // computed by both ends from the node id and the custody count
	InterestDeclared                     // told per group in a WANT: a sample
)

// Duty is what a node does for others (§1.2).
type Duty uint8

const (
	DutyForward Duty = iota // holds relations and forwards into others' interest
	DutyNone                // receives only; serves asks like any node
)

// Join is a peer's declaration for one object type (§2's Join without the wire fields the
// subscription carries).
type Join struct {
	Interest     InterestKind
	CustodyCount uint64 // the input to derived interest (the ENR's cgc), else 0
	Duty         Duty
}

// NodeID is the id derived interest is computed from (the ENR's node id).
type NodeID [32]byte

// RoleTable gives each peer's Join per object type and its node id. Both run on the router
// goroutine; a peer the table does not know has no JOIN for the type.
type RoleTable interface {
	JoinOf(p peer.ID, T ObjectType) (Join, bool)
	NodeIDOf(p peer.ID) (NodeID, bool)
}

// MapRoleTable is a RoleTable in maps, keyed by peer and type name.
type MapRoleTable struct {
	Joins map[peer.ID]map[string]Join
	IDs   map[peer.ID]NodeID
}

func (m *MapRoleTable) JoinOf(p peer.ID, T ObjectType) (Join, bool) {
	j, ok := m.Joins[p][T.Name()]
	return j, ok
}

func (m *MapRoleTable) NodeIDOf(p peer.ID) (NodeID, bool) {
	id, ok := m.IDs[p]
	return id, ok
}

// WithSeacastRoles installs the role table; WithSeacast must come first.
func WithSeacastRoles(table RoleTable) Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok || gs.seacast == nil {
			return errRoleTableBeforePath
		}
		gs.seacast.roles = table
		return nil
	}
}

type seacastErr string

func (e seacastErr) Error() string { return string(e) }

const errRoleTableBeforePath = seacastErr("seacast: WithSeacastRoles needs WithSeacast before it")

// seacastJoinOf is join_recv[p][T] for the group root: p's subscription to the root's topic and
// the table's entry for (p, T). Router goroutine.
func (gs *GossipSubRouter) seacastJoinOf(p peer.ID, root string) (Join, ObjectType, bool) {
	sc := gs.seacast
	if sc == nil || sc.roles == nil {
		return Join{}, nil, false
	}
	T := gs.seacastRootType(root)
	if T == nil {
		return Join{}, nil, false
	}
	topic, known := sc.rootTopic[root]
	if !known {
		return Join{}, nil, false
	}
	j, ok := sc.roles.JoinOf(p, T)
	if !ok {
		return Join{}, nil, false
	}
	// A DECLARED peer subscribes to nothing (a sampler, §4.10): its Join is the table's alone. A
	// DERIVED or ALL peer's relation for the group is per topic, so it needs the subscription.
	if p != gs.p.host.ID() && j.Interest != InterestDeclared {
		if _, subscribed := gs.p.topics[topic][p]; !subscribed {
			return Join{}, nil, false
		}
	}
	return j, T, true
}

// seacastDutyNone says whether the node's own duty for the topic's type is NONE (§1.2: receives
// only; serves asks like any node): then it forwards and offers nothing of the type's chunks.
// Router goroutine.
func (gs *GossipSubRouter) seacastDutyNone(topic string) bool {
	sc := gs.seacast
	if sc == nil || sc.roles == nil {
		return false
	}
	T := sc.types[topic]
	if T == nil {
		return false
	}
	j, ok := sc.roles.JoinOf(gs.p.host.ID(), T)
	return ok && j.Duty == DutyNone
}

// seacastInterestOfPeer is interest_of_peer(p, g): what p is to receive of the group, as far as
// we know; nil when p has no JOIN for the group's type. Router goroutine.
func (gs *GossipSubRouter) seacastInterestOfPeer(p peer.ID, root string) *haveBits {
	j, T, ok := gs.seacastJoinOf(p, root)
	if !ok {
		return nil
	}
	return gs.seacastInterestFrom(p, j, T, root)
}

// seacastInterestOfPeerT is interest_of_peer(p, g) for a group whose type the caller resolved
// though the node runs none of its topics (part 4's repair into a column outside the node's
// custody, seacast_decode.go): p's Join for T, the subscription to topic checked as seacastJoinOf
// checks it. Router goroutine.
func (gs *GossipSubRouter) seacastInterestOfPeerT(p peer.ID, T ObjectType, root, topic string) *haveBits {
	sc := gs.seacast
	if sc == nil || sc.roles == nil {
		return nil
	}
	j, ok := sc.roles.JoinOf(p, T)
	if !ok {
		return nil
	}
	if p != gs.p.host.ID() && j.Interest != InterestDeclared {
		if _, subscribed := gs.p.topics[topic][p]; !subscribed {
			return nil
		}
	}
	return gs.seacastInterestFrom(p, j, T, root)
}

// seacastInterestFrom is p's interest in the group under its Join j for T. Router goroutine.
func (gs *GossipSubRouter) seacastInterestFrom(p peer.ID, j Join, T ObjectType, root string) *haveBits {
	out := &haveBits{}
	switch j.Interest {
	case InterestAll:
		for i := range T.IndexCount(root) {
			out.Set(uint32(i)) // lint:ignore uintcast -- an index below the group's count.
		}
	case InterestDerived:
		id, known := gs.seacast.roles.NodeIDOf(p)
		if !known {
			return out
		}
		indices, all := T.DerivedInterest(root, id, j)
		if all {
			for i := range T.IndexCount(root) {
				out.Set(uint32(i)) // lint:ignore uintcast -- an index below the group's count.
			}
		} else {
			for _, i := range indices {
				out.Set(i)
			}
		}
	case InterestDeclared:
		// Only what p asked us for: the standing WANTs of part 2; none yet.
		if w := gs.seacast.wantP[p][root]; w != nil {
			return w
		}
	}
	return out
}

// seacastWantsFromMe is wants_from_me(p, g) ([I3]): p's interest less what p holds and what we
// pushed it; nil when p has no JOIN for the type, empty when its interest is satisfied.
func (gs *GossipSubRouter) seacastWantsFromMe(p peer.ID, root string) *haveBits {
	in := gs.seacastInterestOfPeer(p, root)
	if in == nil {
		return nil
	}
	if gs.have == nil {
		return in
	}
	if e := gs.have.peers[p][root]; e != nil {
		if e.complete {
			return &haveBits{}
		}
		in = in.AndNot(&e.bits)
	}
	if pe := gs.have.pushed[p][root]; pe != nil {
		in = in.AndNot(&pe.bits)
	}
	return in
}
