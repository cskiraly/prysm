package pubsub

import (
	"strings"
	"testing"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
)

// TestSeacastRoles is the role table's lookups on a hand-built router: a peer with interest ALL,
// one with DERIVED (the type's rule from its node id and custody count), one with DECLARED (no
// standing WANT yet), one the table does not know, and one known but not subscribed; wants_from_me
// after the peer's holding and after our push; serves by interest kind.
func TestSeacastRoles(t *testing.T) {
	root := strings.Repeat("r", haveRootLen)
	topic := "seg/col/0"
	T := &testObjectType{name: "col", params: TypeParams{RNode: 2}, topics: []string{topic},
		derived: func(_ string, id NodeID, j Join) ([]uint32, bool) {
			if j.CustodyCount == 0 {
				return nil, false
			}
			if id[0] == 1 {
				return nil, true // every index
			}
			return []uint32{3, 4}, false
		}}
	all, derivedAll, derivedTwo, declared, unknown, unsubscribed := peer.ID("all"), peer.ID("d1"), peer.ID("d2"), peer.ID("decl"), peer.ID("x"), peer.ID("y")
	table := &MapRoleTable{
		Joins: map[peer.ID]map[string]Join{
			all:          {"col": {Interest: InterestAll, Duty: DutyForward}},
			derivedAll:   {"col": {Interest: InterestDerived, CustodyCount: 8, Duty: DutyForward}},
			derivedTwo:   {"col": {Interest: InterestDerived, CustodyCount: 8, Duty: DutyForward}},
			declared:     {"col": {Interest: InterestDeclared, Duty: DutyNone}},
			unsubscribed: {"col": {Interest: InterestAll, Duty: DutyForward}},
		},
		IDs: map[peer.ID]NodeID{derivedAll: {1}, derivedTwo: {2}},
	}
	gs := &GossipSubRouter{}
	gs.p = &PubSub{host: getDefaultHosts(t, 1)[0], idGen: &msgIDGenerator{Default: seacastTestID, topicGens: map[string]MsgIdFunction{}},
		topics: map[string]map[peer.ID]peerTopicState{topic: {all: {}, derivedAll: {}, derivedTwo: {}, declared: {}, unknown: {}}}}
	gs.have = newHaveState(seacastTestIndex)
	gs.seacast = &seacast{types: map[string]ObjectType{topic: T}, ctr: &SeacastCounters{}, roles: table}
	mid := seacastTestID(&pb.Message{Data: append([]byte(root+string(byte(0))), 'x')})
	gs.seacastNoteRoot(topic, mid)

	count := func(b *haveBits) int {
		if b == nil {
			return -1
		}
		return b.Count()
	}
	for _, c := range []struct {
		p    peer.ID
		want int
	}{{all, 32}, {derivedAll, 32}, {derivedTwo, 2}, {declared, 0}, {unknown, -1}, {unsubscribed, -1}} {
		if got := count(gs.seacastInterestOfPeer(c.p, root)); got != c.want {
			t.Errorf("interest of %s: %d indices, want %d", c.p, got, c.want)
		}
	}
	// wants_from_me: the peer's holding and our push come off its interest.
	if got := count(gs.seacastWantsFromMe(all, root)); got != 32 {
		t.Fatalf("wants from me before anything: %d, want 32", got)
	}
	var held haveBits
	held.Set(0)
	held.Set(1)
	gs.seacastAbsorbV2(all, encodeHaveV2(root, &held, false, false))
	if got := count(gs.seacastWantsFromMe(all, root)); got != 30 {
		t.Fatalf("wants from me after the peer's holding of two: %d, want 30", got)
	}
	if gs.have.pushed[all] == nil {
		gs.have.pushed[all] = map[string]*haveEntry{}
	}
	pe := &haveEntry{}
	pe.bits.Set(2)
	gs.have.pushed[all][root] = pe
	if got := count(gs.seacastWantsFromMe(all, root)); got != 29 {
		t.Fatalf("wants from me after our push of one more: %d, want 29", got)
	}
	gs.have.peers[all][root].complete = true
	if got := count(gs.seacastWantsFromMe(all, root)); got != 0 {
		t.Fatalf("wants from me with the peer's interest satisfied: %d, want 0", got)
	}
	if got := count(gs.seacastWantsFromMe(unknown, root)); got != -1 {
		t.Fatalf("wants from me for a peer the table does not know: %d, want none", got)
	}
}
