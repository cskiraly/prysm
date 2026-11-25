package pubsub

import (
	"strings"
	"testing"
)

type testObjectType struct {
	name       string
	params     TypeParams
	topics     []string
	gateClosed bool
	store      *MapCellStore
	derived    func(root string, id NodeID, j Join) ([]uint32, bool)
	slots      map[string]uint64 // the roots the type can place (seacast_window.go)
	chunkSlots map[string]uint64 // the slot a chunk's copy names, by its group root (ChunkSlot)
	topicOf    func(root string) (string, bool)
	// The two-axis lookups (seacast_cross.go): K (IndexCount when zero), the other pair with its
	// form's id (none when nil), the family (the type's name when nil).
	threshold  int
	indexCount int // the group's indices (32 when zero)
	cross      func(root string, index uint32) (GroupIndex, string, bool)
	family     func(root string) string
}

func (t *testObjectType) Threshold(string) int {
	if t.threshold > 0 {
		return t.threshold
	}
	return 32
}
func (t *testObjectType) Cross(root string, index uint32) (GroupIndex, string, bool) {
	if t.cross == nil {
		return GroupIndex{}, "", false
	}
	return t.cross(root, index)
}
func (t *testObjectType) Family(root string) string {
	if t.family == nil {
		return t.name
	}
	return t.family(root)
}

func (t *testObjectType) Name() string            { return t.name }
func (t *testObjectType) Params() TypeParams      { return t.params }
func (t *testObjectType) Topics() []string        { return t.topics }
func (t *testObjectType) HeaderKnown(string) bool { return !t.gateClosed }
func (t *testObjectType) IndexCount(string) int {
	if t.indexCount > 0 {
		return t.indexCount
	}
	return 32
}

// ChunkSlot is the slot a copy names (seacast-lite.md §4.2's f.slot): in the test ids, the one set for its root.
func (t *testObjectType) ChunkSlot(mid string) (uint64, bool) {
	if len(mid) < haveRootLen {
		return 0, false
	}
	s, ok := t.chunkSlots[mid[:haveRootLen]]
	return s, ok
}
func (t *testObjectType) SlotOf(root string) (uint64, bool) {
	s, ok := t.slots[root]
	return s, ok
}
func (t *testObjectType) TopicOf(root string) (string, bool) {
	if t.topicOf != nil {
		return t.topicOf(root)
	}
	if len(t.topics) == 0 || len(root) != haveRootLen {
		return "", false
	}
	return t.topics[0], true // every group of the type on its first topic, unless the test says otherwise
}
func (t *testObjectType) DerivedInterest(root string, id NodeID, j Join) ([]uint32, bool) {
	if t.derived == nil {
		return nil, true
	}
	return t.derived(root, id, j)
}
func (t *testObjectType) Store() CellStore {
	if t.store == nil {
		return nil
	}
	return t.store
}

// TestSeacastRegistry is the registry's contract: a type owns its topics, two types on one topic
// are refused, a topic the phase matcher accepts is refused at installation, and the path's
// degree follows the type's parameters.
func TestSeacastRegistry(t *testing.T) {
	cols := &testObjectType{name: "col", params: TypeParams{RNode: 2}, topics: []string{"seg/col/0", "seg/col/1"}}
	rows := &testObjectType{name: "row", params: TypeParams{RNode: 2}, topics: []string{"seg/row/0"}}
	both := &testObjectType{name: "dup", topics: []string{"seg/col/1"}}

	newRouter := func(opts ...Option) (*GossipSubRouter, error) {
		ps := &PubSub{rt: &GossipSubRouter{}}
		for _, o := range opts {
			if err := o(ps); err != nil {
				return nil, err
			}
		}
		return ps.rt.(*GossipSubRouter), nil
	}
	// The option needs the HAVE machinery and the phase path installed before it.
	have := WithHaveBitmaps(seacastTestIndex)
	accept := false // the phase matcher's answer for "seg/" topics, switched on later
	phase := WithPhaseForwarding(func(topic string) bool { return accept && strings.HasPrefix(topic, "seg/") }, 2)
	if _, err := newRouter(WithSeacast([]ObjectType{cols}, nil)); err == nil || !strings.Contains(err.Error(), "WithHaveBitmaps") {
		t.Fatalf("the option without the HAVE machinery not refused: %v", err)
	}
	if _, err := newRouter(have, WithSeacast([]ObjectType{cols}, nil)); err == nil || !strings.Contains(err.Error(), "WithPhaseForwarding") {
		t.Fatalf("the option without the phase path not refused: %v", err)
	}
	gs, err := newRouter(have, phase, WithSeacast([]ObjectType{cols, rows}, nil))
	if err != nil {
		t.Fatal(err)
	}
	if gs.seacastType("seg/col/1") != cols || gs.seacastType("seg/row/0") != rows || gs.seacastType("seg/other") != nil {
		t.Fatalf("the registry does not map the topics to their types")
	}
	if d, ok := gs.seacastDegree("seg/col/0"); !ok || d != 2 {
		t.Fatalf("columns' degree %d %v, want 2 true", d, ok)
	}
	if d, ok := gs.seacastDegree("seg/row/0"); !ok || d != 2 {
		t.Fatalf("rows' degree %d %v, want 2 true", d, ok)
	}
	if _, ok := gs.seacastDegree("seg/other"); ok {
		t.Fatalf("a topic off the path has a degree")
	}
	if _, err := newRouter(have, phase, WithSeacast([]ObjectType{cols, both}, nil)); err == nil || !strings.Contains(err.Error(), "owned by types") {
		t.Fatalf("two types on one topic not refused: %v", err)
	}
	accept = true
	if _, err := newRouter(have, phase, WithSeacast([]ObjectType{cols}, nil)); err == nil || !strings.Contains(err.Error(), "also matched by the phase path") {
		t.Fatalf("a topic the phase matcher accepts not refused at installation: %v", err)
	}
	// The matcher comes to accept the topic after the installation (a predicate cannot be checked
	// for what it will say): the Join is refused and counted.
	accept = false
	ctr := &SeacastCounters{}
	gs, err = newRouter(have, phase, WithSeacast([]ObjectType{cols}, ctr))
	if err != nil {
		t.Fatal(err)
	}
	accept = true
	if !gs.seacastJoinRefused("seg/col/0") || ctr.Refused.Load() != 1 {
		t.Fatalf("a Join of a topic both paths claim not refused and counted")
	}
	if gs.seacastJoinRefused("seg/elsewhere") {
		t.Fatalf("a topic the path does not own refused")
	}
}
