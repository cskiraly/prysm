package pubsub

import (
	"reflect"
	"testing"
	"time"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
)

// TestSelectIWantsRecordsOnlyDispatched pins the select/commit seam.
//
// Each request policy records state as a side effect of allowing an id, so selecting freely and
// truncating afterwards would leave phantom entries: ids marked as requested that no IWANT ever
// carried, suppressed until their window expired -- and nondeterministically, since the old
// truncation followed a shuffle. The invariant is that the ids recorded as asked are exactly the
// ids dispatched.
func TestSelectIWantsRecordsOnlyDispatched(t *testing.T) {
	const (
		announced = 20
		allowance = 5
	)

	gs := &GossipSubRouter{
		iwantAsked:  make(map[string]time.Time),
		iwantWindow: time.Minute,
	}

	candidates := make([]string, announced)
	for i := range candidates {
		candidates[i] = string(rune('a' + i))
	}

	got := gs.selectIWants(candidates, nil, peer.ID("peer-1"), allowance)

	if len(got) != allowance {
		t.Fatalf("dispatched %d ids, want the full allowance %d", len(got), allowance)
	}
	if recorded := len(gs.iwantAsked); recorded != len(got) {
		t.Fatalf("recorded %d asks for %d dispatched ids: %d phantom entries",
			recorded, len(got), recorded-len(got))
	}
	dispatched := make(map[string]bool, len(got))
	for _, mid := range got {
		dispatched[mid] = true
	}
	for mid := range gs.iwantAsked {
		if !dispatched[mid] {
			t.Fatalf("id %q recorded as asked but never dispatched", mid)
		}
	}
}

// TestSelectIWantsZeroAllowance asks for nothing and must record nothing.
func TestSelectIWantsZeroAllowance(t *testing.T) {
	gs := &GossipSubRouter{
		iwantAsked:  make(map[string]time.Time),
		iwantWindow: time.Minute,
	}
	if got := gs.selectIWants([]string{"a", "b"}, nil, peer.ID("p"), 0); len(got) != 0 {
		t.Fatalf("dispatched %d ids at zero allowance", len(got))
	}
	if len(gs.iwantAsked) != 0 {
		t.Fatalf("recorded %d asks at zero allowance", len(gs.iwantAsked))
	}
}

// TestPublishBatchRechecksIDontWant pins the drain-time IDONTWANT check.
//
// A batch plans every (peer, message) pair before transmitting any of them, so an IDONTWANT arriving
// because of the batch's own first copies could not prune the remainder -- the relief batch
// publishing exists to provide when the publisher's uplink is the constraint. The tell in
// measurement was a publisher emitting a perfectly constant number of copies at every rate and
// network size; constancy is what says "structural", and nothing in the suite caught it.
func TestPublishBatchRechecksIDontWant(t *testing.T) {
	mkRPC := func(ids ...string) *RPC {
		msgs := make([]*pb.Message, 0, len(ids))
		for _, id := range ids {
			msgs = append(msgs, &pb.Message{Data: []byte(id)})
		}
		return &RPC{RPC: pb.RPC{Publish: msgs}}
	}
	// The id function is the message data, so a test can name ids directly.
	gen := &msgIDGenerator{}
	gen.Default = func(msg *pb.Message) string { return string(msg.GetData()) }
	gs := &GossipSubRouter{
		p:        &PubSub{idGen: gen},
		unwanted: make(map[peer.ID]map[checksum]int),
	}
	victim := peer.ID("p1")
	gs.unwanted[victim] = map[checksum]int{computeChecksum("b"): 3}

	t.Run("an unwanted single-message RPC is dropped entirely", func(t *testing.T) {
		if got := gs.withoutUnwanted(mkRPC("b"), victim); got != nil {
			t.Fatal("an RPC carrying only an unwanted message was still sent")
		}
	})

	t.Run("a wanted message is passed through untouched", func(t *testing.T) {
		in := mkRPC("a")
		got := gs.withoutUnwanted(in, victim)
		if got != in {
			t.Fatal("a wanted RPC was copied; the common case must allocate nothing")
		}
	})

	t.Run("a peer with no IDONTWANT state is untouched", func(t *testing.T) {
		in := mkRPC("b")
		if got := gs.withoutUnwanted(in, peer.ID("other")); got != in {
			t.Fatal("an unrelated peer's RPC was filtered")
		}
	})

	t.Run("a mixed RPC keeps the wanted messages and does not mutate the shared original", func(t *testing.T) {
		// gs.rpcs yields ONE shared *RPC per message to every recipient, so in-place filtering
		// would silently edit what other peers receive.
		in := mkRPC("a", "b", "c")
		got := gs.withoutUnwanted(in, victim)
		if got == in {
			t.Fatal("filtered in place; other peers' sends would be corrupted")
		}
		if len(got.Publish) != 2 ||
			string(got.Publish[0].GetData()) != "a" || string(got.Publish[1].GetData()) != "c" {
			t.Fatalf("kept %d messages, want [a c]", len(got.Publish))
		}
		if len(in.Publish) != 3 {
			t.Fatalf("the shared original now carries %d messages, want 3", len(in.Publish))
		}
	})

	t.Run("control-only content survives an all-unwanted publish list", func(t *testing.T) {
		in := mkRPC("b")
		topic := "t"
		in.Control = &pb.ControlMessage{Ihave: []*pb.ControlIHave{{TopicID: &topic}}}
		got := gs.withoutUnwanted(in, victim)
		if got == nil {
			t.Fatal("dropped an RPC that still carried control messages")
		}
		if len(got.Publish) != 0 {
			t.Fatalf("kept %d publish messages, want none", len(got.Publish))
		}
	})
}

// TestShallowRPCCopyCarriesEveryField guards the field list in shallowRPCCopy.
//
// The copy names pb.RPC's fields one at a time because copying the struct value would take
// protobuf's per-instance state with it. That leaves a maintenance hazard the compiler cannot see:
// a field added upstream is simply absent from the copy, and every filtered send drops it in
// silence. So the fields are enumerated by reflection here rather than restated -- adding one to
// pb.RPC without adding it to shallowRPCCopy fails this test.
func TestShallowRPCCopyCarriesEveryField(t *testing.T) {
	var in RPC
	in.from = peer.ID("origin")
	v := reflect.ValueOf(&in.RPC).Elem()
	rpcType := v.Type()
	for i := range rpcType.NumField() {
		f := rpcType.Field(i)
		if !f.IsExported() {
			continue
		}
		// Non-zero, so the assertion below can tell "carried" from "left at its zero value".
		switch f.Type.Kind() {
		case reflect.Slice:
			v.Field(i).Set(reflect.MakeSlice(f.Type, 1, 1))
		case reflect.Pointer:
			v.Field(i).Set(reflect.New(f.Type.Elem()))
		default:
			t.Fatalf("pb.RPC field %s is a %s, which this test cannot populate; teach it that kind",
				f.Name, f.Type.Kind())
		}
	}

	out := shallowRPCCopy(&in)
	ov := reflect.ValueOf(&out.RPC).Elem()
	for i := range rpcType.NumField() {
		f := rpcType.Field(i)
		if !f.IsExported() {
			continue
		}
		if ov.Field(i).IsZero() {
			t.Errorf("shallowRPCCopy drops pb.RPC field %s; add it there", f.Name)
		}
	}
	if out.from != in.from {
		t.Errorf("from = %q, want %q -- the unexported sender is part of the RPC too", out.from, in.from)
	}
}
