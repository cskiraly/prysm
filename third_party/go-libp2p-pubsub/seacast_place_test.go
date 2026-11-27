package pubsub

import (
	"context"
	"strings"
	"testing"
	"time"

	core "github.com/cskiraly/seacast-go"
	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/host"
)

// TestSeacastRootTypePlacesOneClaimant: a root no message has placed is placed by the types'
// TopicOf when one type places it, as before; when two do, it stays unplaced and unregistered, and
// the repair lookup finds no type either. Each case runs several roots, so that the map order of
// the types cannot hide a placement by whichever type comes first.
func TestSeacastRootTypePlacesOneClaimant(t *testing.T) {
	newRouter := func(types ...ObjectType) *GossipSubRouter {
		t.Helper()
		ps := &PubSub{rt: &GossipSubRouter{}}
		for _, o := range []Option{
			WithHaveBitmaps(seacastTestIndex),
			WithPhaseForwarding(func(string) bool { return false }, 2),
			WithSeacast(types, nil),
		} {
			if err := o(ps); err != nil {
				t.Fatal(err)
			}
		}
		return ps.rt.(*GossipSubRouter)
	}
	root := func(i int) string { return strings.Repeat(string(rune('a'+i)), haveRootLen) }
	// Both answer for every root on their first topic (testObjectType's default TopicOf).
	pay := &testObjectType{name: "pay", topics: []string{"seg/pay/0"}}
	agg := &testObjectType{name: "agg", topics: []string{"seg/agg/0"}}

	gs := newRouter(pay)
	for i := 0; i < 8; i++ {
		if T := gs.seacastRootType(root(i)); T != pay || gs.seacast.rootTopic[root(i)] != "seg/pay/0" {
			t.Fatalf("one type: root %d placed under %v on %q, want pay on seg/pay/0", i, T, gs.seacast.rootTopic[root(i)])
		}
	}

	gs = newRouter(pay, agg)
	for i := 0; i < 8; i++ {
		if T := gs.seacastRootType(root(i)); T != nil {
			t.Fatalf("two claimants: root %d placed under %s", i, T.Name())
		}
		if _, registered := gs.seacast.roots[root(i)]; registered {
			t.Fatalf("two claimants: root %d registered", i)
		}
		if T := gs.seacastRepairType(root(i)); T != nil {
			t.Fatalf("two claimants: root %d repaired as %s", i, T.Name())
		}
	}

	// One of the two places only what it knows: the other places every root.
	known := root(0)
	aggKnows := &testObjectType{name: "agg", topics: []string{"seg/agg/0"}, topicOf: func(r string) (string, bool) {
		return "seg/agg/0", r == known
	}}
	gs = newRouter(pay, aggKnows)
	if T := gs.seacastRootType(root(1)); T != pay {
		t.Fatalf("one claimant: an unknown root placed under %v, want pay", T)
	}
	if T := gs.seacastRootType(known); T != nil {
		t.Fatalf("two claimants: the root both place went to %s", T.Name())
	}
}

// TestSeacastHaveBitmapPlacesByTopic: with two types that both answer TopicOf for any root, a HAVE
// bitmap that is B's first word of a group places the group under the type owning the IHAVE's
// topic, as an offered id does. Before, the bitmap's count check placed it by the TopicOf scan,
// under whichever type the map order gave first.
func TestSeacastHaveBitmapPlacesByTopic(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		hosts := getDefaultHosts(t, 2)
		types := make([][2]*testObjectType, len(hosts))
		pubsubs := getGossipsubsOptFn(ctx, hosts, func(i int, h host.Host) []Option {
			types[i] = [2]*testObjectType{
				{name: "pay", params: TypeParams{RNode: 2}, topics: []string{"seg/pay/0"}, store: NewMapCellStore()},
				{name: "agg", params: TypeParams{RNode: 2}, topics: []string{"seg/agg/0"}, store: NewMapCellStore()},
			}
			return []Option{
				WithMessageIdFn(seacastTestID),
				WithHaveBitmaps(seacastTestIndex),
				WithHaveAnnounceBitmaps(),
				WithPhaseForwarding(func(topic string) bool { return !strings.HasPrefix(topic, "seg/") }, 2),
				WithSeacast([]ObjectType{types[i][0], types[i][1]}, nil),
			}
		})
		for _, ps := range pubsubs {
			for _, topic := range []string{"seg/pay/0", "seg/agg/0"} {
				if _, err := ps.Subscribe(topic); err != nil {
					t.Fatal(err)
				}
			}
		}
		connect(t, hosts[0], hosts[1])
		time.Sleep(2 * time.Second)

		a, b := pubsubs[0].rt.(*GossipSubRouter), pubsubs[1].rt.(*GossipSubRouter)
		topicOf := func(i int) string {
			if i%2 == 0 {
				return "seg/agg/0"
			}
			return "seg/pay/0"
		}
		root := func(i int) string { return strings.Repeat(string(rune('a'+i)), haveRootLen) }
		const roots = 8
		for i := 0; i < roots; i++ {
			var bits haveBits
			bits.Set(0)
			bits.Set(1)
			topic, mid := topicOf(i), encodeHave(root(i), &bits)
			done := make(chan struct{})
			a.p.eval <- func() {
				a.sendRPC(hosts[1].ID(), rpcWithControl(nil, []*pb.ControlIHave{{TopicID: &topic, MessageIDs: []string{mid}}}, nil, nil, nil, nil), false)
				close(done)
			}
			<-done
		}
		time.Sleep(100 * time.Millisecond)

		done := make(chan struct{})
		b.p.eval <- func() {
			defer close(done)
			for i := 0; i < roots; i++ {
				want := types[1][1]
				if topicOf(i) == "seg/pay/0" {
					want = types[1][0]
				}
				if got := b.seacast.roots[root(i)]; got != want {
					name := "none"
					if got != nil {
						name = got.Name()
					}
					t.Errorf("root %d offered on %s: placed under %s, want %s", i, topicOf(i), name, want.Name())
				}
				if got := b.seacast.rootTopic[root(i)]; got != topicOf(i) {
					t.Errorf("root %d: placed on %q, want %q", i, got, topicOf(i))
				}
			}
		}
		<-done
	})
}

// TestSeacastAcceptedChunkPlacesRoot: a type owns two topics (a fork's two digests, overlapping) and,
// like Prysm's before it notes a group, places a root (TopicOf) on the current topic, the second. B
// first meets a root in an offer on the first topic, which places the root there; the first chunk of
// the group B accepts places it again on the type's topic, which the group's joins, ties and gossip
// follow.
func TestSeacastAcceptedChunkPlacesRoot(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		hosts := getDefaultHosts(t, 2)
		topics := []string{"seg/col/0", "seg/col/1"}
		pubsubs := getGossipsubsOptFn(ctx, hosts, func(i int, _ host.Host) []Option {
			T := &testObjectType{name: "col", params: TypeParams{RNode: 2}, topics: topics, store: NewMapCellStore(),
				topicOf: func(root string) (string, bool) { return topics[1], len(root) == haveRootLen }} // the current topic
			return []Option{
				WithMessageIdFn(seacastTestID),
				WithHaveBitmaps(seacastTestIndex),
				WithPhaseForwarding(func(topic string) bool { return !strings.HasPrefix(topic, "seg/") }, 2),
				WithSeacast([]ObjectType{T}, nil),
			}
		})
		for _, ps := range pubsubs {
			for _, topic := range topics {
				if _, err := ps.Subscribe(topic); err != nil {
					t.Fatal(err)
				}
			}
		}
		connect(t, hosts[0], hosts[1])
		time.Sleep(2 * time.Second)

		root := strings.Repeat("p", haveRootLen)
		mid := root + string(byte(2))
		joinPushRun(pubsubs[0], func(gs *GossipSubRouter) { // A offers the chunk on the first topic
			gs.sendRPC(hosts[1].ID(), rpcWithControl(nil, []*pb.ControlIHave{{TopicID: &topics[0], MessageIDs: []string{mid}}}, nil, nil, nil, nil), false)
		})
		time.Sleep(100 * time.Millisecond)
		var first string
		joinPushRun(pubsubs[1], func(gs *GossipSubRouter) { first = gs.seacast.rootTopic[root] })
		if first != topics[0] {
			t.Fatalf("after the offer on %s: the root placed on %q; want %s", topics[0], first, topics[0])
		}
		if err := pubsubs[0].Publish(topics[1], seacastCell(root, 2)); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Second)
		var after string
		var accepted, replaced int64
		joinPushRun(pubsubs[1], func(gs *GossipSubRouter) {
			after, accepted, replaced = gs.seacast.rootTopic[root], gs.seacast.ctr.Accepted.Load(), gs.seacast.ctr.RootsReplaced.Load()
		})
		if accepted != 1 || after != topics[1] || replaced != 1 {
			t.Fatalf("after B accepted the chunk: accepted %d, the root on %q, placed again %d; want 1, %s, 1", accepted, after, replaced, topics[1])
		}
	})
}

// TestSeacastBitmapPlacedBeforeLookup: a type owns two topics (a fork's two digests) and, like Prysm's,
// places a root it has not noted on the current topic, the second (TopicOf). An offer's bitmap of a
// fresh root on the first topic places the root there, on the topic B met it on, before any check
// looks the root up: a lookup would place it on the current topic by TopicOf, and the offer's checks
// read the root's type and window. With nothing noted, the root keeps that topic, until B publishes a
// chunk of the group itself, in a batch (Prysm's source): its own chunk places the root on its type's
// topic, as an accepted chunk does.
func TestSeacastBitmapPlacedBeforeLookup(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		hosts := getDefaultHosts(t, 2)
		topics := []string{"seg/col/0", "seg/col/1"}
		pubsubs := getGossipsubsOptFn(ctx, hosts, func(i int, _ host.Host) []Option {
			T := &testObjectType{name: "col", params: TypeParams{RNode: 2}, topics: topics, store: NewMapCellStore(),
				topicOf: func(root string) (string, bool) { return topics[1], len(root) == haveRootLen }} // the current topic
			return []Option{
				WithMessageIdFn(seacastTestID),
				WithHaveBitmaps(seacastTestIndex),
				WithHaveAnnounceBitmaps(),
				WithPhaseForwarding(func(topic string) bool { return !strings.HasPrefix(topic, "seg/") }, 2),
				WithSeacast([]ObjectType{T}, nil),
			}
		})
		for _, ps := range pubsubs {
			for _, topic := range topics {
				if _, err := ps.Subscribe(topic); err != nil {
					t.Fatal(err)
				}
			}
		}
		connect(t, hosts[0], hosts[1])
		time.Sleep(2 * time.Second)

		root := strings.Repeat("q", haveRootLen)
		var bits haveBits
		bits.Set(2)
		mid := encodeHave(root, &bits)
		joinPushRun(pubsubs[0], func(gs *GossipSubRouter) { // A offers the group's index 2 on the first topic
			gs.sendRPC(hosts[1].ID(), rpcWithControl(nil, []*pb.ControlIHave{{TopicID: &topics[0], MessageIDs: []string{mid}}}, nil, nil, nil, nil), false)
		})
		time.Sleep(100 * time.Millisecond)
		var placed string
		var replaced int64
		joinPushRun(pubsubs[1], func(gs *GossipSubRouter) {
			placed, replaced = gs.seacast.rootTopic[root], gs.seacast.ctr.RootsReplaced.Load()
		})
		if placed != topics[0] || replaced != 0 {
			t.Fatalf("after the bitmap on %s: the root placed on %q, placed again %d; want %s, 0", topics[0], placed, replaced, topics[0])
		}
		var tp *Topic
		joinPushRun(pubsubs[1], func(gs *GossipSubRouter) { tp = gs.p.myTopics[topics[1]] })
		var batch MessageBatch
		if err := tp.AddToBatch(ctx, &batch, seacastCell(root, 2)); err != nil {
			t.Fatal(err)
		}
		if err := pubsubs[1].PublishBatch(&batch); err != nil {
			t.Fatal(err)
		}
		time.Sleep(100 * time.Millisecond)
		joinPushRun(pubsubs[1], func(gs *GossipSubRouter) {
			placed, replaced = gs.seacast.rootTopic[root], gs.seacast.ctr.RootsReplaced.Load()
		})
		if placed != topics[1] || replaced != 1 {
			t.Fatalf("after B published the group on %s: the root placed on %q, placed again %d; want %s, 1", topics[1], placed, replaced, topics[1])
		}
	})
}

// TestSeacastIgnoredOfferPlacesNothing: an offered id places its root on the topic it is offered on,
// before the checks that look the root up, only in a valid id's form (seacast-lite.md §4.4's
// valid_id): an id naming an index at MAX_PLAIN_CHUNKS is ignored and places nothing, and a valid id
// of the same root, offered next, places it. The parser reads a one- or two-byte index, so an id can
// name an index past the bound.
func TestSeacastIgnoredOfferPlacesNothing(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		hosts := getDefaultHosts(t, 2)
		topics := []string{"seg/col/0", "seg/col/1"}
		index := func(mid string) (string, uint32, bool) {
			switch len(mid) {
			case haveRootLen + 1:
				return mid[:haveRootLen], uint32(mid[haveRootLen]), true
			case haveRootLen + 2:
				return mid[:haveRootLen], uint32(mid[haveRootLen])<<8 | uint32(mid[haveRootLen+1]), true
			}
			return "", 0, false
		}
		pubsubs := getGossipsubsOptFn(ctx, hosts, func(i int, _ host.Host) []Option {
			T := &testObjectType{name: "col", params: TypeParams{RNode: 2}, topics: topics, store: NewMapCellStore(),
				topicOf: func(root string) (string, bool) { return topics[1], len(root) == haveRootLen }} // the current topic
			return []Option{
				WithMessageIdFn(seacastTestID),
				WithHaveBitmaps(index),
				WithPhaseForwarding(func(topic string) bool { return !strings.HasPrefix(topic, "seg/") }, 2),
				WithSeacast([]ObjectType{T}, nil),
			}
		})
		for _, ps := range pubsubs {
			for _, topic := range topics {
				if _, err := ps.Subscribe(topic); err != nil {
					t.Fatal(err)
				}
			}
		}
		connect(t, hosts[0], hosts[1])
		time.Sleep(2 * time.Second)

		root := strings.Repeat("i", haveRootLen)
		offer := func(mid string) {
			joinPushRun(pubsubs[0], func(gs *GossipSubRouter) {
				gs.sendRPC(hosts[1].ID(), rpcWithControl(nil, []*pb.ControlIHave{{TopicID: &topics[0], MessageIDs: []string{mid}}}, nil, nil, nil, nil), false)
			})
			time.Sleep(100 * time.Millisecond)
		}
		placed := func() (topic string, known bool) {
			joinPushRun(pubsubs[1], func(gs *GossipSubRouter) {
				_, known = gs.seacast.roots[root]
				topic = gs.seacast.rootTopic[root]
			})
			return topic, known
		}
		past := core.MaxPlainChunks
		offer(root + string([]byte{byte(past >> 8), byte(past)}))
		if topic, known := placed(); known {
			t.Fatalf("after an offer of index %d: the root registered, on %q; want unregistered", past, topic)
		}
		offer(root + string(byte(2)))
		if topic, known := placed(); !known || topic != topics[0] {
			t.Fatalf("after an offer of index 2: registered %v, on %q; want true, %s", known, topic, topics[0])
		}
	})
}
