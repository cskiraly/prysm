package pubsub

import (
	"context"
	"strings"
	"testing"
	"time"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
)

// TestSeacastForgedFirstOffer: two ids can name one chunk when an id carries the content's hash, so a
// forger can offer a chunk's index under an id of its own before any honest copy arrives. Here the ids
// are a root, an index and a variant: X is A's copy of index 3, F the forger C's id for it. C offers F to
// B first, then A publishes X, which B accepts from A's push.
//
// On the path, B records an id only for a chunk it stored (seacast-lite.md §4.4, "Why `expand` looks
// up `known_id`"): C's offer leaves index 3 with no id, so an ask of it to any other announcer goes by
// index, and once B stores X, X is the index's id. Before, B kept F from C's offer and asked the other
// announcers by F.
//
// B answers an ask by index, and an ask under another id of an index it holds, with its own chunk, X
// (§4.5's resolve, [c7]: our chunk at that index); an id of an index B does not hold draws nothing. The
// type's store, which Prysm's payload type also keeps, answered such asks before too, by the index the
// id carries. What resolve adds is the count: an ask under any id of a held index is an ask of B's chunk,
// so one frame draws one answer per chunk, and GOSSIP_RETRANSMISSION (3) bounds the answers per chunk
// and peer whatever ids the asks carry. Before, each id was answered and counted on its own.
//
// Off the path the offered id is still learned, as before.
func TestSeacastForgedFirstOffer(t *testing.T) {
	const topic = "seg/col/0"
	const idLen = haveRootLen + 2 // the root, the index, a variant
	idOf := func(m *pb.Message) string { return string(m.GetData()[:idLen]) }
	index := func(mid string) (string, uint32, bool) {
		if len(mid) != idLen {
			return "", 0, false
		}
		return mid[:haveRootLen], uint32(mid[haveRootLen]), true
	}
	root := strings.Repeat("k", haveRootLen)
	X, F := root+"\x03x", root+"\x03f"
	cellX := append([]byte(X), strings.Repeat("x", 64)...)
	// newCell starts A = 0, C = 1 and B = 2, B linked to A and to C, all on topic.
	newCell := func(t *testing.T, path bool) ([]host.Host, []*PubSub) {
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		hosts := getDefaultHosts(t, 3)
		params := DefaultGossipSubParams()
		params.IDontWantMessageThreshold = 1
		psubs := getGossipsubsOptFn(ctx, hosts, func(int, host.Host) []Option {
			opts := []Option{WithGossipSubParams(params), WithMessageIdFn(idOf), WithHaveBitmaps(index)}
			if !path {
				return opts
			}
			T := &testObjectType{name: "col", params: TypeParams{RNode: 2}, topics: []string{topic}, store: NewMapCellStore()}
			return append(opts,
				WithPhaseForwarding(func(tp string) bool { return tp != topic }, 2),
				WithIWantDiscipline(200*time.Millisecond),
				WithIHaveCommitmentPark(1, 60*time.Second, 30*time.Second, nil, nil),
				WithIHaveParkPerGroup(nil),
				WithOfferTable(nil),
				WithSpecV05(SpecV05{}, nil),
				WithSeacast([]ObjectType{T}, nil),
			)
		})
		for _, ps := range psubs {
			if _, err := ps.Subscribe(topic); err != nil {
				t.Fatal(err)
			}
		}
		connect(t, hosts[0], hosts[2])
		connect(t, hosts[1], hosts[2])
		time.Sleep(2 * time.Second)
		return hosts, psubs
	}
	// offerF sends B, from C, an IHAVE naming F.
	offerF := func(hosts []host.Host, psubs []*PubSub) {
		tp := topic
		joinPushRun(psubs[1], func(gs *GossipSubRouter) {
			gs.sendRPC(hosts[2].ID(), rpcWithControl(nil, []*pb.ControlIHave{{TopicID: &tp, MessageIDs: []string{F}}}, nil, nil, nil, nil), false)
		})
		time.Sleep(100 * time.Millisecond)
	}
	learned := func(gs *GossipSubRouter) string {
		if m := gs.have.mids[root]; m != nil {
			return m.ids[3]
		}
		return ""
	}
	// answers is what B sends for an IWANT of ids from asker, a peer that holds nothing (C holds X by
	// then, from B's forward, and an ask from a holder of the index is dropped), by id.
	answers := func(gs *GossipSubRouter, asker peer.ID, ids ...string) []string {
		var out []string
		for _, m := range gs.handleIWant(asker, &pb.ControlMessage{Iwant: []*pb.ControlIWant{{MessageIDs: ids}}}) {
			out = append(out, idOf(m))
		}
		return out
	}

	t.Run("on the path", func(t *testing.T) {
		synctestTest(t, func(t *testing.T) {
			hosts, psubs := newCell(t, true)
			B := psubs[2]
			offerF(hosts, psubs)
			var id, ask string
			var offered bool
			joinPushRun(B, func(gs *GossipSubRouter) {
				id, ask = learned(gs), gs.chunkAskID(root, 3)
				offered = gs.pullOffered(hosts[1].ID(), F)
			})
			if !offered || id != "" || ask != encodeWant(root, 3) {
				t.Fatalf("after C's offer of F: C's offer kept %v, index 3's id %q, an ask of it %q; want true, none, the ask by index",
					offered, id, ask)
			}

			if err := psubs[0].Publish(topic, cellX); err != nil {
				t.Fatal(err)
			}
			time.Sleep(time.Second)
			var held bool
			var byIndex, byF, unheld, frame, fourth []string
			joinPushRun(B, func(gs *GossipSubRouter) {
				id = learned(gs)
				e := gs.have.mine[seacastGID(root)]
				held = e != nil && e.Bits.Has(3)
				byIndex = answers(gs, "D", encodeWant(root, 3))
				byF = answers(gs, "D", F)
				unheld = answers(gs, "D", root+"\x04f")
				// E asks in one frame under F, under G, a second forged id, and by index: three asks of
				// one chunk, the bound for E. A fourth, under H, finds it reached.
				frame = answers(gs, "E", F, root+"\x03g", encodeWant(root, 3))
				fourth = answers(gs, "E", root+"\x03h")
			})
			if !held || id != X {
				t.Fatalf("after A's X: index 3 held %v, its id %q; want true, %q", held, id, X)
			}
			if len(byIndex) != 1 || byIndex[0] != X {
				t.Fatalf("an ask by index 3: answered %q, want X", byIndex)
			}
			if len(byF) != 1 || byF[0] != X {
				t.Fatalf("an ask by F, another id of index 3: answered %q, want X", byF)
			}
			if len(unheld) != 0 {
				t.Fatalf("an ask by an id of index 4, not held: answered %q, want nothing", unheld)
			}
			if len(frame) != 1 || frame[0] != X {
				t.Fatalf("one frame asking index 3 under F, G and by index: answered %q, want X once", frame)
			}
			if len(fourth) != 0 {
				t.Fatalf("a fourth ask of index 3, under H: answered %q, want nothing (GOSSIP_RETRANSMISSION 3 reached)", fourth)
			}
		})
	})

	t.Run("off the path, the offered id is learned", func(t *testing.T) {
		synctestTest(t, func(t *testing.T) {
			hosts, psubs := newCell(t, false)
			offerF(hosts, psubs)
			var id string
			joinPushRun(psubs[2], func(gs *GossipSubRouter) { id = learned(gs) })
			if id != F {
				t.Fatalf("after C's offer of F: index 3's id %q, want F", id)
			}
		})
	})
}
