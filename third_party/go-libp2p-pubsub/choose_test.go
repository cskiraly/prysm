package pubsub

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
)

// The selector (seacast.md §4.12).

// chooseQueue is a queue with the HAVE dispatch on and the given holder counts per index; nil
// holds keeps rarest dispatch off.
func chooseQueue(holds map[uint32]int, ownFirst bool, h *hoseWindow) *rpcQueue {
	q := newRpcQueue(64)
	q.enableHave(testQueueIndex, true, false)
	if holds != nil {
		q.enableRarest(func(m *pb.Message) (int, bool) {
			_, i, ok := testQueueIndex(m)
			c, known := holds[i]
			return c, ok && known
		})
	}
	q.enableChoose(ownFirst, nil)
	q.hose = h
	return q
}

// entry builds a queue entry: "C" control-only, "A" an answer, "O" an own push, "P" an ordinary
// push; idx is the frame's index.
func entry(t *testing.T, root, kind string, idx byte) *RPC {
	t.Helper()
	if kind == "C" {
		return testControlRPC()
	}
	rpc := testSizedRPC(t, root, idx, kaCellBytes)
	rpc.solicited = kind == "A"
	rpc.own = kind == "O"
	return rpc
}

// drain pops n entries and names each: its kind letter for control, else its index.
func drain(t *testing.T, q *rpcQueue, n int) string {
	t.Helper()
	var got []string
	for range n {
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		out, err := q.Pop(ctx)
		cancel()
		if err != nil {
			t.Fatalf("pop %d: %v", len(got), err)
		}
		if len(out.Publish) == 0 {
			got = append(got, "C")
			continue
		}
		_, i, _ := testQueueIndex(out.Publish[0])
		got = append(got, fmt.Sprint(i))
	}
	return strings.Join(got, " ")
}

func TestChooseKnownAnswer11(t *testing.T) {
	root := strings.Repeat("g", 32)
	// [C0, A, C1, O1, O2]: A is index 1, O1 index 3, O2 index 4, all of equal rarity.
	pinned := func(t *testing.T, q *rpcQueue) {
		t.Helper()
		for _, e := range []*RPC{entry(t, root, "C", 0), entry(t, root, "A", 1), entry(t, root, "C", 0),
			entry(t, root, "O", 3), entry(t, root, "O", 4)} {
			if err := q.Push(e, false); err != nil {
				t.Fatal(err)
			}
		}
	}
	equal := map[uint32]int{1: 2, 3: 2, 4: 2}
	t.Run("the pinned queue", func(t *testing.T) {
		q := chooseQueue(equal, true, nil)
		var own atomic.Int64 // the router's counter (ChooseStats)
		q.enableChoose(true, &own)
		pinned(t, q)
		if got := drain(t, q, 3); got != "C 3 4" {
			t.Fatalf("the draft's choose sends C0, O1, O2 (C 3 4), got %s", got)
		}
		if n := q.outboundStat().OwnFirst; n != 2 || own.Load() != 2 {
			t.Fatalf("own picks ahead of the head %d (router %d), want 2 and 2", n, own.Load())
		}
		old := chooseQueue(equal, false, nil)
		pinned(t, old)
		if got := drain(t, old, 2); got != "C 1" {
			t.Fatalf("the old selector sends C0, A (C 1), got %s", got)
		}
	})
	t.Run("of two own pushes the rarer goes first, the earliest on a tie", func(t *testing.T) {
		q := chooseQueue(map[uint32]int{3: 3, 4: 1, 5: 1}, true, nil)
		for _, e := range []*RPC{entry(t, root, "P", 2), entry(t, root, "O", 3), entry(t, root, "O", 4), entry(t, root, "O", 5)} {
			_ = q.Push(e, false)
		}
		if got := drain(t, q, 3); got != "4 5 3" {
			t.Fatalf("own by rarity, earliest on a tie: want 4 5 3, got %s", got)
		}
	})
	t.Run("without holder counts the earliest own push goes", func(t *testing.T) {
		q := chooseQueue(nil, true, nil)
		for _, e := range []*RPC{entry(t, root, "P", 2), entry(t, root, "C", 0), entry(t, root, "O", 3), entry(t, root, "O", 4)} {
			_ = q.Push(e, false)
		}
		if got := drain(t, q, 2); got != "3 4" {
			t.Fatalf("want the own pushes in order, 3 4, got %s", got)
		}
	})
	t.Run("an own answer is an answer, not an own push", func(t *testing.T) {
		q := chooseQueue(equal, true, nil)
		a := entry(t, root, "A", 3)
		a.own = true // as no enqueue would set it: doSendRPC clears own on an answer
		for _, e := range []*RPC{entry(t, root, "P", 2), entry(t, root, "C", 0), a} {
			_ = q.Push(e, false)
		}
		if got := drain(t, q, 1); got != "2" {
			t.Fatalf("the own rule passes over an answer: want the head 2, got %s", got)
		}
	})
}

// TestOwnChunksFirstReported: OwnChunksFirst reports WithOwnChunksFirst, and nothing without HAVE
// bitmaps; and the rule reaches the writer of each peer's queue (haveQueue), with the router's
// counter of its picks.
func TestOwnChunksFirstReported(t *testing.T) {
	p := peer.ID("p")
	gs := testHaveRouter()
	gs.seacast = &seacast{ctr: &SeacastCounters{}, expired: map[string]struct{}{}}
	q := newRpcQueue(8)
	ps := &PubSub{rt: gs, peers: map[peer.ID]*rpcQueue{p: q}}
	gs.p = ps
	if ps.OwnChunksFirst() {
		t.Fatal("own chunks first reported before the option")
	}
	if err := WithOwnChunksFirst()(ps); err != nil {
		t.Fatal(err)
	}
	if !ps.OwnChunksFirst() {
		t.Fatal("own chunks first not reported after the option")
	}
	if (&PubSub{rt: &GossipSubRouter{}}).OwnChunksFirst() {
		t.Fatal("own chunks first reported without HAVE bitmaps")
	}
	if gs.haveQueue(p) != q {
		t.Fatal("the peer's queue was not found")
	}
	q.queueMu.Lock()
	ownFirst, counter := q.have.ownFirst, q.have.ownPickCtr
	q.queueMu.Unlock()
	if !ownFirst || counter != &gs.have.ownPicks {
		t.Fatalf("the peer's writer: own first %v, the router's pick counter %v; want both", ownFirst, counter == &gs.have.ownPicks)
	}
}

// TestOwnAtEnqueue: what the router marks own. Publish marks a message received from ourselves and
// not rebuilt; a batch marks an RPC whose every message is not rebuilt; WithRebuiltPublication sets
// the mark the two read.
func TestOwnAtEnqueue(t *testing.T) {
	self, other := peer.ID("self"), peer.ID("other")
	m := &Message{Message: &pb.Message{Data: []byte("x")}, ReceivedFrom: self}
	if !ownPublication(m, self) {
		t.Fatal("a message we published is our own")
	}
	if ownPublication(&Message{Message: m.Message, ReceivedFrom: other}, self) {
		t.Fatal("a message received from a peer is not our own")
	}
	var pub PublishOptions
	if err := WithRebuiltPublication()(&pub); err != nil || !pub.rebuilt {
		t.Fatal("WithRebuiltPublication sets the rebuilt mark")
	}
	r := &Message{Message: &pb.Message{Data: []byte("y")}, ReceivedFrom: self, rebuilt: true}
	if ownPublication(r, self) {
		t.Fatal("a rebuilt chunk is not our own")
	}
	rebuilt := map[*pb.Message]bool{r.Message: true}
	if !ownBatch(&RPC{RPC: pb.RPC{Publish: []*pb.Message{m.Message}}}, rebuilt) {
		t.Fatal("a batch RPC of our chunks is own")
	}
	if ownBatch(&RPC{RPC: pb.RPC{Publish: []*pb.Message{m.Message, r.Message}}}, rebuilt) {
		t.Fatal("an RPC with a rebuilt chunk is not own")
	}
	if ownBatch(testControlRPC(), nil) {
		t.Fatal("control is not own")
	}
	if c := shallowRPCCopy(&RPC{own: true, solicited: false}); !c.own {
		t.Fatal("own survives the dispatch-time copy")
	}
}

// TestOwnChunkOnChunkTopics: only a chunk is our own (§4.12's own_chunk). A message we publish on a
// type's topic or on the phase path's is; one on any other topic (a block, an attestation) is not,
// so the writer keeps it in the queue's order. Nor is a rebuilt chunk, or a message from a peer. A
// batch's messages that are not our own chunks are the ones ownBatch refuses.
func TestOwnChunkOnChunkTopics(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		const typeTopic, phaseTopic, stockTopic = "seg/col/0", "phase/0", "beacon_block"
		hosts := getDefaultHosts(t, 1)
		psubs := getGossipsubsOptFn(ctx, hosts, func(int, host.Host) []Option {
			T := &testObjectType{name: "col", params: TypeParams{RNode: 2}, topics: []string{typeTopic}}
			return []Option{
				WithMessageSignaturePolicy(StrictNoSign), WithNoAuthor(),
				WithMessageIdFn(seacastTestID), WithHaveBitmaps(seacastTestIndex), WithHaveAnnounceBitmaps(),
				WithPhaseForwarding(func(topic string) bool { return strings.HasPrefix(topic, "phase/") }, 2),
				WithSeacast([]ObjectType{T}, nil), WithOwnChunksFirst(),
			}
		})
		gs := psubs[0].rt.(*GossipSubRouter)
		self := hosts[0].ID()
		msg := func(topic string, from peer.ID, rebuilt bool) *Message {
			return &Message{Message: &pb.Message{Topic: &topic, Data: []byte(topic)}, ReceivedFrom: from, rebuilt: rebuilt}
		}
		onType, onPhase, stock := msg(typeTopic, self, false), msg(phaseTopic, self, false), msg(stockTopic, self, false)
		rebuilt, fromPeer := msg(typeTopic, self, true), msg(typeTopic, peer.ID("other"), false)
		done := make(chan struct{})
		gs.p.eval <- func() {
			defer close(done)
			for _, c := range []struct {
				name string
				m    *Message
				want bool
			}{
				{"our chunk on a type's topic", onType, true},
				{"our chunk on the phase path", onPhase, true},
				{"our stock message", stock, false},
				{"a chunk we rebuilt", rebuilt, false},
				{"a peer's chunk", fromPeer, false},
			} {
				if got := gs.ownChunk(c.m); got != c.want {
					t.Errorf("%s: own %v, want %v", c.name, got, c.want)
				}
			}
			notOwn := gs.notOwnChunks([]*Message{onType, onPhase, stock, rebuilt})
			if len(notOwn) != 2 || !notOwn[stock.Message] || !notOwn[rebuilt.Message] {
				t.Errorf("a batch's messages not our own: %d, stock %v, rebuilt %v; want the stock and the rebuilt",
					len(notOwn), notOwn[stock.Message], notOwn[rebuilt.Message])
			}
			if !ownBatch(&RPC{RPC: pb.RPC{Publish: []*pb.Message{onType.Message, onPhase.Message}}}, notOwn) {
				t.Error("a batch RPC of our chunks is own")
			}
			if ownBatch(&RPC{RPC: pb.RPC{Publish: []*pb.Message{onType.Message, stock.Message}}}, notOwn) {
				t.Error("a batch RPC with a stock message is not own")
			}
		}
		<-done
	})
}

// kaCellBytes is a cell's data bytes in the selector test's queues.
const kaCellBytes = 2183

// testSizedMsg is a frame for (root, index) of size data bytes, as testIndex reads it (root, the
// index byte, '#', then padding).
func testSizedMsg(root string, index byte, size int) *pb.Message {
	data := append([]byte(root+string([]byte{index})+"#"), make([]byte, size-len(root)-2)...)
	return &pb.Message{Data: data}
}

// testSizedRPC is a data RPC carrying one frame of size data bytes for each index.
func testSizedRPC(t *testing.T, root string, index byte, size int, more ...byte) *RPC {
	t.Helper()
	rpc := &RPC{RPC: pb.RPC{Publish: []*pb.Message{testSizedMsg(root, index, size)}}}
	for _, i := range more {
		rpc.Publish = append(rpc.Publish, testSizedMsg(root, i, size))
	}
	rpc.size = size * len(rpc.Publish)
	return rpc
}

func testControlRPC() *RPC {
	topic := "t"
	return &RPC{RPC: pb.RPC{Control: &pb.ControlMessage{Ihave: []*pb.ControlIHave{{TopicID: &topic, MessageIDs: []string{"x"}}}}}}
}
