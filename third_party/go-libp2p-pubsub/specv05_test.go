package pubsub

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p-pubsub/timecache"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-libp2p/x/simlibp2p"
	"github.com/marcopolo/simnet"
)

// The v0.5 rules (specv05.go) on real routers over simulated links. Each test is a witness the
// model check of v0.4 found, rebuilt on the fork: with the rule off it shows the failure, with the
// rule on it does not. Frames are test ids (testMid) padded past the IDONTWANT threshold, so a
// receiver's HAVE goes to its mesh as in the measured cells.

// v05Hosts is n hosts on links of mbps each way with a one-way latency.
func v05Hosts(t *testing.T, n int, mbps int, latency time.Duration) []host.Host {
	net, meta, err := simlibp2p.SimpleLibp2pNetwork(
		[]simlibp2p.NodeLinkSettingsAndCount{{
			LinkSettings: simnet.NodeBiDiLinkSettings{
				Downlink: simnet.LinkSettings{BitsPerSecond: mbps * simlibp2p.OneMbps},
				Uplink:   simnet.LinkSettings{BitsPerSecond: mbps * simlibp2p.OneMbps},
			},
			Count: n,
		}},
		simnet.StaticLatency(latency),
		simlibp2p.NetworkSettings{},
	)
	if err != nil {
		t.Fatal(err)
	}
	net.Start()
	t.Cleanup(func() {
		for _, h := range meta.Nodes {
			h.Close()
		}
		net.Close()
	})
	return meta.Nodes
}

const v05IDLen = 40 // len(testMid(root, i))

// v05Frame is frame i of root: its test id, padded past the IDONTWANT threshold.
func v05Frame(root string, i byte) []byte {
	return append([]byte(testMid(root, i)), make([]byte, 2048)...)
}

// v05Base is the tier-4 request and HAVE options the rules build on: phase forwarding at R = 2,
// both HAVE bitmaps, the dispatch-time cancel, the ask discipline, the park at k = 1 per group,
// the offer table. Gossip is off, so no heartbeat announcement rescues a stall. The dispatch
// ledger rides along, as in the harness's tier-4 cells: it creates each peer's dispatch state at
// the first send, without which the writer's projection and window start only at the peer's first
// HAVE.
func v05Base(parks *atomic.Int64) []Option {
	params := DefaultGossipSubParams()
	params.Dlazy = 0
	params.GossipFactor = 0
	return []Option{
		WithGossipSubParams(params),
		WithMessageSignaturePolicy(StrictNoSign), WithNoAuthor(),
		WithMessageIdFn(func(m *pb.Message) string { return string(m.Data[:v05IDLen]) }),
		WithPhaseForwarding(func(string) bool { return true }, 2),
		WithHaveBitmaps(testIndex), WithHaveAnnounceBitmaps(), WithHaveDispatch(true, false), WithHaveLedger(),
		WithIWantDiscipline(200 * time.Millisecond),
		WithIHaveCommitmentPark(1, 400*time.Millisecond, 30*time.Second, nil, parks),
		WithIHaveParkPerGroup(nil),
		WithOfferTable(nil),
	}
}

// v05Collect counts the distinct frames each subscription delivers, per root.
type v05Collect struct {
	mu  sync.Mutex
	got []map[string]map[byte]bool
}

func newV05Collect(ctx context.Context, t *testing.T, topics []*Topic) *v05Collect {
	c := &v05Collect{got: make([]map[string]map[byte]bool, len(topics))}
	for i, tp := range topics {
		c.got[i] = make(map[string]map[byte]bool)
		sub, err := tp.Subscribe()
		if err != nil {
			t.Fatal(err)
		}
		go func(i int) {
			for {
				m, err := sub.Next(ctx)
				if err != nil {
					return
				}
				root, idx, ok := testIndex(string(m.Data[:v05IDLen]))
				if !ok {
					continue
				}
				c.mu.Lock()
				if c.got[i][root] == nil {
					c.got[i][root] = make(map[byte]bool)
				}
				c.got[i][root][byte(idx)] = true
				c.mu.Unlock()
			}
		}(i)
	}
	return c
}

func (c *v05Collect) count(i int, root string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.got[i][root])
}

func v05Join(t *testing.T, psubs []*PubSub) []*Topic {
	var topics []*Topic
	for _, ps := range psubs {
		tp, err := ps.Join("t")
		if err != nil {
			t.Fatal(err)
		}
		topics = append(topics, tp)
	}
	return topics
}

// TestSpecV05OfferSkipped is F1: a push projection skips leaves no offer behind. Two nodes on a
// 25 ms link; the source publishes a group of 8 frames, any 4 of which complete it. Its writer pops
// the first two frames, after which the projection (fan-in 2) takes the receiver to 4 and skips
// the rest; the receiver was a push target, so nobody announced them, and it stays at 2. Under A′
// the skipped pushes are offered instead, the receiver asks, and it completes.
func TestSpecV05OfferSkipped(t *testing.T) {
	root := strings.Repeat("a", 32)
	run := func(t *testing.T, on bool) (got int, ctr *SpecV05Counters) {
		ctr = &SpecV05Counters{}
		synctestTest(t, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			hosts := v05Hosts(t, 2, 20, 25*time.Millisecond)
			psubs := getGossipsubsOptFn(ctx, hosts, func(i int, _ host.Host) []Option {
				opts := append(v05Base(nil), WithHaveDispatchProjection(4, 2))
				if on {
					opts = append(opts, WithSpecV05(SpecV05{OfferSkipped: true}, ctr))
				}
				return opts
			})
			topics := v05Join(t, psubs)
			c := newV05Collect(ctx, t, topics)
			connectAll(t, hosts)
			time.Sleep(2 * time.Second)
			var batch MessageBatch
			for i := byte(0); i < 8; i++ {
				if err := topics[0].AddToBatch(ctx, &batch, v05Frame(root, i)); err != nil {
					t.Fatal(err)
				}
			}
			if err := psubs[0].PublishBatch(&batch); err != nil {
				t.Fatal(err)
			}
			time.Sleep(3 * time.Second)
			got = c.count(1, root)
		})
		return got, ctr
	}
	off, _ := run(t, false)
	on, ctr := run(t, true)
	t.Logf("frames at the receiver: %d as measured, %d with A′ (unpushed %d, offers owed %d, sent %d)",
		off, on, ctr.Unpushed.Load(), ctr.OffersOwed.Load(), ctr.OwedSent.Load())
	if off >= 4 {
		t.Fatalf("known answer: without A′ the receiver should stay short of 4, got %d", off)
	}
	if on < 4 || ctr.OwedSent.Load() == 0 {
		t.Fatalf("with A′ the receiver should complete through offers, got %d frames, %d offered", on, ctr.OwedSent.Load())
	}
}

// TestSpecV05OwedAtFullQueue: what a full queue refuses is owed. The source's queue to the
// receiver holds two entries and the source publishes a group of 8 at once: the pushes past the
// queue are refused, and as measured the receiver gets only what fit, since nobody announced the
// rest to a push target. Under A′ each refused push is owed as an offer, which rides the next write;
// the receiver asks, and any answer the full queue refuses is requeued once there is room.
func TestSpecV05OwedAtFullQueue(t *testing.T) {
	root := strings.Repeat("f", 32)
	run := func(t *testing.T, on bool) (got int, ctr *SpecV05Counters) {
		ctr = &SpecV05Counters{}
		synctestTest(t, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			hosts := v05Hosts(t, 2, 20, 25*time.Millisecond)
			psubs := getGossipsubsOptFn(ctx, hosts, func(i int, _ host.Host) []Option {
				opts := v05Base(nil)
				if i == 0 {
					opts = append(opts, WithPeerOutboundQueueSize(2))
				}
				if on {
					opts = append(opts, WithSpecV05(SpecV05{OfferSkipped: true}, ctr))
				}
				return opts
			})
			topics := v05Join(t, psubs)
			c := newV05Collect(ctx, t, topics)
			connectAll(t, hosts)
			time.Sleep(2 * time.Second)
			var batch MessageBatch
			for i := byte(0); i < 8; i++ {
				if err := topics[0].AddToBatch(ctx, &batch, v05Frame(root, i)); err != nil {
					t.Fatal(err)
				}
			}
			if err := psubs[0].PublishBatch(&batch); err != nil {
				t.Fatal(err)
			}
			time.Sleep(3 * time.Second)
			got = c.count(1, root)
		})
		return got, ctr
	}
	off, _ := run(t, false)
	on, ctr := run(t, true)
	t.Logf("frames at the receiver: %d as measured, %d with A′ (unpushed %d, offers owed %d, sent %d, answers owed %d)",
		off, on, ctr.Unpushed.Load(), ctr.OffersOwed.Load(), ctr.OwedSent.Load(), ctr.AnswersOwed.Load())
	if off >= 8 {
		t.Fatalf("known answer: the full queue should cost the receiver frames as measured, got %d of 8", off)
	}
	if on != 8 || ctr.Unpushed.Load() == 0 || ctr.OwedSent.Load() == 0 {
		t.Fatalf("with A′ the receiver should get all 8 through the owed offers, got %d", on)
	}
}

// TestSpecV05OwedOnTheQueue is the writer's side of A′: owed offers ride the next RPC out as one
// IHAVE bitmap per group, less what the peer has told us it holds and the frame the RPC carries,
// or leave alone; an owed answer is queued once there is room, as an answer.
func TestSpecV05OwedOnTheQueue(t *testing.T) {
	root := strings.Repeat("g", 32)
	index := func(m *pb.Message) (string, uint32, bool) { return testIndex(string(m.Data[:v05IDLen])) }
	topic := "t"
	frame := func(i byte) *pb.Message { return &pb.Message{Data: v05Frame(root, i), Topic: &topic} }
	bits := func(is ...uint32) *haveBits {
		var b haveBits
		for _, i := range is {
			b.Set(i)
		}
		return &b
	}
	offered := func(rpc *RPC) []uint32 {
		var out []uint32
		for _, ih := range rpc.GetControl().GetIhave() {
			for _, id := range ih.GetMessageIDs() {
				r, enc, ok := decodeHave(id)
				if !ok || r != root {
					t.Fatalf("not a HAVE bitmap of the root: %q", id)
				}
				var b haveBits
				out = append(out, b.Or(enc)...)
			}
		}
		return out
	}
	ctx := context.Background()

	q := newRpcQueue(1)
	q.enableHave(index, true, false)
	var sent atomic.Int64
	q.owedSent = &sent
	if err := q.Push(&RPC{RPC: pb.RPC{Publish: []*pb.Message{frame(0)}}}, false); err != nil {
		t.Fatal(err)
	}
	if err := q.Push(&RPC{RPC: pb.RPC{Publish: []*pb.Message{frame(9)}}}, false); err != ErrQueueFull {
		t.Fatalf("a second entry must be refused, got %v", err)
	}
	q.owe(topic, root, bits(0, 1, 2, 3))
	q.haveTheirs(root, bits(2).Bytes())
	q.oweAnswer(frame(5))

	out, err := q.Pop(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Publish) != 1 || fmt.Sprint(offered(out)) != "[1 3]" {
		t.Fatalf("the frame should carry the owed offers less its own index and the peer's HAVE: %d frames, offers %v", len(out.Publish), offered(out))
	}
	if sent.Load() != 2 {
		t.Fatalf("two owed indices left, counted %d", sent.Load())
	}
	ans, err := q.Pop(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(ans.Publish) != 1 || !ans.solicited || string(ans.Publish[0].Data[:v05IDLen]) != testMid(root, 5) {
		t.Fatalf("the owed answer should follow as an answer: %+v", ans)
	}
	q.owe(topic, root, bits(7))
	alone, err := q.Pop(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(alone.Publish) != 0 || fmt.Sprint(offered(alone)) != "[7]" {
		t.Fatalf("an owed offer with nothing queued should leave alone: %+v", alone)
	}
}

// TestSpecV05OwedAnswersOfChunksOnly: of the answers a full queue refuses, A′ owes only those of a
// chunk, as the spec's owed_answers holds (a bit per connection, group and index: seacast-lite.md
// §4.1, §8.1). An answer of any other message, a block or an attestation on a topic off the path,
// is dropped as stock gossipsub drops it; owed, it would stay for as long as the connection, one
// for each distinct message a peer that doesn't read asked for. Before the rule was narrowed, both
// answers below were owed.
func TestSpecV05OwedAnswersOfChunksOnly(t *testing.T) {
	live := strings.Repeat("l", 32)
	p := peer.ID("peer")
	topic, stock := "t", "blocks"
	gs := testHaveRouter()
	gs.have.ihaveOn = true
	gs.peers = map[peer.ID]protocol.ID{p: GossipSubID_v12}
	q := newRpcQueue(1)
	gs.p = &PubSub{seenMessages: timecache.NewTimeCache(time.Minute), idGen: newMsgIdGenerator(),
		peers: map[peer.ID]*rpcQueue{p: q}}
	gs.p.idGen.Default = func(m *pb.Message) string { return string(m.Data[:v05IDLen]) }
	ctr := &SpecV05Counters{}
	if err := WithSpecV05(SpecV05{OfferSkipped: true}, ctr)(&PubSub{rt: gs, idGen: gs.p.idGen}); err != nil {
		t.Fatal(err)
	}
	q.enableHave(func(m *pb.Message) (string, uint32, bool) { return testIndex(string(m.Data[:v05IDLen])) }, true, false)

	chunk := &pb.Message{Data: v05Frame(live, 1), Topic: &topic}
	answers := &RPC{RPC: pb.RPC{Publish: []*pb.Message{
		chunk,
		{Data: []byte(strings.Repeat("b", 2*v05IDLen)), Topic: &stock}, // no group, no index
	}}, solicited: true}
	gs.oweRefused(p, q, answers)
	if len(q.owedAnswers) != 1 || q.owedAnswers[0] != chunk || ctr.AnswersOwed.Load() != 1 {
		t.Fatalf("only the chunk should be owed: %d owed, counted %d", len(q.owedAnswers), ctr.AnswersOwed.Load())
	}
}

// owedQueue is a queue of two with the test's index, holding two stock answers, owing a refused
// answer of chunk 1 of root: two pops free both slots, and the second moves the owed answer back
// in, where it is the lane's only entry.
func owedQueue(t *testing.T, root string) (*rpcQueue, func(string) *RPC) {
	t.Helper()
	topic, stock := "t", "blocks"
	index := func(m *pb.Message) (string, uint32, bool) {
		if len(m.Data) < v05IDLen {
			return "", 0, false
		}
		return testIndex(string(m.Data[:v05IDLen]))
	}
	answer := func(s string) *RPC {
		return &RPC{RPC: pb.RPC{Publish: []*pb.Message{{Data: []byte(s), Topic: &stock}}}, solicited: true}
	}
	q := newRpcQueue(2)
	q.enableHave(index, true, false)
	for _, s := range []string{"x1", "x2"} {
		if err := q.Push(answer(s), false); err != nil {
			t.Fatal(err)
		}
	}
	q.oweAnswer(&pb.Message{Data: v05Frame(root, 1), Topic: &topic})
	for range 2 {
		if _, err := q.Pop(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if q.queue.Len() != 1 || len(q.owedAnswers) != 0 {
		t.Fatalf("after two pops the owed answer should be back in the queue: %d queued, %d owed", q.queue.Len(), len(q.owedAnswers))
	}
	return q, answer
}

// popName is what a popped RPC carries: its first frame's two leading bytes, or "ctl".
func popName(t *testing.T, q *rpcQueue, ctx context.Context) string {
	t.Helper()
	out, err := q.Pop(ctx)
	if err != nil {
		t.Fatalf("pop: %v", err)
	}
	if len(out.Publish) == 0 {
		return "ctl"
	}
	return string(out.Publish[0].Data[:2])
}

// TestSpecV05OwedAnswerTakesNoRoom: an owed answer moved back into the queue takes room from no
// push, whoever pushes (seacast-lite.md §7.2 on a queue the stock topics share). With
// the owed answer in, the queue of two admits two more, as an empty queue would: a stock answer and a
// subscription, pushed directly as PubSub.announce does; a third is refused, as without A′. The owed
// answer keeps its place in line and leaves first. Before, the subscription was refused. The same
// chunk owed again while it waits in the queue is not owed twice (finding 7).
func TestSpecV05OwedAnswerTakesNoRoom(t *testing.T) {
	root := strings.Repeat("y", 32)
	q, answer := owedQueue(t, root)
	topic, stock := "t", "blocks"
	q.oweAnswer(&pb.Message{Data: v05Frame(root, 1), Topic: &topic})
	if len(q.owedAnswers) != 0 {
		t.Fatalf("chunk 1 is in the queue already: owed again %d times", len(q.owedAnswers))
	}
	subscribe := true
	if err := q.Push(answer("s1"), false); err != nil {
		t.Fatal(err)
	}
	if err := q.Push(&RPC{RPC: pb.RPC{Subscriptions: []*pb.RPC_SubOpts{{Subscribe: &subscribe, Topicid: &stock}}}}, false); err != nil {
		t.Fatalf("the subscription was refused for the owed answer's room: %v", err)
	}
	if err := q.Push(answer("s3"), false); err != ErrQueueFull {
		t.Fatalf("a third push should be refused, as by a queue of two without A′: %v", err)
	}
	var order []string
	for range 3 {
		order = append(order, popName(t, q, context.Background()))
	}
	if fmt.Sprint(order) != "[yy s1 ctl]" {
		t.Fatalf("the owed answer in its place, then the rest in order: %v", order)
	}
}

// TestSpecV05OwedAnswerCompetesByRarity: among the payload's frames an owed answer is an ordinary
// answer (seacast-lite.md §7.2), so the rarest-first selection scans it beside the chunk answers ahead
// of it, as the spec's rarest does. With chunk 3's answer ahead (three
// holders) and the owed answer of chunk 2 behind it (one), chunk 2 leaves first. Before, the owed
// answer waited behind every entry, payload frames included.
func TestSpecV05OwedAnswerCompetesByRarity(t *testing.T) {
	root := strings.Repeat("r", 32)
	q, _ := owedQueue(t, root)
	popName(t, q, context.Background()) // the owed answer of chunk 1 leaves
	topic := "t"
	ahead := &RPC{RPC: pb.RPC{Publish: []*pb.Message{{Data: v05Frame(root, 3), Topic: &topic}}}, solicited: true}
	if err := q.Push(ahead, false); err != nil {
		t.Fatal(err)
	}
	q.oweAnswer(&pb.Message{Data: v05Frame(root, 2), Topic: &topic})
	q.enableRarest(func(m *pb.Message) (int, bool) {
		if len(m.Data) < v05IDLen {
			return 0, false
		}
		_, i, ok := testIndex(string(m.Data[:v05IDLen]))
		if !ok {
			return 0, false
		}
		return map[uint32]int{2: 1, 3: 3}[i], true
	})
	out, err := q.Pop(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, i, _ := testIndex(string(out.Publish[0].Data[:v05IDLen])); i != 2 || !out.replay {
		t.Fatalf("the rarer owed answer (chunk 2) should leave first, got chunk %d (replay %v)", i, out.replay)
	}
}

// TestSpecV05OwedAnswerWaitsBehind: an owed answer behind another entry leaves after it, and makes
// it wait for nothing. A stock IWANT ahead of the owed answer goes at
// once though the hose window is full: only the owed answer needs a slot. A stock frame ahead of
// it goes first though the rarest-first selection knows the chunk's holders and not the stock
// frame's. Before, the IWANT waited for the hose, and the selection chose the owed answer.
func TestSpecV05OwedAnswerWaitsBehind(t *testing.T) {
	root := strings.Repeat("z", 32)
	t.Run("hose", func(t *testing.T) {
		q, _ := owedQueue(t, root)
		popName(t, q, context.Background()) // the owed answer of chunk 1 leaves
		// A stock IWANT, then a second owed answer, moved in behind it at the next pop.
		ask := &RPC{RPC: pb.RPC{Control: &pb.ControlMessage{Iwant: []*pb.ControlIWant{{MessageIDs: []string{"a-block-of-20-bytes!"}}}}}}
		if err := q.Push(ask, false); err != nil {
			t.Fatal(err)
		}
		topic := "t"
		q.oweAnswer(&pb.Message{Data: v05Frame(root, 2), Topic: &topic})
		h := newTestHose() // a window of 2, both slots taken
		for range 2 {
			if ok, err := h.acquire(context.Background(), nil); !ok || err != nil {
				t.Fatal(ok, err)
			}
		}
		q.hose = h
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		if got := popName(t, q, ctx); got != "ctl" {
			t.Fatalf("the stock IWANT should leave first, got %s", got)
		}
	})
	t.Run("rarest", func(t *testing.T) {
		q, answer := owedQueue(t, root)
		popName(t, q, context.Background()) // the owed answer of chunk 1 leaves
		if err := q.Push(answer("s1"), false); err != nil {
			t.Fatal(err)
		}
		topic := "t"
		q.oweAnswer(&pb.Message{Data: v05Frame(root, 2), Topic: &topic})
		q.enableRarest(func(m *pb.Message) (int, bool) {
			if len(m.Data) < v05IDLen {
				return 0, false // a stock frame: its holders are unknown
			}
			return 1, true
		})
		if got := popName(t, q, context.Background()); got != "s1" {
			t.Fatalf("the stock frame should leave first, got %s", got)
		}
	})
}

// TestSpecV05WriterEdges: an owed answer wakes a writer asleep on an empty queue, and is owed once
// per chunk; a window grant whose frame was discarded goes back; under E an IWANT class loss is drawn
// at the writer and its asks still count as written.
func TestSpecV05WriterEdges(t *testing.T) {
	root := strings.Repeat("w", 32)
	topic := "t"
	index := func(m *pb.Message) (string, uint32, bool) {
		if len(m.Data) < v05IDLen {
			return "", 0, false
		}
		return testIndex(string(m.Data[:v05IDLen]))
	}
	frame := func(i byte) *pb.Message { return &pb.Message{Data: v05Frame(root, i), Topic: &topic} }

	t.Run("an owed answer is owed once per chunk", func(t *testing.T) {
		q := newRpcQueue(4)
		q.enableHave(index, true, false)
		q.oweAnswer(frame(1))
		q.oweAnswer(frame(1)) // the same chunk again, before any pop
		q.oweAnswer(frame(2))
		other := &pb.Message{Data: []byte("plain"), Topic: &topic}
		q.oweAnswer(other)
		q.oweAnswer(&pb.Message{Data: []byte("plain"), Topic: &topic}) // equal payload, another message
		if len(q.owedAnswers) != 4 {
			t.Fatalf("chunks 1 and 2 once each, both ungrouped messages: %d owed", len(q.owedAnswers))
		}
	})

	t.Run("an owed answer wakes a sleeping writer", func(t *testing.T) {
		synctestTest(t, func(t *testing.T) {
			q := newRpcQueue(4)
			q.enableHave(index, true, false)
			var got *RPC
			done := make(chan struct{})
			go func() {
				got, _ = q.Pop(context.Background())
				close(done)
			}()
			synctest.Wait() // the writer is asleep on the empty queue
			q.oweAnswer(frame(1))
			<-done
			if got == nil || len(got.Publish) != 1 || !got.solicited {
				t.Fatalf("the owed answer should leave as an answer: %+v", got)
			}
		})
	})

	t.Run("a grant whose frame was discarded goes back", func(t *testing.T) {
		synctestTest(t, func(t *testing.T) {
			q := newRpcQueue(4)
			q.enableHave(index, true, false)
			q.hose = &hoseWindow{cfg: HoseConfig{Init: 1, Min: 1, Max: 1, Kp: 1, Timeout: time.Second}, w: 1}
			if granted, err := q.hose.acquire(context.Background(), q.urgentCh); !granted || err != nil {
				t.Fatal("the one slot should be granted")
			}
			q.hoseHolding = true // granted for a frame that a discard then removed
			done := make(chan struct{})
			go func() {
				_, _ = q.Pop(context.Background())
				close(done)
			}()
			synctest.Wait() // the writer is asleep on the empty queue
			q.hose.mu.Lock()
			inflight := q.hose.inflight
			q.hose.mu.Unlock()
			q.Close()
			<-done
			if inflight != 0 {
				t.Fatalf("the unused grant should be back, %d in flight", inflight)
			}
		})
	})

	t.Run("under E an IWANT loss is drawn at the writer", func(t *testing.T) {
		q := newRpcQueue(4)
		var reported []string
		q.onAsks = func(mids []string, _ time.Time) { reported = append(reported, mids...) }
		q.askLoss = func(ids []string) (kept, lost []string) {
			for _, id := range ids {
				if id == "lost" {
					lost = append(lost, id)
				} else {
					kept = append(kept, id)
				}
			}
			return kept, lost
		}
		if err := q.Push(rpcWithControl(nil, nil, []*pb.ControlIWant{{MessageIDs: []string{"lost", "kept"}}}, nil, nil, nil), false); err != nil {
			t.Fatal(err)
		}
		if err := q.Push(rpcWithControl(nil, nil, []*pb.ControlIWant{{MessageIDs: []string{"lost"}}}, nil, nil, nil), false); err != nil {
			t.Fatal(err)
		}
		if err := q.Push(rpcWithControl(nil, nil, []*pb.ControlIWant{{MessageIDs: []string{"last"}}}, nil, nil, nil), false); err != nil {
			t.Fatal(err)
		}
		out, err := q.Pop(ctx0())
		if err != nil {
			t.Fatal(err)
		}
		if ids := out.GetControl().GetIwant()[0].GetMessageIDs(); len(ids) != 1 || ids[0] != "kept" {
			t.Fatalf("the lost ask should not be written: %q", ids)
		}
		if len(reported) != 0 {
			t.Fatalf("asks are reported at the write, not at the pop: %v", reported)
		}
		q.noteAsksWritten(out) // the writer writes it
		// The second RPC carried only a lost ask: the writer gets a marker with nothing to write,
		// whose asks are reported at the write like any other (after any send delay).
		out, err = q.Pop(ctx0())
		if err != nil {
			t.Fatal(err)
		}
		if !out.lostOnly || out.GetControl() != nil || len(out.Publish) != 0 {
			t.Fatalf("an all-lost RPC should come as a marker with nothing to write: %+v", out)
		}
		q.noteAsksWritten(out)
		out, err = q.Pop(ctx0())
		if err != nil {
			t.Fatal(err)
		}
		if ids := out.GetControl().GetIwant()[0].GetMessageIDs(); len(ids) != 1 || ids[0] != "last" {
			t.Fatalf("the writer should go on: %q", ids)
		}
		q.noteAsksWritten(out)
		if fmt.Sprint(reported) != "[lost kept lost last]" {
			t.Fatalf("every ask written counts, the lost ones too: %v", reported)
		}
	})
}

func ctx0() context.Context { return context.Background() }

// TestSpecV05IWantLossAtWriter: the real IWANT class loss on the asker. As measured it drops asks
// at admission, before the queue and the window; under E it is drawn at the writer, and every lost
// ask counts as written: it opens its promise like a delivered one.
func TestSpecV05IWantLossAtWriter(t *testing.T) {
	root := strings.Repeat("l", 32)
	run := func(t *testing.T, on bool) (dropped int64, ctr *SpecV05Counters) {
		ctr = &SpecV05Counters{}
		var lossDropped atomic.Int64
		synctestTest(t, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			hosts := v05Hosts(t, 2, 20, time.Millisecond)
			psubs := getGossipsubsOptFn(ctx, hosts, func(i int, _ host.Host) []Option {
				var opts []Option
				if i == 0 {
					opts = append(v05Base(nil), WithPhaseSourcePushQuota(1))
				} else {
					opts = append(v05Base(nil), WithRPCClassLoss("iwant", 50, 7, &lossDropped))
				}
				if on {
					opts = append(opts, WithSpecV05(SpecV05{PromiseAtWrite: true}, ctr))
				}
				return opts
			})
			topics := v05Join(t, psubs)
			newV05Collect(ctx, t, topics)
			connectAll(t, hosts)
			time.Sleep(2 * time.Second)
			var batch MessageBatch
			for i := byte(0); i < 32; i++ {
				if err := topics[0].AddToBatch(ctx, &batch, v05Frame(root, i)); err != nil {
					t.Fatal(err)
				}
			}
			if err := psubs[0].PublishBatch(&batch); err != nil {
				t.Fatal(err)
			}
			time.Sleep(3 * time.Second)
		})
		return lossDropped.Load(), ctr
	}
	offDropped, offCtr := run(t, false)
	onDropped, onCtr := run(t, true)
	t.Logf("asks lost: %d as measured (at admission, %d counted as written); %d with E (at the writer, %d counted as written, %d promises at the write)",
		offDropped, offCtr.AsksLost.Load(), onDropped, onCtr.AsksLost.Load(), onCtr.PromisesAtWrite.Load())
	if offDropped == 0 || offCtr.AsksLost.Load() != 0 {
		t.Fatal("known answer: as measured the loss drops asks at admission and nothing counts them as written")
	}
	// The loss is drawn per (id, peer), so a retry of a lost ask to the same peer is lost again while
	// the first one's promise is open, and that promise keeps its deadline: promises are fewer than
	// lost asks, and every lost ask was counted as written.
	if onDropped == 0 || onCtr.AsksLost.Load() != onDropped || onCtr.PromisesAtWrite.Load() == 0 {
		t.Fatalf("with E every lost ask is drawn at the writer and counted as written: lost %d, counted %d, promises %d",
			onDropped, onCtr.AsksLost.Load(), onCtr.PromisesAtWrite.Load())
	}
}

// v05RejectTracer records rejections.
type v05RejectTracer struct {
	mockRawTracer
	rejected []string
}

func (r *v05RejectTracer) RejectMessage(_ *Message, reason string) {
	r.rejected = append(r.rejected, reason)
}

// TestSpecV05RejectAfterValidation: a chunk whose group died while it was being validated is
// ignored when its validation finishes, and the tracers see its end (no stale per-message state).
func TestSpecV05RejectAfterValidation(t *testing.T) {
	tr := &v05RejectTracer{}
	delivered := false
	p := &PubSub{
		admitMsg: func(*Message) bool { return false },
		tracer:   &pubsubTracer{raw: []RawTracer{tr}, pid: "self"},
		idGen:    newMsgIdGenerator(),
	}
	p.idGen.Default = func(m *pb.Message) string { return string(m.Data) }
	topic := "t"
	msg := &Message{Message: &pb.Message{Data: []byte("chunk"), Topic: &topic}, ReceivedFrom: "peer"}
	func() {
		defer func() {
			if recover() != nil {
				delivered = true // got past the check, into delivery (which this bare PubSub cannot do)
			}
		}()
		p.publishMessage(msg)
	}()
	if delivered || len(tr.rejected) != 1 || tr.rejected[0] != RejectValidationIgnored {
		t.Fatalf("a dead group's chunk should end as an ignored rejection: delivered %v, rejections %v", delivered, tr.rejected)
	}
}

// TestSpecV05LostAskAfterSendDelay: under E an ask lost on the wire opens its promise when it is
// written, after the writer's send delay. The asker writes every RPC 600 ms late and loses 99 % of
// its asks; the source offers it seven chunks. As measured the lost asks' promises run from the
// decision and park the source within 0.8 s; with E they run from the write, which comes later.
func TestSpecV05LostAskAfterSendDelay(t *testing.T) {
	root := strings.Repeat("s", 32)
	run := func(t *testing.T, on bool) (early, late int64) {
		var parks atomic.Int64
		synctestTest(t, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			hosts := v05Hosts(t, 2, 20, time.Millisecond)
			psubs := getGossipsubsOptFn(ctx, hosts, func(i int, _ host.Host) []Option {
				if i == 0 {
					return append(v05Base(nil), WithPhaseSourcePushQuota(1))
				}
				opts := append(v05Base(&parks), WithRPCClassLoss("iwant", 99, 11, nil), WithSendDelay(600*time.Millisecond))
				if on {
					opts = append(opts, WithSpecV05(SpecV05{PromiseAtWrite: true}, nil))
				}
				return opts
			})
			topics := v05Join(t, psubs)
			newV05Collect(ctx, t, topics)
			connectAll(t, hosts)
			time.Sleep(8 * time.Second) // setup's RPCs from the asker are 600 ms late too
			var batch MessageBatch
			for i := byte(0); i < 8; i++ {
				if err := topics[0].AddToBatch(ctx, &batch, v05Frame(root, i)); err != nil {
					t.Fatal(err)
				}
			}
			if err := psubs[0].PublishBatch(&batch); err != nil {
				t.Fatal(err)
			}
			time.Sleep(800 * time.Millisecond)
			early = parks.Load()
			time.Sleep(3 * time.Second)
			late = parks.Load()
		})
		return early, late
	}
	offEarly, offLate := run(t, false)
	onEarly, onLate := run(t, true)
	t.Logf("parks by 0.8 s and by 3.8 s: as measured %d, %d; with E %d, %d", offEarly, offLate, onEarly, onLate)
	if offEarly == 0 {
		t.Fatal("known answer: as measured a lost ask's promise runs from the decision and parks the source within 0.8 s")
	}
	if onEarly != 0 || onLate == 0 {
		t.Fatalf("with E the lost asks' promises run from the write, after the send delay: %d parks by 0.8 s, %d by 3.8 s", onEarly, onLate)
	}
}

// TestSpecV05LastResort is F2: a slow answer parks a node's only provider. The provider publishes
// a group one frame every 300 ms, pushes the receiver its first (push quota 1) and offers the rest,
// and answers every ask 600 ms late. The receiver's first promises break and park the provider for
// the group, so it ignores the later offers and stays short; under B′ the last resort asks the
// parked provider once more, and the receiver completes.
func TestSpecV05LastResort(t *testing.T) {
	root := strings.Repeat("b", 32)
	run := func(t *testing.T, on bool) (got int, parked int64, ctr *SpecV05Counters) {
		ctr = &SpecV05Counters{}
		var parks atomic.Int64
		synctestTest(t, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			hosts := v05Hosts(t, 2, 20, time.Millisecond)
			psubs := getGossipsubsOptFn(ctx, hosts, func(i int, _ host.Host) []Option {
				var opts []Option
				if i == 0 {
					opts = append(v05Base(nil), WithPhaseSourcePushQuota(1), WithIWantWithholding(600*time.Millisecond, nil))
				} else {
					opts = v05Base(&parks)
				}
				if on {
					opts = append(opts, WithSpecV05(SpecV05{LastResort: true}, ctr))
				}
				return opts
			})
			topics := v05Join(t, psubs)
			c := newV05Collect(ctx, t, topics)
			connectAll(t, hosts)
			time.Sleep(2 * time.Second)
			for i := byte(0); i < 8; i++ {
				if err := topics[0].Publish(ctx, v05Frame(root, i)); err != nil {
					t.Fatal(err)
				}
				time.Sleep(300 * time.Millisecond)
			}
			time.Sleep(5 * time.Second)
			got = c.count(1, root)
		})
		return got, parks.Load(), ctr
	}
	off, offParks, _ := run(t, false)
	on, onParks, ctr := run(t, true)
	t.Logf("frames at the receiver: %d as measured (%d parks), %d with B′ (%d parks; last resorts armed %d, asks %d, to parked %d)",
		off, offParks, on, onParks, ctr.LastResortsArmed.Load(), ctr.LastResortAsks.Load(), ctr.ParkedAsked.Load())
	if off >= 4 || offParks == 0 {
		t.Fatalf("known answer: without B′ the parked provider should leave the receiver short of 4, got %d frames, %d parks", off, offParks)
	}
	if on < 4 || ctr.ParkedAsked.Load() == 0 {
		t.Fatalf("with B′ the receiver should complete through the last resort, got %d frames, %d parked asks", on, ctr.ParkedAsked.Load())
	}
}

// TestSpecV05PromiseAtWrite is F5: a peer parked for an ask it never received. The asker's own
// pushes hold its one-slot window, which the muted peer never acknowledges, so its ask waits
// behind them until the slot times out at 1.5 s. Decided at once, the promise breaks at 400 ms
// and parks the peer that is about to answer; started at the write (E), it is kept.
func TestSpecV05PromiseAtWrite(t *testing.T) {
	asker, source := strings.Repeat("c", 32), strings.Repeat("d", 32)
	run := func(t *testing.T, on bool) (got int, parked int64, ctr *SpecV05Counters) {
		ctr = &SpecV05Counters{}
		var parks atomic.Int64
		synctestTest(t, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			hosts := v05Hosts(t, 2, 20, time.Millisecond)
			psubs := getGossipsubsOptFn(ctx, hosts, func(i int, _ host.Host) []Option {
				var opts []Option
				if i == 0 { // the asker
					opts = append(v05Base(&parks), WithHoseWindow(HoseConfig{Target: 20 * time.Millisecond, Kp: 1,
						Init: 1, Min: 1, Max: 1, Timeout: 1500 * time.Millisecond, FrameSlots: true}))
				} else { // the muted source: never acknowledges the asker's frames
					opts = append(v05Base(nil), WithPhaseSourcePushQuota(1), WithControlMute(nil))
				}
				if on {
					opts = append(opts, WithSpecV05(SpecV05{PromiseAtWrite: true}, ctr))
				}
				return opts
			})
			topics := v05Join(t, psubs)
			c := newV05Collect(ctx, t, topics)
			connectAll(t, hosts)
			time.Sleep(2 * time.Second)
			// The asker's pushes first: frame 0 takes the slot, frame 1 waits behind it.
			var mine MessageBatch
			for i := byte(0); i < 2; i++ {
				if err := topics[0].AddToBatch(ctx, &mine, v05Frame(asker, i)); err != nil {
					t.Fatal(err)
				}
			}
			if err := psubs[0].PublishBatch(&mine); err != nil {
				t.Fatal(err)
			}
			time.Sleep(10 * time.Millisecond)
			// Then the source offers the asker its group: the asks queue behind the asker's frame 1.
			var theirs MessageBatch
			for i := byte(0); i < 8; i++ {
				if err := topics[1].AddToBatch(ctx, &theirs, v05Frame(source, i)); err != nil {
					t.Fatal(err)
				}
			}
			if err := psubs[1].PublishBatch(&theirs); err != nil {
				t.Fatal(err)
			}
			time.Sleep(4 * time.Second)
			got = c.count(0, source)
		})
		return got, parks.Load(), ctr
	}
	off, offParks, _ := run(t, false)
	on, onParks, ctr := run(t, true)
	t.Logf("as measured: %d frames, %d parks; with E: %d frames, %d parks (promises opened at the write %d)",
		off, offParks, on, onParks, ctr.PromisesAtWrite.Load())
	if offParks == 0 {
		t.Fatal("known answer: a promise from the decision should break while the ask waits, and park the source")
	}
	if onParks != 0 || ctr.PromisesAtWrite.Load() == 0 {
		t.Fatalf("with E no park for an ask that had not left: %d parks, %d promises at the write", onParks, ctr.PromisesAtWrite.Load())
	}
	if on != 8 {
		t.Fatalf("with E the asker should still get the source's 8 frames, got %d", on)
	}
}

// TestSpecV05CloseWithConn: a promise open when its connection closes goes with it, and parks
// nobody. The receiver asks a provider that never answers, and the connection closes before the
// promise falls due.
func TestSpecV05CloseWithConn(t *testing.T) {
	root := strings.Repeat("e", 32)
	run := func(t *testing.T, on bool) (parked int64, ctr *SpecV05Counters) {
		ctr = &SpecV05Counters{}
		var parks atomic.Int64
		synctestTest(t, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			hosts := v05Hosts(t, 2, 20, time.Millisecond)
			psubs := getGossipsubsOptFn(ctx, hosts, func(i int, _ host.Host) []Option {
				var opts []Option
				if i == 0 {
					opts = append(v05Base(nil), WithPhaseSourcePushQuota(1), WithIWantWithholding(0, nil))
				} else {
					opts = v05Base(&parks)
				}
				if on {
					// E on as well: the writer's late reports must not reopen what the close closed.
					opts = append(opts, WithSpecV05(SpecV05{CloseWithConn: true, PromiseAtWrite: true}, ctr))
				}
				return opts
			})
			topics := v05Join(t, psubs)
			newV05Collect(ctx, t, topics)
			connectAll(t, hosts)
			time.Sleep(2 * time.Second)
			var batch MessageBatch
			for i := byte(0); i < 4; i++ {
				if err := topics[0].AddToBatch(ctx, &batch, v05Frame(root, i)); err != nil {
					t.Fatal(err)
				}
			}
			if err := psubs[0].PublishBatch(&batch); err != nil {
				t.Fatal(err)
			}
			time.Sleep(100 * time.Millisecond)
			if err := hosts[1].Network().ClosePeer(hosts[0].ID()); err != nil {
				t.Fatal(err)
			}
			time.Sleep(2 * time.Second)
		})
		return parks.Load(), ctr
	}
	offParks, _ := run(t, false)
	onParks, ctr := run(t, true)
	t.Logf("parks: %d as measured, %d with the connection rule (promises closed with it %d)", offParks, onParks, ctr.ClosedPromises.Load())
	if offParks == 0 {
		t.Fatal("known answer: the open promises should fall due after the close and park the provider")
	}
	if onParks != 0 || ctr.ClosedPromises.Load() == 0 {
		t.Fatalf("with the rule the promises close with the connection: %d parks, %d closed", onParks, ctr.ClosedPromises.Load())
	}
}

// TestPromiseAtWriteAfterClose: a write reported after the ask closed opens no promise. The writer's
// report reaches the router later, on its event loop, and by then the node may hold the index (a
// decode rebuilds an index without marking its content id seen), the group may have completed (its
// promises closed then, while a written ask stays queued until its report), or the ask may have
// closed otherwise (a delivery, a tie, a decline, its offer dropped), and a newer ask of the same
// chunk to the same peer may be queued, which the late report must not take for its own. Before, the
// report opened a promise against a peer that owed nothing, and promiseDue broke it (no recheck on
// this path). Each case beside a control that still opens one.
func TestPromiseAtWriteAfterClose(t *testing.T) {
	seen := timecache.NewTimeCache(time.Minute)
	defer seen.Done()
	p, root, done := peer.ID("server"), strings.Repeat("r", 32), strings.Repeat("d", 32)
	router := func(ctx context.Context, path bool) *GossipSubRouter {
		gs := newParkRouter(true)
		gs.p = &PubSub{eval: make(chan func(), 16), ctx: ctx, seenMessages: seen}
		gs.iwantAsked = make(map[string]time.Time)
		gs.parkPromise = 400 * time.Millisecond
		gs.v05 = &specV05{SpecV05: SpecV05{PromiseAtWrite: true}, ctr: &SpecV05Counters{}}
		gs.have = newHaveState(testIndex)
		gs.have.mine[root] = &haveEntry{}
		gs.have.mine[root].bits.Set(1) // rebuilt by the decode: held, its content id not in the seen cache
		if path {
			gs.seacast = &seacast{ctr: &SeacastCounters{}, machine: true,
				groups: map[string]*seacastGroup{done: {state: seacastComplete, stopPull: true}}}
		}
		return gs
	}
	opened := func(gs *GossipSubRouter, mid string) bool { _, ok := gs.iwantPromised[mid][p]; return ok }
	t.Run("the index is held", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			gs := router(ctx, false)
			held, open := testMid(root, 1), testMid(root, 2) // asks by content id
			gs.promisesFromWrite(p, []string{held, open}, time.Now())
			if opened(gs, held) || !opened(gs, open) || gs.v05.ctr.PromisesAtWrite.Load() != 1 {
				t.Fatalf("held %v open %v at the write %d, want no promise for the held index and one for the other",
					opened(gs, held), opened(gs, open), gs.v05.ctr.PromisesAtWrite.Load())
			}
			time.Sleep(405 * time.Millisecond) // the open promise is due
			synctest.Wait()
			for len(gs.p.eval) > 0 {
				(<-gs.p.eval)()
			}
			if gs.parkBreaksCtr.Load() != 1 {
				t.Fatalf("breaks %d at 405 ms, want only the unheld index's", gs.parkBreaksCtr.Load())
			}
		})
	})
	t.Run("on the path, the ask closed or its group completed before the report", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			gs := router(ctx, true)
			closed, completed, open := testMid(root, 3), testMid(done, 0), testMid(root, 2)
			gs.seacastAskQueued(p, []string{closed, completed, open})
			gs.seacastAsksClosed(closed) // delivered by another peer, say, before the report
			gs.promisesFromWrite(p, []string{closed, completed, open}, time.Now())
			if opened(gs, closed) || opened(gs, completed) || !opened(gs, open) || gs.v05.ctr.PromisesAtWrite.Load() != 1 {
				t.Fatalf("closed %v completed %v open %v at the write %d, want only the open ask's promise",
					opened(gs, closed), opened(gs, completed), opened(gs, open), gs.v05.ctr.PromisesAtWrite.Load())
			}
			if gs.seacastAskIsQueued(p, completed) || gs.seacastAskIsQueued(p, open) {
				t.Fatal("a reported ask is still queued")
			}
		})
	})
	t.Run("on the path, a late report of a closed ask beside a newer ask of the same chunk", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			gs := router(ctx, true)
			mid := testMid(root, 4)
			gs.seacastAskQueued(p, []string{mid}) // ask A
			time.Sleep(time.Millisecond)
			wroteA := time.Now()        // A is written; its report waits on the router's loop
			gs.seacastAskClosed(p, mid) // A's offer is dropped (F13) before the report lands
			time.Sleep(time.Millisecond)
			gs.seacastAskQueued(p, []string{mid}) // ask B, of a fresh offer, still queued
			gs.promisesFromWrite(p, []string{mid}, wroteA)
			if opened(gs, mid) || !gs.seacastAskIsQueued(p, mid) {
				t.Fatalf("A's late report: promise %v, B queued %v; want no promise and B still queued",
					opened(gs, mid), gs.seacastAskIsQueued(p, mid))
			}
			time.Sleep(time.Millisecond)
			wroteB := time.Now()
			gs.promisesFromWrite(p, []string{mid}, wroteB)
			if !gs.iwantPromised[mid][p].Equal(wroteB) || gs.seacastAskIsQueued(p, mid) {
				t.Fatalf("B's report: promise at %v (want B's write, %v), B queued %v", gs.iwantPromised[mid][p], wroteB,
					gs.seacastAskIsQueued(p, mid))
			}
		})
	})
}
