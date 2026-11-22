package pubsub

import (
	"context"
	"fmt"
	"reflect"
	"sort"
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
			if granted, err := q.hose.acquireClass(context.Background(), q.urgentCh, 0); !granted || err != nil {
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

// TestSpecV05ExpiryAcrossValidation: a chunk whose group dies while the chunk is held in
// validation. The receiver holds chunk 0 of a group; chunk 1's validation blocks until the group's
// holdings have aged out (holdTTL is four 100 ms heartbeats here), then completes. Under D′ the
// chunk ends as an ignored rejection and reaches no subscriber; as measured it is delivered.
func TestSpecV05ExpiryAcrossValidation(t *testing.T) {
	root := strings.Repeat("v", 32)
	run := func(t *testing.T, on bool) (got int, ignored int, expired int64) {
		ctr := &SpecV05Counters{}
		tr := &v05RejectTracer{}
		synctestTest(t, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			hosts := v05Hosts(t, 2, 20, time.Millisecond)
			params := DefaultGossipSubParams()
			params.Dlazy, params.GossipFactor = 0, 0
			params.HeartbeatInterval = 100 * time.Millisecond
			params.HistoryLength, params.HistoryGossip = 3, 2
			psubs := getGossipsubsOptFn(ctx, hosts, func(i int, _ host.Host) []Option {
				opts := []Option{WithSeenMessagesTTL(200 * time.Millisecond)} // before the HAVE state sizes holdTTL
				opts = append(opts, v05Base(nil)...)
				opts = append(opts, WithGossipSubParams(params))
				if i == 1 {
					opts = append(opts, WithRawTracer(tr))
					if on {
						opts = append(opts, WithSpecV05(SpecV05{Expiry: true}, ctr))
					}
				}
				return opts
			})
			release := make(chan struct{})
			if err := psubs[1].RegisterTopicValidator("t", func(_ context.Context, _ peer.ID, m *Message) bool {
				if _, i, ok := testIndex(string(m.Data[:v05IDLen])); ok && i == 1 {
					<-release // held until its group has died
				}
				return true
			}); err != nil {
				t.Fatal(err)
			}
			topics := v05Join(t, psubs)
			c := newV05Collect(ctx, t, topics)
			connectAll(t, hosts)
			time.Sleep(2 * time.Second)
			if err := topics[0].Publish(ctx, v05Frame(root, 0)); err != nil {
				t.Fatal(err)
			}
			time.Sleep(50 * time.Millisecond)
			if err := topics[0].Publish(ctx, v05Frame(root, 1)); err != nil {
				t.Fatal(err)
			}
			time.Sleep(time.Second) // the group's holdings age out meanwhile (four heartbeats)
			close(release)
			time.Sleep(time.Second)
			got = c.count(1, root)
		})
		for _, r := range tr.rejected {
			if r == RejectValidationIgnored {
				ignored++
			}
		}
		return got, ignored, ctr.Expired.Load()
	}
	offGot, offIgnored, _ := run(t, false)
	onGot, onIgnored, onExpired := run(t, true)
	t.Logf("chunks delivered: %d as measured (%d ignored), %d with D′ (%d ignored, %d groups expired)", offGot, offIgnored, onGot, onIgnored, onExpired)
	if offGot != 2 {
		t.Fatalf("known answer: as measured the held chunk is delivered when released, got %d of 2", offGot)
	}
	if onGot != 1 || onIgnored != 1 || onExpired == 0 {
		t.Fatalf("with D′ the held chunk of the dead group ends ignored and undelivered: %d delivered, %d ignored, %d expired", onGot, onIgnored, onExpired)
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

// TestSpecV05Expiry is D′ on a router's own state: when a group's holdings age out, its tombstone
// stays, its pull state goes, and nothing asks for it again. A group dies only GROUP_TTL after its
// last chunk, past the gossip window, so a network test would have nothing to show; the router's
// state shows it directly.
func TestSpecV05Expiry(t *testing.T) {
	root, other := strings.Repeat("x", 32), strings.Repeat("y", 32)
	p := peer.ID("peer")
	topic := "t"
	gs := testHaveRouter()
	gs.have.ihaveOn = true
	gs.peers = map[peer.ID]protocol.ID{p: GossipSubID_v12}
	gs.mesh = map[string]map[peer.ID]struct{}{topic: {p: {}}}
	gs.iwantWindow = 200 * time.Millisecond
	gs.iwantAsked = make(map[string]time.Time)
	gs.iwantPromised = make(map[string]map[peer.ID]time.Time)
	gs.parkGroups = make(map[peer.ID]map[string]*parkEntry)
	gs.offerTable = make(map[string]map[peer.ID]time.Time)
	gs.offerTried = make(map[string]map[peer.ID]time.Time)
	gs.offerTopic = make(map[string]string)
	gs.offerByPeer = make(map[peer.ID]int)
	q := newRpcQueue(16)
	gs.p = &PubSub{seenMessages: timecache.NewTimeCache(time.Minute), idGen: newMsgIdGenerator(),
		peers: map[peer.ID]*rpcQueue{p: q}}
	gs.p.idGen.Default = func(m *pb.Message) string { return string(m.Data[:v05IDLen]) }
	ctr := &SpecV05Counters{}
	host := &PubSub{rt: gs, idGen: gs.p.idGen}
	if err := WithSpecV05(SpecV05{LastResort: true, Expiry: true}, ctr)(host); err != nil {
		t.Fatal(err)
	}
	q.enableHave(func(m *pb.Message) (string, uint32, bool) { return testIndex(string(m.Data[:v05IDLen])) }, true, false)
	q.enableStaleAsks(gs.have.ownHeldID)

	// We hold index 0 of root and were offered 1 and 2; one ask is in flight and its promise open.
	gs.haveNoteArrival(testMid(root, 0))
	gs.haveNoteArrival(testMid(other, 0))
	cands := []string{}
	iwant := map[string]string{}
	gs.considerAnnounced(p, topic, encodeWant(root, 1), iwant, &cands)
	gs.considerAnnounced(p, topic, encodeWant(root, 2), iwant, &cands)
	if len(cands) != 2 {
		t.Fatalf("before expiry both offers are candidates, got %v", cands)
	}
	gs.iwantAsked[encodeWant(root, 1)] = time.Now()
	gs.iwantPromised[encodeWant(root, 1)] = map[peer.ID]time.Time{p: time.Now()}
	// Queued to the peer: a frame of root, an offer of root and one of other, an ask of root.
	frame := &pb.Message{Data: v05Frame(root, 3), Topic: &topic}
	var b haveBits
	b.Set(3)
	mixed := rpcWithControl([]*pb.Message{frame}, []*pb.ControlIHave{{TopicID: &topic,
		MessageIDs: []string{encodeHave(root, &b), encodeHave(other, &b)}}}, []*pb.ControlIWant{{MessageIDs: []string{encodeWant(root, 4)}}}, nil, nil, nil)
	mixed.size = 1
	if err := q.Push(mixed, false); err != nil {
		t.Fatal(err)
	}
	q.owe(topic, root, &b)

	// The heartbeats age root out (the other group was refreshed meanwhile).
	for i := 0; i < gs.have.holdTTL; i++ {
		gs.haveNoteArrival(testMid(other, byte(i+1)))
		gs.haveHeartbeat()
	}
	if !gs.expired(root) || gs.expired(other) {
		t.Fatalf("root should be past its lifetime and other not: %v %v", gs.expired(root), gs.expired(other))
	}
	if len(gs.offerTable) != 0 || len(gs.iwantAsked) != 0 || len(gs.iwantPromised) != 0 || len(gs.v05.offered[p]) != 0 {
		t.Fatalf("root's pull state should be gone: offers %d asked %d promised %d offered %d",
			len(gs.offerTable), len(gs.iwantAsked), len(gs.iwantPromised), len(gs.v05.offered[p]))
	}
	// Nothing asks for it again: a late offer, a retry, the last resort.
	cands = cands[:0]
	gs.considerAnnounced(p, topic, encodeWant(root, 5), map[string]string{}, &cands)
	if len(cands) != 0 || len(gs.offerTable) != 0 {
		t.Fatalf("a late offer of a dead group must be ignored: %v", cands)
	}
	// A late copy is not admitted at ingress (no Preprocess, validation or delivery); a live
	// group's is.
	if host.admitMsg(&Message{Message: &pb.Message{Data: v05Frame(root, 6), Topic: &topic}}) {
		t.Fatal("a chunk of a dead group must not be admitted")
	}
	if !host.admitMsg(&Message{Message: &pb.Message{Data: v05Frame(other, 6), Topic: &topic}}) {
		t.Fatal("a chunk of a live group must be admitted")
	}
	// Nor would it bring the group back, were it to get past.
	gs.haveNoteArrival(testMid(root, 6))
	if _, back := gs.have.mine[root]; back {
		t.Fatal("a late copy must not bring the group back")
	}
	// The queue kept only what is not root's: the other group's offer; the owed offer is gone.
	out, err := q.Pop(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Publish) != 0 || len(out.GetControl().GetIwant()) != 0 || len(out.GetControl().GetIhave()) != 1 {
		t.Fatalf("only the other group's offer should leave: %+v", out)
	}
	if r, _, _ := decodeHave(out.GetControl().GetIhave()[0].GetMessageIDs()[0]); r != other {
		t.Fatalf("the offer left is not the other group's")
	}
	if q.QueuedBytes() != 0 || len(q.owed) != 0 {
		t.Fatalf("queue accounting: %d bytes left, %d owed groups", q.QueuedBytes(), len(q.owed))
	}
	// A writer drops an ask for the dead group, queued after it died, at dispatch.
	if err := q.Push(rpcWithControl(nil, nil, []*pb.ControlIWant{{MessageIDs: []string{encodeWant(root, 7), encodeWant(other, 9)}}}, nil, nil, nil), false); err != nil {
		t.Fatal(err)
	}
	ask, err := q.Pop(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if ids := ask.GetControl().GetIwant()[0].GetMessageIDs(); len(ids) != 1 || ids[0] != encodeWant(other, 9) {
		t.Fatalf("only the live group's ask should leave: %q", ids)
	}
	// The retry and the last resort ask nothing for it; a HAVE for it is ignored.
	gs.retryV05(encodeWant(root, 7))
	gs.lastResort(encodeWant(root, 7))
	gs.askParked(encodeWant(root, 7))
	if q.queue.Len() != 0 || len(gs.iwantAsked) != 0 || len(gs.v05.lrArmedKeys) != 0 {
		t.Fatalf("nothing may be asked for a dead group: queued %d, asked %d, armed %d", q.queue.Len(), len(gs.iwantAsked), len(gs.v05.lrArmedKeys))
	}
	var late haveBits
	late.Set(8)
	if !gs.haveAbsorb(p, encodeHave(root, &late)) || gs.have.peers[p][root] != nil {
		t.Fatal("a HAVE for a dead group must be ignored")
	}
	if ctr.Expired.Load() != 1 || ctr.ExpiredDiscards.Load() != 3 { // the queued ask after the expiry is the writer's
		t.Fatalf("counters: expired %d, discards %d (want 1, 3: the frame, its offer, its ask)", ctr.Expired.Load(), ctr.ExpiredDiscards.Load())
	}
	// The tombstone itself goes after another holdTTL.
	for i := 0; i < gs.have.holdTTL; i++ {
		gs.haveHeartbeat()
	}
	if gs.expired(root) {
		t.Fatal("the tombstone should be gone after another holdTTL")
	}
}

// v05BlockGate declines every ask until its block installs (open), as an application gate that
// cannot authenticate a group before its block.
type v05BlockGate struct{ open atomic.Bool }

func (g *v05BlockGate) Allow(peer.ID, string, string) bool { return g.open.Load() }
func (g *v05BlockGate) Committed(peer.ID, []string)        {}

// TestSpecV05LedgerFromOffers is C′'s ledger: what the gate declined before the block is asked for
// when the block installs. The source pushes the receiver one frame (push quota 1) and offers the
// other seven while the receiver's gate declines them; the block then installs and the application
// replays. As measured the replay reads the deferral ledger, here of two ids, and the receiver ends
// with three frames; under C′ it reads the offer table, which kept every offer.
func TestSpecV05LedgerFromOffers(t *testing.T) {
	root := strings.Repeat("h", 32)
	run := func(t *testing.T, on bool) (got int, ctr *SpecV05Counters) {
		ctr = &SpecV05Counters{}
		synctestTest(t, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			hosts := v05Hosts(t, 2, 20, time.Millisecond)
			gate := &v05BlockGate{}
			deferral := NewRequestDeferral(2)
			psubs := getGossipsubsOptFn(ctx, hosts, func(i int, _ host.Host) []Option {
				opts := v05Base(nil)
				if i == 0 {
					opts = append(opts, WithPhaseSourcePushQuota(1))
				} else {
					opts = append(opts, WithRequestGate(gate, deferral))
				}
				if on {
					opts = append(opts, WithSpecV05(SpecV05{LastResort: true, LedgerFromOffers: true}, ctr))
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
			time.Sleep(time.Second)
			if n := c.count(1, root); n != 1 {
				t.Fatalf("before the block the receiver should hold only the pushed frame, got %d", n)
			}
			gate.open.Store(true)
			deferral.Replay()
			time.Sleep(2 * time.Second)
			got = c.count(1, root)
		})
		return got, ctr
	}
	off, _ := run(t, false)
	on, ctr := run(t, true)
	t.Logf("frames at the receiver after the replay: %d as measured, %d with C′ (ledger asks %d)", off, on, ctr.LedgerAsks.Load())
	if off != 3 {
		t.Fatalf("known answer: as measured the replay recovers the two ids the ledger kept, 3 frames in all, got %d", off)
	}
	if on != 8 {
		t.Fatalf("with C′ the replay should recover every offered frame, got %d", on)
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

// The offer table's per-peer bound, set by a number and by group. By number one count covers
// every id of a peer. By group a peer is recorded for the ids of a group that is vouched for (a
// message of it passed validation here and its holdings are kept), and the bound covers the rest:
// ids of other groups and ids of no group. A frame that only arrived vouches for nothing. A
// group's ids leave the bounded count when it is vouched for and return when its holdings age
// out, and the counts return to zero when the offers are dropped.
func TestOfferPeerBound(t *testing.T) {
	const topic = "t"
	big := peer.ID("big")
	newRouter := func(opts ...Option) (*GossipSubRouter, map[OfferOutcome]int) {
		gs := testHaveRouter()
		gs.offerTable = make(map[string]map[peer.ID]time.Time)
		gs.offerTried = make(map[string]map[peer.ID]time.Time)
		gs.offerTopic = make(map[string]string)
		gs.offerByPeer = make(map[peer.ID]int)
		gs.p = &PubSub{seenMessages: timecache.NewTimeCache(time.Minute), idGen: newMsgIdGenerator()}
		gs.p.idGen.Default = func(m *pb.Message) string { return string(m.Data[:v05IDLen]) }
		count := map[OfferOutcome]int{}
		host := &PubSub{rt: gs}
		opts = append(opts, WithOfferOutcomes(func(_ string, o OfferOutcome) { count[o]++ }))
		for _, opt := range opts {
			if err := opt(host); err != nil {
				t.Fatal(err)
			}
		}
		return gs, count
	}
	root := func(b byte) string { return strings.Repeat(string([]byte{b}), haveRootLen) }
	// announce has big announce n indices of r as asks by index, from index first on.
	announce := func(gs *GossipSubRouter, r string, first, n int) {
		for i := first; i < first+n; i++ {
			gs.recordOffer(encodeWant(r, uint32(i)), topic, big)
		}
	}
	// arrive is a frame of r reaching the router; validated is the same frame handed on after its
	// validation.
	arrive := func(gs *GossipSubRouter, r string) { gs.haveNoteArrival(testMid(r, 7)) }
	validated := func(gs *GossipSubRouter, r string) {
		gs.offerVouch(&Message{Message: &pb.Message{Data: []byte(testMid(r, 7))}})
	}
	dropAll := func(gs *GossipSubRouter) {
		for mid := range gs.offerTable {
			gs.clearOffers(mid)
		}
		if gs.offerByPeer[big] != 0 || len(gs.offerByPeerRoot) != 0 {
			t.Errorf("after the offers are dropped: %d ids, %d peers with a count by group", gs.offerByPeer[big], len(gs.offerByPeerRoot))
		}
	}
	held, other, forged := root(1), root(2), root(3)

	// By number: one count over two groups and an id of no group, the bound at 300 and at the
	// default.
	for _, bound := range []int{300, offerPerPeer} {
		var opts []Option
		if bound != offerPerPeer {
			opts = append(opts, WithOfferPeerBound(bound))
		}
		gs, count := newRouter(opts...)
		announce(gs, held, 1000, 200)
		announce(gs, other, 0, 106)
		gs.recordOffer("no-group", topic, big)
		if count[OfferRecorded] != bound || count[OfferRefusedPeerBound] != 200+106+1-bound {
			t.Errorf("bound %d: %d recorded, %d refused", bound, count[OfferRecorded], count[OfferRefusedPeerBound])
		}
	}

	// By group. A frame of a group that arrived and did not pass validation leaves holdings and
	// vouches for nothing: the plain bound.
	gs, count := newRouter(WithOfferPeerBoundByGroup())
	arrive(gs, forged)
	announce(gs, forged, 1000, 300)
	if count[OfferRecorded] != offerPerPeer || count[OfferRefusedPeerBound] != 300-offerPerPeer {
		t.Errorf("by group, a frame that only arrived: %d recorded, %d refused", count[OfferRecorded], count[OfferRefusedPeerBound])
	}
	dropAll(gs)

	// Nothing vouched for: the plain bound, over both groups and an id of no group.
	gs, count = newRouter(WithOfferPeerBoundByGroup())
	announce(gs, held, 1000, 200)
	announce(gs, other, 0, 100)
	gs.recordOffer("no-group", topic, big)
	if count[OfferRecorded] != offerPerPeer || count[OfferRefusedPeerBound] != 200+100+1-offerPerPeer {
		t.Errorf("by group, nothing vouched for: %d recorded, %d refused", count[OfferRecorded], count[OfferRefusedPeerBound])
	}
	// A frame of one group arrives and passes validation: its 200 ids no longer count against
	// the bound, so the other group's ids and the id of no group are recorded now, and further
	// ids of the vouched group are too.
	arrive(gs, held)
	validated(gs, held)
	announce(gs, other, 0, 100)
	gs.recordOffer("no-group", topic, big)
	announce(gs, held, 1000, 600)
	if got := gs.offerByPeerRoot[big][held]; got != 600 {
		t.Errorf("by group: %d ids of the vouched group recorded, want 600", got)
	}
	if got := gs.offerByPeerRoot[big][other]; got != 100 {
		t.Errorf("by group: %d ids of the other group recorded, want 100", got)
	}
	if gs.offerByPeer[big] != 600+100+1 {
		t.Errorf("by group: %d ids of the peer recorded, want %d", gs.offerByPeer[big], 600+100+1)
	}
	// The group nothing vouches for stays under the plain bound: 100 are in and the id of no
	// group counts too, so 155 more fit and the 9 after them are refused.
	before := count[OfferRefusedPeerBound]
	announce(gs, other, 0, 100+155+9)
	if got := gs.offerByPeerRoot[big][other]; got != offerPerPeer-1 {
		t.Errorf("by group: %d ids of the other group recorded, want %d", got, offerPerPeer-1)
	}
	if got := count[OfferRefusedPeerBound] - before; got != 9 {
		t.Errorf("by group: %d refusals of the other group's ids, want 9", got)
	}
	// A vouched group is bounded by the indices a bitmap can name.
	before = count[OfferRefusedPeerBound]
	announce(gs, held, 1000, offerPerPeerGroup+5)
	if got := gs.offerByPeerRoot[big][held]; got != offerPerPeerGroup {
		t.Errorf("by group: %d ids of the vouched group recorded, want %d", got, offerPerPeerGroup)
	}
	if got := count[OfferRefusedPeerBound] - before; got != 5 {
		t.Errorf("by group: %d refusals at the group's bound, want 5", got)
	}
	// The group's holdings age out: the heartbeat's sweep ends the vouching, the group's ids are
	// back in the bounded count, and the peer's next id that nothing vouches for is refused.
	// (The expiry rule, when on, drops the group's offers at this moment instead.)
	delete(gs.have.mine, held)
	gs.sweepOffers()
	if gs.offerGroupVouched(held) || len(gs.offerVouched) != 0 {
		t.Error("by group: a group without holdings is still vouched for")
	}
	before = count[OfferRefusedPeerBound]
	gs.recordOffer("another-id-of-no-group", topic, big)
	if got := count[OfferRefusedPeerBound] - before; got != 1 {
		t.Errorf("by group, after the holdings aged out: %d refusals of a new id, want 1", got)
	}
	// The peer's connection closes: its pairs and both its counts go.
	gs.v05 = &specV05{SpecV05: SpecV05{CloseWithConn: true}, ctr: &SpecV05Counters{}}
	gs.v05PeerGone(big)
	if _, in := gs.offerByPeer[big]; in || len(gs.offerByPeerRoot) != 0 || len(gs.offerTable) != 0 {
		t.Errorf("after the peer's close: %d ids, %d peers with a count by group, %d ids in the table",
			gs.offerByPeer[big], len(gs.offerByPeerRoot), len(gs.offerTable))
	}

	// An id is counted under its group whether it is an ask by index or a message id, and one
	// index announced both ways counts twice.
	gs, _ = newRouter(WithOfferPeerBoundByGroup())
	arrive(gs, held)
	validated(gs, held)
	gs.recordOffer(testMid(held, 9), topic, big)
	gs.recordOffer(encodeWant(held, 9), topic, big)
	if got := gs.offerByPeerRoot[big][held]; got != 2 {
		t.Errorf("by group: %d ids of index 9 counted under the group, want 2", got)
	}
	dropAll(gs)

	if err := WithOfferPeerBound(0)(&PubSub{rt: gs}); err == nil {
		t.Error("a bound of 0 was accepted")
	}
	if err := WithOfferPeerBound(8)(&PubSub{rt: testHaveRouter()}); err == nil {
		t.Error("a bound without the offer table was accepted")
	}
	if err := WithOfferPeerBoundByGroup()(&PubSub{rt: testHaveRouter()}); err == nil {
		t.Error("the bound by group without the offer table was accepted")
	}
}

// The offer table's diagnostic hook sees what the table did with each announcement of a pair it
// had not recorded (a recording once, a refusal at each announcement), and the table is the same
// with the hook as without: the same announcers recorded for the same ids.
func TestOfferOutcomes(t *testing.T) {
	const topic = "t"
	type event struct {
		mid string
		o   OfferOutcome
	}
	run := func(hooked bool) ([]event, map[string][]string, map[peer.ID]int) {
		gs := testHaveRouter()
		gs.offerTable = make(map[string]map[peer.ID]time.Time)
		gs.offerTried = make(map[string]map[peer.ID]time.Time)
		gs.offerTopic = make(map[string]string)
		gs.offerByPeer = make(map[peer.ID]int)
		gs.p = &PubSub{seenMessages: timecache.NewTimeCache(time.Minute), idGen: newMsgIdGenerator()}
		var got []event
		if hooked {
			host := &PubSub{rt: gs}
			if err := WithOfferOutcomes(func(mid string, o OfferOutcome) { got = append(got, event{mid, o}) })(host); err != nil {
				t.Fatal(err)
			}
		}
		// A pair below every bound: recorded once; announced again it is only refreshed.
		gs.recordOffer("once", topic, peer.ID("small"))
		gs.recordOffer("once", topic, peer.ID("small"))
		// One peer announces offerPerPeer + 44 ids, twice: the last 44 are refused at the per-peer
		// bound both times; the recorded ones are not reported the second time.
		for round := 0; round < 2; round++ {
			for i := 0; i < offerPerPeer+44; i++ {
				gs.recordOffer(fmt.Sprintf("big-%03d", i), topic, peer.ID("big"))
			}
		}
		// offerPerID + 3 peers announce one id, twice: the last 3 are refused at the per-id bound
		// both times; the recorded announcers are only refreshed the second time.
		for round := 0; round < 2; round++ {
			for i := 0; i < offerPerID+3; i++ {
				gs.recordOffer("popular", topic, peer.ID(fmt.Sprintf("p%02d", i)))
			}
		}
		table := make(map[string][]string, len(gs.offerTable))
		for mid, peers := range gs.offerTable {
			for p := range peers {
				table[mid] = append(table[mid], string(p))
			}
			sort.Strings(table[mid])
			if gs.offerTopic[mid] != topic {
				t.Errorf("id %s has topic %q", mid, gs.offerTopic[mid])
			}
		}
		return got, table, gs.offerByPeer
	}
	got, table, byPeer := run(true)
	count := map[OfferOutcome]int{}
	for _, e := range got {
		count[e.o]++
	}
	want := map[OfferOutcome]int{OfferRecorded: 1 + offerPerPeer + offerPerID, OfferRefusedPeerBound: 2 * 44, OfferRefusedIDBound: 2 * 3}
	for o, n := range want {
		if count[o] != n {
			t.Errorf("outcome %d: %d reports, want %d", o, count[o], n)
		}
	}
	// The first report is the one recording of the small pair; nothing follows for its refresh.
	if len(got) < 2 || got[0] != (event{"once", OfferRecorded}) || got[1].mid == "once" {
		t.Errorf("the refreshed pair: reports begin %v", got[:min(2, len(got))])
	}
	// What was refused is not in the table, and what was recorded is.
	if _, in := table[fmt.Sprintf("big-%03d", offerPerPeer)]; in {
		t.Error("an id refused at the per-peer bound is in the table")
	}
	if len(table["popular"]) != offerPerID || len(table) != 2+offerPerPeer || byPeer[peer.ID("big")] != offerPerPeer {
		t.Errorf("the table: %d ids, %d announcers of the popular id, %d ids of the big peer", len(table), len(table["popular"]), byPeer[peer.ID("big")])
	}
	// The same table without the hook: every id with the same announcers, every peer's count.
	none, table0, byPeer0 := run(false)
	if len(none) != 0 || !reflect.DeepEqual(table, table0) || !reflect.DeepEqual(byPeer, byPeer0) {
		t.Errorf("the table differs with the hook: %d ids against %d", len(table), len(table0))
	}
	if err := WithOfferOutcomes(nil)(&PubSub{rt: testHaveRouter()}); err == nil {
		t.Error("the option was accepted without an offer table")
	}
}
