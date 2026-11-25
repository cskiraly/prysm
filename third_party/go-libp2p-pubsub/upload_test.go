package pubsub

import (
	"context"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-varint"
	"google.golang.org/protobuf/proto"
)

func TestUploadByClass(t *testing.T) {
	root := strings.Repeat("g", 32)
	t.Run("a write splits into its classes and the rest is framing", func(t *testing.T) {
		var u uploadCounter
		ans := &RPC{RPC: pb.RPC{
			Publish: []*pb.Message{{Data: []byte(testMid(root, 0))}, {Data: []byte(testMid(root, 1))}},
			Control: &pb.ControlMessage{Ihave: []*pb.ControlIHave{{MessageIDs: []string{"a", "b"}}}},
		}, solicited: true}
		push := testDataRPC(root, 2)
		ctl := &RPC{RPC: pb.RPC{Control: &pb.ControlMessage{Idontwant: []*pb.ControlIDontWant{{MessageIDs: []string{"c"}}}}}}
		written := 0
		for _, r := range []*RPC{ans, push, ctl} {
			n := proto.Size(&r.RPC) + 2 // a two-byte length prefix
			written += n
			u.note(r, n)
		}
		s := u.s
		if s.AnswerFrames != 2 || s.AnswerRPCs != 1 || s.MaxAnswerFrames != 2 || s.PushFrames != 1 || s.PushRPCs != 1 {
			t.Fatalf("frames %+v, want 2 answers in one write and 1 push", s)
		}
		if s.IHaveBytes == 0 || s.IDontWantBytes == 0 || s.IWantBytes != 0 {
			t.Fatalf("control %+v, want IHAVE and IDONTWANT bytes only", s)
		}
		sum := s.PushBytes + s.AnswerBytes + s.IHaveBytes + s.IWantBytes + s.IDontWantBytes + s.GraftPruneBytes + s.OtherBytes
		if s.AllBytes != int64(written) || sum != s.AllBytes || s.OtherBytes < 6 {
			t.Fatalf("all %d sum %d other %d, want %d, equal, and at least the prefixes and control tags", s.AllBytes, sum, s.OtherBytes, written)
		}
	})
	t.Run("control ids are counted by kind, HAVE bitmaps and asks by index apart", func(t *testing.T) {
		var u uploadCounter
		var bits haveBits
		bits.Set(0)
		bits.Set(3)
		have := encodeHave(root, &bits)
		want := encodeWant(root, 5)
		ctl := &RPC{RPC: pb.RPC{Control: &pb.ControlMessage{
			Ihave:     []*pb.ControlIHave{{MessageIDs: []string{have, "plain"}}},
			Iwant:     []*pb.ControlIWant{{MessageIDs: []string{want, want, "plain"}}},
			Idontwant: []*pb.ControlIDontWant{{MessageIDs: []string{have}}},
		}}}
		u.note(ctl, proto.Size(&ctl.RPC))
		s := u.s
		if s.IHaveIDs != 2 || s.IHaveBitmaps != 1 || s.IWantIDs != 3 || s.IWantAsks != 2 || s.IDontWantIDs != 1 || s.IDontWantBitmaps != 1 {
			t.Fatalf("ids %+v, want IHAVE 2 (1 bitmap), IWANT 3 (2 asks), IDONTWANT 1 (1 bitmap)", s)
		}
	})
	t.Run("frames are counted by topic, pushes and answers apart", func(t *testing.T) {
		var u uploadCounter
		frame := func(topic string, n int) *pb.Message {
			return &pb.Message{Topic: &topic, Data: make([]byte, n)}
		}
		push := &RPC{RPC: pb.RPC{Publish: []*pb.Message{frame("a", 100), frame("a", 300), frame("b", 50)}}}
		ans := &RPC{RPC: pb.RPC{Publish: []*pb.Message{frame("b", 700)}}, solicited: true}
		u.note(push, proto.Size(&push.RPC))
		u.note(ans, proto.Size(&ans.RPC))
		size := func(ms ...*pb.Message) int64 {
			var n int64
			for _, m := range ms {
				n += fieldSize(proto.Size(m))
			}
			return n
		}
		got := u.byTopicCopy()
		want := map[string]TopicUpload{
			"a": {PushBytes: size(push.Publish[0], push.Publish[1]), PushFrames: 2},
			"b": {PushBytes: size(push.Publish[2]), PushFrames: 1, AnswerBytes: size(ans.Publish[0]), AnswerFrames: 1},
		}
		if len(got) != len(want) || got["a"] != want["a"] || got["b"] != want["b"] {
			t.Fatalf("by topic %+v, want %+v", got, want)
		}
		if got["a"].PushBytes+got["b"].PushBytes != u.s.PushBytes || got["b"].AnswerBytes != u.s.AnswerBytes {
			t.Fatalf("by topic %+v doesn't add up to the classes %+v", got, u.s)
		}
	})
	t.Run("a filtered copy stays an answer", func(t *testing.T) {
		if !shallowRPCCopy(&RPC{solicited: true}).solicited {
			t.Fatal("shallowRPCCopy dropped solicited")
		}
	})
	t.Run("frame slots queue a batched answer one frame per RPC", func(t *testing.T) {
		h := newTestHose()
		h.cfg.FrameSlots = true
		gs := &GossipSubRouter{p: &PubSub{hose: h}, sendingSolicited: true}
		q := newRpcQueue(16)
		ans := &RPC{RPC: pb.RPC{
			Publish: []*pb.Message{{Data: []byte(testMid(root, 0))}, {Data: []byte(testMid(root, 1))}, {Data: []byte(testMid(root, 2))}},
			Control: &pb.ControlMessage{Ihave: []*pb.ControlIHave{{MessageIDs: []string{"a"}}}},
		}}
		if !gs.doSendRPC(ans, "peer", q, false) {
			t.Fatal("send failed")
		}
		for i := range 3 {
			r, err := q.Pop(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			if len(r.Publish) != 1 || !r.solicited || (r.Control != nil) != (i == 0) {
				t.Fatalf("rpc %d: %d frames, solicited %v, control %v; want one frame, an answer, control on the first only", i, len(r.Publish), r.solicited, r.Control != nil)
			}
		}
	})
}

// writesTracer adds up, from the RPCs the node's writes completed, the bytes each write put on its
// stream (the length prefix and the RPC) and each topic's frames by class, as the writer frames them.
type writesTracer struct {
	mockRawTracer
	mu      sync.Mutex
	written int64
	topics  map[string]TopicUpload
}

func (w *writesTracer) SendRPCCompleted(rpc *RPC, _ peer.ID, _ time.Duration) {
	w.mu.Lock()
	defer w.mu.Unlock()
	size := proto.Size(&rpc.RPC)
	w.written += int64(varint.UvarintSize(uint64(size)) + size)
	for _, m := range rpc.Publish {
		tu := w.topics[m.GetTopic()]
		n := int64(1 + varint.UvarintSize(uint64(proto.Size(m))) + proto.Size(m))
		if rpc.solicited {
			tu.AnswerBytes += n
			tu.AnswerFrames++
		} else {
			tu.PushBytes += n
			tu.PushFrames++
		}
		w.topics[m.GetTopic()] = tu
	}
}

// TestUploadByTopicMatchesTheWrites: over real streams, a node's counts by topic and its written
// bytes are those of the RPCs its writes completed. The publisher pushes ten messages of known sizes
// on one topic and nothing on the other it is in.
func TestUploadByTopicMatchesTheWrites(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		hosts := getDefaultHosts(t, 2)
		tracer := &writesTracer{topics: make(map[string]TopicUpload)}
		psubs := []*PubSub{getGossipsub(ctx, hosts[0], WithRawTracer(tracer)), getGossipsub(ctx, hosts[1])}
		var sub *Subscription
		for i, ps := range psubs {
			for _, topic := range []string{"t", "u"} {
				s, err := ps.Subscribe(topic)
				if err != nil {
					t.Fatal(err)
				}
				if i == 1 && topic == "t" {
					sub = s
				}
			}
		}
		connect(t, hosts[0], hosts[1])
		time.Sleep(2 * time.Second)
		for i := range 10 {
			if err := psubs[0].Publish("t", make([]byte, 100*(i+1))); err != nil {
				t.Fatal(err)
			}
			if _, err := sub.Next(ctx); err != nil {
				t.Fatal(err)
			}
		}
		time.Sleep(time.Second)
		synctest.Wait()

		stats, byTopic := psubs[0].UploadStats(), psubs[0].UploadByTopic()
		tracer.mu.Lock()
		defer tracer.mu.Unlock()
		if want := tracer.topics["t"]; byTopic["t"] != want || want.PushFrames != 10 {
			t.Fatalf("topic t %+v, want the writes' %+v, ten frames pushed", byTopic["t"], want)
		}
		if got := byTopic["u"]; got != (TopicUpload{}) {
			t.Fatalf("topic u %+v, want nothing written", got)
		}
		if stats.AllBytes != tracer.written {
			t.Fatalf("all bytes %d, want the writes' %d", stats.AllBytes, tracer.written)
		}
		if byTopic["t"].PushBytes != stats.PushBytes {
			t.Fatalf("topic t's push bytes %d, the class's %d: want equal, the only topic pushed", byTopic["t"].PushBytes, stats.PushBytes)
		}
	})
}
