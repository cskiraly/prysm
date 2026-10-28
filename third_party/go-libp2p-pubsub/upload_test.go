package pubsub

import (
	"strings"
	"testing"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
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
			u.note(r, n, "to")
		}
		s := u.s
		if s.AnswerFrames != 2 || s.AnswerRPCs != 1 || s.MaxAnswerFrames != 2 || s.PushFrames != 1 || s.PushRPCs != 1 {
			t.Fatalf("frames %+v, want 2 answers in one write and 1 push", s)
		}
		if len(u.pushTo) != 1 || u.pushTo["to"] != 1 {
			t.Fatalf("pushes by peer %v, want the one push to its peer and no answers", u.pushTo)
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
		u.note(ctl, proto.Size(&ctl.RPC), "to")
		s := u.s
		if s.IHaveIDs != 2 || s.IHaveBitmaps != 1 || s.IWantIDs != 3 || s.IWantAsks != 2 || s.IDontWantIDs != 1 || s.IDontWantBitmaps != 1 {
			t.Fatalf("ids %+v, want IHAVE 2 (1 bitmap), IWANT 3 (2 asks), IDONTWANT 1 (1 bitmap)", s)
		}
	})
	t.Run("pushes are counted per frame when the writer has an id function, answers never", func(t *testing.T) {
		var u uploadCounter
		id := func(m *pb.Message) string { return string(m.Data) }
		for _, to := range []string{"p1", "p2"} {
			u.noteWithID(testDataRPC(root, 2), 10, peer.ID(to), id)
		}
		ans := &RPC{RPC: pb.RPC{Publish: []*pb.Message{{Data: []byte(testMid(root, 2))}}}, solicited: true}
		u.noteWithID(ans, 10, "p3", id)
		u.note(testDataRPC(root, 2), 10, "p4") // no id function: counted as a push, not per frame
		if len(u.pushPerFrame) != 1 || u.pushPerFrame[testMid(root, 2)] != 2 || u.s.PushFrames != 3 || u.s.AnswerFrames != 1 {
			t.Fatalf("per frame %v, push frames %d, answers %d; want the frame pushed twice, 3 push frames, 1 answer", u.pushPerFrame, u.s.PushFrames, u.s.AnswerFrames)
		}
	})
	t.Run("a filtered copy stays an answer", func(t *testing.T) {
		if !shallowRPCCopy(&RPC{solicited: true}).solicited {
			t.Fatal("shallowRPCCopy dropped solicited")
		}
	})
	t.Run("frame slots queue a batched answer one frame per RPC", func(t *testing.T) {
		h := newTestHose(false)
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
