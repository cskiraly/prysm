package pubsub

import (
	"strings"
	"testing"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"
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
