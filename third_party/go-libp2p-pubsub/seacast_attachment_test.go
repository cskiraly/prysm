package pubsub

import (
	"bytes"
	"strings"
	"testing"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
)

type testAttachedGroup struct {
	AttachedObject
	held     bool
	deferred []DeferredChunk
}

func (t *testAttachedGroup) AllowOffer(string) bool { return !t.held }
func (t *testAttachedGroup) PrepareWrite(w DeferredChunk, data []byte) ([]byte, bool) {
	if t.held {
		t.deferred = append(t.deferred, w)
		return nil, false
	}
	return append(bytes.Clone(data), []byte("claim")...), true
}

func TestAttachedChunkDeferralAtDispatch(t *testing.T) {
	const topic = "aggregate"
	root := strings.Repeat("k", 32)
	a := &testAttachedGroup{held: true}
	index := func(string) (string, uint32, bool) { return root, 0, true }
	ps := &PubSub{idGen: newMsgIdGenerator(), attachedTopics: map[string]AttachedObject{topic: a}}
	gs := &GossipSubRouter{have: newHaveState(index)}
	ps.rt = gs
	q := ps.newPeerQueue(peer.ID("receiver"))
	q.enableHave(func(*pb.Message) (string, uint32, bool) { return index("") }, true, false)
	q.enableProjection(1, 1)
	name := topic
	msg := &pb.Message{Topic: &name, Data: []byte("bare")}
	for _, solicited := range []bool{false, true} {
		req := rpcWithMessages(msg)
		req.solicited = solicited
		if out := q.dispatch(req); out != nil {
			t.Fatal("held chunk escaped the writer")
		}
	}
	if len(a.deferred) != 2 || a.deferred[0].Solicited || !a.deferred[1].Solicited || a.deferred[0].Peer != q.peer {
		t.Fatalf("deferred pushes and replies: %+v", a.deferred)
	}
	if q.have.popped[root] != nil {
		t.Fatal("a deferred chunk counted toward the receiver's projected holdings")
	}
	bits := new(haveBits)
	bits.Set(0)
	offer := encodeHave(root, bits)
	ctrl := rpcWithControl(nil, []*pb.ControlIHave{{TopicID: &name, MessageIDs: []string{offer}}}, nil, nil, nil, []*pb.ControlIDontWant{{MessageIDs: []string{offer}}})
	filtered := ps.filterAttachedOffers(ctrl)
	if len(filtered.Control.Ihave) != 0 || len(filtered.Control.Idontwant) != 1 || len(ctrl.Control.Ihave) != 1 {
		t.Fatal("holding or another peer's offer was altered")
	}
	a.held = false
	if len(ps.filterAttachedOffers(ctrl).Control.Ihave) != 1 {
		t.Fatal("released group did not advertise its chunks")
	}
	out := q.dispatch(rpcWithMessages(msg))
	if out == nil || !bytes.Equal(out.Publish[0].Data, []byte("bareclaim")) || !bytes.Equal(msg.Data, []byte("bare")) {
		t.Fatal("released chunk did not get a private, claim-bearing write")
	}
}
