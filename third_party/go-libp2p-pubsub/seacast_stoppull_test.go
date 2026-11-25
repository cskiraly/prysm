package pubsub

import (
	"context"
	"strings"
	"testing"
	"time"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p-pubsub/timecache"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
)

// TestSeacastStopPullAtWriters is the review of part 4's gate, finding 1 (seacast-lite.md §7.2 at
// completion): asks of a group queued before it completed leave no more once it does; its v1 WANT
// and its ask by chunk id are dropped from the queue and counted, the probe's v4 WANT (exempt) and
// another group's ask stay, and the group's offer in the same RPC is untouched.
func TestSeacastStopPullAtWriters(t *testing.T) {
	root, other := strings.Repeat("x", 32), strings.Repeat("y", 32)
	p := peer.ID("peer")
	topic := "t"
	gs := testHaveRouter()
	gs.peers = map[peer.ID]protocol.ID{p: GossipSubID_v12}
	ctr := &SeacastCounters{}
	probe := encodeWantV2(root, 7, false, 0)
	gs.seacast = &seacast{ctr: ctr, machine: true, probeAsks: map[string]struct{}{probe: {}}}
	q := newRpcQueue(16)
	gs.p = &PubSub{seenMessages: timecache.NewTimeCache(time.Minute), idGen: newMsgIdGenerator(),
		peers: map[peer.ID]*rpcQueue{p: q}}
	var b haveBits
	b.Set(3)
	offer := encodeHave(root, &b)
	rpc := rpcWithControl(nil, []*pb.ControlIHave{{TopicID: &topic, MessageIDs: []string{offer}}},
		[]*pb.ControlIWant{{MessageIDs: []string{encodeWant(root, 5), testMid(root, 6), probe, encodeWant(other, 5)}}}, nil, nil, nil)
	rpc.size = 1
	if err := q.Push(rpc, false); err != nil {
		t.Fatal(err)
	}
	// An RPC of the group's asks alone with a control extension: the asks go, the extension stays
	// (the second check of part 4's gate, finding 21).
	ext := rpcWithControl(nil, nil, []*pb.ControlIWant{{MessageIDs: []string{encodeWant(root, 9)}}}, nil, nil, nil)
	ext.Control.Extensions = &pb.ControlExtensions{}
	ext.size = 1
	if err := q.Push(ext, false); err != nil {
		t.Fatal(err)
	}

	gs.seacastDropQueuedAsks(root)

	out, err := q.Pop(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	ids := out.GetControl().GetIwant()[0].GetMessageIDs()
	if len(ids) != 2 || ids[0] != probe || ids[1] != encodeWant(other, 5) {
		t.Fatalf("asks left: %q; want the probe's and the other group's", ids)
	}
	if ih := out.GetControl().GetIhave(); len(ih) != 1 || ih[0].GetMessageIDs()[0] != offer {
		t.Fatalf("the group's offer must stay: %+v", ih)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	second, err := q.Pop(ctx)
	if err != nil {
		t.Fatalf("the extension's RPC was dropped whole: %v", err)
	}
	if second.GetControl().GetExtensions() == nil || len(second.GetControl().GetIwant()) != 0 {
		t.Fatalf("the extension's RPC: %+v; want its extension kept and its ask gone", second.GetControl())
	}
	if n := ctr.StopPullDiscards.Load(); n != 3 {
		t.Fatalf("asks dropped %d, want 3", n)
	}
}

// TestSeacastFillClose is the review of part 4's gate, finding 6 (seacast.md §4.2's fill, :676): a
// rebuilt cell closes the asks of it under its own id and under the ask by index, their promises, as
// the network fill does through haveArrived; another index's stay.
func TestSeacastFillClose(t *testing.T) {
	root := strings.Repeat("x", 32)
	p := peer.ID("peer")
	gs := testHaveRouter()
	gs.seacast = &seacast{ctr: &SeacastCounters{}}
	mid, want, keep := testMid(root, 6), encodeWant(root, 6), encodeWant(root, 5)
	gs.iwantPromised = map[string]map[peer.ID]time.Time{mid: {p: time.Now()}, want: {p: time.Now()}, keep: {p: time.Now()}}
	gs.seacastFillClose(root, 6, mid)
	if _, open := gs.iwantPromised[want]; open {
		t.Fatal("the ask by index stays promised after the decoded fill")
	}
	if _, open := gs.iwantPromised[mid]; open {
		t.Fatal("the ask by chunk id stays promised after the decoded fill")
	}
	if _, open := gs.iwantPromised[keep]; !open {
		t.Fatal("another index's promise was closed")
	}
}
