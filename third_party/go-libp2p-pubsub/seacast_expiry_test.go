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

// TestGroupExpiryClosesItsAskPromises: the score's promises of an expired group's asks close with
// it. An ask still queued is discarded and never sent, and an answer that arrives after the expiry
// is dropped at admission, so the peer asked could not keep the promise. Another group's promise
// stays and is charged when it breaks.
func TestGroupExpiryClosesItsAskPromises(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		hosts := getDefaultHosts(t, 1)
		ps := getGossipsub(ctx, hosts[0], WithMessageIdFn(seacastTestID), WithHaveBitmaps(seacastTestIndex),
			WithPromiseTracking())
		gs := ps.rt.(*GossipSubRouter)
		addRouterPeers(ps, "A", "B")
		expired, live := strings.Repeat("e", haveRootLen), strings.Repeat("l", haveRootLen)
		done := make(chan struct{})
		ps.eval <- func() {
			gs.gossipTracer.AddPromise("A", []string{expired + "\x01", expired + "\x02"})
			gs.gossipTracer.AddPromise("B", []string{live + "\x01"})
			gs.expireGroup(expired)
			close(done)
		}
		<-done
		time.Sleep(GossipSubIWantFollowupTime + 2*GossipSubHeartbeatInterval)
		if c, _ := ps.BrokenPromises(); len(c) != 1 || c["B"] != 1 {
			t.Fatalf("broken promises charged %v; want only B 1 (A's asks were of the expired group)", c)
		}
	})
}

// TestDiscardGroupForgetsPoppedProjectionWithoutHave: a group's discard from a peer's queue drops
// what was popped of it to the peer, which sent no HAVE for it, and leaves another group's.
func TestDiscardGroupForgetsPoppedProjectionWithoutHave(t *testing.T) {
	q := newRpcQueue(16)
	q.have = &queueHave{
		index: func(m *pb.Message) (string, uint32, bool) {
			return string(m.GetData()), 0, true
		},
		projectNeed: 1,
		popped:      make(map[string]*haveBits),
	}
	for _, root := range []string{"expired", "live"} {
		q.have.notePopped(&pb.Message{Data: []byte(root)})
	}

	q.discardGroup("expired", func(m *pb.Message) bool { return string(m.GetData()) == "expired" },
		func(string) bool { return false })
	if expired, live := q.have.popped["expired"] != nil, q.have.popped["live"] != nil; expired || !live {
		t.Fatalf("projection entries after discard: expired %v, live %v; want false true", expired, live)
	}
}

// TestSeacastExpiryV4Forms is the review of part 4's build, finding 2's last part: a group's
// expiry discards its queued control entries of the v4 forms too (a holding with its flags, a
// WANT: the probe's, a standing one), as it did the v1 forms, counts them, and
// drops the group's ask exemptions; another group's entries stay. Its positive control: the same
// entries of the other group leave the queue.
func TestSeacastExpiryV4Forms(t *testing.T) {
	root, other := strings.Repeat("x", 32), strings.Repeat("y", 32)
	p := peer.ID("peer")
	topic := "t"
	gs := testHaveRouter()
	gs.peers = map[peer.ID]protocol.ID{p: GossipSubID_v12}
	ctr := &SeacastCounters{}
	probeRoot, probeOther := encodeWantV2(root, 3, false, 0), encodeWantV2(other, 3, false, 0)
	gs.seacast = &seacast{ctr: ctr, probeAsks: map[string]struct{}{probeRoot: {}, probeOther: {}}}
	q := newRpcQueue(16)
	gs.p = &PubSub{seenMessages: timecache.NewTimeCache(time.Minute), idGen: newMsgIdGenerator(),
		peers: map[peer.ID]*rpcQueue{p: q}}

	var b haveBits
	b.Set(3)
	holdRoot, holdOther := encodeHaveV2(root, &b, false, true), encodeHaveV2(other, &b, false, true)
	standingRoot := encodeWantV2(root, 5, true, 0)
	rpc := rpcWithControl(nil, []*pb.ControlIHave{{TopicID: &topic, MessageIDs: []string{holdRoot, holdOther}}},
		[]*pb.ControlIWant{{MessageIDs: []string{probeRoot, standingRoot, probeOther}}}, nil, nil, nil)
	rpc.size = 1
	if err := q.Push(rpc, false); err != nil {
		t.Fatal(err)
	}

	gs.expireGroup(root)

	out, err := q.Pop(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	ihave, iwant := out.GetControl().GetIhave(), out.GetControl().GetIwant()
	if len(ihave) != 1 || len(ihave[0].GetMessageIDs()) != 1 || ihave[0].GetMessageIDs()[0] != holdOther {
		t.Fatalf("only the other group's v4 holding should leave: %+v", ihave)
	}
	if len(iwant) != 1 || len(iwant[0].GetMessageIDs()) != 1 || iwant[0].GetMessageIDs()[0] != probeOther {
		t.Fatalf("only the other group's v4 WANT should leave: %+v", iwant)
	}
	if n := ctr.ExpiredV4Discards.Load(); n != 3 {
		t.Fatalf("v4 entries discarded %d, want 3 (the holding, the probe, the standing WANT)", n)
	}
	if _, kept := gs.seacast.probeAsks[probeRoot]; kept {
		t.Fatal("the expired group's ask exemption should be gone")
	}
	if _, kept := gs.seacast.probeAsks[probeOther]; !kept {
		t.Fatal("the live group's ask exemption should stay")
	}
}
