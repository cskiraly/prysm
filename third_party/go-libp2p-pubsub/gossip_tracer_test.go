package pubsub

import (
	"context"
	"testing"
	"time"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"

	"github.com/libp2p/go-libp2p/core/peer"
)

func TestBrokenPromises(t *testing.T) {
	// tests that unfullfilled promises are tracked correctly
	synctestTest(t, func(t *testing.T) {

		gt := newGossipTracer()
		gt.followUpTime = 100 * time.Millisecond

		peerA := peer.ID("A")
		peerB := peer.ID("B")
		peerC := peer.ID("C")

		var mids []string
		for i := range 100 {
			m := makeTestMessage(i)
			m.From = []byte(peerA)
			mid := DefaultMsgIdFn(m)
			mids = append(mids, mid)
		}

		gt.AddPromise(peerA, mids)
		gt.AddPromise(peerB, mids)
		gt.AddPromise(peerC, mids)

		// no broken promises yet
		brokenPromises := gt.GetBrokenPromises()
		if brokenPromises != nil {
			t.Fatal("expected no broken promises")
		}

		// throttle one of the peers to save his promises
		gt.ThrottlePeer(peerC)

		// make promises break
		time.Sleep(gt.followUpTime + time.Millisecond)

		brokenPromises = gt.GetBrokenPromises()
		if len(brokenPromises) != 2 {
			t.Fatalf("expected 2 broken prmises, got %d", len(brokenPromises))
		}

		brokenPromisesA := brokenPromises[peerA]
		if brokenPromisesA != 1 {
			t.Fatalf("expected 1 broken promise from A, got %d", brokenPromisesA)
		}

		brokenPromisesB := brokenPromises[peerB]
		if brokenPromisesB != 1 {
			t.Fatalf("expected 1 broken promise from A, got %d", brokenPromisesB)
		}

		// verify that the peerPromises map has been vacated
		if len(gt.peerPromises) != 0 {
			t.Fatal("expected empty peerPromises map")
		}
	})
}

func TestNoBrokenPromises(t *testing.T) {
	// like above, but this time we deliver messages to fullfil the promises
	synctestTest(t, func(t *testing.T) {

		gt := newGossipTracer()
		gt.followUpTime = 100 * time.Millisecond

		peerA := peer.ID("A")
		peerB := peer.ID("B")

		var msgs []*pb.Message
		var mids []string
		for i := range 100 {
			m := makeTestMessage(i)
			m.From = []byte(peerA)
			msgs = append(msgs, m)
			mid := DefaultMsgIdFn(m)
			mids = append(mids, mid)
		}

		gt.AddPromise(peerA, mids)
		gt.AddPromise(peerB, mids)

		for _, m := range msgs {
			gt.DeliverMessage(&Message{Message: m})
		}

		time.Sleep(gt.followUpTime + time.Millisecond)

		// there should be no broken promises
		brokenPromises := gt.GetBrokenPromises()
		if brokenPromises != nil {
			t.Fatal("expected no broken promises")
		}

		// verify that the peerPromises map has been vacated
		if len(gt.peerPromises) != 0 {
			t.Fatal("expected empty peerPromises map")
		}
	})
}

// TestPromiseTracking is WithPromiseTracking's known answer: without scoring, a broken promise is charged and counted per peer.
func TestPromiseTracking(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		hosts := getDefaultHosts(t, 2)
		plain := getGossipsub(ctx, hosts[0])
		if c, o := plain.BrokenPromises(); c != nil || o != nil {
			t.Fatal("no gossip tracer, so nothing is counted")
		}
		ps := getGossipsub(ctx, hosts[1], WithPromiseTracking())
		gt := ps.rt.(*GossipSubRouter).gossipTracer
		if gt == nil {
			t.Fatal("promise tracking must install the gossip tracer")
		}
		gt.AddPromise("A", []string{"m1"})
		gt.AddPromise("A", []string{"m2"})
		gt.AddPromise("B", []string{"m3"})
		if c, o := ps.BrokenPromises(); len(c) != 0 || o["A"] != 2 || o["B"] != 1 {
			t.Fatalf("before the follow-up time: charged %v outstanding %v", c, o)
		}
		time.Sleep(GossipSubIWantFollowupTime + 2*GossipSubHeartbeatInterval)
		if c, o := ps.BrokenPromises(); c["A"] != 2 || c["B"] != 1 || len(o) != 0 {
			t.Fatalf("after it: charged %v outstanding %v, want A 2, B 1, none outstanding", c, o)
		}
		ps.ResetBrokenPromises()
		if c, _ := ps.BrokenPromises(); len(c) != 0 {
			t.Fatalf("a reset clears the charged count, got %v", c)
		}
	})
}
