package pubsub

import (
	"context"
	"strings"
	"testing"
	"time"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"
)

func testUrgentRPC() *RPC {
	rpc := &RPC{RPC: pb.RPC{Control: &pb.ControlMessage{Idontwant: []*pb.ControlIDontWant{{MessageIDs: []string{"x"}}}}}}
	rpc.size = 10
	return rpc
}

// popInto runs Pop in a goroutine and delivers its result on a channel, so a test can assert
// that a pop is blocked (nothing arrives) or released (something does).
func popInto(ctx context.Context, q *rpcQueue) <-chan *RPC {
	ch := make(chan *RPC, 1)
	go func() {
		rpc, err := q.Pop(ctx)
		if err != nil {
			rpc = nil
		}
		ch <- rpc
	}()
	return ch
}

func expectBlocked(t *testing.T, ch <-chan *RPC, what string) {
	t.Helper()
	select {
	case rpc := <-ch:
		t.Fatalf("%s: popped %v while the slot was held", what, rpc)
	case <-time.After(50 * time.Millisecond):
	}
}

func expectPop(t *testing.T, ch <-chan *RPC, what string) *RPC {
	t.Helper()
	select {
	case rpc := <-ch:
		if rpc == nil {
			t.Fatalf("%s: pop failed", what)
		}
		return rpc
	case <-time.After(2 * time.Second):
		t.Fatalf("%s: pop did not return", what)
		return nil
	}
}

// One slot shared by two peers' queues: data writes never overlap, an urgent RPC to the waiting
// peer leaves while the other holds the slot, rotate hands the slot over on the write's return,
// sticky keeps it while the holder's queue has data, and a closed queue gives its slot back.
func TestDispatchCredits(t *testing.T) {
	root := strings.Repeat("g", 32)
	ctx := t.Context()

	t.Run("rotate: one at a time, urgent bypasses, the slot moves on write return", func(t *testing.T) {
		c := &dispatchCredits{free: 1}
		q1, q2 := newRpcQueue(16), newRpcQueue(16)
		q1.credits, q2.credits = c, c
		for _, q := range []*rpcQueue{q1, q2} {
			if err := q.Push(testDataRPC(root, 0), false); err != nil {
				t.Fatal(err)
			}
		}
		if rpc, err := q1.Pop(ctx); err != nil || len(rpc.Publish) != 1 {
			t.Fatalf("q1 pop: %v %v", rpc, err)
		}
		if !q1.holding {
			t.Fatal("q1 should hold the slot")
		}
		ch := popInto(ctx, q2)
		expectBlocked(t, ch, "q2 data")
		if err := q2.UrgentPush(testUrgentRPC(), false); err != nil {
			t.Fatal(err)
		}
		if rpc := expectPop(t, ch, "q2 urgent"); len(rpc.Publish) != 0 || rpc.Control == nil {
			t.Fatalf("q2 should have popped the urgent RPC, got %v", rpc)
		}
		if q2.holding {
			t.Fatal("an urgent pop must not take the slot")
		}
		ch = popInto(ctx, q2)
		expectBlocked(t, ch, "q2 data again")
		q1.afterWrite()
		if q1.holding {
			t.Fatal("rotate must return the slot on write return")
		}
		if rpc := expectPop(t, ch, "q2 data after handover"); len(rpc.Publish) != 1 {
			t.Fatalf("q2 should have popped its frame, got %v", rpc)
		}
		if !q2.holding {
			t.Fatal("q2 should hold the slot now")
		}
		q2.afterWrite()
		if c.free != 1 || len(c.waiters) != 0 {
			t.Fatalf("slot not back in the pool: free %d waiters %d", c.free, len(c.waiters))
		}
	})

	t.Run("sticky: the holder keeps the slot while its queue has data", func(t *testing.T) {
		c := &dispatchCredits{free: 1, sticky: true}
		q1, q2 := newRpcQueue(16), newRpcQueue(16)
		q1.credits, q2.credits = c, c
		for _, i := range []byte{0, 1} {
			if err := q1.Push(testDataRPC(root, i), false); err != nil {
				t.Fatal(err)
			}
		}
		if err := q2.Push(testDataRPC(root, 2), false); err != nil {
			t.Fatal(err)
		}
		if _, err := q1.Pop(ctx); err != nil {
			t.Fatal(err)
		}
		q1.afterWrite()
		if !q1.holding {
			t.Fatal("sticky must keep the slot while data is queued")
		}
		ch := popInto(ctx, q2)
		expectBlocked(t, ch, "q2 under sticky")
		if _, err := q1.Pop(ctx); err != nil {
			t.Fatal(err)
		}
		q1.afterWrite()
		if q1.holding {
			t.Fatal("sticky must return the slot when the queue drains")
		}
		expectPop(t, ch, "q2 after q1 drained")
	})

	t.Run("a queue that empties by cancel, or closes, gives the slot back", func(t *testing.T) {
		c := &dispatchCredits{free: 1, sticky: true}
		q1, q2 := newRpcQueue(16), newRpcQueue(16)
		q1.credits, q2.credits = c, c
		q1.enableHave(testQueueIndex, true, false)
		if err := q1.Push(testDataRPC(root, 0), false); err != nil {
			t.Fatal(err)
		}
		if err := q2.Push(testDataRPC(root, 1), false); err != nil {
			t.Fatal(err)
		}
		var theirs haveBits
		theirs.Set(0)
		q1.haveTheirs(root, theirs.Bytes())
		pctx, cancel := context.WithCancel(ctx)
		defer cancel()
		ch1 := popInto(pctx, q1) // takes the slot, cancels its only frame, waits for data: slot back
		ch2 := popInto(ctx, q2)
		expectPop(t, ch2, "q2 while q1 waits empty")
		q2.afterWrite()
		if err := q1.Push(testDataRPC(root, 3), false); err != nil {
			t.Fatal(err)
		}
		expectPop(t, ch1, "q1 with new data")
		q1.Close()
		if q1.holding || c.free != 1 {
			t.Fatalf("close must return the slot: holding %v free %d", q1.holding, c.free)
		}
	})
}
