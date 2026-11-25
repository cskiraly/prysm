package pubsub

import (
	"context"
	"strings"
	"testing"
	"time"
)

func newTestHose() *hoseWindow {
	cfg := HoseConfig{Target: 20 * time.Millisecond, Kp: 0.1, Init: 2, Min: 1, Max: 8, Timeout: time.Second}
	return &hoseWindow{cfg: cfg, w: cfg.Init, wLo: cfg.Init, wHi: cfg.Init}
}

func TestHoseWindow(t *testing.T) {
	ctx := context.Background()
	t.Run("gates at the window and wakes on release", func(t *testing.T) {
		h := newTestHose()
		for range 2 {
			if ok, err := h.acquire(ctx, nil); !ok || err != nil {
				t.Fatalf("acquire under the window: %v %v", ok, err)
			}
		}
		got := make(chan bool, 1)
		go func() {
			ok, _ := h.acquire(ctx, nil)
			got <- ok
		}()
		select {
		case <-got:
			t.Fatal("third acquire passed a window of 2")
		case <-time.After(20 * time.Millisecond):
		}
		h.release(20*time.Millisecond, true, false) // on target: W unchanged, one slot back
		if ok := <-got; !ok {
			t.Fatal("waiter not granted on release")
		}
		if h.inflight != 2 {
			t.Fatalf("inflight %d, want 2", h.inflight)
		}
	})
	t.Run("delay under target opens the window, over target closes it", func(t *testing.T) {
		h := newTestHose()
		_, _ = h.acquire(ctx, nil)
		h.release(0, true, false) // 20 ms under: +2
		if h.w != 4 {
			t.Fatalf("w %.2f after an early ack, want 4", h.w)
		}
		_, _ = h.acquire(ctx, nil)
		h.release(100*time.Millisecond, true, false) // 80 ms over: -8, clamped at Min
		if h.w != 1 {
			t.Fatalf("w %.2f after a late ack, want the floor 1", h.w)
		}
		if h.acks != 2 || len(h.samples) != 2 {
			t.Fatalf("acks %d samples %d, want 2 and 2", h.acks, len(h.samples))
		}
	})
	t.Run("a stats reset keeps the window and the frames in flight", func(t *testing.T) {
		h := newTestHose()
		ps := &PubSub{hose: h}
		_, _ = h.acquire(ctx, nil)
		_, _ = h.acquire(ctx, nil)
		h.release(0, true, true) // +2: w 4, one frame still out
		ps.ResetHoseStats()
		s, _ := ps.HoseStats()
		if s.W != 4 || s.WLo != 4 || s.WHi != 4 || s.Acks != 0 || s.Answers != 0 || s.PeakInflight != 1 || h.inflight != 1 {
			t.Fatalf("after reset %+v inflight %d, want w 4 range 4..4, no acks or answers, peak and inflight 1", s, h.inflight)
		}
	})
	t.Run("a freed slot goes to the first waiter", func(t *testing.T) {
		h := newTestHose()
		_, _ = h.acquire(ctx, nil)
		_, _ = h.acquire(ctx, nil)
		order := make(chan string, 3)
		queued := 0
		wait := func(name string) {
			go func() {
				if ok, _ := h.acquire(ctx, nil); ok {
					order <- name
				}
			}()
			queued++
			for { // queue the waiters in a known order
				h.mu.Lock()
				n := len(h.waiters)
				h.mu.Unlock()
				if n == queued {
					break
				}
				time.Sleep(time.Millisecond)
			}
		}
		wait("a1")
		wait("a2")
		wait("a3")
		var got []string
		for range 3 {
			h.release(20*time.Millisecond, true, false)
			got = append(got, <-order)
		}
		if strings.Join(got, " ") != "a1 a2 a3" {
			t.Fatalf("grant order %v, want a1 a2 a3", got)
		}
	})
	t.Run("an unused slot and a timeout give the slot back", func(t *testing.T) {
		h := newTestHose()
		_, _ = h.acquire(ctx, nil)
		_, _ = h.acquire(ctx, nil)
		h.unused()
		h.release(0, false, false)
		if h.inflight != 0 || h.timeouts != 1 || h.w != 2 {
			t.Fatalf("inflight %d timeouts %d w %.2f, want 0, 1 and 2", h.inflight, h.timeouts, h.w)
		}
	})
}

func TestHoseLateAckSamples(t *testing.T) {
	q := newRpcQueue(16)
	q.enableHave(testQueueIndex, true, false)
	h := newTestHose() // target 20 ms, Kp 0.1, window 2
	h.cfg.Timeout = 50 * time.Millisecond
	q.hose = h
	root := strings.Repeat("g", 32)
	if err := q.Push(testDataRPC(root, 3), false); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if _, err := q.Pop(ctx); err != nil {
		t.Fatal(err)
	}
	time.Sleep(80 * time.Millisecond) // past the timeout: the slot is back, the frame expired
	h.mu.Lock()
	inflight, timeouts := h.inflight, h.timeouts
	h.mu.Unlock()
	if inflight != 0 || timeouts != 1 {
		t.Fatalf("after the timeout inflight %d timeouts %d, want 0 and 1", inflight, timeouts)
	}
	q.queueMu.Lock()
	q.hoseSent.minD = time.Millisecond   // the base round trip, from a fast acknowledgement seen before
	q.hoseAckedLocked(root, []uint32{3}) // the late acknowledgement
	q.queueMu.Unlock()
	s, _ := (&PubSub{hose: h}).HoseStats()
	if s.LateAcks != 1 || s.Acks != 0 || h.inflight != 0 {
		t.Fatalf("late acks %d acks %d inflight %d, want 1, 0 and 0: sampled, no second release", s.LateAcks, s.Acks, h.inflight)
	}
	if s.W >= 2 {
		t.Fatalf("a delay of ~80 ms over a 20 ms target must close the window, W %.2f", s.W)
	}
}

// TestHoseSentForgetsSettledGroups: a group whose frames to the peer were all acknowledged, or
// expired past the late horizon, leaves the queue's in-flight map, so a long-lived connection keeps
// no entry per group it was fed.
func TestHoseSentForgetsSettledGroups(t *testing.T) {
	q := newRpcQueue(16)
	q.enableHave(testQueueIndex, true, false)
	h := newTestHose()
	h.cfg.Timeout = 5 * time.Millisecond // the late horizon is ten of them
	q.hose = h
	acked, lost := strings.Repeat("a", 32), strings.Repeat("l", 32)
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	for _, root := range []string{acked, lost} {
		if err := q.Push(testDataRPC(root, 3), false); err != nil {
			t.Fatal(err)
		}
		if _, err := q.Pop(ctx); err != nil {
			t.Fatal(err)
		}
	}
	held := func(root string) bool {
		q.queueMu.Lock()
		defer q.queueMu.Unlock()
		_, ok := q.hoseSent.at[root]
		return ok
	}
	if !held(acked) || !held(lost) {
		t.Fatal("the popped frames are not in flight")
	}
	q.queueMu.Lock()
	q.hoseAckedLocked(acked, []uint32{3})
	q.queueMu.Unlock()
	if held(acked) {
		t.Error("a group whose every frame was acknowledged is still in the in-flight map")
	}
	deadline := time.Now().Add(time.Second)
	for held(lost) && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if held(lost) {
		t.Error("a group whose every frame expired past the late horizon is still in the in-flight map")
	}
}

// TestHoseSentKeepsAGroupPartlyAcknowledged: a group stays in the in-flight map while any of its
// frames to the peer is unacknowledged, with that frame's entry, and leaves with the last.
func TestHoseSentKeepsAGroupPartlyAcknowledged(t *testing.T) {
	q := newRpcQueue(16)
	q.enableHave(testQueueIndex, true, false)
	q.hose = newTestHose()
	root := strings.Repeat("p", 32)
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	for _, idx := range []byte{3, 4} {
		if err := q.Push(testDataRPC(root, idx), false); err != nil {
			t.Fatal(err)
		}
		if _, err := q.Pop(ctx); err != nil {
			t.Fatal(err)
		}
	}
	inFlight := func() (rootHeld, fourHeld bool) {
		q.queueMu.Lock()
		defer q.queueMu.Unlock()
		byIdx, ok := q.hoseSent.at[root]
		_, four := byIdx[4]
		return ok, four
	}
	q.queueMu.Lock()
	q.hoseAckedLocked(root, []uint32{3})
	q.queueMu.Unlock()
	if rootHeld, fourHeld := inFlight(); !rootHeld || !fourHeld {
		t.Fatalf("after frame 3's acknowledgement: group held %v, frame 4 held %v; want true true", rootHeld, fourHeld)
	}
	q.queueMu.Lock()
	q.hoseAckedLocked(root, []uint32{4})
	q.queueMu.Unlock()
	if rootHeld, _ := inFlight(); rootHeld {
		t.Error("the group is still in the in-flight map after its last frame's acknowledgement")
	}
}
