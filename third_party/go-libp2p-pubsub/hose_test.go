package pubsub

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

func newTestHose(passive bool) *hoseWindow {
	cfg := HoseConfig{Target: 20 * time.Millisecond, Kp: 0.1, Init: 2, Min: 1, Max: 8, Timeout: time.Second, Passive: passive}
	return &hoseWindow{cfg: cfg, w: cfg.Init, wLo: cfg.Init, wHi: cfg.Init}
}

func TestHoseWindow(t *testing.T) {
	ctx := context.Background()
	t.Run("gates at the window and wakes on release", func(t *testing.T) {
		h := newTestHose(false)
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
		h := newTestHose(false)
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
	t.Run("passive never blocks and still steps", func(t *testing.T) {
		h := newTestHose(true)
		for range 20 {
			if ok, _ := h.acquire(ctx, nil); !ok {
				t.Fatal("passive window blocked")
			}
		}
		h.release(0, true, false)
		if h.inflight != 19 || h.w != 4 {
			t.Fatalf("inflight %d w %.2f, want 19 and 4", h.inflight, h.w)
		}
	})
	t.Run("a stats reset keeps the window and the frames in flight", func(t *testing.T) {
		h := newTestHose(false)
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
	t.Run("a freed slot goes to the lowest class waiting, first come within it", func(t *testing.T) {
		h := newTestHose(false)
		_, _ = h.acquire(ctx, nil)
		_, _ = h.acquire(ctx, nil)
		order := make(chan string, 3)
		queued := 0
		wait := func(name string, cls int) {
			go func() {
				if ok, _ := h.acquireClass(ctx, nil, cls); ok {
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
		wait("b1", 1)
		wait("a1", 0)
		wait("a2", 0)
		var got []string
		for range 3 {
			h.release(20*time.Millisecond, true, false)
			got = append(got, <-order)
		}
		if strings.Join(got, " ") != "a1 a2 b1" {
			t.Fatalf("grant order %v, want a1 a2 b1", got)
		}
	})
	t.Run("an unused slot and a timeout give the slot back", func(t *testing.T) {
		h := newTestHose(false)
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
	h := newTestHose(false) // target 20 ms, Kp 0.1, window 2
	h.cfg.Timeout = 50 * time.Millisecond
	h.cfg.BaseRTT = func(peer.ID) time.Duration { return time.Millisecond } // as the transport's min RTT would
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
	q.hoseAckedLocked(root, []uint32{3}, time.Time{}, nil) // the late acknowledgement
	q.queueMu.Unlock()
	s, _ := (&PubSub{hose: h}).HoseStats()
	if s.LateAcks != 1 || s.Acks != 0 || h.inflight != 0 {
		t.Fatalf("late acks %d acks %d inflight %d, want 1, 0 and 0: sampled, no second release", s.LateAcks, s.Acks, h.inflight)
	}
	if s.W >= 2 {
		t.Fatalf("a delay of ~80 ms over a 20 ms target must close the window, W %.2f", s.W)
	}
}
