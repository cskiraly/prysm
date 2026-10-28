package pubsub

import (
	"math"
	"testing"
	"time"
)

func TestHoseEstimate(t *testing.T) {
	newH := func(prior float64) *hoseWindow {
		cfg := HoseConfig{Target: 20 * time.Millisecond, Kp: 0.1, Init: 2, Min: 1, Max: 10000, Timeout: time.Second}
		e, err := newHoseEstimator(HoseEstimate{
			Delivered: func() uint64 { return 0 }, MeanRTT: func() time.Duration { return 80 * time.Millisecond },
			FrameBytes: 1000, Prior: prior,
		})
		if err != nil {
			t.Fatal(err)
		}
		return &hoseWindow{cfg: cfg, w: cfg.Init, est: e}
	}
	now := time.Now()
	t.Run("the window is the measured maximum times round trip plus target", func(t *testing.T) {
		h := newH(1e5)
		h.est.samples = []rateSample{{now, 4e5}, {now, 6e5}, {now, 5e5}}
		h.applyEstimate(now, nil)
		if want := 6e5 * 0.1 / 1000; math.Abs(h.w-want) > 1e-9 {
			t.Fatalf("w %.2f, want %.2f from the maximum sample", h.w, want)
		}
	})
	t.Run("an unmeasured window that binds probes up, one that does not holds the prior", func(t *testing.T) {
		h := newH(1e5)
		h.applyEstimate(now, nil)
		if want := 1e5 * 0.1 / 1000; math.Abs(h.w-want) > 1e-9 {
			t.Fatalf("w %.2f, want the prior's %.2f", h.w, want)
		}
		for range 2 { // a backlog waits at the window each tick; growing it wakes the waiter
			h.waiters, h.waitCls = []chan struct{}{make(chan struct{})}, []int{0}
			h.applyEstimate(now, nil)
		}
		if want := 1e5 * 1.25 * 1.25 * 0.1 / 1000; math.Abs(h.w-want) > 1e-9 {
			t.Fatalf("w %.3f after two binding ticks, want %.3f", h.w, want)
		}
	})
	t.Run("samples past the horizon expire and the estimate is kept", func(t *testing.T) {
		h := newH(1e5)
		h.est.samples = []rateSample{{now, 6e5}}
		h.applyEstimate(now, nil)
		h.applyEstimate(now.Add(31*time.Second), nil)
		if len(h.est.samples) != 0 || math.Abs(h.w-60) > 1e-9 {
			t.Fatalf("%d samples, w %.2f; want none and the kept 60", len(h.est.samples), h.w)
		}
	})
	t.Run("from acknowledgements, an unqueued rate only raises, a queued one measures", func(t *testing.T) {
		h := newH(1e5) // prior 1e5 B/s; frames of 1000 B over three 100 ms ticks
		h.est.cfg.Delivered = nil
		tick := func(acks int, q time.Duration) {
			h.est.recent[h.est.n%3] = acks
			h.est.n++
			h.applyAcked(now, []time.Duration{q})
		}
		for range 3 {
			tick(10, 5*time.Millisecond) // 1e5 B/s, no queue: a lower bound equal to the prior
		}
		if want := 1e5 * 0.1 / 1000; math.Abs(h.w-want) > 1e-9 {
			t.Fatalf("w %.2f after unqueued ticks at the prior's rate, want %.2f", h.w, want)
		}
		for range 3 {
			tick(4, 5*time.Millisecond) // lower and unqueued: must not lower the estimate
		}
		if want := 1e5 * 0.1 / 1000; math.Abs(h.w-want) > 1e-9 {
			t.Fatalf("w %.2f after low unqueued ticks, want the kept %.2f", h.w, want)
		}
		tick(4, 40*time.Millisecond) // queued: a measurement of 4e4 B/s
		if want := 4e4 * 0.1 / 1000; math.Abs(h.w-want) > 1e-9 {
			t.Fatalf("w %.2f after a queued tick, want the measured %.2f", h.w, want)
		}
		h.waiters, h.waitCls = []chan struct{}{make(chan struct{})}, []int{0}
		tick(4, 5*time.Millisecond) // binding with room: probe
		if want := 4e4 * 1.25 * 0.1 / 1000; math.Abs(h.w-want) > 1e-9 {
			t.Fatalf("w %.3f after a binding unqueued tick, want the probe's %.3f", h.w, want)
		}
	})
	t.Run("locally, a limited round probes while the rate follows and measures at its plateau", func(t *testing.T) {
		h := newH(1e5)
		h.est.cfg.Delivered, h.est.cfg.LocalSaturation = nil, true
		round := func(acks int, limited bool) {
			for range 3 {
				if limited {
					h.waiters, h.waitCls = []chan struct{}{make(chan struct{})}, []int{0}
				}
				h.est.recent[h.est.n%3] = acks
				h.est.n++
				h.applyLocal(now)
			}
		}
		round(10, true) // 1e5 B/s, the window the limit: a probe
		if want := 1e5 * 1.25 * 0.1 / 1000; math.Abs(h.w-want) > 1e-9 {
			t.Fatalf("w %.3f after the first limited round, want the probe's %.3f", h.w, want)
		}
		round(12, true) // +20 percent: it follows, probe again
		if want := 1.2e5 * 1.25 * 0.1 / 1000; h.w < 1.25e5*1.25*0.1/1000-1e-9 {
			t.Fatalf("w %.3f after a following round, want a second probe (>= %.3f)", h.w, want)
		}
		round(12, true) // flat: the plateau, a measurement of 1.2e5
		if want := 1.2e5 * 0.1 / 1000; math.Abs(h.w-want) > 1e-9 {
			t.Fatalf("w %.3f at the plateau, want the measured %.3f", h.w, want)
		}
		h.waiters, h.waitCls = nil, nil
		round(6, false) // not limited, lower: a lower bound does not lower it
		if want := 1.2e5 * 0.1 / 1000; math.Abs(h.w-want) > 1e-9 {
			t.Fatalf("w %.3f after an unlimited low round, want the kept %.3f", h.w, want)
		}
	})
	t.Run("an acknowledgement under an estimate counts toward the tick", func(t *testing.T) {
		h := newH(1e5)
		h.inflight = 1
		h.release(30*time.Millisecond, true, false)
		if h.tickAcks != 1 || len(h.tickQ) != 1 {
			t.Fatalf("tick acks %d samples %d, want 1 and 1", h.tickAcks, len(h.tickQ))
		}
	})
	t.Run("acknowledgements no longer step an estimated window", func(t *testing.T) {
		h := newH(1e5)
		h.w = 50
		h.inflight = 1
		h.release(500*time.Millisecond, true, false)
		if h.w != 50 || h.acks != 1 {
			t.Fatalf("w %.2f acks %d, want 50 and 1", h.w, h.acks)
		}
	})
}
