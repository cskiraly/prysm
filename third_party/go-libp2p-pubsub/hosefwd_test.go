package pubsub

import (
	"math"
	"strings"
	"testing"
	"time"
)

func TestHoseForwardEstimate(t *testing.T) {
	const frame = 8192
	const capBps = 50e6 / 8 // the uplink, bytes per second
	newH := func(prior float64) *hoseWindow {
		cfg := HoseConfig{Target: 20 * time.Millisecond, Kp: 0.1, Init: 2, Min: 1, Max: 100000, Timeout: time.Second}
		e, err := newHoseEstimator(HoseEstimate{
			MeanRTT: func() time.Duration { return 112 * time.Millisecond }, FrameBytes: frame, Prior: prior, Forward: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		return &hoseWindow{cfg: cfg, w: cfg.Init, est: e}
	}
	type peerLink struct {
		minOWD  time.Duration
		haveOWD bool
		offset  time.Duration // the peer's clock ahead of ours plus propagation
	}
	// burst sends n frames at t0 into a queue draining at capBps, round-robin over peers, and
	// acknowledges each with the peer's stamp; the first frame of each peer sees an empty queue.
	burst := func(h *hoseWindow, peers []*peerLink, t0 time.Time, n int) {
		per := time.Duration(float64(frame) / capBps * float64(time.Second))
		for i := range n {
			p := peers[i%len(peers)]
			exit := t0.Add(time.Duration(i+1) * per) // popped at t0, left the uplink here
			h.noteForward(&p.minOWD, &p.haveOWD, t0, exit.Add(p.offset), int64(i*frame), false)
		}
	}
	t0 := time.Unix(1000, 0)
	t.Run("stamps round-trip and are found among an acknowledgement's ids", func(t *testing.T) {
		at, root := time.Unix(0, 1234567890123), strings.Repeat("r", haveRootLen)
		got, idx, ok := hoseStampOf([]string{"HAVE-ish", encodeHoseStamp(at, root, []uint32{3, 250})})
		if !ok || !got.Equal(at) || len(idx[root]) != 2 || !idx[root][3] || !idx[root][250] {
			t.Fatalf("got %v %v %v", got, idx, ok)
		}
		if _, _, _, ok := decodeHoseStamp("HTS\x01" + strings.Repeat("x", 8+haveRootLen)); ok {
			t.Fatal("a wrong magic decoded")
		}
	})
	t.Run("a backlogged burst reads the uplink whatever the clocks and paths", func(t *testing.T) {
		h := newH(capBps * 0.3)
		peers := []*peerLink{{offset: 3 * time.Second}, {offset: -40 * time.Millisecond}, {offset: 70 * time.Millisecond}, {offset: 5 * time.Millisecond}}
		// a first, short exchange teaches each peer its base: an idle frame each
		for i, p := range peers {
			pop := t0.Add(time.Duration(i) * time.Millisecond)
			h.noteForward(&p.minOWD, &p.haveOWD, pop, pop.Add(p.offset), 0, false)
		}
		burst(h, peers, t0.Add(time.Second), 200)
		h.applyForward(t0.Add(3 * time.Second))
		est := math.Float64frombits(h.est.est.Load())
		if r := est / capBps; r < 0.97 || r > 1.03 {
			t.Fatalf("estimate %.3f × the uplink, want 1 (valid samples %d)", r, h.est.valid.Load())
		}
		if want := capBps * 0.132 / frame; math.Abs(h.w-want) > 1 {
			t.Fatalf("window %.1f, want %.1f", h.w, want)
		}
	})
	t.Run("frames with no queue measure nothing and a binding window probes up", func(t *testing.T) {
		h := newH(capBps * 0.3)
		p := &peerLink{offset: 30 * time.Millisecond}
		for i := range 50 {
			pop := t0.Add(time.Duration(i) * 10 * time.Millisecond)
			h.noteForward(&p.minOWD, &p.haveOWD, pop, pop.Add(p.offset), int64(i*frame), false)
		}
		h.waiters, h.waitCls = []chan struct{}{make(chan struct{})}, []int{0}
		h.applyForward(t0.Add(2 * time.Second))
		if h.est.valid.Load() != 0 {
			t.Fatalf("%d samples from an idle link", h.est.valid.Load())
		}
		if est := math.Float64frombits(h.est.est.Load()); math.Abs(est/(capBps*0.3*1.25)-1) > 1e-9 {
			t.Fatalf("estimate %.0f, want the prior probed once", est)
		}
	})
	t.Run("a run too short or too recent is not read", func(t *testing.T) {
		h := newH(capBps)
		p := &peerLink{offset: time.Millisecond}
		h.noteForward(&p.minOWD, &p.haveOWD, t0, t0.Add(p.offset), 0, false)
		burst(h, []*peerLink{p}, t0.Add(time.Second), hoseFwdMinRun-1)
		h.applyForward(t0.Add(2 * time.Second))
		burst(h, []*peerLink{p}, t0.Add(3*time.Second), 100)
		h.applyForward(t0.Add(3*time.Second + 100*time.Millisecond)) // inside the lag
		if h.est.valid.Load() != 0 || len(h.est.fwdPending) == 0 {
			t.Fatalf("valid %d pending %d; want none read and the recent run kept", h.est.valid.Load(), len(h.est.fwdPending))
		}
	})
}
