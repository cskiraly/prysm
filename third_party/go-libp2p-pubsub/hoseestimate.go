package pubsub

import (
	"context"
	"math"
	"sort"
	"sync/atomic"
	"time"
)

// --- The hose window from a capacity estimate: the slow form ---
//
// The per-ack controller cannot settle inside one message (a fast source's upload is smaller
// than one bandwidth-delay product), so here the window is set, not stepped: W = Ĉ × (RTT + D0) /
// frame, with Ĉ the node's uplink capacity and RTT the mean base round trip to its peers.
//
// Ĉ is the maximum delivery rate over a horizon of recent ticks. Two sources:
//
//   - Acknowledged (Delivered nil, the default): the frames the node's peers acknowledged over
//     the last three ticks, times the frame size (three, so acknowledgements that arrive in a
//     bunch do not read as a rate). A receiver acknowledges only what arrived, so a queue
//     building in the uplink cannot inflate it. Most ticks the node has little to send, and such
//     a rate is only a lower bound on the capacity: it may raise the estimate, never lower it.
//     A tick whose acknowledgements show a queue (median queueing ≥ D0) had the link saturated,
//     and its rate is a measurement: the maximum of those inside the horizon sets the estimate,
//     up or down. A too-small window only measures itself, so while it binds with no queue
//     showing -- the link has room -- the estimate grows by ProbeGain. (BBR's reasoning: probe
//     while delivery can still rise.) A first form took every tick's rate as a measurement and
//     read relays, which rarely fill their uplink, at half their capacity (Q133 smoke 2).
//   - Local (LocalSaturation): the same acknowledged rate, but the node decides locally when it
//     measures capacity, since the one saturation signal an acknowledgement carries mixes the
//     receiver's reverse-path queue into the sender's (Q133 smoke 3). Locally a node sees only
//     whether its window is the limit: pops waiting for a slot. So, BBR's full-pipe test: in a
//     round (three ticks) with the window the limit throughout, grow the window by ProbeGain
//     while the delivery rate still follows (≥ +10 percent over the last round); when it stops
//     following, the link is full and the plateau is a measurement. A round without the window
//     the limit is a lower bound. A prior too high never binds and is never corrected, which
//     leaves the node as it would be without a window.
//   - Transport (Delivered set): bytes sent less lost on every connection, counted over ticks
//     with a writer blocked in its stream write at both ends. Kept for the record: it failed its
//     known answer (Q133 smoke), because QUIC rarely blocks a write -- the backlog sits in the link
//     queue, not the transport -- and bytes sent into that queue overstate delivery (up to 1.5x).

// HoseEstimate turns the window's controller into a capacity estimate (HoseConfig.Estimate).
type HoseEstimate struct {
	Tick    time.Duration // the sampling tick (default 100 ms)
	Horizon time.Duration // the maximum filter's reach (default 30 s)
	// Delivered, when set, is the transport source: bytes sent less lost on every connection so
	// far; nil takes the rate from acknowledged frames. MeanRTT is the mean base round trip.
	Delivered  func() uint64
	MeanRTT    func() time.Duration
	FrameBytes int     // the mean frame the window counts
	Prior      float64 // bytes per second before the first sample
	ProbeGain  float64 // growth a tick while the window binds unmeasured (default 1.25)
	// LocalSaturation takes the acknowledged rate but measures capacity only at a local
	// plateau: the window the limit for a round and the rate no longer following it.
	LocalSaturation bool
	// Forward takes capacity from the stamped forward delay (hosefwd.go): runs of frames whose
	// forward queue stays at or above ForwardQueue (default 10 ms). Every peer must stamp.
	Forward      bool
	ForwardQueue time.Duration
}

// hoseEstimator is the estimate's state, owned by its goroutine except where noted.
type hoseEstimator struct {
	cfg     HoseEstimate
	samples []rateSample // measurements inside the horizon, oldest first
	probe   float64      // the probing rate while unmeasured, bytes per second
	recent  [3]int       // frames acknowledged in the last three ticks, the acknowledged source's window
	n       int          // ticks seen, to fill recent

	// The local source's round: ticks in it, whether the window was the limit in all of them, and
	// the last limited round's rate.
	roundTicks   int
	roundLimited bool
	roundRef     float64

	fwdPending []fwdSample  // the forward source's frames not yet read, oldest exit first
	floors     []rateSample // the forward source's rates measured under a binding window, lower bounds

	// Observation, read by HoseEstimateStats.
	est, last          atomic.Uint64 // math.Float64bits: the estimate in use, the last valid sample
	valid, ticks       atomic.Int64
	probes, measured   atomic.Int64
	fwdSeen, fwdQueued atomic.Int64 // the forward source's frames read, and those in backlogged runs
}

type rateSample struct {
	at   time.Time
	rate float64
}

// estimateLoop drives the window from the estimate until ctx ends. blocked is the node's count of
// writers inside a stream write.
func (h *hoseWindow) estimateLoop(ctx context.Context, blocked *atomic.Int64) {
	e := h.est
	t := time.NewTicker(e.cfg.Tick)
	defer t.Stop()
	transport := e.cfg.Delivered != nil
	var prevAt time.Time
	var prev uint64
	var prevBlocked bool
	if transport {
		prevAt, prev, prevBlocked = time.Now(), e.cfg.Delivered(), blocked.Load() > 0
	}
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-t.C:
			e.ticks.Add(1)
			h.mu.Lock()
			acks, qs := h.tickAcks, h.tickQ
			h.tickAcks, h.tickQ = 0, nil
			h.mu.Unlock()
			if transport {
				cur, isBlocked := e.cfg.Delivered(), blocked.Load() > 0
				if isBlocked && prevBlocked && cur >= prev {
					e.add(now, float64(cur-prev)/now.Sub(prevAt).Seconds())
				}
				prevAt, prev, prevBlocked = now, cur, isBlocked
				h.applyEstimate(now, qs)
				continue
			}
			e.recent[e.n%len(e.recent)] = acks
			e.n++
			if e.cfg.Forward {
				h.applyForward(now)
				continue
			}
			if e.cfg.LocalSaturation {
				h.applyLocal(now)
			} else {
				h.applyAcked(now, qs)
			}
		}
	}
}

func (e *hoseEstimator) add(now time.Time, r float64) {
	e.samples = append(e.samples, rateSample{now, r})
	e.valid.Add(1)
	e.last.Store(math.Float64bits(r))
}

// applyAcked is the acknowledged source's step: the tick's rate over the last three ticks, a
// measurement when qs shows a queue, else a lower bound; a probe when the window binds unqueued.
func (h *hoseWindow) applyAcked(now time.Time, qs []time.Duration) {
	e := h.est
	r := e.ackedRate()
	queued := len(qs) > 0 && medianDuration(qs) >= h.cfg.Target
	if queued && r > 0 {
		e.add(now, r)
	}
	cut := 0
	for cut < len(e.samples) && now.Sub(e.samples[cut].at) > e.cfg.Horizon {
		cut++
	}
	e.samples = e.samples[cut:]
	prev := math.Float64frombits(e.est.Load())
	rate := prev
	if len(e.samples) > 0 {
		measured := 0.0
		for _, s := range e.samples {
			measured = math.Max(measured, s.rate)
		}
		rate = measured
		e.measured.Add(1)
	}
	rate = math.Max(rate, r) // any delivery rate is a lower bound
	h.mu.Lock()
	binds := len(h.waiters) > 0
	h.mu.Unlock()
	if binds && !queued {
		rate = math.Max(rate, prev) * e.cfg.ProbeGain
		e.probes.Add(1)
	}
	e.est.Store(math.Float64bits(rate))
	h.setWindow(rate * (e.cfg.MeanRTT() + h.cfg.Target).Seconds() / float64(e.cfg.FrameBytes))
}

// ackedRate is the acknowledged rate over the last three ticks, bytes per second.
func (e *hoseEstimator) ackedRate() float64 {
	frames := 0
	for _, a := range e.recent {
		frames += a
	}
	ticks := min(e.n, len(e.recent))
	return float64(frames*e.cfg.FrameBytes) / (float64(ticks) * e.cfg.Tick.Seconds())
}

// applyLocal is the local source's step: a lower bound each tick the window is not the limit, and
// at the end of each round with the window the limit throughout, a probe while the rate follows
// or a measurement at its plateau.
func (h *hoseWindow) applyLocal(now time.Time) {
	e := h.est
	r := e.ackedRate()
	h.mu.Lock()
	binds := len(h.waiters) > 0
	h.mu.Unlock()
	if e.roundTicks == 0 {
		e.roundLimited = true
	}
	e.roundTicks++
	e.roundLimited = e.roundLimited && binds
	prev := math.Float64frombits(e.est.Load())
	rate := prev
	probe, plateau := false, false
	if e.roundTicks == len(e.recent) {
		switch {
		case !e.roundLimited:
			e.roundRef = 0
		case e.roundRef > 0 && r < 1.1*e.roundRef:
			e.add(now, math.Max(r, e.roundRef)) // the plateau: the link, not the window, was the limit
			e.roundRef, plateau = r, true
		default:
			probe = true // the window was the limit and the rate still followed it
			e.roundRef = r
		}
		e.roundTicks = 0
	}
	cut := 0
	for cut < len(e.samples) && now.Sub(e.samples[cut].at) > e.cfg.Horizon {
		cut++
	}
	e.samples = e.samples[cut:]
	if len(e.samples) > 0 {
		measured := 0.0
		for _, s := range e.samples {
			measured = math.Max(measured, s.rate)
		}
		rate = measured
		e.measured.Add(1)
	}
	if e.roundRef > 0 && !plateau {
		rate = math.Max(rate, prev) // a probe holds its window until a round decides it
	}
	if !binds {
		rate = math.Max(rate, r) // a rate the window did not hold back is a lower bound
	}
	if probe {
		rate = math.Max(rate, prev) * e.cfg.ProbeGain
		e.probes.Add(1)
	}
	e.est.Store(math.Float64bits(rate))
	h.setWindow(rate * (e.cfg.MeanRTT() + h.cfg.Target).Seconds() / float64(e.cfg.FrameBytes))
}

// applyEstimate is the transport source's step: the maximum of the samples inside the horizon,
// or a probe while the window binds unmeasured.
func (h *hoseWindow) applyEstimate(now time.Time, _ []time.Duration) {
	e := h.est
	cut := 0
	for cut < len(e.samples) && now.Sub(e.samples[cut].at) > e.cfg.Horizon {
		cut++
	}
	e.samples = e.samples[cut:]
	measured := 0.0
	for _, s := range e.samples {
		measured = math.Max(measured, s.rate)
	}
	prev := math.Float64frombits(e.est.Load())
	h.mu.Lock()
	binds := len(h.waiters) > 0
	h.mu.Unlock()
	rate := prev
	switch {
	case measured > 0:
		rate = measured
		e.measured.Add(1)
	case binds:
		if e.probe == 0 {
			e.probe = prev
		}
		e.probe *= e.cfg.ProbeGain
		rate = e.probe
		e.probes.Add(1)
	}
	if measured > 0 {
		e.probe = 0
	}
	e.est.Store(math.Float64bits(rate))
	h.setWindow(rate * (e.cfg.MeanRTT() + h.cfg.Target).Seconds() / float64(e.cfg.FrameBytes))
}

func medianDuration(xs []time.Duration) time.Duration {
	s := append([]time.Duration(nil), xs...)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	return s[len(s)/2]
}

// setWindow moves the window to w frames, inside the configured bounds, and wakes waiters.
func (h *hoseWindow) setWindow(w float64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.w = math.Min(h.cfg.Max, math.Max(h.cfg.Min, w))
	h.wLo, h.wHi = math.Min(h.wLo, h.w), math.Max(h.wHi, h.w)
	h.wakeLocked()
}

// HoseEstimateStat is the capacity estimate, observed.
type HoseEstimateStat struct {
	Estimate, LastSample float64 // bytes per second
	Ticks, Valid         int64   // ticks, and those with the transport as the bottleneck
	Probes, Measured     int64   // ticks spent probing, and set from a measured estimate
	FwdSeen, FwdQueued   int64   // the forward source's frames read, and those in backlogged runs
}

// HoseEstimateStats reads the node's capacity estimate; ok is false when none runs.
func (p *PubSub) HoseEstimateStats() (HoseEstimateStat, bool) {
	if p.hose == nil || p.hose.est == nil {
		return HoseEstimateStat{}, false
	}
	e := p.hose.est
	return HoseEstimateStat{
		Estimate: math.Float64frombits(e.est.Load()), LastSample: math.Float64frombits(e.last.Load()),
		Ticks: e.ticks.Load(), Valid: e.valid.Load(), Probes: e.probes.Load(), Measured: e.measured.Load(),
		FwdSeen: e.fwdSeen.Load(), FwdQueued: e.fwdQueued.Load(),
	}, true
}

// newHoseEstimator fills the estimate's defaults.
func newHoseEstimator(cfg HoseEstimate) (*hoseEstimator, error) {
	if cfg.MeanRTT == nil || cfg.FrameBytes <= 0 || cfg.Prior <= 0 {
		return nil, errHoseEstimate
	}
	if cfg.Tick <= 0 {
		cfg.Tick = 100 * time.Millisecond
	}
	if cfg.Horizon <= 0 {
		cfg.Horizon = 30 * time.Second
	}
	if cfg.ProbeGain <= 1 {
		cfg.ProbeGain = 1.25
	}
	if cfg.ForwardQueue <= 0 {
		cfg.ForwardQueue = 10 * time.Millisecond
	}
	e := &hoseEstimator{cfg: cfg}
	e.est.Store(math.Float64bits(cfg.Prior))
	return e, nil
}
