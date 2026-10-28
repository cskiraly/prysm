package pubsub

import (
	"encoding/binary"
	"fmt"
	"math"
	"sort"
	"time"
)

// --- The capacity estimate from forward delay ---
//
// Q133's acknowledged sources failed because an acknowledgement's timing carries the receiver's
// uplink queue on the way back. Here the receiver stamps its receive time into the hose
// acknowledgement, so the sender reads the one-way delay of each frame: receive stamp − pop time.
// Per peer, the minimum of it is propagation plus the clock offset (LEDBAT's base delay, RFC
// 6817), so the excess is the forward queue alone, whatever the clocks. Queueing at the sender's
// uplink shows in every peer at once; at one receiver's downlink, in that peer only.
//
// Each frame then has an exit time on the sender's clock, stamp − the peer's base delay: the
// moment it left the sender's queue, give or take the receiver's. The stamp names the indices it
// times -- the frames of the RPC just received -- since the HAVE beside it covers every frame the
// peer holds, including ours still in flight that it got from others first (the first form timed
// those too, at one exit, and read the publisher at 4.4 × its uplink).
//
// The rate is FIFO arithmetic. Each frame also carries the node's written-bytes count at its pop;
// in a backlog, what was written between two frames' pops leaves between their exits, frames to
// every peer, unacknowledged ones and control alike. Over a run of frames whose forward queue stays
// above the threshold, the slope of written bytes against exit time is the uplink's rate -- no
// plateau, and no count of acknowledged frames, which under-read (Q133 smoke 2, again in the first
// form). Outside such runs a binding window with no queue showing is probed up.

const hoseStampMagic = "HTS\x02"

// encodeHoseStamp is the receive stamp's pseudo id: the time, a group root and the indices of
// that group it times, carried beside the acknowledging HAVE.
func encodeHoseStamp(at time.Time, root string, idxs []uint32) string {
	b := make([]byte, 0, len(hoseStampMagic)+8+len(root)+2*len(idxs))
	b = append(b, hoseStampMagic...)
	b = binary.BigEndian.AppendUint64(b, uint64(at.UnixNano()))
	b = append(b, root...)
	for _, i := range idxs {
		b = binary.BigEndian.AppendUint16(b, uint16(i))
	}
	return string(b)
}

// decodeHoseStamp reads a receive stamp, or reports that mid is not one.
func decodeHoseStamp(mid string) (at time.Time, root string, idxs []uint32, ok bool) {
	h := len(hoseStampMagic) + 8 + haveRootLen
	if len(mid) < h || (len(mid)-h)%2 != 0 || mid[:len(hoseStampMagic)] != hoseStampMagic {
		return time.Time{}, "", nil, false
	}
	at = time.Unix(0, int64(binary.BigEndian.Uint64([]byte(mid[len(hoseStampMagic):]))))
	root = mid[len(hoseStampMagic)+8 : h]
	for i := h; i < len(mid); i += 2 {
		idxs = append(idxs, uint32(binary.BigEndian.Uint16([]byte(mid[i:i+2]))))
	}
	return at, root, idxs, true
}

// WithHoseAckStamp adds the receive stamp to every hose acknowledgement this node sends.
func WithHoseAckStamp() Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok {
			return fmt.Errorf("hose ack stamps require a gossipsub router")
		}
		gs.hoseStamp = true
		return nil
	}
}

// hoseAckIDs is an acknowledgement's ids: the HAVEs, and with stamps one receive stamp per
// group root naming the indices of mids, the frames just received.
func (gs *GossipSubRouter) hoseAckIDs(mids []string) []string {
	ids := gs.haveIDontWantIDs(mids)
	if !gs.hoseStamp || gs.have == nil {
		return ids
	}
	now := time.Now()
	byRoot := make(map[string][]uint32)
	var roots []string
	for _, mid := range mids {
		root, idx, ok := gs.have.index(mid)
		if !ok || len(root) != haveRootLen {
			continue
		}
		if byRoot[root] == nil {
			roots = append(roots, root)
		}
		byRoot[root] = append(byRoot[root], idx)
	}
	if len(roots) == 0 {
		return ids
	}
	ids = append([]string(nil), ids...)
	for _, root := range roots {
		ids = append(ids, encodeHoseStamp(now, root, byRoot[root]))
	}
	return ids
}

// hoseStampOf finds the receive stamps among an IDONTWANT's ids: the time, and the indices they
// time by root.
func hoseStampOf(ids []string) (time.Time, map[string]map[uint32]bool, bool) {
	var at time.Time
	var out map[string]map[uint32]bool
	for _, id := range ids {
		t, root, idxs, ok := decodeHoseStamp(id)
		if !ok {
			continue
		}
		if out == nil {
			out = make(map[string]map[uint32]bool)
		}
		set := out[root]
		if set == nil {
			set = make(map[uint32]bool)
			out[root] = set
		}
		for _, i := range idxs {
			set[i] = true
		}
		at = t
	}
	return at, out, out != nil
}

// fwdSample is one stamped frame: its exit time on our clock, its forward queue, and the node's
// written bytes at its pop.
type fwdSample struct {
	exit  time.Time
	q     time.Duration
	wrote int64
	bound bool
}

// hoseFwdMinRun is the fewest frames a backlogged run needs before its span reads as a rate.
const hoseFwdMinRun = 16

// hoseFwdLag is how long after its exit a frame's acknowledgement is waited for before its run is
// read; later ones are dropped from the rate, which only lowers it.
const hoseFwdLag = 500 * time.Millisecond

// noteForward records a stamped acknowledgement of a frame popped at popped. minOWD is the peer's
// running minimum one-way delay, updated here. Caller holds the queue lock.
func (h *hoseWindow) noteForward(minOWD *time.Duration, have *bool, popped, stamp time.Time, wrote int64, bound bool) {
	if h.est == nil || !h.est.cfg.Forward {
		return
	}
	owd := stamp.Sub(popped)
	if !*have || owd < *minOWD {
		*minOWD, *have = owd, true
	}
	q := owd - *minOWD
	h.mu.Lock()
	h.fwd = append(h.fwd, fwdSample{exit: popped.Add(q), q: q, wrote: wrote, bound: bound})
	h.mu.Unlock()
}

// applyForward is the forward source's step: read the runs whose acknowledgements are in, add a
// sample for each long enough, then set the window from the maximum inside the horizon, or probe
// while it binds with no queue showing.
func (h *hoseWindow) applyForward(now time.Time) {
	e := h.est
	h.mu.Lock()
	queuedNow := false // the latest acknowledgements, not the lagged runs, say whether the link is full now
	for _, s := range h.fwd {
		queuedNow = queuedNow || s.q >= e.cfg.ForwardQueue
	}
	e.fwdPending = append(e.fwdPending, h.fwd...)
	h.fwd = h.fwd[:0]
	binds := len(h.waiters) > 0
	h.mu.Unlock()
	sort.Slice(e.fwdPending, func(i, j int) bool { return e.fwdPending[i].exit.Before(e.fwdPending[j].exit) })
	thr := e.cfg.ForwardQueue
	cut := 0
	for cut < len(e.fwdPending) && now.Sub(e.fwdPending[cut].exit) >= hoseFwdLag {
		cut++
	}
	ready := e.fwdPending[:cut]
	queued, unbound := 0, false
	for i := 0; i < len(ready); {
		if ready[i].q < thr {
			i++
			continue
		}
		j := i
		for j < len(ready) && ready[j].q >= thr {
			j++
		}
		queued += j - i
		if r, ok := runRate(ready[i:j]); ok {
			bound := 0
			for _, s := range ready[i:j] {
				if s.bound {
					bound++
				}
			}
			if 2*bound > j-i {
				// Sent under a binding window: the window, perhaps not the link, set the pace, so
				// the rate is only a floor.
				e.floors = append(e.floors, rateSample{now, r})
			} else {
				e.add(now, r)
				unbound = true
			}
		}
		i = j
	}
	e.fwdQueued.Add(int64(queued))
	e.fwdSeen.Add(int64(len(ready)))
	e.fwdPending = append(e.fwdPending[:0], e.fwdPending[cut:]...)
	cut = 0
	for cut < len(e.samples) && now.Sub(e.samples[cut].at) > e.cfg.Horizon {
		cut++
	}
	e.samples = e.samples[cut:]
	cut = 0
	for cut < len(e.floors) && now.Sub(e.floors[cut].at) > e.cfg.Horizon {
		cut++
	}
	e.floors = e.floors[cut:]
	prev := math.Float64frombits(e.est.Load())
	rate := prev
	measured := 0.0
	for _, s := range e.samples {
		measured = math.Max(measured, s.rate)
	}
	if measured > 0 {
		rate = measured
		e.measured.Add(1)
	}
	// A probe is a floor until the link is seen full with the window out of the way; a rate
	// measured under a binding window is a floor too, never a cap.
	if unbound {
		e.probe = 0
	}
	for _, s := range e.floors {
		rate = math.Max(rate, s.rate)
	}
	rate = math.Max(rate, e.probe)
	if binds && !queuedNow {
		e.probe = math.Max(rate, prev) * e.cfg.ProbeGain
		rate = e.probe
		e.probes.Add(1)
	}
	e.est.Store(math.Float64bits(rate))
	h.setWindow(rate * (e.cfg.MeanRTT() + h.cfg.Target).Seconds() / float64(e.cfg.FrameBytes))
}

// runRate is a backlogged run's rate: the least-squares slope of written bytes against exit
// time, so no one frame's exit, off by its peer's base, decides it.
func runRate(run []fwdSample) (float64, bool) {
	if len(run) < hoseFwdMinRun {
		return 0, false
	}
	t0 := run[0].exit
	var sx, sy float64
	for _, s := range run {
		sx += s.exit.Sub(t0).Seconds()
		sy += float64(s.wrote)
	}
	n := float64(len(run))
	mx, my := sx/n, sy/n
	var sxx, sxy float64
	for _, s := range run {
		dx := s.exit.Sub(t0).Seconds() - mx
		sxx += dx * dx
		sxy += dx * (float64(s.wrote) - my)
	}
	if sxx <= 0 || sxy <= 0 {
		return 0, false
	}
	return sxy / sxx, true
}
