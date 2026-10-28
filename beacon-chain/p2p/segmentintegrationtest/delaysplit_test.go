package segmentintegrationtest

// The hose window's delay, split (Q131). The window steps on D̂ = ack − pop − base RTT, and
// without link priority the ack queues behind the receiver's own upload, so D̂ is the sender's
// queueing plus the receiver's (Computer Networks 2014, eq. 6). The dispatch ledger has, per
// frame a node popped to a peer, the pop and the peer's first HAVE covering it (the ack); the
// receiver's tracer has the frame's arrival from that sender, first copy or duplicate. So
// pop → ack splits into forward (pop → arrival) and reverse (arrival → ack), and each is also
// read above its per-pair minimum, the base filter LEDBAT uses, which takes propagation out.

import (
	"fmt"
	"sort"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

type arrivalKey struct {
	from peer.ID
	what digest
}

// delaySplitLine reads the frames the nodes in set popped since start.
func delaySplitLine(name string, set []int, nw *simNetwork, tracers []*recordingTracer, start time.Time) string {
	idx := make(map[peer.ID]int, len(nw.Hosts))
	for i, h := range nw.Hosts {
		idx[h.ID()] = i
	}
	arrivals := make([]map[arrivalKey]time.Time, len(tracers))
	dupArrival := make([]map[arrivalKey]bool, len(tracers))
	arrivalsOf := func(j int) {
		if arrivals[j] != nil {
			return
		}
		a, d := map[arrivalKey]time.Time{}, map[arrivalKey]bool{}
		tr := tracers[j]
		tr.Mu.Lock()
		for k, evs := range [][]recvEvent{tr.Recvs, tr.Dups} {
			for _, e := range evs {
				key := arrivalKey{e.From, e.What}
				if t, ok := a[key]; !ok || e.At.Before(t) {
					a[key] = e.At
					d[key] = k == 1
				}
			}
		}
		tr.Mu.Unlock()
		arrivals[j], dupArrival[j] = a, d
	}
	type sample struct {
		pair     [2]int
		fwd, rev time.Duration
		dup      bool
	}
	var samples []sample
	noArrival := 0
	for _, i := range set {
		me := nw.Hosts[i].ID()
		for _, e := range nw.Pubsubs[i].HaveLedger() {
			if e.PopAt.IsZero() || e.PopAt.Before(start) || e.HaveAt.IsZero() || e.HaveAt.Before(e.PopAt) {
				continue
			}
			j, ok := idx[e.Peer]
			if !ok {
				continue
			}
			arrivalsOf(j)
			key := arrivalKey{me, digest(e.Digest)}
			at, ok := arrivals[j][key]
			if !ok || at.Before(e.PopAt) || e.HaveAt.Before(at) {
				noArrival++
				continue
			}
			samples = append(samples, sample{pair: [2]int{i, j}, fwd: at.Sub(e.PopAt), rev: e.HaveAt.Sub(at), dup: dupArrival[j][key]})
		}
	}
	if len(samples) == 0 {
		return fmt.Sprintf("delay split [%s %d]: no acked frames (%d without a matching arrival)", name, len(set), noArrival)
	}
	minF, minR := map[[2]int]time.Duration{}, map[[2]int]time.Duration{}
	for _, s := range samples {
		if m, ok := minF[s.pair]; !ok || s.fwd < m {
			minF[s.pair] = s.fwd
		}
		if m, ok := minR[s.pair]; !ok || s.rev < m {
			minR[s.pair] = s.rev
		}
	}
	var fwd, rev, fwdQ, revQ []float64
	dups := 0
	for _, s := range samples {
		fwd, rev = append(fwd, float64(s.fwd)), append(rev, float64(s.rev))
		fwdQ, revQ = append(fwdQ, float64(s.fwd-minF[s.pair])), append(revQ, float64(s.rev-minR[s.pair]))
		if s.dup {
			dups++
		}
	}
	for _, v := range [][]float64{fwd, rev, fwdQ, revQ} {
		sort.Float64s(v)
	}
	return fmt.Sprintf("delay split [%s %d]: %d acked frames (%d arrived as duplicates; %d without a matching arrival); forward p50 %s p90 %s, above base p50 %s p90 %s; reverse (ack) p50 %s p90 %s, above base p50 %s p90 %s",
		name, len(set), len(samples), dups, noArrival,
		msOf(quantile(fwd, 0.5)), msOf(quantile(fwd, 0.9)), msOf(quantile(fwdQ, 0.5)), msOf(quantile(fwdQ, 0.9)),
		msOf(quantile(rev, 0.5)), msOf(quantile(rev, 0.9)), msOf(quantile(revQ, 0.5)), msOf(quantile(revQ, 0.9)))
}
