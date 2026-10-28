package segmentintegrationtest

import (
	"fmt"
	"math"
	"os"
	"slices"
	"sort"
	"strconv"
	"testing"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
	simlibp2p "github.com/libp2p/go-libp2p/x/simlibp2p"
	"github.com/quic-go/quic-go"
)

// hoseKnobs is the hose window a cell runs (Hose Rate Control), read from the
// environment:
//
//	SEGMENT_HOSE=passive|on            the window records only, or gates data pops
//	SEGMENT_HOSE_TARGET_MS (20)        the queueing-delay target D0
//	SEGMENT_HOSE_KP (0.05)             window frames per ms of delay error
//	SEGMENT_HOSE_INIT/MIN/MAX (32/2/512) the window's start and bounds, in frames
//	SEGMENT_HOSE_TIMEOUT_MS (500)      a frame's slot comes back unacknowledged after this
//	SEGMENT_HOSE_SOURCE_ONLY=1         only the publisher has a window; every node acknowledges
//	SEGMENT_HOSE_OFFERS=<ms>           phase announces become offers under the window, closed after <ms>
//	SEGMENT_HOSE_STATIC=1              no controller: each node's window is fixed at its configured
//	                                   uplink × (SEGMENT_HOSE_STATIC_RTT_MS (112) + D0) / the mean frame
//	SEGMENT_HOSE_FRAME_SLOTS=1         a batched answer takes a slot per frame, not one for the batch
//	SEGMENT_HOSE_PEER_CAP=<frames>     at most this many frames in flight to any one peer
//	SEGMENT_HOSE_ESTIMATE=acks|local|transport the window from a capacity estimate (fork
//	                                   hoseestimate.go), a maximum over 30 s of delivery rates:
//	                                   acknowledged frames measured when their queueing shows (acks,
//	                                   also =1), acknowledged frames measured at a local plateau
//	                                   (local), QUIC's sent-less-lost bytes (transport), or runs
//	                                   of frames queued in the stamped forward delay (forward)
//	SEGMENT_HOSE_ESTIMATE_PRIOR (1)    the estimate's start, as a multiple of the node's uplink
//
// The static window is an oracle, like the regime rule: the frames in flight a link at its
// capacity holds when each waits the target and a round trip (Little's law), with 112 ms the geo
// model's mean round trip (one-way mean 56). It is the best a slow controller could learn in a
// cell whose links never change.
//
// Every node acknowledges received frames to their sender whenever the hose is set. The base
// round trip is the QUIC connection's minimum RTT, read through the host at acknowledgement time.
type hoseKnobs struct {
	estimate   bool // SEGMENT_HOSE_ESTIMATE
	transport  bool // the estimate from QUIC's counters, not acknowledged frames
	local      bool // the estimate measured at a local plateau
	forward    bool // the estimate from stamped forward delay; every node stamps its acknowledgements
	sourceOnly bool
	frameBytes int
	prior      float64       // the estimate's start, a multiple of the node's uplink
	staticRTT  time.Duration // SEGMENT_HOSE_STATIC; zero for the controller
	offers     time.Duration
	uplink     []int        // each node's configured uplink, bits per second, set by sized
	static     []float64    // each node's fixed window (or the estimate's first), set by sized
	noAck      map[int]bool // SEGMENT_FAIL_NO_ACK: the withholders send no hose acks, set by sized
	cfg        pubsub.HoseConfig
}

func hoseFromEnv(t *testing.T) *hoseKnobs {
	t.Helper()
	mode := os.Getenv("SEGMENT_HOSE")
	if mode == "" {
		return nil
	}
	if mode != "passive" && mode != "on" {
		t.Fatalf("bad SEGMENT_HOSE %q (want passive or on)", mode)
	}
	f := func(name string, def float64) float64 {
		v := os.Getenv(name)
		if v == "" {
			return def
		}
		x, err := strconv.ParseFloat(v, 64)
		if err != nil || x <= 0 {
			t.Fatalf("bad %s %q", name, v)
		}
		return x
	}
	ms := func(name string, def float64) time.Duration {
		return time.Duration(f(name, def) * float64(time.Millisecond))
	}
	k := &hoseKnobs{sourceOnly: os.Getenv("SEGMENT_HOSE_SOURCE_ONLY") != ""}
	if os.Getenv("SEGMENT_HOSE_OFFERS") != "" {
		k.offers = ms("SEGMENT_HOSE_OFFERS", 300)
	}
	k.cfg = pubsub.HoseConfig{
		Target: ms("SEGMENT_HOSE_TARGET_MS", 20), Kp: f("SEGMENT_HOSE_KP", 0.05),
		Init: f("SEGMENT_HOSE_INIT", 32), Min: f("SEGMENT_HOSE_MIN", 2), Max: f("SEGMENT_HOSE_MAX", 512),
		Timeout: ms("SEGMENT_HOSE_TIMEOUT_MS", 500), Passive: mode == "passive",
		FrameSlots: os.Getenv("SEGMENT_HOSE_FRAME_SLOTS") != "",
		// SEGMENT_HOSE_PEER_CAP=<frames>: no more than this many frames in flight to any one peer.
		PeerCap: int(f("SEGMENT_HOSE_PEER_CAP", 0)),
	}
	if os.Getenv("SEGMENT_HOSE_STATIC") != "" || os.Getenv("SEGMENT_HOSE_ESTIMATE") != "" {
		k.staticRTT = ms("SEGMENT_HOSE_STATIC_RTT_MS", 112)
	}
	if os.Getenv("SEGMENT_HOSE_ESTIMATE") != "" {
		k.estimate, k.prior = true, f("SEGMENT_HOSE_ESTIMATE_PRIOR", 1)
		switch v := os.Getenv("SEGMENT_HOSE_ESTIMATE"); v {
		case "1", "acks":
		case "local":
			k.local = true
		case "transport":
			k.transport = true
		case "forward":
			k.forward = true
		default:
			t.Fatalf("bad SEGMENT_HOSE_ESTIMATE %q (want acks, local, transport or forward)", v)
		}
		if os.Getenv("SEGMENT_HOSE_STATIC") != "" {
			t.Fatal("SEGMENT_HOSE_ESTIMATE and SEGMENT_HOSE_STATIC are two ways to set one window")
		}
	}
	if os.Getenv("SEGMENT_HAVE_BITMAP") == "" || os.Getenv("SEGMENT_HAVE_DISPATCH") == "" {
		t.Fatal("SEGMENT_HOSE needs SEGMENT_HAVE_BITMAP and SEGMENT_HAVE_DISPATCH")
	}
	return k
}

// sized is the knobs for a cell on links whose frames average frameBytes: under
// SEGMENT_HOSE_STATIC each node's window from its uplink, else k unchanged.
func (k *hoseKnobs) sized(t *testing.T, links []simlibp2p.NodeLinkSettingsAndCount, frameBytes int) *hoseKnobs {
	t.Helper()
	if k == nil || k.staticRTT == 0 || frameBytes <= 0 {
		return k
	}
	c := *k
	c.static, c.uplink, c.frameBytes = nil, nil, frameBytes
	n := 0
	for _, l := range links {
		n += l.Count
	}
	if os.Getenv("SEGMENT_FAIL_NO_ACK") != "" {
		if c.noAck = failWithholdSet(t, n); len(c.noAck) == 0 {
			t.Fatal("SEGMENT_FAIL_NO_ACK requires SEGMENT_FAIL_WITHHOLD_PCT")
		}
		t.Logf("failure exposure: %d withholders send no hose acks", len(c.noAck))
	}
	// SEGMENT_HOSE_CONFIGURED_SCALE=<x>: each window sized for x times the node's actual uplink, a
	// capacity measured idle and since taken in part by other traffic (x > 1: too generous).
	scale := 1.0
	if v := os.Getenv("SEGMENT_HOSE_CONFIGURED_SCALE"); v != "" {
		x, err := strconv.ParseFloat(v, 64)
		if err != nil || x <= 0 {
			t.Fatalf("bad SEGMENT_HOSE_CONFIGURED_SCALE %q (want a positive factor)", v)
		}
		scale = x
		t.Logf("      hose static: windows sized for %.2f × the actual uplink", scale)
	}
	// SEGMENT_HOSE_STATIC_CAP=<frames>: no window above this many frames, so a fast node's backlog
	// also waits where cancel and the decode claim reach it, rather than at its peers' downlinks.
	capFrames := math.Inf(1)
	if v := os.Getenv("SEGMENT_HOSE_STATIC_CAP"); v != "" {
		x, err := strconv.ParseFloat(v, 64)
		if err != nil || x < 1 {
			t.Fatalf("bad SEGMENT_HOSE_STATIC_CAP %q (want frames >= 1)", v)
		}
		capFrames = x
		t.Logf("      hose static: windows capped at %.0f frames", capFrames)
	}
	for _, l := range links {
		bytesPerSec := float64(l.LinkSettings.Uplink.BitsPerSecond) / 8 * scale
		if c.estimate {
			bytesPerSec *= c.prior // the estimate starts from the prior's window
		}
		w := math.Min(capFrames, math.Ceil(bytesPerSec*(c.staticRTT+c.cfg.Target).Seconds()/float64(frameBytes)))
		for range l.Count {
			c.static = append(c.static, w)
			c.uplink = append(c.uplink, l.LinkSettings.Uplink.BitsPerSecond)
		}
	}
	if c.estimate {
		t.Logf("      hose estimate (transport %v): prior %.2f × uplink, first window node 0 %.0f", c.transport, c.prior, c.static[0])
		return &c
	}
	t.Logf("      hose static: round trip %v + target %v over %d-byte frames: node 0 window %.0f, relays %.0f..%.0f",
		c.staticRTT, c.cfg.Target, frameBytes, c.static[0], slices.Min(c.static[1:]), slices.Max(c.static[1:]))
	return &c
}

// opts is node i's hose options; nw is read lazily, when the window asks for a base round trip.
func (k *hoseKnobs) opts(i int, nw func() *simNetwork) []pubsub.Option {
	if k == nil {
		return nil
	}
	opts := []pubsub.Option{pubsub.WithHoseAck()}
	if k.forward {
		opts = append(opts, pubsub.WithHoseAckStamp())
	}
	if k.noAck[i] {
		opts = nil // a sender's slot for a frame pushed here waits out its timeout
	}
	if k.sourceOnly && i != 0 {
		return opts
	}
	cfg := k.cfg
	switch {
	case k.estimate && k.static != nil:
		cfg.Init = math.Min(math.Max(k.static[i], cfg.Min), cfg.Max)
		cfg.Max = math.Max(cfg.Max, 100000) // the estimate, not a configured cap, bounds it
		cfg.Estimate = &pubsub.HoseEstimate{
			MeanRTT: func() time.Duration {
				if r := quicMeanMinRTT(nw(), i); r > 0 {
					return r
				}
				return k.staticRTT
			},
			FrameBytes:      k.frameBytes,
			Prior:           float64(k.uplink[i]) / 8 * k.prior,
			LocalSaturation: k.local,
			Forward:         k.forward,
		}
		if k.transport {
			cfg.Estimate.Delivered = func() uint64 { return quicDelivered(nw(), i) }
		}
	case k.static != nil:
		cfg.Init, cfg.Min, cfg.Max = k.static[i], k.static[i], k.static[i]
	}
	cfg.BaseRTT = func(p peer.ID) time.Duration { return quicMinRTT(nw(), i, p) }
	opts = append(opts, pubsub.WithHoseWindow(cfg))
	if k.offers > 0 {
		opts = append(opts, pubsub.WithHoseOffers(k.offers))
	}
	return opts
}

// quicConns is node i's QUIC connections.
func quicConns(nw *simNetwork, i int) []*quic.Conn {
	if nw == nil || i >= len(nw.Hosts) {
		return nil
	}
	var out []*quic.Conn
	for _, c := range nw.Hosts[i].Network().Conns() {
		var qc *quic.Conn
		if c.As(&qc) && qc != nil {
			out = append(out, qc)
		}
	}
	return out
}

// quicDelivered is node i's bytes sent less bytes lost over all its QUIC connections.
func quicDelivered(nw *simNetwork, i int) uint64 {
	var sent, lost uint64
	for _, qc := range quicConns(nw, i) {
		s := qc.ConnectionStats()
		sent, lost = sent+s.BytesSent, lost+s.BytesLost
	}
	if lost > sent {
		return 0
	}
	return sent - lost
}

// quicMeanMinRTT is the mean minimum RTT over node i's QUIC connections; zero when none.
func quicMeanMinRTT(nw *simNetwork, i int) time.Duration {
	var sum time.Duration
	n := 0
	for _, qc := range quicConns(nw, i) {
		if r := qc.ConnectionStats().MinRTT; r > 0 {
			sum += r
			n++
		}
	}
	if n == 0 {
		return 0
	}
	return sum / time.Duration(n)
}

// hoseEstimateLines is the known answer for the estimate: per population, the estimate over the
// node's configured uplink, the ticks with the transport as the bottleneck, and the probing.
func hoseEstimateLines(t *testing.T, nw *simNetwork, pub int, k *hoseKnobs) []string {
	if nw == nil || k == nil || !k.estimate {
		return nil
	}
	line := func(name string, set []int) string {
		var ratio []float64
		var valid, ticks, probes, seen, queued int64
		var rtt []float64
		for _, i := range set {
			s, ok := nw.Pubsubs[i].HoseEstimateStats()
			if !ok || k.uplink[i] <= 0 {
				continue
			}
			ratio = append(ratio, s.Estimate*8/float64(k.uplink[i]))
			valid, ticks, probes = valid+s.Valid, ticks+s.Ticks, probes+s.Probes
			seen, queued = seen+s.FwdSeen, queued+s.FwdQueued
			rtt = append(rtt, hoseMs(quicMeanMinRTT(nw, i)))
		}
		if len(ratio) == 0 {
			return ""
		}
		sort.Float64s(ratio)
		fwd := ""
		if k.forward {
			fwd = fmt.Sprintf("; forward frames %d, %d in backlogged runs", seen, queued)
		}
		return fmt.Sprintf("hose estimate [%s %d]: estimate / uplink p10 %.2f p50 %.2f p90 %.2f (min %.2f max %.2f); ticks valid %d of %d, probing %d; mean min RTT p50 %.0fms%s",
			name, len(ratio), ratio[len(ratio)/10], ratio[len(ratio)/2], ratio[len(ratio)*9/10], ratio[0], ratio[len(ratio)-1],
			valid, ticks, probes, hoseMedian(rtt), fwd)
	}
	lines := []string{line("publisher", []int{pub})}
	names, pops := populations(t, len(nw.Pubsubs), pub)
	for _, name := range names {
		if l := line(name, pops[name]); l != "" {
			lines = append(lines, l)
		}
	}
	return lines
}

// quicMinRTT is the smallest minimum RTT over node i's QUIC connections to p; zero when unknown.
func quicMinRTT(nw *simNetwork, i int, p peer.ID) time.Duration {
	if nw == nil || i >= len(nw.Hosts) {
		return 0
	}
	var best time.Duration
	for _, c := range nw.Hosts[i].Network().ConnsToPeer(p) {
		var qc *quic.Conn
		if !c.As(&qc) || qc == nil {
			continue
		}
		if r := qc.ConnectionStats().MinRTT; r > 0 && (best == 0 || r < best) {
			best = r
		}
	}
	return best
}

// hoseNewWindow starts every node's hose observation afresh for the next message of a
// multi-message cell and returns the offer counters so far, which stay cumulative: they are the
// offer pump's own state (its answered share), not an instrument.
func hoseNewWindow(nw *simNetwork) []pubsub.OfferStat {
	base := make([]pubsub.OfferStat, len(nw.Pubsubs))
	for i, ps := range nw.Pubsubs {
		ps.ResetHoseStats()
		base[i], _ = ps.HoseOfferStats()
	}
	return base
}

// hoseLines reports the window per class and for the publisher pub; offBase, when set, is the
// offer counters at the message's start.
func hoseLines(t *testing.T, nw *simNetwork, pub int, offBase []pubsub.OfferStat) []string {
	if nw == nil {
		return nil
	}
	var ansN int
	var a50s, a90s []float64
	var off pubsub.OfferStat
	offers := false
	var late int64
	read := func(set []int) (w, lo, hi, qd50, qd90, qmean, wait float64, acks, touts, gated int64, peak int, n int) {
		late = 0
		var ws, los, his, q50, q90, qm, wt []float64
		ansN, a50s, a90s, off, offers = 0, nil, nil, pubsub.OfferStat{}, false
		for _, i := range set {
			s, ok := nw.Pubsubs[i].HoseStats()
			if !ok {
				continue
			}
			n++
			ws, los, his = append(ws, s.W), append(los, s.WLo), append(his, s.WHi)
			q50, q90, qm = append(q50, hoseMs(s.QueueP50)), append(q90, hoseMs(s.QueueP90)), append(qm, hoseMs(s.QueueMean))
			wt = append(wt, hoseMs(s.WaitTime))
			acks += s.Acks
			touts += s.Timeouts
			late += s.LateAcks
			gated += s.Gated
			peak = max(peak, s.PeakInflight)
			if s.Answers > 0 {
				ansN += s.Answers
				a50s, a90s = append(a50s, hoseMs(s.AnswerP50)), append(a90s, hoseMs(s.AnswerP90))
			}
			if o, ok := nw.Pubsubs[i].HoseOfferStats(); ok {
				offers = true
				if offBase != nil {
					b := offBase[i]
					o.Queued, o.Sent, o.Answered = o.Queued-b.Queued, o.Sent-b.Sent, o.Answered-b.Answered
					o.Covered, o.Expired, o.Stale = o.Covered-b.Covered, o.Expired-b.Expired, o.Stale-b.Stale
				}
				off.Queued, off.Sent, off.Answered = off.Queued+o.Queued, off.Sent+o.Sent, off.Answered+o.Answered
				off.Covered, off.Expired, off.Stale = off.Covered+o.Covered, off.Expired+o.Expired, off.Stale+o.Stale
			}
		}
		if n == 0 {
			return
		}
		return hoseMedian(ws), hoseMedian(los), hoseMedian(his), hoseMedian(q50), hoseMedian(q90), hoseMedian(qm), hoseMedian(wt), acks, touts, gated, peak, n
	}
	names, pops := populations(t, len(nw.Pubsubs), pub)
	var lines []string
	emit := func(name string, set []int) {
		w, lo, hi, q50, q90, qm, wait, acks, touts, gated, peak, n := read(set)
		if n == 0 {
			return
		}
		l := fmt.Sprintf("hose [%s %d]: W end %.1f low %.1f high %.1f; queueing p50 %.0fms p90 %.0fms mean %.0fms; acks %d timeouts %d (late acks %d) gated %d wait %.0fms peak in flight %d",
			name, n, w, lo, hi, q50, q90, qm, acks, touts, late, gated, wait, peak)
		if ansN > 0 {
			l += fmt.Sprintf("; answers %d, queueing p50 %.0fms p90 %.0fms", ansN, hoseMedian(a50s), hoseMedian(a90s))
		}
		if offers {
			l += fmt.Sprintf("; offers queued %d sent %d answered %d covered %d expired %d stale %d", off.Queued, off.Sent, off.Answered, off.Covered, off.Expired, off.Stale)
		}
		lines = append(lines, l)
	}
	emit("publisher", []int{pub})
	for _, name := range names {
		emit(name, pops[name])
	}
	return lines
}

func hoseMs(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }

func hoseMedian(xs []float64) float64 {
	s := append([]float64(nil), xs...)
	sort.Float64s(s)
	return s[len(s)/2]
}
