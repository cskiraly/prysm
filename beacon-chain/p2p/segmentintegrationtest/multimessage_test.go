package segmentintegrationtest

// Multi-message cells: M payloads diffused one after another on one standing network, a slot
// apart, so every mechanism with memory (transport windows, the hose window, IWANT discipline,
// holder evidence) is measured warm as well as cold. Message 1 is the single-message cell (same
// payload, network and mesh; equal within the cell's run-to-run spread, since goroutine
// scheduling is not pinned); each later one has its own content, and every instrument is
// windowed to it.
// Messages within a cell share the topology, the link draw and the meshes, so they are samples
// within a network: seeds stay the unit of network-to-network variance.
//
//	SEGMENT_MESSAGES=M              messages per cell (1)
//	SEGMENT_MESSAGE_GAP_MS (12000)  publish to publish, virtual time; must exceed the deadline
//	SEGMENT_PUBLISHER=fixed|rotate|<node>
//	                                node 0 publishes every message, or message k's publisher is
//	                                node (k-1)·n/M, spreading the source over the network, or
//	                                node <node> publishes every message, the first included
//
// What carries over from one message to the next with the default gap of a slot: the IHAVE park
// (30 s) and the seen cache (minutes) outlive it; IDONTWANT and HAVE entries age out on
// heartbeats within it; peer scores and strikes persist. Distinct payloads keep the seen cache
// and the HAVE bitmaps from suppressing a later message; the rest is the network's memory, as on
// mainnet with the same parameters.

import (
	"fmt"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

// multiMessage is the optional variant hook a multi-message cell needs: before message m >= 2
// the variant switches to that message's payload and publisher, and restarts its own
// per-message instruments.
type multiMessage interface {
	nextMessage(t *testing.T, m, pub int)
}

// messagePlan is a cell's message schedule: the publisher of each message and the gap.
type messagePlan struct {
	pubs   []int
	gap    time.Duration
	node   int // the fixed publisher, 0 unless SEGMENT_PUBLISHER names a node
	rotate bool
}

// firstPublisher is the optional variant hook a plan whose first message is not node 0's needs:
// the variant takes pub as message 1's publisher before its setup.
type firstPublisher interface {
	setFirstPublisher(pub int)
}

func messagePlanFromEnv(t *testing.T, n int, deadline time.Duration) messagePlan {
	t.Helper()
	m := 1
	if v := os.Getenv("SEGMENT_MESSAGES"); v != "" {
		k, err := strconv.Atoi(v)
		if err != nil || k < 1 {
			t.Fatalf("bad SEGMENT_MESSAGES %q (want >= 1)", v)
		}
		m = k
	}
	p := messagePlan{gap: 12 * time.Second}
	if v := os.Getenv("SEGMENT_MESSAGE_GAP_MS"); v != "" {
		k, err := strconv.Atoi(v)
		if err != nil || k < 1 {
			t.Fatalf("bad SEGMENT_MESSAGE_GAP_MS %q", v)
		}
		p.gap = time.Duration(k) * time.Millisecond
	}
	switch v := os.Getenv("SEGMENT_PUBLISHER"); v {
	case "", "fixed":
	case "rotate":
		p.rotate = true
	default:
		k, err := strconv.Atoi(v)
		if err != nil || k < 0 || k >= n {
			t.Fatalf("bad SEGMENT_PUBLISHER %q (want fixed, rotate or a node index below %d)", v, n)
		}
		p.node = k
	}
	if m > 1 && p.gap <= deadline {
		// The horizon is read at the deadline after publish; the next message must not start first.
		t.Fatalf("SEGMENT_MESSAGE_GAP_MS %v must exceed the deadline %v", p.gap, deadline)
	}
	if p.rotate || p.node != 0 {
		// Knobs that single out node 0 as the source would no longer describe the publisher.
		v := os.Getenv("SEGMENT_PUBLISHER")
		for _, k := range []string{"SEGMENT_BUILDER_UP_MBPS", "SEGMENT_DISPATCH_CREDITS_SOURCE", "SEGMENT_HOSE_SOURCE_ONLY"} {
			if os.Getenv(k) != "" {
				t.Fatalf("SEGMENT_PUBLISHER=%s cannot run with %s, which is about node 0 as the source", v, k)
			}
		}
		if len(failWithholdSet(t, n)) > 0 || len(failSpoofSet(t, n)) > 0 {
			t.Fatalf("SEGMENT_PUBLISHER=%s cannot run with injected attackers", v)
		}
		if w, _ := narrowHolders(t, n); w > 0 {
			t.Fatalf("SEGMENT_PUBLISHER=%s cannot run with SEGMENT_FAIL_PUBLISH_NARROW", v)
		}
	}
	if p.node != 0 {
		// The pair and the uplink-class push degree also read node 0 as the source.
		for _, k := range []string{"SEGMENT_PAIR", "SEGMENT_A_PHASE_R_FAST"} {
			if os.Getenv(k) != "" {
				t.Fatalf("SEGMENT_PUBLISHER=%d cannot run with %s, which is about node 0 as the source", p.node, k)
			}
		}
	}
	for k := range m {
		pub := p.node
		if p.rotate {
			pub = k * n / m
		}
		p.pubs = append(p.pubs, pub)
	}
	return p
}

// messageSummary is one message's headline, kept for the cell's closing line.
type messageSummary struct {
	p50, p90, p99 time.Duration // -1: censored
	rxPerNode     int
}

func summarise(s diffusionStats) messageSummary {
	sorted := append([]time.Duration(nil), s.compTimes...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	q := func(p float64) time.Duration {
		// Nearest rank over every receiver, as CompletionStats: censored nodes rank last.
		idx := int(math.Ceil(p*float64(s.n-1))) - 1
		if idx >= 0 && idx < len(sorted) {
			return sorted[idx]
		}
		return -1
	}
	// Payload bytes on either path: variant B's ride in partial messages, A's and C's never do.
	return messageSummary{p50: q(0.5), p90: q(0.9), p99: q(0.99), rxPerNode: (s.rxBytes + s.partialRx) / s.n}
}

// messagesLine is the cell's closing line: message 1 (cold) apart, then the median and range of
// messages 2..M, each quantile taken per message first.
func messagesLine(p messagePlan, sums []messageSummary) string {
	mode := "fixed"
	if p.rotate {
		mode = "rotate"
	}
	ms := func(d time.Duration) string {
		if d < 0 {
			return "cens"
		}
		return strconv.FormatInt(d.Milliseconds(), 10)
	}
	var b strings.Builder
	fmt.Fprintf(&b, "messages: M=%d gap %v publisher %s %v; cold (1): p50 %s p90 %s p99 %s ms rx/node %dB",
		len(p.pubs), p.gap, mode, p.pubs, ms(sums[0].p50), ms(sums[0].p90), ms(sums[0].p99), sums[0].rxPerNode)
	warm := sums[1:]
	if len(warm) == 0 {
		return b.String()
	}
	stat := func(get func(messageSummary) time.Duration) string {
		var xs []time.Duration
		for _, s := range warm {
			x := get(s)
			if x < 0 {
				x = time.Duration(math.MaxInt64) // censored ranks last
			}
			xs = append(xs, x)
		}
		sort.Slice(xs, func(i, j int) bool { return xs[i] < xs[j] })
		f := func(d time.Duration) string {
			if d == time.Duration(math.MaxInt64) {
				return "cens"
			}
			return ms(d)
		}
		return fmt.Sprintf("%s [%s..%s]", f(xs[len(xs)/2]), f(xs[0]), f(xs[len(xs)-1]))
	}
	var rx []int
	for _, s := range warm {
		rx = append(rx, s.rxPerNode)
	}
	sort.Ints(rx)
	fmt.Fprintf(&b, "; warm (2..%d) median [min..max]: p50 %s p90 %s p99 %s ms rx/node %dB",
		len(p.pubs), stat(func(s messageSummary) time.Duration { return s.p50 }),
		stat(func(s messageSummary) time.Duration { return s.p90 }),
		stat(func(s messageSummary) time.Duration { return s.p99 }), rx[len(rx)/2])
	return b.String()
}

// meshSizes is every node's mesh degree now, without waiting for it to settle.
func meshSizes(tracers []*recordingTracer) []int {
	sizes := make([]int, len(tracers))
	for i, tr := range tracers {
		sizes[i] = tr.MeshPeers()
	}
	return sizes
}
