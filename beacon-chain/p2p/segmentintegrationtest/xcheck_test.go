package segmentintegrationtest

// The cross-check with the specification's toy simulator (the spec repository's toy/), two
// instruments:
//
// SEGMENT_XC_DUMP=<path> writes the network a cell runs on, for the toy to load
// (toy/run.py --topology <path>): gzip'd JSON lines. The first line is the network: each node's
// uplink and downlink, each connection with the latency model's one-way delay, each node's push
// degree, and the faulty set with its knobs. Then one line per message, written at its publish:
// the publisher and every node's mesh as it stands then. With the dump the toy runs on the
// harness's graph, latencies, link classes and meshes, so what still differs between the two is
// the link model and the rules, not the topology.
//
// The xcheck line, at the horizon, per population and for the publisher: what the nodes sent,
// counted at the stream write after every dispatch-time cancel (pubsub.UploadStat), what they
// received, and the invariants the specification's rules imply, each a count that must be zero:
//
//	budget   relays that pushed one frame more often than their push degree (frames over it);
//	window   nodes whose frames in flight exceeded their hose window;
//	overlap  asks for an id sent while an earlier ask for it was open: inside the discipline
//	         window (SEGMENT_IWANT_DISCIPLINE_MS) and unanswered;
//	late     duplicates their sender popped strictly after the receiver's HAVE for them had
//	         reached it (pops at the same virtual instant, a goroutine-order race, apart as
//	         late_same);
//	fault    frames sent by silent relays, answers sent by withholders, HAVEs sent by mute nodes.
//
// The toy prints the same line from its own counters (toy/xcheck.py), so a reader can set the two
// side by side. Every field is key=value; a reader splits on spaces.

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
	simlibp2p "github.com/libp2p/go-libp2p/x/simlibp2p"
)

// xcDump is the open SEGMENT_XC_DUMP file; nil when the instrument is off.
type xcDump struct {
	path string
	f    *os.File
	zw   *gzip.Writer
	w    *bufio.Writer
	enc  *json.Encoder
	msgs int
}

type xcEdge struct {
	A   int     `json:"a"`
	B   int     `json:"b"`
	OWD float64 `json:"owd_ms"`
}

type xcNetRecord struct {
	Kind    string   `json:"kind"`
	N       int      `json:"n"`
	Seed    uint64   `json:"seed"`
	Latency string   `json:"latency"`
	Up      []int    `json:"up_bps"`
	Down    []int    `json:"down_bps"`
	Edges   []xcEdge `json:"edges"`
	Budget  []int    `json:"push_degree,omitempty"`
	Faulty  []int    `json:"faulty"`
	Silent  bool     `json:"silent"`
	Mute    bool     `json:"mute"`
	Under   bool     `json:"underreport"`
	NoAck   bool     `json:"noack"`
}

type xcMeshRecord struct {
	Kind string  `json:"kind"`
	Msg  int     `json:"msg"`
	Pub  int     `json:"pub"`
	Mesh [][]int `json:"mesh"`
}

// xcDumpOpen opens the dump named by SEGMENT_XC_DUMP, or returns nil.
func xcDumpOpen(t *testing.T) *xcDump {
	t.Helper()
	path := os.Getenv("SEGMENT_XC_DUMP")
	if path == "" {
		return nil
	}
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("SEGMENT_XC_DUMP: %v", err)
	}
	zw := gzip.NewWriter(f)
	w := bufio.NewWriterSize(zw, 1<<20)
	return &xcDump{path: path, f: f, zw: zw, w: w, enc: json.NewEncoder(w)}
}

// net writes the network record: links in node order as meshLinks built them, the graph's
// connections with their modelled one-way delays, each node's push degree, the faulty set.
func (d *xcDump) net(t *testing.T, p diffusionParams, links []simlibp2p.NodeLinkSettingsAndCount, edges []edge, degree func(int) int) {
	t.Helper()
	if d == nil {
		return
	}
	rec := xcNetRecord{Kind: "net", N: p.n, Seed: p.seed, Latency: os.Getenv("SEGMENT_LATENCY_MODEL")}
	if rec.Latency == "" {
		rec.Latency = fmt.Sprintf("uniform %dms", p.latency.Milliseconds())
	}
	for _, l := range links {
		for range l.Count {
			rec.Up = append(rec.Up, l.LinkSettings.Uplink.BitsPerSecond)
			rec.Down = append(rec.Down, l.LinkSettings.Downlink.BitsPerSecond)
		}
	}
	if len(rec.Up) != p.n {
		t.Fatalf("SEGMENT_XC_DUMP: %d link settings for %d nodes", len(rec.Up), p.n)
	}
	geo := cellGeoFor(t, p.n, p.latency)
	for _, e := range edges {
		rec.Edges = append(rec.Edges, xcEdge{A: e.A, B: e.B, OWD: float64(geo.oneWay(e.A, e.B).Microseconds()) / 1000})
	}
	if degree != nil {
		for i := range p.n {
			rec.Budget = append(rec.Budget, degree(i))
		}
	}
	rec.Faulty = []int{}
	for i := range failWithholdSet(t, p.n) {
		rec.Faulty = append(rec.Faulty, i)
	}
	sort.Ints(rec.Faulty)
	rec.Silent = os.Getenv("SEGMENT_FAIL_RELAY_SILENCE") != ""
	rec.Mute = os.Getenv("SEGMENT_FAIL_MUTE") != ""
	rec.Under = os.Getenv("SEGMENT_FAIL_UNDERREPORT") != ""
	rec.NoAck = os.Getenv("SEGMENT_FAIL_NO_ACK") != ""
	if err := d.enc.Encode(rec); err != nil {
		t.Fatalf("SEGMENT_XC_DUMP: %v", err)
	}
}

// mesh writes message m's record at its publish: the publisher and every node's mesh peers.
func (d *xcDump) mesh(t *testing.T, m, pub int, nw *simNetwork, tracers []*recordingTracer) {
	t.Helper()
	if d == nil {
		return
	}
	idx := make(map[peer.ID]int, len(nw.Hosts))
	for i, h := range nw.Hosts {
		idx[h.ID()] = i
	}
	rec := xcMeshRecord{Kind: "mesh", Msg: m, Pub: pub, Mesh: make([][]int, len(tracers))}
	for i, tr := range tracers {
		ps := []int{}
		for _, p := range tr.MeshPeerIDs() {
			if j, ok := idx[p]; ok {
				ps = append(ps, j)
			}
		}
		sort.Ints(ps)
		rec.Mesh[i] = ps
	}
	if err := d.enc.Encode(rec); err != nil {
		t.Fatalf("SEGMENT_XC_DUMP: %v", err)
	}
	d.msgs++
}

// close flushes the dump and logs where it went.
func (d *xcDump) close(t *testing.T) {
	t.Helper()
	if d == nil {
		return
	}
	for _, err := range []error{d.w.Flush(), d.zw.Close(), d.f.Close()} {
		if err != nil {
			t.Errorf("SEGMENT_XC_DUMP: %v", err)
		}
	}
	t.Logf("      xcheck dump: the network and %d meshes -> %s", d.msgs, d.path)
}

// pushDegreeOf is a variant's per-node push degree, nil when it has none to check.
func pushDegreeOf(v variant) func(int) int {
	if pd, ok := v.(interface{ pushDegree(i int) int }); ok {
		return pd.pushDegree
	}
	return nil
}

// xcAskWindow is the pull discipline's window, 0 when the cell runs none.
func xcAskWindow() time.Duration {
	ms, err := strconv.Atoi(os.Getenv("SEGMENT_IWANT_DISCIPLINE_MS"))
	if err != nil || ms <= 0 {
		return 0
	}
	return time.Duration(ms) * time.Millisecond
}

// xcTake reads every node's cross-check counters at the horizon. It runs after take has read
// the request ledgers and the duplicates.
func (h *horizonSnapshot) xcTake(nw *simNetwork, tracers []*recordingTracer) {
	h.revoked = pubsub.HaveRevoked.Load()
	h.asksHeld, h.staleAsks = pubsub.AsksForHeldDropped.Load(), pubsub.StaleAsksDropped.Load()
	n := len(tracers)
	h.xc = make([]xcNode, n)
	for i := range tracers {
		if i >= len(nw.Pubsubs) {
			break
		}
		ps := nw.Pubsubs[i]
		x := &h.xc[i]
		x.up = ps.UploadStats()
		deg := 0
		if h.degree != nil {
			deg = h.degree(i)
		}
		for _, c := range ps.UploadPushPerFrame() {
			x.pushMax = max(x.pushMax, int(c))
			if deg > 0 && int(c) > deg {
				x.overBudget++
			}
		}
		x.hose, x.hoseOn = ps.HoseStats()
		x.rxFrames, _ = tracers[i].RecvFrames()
		if l := h.ledgers[i]; l != nil && h.askWindow > 0 {
			for _, r := range l.Records {
				for k := 1; k < len(r.Asks); k++ {
					a := r.Asks[k]
					open := r.Arrived.IsZero() || r.Arrived.After(a.At)
					if open && a.At.Sub(r.Asks[k-1].At) < h.askWindow-time.Millisecond {
						x.overlap++
					}
				}
			}
		}
		for _, s := range h.outbound[i] {
			x.cancelled += s.Cancelled
		}
		x.asksIn, x.asksInMesh = ps.AsksReceived()
		x.ihaveAt, x.pushAt, x.ansAt = ps.UploadTimes()
	}
	// The asks to the publisher, first or later, from the askers' request ledgers.
	if h.pub < len(nw.Hosts) {
		pubID := nw.Hosts[h.pub].ID()
		for i, l := range h.ledgers {
			if i == h.pub || l == nil {
				continue
			}
			for _, r := range l.Records {
				for k, a := range r.Asks {
					if a.Peer != pubID {
						continue
					}
					if k == 0 {
						h.askFirst++
					} else {
						h.askRetry++
					}
				}
			}
		}
	}
}

// msAfter is the first, median and last of ts in ms after start, -1 each when ts is empty.
func msAfter(ts []time.Time, start time.Time) (first, p50, last int64) {
	if len(ts) == 0 {
		return -1, -1, -1
	}
	d := make([]int64, len(ts))
	for i, t := range ts {
		d[i] = t.Sub(start).Milliseconds()
	}
	sort.Slice(d, func(a, b int) bool { return d[a] < d[b] })
	return d[0], d[len(d)/2], d[len(d)-1]
}

// xcNode is one node's cross-check reading at the horizon.
type xcNode struct {
	up                     pubsub.UploadStat
	pushMax, overBudget    int
	hose                   pubsub.HoseStat
	hoseOn                 bool
	rxFrames, overlap      int
	cancelled              int64
	asksIn, asksInMesh     int64
	ihaveAt, pushAt, ansAt []time.Time
}

// xcheckLine renders the set's counters and invariants. Faults count only the injected set's
// nodes, under the knobs that make each a violation.
func (h *horizonSnapshot) xcheckLine(set []int) string {
	if len(h.xc) == 0 {
		return ""
	}
	var push, ans, cancel, ihaveIDs, ihaveBits, iwantIDs, asks, idwIDs, idwBits, dups, rx int64
	var budgetNodes, overFrames, pushMax, winNodes, peak, wHi, overlap, late, lateSame, fault int
	var lateGaps []float64
	var asksIn, asksInMesh int64
	silent := os.Getenv("SEGMENT_FAIL_RELAY_SILENCE") != ""
	mute := os.Getenv("SEGMENT_FAIL_MUTE") != ""
	slowWithhold := os.Getenv("SEGMENT_FAIL_WITHHOLD_SLOW_MS") != ""
	for _, i := range set {
		if i >= len(h.xc) {
			continue
		}
		x := h.xc[i]
		u := x.up
		push += u.PushFrames
		ans += u.AnswerFrames
		ihaveIDs += u.IHaveIDs
		ihaveBits += u.IHaveBitmaps
		iwantIDs += u.IWantIDs
		asks += u.IWantAsks
		idwIDs += u.IDontWantIDs
		idwBits += u.IDontWantBitmaps
		cancel += x.cancelled
		dups += int64(h.dups[i])
		rx += int64(x.rxFrames)
		if i != h.pub {
			pushMax = max(pushMax, x.pushMax)
			if x.overBudget > 0 {
				budgetNodes++
				overFrames += x.overBudget
			}
		}
		if x.hoseOn {
			peak = max(peak, x.hose.PeakInflight)
			w := int(math.Ceil(x.hose.WHi))
			wHi = max(wHi, w)
			if x.hose.PeakInflight > w {
				winNodes++
			}
		}
		overlap += x.overlap
		asksIn += x.asksIn
		asksInMesh += x.asksInMesh
		late += h.dupStats[i].failedStrict
		for _, g := range h.dupStats[i].lateGap {
			lateGaps = append(lateGaps, float64(g.Milliseconds()))
		}
		lateSame += h.dupStats[i].failed - h.dupStats[i].failedStrict
		if h.attacker[i] {
			if silent {
				fault += int(u.PushFrames + u.AnswerFrames)
			} else if !slowWithhold {
				fault += int(u.AnswerFrames)
			}
			if mute {
				fault += int(u.IHaveBitmaps + u.IDontWantIDs)
			}
		}
	}
	f := []string{
		fmt.Sprintf("push=%d", push), fmt.Sprintf("answer=%d", ans), fmt.Sprintf("cancel=%d", cancel),
		fmt.Sprintf("ask=%d", asks), fmt.Sprintf("iwant_ids=%d", iwantIDs),
		fmt.Sprintf("have_ihave=%d", ihaveBits), fmt.Sprintf("ihave_ids=%d", ihaveIDs),
		fmt.Sprintf("have_idontwant=%d", idwBits), fmt.Sprintf("idontwant_ids=%d", idwIDs),
		fmt.Sprintf("dup=%d", dups), fmt.Sprintf("rx=%d", rx),
		"|",
		fmt.Sprintf("inv_budget=%d", budgetNodes), fmt.Sprintf("inv_budget_frames=%d", overFrames),
		fmt.Sprintf("inv_window=%d", winNodes), fmt.Sprintf("inv_overlap=%d", overlap),
		fmt.Sprintf("inv_late=%d", late), fmt.Sprintf("inv_fault=%d", fault),
		"|",
		fmt.Sprintf("push_max=%d", pushMax), fmt.Sprintf("peak=%d", peak), fmt.Sprintf("w=%d", wHi),
		fmt.Sprintf("late_same=%d", lateSame), fmt.Sprintf("ask_window_ms=%d", h.askWindow.Milliseconds()),
		fmt.Sprintf("revoked_all=%d", h.revoked), fmt.Sprintf("asks_held_dropped_all=%d", h.asksHeld),
		fmt.Sprintf("stale_asks_dropped_all=%d", h.staleAsks),
	}
	f = append(f, "|", fmt.Sprintf("asks_in=%d", asksIn), fmt.Sprintf("asks_in_mesh=%d", asksInMesh))
	if len(set) == 1 && set[0] == h.pub && set[0] < len(h.xc) {
		// The source's timeline, ms after publish: its IHAVE writes, its pushes, its answers; and
		// the asks it received, by whether they were their asker's first ask for the id.
		x := h.xc[set[0]]
		for _, s := range []struct {
			name string
			ts   []time.Time
		}{{"ihave", x.ihaveAt}, {"push", x.pushAt}, {"answer", x.ansAt}} {
			a, b, c := msAfter(s.ts, h.startAt)
			f = append(f, fmt.Sprintf("%s_n=%d", s.name, len(s.ts)), fmt.Sprintf("%s_first_ms=%d", s.name, a),
				fmt.Sprintf("%s_p50_ms=%d", s.name, b), fmt.Sprintf("%s_last_ms=%d", s.name, c))
		}
		f = append(f, fmt.Sprintf("asks_first=%d", h.askFirst), fmt.Sprintf("asks_retry=%d", h.askRetry))
	}
	if len(lateGaps) > 0 {
		sort.Float64s(lateGaps)
		f = append(f, fmt.Sprintf("late_gap_p50_ms=%.0f", quantile(lateGaps, 0.5)), fmt.Sprintf("late_gap_max_ms=%.0f", lateGaps[len(lateGaps)-1]))
	}
	return strings.Join(f, " ")
}
