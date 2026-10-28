package segmentintegrationtest

// SEGMENT_PUBLISH_REPLAY=<spy trace>: the publisher releases each message's frames at the offsets
// a stem received them in the trace's stem rows (SEGMENT_SRC_STEM's hand-off), one publish per frame
// as the pipelined stem does, and the frames it has not released as one batch at the K-th: a cell
// published from the stem on the hand-off's arrival schedule, without the hand-off (card P6's
// pacing baseline). Run it from the stem the trace names, at the trace's seed.
//
// SEGMENT_SRC_ADMIT is the virtual receiver: the publisher admits its own
// frames into its router on the schedule a hop-1 relay would receive them, so its HAVE, offers,
// pushes and answers follow the admitted frames only; with the default source HAVE and
// SEGMENT_A_PHASE_R_SOURCE equal to the relays' copies it forwards as a relay. The rest go at the
// K-th, a relay's decode.
//
//	rate:<ms>      K frames, a fixed permutation of all of them per message, evenly from 0 to <ms>
//	relay:<trace>  per message, the first arrivals of distinct frames at the source's median hop-1
//	               relay in that spy trace (push and answer rows), from the first at 0; run it at
//	               the trace's seed

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

// replayFrame is one frame of the schedule: its index and its publish-relative release offset.
type replayFrame struct {
	at  time.Duration
	idx int
}

// publishReplay is the schedule per message, frames in release order; rate, when set, builds each
// message's schedule at its first publish (SEGMENT_SRC_ADMIT=rate).
type publishReplay struct {
	path string
	at   map[int][]replayFrame
	rate time.Duration
}

// publishReplayFromEnv reads SEGMENT_PUBLISH_REPLAY's trace; nil when unset.
func publishReplayFromEnv(t *testing.T) *publishReplay {
	t.Helper()
	path := os.Getenv("SEGMENT_PUBLISH_REPLAY")
	if v := os.Getenv("SEGMENT_SRC_ADMIT"); v != "" {
		if path != "" {
			t.Fatal("SEGMENT_SRC_ADMIT and SEGMENT_PUBLISH_REPLAY are exclusive")
		}
		return admitFromEnv(t, v)
	}
	if path == "" {
		return nil
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("SEGMENT_PUBLISH_REPLAY: %v", err)
	}
	defer func() { _ = f.Close() }()
	zr, err := gzip.NewReader(f)
	if err != nil {
		t.Fatalf("SEGMENT_PUBLISH_REPLAY %s: %v", path, err)
	}
	r := &publishReplay{path: path, at: map[int][]replayFrame{}}
	// A two-hop stem's trace has stem rows at both stems; which schedule to replay is not this one's call.
	stems := map[int]map[string]bool{}
	sc := bufio.NewScanner(zr)
	for sc.Scan() {
		fs := strings.Split(sc.Text(), ",")
		if len(fs) < 7 || fs[0] != "E" || fs[6] != "stem" {
			continue
		}
		m, err1 := strconv.Atoi(fs[1])
		idx, err2 := strconv.Atoi(fs[4])
		ns, err3 := strconv.ParseInt(fs[5], 10, 64)
		if err1 != nil || err2 != nil || err3 != nil {
			t.Fatalf("SEGMENT_PUBLISH_REPLAY %s: bad stem row %q", path, sc.Text())
		}
		r.at[m] = append(r.at[m], replayFrame{at: time.Duration(ns), idx: idx})
		if stems[m] == nil {
			stems[m] = map[string]bool{}
		}
		stems[m][fs[2]] = true
	}
	if err := sc.Err(); err != nil {
		t.Fatalf("SEGMENT_PUBLISH_REPLAY %s: %v", path, err)
	}
	for m, s := range stems {
		if len(s) > 1 {
			t.Fatalf("SEGMENT_PUBLISH_REPLAY %s: message %d has stem rows at %d nodes (a two-hop stem's trace)", path, m, len(s))
		}
	}
	if len(r.at) == 0 {
		t.Fatalf("SEGMENT_PUBLISH_REPLAY %s: no stem rows", path)
	}
	for m := range r.at {
		sort.SliceStable(r.at[m], func(a, b int) bool { return r.at[m][a].at < r.at[m][b].at })
	}
	return r
}

// publish releases message m's frames on its schedule through publish, and at the k-th distinct
// frame every other of the n frames at once; it returns the release report.
func (r *publishReplay) publish(m, k, n int, publish func(idx []int) error) (string, error) {
	if r.rate > 0 && r.at[m] == nil {
		r.at[m] = rateSchedule(m, k, n, r.rate)
	}
	fr := r.at[m]
	if len(fr) < k {
		return "", fmt.Errorf("SEGMENT_PUBLISH_REPLAY %s: message %d has %d stem rows, want %d", r.path, m, len(fr), k)
	}
	t0 := time.Now()
	got := make(map[int]bool, k)
	for _, f := range fr {
		if d := time.Until(t0.Add(f.at)); d > 0 {
			time.Sleep(d)
		}
		if err := publish([]int{f.idx}); err != nil {
			return "", err
		}
		got[f.idx] = true
		if len(got) < k {
			continue
		}
		var rest []int
		for i := range n {
			if !got[i] {
				rest = append(rest, i)
			}
		}
		if len(rest) > 0 {
			if err := publish(rest); err != nil {
				return "", err
			}
		}
		return fmt.Sprintf("publish replay: %d frames released one by one from +%.1fms to +%.1fms (the K-th), then %d at once; schedule %s",
			len(got), float64(fr[0].at.Microseconds())/1000, float64(time.Since(t0).Microseconds())/1000, len(rest), r.path), nil
	}
	return "", fmt.Errorf("SEGMENT_PUBLISH_REPLAY %s: message %d has %d distinct frames, want %d", r.path, m, len(got), k)
}

// admitFromEnv reads SEGMENT_SRC_ADMIT=rate:<ms>|relay:<trace>.
func admitFromEnv(t *testing.T, v string) *publishReplay {
	t.Helper()
	if ms, ok := strings.CutPrefix(v, "rate:"); ok {
		d, err := strconv.ParseFloat(ms, 64)
		if err != nil || d <= 0 {
			t.Fatalf("bad SEGMENT_SRC_ADMIT %q (want rate:<ms> above 0)", v)
		}
		return &publishReplay{path: v, at: map[int][]replayFrame{}, rate: time.Duration(d * float64(time.Millisecond))}
	}
	path, ok := strings.CutPrefix(v, "relay:")
	if !ok {
		t.Fatalf("bad SEGMENT_SRC_ADMIT %q (want rate:<ms> or relay:<trace>)", v)
	}
	at, err := relayInflow(path)
	if err != nil {
		t.Fatalf("SEGMENT_SRC_ADMIT %s: %v", v, err)
	}
	return &publishReplay{path: v, at: at}
}

// rateSchedule releases k of n frames, a permutation fixed by m, evenly from 0 to d.
func rateSchedule(m, k, n int, d time.Duration) []replayFrame {
	idx := make([]int, n)
	for i := range idx {
		idx[i] = i
	}
	for i := n - 1; i > 0; i-- {
		j := int(mix64(uint64(m)<<32^uint64(i)^0x41444d4954) % uint64(i+1))
		idx[i], idx[j] = idx[j], idx[i]
	}
	fr := make([]replayFrame, k)
	for i := range fr {
		at := time.Duration(0)
		if k > 1 {
			at = d * time.Duration(i) / time.Duration(k-1)
		}
		fr[i] = replayFrame{at: at, idx: idx[i]}
	}
	return fr
}

// relayInflow is, per message of a spy trace, the first arrivals of distinct frames at the hop-1
// relay (a mesh peer of the publisher at publish) whose need-th arrival is the median among them,
// from its first arrival at 0.
func relayInflow(path string) (map[int][]replayFrame, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	zr, err := gzip.NewReader(f)
	if err != nil {
		return nil, err
	}
	pub, need := map[int]string{}, map[int]int{}
	mesh := map[int]map[string][]string{}
	first := map[int]map[string]map[int]int64{} // message -> node -> frame -> first arrival
	sc := bufio.NewScanner(zr)
	for sc.Scan() {
		fs := strings.Split(sc.Text(), ",")
		if len(fs) < 4 {
			continue
		}
		m, err := strconv.Atoi(fs[1])
		if err != nil {
			continue
		}
		switch {
		case fs[0] == "H" && len(fs) >= 6:
			pub[m] = fs[2]
			need[m], _ = strconv.Atoi(fs[5])
		case fs[0] == "M":
			if mesh[m] == nil {
				mesh[m] = map[string][]string{}
			}
			mesh[m][fs[2]] = append(mesh[m][fs[2]], fs[3])
			mesh[m][fs[3]] = append(mesh[m][fs[3]], fs[2])
		case fs[0] == "E" && len(fs) >= 7 && (fs[6] == "push" || fs[6] == "answer"):
			idx, err1 := strconv.Atoi(fs[4])
			ns, err2 := strconv.ParseInt(fs[5], 10, 64)
			if err1 != nil || err2 != nil || idx < 0 {
				continue
			}
			if first[m] == nil {
				first[m] = map[string]map[int]int64{}
			}
			if first[m][fs[2]] == nil {
				first[m][fs[2]] = map[int]int64{}
			}
			if t0, seen := first[m][fs[2]][idx]; !seen || ns < t0 {
				first[m][fs[2]][idx] = ns
			}
		}
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	out := map[int][]replayFrame{}
	for m, p := range pub {
		type cand struct {
			kth int64
			fr  []replayFrame
		}
		var cs []cand
		for _, o := range mesh[m][p] {
			got := first[m][o]
			if len(got) < need[m] {
				continue
			}
			fr := make([]replayFrame, 0, len(got))
			for idx, ns := range got {
				fr = append(fr, replayFrame{at: time.Duration(ns), idx: idx})
			}
			sort.Slice(fr, func(a, b int) bool { return fr[a].at < fr[b].at || fr[a].at == fr[b].at && fr[a].idx < fr[b].idx })
			t0 := fr[0].at
			for i := range fr {
				fr[i].at -= t0
			}
			cs = append(cs, cand{kth: int64(fr[need[m]-1].at), fr: fr})
		}
		if len(cs) == 0 {
			return nil, fmt.Errorf("message %d: no hop-1 relay reached need", m)
		}
		sort.Slice(cs, func(a, b int) bool { return cs[a].kth < cs[b].kth })
		out[m] = cs[len(cs)/2].fr
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no H rows")
	}
	return out, nil
}

func TestAdmitSchedules(t *testing.T) {
	t.Run("rate spreads K frames evenly over distinct indices", func(t *testing.T) {
		fr := rateSchedule(3, 64, 128, 450*time.Millisecond)
		seen := map[int]bool{}
		for i, f := range fr {
			if f.idx < 0 || f.idx >= 128 || seen[f.idx] {
				t.Fatalf("frame %d: index %d repeated or out of range", i, f.idx)
			}
			seen[f.idx] = true
		}
		if fr[0].at != 0 || fr[63].at != 450*time.Millisecond {
			t.Fatalf("first at %v, K-th at %v, want 0 and 450ms", fr[0].at, fr[63].at)
		}
	})
	t.Run("relay inflow takes the median hop-1 relay from its first arrival", func(t *testing.T) {
		path := t.TempDir() + "/tr.csv.gz"
		f, err := os.Create(path)
		if err != nil {
			t.Fatal(err)
		}
		zw := gzip.NewWriter(f)
		// Publisher 0, need 2; hop-1 relays 1, 2, 3 reach need at 30, 50, 90 ms; node 4 is not a mesh peer.
		rows := []string{"H,1,0,5,4,2", "M,1,0,1", "M,1,2,0", "M,1,0,3",
			"E,1,1,0,0,10000000,push,1", "E,1,1,0,1,30000000,push,1",
			"E,1,2,0,2,20000000,push,1", "E,1,2,3,3,50000000,answer,1", "E,1,2,0,2,60000000,push,1",
			"E,1,3,0,0,40000000,push,1", "E,1,3,0,1,90000000,push,1",
			"E,1,4,0,0,1000000,push,1", "E,1,4,0,1,2000000,push,1", "E,1,2,0,-1,1000000,have,4"}
		for _, r := range rows {
			if _, err := zw.Write([]byte(r + "\n")); err != nil {
				t.Fatal(err)
			}
		}
		if err := zw.Close(); err != nil {
			t.Fatal(err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
		at, err := relayInflow(path)
		if err != nil {
			t.Fatal(err)
		}
		fr := at[1]
		if len(fr) != 2 || fr[0].idx != 2 || fr[0].at != 0 || fr[1].idx != 3 || fr[1].at != 30*time.Millisecond {
			t.Fatalf("got %+v, want relay 2's frames 2 at 0 and 3 at 30ms", fr)
		}
	})
}
