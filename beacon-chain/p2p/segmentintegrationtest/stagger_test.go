package segmentintegrationtest

// Staggered first contact (SEGMENT_SRC_STAGGER=<k>:<ms>): for <ms> from
// each message's start the publisher sends data only to k of its mesh peers at publish, drawn afresh
// per message; the others get their first data from those peers or anyone else, so the source is the
// first data sender at about k of its mesh, as a relay is at part of its own (the root pattern).
// Its announcements are untouched: it names its frames to
// every peer as before, pushes only to the chosen, and answers a held peer's ask when the window ends,
// as over a congested link (fork WithOriginDataHold). Meant with SEGMENT_SRC_ADMIT, the virtual
// receiver, so the late peers do not meet a complete node. Exclusive with SEGMENT_SRC_STEM and
// SEGMENT_SRC_ROUTES, whose publishers are not the source, and with SEGMENT_PAIR.

import (
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
)

// staggerState is a cell's stagger rule and the message in flight; the hold's counters are the
// publisher's this message.
type staggerState struct {
	k               int
	w               time.Duration
	seed            uint64
	hosts           []peer.ID
	pushes, answers atomic.Int64
	cur             atomic.Pointer[staggerMsg]
}

// staggerMsg is one message's hold: its publisher, the publisher's mesh at publish and the chosen
// among it by node index, the chosen by id, the message's ids and the window's end.
type staggerMsg struct {
	msg, pub     int
	mesh, chosen []int
	unheld       map[peer.ID]bool
	ids          map[string]int
	until        time.Time
}

// staggerFromEnv reads SEGMENT_SRC_STAGGER; nil when off.
func staggerFromEnv(t *testing.T) *staggerState {
	t.Helper()
	v := os.Getenv("SEGMENT_SRC_STAGGER")
	if v == "" {
		return nil
	}
	for _, k := range []string{"SEGMENT_SRC_STEM", "SEGMENT_SRC_ROUTES"} {
		if os.Getenv(k) != "" {
			t.Fatalf("SEGMENT_SRC_STAGGER and %s are exclusive", k)
		}
	}
	ks, ms, ok := strings.Cut(v, ":")
	k, err1 := strconv.Atoi(ks)
	d, err2 := strconv.ParseFloat(ms, 64)
	if !ok || err1 != nil || err2 != nil || k < 1 || d <= 0 {
		t.Fatalf("bad SEGMENT_SRC_STAGGER %q (want <k>:<ms>, k >= 1 and ms above 0)", v)
	}
	return &staggerState{k: k, w: time.Duration(d * float64(time.Millisecond))}
}

// setup records the cell's graph seed and peer ids.
func (s *staggerState) setup(seed uint64, nw *simNetwork) {
	s.seed = seed
	s.hosts = make([]peer.ID, len(nw.Hosts))
	for i, h := range nw.Hosts {
		s.hosts[i] = h.ID()
	}
}

// opt is node i's hold: the current message's frames, from the peers not chosen, until the window
// ends, when i publishes it.
func (s *staggerState) opt(i int) pubsub.Option {
	return pubsub.WithOriginDataHold(func(p peer.ID, mid string) (time.Time, bool) { return s.held(i, p, mid) }, &s.pushes, &s.answers)
}

// held is node i's predicate; any node's event loop.
func (s *staggerState) held(i int, p peer.ID, mid string) (time.Time, bool) {
	m := s.cur.Load()
	if m == nil || m.pub != i || m.unheld[p] {
		return time.Time{}, false
	}
	if _, ok := m.ids[mid]; !ok {
		return time.Time{}, false
	}
	return m.until, true
}

// arm draws message m's chosen among publisher pub's mesh at publish and opens its window at start.
func (s *staggerState) arm(m, pub int, mesh []peer.ID, arm wireArm, start time.Time) {
	idx := make(map[peer.ID]int, len(s.hosts))
	for i, h := range s.hosts {
		idx[h] = i
	}
	var nodes []int
	for _, p := range mesh {
		if j, ok := idx[p]; ok {
			nodes = append(nodes, j)
		}
	}
	slices.Sort(nodes)
	chosen := drawStagger(s.seed^uint64(pub+1)<<24^uint64(m)<<44^0x5354414747, nodes, s.k)
	unheld := make(map[peer.ID]bool, len(chosen))
	for _, j := range chosen {
		unheld[s.hosts[j]] = true
	}
	ids, _ := spyMessageIDs(arm)
	s.pushes.Store(0)
	s.answers.Store(0)
	s.cur.Store(&staggerMsg{msg: m, pub: pub, mesh: nodes, chosen: chosen, unheld: unheld, ids: ids, until: start.Add(s.w)})
}

// drawStagger draws k of nodes under key, returned in ascending order; all of them when k covers them.
func drawStagger(key uint64, nodes []int, k int) []int {
	pool := slices.Clone(nodes)
	if k >= len(pool) {
		return pool
	}
	for i := range k {
		j := i + int(mix64(key^uint64(i+1)*0x9e3779b97f4a7c15)%uint64(len(pool)-i))
		pool[i], pool[j] = pool[j], pool[i]
	}
	out := pool[:k]
	slices.Sort(out)
	return out
}

// lines reports the message's hold: the chosen, the window and the hold's counters, then when the
// publisher's data first reached the chosen and the other mesh peers at publish, from the tracers.
func (s *staggerState) lines(v *q6Variant, st diffusionStats) []string {
	m := s.cur.Load()
	if m == nil {
		return []string{"stagger: not armed"}
	}
	out := []string{fmt.Sprintf("stagger: P = nodes %v (%d of the publisher %d's %d mesh peers at publish %v), W = %v; %d pushes withheld, %d answers deferred",
		m.chosen, len(m.chosen), m.pub, len(m.mesh), m.mesh, s.w, s.pushes.Load(), s.answers.Load())}
	if st.tracers == nil {
		return out
	}
	pubID := s.hosts[m.pub]
	first := func(j int) (time.Duration, bool) {
		tr := st.tracers[j]
		tr.Mu.Lock()
		defer tr.Mu.Unlock()
		var at time.Duration
		ok := false
		for _, evs := range [][]recvEvent{tr.Recvs, tr.Dups} {
			for _, e := range evs {
				if _, known := v.arm.known[e.What]; !known || e.From != pubID {
					continue
				}
				if d := e.At.Sub(st.startAt); !ok || d < at {
					at, ok = d, true
				}
			}
		}
		return at, ok
	}
	ms := func(d time.Duration) string { return fmt.Sprintf("+%.1fms", float64(d.Microseconds())/1000) }
	var chosenAt, restAt []string
	early, never := 0, 0
	for _, j := range m.mesh {
		at, ok := first(j)
		label := "never"
		if ok {
			label = ms(at)
		}
		if slices.Contains(m.chosen, j) {
			chosenAt = append(chosenAt, fmt.Sprintf("%d %s", j, label))
			continue
		}
		restAt = append(restAt, fmt.Sprintf("%d %s", j, label))
		switch {
		case !ok:
			never++
		case at < s.w:
			early++
		}
	}
	out = append(out, fmt.Sprintf("stagger first data from the publisher: P [%s]; the other mesh peers [%s]; %d of them before W, %d never",
		strings.Join(chosenAt, ", "), strings.Join(restAt, ", "), early, never))
	return out
}

func TestStaggerDraw(t *testing.T) {
	nodes := []int{3, 9, 14, 27, 40, 41, 77, 120}
	t.Run("k distinct mesh peers, ascending, fixed by the key", func(t *testing.T) {
		for key := range uint64(200) {
			got := drawStagger(key, nodes, 3)
			if len(got) != 3 || !slices.IsSorted(got) || !slices.Equal(got, drawStagger(key, nodes, 3)) {
				t.Fatalf("key %d: %v", key, got)
			}
			for i, j := range got {
				if !slices.Contains(nodes, j) || (i > 0 && got[i-1] == j) {
					t.Fatalf("key %d: %v repeats or leaves the mesh", key, got)
				}
			}
		}
		if !slices.Equal(drawStagger(1, nodes, 9), nodes) {
			t.Fatal("k above the mesh size does not take the whole mesh")
		}
	})
	t.Run("every mesh peer is chosen at k over the mesh size", func(t *testing.T) {
		count := map[int]int{}
		const keys = 8000
		for key := range uint64(keys) {
			for _, j := range drawStagger(mix64(key), nodes, 2) {
				count[j]++
			}
		}
		for _, j := range nodes {
			if want := keys * 2 / len(nodes); count[j] < want*9/10 || count[j] > want*11/10 {
				t.Fatalf("node %d chosen %d times, want %d within 10%%", j, count[j], want)
			}
		}
	})
	t.Run("the hold is the publisher's, on its message, from the peers not chosen, until W", func(t *testing.T) {
		s := &staggerState{k: 1, w: 150 * time.Millisecond, seed: 7, hosts: []peer.ID{"p0", "p1", "p2", "p3"}}
		arm := wireArm{msgs: [][]byte{[]byte("frame a"), []byte("frame b")}}
		start := time.Now()
		s.arm(1, 0, []peer.ID{"p2", "p1", "p3"}, arm, start)
		m := s.cur.Load()
		if len(m.chosen) != 1 || !slices.Equal(m.mesh, []int{1, 2, 3}) {
			t.Fatalf("mesh %v, chosen %v", m.mesh, m.chosen)
		}
		ids := segmentIDs(arm)
		var other peer.ID
		for i, p := range s.hosts[1:] {
			i++ // the publisher, node 0, is never its own destination
			until, held := s.held(0, p, ids[1])
			want := i != m.chosen[0]
			if held != want || (held && !until.Equal(start.Add(150*time.Millisecond))) {
				t.Fatalf("peer %d: held %v until %v, want %v until W", i, held, until, want)
			}
			if held {
				other = p
			}
		}
		if _, held := s.held(1, other, ids[0]); held {
			t.Fatal("held at a node that is not the publisher")
		}
		if _, held := s.held(0, other, "another message"); held {
			t.Fatal("held an id outside the message")
		}
	})
}
