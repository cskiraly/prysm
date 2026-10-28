package segmentintegrationtest

// Family B's routes (SEGMENT_SRC_ROUTES=<k>): the source
// reaches k publishers over routes on which no node knows it is next to the origin. Each message
// draws k node-disjoint routes afresh; every route node is drawn uniformly among all nodes but the
// source. Its connections are not left out: a rule that never lets a publisher be the source's gossip
// neighbour names it as the one node adjacent to no publisher, in ~20 messages at k = 2;
// uniform, a publisher is its neighbour at the base rate.
//
// SEGMENT_SRC_ROUTES_Q=<q> is Crowds' coin: after each hop the route continues to another node with
// probability q, else that node publishes; 0 (the default) is one hop.
//
// SEGMENT_SRC_ROUTES_WITHHOLD=<r> makes route r's publisher (1-based) take the key and publish
// nothing; it still holds the payload, so it completes, and its router relays as any node's.
//
// SEGMENT_SRC_ROUTES_FANOUT=<m> fans each route out: its last node publishes nothing and passes the
// key one hop on to m slice publishers, drawn per message uniformly among the nodes on no route.
// The frames are dealt round-robin over all k·m slice publishers, each pre-positioned with its own,
// so each frame is published once and the payload enters the network at k·m points. The source's
// first hops stay k, which keeps Crowds' predecessor attack where it is. A slice publisher holds
// only its frames, so it is waited for like any node; a withholding route's publishers get no key.
//
// The copies are pre-positioned: every route node already holds the payload's ciphertext when the
// message starts, its bytes moved seconds earlier and not simulated. At the start the source sends a
// 32-byte key along each route over standing dedicated connections outside the gossip graph,
// modelled as each pair's one-way delay plus routeKeyHop, not as libp2p connections, which would
// join the gossip graph. The route's last node decrypts and publishes the whole payload through the
// source's batch path with the source's knobs, and completes when it publishes. The source's router
// never learns the message, so it relays it like any node, and is not waited for.

import (
	"context"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// routeKeyHop is a key's handling at each route node: receive, look up the route, send on.
const routeKeyHop = 2 * time.Millisecond

// routeMaxHops bounds a route; at q = 1/2 a longer one has probability 2^-15.
const routeMaxHops = 16

// routeState is a cell's route rule and the message in flight; its mutex guards the latter.
type routeState struct {
	k        int
	q        float64
	withhold int
	fanout   int
	seed     uint64
	geo      cellGeo

	mu  sync.Mutex
	msg int
	pub int
	// routes is each route's nodes from the first hop to the publisher; keyAt when the key reached
	// each, published when the publisher's publish returned, from the message's start.
	routes    [][]int
	keyAt     [][]time.Duration
	published []time.Duration
	// fan is each route's slice publishers under SEGMENT_SRC_ROUTES_FANOUT; fanKey and fanPub when
	// the key reached each and when its publish returned.
	fan    [][]int
	fanKey [][]time.Duration
	fanPub [][]time.Duration
	signal func(int, time.Duration)
	start  time.Time
	err    error
}

// routesFromEnv reads SEGMENT_SRC_ROUTES and its companions; nil when off.
func routesFromEnv(t *testing.T, seed uint64, geo cellGeo) *routeState {
	t.Helper()
	v := os.Getenv("SEGMENT_SRC_ROUTES")
	if v == "" {
		for _, k := range []string{"SEGMENT_SRC_ROUTES_Q", "SEGMENT_SRC_ROUTES_WITHHOLD", "SEGMENT_SRC_ROUTES_FANOUT"} {
			if os.Getenv(k) != "" {
				t.Fatalf("%s needs SEGMENT_SRC_ROUTES", k)
			}
		}
		return nil
	}
	if os.Getenv("SEGMENT_SRC_STEM") != "" {
		t.Fatal("SEGMENT_SRC_ROUTES and SEGMENT_SRC_STEM are exclusive")
	}
	k, err := strconv.Atoi(v)
	if err != nil || k < 1 {
		t.Fatalf("bad SEGMENT_SRC_ROUTES %q (want a route count >= 1)", v)
	}
	s := &routeState{k: k, seed: seed, geo: geo}
	if v := os.Getenv("SEGMENT_SRC_ROUTES_Q"); v != "" {
		q, err := strconv.ParseFloat(v, 64)
		if err != nil || q < 0 || q >= 1 {
			t.Fatalf("bad SEGMENT_SRC_ROUTES_Q %q (want 0 <= q < 1)", v)
		}
		s.q = q
	}
	if v := os.Getenv("SEGMENT_SRC_ROUTES_WITHHOLD"); v != "" {
		w, err := strconv.Atoi(v)
		if err != nil || w < 1 || w > k {
			t.Fatalf("bad SEGMENT_SRC_ROUTES_WITHHOLD %q (want a route from 1 to %d)", v, k)
		}
		s.withhold = w
	}
	if v := os.Getenv("SEGMENT_SRC_ROUTES_FANOUT"); v != "" {
		m, err := strconv.Atoi(v)
		if err != nil || m < 1 {
			t.Fatalf("bad SEGMENT_SRC_ROUTES_FANOUT %q (want slice publishers per route >= 1)", v)
		}
		s.fanout = m
	}
	return s
}

// next draws message m's routes from publisher pub.
func (s *routeState) next(t *testing.T, nw *simNetwork, m, pub int) {
	t.Helper()
	var pool []int
	for j := range nw.Hosts {
		if j != pub {
			pool = append(pool, j)
		}
	}
	key := s.seed ^ uint64(pub+1)<<24 ^ uint64(m)<<44 ^ 0x524f555445
	routes, ok := drawRoutes(key, pool, s.k, s.q)
	if !ok {
		t.Fatalf("SEGMENT_SRC_ROUTES: %d candidates are too few for %d routes from node %d", len(pool), s.k, pub)
	}
	var fan [][]int
	if s.fanout > 0 {
		// The slice publishers: m per route, a zero-coin draw from the nodes on no route.
		rest := slices.DeleteFunc(slices.Clone(pool), func(j int) bool {
			for _, r := range routes {
				if slices.Contains(r, j) {
					return true
				}
			}
			return false
		})
		ones, ok := drawRoutes(key^0x46414e, rest, s.k*s.fanout, 0)
		if !ok {
			t.Fatalf("SEGMENT_SRC_ROUTES_FANOUT: %d candidates are too few for %d slice publishers", len(rest), s.k*s.fanout)
		}
		fan = make([][]int, s.k)
		for i, o := range ones {
			fan[i/s.fanout] = append(fan[i/s.fanout], o[0])
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.msg, s.pub, s.routes, s.fan = m, pub, routes, fan
	s.keyAt = make([][]time.Duration, s.k)
	s.published = make([]time.Duration, s.k)
	s.fanKey, s.fanPub = make([][]time.Duration, s.k), make([][]time.Duration, s.k)
	for r := range fan {
		s.fanKey[r], s.fanPub[r] = make([]time.Duration, s.fanout), make([]time.Duration, s.fanout)
	}
	s.signal, s.err = nil, nil
}

// drawRoutes draws k node-disjoint routes from pool under key: each a first hop, then another with
// probability q after each hop, up to routeMaxHops; false when pool runs out.
func drawRoutes(key uint64, pool []int, k int, q float64) ([][]int, bool) {
	pool = slices.Clone(pool)
	draws := uint64(0)
	rnd := func() uint64 { draws++; return mix64(key ^ draws*0x9e3779b97f4a7c15) }
	routes := make([][]int, k)
	for r := range routes {
		for len(routes[r]) == 0 || (len(routes[r]) < routeMaxHops && float64(rnd()>>11)/(1<<53) < q) {
			if len(pool) == 0 {
				return nil, false
			}
			i := int(rnd() % uint64(len(pool)))
			routes[r] = append(routes[r], pool[i])
			pool = slices.Delete(pool, i, i+1)
		}
	}
	return routes, true
}

// publishers are the routes' last nodes, the withholder's included: each holds the payload. Under
// the fan-out none does, and every node is waited for.
func (s *routeState) publishers() []int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fanout > 0 {
		return nil
	}
	out := make([]int, 0, len(s.routes))
	for _, r := range s.routes {
		out = append(out, r[len(r)-1])
	}
	return out
}

// armSignal hands the routes the driver's completion signal and the message's start.
func (s *routeState) armSignal(signal func(int, time.Duration), start time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.signal, s.start = signal, start
}

// sliceOf is slice publisher j's frames of n under the fan-out: every (k·m)-th from j.
func (s *routeState) sliceOf(j, n int) []int {
	var idx []int
	for f := j; f < n; f += s.k * s.fanout {
		idx = append(idx, f)
	}
	return idx
}

// release sends the key along every route; each publisher publishes through publish on its arrival,
// all n frames (idx nil) or, under the fan-out, each slice publisher its slice. It returns at once;
// a publish error is the message's error, reported by lines.
func (s *routeState) release(ctx context.Context, n int, publish func(node int, idx []int) error) error {
	s.mu.Lock()
	routes, pub, fan := slices.Clone(s.routes), s.pub, slices.Clone(s.fan)
	s.mu.Unlock()
	fail := func(err error) {
		s.mu.Lock()
		if s.err == nil {
			s.err = err
		}
		s.mu.Unlock()
	}
	for r, route := range routes {
		go func() {
			prev := pub
			for _, node := range route {
				select {
				case <-time.After(s.geo.oneWay(prev, node) + routeKeyHop):
				case <-ctx.Done():
					return
				}
				s.mu.Lock()
				s.keyAt[r] = append(s.keyAt[r], time.Since(s.start))
				s.mu.Unlock()
				prev = node
			}
			s.mu.Lock()
			signal, at := s.signal, time.Since(s.start)
			s.mu.Unlock()
			if fan != nil {
				if s.withhold == r+1 {
					return
				}
				for i, node := range fan[r] {
					go func() {
						select {
						case <-time.After(s.geo.oneWay(prev, node) + routeKeyHop):
						case <-ctx.Done():
							return
						}
						s.mu.Lock()
						s.fanKey[r][i] = time.Since(s.start)
						s.mu.Unlock()
						if err := publish(node, s.sliceOf(r*s.fanout+i, n)); err != nil {
							fail(fmt.Errorf("route %d, slice publisher %d: publish: %w", r+1, node, err))
							return
						}
						s.mu.Lock()
						s.fanPub[r][i] = time.Since(s.start)
						s.mu.Unlock()
					}()
				}
				return
			}
			if s.withhold != r+1 {
				if err := publish(prev, nil); err != nil {
					fail(fmt.Errorf("route %d, node %d: publish: %w", r+1, prev, err))
					return
				}
				s.mu.Lock()
				s.published[r] = time.Since(s.start)
				s.mu.Unlock()
			}
			if signal != nil {
				signal(prev, at)
			}
		}()
	}
	return nil
}

// lines reports the message's routes: each hop, when the key reached it, and the publisher's first
// hop and upload; the error, if a publish failed.
func (s *routeState) lines(v *q6Variant, st diffusionStats) ([]string, error) {
	s.mu.Lock()
	routes, keyAt, published, err := s.routes, s.keyAt, s.published, s.err
	fan, fanKey, fanPub := s.fan, s.fanKey, s.fanPub
	s.mu.Unlock()
	ms := func(d time.Duration) string {
		if d == 0 {
			return "never"
		}
		return fmt.Sprintf("+%.1fms", float64(d.Microseconds())/1000)
	}
	var out []string
	for r, route := range routes {
		hops := make([]string, len(route))
		for j, node := range route {
			at := "never"
			if j < len(keyAt[r]) {
				at = ms(keyAt[r][j])
			}
			hops[j] = fmt.Sprintf("node %d (key %s)", node, at)
		}
		mode := fmt.Sprintf("q %.2f, pre-positioned, key over dedicated links", s.q)
		if s.withhold == r+1 {
			mode += ", withholding: publishes nothing"
		}
		label := fmt.Sprintf("route%d", r+1)
		if fan != nil {
			if s.withhold == r+1 {
				mode += ", its slice publishers get no key"
			}
			fs := make([]string, len(fan[r]))
			for i, node := range fan[r] {
				fs[i] = fmt.Sprintf("node %d (key %s, publish returned %s)", node, ms(fanKey[r][i]), ms(fanPub[r][i]))
			}
			out = append(out, fmt.Sprintf("%s (%s): %d hop(s) from the publisher %d: %s; fan-out to %d slice publishers: %s",
				label, mode, len(route), st.pub, strings.Join(hops, " -> "), len(fan[r]), strings.Join(fs, ", ")))
			continue
		}
		pubNode := route[len(route)-1]
		out = append(out, fmt.Sprintf("%s (%s): %d hop(s) from the publisher %d: %s; publish returned %s",
			label, mode, len(route), st.pub, strings.Join(hops, " -> "), ms(published[r])))
		if st.tracers != nil {
			out = append(out, label+" "+firstHopLine(v.arm, st, v.nw.Hosts[pubNode].ID(), label))
		}
		out = append(out, uploadLine(v.nw, label, []int{pubNode}))
	}
	return out, err
}

func TestDrawRoutes(t *testing.T) {
	pool := make([]int, 400)
	for i := range pool {
		pool[i] = i + 100
	}
	t.Run("disjoint and from the pool", func(t *testing.T) {
		for key := range uint64(200) {
			routes, ok := drawRoutes(key, pool, 2, 0.5)
			if !ok || len(routes) != 2 {
				t.Fatalf("key %d: %v, %v", key, routes, ok)
			}
			seen := map[int]bool{}
			for _, r := range routes {
				for _, n := range r {
					if n < 100 || n >= 500 || seen[n] {
						t.Fatalf("key %d: node %d outside the pool or on two routes: %v", key, n, routes)
					}
					seen[n] = true
				}
			}
		}
	})
	t.Run("lengths are one plus geometric", func(t *testing.T) {
		for _, q := range []float64{0, 0.5, 2.0 / 3} {
			total, n := 0, 4000
			for key := range uint64(n) {
				routes, _ := drawRoutes(key, pool, 1, q)
				total += len(routes[0])
			}
			if mean, want := float64(total)/float64(n), 1/(1-q); mean < want*0.95 || mean > want*1.05 {
				t.Fatalf("q %.2f: mean length %.3f, want %.3f within 5%%", q, mean, want)
			}
		}
	})
	t.Run("fan-out slices partition the frames", func(t *testing.T) {
		s := &routeState{k: 2, fanout: 16}
		seen := map[int]int{}
		for j := range 32 {
			for _, f := range s.sliceOf(j, 128) {
				seen[f]++
			}
		}
		if len(seen) != 128 {
			t.Fatalf("%d distinct frames dealt, want 128", len(seen))
		}
		for f, c := range seen {
			if c != 1 {
				t.Fatalf("frame %d dealt %d times", f, c)
			}
		}
		if got := len(s.sliceOf(0, 128)); got != 4 {
			t.Fatalf("slice 0 has %d frames, want 4", got)
		}
	})
	t.Run("a small pool runs out", func(t *testing.T) {
		if _, ok := drawRoutes(1, pool[:1], 2, 0); ok {
			t.Fatal("two disjoint routes from one node")
		}
	})
}
