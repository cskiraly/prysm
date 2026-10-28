package segmentintegrationtest

// The frame pool around a receiver (SEGMENT_OVERLAP=1, Q135b). A lower-rate code (idea #10) was
// to halve duplicates by doubling the coded space, and at r = 2 it did not (Q134). The candidate:
// a pusher sends only frames it holds, and a receiver's feeders hold largely the same frames
// from shared upstream, so the pool a receiver draws on is their union, which need not grow
// with N. Read from the tracers after each message: for each receiver, at the moment it holds
// K/2 distinct frames, the union of what its feeders (every peer that sent it a frame of this
// message) held then, with and without the publisher; the chance a uniform pick from that pool
// is one it already has; and the mean pairwise Jaccard of the feeders' holdings. Network-wide,
// how many of the N frames reached any relay, and how many reached a tenth of the nodes.

import (
	"fmt"
	"math/bits"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

// frameSet is a set of frame indices, N <= 256 (the codec's cap).
type frameSet [4]uint64

func (f *frameSet) add(i int) { f[i>>6] |= 1 << (uint(i) & 63) } // lint:ignore uintcast -- a frame index, < 256.
func (f *frameSet) union(g frameSet) {
	f[0], f[1], f[2], f[3] = f[0]|g[0], f[1]|g[1], f[2]|g[2], f[3]|g[3]
}
func (f frameSet) count() int {
	return bits.OnesCount64(f[0]) + bits.OnesCount64(f[1]) + bits.OnesCount64(f[2]) + bits.OnesCount64(f[3])
}
func (f frameSet) and(g frameSet) frameSet {
	return frameSet{f[0] & g[0], f[1] & g[1], f[2] & g[2], f[3] & g[3]}
}

type heldAt struct {
	at  time.Time
	idx int
}

func overlapOn() bool { return os.Getenv("SEGMENT_OVERLAP") != "" }

// overlapLine reads the message that started at s.startAt.
func overlapLine(arm wireArm, s diffusionStats, hosts []peer.ID) string {
	n, N, K := len(s.tracers), len(arm.msgs), arm.completeAt()
	if N == 0 || N > 256 || K < 2 {
		return "overlap: needs 2 <= K and N <= 256"
	}
	idx := make(map[peer.ID]int, n)
	for i, h := range hosts {
		idx[h] = i
	}
	firsts := make([][]heldAt, n)      // each node's first copies, in arrival order
	feeders := make([]map[int]bool, n) // who sent each node a frame of this message
	for i, tr := range s.tracers {
		feeders[i] = map[int]bool{}
		tr.Mu.Lock()
		for k, evs := range [][]recvEvent{tr.Recvs, tr.Dups} {
			for _, e := range evs {
				fi, ok := arm.known[e.What]
				if !ok || e.At.Before(s.startAt) {
					continue
				}
				if j, ok := idx[e.From]; ok {
					feeders[i][j] = true
				}
				if k == 0 {
					firsts[i] = append(firsts[i], heldAt{e.At, fi})
				}
			}
		}
		tr.Mu.Unlock()
		sort.Slice(firsts[i], func(a, b int) bool { return firsts[i][a].at.Before(firsts[i][b].at) })
	}
	var all frameSet
	for i := range N {
		all.add(i)
	}
	holding := func(j int, t time.Time) frameSet {
		if j == s.pub {
			return all
		}
		var f frameSet
		for _, h := range firsts[j] {
			if h.at.After(t) {
				break
			}
			f.add(h.idx)
		}
		return f
	}
	var pools, poolsNoPub, dupOdds, jacc, nFeed []float64
	fedByPub := 0
	for i := range n {
		if i == s.pub || len(firsts[i]) < K/2 {
			continue
		}
		t := firsts[i][K/2-1].at
		mine := holding(i, t)
		var pool, poolNP frameSet
		var sets []frameSet
		for j := range feeders[i] {
			h := holding(j, t)
			pool.union(h)
			if j == s.pub {
				fedByPub++
				continue
			}
			poolNP.union(h)
			sets = append(sets, h)
		}
		if pool.count() == 0 {
			continue
		}
		pools = append(pools, float64(pool.count()))
		poolsNoPub = append(poolsNoPub, float64(poolNP.count()))
		dupOdds = append(dupOdds, float64(pool.and(mine).count())/float64(pool.count()))
		nFeed = append(nFeed, float64(len(feeders[i])))
		var js []float64
		for a := 0; a < len(sets); a++ {
			for b := a + 1; b < len(sets); b++ {
				u := sets[a]
				u.union(sets[b])
				if c := u.count(); c > 0 {
					js = append(js, float64(sets[a].and(sets[b]).count())/float64(c))
				}
			}
		}
		if len(js) > 0 {
			jacc = append(jacc, meanOf(js))
		}
	}
	reach := make([]int, N)
	for i := range n {
		if i == s.pub {
			continue
		}
		for _, h := range firsts[i] {
			reach[h.idx]++
		}
	}
	anyRelay, tenth := 0, 0
	for _, c := range reach {
		if c > 0 {
			anyRelay++
		}
		if c*10 >= n {
			tenth++
		}
	}
	if len(pools) == 0 {
		return fmt.Sprintf("overlap: no receiver reached K/2 = %d", K/2)
	}
	q := func(v []float64, p float64) float64 {
		if len(v) == 0 {
			return 0
		}
		s := append([]float64(nil), v...)
		sort.Float64s(s)
		return s[int(p*float64(len(s)-1))]
	}
	return fmt.Sprintf("overlap (N %d, K %d) at K/2: feeders p50 %.0f; pool p10 %.0f p50 %.0f p90 %.0f, without the publisher p50 %.0f; a uniform pick from the pool is held p50 %.2f p90 %.2f; feeders' pairwise Jaccard p50 %.2f; %d of %d receivers fed by the publisher; frames reaching a relay %d, a tenth of the nodes %d",
		N, K, q(nFeed, 0.5), q(pools, 0.1), q(pools, 0.5), q(pools, 0.9), q(poolsNoPub, 0.5), q(dupOdds, 0.5), q(dupOdds, 0.9), q(jacc, 0.5), fedByPub, len(pools), anyRelay, tenth)
}

func meanOf(v []float64) float64 {
	s := 0.0
	for _, x := range v {
		s += x
	}
	return s / float64(len(v))
}

func TestOverlapLine(t *testing.T) {
	// Publisher 0 holds all 8 frames; K = 4. Node 1 gets 0..3 from the publisher; node 2 gets 0, 1
	// from node 1 and 4, 5 from the publisher.
	start := time.Unix(1000, 0)
	at := func(ms int) time.Time { return start.Add(time.Duration(ms) * time.Millisecond) }
	arm := wireArm{msgs: make([][]byte, 8), known: map[digest]int{}, need: 4}
	var ds [8]digest
	for i := range ds {
		ds[i] = digestOf([]byte{byte(i)})
		arm.known[ds[i]] = i
	}
	hosts := []peer.ID{"p0", "p1", "p2"}
	trs := []*recordingTracer{{}, {}, {}}
	for i := range 4 {
		trs[1].Recvs = append(trs[1].Recvs, recvEvent{At: at(10 + i), From: "p0", What: ds[i]})
	}
	trs[2].Recvs = append(trs[2].Recvs,
		recvEvent{At: at(20), From: "p1", What: ds[0]}, recvEvent{At: at(21), From: "p1", What: ds[1]},
		recvEvent{At: at(30), From: "p0", What: ds[4]}, recvEvent{At: at(31), From: "p0", What: ds[5]})
	s := diffusionStats{tracers: trs, pub: 0, startAt: start}
	got := overlapLine(arm, s, hosts)
	// Node 1 at its 2nd frame (t 11): fed by the publisher only, pool 8, 2 of them held.
	// Node 2 at its 2nd frame (t 21): fed by p1 (holding 0..3) and the publisher: pool 8, 4 without it.
	for _, want := range []string{"feeders p50 1", "pool p10 8 p50 8 p90 8, without the publisher p50 0", "is held p50 0.25", "2 of 2 receivers fed by the publisher", "frames reaching a relay 6"} {
		if !strings.Contains(got, want) {
			t.Fatalf("overlap line %q lacks %q", got, want)
		}
	}
}
