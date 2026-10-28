package segmentintegrationtest

// SEGMENT_SRC_STEM_SELECT=uniform|capable: which of the publisher's connections card P6's stem is
// drawn from (SEGMENT_SRC_STEM=msg|epoch). The candidates narrow in levels, each the one before
// filtered further:
//
//	connections       every connection of the sender
//	outbound          connections the sender dialled (libp2p's direction), which an attacker
//	                  cannot place by dialling in; the harness dials each edge from its lower
//	                  index, so node 0, the source, dialled every one of its connections
//	outside region R  not in the sender's own geo region (SEGMENT_LATENCY_MODEL=geo only; lifted
//	                  by SEGMENT_SRC_STEM_REGION=any)
//	capable           capable only: an uplink of at least SEGMENT_SRC_STEM_MIN_MBPS (default 25;
//	                  the uplink meshLinks built stands in for an observed throughput) and a
//	                  one-way delay of at most SEGMENT_SRC_STEM_MAX_OWD_MS (default 60; the QUIC
//	                  min RTT over two, as a node measures it)
//	outside its mesh  as without the policy: a stem in the mesh pushes the frames straight back
//
// The draw goes down the levels: a node drawn at one level stands while it survives the next,
// and a draw among the next level's nodes replaces it when it does not. The result is uniform
// over the last level; a policy level that empties falls back to the one before, and without a
// policy it is the draw before the policy existed, hashed from the graph seed and the sender (and
// the message, per message). The mesh comes last because it is the one level a run redraws (host
// identities order the grafts): the policy's pick is the same in every arm at a seed unless it
// lands in that run's mesh. Per epoch the stem is kept while it stays in the last level.

import (
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/OffchainLabs/prysm/v7/testing/require"
	"github.com/libp2p/go-libp2p/core/network"
	simlibp2p "github.com/libp2p/go-libp2p/x/simlibp2p"
)

// stemPolicy is SEGMENT_SRC_STEM_SELECT.
type stemPolicy struct {
	name string
	// region keeps the stem out of the sender's region; minBps and maxOneWay are capable's floor
	// and cap.
	region    bool
	minBps    int
	maxOneWay time.Duration
}

// stemPolicyFromEnv reads the policy; nil when SEGMENT_SRC_STEM_SELECT is unset.
func stemPolicyFromEnv(t *testing.T) *stemPolicy {
	t.Helper()
	intEnv := func(name string, def int) int {
		v := os.Getenv(name)
		if v == "" {
			return def
		}
		k, err := strconv.Atoi(v)
		if err != nil || k <= 0 {
			t.Fatalf("bad %s %q (want a positive integer)", name, v)
		}
		return k
	}
	v := os.Getenv("SEGMENT_SRC_STEM_SELECT")
	capKnobs := os.Getenv("SEGMENT_SRC_STEM_MIN_MBPS") != "" || os.Getenv("SEGMENT_SRC_STEM_MAX_OWD_MS") != ""
	switch v {
	case "":
		if capKnobs || os.Getenv("SEGMENT_SRC_STEM_REGION") != "" {
			t.Fatal("SEGMENT_SRC_STEM_REGION, SEGMENT_SRC_STEM_MIN_MBPS and SEGMENT_SRC_STEM_MAX_OWD_MS need SEGMENT_SRC_STEM_SELECT")
		}
		return nil
	case "uniform", "capable":
	default:
		t.Fatalf("bad SEGMENT_SRC_STEM_SELECT %q (want uniform or capable)", v)
	}
	p := &stemPolicy{name: v, region: true}
	switch r := os.Getenv("SEGMENT_SRC_STEM_REGION"); r {
	case "":
	case "any":
		p.region = false
	default:
		t.Fatalf("bad SEGMENT_SRC_STEM_REGION %q (want any)", r)
	}
	if v == "capable" {
		p.minBps = intEnv("SEGMENT_SRC_STEM_MIN_MBPS", 25) * simlibp2p.OneMbps
		p.maxOneWay = time.Duration(intEnv("SEGMENT_SRC_STEM_MAX_OWD_MS", 60)) * time.Millisecond
	} else if capKnobs {
		t.Fatal("SEGMENT_SRC_STEM_MIN_MBPS and SEGMENT_SRC_STEM_MAX_OWD_MS need SEGMENT_SRC_STEM_SELECT=capable")
	}
	return p
}

// stemLevel is one candidate set of a stem draw and the salt of a draw among it.
type stemLevel struct {
	name  string
	salt  uint64
	nodes []int
}

// stemDraw is how a message's stem was chosen, for its report.
type stemDraw struct {
	levels []stemLevel
	// at is the level the stem was drawn at, used the last non-empty one; dropped the policy
	// level that emptied, if any; kept marks a per-epoch stem carried over, fixed a
	// SEGMENT_SRC_STEM=<node> one.
	at, used    int
	dropped     string
	kept, fixed bool
	up          int
	oneWay      time.Duration
}

// dialled reports whether node i opened a connection to node j, libp2p's outbound direction at i.
func dialled(nw *simNetwork, i, j int) bool {
	for _, c := range nw.Hosts[i].Network().ConnsToPeer(nw.Hosts[j].ID()) {
		if c.Stat().Direction == network.DirOutbound {
			return true
		}
	}
	return false
}

// stemLevels are sender from's candidate sets for its stem: base, the policy's filters, of which
// the first to empty ends the list and is returned as dropped, then those outside mesh. The mesh
// is filtered last and never skipped: when it empties the last level, the level before is
// filtered instead.
func stemLevels(nw *simNetwork, p *stemPolicy, uplinks []int, geo cellGeo, from int, base []int, mesh map[int]bool) (lv []stemLevel, dropped string) {
	filter := func(nodes []int, keep func(int) bool) []int {
		var out []int
		for _, i := range nodes {
			if keep(i) {
				out = append(out, i)
			}
		}
		return out
	}
	lv = []stemLevel{{name: "connections", nodes: base}}
	add := func(name string, salt uint64, keep func(int) bool) {
		if dropped != "" {
			return
		}
		out := filter(lv[len(lv)-1].nodes, keep)
		if len(out) == 0 {
			dropped = name
			return
		}
		lv = append(lv, stemLevel{name: name, salt: salt, nodes: out})
	}
	if p != nil {
		add("outbound", 0x6f7574626f756e64, func(i int) bool { return dialled(nw, from, i) })
		if p.region && geo.region != nil {
			add("outside region "+geo.regionName(from), 0x726567696f6e, func(i int) bool { return geo.region[i] != geo.region[from] })
		}
		if p.name == "capable" {
			add(fmt.Sprintf("capable (>= %d Mbps up, <= %v one-way)", p.minBps/simlibp2p.OneMbps, p.maxOneWay), 0x63617061626c65, func(i int) bool {
				ow := quicMinRTT(nodeHost(nw, from), nw.Hosts[i].ID()) / 2
				return i < len(uplinks) && uplinks[i] >= p.minBps && ow > 0 && ow <= p.maxOneWay
			})
		}
	}
	// The salt is the redraw's before the policy existed, which a draw over base and the mesh reproduces.
	for k := len(lv) - 1; k >= 0; k-- {
		if out := filter(lv[k].nodes, func(i int) bool { return !mesh[i] }); len(out) > 0 || k == 0 {
			if k < len(lv)-1 {
				dropped = lv[k+1].name + " outside its mesh"
			}
			return append(lv[:k+1], stemLevel{name: "outside its mesh", salt: 0x6d657368, nodes: out}), dropped
		}
	}
	return lv, dropped
}

// lastLevel is the last non-empty level, or -1.
func lastLevel(lv []stemLevel) int {
	used := -1
	for k, l := range lv {
		if len(l.nodes) == 0 {
			break
		}
		used = k
	}
	return used
}

// stemChain draws down the levels from key; it returns the stem and the level it was drawn at.
func stemChain(key uint64, lv []stemLevel) (stem, at int) {
	stem = -1
	for k := 0; k <= lastLevel(lv); k++ {
		l := lv[k].nodes
		if stem >= 0 && slices.Contains(l, stem) {
			continue
		}
		stem, at = l[mix64(key^lv[k].salt)%uint64(len(l))], k
	}
	return stem, at
}

// line reports the draw: the levels, where the stem was drawn, and what it offers the sender.
func (d stemDraw) line(label, policy string, from, stem int, geo cellGeo) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s select (%s): node %d's", label, policy, from)
	for k, l := range d.levels {
		if k > 0 {
			b.WriteString(" ->")
		}
		fmt.Fprintf(&b, " %s %d", l.name, len(l.nodes))
	}
	how := fmt.Sprintf("drawn at '%s'", d.levels[d.at].name)
	switch {
	case d.fixed:
		how = "fixed"
	case d.kept:
		how = "kept from the epoch"
	}
	fmt.Fprintf(&b, "; node %d %s", stem, how)
	if d.dropped != "" {
		fmt.Fprintf(&b, ", fell back: '%s' empty", d.dropped)
	}
	fmt.Fprintf(&b, "; uplink %g Mbps, one-way %v (QUIC min RTT / 2 at the draw)", float64(d.up)/float64(simlibp2p.OneMbps), d.oneWay.Round(100*time.Microsecond))
	if r := geo.regionName(stem); r != "" {
		fmt.Fprintf(&b, ", region %s (sender %s)", r, geo.regionName(from))
	}
	return b.String()
}

// TestStemChain is the draw's known answer: the old draw over two levels, uniform over the last
// non-empty level, and a fallback past an empty one.
func TestStemChain(t *testing.T) {
	cand := []int{3, 5, 8, 9, 11, 14, 20, 21, 30, 42}
	out := []int{5, 9, 11, 20, 21, 30, 42}
	surv := []int{9, 21, 42}
	two := []stemLevel{{nodes: cand}, {salt: 0x6d657368, nodes: out}}
	t.Run("two levels are the draw before the policy", func(t *testing.T) {
		for k := range uint64(2000) {
			key := k * 0x9e3779b97f4a7c15
			want := cand[mix64(key)%uint64(len(cand))]
			if !slices.Contains(out, want) {
				want = out[mix64(key^0x6d657368)%uint64(len(out))]
			}
			got, _ := stemChain(key, two)
			require.Equal(t, want, got)
		}
	})
	t.Run("uniform over the last level", func(t *testing.T) {
		three := append(slices.Clone(two), stemLevel{salt: 0x6f7574626f756e64, nodes: surv})
		count := map[int]int{}
		const draws = 30000
		for k := range uint64(draws) {
			s, _ := stemChain(k*0x9e3779b97f4a7c15+1, three)
			count[s]++
		}
		require.Equal(t, len(surv), len(count))
		for _, s := range surv {
			require.Equal(t, true, count[s] > draws/3*9/10 && count[s] < draws/3*11/10, fmt.Sprintf("node %d drawn %d of %d", s, count[s], draws))
		}
	})
	t.Run("an empty level falls back to the one before", func(t *testing.T) {
		lv := append(slices.Clone(two), stemLevel{salt: 1})
		require.Equal(t, 1, lastLevel(lv))
		for k := range uint64(200) {
			a, _ := stemChain(k, lv)
			b, _ := stemChain(k, two)
			require.Equal(t, b, a)
		}
	})
}
