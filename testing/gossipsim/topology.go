// Package gossipsim is a multi-node gossipsub harness over simlibp2p: topology generators,
// per-node link and latency models, a production-matching pubsub configuration, and a raw
// tracer that accounts for every byte on the wire.
//
// It exists because two studies want the same substrate. It was written for the payload
// segmentation work and promoted here, unchanged in behaviour, for the RowDAS work.
// Anything specific to one study -- which environment variables select an arm,
// what a payload means, which fork options an experiment sweeps -- stays in that study's
// package and reaches this one through NetworkConfig.PubsubOpts and NetworkConfig.PerNodeOpts.
//
// In this harness the network is fully reachable and every node has a public address, so a
// topology is not a network property -- it is exactly the set of pairs that dial each other.
// With UseBlankHost there is no DHT, identify address discovery or autonat, so nothing adds an
// edge behind the test and the graph stays what was built.
//
// Two consequences worth knowing before designing an experiment:
//
//   - Link settings are per node, not per edge. All of a node's traffic shares one uplink and
//     one downlink, so degree buys a node no bandwidth. Experiments that need path diversity to
//     show up must constrain uplinks and leave downlinks generous.
//   - Latency is per packet and the packet carries its source and destination, so a latency
//     matrix is possible. Addresses are assigned deterministically from the node index, which is
//     what LatencyMatrix relies on.
package gossipsim

import (
	"fmt"
	"math/rand/v2"
	"slices"
)

const (
	// swapsPerEdge is how many double edge swaps per edge RandomRegular applies. Well above
	// what mixing needs at these sizes; the cost is negligible next to standing up a host.
	swapsPerEdge = 50
	// regularGraphAttempts bounds the extra mixing rounds spent trying to reconnect a graph a
	// swap split. Exhausting it means the parameters are degenerate, not that we were unlucky.
	regularGraphAttempts = 200
)

// Edge is an undirected connection between two node indices, normalised so A < B.
type Edge struct {
	A, B int
}

func NewEdge(i, j int) Edge {
	if i > j {
		i, j = j, i
	}
	return Edge{A: i, B: j}
}

// Line returns the edges of a path graph: 0-1-2-...-(n-1). Interior degree is 2, far below
// gossipsub's Dlo, so a line isolates a mechanism rather than predicting production.
func Line(n int) []Edge {
	if n < 2 {
		return nil
	}
	out := make([]Edge, 0, n-1)
	for i := range n - 1 {
		out = append(out, NewEdge(i, i+1))
	}
	return out
}

// Star returns the edges from node 0 to every other node. Node 0 is the publisher, and its
// uplink is shared across all receivers, which is what makes send ordering observable.
func Star(n int) []Edge {
	if n < 2 {
		return nil
	}
	out := make([]Edge, 0, n-1)
	for i := 1; i < n; i++ {
		out = append(out, NewEdge(0, i))
	}
	return out
}

// Grid returns the edges of a w x h lattice, indexed row-major as y*w+x. Interior nodes have
// four neighbours, which is the smallest topology where a node can receive different segments
// from different neighbours at once.
func Grid(w, h int) []Edge {
	out := make([]Edge, 0, 2*w*h)
	for y := range h {
		for x := range w {
			i := y*w + x
			if x+1 < w {
				out = append(out, NewEdge(i, i+1))
			}
			if y+1 < h {
				out = append(out, NewEdge(i, i+w))
			}
		}
	}
	return out
}

// Circulant returns a deterministic d-regular graph: each node joined to its d/2 nearest
// neighbours in each direction around a ring, plus the opposite node when d is odd. It is
// Connected by construction, which is why RandomRegular starts here.
func Circulant(n, d int) []Edge {
	present := make(map[Edge]bool, n*d/2)
	for i := range n {
		for k := 1; k <= d/2; k++ {
			present[NewEdge(i, (i+k)%n)] = true
		}
	}
	if d%2 == 1 {
		for i := range n / 2 {
			present[NewEdge(i, i+n/2)] = true
		}
	}
	return SortedEdges(present)
}

// RandomRegular returns a Connected random d-regular graph over n nodes, deterministic in seed.
//
// Rejection sampling from the pairing model is not an option here: the chance that a random
// pairing is simple falls off as roughly exp(-d^2/4), which at d=8 is about 1e-8. Instead this
// starts from a Circulant and mixes it with double edge swaps, the standard degree-preserving
// move: replace (a,b) and (c,d) with (a,c) and (b,d), rejecting any swap that would create a
// self-loop or a duplicate. Degree is exactly preserved at every step, so the result is
// d-regular by construction rather than by luck.
//
// The honest caveat is that this is an MCMC walk over simple d-regular graphs, so the output is
// near-uniform rather than uniform, and a short walk would leave the Circulant's ring structure
// visible. swapsPerEdge is set well above what mixing needs for the sizes we run.
func RandomRegular(n, d int, seed uint64) ([]Edge, error) {
	if d >= n {
		return nil, fmt.Errorf("degree %d needs more than %d nodes", d, n)
	}
	if n*d%2 != 0 {
		return nil, fmt.Errorf("n*d must be even, got n=%d d=%d", n, d)
	}
	if d%2 == 1 && n%2 == 1 {
		return nil, fmt.Errorf("odd degree %d needs an even node count, got %d", d, n)
	}
	r := rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
	es := Circulant(n, d)
	present := make(map[Edge]bool, len(es))
	for _, e := range es {
		present[e] = true
	}

	swap := func() {
		i, j := r.IntN(len(es)), r.IntN(len(es))
		if i == j {
			return
		}
		e1, e2 := es[i], es[j]
		var f1, f2 Edge
		if r.IntN(2) == 0 {
			f1, f2 = NewEdge(e1.A, e2.A), NewEdge(e1.B, e2.B)
		} else {
			f1, f2 = NewEdge(e1.A, e2.B), NewEdge(e1.B, e2.A)
		}
		if f1.A == f1.B || f2.A == f2.B || f1 == f2 || present[f1] || present[f2] {
			return
		}
		delete(present, e1)
		delete(present, e2)
		present[f1], present[f2] = true, true
		es[i], es[j] = f1, f2
	}

	for range swapsPerEdge * len(es) {
		swap()
	}
	// Swaps preserve degree but can in principle disconnect the graph, which would silently
	// starve some nodes of every message. Keep mixing rather than returning a split graph.
	for range regularGraphAttempts {
		if Connected(n, es) {
			return SortedEdges(present), nil
		}
		for range len(es) {
			swap()
		}
	}
	return nil, fmt.Errorf("no Connected %d-regular graph on %d nodes after mixing", d, n)
}

// SubnetAware builds a graph in which every node has peers covering all of its subnets, unioned
// with a global random regular graph.
//
// It exists because a global random graph does not connect subnet members, and every experiment
// that measures a per-subnet quantity is silently conditioned on whether it happened to. The
// arithmetic: if a subnet holds a fraction f of the network and the global graph has degree d, the
// expected degree *within* the subnet is d*f -- independent of the node count, so scaling out does
// not help. At 32 of 128 columns per node that is 10*0.25 = 2.5, so some members have no in-subnet
// neighbour and their topic mesh cannot form; a realistic 8 columns per node gives 10*0.0625 = 0.6.
// Measured, that left 31 members isolated at 24 nodes and 513 at 128 nodes.
//
// Real nodes do not peer at random. They query discv5 per subnet -- in Prysm,
// `filterPeerForDataRowsSubnet` and the data-column subnet queries -- so in-subnet connectivity is
// arranged rather than accidental.
//
// **The peer economy is the part worth getting right.** A first version of this drew a ring plus
// chords *per subnet*, which is fine when there are a few subnets and catastrophic when there are
// many: a node custodying 32 of 128 columns is in 32 subnets, so it collected 32 rings and the
// graph saturated to complete (276 edges over 24 nodes, degree 23 of a possible 23). That is not
// what peering costs in reality, because **one peer covers many of your subnets at once** -- a peer
// custodying 32 columns satisfies, on average, a quarter of your subnets by itself. So this does
// greedy set cover per node: repeatedly add the candidate peer that covers the most of the node's
// still-deficient subnets. The result at 24 nodes and 32 columns each is degree ~14 rather than 23,
// and every member still has minPerSubnet peers in every subnet it belongs to.
//
// members[k] is the node list of subnet k. A node may appear in many subnets; nodes in no subnet
// are still connected by the global graph. minPerSubnet is capped per subnet at |subnet|-1, since
// a subnet of two members can only ever give each one peer.
func SubnetAware(n, globalDegree, minPerSubnet int, members [][]int, seed uint64) ([]Edge, error) {
	if n <= 1 {
		return nil, fmt.Errorf("subnet-aware graph needs more than one node, got %d", n)
	}
	present := make(map[Edge]bool)

	// The global layer first, so the graph is connected even when the subnets are not, and so the
	// coverage pass can count the in-subnet peers it already has for free.
	if globalDegree > 0 {
		global, err := RandomRegular(n, globalDegree, seed)
		if err != nil {
			return nil, fmt.Errorf("global layer: %w", err)
		}
		for _, e := range global {
			present[e] = true
		}
	}

	subnetsOf := make(map[int][]int, n)
	inSubnet := make([]map[int]bool, len(members))
	for k, subnet := range members {
		inSubnet[k] = make(map[int]bool, len(subnet))
		for _, node := range subnet {
			subnetsOf[node] = append(subnetsOf[node], k)
			inSubnet[k][node] = true
		}
	}

	// Deterministic in seed. The node order matters -- an edge added for one node's coverage also
	// counts toward its peer's -- so it is shuffled rather than left as 0..n-1, which would
	// systematically favour the low indices.
	r := rand.New(rand.NewPCG(seed^0x5deece66d, seed^0xdeadbeefcafe))
	order := make([]int, n)
	for i := range order {
		order[i] = i
	}
	r.Shuffle(n, func(i, j int) { order[i], order[j] = order[j], order[i] })

	for _, node := range order {
		// Deficient subnets of this node, by how many peers it still needs in each.
		for {
			need := make(map[int]int)
			for _, k := range subnetsOf[node] {
				want := min(minPerSubnet, len(members[k])-1)
				if want <= 0 {
					continue
				}
				have := 0
				for _, other := range members[k] {
					if other != node && present[NewEdge(node, other)] {
						have++
					}
				}
				if have < want {
					need[k] = want - have
				}
			}
			if len(need) == 0 {
				break
			}

			// Greedy: the candidate covering the most deficient subnets. Ties are broken by a
			// deterministic coin so the choice is not an artifact of node numbering.
			bestPeer, bestCover, ties := -1, 0, 0
			for candidate := range n {
				if candidate == node || present[NewEdge(node, candidate)] {
					continue
				}
				cover := 0
				for k := range need {
					if inSubnet[k][candidate] {
						cover++
					}
				}
				if cover == 0 {
					continue
				}
				switch {
				case cover > bestCover:
					bestPeer, bestCover, ties = candidate, cover, 1
				case cover == bestCover:
					ties++
					if r.IntN(ties) == 0 {
						bestPeer = candidate
					}
				}
			}
			if bestPeer < 0 {
				// No candidate can help: every member of every deficient subnet is already a
				// peer. Nothing more to add for this node.
				break
			}
			present[NewEdge(node, bestPeer)] = true
		}
	}

	edges := SortedEdges(present)
	if !Connected(n, edges) {
		return nil, fmt.Errorf("subnet-aware graph on %d nodes is not connected", n)
	}

	return edges, nil
}

// EnsureSubnetReach adds the fewest edges needed to give each of `publishers` at least one peer
// in every subnet, and returns the augmented edge set with the number of edges added.
//
// It exists because a publisher reaches a topic it does not subscribe to through gossipsub's
// fanout, and fanout needs a peer that *is* subscribed: `MeshPeers` falls back to fanout, but an
// empty fanout set delivers to nobody and there is no relay to recover from it. A publisher with
// no peer in a subnet therefore silently fails to deliver that whole topic -- which reads in the
// results as a protocol failure rather than as a graph that could not carry the publish.
//
// That is not a hypothetical. Measured at 24 nodes with 32 columns each, one seed in 25 left the
// publisher with no peer in one of the custody groups, and 192 of 768 (node, column) pairs never
// completed on the column path. It is a real phenomenon -- a proposer does have to find peers in
// all 128 subnets -- but it is a *publish reachability* question, and leaving it to the draw makes
// every other experiment over that graph a lottery between two unrelated effects.
//
// Real nodes do not leave it to the draw either: a proposer with blobs to publish queries discv5
// for each column subnet, and supernodes subscribed to everything are what it finds.
func EnsureSubnetReach(edges []Edge, members [][]int, publishers []int, seed uint64) ([]Edge, int) {
	present := make(map[Edge]bool, len(edges))
	for _, e := range edges {
		present[e] = true
	}

	// Deterministic in seed, and mixed differently from the layers above so the choice of
	// stand-in peer is not correlated with the chords drawn there.
	r := rand.New(rand.NewPCG(seed^0x0eac4_0000, seed^0x9e3779b97f4a7c15))
	added := 0
	for _, publisher := range publishers {
		for _, subnet := range members {
			if len(subnet) == 0 {
				continue
			}
			reached := false
			for _, node := range subnet {
				if node == publisher || present[NewEdge(publisher, node)] {
					reached = true
					break
				}
			}
			if reached {
				continue
			}
			nodes := slices.Clone(subnet)
			slices.Sort(nodes)
			peer := nodes[r.IntN(len(nodes))]
			present[NewEdge(publisher, peer)] = true
			added++
		}
	}
	if added == 0 {
		return edges, 0
	}

	return SortedEdges(present), added
}

// SortedEdges flattens an edge set into a deterministic order. Map iteration is not ordered, and
// an experiment that dials in a different order every run is not reproducible.
func SortedEdges(present map[Edge]bool) []Edge {
	out := make([]Edge, 0, len(present))
	for e := range present {
		out = append(out, e)
	}
	slices.SortFunc(out, func(x, y Edge) int {
		if x.A != y.A {
			return x.A - y.A
		}
		return x.B - y.B
	})
	return out
}

// Degrees counts each node's neighbours.
func Degrees(n int, es []Edge) []int {
	out := make([]int, n)
	for _, e := range es {
		out[e.A]++
		out[e.B]++
	}
	return out
}

// Connected reports whether every node is reachable from node 0. A disconnected graph would
// silently invalidate an experiment: some nodes simply never receive anything.
func Connected(n int, es []Edge) bool {
	if n == 0 {
		return true
	}
	adj := make([][]int, n)
	for _, e := range es {
		adj[e.A] = append(adj[e.A], e.B)
		adj[e.B] = append(adj[e.B], e.A)
	}
	seen := make([]bool, n)
	seen[0] = true
	queue := []int{0}
	count := 1
	for len(queue) > 0 {
		v := queue[0]
		queue = queue[1:]
		for _, w := range adj[v] {
			if !seen[w] {
				seen[w] = true
				count++
				queue = append(queue, w)
			}
		}
	}
	return count == n
}

func EqualEdges(a, b []Edge) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// Mix64 is the splitmix64 finalizer; raw xors of small integers do not rank (the
// hash-low-bits lesson), so every selection hash below avalanches first.
func Mix64(x uint64) uint64 {
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return x
}
