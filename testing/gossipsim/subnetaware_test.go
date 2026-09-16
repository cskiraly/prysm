package gossipsim

import (
	"fmt"
	"math/rand/v2"
	"testing"
)

// stridedSubnets is the custody shape the row harness uses: node i holds a contiguous block of
// `perNode` columns, wrapping, so a column's holders are an arithmetic progression. That structure
// is what defeats a global random graph, so it is what the test has to use.
func stridedSubnets(n, perNode, columns int) [][]int {
	members := make([][]int, columns)
	for i := range n {
		for k := range perNode {
			c := (i*perNode + k) % columns
			members[c] = append(members[c], i)
		}
	}

	return members
}

// randomSubnets is custody as a node ID actually gives it: one independent draw per node. It is
// here because the strided shape above is degenerate in a way that could be blamed for the
// isolation result -- `i*perNode mod columns` takes only `columns/gcd(perNode,columns)` values, so
// 24 nodes at 32 columns produce four custody sets rather than 24. The isolation problem survives
// the fix, because it depends only on the *fraction* of the network a subnet holds.
func randomSubnets(n, perNode, columns int, seed uint64) [][]int {
	members := make([][]int, columns)
	for i := range n {
		r := rand.New(rand.NewPCG(seed, uint64(i)))
		perm := r.Perm(columns)
		for _, c := range perm[:perNode] {
			members[c] = append(members[c], i)
		}
	}

	return members
}

// isolatedInSubnet counts subnet members with no edge to another member of the same subnet. That
// is the quantity that decides whether a topic mesh can form at all, and the reason the old
// topology silently invalidated per-subnet measurements.
func isolatedInSubnet(edges []Edge, members [][]int) int {
	adjacent := make(map[Edge]bool, len(edges))
	for _, e := range edges {
		adjacent[e] = true
	}

	isolated := 0
	for _, subnet := range members {
		if len(subnet) < 2 {
			continue
		}
		inSubnet := make(map[int]bool, len(subnet))
		for _, node := range subnet {
			inSubnet[node] = true
		}
		for _, node := range subnet {
			found := false
			for _, other := range subnet {
				if other != node && adjacent[NewEdge(node, other)] {
					found = true
					break
				}
			}
			if !found {
				isolated++
			}
		}
	}

	return isolated
}

// TestSubnetAwareLeavesNoMemberIsolated is the property the whole generator exists for. A member
// with no in-subnet neighbour cannot form a mesh on that topic, so its subnet never settles and
// any per-subnet measurement over that graph is measuring the topology instead of the protocol.
func TestSubnetAwareLeavesNoMemberIsolated(t *testing.T) {
	shapes := []struct {
		n, perNode, columns int
	}{
		{24, 32, 128}, // the shape R2 has been measured at
		{64, 32, 128}, // where the failures got worse, not better
		{128, 8, 128}, // realistic custody: 6.25% of the network per subnet
		{256, 4, 128}, // the minimum-custody node
	}
	// Every shape is checked under both custody models: strided, which is what the harness
	// used to use, and per-node random, which is what a node ID gives. The guarantee must not
	// depend on the structure of the membership.
	custodies := []struct {
		name string
		of   func(n, perNode, columns int) [][]int
	}{
		{"strided", stridedSubnets},
		{"random", func(n, perNode, columns int) [][]int {
			return randomSubnets(n, perNode, columns, 0x8371)
		}},
	}

	for _, shape := range shapes {
		for _, custody := range custodies {
			name := fmt.Sprintf("%dn_%dcol_%s", shape.n, shape.perNode, custody.name)
			t.Run(name, func(t *testing.T) {
				members := custody.of(shape.n, shape.perNode, shape.columns)
				edges, err := SubnetAware(shape.n, 10, 4, members, 0x8371)
				if err != nil {
					t.Fatalf("SubnetAware: %v", err)
				}
				if got := isolatedInSubnet(edges, members); got != 0 {
					t.Fatalf("%d subnet members have no in-subnet neighbour", got)
				}
				if !Connected(shape.n, edges) {
					t.Fatal("graph is not connected")
				}

				degrees := Degrees(shape.n, edges)
				maxDegree, minDegree := 0, degrees[0]
				for _, d := range degrees {
					maxDegree = max(maxDegree, d)
					minDegree = min(minDegree, d)
				}
				t.Logf("%s: %d edges, degree %d..%d", name, len(edges), minDegree, maxDegree)
			})
		}
	}
}

// TestEnsureSubnetReachGivesPublisherAPeerEverywhere pins the other half of the guarantee. A
// publisher reaches a topic it has not subscribed to through fanout, and fanout can only deliver
// to a peer that *is* subscribed -- so a publisher with no peer in a subnet drops that whole topic
// with no error anywhere. Measured cost before this existed: one seed in 25 lost 192 of 768
// (node, column) completions, which read as a protocol failure and was a graph that could not
// carry the publish.
func TestEnsureSubnetReachGivesPublisherAPeerEverywhere(t *testing.T) {
	const n, perNode, columns = 24, 32, 128
	members := randomSubnets(n, perNode, columns, 0x8371)
	base, err := SubnetAware(n, 10, 4, members, 0x8371)
	if err != nil {
		t.Fatalf("SubnetAware: %v", err)
	}

	publisher := 0
	unreached := func(edges []Edge) int {
		adjacent := make(map[Edge]bool, len(edges))
		for _, e := range edges {
			adjacent[e] = true
		}
		count := 0
		for _, subnet := range members {
			if len(subnet) == 0 {
				continue
			}
			reached := false
			for _, node := range subnet {
				if node == publisher || adjacent[NewEdge(publisher, node)] {
					reached = true
					break
				}
			}
			if !reached {
				count++
			}
		}

		return count
	}

	before := unreached(base)
	t.Logf("before: publisher %d cannot reach %d of %d subnets", publisher, before, columns)
	if before == 0 {
		t.Skip("this draw already reaches every subnet; the guarantee is untestable on it")
	}

	augmented, added := EnsureSubnetReach(base, members, []int{publisher}, 0x8371)
	if got := unreached(augmented); got != 0 {
		t.Fatalf("publisher still cannot reach %d subnets after EnsureSubnetReach", got)
	}
	if added != before {
		t.Fatalf("added %d edges for %d unreached subnets; want one each", added, before)
	}
	// The guarantee must not cost connectivity or the rest of the graph.
	if !Connected(n, augmented) {
		t.Fatal("augmented graph is not connected")
	}
	for _, e := range base {
		if !slicesContains(augmented, e) {
			t.Fatalf("EnsureSubnetReach dropped edge %v", e)
		}
	}
	t.Logf("after: %d edges added, %d total", added, len(augmented))
}

func slicesContains(edges []Edge, want Edge) bool {
	for _, e := range edges {
		if e == want {
			return true
		}
	}

	return false
}

// TestRandomRegularLeavesMembersIsolated is the negative control, and the reason the generator
// above exists rather than a bigger degree. It pins the arithmetic: the expected in-subnet degree
// is globalDegree * (subnet fraction), independent of node count, so a global graph cannot be
// scaled out of this problem.
func TestRandomRegularLeavesMembersIsolated(t *testing.T) {
	for _, shape := range []struct{ n, perNode int }{{24, 32}, {64, 32}, {128, 8}} {
		name := fmt.Sprintf("%dn_%dcol", shape.n, shape.perNode)
		t.Run(name, func(t *testing.T) {
			// Random custody, not strided, so the result cannot be blamed on the strided shape's
			// degeneracy. The arithmetic only involves the subnet's share of the network.
			members := randomSubnets(shape.n, shape.perNode, 128, 0x8371)
			edges, err := RandomRegular(shape.n, 10, 0x8371)
			if err != nil {
				t.Fatalf("RandomRegular: %v", err)
			}
			isolated := isolatedInSubnet(edges, members)
			t.Logf("%s: %d subnet members isolated under a degree-10 global graph", name, isolated)
			if isolated == 0 {
				t.Fatal("expected the global graph to isolate members; if this passes the premise changed")
			}
		})
	}
}
