package rowintegrationtest

// R2: does row dissemination cost column completion anything?
//
// EIP-8371 §Availability Preservation makes row topics an optimisation only: the column topics
// stay the authoritative path and must not be delayed. That is a regression test, and it is the one
// question where the answer "no difference" is the good answer -- so the experiment has to be
// sensitive enough that a difference would show, which is what the seed sweep is for.
//
// Setup. Every column subnet gets its cells from the proposer, so columns complete on their own
// path and the row axis is pure extra load on the same uplinks. 24 nodes each custodying 32 columns
// gives six subscribers per column subnet -- a real mesh rather than R9's single subscriber -- and a
// custody union of all 128, so the row axis does the whole of its real work: pooling to the
// threshold, recovering, and (in the third arm) cross-forwarding.
//
// Arms:
//
//	base            no row topic at all. Nodes never join it, so there is no row traffic.
//	rows            row topic subscribed by every node, each publishing the cells its custody
//	                gives it, pooling to the threshold.
//	rows+push       and cross-forwarding a recovered row into non-custodied columns. With every
//	                column already published this adds traffic and can add nothing else, which is
//	                deliberate: it prices the push on a healthy network.
//
// Prediction, written before running: `rows` is within seed noise of `base` on median column
// completion, and the p95 tail is where any cost shows -- row cells are 2 KiB each and R3 measured
// the row exchange duplicating them by about one copy per mesh peer, so the row axis puts megabytes
// on the same links that carry a few hundred kilobytes of column cells. If contention bites, it
// should bite the tail. `rows+push` should be the worst of the three, since on a healthy network
// its traffic buys nothing.
//
// The falsification condition from experiments.md is median or p95 in `rows` worse than `base`
// beyond seed noise; that would make RowDAS a latency regression regardless of its CPU win.

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/OffchainLabs/go-bitfield"
	"github.com/OffchainLabs/prysm/v7/beacon-chain/core/peerdas"
	"github.com/OffchainLabs/prysm/v7/beacon-chain/p2p/partialdatacolumnbroadcaster"
	fieldparams "github.com/OffchainLabs/prysm/v7/config/fieldparams"
	"github.com/OffchainLabs/prysm/v7/consensus-types/blocks"
	"github.com/OffchainLabs/prysm/v7/testing/gossipsim"
	"github.com/OffchainLabs/prysm/v7/testing/require"
)

const r2Columns = uint64(fieldparams.NumberOfColumns)

// The shape is configurable because the headline claim rests on it. Defaults are the shape the
// result was first measured at -- 24 nodes, 32 columns each, 4 blobs, 3 seeds -- so an unattended
// run costs what it always did. `ROWDAS_R2_SEEDS=50` tightens the latency claim;
// `ROWDAS_R2_NODES` and `ROWDAS_R2_BLOBS` move it to a shape closer to mainnet, where 4 blobs is
// the least realistic part: the row axis's share of each column scales as
// rows_subscribed / blob_count, so 4 blobs flatters it by 8x against 32.
func r2Nodes() int          { return envInt("ROWDAS_R2_NODES", 24) }
func r2ColumnsPerNode() int { return envInt("ROWDAS_R2_COLUMNS", 32) }

// r2Blobs must exceed one, or a node's own row cells complete its columns locally and the column
// path is never exercised. See the comment in runR2Arm.
func r2Blobs() int { return envInt("ROWDAS_R2_BLOBS", 4) }

// r2Seeds returns the seeds to sweep. One graph is one sample and the claim is about latency, so
// every arm sees the same graphs -- the parent measurement plan's common-random-numbers
// discipline.
func r2Seeds() []uint64 {
	count := envInt("ROWDAS_R2_SEEDS", 3)
	seeds := make([]uint64, 0, count)
	for i := range count {
		seeds = append(seeds, 0x8371_0001+uint64(i))
	}

	return seeds
}

func envInt(name string, def int) int {
	value, ok := os.LookupEnv(name)
	if !ok {
		return def
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed <= 0 {
		return def
	}

	return parsed
}

// r2Custody gives node i a pseudorandom subset of the columns, one draw per node, which is what
// custody derived from a node ID looks like.
//
// It used to hand node i the *contiguous* block starting at i*perNode, wrapping, which is much
// worse than it reads. `i*perNode mod 128` takes only `128/gcd(perNode,128)` distinct values, so
// the network collapses into that many custody sets: at 24 nodes with 32 columns each there were
// exactly **four**, six nodes to a set, each set holding a disjoint block of 32 columns. Every one
// of the 128 "column subnets" was then one of only four node sets, a node shared no column with 18
// of its 23 peers, and any measurement per column subnet had four independent samples rather than
// 128. Realistic custody sizes did not escape it either -- 128 nodes at 8 columns each gave 16 sets
// of 8 nodes.
//
// The failure that exposed it: one seed's `base` arm stalled at exactly 576/768 completions, which
// is 192 = 32 columns x 6 nodes -- one whole custody group, unreachable from the publisher because
// the group's six nodes happened to include none of its peers. See EnsureSubnetReach.
func r2Custody(i int) []uint64 {
	perNode := r2CustodySize(i)
	// Per node, not per network: custody is a property of the node ID, so it must not move when
	// the graph seed moves -- that is what makes the arms comparable under common random numbers.
	r := rand.New(rand.NewPCG(0x8371_c057_0d47, uint64(i)))
	shuffled := make([]uint64, r2Columns)
	for c := range shuffled {
		shuffled[c] = uint64(c)
	}
	r.Shuffle(len(shuffled), func(a, b int) { shuffled[a], shuffled[b] = shuffled[b], shuffled[a] })
	columns := shuffled[:perNode]
	slices.Sort(columns)

	return columns
}

// r2CustodySize is node i's custody size: uniform r2ColumnsPerNode unless ROWDAS_R2_CUSTODY_MIX
// describes a heterogeneous network as comma-separated `columns:count` pairs, e.g.
// "8:104,32:20,128:4" (R18). The counts must sum to the node count. Which node lands in which
// class is a deterministic shuffle seeded by nothing but node identity -- like the custody draw
// above, and for the same reason: class is a property of the node, so it must not move with the
// graph seed or the arm.
func r2CustodySize(i int) int {
	mix := os.Getenv("ROWDAS_R2_CUSTODY_MIX")
	if mix == "" {
		return r2ColumnsPerNode()
	}
	sizes := make([]int, 0, r2Nodes())
	for pair := range strings.SplitSeq(mix, ",") {
		columnsStr, countStr, ok := strings.Cut(strings.TrimSpace(pair), ":")
		if !ok {
			panic(fmt.Sprintf("ROWDAS_R2_CUSTODY_MIX pair %q is not columns:count", pair))
		}
		columns, err1 := strconv.Atoi(columnsStr)
		count, err2 := strconv.Atoi(countStr)
		if err1 != nil || err2 != nil || columns < 1 || columns > int(r2Columns) || count < 1 {
			panic(fmt.Sprintf("ROWDAS_R2_CUSTODY_MIX pair %q is out of range", pair))
		}
		for range count {
			sizes = append(sizes, columns)
		}
	}
	if len(sizes) != r2Nodes() {
		panic(fmt.Sprintf("ROWDAS_R2_CUSTODY_MIX describes %d nodes, the network has %d", len(sizes), r2Nodes()))
	}
	r := rand.New(rand.NewPCG(0x8371_c1a5_50f7, 0))
	r.Shuffle(len(sizes), func(a, b int) { sizes[a], sizes[b] = sizes[b], sizes[a] })

	return sizes[i]
}

// r2ExpectedCompletions is the number of (node, column) custody pairs the network must complete.
func r2ExpectedCompletions() int {
	expected := 0
	for i := range r2Nodes() {
		expected += r2CustodySize(i)
	}

	return expected
}

type r2Arm struct {
	name string
	rows bool
	push bool
	// pushEager restores the pre-item-16 eager publish (R17's comparison arm); default false is
	// advertisement-only. pushGated applies the relay gate: forward only the columns whose cells
	// this node had to reconstruct rather than receive row-wise.
	pushEager bool
	pushGated bool
	// deferMS is D17 clause 1's request deferral for this arm, in milliseconds. Zero uses
	// whatever the broadcaster default is (currently zero: off).
	deferMS int
	// silent puts every node on the row subnet without any of them ever publishing a row, which
	// is R6's suppression: the members are present and refuse to serve. The subscription and its
	// mesh are still paid for, which is the part of the claim worth testing -- a suppressed row
	// subnet must degrade to the status quo, not to something worse than not having one.
	silent bool
	// delay holds the row axis back after the proposer's columns go out, which is EIP-8371's own
	// hedge: "Since cells might arrive from three different sources (getBlobs, columns, rows) a
	// node MAY choose to delay the request of cells from rows."
	//
	// It is an arm rather than a constant because the regression it addresses is shape-dependent.
	// At 24 nodes with 32-column custody the row axis is worth -312 ms with no delay at all; at
	// 128 nodes with realistic 8-column custody it *costs* 478 ms at 4 blobs and 3.3 s at 32,
	// because a row subscription moves one full row (128 cells) whatever the node's custody, and
	// only `custody` of those cells help its own columns. Delaying the row exchange until the
	// column path has had its turn is what should recover that, and the value it takes is R7's
	// deliverable: the EIP leaves the bound TBD.
	delay time.Duration
}

type r2Result struct {
	arm         string
	seed        uint64
	completions int
	expected    int
	p50, p95    time.Duration
	maxAt       time.Duration
	partialB    int
	rowB, colB  int
	// Count-type statistics, the quantities R3(b) sweeps: generated row actions in total and by
	// the reasons that respond to the announce policy, and traced row RPCs.
	rowActions int
	rowRPCs    int
	availAdd   int
	reqAdd     int
	reqChurn   int
	// Per-custody-class folds (R18), keyed by custody size. Nil under uniform custody.
	classP50         map[int]time.Duration
	classCompletions map[int]int
	classActions     map[int]int
	classRowB        map[int]int
}

// TestR2RowTrafficVersusColumnCompletion is R2.
func TestR2RowTrafficVersusColumnCompletion(t *testing.T) {
	arms := []r2Arm{
		{name: "base", rows: false, push: false},
		{name: "rows", rows: true, push: false},
		{name: "rows+push", rows: true, push: true},
	}
	// ROWDAS_R2_ROW_DELAY_MS adds one delayed arm per comma-separated value, which puts the
	// EIP's line-56 hedge under test: "a node MAY choose to delay the request of cells from
	// rows". A list rather than a single value so one run maps the mitigation curve on the same
	// graphs -- the delay that removes the harm is what R7 owes the EIP, and it is only
	// meaningful relative to how long the column path itself takes.
	for _, value := range strings.Split(os.Getenv("ROWDAS_R2_ROW_DELAY_MS"), ",") {
		ms, err := strconv.Atoi(strings.TrimSpace(value))
		if err != nil || ms <= 0 {
			continue
		}
		arms = append(arms, r2Arm{
			name:  fmt.Sprintf("rows+delay%dms", ms),
			rows:  true,
			delay: time.Duration(ms) * time.Millisecond,
		})
	}
	// One graph is one sample, and the claim is about latency -- so the seed is swept, per the
	// parent measurement plan's common-random-numbers discipline: every arm sees the same graphs.
	seeds := r2Seeds()
	t.Logf("R2 shape: %d nodes, %d columns each, %d blobs, %d seeds",
		r2Nodes(), r2ColumnsPerNode(), r2Blobs(), len(seeds))

	// Seed outer, arm inner. Each seed then yields a *complete paired sample* -- the same graph
	// with and without the row axis -- so a long sweep reports as it goes instead of only at the
	// end, and a run stopped early still has usable pairs. Arm-outer gave nothing comparable until
	// two thirds of the way through.
	var results []r2Result
	for _, seed := range seeds {
		bySeed := make(map[string]r2Result, len(arms))
		for _, arm := range arms {
			t.Run(fmt.Sprintf("seed-%x/%s", seed, arm.name), func(t *testing.T) {
				r := runR2Arm(t, arm, seed)
				results = append(results, r)
				bySeed[arm.name] = r
			})
		}
		// The running paired line, so partial results are readable in the log.
		if base, ok := bySeed["base"]; ok {
			for _, arm := range arms {
				if arm.name == "base" {
					continue
				}
				got, ok := bySeed[arm.name]
				if !ok {
					continue
				}
				verdict := "slower"
				if got.p50 < base.p50 {
					verdict = "faster"
				}
				t.Logf("  seed %x paired: %-12s %v against base %v -> %v by %v",
					seed, arm.name, got.p50.Round(time.Millisecond), base.p50.Round(time.Millisecond),
					verdict, (got.p50 - base.p50).Round(time.Millisecond))
			}
		}
	}

	t.Log("R2 column completion under row traffic")
	t.Logf("  %-12s %12s %12s %10s %10s %10s %12s", "arm", "seed", "completions", "p50", "p95", "max", "partial B")
	for _, r := range results {
		t.Logf("  %-12s %12x %6d/%-5d %10v %10v %10v %12d",
			r.arm, r.seed, r.completions, r.expected,
			r.p50.Round(time.Millisecond), r.p95.Round(time.Millisecond), r.maxAt.Round(time.Millisecond),
			r.partialB)
	}

	// Per-arm medians across seeds, which is what the claim is about.
	summarise := func(name string) (p50, p95 time.Duration, completions, expected int) {
		var p50s, p95s []time.Duration
		for _, r := range results {
			if r.arm != name {
				continue
			}
			p50s = append(p50s, r.p50)
			p95s = append(p95s, r.p95)
			completions += r.completions
			expected += r.expected
		}
		slices.Sort(p50s)
		slices.Sort(p95s)
		if len(p50s) == 0 {
			return 0, 0, completions, expected
		}

		return p50s[len(p50s)/2], p95s[len(p95s)/2], completions, expected
	}

	t.Logf("  %-12s %10s %10s %14s", "arm", "med p50", "med p95", "completed")
	for _, arm := range arms {
		p50, p95, completions, expected := summarise(arm.name)
		t.Logf("  %-12s %10v %10v %7d/%-6d",
			arm.name, p50.Round(time.Millisecond), p95.Round(time.Millisecond), completions, expected)
	}

	// The arms share graphs, so the comparison that uses the design is *paired*: per seed, the
	// same connectivity draw with and without the row axis. Comparing medians of medians throws
	// that away and leaves the graph draw in the noise, which is why three seeds looked like all
	// the resolution there was.
	paired := func(name string) (deltas []time.Duration, better int) {
		bySeed := make(map[uint64]time.Duration, len(seeds))
		for _, r := range results {
			if r.arm == "base" {
				bySeed[r.seed] = r.p50
			}
		}
		for _, r := range results {
			if r.arm != name {
				continue
			}
			basep50, ok := bySeed[r.seed]
			if !ok {
				continue
			}
			deltas = append(deltas, r.p50-basep50)
			if r.p50 < basep50 {
				better++
			}
		}
		slices.Sort(deltas)

		return deltas, better
	}

	for _, arm := range arms {
		if arm.name == "base" {
			continue
		}
		deltas, better := paired(arm.name)
		if len(deltas) == 0 {
			continue
		}
		var total time.Duration
		for _, d := range deltas {
			total += d
		}
		t.Logf("  paired against base, %s: median %v, mean %v, best %v, worst %v, faster on %d of %d seeds",
			arm.name,
			deltas[len(deltas)/2].Round(time.Millisecond),
			(total / time.Duration(len(deltas))).Round(time.Millisecond),
			deltas[0].Round(time.Millisecond),
			deltas[len(deltas)-1].Round(time.Millisecond),
			better, len(deltas))
	}

	basep50, basep95, baseDone, baseExpected := summarise("base")
	rowsp50, rowsp95, rowsDone, _ := summarise("rows")

	// With enough seeds the paired sign test is the claim, and it is stricter than a band on the
	// medians: the row axis should be faster on most graphs, not merely faster on average. Below
	// ten seeds there is not enough to ask that of, so it stays a log line.
	if rowsDeltas, rowsBetter := paired("rows"); len(rowsDeltas) >= 10 {
		require.Equal(t, true, 2*rowsBetter > len(rowsDeltas),
			fmt.Sprintf("the row axis should be faster on most graphs: %d of %d", rowsBetter, len(rowsDeltas)))
	}

	// Completion is not allowed to be *lost* by the row axis, which is the claim. It is not
	// asserted to be total: at 24 nodes on a degree-10 graph a column subnet's six subscribers can
	// mesh into a component that never reaches the proposer's cells, and one seed reproducibly
	// leaves two nodes with none of their 32 columns. That is a property of the connectivity draw
	// at this scale, not of RowDAS, so the assertion compares the arms rather than demanding 100%.
	require.Equal(t, true, baseDone > baseExpected*9/10,
		fmt.Sprintf("base should complete most columns, got %d of %d", baseDone, baseExpected))
	require.Equal(t, true, rowsDone >= baseDone,
		fmt.Sprintf("the row axis must not cost completions: rows %d, base %d", rowsDone, baseDone))

	// And the latency claim. A 2x band rather than a tight one: three seeds at 24 nodes cannot
	// resolve better than that, and saying so is more useful than a tight bound that fails on
	// noise. What this rules out is an order-of-magnitude regression.
	require.Equal(t, true, rowsp50 < 2*basep50,
		fmt.Sprintf("median column completion: rows %v against base %v", rowsp50, basep50))
	require.Equal(t, true, rowsp95 < 2*basep95,
		fmt.Sprintf("p95 column completion: rows %v against base %v", rowsp95, basep95))
}

// r2NetworkOpts builds one arm's network configuration.
//
// It is a separate function so the paired experiment's central invariant is testable directly:
// `base` and `rows` must be measured on the *same* physical graph, and the graph is drawn from
// these options. TestR2ArmsShareOneGraph calls this for both arms and compares the drawn edge sets
// exactly -- equal edge counts would not have caught the original bug, where arm-dependent
// membership silently gave the two arms different topologies of the same size.
//
// Returns the predicates alongside the options because the caller needs both: intendedMember for
// the member/outsider split in the results, subscribes for gating the publish loops.
func r2NetworkOpts(t *testing.T, arm r2Arm, seed uint64) (opts networkOpts, intendedMember, subscribes func(int) bool) {
	t.Helper()

	const rowIndex = uint64(0)
	subnet, err := peerdas.RowSubnetForBlob(rowIndex, harnessSlot)
	require.NoError(t, err)

	// Row-subnet membership as a fraction of the network, which is the fidelity question the EIP
	// author raised. A node is on *one* row subnet, and only `blob_count` of ROW_SUBNET_COUNT
	// subnets carry a blob in a slot -- so the share of nodes with any row traffic at all is
	// `blob_count / 128`, about 3% at 4 blobs. This experiment's default puts *every* node on the
	// subnet carrying blob 0, which is faithful only at 128 blobs, and overstates aggregate row
	// load by 128/blob_count everywhere else.
	//
	// ROWDAS_R2_ROW_MEMBER_MOD=k makes every kth node a member instead, so member and non-member
	// column completion can be compared inside one run. That distinguishes a cost a row subscriber
	// pays for itself from interference it imposes on the rest of the network -- and only the second
	// would make the row axis a network-wide latency problem.
	memberMod := envInt("ROWDAS_R2_ROW_MEMBER_MOD", 1)
	// ROWDAS_R2_ROW_SKIP_PROPOSER=1 takes node 0 -- the proposer -- off the row subnet.
	//
	// It tests the hotspot hypothesis directly. The proposer publishes all 128 columns, and D13's
	// crossFillRowFromHeldColumns seeds its row state from every column it holds, so it is the
	// *only* node holding any cell of the row when the exchange starts. Request de-confliction then
	// sends all members' requests to the one node that can answer: 128 nodes x 128 cells x 2 KB is
	// 32 MB out of a single uplink, 5.1 s at 50 Mbps, against a measured penalty of +3.13 s.
	//
	// Mainnet does not have this shape. A proposer is on one row subnet of 128, so it is the unique
	// source for that subnet only; members of the other subnets have no row source until the column
	// path spreads cells, which is exactly the ordering R7's deferral imposes deliberately.
	skipProposer := envInt("ROWDAS_R2_ROW_SKIP_PROPOSER", 0) != 0
	// Two distinct predicates, and conflating them confounds the experiment.
	//
	// intendedMember is who the row subnet *would* consist of. It does not depend on the arm, so it
	// is what the connectivity graph is built from and what the member/outsider split is reported
	// against -- both arms then sit on the same physical graph and the split means the same thing in
	// each.
	//
	// subscribes is who actually joins the topic here, which is intendedMember gated on the arm
	// enabling the row axis at all.
	intendedMember = func(i int) bool {
		if skipProposer && i == 0 {
			return false
		}

		return i%memberMod == 0
	}
	subscribes = func(i int) bool { return arm.rows && intendedMember(i) }

	opts = networkOpts{
		n:          r2Nodes(),
		rowSubnet:  subnet,
		columnsFor: r2Custody,
		rowMembers: subscribes,
		// Arm-independent, so `base` and `rows` are drawn on the same graph.
		rowGraphMembers: intendedMember,
		columnCount:     r2Columns,
		graphSeed:       seed,
		// The global degree is overridable so the topology change can be compared at *equal mean
		// degree*. Subnet-aware edges are additional, so leaving the global degree alone raises the
		// mean from 10 to about 13, which speeds the column path on its own and would confound the
		// comparison with the old topology.
		graphDegree: envInt("ROWDAS_R2_GRAPH_DEGREE", 0),
		// On by default: a global-only graph leaves column-subnet members with no in-subnet
		// neighbour, so their mesh never settles and the completion measurement is conditioned on
		// which draws happened to work. ROWDAS_R2_SUBNET_AWARE=0 reproduces the old topology, which
		// is the only way to see what it was doing to the numbers.
		subnetAware: envInt("ROWDAS_R2_SUBNET_AWARE", 1) != 0,
		// Node 0 is the proposer below, and it publishes all 128 columns while custodying a
		// handful. Without this one seed in 25 lost a whole custody group off the column path.
		publishers: []int{0},
	}

	return opts, intendedMember, subscribes
}

func runR2Arm(t *testing.T, arm r2Arm, seed uint64) r2Result {
	t.Helper()

	if arm.deferMS > 0 {
		prev := partialdatacolumnbroadcaster.RowRequestDeferral
		partialdatacolumnbroadcaster.RowRequestDeferral = time.Duration(arm.deferMS) * time.Millisecond
		defer func() { partialdatacolumnbroadcaster.RowRequestDeferral = prev }()
	}

	// Four blobs, not one, and this is the correction that made the experiment measure anything.
	//
	// With one blob a column holds exactly one cell, and a node's own row cells for its custodied
	// columns *are* that cell -- so PublishRow's local cross-fill completed every custodied column
	// with no network traffic at all, and the `rows` arm came out 25x faster than `base` on 40x
	// fewer bytes. That is a real property of the cross-fill bridge, and it is not what R2 is
	// asking. At four blobs the cross-fill supplies one cell of four and the column path has to
	// deliver the rest, so column completion is a column-path measurement again.
	m := newMatrix(t, r2Blobs())
	// Blob 0's row, matching r2NetworkOpts, which derives the subnet from the same index.
	const rowIndex = uint64(0)

	opts, intendedMember, subscribes := r2NetworkOpts(t, arm, seed)
	isMember := intendedMember
	nw, stop := newRowColumnNetwork(t, opts)
	defer stop()

	// Every node asks its own subnets for everything, as a real node does on block arrival.
	for _, node := range nw.nodes {
		require.NoError(t, node.broadcaster.Publish(context.Background(), func(yield func(string, blocks.PartialDataColumn) bool) {
			for _, columnIndex := range node.columns {
				column := m.column(t, columnIndex, nil)
				requests := bitfield.NewBitlist(uint64(m.blobCount)).Not()
				require.NoError(t, column.SetPartsRequests(requests))
				if !yield(columnTopic(t, columnIndex), column) {
					return
				}
			}
		}))
	}

	start := time.Now()

	// The proposer publishes every column, so the column path can complete on its own.
	proposer := nw.nodes[0]
	require.NoError(t, proposer.broadcaster.Publish(context.Background(), func(yield func(string, blocks.PartialDataColumn) bool) {
		allRows := make([]uint64, 0, r2Blobs())
		for blobIndex := range uint64(r2Blobs()) {
			allRows = append(allRows, blobIndex)
		}
		for columnIndex := range r2Columns {
			column := m.column(t, columnIndex, allRows)
			if !yield(columnTopic(t, columnIndex), column) {
				return
			}
		}
	}))

	// The row axis, competing for the same uplinks.
	//
	// Publishing an *empty* row, not one pre-loaded with this node's custody. Handing the node
	// its custody's row cells looks harmless and is not: PublishRow cross-fills them straight
	// into the node's column states, locally and for free, so the `rows` arm would start with
	// one of every column's four cells already in hand while `base` starts with none. That
	// alone made column completion look 30% faster and column bytes 22% lower, and it made the
	// cross-fill saving look "confirmed" at exactly the 3/4 ratio it had been given.
	//
	// The faithful flow is the one the node actually runs: the proposer's columns arrive over
	// the column topics, cross-fill populates the row state from them, and the row topic pools
	// what is still missing. So the only difference between the arms is whether the row topic
	// exists -- which is what R2 asks.
	if arm.rows && !arm.silent {
		// The column path gets first refusal for `arm.delay`. Measured from the same instant as
		// the completion clock, so the delay is charged against the latency it is meant to save.
		if arm.delay > 0 {
			time.Sleep(arm.delay)
		}
		// Gated on subscription, not on the arm. Every node *joins* the row topic so it can be
		// reached, and only Subscribe is gated -- so publishing from a non-subscriber creates
		// published row state on it and pushes through fanout, and cross-fill then keeps
		// republishing that outsider's row. Without this gate, `ROWDAS_R2_ROW_MEMBER_MOD=k` would
		// leave all 128 nodes as row participants while claiming k of them were, which would have
		// made the membership calibration measure nothing.
		for i, node := range nw.nodes {
			if !subscribes(i) {
				continue
			}
			empty, err := blocks.NewPartialDataRow(m.root, m.header, rowIndex, m.commitments, m.inclusionProof)
			require.NoError(t, err)
			require.NoError(t, node.broadcaster.PublishRow(context.Background(), nw.topic, empty))
		}
	}

	// Wait for the column path to finish, which is what is being timed. Poll on completions
	// rather than on quiet: quiet cannot tell a finished transfer from one that has not started,
	// which is the mistake R1(b) made.
	// Stop on a stall rather than on a fixed deadline. One seed reproducibly leaves 64 (node,
	// column) pairs uncompleted, and waiting the full deadline out cost 90 s per arm -- five of
	// the run's six minutes. A stall window is safe here in a way that waitUntilQuiet was not in
	// R1(b): the quantity is monotonic and the thing being waited for is counted directly, so "no
	// new completions for 10 s" cannot be confused with "has not started".
	expected := r2ExpectedCompletions()
	const stallWindow = 10 * time.Second
	deadline := time.Now().Add(90 * time.Second)
	lastProgress := time.Now()
	previous := -1
	for time.Now().Before(deadline) {
		done := 0
		for _, node := range nw.nodes {
			done += len(node.callbacks.columnsCompleteSnapshot())
		}
		if done >= expected {
			break
		}
		if done != previous {
			previous = done
			lastProgress = time.Now()
		} else if time.Since(lastProgress) > stallWindow {
			t.Logf("    completions stalled at %d/%d for %v", done, expected, stallWindow)
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	// The row axis gets to finish too, so its bytes are counted and the push has something to
	// push. This is after the column measurement, which is the point: the columns were racing it.
	if arm.rows && arm.push {
		prevEager := partialdatacolumnbroadcaster.CrossForwardEagerPush
		partialdatacolumnbroadcaster.CrossForwardEagerPush = arm.pushEager
		defer func() { partialdatacolumnbroadcaster.CrossForwardEagerPush = prevEager }()
		groupID := m.rowGroupID(t, rowIndex)
		for i, node := range nw.nodes {
			// Same gate as the initial publish: only row participants recover and cross-forward.
			if !subscribes(i) {
				continue
			}
			row, err := node.broadcaster.RowSnapshot(context.Background(), nw.topic, groupID)
			require.NoError(t, err)
			if row == nil || !row.ReconstructionThresholdMet() || row.IsComplete() {
				continue
			}
			// The relay gate's input, snapshotted before recovery fills everything in: a cell
			// that arrived row-wise had a source in the subnet, so its column has a relay.
			heldBefore := bitfield.Bitlist(append([]byte(nil), row.Included...))
			require.NoError(t, peerdas.RecoverRow(row))
			require.NoError(t, node.broadcaster.PublishRow(context.Background(), nw.topic, *row))
			columns := nonCustodiedOf(node.columns)
			if arm.pushGated {
				gated := columns[:0]
				for _, columnIndex := range columns {
					if columnIndex < heldBefore.Len() && heldBefore.BitAt(columnIndex) {
						continue
					}
					gated = append(gated, columnIndex)
				}
				columns = gated
			}
			if len(columns) == 0 {
				continue
			}
			_, err = node.broadcaster.CrossForwardRow(context.Background(), nw.topic, row, columns)
			require.NoError(t, err)
		}
		// The advertisement channel is heartbeat-paced: its traffic exists only on the
		// heartbeats after the forward. Drain before reading counters, or the ads arms read
		// as free -- which is exactly how R17's first run misread them.
		nw.waitUntilQuiet(t, 30*time.Second)
	}

	// Read the completion times, deduped per (node, column) and counted only where the node
	// custodies the column -- the same rule R9 needed.
	type nodeColumn struct {
		node   int
		column uint64
	}
	seen := make(map[nodeColumn]bool)
	var delays, memberDelays, outsiderDelays []time.Duration
	// Folds by custody class (R18): keyed by custody size, populated only when a mix is set --
	// with uniform custody there is one class and the aggregate already tells the story.
	classDelays := map[int][]time.Duration{}
	classRowB := map[int]int{}
	partialB, rowB, colB := 0, 0, 0
	droppedRPCs, undeliverableMsgs := 0, 0
	perNode := make([]int, 0, r2Nodes())
	perNodeAll := make([]int, 0, r2Nodes())
	duplicates, allBytes := 0, 0
	rowRPCs, colRPCs := 0, 0
	rowSentRPCsTotal, rowRecvRPCsTotal := 0, 0
	for i, node := range nw.nodes {
		custodied := make(map[uint64]bool, len(node.columns))
		for _, held := range node.columns {
			custodied[held] = true
		}
		for _, event := range node.callbacks.columnsCompleteSnapshot() {
			if !custodied[event.columnIndex] {
				continue
			}
			key := nodeColumn{node: i, column: event.columnIndex}
			if seen[key] {
				continue
			}
			seen[key] = true
			delay := event.at.Sub(start)
			delays = append(delays, delay)
			classDelays[r2CustodySize(i)] = append(classDelays[r2CustodySize(i)], delay)
			if isMember(i) {
				memberDelays = append(memberDelays, delay)
			} else {
				outsiderDelays = append(outsiderDelays, delay)
			}
		}
		recv, sent, _, _ := rowBytes(nw.tracers[i])
		partialB += recv + sent
		// Split by axis, measured rather than differenced against the base arm. The tracer keys
		// partial-message bytes by topic, so each axis can be read directly.
		rowRecv, rowSent, rowRecvRPCs, rowSentRPCs := nw.tracers[i].PartialCountsByTopicPrefix(partialdatacolumnbroadcaster.DataRowPrefix)
		classRowB[r2CustodySize(i)] += rowRecv + rowSent
		colRecv, colSent, colRecvRPCs, colSentRPCs := nw.tracers[i].PartialCountsByTopicPrefix("data_column_sidecar_")
		// Message counts, which R2 has discarded from this very call all along. If the cost is per
		// message rather than per byte -- a packet-scheduled link, a per-RPC queue -- then bytes
		// cannot explain the latency and counts can. The row axis republishes a column state on
		// every cross-filled cell (crossfill.go afterColumnExtended -> publishPartialCol), so its
		// message count should rise much faster than its bytes.
		rowRPCs += rowRecvRPCs + rowSentRPCs
		colRPCs += colRecvRPCs + colSentRPCs
		// Kept separate as well: summing directions is what made the first attribution circular.
		rowSentRPCsTotal += rowSentRPCs
		rowRecvRPCsTotal += rowRecvRPCs
		rowB += rowRecv + rowSent
		colB += colRecv + colSent
		// Queue losses, which R2 has never read and should have. A dropped RPC is only recovered
		// through IHAVE/IWANT on the next heartbeat, so a handful of drops buys seconds of delay at
		// no cost in bytes or CPU -- which is exactly the signature this experiment is showing at
		// realistic custody: 1% of the link, 0.7 of 16 cores, and multi-second stalls. The tracer
		// has counted them all along.
		dropped, undeliverable := nw.tracers[i].Losses()
		droppedRPCs += dropped
		undeliverableMsgs += undeliverable
		// Per-node totals, because the aggregate hides the thing that matters. A mean of 507 KB
		// per node over 4.65 s is 1.7% of a 50 Mbps link and cannot by itself delay anything --
		// but if a few nodes carry most of it, their links saturate for seconds and everything
		// queued behind is late. The first node to reach the 64-cell threshold becomes every
		// peer's source, so hotspots are the expected shape rather than a surprise.
		perNode = append(perNode, recv+sent)
		// Total wire bytes, not just partial-message payload. R2 has only ever read
		// PartialCounts(), so every byte figure it has reported is the partial-message subset --
		// which cannot explain a 2.6 s penalty at 3.6% of the link, and is the obvious place for
		// the missing traffic to be hiding. Duplicates come from the same call because a mesh
		// carrying the same cell twice is bandwidth spent for nothing.
		dup, _, allRecv, allSent := nw.tracers[i].Counts()
		duplicates += dup
		allBytes += allRecv + allSent
		perNodeAll = append(perNodeAll, allRecv+allSent)
	}
	slices.Sort(delays)

	if droppedRPCs > 0 || undeliverableMsgs > 0 {
		t.Logf("    queue losses: %d dropped RPCs, %d undeliverable messages",
			droppedRPCs, undeliverableMsgs)
	}
	// Mesh sizes per axis. A row topic with every node on it fills gossipsub's D=8 mesh, while a
	// column topic whose members are ~4 of a node's peers cannot reach Dlo=6 -- so a row cell gets
	// twice the fanout per hop that a column cell does, which is a way for the row axis to dominate
	// the wire that has nothing to do with how many cells it wants.
	rowMesh, colMesh, colTopics, rowSubscribers := 0, 0, 0, 0
	for i, node := range nw.nodes {
		rowMesh += nw.tracers[i].MeshPeersInTopic(nw.topic)
		if subscribes(i) {
			rowSubscribers++
		}
		for _, held := range node.columns {
			colMesh += nw.tracers[i].MeshPeersInTopic(columnTopic(t, held))
			colTopics++
		}
	}
	// Divide by the subscriber count, not the node count: dividing row mesh peers by every node
	// while labelling the result "per member" understates it whenever membership is sparse.
	t.Logf("    mesh peers: row %.1f per subscriber (%d subscribers), column %.1f per (node, column)",
		safeDiv(float64(rowMesh), float64(rowSubscribers)), rowSubscribers,
		safeDiv(float64(colMesh), float64(colTopics)))
	// Attribution: which comparison in partialForPeer generated the traffic. Generated counts come
	// from the broadcaster at the point of decision; the tracer's counts are post-admission sends,
	// so the gap between them is the drop rate -- otherwise invisible, since the extension's send
	// returns no acceptance signal.
	genRowTotal, genColTotal := 0, 0
	rowByReason := map[string]int{}
	colByReason := map[string]int{}
	classActions := map[int]int{}
	perNodeColActions := make([]int, 0, len(nw.nodes))
	for i, node := range nw.nodes {
		rows, cols := node.broadcaster.ActionReasonCounts()
		nodeCol, nodeRow := 0, 0
		for _, n := range rows {
			genRowTotal += n
			nodeRow += n
		}
		for _, n := range cols {
			genColTotal += n
			nodeCol += n
		}
		classActions[r2CustodySize(i)] += nodeRow + nodeCol
		perNodeColActions = append(perNodeColActions, nodeCol)
		for _, want := range []struct {
			label  string
			reason blocks.ActionReason
		}{
			{"eager", blocks.ReasonEagerPush},
			{"cells", blocks.ReasonCells},
			{"first-meta", blocks.ReasonFirstMetadata},
			{"avail-add", blocks.ReasonAvailabilityAdd},
			{"req-add", blocks.ReasonRequestAdd},
			{"req-churn", blocks.ReasonRequestChurn},
		} {
			rowByReason[want.label] += partialdatacolumnbroadcaster.FoldActionReasons(rows, want.reason)
			colByReason[want.label] += partialdatacolumnbroadcaster.FoldActionReasons(cols, want.reason)
		}
	}
	if genRowTotal > 0 || genColTotal > 0 {
		t.Logf("    generated actions: %d row, %d column (row by reason: eager %d, cells %d, first-meta %d, avail-add %d, req-add %d, req-churn %d)",
			genRowTotal, genColTotal,
			rowByReason["eager"], rowByReason["cells"], rowByReason["first-meta"],
			rowByReason["avail-add"], rowByReason["req-add"], rowByReason["req-churn"])
		// The column axis's histogram, mirrored, plus the per-node spread. R16 needs both: the
		// dominant reason says what the push arm's column traffic is, and whether it concentrates
		// on the few nodes that recovered a row or spreads across every subscriber says whose it is.
		slices.Sort(perNodeColActions)
		t.Logf("    column by reason: eager %d, cells %d, first-meta %d, avail-add %d, req-add %d, req-churn %d; per node p50 %d, p95 %d, max %d",
			colByReason["eager"], colByReason["cells"], colByReason["first-meta"],
			colByReason["avail-add"], colByReason["req-add"], colByReason["req-churn"],
			perNodeColActions[len(perNodeColActions)/2],
			perNodeColActions[min(len(perNodeColActions)-1, len(perNodeColActions)*95/100)],
			perNodeColActions[len(perNodeColActions)-1])
		// Sends are traced after queue admission, so generated-minus-sent is what gossipsub
		// dropped. Reasons are counted per node for both directions of nothing -- generated is
		// inherently one-directional -- so compare against sends only.
		t.Logf("    generated %d row actions against %d row sends traced: %d unaccounted (drops or in-flight)",
			genRowTotal, rowSentRPCsTotal, genRowTotal-rowSentRPCsTotal)
	}

	t.Logf("    partial RPCs: %d on row topics (%d sent, %d received), %d on column topics (%.1f KB and %.1f KB per RPC)",
		rowRPCs, rowSentRPCsTotal, rowRecvRPCsTotal, colRPCs,
		safeDiv(float64(rowB)/1024, float64(rowRPCs)), safeDiv(float64(colB)/1024, float64(colRPCs)))
	if len(perNodeAll) > 0 {
		slices.Sort(perNodeAll)
		// NOT wire bytes: Counts() measures gossip Message.Data only (tracer.go:185) -- no
		// partial-message payload, no topic/group fields, no protobuf framing, no QUIC overhead,
		// ACKs or retransmissions. On these topics all data is partial messages, so this reads
		// zero, which is correct and says nothing about the wire.
		t.Logf("    full-message gossip payload (NOT wire bytes): %.1f MB total, per node mean %.0f KB, p95 %.0f KB, max %.0f KB; %d duplicate messages",
			float64(allBytes)/1e6,
			float64(allBytes)/float64(len(perNodeAll))/1024,
			float64(perNodeAll[min(len(perNodeAll)-1, len(perNodeAll)*95/100)])/1024,
			float64(perNodeAll[len(perNodeAll)-1])/1024,
			duplicates)
	}
	if len(perNode) > 0 {
		slices.Sort(perNode)
		total := 0
		for _, bytes := range perNode {
			total += bytes
		}
		t.Logf("    bytes per node (recv+sent): mean %.0f KB, p50 %.0f KB, p95 %.0f KB, max %.0f KB",
			float64(total)/float64(len(perNode))/1024,
			float64(perNode[len(perNode)/2])/1024,
			float64(perNode[min(len(perNode)-1, len(perNode)*95/100)])/1024,
			float64(perNode[len(perNode)-1])/1024)
	}

	// The member/non-member split, logged only when there is a split to report.
	if len(memberDelays) > 0 && len(outsiderDelays) > 0 {
		slices.Sort(memberDelays)
		slices.Sort(outsiderDelays)
		t.Logf("    row members: %d completions, p50 %v | outsiders: %d completions, p50 %v",
			len(memberDelays), memberDelays[len(memberDelays)/2].Round(time.Millisecond),
			len(outsiderDelays), outsiderDelays[len(outsiderDelays)/2].Round(time.Millisecond))
	}

	// Where a shortfall lands matters for reading it: a few columns missing everywhere is
	// different from a couple of nodes getting nothing, and only the second is a connectivity
	// story.
	if len(delays) < expected {
		short := make([]string, 0, r2Nodes())
		for i, node := range nw.nodes {
			done := 0
			for key := range seen {
				if key.node == i {
					done++
				}
			}
			if done < len(node.columns) {
				short = append(short, fmt.Sprintf("node %d: %d/%d", i, done, len(node.columns)))
			}
		}
		t.Logf("    shortfall by node: %v", short)
	}

	res := r2Result{arm: arm.name, seed: seed, completions: len(delays), expected: expected,
		partialB: partialB, rowB: rowB, colB: colB,
		rowActions: genRowTotal, rowRPCs: rowRPCs,
		availAdd: rowByReason["avail-add"], reqAdd: rowByReason["req-add"], reqChurn: rowByReason["req-churn"]}
	if len(classDelays) > 1 {
		res.classP50 = map[int]time.Duration{}
		res.classCompletions = map[int]int{}
		res.classActions = classActions
		res.classRowB = classRowB
		classes := make([]int, 0, len(classDelays))
		for class, classDelay := range classDelays {
			slices.Sort(classDelay)
			res.classP50[class] = classDelay[len(classDelay)/2]
			res.classCompletions[class] = len(classDelay)
			classes = append(classes, class)
		}
		slices.Sort(classes)
		for _, class := range classes {
			t.Logf("    custody %3d: %d completions, p50 %v; actions %d, row bytes %d",
				class, res.classCompletions[class], res.classP50[class].Round(time.Millisecond),
				classActions[class], classRowB[class])
		}
	}
	require.Equal(t, partialB, rowB+colB,
		"every partial byte should be attributed to one axis or the other")
	if len(delays) > 0 {
		res.p50 = delays[len(delays)/2]
		res.p95 = delays[min(len(delays)-1, len(delays)*95/100)]
		res.maxAt = delays[len(delays)-1]
	}
	t.Logf("  %s seed %x: %d/%d completions, p50 %v p95 %v max %v, partial bytes %d (row %d, column %d)",
		arm.name, seed, res.completions, expected,
		res.p50.Round(time.Millisecond), res.p95.Round(time.Millisecond), res.maxAt.Round(time.Millisecond),
		res.partialB, res.rowB, res.colB)

	return res
}

// nonCustodiedOf is nonCustodied for R2's column space. Kept separate from R9's because the two
// experiments have different column counts and a shared helper would hide that.
func nonCustodiedOf(columns []uint64) []uint64 {
	held := make(map[uint64]bool, len(columns))
	for _, columnIndex := range columns {
		held[columnIndex] = true
	}
	out := make([]uint64, 0, r2Columns)
	for columnIndex := range r2Columns {
		if !held[columnIndex] {
			out = append(out, columnIndex)
		}
	}

	return out
}

// safeDiv is a division that reports zero rather than NaN for an empty denominator, so a log line
// for an arm with no row traffic stays readable.
func safeDiv(a, b float64) float64 {
	if b == 0 {
		return 0
	}

	return a / b
}

// TestR2ArmsShareOneGraph pins the paired experiment's central invariant: every arm is measured on
// the same physical graph.
//
// This is a regression test for a real defect. `rowMembers` used to fold in "does this arm enable
// the row axis at all", and it was also what the connectivity graph was drawn from -- so `base`,
// whose membership was empty, got a *different* topology from `rows`. The comparison then attributed
// a topology difference to the row axis. The fix split the predicate in two: `rowGraphMembers` is
// arm-independent and builds the graph, `rowMembers` is arm-gated and drives subscription.
//
// Comparing edge *counts* would not have caught it, and that is measured rather than argued.
// Reintroducing the defect (`rowGraphMembers: nil`, so the fallback re-arms the arm-gated
// predicate) makes the sparse shape differ at **246 edges against 246** -- identical totals,
// different pairs, because the greedy cover pass adds the same number of edges to a different
// member set. The assertion is therefore set equality, and it names the first differing pair.
//
// The dense shape does not catch it, deliberately kept anyway as the default's control: with every
// node a member, the global draw already covers the row subnet and the cover pass adds nothing in
// either arm, so the graphs agree even with the bug present. The sparse shapes are where the
// experiment's realistic-custody regime lives, and they are the ones that fail.
//
// The fallback in mustEdges -- `rowGraphMembers` defaulting to `rowMembers` when unset -- is what
// keeps the bug available to a future paired experiment, so this test guards the call site rather
// than the drawing function.
func TestR2ArmsShareOneGraph(t *testing.T) {
	// Membership shapes that exercise the draw differently. Dense is the default, where every node
	// is a member and the cover edges are nearly free; sparse is where the original bug bit, since
	// an arm-dependent member set changes how much cover the greedy pass has to add.
	for _, shape := range []struct {
		name       string
		memberMod  string
		skipPropos string
	}{
		{name: "dense", memberMod: "1", skipPropos: "0"},
		{name: "sparse", memberMod: "4", skipPropos: "0"},
		{name: "sparse-no-proposer", memberMod: "4", skipPropos: "1"},
	} {
		t.Run(shape.name, func(t *testing.T) {
			t.Setenv("ROWDAS_R2_ROW_MEMBER_MOD", shape.memberMod)
			t.Setenv("ROWDAS_R2_ROW_SKIP_PROPOSER", shape.skipPropos)

			arms := []r2Arm{
				{name: "base", rows: false},
				{name: "rows", rows: true},
				{name: "rows+push", rows: true, push: true},
				{name: "rows-delayed", rows: true, delay: 250 * time.Millisecond},
			}

			// Several seeds, because a single draw could agree by luck -- especially in the dense
			// shape, where the two member sets differ least.
			for _, seed := range []uint64{0x8371, 0x1, 0xbeef} {
				want := drawR2Edges(t, arms[0], seed)
				require.NotEqual(t, 0, len(want), "graph should not be empty")

				for _, arm := range arms[1:] {
					got := drawR2Edges(t, arm, seed)
					requireSameEdgeSet(t, want, got, arms[0].name, arm.name, seed)
				}
			}
		})
	}
}

// drawR2Edges runs the real option-building and graph-drawing path for one arm.
func drawR2Edges(t *testing.T, arm r2Arm, seed uint64) []gossipsim.Edge {
	t.Helper()

	opts, _, _ := r2NetworkOpts(t, arm, seed)

	// The degree newNetwork would compute, reproduced here because it is derived inside that
	// function from the mesh parameters rather than passed in. If that derivation changes, this
	// test draws a different graph than the experiment does -- but it still compares arms against
	// each other on identical inputs, which is the property under test.
	gsp := gossipsim.MeshParams(gossipsim.NewOverrides().MeshDegree, gossipsim.NewOverrides().IDontWantThreshold)
	degree := min(gsp.D+2, opts.n-1)
	if opts.graphDegree > 0 {
		degree = opts.graphDegree
	}
	if degree%2 == 1 && opts.n%2 == 1 {
		degree--
	}

	return mustEdges(t, opts, opts.n, degree, seed)
}

// requireSameEdgeSet compares two graphs as undirected sets, reporting the first pair that differs.
func requireSameEdgeSet(t *testing.T, want, got []gossipsim.Edge, wantName, gotName string, seed uint64) {
	t.Helper()

	key := func(e gossipsim.Edge) [2]int {
		if e.A > e.B {
			return [2]int{e.B, e.A}
		}

		return [2]int{e.A, e.B}
	}

	wantSet := make(map[[2]int]struct{}, len(want))
	for _, e := range want {
		wantSet[key(e)] = struct{}{}
	}
	gotSet := make(map[[2]int]struct{}, len(got))
	for _, e := range got {
		gotSet[key(e)] = struct{}{}
	}

	for k := range wantSet {
		if _, ok := gotSet[k]; !ok {
			t.Fatalf("seed %#x: edge %d-%d is in arm %q but not in arm %q "+
				"(%d vs %d edges) -- the arms are not on the same graph",
				seed, k[0], k[1], wantName, gotName, len(wantSet), len(gotSet))
		}
	}
	for k := range gotSet {
		if _, ok := wantSet[k]; !ok {
			t.Fatalf("seed %#x: edge %d-%d is in arm %q but not in arm %q "+
				"(%d vs %d edges) -- the arms are not on the same graph",
				seed, k[0], k[1], gotName, wantName, len(gotSet), len(wantSet))
		}
	}
}
