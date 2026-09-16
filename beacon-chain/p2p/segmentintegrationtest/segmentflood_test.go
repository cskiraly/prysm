package segmentintegrationtest

// The unknown-group flood -- notes/adversarial-cell-design.md, experiment A.
//
// What it attacks. A receiver cannot authenticate a segment group before it holds the slot's
// commitments, so between a candidate's arrival and that installation it can only buffer. The
// bounded-admission queue exists for exactly that window; this is the injection that drives it to
// its caps while an honest group diffuses, so `dropped`, `evicted` and `peak queue` -- zero across
// every honest cell ever run -- are exercised rather than asserted.
//
// What the adversary is. An ordinary publisher. It forges nothing: a group id is committed or it is
// not, and nothing on the wire asserts otherwise, so this is resource exhaustion and never
// authentication bypass. Its candidates carry the same wire shape as an honest segment (SSZ offset,
// structural prefix, snappy) under a fabricated (root, group) pair, and each pair is fresh so the
// seen cache cannot suppress them.
//
// The two regimes, and why both must be run. With the slot's commitments installed a fabricated
// group is refused outright and costs a parse -- the flood is a non-event, which is the point of
// modelling the bid at all. Before they install, nothing distinguishes it from an honest early
// arrival and it must be buffered. So the same injection is expected to move the counters in one
// regime and leave them at zero in the other, and a run that fails *either* half is a wiring
// failure rather than evidence about the design. SEGMENT_ADV_AFTER_MS aims the flood at either.
//
// What this does not model. The validator here never parses a candidate's body, so the flood is
// well-formed as far as anything in this cell inspects, and its cost to the receiver is a parse and
// a queue slot rather than proof verification. That understates neither side: an attacker builds
// its own Merkle tree, so valid proofs against a fabricated group are as cheap for it as invalid
// ones. Real validation CPU is F6 and is not built.

import (
	"bytes"
	"context"
	"encoding/binary"
	"maps"
	"os"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/OffchainLabs/prysm/v7/beacon-chain/p2p/encoder"
	"github.com/OffchainLabs/prysm/v7/consensus-types/primitives"
	ethpb "github.com/OffchainLabs/prysm/v7/proto/prysm/v1alpha1"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
)

// floodConfig is one cell's adversary. The zero value is no adversary at all.
type floodConfig struct {
	nodes    int           // how many adversarial publishers
	groups   int           // distinct fabricated (root, group) pairs, rotated
	bytes    int           // candidate payload size
	interval time.Duration // gap between one adversary's publishes
	after    time.Duration // delay from publish before the flood starts
}

func (c floodConfig) enabled() bool { return c.nodes > 0 }

// floodConfigFromEnv reads the adversary's knobs. Disabled unless SEGMENT_ADV_NODES is set, so
// every existing cell is unaffected.
//
// The defaults describe a *line-rate* adversary at the standing operating point: one node, eight
// rotated pairs (the queue's maxRoots, so the root-count bound is reached rather than approached),
// candidates the size of an honest segment, and an interval short against delta_install. Rate is
// deliberately not swept -- an attacker sends as fast as it can, so nothing is learned by slowing
// it, and the variable that decides the outcome is the window, not the rate.
func floodConfigFromEnv(t *testing.T) floodConfig {
	v := os.Getenv("SEGMENT_ADV_NODES")
	if v == "" {
		return floodConfig{}
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 0 {
		t.Fatalf("bad SEGMENT_ADV_NODES %q", v)
	}
	c := floodConfig{
		nodes:    n,
		groups:   8,
		bytes:    segmentSizeBytes(t),
		interval: time.Millisecond,
		after:    0,
	}
	c.groups = envPositive(t, "SEGMENT_ADV_GROUPS", c.groups)
	c.bytes = envPositive(t, "SEGMENT_ADV_BYTES", c.bytes)
	c.interval = envDuration("SEGMENT_ADV_INTERVAL_MS", c.interval)
	c.after = envDuration("SEGMENT_ADV_AFTER_MS", c.after)
	if c.interval <= 0 {
		// A publish loop with no gap never yields to the bubble's clock, so virtual time stops
		// and the cell hangs rather than flooding. The rate limit is the harness's, not the
		// adversary's, and it is documented rather than silently applied.
		t.Fatal("SEGMENT_ADV_INTERVAL_MS must be positive: a zero gap stalls the virtual clock")
	}
	return c
}

func envPositive(t *testing.T, name string, def int) int {
	v := os.Getenv(name)
	if v == "" {
		return def
	}
	k, err := strconv.Atoi(v)
	if err != nil || k <= 0 {
		t.Fatalf("bad %s %q", name, v)
	}
	return k
}

// floodKey builds the nth fabricated claim: its own root and its own group, neither committed by
// any block. Derived rather than random so a cell stays reproducible under a fixed seed, and
// domain-separated from cellBlockRoot so an adversary can never collide with the honest root.
func floodKey(slot primitives.Slot, n int, index uint32) segKey {
	var seed [40]byte
	copy(seed[:], "SEGMENT_ADVERSARY_V1")
	binary.LittleEndian.PutUint64(seed[24:], uint64(n))
	binary.LittleEndian.PutUint64(seed[32:], uint64(slot))
	h := geoMix(binary.LittleEndian.Uint64(seed[24:]) ^ 0x5bf0_3635_ca8d_9a11)
	k := segKey{slot: uint64(slot), index: index}
	binary.LittleEndian.PutUint64(k.blockRoot[:], h)
	binary.LittleEndian.PutUint64(k.blockRoot[8:], h^0xdead_beef_0bad_f00d)
	binary.LittleEndian.PutUint64(k.group[:], h^0x1234_5678_9abc_def0)
	binary.LittleEndian.PutUint64(k.group[8:], h^0x0f0f_0f0f_0f0f_0f0f)
	return k
}

// floodMessage encodes one candidate through the same gossip encoder the honest arms use, so the
// wire shape the id function reads is identical and the flood is refused for what it claims rather
// than for how it is framed.
func floodMessage(k segKey, size int) ([]byte, error) {
	seg := &ethpb.ExecutionPayloadSegment{
		Segment: prependSegPrefix(k, mainnetLikePayload(size, uint64(k.index)+7)),
	}
	var buf bytes.Buffer
	enc := encoder.SszNetworkEncoder{}
	if _, err := enc.EncodeGossip(&buf, seg); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// floodStats is what the cell reports about the injection itself. Without it a null result cannot
// be told from an injection that never ran -- the failure that looks exactly like success.
type floodStats struct {
	published atomic.Int64
	failed    atomic.Int64
}

// adversaryNode reports which node index the a'th adversary occupies. One definition, so the
// publish loop and the attribution below cannot drift apart.
func adversaryNode(n, a int) int { return n - 1 - a }

// adversaryPeers is the attacker set, by peer id, for attributing losses.
//
// Losses are attributed to the *immediate sender*, which is only meaningful because the admission
// rule never relays what it defers or refuses: a fabricated candidate therefore reaches the
// adversary's own mesh neighbours and stops. That containment is worth checking rather than
// assuming -- if a flood candidate ever arrives from an honest relay, this attribution is wrong and
// so is any fairness conclusion drawn from it.
func adversaryPeers(nw *simNetwork, cfg floodConfig) map[peer.ID]bool {
	if !cfg.enabled() {
		return nil
	}
	adv := make(map[peer.ID]bool, cfg.nodes)
	for a := range cfg.nodes {
		adv[nw.Hosts[adversaryNode(nw.Len(), a)].ID()] = true
	}
	return adv
}

// reportExposure prints the dose-response curve: what a node suffered against how many attackers
// actually reached it.
//
// This is the shape the aggregate counters destroy. A fabricated candidate is never relayed, so a
// node is pressured only by attackers pushing to it directly, and with mesh degree D and attacker
// share phi the mean dose is about D*phi -- below one for any plausible share. The mean node is
// therefore untouched and the damage lives entirely in the tail that drew two or three, so a
// per-cell average answers a question nobody asked. One cell contains the whole curve.
//
// Completion comes from the caller because the admission queue does not know it; nodes that never
// completed are reported as their own column rather than dropped, since censoring is the outcome
// that matters most and is exactly what an average hides.
func reportExposure(t *testing.T, admits []*segmentAdmission, adv map[peer.ID]bool, self []peer.ID, completed func(int) bool) {
	t.Helper()
	if len(adv) == 0 {
		return
	}
	type bucket struct {
		nodes, censored, lostHonest, dropped, deferred int
		peaks                                          []int
	}
	byDose := map[int]*bucket{}
	for i, a := range admits {
		if a == nil || adv[self[i]] {
			continue // an attacker's own queue is not a victim's
		}
		k := a.exposure()
		b := byDose[k]
		if b == nil {
			b = &bucket{}
			byDose[k] = b
		}
		def, drop, lost, peak := a.outcome()
		b.nodes++
		b.deferred += def
		b.dropped += drop
		b.lostHonest += lost
		b.peaks = append(b.peaks, peak)
		if !completed(i) {
			b.censored++
		}
	}
	doses := slices.Sorted(maps.Keys(byDose))
	t.Logf("        exposure curve (attackers reaching a node -> what it cost that node):")
	for _, k := range doses {
		b := byDose[k]
		slices.Sort(b.peaks)
		t.Logf("          %d attacker(s): %3d nodes, censored %d, honest losses %d, dropped %d, deferred %d, peak held p50 %dKiB max %dKiB",
			k, b.nodes, b.censored, b.lostHonest, b.dropped, b.deferred,
			b.peaks[len(b.peaks)/2]>>10, b.peaks[len(b.peaks)-1]>>10)
	}
}

// checkFlood fails the cell early if the adversary cannot run, so a misconfiguration surfaces as a
// setup error rather than as a clean-looking null result.
func checkFlood(t *testing.T, cfg floodConfig, n int, gates *segmentGates) {
	t.Helper()
	if !cfg.enabled() {
		return
	}
	if cfg.nodes >= n {
		t.Fatalf("SEGMENT_ADV_NODES %d needs fewer nodes than the cell's %d", cfg.nodes, n)
	}
	// A flood in shadow mode is not a weaker experiment, it is a different one. Shadow accepts
	// every candidate after recording what it would have done, and gossipsub relays what is
	// accepted -- so fabricated candidates spread network-wide instead of stopping at the
	// adversary's neighbours. The queue then fills from honest relays, and the loss attribution in
	// adversaryPeers reports the opposite of the truth. Refuse rather than measure that.
	if len(gates.admits) > 0 && gates.admits[0].shadow {
		t.Fatal("a flood cell must enforce admission (set SEGMENT_GATE_SHADOW=0, or SEGMENT_ADMIT_SHADOW=0): " +
			"in shadow mode every fabricated candidate is accepted and relayed")
	}
}

// runFlood publishes fabricated candidates until ctx ends.
//
// Every candidate is minted fresh, and that is forced rather than chosen: gossipsub keys its seen
// cache by message id, so a resent candidate is suppressed at the first hop and never reaches a
// validator at all. The first version of this flood resent eight messages and moved no counter --
// the seen cache is a complete defence against a repeating flood, which is worth knowing because it
// means the cheap attack is not the one to bound. What costs the victim is *distinct* ids, so the
// adversary pays an encode per candidate and the index is advanced every publish.
//
// Adversaries are taken from the end of the node range so they cannot collide with the builder
// (node 0), and each publishes through its own topic handle, which is what makes them ordinary
// participants rather than a harness back door.
func runFlood(ctx context.Context, nw *simNetwork, topics []*pubsub.Topic,
	cfg floodConfig, slot primitives.Slot, wg *sync.WaitGroup) *floodStats {
	stats := &floodStats{}
	if !cfg.enabled() {
		return stats
	}
	for a := range cfg.nodes {
		node := nw.Len() - 1 - a
		wg.Add(1)
		go func(node, a int) {
			defer wg.Done()
			select {
			case <-time.After(cfg.after):
			case <-ctx.Done():
				return
			}
			for i := 0; ; i++ {
				select {
				case <-ctx.Done():
					return
				case <-time.After(cfg.interval):
				}
				// Rotate the group so the root-count bound is attacked too, and advance the index
				// so every candidate is a distinct id.
				k := floodKey(slot, a*cfg.groups+i%cfg.groups, uint32(i)) // lint:ignore uintcast -- loop counter.
				msg, err := floodMessage(k, cfg.bytes)
				if err != nil {
					stats.failed.Add(1)
					continue
				}
				if err := topics[node].Publish(ctx, msg); err != nil {
					stats.failed.Add(1)
					continue
				}
				stats.published.Add(1)
			}
		}(node, a)
	}
	return stats
}
