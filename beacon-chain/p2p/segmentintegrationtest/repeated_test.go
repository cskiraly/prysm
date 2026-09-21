package segmentintegrationtest

// Repeated-payload harness.
//
// Every other mesh test publishes one payload and tears down. Real block propagation is one
// payload per slot, forever, on a standing peer set, where each node reaches a new block
// already holding warm per-peer estimates and scores. This drives a *sequence* of payload
// diffusions on one standing network so router state (per-peer response-time estimates, the
// adaptive hedge, scores) accumulates across slots -- the only regime in which the (N, q, g)
// per-peer quantile timer can be estimated at all (Q30: one payload is ~2 heartbeats, no
// convergence).
//
// Two things make it a *realism* test rather than a static replay: the publisher rotates
// (a different proposer each slot, so the diffusion tree re-roots and the samples a node
// collects vary), and each slot carries a fresh payload (distinct digests, no cross-slot
// dedup). Latency heterogeneity (SEGMENT_LATENCY_MODEL=geo) and background traffic
// (SEGMENT_BG_*) compose as usual. What it does NOT yet model: churn (peers persist for the
// whole run) -- recorded as the remaining realism gap.
//
// Knobs: SEGMENT_SLOTS (default 8), SEGMENT_SLOT_INTERVAL_MS (default 1000). Reuses the mesh
// size / degree / arm / discipline / adaptive-hedge knobs.

import (
	"context"
	"os"
	"strconv"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/OffchainLabs/prysm/v7/config/params"
	"github.com/OffchainLabs/prysm/v7/consensus-types/primitives"
	"github.com/OffchainLabs/prysm/v7/crypto/bls"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	simlibp2p "github.com/libp2p/go-libp2p/x/simlibp2p"
	"github.com/marcopolo/simnet"
)

func TestRepeatedPayload(t *testing.T) {
	if os.Getenv("SEGMENT_SLOW_TESTS") == "" {
		t.Skip("repeated-payload harness is opt-in; set SEGMENT_SLOW_TESTS")
	}
	n := 100
	if v := os.Getenv("SEGMENT_MESH_SIZES"); v != "" {
		k, err := strconv.Atoi(v) // single size only for this harness
		if err != nil {
			t.Fatalf("bad SEGMENT_MESH_SIZES %q", v)
		}
		n = k
	}
	slots := 8
	if v := os.Getenv("SEGMENT_SLOTS"); v != "" {
		k, err := strconv.Atoi(v)
		if err != nil || k <= 0 {
			t.Fatalf("bad SEGMENT_SLOTS %q", v)
		}
		slots = k
	}
	slotInterval := envDuration("SEGMENT_SLOT_INTERVAL_MS", time.Second)
	links := meshLinks(t, n, defaultRate)
	phaseR := 2
	if v := os.Getenv("SEGMENT_A_PHASE_R"); v != "" {
		k, err := strconv.Atoi(v)
		if err != nil || k < 0 {
			t.Fatalf("bad SEGMENT_A_PHASE_R %q", v)
		}
		phaseR = k
	}
	payloadLen := 1 << 20
	deadline := failureDeadline(t)

	params.SetupTestConfigCleanup(t)
	sk, err := bls.RandKey()
	if err != nil {
		t.Fatal(err)
	}

	// Pre-build every slot's segmentation up front (outside the bubble): distinct payloads
	// give distinct digests, so a node's cross-slot digest set never aliases.
	arms := make([]wireArm, slots)
	for s := range arms {
		arms[s] = segmentedArm(t, mainnetLikePayload(payloadLen, uint64(1000+s)), segmentSizeBytes(t), sk, primitives.Slot(2048+s))
	}

	var latency simlibp2p.LatencyFunc
	latSummary := "uniform"
	if os.Getenv("SEGMENT_LATENCY_MODEL") == "geo" {
		latency, latSummary = geoLatency(t, meshGraphSeed(t), n)
	}

	synctest.Test(t, func(t *testing.T) {
		nwLatency := latency
		if nwLatency == nil {
			nwLatency = simnet.StaticLatency(defaultLatency)
		}
		nw, stop := newSimNetwork(t, networkConfig{
			links:   links,
			edges:   meshGraph(t, n),
			latency: nwLatency,
			perNodeOpts: func(int) []pubsub.Option {
				return phasePubsubOpts(t, phaseR)
			},
		})
		var wg sync.WaitGroup
		defer stop()
		defer wg.Wait()

		ctx, cancel := context.WithTimeout(context.Background(), networkOpTimeout)
		defer cancel()

		// One shared segment topic; every node subscribes once and drains for the whole run,
		// recording each distinct payload-bearing digest's arrival time. Persistent drain is
		// required: a node that stopped reading between slots would overflow its buffer, which
		// reads as loss.
		topicStr := segmentTopic()
		topics := make([]*pubsub.Topic, n)
		seen := make([]map[digest]time.Time, n)
		var mu sync.Mutex
		known := map[digest]bool{}
		for _, a := range arms {
			for d := range a.known {
				known[d] = true
			}
		}
		for i, ps := range nw.Pubsubs {
			th, err := ps.Join(topicStr)
			if err != nil {
				t.Fatal(err)
			}
			sub, err := th.Subscribe(pubsub.WithBufferSize(subscriptionBuffer))
			if err != nil {
				t.Fatal(err)
			}
			topics[i] = th
			seen[i] = map[digest]time.Time{}
			node := i
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					msg, err := sub.Next(ctx)
					if err != nil {
						return
					}
					d := digestOf(msg.Data)
					if !known[d] {
						continue
					}
					mu.Lock()
					if _, ok := seen[node][d]; !ok {
						seen[node][d] = time.Now()
					}
					mu.Unlock()
				}
			}()
		}
		time.Sleep(meshFormation)
		synctest.Wait()

		t.Logf("repeated-payload: n=%d slots=%d interval=%v latency=%s degree=%d",
			n, slots, slotInterval, latSummary, connectivityDegree(t, n))

		// By default the publisher rotates so no one node's uplink dominates. Fixing it
		// (SEGMENT_FIXED_PUBLISHER=1) isolates cold-vs-hot warming from publisher identity:
		// the same node publishes every slot, so slot 0 vs later slots differ only in how
		// warm the estimators and congestion windows are.
		fixedPub := os.Getenv("SEGMENT_FIXED_PUBLISHER") != ""
		for s := 0; s < slots; s++ {
			arm := arms[s]
			pub := s % n
			if fixedPub {
				pub = 0
			}
			start := time.Now()
			var batch pubsub.MessageBatch
			for _, m := range arm.msgs {
				if err := topics[pub].AddToBatch(ctx, &batch, m); err != nil {
					t.Fatal(err)
				}
			}
			if err := nw.Pubsubs[pub].PublishBatch(&batch); err != nil {
				t.Fatal(err)
			}

			// Advance virtual time in steps until every non-publisher holds the slot's set or
			// the deadline passes; completion times come from the recorded arrivals, so the
			// step granularity bounds only when we stop, not the measured times.
			need := arm.completeAt()
			var comp []time.Duration
			for {
				synctest.Wait()
				comp = comp[:0]
				done := 0
				mu.Lock()
				for i := 0; i < n; i++ {
					if i == pub {
						continue
					}
					last := time.Duration(-1)
					have := 0
					for d := range arm.known {
						if at, ok := seen[i][d]; ok {
							have++
							if e := at.Sub(start); e > last {
								last = e
							}
						}
					}
					if have >= need {
						comp = append(comp, last)
						done++
					}
				}
				mu.Unlock()
				if done >= n-1 || time.Since(start) >= networkOpTimeout-time.Second {
					break
				}
				time.Sleep(50 * time.Millisecond)
			}
			t.Logf("  slot %2d pub %-3d %3d/%d  %s", s, pub, len(comp), n-1,
				completionStats(comp, n-1, deadline))
			// Space slots so timers/estimators see inter-slot gaps, as real slots do.
			if rem := slotInterval - time.Since(start); rem > 0 {
				time.Sleep(rem)
			}
			synctest.Wait()
		}
	})
}
