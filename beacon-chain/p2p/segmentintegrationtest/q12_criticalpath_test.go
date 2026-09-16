package segmentintegrationtest

// Q12 diagnostic: what is actually on the critical path?
//
// Two hypotheses explain the measured completion times equally well, because both are
// bytes/bandwidth and therefore co-scale across every rate we swept:
//
//	H-pub   the publisher's redundant fan-out. It uploads D copies (6 MB, ~972 ms at 50 Mbps),
//	        and observed completion was 1.029 s.
//	H-node  aggregate per-node traffic. Every node receives 4.19 MB for a 760 KB payload -- 5.5
//	        copies -- which is ~670 ms of downlink each, plus comparable uplink to forward.
//
// Distinguishing them by rate is impossible; they move together. But the tracer records every
// send and receive with a timestamp and a peer identity, so the actual chain of custody for the
// last-needed segment can be reconstructed. That names the bottleneck instead of inferring it.
//
// Note against H-pub: the default RoundRobinMessageIDScheduler yields one RPC per message id per
// pass, so the publisher's *first* pass already emits every segment exactly once -- 760 KB, ~122 ms
// at 50 Mbps. If the publisher were binding, the critical segment would have to come from a late
// pass. That is what this measures.

import (
	"context"
	"sort"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/OffchainLabs/prysm/v7/config/params"
	"github.com/OffchainLabs/prysm/v7/consensus-types/primitives"
	"github.com/OffchainLabs/prysm/v7/container/segments"
	"github.com/OffchainLabs/prysm/v7/crypto/bls"
	"github.com/OffchainLabs/prysm/v7/testing/require"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
)

// arrival is the first time a node saw a particular message, and who sent it.
type arrival struct {
	at   time.Duration
	from int // node index, -1 if unknown
}

func TestQ12CriticalPath(t *testing.T) {
	requireSlowTests(t)
	const n = 150
	params.SetupTestConfigCleanup(t)
	sk, err := bls.RandKey()
	require.NoError(t, err)
	payload := mainnetLikePayload(1<<20, 11)
	segs := segmentedArm(t, payload, segments.DefaultSegmentSize, sk, primitives.Slot(2048))

	synctest.Test(t, func(t *testing.T) {
		tracers := make([]*recordingTracer, n)
		for i := range tracers {
			tracers[i] = newRecordingTracer()
		}
		nw, stop := newSimNetwork(t, networkConfig{
			links: uniformLinks(n, defaultRate),
			edges: meshGraph(t, n),
			perNodeOpts: func(i int) []pubsub.Option {
				return []pubsub.Option{pubsub.WithRawTracer(tracers[i])}
			},
		})
		topics, subs, cancelSubs := joinAndSubscribeAll(t, nw, false)
		meshSummary := summariseMesh(awaitMesh(t, tracers, false))
		var wg sync.WaitGroup
		defer stop()
		defer wg.Wait()
		defer cancelSubs()

		idx := make(map[peer.ID]int, n)
		for i, h := range nw.Hosts {
			idx[h.ID()] = i
		}

		ctx, cancel := context.WithTimeout(context.Background(), networkOpTimeout)
		defer cancel()
		done := make(chan struct{}, n)
		for i := 1; i < n; i++ {
			wg.Add(1)
			go func(node int) {
				defer wg.Done()
				seen := make(map[digest]bool, len(segs.msgs))
				for len(seen) < len(segs.msgs) {
					msg, err := subs[node].Next(ctx)
					if err != nil {
						return
					}
					if _, ok := segs.known[digestOf(msg.Data)]; ok {
						seen[digestOf(msg.Data)] = true
					}
				}
				done <- struct{}{}
			}(i)
		}

		start := time.Now()
		var batch pubsub.MessageBatch
		for _, m := range segs.msgs {
			require.NoError(t, topics[0].AddToBatch(ctx, &batch, m))
		}
		require.NoError(t, nw.Pubsubs[0].PublishBatch(&batch))
		for range n - 1 {
			<-done
		}
		total := time.Since(start)
		synctest.Wait()

		// Reconstruct first-arrival per (node, segment).
		first := make([]map[digest]arrival, n)
		for i := range n {
			first[i] = map[digest]arrival{}
			tracers[i].Mu.Lock()
			for _, e := range tracers[i].Recvs {
				if _, ok := segs.known[e.What]; !ok {
					continue
				}
				if _, seen := first[i][e.What]; seen {
					continue
				}
				from, ok := idx[e.From]
				if !ok {
					from = -1
				}
				first[i][e.What] = arrival{at: e.At.Sub(start), from: from}
			}
			tracers[i].Mu.Unlock()
		}

		// Publisher send times and their rank, i.e. which round-robin pass each segment went out in.
		pubSend := map[digest]time.Duration{}
		pubRank := map[digest]int{}
		tracers[0].Mu.Lock()
		rank := 0
		for _, s := range tracers[0].Sends {
			if _, ok := segs.known[s.What]; !ok {
				continue
			}
			rank++
			if _, seen := pubSend[s.What]; !seen {
				pubSend[s.What] = s.At.Sub(start)
				pubRank[s.What] = rank
			}
		}
		pubTotalSends := rank
		tracers[0].Mu.Unlock()

		// The node that finished last, and the segment it finished on.
		lastNode, lastAt := -1, time.Duration(-1)
		for i := 1; i < n; i++ {
			var nodeDone time.Duration
			if len(first[i]) < len(segs.msgs) {
				continue
			}
			for _, a := range first[i] {
				if a.at > nodeDone {
					nodeDone = a.at
				}
			}
			if nodeDone > lastAt {
				lastNode, lastAt = i, nodeDone
			}
		}
		require.Equal(t, true, lastNode > 0, "no node completed")

		var critDigest digest
		for d, a := range first[lastNode] {
			if a.at == lastAt {
				critDigest = d
			}
		}

		t.Logf("n=%d, %d Mbps, %v one-way, connectivity degree %d (mesh %s). Completion (all nodes): %v",
			n, defaultRate/1_000_000, defaultLatency, connectivityDegree(t, n), meshSummary,
			total.Round(time.Millisecond))
		t.Logf("publisher emitted %d segment sends total (%d segments x D); "+
			"first pass ends around send #%d", pubTotalSends, len(segs.msgs), len(segs.msgs))
		t.Logf("last node to complete: %d at %v, waiting on segment %d",
			lastNode, lastAt.Round(time.Millisecond), segs.known[critDigest])
		t.Logf("  that segment left the publisher at %v, as publisher send #%d of %d  (pass %d of D)",
			pubSend[critDigest].Round(time.Millisecond), pubRank[critDigest], pubTotalSends,
			1+(pubRank[critDigest]-1)/len(segs.msgs))

		// Walk the chain of custody for the critical segment back to the publisher.
		t.Logf("  chain of custody, newest first:")
		cur, hops := lastNode, 0
		for cur > 0 && hops < 20 {
			a, ok := first[cur][critDigest]
			if !ok {
				t.Logf("    node %d: no record", cur)
				break
			}
			var held time.Duration
			if a.from == 0 {
				held = pubSend[critDigest]
			} else if a.from > 0 {
				held = first[a.from][critDigest].at
			}
			t.Logf("    node %-4d received at %8v from node %-4d (which had it at %8v; gap %v)",
				cur, a.at.Round(time.Millisecond), a.from, held.Round(time.Millisecond),
				(a.at - held).Round(time.Millisecond))
			cur = a.from
			hops++
		}

		// Where did time go for the last node: its own downlink, or waiting?
		_, _, rxBytes, txBytes := tracers[lastNode].Counts()
		rxTime := time.Duration(float64(rxBytes*8) / float64(defaultRate) * float64(time.Second))
		txTime := time.Duration(float64(txBytes*8) / float64(defaultRate) * float64(time.Second))
		t.Logf("last node's own link occupancy: rx %v (%d B), tx %v (%d B) against completion %v",
			rxTime.Round(time.Millisecond), rxBytes, txTime.Round(time.Millisecond), txBytes,
			lastAt.Round(time.Millisecond))

		// Distribution of completion, so a single tail node is not mistaken for the norm.
		var all []time.Duration
		for i := 1; i < n; i++ {
			if len(first[i]) < len(segs.msgs) {
				continue
			}
			var d time.Duration
			for _, a := range first[i] {
				if a.at > d {
					d = a.at
				}
			}
			all = append(all, d)
		}
		sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
		pct := func(p int) time.Duration { return all[(len(all)-1)*p/100] }
		t.Logf("completion distribution: p0 %v p50 %v p90 %v p99 %v p100 %v",
			pct(0).Round(time.Millisecond), pct(50).Round(time.Millisecond),
			pct(90).Round(time.Millisecond), pct(99).Round(time.Millisecond),
			pct(100).Round(time.Millisecond))

		// Publisher send span: if its last useful send lands long before completion, H-pub is out.
		var pubFirst, pubLast time.Duration
		pubFirst = time.Hour
		for _, at := range pubSend {
			if at < pubFirst {
				pubFirst = at
			}
			if at > pubLast {
				pubLast = at
			}
		}
		t.Logf("publisher's FIRST-copy sends span %v..%v; all %d segments were out once by %v",
			pubFirst.Round(time.Millisecond), pubLast.Round(time.Millisecond),
			len(pubSend), pubLast.Round(time.Millisecond))
		t.Logf("=> if that is far below p50 completion (%v), the publisher's fan-out is NOT binding",
			pct(50).Round(time.Millisecond))
	})
}
