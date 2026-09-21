package segmentintegrationtest

// Q10: does the configuration we actually ship help?
//
// Every timing arm before this compared segmented gossip against whole-message gossip, which
// describes a protocol we do not ship. `publishEnvelopeSegments` runs *after*
// `vs.P2P.Broadcast(ctx, signed)`, so enabling the flag sends the whole envelope **and then**
// the segments -- deliberately, so a peer not subscribed to the segment topic still receives the
// payload and the feature can be switched off network-wide without stranding anyone.
//
// The two go to different topics, hence different gossipsub meshes, but share one per-peer
// stream and outbound queue. So on the first hop the whole envelope is queued ahead of every
// segment: the sender transmits roughly 2x the bytes, and the segments start late.
//
// PREDICTION, fixed before running.
//
// Let W be the whole message's wire size, wmax the largest segment's, R the per-node uplink.
// Whole-only costs about h*(W/R). Segments behind a whole envelope cost about 2W/R to clear the
// first hop, then pipeline at wmax/R per hop. With W/R = 434 ms and a measured marginal hop near
// 48 ms, the crossover is where 2W/R + (h-1)*wmax/R < h*W/R, which is h > 2.1. So:
//
//   - h=1, h=2: the shipped path is SLOWER to a usable envelope than whole-only. The segments
//     are pure added traffic and the whole envelope arrives first regardless.
//   - h=3: roughly a tie.
//   - h>=4: the shipped path wins, because pipelining recovers the doubled first hop.
//   - At every h it is worse than segmented-only, and it always moves about twice the bytes.
//
// If that holds, the mechanism works but the ordering is wrong: segments should precede the
// whole envelope, or replace it for peers that subscribe to the segment topic.
//
// FALSIFIES: if the shipped path matches segmented-only, the two topics are not in fact sharing
// a send queue and the concern is unfounded. If it never beats whole-only even at h=5, the
// doubled first hop is not recoverable and the feature cannot help as wired at all.

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/OffchainLabs/prysm/v7/beacon-chain/p2p"
	"github.com/OffchainLabs/prysm/v7/beacon-chain/p2p/encoder"
	"github.com/OffchainLabs/prysm/v7/config/params"
	"github.com/OffchainLabs/prysm/v7/consensus-types/primitives"
	"github.com/OffchainLabs/prysm/v7/container/segments"
	"github.com/OffchainLabs/prysm/v7/crypto/bls"
	"github.com/OffchainLabs/prysm/v7/testing/require"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
)

// envelopeTopic is the real whole-envelope topic. Using the real string matters: gossipsub keeps
// a separate mesh per topic, and that separation is part of what is being measured. The payload
// carried on it is the same wrapper the segment topic uses, so encoding is identical between arms
// and cannot confound the timing.
func envelopeTopic() string {
	digest := params.ForkDigest(0)
	return fmt.Sprintf(p2p.ExecutionPayloadEnvelopeTopicFormat, digest) +
		encoder.SszNetworkEncoder{}.ProtocolSuffix()
}

// shippedResult is what one cell of the grid produced.
type shippedResult struct {
	usable     time.Duration // first moment a complete envelope is available
	viaWhole   time.Duration // when the whole envelope arrived, zero if never sent
	viaSegment time.Duration // when reassembly completed, zero if never sent
	bytesSent  int           // gossip payload bytes the publisher put on the wire
	drops      int
}

// runShipped drives one arm across a line and reports when a usable envelope first exists.
//
// sendWhole and sendSegments select the arm. Both true reproduces production, in production's
// order: whole first, then segments.
func runShipped(t *testing.T, hops int, whole wireArm, segs wireArm, sendWhole, sendSegments, segmentsFirst bool) shippedResult {
	t.Helper()
	nodes := hops + 1
	tracers := make([]*recordingTracer, nodes)
	for i := range tracers {
		tracers[i] = newRecordingTracer()
	}
	nw, stop := newSimNetwork(t, networkConfig{
		links: uniformLinks(nodes, defaultRate),
		edges: line(nodes),
		perNodeOpts: func(i int) []pubsub.Option {
			return []pubsub.Option{pubsub.WithRawTracer(tracers[i])}
		},
	})

	// Join and subscribe both topics on every node, as a real gloas node would.
	segTopicStr, envTopicStr := segmentTopic(), envelopeTopic()
	segTopics := make([]*pubsub.Topic, nodes)
	envTopics := make([]*pubsub.Topic, nodes)
	segSubs := make([]*pubsub.Subscription, nodes)
	envSubs := make([]*pubsub.Subscription, nodes)
	for i, ps := range nw.Pubsubs {
		st, err := ps.Join(segTopicStr)
		require.NoError(t, err)
		et, err := ps.Join(envTopicStr)
		require.NoError(t, err)
		ss, err := st.Subscribe()
		require.NoError(t, err)
		es, err := et.Subscribe()
		require.NoError(t, err)
		segTopics[i], envTopics[i], segSubs[i], envSubs[i] = st, et, ss, es
	}

	var drainers sync.WaitGroup
	defer stop()
	defer drainers.Wait()
	defer func() {
		for i := range nodes {
			segSubs[i].Cancel()
			envSubs[i].Cancel()
		}
	}()
	// Drain every subscription except the far end's, on both topics. An unread subscription
	// fills and pubsub then drops, which reads as a slow network.
	drain := func(sub *pubsub.Subscription) {
		drainers.Add(1)
		go func() {
			defer drainers.Done()
			for {
				if _, err := sub.Next(context.Background()); err != nil {
					return
				}
			}
		}()
	}
	for i := range nodes - 1 {
		drain(segSubs[i])
		drain(envSubs[i])
	}
	time.Sleep(meshFormation)
	synctest.Wait()

	// Collect at the far end from both topics concurrently: whichever completes first is when a
	// usable envelope exists, which is the only metric that matters to a node.
	ctx, cancelCtx := context.WithTimeout(context.Background(), networkOpTimeout)
	defer cancelCtx()

	type arrival struct {
		at      time.Duration
		segment bool
	}
	found := make(chan arrival, 2)
	start := time.Now()

	if sendWhole {
		go func() {
			wd := digestOf(whole.msgs[0])
			for {
				msg, err := envSubs[nodes-1].Next(ctx)
				if err != nil {
					return
				}
				if digestOf(msg.Data) == wd {
					found <- arrival{time.Since(start), false}
					return
				}
			}
		}()
	}
	if sendSegments {
		go func() {
			seen := make(map[digest]bool, len(segs.msgs))
			for len(seen) < len(segs.msgs) {
				msg, err := segSubs[nodes-1].Next(ctx)
				if err != nil {
					return
				}
				if _, ok := segs.known[digestOf(msg.Data)]; ok {
					seen[digestOf(msg.Data)] = true
				}
			}
			found <- arrival{time.Since(start), true}
		}()
	}

	// Publication order. Production sends the whole envelope first; segmentsFirst inverts it,
	// which is the candidate fix.
	publishWhole := func() {
		if sendWhole {
			require.NoError(t, envTopics[0].Publish(ctx, whole.msgs[0]))
		}
	}
	// Segments go out exactly as production sends them: accumulated into one pubsub.MessageBatch
	// via Topic.AddToBatch, then PublishBatch'd, so the scheduler sees them as one group. That
	// matters -- BroadcastSegments batches, and a loop of Topic.Publish is a different mechanism
	// whose ordering the scheduler never gets to influence.
	publishSegments := func() {
		if !sendSegments {
			return
		}
		var batch pubsub.MessageBatch
		for _, m := range segs.msgs {
			require.NoError(t, segTopics[0].AddToBatch(ctx, &batch, m))
		}
		require.NoError(t, nw.Pubsubs[0].PublishBatch(&batch))
	}
	if segmentsFirst {
		publishSegments()
		publishWhole()
	} else {
		publishWhole()
		publishSegments()
	}

	out := shippedResult{}
	expect := 0
	if sendWhole {
		expect++
	}
	if sendSegments {
		expect++
	}
	for range expect {
		select {
		case a := <-found:
			if a.segment {
				out.viaSegment = a.at
			} else {
				out.viaWhole = a.at
			}
			if out.usable == 0 || a.at < out.usable {
				out.usable = a.at
			}
		case <-ctx.Done():
			t.Fatalf("timed out waiting for delivery (whole=%v segments=%v)", out.viaWhole, out.viaSegment)
		}
	}
	synctest.Wait()

	_, _, _, sent := tracers[0].Counts()
	out.bytesSent = sent
	for _, tr := range tracers {
		d, u := tr.Losses()
		out.drops += d + u
	}
	return out
}

// TestQ10ShippedConfiguration measures the three arms that matter: whole-only, segmented-only,
// and the whole-then-segments path the feature flag actually enables.
func TestQ10ShippedConfiguration(t *testing.T) {
	hopCounts := []int{1, 2, 3, 4, 5}

	var whole, segs wireArm
	synctest.Test(t, func(t *testing.T) {
		params.SetupTestConfigCleanup(t)
		sk, err := bls.RandKey()
		require.NoError(t, err)
		// Mainnet-like compressibility: the realistic case, not an extreme.
		payload := mainnetLikePayload(q2PayloadLen, 11)
		whole = wholeArm(t, payload)
		segs = segmentedArm(t, payload, segments.DefaultSegmentSize, sk, primitives.Slot(2048))
	})
	t.Logf("payload %d KiB mainnet-like: whole %dB on the wire, %d segments totalling %dB (wmax %dB)",
		q2PayloadLen>>10, whole.total, len(segs.msgs), segs.total, segs.max)

	arms := []struct {
		name                                   string
		sendWhole, sendSegments, segmentsFirst bool
	}{
		{"whole-only", true, false, false},
		{"segmented-only", false, true, false},
		{"whole-then-segments (SHIPPED)", true, true, false},
		{"segments-then-whole (FIX?)", true, true, true},
	}

	results := map[string][]shippedResult{}
	for _, arm := range arms {
		for _, hops := range hopCounts {
			synctest.Test(t, func(t *testing.T) {
				params.SetupTestConfigCleanup(t)
				results[arm.name] = append(results[arm.name],
					runShipped(t, hops, whole, segs, arm.sendWhole, arm.sendSegments, arm.segmentsFirst))
			})
		}
	}

	for _, arm := range arms {
		rows := results[arm.name]
		first, last := rows[0], rows[len(rows)-1]
		slope := (last.usable - first.usable) / time.Duration(hopCounts[len(hopCounts)-1]-hopCounts[0])
		t.Logf("%-32s slope/hop %9v  publisher sent %8dB",
			arm.name, slope.Round(time.Microsecond), last.bytesSent)
		for i, r := range rows {
			detail := ""
			if r.viaWhole > 0 && r.viaSegment > 0 {
				win := "whole"
				if r.viaSegment < r.viaWhole {
					win = "segments"
				}
				detail = fmt.Sprintf("  (whole %v, segments %v -> %s first)",
					r.viaWhole.Round(time.Microsecond), r.viaSegment.Round(time.Microsecond), win)
			}
			flag := ""
			if r.drops > 0 {
				flag = fmt.Sprintf("  *** %d DROPS", r.drops)
			}
			t.Logf("    h=%d usable=%10v%s%s", hopCounts[i], r.usable.Round(time.Microsecond), detail, flag)
		}
	}

	// The comparison the whole question turns on.
	t.Logf("usable-envelope time against whole-only:")
	for i, hops := range hopCounts {
		w := results["whole-only"][i].usable
		sh := results["whole-then-segments (SHIPPED)"][i].usable
		fx := results["segments-then-whole (FIX?)"][i].usable
		t.Logf("    h=%d  whole-only %10v | shipped %10v (%.2fx) | segments-first %10v (%.2fx)",
			hops, w.Round(time.Microsecond),
			sh.Round(time.Microsecond), float64(sh)/float64(w),
			fx.Round(time.Microsecond), float64(fx)/float64(w))
	}

	totalDrops := 0
	for _, arm := range arms {
		require.Equal(t, len(hopCounts), len(results[arm.name]), arm.name)
		for _, r := range results[arm.name] {
			require.Equal(t, true, r.usable > 0, "cell produced no usable envelope")
			totalDrops += r.drops
		}
	}
	require.Equal(t, 0, totalDrops, "queue drops invalidate the affected timings")

	// The shipped arm must move more bytes than whole-only: it sends both. If it does not, the
	// arms are not doing what they claim.
	shippedBytes := results["whole-then-segments (SHIPPED)"][0].bytesSent
	wholeBytes := results["whole-only"][0].bytesSent
	require.Equal(t, true, shippedBytes > wholeBytes,
		fmt.Sprintf("shipped arm should send more than whole-only, got %d vs %d", shippedBytes, wholeBytes))
	t.Logf("publisher bytes at h=1: whole-only %dB, shipped %dB (%.2fx)",
		wholeBytes, shippedBytes, float64(shippedBytes)/float64(wholeBytes))
}
