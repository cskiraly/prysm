package segmentintegrationtest

// Instrumentation bindings for the shared gossipsub harness in testing/gossipsim.
//
// The recording tracer, the digest table it keys events by, the byte accounting and the payload
// generators were written here and promoted to testing/gossipsim so the RowDAS study could use
// the same substrate. The aliases below keep this package's names.
//
// The two tests are unchanged. They are what justifies trusting every byte figure the study
// reports, so they stay next to it.

import (
	"bytes"
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/OffchainLabs/prysm/v7/beacon-chain/p2p/encoder"
	"github.com/OffchainLabs/prysm/v7/config/params"
	"github.com/OffchainLabs/prysm/v7/consensus-types/primitives"
	"github.com/OffchainLabs/prysm/v7/container/segments"
	"github.com/OffchainLabs/prysm/v7/crypto/bls"
	ethpb "github.com/OffchainLabs/prysm/v7/proto/prysm/v1alpha1"
	"github.com/OffchainLabs/prysm/v7/testing/gossipsim"
	"github.com/OffchainLabs/prysm/v7/testing/require"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	simlibp2p "github.com/libp2p/go-libp2p/x/simlibp2p"
)

type (
	digest          = gossipsim.Digest
	recordingTracer = gossipsim.RecordingTracer
	controlCounts   = gossipsim.ControlCounts
)

func digestOf(b []byte) digest { return gossipsim.DigestOf(b) }

func newRecordingTracer() *recordingTracer { return gossipsim.NewRecordingTracer() }

func completionStats(times []time.Duration, population int, deadline time.Duration) string {
	return gossipsim.CompletionStats(times, population, deadline)
}

func highEntropyPayload(n int, seed uint64) []byte { return gossipsim.HighEntropyPayload(n, seed) }

const mainnetPaddedFraction = gossipsim.MainnetPaddedFraction

func mainnetLikePayload(n int, seed uint64) []byte { return gossipsim.MainnetLikePayload(n, seed) }

func clusteredEntropyPayload(n int, padded float64, runWords int, seed uint64) []byte {
	return gossipsim.ClusteredEntropyPayload(n, padded, runWords, seed)
}

func mixedEntropyPayload(n int, padded float64, seed uint64) []byte {
	return gossipsim.MixedEntropyPayload(n, padded, seed)
}

// TestPayloadEntropy pins the compression trap that has already produced one wrong conclusion.
// The check runs through the real gossip encoder, because that is where snappy sits.
func TestPayloadEntropy(t *testing.T) {
	const n = 1 << 20
	encoded := func(b []byte) int {
		var buf bytes.Buffer
		_, err := encoder.SszNetworkEncoder{}.EncodeGossip(&buf, &ethpb.ExecutionPayloadSegment{Segment: b})
		require.NoError(t, err)
		return buf.Len()
	}

	patterned := encoded(deterministicPayload(n))
	random := encoded(highEntropyPayload(n, 1))
	t.Logf("1 MiB on the wire: patterned %d B (%.1f:1), high entropy %d B (%.2f:1)",
		patterned, float64(n)/float64(patterned), random, float64(n)/float64(random))

	// The patterned payload is what the existing two-node test uses, so its "1 MiB over a
	// 20 Mbps link" moves an order of magnitude less than it appears to.
	require.Equal(t, true, patterned < n/10, "patterned payload should compress heavily")
	require.Equal(t, true, random > n, "high-entropy payload should not compress at all")
	require.DeepEqual(t, highEntropyPayload(n, 1), highEntropyPayload(n, 1), "must be reproducible")
}

// TestInstrumentationObservesTheWire validates the tracer before any measurement trusts it: the
// publisher's sends and the receiver's deliveries must both line up with what was published.
func TestInstrumentationObservesTheWire(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		params.SetupTestConfigCleanup(t)
		tracers := []*recordingTracer{newRecordingTracer(), newRecordingTracer()}
		nw, stop := newSimNetwork(t, networkConfig{
			links: uniformLinks(2, 20*simlibp2p.OneMbps),
			edges: line(2),
			perNodeOpts: func(i int) []pubsub.Option {
				return []pubsub.Option{pubsub.WithRawTracer(tracers[i])}
			},
		})
		defer stop()

		sk, err := bls.RandKey()
		require.NoError(t, err)
		const slot = primitives.Slot(96)
		payload := highEntropyPayload(256<<10, 7)

		// Subscribe the publisher too, not just the receiver. A node that joins without
		// subscribing publishes through fanout rather than the mesh, and a real Prysm node
		// subscribes to the topics it publishes on -- so not subscribing here would measure the
		// wrong code path. This is what joinAndSubscribeAll exists to get right by default.
		topics, subs, cancelSubs := joinAndSubscribeAll(t, nw, false)
		defer cancelSubs()
		topic0, sub1 := topics[0], subs[1]

		segs := signedSegments(t, sk, slot, 3, payload, segments.DefaultSegmentSize)
		// The digest table maps wire bytes back to segment index, so the tracer needs no decoder.
		known := make(map[digest]int, len(segs))
		enc := encoder.SszNetworkEncoder{}
		wire := make([][]byte, len(segs))
		for i, s := range segs {
			var buf bytes.Buffer
			_, err := enc.EncodeGossip(&buf, s)
			require.NoError(t, err)
			wire[i] = buf.Bytes()
			known[digestOf(wire[i])] = i
		}

		for _, w := range wire {
			require.NoError(t, topic0.Publish(context.Background(), w))
		}
		for range segs {
			_, err := sub1.Next(context.Background())
			require.NoError(t, err)
		}
		synctest.Wait()

		// The publisher forwarded every segment to its one peer, in publication order.
		sent := tracers[0].SendOrderTo(nw.Hosts[1].ID(), known)
		require.Equal(t, len(segs), len(sent), "publisher should have sent every segment")
		for i, idx := range sent {
			require.Equal(t, i, idx, "publisher sent out of order")
		}

		// The receiver saw them all, and the tracer's view matches the subscription's.
		got := tracers[1].ArrivalOrder(known)
		require.Equal(t, len(segs), len(got), "receiver should have been delivered every segment")

		_, firstIdx, ok := tracers[1].FirstForward(known)
		require.Equal(t, false, ok, "a leaf node has nobody to forward to")
		_ = firstIdx

		_, _, recvBytes, sentBytes := tracers[1].Counts()
		require.Equal(t, true, recvBytes > 0, "receiver should have counted bytes")
		require.Equal(t, 0, sentBytes, "a leaf node sends no payload")
		require.Equal(t, 1, tracers[0].MeshPeers(), "publisher should be grafted to its one peer")
	})
}
