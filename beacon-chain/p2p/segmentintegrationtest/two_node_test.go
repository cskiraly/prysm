// Package segmentintegrationtest exercises segmented payload distribution over a real
// gossipsub mesh on a simulated, bandwidth-limited network.
//
// Everything else in this feature is tested in-process: the codec round-trips, the
// reassembler accepts verified segments, the publisher derives what the signer committed to.
// None of that shows a segment surviving encoding, a mesh, a bandwidth-limited link and
// decoding on another host, which is the only way the pieces are actually used.
package segmentintegrationtest

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"testing/synctest"
	"time"

	"github.com/OffchainLabs/prysm/v7/beacon-chain/p2p"
	"github.com/OffchainLabs/prysm/v7/beacon-chain/p2p/encoder"
	"github.com/OffchainLabs/prysm/v7/beacon-chain/verification/segmentauth"
	"github.com/OffchainLabs/prysm/v7/config/params"
	"github.com/OffchainLabs/prysm/v7/consensus-types/primitives"
	"github.com/OffchainLabs/prysm/v7/container/segments"
	"github.com/OffchainLabs/prysm/v7/crypto/bls"
	ethpb "github.com/OffchainLabs/prysm/v7/proto/prysm/v1alpha1"
	"github.com/OffchainLabs/prysm/v7/testing/require"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
	simlibp2p "github.com/libp2p/go-libp2p/x/simlibp2p"
	"github.com/marcopolo/simnet"
)

const (
	// linkRate configures the simulated link. Note that observed completion times are well
	// above this rate, so it is not currently acting as the bottleneck -- see the note at the
	// timing log below. Kept because it is the right knob once that is resolved.
	linkRate = 20 * simlibp2p.OneMbps
	// payloadLen is roughly an execution payload envelope's order of magnitude.
	payloadLen = 1 << 20
)

// twoNodes brings up two connected hosts with gossipsub on a simulated network.
//
// The returned stop function must be deferred inside the synctest bubble. t.Cleanup runs
// after the bubble has exited, and a bubble whose goroutines are still blocked panics with
// a deadlock -- gossipsub keeps a process loop and validation workers running until its
// context is cancelled.
func twoNodes(t *testing.T) (*pubsub.PubSub, *pubsub.PubSub, func()) {
	t.Helper()
	network, meta, err := simlibp2p.SimpleLibp2pNetwork([]simlibp2p.NodeLinkSettingsAndCount{
		{LinkSettings: simnet.NodeBiDiLinkSettings{
			Downlink: simnet.LinkSettings{BitsPerSecond: linkRate},
			Uplink:   simnet.LinkSettings{BitsPerSecond: linkRate},
		}, Count: 2},
	}, simnet.StaticLatency(5*time.Millisecond), simlibp2p.NetworkSettings{UseBlankHost: true})
	require.NoError(t, err)
	network.Start()

	h1, h2 := meta.Nodes[0], meta.Nodes[1]
	synctest.Wait()

	ctx, cancel := context.WithCancel(context.Background())
	opts := []pubsub.Option{
		pubsub.WithMessageSigning(false),
		pubsub.WithStrictSignatureVerification(false),
	}
	ps1, err := pubsub.NewGossipSub(ctx, h1, opts...)
	require.NoError(t, err)
	ps2, err := pubsub.NewGossipSub(ctx, h2, opts...)
	require.NoError(t, err)

	require.NoError(t, h1.Connect(context.Background(), peer.AddrInfo{ID: h2.ID(), Addrs: h2.Addrs()}))
	time.Sleep(300 * time.Millisecond)
	synctest.Wait()

	stop := func() {
		cancel()
		for _, n := range meta.Nodes {
			_ = n.Close()
		}
		network.Close()
		synctest.Wait()
	}
	return ps1, ps2, stop
}

// segmentTopic is the real topic string a node would join.
//
// The fork digest is Fulu's, not epoch 0's, and that is not cosmetic. p2p.MsgID branches on the
// digest's fork epoch: below Altair it computes the pre-Altair message id, which hashes only the
// domain and the data and is therefore **topic-independent**. Every cell before this used
// ForkDigest(0) -- epoch 0, far below AltairForkEpoch -- so identical bytes on two different topics
// collided by message id, and since seen state and the mcache are keyed by id alone that collision
// would let a wrong-topic copy suppress the right one. No measurement was affected (variant C
// publishes each index's bytes on exactly one topic, and the id width is 20 bytes either way), but
// it was not the production id. Gloas's fork epoch is unset in the mainnet config, so Fulu is the
// latest digest that resolves.
func segmentTopic() string {
	digest := params.ForkDigest(params.BeaconConfig().FuluForkEpoch)
	return fmt.Sprintf(p2p.ExecutionPayloadSegmentTopicFormat, digest) +
		encoder.SszNetworkEncoder{}.ProtocolSuffix()
}

// harnessCommitment stands in for the commitment set a receiver builds from accepted bids: it
// admits exactly the group the harness payload segments into.
func harnessCommitment(t *testing.T, payload []byte, segmentSize int) segmentauth.CommittedGroups {
	t.Helper()
	h, err := segments.HasherByID(segments.HashSHA256)
	require.NoError(t, err)
	d, _, err := segments.Commit(payload, segmentSize, h)
	require.NoError(t, err)
	return commitmentFor(t, d)
}

// commitmentFor admits exactly one descriptor's group.
func commitmentFor(t *testing.T, d *segments.Descriptor) segmentauth.CommittedGroups {
	t.Helper()
	h, err := segments.HasherByID(segments.HashSHA256)
	require.NoError(t, err)
	want := string(d.GroupID(h))
	return func(g []byte) bool { return string(g) == want }
}

// signedSegments produces the wire protos for a payload, exactly as the publisher does.
func signedSegments(t *testing.T, _ bls.SecretKey, _ primitives.Slot, _ primitives.BuilderIndex, payload []byte, segmentSize int) []*ethpb.ExecutionPayloadSegment {
	t.Helper()
	h, err := segments.HasherByID(segments.HashSHA256)
	require.NoError(t, err)
	msgs, err := segments.BuildSegmentMessages(payload, segmentSize, h)
	require.NoError(t, err)

	out := make([]*ethpb.ExecutionPayloadSegment, len(msgs))
	for i, m := range msgs {
		enc, err := m.Marshal()
		require.NoError(t, err)
		out[i] = &ethpb.ExecutionPayloadSegment{Segment: enc}
	}
	return out
}

// TestTwoNodeSegmentExchange is the end-to-end claim: segments published by one node are
// reassembled byte-for-byte by another, over a real mesh, with real authentication.
func TestTwoNodeSegmentExchange(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		params.SetupTestConfigCleanup(t)
		ps1, ps2, stop := twoNodes(t)
		defer stop()

		sk, err := bls.RandKey()
		require.NoError(t, err)
		const slot = primitives.Slot(2048)
		const builder = primitives.BuilderIndex(7)
		payload := deterministicPayload(payloadLen)

		topicStr := segmentTopic()
		topic1, err := ps1.Join(topicStr)
		require.NoError(t, err)
		topic2, err := ps2.Join(topicStr)
		require.NoError(t, err)

		sub2, err := topic2.Subscribe()
		require.NoError(t, err)
		defer sub2.Cancel()
		// Let the mesh form before publishing, or the first segments land nowhere.
		time.Sleep(500 * time.Millisecond)
		synctest.Wait()

		reassembler, err := segments.NewReassembler(segments.ReassemblerConfig{
			Auth: segmentauth.New(harnessCommitment(t, payload, segments.DefaultSegmentSize)),
		})
		require.NoError(t, err)

		segs := signedSegments(t, sk, slot, builder, payload, segments.DefaultSegmentSize)
		t.Logf("publishing %d segments for a %d KiB payload over a %d Mbps link",
			len(segs), payloadLen>>10, linkRate/simlibp2p.OneMbps)

		start := time.Now()
		publishAll(t, topic1, segs)

		var reassembled []byte
		received := 0
		for reassembled == nil {
			msg, err := sub2.Next(context.Background())
			require.NoError(t, err)
			received++
			out := feed(t, reassembler, msg.Data)
			if out != nil {
				reassembled = out
			}
		}
		elapsed := time.Since(start)

		// Assertions are inline rather than in subtests: t.Run is not permitted inside a
		// synctest bubble, which is what gives this test a deterministic clock.
		require.Equal(t, true, bytes.Equal(payload, reassembled), "reassembled payload differs from the original")
		require.Equal(t, len(segs), received, "should consume exactly the published segments")
		require.Equal(t, 0, reassembler.Groups(), "completion should release the group")
		require.Equal(t, 0, reassembler.Bytes(), "completion should release the buffer")

		// Logged for visibility, NOT as a throughput result. The configured link is 20 Mbps,
		// where 1 MiB should take about 419ms; observed virtual time is an order of magnitude
		// below that, so the simulated bandwidth limit is evidently not constraining these
		// transfers. Treat this as "it completed", and do not derive a bandwidth or
		// time-to-first-byte claim from it until the link model is understood.
		t.Logf("virtual time to complete (not a bandwidth measurement): %v", elapsed)
	})
}

// TestTwoNodeSegmentRejection checks that a corrupted segment is refused on the far side
// without poisoning the group, and that the payload still completes afterwards.
func TestTwoNodeSegmentRejection(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		params.SetupTestConfigCleanup(t)
		ps1, ps2, stop := twoNodes(t)
		defer stop()

		sk, err := bls.RandKey()
		require.NoError(t, err)
		const slot = primitives.Slot(64)
		const builder = primitives.BuilderIndex(1)
		// Small payload: this test is about the rejection path, not throughput.
		payload := deterministicPayload(96 << 10)

		topicStr := segmentTopic()
		topic1, err := ps1.Join(topicStr)
		require.NoError(t, err)
		topic2, err := ps2.Join(topicStr)
		require.NoError(t, err)
		sub2, err := topic2.Subscribe()
		require.NoError(t, err)
		defer sub2.Cancel()
		time.Sleep(500 * time.Millisecond)
		synctest.Wait()

		reassembler, err := segments.NewReassembler(segments.ReassemblerConfig{
			Auth: segmentauth.New(harnessCommitment(t, payload, segments.DefaultSegmentSize)),
		})
		require.NoError(t, err)

		segs := signedSegments(t, sk, slot, builder, payload, 32<<10)
		require.Equal(t, true, len(segs) >= 3, "need several segments to corrupt one")

		// Corrupt a segment's payload bytes in place, leaving its proof intact: the tree no
		// longer reproduces the root, which is what a receiver must catch.
		corrupted := &ethpb.ExecutionPayloadSegment{Segment: bytes.Clone(segs[1].Segment)}
		corrupted.Segment[len(corrupted.Segment)-1] ^= 0xff

		publishAll(t, topic1, []*ethpb.ExecutionPayloadSegment{segs[0], corrupted})

		rejected := 0
		for range 2 {
			msg, err := sub2.Next(context.Background())
			require.NoError(t, err)
			if out := feedAllowingError(t, reassembler, msg.Data, &rejected); out != nil {
				t.Fatal("should not have completed yet")
			}
		}

		require.Equal(t, 1, rejected, "the corrupted segment should have been refused")
		// One good segment is buffered; the bad one bought nothing.
		require.Equal(t, 1, reassembler.Groups(), "the group should survive a bad segment")

		// The rest of the payload, including an honest copy of the corrupted index.
		publishAll(t, topic1, segs[1:])
		var reassembled []byte
		for reassembled == nil {
			msg, err := sub2.Next(context.Background())
			require.NoError(t, err)
			if out := feedAllowingError(t, reassembler, msg.Data, &rejected); out != nil {
				reassembled = out
			}
		}
		require.Equal(t, true, bytes.Equal(payload, reassembled), "payload should still complete after a rejection")
	})
}

// publishAll encodes and publishes each segment the way Prysm's gossip encoder would.
func publishAll(t *testing.T, topic *pubsub.Topic, segs []*ethpb.ExecutionPayloadSegment) {
	t.Helper()
	enc := encoder.SszNetworkEncoder{}
	for _, s := range segs {
		buf := new(bytes.Buffer)
		_, err := enc.EncodeGossip(buf, s)
		require.NoError(t, err)
		require.NoError(t, topic.Publish(context.Background(), buf.Bytes()))
	}
	synctest.Wait()
}

// feed decodes a received gossip message and hands the segment to the reassembler.
func feed(t *testing.T, r *segments.Reassembler, data []byte) []byte {
	t.Helper()
	pb := &ethpb.ExecutionPayloadSegment{}
	require.NoError(t, encoder.SszNetworkEncoder{}.DecodeGossip(data, pb))
	m, h, err := segments.UnmarshalSegmentMessage(pb.Segment)
	require.NoError(t, err)
	out, err := r.Add(h, m)
	require.NoError(t, err)
	return out
}

// feedAllowingError is feed for cases where a rejection is the expected outcome.
func feedAllowingError(t *testing.T, r *segments.Reassembler, data []byte, rejected *int) []byte {
	t.Helper()
	pb := &ethpb.ExecutionPayloadSegment{}
	require.NoError(t, encoder.SszNetworkEncoder{}.DecodeGossip(data, pb))
	m, h, err := segments.UnmarshalSegmentMessage(pb.Segment)
	if err != nil {
		*rejected++
		return nil
	}
	out, err := r.Add(h, m)
	if err != nil {
		*rejected++
		return nil
	}
	return out
}

// deterministicPayload builds a reproducible payload so failures are debuggable.
func deterministicPayload(n int) []byte {
	out := make([]byte, n)
	for i := range out {
		out[i] = byte(i*31 + 7)
	}
	return out
}
