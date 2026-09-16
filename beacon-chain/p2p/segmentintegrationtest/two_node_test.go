// Package segmentintegrationtest exercises segmented payload distribution over a real
// gossipsub mesh on a simulated, bandwidth-limited network.
//
// Everything else in this feature is tested in-process: the codec round-trips, the
// reassembler accepts verified segments, the publisher derives the segmentation. None of
// that shows a segment surviving encoding, a mesh, a bandwidth-limited link and decoding on
// another host, which is the only way the pieces are actually used.
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
	"github.com/OffchainLabs/prysm/v7/beacon-chain/p2p/internal/segmentgossip"
	"github.com/OffchainLabs/prysm/v7/beacon-chain/verification/segmentauth"
	"github.com/OffchainLabs/prysm/v7/config/params"
	"github.com/OffchainLabs/prysm/v7/container/segments"
	ethpb "github.com/OffchainLabs/prysm/v7/proto/prysm/v1alpha1"
	"github.com/OffchainLabs/prysm/v7/testing/require"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pubsubpb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
	simlibp2p "github.com/libp2p/go-libp2p/x/simlibp2p"
	"github.com/marcopolo/simnet"
)

const (
	// linkRate configures the simulated link.
	linkRate = 20 * simlibp2p.OneMbps
	// payloadLen is roughly an execution payload envelope's order of magnitude.
	payloadLen = 1 << 20
	// pubsubQueueSize is Prysm's default per-peer outbound queue. The library's default is 32,
	// and a batch of more segments than that is silently truncated at the queue, so the
	// exchange must run with the production size or a 1 MiB payload at 16 KiB never completes.
	pubsubQueueSize = 600
	// receiveDeadline bounds the wait for the next segment, in the bubble's virtual time, so a
	// dropped segment fails the test instead of hanging it.
	receiveDeadline = 30 * time.Second
)

// twoNodes brings up two connected hosts with gossipsub on a simulated network.
//
// The returned stop function must be deferred inside the synctest bubble. t.Cleanup runs
// after the bubble has exited, and a bubble whose goroutines are still blocked panics with
// a deadlock -- gossipsub keeps a process loop and validation workers running until its
// context is cancelled.
func twoNodes(t *testing.T, extra ...pubsub.Option) (*pubsub.PubSub, *pubsub.PubSub, func()) {
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
	// Prysm's id function, so the ids announced and delivered here are the ones a node
	// computes: on the segment topic, the segment's claim ahead of the content id.
	var genesisValidatorsRoot [32]byte
	opts := append([]pubsub.Option{
		pubsub.WithMessageSigning(false),
		pubsub.WithStrictSignatureVerification(false),
		pubsub.WithPeerOutboundQueueSize(pubsubQueueSize),
		pubsub.WithMessageIdFn(func(pmsg *pubsubpb.Message) string {
			return p2p.MsgID(genesisValidatorsRoot[:], pmsg)
		}),
	}, extra...)
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
// The fork digest is Fulu's, not epoch 0's, and that is not cosmetic: p2p.MsgID branches on
// the digest's fork epoch, and below Altair it computes the pre-Altair message id, which is
// topic-independent. Gloas's fork epoch is unset in the mainnet config, so Fulu is the latest
// digest that resolves.
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
	want := string(d.GroupID(h))
	return func(g []byte) bool { return string(g) == want }
}

// wireSegments produces the wire protos for a payload, exactly as the publisher does.
func wireSegments(t *testing.T, payload []byte, segmentSize int) []*ethpb.ExecutionPayloadSegment {
	t.Helper()
	h, err := segments.HasherByID(segments.HashSHA256)
	require.NoError(t, err)
	msgs, err := segments.BuildSegmentMessages(payload, segmentSize, h)
	require.NoError(t, err)

	out := make([]*ethpb.ExecutionPayloadSegment, len(msgs))
	for i, m := range msgs {
		pb, err := m.ToProto()
		require.NoError(t, err)
		out[i] = pb
	}
	return out
}

// TestTwoNodeSegmentExchange is the end-to-end claim: segments published by one node are
// reassembled byte-for-byte by another, over a real mesh, with real authentication.
func TestTwoNodeSegmentExchange(t *testing.T) {
	exchange(t, nil)
}

// TestTwoNodeSegmentExchangeWithSegmentPolicy is the same exchange under the production
// segment topic policy: phase forwarding, the IWANT discipline, the park and the offer table.
func TestTwoNodeSegmentExchangeWithSegmentPolicy(t *testing.T) {
	exchange(t, segmentgossip.Options(p2p.GossipExecutionPayloadSegmentMessage))
}

func exchange(t *testing.T, opts []pubsub.Option) {
	synctest.Test(t, func(t *testing.T) {
		params.SetupTestConfigCleanup(t)
		ps1, ps2, stop := twoNodes(t, opts...)
		defer stop()

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
			Auth: segmentauth.New(harnessCommitment(t, payload, segmentauth.DefaultSegmentSize)),
		})
		require.NoError(t, err)

		segs := wireSegments(t, payload, segmentauth.DefaultSegmentSize)
		t.Logf("publishing %d segments for a %d KiB payload over a %d Mbps link",
			len(segs), payloadLen>>10, linkRate/simlibp2p.OneMbps)

		publishAll(t, topic1, segs)

		var reassembled []byte
		received := 0
		recvCtx, cancelRecv := context.WithTimeout(context.Background(), receiveDeadline)
		defer cancelRecv()
		for reassembled == nil {
			msg, err := sub2.Next(recvCtx)
			require.NoError(t, err, "a segment never arrived: %d of %d received", received, len(segs))
			received++
			pb := decodeSegment(t, msg.Data)
			requireClaim(t, msg.ID, pb)
			out := feed(t, reassembler, pb)
			if out != nil {
				reassembled = out
			}
		}

		// Assertions are inline rather than in subtests: t.Run is not permitted inside a
		// synctest bubble, which is what gives this test a deterministic clock.
		require.Equal(t, true, bytes.Equal(payload, reassembled), "reassembled payload differs from the original")
		require.Equal(t, len(segs), received, "should consume exactly the published segments")
		require.Equal(t, 0, reassembler.Bytes(), "completion should release the buffer")
		first, h, err := segments.FromProto(segs[0])
		require.NoError(t, err)
		require.Equal(t, true, reassembler.Complete(first.Descriptor.GroupID(h)), "the group should be marked complete")
	})
}

// TestTwoNodeSegmentRejection checks that a corrupted segment is refused on the far side
// without poisoning the group, and that the payload still completes afterwards.
func TestTwoNodeSegmentRejection(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		params.SetupTestConfigCleanup(t)
		ps1, ps2, stop := twoNodes(t)
		defer stop()

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
			Auth: segmentauth.New(harnessCommitment(t, payload, segmentauth.DefaultSegmentSize)),
		})
		require.NoError(t, err)

		segs := wireSegments(t, payload, segmentauth.DefaultSegmentSize)
		require.Equal(t, true, len(segs) >= 3, "need several segments to corrupt one")

		// Corrupt a segment's payload bytes, leaving its proof intact: the tree no longer
		// reproduces the root, which is what a receiver must catch.
		corrupted := &ethpb.ExecutionPayloadSegment{
			SegmentDescriptor: segs[1].SegmentDescriptor,
			Index:             segs[1].Index,
			Proof:             segs[1].Proof,
			Data:              bytes.Clone(segs[1].Data),
		}
		corrupted.Data[len(corrupted.Data)-1] ^= 0xff

		publishAll(t, topic1, []*ethpb.ExecutionPayloadSegment{segs[0], corrupted})

		rejected := 0
		recvCtx, cancelRecv := context.WithTimeout(context.Background(), receiveDeadline)
		defer cancelRecv()
		for range 2 {
			msg, err := sub2.Next(recvCtx)
			require.NoError(t, err)
			pb := decodeSegment(t, msg.Data)
			// The corrupted segment's id still carries its claim: the id names what the
			// message says it is, and the proof is what refuses it.
			requireClaim(t, msg.ID, pb)
			if out := feedAllowingError(t, reassembler, pb, &rejected); out != nil {
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
			msg, err := sub2.Next(recvCtx)
			require.NoError(t, err, "a segment never arrived")
			if out := feedAllowingError(t, reassembler, decodeSegment(t, msg.Data), &rejected); out != nil {
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

// decodeSegment decodes a received gossip message the way the sync validator does.
func decodeSegment(t *testing.T, data []byte) *ethpb.ExecutionPayloadSegment {
	t.Helper()
	pb := &ethpb.ExecutionPayloadSegment{}
	require.NoError(t, encoder.SszNetworkEncoder{}.DecodeGossip(data, pb))
	return pb
}

// requireClaim checks that the id gossipsub delivered a segment under is the structured id:
// the segment's own root and index, readable by any node that sees the id announced.
func requireClaim(t *testing.T, mid string, pb *ethpb.ExecutionPayloadSegment) {
	t.Helper()
	claim, ok := segmentgossip.ParseMessageID(mid)
	require.Equal(t, true, ok, "a segment's message id should carry its claim; got %d bytes", len(mid))
	require.Equal(t, true, bytes.Equal(pb.SegmentDescriptor.Root, claim.Root[:]), "the id names another group")
	require.Equal(t, pb.Index, claim.Index, "the id names another index")
}

// feed hands a decoded segment to the reassembler.
func feed(t *testing.T, r *segments.Reassembler, pb *ethpb.ExecutionPayloadSegment) []byte {
	t.Helper()
	m, h, err := segments.FromProto(pb)
	require.NoError(t, err)
	out, err := r.Add(h, m)
	require.NoError(t, err)
	return out
}

// feedAllowingError is feed for cases where a rejection is the expected outcome.
func feedAllowingError(t *testing.T, r *segments.Reassembler, pb *ethpb.ExecutionPayloadSegment, rejected *int) []byte {
	t.Helper()
	m, h, err := segments.FromProto(pb)
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
