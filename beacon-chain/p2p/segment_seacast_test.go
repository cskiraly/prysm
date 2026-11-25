package p2p_test

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/OffchainLabs/prysm/v7/beacon-chain/p2p"
	"github.com/OffchainLabs/prysm/v7/beacon-chain/p2p/encoder"
	"github.com/OffchainLabs/prysm/v7/beacon-chain/p2p/internal/segmentgossip"
	"github.com/OffchainLabs/prysm/v7/beacon-chain/verification/segmentauth"
	"github.com/OffchainLabs/prysm/v7/config/params"
	"github.com/OffchainLabs/prysm/v7/container/segments"
	enginev1 "github.com/OffchainLabs/prysm/v7/proto/engine/v1"
	ethpb "github.com/OffchainLabs/prysm/v7/proto/prysm/v1alpha1"
	"github.com/OffchainLabs/prysm/v7/testing/require"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pubsubpb "github.com/libp2p/go-libp2p-pubsub/pb"
)

// seacastEnvelope is a signed envelope with txCount transactions of txSize bytes, random when random
// is set (incompressible, the decode's worst case), patterned otherwise.
func seacastEnvelope(t testing.TB, txCount, txSize int, random bool) *ethpb.SignedExecutionPayloadEnvelope {
	rng := rand.New(rand.NewPCG(1, uint64(txCount)))
	txs := make([][]byte, txCount)
	for i := range txs {
		txs[i] = make([]byte, txSize)
		for j := range txs[i] {
			if random {
				txs[i][j] = byte(rng.Uint32())
			} else {
				txs[i][j] = byte(i + j)
			}
		}
	}
	txList, err := enginev1.NewProgressiveTransactionList(txs)
	require.NoError(t, err)
	return &ethpb.SignedExecutionPayloadEnvelope{
		Message: &ethpb.ExecutionPayloadEnvelope{
			Payload: &enginev1.ExecutionPayloadGloas{
				Transactions:  txList,
				ParentHash:    make([]byte, 32),
				FeeRecipient:  make([]byte, 20),
				StateRoot:     make([]byte, 32),
				ReceiptsRoot:  make([]byte, 32),
				LogsBloom:     make([]byte, 256),
				PrevRandao:    make([]byte, 32),
				ExtraData:     []byte{},
				BaseFeePerGas: make([]byte, 32),
				BlockHash:     make([]byte, 32),
			},
			ExecutionRequests:     &enginev1.ExecutionRequestsGloas{},
			BeaconBlockRoot:       bytes.Repeat([]byte{0xbe}, 32),
			ParentBeaconBlockRoot: make([]byte, 32),
		},
		Signature: make([]byte, 96),
	}
}

// sourceGroup is a group as its source publishes it: the segments cut with the publisher's default
// parameters, each framed as BroadcastSegments frames it, on the segment topic.
type sourceGroup struct {
	segs  []*segments.SegmentMessage
	wire  []*pubsubpb.Message
	topic string
	group string // the group's key, as the payload path knows it
	n, k  int
}

func newSourceGroup(t testing.TB, env *ethpb.SignedExecutionPayloadEnvelope) *sourceGroup {
	segs, err := segmentauth.SegmentMessagesForEnvelope(env, segmentauth.DefaultParams())
	require.NoError(t, err)
	digest := params.ForkDigest(params.BeaconConfig().FuluForkEpoch)
	g := &sourceGroup{
		segs:  segs,
		topic: fmt.Sprintf(p2p.ExecutionPayloadSegmentTopicFormat, digest) + encoder.SszNetworkEncoder{}.ProtocolSuffix(),
		group: string(segs[0].Group),
		n:     len(segs),
		k:     int(segs[0].Descriptor.Required()),
	}
	for _, s := range segs {
		pb, err := s.ToProto()
		require.NoError(t, err)
		var buf bytes.Buffer
		_, err = encoder.SszNetworkEncoder{}.EncodeGossip(&buf, pb)
		require.NoError(t, err)
		tp := g.topic
		g.wire = append(g.wire, &pubsubpb.Message{Data: buf.Bytes(), Topic: &tp})
	}
	return g
}

// holding is a payload type that holds a random K of the group, as the router's store would after
// accepting them, and the indices it holds.
func (g *sourceGroup) holding(t testing.TB, seed uint64) (*segmentgossip.PayloadType, []uint32) {
	typ := segmentgossip.NewPayloadType(segmentgossip.PayloadConfig{
		Topics:       []string{g.topic},
		CurrentTopic: func() string { return g.topic },
		CurrentSlot:  func() uint64 { return 7 },
		UplinkMbps:   100,
	})
	require.Equal(t, true, typ.NoteGroup(g.group, g.topic, g.n, g.k))
	order := rand.New(rand.NewPCG(seed, 2)).Perm(g.n)
	held := make([]uint32, 0, g.k)
	for _, i := range order[:g.k] {
		idx := uint32(i) // lint:ignore uintcast -- an index below the group's count.
		typ.Store().Put(g.group, idx, &pubsub.Message{Message: g.wire[i]})
		held = append(held, idx)
	}
	slices.Sort(held)
	return typ, held
}

// TestPayloadTypeDecodeIsTheSource: from a random K of a coded group, the decoder's message for every missing index has the source's wire bytes and the
// source's message id, and it returns nothing for a held index.
func TestPayloadTypeDecodeIsTheSource(t *testing.T) {
	params.SetupTestConfigCleanup(t)
	g := newSourceGroup(t, seacastEnvelope(t, 40, 4096, false))
	require.Equal(t, segments.VersionCoded, g.segs[0].Descriptor.Version)
	require.Equal(t, 2*g.k, g.n)

	for seed := range uint64(5) {
		t.Run(fmt.Sprintf("seed %d", seed), func(t *testing.T) {
			typ, held := g.holding(t, seed)
			cells, ok := typ.Decode(g.group, held)()
			require.Equal(t, true, ok)
			require.Equal(t, g.n-g.k, len(cells))
			for i := range g.n {
				idx := uint32(i) // lint:ignore uintcast -- an index below the group's count.
				m, rebuilt := cells[idx]
				if slices.Contains(held, idx) {
					require.Equal(t, false, rebuilt, "held index %d rebuilt", i)
					continue
				}
				require.Equal(t, true, rebuilt, "missing index %d not rebuilt", i)
				require.Equal(t, g.topic, m.GetTopic())
				require.Equal(t, true, bytes.Equal(g.wire[i].Data, m.GetData()), "index %d: wire bytes differ", i)
				require.Equal(t, p2p.MsgID(g.wire[i]), p2p.MsgID(m.Message), "index %d: message id differs", i)
			}
		})
	}
}

// TestPayloadTypeDecodeRefuses checks that the decoder fails, rather than invent segments, from a group it
// cannot rebuild: one short of K, or with a segment of another group in the store.
func TestPayloadTypeDecodeRefuses(t *testing.T) {
	params.SetupTestConfigCleanup(t)
	g := newSourceGroup(t, seacastEnvelope(t, 40, 4096, false))

	t.Run("one short of K", func(t *testing.T) {
		typ, held := g.holding(t, 1)
		_, ok := typ.Decode(g.group, held[1:])()
		require.Equal(t, false, ok)
	})
	t.Run("a segment of another group", func(t *testing.T) {
		other := newSourceGroup(t, seacastEnvelope(t, 41, 4096, false))
		typ, held := g.holding(t, 1)
		typ.Store().Put(g.group, 1000, &pubsub.Message{Message: other.wire[0]})
		_, ok := typ.Decode(g.group, append(held, 1000))()
		require.Equal(t, false, ok)
	})
}

// TestPayloadTypeShape checks the type's answers for a group before and after its first segment passed.
func TestPayloadTypeShape(t *testing.T) {
	typ := segmentgossip.NewPayloadType(segmentgossip.PayloadConfig{
		Topics:       []string{"/eth2/aaaaaaaa/execution_payload_segment/ssz_snappy"},
		CurrentTopic: func() string { return "/eth2/aaaaaaaa/execution_payload_segment/ssz_snappy" },
		CurrentSlot:  func() uint64 { return 9 },
		UplinkMbps:   100,
	})
	root := string(bytes.Repeat([]byte{1}, 32))
	require.Equal(t, segmentgossip.MaxPlainChunks, typ.IndexCount(root))
	require.Equal(t, segmentgossip.MaxPlainChunks, typ.Threshold(root))
	_, ok := typ.SlotOf(root)
	require.Equal(t, false, ok)
	topic, ok := typ.TopicOf(root)
	require.Equal(t, true, ok)
	require.Equal(t, "/eth2/aaaaaaaa/execution_payload_segment/ssz_snappy", topic)
	_, ok = typ.TopicOf("short")
	require.Equal(t, false, ok)
	// Before the node knows its fork digest, an unrecorded group has no topic.
	early := segmentgossip.NewPayloadType(segmentgossip.PayloadConfig{
		CurrentTopic: func() string { return "" },
		CurrentSlot:  func() uint64 { return 0 },
		UplinkMbps:   100,
	})
	_, ok = early.TopicOf(root)
	require.Equal(t, false, ok)

	older := "/eth2/00000000/execution_payload_segment/ssz_snappy"
	require.Equal(t, true, typ.NoteGroup(root, older, 128, 64))
	require.Equal(t, false, typ.NoteGroup(root, "/eth2/ffffffff/execution_payload_segment/ssz_snappy", 1, 1))
	require.Equal(t, 128, typ.IndexCount(root))
	require.Equal(t, 64, typ.Threshold(root))
	slot, ok := typ.SlotOf(root)
	require.Equal(t, true, ok)
	require.Equal(t, uint64(9), slot)
	// The recorded topic, not the current digest's: a rebuilt segment keeps its message id.
	topic, ok = typ.TopicOf(root)
	require.Equal(t, true, ok)
	require.Equal(t, older, topic)

	typ.Store().Put(root, 0, &pubsub.Message{Message: &pubsubpb.Message{}})
	typ.Advance(10, 2)
	_, held := typ.Store().Get(root, 0)
	require.Equal(t, true, held, "a live group's segment dropped")
	// The router expires a group once slot+window <= its slot; the type drops its segments at the same
	// slot.
	typ.Advance(11, 2)
	_, held = typ.Store().Get(root, 0)
	require.Equal(t, false, held, "an expired group's segment kept")
	// Its record stays, so a late segment, offer or ask still finds the group's slot and the router
	// drops it as expired, and the group is not noted again at a later slot.
	typ.Advance(12, 2)
	slot, ok = typ.SlotOf(root)
	require.Equal(t, true, ok, "the expired group's slot forgotten")
	require.Equal(t, uint64(9), slot)
	require.Equal(t, false, typ.NoteGroup(root, older, 128, 64), "the expired group noted again")
	typ.Advance(9+typ.RecordSlots()-1, 2)
	_, ok = typ.SlotOf(root)
	require.Equal(t, true, ok)
	typ.Advance(9+typ.RecordSlots(), 2)
	_, ok = typ.SlotOf(root)
	require.Equal(t, false, ok, "the record kept past RecordSlots")
	require.Equal(t, segmentgossip.MaxPlainChunks, typ.IndexCount(root))

	// Once the router has a slot, a group is noted at the router's slot, not the clock's (9 here), so
	// the type and the router read one clock.
	fresh := string(bytes.Repeat([]byte{2}, 32))
	require.Equal(t, true, typ.NoteGroup(fresh, older, 4, 2))
	slot, ok = typ.SlotOf(fresh)
	require.Equal(t, true, ok)
	require.Equal(t, uint64(9+typ.RecordSlots()), slot)
}

// The type keeps a group's record for two epochs of the configured slots, the span of the node's seen
// cache (setPubSubParameters), not a fixed 64 slots.
func TestPayloadTypeRecordSpanFollowsTheEpoch(t *testing.T) {
	for _, c := range []struct {
		name string
		cfg  *params.BeaconChainConfig
		span uint64
	}{
		{"minimal, 8 slots an epoch", params.MinimalSpecConfig(), 16},
		{"mainnet, 32 slots an epoch", params.MainnetConfig(), 64},
	} {
		t.Run(c.name, func(t *testing.T) {
			params.SetupTestConfigCleanup(t)
			params.OverrideBeaconConfig(c.cfg.Copy())
			const topic = "/eth2/aaaaaaaa/execution_payload_segment/ssz_snappy"
			typ := segmentgossip.NewPayloadType(segmentgossip.PayloadConfig{
				Topics:       []string{topic},
				CurrentTopic: func() string { return topic },
				CurrentSlot:  func() uint64 { return 9 },
				UplinkMbps:   100,
			})
			root := string(bytes.Repeat([]byte{1}, 32))
			require.Equal(t, true, typ.NoteGroup(root, topic, 4, 2))
			typ.Advance(9+c.span-1, 2)
			_, ok := typ.SlotOf(root)
			require.Equal(t, true, ok, "the record dropped before two epochs")
			typ.Advance(9+c.span, 2)
			_, ok = typ.SlotOf(root)
			require.Equal(t, false, ok, "the record kept past two epochs")
		})
	}
}

// BenchmarkPayloadTypeDecode is the decoder's cost: the decode from K held of an incompressible envelope of about
// 1 MiB, and of the largest the publisher still codes (128 data segments of 16 KiB, just under 2 MiB; a larger
// envelope is a plain group, which never decodes).
func BenchmarkPayloadTypeDecode(b *testing.B) {
	params.SetupTestConfigCleanup(b)
	for _, c := range []struct {
		name    string
		txCount int
	}{{"1MiB", 256}, {"2MiB", 511}} {
		b.Run(c.name, func(b *testing.B) {
			g := newSourceGroup(b, seacastEnvelope(b, c.txCount, 4096, true))
			if g.segs[0].Descriptor.Version != segments.VersionCoded {
				b.Fatalf("%s: a plain group of %d segments", c.name, g.n)
			}
			typ, held := g.holding(b, 3)
			for b.Loop() {
				if _, ok := typ.Decode(g.group, held)(); !ok {
					b.Fatal("decode failed")
				}
			}
			b.ReportMetric(float64(g.n), "segments")
		})
	}
}
