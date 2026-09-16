package p2p_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/OffchainLabs/prysm/v7/beacon-chain/p2p"
	"github.com/OffchainLabs/prysm/v7/beacon-chain/p2p/encoder"
	"github.com/OffchainLabs/prysm/v7/beacon-chain/p2p/internal/segmentgossip"
	"github.com/OffchainLabs/prysm/v7/config/params"
	"github.com/OffchainLabs/prysm/v7/container/segments"
	"github.com/OffchainLabs/prysm/v7/crypto/hash"
	"github.com/OffchainLabs/prysm/v7/encoding/bytesutil"
	"github.com/OffchainLabs/prysm/v7/testing/require"
	"github.com/golang/snappy"
	pubsubpb "github.com/libp2p/go-libp2p-pubsub/pb"
)

// TestMsgIDSegmentTopic checks the id a node computes for a segment gossip message: the
// segment's root and index in the clear, then the spec's 20-byte content id over the same
// message, and the plain content id everywhere the message is not a segment.
func TestMsgIDSegmentTopic(t *testing.T) {
	params.SetupTestConfigCleanup(t)
	digest := params.ForkDigest(params.BeaconConfig().FuluForkEpoch)
	suffix := encoder.SszNetworkEncoder{}.ProtocolSuffix()
	segTopic := fmt.Sprintf(p2p.ExecutionPayloadSegmentTopicFormat, digest) + suffix
	envelopeTopic := fmt.Sprintf(p2p.ExecutionPayloadEnvelopeTopicFormat, digest) + suffix
	var genesis [32]byte

	h, err := segments.HasherByID(segments.HashSHA256)
	require.NoError(t, err)
	msgs, err := segments.BuildSegmentMessages(bytes.Repeat([]byte{0xc3}, 5000), 1024, h)
	require.NoError(t, err)
	seg := msgs[3]
	pb, err := seg.ToProto()
	require.NoError(t, err)
	body, err := pb.MarshalSSZ()
	require.NoError(t, err)
	wire := snappy.Encode(nil, body)

	// The spec's post-Altair content id, computed by hand as the other tests here do.
	contentID := func(topic string) string {
		combined := append(params.BeaconConfig().MessageDomainValidSnappy[:], bytesutil.Uint64ToBytesLittleEndian(uint64(len(topic)))...)
		combined = append(combined, topic...)
		combined = append(combined, body...)
		d := hash.Hash(combined)
		return string(d[:20])
	}

	t.Run("segment topic: claim then content id", func(t *testing.T) {
		mid := p2p.MsgID(genesis[:], &pubsubpb.Message{Data: wire, Topic: &segTopic})
		require.Equal(t, segmentgossip.MessageIDLen, len(mid))
		claim, ok := segmentgossip.ParseMessageID(mid)
		require.Equal(t, true, ok)
		require.Equal(t, true, bytes.Equal(seg.Descriptor.Root, claim.Root[:]))
		require.Equal(t, seg.Index, claim.Index)
		require.Equal(t, contentID(segTopic), mid[segmentgossip.ClaimLen:])
	})

	t.Run("the same bytes on another topic keep the spec id", func(t *testing.T) {
		mid := p2p.MsgID(genesis[:], &pubsubpb.Message{Data: wire, Topic: &envelopeTopic})
		require.Equal(t, contentID(envelopeTopic), mid)
	})

	t.Run("a body that is not a segment container gets the content id alone", func(t *testing.T) {
		junk := snappy.Encode(nil, []byte("not a segment"))
		mid := p2p.MsgID(genesis[:], &pubsubpb.Message{Data: junk, Topic: &segTopic})
		require.Equal(t, 20, len(mid))
	})

	t.Run("invalid snappy on the segment topic gets the invalid-snappy id", func(t *testing.T) {
		junk := []byte("JUNKJUNKJUNKJUNKJUNKJUNKJUNKJUNK")
		mid := p2p.MsgID(genesis[:], &pubsubpb.Message{Data: junk, Topic: &segTopic})
		require.Equal(t, 20, len(mid))
	})

	t.Run("distinct bodies with one claim get distinct ids", func(t *testing.T) {
		other := &segments.SegmentMessage{Descriptor: seg.Descriptor, Index: seg.Index, Proof: seg.Proof, Data: bytes.Clone(seg.Data)}
		other.Data[0] ^= 1
		opb, err := other.ToProto()
		require.NoError(t, err)
		obody, err := opb.MarshalSSZ()
		require.NoError(t, err)
		a := p2p.MsgID(genesis[:], &pubsubpb.Message{Data: wire, Topic: &segTopic})
		b := p2p.MsgID(genesis[:], &pubsubpb.Message{Data: snappy.Encode(nil, obody), Topic: &segTopic})
		require.Equal(t, a[:segmentgossip.ClaimLen], b[:segmentgossip.ClaimLen], "same claim")
		require.Equal(t, false, a == b, "different content halves")
	})
}
