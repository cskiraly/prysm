package segmentgossip_test

import (
	"bytes"
	"strings"
	"testing"

	"github.com/OffchainLabs/prysm/v7/beacon-chain/p2p/internal/segmentgossip"
	ethpb "github.com/OffchainLabs/prysm/v7/proto/prysm/v1alpha1"
	"github.com/OffchainLabs/prysm/v7/testing/require"
)

// testSegment is a wire segment with recognisable root and index bytes and a non-empty
// proof and body, so a misread offset lands on something that is not zero.
func testSegment() *ethpb.ExecutionPayloadSegment {
	root := make([]byte, 32)
	for i := range root {
		root[i] = byte(0xa0 + i)
	}
	proof := make([]byte, 32)
	for i := range proof {
		proof[i] = byte(0x50 + i)
	}
	return &ethpb.ExecutionPayloadSegment{
		SegmentDescriptor: &ethpb.SegmentDescriptor{
			Version:     1,
			HashId:      0,
			Count:       7,
			SegmentSize: 16 << 10,
			TotalLength: 6*(16<<10) + 100,
			Root:        root,
		},
		Index: 0x01020304,
		Proof: [][]byte{proof},
		Data:  []byte("segment body"),
	}
}

const contentID = "0123456789abcdefghij"

// TestMessageIDOffsets pins the fixed-offset read against a real SSZ encode and decode: if
// the wire type ever gains a field ahead of the root or the index, the id function would read
// the wrong bytes, and this is the alarm.
func TestMessageIDOffsets(t *testing.T) {
	seg := testSegment()
	enc, err := seg.MarshalSSZ()
	require.NoError(t, err)

	mid := segmentgossip.MessageID(enc, contentID)
	require.Equal(t, segmentgossip.MessageIDLen, len(mid))

	claim, ok := segmentgossip.ParseMessageID(mid)
	require.Equal(t, true, ok)
	require.Equal(t, true, bytes.Equal(seg.SegmentDescriptor.Root, claim.Root[:]), "root misread")
	require.Equal(t, seg.Index, claim.Index, "index misread")
	require.Equal(t, contentID, mid[segmentgossip.ClaimLen:], "the content half rides verbatim")

	// Agrees with a full decode, so the offsets are not merely self-consistent.
	back := &ethpb.ExecutionPayloadSegment{}
	require.NoError(t, back.UnmarshalSSZ(enc))
	require.Equal(t, true, bytes.Equal(back.SegmentDescriptor.Root, claim.Root[:]))
	require.Equal(t, back.Index, claim.Index)
}

func TestMessageIDFallsBack(t *testing.T) {
	seg := testSegment()
	enc, err := seg.MarshalSSZ()
	require.NoError(t, err)

	for _, tc := range []struct {
		name    string
		decoded []byte
		content string
	}{
		{"short body", enc[:40], contentID},
		{"first offset word does not point past the fixed part", func() []byte {
			b := bytes.Clone(enc)
			b[60]++
			return b
		}(), contentID},
		{"empty body", nil, contentID},
		{"content id of the wrong width", enc, "short"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mid := segmentgossip.MessageID(tc.decoded, tc.content)
			require.Equal(t, tc.content, mid, "a body that is not the container gets the content id alone")
			_, ok := segmentgossip.ParseMessageID(mid)
			require.Equal(t, false, ok)
		})
	}
}

func TestParseMessageIDRejectsOtherWidths(t *testing.T) {
	for _, mid := range []string{"", contentID, strings.Repeat("x", segmentgossip.MessageIDLen-1), strings.Repeat("x", segmentgossip.MessageIDLen+1)} {
		_, ok := segmentgossip.ParseMessageID(mid)
		require.Equal(t, false, ok, "width %d", len(mid))
	}
}

func TestTopicMatcher(t *testing.T) {
	is := segmentgossip.TopicMatcher("execution_payload_segment")
	require.Equal(t, true, is("/eth2/01020304/execution_payload_segment/ssz_snappy"))
	require.Equal(t, false, is("/eth2/01020304/execution_payload/ssz_snappy"))
	require.Equal(t, false, is("/eth2/01020304/execution_payload_bid/ssz_snappy"))
	require.Equal(t, false, is("/eth2/01020304/beacon_block/ssz_snappy"))
}
