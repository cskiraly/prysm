package segments

import (
	"bytes"
	"fmt"
	"math/bits"
	"testing"

	ethpb "github.com/OffchainLabs/prysm/v7/proto/prysm/v1alpha1"
	"github.com/OffchainLabs/prysm/v7/testing/require"
)

func TestMaxProofDepth(t *testing.T) {
	require.Equal(t, bits.Len(uint(MaxSegments-1)), MaxProofDepth)
}

func TestSegmentMessageRoundTrip(t *testing.T) {
	for _, id := range []HashID{HashSHA256, HashKeccak256} {
		h, err := HasherByID(id)
		require.NoError(t, err)
		for _, tc := range []struct {
			name        string
			msgLen      int
			segmentSize int
		}{
			{"single segment", 20, 64},
			{"many segments", 1000, 64},
			{"exact multiple", 256, 64},
			{"one byte segments", 5, 1},
		} {
			t.Run(fmt.Sprintf("%s/hash %d", tc.name, id), func(t *testing.T) {
				msgs, err := BuildSegmentMessages(msgOfLen(tc.msgLen), tc.segmentSize, h)
				require.NoError(t, err)
				for _, m := range msgs {
					pb, err := m.ToProto()
					require.NoError(t, err)
					// Through SSZ, as gossip carries it.
					enc, err := pb.MarshalSSZ()
					require.NoError(t, err)
					back := &ethpb.ExecutionPayloadSegment{}
					require.NoError(t, back.UnmarshalSSZ(enc))
					got, gotHasher, err := FromProto(back)
					require.NoError(t, err)
					require.Equal(t, h.ID(), gotHasher.ID())
					require.DeepEqual(t, m.Descriptor, got.Descriptor)
					require.Equal(t, m.Index, got.Index)
					require.DeepEqual(t, m.Proof, got.Proof)
					require.DeepEqual(t, m.Data, got.Data)
					// Every decoded segment must verify against the committed root.
					require.NoError(t, got.Verify(gotHasher))
				}
			})
		}
	}
}

func TestFromProtoRejects(t *testing.T) {
	h, err := HasherByID(HashSHA256)
	require.NoError(t, err)
	msgs, err := BuildSegmentMessages(msgOfLen(500), 64, h)
	require.NoError(t, err)
	mutate := func(f func(pb *ethpb.ExecutionPayloadSegment)) *ethpb.ExecutionPayloadSegment {
		pb, err := msgs[1].ToProto()
		require.NoError(t, err)
		f(pb)
		return pb
	}
	tooDeep := func(pb *ethpb.ExecutionPayloadSegment) {
		for len(pb.Proof) <= MaxProofDepth {
			pb.Proof = append(pb.Proof, make([]byte, 32))
		}
	}
	for _, tc := range []struct {
		name string
		pb   *ethpb.ExecutionPayloadSegment
		want error
	}{
		{"nil message", nil, ErrDescriptorMismatch},
		{"nil descriptor", &ethpb.ExecutionPayloadSegment{}, ErrDescriptorMismatch},
		{"unknown hash id", mutate(func(pb *ethpb.ExecutionPayloadSegment) { pb.SegmentDescriptor.HashId = 200 }), ErrUnknownHashID},
		{"hash id beyond a byte", mutate(func(pb *ethpb.ExecutionPayloadSegment) { pb.SegmentDescriptor.HashId = 1 << 9 }), ErrWireField},
		{"version beyond a byte", mutate(func(pb *ethpb.ExecutionPayloadSegment) { pb.SegmentDescriptor.Version = 1 << 9 }), ErrWireField},
		{"encoding beyond a byte", mutate(func(pb *ethpb.ExecutionPayloadSegment) { pb.SegmentDescriptor.Encoding = 1 << 9 }), ErrWireField},
		{"root of the wrong width", mutate(func(pb *ethpb.ExecutionPayloadSegment) { pb.SegmentDescriptor.Root = pb.SegmentDescriptor.Root[:31] }), ErrDescriptorMismatch},
		{"proof element of the wrong width", mutate(func(pb *ethpb.ExecutionPayloadSegment) { pb.Proof[0] = pb.Proof[0][:31] }), ErrProofDigestSize},
		{"too many proof elements", mutate(tooDeep), ErrProofCount},
		{"oversized data", mutate(func(pb *ethpb.ExecutionPayloadSegment) { pb.Data = make([]byte, MaxSegmentSize+1) }), ErrSegmentSize},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := FromProto(tc.pb)
			require.ErrorIs(t, err, tc.want)
		})
	}

	t.Run("the wire type bounds proof and data itself", func(t *testing.T) {
		// SSZ enforces the list bounds on encode, so a message beyond them never reaches a peer.
		_, err := mutate(tooDeep).MarshalSSZ()
		require.Equal(t, true, err != nil, "a proof deeper than MaxProofDepth marshalled")
		_, err = mutate(func(pb *ethpb.ExecutionPayloadSegment) { pb.Data = make([]byte, MaxSegmentSize+1) }).MarshalSSZ()
		require.Equal(t, true, err != nil, "data beyond MaxSegmentSize marshalled")
	})
}

func TestToProtoRejects(t *testing.T) {
	h, err := HasherByID(HashSHA256)
	require.NoError(t, err)
	msgs, err := BuildSegmentMessages(msgOfLen(200), 64, h)
	require.NoError(t, err)

	t.Run("nil descriptor", func(t *testing.T) {
		m := &SegmentMessage{}
		_, err := m.ToProto()
		require.ErrorIs(t, err, ErrDescriptorMismatch)
	})
	t.Run("proof element of wrong size", func(t *testing.T) {
		m := *msgs[0]
		m.Proof = [][]byte{{1, 2, 3}}
		_, err := m.ToProto()
		require.ErrorIs(t, err, ErrProofDigestSize)
	})
	t.Run("too many proof elements", func(t *testing.T) {
		m := *msgs[0]
		m.Proof = make([][]byte, MaxProofDepth+1)
		for i := range m.Proof {
			m.Proof[i] = make([]byte, 32)
		}
		_, err := m.ToProto()
		require.ErrorIs(t, err, ErrProofCount)
	})
	t.Run("oversized data", func(t *testing.T) {
		m := *msgs[0]
		m.Data = make([]byte, MaxSegmentSize+1)
		_, err := m.ToProto()
		require.ErrorIs(t, err, ErrSegmentSize)
	})
}

func TestBuildSegmentMessagesRejects(t *testing.T) {
	h, err := HasherByID(HashSHA256)
	require.NoError(t, err)
	t.Run("empty message", func(t *testing.T) {
		_, err := BuildSegmentMessages(nil, 64, h)
		require.ErrorIs(t, err, ErrEmptyMessage)
	})
	t.Run("bad segment size", func(t *testing.T) {
		_, err := BuildSegmentMessages(msgOfLen(10), 0, h)
		require.ErrorIs(t, err, ErrSegmentSize)
	})
}

// TestBuildLayout covers the one builder the publisher uses: the encoding rides in the
// descriptor through the wire, plain and coded groups come from the same call, and what the
// layout cannot express is refused.
func TestBuildLayout(t *testing.T) {
	h, err := HasherByID(HashSHA256)
	require.NoError(t, err)
	msg := msgOfLen(700)

	for _, tc := range []struct {
		name   string
		layout Layout
		coded  bool
	}{
		{"plain raw", Layout{SegmentSize: 64, Hasher: h}, false},
		{"plain snappy", Layout{SegmentSize: 64, Hasher: h, Encoding: EncodingSnappy}, false},
		{"coded snappy", Layout{SegmentSize: 64, Hasher: h, Encoding: EncodingSnappy, Parity: 3}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			msgs, err := Build(msg, tc.layout)
			require.NoError(t, err)
			d := msgs[0].Descriptor
			require.Equal(t, tc.layout.Encoding, d.Encoding)
			require.Equal(t, tc.coded, d.Version == VersionCoded)
			require.Equal(t, 11+tc.layout.Parity, len(msgs))
			pb, err := msgs[0].ToProto()
			require.NoError(t, err)
			enc, err := pb.MarshalSSZ()
			require.NoError(t, err)
			back := &ethpb.ExecutionPayloadSegment{}
			require.NoError(t, back.UnmarshalSSZ(enc))
			got, gotHasher, err := FromProto(back)
			require.NoError(t, err)
			require.DeepEqual(t, d, got.Descriptor)
			require.NoError(t, got.Verify(gotHasher))
		})
	}

	t.Run("encodings give distinct groups", func(t *testing.T) {
		raw, err := Build(msg, Layout{SegmentSize: 64, Hasher: h})
		require.NoError(t, err)
		snap, err := Build(msg, Layout{SegmentSize: 64, Hasher: h, Encoding: EncodingSnappy})
		require.NoError(t, err)
		require.DeepEqual(t, raw[0].Descriptor.Root, snap[0].Descriptor.Root, "same bytes, same tree")
		require.Equal(t, false, bytes.Equal(raw[0].Descriptor.GroupID(h), snap[0].Descriptor.GroupID(h)), "different groups")
	})

	t.Run("rejects", func(t *testing.T) {
		for _, tc := range []struct {
			name   string
			layout Layout
			want   error
		}{
			{"nil hasher", Layout{SegmentSize: 64}, ErrDescriptorMismatch},
			{"unknown encoding", Layout{SegmentSize: 64, Hasher: h, Encoding: EncodingSnappy + 1}, ErrEncoding},
			{"negative parity", Layout{SegmentSize: 64, Hasher: h, Parity: -1}, ErrCodedShape},
		} {
			_, err := Build(msg, tc.layout)
			require.ErrorIs(t, err, tc.want, tc.name)
		}
	})

	t.Run("a descriptor with an unknown encoding does not validate", func(t *testing.T) {
		msgs, err := Build(msg, Layout{SegmentSize: 64, Hasher: h})
		require.NoError(t, err)
		bad := *msgs[0].Descriptor
		bad.Encoding = EncodingSnappy + 1
		require.ErrorIs(t, bad.Validate(h), ErrEncoding)
	})
}
