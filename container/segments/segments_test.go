package segments

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/OffchainLabs/prysm/v7/testing/require"
)

// msgOfLen builds a deterministic message so failures are reproducible.
func msgOfLen(n int) []byte {
	out := make([]byte, n)
	for i := range out {
		out[i] = byte(i*7 + 13)
	}
	return out
}

func TestCommitVerifyJoin(t *testing.T) {
	cases := []struct {
		name        string
		msgLen      int
		segmentSize int
		wantCount   uint32
	}{
		{"single partial segment", 10, 64, 1},
		{"single exact segment", 64, 64, 1},
		{"two segments, last partial", 65, 64, 2},
		{"power of two count", 256, 64, 4},
		{"non power of two count", 5*64 + 1, 64, 6},
		{"one byte segments", 9, 1, 9},
		{"large-ish message", 100000, 1024, 98},
	}
	for _, id := range []HashID{HashSHA256, HashKeccak256} {
		h, err := HasherByID(id)
		require.NoError(t, err)
		for _, tc := range cases {
			t.Run(fmt.Sprintf("hash%d/%s", id, tc.name), func(t *testing.T) {
				msg := msgOfLen(tc.msgLen)
				d, segs, err := Commit(msg, tc.segmentSize, h)
				require.NoError(t, err)
				require.Equal(t, tc.wantCount, d.Count)
				require.Equal(t, uint64(tc.msgLen), d.TotalLength)
				require.NoError(t, d.Validate(h))

				tree, err := BuildTree(h, segs)
				require.NoError(t, err)
				require.DeepEqual(t, d.Root, tree.Root())

				// Every segment verifies against the committed root.
				for i := range segs {
					proof, err := tree.Proof(i)
					require.NoError(t, err)
					require.NoError(t, VerifySegment(d, h, i, segs[i], proof))
				}

				got, err := Join(d, h, segs)
				require.NoError(t, err)
				require.DeepEqual(t, msg, got)
			})
		}
	}
}

func TestVerifySegmentRejects(t *testing.T) {
	h, err := HasherByID(HashSHA256)
	require.NoError(t, err)
	msg := msgOfLen(5*64 + 7)
	d, segs, err := Commit(msg, 64, h)
	require.NoError(t, err)
	tree, err := BuildTree(h, segs)
	require.NoError(t, err)
	proof0, err := tree.Proof(0)
	require.NoError(t, err)

	t.Run("tampered segment byte", func(t *testing.T) {
		bad := bytes.Clone(segs[0])
		bad[0] ^= 0xff
		require.ErrorIs(t, VerifySegment(d, h, 0, bad, proof0), ErrRootMismatch)
	})
	t.Run("tampered proof element", func(t *testing.T) {
		bad := make([][]byte, len(proof0))
		for i := range proof0 {
			bad[i] = bytes.Clone(proof0[i])
		}
		bad[0][0] ^= 0xff
		require.ErrorIs(t, VerifySegment(d, h, 0, segs[0], bad), ErrRootMismatch)
	})
	t.Run("reordered proof cannot steer the path", func(t *testing.T) {
		proof2, err := tree.Proof(2)
		require.NoError(t, err)
		reversed := make([][]byte, len(proof2))
		for i := range proof2 {
			reversed[i] = proof2[len(proof2)-1-i]
		}
		require.ErrorIs(t, VerifySegment(d, h, 2, segs[2], reversed), ErrRootMismatch)
	})
	t.Run("right segment wrong index", func(t *testing.T) {
		require.ErrorIs(t, VerifySegment(d, h, 1, segs[0], proof0), ErrRootMismatch)
	})
	t.Run("index beyond count", func(t *testing.T) {
		require.ErrorIs(t, VerifySegment(d, h, int(d.Count), segs[0], proof0), ErrIndexOutOfRange)
	})
	t.Run("short segment at non-final index", func(t *testing.T) {
		require.ErrorIs(t, VerifySegment(d, h, 0, segs[0][:10], proof0), ErrSegmentLength)
	})
	t.Run("full-size segment at final index", func(t *testing.T) {
		last := int(d.Count) - 1
		proof, err := tree.Proof(last)
		require.NoError(t, err)
		padded := make([]byte, d.SegmentSize)
		copy(padded, segs[last])
		require.ErrorIs(t, VerifySegment(d, h, last, padded, proof), ErrSegmentLength)
	})
	t.Run("truncated proof", func(t *testing.T) {
		require.ErrorIs(t, VerifySegment(d, h, 0, segs[0], proof0[:len(proof0)-1]), ErrProofLength)
	})
	t.Run("wrong digest size in proof", func(t *testing.T) {
		bad := make([][]byte, len(proof0))
		copy(bad, proof0)
		bad[0] = []byte{1, 2, 3}
		require.ErrorIs(t, VerifySegment(d, h, 0, segs[0], bad), ErrProofDigestSize)
	})
	t.Run("hasher disagrees with descriptor", func(t *testing.T) {
		other, err := HasherByID(HashKeccak256)
		require.NoError(t, err)
		require.ErrorIs(t, VerifySegment(d, other, 0, segs[0], proof0), ErrDescriptorMismatch)
	})
}

func TestDescriptorValidate(t *testing.T) {
	h, err := HasherByID(HashSHA256)
	require.NoError(t, err)
	valid := func() *Descriptor {
		d, _, err := Commit(msgOfLen(200), 64, h)
		require.NoError(t, err)
		return d
	}

	t.Run("count inconsistent with length", func(t *testing.T) {
		d := valid()
		d.Count += 1
		require.ErrorIs(t, d.Validate(h), ErrDescriptorMismatch)
	})
	t.Run("zero count", func(t *testing.T) {
		d := valid()
		d.Count = 0
		require.ErrorIs(t, d.Validate(h), ErrTooManySegments)
	})
	t.Run("count above cap", func(t *testing.T) {
		d := valid()
		d.Count = MaxSegments + 1
		require.ErrorIs(t, d.Validate(h), ErrTooManySegments)
	})
	t.Run("zero segment size", func(t *testing.T) {
		d := valid()
		d.SegmentSize = 0
		require.ErrorIs(t, d.Validate(h), ErrSegmentSize)
	})
	t.Run("total length above cap", func(t *testing.T) {
		d := valid()
		d.TotalLength = MaxTotalLength + 1
		require.ErrorIs(t, d.Validate(h), ErrTotalLength)
	})
	t.Run("wrong root size", func(t *testing.T) {
		d := valid()
		d.Root = d.Root[:16]
		require.ErrorIs(t, d.Validate(h), ErrDescriptorMismatch)
	})
	t.Run("unknown version", func(t *testing.T) {
		d := valid()
		d.Version = Version + 1
		require.ErrorIs(t, d.Validate(h), ErrDescriptorMismatch)
	})
}

func TestDescriptorGroupID(t *testing.T) {
	h, err := HasherByID(HashSHA256)
	require.NoError(t, err)
	d, _, err := Commit(msgOfLen(1000), 128, h)
	require.NoError(t, err)

	t.Run("canonical bytes have the fixed layout", func(t *testing.T) {
		enc := d.MarshalCanonical()
		require.Equal(t, descriptorFixedLen+h.Size(), len(enc))
		require.Equal(t, Version, enc[0])
		require.Equal(t, byte(h.ID()), enc[1])
		require.Equal(t, byte(EncodingRaw), enc[2])
	})
	t.Run("the encoding is part of the group id", func(t *testing.T) {
		other := *d
		other.Encoding = EncodingSnappy
		require.Equal(t, false, bytes.Equal(d.GroupID(h), other.GroupID(h)))
	})
	t.Run("group id is stable and descriptor-bound", func(t *testing.T) {
		same := *d
		same.Root = bytes.Clone(d.Root)
		require.DeepEqual(t, d.GroupID(h), same.GroupID(h))
		other := *d
		other.SegmentSize += 1
		require.Equal(t, false, bytes.Equal(d.GroupID(h), other.GroupID(h)))
	})
}

func TestSplitRejects(t *testing.T) {
	t.Run("empty message", func(t *testing.T) {
		_, err := Split(nil, 64)
		require.ErrorIs(t, err, ErrEmptyMessage)
	})
	t.Run("zero segment size", func(t *testing.T) {
		_, err := Split(msgOfLen(10), 0)
		require.ErrorIs(t, err, ErrSegmentSize)
	})
	t.Run("negative segment size", func(t *testing.T) {
		_, err := Split(msgOfLen(10), -1)
		require.ErrorIs(t, err, ErrSegmentSize)
	})
	t.Run("segment size above cap", func(t *testing.T) {
		_, err := Split(msgOfLen(10), MaxSegmentSize+1)
		require.ErrorIs(t, err, ErrSegmentSize)
	})
	t.Run("too many segments", func(t *testing.T) {
		_, err := Split(msgOfLen(MaxSegments+1), 1)
		require.ErrorIs(t, err, ErrTooManySegments)
	})
}

func TestJoinRejects(t *testing.T) {
	h, err := HasherByID(HashSHA256)
	require.NoError(t, err)
	d, segs, err := Commit(msgOfLen(200), 64, h)
	require.NoError(t, err)

	t.Run("missing segment count", func(t *testing.T) {
		_, err := Join(d, h, segs[:len(segs)-1])
		require.ErrorIs(t, err, ErrIncompleteSegments)
	})
	t.Run("nil hole", func(t *testing.T) {
		holed := make([][]byte, len(segs))
		copy(holed, segs)
		holed[1] = nil
		_, err := Join(d, h, holed)
		require.ErrorIs(t, err, ErrIncompleteSegments)
	})
	t.Run("wrong length segment", func(t *testing.T) {
		bad := make([][]byte, len(segs))
		copy(bad, segs)
		bad[0] = bad[0][:1]
		_, err := Join(d, h, bad)
		require.ErrorIs(t, err, ErrSegmentLength)
	})
}

func TestHasherRegistry(t *testing.T) {
	t.Run("unknown id", func(t *testing.T) {
		_, err := HasherByID(HashID(200))
		require.ErrorIs(t, err, ErrUnknownHashID)
	})
	t.Run("distinct hashes produce distinct roots", func(t *testing.T) {
		sha, err := HasherByID(HashSHA256)
		require.NoError(t, err)
		kec, err := HasherByID(HashKeccak256)
		require.NoError(t, err)
		msg := msgOfLen(500)
		dSha, _, err := Commit(msg, 64, sha)
		require.NoError(t, err)
		dKec, _, err := Commit(msg, 64, kec)
		require.NoError(t, err)
		require.Equal(t, false, bytes.Equal(dSha.Root, dKec.Root))
	})
	t.Run("leaf node and descriptor domains are separated", func(t *testing.T) {
		h, err := HasherByID(HashSHA256)
		require.NoError(t, err)
		payload := []byte("same bytes in three domains")
		leaf := h.HashLeaf(payload)
		raw := h.Hash(payload)
		require.Equal(t, false, bytes.Equal(leaf, raw))
		// A leaf digest must not be reproducible as a node digest of the same material.
		zero := make([]byte, h.Size())
		require.Equal(t, false, bytes.Equal(leaf, h.HashNode(payload, zero)))
	})
}

func TestBuildTreeAndProofEdges(t *testing.T) {
	h, err := HasherByID(HashSHA256)
	require.NoError(t, err)

	t.Run("no leaves", func(t *testing.T) {
		_, err := BuildTree(h, nil)
		require.ErrorIs(t, err, ErrNoLeaves)
	})
	t.Run("single leaf has empty proof", func(t *testing.T) {
		tree, err := BuildTree(h, [][]byte{[]byte("only")})
		require.NoError(t, err)
		proof, err := tree.Proof(0)
		require.NoError(t, err)
		require.Equal(t, 0, len(proof))
		require.DeepEqual(t, h.HashLeaf([]byte("only")), tree.Root())
		require.NoError(t, VerifyProof(h, tree.Root(), []byte("only"), 0, 1, proof))
	})
	t.Run("proof index out of range", func(t *testing.T) {
		tree, err := BuildTree(h, [][]byte{[]byte("a"), []byte("b"), []byte("c")})
		require.NoError(t, err)
		require.Equal(t, 3, tree.Count())
		_, err = tree.Proof(3)
		require.ErrorIs(t, err, ErrIndexOutOfRange)
	})
	t.Run("padded leaves do not admit a forged extra segment", func(t *testing.T) {
		// Count 3 pads to width 4; index 3 must be unreachable via the descriptor path.
		segs := [][]byte{[]byte("aa"), []byte("bb"), []byte("cc")}
		tree, err := BuildTree(h, segs)
		require.NoError(t, err)
		err = VerifyProof(h, tree.Root(), make([]byte, h.Size()), 3, 3, [][]byte{})
		require.ErrorIs(t, err, ErrIndexOutOfRange)
	})
}
