package segments

import (
	"testing"

	ethpb "github.com/OffchainLabs/prysm/v7/proto/prysm/v1alpha1"
	"github.com/OffchainLabs/prysm/v7/testing/require"
)

func testHasher(t *testing.T) Hasher {
	t.Helper()
	h, err := HasherByID(HashSHA256)
	require.NoError(t, err)
	return h
}

func TestCodedRoundTrip(t *testing.T) {
	h := testHasher(t)
	cases := []struct {
		name        string
		msgLen      int
		segmentSize int
		parity      int
	}{
		{"exact segments", 4 * 64, 64, 4},
		{"short last segment", 4*64 + 7, 64, 4},
		{"single data segment", 10, 64, 2},
		{"heavy parity", 2 * 64, 64, 30},
		{"field-size group", 200 * 32, 32, 56},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			msg := msgOfLen(tc.msgLen)
			msgs, err := BuildCodedSegmentMessages(msg, tc.segmentSize, tc.parity, h)
			require.NoError(t, err)
			d := msgs[0].Descriptor
			k := int(d.Required())
			n := int(d.Count)
			require.Equal(t, len(msgs), n)
			require.Equal(t, VersionCoded, d.Version)
			require.Equal(t, (tc.msgLen+tc.segmentSize-1)/tc.segmentSize, k)
			require.Equal(t, k+tc.parity, n)
			require.NoError(t, d.Validate(h))

			// Every segment, parity included, proves against the committed root, and survives
			// the wire type.
			for _, m := range msgs {
				require.NoError(t, m.Verify(h))
				pb, err := m.ToProto()
				require.NoError(t, err)
				enc, err := pb.MarshalSSZ()
				require.NoError(t, err)
				back := &ethpb.ExecutionPayloadSegment{}
				require.NoError(t, back.UnmarshalSSZ(enc))
				got, gotHasher, err := FromProto(back)
				require.NoError(t, err)
				require.NoError(t, got.Verify(gotHasher))
				require.DeepEqual(t, d, got.Descriptor)
			}

			keeps := map[string]func(i int) bool{
				"first k":     func(i int) bool { return i < k },
				"last k":      func(i int) bool { return i >= n-k },
				"every other": func(i int) bool { return i%2 == 0 || i >= n-(k-(n+1)/2) },
				"parity only": func(i int) bool { return i >= k || i < k-(n-k) },
			}
			for name, keep := range keeps {
				t.Run(name, func(t *testing.T) {
					segs := make([][]byte, n)
					kept := 0
					for i, m := range msgs {
						if keep(i) && kept < k {
							segs[i] = m.Data
							kept++
						}
					}
					if kept < k {
						t.Skipf("selector keeps %d of %d", kept, k)
					}
					got, err := RecoverAndVerify(d, h, segs)
					require.NoError(t, err)
					require.DeepEqual(t, msg, got)
				})
			}

			t.Run("one short of k is refused", func(t *testing.T) {
				segs := make([][]byte, n)
				for i := 0; i < k-1; i++ {
					segs[i] = msgs[i].Data
				}
				_, err := RecoverAndVerify(d, h, segs)
				require.ErrorIs(t, err, ErrNotEnoughSegments)
			})
		})
	}
}

func TestCodedRejectsInconsistentCodeword(t *testing.T) {
	h := testHasher(t)
	msg := msgOfLen(4 * 64)
	msgs, err := BuildCodedSegmentMessages(msg, 64, 4, h)
	require.NoError(t, err)
	d := msgs[0].Descriptor
	k := int(d.Required())
	n := int(d.Count)

	// A builder that commits leaves which are not a codeword: keep the systematic segments,
	// corrupt one parity segment, and commit the mix. Every leaf still proves against the
	// root -- the tree binds arbitrary leaves -- so only the codeword check can catch it.
	leaves := make([][]byte, n)
	for i, m := range msgs {
		leaves[i] = m.Data
	}
	bad := make([]byte, len(leaves[k]))
	copy(bad, leaves[k])
	bad[0] ^= 1
	leaves[k] = bad
	tree, err := BuildTree(h, leaves)
	require.NoError(t, err)
	badDesc := &Descriptor{
		Version:     VersionCoded,
		HashID:      d.HashID,
		Count:       d.Count,
		SegmentSize: d.SegmentSize,
		TotalLength: d.TotalLength,
		Root:        tree.Root(),
	}

	t.Run("a decode through the corrupted parity is refused", func(t *testing.T) {
		segs := make([][]byte, n)
		segs[k] = bad
		for i := 1; i < k; i++ {
			segs[i] = leaves[i]
		}
		_, err = RecoverAndVerify(badDesc, h, segs)
		require.ErrorIs(t, err, ErrCodewordMismatch)
	})

	t.Run("a decode from the systematic segments alone is refused too", func(t *testing.T) {
		// It reconstructs the payload, but a group where subsets disagree is not a valid group.
		segs := make([][]byte, n)
		for i := 0; i < k; i++ {
			segs[i] = leaves[i]
		}
		_, err = RecoverAndVerify(badDesc, h, segs)
		require.ErrorIs(t, err, ErrCodewordMismatch)
	})
}

func TestCodedDescriptor(t *testing.T) {
	h := testHasher(t)
	msgs, err := BuildCodedSegmentMessages(msgOfLen(4*64), 64, 4, h)
	require.NoError(t, err)
	d := *msgs[0].Descriptor

	t.Run("parity segment lengths are full", func(t *testing.T) {
		for i := int(d.Required()); i < int(d.Count); i++ {
			got, err := d.SegmentLength(i)
			require.NoError(t, err)
			require.Equal(t, int(d.SegmentSize), got)
		}
	})
	t.Run("count at or below implied is rejected", func(t *testing.T) {
		bad := d
		bad.Count = bad.Required()
		require.ErrorIs(t, bad.Validate(h), ErrDescriptorMismatch)
	})
	t.Run("count beyond the field is rejected", func(t *testing.T) {
		bad := d
		bad.Count = MaxCodedSegments + 1
		require.ErrorIs(t, bad.Validate(h), ErrDescriptorMismatch)
	})
	t.Run("short last systematic segment stays short", func(t *testing.T) {
		short, err := BuildCodedSegmentMessages(msgOfLen(4*64+7), 64, 4, h)
		require.NoError(t, err)
		sd := short[0].Descriptor
		got, err := sd.SegmentLength(int(sd.Required()) - 1)
		require.NoError(t, err)
		require.Equal(t, 7, got)
	})
	t.Run("a plain group's required count is its count", func(t *testing.T) {
		plain, err := BuildSegmentMessages(msgOfLen(4*64), 64, h)
		require.NoError(t, err)
		require.Equal(t, plain[0].Descriptor.Count, plain[0].Descriptor.Required())
	})
	t.Run("join refuses a coded group", func(t *testing.T) {
		segs := make([][]byte, d.Count)
		for i, m := range msgs {
			segs[i] = m.Data
		}
		_, err := Join(&d, h, segs)
		require.ErrorIs(t, err, ErrDescriptorMismatch)
	})
	t.Run("coded groups differ from plain ones in group id", func(t *testing.T) {
		plain, err := BuildSegmentMessages(msgOfLen(4*64), 64, h)
		require.NoError(t, err)
		require.Equal(t, false, string(plain[0].Descriptor.GroupID(h)) == string(d.GroupID(h)))
	})
}

func TestBuildCodedSegmentMessagesRejects(t *testing.T) {
	h := testHasher(t)
	for _, tc := range []struct {
		name        string
		msgLen      int
		segmentSize int
		parity      int
	}{
		{"no parity", 256, 64, 0},
		{"negative parity", 256, 64, -1},
		{"beyond the field", 200 * 32, 32, 57},
		{"empty message", 0, 64, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := BuildCodedSegmentMessages(msgOfLen(tc.msgLen), tc.segmentSize, tc.parity, h)
			require.NotNil(t, err)
		})
	}
}

// Benchmarks for the coded path at the branch's operating point: a 1 MiB payload at 16 KiB,
// rate one half. This CPU cost is what a deployment adds on top of every completion the
// study measured, and it is the table implementation's ceiling, not the floor a SIMD library
// would set.

const benchSegmentSize = 16 << 10

func benchPayload() []byte {
	out := make([]byte, 1<<20)
	for i := range out {
		out[i] = byte(i*2654435761 + i>>8)
	}
	return out
}

func BenchmarkCodedBuild(b *testing.B) {
	h, err := HasherByID(HashSHA256)
	if err != nil {
		b.Fatal(err)
	}
	payload := benchPayload()
	b.SetBytes(1 << 20)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := BuildCodedSegmentMessages(payload, benchSegmentSize, 64, h); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCodedRecover(b *testing.B) {
	h, err := HasherByID(HashSHA256)
	if err != nil {
		b.Fatal(err)
	}
	payload := benchPayload()
	msgs, err := BuildCodedSegmentMessages(payload, benchSegmentSize, 64, h)
	if err != nil {
		b.Fatal(err)
	}
	d := msgs[0].Descriptor
	k := int(d.Required()) // lint:ignore uintcast -- bounded by MaxCodedSegments via Validate.
	n := int(d.Count)

	// Worst case: every systematic segment missing, all k recovered from parity alone.
	worst := make([][]byte, n)
	for i := k; i < n; i++ {
		worst[i] = msgs[i].Data
	}
	// Typical case: half pushed, half pulled.
	typical := make([][]byte, n)
	for i := 0; i < k/2; i++ {
		typical[i] = msgs[i].Data
	}
	for i := k; i < k+k/2; i++ {
		typical[i] = msgs[i].Data
	}

	for name, segs := range map[string][][]byte{"worst-all-parity": worst, "typical-half": typical} {
		b.Run(name, func(b *testing.B) {
			b.SetBytes(1 << 20)
			for i := 0; i < b.N; i++ {
				in := make([][]byte, n)
				copy(in, segs)
				if _, err := RecoverAndVerify(d, h, in); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
