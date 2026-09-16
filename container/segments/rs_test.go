package segments

import (
	"fmt"
	"testing"

	"github.com/OffchainLabs/prysm/v7/testing/require"
)

func testHasher(t *testing.T) Hasher {
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
			require.Equal(t, uint8(VersionCoded), d.Version)
			require.NoError(t, d.Validate(h))

			// Every segment, parity included, proves against the committed root.
			for _, m := range msgs {
				require.NoError(t, m.Verify(h))
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

	// A builder that commits leaves which are not a codeword: keep the systematic
	// segments, corrupt one parity segment, and commit the mix. Every leaf still proves
	// against the root -- the tree binds arbitrary leaves -- so only the codeword check
	// can catch it.
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

	// A decode that uses the corrupted parity segment must be rejected.
	segs := make([][]byte, n)
	segs[k] = bad
	for i := 1; i < k; i++ {
		segs[i] = leaves[i]
	}
	_, err = RecoverAndVerify(badDesc, h, segs)
	require.ErrorIs(t, err, ErrCodewordMismatch)

	// A decode from the systematic segments alone reconstructs the payload but must
	// still be rejected: a group where subsets disagree is not a valid group.
	segs = make([][]byte, n)
	for i := 0; i < k; i++ {
		segs[i] = leaves[i]
	}
	_, err = RecoverAndVerify(badDesc, h, segs)
	require.ErrorIs(t, err, ErrCodewordMismatch)
}

func TestCodedDescriptorValidate(t *testing.T) {
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
		require.NotNil(t, bad.Validate(h))
	})
	t.Run("count beyond the field is rejected", func(t *testing.T) {
		bad := d
		bad.Count = MaxCodedSegments + 1
		require.NotNil(t, bad.Validate(h))
	})
	t.Run("short last systematic segment stays short", func(t *testing.T) {
		short, err := BuildCodedSegmentMessages(msgOfLen(4*64+7), 64, 4, h)
		require.NoError(t, err)
		sd := short[0].Descriptor
		got, err := sd.SegmentLength(int(sd.Required()) - 1)
		require.NoError(t, err)
		require.Equal(t, 7, got)
	})
}

func TestReassemblerCodedGroup(t *testing.T) {
	h := testHasher(t)
	msg := msgOfLen(4*64 + 7)
	msgs, err := BuildCodedSegmentMessages(msg, 64, 4, h)
	require.NoError(t, err)
	d := msgs[0].Descriptor
	k := int(d.Required())
	n := int(d.Count)
	groupID := d.GroupID(h)

	r, err := NewReassembler(ReassemblerConfig{AllowUnauthenticated: true, Retain: true})
	require.NoError(t, err)

	// Feed parity-heavy: skip systematic index 1 and 3, take parity instead.
	feed := []int{0, 2, n - 1, n - 2, n - 3}
	require.Equal(t, k, len(feed))
	var got []byte
	for i, idx := range feed {
		out, err := r.Add(h, msgs[idx])
		require.NoError(t, err)
		if i < k-1 {
			require.Equal(t, 0, len(out), fmt.Sprintf("delivered early at segment %d", i))
		} else {
			got = out
		}
	}
	require.DeepEqual(t, msg, got)
	require.Equal(t, true, r.Complete(groupID))

	t.Run("held advertises only received segments", func(t *testing.T) {
		held, ok := r.Held(groupID)
		require.Equal(t, true, ok)
		for _, idx := range feed {
			require.Equal(t, true, held.Has(uint32(idx)))
		}
		require.Equal(t, false, held.Has(1))
		require.Equal(t, false, held.Has(3))
	})
	t.Run("recovered segments cannot be served", func(t *testing.T) {
		_, ok := r.Segment(groupID, 1)
		require.Equal(t, false, ok)
		m, ok := r.Segment(groupID, uint32(n-1))
		require.Equal(t, true, ok)
		require.NoError(t, m.Verify(h))
	})
	t.Run("later segments buffer without a second delivery", func(t *testing.T) {
		out, err := r.Add(h, msgs[1])
		require.NoError(t, err)
		require.Equal(t, 0, len(out))
		m, ok := r.Segment(groupID, 1)
		require.Equal(t, true, ok)
		require.NoError(t, m.Verify(h))
	})
}

// Benchmarks for the coded path at the standing operating point (1 MiB payload, 32 KiB
// segments, rate 1/2). The virtual-clock harness cannot see this cost, so these numbers are
// the CPU term a real deployment adds on top of every measured completion time.
//
// They are also a ceiling, not a floor: the codec is the simplest table-lookup GF(2^8)
// implementation, kept dependency-free on purpose. SIMD Reed-Solomon (e.g.
// klauspost/reedsolomon) runs the same arithmetic one to two orders of magnitude faster.

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
		if _, err := BuildCodedSegmentMessages(payload, DefaultSegmentSize, 32, h); err != nil {
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
	msgs, err := BuildCodedSegmentMessages(payload, DefaultSegmentSize, 32, h)
	if err != nil {
		b.Fatal(err)
	}
	d := msgs[0].Descriptor
	k := int(d.Required()) // lint:ignore uintcast -- bounded by MaxSegments via Validate.
	n := int(d.Count)

	// Worst case: every systematic segment missing, all k recovered from parity alone.
	worst := make([][]byte, n)
	for i := k; i < n; i++ {
		worst[i] = msgs[i].Data
	}
	// Typical case: half pushed, half pulled -- 16 of 32 systematic present.
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
