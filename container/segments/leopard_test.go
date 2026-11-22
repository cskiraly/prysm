package segments

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"testing"

	"github.com/OffchainLabs/prysm/v7/testing/require"
	"github.com/klauspost/reedsolomon"
)

// formulaBytes is a message another implementation can write down: byte o is the top byte of
// (o + 1) * 2654435761 modulo 2^32. The pinned vectors take their input from it.
func formulaBytes(n int) []byte {
	out := make([]byte, n)
	for o := range out {
		out[o] = byte((uint32(o+1) * 2654435761) >> 24) // lint:ignore uintcast -- test data, wraps by design.
	}
	return out
}

// randomBytes is a reproducible message for tests that pin no bytes.
func randomBytes(n int, seed int64) []byte {
	out := make([]byte, n)
	r := rand.New(rand.NewSource(seed)) // #nosec G404 -- test data.
	_, _ = r.Read(out)
	return out
}

func TestLeopardRoundTrip(t *testing.T) {
	h := testHasher(t)
	cases := []struct {
		name        string
		msgLen      int
		segmentSize int
		parity      int
	}{
		{"N=256", 128 * 100, 100, 128},
		{"N=258", 129 * 100, 100, 129},
		{"N=512", 256 * 100, 100, 256},
		{"N=1024", 512 * 64, 64, 512},
		{"N=512, short last segment", 255*100 + 7, 100, 256},
		{"N=258, last segment one byte", 128*333 + 1, 333, 129},
		{"segment size a multiple of the block", 8 * 128, 128, 8},
		{"single data segment", 10, 64, 2},
		{"single data segment, single parity", 10, 64, 1},
		{"segment size 1", 5, 1, 7},
		{"three times the parity", 3 * 70, 70, 9},
		{"three times the parity, N=400", 100 * 65, 65, 300},
		// N = 2K at the tuning grid's other data counts.
		{"N=8", 4 * 100, 100, 4},
		{"N=64", 32 * 100, 100, 32},
		{"N=128", 64 * 100, 100, 64},
		// 256 shards of 256 bytes: 64 KiB in all, from where the GF(2^8) decoder takes its other
		// path when at most a quarter of the parity count is missing (the "all but" sets below).
		{"N=256, 64 KiB of shards", 128 * 256, 256, 128},
		// Shards above 32 KiB, which the GF(2^8) code works through in pieces of that size.
		{"shards of 40000 bytes", 4*40000 - 5, 40000, 4},
		{"the bound", 4096 * 3, 3, 4096},
		{"the bound, three times the parity", 2048 * 3, 3, 6144},
	}
	ran := map[uint8]int{}
	for _, version := range []uint8{VersionCodedLeopard, VersionCodedLeopard8} {
		for _, tc := range cases {
			// Every shape the code takes: all of them over GF(2^16), those of at most 128 parity
			// segments over GF(2^8).
			if !leopardShapeOK(version, (tc.msgLen+tc.segmentSize-1)/tc.segmentSize, tc.parity) {
				continue
			}
			ran[version]++
			t.Run(fmt.Sprintf("version %d/%s", version, tc.name), func(t *testing.T) {
				msg := randomBytes(tc.msgLen, int64(tc.msgLen))
				msgs, err := BuildCodedSegmentMessagesVersion(version, msg, tc.segmentSize, tc.parity, h)
				require.NoError(t, err)
				d := msgs[0].Descriptor
				k := int(d.Required())
				n := int(d.Count)
				require.Equal(t, len(msgs), n)
				require.Equal(t, n, k+tc.parity)
				require.Equal(t, version, d.Version)
				require.NoError(t, d.Validate(h))

				// Every segment proves against the committed root; data segments keep their
				// length, parity segments have the code's shard size.
				for i, m := range msgs {
					require.NoError(t, m.Verify(h))
					switch {
					case i < k-1:
						require.Equal(t, tc.segmentSize, len(m.Data))
					case i == k-1:
						require.Equal(t, tc.msgLen-(k-1)*tc.segmentSize, len(m.Data))
					default:
						// A whole shard over GF(2^16); over GF(2^8) the segment size, as the data.
						want := leopardShardSize(tc.segmentSize)
						if version == VersionCodedLeopard8 {
							want = tc.segmentSize
						}
						require.Equal(t, want, len(m.Data))
					}
				}
				// A parity segment survives the wire encoding.
				raw, err := msgs[n-1].Marshal()
				require.NoError(t, err)
				back, _, err := UnmarshalSegmentMessage(raw)
				require.NoError(t, err)
				require.NoError(t, back.Verify(h))
				require.DeepEqual(t, msgs[n-1].Data, back.Data)

				// Parity is at least as much as data, so K parity segments alone are a set.
				sets := map[string][]int{
					"the data":            seq(0, k),
					"the first k+1":       seq(0, k+1),
					"all":                 seq(0, n),
					"the first k parity":  seq(k, 2*k),
					"the last k segments": seq(n-k, n),
				}
				r := rand.New(rand.NewSource(int64(n))) // #nosec G404 -- test data.
				for i := 0; i < 20; i++ {
					sets[fmt.Sprintf("random %d", i)] = r.Perm(n)[:k]
				}
				// Nearly everything present and a data segment among the few missing: the decoder's
				// path for at most a quarter of the parity count missing.
				for miss := 1; miss <= tc.parity/4 && miss <= 3; miss++ {
					for i := 0; i < 3; i++ {
						perm := r.Perm(n)
						gone := map[int]bool{r.Intn(k): true}
						for _, j := range perm {
							if len(gone) == miss {
								break
							}
							gone[j] = true
						}
						var set []int
						for j := 0; j < n; j++ {
							if !gone[j] {
								set = append(set, j)
							}
						}
						sets[fmt.Sprintf("all but %d, try %d", miss, i)] = set
					}
				}
				for name, set := range sets {
					segs := make([][]byte, n)
					for _, i := range set {
						segs[i] = msgs[i].Data
					}
					got, err := RecoverAndVerify(d, h, segs)
					require.NoError(t, err, name)
					if !bytes.Equal(msg, got) {
						t.Fatalf("%s: recovered payload differs", name)
					}
				}

				// One segment short of k is not enough.
				segs := make([][]byte, n)
				for _, i := range r.Perm(n)[:k-1] {
					segs[i] = msgs[i].Data
				}
				_, err = RecoverAndVerify(d, h, segs)
				require.ErrorIs(t, err, ErrNotEnoughSegments)
			})
		}
	}
	require.Equal(t, len(cases), ran[VersionCodedLeopard])
	require.Equal(t, 11, ran[VersionCodedLeopard8]) // the cases of at most 128 parity segments
}

func seq(from, to int) []int {
	out := make([]int, 0, to-from)
	for i := from; i < to; i++ {
		out = append(out, i)
	}
	return out
}

// commitLeaves is a builder that commits whatever leaves it likes under a Leopard descriptor.
func commitLeaves(t *testing.T, h Hasher, d Descriptor, leaves [][]byte) *Descriptor {
	t.Helper()
	tree, err := BuildTree(h, leaves)
	require.NoError(t, err)
	d.Root = tree.Root()
	return &d
}

// A group is rejected from every set of its segments when its committed leaves are not a
// codeword, and not only from the sets that happen to hold the bad leaf.
func TestLeopardRejectsInconsistentCodeword(t *testing.T) {
	// The matrix code too: the check is one for every coded version.
	for _, version := range []uint8{VersionCoded, VersionCodedLeopard, VersionCodedLeopard8} {
		t.Run(fmt.Sprintf("version %d", version), func(t *testing.T) { testLeopardInconsistent(t, version) })
	}
}

func testLeopardInconsistent(t *testing.T, version uint8) {
	h := testHasher(t)
	const k, parity, size = 5, 5, 100
	n := k + parity
	everySet := func(t *testing.T, d *Descriptor, leaves [][]byte) {
		t.Helper()
		count := 0
		for mask := 0; mask < 1<<n; mask++ {
			segs := make([][]byte, n)
			have := 0
			for i := 0; i < n; i++ {
				if mask&(1<<i) != 0 {
					segs[i] = leaves[i]
					have++
				}
			}
			if have < k {
				continue
			}
			count++
			if _, err := RecoverAndVerify(d, h, segs); err != ErrCodewordMismatch {
				t.Fatalf("set %010b (%d segments): err = %v, want ErrCodewordMismatch", mask, have, err)
			}
		}
		require.Equal(t, 638, count) // the sets of 5 to 10 out of 10
	}
	honest := func(msgLen int) (*Descriptor, [][]byte) {
		msgs, err := BuildCodedSegmentMessagesVersion(version, randomBytes(msgLen, 3), size, parity, h)
		require.NoError(t, err)
		leaves := make([][]byte, n)
		for i, m := range msgs {
			leaves[i] = bytes.Clone(m.Data)
		}
		return msgs[0].Descriptor, leaves
	}

	t.Run("a corrupt parity segment", func(t *testing.T) {
		d, leaves := honest(k * size)
		leaves[k+1][0] ^= 1
		everySet(t, commitLeaves(t, h, *d, leaves), leaves)
	})
	t.Run("a corrupt data segment", func(t *testing.T) {
		d, leaves := honest(k * size)
		leaves[2][size-1] ^= 0x80
		everySet(t, commitLeaves(t, h, *d, leaves), leaves)
	})
	// The parity a builder would commit for these padded data shards, in wire form.
	wireParity := func(d *Descriptor, padded [][]byte) [][]byte {
		var par [][]byte
		var err error
		if version == VersionCoded {
			par, err = rsParity(padded, parity)
		} else {
			par, err = leopardParity(version, padded, parity)
		}
		require.NoError(t, err)
		out := make([][]byte, parity)
		for i, p := range par {
			out[i], err = wireShard(d, k+i, p)
			require.NoError(t, err)
		}
		return out
	}
	everySetAccepts := func(t *testing.T, d *Descriptor, leaves [][]byte) {
		t.Helper()
		for mask := 0; mask < 1<<n; mask++ {
			segs := make([][]byte, n)
			have := 0
			for i := 0; i < n; i++ {
				if mask&(1<<i) != 0 {
					segs[i] = leaves[i]
					have++
				}
			}
			if have < k {
				continue
			}
			if _, err := RecoverAndVerify(d, h, segs); err != nil {
				t.Fatalf("set %010b (%d segments): %v", mask, have, err)
			}
		}
	}
	t.Run("parity over a non-zero pad of the last data segment", func(t *testing.T) {
		d, leaves := honest((k-1)*size + 7)
		padded := paddedShards(d, leaves[:k])
		padded[k-1][50] = 0xAA
		copy(leaves[k:], wireParity(d, padded))
		everySet(t, commitLeaves(t, h, *d, leaves), leaves)
	})
	// Over GF(2^16) a symbol's two bytes sit half a block apart, so a pad byte beyond the segment
	// size changes parity bytes that are on the wire: the group is inconsistent. Over GF(2^8) it
	// changes only the parity bytes beyond the segment size, which are on nobody's wire: the
	// committed leaves are the honest group's, and it passes from every set.
	t.Run("parity over a non-zero pad up to the block", func(t *testing.T) {
		if version == VersionCoded {
			t.Skip("the matrix code's shard is the segment: there is no such pad")
		}
		d, leaves := honest(k * size)
		padded := paddedShards(d, leaves[:k])
		require.Equal(t, 128, len(padded[0]))
		padded[1][size+3] = 1 // beyond the segment size, inside the code's shard
		before := bytes.Join(leaves[k:], nil)
		copy(leaves[k:], wireParity(d, padded))
		if version == VersionCodedLeopard8 {
			require.Equal(t, true, bytes.Equal(before, bytes.Join(leaves[k:], nil)))
			everySetAccepts(t, commitLeaves(t, h, *d, leaves), leaves)
			return
		}
		everySet(t, commitLeaves(t, h, *d, leaves), leaves)
	})
	t.Run("the honest group passes from every set", func(t *testing.T) {
		d, leaves := honest((k-1)*size + 7)
		everySetAccepts(t, d, leaves)
	})
}

func TestLeopardDescriptorValidate(t *testing.T) {
	h := testHasher(t)
	msgs, err := BuildCodedSegmentMessagesVersion(VersionCodedLeopard, randomBytes(4*100, 1), 100, 4, h)
	require.NoError(t, err)
	d := *msgs[0].Descriptor

	t.Run("count at or below implied is rejected", func(t *testing.T) {
		bad := d
		bad.Count = bad.Required()
		require.NotNil(t, bad.Validate(h))
	})
	t.Run("count above 256 is accepted, above the bound rejected", func(t *testing.T) {
		big := d
		big.SegmentSize, big.TotalLength = 1, 4096 // K = 4096
		big.Count = MaxLeopardSegments
		require.NoError(t, big.Validate(h))
		big.Count = MaxLeopardSegments + 1
		require.NotNil(t, big.Validate(h))
		big.SegmentSize, big.TotalLength = 1, 200 // K = 200
		big.Count = 400
		require.NoError(t, big.Validate(h))
	})
	t.Run("parity is between one and three segments a data segment", func(t *testing.T) {
		s := d // K = 4
		for count, ok := range map[uint32]bool{5: false, 7: false, 8: true, 12: true, 16: true, 17: false, 8192: false} {
			s.Count = count
			if got := s.Validate(h) == nil; got != ok {
				t.Errorf("K = 4, count %d: valid = %v, want %v", count, got, ok)
			}
		}
		_, err := BuildCodedSegmentMessagesVersion(VersionCodedLeopard, randomBytes(4*100, 1), 100, 3, h)
		require.ErrorIs(t, err, ErrCodedShape)
		_, err = BuildCodedSegmentMessagesVersion(VersionCodedLeopard, randomBytes(4*100, 1), 100, 13, h)
		require.ErrorIs(t, err, ErrCodedShape)
	})
	// One short data segment and thousands of full-size parity segments: valid by its counts
	// alone, and gigabytes to check. The parity bound refuses it before anything is allocated.
	t.Run("a descriptor that would take gigabytes to check is refused", func(t *testing.T) {
		hostile := d
		hostile.Count, hostile.SegmentSize, hostile.TotalLength = MaxLeopardSegments, MaxSegmentSize, 1
		require.NotNil(t, hostile.Validate(h))
		segs := make([][]byte, hostile.Count)
		segs[0] = []byte{0}
		_, err := RecoverAndVerify(&hostile, h, segs)
		require.ErrorIs(t, err, ErrDescriptorMismatch)
	})
	// Version 2 refused this before the comparison was reordered too, further down (the wrapped
	// sum passed the count check and the matrix refused it); version 3 panicked.
	t.Run("a parity count near the top of int is refused, under every code", func(t *testing.T) {
		for _, version := range []uint8{VersionCoded, VersionCodedLeopard, VersionCodedLeopard8} {
			_, err := BuildCodedSegmentMessagesVersion(version, []byte{1}, 1, math.MaxInt, h)
			require.ErrorIs(t, err, ErrCodedShape)
		}
		// The encoders' own wrapper too, which the builder's check stands in front of.
		for _, version := range []uint8{VersionCodedLeopard, VersionCodedLeopard8} {
			for _, parity := range []int{math.MaxInt, math.MaxInt/2 + 2, 1 << 40} {
				_, err := leopardEncoder(version, 1, parity)
				require.ErrorIs(t, err, ErrCodedShape)
			}
		}
	})
	// The code's workspace is twice the parity count's next power of two, in shards, taken by
	// the library in one allocation: a valid descriptor must keep it within MaxLeopardWorkBytes.
	t.Run("a group whose workspace passes the bound is refused", func(t *testing.T) {
		require.Equal(t, uint64(1<<30), leopardWorkBytes(512, 1<<20))
		require.Equal(t, uint64(2*8192*64), leopardWorkBytes(4097, 1))
		require.Equal(t, uint64(512<<20), leopardWorkBytes(8192, 32<<10))
		w := d
		w.SegmentSize, w.TotalLength = MaxSegmentSize, 256*MaxSegmentSize // K = 256
		w.Count = 512                                                     // 256 parity: 2 * 256 MiB of work
		require.NoError(t, w.Validate(h))
		w.Count = 256 + 512 // 512 parity: 2 * 512 MiB, the bound itself
		require.NoError(t, w.Validate(h))
		w.Count = 256 + 513 // 513 parity: 1024 is the next power of two, 2 * 1024 MiB
		require.NotNil(t, w.Validate(h))
		_, err := BuildCodedSegmentMessagesVersion(VersionCodedLeopard, make([]byte, 2*MaxSegmentSize), MaxSegmentSize, 6, h)
		require.NoError(t, err) // 6 parity of 1 MiB: 16 MiB of work
		_, err = BuildCodedSegmentMessagesVersion(VersionCodedLeopard, make([]byte, 171*MaxSegmentSize), MaxSegmentSize, 513, h)
		require.ErrorIs(t, err, ErrCodedShape)
		w.Count = 1024 // 768 parity, the shape that overflowed a 32-bit allocation
		require.NotNil(t, w.Validate(h))
		// The largest group of 32 KiB segments is inside it.
		w.SegmentSize, w.TotalLength, w.Count = 32<<10, 4096*(32<<10), MaxLeopardSegments
		require.NoError(t, w.Validate(h))
	})
	t.Run("parts metadata takes a coded count up to the Leopard bound", func(t *testing.T) {
		for count, ok := range map[uint32]bool{MaxCodedSegments + 1: true, MaxLeopardSegments: true, MaxLeopardSegments + 1: false} {
			m, err := NewPartsMetadata(count)
			require.NoError(t, err)
			m.Required = count / 2
			enc, err := m.Marshal()
			require.NoError(t, err)
			require.Equal(t, PartialVersionCoded, enc[0])
			_, err = UnmarshalPartsMetadata(enc)
			if got := err == nil; got != ok {
				t.Errorf("coded metadata of count %d: accepted = %v, want %v (%v)", count, got, ok, err)
			}
		}
	})
	t.Run("the GF(2^8) code keeps its bound", func(t *testing.T) {
		old := d
		old.Version = VersionCoded
		old.Count = MaxCodedSegments
		require.NoError(t, old.Validate(h))
		old.Count = MaxCodedSegments + 1
		require.NotNil(t, old.Validate(h))
		_, err := BuildCodedSegmentMessages(randomBytes(200*10, 1), 10, 57, h)
		require.ErrorIs(t, err, ErrCodedShape)
	})
	t.Run("the builders refuse a group above the bound and a version that is not coded", func(t *testing.T) {
		_, err := BuildCodedSegmentMessagesVersion(VersionCodedLeopard, randomBytes(4097*2, 1), 2, 4097, h)
		require.ErrorIs(t, err, ErrCodedShape)
		_, err = BuildCodedSegmentMessagesVersion(Version, randomBytes(100, 1), 10, 10, h)
		require.ErrorIs(t, err, ErrCodedShape)
	})
	t.Run("a segment of the wrong length is refused", func(t *testing.T) {
		segs := make([][]byte, d.Count)
		for i, m := range msgs {
			segs[i] = m.Data
		}
		segs[5] = segs[5][:100] // a parity segment at the data length
		_, err := RecoverAndVerify(&d, h, segs)
		require.ErrorIs(t, err, ErrSegmentLength)
		require.NotNil(t, VerifySegment(&d, h, 5, segs[5], msgs[5].Proof))
	})
	t.Run("the two codes give different groups for one payload", func(t *testing.T) {
		old, err := BuildCodedSegmentMessages(randomBytes(4*100, 1), 100, 4, h)
		require.NoError(t, err)
		require.Equal(t, false, bytes.Equal(old[0].Descriptor.GroupID(h), d.GroupID(h)))
	})
}

func TestReassemblerLeopardGroup(t *testing.T) {
	h := testHasher(t)
	msg := randomBytes(299*100+7, 9)
	msgs, err := BuildCodedSegmentMessagesVersion(VersionCodedLeopard, msg, 100, 300, h)
	require.NoError(t, err)
	d := msgs[0].Descriptor
	k := int(d.Required())
	n := int(d.Count)
	require.Equal(t, 600, n)

	r, err := NewReassembler(ReassemblerConfig{AllowUnauthenticated: true, Retain: true})
	require.NoError(t, err)
	order := rand.New(rand.NewSource(1)).Perm(n) // #nosec G404 -- test data.
	var got []byte
	for i, idx := range order[:k] {
		out, err := r.Add(h, msgs[idx])
		require.NoError(t, err)
		if i < k-1 {
			require.Equal(t, 0, len(out), fmt.Sprintf("delivered early at segment %d", i))
		} else {
			got = out
		}
	}
	require.Equal(t, true, bytes.Equal(msg, got))
	require.Equal(t, true, r.Complete(d.GroupID(h)))

	t.Run("an inconsistent group is rejected at K and dropped", func(t *testing.T) {
		honest, err := BuildCodedSegmentMessagesVersion(VersionCodedLeopard, randomBytes(5*100, 2), 100, 5, h)
		require.NoError(t, err)
		leaves := make([][]byte, len(honest))
		for i, m := range honest {
			leaves[i] = bytes.Clone(m.Data)
		}
		leaves[7][0] ^= 1
		tree, err := BuildTree(h, leaves)
		require.NoError(t, err)
		bad := *honest[0].Descriptor
		bad.Root = tree.Root()
		r, err := NewReassembler(ReassemblerConfig{AllowUnauthenticated: true, Retain: true})
		require.NoError(t, err)
		for i := 0; i < 5; i++ {
			proof, err := tree.Proof(i)
			require.NoError(t, err)
			out, err := r.Add(h, &SegmentMessage{Descriptor: &bad, Index: uint32(i), Proof: proof, Data: leaves[i]})
			if i < 4 {
				require.NoError(t, err)
				require.Equal(t, 0, len(out))
				continue
			}
			require.ErrorIs(t, err, ErrCodewordMismatch)
		}
		_, held := r.Held(bad.GroupID(h))
		require.Equal(t, false, held)
	})
}

// Groups of both Leopard codes built and decoded side by side, for the race detector: each
// field's tables are shared by every call (each call has its own encoder and work buffers).
func TestLeopardConcurrentDecodes(t *testing.T) {
	h := testHasher(t)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			msg := randomBytes(40*100+g, int64(g))
			version := VersionCodedLeopard
			if g%2 == 1 {
				version = VersionCodedLeopard8
			}
			msgs, err := BuildCodedSegmentMessagesVersion(version, msg, 100, 41, h)
			if err != nil {
				t.Error(err)
				return
			}
			d := msgs[0].Descriptor
			r := rand.New(rand.NewSource(int64(g))) // #nosec G404 -- test data.
			for i := 0; i < 20; i++ {
				segs := make([][]byte, d.Count)
				for _, j := range r.Perm(len(segs))[:d.Required()] {
					segs[j] = msgs[j].Data
				}
				got, err := RecoverAndVerify(d, h, segs)
				if err != nil || !bytes.Equal(got, msg) {
					t.Errorf("group %d, try %d: %v", g, i, err)
					return
				}
			}
		}(g)
	}
	wg.Wait()
}

// Pinned vectors: the SHA-256 of a group's parity segments, concatenated, and the group's root,
// for inputs given by a formula (formulaBytes). They pin the code's bytes against a change of
// the library or of the padding rule, on whichever of the library's paths the host's CPU takes.
func TestLeopardVectors(t *testing.T) {
	h := testHasher(t)
	cases := []struct {
		msgLen, segmentSize, parity int
		parityHash, root            string
	}{
		{4 * 64, 64, 4,
			"249d92041b21c3689ab1bb66d675fb86d3e0f7af801a32b06875e1d7d0294460",
			"582a353d202bd502c8c71dc6e5780eeb0db1d57ea6f649abc95235e993d31af8"}, // K a power of two, whole blocks
		{63*100 + 7, 100, 64,
			"aae4e1c60c522245bce13bdfa787d15d4f34ce83787bc4a9ede41fc8ea546031",
			"c6e14bee11e4f637f414d746f63fb1592197f1e40ade95890a617e1abb759533"}, // a short last segment, a size off the block
		{5*333 + 1, 333, 18,
			"958addabd33bd8ec8b776e8bd95c61f9fd5c287b594b3f96e0d3cef373b9eb51",
			"ca9693d22a10b74146e4e232f0a4f18f97e46c6bcc73a73a8b048de2edc08597"}, // K = 6, not a power of two; three times the parity
		{256 * 11328, 11328, 256,
			"6f23af3bb808d260ba8b70ae3a59a4ed23a57f40791f1664e5279843a34819bf",
			"dd7a3716b985c71d7d28e58ec4c067c572c8b4e3e884b2541ae6764ed8288640"}, // N = 512
		{1023*70 + 69, 70, 1024,
			"8aeebe6041ee8842db55552ce0b94be2290f5504ba74fcc62a86215ebf712b50",
			"080bebbb814ad4f729c3bbdca1b489f15be7f4da466a850b09840afecb8569b9"}, // N = 2048
	}
	for _, tc := range cases {
		msgs, err := BuildCodedSegmentMessagesVersion(VersionCodedLeopard, formulaBytes(tc.msgLen), tc.segmentSize, tc.parity, h)
		require.NoError(t, err)
		sum := sha256.New()
		for _, m := range msgs[len(msgs)-tc.parity:] {
			sum.Write(m.Data)
		}
		gotParity := hex.EncodeToString(sum.Sum(nil))
		gotRoot := hex.EncodeToString(msgs[0].Descriptor.Root)
		if gotParity != tc.parityHash || gotRoot != tc.root {
			t.Errorf("%d bytes in %d-byte segments, %d parity: parity %s root %s", tc.msgLen, tc.segmentSize, tc.parity, gotParity, gotRoot)
		}
	}
}

// The code written down from its definition alone, slowly, must give the library's parity.
//
// The definition, for K data and K' parity segments:
//   - The field is GF(2^16) = GF(2)[x] / (x^16 + x^5 + x^3 + x^2 + 1), polynomial 0x1002D.
//   - A 16-bit number v names the field element that is the sum of the basis elements
//     leopardBasis[b] over the set bits b of v. The same naming gives the evaluation points:
//     point i is the element that the number i names.
//   - A shard is cut into blocks of 64 bytes. Symbol s of a block (s = 0..31) is the 16-bit
//     number whose low byte is the block's byte s and whose high byte is its byte 32+s.
//   - Let m be the least power of two at or above K'. Per symbol position, f is the polynomial
//     of degree below m that takes data segment i's symbol at point m+i for i < K, and zero at
//     points m+K..2m-1. Parity segment j's symbol is f at point j, for j < K'.
//
// This holds while K <= m, which covers every group with at least as much parity as data: the
// only groups a descriptor may describe (leopardShapeOK). The last four shapes are there for the
// points, and three of them are the code alone, not a group's shape. 300 + 300 has m = 512 and
// points up to 811. One data segment with 4096 parity has m = 4096: the data at point 4096, zeros
// up to 8191. With 4097 and with 6144 parity m is 8192, the largest a group of MaxLeopardSegments
// can have (2048 data, 6144 parity): 40 data segments at points 8192 to 8231, zeros up to 16383.
// The points of a group stay below 16384, so basis elements 14 and 15 never name a point.
func TestLeopardMatchesDefinition(t *testing.T) {
	for _, shape := range [][2]int{{1, 1}, {2, 2}, {3, 3}, {4, 4}, {5, 5}, {8, 8}, {13, 13}, {3, 9}, {7, 7}, {7, 8}, {300, 300}, {1, 4096}, {40, 4097}, {40, 6144}} {
		k, parity := shape[0], shape[1]
		size := 128
		if k+parity > 64 {
			size = 64
		}
		data := make([][]byte, k)
		for i := range data {
			data[i] = randomBytes(size, int64(100*k+i))
		}
		got, err := leopardParity(VersionCodedLeopard, data, parity)
		require.NoError(t, err)
		// Every parity segment, or eight of them where the slow reference would take minutes.
		rows := seq(0, parity)
		if k*parity > 1<<17 {
			rows = []int{0, 1, parity / 3, parity / 2, 4095, 4096, parity - 2, parity - 1}
		}
		want := definitionParity(data, parity, rows)
		for i, j := range rows {
			if !bytes.Equal(got[j], want[i]) {
				t.Fatalf("K=%d parity=%d: parity segment %d differs from the definition", k, parity, j)
			}
		}
	}
}

var leopardBasis = [16]uint16{
	0x0001, 0xACCA, 0x3C0E, 0x163E, 0xC582, 0xED2E, 0x914C, 0x4012,
	0x6C98, 0x10D8, 0x6A72, 0xB900, 0xFDB8, 0xFB34, 0xFF38, 0x991E,
}

// gf16Mul multiplies two elements of GF(2^16) in the standard representation, modulo 0x1002D.
func gf16Mul(a, b uint16) uint16 {
	var acc uint32
	x := uint32(a)
	for i := 0; i < 16; i++ {
		if b&(1<<i) != 0 {
			acc ^= x
		}
		x <<= 1
		if x&0x10000 != 0 {
			x ^= 0x1002D
		}
	}
	return uint16(acc)
}

func gf16Inv(a uint16) uint16 {
	// a^(2^16 - 2)
	out := uint16(1)
	sq := a
	for e := 65534; e > 0; e >>= 1 {
		if e&1 != 0 {
			out = gf16Mul(out, sq)
		}
		sq = gf16Mul(sq, sq)
	}
	return out
}

// element is the field element that the number v names.
func element(v uint16) uint16 {
	var e uint16
	for b := 0; b < 16; b++ {
		if v&(1<<b) != 0 {
			e ^= leopardBasis[b]
		}
	}
	return e
}

// definitionParity is the parity segments numbered rows, of parity, for data, by the definition.
func definitionParity(data [][]byte, parity int, rows []int) [][]byte {
	k := len(data)
	size := len(data[0])
	m := 1
	for m < parity {
		m <<= 1
	}
	name := make(map[uint16]uint16, 1<<16) // element -> the number that names it
	for v := 0; v < 1<<16; v++ {
		name[element(uint16(v))] = uint16(v)
	}
	// coef[j][i] is the Lagrange weight of the value at point m+i in f(point j), over the m
	// interpolation points m..2m-1.
	pts := make([]uint16, m)
	for l := range pts {
		pts[l] = element(uint16(m + l))
	}
	coef := make([][]uint16, len(rows))
	for r, j := range rows {
		coef[r] = make([]uint16, k)
		x := element(uint16(j))
		for i := 0; i < k; i++ {
			xi := pts[i]
			num, den := uint16(1), uint16(1)
			for l, xl := range pts {
				if l == i {
					continue
				}
				num = gf16Mul(num, x^xl)
				den = gf16Mul(den, xi^xl)
			}
			coef[r][i] = gf16Mul(num, gf16Inv(den))
		}
	}
	out := make([][]byte, len(rows))
	for j := range out {
		out[j] = make([]byte, size)
	}
	for block := 0; block < size; block += 64 {
		for s := 0; s < 32; s++ {
			lo, hi := block+s, block+32+s
			for j := range rows {
				var acc uint16
				for i := 0; i < k; i++ {
					sym := uint16(data[i][lo]) | uint16(data[i][hi])<<8
					acc ^= gf16Mul(coef[j][i], element(sym))
				}
				v := name[acc]
				out[j][lo], out[j][hi] = byte(v), byte(v>>8)
			}
		}
	}
	return out
}

// The library's plain GF(2^8) code is the table code of rs.go, byte for byte: below 257
// segments the table code has a fast implementation with the same wire bytes.
func TestLibraryGF8EqualsTable(t *testing.T) {
	for _, s := range [][2]int{{1, 2}, {4, 4}, {4, 8}, {5, 3}, {16, 16}, {64, 64}, {128, 128}, {200, 56}, {2, 30}, {1, 255}, {255, 1}} {
		k, parity := s[0], s[1]
		data := make([][]byte, k)
		for i := range data {
			data[i] = randomBytes(77, int64(1000*k+i))
		}
		want, err := rsParity(data, parity)
		require.NoError(t, err)
		got, err := libraryGF8Parity(data, parity)
		require.NoError(t, err)
		for i := range want {
			if !bytes.Equal(want[i], got[i]) {
				t.Fatalf("K=%d parity=%d: parity segment %d differs", k, parity, i)
			}
		}
	}
}

// libraryGF8Parity is rsParity by the library's plain encoder, on one goroutine.
func libraryGF8Parity(data [][]byte, parity int) ([][]byte, error) {
	k := len(data)
	enc, err := reedsolomon.New(k, parity, reedsolomon.WithMaxGoroutines(1))
	if err != nil {
		return nil, err
	}
	shards := make([][]byte, k+parity)
	copy(shards, data)
	for i := k; i < k+parity; i++ {
		shards[i] = make([]byte, len(data[0]))
	}
	if err := enc.Encode(shards); err != nil {
		return nil, err
	}
	return shards[k:], nil
}
