package segments

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"math/rand"
	"testing"

	"github.com/OffchainLabs/prysm/v7/testing/require"
)

// What is particular to Leopard over GF(2^8) (VersionCodedLeopard8). The tests the two Leopard
// codes share are in leopard_test.go.

func TestLeopard8DescriptorValidate(t *testing.T) {
	h := testHasher(t)
	msgs, err := BuildCodedSegmentMessagesVersion(VersionCodedLeopard8, randomBytes(4*100, 1), 100, 4, h)
	require.NoError(t, err)
	d := *msgs[0].Descriptor
	require.Equal(t, VersionCodedLeopard8, d.Version)

	t.Run("every segment has the data's length, as under the matrix code", func(t *testing.T) {
		old, err := BuildCodedSegmentMessages(randomBytes(4*100, 1), 100, 4, h)
		require.NoError(t, err)
		for i, m := range msgs {
			require.Equal(t, 100, len(m.Data))
			require.Equal(t, len(old[i].Data), len(m.Data))
			require.NoError(t, m.Verify(h))
		}
		// The same data segments, another code's parity, another group.
		require.Equal(t, true, bytes.Equal(old[0].Data, msgs[0].Data))
		require.Equal(t, false, bytes.Equal(old[4].Data, msgs[4].Data))
		require.Equal(t, false, bytes.Equal(old[0].Descriptor.GroupID(h), d.GroupID(h)))
	})
	t.Run("parity is between one and three segments a data segment, and at most 128", func(t *testing.T) {
		s := d // K = 4
		for count, ok := range map[uint32]bool{5: false, 7: false, 8: true, 12: true, 16: true, 17: false} {
			s.Count = count
			if got := s.Validate(h) == nil; got != ok {
				t.Errorf("K = 4, count %d: valid = %v, want %v", count, got, ok)
			}
		}
		for _, c := range []struct {
			k, count uint32
			ok       bool
		}{
			{128, 256, true}, {128, 257, false}, {127, 255, true}, {127, 256, false},
			{64, 128, true}, {64, 192, true}, {64, 193, false}, {43, 43 + 128, true}, {43, 43 + 129, false},
			{129, 258, false}, {1, 2, true}, {1, 4, true}, {1, 5, false},
		} {
			s.SegmentSize, s.TotalLength, s.Count = 1, uint64(c.k), c.count
			if got := s.Validate(h) == nil; got != c.ok {
				t.Errorf("K = %d, count %d: valid = %v, want %v", c.k, c.count, got, c.ok)
			}
		}
	})
	// The library takes any shape of at most 256 shards and indexes the field by the parity
	// count's next power of two plus the data count; beyond 256 that is out of its tables.
	t.Run("the encoder refuses the shapes the field has no points for", func(t *testing.T) {
		for _, c := range []struct {
			k, parity int
			ok        bool
		}{
			{128, 128, true}, {1, 128, true}, {127, 129, false}, {1, 129, false}, {129, 127, false},
			{192, 64, true}, {193, 64, false}, {200, 56, false}, {1, 255, false}, {0, 4, false}, {4, 0, false},
		} {
			_, err := leopardEncoder(VersionCodedLeopard8, c.k, c.parity)
			if got := err == nil; got != c.ok {
				t.Errorf("k = %d, parity = %d: encoder = %v, want %v (%v)", c.k, c.parity, got, c.ok, err)
			}
		}
		_, err := BuildCodedSegmentMessagesVersion(VersionCodedLeopard8, randomBytes(129, 1), 1, 129, h)
		require.ErrorIs(t, err, ErrCodedShape)
		_, err = BuildCodedSegmentMessagesVersion(VersionCodedLeopard8, randomBytes(4, 1), 1, 3, h)
		require.ErrorIs(t, err, ErrCodedShape)
	})
	t.Run("a segment of the wrong length is refused", func(t *testing.T) {
		segs := make([][]byte, d.Count)
		for i, m := range msgs {
			segs[i] = m.Data
		}
		segs[5] = make([]byte, 128) // a parity segment at the code's shard length
		_, err := RecoverAndVerify(&d, h, segs)
		require.ErrorIs(t, err, ErrSegmentLength)
	})
}

func TestReassemblerLeopard8Group(t *testing.T) {
	h := testHasher(t)
	msg := randomBytes(127*100+7, 9)
	msgs, err := BuildCodedSegmentMessagesVersion(VersionCodedLeopard8, msg, 100, 128, h)
	require.NoError(t, err)
	d := msgs[0].Descriptor
	k := int(d.Required())
	n := int(d.Count)
	require.Equal(t, 256, n)
	r, err := NewReassembler(ReassemblerConfig{AllowUnauthenticated: true, Retain: true})
	require.NoError(t, err)
	order := rand.New(rand.NewSource(1)).Perm(n) // #nosec G404 -- test data.
	var got []byte
	for i, idx := range order[:k] {
		out, err := r.Add(h, msgs[idx])
		require.NoError(t, err)
		if i == k-1 {
			got = out
		}
	}
	require.Equal(t, true, bytes.Equal(msg, got))
	require.Equal(t, true, r.Complete(d.GroupID(h)))
}

// Pinned vectors, as TestLeopardVectors: the input is formulaBytes.
func TestLeopard8Vectors(t *testing.T) {
	h := testHasher(t)
	cases := []struct {
		msgLen, segmentSize, parity int
		parityHash, root            string
	}{
		{4 * 64, 64, 4,
			"6934c58c3cabc9c125d48e411e671e85f8eb6f0b47cd3737fe05d2af4bd88093",
			"a88e0c001d002c4f2c71e270f3c170cba48f2c6c0e22429530e8f15cee6e8ef7"}, // K a power of two, whole blocks
		{63*100 + 7, 100, 64,
			"9ddf54c7ef2fdb2b949f4ba03bb72fb4a49a313d221008dec6ee41d664f69d9c",
			"a08b15793a2907c1d3adf4bcde86d90a2ff8ca2e74c6ce649b75cd87c2969717"}, // a short last segment, a size off the block
		{5*333 + 1, 333, 18,
			"e8b6c89df36b3d12944147b7ec20ce3f6e61bd85fa1e6c12f7c2c8ceb6e8d851",
			"6d270082b4cd1823e4c531df2a9f3e1d793dc832b7f5ac2588dec6f312e0d100"}, // K = 6, not a power of two; three times the parity
		{128 * 11328, 11328, 128,
			"81cc94659868d9a33fc5f171b1d2152167a050d916d86ab04f685577935cd300",
			"c34296389e9603defc34ecd68df991271e73e78b8171026d522c0e9bccc6f339"}, // N = 256, the bound
	}
	for _, tc := range cases {
		msgs, err := BuildCodedSegmentMessagesVersion(VersionCodedLeopard8, formulaBytes(tc.msgLen), tc.segmentSize, tc.parity, h)
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
// The definition, for K data and K' parity segments with K <= K' <= 128:
//   - The field is GF(2^8) = GF(2)[x] / (x^8 + x^4 + x^3 + x^2 + 1), polynomial 0x11D: the field
//     of the matrix code (gf256.go), whose multiplication table this test uses.
//   - A byte v names the field element that is the sum of the basis elements leopard8Basis[b]
//     over the set bits b of v. The same naming gives the evaluation points: point i is the
//     element that the number i names.
//   - Every byte of a shard is a symbol.
//   - Let m be the least power of two at or above K'. Per byte position, f is the polynomial of
//     degree below m that takes data segment i's byte at point m+i for i < K, and zero at points
//     m+K..2m-1. Parity segment j's byte is f at point j, for j < K'.
//
// Every shape a descriptor may have is covered (leopardShapeOK): K <= K' gives K <= m, and
// K' <= 128 keeps the points below 256. The shapes here have every m from 1 to 128 and go up to
// the bound: 128 + 128 has m = 128 and uses all 256 points.
func TestLeopard8MatchesDefinition(t *testing.T) {
	for _, shape := range [][2]int{{1, 1}, {2, 2}, {3, 3}, {4, 4}, {5, 5}, {8, 8}, {13, 13}, {3, 9}, {7, 7}, {7, 8}, {20, 20}, {32, 32}, {33, 40}, {64, 64}, {42, 126}, {100, 100}, {128, 128}, {1, 128}} {
		k, parity := shape[0], shape[1]
		data := make([][]byte, k)
		for i := range data {
			data[i] = randomBytes(64, int64(100*k+i))
		}
		got, err := leopardParity(VersionCodedLeopard8, data, parity)
		require.NoError(t, err)
		want := definition8Parity(data, parity)
		for j := range want {
			if !bytes.Equal(got[j], want[j]) {
				t.Fatalf("K=%d parity=%d: parity segment %d differs from the definition", k, parity, j)
			}
		}
	}
}

var leopard8Basis = [8]byte{1, 214, 152, 146, 86, 200, 88, 230}

// element8 is the field element that the byte v names.
func element8(v byte) byte {
	var e byte
	for b := 0; b < 8; b++ {
		if v&(1<<b) != 0 {
			e ^= leopard8Basis[b]
		}
	}
	return e
}

func definition8Parity(data [][]byte, parity int) [][]byte {
	k := len(data)
	size := len(data[0])
	m := 1
	for m < parity {
		m <<= 1
	}
	var name [256]byte // element -> the byte that names it
	for v := 0; v < 256; v++ {
		name[element8(byte(v))] = byte(v)
	}
	coef := make([][]byte, parity)
	for j := range coef {
		coef[j] = make([]byte, k)
		x := element8(byte(j))
		for i := 0; i < k; i++ {
			xi := element8(byte(m + i))
			num, den := byte(1), byte(1)
			for l := 0; l < m; l++ {
				if l == i {
					continue
				}
				xl := element8(byte(m + l))
				num = gfMul[num][x^xl]
				den = gfMul[den][xi^xl]
			}
			coef[j][i] = gfMul[num][gfInv[den]]
		}
	}
	out := make([][]byte, parity)
	for j := range out {
		out[j] = make([]byte, size)
		for pos := 0; pos < size; pos++ {
			var acc byte
			for i := 0; i < k; i++ {
				acc ^= gfMul[coef[j][i]][element8(data[i][pos])]
			}
			out[j][pos] = name[acc]
		}
	}
	return out
}
