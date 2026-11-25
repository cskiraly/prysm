package segments

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/OffchainLabs/prysm/v7/testing/require"
	"github.com/klauspost/reedsolomon"
)

// The table implementation of the code, kept as the oracle the codec's arithmetic is checked
// against: the systematic Vandermonde matrix over GF(2^8) mod 0x11d, built and applied a byte
// at a time with the tables in gf256_test.go.

// oracleMatrix returns the systematic n x k encoding matrix, row-major.
//
// Rows are the Vandermonde matrix V[i][j] = i^j multiplied by the inverse of its top k x k
// block, so the first k rows are the identity (systematic) and any k rows remain
// invertible: any k rows of V form a Vandermonde matrix with distinct evaluation points.
func oracleMatrix(t testing.TB, n, k int) []byte {
	t.Helper()
	require.Equal(t, true, k > 0 && n >= k && n <= MaxCodedSegments, "shape n=%d k=%d", n, k)
	v := make([]byte, n*k)
	for i := 0; i < n; i++ {
		x := byte(1)
		for j := 0; j < k; j++ {
			v[i*k+j] = x
			x = gfMul[x][byte(i)]
		}
	}
	top := make([]byte, k*k)
	copy(top, v[:k*k])
	require.Equal(t, true, gfInvertMatrix(top, k), "singular top block")
	m := make([]byte, n*k)
	for i := 0; i < n; i++ {
		for j := 0; j < k; j++ {
			var acc byte
			for l := 0; l < k; l++ {
				acc ^= gfMul[v[i*k+l]][top[l*k+j]]
			}
			m[i*k+j] = acc
		}
	}
	return m
}

// oracleParity computes the parity shards for k equal-length data shards.
func oracleParity(t testing.TB, data [][]byte, parity int) [][]byte {
	t.Helper()
	k := len(data)
	m := oracleMatrix(t, k+parity, k)
	out := make([][]byte, parity)
	for i := range out {
		out[i] = make([]byte, len(data[0]))
	}
	gfMatMul(out, m[k*k:], k, data)
	return out
}

// oracleData returns k random shards of size bytes.
func oracleData(k, size int, r *rand.Rand) [][]byte {
	data := make([][]byte, k)
	for i := range data {
		data[i] = make([]byte, size)
		for j := range data[i] {
			data[i][j] = byte(r.Uint32())
		}
	}
	return data
}

// firstDiff returns the first index at which two shard lists differ, or -1 when they are equal.
func firstDiff(a, b [][]byte) int {
	for i := range max(len(a), len(b)) {
		if i >= len(a) || i >= len(b) || !bytes.Equal(a[i], b[i]) {
			return i
		}
	}
	return -1
}

// recoverFrom keeps the given indices of a codeword, copied, and recovers the rest.
func recoverFrom(t *testing.T, codeword [][]byte, keep []int, k int) [][]byte {
	t.Helper()
	shards := make([][]byte, len(codeword))
	for _, i := range keep {
		shards[i] = bytes.Clone(codeword[i])
	}
	require.NoError(t, rsRecover(shards, k))
	return shards
}

// TestRSMatchesOracle checks the codec's arithmetic against the oracle: the same parity byte for
// byte, and the whole codeword back from K of its segments, all of the parity among them when
// there is that much.
func TestRSMatchesOracle(t *testing.T) {
	for _, k := range rsBenchKs {
		parities := []int{k}
		if k > 1 {
			parities = append(parities, 1)
		}
		for _, parity := range parities {
			for _, size := range []int{1, 100, 16384} {
				t.Run(fmt.Sprintf("k=%d parity=%d size=%d", k, parity, size), func(t *testing.T) {
					r := rand.New(rand.NewPCG(uint64(k), uint64(size)))
					data := oracleData(k, size, r)
					want := oracleParity(t, data, parity)
					got, err := rsParity(data, parity)
					require.NoError(t, err)
					require.Equal(t, -1, firstDiff(want, got), "parity differs from the oracle's")

					codeword := append(append([][]byte{}, data...), want...)
					n := k + parity
					subsets := [][]int{}
					last := make([]int, 0, k)
					for i := n - k; i < n; i++ {
						last = append(last, i)
					}
					subsets = append(subsets, last)
					for range 4 {
						subsets = append(subsets, r.Perm(n)[:k])
					}
					for _, keep := range subsets {
						require.Equal(t, -1, firstDiff(codeword, recoverFrom(t, codeword, keep, k)),
							"codeword not recovered from %v", keep)
					}
				})
			}
		}
	}
}

// TestRSOracleTellsCodesApart is the oracle comparison's control. A different code over the same
// field, the library's Cauchy matrix, gives different parity at every K above 1; and one bit
// changed in one of the K segments recovery starts from changes every segment it recovers.
func TestRSOracleTellsCodesApart(t *testing.T) {
	for _, k := range rsBenchKs[1:] {
		t.Run(fmt.Sprintf("k=%d", k), func(t *testing.T) {
			r := rand.New(rand.NewPCG(uint64(k), 1))
			data := oracleData(k, 100, r)
			want := oracleParity(t, data, k)

			cauchy, err := reedsolomon.New(k, k, reedsolomon.WithCauchyMatrix())
			require.NoError(t, err)
			shards := append(append([][]byte{}, data...), make([][]byte, k)...)
			for i := k; i < 2*k; i++ {
				shards[i] = make([]byte, 100)
			}
			require.NoError(t, cauchy.Encode(shards))
			require.NotEqual(t, -1, firstDiff(want, shards[k:]), "the Cauchy code's parity matches the oracle's")

			codeword := append(append([][]byte{}, data...), want...)
			perm := r.Perm(2 * k)
			keep, missing := perm[:k], perm[k:]
			changedWord := append([][]byte{}, codeword...)
			changedWord[keep[0]] = bytes.Clone(codeword[keep[0]])
			changedWord[keep[0]][0] ^= 1
			got := recoverFrom(t, changedWord, keep, k)
			for _, i := range missing {
				require.Equal(t, false, bytes.Equal(codeword[i], got[i]),
					"recovered segment %d unchanged by a change to segment %d", i, keep[0])
			}
		})
	}
}
