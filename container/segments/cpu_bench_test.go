package segments

import (
	"bytes"
	"fmt"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/klauspost/reedsolomon"
)

// The CPU column: what a coded group costs its builder and each receiver, per shape of the
// payload-size tuning grid and per code. The virtual-clock harness cannot see this cost.
//
// Run on one thread:
//
//	GOMAXPROCS=1 go test -run '^$' -bench '^BenchmarkCPU$' -benchtime 20x -count 5 ./container/segments/
//
// SEGMENT_BENCH_SHAPES=<payload KiB>:<K>,... limits the shapes; SEGMENT_BENCH_CODES=<code>,...
// the codes.
//
// Operations, per group:
//   - encode: the builder's whole work (split, pad, parity, tree, a proof per segment).
//   - tree: the tree and proofs alone, over the same leaves (a part of encode).
//   - proofs: a receiver checks K segments against the root, each on arrival.
//   - decode-half, decode-parity: the receiver's decode and codeword check, from half the data
//     and half the parity, and from the parity alone.
//
// Codes:
//   - table: the GF(2^8) code by rs.go's tables (VersionCoded), where N <= 256.
//   - gf8lib: the same code, byte for byte, by the library's plain encoder.
//   - leo8: Leopard-RS over GF(2^8) (VersionCodedLeopard8), where N <= 256.
//   - leo16: Leopard-RS over GF(2^16) (VersionCodedLeopard).
//   - plain: no code, K segments of the uncompressed payload (the specification's shape above
//     MAX_CHUNKS); encode is the tree and proofs, decode the join.
//
// gf8lib has no descriptor version of its own: it is version 2's code by another implementation,
// and its builder and receiver are written out here. The receiver does what RecoverAndVerify does
// (the data, its pad, all the parity again, the tree); it validates neither the descriptor nor
// the segments' lengths, which the real paths do.
//
// What the rows are, for a table that compares them:
//   - Every code's times include making its encoder and every allocation: they are a group's
//     cost as this package pays it, not the code's arithmetic alone.
//   - table is rs.go as it is: it builds its matrix per call, twice in a check, and computes all K
//     data shards when any is missing. gf8lib is what that code costs with a fast implementation.
//   - Every code's receiver does the one check: recover the data, encode all the parity again,
//     build the tree. (Until the matrix code's check was made sound, table and gf8lib recomputed
//     only the segments a node lacked: tables measured before that have lower decode figures for
//     these two rows, by up to a half from half the data and up to two times from parity alone.)
//   - plain's encode is BuildSegmentMessages, which builds the tree twice (Commit, then the
//     proofs' tree); its decode-half is the join of all K segments: a plain group has no other.
//   - proofs checks the K data segments' proofs, the fewest a receiver checks.

// cpuShapes is the payload-size tuning grid: the payload in KiB and the data segment count K,
// N = 2K.
var cpuShapes = [][2]int{
	{128, 32}, {128, 16}, {128, 8}, {128, 4},
	{256, 64}, {256, 32}, {256, 16}, {256, 8},
	{512, 128}, {512, 64}, {512, 32}, {512, 16},
	{1024, 256}, {1024, 128}, {1024, 64}, {1024, 32}, {1024, 16},
	{2048, 256}, {2048, 128}, {2048, 64}, {2048, 32},
	{4096, 256}, {4096, 128}, {4096, 64},
}

// cpuCompressed is the compressed length of a mainnet-like payload of each size: the medians of
// the payload-size sweep behind SEACAST's Figure 6 (in MiB to three places).
var cpuCompressed = map[int]int{
	128: 93323, 256: 186647, 512: 373293, 1024: 745537, 2048: 1492123, 4096: 2979004,
}

type cpuCode struct {
	name   string
	maxN   int
	encode func(msg []byte, size, k int, h Hasher) ([]*SegmentMessage, error)
	decode func(d *Descriptor, h Hasher, segs [][]byte) ([]byte, error)
}

func cpuCodes() []cpuCode {
	lib := func(k, parity int) (reedsolomon.Encoder, error) {
		return reedsolomon.New(k, parity, reedsolomon.WithMaxGoroutines(1))
	}
	return []cpuCode{
		{name: "table", maxN: MaxCodedSegments,
			encode: func(msg []byte, size, k int, h Hasher) ([]*SegmentMessage, error) {
				return BuildCodedSegmentMessages(msg, size, k, h)
			},
			decode: RecoverAndVerify},
		{name: "gf8lib", maxN: MaxCodedSegments,
			encode: func(msg []byte, size, k int, h Hasher) ([]*SegmentMessage, error) {
				return benchBuild(lib, msg, size, k, h)
			},
			decode: func(d *Descriptor, h Hasher, segs [][]byte) ([]byte, error) {
				return benchRecover(lib, d, h, segs)
			}},
		{name: "leo8", maxN: MaxCodedSegments,
			encode: func(msg []byte, size, k int, h Hasher) ([]*SegmentMessage, error) {
				return BuildCodedSegmentMessagesVersion(VersionCodedLeopard8, msg, size, k, h)
			},
			decode: RecoverAndVerify},
		{name: "leo16", maxN: MaxLeopardSegments,
			encode: func(msg []byte, size, k int, h Hasher) ([]*SegmentMessage, error) {
				return BuildCodedSegmentMessagesVersion(VersionCodedLeopard, msg, size, k, h)
			},
			decode: RecoverAndVerify},
	}
}

func padTo(s []byte, size int) []byte {
	if len(s) == size {
		return s
	}
	p := make([]byte, size)
	copy(p, s)
	return p
}

// benchBuild is BuildCodedSegmentMessages with the library's plain encoder for the parity.
func benchBuild(newEnc func(k, parity int) (reedsolomon.Encoder, error), msg []byte, size, parity int, h Hasher) ([]*SegmentMessage, error) {
	segs, err := Split(msg, size)
	if err != nil {
		return nil, err
	}
	k := len(segs)
	enc, err := newEnc(k, parity)
	if err != nil {
		return nil, err
	}
	shards := make([][]byte, k+parity)
	for i, s := range segs {
		shards[i] = padTo(s, size)
	}
	for i := k; i < k+parity; i++ {
		shards[i] = make([]byte, size)
	}
	if err := enc.Encode(shards); err != nil {
		return nil, err
	}
	leaves := append(append(make([][]byte, 0, k+parity), segs...), shards[k:]...)
	tree, err := BuildTree(h, leaves)
	if err != nil {
		return nil, err
	}
	d := &Descriptor{Version: VersionCoded, HashID: h.ID(), Count: uint32(k + parity), SegmentSize: uint32(size), TotalLength: uint64(len(msg)), Root: tree.Root()}
	out := make([]*SegmentMessage, len(leaves))
	for i := range leaves {
		proof, err := tree.Proof(i)
		if err != nil {
			return nil, err
		}
		out[i] = &SegmentMessage{Descriptor: d, Index: uint32(i), Proof: proof, Data: leaves[i]}
	}
	return out, nil
}

// benchRecover is the receiver's decode and codeword check with the library's plain decoder, as
// RecoverAndVerify does it for every code: the data, its pad, all the parity again, the tree.
func benchRecover(newEnc func(k, parity int) (reedsolomon.Encoder, error), d *Descriptor, h Hasher, segs [][]byte) ([]byte, error) {
	k := int(d.Required()) // lint:ignore uintcast -- test shapes.
	n := len(segs)
	enc, err := newEnc(k, n-k)
	if err != nil {
		return nil, err
	}
	shards := make([][]byte, n)
	for i, s := range segs {
		if s != nil {
			shards[i] = padTo(s, int(d.SegmentSize))
		}
	}
	if err := enc.ReconstructData(shards); err != nil {
		return nil, err
	}
	last := int(d.TotalLength) - (k-1)*int(d.SegmentSize) // lint:ignore uintcast -- test shapes.
	for _, b := range shards[k-1][last:] {
		if b != 0 {
			return nil, ErrCodewordMismatch
		}
	}
	for i := k; i < n; i++ {
		shards[i] = make([]byte, int(d.SegmentSize))
	}
	if err := enc.Encode(shards); err != nil {
		return nil, err
	}
	leaves := make([][]byte, n)
	for i, s := range shards {
		leaves[i] = s
		if i == k-1 {
			leaves[i] = s[:last]
		}
	}
	tree, err := BuildTree(h, leaves)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(tree.Root(), d.Root) {
		return nil, ErrCodewordMismatch
	}
	out := make([]byte, 0, d.TotalLength)
	for i := 0; i < k; i++ {
		out = append(out, leaves[i]...)
	}
	return out, nil
}

func benchWanted(env, name string) bool {
	v := os.Getenv(env)
	if v == "" {
		return true
	}
	for _, f := range strings.Split(v, ",") {
		if strings.TrimSpace(f) == name {
			return true
		}
	}
	return false
}

func BenchmarkCPU(b *testing.B) {
	h, err := HasherByID(HashSHA256)
	if err != nil {
		b.Fatal(err)
	}
	if n := runtime.GOMAXPROCS(0); n != 1 && os.Getenv("SEGMENT_BENCH_ANY_PROCS") == "" {
		b.Fatalf("GOMAXPROCS is %d: the CPU column is measured on one thread (GOMAXPROCS=1)", n)
	}
	// The library builds its GF(2^16) tables at first use; that is not a group's cost.
	if _, err := leopardParity(VersionCodedLeopard, [][]byte{make([]byte, 64)}, 1); err != nil {
		b.Fatal(err)
	}
	for _, shape := range cpuShapes {
		payload, k := shape[0], shape[1]
		if !benchWanted("SEGMENT_BENCH_SHAPES", fmt.Sprintf("%d:%d", payload, k)) {
			continue
		}
		msg := randomBytes(cpuCompressed[payload], int64(payload))
		size := (len(msg) + k - 1) / k
		for _, code := range cpuCodes() {
			if 2*k > code.maxN || !benchWanted("SEGMENT_BENCH_CODES", code.name) {
				continue
			}
			msgs, err := code.encode(msg, size, k, h)
			if err != nil {
				b.Fatal(err)
			}
			d := msgs[0].Descriptor
			n := len(msgs)
			leaves := make([][]byte, n)
			wire := 0
			for i, m := range msgs {
				leaves[i] = m.Data
				wire += len(m.Data)
			}
			half, parity := make([][]byte, n), make([][]byte, n)
			for i := 0; i < k/2; i++ {
				half[i] = leaves[i]
			}
			for i := k; i < n-k/2; i++ {
				half[i] = leaves[i]
			}
			copy(parity[k:], leaves[k:])
			for _, set := range [][][]byte{half, parity} {
				got, err := code.decode(d, h, append([][]byte(nil), set...))
				if err != nil || !bytes.Equal(got, msg) {
					b.Fatalf("%s %d KiB K=%d: decode fails: %v", code.name, payload, k, err)
				}
			}
			name := fmt.Sprintf("p=%dk/K=%d/chunk=%d/bytes=%d/code=%s", payload, k, size, wire, code.name)
			b.Run(name+"/op=encode", func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					if _, err := code.encode(msg, size, k, h); err != nil {
						b.Fatal(err)
					}
				}
			})
			b.Run(name+"/op=tree", func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					tree, err := BuildTree(h, leaves)
					if err != nil {
						b.Fatal(err)
					}
					for j := range leaves {
						if _, err := tree.Proof(j); err != nil {
							b.Fatal(err)
						}
					}
				}
			})
			if code.name == "table" || code.name == "leo16" {
				b.Run(name+"/op=proofs", func(b *testing.B) {
					for i := 0; i < b.N; i++ {
						for j := 0; j < k; j++ {
							if err := VerifySegment(d, h, j, leaves[j], msgs[j].Proof); err != nil {
								b.Fatal(err)
							}
						}
					}
				})
			}
			for setName, set := range map[string][][]byte{"decode-half": half, "decode-parity": parity} {
				b.Run(name+"/op="+setName, func(b *testing.B) {
					for i := 0; i < b.N; i++ {
						if _, err := code.decode(d, h, append([][]byte(nil), set...)); err != nil {
							b.Fatal(err)
						}
					}
				})
			}
		}
		// The uncoded group of the uncompressed payload in K segments, where the grid has it.
		if payload == 4096 && k == 256 && benchWanted("SEGMENT_BENCH_CODES", "plain") {
			body := randomBytes(payload<<10, 5)
			size := len(body) / k
			msgs, err := BuildSegmentMessages(body, size, h)
			if err != nil {
				b.Fatal(err)
			}
			d := msgs[0].Descriptor
			leaves := make([][]byte, len(msgs))
			for i, m := range msgs {
				leaves[i] = m.Data
			}
			name := fmt.Sprintf("p=%dk/K=%d/chunk=%d/bytes=%d/code=plain", payload, k, size, len(body))
			b.Run(name+"/op=encode", func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					if _, err := BuildSegmentMessages(body, size, h); err != nil {
						b.Fatal(err)
					}
				}
			})
			b.Run(name+"/op=proofs", func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					for j := range leaves {
						if err := VerifySegment(d, h, j, leaves[j], msgs[j].Proof); err != nil {
							b.Fatal(err)
						}
					}
				}
			})
			b.Run(name+"/op=decode-half", func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					if _, err := Join(d, h, leaves); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

// TestLeopardTables reports what the library's GF(2^16) tables cost at first use. It runs
// only when asked (SEGMENT_BENCH_TABLES=1), in a process that has not used the code yet:
//
//	SEGMENT_BENCH_TABLES=1 go test -run '^TestLeopardTables$' -v ./container/segments/
func TestLeopardTables(t *testing.T) {
	if os.Getenv("SEGMENT_BENCH_TABLES") == "" {
		t.Skip("set SEGMENT_BENCH_TABLES=1")
	}
	reportTables(t)
}

func reportTables(t *testing.T) {
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	start := time.Now()
	if _, err := leopardParity(VersionCodedLeopard, [][]byte{make([]byte, 64)}, 1); err != nil {
		t.Fatal(err)
	}
	took := time.Since(start)
	runtime.GC()
	runtime.ReadMemStats(&after)
	t.Logf("Leopard GF(2^16) tables at first use: %v, %.1f MiB of heap kept", took.Round(time.Millisecond), float64(after.HeapAlloc-before.HeapAlloc)/(1<<20))
}
