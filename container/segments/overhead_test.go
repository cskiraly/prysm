package segments

import (
	"fmt"
	"math"
	"testing"

	"github.com/OffchainLabs/prysm/v7/testing/require"
)

// authorityLen is the authority material a segment carries: none.
//
// It was a 112-byte detached signature blob (slot, builder index, 96-byte signature), then a
// 40-byte anchor (slot, block root), and is now nothing at all -- a receiver tests the derived
// group id against the commitments it holds, so the sender asserts no authority. That is the
// whole of the per-segment saving, and it is why the overhead below is proofs plus framing.
const authorityLen = 0

// TestMeasuredWireOverhead measures what segmentation actually costs on the wire.
//
// The design note carries a table of these figures. It was originally computed by hand, and
// hand arithmetic goes stale silently -- binding the descriptor signature to a slot widened
// the auth blob by 8 bytes and every published figure became slightly wrong. This measures
// the real marshalled bytes so the table can be regenerated instead of recomputed.
func TestMeasuredWireOverhead(t *testing.T) {
	const payloadLen = 1 << 20 // 1 MiB, a plausible envelope
	h, err := HasherByID(HashSHA256)
	require.NoError(t, err)
	msg := msgOfLen(payloadLen)

	type row struct {
		segmentSize  int
		count        int
		depth        int
		perSegment   int
		total        int
		pctTotal     float64
		pctAuthority float64
		pctProof     float64
	}
	var rows []row

	for _, segmentSize := range []int{4 << 10, 8 << 10, 32 << 10, 64 << 10} {
		msgs, err := BuildSegmentMessages(msg, segmentSize, h)
		require.NoError(t, err)

		encodedTotal := 0
		for _, m := range msgs {
			enc, err := m.Marshal()
			require.NoError(t, err)
			encodedTotal += len(enc)
		}
		overhead := encodedTotal - payloadLen
		depth := len(msgs[0].Proof)
		rows = append(rows, row{
			segmentSize:  segmentSize,
			count:        len(msgs),
			depth:        depth,
			perSegment:   overhead / len(msgs),
			total:        overhead,
			pctTotal:     100 * float64(overhead) / float64(payloadLen),
			pctAuthority: 100 * float64(authorityLen*len(msgs)) / float64(payloadLen),
			pctProof:     100 * float64(depth*h.Size()*len(msgs)) / float64(payloadLen),
		})
	}

	t.Log("measured wire overhead for a 1 MiB payload, sha256:")
	t.Logf("%10s %6s %6s %10s %10s %9s %9s %9s", "segSize", "K", "depth", "per-seg", "total", "total%", "authority%", "proof%")
	for _, r := range rows {
		t.Logf("%10s %6d %6d %10d %10d %8.2f%% %8.2f%% %8.2f%%",
			fmt.Sprintf("%dKiB", r.segmentSize>>10), r.count, r.depth, r.perSegment, r.total,
			r.pctTotal, r.pctAuthority, r.pctProof)
	}

	t.Run("overhead falls as segments grow", func(t *testing.T) {
		for i := 1; i < len(rows); i++ {
			require.Equal(t, true, rows[i].pctTotal < rows[i-1].pctTotal,
				"overhead should fall with larger segments")
		}
	})

	t.Run("proofs dominate the overhead", func(t *testing.T) {
		// The central claim behind choosing larger segments. If this ever flips, the
		// header-segment alternative loses most of its motivation.
		for _, r := range rows {
			require.Equal(t, true, r.pctProof > r.pctAuthority,
				fmt.Sprintf("at %dKiB, proofs %.2f%% should exceed authority material %.2f%%",
					r.segmentSize>>10, r.pctProof, r.pctAuthority))
		}
	})

	t.Run("default segment size keeps overhead near one percent", func(t *testing.T) {
		var def *row
		for i := range rows {
			if rows[i].segmentSize == DefaultSegmentSize {
				def = &rows[i]
			}
		}
		require.NotNil(t, def)
		require.Equal(t, true, def.pctTotal < 1.5,
			fmt.Sprintf("default segment size overhead is %.2f%%", def.pctTotal))
	})

	t.Run("per-segment overhead matches the wire layout", func(t *testing.T) {
		// Guards the arithmetic in the design note: fixed fields plus one digest per proof
		// level. A mismatch means a field was added without the note being updated.
		for _, r := range rows {
			want := descriptorFixedLen + h.Size() + indexLen + proofCountLen +
				authorityLen + dataLenLen + r.depth*h.Size()
			require.Equal(t, want, r.perSegment,
				fmt.Sprintf("at %dKiB the per-segment overhead should be the sum of the wire fields", r.segmentSize>>10))
		}
	})

	t.Run("proof cost grows as K log K", func(t *testing.T) {
		// Sanity check on the shape of the growth, not just the direction: doubling K should
		// more than double proof bytes, because depth rises too.
		for i := 1; i < len(rows); i++ {
			bigger, smaller := rows[i-1], rows[i] // rows go from small segments (large K) down
			ratioK := float64(bigger.count) / float64(smaller.count)
			ratioProof := bigger.pctProof / smaller.pctProof
			require.Equal(t, true, ratioProof > ratioK*0.99,
				fmt.Sprintf("proof bytes should grow at least as fast as K: K x%.1f, proof x%.1f", ratioK, ratioProof))
		}
	})
}

// BenchmarkCommit measures splitting plus tree construction, the publisher's cost.
func BenchmarkCommit(b *testing.B) {
	const payloadLen = 1 << 20
	msg := msgOfLen(payloadLen)
	for _, id := range []HashID{HashSHA256, HashKeccak256} {
		h, err := HasherByID(id)
		if err != nil {
			b.Fatal(err)
		}
		for _, segmentSize := range []int{4 << 10, 32 << 10} {
			b.Run(fmt.Sprintf("hash%d/%dKiB", id, segmentSize>>10), func(b *testing.B) {
				b.SetBytes(payloadLen)
				for b.Loop() {
					if _, _, err := Commit(msg, segmentSize, h); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

// BenchmarkVerifySegment measures the per-segment receive cost, excluding authentication.
// This is the work a peer can make us do for free, so it wants to stay cheap.
func BenchmarkVerifySegment(b *testing.B) {
	h, err := HasherByID(HashSHA256)
	if err != nil {
		b.Fatal(err)
	}
	for _, segmentSize := range []int{4 << 10, 32 << 10} {
		msgs, err := BuildSegmentMessages(msgOfLen(1<<20), segmentSize, h)
		if err != nil {
			b.Fatal(err)
		}
		b.Run(fmt.Sprintf("%dKiB/K%d", segmentSize>>10, len(msgs)), func(b *testing.B) {
			b.SetBytes(int64(segmentSize))
			i := 0
			for b.Loop() {
				m := msgs[i%len(msgs)]
				if err := m.Verify(h); err != nil {
					b.Fatal(err)
				}
				i++
			}
		})
	}
}

// BenchmarkUnmarshalSegmentMessage measures decode cost, the very first thing a peer can
// trigger.
func BenchmarkUnmarshalSegmentMessage(b *testing.B) {
	h, err := HasherByID(HashSHA256)
	if err != nil {
		b.Fatal(err)
	}
	msgs, err := BuildSegmentMessages(msgOfLen(1<<20), DefaultSegmentSize, h)
	if err != nil {
		b.Fatal(err)
	}
	enc, err := msgs[0].Marshal()
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(len(enc)))
	for b.Loop() {
		if _, _, err := UnmarshalSegmentMessage(enc); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkReassemble measures a whole message arriving segment by segment, which is the
// aggregate receive cost for one payload.
func BenchmarkReassemble(b *testing.B) {
	const payloadLen = 1 << 20
	h, err := HasherByID(HashSHA256)
	if err != nil {
		b.Fatal(err)
	}
	for _, segmentSize := range []int{4 << 10, 32 << 10} {
		msgs, err := BuildSegmentMessages(msgOfLen(payloadLen), segmentSize, h)
		if err != nil {
			b.Fatal(err)
		}
		b.Run(fmt.Sprintf("%dKiB/K%d", segmentSize>>10, len(msgs)), func(b *testing.B) {
			b.SetBytes(payloadLen)
			for b.Loop() {
				r, err := NewReassembler(ReassemblerConfig{AllowUnauthenticated: true})
				if err != nil {
					b.Fatal(err)
				}
				for _, m := range msgs {
					if _, err := r.Add(h, m); err != nil {
						b.Fatal(err)
					}
				}
			}
		})
	}
}

// BenchmarkTreeDepthScaling isolates tree construction across the usable K range, so the
// cost of choosing many small segments is visible on its own.
func BenchmarkTreeDepthScaling(b *testing.B) {
	h, err := HasherByID(HashSHA256)
	if err != nil {
		b.Fatal(err)
	}
	for _, count := range []int{16, 128, 1024, 8192} {
		segs := make([][]byte, count)
		for i := range segs {
			segs[i] = msgOfLen(256)
		}
		b.Run(fmt.Sprintf("K%d/depth%d", count, int(math.Ceil(math.Log2(float64(count))))), func(b *testing.B) {
			for b.Loop() {
				if _, err := BuildTree(h, segs); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
