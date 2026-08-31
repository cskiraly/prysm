package gossipsim

import (
	"math/rand/v2"
)

// HighEntropyPayload returns n bytes that snappy cannot compress, reproducibly.
//
// Deterministic in seed so a failure can be replayed, but incompressible so the link carries
// what the test thinks it carries. Every timing experiment must use this rather than
// deterministicPayload.
func HighEntropyPayload(n int, seed uint64) []byte {
	r := rand.New(rand.NewPCG(seed, seed^0xda3e39cb94b95bdb))
	out := make([]byte, n)
	for i := 0; i+8 <= len(out); i += 8 {
		v := r.Uint64()
		out[i], out[i+1], out[i+2], out[i+3] = byte(v), byte(v>>8), byte(v>>16), byte(v>>24)
		out[i+4], out[i+5], out[i+6], out[i+7] = byte(v>>32), byte(v>>40), byte(v>>48), byte(v>>56)
	}
	for i := len(out) - len(out)%8; i < len(out); i++ {
		out[i] = byte(r.Uint32())
	}
	return out
}

// MixedEntropyPayload generates bytes at a controllable compressibility, shaped like an
// execution payload rather than like noise.
//
// Real payloads are neither random nor uniform. RLP-encoded transactions carry incompressible
// material -- signatures, hashes, addresses -- interleaved with highly compressible material,
// because ABI encoding pads every value to a 32-byte word and most values are small. So the
// generator emits 32-byte words, a `padded` fraction of which are a small value in an otherwise
// zero word. That reproduces the leading-zero structure snappy actually exploits.
//
// This matters because segment framing compresses each segment independently, so it forfeits
// cross-segment redundancy. The size of that forfeit is a function of compressibility, which
// means an overhead number quoted without one is meaningless.
// MainnetPaddedFraction is calibrated so MixedEntropyPayload reproduces the snappy ratio of a
// real mainnet execution payload, 1.40:1. That figure is the one number this harness could not
// derive for itself, and it is what makes an overhead result meaningful rather than a range.
const MainnetPaddedFraction = 0.41

// MainnetLikePayload is the realistic case: incompressible material interleaved with the
// leading-zero padding ABI encoding produces, at mainnet's measured 1.40:1.
func MainnetLikePayload(n int, seed uint64) []byte {
	return MixedEntropyPayload(n, MainnetPaddedFraction, seed)
}

// ClusteredEntropyPayload has the same overall compressibility as MixedEntropyPayload but
// concentrates it in runs, which is what a real block looks like: a few transactions carrying
// large ABI-padded calldata next to many small transfers, rather than compressibility sprinkled
// evenly over every 32-byte word.
//
// This matters more than it looks. The marginal cost of a hop is driven by `wmax`, the LARGEST
// compressed segment, not the mean. Under i.i.d. padding every segment compresses about equally
// and wmax is close to the mean -- the best possible case for segmentation. Clustering pushes
// wmax above the mean, so a uniform generator flatters the scheme.
func ClusteredEntropyPayload(n int, padded float64, runWords int, seed uint64) []byte {
	r := rand.New(rand.NewPCG(seed, seed^0x8ebc6af09c88c6e3))
	out := make([]byte, n)
	const word = 32
	off := 0
	for off < n {
		// A run of consecutive words that are all padded or all random.
		runLen := (1 + r.IntN(2*runWords)) * word
		fill := r.Float64() < padded
		for w := 0; w < runLen && off < n; w += word {
			end := min(off+word, n)
			if fill {
				for i := end - 1; i >= end-min(3, end-off); i-- {
					out[i] = byte(r.Uint32())
				}
			} else {
				for i := off; i < end; i++ {
					out[i] = byte(r.Uint32())
				}
			}
			off = end
		}
	}
	return out
}

func MixedEntropyPayload(n int, padded float64, seed uint64) []byte {
	r := rand.New(rand.NewPCG(seed, seed^0x2545f4914f6cdd1d))
	out := make([]byte, n)
	const word = 32
	for off := 0; off < n; off += word {
		end := min(off+word, n)
		if r.Float64() < padded {
			// A small value right-aligned in a zero word: what an ABI-encoded uint or a short
			// length prefix looks like on the wire.
			for i := end - 1; i >= end-min(3, end-off); i-- {
				out[i] = byte(r.Uint32())
			}
			continue
		}
		for i := off; i < end; i++ {
			out[i] = byte(r.Uint32())
		}
	}
	return out
}
