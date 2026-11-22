package segments

import (
	"fmt"

	"github.com/klauspost/reedsolomon"
)

// Two further Reed-Solomon codes for coded groups: Leopard-RS over GF(2^16) and over GF(2^8), the
// FFT code of Lin, Al-Naffouri, Han and Chung ("Novel Polynomial Basis with Fast Fourier Transform
// and Its Application to Reed-Solomon Erasure Codes", 2016), from
// github.com/klauspost/reedsolomon. Both encode and decode in O(n log n) segment operations.
//
// A descriptor names its code by its version: VersionCoded is the GF(2^8) matrix code of rs.go,
// VersionCodedLeopard is Leopard over GF(2^16), VersionCodedLeopard8 Leopard over GF(2^8).
// Everything else about a coded group is shared.
//
//   - GF(2^16) has 65536 evaluation points, so a group is no longer bounded by the 256 points of
//     GF(2^8). Its decoder has a cost that does not shrink with the group (a transform over the
//     whole field), and the library keeps about 72 MiB of tables for it once it is first used.
//   - GF(2^8) keeps the bound of 256 segments, has neither of those costs, and is the cheaper of
//     the two for small groups.

// VersionCodedLeopard marks a coded descriptor whose parity is Leopard-RS over GF(2^16).
const VersionCodedLeopard uint8 = 3

// VersionCodedLeopard8 marks a coded descriptor whose parity is Leopard-RS over GF(2^8).
const VersionCodedLeopard8 uint8 = 4

// MaxLeopardSegments caps a GF(2^16) Leopard group's total segment count. The code itself allows
// 65536; the bound is the HAVE bitmap that announces a group's segments (8192 indices), which is
// below the proof list's 16384 leaves (MaxSegments).
const MaxLeopardSegments = 8192

// MaxLeopard8Parity caps a GF(2^8) Leopard group's parity count. The code places the parity at
// the first points of the field, up to the parity count's next power of two, and the data after
// them: with at least as much parity as data, 128 parity segments are the most whose data still
// has points among the field's 256. A group of this code therefore has at most 128 data segments
// and at most MaxCodedSegments segments.
const MaxLeopard8Parity = 128

// MaxLeopardParityPerData caps a Leopard coded group's parity at this many parity segments a data
// segment (rate 1/4). Together with the lower bound, at least one parity segment a data segment,
// it does two things. It keeps a group inside the shapes the code's short definition covers: with
// at least as much parity as data the code is one polynomial of degree below the parity count's
// next power of two (leopard_test.go, TestLeopardMatchesDefinition). And it ties the number of
// segments a receiver computes to check a group to the number that carry the payload: without it
// a descriptor of one short data segment and thousands of full-size parity segments would be
// valid, and checking it would take gigabytes.
const MaxLeopardParityPerData = 3

// MaxLeopardWorkBytes caps the workspace the GF(2^16) code needs for one group: twice the parity
// count's next power of two, in shards. The library takes it in one allocation, whose length must
// fit an int on every platform, and a receiver holds it while it checks a group. 1 GiB admits
// every group of at most MaxLeopardSegments segments of 32 KiB (512 MiB at the most), and with the
// package's larger segment sizes it is what bounds a group: 256 parity segments of 1 MiB. The
// GF(2^8) code works through a group in pieces of 32 KiB and needs no such bound: its workspace is
// at most 2 * 128 pieces.
const MaxLeopardWorkBytes = 1 << 30

// leopardWorkBytes is the GF(2^16) code's workspace for parity parity segments of segmentSize
// bytes. parity is at most MaxLeopardSegments and segmentSize at most MaxSegmentSize when it is
// called, so the product fits 64 bits with room.
func leopardWorkBytes(parity int, segmentSize uint32) uint64 {
	m := uint64(1)
	for m < uint64(parity) {
		m <<= 1
	}
	shard := (uint64(segmentSize) + leopardBlock - 1) / leopardBlock * leopardBlock
	return 2 * m * shard
}

// isLeopard reports whether version names one of the two Leopard codes.
func isLeopard(version uint8) bool {
	return version == VersionCodedLeopard || version == VersionCodedLeopard8
}

// leopardShapeOK reports whether k data and parity parity segments are a shape a group of the
// Leopard code that version names may have. It adds nothing, so no count can overflow in it.
func leopardShapeOK(version uint8, k, parity int) bool {
	if k <= 0 || parity < k || parity > MaxLeopardParityPerData*k {
		return false
	}
	if version == VersionCodedLeopard8 {
		// k <= parity, so the group has at most 2 * 128 segments.
		return parity <= MaxLeopard8Parity
	}
	return k <= MaxLeopardSegments && parity <= MaxLeopardSegments-k
}

// leopardBlock is the unit of a Leopard shard, 64 bytes. Over GF(2^16) a block is 32 symbols of
// 16 bits, the low bytes in its first half and the high bytes in its second; over GF(2^8) every
// byte is a symbol.
const leopardBlock = 64

// leopardShardSize is the length a segment has inside a Leopard code: segmentSize rounded up to a
// whole number of blocks. Segments are padded to it with zeros for coding. Data segments travel
// unpadded under both codes. A parity segment has this length on the wire under GF(2^16), where a
// symbol's two bytes sit half a block apart and the pad of the data spreads into the parity; under
// GF(2^8) the parity is zero wherever all the data is, so it travels at segmentSize, as under the
// matrix code.
func leopardShardSize(segmentSize int) int {
	return (segmentSize + leopardBlock - 1) / leopardBlock * leopardBlock
}

// maxCodedSegments is the bound on a coded group's count under the code a version names.
func maxCodedSegments(version uint8) int {
	if version == VersionCodedLeopard {
		return MaxLeopardSegments
	}
	return MaxCodedSegments
}

// Coded reports whether the descriptor describes a Reed-Solomon coded group, under any code.
func (d *Descriptor) Coded() bool {
	return d.Version == VersionCoded || isLeopard(d.Version)
}

// shardSize is the length of the group's segments inside its code.
func (d *Descriptor) shardSize() int {
	if isLeopard(d.Version) {
		return leopardShardSize(int(d.SegmentSize))
	}
	return int(d.SegmentSize)
}

// leopardEncoder is the library's encoder of the code version names, for k data and parity parity
// shards. It refuses what the library cannot code. The shapes a group may have are narrower,
// leopardShapeOK's, checked where a group is built or validated.
func leopardEncoder(version uint8, k, parity int) (reedsolomon.Encoder, error) {
	var opt reedsolomon.Option
	switch version {
	case VersionCodedLeopard:
		if k <= 0 || parity <= 0 || k > MaxLeopardSegments || parity > MaxLeopardSegments-k {
			return nil, fmt.Errorf("%w: k=%d parity=%d, max %d", ErrCodedShape, k, parity, MaxLeopardSegments)
		}
		opt = reedsolomon.WithLeopardGF16(true)
	case VersionCodedLeopard8:
		// The library indexes the field's 256 points by the parity count's next power of two plus
		// the data count, and does not check that the sum is among them. The parity count is
		// bounded before its power of two is sought, so the search ends.
		if k <= 0 || parity <= 0 || parity > MaxCodedSegments {
			return nil, fmt.Errorf("%w: k=%d parity=%d over GF(2^8)", ErrCodedShape, k, parity)
		}
		m := 1
		for m < parity {
			m <<= 1
		}
		if k > MaxCodedSegments-m {
			return nil, fmt.Errorf("%w: k=%d parity=%d over GF(2^8)", ErrCodedShape, k, parity)
		}
		opt = reedsolomon.WithLeopardGF(true)
	default:
		return nil, fmt.Errorf("%w: version %d is not a Leopard code", ErrCodedShape, version)
	}
	// The GF(2^8) encoder would keep a decoding matrix per pattern of missing shards; an encoder
	// lives for one call here, so there is nothing for it to reuse.
	enc, err := reedsolomon.New(k, parity, opt, reedsolomon.WithInversionCache(false))
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrCodedShape, err)
	}
	return enc, nil
}

// leopardParity computes the parity shards for k data shards of one length, a multiple of
// leopardBlock, under the Leopard code version names.
func leopardParity(version uint8, data [][]byte, parity int) ([][]byte, error) {
	enc, err := leopardEncoder(version, len(data), parity)
	if err != nil {
		return nil, err
	}
	return leopardParityWith(enc, data, parity)
}

func leopardParityWith(enc reedsolomon.Encoder, data [][]byte, parity int) ([][]byte, error) {
	k := len(data)
	size := len(data[0])
	shards := make([][]byte, k+parity)
	copy(shards, data)
	// The encoder writes into a parity shard it is given; one it is not given would alias its
	// pooled work buffers.
	for i := k; i < k+parity; i++ {
		shards[i] = make([]byte, size)
	}
	if err := enc.Encode(shards); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrCodedShape, err)
	}
	return shards[k:], nil
}

// leopardRecoverData fills the nil data entries of shards in place. shards has the full coded
// length; at least k entries must be non-nil, all of one length.
func leopardRecoverData(enc reedsolomon.Encoder, shards [][]byte, k int) error {
	if err := enc.ReconstructData(shards); err != nil {
		if err == reedsolomon.ErrTooFewShards {
			return fmt.Errorf("%w: need %d", ErrNotEnoughSegments, k)
		}
		return fmt.Errorf("%w: %v", ErrCodedShape, err)
	}
	return nil
}
