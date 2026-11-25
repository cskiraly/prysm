package segments

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/klauspost/reedsolomon"
)

// Reed-Solomon extension of a segmented message: K systematic segments (the payload,
// exactly as Version splits it) plus parity segments, any K of the total recovering the
// payload. The descriptor layout is unchanged; Version distinguishes the interpretation.
// Count is the coded total N, and K stays derivable as ceil(TotalLength/SegmentSize), so a
// coded descriptor authenticates K and N with the same fields, and the same commitment, as a
// plain one.
//
// The code is the textbook systematic Vandermonde construction over GF(2^8), the default code
// of klauspost/reedsolomon, which computes it here with SIMD. The tests keep a table
// implementation of the same construction as the oracle the library's parity and recovery
// are checked against.

// VersionCoded marks a descriptor whose Count exceeds the segment count implied by
// TotalLength: the excess is Reed-Solomon parity.
const VersionCoded uint8 = 2

// MaxCodedSegments caps a coded group's total segment count. The codec evaluates the code
// at distinct points of GF(2^8), of which there are 256.
const MaxCodedSegments = 256

var (
	// ErrCodedShape is returned when a coded group's parameters are out of range.
	ErrCodedShape = errors.New("invalid coded group shape")
	// ErrNotEnoughSegments is returned when recovery has fewer than Required segments.
	ErrNotEnoughSegments = errors.New("not enough segments to recover")
	// ErrCodewordMismatch is returned when the recovered codeword disagrees with the
	// committed root, i.e. the committed segments were not a consistent codeword.
	ErrCodewordMismatch = errors.New("committed segments are not a consistent codeword")
)

// rsCodec returns the library's coder for k data and parity segments. Its default code is the
// systematic Vandermonde matrix over GF(2^8) mod 0x11d: V[i][j] = i^j times the inverse of its
// top k x k block. Above 256 segments the library changes field, so the shape is checked here.
func rsCodec(k, parity int) (reedsolomon.Encoder, error) {
	if k <= 0 || parity < 0 || k+parity > MaxCodedSegments {
		return nil, fmt.Errorf("%w: n=%d k=%d", ErrCodedShape, k+parity, k)
	}
	return reedsolomon.New(k, parity)
}

// rsParity computes the parity shards for k equal-length data shards.
func rsParity(data [][]byte, parity int) ([][]byte, error) {
	k := len(data)
	enc, err := rsCodec(k, parity)
	if err != nil {
		return nil, err
	}
	shards := make([][]byte, k+parity)
	copy(shards, data)
	for i := k; i < len(shards); i++ {
		shards[i] = make([]byte, len(data[0]))
	}
	if err := enc.Encode(shards); err != nil {
		return nil, fmt.Errorf("encode: %w", err)
	}
	return shards[k:], nil
}

// rsRecover fills the nil entries of shards in place. shards has the full coded length n;
// at least k entries must be non-nil and of equal length. The library decodes from the first
// k present shards and recomputes only what is missing.
func rsRecover(shards [][]byte, k int) error {
	n := len(shards)
	dec, err := rsCodec(k, n-k)
	if err != nil {
		return err
	}
	present := 0
	for _, s := range shards {
		if s != nil {
			present++
		}
	}
	if present < k {
		return fmt.Errorf("%w: have %d, need %d", ErrNotEnoughSegments, present, k)
	}
	if err := dec.Reconstruct(shards); err != nil {
		return fmt.Errorf("reconstruct: %w", err)
	}
	return nil
}

// paddedShards returns the segments padded to SegmentSize for coding. Only the final
// systematic segment may be short on the wire; parity segments are always full length.
func paddedShards(d *Descriptor, segs [][]byte) [][]byte {
	out := make([][]byte, len(segs))
	size := int(d.SegmentSize)
	for i, s := range segs {
		if s == nil || len(s) == size {
			out[i] = s
			continue
		}
		p := make([]byte, size)
		copy(p, s)
		out[i] = p
	}
	return out
}

// wireShard returns a shard in its wire form: the final systematic segment truncated to
// its descriptor length, everything else unchanged.
func wireShard(d *Descriptor, index int, shard []byte) ([]byte, error) {
	want, err := d.SegmentLength(index)
	if err != nil {
		return nil, err
	}
	if len(shard) < want {
		return nil, fmt.Errorf("%w: shard %d bytes, want %d", ErrSegmentLength, len(shard), want)
	}
	return shard[:want], nil
}

// BuildCodedSegmentMessages produces the wire messages for a Reed-Solomon coded group of raw
// bytes: the systematic segments of msg followed by parity segments, committed together.
func BuildCodedSegmentMessages(msg []byte, segmentSize, parity int, h Hasher) ([]*SegmentMessage, error) {
	if parity <= 0 {
		return nil, fmt.Errorf("%w: parity %d", ErrCodedShape, parity)
	}
	return Build(msg, Layout{SegmentSize: segmentSize, Hasher: h, Parity: parity})
}

func buildCoded(msg []byte, l Layout) ([]*SegmentMessage, error) {
	h := l.Hasher
	segs, err := Split(msg, l.SegmentSize)
	if err != nil {
		return nil, err
	}
	k := len(segs)
	n := k + l.Parity
	if n > MaxCodedSegments {
		return nil, fmt.Errorf("%w: %d segments, max %d", ErrCodedShape, n, MaxCodedSegments)
	}
	d := &Descriptor{
		Version:     VersionCoded,
		HashID:      h.ID(),
		Encoding:    l.Encoding,
		Count:       uint32(n),
		SegmentSize: uint32(l.SegmentSize),
		TotalLength: uint64(len(msg)),
	}
	// The codec zero-pads the final systematic segment; the wire carries it truncated, and
	// the commitment covers the wire form.
	par, err := rsParity(paddedShards(d, segs), l.Parity)
	if err != nil {
		return nil, err
	}
	leaves := make([][]byte, 0, n)
	leaves = append(leaves, segs...)
	leaves = append(leaves, par...)
	tree, err := BuildTree(h, leaves)
	if err != nil {
		return nil, err
	}
	d.Root = tree.Root()
	out := make([]*SegmentMessage, n)
	for i := range leaves {
		proof, err := tree.Proof(i)
		if err != nil {
			return nil, err
		}
		out[i] = &SegmentMessage{
			Descriptor: d,
			Index:      uint32(i),
			Proof:      proof,
			Data:       leaves[i],
		}
	}
	return out, nil
}

// RecoverAndVerify reconstructs a coded group's payload from any Required() of its
// segments, and rejects a group whose committed segments were not a consistent codeword.
//
// segs has Count entries, nil for missing; present entries must already be proof-verified.
// The consistency check matters because a Merkle root binds arbitrary leaves: a builder
// could commit segments that are not a codeword, making different K-subsets decode to
// different payloads. Recomputing the missing shards and rebuilding the tree pins the
// decoded payload to the committed root, so every subset either yields the same payload or
// fails here.
func RecoverAndVerify(d *Descriptor, h Hasher, segs [][]byte) ([]byte, error) {
	if d.Version != VersionCoded {
		return nil, fmt.Errorf("%w: version %d", ErrDescriptorMismatch, d.Version)
	}
	if err := d.Validate(h); err != nil {
		return nil, err
	}
	if uint32(len(segs)) != d.Count {
		return nil, fmt.Errorf("%w: %d entries, want %d", ErrDescriptorMismatch, len(segs), d.Count)
	}
	k := int(d.Required()) // lint:ignore uintcast -- bounded by MaxSegments via Validate.
	shards := paddedShards(d, segs)
	if err := rsRecover(shards, k); err != nil {
		return nil, err
	}
	// Rebuild the tree over the full codeword in wire form and compare roots.
	leaves := make([][]byte, len(shards))
	for i, s := range shards {
		w, err := wireShard(d, i, s)
		if err != nil {
			return nil, err
		}
		leaves[i] = w
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
	if uint64(len(out)) != d.TotalLength {
		return nil, fmt.Errorf("%w: recovered %d bytes, want %d", ErrDescriptorMismatch, len(out), d.TotalLength)
	}
	return out, nil
}
