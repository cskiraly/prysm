package segments

import (
	"bytes"
	"errors"
	"fmt"
)

// Reed-Solomon extension of a segmented message: K systematic segments (the payload,
// exactly as Version 1 splits it) plus parity segments, any K of the total recovering the
// payload. The descriptor layout is unchanged; Version distinguishes the interpretation.
// Count is the coded total N, and K stays derivable as ceil(TotalLength/SegmentSize), so a
// coded descriptor authenticates K and N with the same bytes and the same group id as an
// uncoded one.

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

// rsMatrix returns the systematic n x k encoding matrix, row-major.
//
// Rows are the Vandermonde matrix V[i][j] = i^j multiplied by the inverse of its top k x k
// block, so the first k rows are the identity (systematic) and any k rows remain
// invertible: any k rows of V form a Vandermonde matrix with distinct evaluation points.
func rsMatrix(n, k int) ([]byte, error) {
	if k <= 0 || n < k || n > MaxCodedSegments {
		return nil, fmt.Errorf("%w: n=%d k=%d", ErrCodedShape, n, k)
	}
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
	if !gfInvertMatrix(top, k) {
		return nil, fmt.Errorf("%w: singular top block", ErrCodedShape)
	}
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
	return m, nil
}

// rsParity computes the parity shards for k equal-length data shards.
func rsParity(data [][]byte, parity int) ([][]byte, error) {
	k := len(data)
	m, err := rsMatrix(k+parity, k)
	if err != nil {
		return nil, err
	}
	out := make([][]byte, parity)
	for i := range out {
		out[i] = make([]byte, len(data[0]))
	}
	gfMatMul(out, m[k*k:], k, data)
	return out, nil
}

// rsRecover fills the nil entries of shards in place. shards has the full coded length n;
// at least k entries must be non-nil and of equal length.
func rsRecover(shards [][]byte, k int) error {
	n := len(shards)
	m, err := rsMatrix(n, k)
	if err != nil {
		return err
	}
	// Take the first k present shards and the matrix rows that produced them.
	sub := make([]byte, 0, k*k)
	in := make([][]byte, 0, k)
	for i := 0; i < n && len(in) < k; i++ {
		if shards[i] == nil {
			continue
		}
		sub = append(sub, m[i*k:(i+1)*k]...)
		in = append(in, shards[i])
	}
	if len(in) < k {
		return fmt.Errorf("%w: have %d, need %d", ErrNotEnoughSegments, len(in), k)
	}
	if !gfInvertMatrix(sub, k) {
		// Unreachable for a well-formed matrix: any k rows are invertible by construction.
		return fmt.Errorf("%w: singular recovery matrix", ErrCodedShape)
	}
	data := make([][]byte, k)
	for i := range data {
		data[i] = make([]byte, len(in[0]))
	}
	gfMatMul(data, sub, k, in)
	// Recompute only what is missing; everything present is already verified.
	for i := 0; i < n; i++ {
		if shards[i] != nil {
			continue
		}
		shard := make([]byte, len(in[0]))
		gfMatMul([][]byte{shard}, m[i*k:(i+1)*k], k, data)
		shards[i] = shard
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

// BuildCodedSegmentMessages produces the wire messages for a Reed-Solomon coded group:
// the systematic segments of msg followed by parity segments, committed together.
func BuildCodedSegmentMessages(msg []byte, segmentSize, parity int, h Hasher) ([]*SegmentMessage, error) {
	if parity <= 0 {
		return nil, fmt.Errorf("%w: parity %d", ErrCodedShape, parity)
	}
	segs, err := Split(msg, segmentSize)
	if err != nil {
		return nil, err
	}
	k := len(segs)
	n := k + parity
	if n > MaxCodedSegments {
		return nil, fmt.Errorf("%w: %d segments, max %d", ErrCodedShape, n, MaxCodedSegments)
	}
	d := &Descriptor{
		Version:     VersionCoded,
		HashID:      h.ID(),
		Count:       uint32(n),
		SegmentSize: uint32(segmentSize),
		TotalLength: uint64(len(msg)),
	}
	// The codec zero-pads the final systematic segment; the wire carries it truncated, and
	// the commitment covers the wire form.
	par, err := rsParity(paddedShards(d, segs), parity)
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
