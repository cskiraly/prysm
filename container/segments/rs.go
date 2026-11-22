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
// TotalLength: the excess is Reed-Solomon parity, of the GF(2^8) matrix code of this file.
// VersionCodedLeopard and VersionCodedLeopard8 (leopard.go) mark the same with other codes.
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

// rsRecoverData fills the nil data entries of shards in place. shards has the full coded length
// n; at least k entries must be non-nil and of equal length. Parity entries are left as they are.
func rsRecoverData(shards [][]byte, k int) error {
	n := len(shards)
	m, err := rsMatrix(n, k)
	if err != nil {
		return err
	}
	// Take the first k present shards and the matrix rows that produced them.
	sub := make([]byte, 0, k*k)
	in := make([][]byte, 0, k)
	missing := false
	for i := 0; i < n && len(in) < k; i++ {
		if shards[i] == nil {
			missing = missing || i < k
			continue
		}
		sub = append(sub, m[i*k:(i+1)*k]...)
		in = append(in, shards[i])
	}
	if len(in) < k {
		return fmt.Errorf("%w: have %d, need %d", ErrNotEnoughSegments, len(in), k)
	}
	if !missing {
		return nil // the first k present shards are the data
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
	for i := 0; i < k; i++ {
		if shards[i] == nil {
			shards[i] = data[i]
		}
	}
	return nil
}

// paddedShards returns the segments padded for coding, to the code's shard size: SegmentSize
// under the matrix code, where only the final systematic segment is short on the wire, and
// SegmentSize rounded up to the code's block under the two Leopard codes (under Leopard GF(2^8)
// a parity segment on the wire is shorter than its shard too).
func paddedShards(d *Descriptor, segs [][]byte) [][]byte {
	out := make([][]byte, len(segs))
	size := d.shardSize()
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
// the systematic segments of msg followed by parity segments, committed together. The code
// is the GF(2^8) one (VersionCoded).
func BuildCodedSegmentMessages(msg []byte, segmentSize, parity int, h Hasher) ([]*SegmentMessage, error) {
	return BuildCodedSegmentMessagesVersion(VersionCoded, msg, segmentSize, parity, h)
}

// BuildCodedSegmentMessagesVersion is BuildCodedSegmentMessages under the code that version
// names: VersionCoded, VersionCodedLeopard or VersionCodedLeopard8.
func BuildCodedSegmentMessagesVersion(version uint8, msg []byte, segmentSize, parity int, h Hasher) ([]*SegmentMessage, error) {
	if version != VersionCoded && !isLeopard(version) {
		return nil, fmt.Errorf("%w: version %d is not a coded one", ErrCodedShape, version)
	}
	if parity <= 0 {
		return nil, fmt.Errorf("%w: parity %d", ErrCodedShape, parity)
	}
	segs, err := Split(msg, segmentSize)
	if err != nil {
		return nil, err
	}
	k := len(segs)
	// Compared before the sum is taken: a parity near the top of int must not wrap it.
	if limit := maxCodedSegments(version); parity > limit-k {
		return nil, fmt.Errorf("%w: %d segments, max %d", ErrCodedShape, uint64(k)+uint64(parity), limit)
	}
	if isLeopard(version) && !leopardShapeOK(version, k, parity) {
		return nil, fmt.Errorf("%w: %d data and %d parity segments, the Leopard codes take %d to %d parity (at most %d over GF(2^8))", ErrCodedShape, k, parity, k, MaxLeopardParityPerData*k, MaxLeopard8Parity)
	}
	if version == VersionCodedLeopard {
		// segmentSize is at most MaxSegmentSize (Split), so the conversion keeps its value.
		if work := leopardWorkBytes(parity, uint32(segmentSize)); work > MaxLeopardWorkBytes { // lint:ignore uintcast -- bounded by Split.
			return nil, fmt.Errorf("%w: %d parity segments of %d bytes need %d bytes of workspace, max %d", ErrCodedShape, parity, segmentSize, work, MaxLeopardWorkBytes)
		}
	}
	n := k + parity
	d := &Descriptor{
		Version:     version,
		HashID:      h.ID(),
		Count:       uint32(n),
		SegmentSize: uint32(segmentSize),
		TotalLength: uint64(len(msg)),
	}
	// The codec zero-pads the final systematic segment; the wire carries it truncated, and
	// the commitment covers the wire form.
	var par [][]byte
	if isLeopard(version) {
		par, err = leopardParity(version, paddedShards(d, segs), parity)
	} else {
		par, err = rsParity(paddedShards(d, segs), parity)
	}
	if err != nil {
		return nil, err
	}
	leaves := make([][]byte, 0, n)
	leaves = append(leaves, segs...)
	for i, p := range par {
		// A parity shard in wire form: itself, except under Leopard GF(2^8), where the code's
		// shard is longer than the segment and zero beyond it.
		w, err := wireShard(d, k+i, p)
		if err != nil {
			return nil, err
		}
		leaves = append(leaves, w)
	}
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

// RecoverAndVerify reconstructs a coded group's payload from any Required() or more of its
// segments, and rejects a group whose committed segments were not a consistent codeword.
//
// segs has Count entries, nil for missing; present entries must already be proof-verified.
// The consistency check matters because a Merkle root binds arbitrary leaves: a builder
// could commit segments that are not a codeword, making different K-subsets decode to
// different payloads, or pass from some sets and fail from others.
//
// It decides by the committed leaves alone, whatever set of them it is given: it recovers the
// data segments, takes them in wire form, encodes the parity again from them, and requires
// the tree over that codeword to have the committed root. An honest group passes from every
// set. A group whose committed leaves are not the codeword of its data, padded with zeros,
// fails from every set, because the codeword built here then differs from the committed leaves
// in at least one leaf. Recomputing only the segments that are missing would not do: a set
// that holds a bad parity segment beyond its first K, or lacks the one segment that shows a
// non-zero pad, would pass.
func RecoverAndVerify(d *Descriptor, h Hasher, segs [][]byte) ([]byte, error) {
	if !d.Coded() {
		return nil, fmt.Errorf("%w: version %d", ErrDescriptorMismatch, d.Version)
	}
	if err := d.Validate(h); err != nil {
		return nil, err
	}
	if uint32(len(segs)) != d.Count {
		return nil, fmt.Errorf("%w: %d entries, want %d", ErrDescriptorMismatch, len(segs), d.Count)
	}
	k := int(d.Required()) // lint:ignore uintcast -- bounded by MaxSegments via Validate.
	n := len(segs)
	size := d.shardSize()
	shards := make([][]byte, n)
	present := 0
	for i, s := range segs {
		if s == nil {
			continue
		}
		want, err := d.SegmentLength(i)
		if err != nil {
			return nil, err
		}
		if len(s) != want {
			return nil, fmt.Errorf("%w: %d bytes at index %d, want %d", ErrSegmentLength, len(s), i, want)
		}
		if want != size {
			p := make([]byte, size)
			copy(p, s)
			s = p
		}
		shards[i] = s
		present++
	}
	if present < k {
		return nil, fmt.Errorf("%w: have %d, need %d", ErrNotEnoughSegments, present, k)
	}
	code, err := coderFor(d.Version, k, n-k)
	if err != nil {
		return nil, err
	}
	if err := code.recoverData(shards, k); err != nil {
		return nil, err
	}
	// A recovered data segment with a non-zero pad cannot belong to a codeword of zero-padded
	// data, so the committed leaves are not one.
	if _, zero, err := codedWire(d, shards[:k]); err != nil {
		return nil, err
	} else if !zero {
		return nil, ErrCodewordMismatch
	}
	par, err := code.parity(shards[:k], n-k)
	if err != nil {
		return nil, err
	}
	// The codeword of the recovered data. Under Leopard GF(2^8) its parity is zero beyond the
	// segment size, because all the data is; the wire form leaves those bytes out.
	leaves, _, err := codedWire(d, append(shards[:k:k], par...))
	if err != nil {
		return nil, err
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

// coder is a coded version's two operations on shards padded to the code's shard size.
type coder struct {
	// recoverData fills the nil data entries of shards in place, from any k present entries.
	recoverData func(shards [][]byte, k int) error
	// parity computes the parity shards of k data shards.
	parity func(data [][]byte, parity int) ([][]byte, error)
}

// coderFor is the code a version names, for a group of k data and parity parity segments.
func coderFor(version uint8, k, parity int) (coder, error) {
	if !isLeopard(version) {
		return coder{recoverData: rsRecoverData, parity: rsParity}, nil
	}
	// One encoder for both steps, so the second finds the first's work buffers in its pool.
	enc, err := leopardEncoder(version, k, parity)
	if err != nil {
		return coder{}, err
	}
	return coder{
		recoverData: func(shards [][]byte, k int) error { return leopardRecoverData(enc, shards, k) },
		parity:      func(data [][]byte, parity int) ([][]byte, error) { return leopardParityWith(enc, data, parity) },
	}, nil
}

// codedWire returns a codeword's shards in wire form, and reports whether every byte the wire
// form leaves out is zero. Data segments lose their pad (wireShard); parity segments keep the
// shard's length, except under Leopard GF(2^8), where they lose the pad too (SegmentLength).
func codedWire(d *Descriptor, shards [][]byte) ([][]byte, bool, error) {
	leaves := make([][]byte, len(shards))
	zero := true
	for i, s := range shards {
		w, err := wireShard(d, i, s)
		if err != nil {
			return nil, false, err
		}
		for _, b := range s[len(w):] {
			if b != 0 {
				zero = false
			}
		}
		leaves[i] = w
	}
	return leaves, zero, nil
}
