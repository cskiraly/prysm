package segments

import (
	"crypto/subtle"
	"errors"
	"fmt"
	"math/bits"
)

var (
	// ErrNoLeaves is returned when building a tree over zero leaves.
	ErrNoLeaves = errors.New("no leaves to build a tree over")
	// ErrIndexOutOfRange is returned for a leaf index beyond the leaf count.
	ErrIndexOutOfRange = errors.New("leaf index out of range")
	// ErrProofLength is returned when a proof does not match the tree depth.
	ErrProofLength = errors.New("proof length does not match tree depth")
	// ErrProofDigestSize is returned when a proof element is not the hasher's digest size.
	ErrProofDigestSize = errors.New("proof element has wrong digest size")
	// ErrRootMismatch is returned when a proof does not reproduce the expected root.
	ErrRootMismatch = errors.New("proof does not reproduce the expected root")
)

// Tree is a Merkle tree over segment leaves, padded to a power-of-two width.
//
// Padding uses a zero digest rather than a repeated real leaf. That is only unambiguous
// because the leaf count is carried in the authenticated Descriptor: without it, two
// different leaf counts could produce the same root.
type Tree struct {
	hasher Hasher
	// layers[0] holds leaf digests; the last layer holds the single root.
	layers [][][]byte
	// count is the number of real (unpadded) leaves.
	count int
}

// depth returns the number of proof elements for this tree.
func (t *Tree) depth() int {
	return len(t.layers) - 1
}

// Root returns the tree root.
func (t *Tree) Root() []byte {
	root := t.layers[len(t.layers)-1][0]
	out := make([]byte, len(root))
	copy(out, root)
	return out
}

// Count returns the number of real leaves.
func (t *Tree) Count() int { return t.count }

// BuildTree hashes each segment into a leaf and builds the tree above them.
func BuildTree(h Hasher, segs [][]byte) (*Tree, error) {
	if len(segs) == 0 {
		return nil, ErrNoLeaves
	}
	width := 1 << bits.Len(uint(len(segs)-1))
	leaves := make([][]byte, width)
	for i, s := range segs {
		leaves[i] = h.HashLeaf(s)
	}
	// Pad the remainder with a zero digest.
	for i := len(segs); i < width; i++ {
		leaves[i] = make([]byte, h.Size())
	}
	layers := [][][]byte{leaves}
	for len(layers[len(layers)-1]) > 1 {
		prev := layers[len(layers)-1]
		next := make([][]byte, len(prev)/2)
		for i := 0; i < len(prev); i += 2 {
			next[i/2] = h.HashNode(prev[i], prev[i+1])
		}
		layers = append(layers, next)
	}
	return &Tree{hasher: h, layers: layers, count: len(segs)}, nil
}

// Proof returns the sibling path authenticating the leaf at index.
func (t *Tree) Proof(index int) ([][]byte, error) {
	if index < 0 || index >= t.count {
		return nil, fmt.Errorf("%w: %d not in [0,%d)", ErrIndexOutOfRange, index, t.count)
	}
	proof := make([][]byte, 0, t.depth())
	for level := 0; level < t.depth(); level++ {
		sibling := index ^ 1
		node := make([]byte, len(t.layers[level][sibling]))
		copy(node, t.layers[level][sibling])
		proof = append(proof, node)
		index /= 2
	}
	return proof, nil
}

// VerifyProof recomputes the root from a segment and its proof.
//
// The direction at each level is derived from index, never taken from the proof, so a peer
// cannot steer the path by reordering elements.
func VerifyProof(h Hasher, root []byte, seg []byte, index, count int, proof [][]byte) error {
	if count <= 0 {
		return ErrNoLeaves
	}
	if index < 0 || index >= count {
		return fmt.Errorf("%w: %d not in [0,%d)", ErrIndexOutOfRange, index, count)
	}
	width := 1 << bits.Len(uint(count-1))
	wantDepth := bits.Len(uint(width - 1))
	if len(proof) != wantDepth {
		return fmt.Errorf("%w: got %d, want %d", ErrProofLength, len(proof), wantDepth)
	}
	for _, p := range proof {
		if len(p) != h.Size() {
			return fmt.Errorf("%w: got %d, want %d", ErrProofDigestSize, len(p), h.Size())
		}
	}
	if len(root) != h.Size() {
		return fmt.Errorf("%w: root has %d bytes, want %d", ErrProofDigestSize, len(root), h.Size())
	}
	cur := h.HashLeaf(seg)
	for _, sibling := range proof {
		if index%2 == 0 {
			cur = h.HashNode(cur, sibling)
		} else {
			cur = h.HashNode(sibling, cur)
		}
		index /= 2
	}
	if subtle.ConstantTimeCompare(cur, root) != 1 {
		return ErrRootMismatch
	}
	return nil
}
