// Package segments splits a large message into K segments committed by a Merkle tree,
// so each segment can be authenticated independently of the others.
package segments

import (
	"errors"
	"fmt"

	"github.com/OffchainLabs/prysm/v7/crypto/hash"
)

// HashID identifies a hash function on the wire. Values are part of the segment format.
type HashID uint8

const (
	// HashSHA256 is the default and matches the hash used by SSZ.
	HashSHA256 HashID = 0
	// HashKeccak256 is provided for interoperability with execution-layer tooling.
	HashKeccak256 HashID = 1
)

// Leaf and node hashing are domain-separated so a leaf digest can never be reinterpreted
// as an internal node. A variable-width tree has no fixed depth to prevent that otherwise.
const (
	leafPrefix       byte = 0x00
	nodePrefix       byte = 0x01
	descriptorPrefix byte = 0x02
)

// ErrUnknownHashID is returned for a hash ID that is not registered.
var ErrUnknownHashID = errors.New("unknown hash id")

// Hasher hashes segment leaves and internal tree nodes.
type Hasher interface {
	// ID returns the wire identifier for this hash function.
	ID() HashID
	// Size returns the digest length in bytes.
	Size() int
	// HashLeaf hashes segment data into a leaf digest.
	HashLeaf(b []byte) []byte
	// HashNode hashes two child digests into a parent digest.
	HashNode(l, r []byte) []byte
	// Hash hashes raw bytes with no domain prefix. Callers outside this package should
	// prefer HashLeaf or HashNode; this exists for domain-separated framing built here.
	Hash(b []byte) []byte
}

// hashers maps each wire ID to its implementation.
var hashers = map[HashID]Hasher{
	HashSHA256:    prefixHasher{id: HashSHA256, fn: hash.Hash},
	HashKeccak256: prefixHasher{id: HashKeccak256, fn: hash.Keccak256},
}

// HasherByID returns the Hasher registered for id.
func HasherByID(id HashID) (Hasher, error) {
	h, ok := hashers[id]
	if !ok {
		return nil, fmt.Errorf("%w: %d", ErrUnknownHashID, id)
	}
	return h, nil
}

// prefixHasher frames leaves and nodes with their domain prefix and digests them with fn. The
// registered hash functions differ only in fn; the framing is what the wire format fixes.
type prefixHasher struct {
	id HashID
	fn func([]byte) [32]byte
}

func (h prefixHasher) ID() HashID { return h.id }

func (h prefixHasher) Size() int { return 32 }

func (h prefixHasher) HashLeaf(b []byte) []byte { return h.hashPrefixed(leafPrefix, b) }

func (h prefixHasher) HashNode(l, r []byte) []byte { return h.hashPrefixed(nodePrefix, l, r) }

func (h prefixHasher) Hash(b []byte) []byte {
	d := h.fn(b)
	return d[:]
}

// hashPrefixed digests prefix followed by parts, concatenated in one buffer.
func (h prefixHasher) hashPrefixed(prefix byte, parts ...[]byte) []byte {
	n := 1
	for _, p := range parts {
		n += len(p)
	}
	buf := make([]byte, 0, n)
	buf = append(buf, prefix)
	for _, p := range parts {
		buf = append(buf, p...)
	}
	d := h.fn(buf)
	return d[:]
}
