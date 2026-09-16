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
	HashSHA256:    sha256Hasher{},
	HashKeccak256: keccak256Hasher{},
}

// HasherByID returns the Hasher registered for id.
func HasherByID(id HashID) (Hasher, error) {
	h, ok := hashers[id]
	if !ok {
		return nil, fmt.Errorf("%w: %d", ErrUnknownHashID, id)
	}
	return h, nil
}

type sha256Hasher struct{}

func (sha256Hasher) ID() HashID { return HashSHA256 }

func (sha256Hasher) Size() int { return 32 }

func (sha256Hasher) HashLeaf(b []byte) []byte {
	buf := make([]byte, 0, len(b)+1)
	buf = append(buf, leafPrefix)
	buf = append(buf, b...)
	d := hash.Hash(buf)
	return d[:]
}

func (sha256Hasher) Hash(b []byte) []byte {
	d := hash.Hash(b)
	return d[:]
}

func (sha256Hasher) HashNode(l, r []byte) []byte {
	buf := make([]byte, 0, len(l)+len(r)+1)
	buf = append(buf, nodePrefix)
	buf = append(buf, l...)
	buf = append(buf, r...)
	d := hash.Hash(buf)
	return d[:]
}

type keccak256Hasher struct{}

func (keccak256Hasher) ID() HashID { return HashKeccak256 }

func (keccak256Hasher) Size() int { return 32 }

func (keccak256Hasher) HashLeaf(b []byte) []byte {
	buf := make([]byte, 0, len(b)+1)
	buf = append(buf, leafPrefix)
	buf = append(buf, b...)
	d := hash.Keccak256(buf)
	return d[:]
}

func (keccak256Hasher) Hash(b []byte) []byte {
	d := hash.Keccak256(b)
	return d[:]
}

func (keccak256Hasher) HashNode(l, r []byte) []byte {
	buf := make([]byte, 0, len(l)+len(r)+1)
	buf = append(buf, nodePrefix)
	buf = append(buf, l...)
	buf = append(buf, r...)
	d := hash.Keccak256(buf)
	return d[:]
}
