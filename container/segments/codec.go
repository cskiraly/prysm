package segments

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math/bits"

	"github.com/OffchainLabs/prysm/v7/math"
)

// Field widths for the segment wire format.
const (
	indexLen      = 4
	proofCountLen = 1
	dataLenLen    = 4
)

var (
	// ErrShortBuffer is returned when a buffer ends before a field is complete.
	ErrShortBuffer = errors.New("buffer too short for segment message")
	// ErrTrailingBytes is returned when a buffer has bytes left after decoding.
	ErrTrailingBytes = errors.New("trailing bytes after segment message")
	// ErrProofCount is returned for a proof element count beyond the tree depth limit.
	ErrProofCount = errors.New("proof element count out of range")
)

// maxProofCount is the deepest proof any valid descriptor can require.
var maxProofCount = bits.Len(uint(MaxSegments - 1))

// SegmentMessage is the on-wire form of one segment of a segmented message.
//
// A Merkle proof only proves a segment belongs to Descriptor.Root, so something outside the
// tree has to establish that Root is legitimate. Nothing here does, deliberately: the receiver
// already knows which group ids are committed, from the bids it accepted with the blocks, so
// authenticating a segment is a membership test on Descriptor.GroupID rather than anything the
// sender asserts.
//
// That is why there is no authority field. An earlier revision carried the slot and block root
// a segment claimed authority from, which was redundant -- the group id already identifies the
// group, and the receiver either knows it is committed or does not -- and actively harmful: a
// field outside the descriptor and outside the tree can be rewritten without invalidating any
// proof, so it needed its own invariant, its own error and its own tests. Naming the block is
// still useful at *announcement* time, before the body is fetched, and that belongs in the
// message id.
type SegmentMessage struct {
	Descriptor *Descriptor
	Index      uint32
	Proof      [][]byte
	Data       []byte
}

// Marshal encodes the segment message.
func (m *SegmentMessage) Marshal() ([]byte, error) {
	if m.Descriptor == nil {
		return nil, fmt.Errorf("%w: nil descriptor", ErrDescriptorMismatch)
	}
	if len(m.Proof) > maxProofCount {
		return nil, fmt.Errorf("%w: %d elements", ErrProofCount, len(m.Proof))
	}
	// Bound the data here, not only on decode, so MaxSegmentMessageSize really is the largest
	// buffer this function can produce -- the gossip wire type's ssz_max is derived from it.
	if len(m.Data) > MaxSegmentSize {
		return nil, fmt.Errorf("%w: %d bytes", ErrSegmentSize, len(m.Data))
	}
	digestSize := len(m.Descriptor.Root)
	for _, p := range m.Proof {
		if len(p) != digestSize {
			return nil, fmt.Errorf("%w: %d bytes, want %d", ErrProofDigestSize, len(p), digestSize)
		}
	}
	desc := m.Descriptor.MarshalCanonical()
	total := len(desc) + indexLen + proofCountLen + len(m.Proof)*digestSize +
		dataLenLen + len(m.Data)
	out := make([]byte, 0, total)
	out = append(out, desc...)
	out = binary.LittleEndian.AppendUint32(out, m.Index)
	out = append(out, byte(len(m.Proof)))
	for _, p := range m.Proof {
		out = append(out, p...)
	}
	out = binary.LittleEndian.AppendUint32(out, uint32(len(m.Data)))
	out = append(out, m.Data...)
	return out, nil
}

// UnmarshalSegmentMessage decodes a segment message and returns the hasher it names.
//
// The descriptor is self-describing: byte 1 is the hash ID, which fixes the digest size and
// therefore every subsequent field boundary. Decoding is strict — exact length, no trailing
// bytes, every length bounded before allocation.
func UnmarshalSegmentMessage(b []byte) (*SegmentMessage, Hasher, error) {
	// Need version and hash ID before anything else can be sized.
	if len(b) < 2 {
		return nil, nil, fmt.Errorf("%w: %d bytes", ErrShortBuffer, len(b))
	}
	h, err := HasherByID(HashID(b[1]))
	if err != nil {
		return nil, nil, err
	}
	digestSize := h.Size()

	off := 0
	descLen := descriptorFixedLen + digestSize
	if len(b) < off+descLen {
		return nil, nil, fmt.Errorf("%w: descriptor needs %d bytes", ErrShortBuffer, descLen)
	}
	d, err := UnmarshalCanonical(b[off:off+descLen], digestSize)
	if err != nil {
		return nil, nil, err
	}
	off += descLen

	if len(b) < off+indexLen+proofCountLen {
		return nil, nil, fmt.Errorf("%w: index and proof count", ErrShortBuffer)
	}
	index := binary.LittleEndian.Uint32(b[off : off+indexLen])
	off += indexLen
	proofCount := int(b[off])
	off += proofCountLen
	if proofCount > maxProofCount {
		return nil, nil, fmt.Errorf("%w: %d, max %d", ErrProofCount, proofCount, maxProofCount)
	}

	if len(b) < off+proofCount*digestSize {
		return nil, nil, fmt.Errorf("%w: %d proof elements", ErrShortBuffer, proofCount)
	}
	proof := make([][]byte, proofCount)
	for i := range proof {
		proof[i] = make([]byte, digestSize)
		copy(proof[i], b[off:off+digestSize])
		off += digestSize
	}

	if len(b) < off+dataLenLen {
		return nil, nil, fmt.Errorf("%w: data length", ErrShortBuffer)
	}
	rawDataLen := binary.LittleEndian.Uint32(b[off : off+dataLenLen])
	off += dataLenLen
	if rawDataLen > MaxSegmentSize {
		return nil, nil, fmt.Errorf("%w: %d bytes", ErrSegmentSize, rawDataLen)
	}
	// Bounded above first, then converted through a checked cast: int is 32-bit on some
	// platforms, so a raw uint32 cast could wrap negative.
	dataLen, err := math.Int(uint64(rawDataLen))
	if err != nil {
		return nil, nil, fmt.Errorf("%w: data length %d: %w", ErrSegmentSize, rawDataLen, err)
	}
	if len(b) < off+dataLen {
		return nil, nil, fmt.Errorf("%w: segment data", ErrShortBuffer)
	}
	data := make([]byte, dataLen)
	copy(data, b[off:off+dataLen])
	off += dataLen

	if off != len(b) {
		return nil, nil, fmt.Errorf("%w: %d bytes left", ErrTrailingBytes, len(b)-off)
	}
	return &SegmentMessage{Descriptor: d, Index: index, Proof: proof, Data: data}, h, nil
}

// Verify checks the descriptor, the segment length and the Merkle proof.
//
// It does NOT establish that the descriptor is legitimate: deciding whether this group id is
// one the chain committed to is the caller's job, because only the caller knows which bids it
// has accepted.
func (m *SegmentMessage) Verify(h Hasher) error {
	return VerifySegment(m.Descriptor, h, int(m.Index), m.Data, m.Proof)
}

// BuildSegmentMessages produces the wire messages for every segment of msg.
func BuildSegmentMessages(msg []byte, segmentSize int, h Hasher) ([]*SegmentMessage, error) {
	d, segs, tree, err := commit(msg, segmentSize, h)
	if err != nil {
		return nil, err
	}
	out := make([]*SegmentMessage, len(segs))
	for i := range segs {
		proof, err := tree.Proof(i)
		if err != nil {
			return nil, err
		}
		out[i] = &SegmentMessage{
			Descriptor: d,
			Index:      uint32(i),
			Proof:      proof,
			Data:       segs[i],
		}
	}
	return out, nil
}

// MaxSegmentMessageSize is the largest buffer Marshal can produce, and therefore the
// bound the gossip wire type must allow. Kept in sync with the ssz_max on
// ethpb.ExecutionPayloadSegment by TestMaxSegmentMessageSize.
const MaxSegmentMessageSize = descriptorFixedLen + 32 + // descriptor with a 32-byte root
	indexLen + proofCountLen + 14*32 + // deepest proof: ceil(log2 MaxSegments) = 14
	dataLenLen + MaxSegmentSize
