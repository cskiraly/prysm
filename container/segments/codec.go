package segments

import (
	"bytes"
	"errors"
	"fmt"

	ethpb "github.com/OffchainLabs/prysm/v7/proto/prysm/v1alpha1"
)

// MaxProofDepth is the deepest proof any valid descriptor can require, ceil(log2 MaxSegments),
// and the bound on the wire type's proof list. TestMaxProofDepth pins the two together.
const MaxProofDepth = 14

// digestSize is the one digest width the wire type carries; every registered hasher produces it.
const digestSize = 32

var (
	// ErrProofCount is returned for a proof element count beyond the tree depth limit.
	ErrProofCount = errors.New("proof element count out of range")
	// ErrWireField is returned when a wire field does not fit the descriptor's type.
	ErrWireField = errors.New("wire field out of range")
)

// SegmentMessage is one segment of a segmented message: the descriptor it belongs to, its
// index, its Merkle proof and its bytes. On the wire it travels as ethpb.ExecutionPayloadSegment,
// an SSZ container; ToProto and FromProto convert, and FromProto bounds every field.
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

// ToProto converts the message to its gossip wire type, refusing anything the wire cannot carry.
func (m *SegmentMessage) ToProto() (*ethpb.ExecutionPayloadSegment, error) {
	d := m.Descriptor
	if d == nil {
		return nil, fmt.Errorf("%w: nil descriptor", ErrDescriptorMismatch)
	}
	if len(m.Proof) > MaxProofDepth {
		return nil, fmt.Errorf("%w: %d elements", ErrProofCount, len(m.Proof))
	}
	if len(m.Data) > MaxSegmentSize {
		return nil, fmt.Errorf("%w: %d bytes", ErrSegmentSize, len(m.Data))
	}
	if len(d.Root) != digestSize {
		return nil, fmt.Errorf("%w: root has %d bytes, want %d", ErrProofDigestSize, len(d.Root), digestSize)
	}
	proof := make([][]byte, len(m.Proof))
	for i, p := range m.Proof {
		if len(p) != digestSize {
			return nil, fmt.Errorf("%w: %d bytes, want %d", ErrProofDigestSize, len(p), digestSize)
		}
		proof[i] = bytes.Clone(p)
	}
	return &ethpb.ExecutionPayloadSegment{
		SegmentDescriptor: &ethpb.SegmentDescriptor{
			Version:     uint32(d.Version),
			HashId:      uint32(d.HashID),
			Count:       d.Count,
			SegmentSize: d.SegmentSize,
			TotalLength: d.TotalLength,
			Root:        bytes.Clone(d.Root),
		},
		Index: m.Index,
		Proof: proof,
		Data:  bytes.Clone(m.Data),
	}, nil
}

// FromProto converts a decoded wire message and returns the hasher its descriptor names.
//
// Strict: a field the descriptor's type cannot hold, an unknown hash id, a digest of the wrong
// width, too many proof elements or oversized data are all rejected here, so a caller can hand
// the result straight to Verify. SSZ already bounds the proof list and the data on decode; the
// checks are repeated so a message built in-process is held to the same bounds.
func FromProto(pb *ethpb.ExecutionPayloadSegment) (*SegmentMessage, Hasher, error) {
	if pb == nil || pb.SegmentDescriptor == nil {
		return nil, nil, fmt.Errorf("%w: nil descriptor", ErrDescriptorMismatch)
	}
	pd := pb.SegmentDescriptor
	if pd.Version > 0xff || pd.HashId > 0xff {
		return nil, nil, fmt.Errorf("%w: version %d, hash id %d", ErrWireField, pd.Version, pd.HashId)
	}
	h, err := HasherByID(HashID(pd.HashId))
	if err != nil {
		return nil, nil, err
	}
	if len(pd.Root) != h.Size() {
		return nil, nil, fmt.Errorf("%w: root %d bytes, want %d", ErrDescriptorMismatch, len(pd.Root), h.Size())
	}
	if len(pb.Proof) > MaxProofDepth {
		return nil, nil, fmt.Errorf("%w: %d, max %d", ErrProofCount, len(pb.Proof), MaxProofDepth)
	}
	if len(pb.Data) > MaxSegmentSize {
		return nil, nil, fmt.Errorf("%w: %d bytes", ErrSegmentSize, len(pb.Data))
	}
	proof := make([][]byte, len(pb.Proof))
	for i, p := range pb.Proof {
		if len(p) != h.Size() {
			return nil, nil, fmt.Errorf("%w: got %d, want %d", ErrProofDigestSize, len(p), h.Size())
		}
		proof[i] = bytes.Clone(p)
	}
	d := &Descriptor{
		Version:     uint8(pd.Version),
		HashID:      HashID(pd.HashId),
		Count:       pd.Count,
		SegmentSize: pd.SegmentSize,
		TotalLength: pd.TotalLength,
		Root:        bytes.Clone(pd.Root),
	}
	return &SegmentMessage{Descriptor: d, Index: pb.Index, Proof: proof, Data: bytes.Clone(pb.Data)}, h, nil
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
