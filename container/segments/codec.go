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

// MaxWireSegmentSize is the wire type's bound on a segment's data, seacast-lite.md's
// MAX_SEGMENT_SIZE: above snappy's worst case at the default segment size. The library's
// own bound, MaxSegmentSize, is wider; a segment that travels is held to this one.
const MaxWireSegmentSize = 32 << 10

var (
	// ErrProofCount is returned for a proof element count beyond the tree depth limit.
	ErrProofCount = errors.New("proof element count out of range")
	// ErrWireField is returned when a wire field does not fit the descriptor's type.
	ErrWireField = errors.New("wire field out of range")
	// ErrGroupKey is returned for a segment whose group key is not its descriptor's at its slot.
	ErrGroupKey = errors.New("group key does not match the descriptor and slot")
)

// SegmentMessage is one segment of a segmented message: the descriptor it belongs to, its
// index, its Merkle proof and its bytes, and what the segment names besides its group's own
// fields. On the wire it travels as ethpb.ExecutionPayloadSegment, SEACAST's Chunk
// (seacast-lite.md §2); ToProto and FromProto convert, and FromProto bounds every field.
//
// A Merkle proof only proves a segment belongs to Descriptor.Root, so something outside the
// tree has to establish that Root is legitimate. The chunk names it: Slot and BeaconBlockRoot
// say which block's bid commits the group, and Group is the group's key at that slot,
// GroupKey(Descriptor.GroupID, Slot), which binds every field of the descriptor and the slot.
// The receiver checks the key against the descriptor and the slot, and the block against
// what it has installed (seacast-lite.md §3's level 1). Build leaves the three fields zero: the
// publisher sets them (WithContext), since only it knows the block.
type SegmentMessage struct {
	Slot            uint64
	BeaconBlockRoot []byte
	Group           []byte
	Descriptor      *Descriptor
	Index           uint32
	Proof           [][]byte
	Data            []byte
}

// WithContext sets the segments' slot and block root, and their group key at that slot, in
// place: what the publisher of a group adds to the segments Build cut.
func WithContext(segs []*SegmentMessage, h Hasher, slot uint64, beaconBlockRoot []byte) error {
	if len(beaconBlockRoot) != digestSize {
		return fmt.Errorf("%w: block root has %d bytes, want %d", ErrWireField, len(beaconBlockRoot), digestSize)
	}
	var key []byte
	for _, m := range segs {
		if m == nil || m.Descriptor == nil {
			return fmt.Errorf("%w: nil descriptor", ErrDescriptorMismatch)
		}
		if key == nil {
			key = GroupKey(m.Descriptor.GroupID(h), slot)
		}
		m.Slot, m.BeaconBlockRoot, m.Group = slot, bytes.Clone(beaconBlockRoot), bytes.Clone(key)
	}
	return nil
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
	if len(m.Data) > MaxWireSegmentSize {
		return nil, fmt.Errorf("%w: %d bytes", ErrSegmentSize, len(m.Data))
	}
	if len(m.BeaconBlockRoot) != digestSize || len(m.Group) != digestSize {
		return nil, fmt.Errorf("%w: block root %d bytes, group %d, want %d", ErrWireField, len(m.BeaconBlockRoot), len(m.Group), digestSize)
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
		Slot:            m.Slot,
		BeaconBlockRoot: bytes.Clone(m.BeaconBlockRoot),
		Group:           bytes.Clone(m.Group),
		SegmentDescriptor: &ethpb.SegmentDescriptor{
			Version:     []byte{d.Version},
			HashId:      []byte{byte(d.HashID)},
			Encoding:    []byte{byte(d.Encoding)},
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
	if len(pd.Version) != 1 || len(pd.HashId) != 1 || len(pd.Encoding) != 1 {
		return nil, nil, fmt.Errorf("%w: version %d bytes, hash id %d, encoding %d, want 1", ErrWireField, len(pd.Version), len(pd.HashId), len(pd.Encoding))
	}
	if len(pb.BeaconBlockRoot) != digestSize || len(pb.Group) != digestSize {
		return nil, nil, fmt.Errorf("%w: block root %d bytes, group %d, want %d", ErrWireField, len(pb.BeaconBlockRoot), len(pb.Group), digestSize)
	}
	h, err := HasherByID(HashID(pd.HashId[0]))
	if err != nil {
		return nil, nil, err
	}
	if len(pd.Root) != h.Size() {
		return nil, nil, fmt.Errorf("%w: root %d bytes, want %d", ErrDescriptorMismatch, len(pd.Root), h.Size())
	}
	if len(pb.Proof) > MaxProofDepth {
		return nil, nil, fmt.Errorf("%w: %d, max %d", ErrProofCount, len(pb.Proof), MaxProofDepth)
	}
	if len(pb.Data) > MaxWireSegmentSize {
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
		Version:     pd.Version[0],
		HashID:      HashID(pd.HashId[0]),
		Encoding:    Encoding(pd.Encoding[0]),
		Count:       pd.Count,
		SegmentSize: pd.SegmentSize,
		TotalLength: pd.TotalLength,
		Root:        bytes.Clone(pd.Root),
	}
	return &SegmentMessage{
		Slot:            pb.Slot,
		BeaconBlockRoot: bytes.Clone(pb.BeaconBlockRoot),
		Group:           bytes.Clone(pb.Group),
		Descriptor:      d,
		Index:           pb.Index,
		Proof:           proof,
		Data:            bytes.Clone(pb.Data),
	}, h, nil
}

// Verify checks the descriptor, the segment length and the Merkle proof. The group key is
// VerifyGroupKey's: a segment cut by Build has none until its publisher sets it.
//
// It does NOT establish that the descriptor is legitimate: deciding whether this group id is
// one the chain committed to is the caller's job, because only the caller knows which bids it
// has accepted.
func (m *SegmentMessage) Verify(h Hasher) error {
	return VerifySegment(m.Descriptor, h, int(m.Index), m.Data, m.Proof)
}

// VerifyGroupKey checks that the segment's group key is its descriptor's group id at its slot
// (seacast-lite.md §3: group_key(group_id(d), slot) == f.group).
func (m *SegmentMessage) VerifyGroupKey(h Hasher) error {
	if m.Descriptor == nil {
		return fmt.Errorf("%w: nil descriptor", ErrDescriptorMismatch)
	}
	if !bytes.Equal(m.Group, GroupKey(m.Descriptor.GroupID(h), m.Slot)) {
		return fmt.Errorf("%w: the group key is not the descriptor's at slot %d", ErrGroupKey, m.Slot)
	}
	return nil
}

// Layout is how a message is cut and committed: the segment size, the hash, the encoding
// the message bytes are already in, and the parity segments to add, zero for a plain group.
type Layout struct {
	SegmentSize int
	Hasher      Hasher
	Encoding    Encoding
	Parity      int
}

// Build produces the wire messages of msg under the layout: a plain group when Parity is
// zero, a Reed-Solomon coded group otherwise.
func Build(msg []byte, l Layout) ([]*SegmentMessage, error) {
	if l.Hasher == nil {
		return nil, fmt.Errorf("%w: nil hasher", ErrDescriptorMismatch)
	}
	if l.Encoding > EncodingSnappy {
		return nil, fmt.Errorf("%w: %d", ErrEncoding, l.Encoding)
	}
	switch {
	case l.Parity < 0:
		return nil, fmt.Errorf("%w: parity %d", ErrCodedShape, l.Parity)
	case l.Parity > 0:
		return buildCoded(msg, l)
	default:
		return buildPlain(msg, l)
	}
}

// BuildSegmentMessages produces the wire messages for every segment of msg, as a plain group
// of raw bytes.
func BuildSegmentMessages(msg []byte, segmentSize int, h Hasher) ([]*SegmentMessage, error) {
	return Build(msg, Layout{SegmentSize: segmentSize, Hasher: h})
}

func buildPlain(msg []byte, l Layout) ([]*SegmentMessage, error) {
	d, segs, tree, err := commit(msg, l.SegmentSize, l.Hasher, l.Encoding)
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
