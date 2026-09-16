package segments

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/OffchainLabs/prysm/v7/math"
)

// Version marks a plain group: the message split into Count segments, every one of them
// needed. VersionCoded, in rs.go, marks a Reed-Solomon coded group. Bump on any layout change.
const Version uint8 = 1

// Encoding names what was done to the message before it was segmented, so the receiver
// knows how to read the reassembled bytes. The library carries it in the descriptor,
// committed with the rest, and does not act on it: the caller that segmented the bytes is
// the one that encoded them.
type Encoding uint8

const (
	// EncodingRaw means the reassembled bytes are the message.
	EncodingRaw Encoding = 0
	// EncodingSnappy means the reassembled bytes are the snappy block compression of the
	// message. Compressing first makes every segment dense on the wire and lets the group
	// carry fewer of them.
	EncodingSnappy Encoding = 1
)

// Bounds on segmentation, enforced on both the producing and consuming side so a peer
// cannot make us allocate an unbounded number of segments or an oversized buffer.
const (
	// MaxSegments caps the segment count.
	MaxSegments = 16384
	// MaxSegmentSize caps a single segment's length in bytes.
	MaxSegmentSize = 1 << 20
	// MaxTotalLength caps the reassembled message length in bytes.
	MaxTotalLength = 256 << 20
)

var (
	// ErrEmptyMessage is returned when segmenting zero bytes.
	ErrEmptyMessage = errors.New("cannot segment an empty message")
	// ErrSegmentSize is returned for a non-positive or oversized segment size.
	ErrSegmentSize = errors.New("invalid segment size")
	// ErrTooManySegments is returned when the segment count exceeds MaxSegments.
	ErrTooManySegments = errors.New("too many segments")
	// ErrTotalLength is returned for a message longer than MaxTotalLength.
	ErrTotalLength = errors.New("message too long")
	// ErrDescriptorMismatch is returned when a descriptor is internally inconsistent.
	ErrDescriptorMismatch = errors.New("descriptor is inconsistent")
	// ErrSegmentLength is returned when a segment's length disagrees with the descriptor.
	ErrSegmentLength = errors.New("segment length disagrees with descriptor")
	// ErrIncompleteSegments is returned when reassembly is missing a segment.
	ErrIncompleteSegments = errors.New("missing one or more segments")
	// ErrEncoding is returned for a content encoding this library does not know.
	ErrEncoding = errors.New("unknown content encoding")
)

// Descriptor is the authenticated header for a segmented message.
//
// Every field that affects reassembly lives here, so pinning the first descriptor seen for
// a group is enough to reject peers that later contradict it. Authenticating the descriptor
// is the caller's responsibility: a Merkle root alone proves only internal consistency.
//
// Count is the number of segments on the wire. For a plain group that is also the number
// needed to reassemble the message; for a coded group Required() of them are, and the rest
// are parity.
type Descriptor struct {
	Version     uint8
	HashID      HashID
	Encoding    Encoding
	Count       uint32
	SegmentSize uint32
	TotalLength uint64
	Root        []byte
}

// descriptorFixedLen is the canonical encoded length before the variable-length root.
const descriptorFixedLen = 1 + 1 + 1 + 4 + 4 + 8

// Commit splits msg into segments of segmentSize and builds the commitment over them, as a
// plain group of raw bytes.
func Commit(msg []byte, segmentSize int, h Hasher) (*Descriptor, [][]byte, error) {
	d, segs, _, err := commit(msg, segmentSize, h, EncodingRaw)
	return d, segs, err
}

// commit is Commit keeping the tree, so a caller that also needs proofs builds it once.
func commit(msg []byte, segmentSize int, h Hasher, enc Encoding) (*Descriptor, [][]byte, *Tree, error) {
	segs, err := Split(msg, segmentSize)
	if err != nil {
		return nil, nil, nil, err
	}
	tree, err := BuildTree(h, segs)
	if err != nil {
		return nil, nil, nil, err
	}
	d := &Descriptor{
		Version:     Version,
		HashID:      h.ID(),
		Encoding:    enc,
		Count:       uint32(len(segs)),
		SegmentSize: uint32(segmentSize),
		TotalLength: uint64(len(msg)),
		Root:        tree.Root(),
	}
	return d, segs, tree, nil
}

// Split chops msg into segments of segmentSize; the final segment may be shorter.
func Split(msg []byte, segmentSize int) ([][]byte, error) {
	if len(msg) == 0 {
		return nil, ErrEmptyMessage
	}
	if len(msg) > MaxTotalLength {
		return nil, fmt.Errorf("%w: %d bytes", ErrTotalLength, len(msg))
	}
	if segmentSize <= 0 || segmentSize > MaxSegmentSize {
		return nil, fmt.Errorf("%w: %d", ErrSegmentSize, segmentSize)
	}
	count := (len(msg) + segmentSize - 1) / segmentSize
	if count > MaxSegments {
		return nil, fmt.Errorf("%w: %d", ErrTooManySegments, count)
	}
	segs := make([][]byte, count)
	for i := range segs {
		start := i * segmentSize
		end := min(start+segmentSize, len(msg))
		segs[i] = msg[start:end]
	}
	return segs, nil
}

// Required returns the number of segments needed to reassemble the message: every one of
// them for a plain group, the systematic count for a coded one.
func (d *Descriptor) Required() uint32 {
	if d.Version == VersionCoded {
		return uint32((d.TotalLength + uint64(d.SegmentSize) - 1) / uint64(d.SegmentSize))
	}
	return d.Count
}

// Validate checks that a descriptor is self-consistent and within bounds.
func (d *Descriptor) Validate(h Hasher) error {
	if d.Version != Version && d.Version != VersionCoded {
		return fmt.Errorf("%w: version %d, want %d or %d", ErrDescriptorMismatch, d.Version, Version, VersionCoded)
	}
	if d.HashID != h.ID() {
		return fmt.Errorf("%w: hash id %d, hasher %d", ErrDescriptorMismatch, d.HashID, h.ID())
	}
	if d.Encoding > EncodingSnappy {
		return fmt.Errorf("%w: %d", ErrEncoding, d.Encoding)
	}
	if len(d.Root) != h.Size() {
		return fmt.Errorf("%w: root %d bytes, want %d", ErrDescriptorMismatch, len(d.Root), h.Size())
	}
	if d.Count == 0 || d.Count > MaxSegments {
		return fmt.Errorf("%w: count %d", ErrTooManySegments, d.Count)
	}
	if d.SegmentSize == 0 || d.SegmentSize > MaxSegmentSize {
		return fmt.Errorf("%w: %d", ErrSegmentSize, d.SegmentSize)
	}
	if d.TotalLength == 0 || d.TotalLength > MaxTotalLength {
		return fmt.Errorf("%w: %d", ErrTotalLength, d.TotalLength)
	}
	// The count must be exactly the count implied by the length and segment size, so a peer
	// cannot pad the tree with extra leaves or truncate it. A coded group's count exceeds the
	// implied count by its parity, bounded by the code's field size instead.
	want := (d.TotalLength + uint64(d.SegmentSize) - 1) / uint64(d.SegmentSize)
	if d.Version == VersionCoded {
		if uint64(d.Count) <= want || d.Count > MaxCodedSegments {
			return fmt.Errorf("%w: coded count %d, implied %d, max %d", ErrDescriptorMismatch, d.Count, want, MaxCodedSegments)
		}
		return nil
	}
	if uint64(d.Count) != want {
		return fmt.Errorf("%w: count %d, implied %d", ErrDescriptorMismatch, d.Count, want)
	}
	return nil
}

// SegmentLength returns the expected byte length of the segment at index. Only the final
// systematic segment may be short; a coded group's parity segments are always full length.
func (d *Descriptor) SegmentLength(index int) (int, error) {
	if index < 0 || uint32(index) >= d.Count {
		return 0, fmt.Errorf("%w: %d not in [0,%d)", ErrIndexOutOfRange, index, d.Count)
	}
	if uint32(index) == d.Required()-1 {
		last := d.TotalLength - uint64(d.Required()-1)*uint64(d.SegmentSize)
		// Checked conversion: TotalLength is attacker-influenced and int is 32-bit on some
		// platforms, so a raw cast could wrap.
		n, err := math.Int(last)
		if err != nil {
			return 0, fmt.Errorf("%w: final segment length %d: %w", ErrDescriptorMismatch, last, err)
		}
		return n, nil
	}
	return int(d.SegmentSize), nil
}

// MarshalCanonical encodes the descriptor in a fixed little-endian layout. It is the group
// id's hash preimage and the descriptor-equality key, not a wire format: on the wire the
// descriptor travels as SSZ inside ethpb.ExecutionPayloadSegment.
func (d *Descriptor) MarshalCanonical() []byte {
	out := make([]byte, descriptorFixedLen+len(d.Root))
	out[0] = d.Version
	out[1] = byte(d.HashID)
	out[2] = byte(d.Encoding)
	binary.LittleEndian.PutUint32(out[3:7], d.Count)
	binary.LittleEndian.PutUint32(out[7:11], d.SegmentSize)
	binary.LittleEndian.PutUint64(out[11:19], d.TotalLength)
	copy(out[descriptorFixedLen:], d.Root)
	return out
}

// GroupID derives a stable identifier for the segmented message from the descriptor.
//
// Domain-separated from leaf and node hashing so a descriptor digest can never collide
// with a tree digest.
func (d *Descriptor) GroupID(h Hasher) []byte {
	canonical := d.MarshalCanonical()
	buf := make([]byte, 0, len(canonical)+1)
	buf = append(buf, descriptorPrefix)
	buf = append(buf, canonical...)
	return h.Hash(buf)
}

// VerifySegment checks a segment's length against the descriptor and its proof against the
// committed root. Both checks are required: a proof alone does not constrain length.
func VerifySegment(d *Descriptor, h Hasher, index int, seg []byte, proof [][]byte) error {
	if err := d.Validate(h); err != nil {
		return err
	}
	wantLen, err := d.SegmentLength(index)
	if err != nil {
		return err
	}
	if len(seg) != wantLen {
		return fmt.Errorf("%w: %d bytes at index %d, want %d", ErrSegmentLength, len(seg), index, wantLen)
	}
	return VerifyProof(h, d.Root, seg, index, int(d.Count), proof)
}

// Join reassembles the original message from a complete, ordered set of segments. A coded
// group is reassembled by RecoverAndVerify instead, from any Required of its segments.
func Join(d *Descriptor, h Hasher, segs [][]byte) ([]byte, error) {
	if err := d.Validate(h); err != nil {
		return nil, err
	}
	if d.Version == VersionCoded {
		return nil, fmt.Errorf("%w: coded group, use RecoverAndVerify", ErrDescriptorMismatch)
	}
	if uint32(len(segs)) != d.Count {
		return nil, fmt.Errorf("%w: have %d, want %d", ErrIncompleteSegments, len(segs), d.Count)
	}
	out := make([]byte, 0, d.TotalLength)
	for i, s := range segs {
		if s == nil {
			return nil, fmt.Errorf("%w: index %d", ErrIncompleteSegments, i)
		}
		wantLen, err := d.SegmentLength(i)
		if err != nil {
			return nil, err
		}
		if len(s) != wantLen {
			return nil, fmt.Errorf("%w: %d bytes at index %d, want %d", ErrSegmentLength, len(s), i, wantLen)
		}
		out = append(out, s...)
	}
	if uint64(len(out)) != d.TotalLength {
		return nil, fmt.Errorf("%w: reassembled %d bytes, want %d", ErrDescriptorMismatch, len(out), d.TotalLength)
	}
	return out, nil
}
