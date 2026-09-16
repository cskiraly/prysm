package segments

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/OffchainLabs/prysm/v7/math"
	"github.com/golang/snappy"
)

// Wire formats for carrying segments as gossipsub partial-message parts.
//
// Two byte strings travel per RPC and both are application-defined: PartsMetadata says
// which segments a peer holds and which it wants, and PartialMessage carries segments
// themselves. Neither is a consensus type. They are defined here, beside the segment codec,
// for the same reason SegmentMessage is: the format stays free to change without touching
// proto or SSZ, and everything that has to agree on a byte layout lives in one package.
//
// A deliberate omission: PartsMetadata does not carry the descriptor. A peer that has only
// ever seen metadata therefore cannot open a reassembly group, because opening one requires
// an authenticated descriptor and that only arrives with a segment. An announce is thus the
// cheapest thing a peer can send us -- it costs a bitmap, not a signature verification.

// Format version for the partial-message wire encodings. Bump on any layout change.
const PartialVersion uint8 = 1

// PartialVersionCoded marks metadata for a coded group, which additionally carries the
// number of segments required for reassembly. Used only when Required differs from Count,
// so every logical value has exactly one encoding.
const PartialVersionCoded uint8 = 2

// PartialVersionCompressed marks a PartialMessage whose segments are each snappy
// block-compressed, the way the gossip encoder compresses an ordinary message. Ordinary
// segment gossip therefore travels compressed while the partial path, which never passes
// through the topic encoder, travelled raw; this version closes that gap without changing
// the metadata format. A decoder accepts both.
const PartialVersionCompressed uint8 = 3

const (
	// partsMetadataFixedLen is the version byte plus the segment count.
	partsMetadataFixedLen = 1 + 4
	// partsMetadataCodedFixedLen additionally carries the required count.
	partsMetadataCodedFixedLen = 1 + 4 + 4
	// partialMessageFixedLen is the version byte plus the segment count.
	partialMessageFixedLen = 1 + 2
	// partialSegmentLenLen is the width of each segment's length prefix.
	partialSegmentLenLen = 4
)

// MaxPartialMessageSegments caps how many segments one PartialMessage may carry.
//
// The bound exists so a decode allocates a slice sized by a peer-supplied count only after
// the count is known sane. MaxSegments is the natural ceiling: a group never has more.
const MaxPartialMessageSegments = MaxSegments

var (
	// ErrPartialVersion is returned for an unrecognised partial-message format version.
	ErrPartialVersion = errors.New("unsupported partial message version")
	// ErrBitmapLength is returned when a bitmap's length disagrees with its count.
	ErrBitmapLength = errors.New("bitmap length disagrees with count")
	// ErrBitmapPadding is returned when a bitmap's unused trailing bits are set.
	ErrBitmapPadding = errors.New("bitmap has padding bits set")
	// ErrIndexRange is returned for a segment index outside the group.
	ErrIndexRange = errors.New("segment index out of range")
	// ErrCountMismatch is returned when two views of a group disagree on its segment count.
	ErrCountMismatch = errors.New("segment count mismatch")
)

// Bitmap is a fixed-width set of segment indices.
//
// The width is carried separately, in PartsMetadata.Count, rather than encoded with a
// sentinel bit. That keeps the decode strict in a way a sentinel cannot: every bit above
// the count must be zero, so there is exactly one encoding of any given set and a peer
// cannot smuggle bits past the end.
type Bitmap struct {
	count uint32
	bits  []byte
}

// NewBitmap returns an empty bitmap over count indices.
func NewBitmap(count uint32) (*Bitmap, error) {
	if count == 0 || count > MaxSegments {
		return nil, fmt.Errorf("%w: count %d", ErrTooManySegments, count)
	}
	return &Bitmap{count: count, bits: make([]byte, bitmapBytes(count))}, nil
}

// bitmapBytes is the byte width of a bitmap over count indices.
func bitmapBytes(count uint32) int {
	return int((count + 7) / 8)
}

// Count returns the number of indices the bitmap covers.
func (b *Bitmap) Count() uint32 {
	if b == nil {
		return 0
	}
	return b.count
}

// Has reports whether index is set. An out-of-range index is not set rather than an error,
// so callers can probe freely.
func (b *Bitmap) Has(index uint32) bool {
	if b == nil || index >= b.count {
		return false
	}
	return b.bits[index/8]&(1<<(index%8)) != 0
}

// Set marks index as present.
func (b *Bitmap) Set(index uint32) error {
	if b == nil || index >= b.count {
		return fmt.Errorf("%w: %d not in [0,%d)", ErrIndexRange, index, b.Count())
	}
	b.bits[index/8] |= 1 << (index % 8)
	return nil
}

// Clear unmarks index.
func (b *Bitmap) Clear(index uint32) error {
	if b == nil || index >= b.count {
		return fmt.Errorf("%w: %d not in [0,%d)", ErrIndexRange, index, b.Count())
	}
	b.bits[index/8] &^= 1 << (index % 8)
	return nil
}

// Len returns how many indices are set.
func (b *Bitmap) Len() int {
	if b == nil {
		return 0
	}
	var n int
	for i := uint32(0); i < b.count; i++ {
		if b.Has(i) {
			n++
		}
	}
	return n
}

// Full reports whether every index is set.
func (b *Bitmap) Full() bool {
	return b != nil && b.Len() == int(b.count)
}

// Clone returns an independent copy.
func (b *Bitmap) Clone() *Bitmap {
	if b == nil {
		return nil
	}
	out := &Bitmap{count: b.count, bits: make([]byte, len(b.bits))}
	copy(out.bits, b.bits)
	return out
}

// Missing yields the indices that are not set, in ascending order.
func (b *Bitmap) Missing() []uint32 {
	if b == nil {
		return nil
	}
	var out []uint32
	for i := uint32(0); i < b.count; i++ {
		if !b.Has(i) {
			out = append(out, i)
		}
	}
	return out
}

// Indices yields the indices that are set, in ascending order.
func (b *Bitmap) Indices() []uint32 {
	if b == nil {
		return nil
	}
	var out []uint32
	for i := uint32(0); i < b.count; i++ {
		if b.Has(i) {
			out = append(out, i)
		}
	}
	return out
}

// AndNot returns the indices set in b but not in other.
//
// A nil other means "the peer has told us nothing", which is different from "the peer has
// nothing": both yield everything we hold, but only because a peer we know nothing about is
// treated as holding nothing.
func (b *Bitmap) AndNot(other *Bitmap) []uint32 {
	if b == nil {
		return nil
	}
	var out []uint32
	for i := uint32(0); i < b.count; i++ {
		if b.Has(i) && !other.Has(i) {
			out = append(out, i)
		}
	}
	return out
}

// checkPadding reports an error if any bit at or above the count is set.
func (b *Bitmap) checkPadding() error {
	if len(b.bits) == 0 {
		return nil
	}
	// Only the final byte can hold padding, since the width is ceil(count/8).
	used := b.count % 8
	if used == 0 {
		return nil
	}
	if b.bits[len(b.bits)-1]>>used != 0 {
		return fmt.Errorf("%w: count %d", ErrBitmapPadding, b.count)
	}
	return nil
}

// PartsMetadata is a peer's view of one group: what it holds, and what it wants.
//
// Requests is carried explicitly rather than inferred as the complement of Available,
// because the two differ in the case that matters. A peer that holds nothing and wants
// nothing is a peer that is only forwarding to others; treating its empty Available as a
// request for everything would have every announce trigger a full push.
type PartsMetadata struct {
	// Count is the group's segment count, so a receiver can size bitmaps before it has
	// ever seen a descriptor.
	Count uint32
	// Required is how many segments reassemble the group: Count for a plain group, fewer
	// for a coded one. Carried so an announce-only receiver can bound its requests at what
	// it needs rather than at everything the group contains.
	Required uint32
	// Available is the set of segments the sender holds.
	Available *Bitmap
	// Requests is the set of segments the sender wants.
	Requests *Bitmap
}

// NewPartsMetadata returns empty metadata over count segments, all of them required.
func NewPartsMetadata(count uint32) (*PartsMetadata, error) {
	available, err := NewBitmap(count)
	if err != nil {
		return nil, err
	}
	requests, err := NewBitmap(count)
	if err != nil {
		return nil, err
	}
	return &PartsMetadata{Count: count, Required: count, Available: available, Requests: requests}, nil
}

// Clone returns an independent copy.
func (m *PartsMetadata) Clone() *PartsMetadata {
	if m == nil {
		return nil
	}
	return &PartsMetadata{
		Count:     m.Count,
		Required:  m.Required,
		Available: m.Available.Clone(),
		Requests:  m.Requests.Clone(),
	}
}

// Equal reports whether two metadata values encode the same thing. Used to suppress
// redundant sends, which the extension asks implementations to avoid.
func (m *PartsMetadata) Equal(other *PartsMetadata) bool {
	if m == nil || other == nil {
		return m == nil && other == nil
	}
	if m.Count != other.Count {
		return false
	}
	a, err := m.Marshal()
	if err != nil {
		return false
	}
	b, err := other.Marshal()
	if err != nil {
		return false
	}
	return string(a) == string(b)
}

// Marshal encodes the metadata as version || count || available || requests.
//
// The length is fixed by the count, so there are no length prefixes to disagree with the
// payload and nothing to bound at decode time beyond the count itself.
func (m *PartsMetadata) Marshal() ([]byte, error) {
	if m == nil {
		return nil, fmt.Errorf("%w: nil metadata", ErrBitmapLength)
	}
	if m.Count == 0 || m.Count > MaxSegments {
		return nil, fmt.Errorf("%w: count %d", ErrTooManySegments, m.Count)
	}
	width := bitmapBytes(m.Count)
	if m.Available.Count() != m.Count || m.Requests.Count() != m.Count {
		return nil, fmt.Errorf("%w: available %d, requests %d, count %d",
			ErrBitmapLength, m.Available.Count(), m.Requests.Count(), m.Count)
	}
	if m.Required == 0 || m.Required > m.Count {
		return nil, fmt.Errorf("%w: required %d of %d", ErrCountMismatch, m.Required, m.Count)
	}
	// A coded group gets the layout that carries Required; a plain group keeps the
	// Version 1 bytes, so the two never alias.
	if m.Required < m.Count {
		out := make([]byte, 0, partsMetadataCodedFixedLen+2*width)
		out = append(out, PartialVersionCoded)
		out = binary.LittleEndian.AppendUint32(out, m.Count)
		out = binary.LittleEndian.AppendUint32(out, m.Required)
		out = append(out, m.Available.bits...)
		out = append(out, m.Requests.bits...)
		return out, nil
	}
	out := make([]byte, 0, partsMetadataFixedLen+2*width)
	out = append(out, PartialVersion)
	out = binary.LittleEndian.AppendUint32(out, m.Count)
	out = append(out, m.Available.bits...)
	out = append(out, m.Requests.bits...)
	return out, nil
}

// UnmarshalPartsMetadata decodes metadata produced by Marshal.
//
// Strict: exact length, no trailing bytes, and no padding bits set above the count. The
// count bound is checked before the length so a huge count is refused without arithmetic on
// it, and the padding check makes the encoding canonical.
func UnmarshalPartsMetadata(b []byte) (*PartsMetadata, error) {
	if len(b) < partsMetadataFixedLen {
		return nil, fmt.Errorf("%w: %d bytes", ErrShortBuffer, len(b))
	}
	if b[0] != PartialVersion && b[0] != PartialVersionCoded {
		return nil, fmt.Errorf("%w: %d", ErrPartialVersion, b[0])
	}
	count := binary.LittleEndian.Uint32(b[1:5])
	if count == 0 || count > MaxSegments {
		return nil, fmt.Errorf("%w: count %d", ErrTooManySegments, count)
	}
	required := count
	fixed := partsMetadataFixedLen
	if b[0] == PartialVersionCoded {
		// A coded group is bounded by the codec's field, not by MaxSegments, and a coded
		// encoding where nothing is coded is refused so the encoding stays canonical.
		if count > MaxCodedSegments {
			return nil, fmt.Errorf("%w: coded count %d", ErrTooManySegments, count)
		}
		if len(b) < partsMetadataCodedFixedLen {
			return nil, fmt.Errorf("%w: %d bytes", ErrShortBuffer, len(b))
		}
		required = binary.LittleEndian.Uint32(b[5:9])
		if required == 0 || required >= count {
			return nil, fmt.Errorf("%w: required %d of %d", ErrCountMismatch, required, count)
		}
		fixed = partsMetadataCodedFixedLen
	}
	width := bitmapBytes(count)
	if len(b) != fixed+2*width {
		return nil, fmt.Errorf("%w: %d bytes, want %d", ErrBitmapLength, len(b), fixed+2*width)
	}
	available := &Bitmap{count: count, bits: make([]byte, width)}
	copy(available.bits, b[fixed:fixed+width])
	requests := &Bitmap{count: count, bits: make([]byte, width)}
	copy(requests.bits, b[fixed+width:])
	if err := available.checkPadding(); err != nil {
		return nil, err
	}
	if err := requests.checkPadding(); err != nil {
		return nil, err
	}
	return &PartsMetadata{Count: count, Required: required, Available: available, Requests: requests}, nil
}

// MarshalPartialMessage encodes segments as version || count || (length || segment)*.
//
// Several segments travel in one part deliberately: the whole point of the partial-message
// substrate is that a peer can ask for exactly the segments it lacks and receive them in
// one RPC, rather than one message per segment as ordinary gossip forces.
func MarshalPartialMessage(msgs []*SegmentMessage) ([]byte, error) {
	return marshalPartialMessage(msgs, false)
}

// MarshalPartialMessageCompressed is MarshalPartialMessage with every segment snappy
// block-compressed, under PartialVersionCompressed. Same layout otherwise; each length
// prefix counts compressed bytes.
func MarshalPartialMessageCompressed(msgs []*SegmentMessage) ([]byte, error) {
	return marshalPartialMessage(msgs, true)
}

func marshalPartialMessage(msgs []*SegmentMessage, compress bool) ([]byte, error) {
	if len(msgs) == 0 {
		return nil, nil
	}
	if len(msgs) > MaxPartialMessageSegments {
		return nil, fmt.Errorf("%w: %d segments", ErrTooManySegments, len(msgs))
	}
	encoded := make([][]byte, 0, len(msgs))
	total := partialMessageFixedLen
	for _, m := range msgs {
		enc, err := m.Marshal()
		if err != nil {
			return nil, err
		}
		if compress {
			enc = snappy.Encode(nil, enc)
		}
		encoded = append(encoded, enc)
		total += partialSegmentLenLen + len(enc)
	}
	out := make([]byte, 0, total)
	if compress {
		out = append(out, PartialVersionCompressed)
	} else {
		out = append(out, PartialVersion)
	}
	out = binary.LittleEndian.AppendUint16(out, uint16(len(msgs)))
	for _, enc := range encoded {
		out = binary.LittleEndian.AppendUint32(out, uint32(len(enc)))
		out = append(out, enc...)
	}
	return out, nil
}

// UnmarshalPartialMessage decodes a part produced by MarshalPartialMessage.
//
// Each segment is decoded with UnmarshalSegmentMessage, so every segment in the part gets
// the same strict treatment a gossip segment does. The returned hashers are per segment
// because a part may in principle mix hash IDs; the caller decides whether to allow that.
func UnmarshalPartialMessage(b []byte) ([]*SegmentMessage, []Hasher, error) {
	if len(b) < partialMessageFixedLen {
		return nil, nil, fmt.Errorf("%w: %d bytes", ErrShortBuffer, len(b))
	}
	compressed := b[0] == PartialVersionCompressed
	if b[0] != PartialVersion && !compressed {
		return nil, nil, fmt.Errorf("%w: %d", ErrPartialVersion, b[0])
	}
	// A compressed segment may exceed the raw bound by snappy's framing overhead; the
	// decoded length is checked against the raw bound before anything is allocated.
	maxWire := uint32(MaxSegmentMessageSize)
	if compressed {
		maxWire = uint32(snappy.MaxEncodedLen(MaxSegmentMessageSize))
	}
	// uint16 fits any int, so the width is not the concern here; the bound is.
	count, err := math.Int(uint64(binary.LittleEndian.Uint16(b[1:3])))
	if err != nil {
		return nil, nil, err
	}
	if count == 0 {
		return nil, nil, fmt.Errorf("%w: zero segments", ErrShortBuffer)
	}
	if count > MaxPartialMessageSegments {
		return nil, nil, fmt.Errorf("%w: %d segments", ErrTooManySegments, count)
	}
	msgs := make([]*SegmentMessage, 0, count)
	hashers := make([]Hasher, 0, count)
	off := partialMessageFixedLen
	for range count {
		if off+partialSegmentLenLen > len(b) {
			return nil, nil, fmt.Errorf("%w: truncated segment length", ErrShortBuffer)
		}
		n := binary.LittleEndian.Uint32(b[off : off+partialSegmentLenLen])
		off += partialSegmentLenLen
		if n > maxWire {
			return nil, nil, fmt.Errorf("%w: segment %d bytes", ErrSegmentSize, n)
		}
		size, err := math.Int(uint64(n))
		if err != nil {
			return nil, nil, err
		}
		end := off + size
		if end > len(b) {
			return nil, nil, fmt.Errorf("%w: truncated segment body", ErrShortBuffer)
		}
		body := b[off:end]
		if compressed {
			dl, err := snappy.DecodedLen(body)
			if err != nil {
				return nil, nil, fmt.Errorf("%w: %v", ErrSegmentSize, err)
			}
			if dl > MaxSegmentMessageSize {
				return nil, nil, fmt.Errorf("%w: segment decodes to %d bytes", ErrSegmentSize, dl)
			}
			body, err = snappy.Decode(nil, body)
			if err != nil {
				return nil, nil, fmt.Errorf("%w: %v", ErrSegmentSize, err)
			}
		}
		m, h, err := UnmarshalSegmentMessage(body)
		if err != nil {
			return nil, nil, err
		}
		msgs = append(msgs, m)
		hashers = append(hashers, h)
		off = end
	}
	if off != len(b) {
		return nil, nil, fmt.Errorf("%w: %d bytes left", ErrTrailingBytes, len(b)-off)
	}
	return msgs, hashers, nil
}
