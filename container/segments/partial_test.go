package segments

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/OffchainLabs/prysm/v7/testing/require"
)

func TestBitmap(t *testing.T) {
	t.Run("set, clear and query", func(t *testing.T) {
		b, err := NewBitmap(17)
		require.NoError(t, err)
		require.Equal(t, uint32(17), b.Count())
		require.Equal(t, 0, b.Len())
		require.Equal(t, false, b.Full())

		require.NoError(t, b.Set(0))
		require.NoError(t, b.Set(16))
		require.Equal(t, true, b.Has(0))
		require.Equal(t, true, b.Has(16))
		require.Equal(t, false, b.Has(1))
		require.Equal(t, 2, b.Len())

		require.NoError(t, b.Clear(0))
		require.Equal(t, false, b.Has(0))
		require.Equal(t, 1, b.Len())
	})

	t.Run("out of range", func(t *testing.T) {
		b, err := NewBitmap(8)
		require.NoError(t, err)
		require.ErrorIs(t, b.Set(8), ErrIndexRange)
		require.ErrorIs(t, b.Clear(8), ErrIndexRange)
		// Probing out of range is not an error, it is simply absent.
		require.Equal(t, false, b.Has(8))
	})

	t.Run("nil is empty, not a panic", func(t *testing.T) {
		var b *Bitmap
		require.Equal(t, uint32(0), b.Count())
		require.Equal(t, 0, b.Len())
		require.Equal(t, false, b.Has(0))
		require.Equal(t, false, b.Full())
		require.Equal(t, 0, len(b.Missing()))
		require.Equal(t, 0, len(b.Indices()))
		require.ErrorIs(t, b.Set(0), ErrIndexRange)
	})

	t.Run("full", func(t *testing.T) {
		b, err := NewBitmap(3)
		require.NoError(t, err)
		for i := uint32(0); i < 3; i++ {
			require.NoError(t, b.Set(i))
		}
		require.Equal(t, true, b.Full())
		require.Equal(t, 0, len(b.Missing()))
	})

	t.Run("missing and indices", func(t *testing.T) {
		b, err := NewBitmap(5)
		require.NoError(t, err)
		require.NoError(t, b.Set(1))
		require.NoError(t, b.Set(3))
		require.DeepEqual(t, []uint32{1, 3}, b.Indices())
		require.DeepEqual(t, []uint32{0, 2, 4}, b.Missing())
	})

	t.Run("and not treats nil as holding nothing", func(t *testing.T) {
		mine, err := NewBitmap(4)
		require.NoError(t, err)
		require.NoError(t, mine.Set(0))
		require.NoError(t, mine.Set(2))

		theirs, err := NewBitmap(4)
		require.NoError(t, err)
		require.NoError(t, theirs.Set(2))

		require.DeepEqual(t, []uint32{0}, mine.AndNot(theirs))
		require.DeepEqual(t, []uint32{0, 2}, mine.AndNot(nil))
	})

	t.Run("clone is independent", func(t *testing.T) {
		b, err := NewBitmap(8)
		require.NoError(t, err)
		require.NoError(t, b.Set(3))
		c := b.Clone()
		require.NoError(t, c.Set(4))
		require.Equal(t, false, b.Has(4))
		require.Equal(t, true, c.Has(3))
	})

	t.Run("count bounds", func(t *testing.T) {
		_, err := NewBitmap(0)
		require.ErrorIs(t, err, ErrTooManySegments)
		_, err = NewBitmap(MaxSegments + 1)
		require.ErrorIs(t, err, ErrTooManySegments)
	})
}

func TestPartsMetadataRoundTrip(t *testing.T) {
	// Counts either side of a byte boundary, so the padding rules get exercised both when
	// there are spare bits in the final byte and when there are none.
	for _, count := range []uint32{1, 7, 8, 9, 255, 256, MaxSegments} {
		m, err := NewPartsMetadata(count)
		require.NoError(t, err)
		require.NoError(t, m.Available.Set(0))
		require.NoError(t, m.Available.Set(count-1))
		require.NoError(t, m.Requests.Set(count/2))

		enc, err := m.Marshal()
		require.NoError(t, err)
		back, err := UnmarshalPartsMetadata(enc)
		require.NoError(t, err)
		require.Equal(t, count, back.Count)
		require.Equal(t, true, back.Available.Has(0))
		require.Equal(t, true, back.Available.Has(count-1))
		require.Equal(t, true, back.Requests.Has(count/2))
		require.Equal(t, true, m.Equal(back))
	}
}

func TestPartsMetadataStrictDecode(t *testing.T) {
	valid := func(t *testing.T) []byte {
		m, err := NewPartsMetadata(9)
		require.NoError(t, err)
		require.NoError(t, m.Available.Set(8))
		enc, err := m.Marshal()
		require.NoError(t, err)
		// version(1) + count(4) + available(2) + requests(2)
		require.Equal(t, 9, len(enc))
		return enc
	}

	t.Run("short buffer", func(t *testing.T) {
		_, err := UnmarshalPartsMetadata([]byte{1, 0, 0})
		require.ErrorIs(t, err, ErrShortBuffer)
	})

	t.Run("wrong version", func(t *testing.T) {
		enc := valid(t)
		enc[0] = PartialVersionCoded + 1
		_, err := UnmarshalPartsMetadata(enc)
		require.ErrorIs(t, err, ErrPartialVersion)
	})

	t.Run("coded round trip", func(t *testing.T) {
		m, err := NewPartsMetadata(12)
		require.NoError(t, err)
		m.Required = 9
		require.NoError(t, m.Available.Set(10))
		require.NoError(t, m.Requests.Set(3))
		enc, err := m.Marshal()
		require.NoError(t, err)
		require.Equal(t, PartialVersionCoded, enc[0])
		back, err := UnmarshalPartsMetadata(enc)
		require.NoError(t, err)
		require.Equal(t, uint32(12), back.Count)
		require.Equal(t, uint32(9), back.Required)
		require.Equal(t, true, back.Available.Has(10))
		require.Equal(t, true, back.Requests.Has(3))
		require.Equal(t, true, m.Equal(back))
	})

	t.Run("coded encoding with nothing coded is refused", func(t *testing.T) {
		m, err := NewPartsMetadata(12)
		require.NoError(t, err)
		m.Required = 12
		enc, err := m.Marshal()
		require.NoError(t, err)
		require.Equal(t, PartialVersion, enc[0])
		// Hand-build the redundant coded form; the decoder must refuse it.
		coded := append([]byte{PartialVersionCoded}, enc[1:5]...)
		coded = append(coded, enc[1:5]...)
		coded = append(coded, enc[5:]...)
		_, err = UnmarshalPartsMetadata(coded)
		require.ErrorIs(t, err, ErrCountMismatch)
	})

	t.Run("coded required of zero is refused", func(t *testing.T) {
		m, err := NewPartsMetadata(12)
		require.NoError(t, err)
		m.Required = 9
		enc, err := m.Marshal()
		require.NoError(t, err)
		enc[5], enc[6], enc[7], enc[8] = 0, 0, 0, 0
		_, err = UnmarshalPartsMetadata(enc)
		require.ErrorIs(t, err, ErrCountMismatch)
	})

	t.Run("zero count", func(t *testing.T) {
		enc := valid(t)
		enc[1], enc[2], enc[3], enc[4] = 0, 0, 0, 0
		_, err := UnmarshalPartsMetadata(enc)
		require.ErrorIs(t, err, ErrTooManySegments)
	})

	t.Run("count beyond the maximum is refused before it is used", func(t *testing.T) {
		enc := valid(t)
		enc[1], enc[2], enc[3], enc[4] = 0xff, 0xff, 0xff, 0xff
		_, err := UnmarshalPartsMetadata(enc)
		require.ErrorIs(t, err, ErrTooManySegments)
	})

	t.Run("length disagrees with count", func(t *testing.T) {
		enc := valid(t)
		_, err := UnmarshalPartsMetadata(enc[:len(enc)-1])
		require.ErrorIs(t, err, ErrBitmapLength)
		_, err = UnmarshalPartsMetadata(append(enc, 0))
		require.ErrorIs(t, err, ErrBitmapLength)
	})

	t.Run("padding bits above the count are rejected", func(t *testing.T) {
		// Count 9 leaves seven spare bits in the second byte of each bitmap. Setting one
		// must fail, otherwise the encoding is not canonical.
		enc := valid(t)
		enc[6] |= 1 << 2
		_, err := UnmarshalPartsMetadata(enc)
		require.ErrorIs(t, err, ErrBitmapPadding)

		enc = valid(t)
		enc[8] |= 1 << 7
		_, err = UnmarshalPartsMetadata(enc)
		require.ErrorIs(t, err, ErrBitmapPadding)
	})
}

func TestPartsMetadataEqual(t *testing.T) {
	a, err := NewPartsMetadata(8)
	require.NoError(t, err)
	b, err := NewPartsMetadata(8)
	require.NoError(t, err)
	require.Equal(t, true, a.Equal(b))

	require.NoError(t, a.Available.Set(1))
	require.Equal(t, false, a.Equal(b))

	c, err := NewPartsMetadata(9)
	require.NoError(t, err)
	require.Equal(t, false, b.Equal(c))

	var nilMeta *PartsMetadata
	require.Equal(t, false, a.Equal(nilMeta))
	require.Equal(t, true, nilMeta.Equal(nil))
}

func TestPartialMessageRoundTrip(t *testing.T) {
	h, err := HasherByID(HashSHA256)
	require.NoError(t, err)
	built, err := BuildSegmentMessages(msgOfLen(5000), 1024, h)
	require.NoError(t, err)
	require.Equal(t, 5, len(built))

	t.Run("empty input encodes to nothing", func(t *testing.T) {
		enc, err := MarshalPartialMessage(nil)
		require.NoError(t, err)
		require.Equal(t, 0, len(enc))
	})

	t.Run("a subset survives the round trip", func(t *testing.T) {
		want := []*SegmentMessage{built[3], built[0], built[4]}
		enc, err := MarshalPartialMessage(want)
		require.NoError(t, err)
		back, hashers, err := UnmarshalPartialMessage(enc)
		require.NoError(t, err)
		require.Equal(t, len(want), len(back))
		require.Equal(t, len(want), len(hashers))
		for i, m := range back {
			require.Equal(t, want[i].Index, m.Index)
			require.DeepEqual(t, want[i].Data, m.Data)
			require.NoError(t, m.Verify(hashers[i]))
		}
	})

	t.Run("every segment in the part is verifiable", func(t *testing.T) {
		enc, err := MarshalPartialMessage(built)
		require.NoError(t, err)
		back, hashers, err := UnmarshalPartialMessage(enc)
		require.NoError(t, err)
		for i, m := range back {
			require.NoError(t, m.Verify(hashers[i]))
		}
	})
}

func TestPartialMessageStrictDecode(t *testing.T) {
	h, err := HasherByID(HashSHA256)
	require.NoError(t, err)
	built, err := BuildSegmentMessages(msgOfLen(2048), 1024, h)
	require.NoError(t, err)
	valid, err := MarshalPartialMessage(built)
	require.NoError(t, err)

	t.Run("short buffer", func(t *testing.T) {
		_, _, err := UnmarshalPartialMessage([]byte{1})
		require.ErrorIs(t, err, ErrShortBuffer)
	})

	t.Run("wrong version", func(t *testing.T) {
		enc := append([]byte(nil), valid...)
		enc[0] = PartialVersion + 1
		_, _, err := UnmarshalPartialMessage(enc)
		require.ErrorIs(t, err, ErrPartialVersion)
	})

	t.Run("zero segments", func(t *testing.T) {
		_, _, err := UnmarshalPartialMessage([]byte{PartialVersion, 0, 0})
		require.ErrorIs(t, err, ErrShortBuffer)
	})

	t.Run("truncated body", func(t *testing.T) {
		_, _, err := UnmarshalPartialMessage(valid[:len(valid)-1])
		require.ErrorIs(t, err, ErrShortBuffer)
	})

	t.Run("trailing bytes", func(t *testing.T) {
		_, _, err := UnmarshalPartialMessage(append(append([]byte(nil), valid...), 0))
		require.ErrorIs(t, err, ErrTrailingBytes)
	})

	t.Run("declared length beyond the segment bound", func(t *testing.T) {
		enc := append([]byte(nil), valid...)
		// The first segment's length prefix sits right after version and count.
		enc[3], enc[4], enc[5], enc[6] = 0xff, 0xff, 0xff, 0xff
		_, _, err := UnmarshalPartialMessage(enc)
		require.ErrorIs(t, err, ErrSegmentSize)
	})
}

func TestPartialMessageCompressed(t *testing.T) {
	h, err := HasherByID(HashSHA256)
	require.NoError(t, err)
	// Low-entropy payload so the compressed part is measurably smaller than the raw one.
	built, err := BuildSegmentMessages(bytes.Repeat([]byte("segment payload "), 512), 2048, h)
	require.NoError(t, err)
	require.Equal(t, 4, len(built))

	raw, err := MarshalPartialMessage(built)
	require.NoError(t, err)
	enc, err := MarshalPartialMessageCompressed(built)
	require.NoError(t, err)
	require.Equal(t, PartialVersionCompressed, enc[0])
	if len(enc) >= len(raw) {
		t.Fatalf("compressed part %d bytes is not smaller than raw %d bytes", len(enc), len(raw))
	}

	t.Run("round trip verifies every segment", func(t *testing.T) {
		back, hashers, err := UnmarshalPartialMessage(enc)
		require.NoError(t, err)
		require.Equal(t, len(built), len(back))
		for i, m := range back {
			require.Equal(t, built[i].Index, m.Index)
			require.DeepEqual(t, built[i].Data, m.Data)
			require.NoError(t, m.Verify(hashers[i]))
		}
	})

	t.Run("a raw part still decodes", func(t *testing.T) {
		back, _, err := UnmarshalPartialMessage(raw)
		require.NoError(t, err)
		require.Equal(t, len(built), len(back))
	})

	t.Run("corrupt compressed body is rejected", func(t *testing.T) {
		bad := append([]byte(nil), enc...)
		// The first segment's body starts after version, count and its length prefix.
		bad[partialMessageFixedLen+partialSegmentLenLen] ^= 0xff
		_, _, err := UnmarshalPartialMessage(bad)
		require.NotNil(t, err)
	})

	t.Run("declared decoded length beyond the segment bound is rejected", func(t *testing.T) {
		// A snappy block whose varint header promises more than any segment may hold.
		huge := make([]byte, 0, 16)
		huge = binary.AppendUvarint(huge, uint64(MaxSegmentMessageSize)+1)
		part := []byte{PartialVersionCompressed, 1, 0}
		part = binary.LittleEndian.AppendUint32(part, uint32(len(huge)))
		part = append(part, huge...)
		_, _, err := UnmarshalPartialMessage(part)
		require.ErrorIs(t, err, ErrSegmentSize)
	})
}
