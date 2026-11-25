package segments

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"testing"

	"github.com/OffchainLabs/prysm/v7/testing/require"
)

// testSlot and testBlockRoot are the context the tests give segments.
const testSlot = 7

var testBlockRoot = bytes.Repeat([]byte{0xbb}, 32)

// withTestContext gives segments cut by Build the context a publisher gives them.
func withTestContext(t *testing.T, msgs []*SegmentMessage, h Hasher) {
	t.Helper()
	require.NoError(t, WithContext(msgs, h, testSlot, testBlockRoot))
}

// TestGroupKey checks GroupKey against seacast-lite.md §1's formula written out byte by byte:
// sha256(0x03 || gid || uint64_le(slot)).
func TestGroupKey(t *testing.T) {
	gid := bytes.Repeat([]byte{0x11}, 32)
	pre := append([]byte{0x03}, gid...)
	pre = append(pre, 0x2a, 0, 0, 0, 0, 0, 0, 0) // slot 42, little-endian
	want := sha256.Sum256(pre)
	require.DeepEqual(t, want[:], GroupKey(gid, 42))
	require.Equal(t, false, bytes.Equal(GroupKey(gid, 42), GroupKey(gid, 43)), "the key binds the slot")
}

// TestChunkLayout pins the wire form to seacast-lite.md §2's Chunk: slot, beacon_block_root, group,
// the 51-byte descriptor, index, then the offsets of proof and data, the proof and the data. The
// group id is sha256(0x02 || the descriptor's 51 bytes) (§1).
func TestChunkLayout(t *testing.T) {
	h, err := HasherByID(HashSHA256)
	require.NoError(t, err)
	msgs, err := Build(msgOfLen(700), Layout{SegmentSize: 64, Hasher: h, Encoding: EncodingSnappy, Parity: 3})
	require.NoError(t, err)
	withTestContext(t, msgs, h)
	m := msgs[2]
	pb, err := m.ToProto()
	require.NoError(t, err)
	enc, err := pb.MarshalSSZ()
	require.NoError(t, err)

	d := m.Descriptor
	desc := make([]byte, 0, 51)
	desc = append(desc, d.Version, byte(d.HashID), byte(d.Encoding))
	desc = binary.LittleEndian.AppendUint32(desc, d.Count)
	desc = binary.LittleEndian.AppendUint32(desc, d.SegmentSize)
	desc = binary.LittleEndian.AppendUint64(desc, d.TotalLength)
	desc = append(desc, d.Root...)
	require.Equal(t, 51, len(desc))
	gid := sha256.Sum256(append([]byte{0x02}, desc...))
	require.DeepEqual(t, gid[:], d.GroupID(h))

	require.DeepEqual(t, binary.LittleEndian.AppendUint64(nil, testSlot), enc[0:8])
	require.DeepEqual(t, testBlockRoot, enc[8:40])
	require.DeepEqual(t, GroupKey(gid[:], testSlot), enc[40:72])
	require.DeepEqual(t, desc, enc[72:123])
	require.Equal(t, m.Index, binary.LittleEndian.Uint32(enc[123:127]))
	require.Equal(t, uint32(135), binary.LittleEndian.Uint32(enc[127:131]))
	dataAt := 135 + 32*len(m.Proof)
	require.Equal(t, uint32(dataAt), binary.LittleEndian.Uint32(enc[131:135]))
	for i, p := range m.Proof {
		require.DeepEqual(t, p, enc[135+32*i:135+32*(i+1)])
	}
	require.DeepEqual(t, m.Data, enc[dataAt:])
}

// TestVerifyGroupKey: the key binds every field of the descriptor and the slot.
func TestVerifyGroupKey(t *testing.T) {
	h, err := HasherByID(HashSHA256)
	require.NoError(t, err)
	msgs, err := Build(msgOfLen(500), Layout{SegmentSize: 64, Hasher: h})
	require.NoError(t, err)
	withTestContext(t, msgs, h)
	require.NoError(t, msgs[0].VerifyGroupKey(h))

	for _, tc := range []struct {
		name string
		f    func(m *SegmentMessage)
	}{
		{"another slot", func(m *SegmentMessage) { m.Slot++ }},
		{"another total length", func(m *SegmentMessage) { d := *m.Descriptor; d.TotalLength--; m.Descriptor = &d }},
		{"another encoding", func(m *SegmentMessage) { d := *m.Descriptor; d.Encoding = EncodingSnappy; m.Descriptor = &d }},
		{"another key", func(m *SegmentMessage) { m.Group = bytes.Repeat([]byte{1}, 32) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := *msgs[0]
			tc.f(&m)
			require.ErrorIs(t, m.VerifyGroupKey(h), ErrGroupKey)
		})
	}

	t.Run("WithContext takes a 32-byte block root only", func(t *testing.T) {
		require.ErrorIs(t, WithContext(msgs, h, 1, make([]byte, 31)), ErrWireField)
	})
}
