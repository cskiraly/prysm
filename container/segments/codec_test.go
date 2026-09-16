package segments

import (
	"testing"

	"github.com/OffchainLabs/prysm/v7/testing/require"
)

func TestSegmentMessageRoundTrip(t *testing.T) {
	for _, id := range []HashID{HashSHA256, HashKeccak256} {
		h, err := HasherByID(id)
		require.NoError(t, err)
		for _, tc := range []struct {
			name        string
			msgLen      int
			segmentSize int
		}{
			{"single segment", 20, 64},
			{"many segments", 1000, 64},
			{"exact multiple", 256, 64},
			{"one byte segments", 5, 1},
		} {
			t.Run(tc.name, func(t *testing.T) {
				msgs, err := BuildSegmentMessages(msgOfLen(tc.msgLen), tc.segmentSize, h)
				require.NoError(t, err)
				for _, m := range msgs {
					enc, err := m.Marshal()
					require.NoError(t, err)
					got, gotHasher, err := UnmarshalSegmentMessage(enc)
					require.NoError(t, err)
					require.Equal(t, h.ID(), gotHasher.ID())
					require.DeepEqual(t, m.Descriptor, got.Descriptor)
					require.Equal(t, m.Index, got.Index)
					require.DeepEqual(t, m.Proof, got.Proof)
					require.DeepEqual(t, m.Data, got.Data)
					// Every decoded segment must verify against the committed root.
					require.NoError(t, got.Verify(gotHasher))
				}
			})
		}
	}
}

func TestUnmarshalSegmentMessageRejects(t *testing.T) {
	h, err := HasherByID(HashSHA256)
	require.NoError(t, err)
	msgs, err := BuildSegmentMessages(msgOfLen(500), 64, h)
	require.NoError(t, err)
	good, err := msgs[1].Marshal()
	require.NoError(t, err)

	t.Run("empty buffer", func(t *testing.T) {
		_, _, err := UnmarshalSegmentMessage(nil)
		require.ErrorIs(t, err, ErrShortBuffer)
	})
	t.Run("one byte buffer", func(t *testing.T) {
		_, _, err := UnmarshalSegmentMessage(good[:1])
		require.ErrorIs(t, err, ErrShortBuffer)
	})
	t.Run("unknown hash id", func(t *testing.T) {
		bad := make([]byte, len(good))
		copy(bad, good)
		bad[1] = 200
		_, _, err := UnmarshalSegmentMessage(bad)
		require.ErrorIs(t, err, ErrUnknownHashID)
	})
	t.Run("trailing bytes", func(t *testing.T) {
		_, _, err := UnmarshalSegmentMessage(append(append([]byte{}, good...), 0x00))
		require.ErrorIs(t, err, ErrTrailingBytes)
	})
	t.Run("truncated at every length", func(t *testing.T) {
		// Any prefix shorter than the whole message must fail, never partially decode.
		for cut := 2; cut < len(good); cut++ {
			_, _, err := UnmarshalSegmentMessage(good[:cut])
			require.Equal(t, true, err != nil, "prefix of %d bytes decoded", cut)
		}
	})
	t.Run("proof count above tree depth limit", func(t *testing.T) {
		bad := make([]byte, len(good))
		copy(bad, good)
		// Proof count sits right after the descriptor and index.
		off := descriptorFixedLen + h.Size() + indexLen
		bad[off] = byte(maxProofCount + 1)
		_, _, err := UnmarshalSegmentMessage(bad)
		require.ErrorIs(t, err, ErrProofCount)
	})
}

func TestMarshalRejects(t *testing.T) {
	h, err := HasherByID(HashSHA256)
	require.NoError(t, err)
	msgs, err := BuildSegmentMessages(msgOfLen(200), 64, h)
	require.NoError(t, err)

	t.Run("nil descriptor", func(t *testing.T) {
		m := &SegmentMessage{}
		_, err := m.Marshal()
		require.ErrorIs(t, err, ErrDescriptorMismatch)
	})
	t.Run("proof element of wrong size", func(t *testing.T) {
		m := *msgs[0]
		m.Proof = [][]byte{{1, 2, 3}}
		_, err := m.Marshal()
		require.ErrorIs(t, err, ErrProofDigestSize)
	})
}

func TestBuildSegmentMessagesRejects(t *testing.T) {
	h, err := HasherByID(HashSHA256)
	require.NoError(t, err)
	t.Run("empty message", func(t *testing.T) {
		_, err := BuildSegmentMessages(nil, 64, h)
		require.ErrorIs(t, err, ErrEmptyMessage)
	})
	t.Run("bad segment size", func(t *testing.T) {
		_, err := BuildSegmentMessages(msgOfLen(10), 0, h)
		require.ErrorIs(t, err, ErrSegmentSize)
	})
}
