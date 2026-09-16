package segmentauth

import (
	"testing"

	"github.com/OffchainLabs/prysm/v7/consensus-types/primitives"
	"github.com/OffchainLabs/prysm/v7/container/segments"
	"github.com/OffchainLabs/prysm/v7/testing/require"
)

const testSlot = primitives.Slot(4096)

// groupFor segments a message and returns its descriptor, group id and hasher.
func groupFor(t *testing.T, msg []byte) (*segments.Descriptor, []byte, segments.Hasher) {
	t.Helper()
	h, err := segments.HasherByID(segments.HashSHA256)
	require.NoError(t, err)
	d, _, err := segments.Commit(msg, 64, h)
	require.NoError(t, err)
	return d, d.GroupID(h), h
}

func fixedSlot(s primitives.Slot) CurrentSlot { return func() primitives.Slot { return s } }

// TestAuthenticateIsMembership is the whole scheme: a group is authentic when the chain
// committed to it, and the sender asserts nothing.
func TestAuthenticateIsMembership(t *testing.T) {
	d, id, _ := groupFor(t, make([]byte, 500))
	other, otherID, _ := groupFor(t, make([]byte, 700))
	require.Equal(t, false, string(id) == string(otherID))

	committed := func(g []byte) bool { return string(g) == string(id) }

	t.Run("a committed group authenticates", func(t *testing.T) {
		require.NoError(t, New(committed).AuthenticateDescriptor(d, id))
	})

	t.Run("an uncommitted group does not", func(t *testing.T) {
		require.ErrorIs(t, New(committed).AuthenticateDescriptor(other, otherID), ErrNotCommitted)
	})

	t.Run("an empty commitment set admits nothing", func(t *testing.T) {
		err := New(func([]byte) bool { return false }).AuthenticateDescriptor(d, id)
		require.ErrorIs(t, err, ErrNotCommitted)
	})
}

// TestFirstSeenAdmitsOneGroupPerSlot covers the interim authority's only guarantee, and the
// property that makes it safe to run at all: the key is our own clock, so a peer cannot
// choose it, flood it, or grow the table.
func TestFirstSeenAdmitsOneGroupPerSlot(t *testing.T) {
	d1, id1, _ := groupFor(t, make([]byte, 500))
	d2, id2, _ := groupFor(t, make([]byte, 700))

	t.Run("the first group is admitted and repeats are idempotent", func(t *testing.T) {
		f := NewFirstSeen(fixedSlot(testSlot))
		require.NoError(t, f.AuthenticateDescriptor(d1, id1))
		require.NoError(t, f.AuthenticateDescriptor(d1, id1))
		require.Equal(t, 1, f.Len())
	})

	t.Run("a second group in the same slot is refused", func(t *testing.T) {
		f := NewFirstSeen(fixedSlot(testSlot))
		require.NoError(t, f.AuthenticateDescriptor(d1, id1))
		require.ErrorIs(t, f.AuthenticateDescriptor(d2, id2), ErrFirstSeenConflict)
		require.Equal(t, 1, f.Len(), "a refused group must not occupy an entry")
	})

	t.Run("a peer cannot grow the table however much it sends", func(t *testing.T) {
		// The per-root scheme this replaced took its key from the wire, so a peer opened an
		// entry per root it named. Keyed on our clock, 5000 distinct groups hold one entry.
		f := NewFirstSeen(fixedSlot(testSlot))
		for i := 0; i < 5000; i++ {
			d, id, _ := groupFor(t, []byte{byte(i), byte(i >> 8)})
			_ = f.AuthenticateDescriptor(d, id)
		}
		require.Equal(t, 1, f.Len())
	})

	t.Run("the table stays bounded as slots advance", func(t *testing.T) {
		current := testSlot
		f := NewFirstSeen(func() primitives.Slot { return current })
		for i := 0; i < 50; i++ {
			current = testSlot + primitives.Slot(i)
			d, id, _ := groupFor(t, []byte{byte(i)})
			require.NoError(t, f.AuthenticateDescriptor(d, id))
			require.Equal(t, true, f.Len() <= int(SlotRetention)+1,
				"at most SlotRetention+1 slots should be retained")
		}
	})

	t.Run("a new slot is a fresh decision", func(t *testing.T) {
		current := testSlot
		f := NewFirstSeen(func() primitives.Slot { return current })
		require.NoError(t, f.AuthenticateDescriptor(d1, id1))
		require.ErrorIs(t, f.AuthenticateDescriptor(d2, id2), ErrFirstSeenConflict)
		current = testSlot + 1
		require.NoError(t, f.AuthenticateDescriptor(d2, id2))
	})
}

// TestEndToEndWithReassembler drives the receive path: wire bytes in, authenticated group out,
// original message rebuilt.
func TestEndToEndWithReassembler(t *testing.T) {
	msg := make([]byte, 1000)
	for i := range msg {
		msg[i] = byte(i * 7)
	}
	_, groupID, h := groupFor(t, msg)

	built, err := segments.BuildSegmentMessages(msg, 64, h)
	require.NoError(t, err)

	auth := New(func(g []byte) bool { return string(g) == string(groupID) })
	r, err := segments.NewReassembler(segments.ReassemblerConfig{Auth: auth})
	require.NoError(t, err)

	var out []byte
	for _, m := range built {
		pb, err := m.ToProto()
		require.NoError(t, err)
		decoded, hasher, err := segments.FromProto(pb)
		require.NoError(t, err)
		got, err := r.Add(hasher, decoded)
		require.NoError(t, err)
		if got != nil {
			out = got
		}
	}
	require.DeepEqual(t, msg, out)

	t.Run("an uncommitted group is not admitted", func(t *testing.T) {
		r2, err := segments.NewReassembler(segments.ReassemblerConfig{Auth: auth})
		require.NoError(t, err)
		stray, err := segments.BuildSegmentMessages(append(append([]byte{}, msg...), 0xff), 64, h)
		require.NoError(t, err)
		_, err = r2.Add(h, stray[0])
		require.ErrorIs(t, err, segments.ErrUnauthenticatedDescriptor)
	})
}
