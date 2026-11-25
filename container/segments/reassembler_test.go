package segments

import (
	"errors"
	"testing"
	"time"

	"github.com/OffchainLabs/prysm/v7/testing/require"
)

// fakeClock lets TTL behaviour be tested without sleeping.
type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time          { return c.t }
func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }

func newTestClock() *fakeClock {
	return &fakeClock{t: time.Unix(1600000000, 0)}
}

// newTestReassembler defaults to accept-all authentication so tests that are not about
// authentication stay readable.
func newTestReassembler(t *testing.T, cfg ReassemblerConfig) *Reassembler {
	if cfg.Auth == nil && !cfg.AllowUnauthenticated {
		cfg.Auth = UnsafeAcceptAllDescriptors{}
	}
	r, err := NewReassembler(cfg)
	require.NoError(t, err)
	return r
}

// countingAuth records calls and can be made to fail.
type countingAuth struct {
	calls int
	fail  error
}

func (c *countingAuth) AuthenticateDescriptor(_ *Descriptor, _ []byte) error {
	c.calls++
	return c.fail
}

func TestReassemblerCompletes(t *testing.T) {
	h, err := HasherByID(HashSHA256)
	require.NoError(t, err)
	msg := msgOfLen(1000)
	msgs, err := BuildSegmentMessages(msg, 64, h)
	require.NoError(t, err)

	t.Run("in order", func(t *testing.T) {
		r := newTestReassembler(t, ReassemblerConfig{})
		var out []byte
		for i, m := range msgs {
			got, err := r.Add(h, m)
			require.NoError(t, err)
			if i < len(msgs)-1 {
				require.Equal(t, true, got == nil, "completed early at %d", i)
			} else {
				out = got
			}
		}
		require.DeepEqual(t, msg, out)
		// Completing a group must release its budget; the entry stays, marked delivered,
		// so a late segment cannot reopen it.
		require.Equal(t, 0, r.Bytes())
		require.Equal(t, 1, r.Groups())
		require.Equal(t, true, r.Complete(msgs[0].Descriptor.GroupID(h)))
	})

	t.Run("delivered once", func(t *testing.T) {
		r := newTestReassembler(t, ReassemblerConfig{})
		for _, m := range msgs {
			_, err := r.Add(h, m)
			require.NoError(t, err)
		}
		// Every segment again, after delivery: verified, accepted, buffered nowhere, and the
		// message is not returned a second time.
		for _, m := range msgs {
			got, err := r.Add(h, m)
			require.NoError(t, err)
			require.Equal(t, true, got == nil)
		}
		require.Equal(t, 0, r.Bytes())
		require.Equal(t, 1, r.Groups())
		require.Equal(t, true, r.Has(msgs[0].Descriptor.GroupID(h)), "a delivered group is still open")
	})

	t.Run("reverse order", func(t *testing.T) {
		r := newTestReassembler(t, ReassemblerConfig{})
		var out []byte
		for i := len(msgs) - 1; i >= 0; i-- {
			got, err := r.Add(h, msgs[i])
			require.NoError(t, err)
			if got != nil {
				out = got
			}
		}
		require.DeepEqual(t, msg, out)
	})

	t.Run("duplicates are idempotent", func(t *testing.T) {
		r := newTestReassembler(t, ReassemblerConfig{})
		_, err := r.Add(h, msgs[0])
		require.NoError(t, err)
		afterFirst := r.Bytes()
		for range 5 {
			got, err := r.Add(h, msgs[0])
			require.NoError(t, err)
			require.Equal(t, true, got == nil)
		}
		require.Equal(t, afterFirst, r.Bytes())
		require.Equal(t, 1, r.Groups())
	})

	t.Run("two messages tracked independently", func(t *testing.T) {
		r := newTestReassembler(t, ReassemblerConfig{})
		other, err := BuildSegmentMessages(msgOfLen(300), 64, h)
		require.NoError(t, err)
		_, err = r.Add(h, msgs[0])
		require.NoError(t, err)
		_, err = r.Add(h, other[0])
		require.NoError(t, err)
		require.Equal(t, 2, r.Groups())
	})
}

func TestReassemblerRejectsUnverified(t *testing.T) {
	h, err := HasherByID(HashSHA256)
	require.NoError(t, err)
	msgs, err := BuildSegmentMessages(msgOfLen(500), 64, h)
	require.NoError(t, err)

	t.Run("bad proof consumes no budget", func(t *testing.T) {
		r := newTestReassembler(t, ReassemblerConfig{})
		bad := *msgs[0]
		bad.Data = append([]byte{}, msgs[0].Data...)
		bad.Data[0] ^= 0xff
		_, err := r.Add(h, &bad)
		require.ErrorIs(t, err, ErrRootMismatch)
		// Verification precedes buffering, so nothing was allocated for this peer.
		require.Equal(t, 0, r.Groups())
		require.Equal(t, 0, r.Bytes())
	})

	t.Run("wrong length consumes no budget", func(t *testing.T) {
		r := newTestReassembler(t, ReassemblerConfig{})
		bad := *msgs[0]
		bad.Data = msgs[0].Data[:8]
		_, err := r.Add(h, &bad)
		require.ErrorIs(t, err, ErrSegmentLength)
		require.Equal(t, 0, r.Bytes())
	})
}

func TestReassemblerBounds(t *testing.T) {
	h, err := HasherByID(HashSHA256)
	require.NoError(t, err)

	t.Run("group cap", func(t *testing.T) {
		r := newTestReassembler(t, ReassemblerConfig{MaxGroups: 2})
		for i := range 3 {
			// Distinct lengths give distinct descriptors, hence distinct groups.
			msgs, err := BuildSegmentMessages(msgOfLen(200+i*64), 64, h)
			require.NoError(t, err)
			_, err = r.Add(h, msgs[0])
			if i < 2 {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, ErrTooManyGroups)
			}
		}
		require.Equal(t, 2, r.Groups())
	})

	t.Run("byte budget", func(t *testing.T) {
		msgs, err := BuildSegmentMessages(msgOfLen(1000), 64, h)
		require.NoError(t, err)
		// Room for exactly one 64-byte segment.
		r := newTestReassembler(t, ReassemblerConfig{MaxBytes: 64})
		_, err = r.Add(h, msgs[0])
		require.NoError(t, err)
		_, err = r.Add(h, msgs[1])
		require.ErrorIs(t, err, ErrBufferFull)
		require.Equal(t, 64, r.Bytes())
	})
}

func TestReassemblerTTL(t *testing.T) {
	h, err := HasherByID(HashSHA256)
	require.NoError(t, err)
	msgs, err := BuildSegmentMessages(msgOfLen(500), 64, h)
	require.NoError(t, err)
	clock := newTestClock()
	r := newTestReassembler(t, ReassemblerConfig{TTL: time.Minute, Now: clock.now})

	_, err = r.Add(h, msgs[0])
	require.NoError(t, err)
	require.Equal(t, 1, r.Groups())

	t.Run("kept before the ttl", func(t *testing.T) {
		clock.advance(30 * time.Second)
		r.Prune()
		require.Equal(t, 1, r.Groups())
	})
	t.Run("evicted after the ttl", func(t *testing.T) {
		clock.advance(2 * time.Minute)
		r.Prune()
		require.Equal(t, 0, r.Groups())
		require.Equal(t, 0, r.Bytes())
	})
	t.Run("eviction frees budget for new groups", func(t *testing.T) {
		_, err := r.Add(h, msgs[0])
		require.NoError(t, err)
		require.Equal(t, 1, r.Groups())
	})
}

func TestReassemblerDrop(t *testing.T) {
	h, err := HasherByID(HashSHA256)
	require.NoError(t, err)
	msgs, err := BuildSegmentMessages(msgOfLen(500), 64, h)
	require.NoError(t, err)
	r := newTestReassembler(t, ReassemblerConfig{})
	_, err = r.Add(h, msgs[0])
	require.NoError(t, err)
	require.Equal(t, 1, r.Groups())

	r.Drop(msgs[0].Descriptor.GroupID(h))
	require.Equal(t, 0, r.Groups())
	require.Equal(t, 0, r.Bytes())

	t.Run("dropping an unknown group is a no-op", func(t *testing.T) {
		r.Drop([]byte("not a group"))
		require.Equal(t, 0, r.Groups())
	})
}

func TestReassemblerAuthentication(t *testing.T) {
	h, err := HasherByID(HashSHA256)
	require.NoError(t, err)
	msgs, err := BuildSegmentMessages(msgOfLen(500), 64, h)
	require.NoError(t, err)

	t.Run("constructor refuses to run unauthenticated by default", func(t *testing.T) {
		_, err := NewReassembler(ReassemblerConfig{})
		require.ErrorIs(t, err, ErrNoAuthenticator)
	})
	t.Run("opting out must be explicit", func(t *testing.T) {
		r, err := NewReassembler(ReassemblerConfig{AllowUnauthenticated: true})
		require.NoError(t, err)
		_, err = r.Add(h, msgs[0])
		require.NoError(t, err)
	})
	t.Run("authenticated once per group, not per segment", func(t *testing.T) {
		auth := &countingAuth{}
		r := newTestReassembler(t, ReassemblerConfig{Auth: auth})
		for _, m := range msgs {
			_, err := r.Add(h, m)
			require.NoError(t, err)
		}
		require.Equal(t, 1, auth.calls)
	})
	t.Run("failure rejects the group and consumes no budget", func(t *testing.T) {
		auth := &countingAuth{fail: errors.New("bad signature")}
		r := newTestReassembler(t, ReassemblerConfig{Auth: auth})
		_, err := r.Add(h, msgs[0])
		require.ErrorIs(t, err, ErrUnauthenticatedDescriptor)
		require.Equal(t, 0, r.Groups())
		require.Equal(t, 0, r.Bytes())
	})
	t.Run("failure is retried on the next segment of the same group", func(t *testing.T) {
		// Nothing is pinned for a rejected group, so a later segment carrying valid auth
		// material can still open it.
		auth := &countingAuth{fail: errors.New("bad signature")}
		r := newTestReassembler(t, ReassemblerConfig{Auth: auth})
		_, err := r.Add(h, msgs[0])
		require.ErrorIs(t, err, ErrUnauthenticatedDescriptor)
		auth.fail = nil
		_, err = r.Add(h, msgs[1])
		require.NoError(t, err)
		require.Equal(t, 1, r.Groups())
	})
}

func TestReassemblerHas(t *testing.T) {
	h, err := HasherByID(HashSHA256)
	require.NoError(t, err)
	msgs, err := BuildSegmentMessages(msgOfLen(500), 64, h)
	require.NoError(t, err)
	groupID := msgs[0].Descriptor.GroupID(h)
	r := newTestReassembler(t, ReassemblerConfig{})

	t.Run("absent before the first segment", func(t *testing.T) {
		require.Equal(t, false, r.Has(groupID))
	})
	t.Run("present once open", func(t *testing.T) {
		_, err := r.Add(h, msgs[0])
		require.NoError(t, err)
		require.Equal(t, true, r.Has(groupID))
	})
	t.Run("still present after completion", func(t *testing.T) {
		// A delivered group stays open until its TTL, so a late segment joins it, proves
		// against the pinned root and costs no authentication, instead of reopening a
		// group that would have to be authenticated again.
		for _, m := range msgs[1:] {
			_, err := r.Add(h, m)
			require.NoError(t, err)
		}
		require.Equal(t, true, r.Has(groupID))
		require.Equal(t, true, r.Complete(groupID))
	})
	t.Run("absent for an unauthenticated group", func(t *testing.T) {
		// A rejected descriptor must leave no trace, or Has would report a group that
		// does not exist and callers would skip authentication for it.
		failing := newTestReassembler(t, ReassemblerConfig{Auth: &countingAuth{fail: errors.New("nope")}})
		_, err := failing.Add(h, msgs[0])
		require.Equal(t, true, err != nil)
		require.Equal(t, false, failing.Has(groupID))
	})
}

// TestReassemblerCodedGroup drives a coded group through the reassembler: complete at
// Required segments whichever they are, the recovered payload returned once, and the
// remaining segments of the group accepted without effect.
func TestReassemblerCodedGroup(t *testing.T) {
	h := testHasher(t)
	msg := msgOfLen(4*64 + 7)
	msgs, err := BuildCodedSegmentMessages(msg, 64, 4, h)
	require.NoError(t, err)
	d := msgs[0].Descriptor
	k := int(d.Required())
	n := int(d.Count)
	groupID := d.GroupID(h)

	r := newTestReassembler(t, ReassemblerConfig{})

	// Feed parity-heavy: skip systematic indices 1 and 3, take parity instead.
	feed := []int{0, 2, n - 1, n - 2, n - 3}
	require.Equal(t, k, len(feed))
	var got []byte
	for i, idx := range feed {
		out, err := r.Add(h, msgs[idx])
		require.NoError(t, err)
		if i < k-1 {
			require.Equal(t, true, out == nil, "delivered early at segment %d", i)
			require.Equal(t, false, r.Complete(groupID))
		} else {
			got = out
		}
	}
	require.DeepEqual(t, msg, got)
	require.Equal(t, true, r.Complete(groupID))
	require.Equal(t, 0, r.Bytes(), "completion releases the buffers")

	t.Run("the rest of the group buffers nothing and delivers nothing", func(t *testing.T) {
		for _, idx := range []int{1, 3, k} {
			out, err := r.Add(h, msgs[idx])
			require.NoError(t, err)
			require.Equal(t, true, out == nil)
		}
		require.Equal(t, 0, r.Bytes())
	})

	t.Run("a delivered group ages out like any other", func(t *testing.T) {
		clock := newTestClock()
		r := newTestReassembler(t, ReassemblerConfig{TTL: time.Minute, Now: clock.now})
		for _, idx := range feed {
			_, err := r.Add(h, msgs[idx])
			require.NoError(t, err)
		}
		require.Equal(t, true, r.Complete(groupID))
		clock.advance(2 * time.Minute)
		r.Prune()
		require.Equal(t, false, r.Complete(groupID))
		require.Equal(t, 0, r.Groups())
	})

	t.Run("an inconsistent codeword drops the group", func(t *testing.T) {
		// Commit systematic segments and one corrupted parity segment together; every leaf
		// proves against the root, so only recovery can refuse it, and it must not be retried
		// on every later arrival.
		leaves := make([][]byte, n)
		for i, m := range msgs {
			leaves[i] = m.Data
		}
		bad := make([]byte, len(leaves[k]))
		copy(bad, leaves[k])
		bad[0] ^= 1
		leaves[k] = bad
		tree, err := BuildTree(h, leaves)
		require.NoError(t, err)
		badDesc := &Descriptor{Version: VersionCoded, HashID: d.HashID, Count: d.Count, SegmentSize: d.SegmentSize, TotalLength: d.TotalLength, Root: tree.Root()}
		r := newTestReassembler(t, ReassemblerConfig{})
		for i := 0; i < k; i++ {
			proof, err := tree.Proof(i)
			require.NoError(t, err)
			out, err := r.Add(h, &SegmentMessage{Descriptor: badDesc, Index: uint32(i), Proof: proof, Data: leaves[i]})
			if i < k-1 {
				require.NoError(t, err)
				require.Equal(t, true, out == nil)
				continue
			}
			require.ErrorIs(t, err, ErrCodewordMismatch)
		}
		require.Equal(t, 0, r.Groups())
		require.Equal(t, 0, r.Bytes())
	})
}
