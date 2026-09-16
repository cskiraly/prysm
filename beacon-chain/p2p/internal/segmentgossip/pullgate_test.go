package segmentgossip

import (
	"encoding/binary"
	"strings"
	"testing"
	"time"

	"github.com/OffchainLabs/prysm/v7/testing/require"
	"github.com/libp2p/go-libp2p/core/peer"
)

// idFor builds a structured id for a claim with an arbitrary content half.
func idFor(root [32]byte, index uint32, content string) string {
	id := make([]byte, 0, MessageIDLen)
	id = append(id, root[:]...)
	id = binary.LittleEndian.AppendUint32(id, index)
	id = append(id, content...)
	return string(id)
}

func TestPullGate(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	g := NewPullGate()
	g.now = func() time.Time { return now }
	p := peer.ID("announcer")
	var done, other [32]byte
	done[0], other[0] = 1, 2
	content := strings.Repeat("c", 20)

	t.Run("nothing declined before completion", func(t *testing.T) {
		require.Equal(t, true, g.Allow(p, "t", idFor(done, 3, content)))
	})

	g.MarkComplete(done)

	t.Run("every segment of the completed group is declined", func(t *testing.T) {
		for _, idx := range []uint32{0, 3, 127} {
			require.Equal(t, false, g.Allow(p, "t", idFor(done, idx, content)), "index %d", idx)
			require.Equal(t, false, g.Allow(p, "t", idFor(done, idx, strings.Repeat("d", 20))), "any body of index %d", idx)
		}
	})

	t.Run("other groups and unclaimed ids pass", func(t *testing.T) {
		require.Equal(t, true, g.Allow(p, "t", idFor(other, 3, content)))
		require.Equal(t, true, g.Allow(p, "t", content), "a 20-byte content id")
		require.Equal(t, true, g.Allow(p, "t", ""), "an empty id")
	})

	t.Run("the veto expires with the retention", func(t *testing.T) {
		now = now.Add(completeRetention)
		require.Equal(t, true, g.Allow(p, "t", idFor(done, 3, content)))
		// A later completion sweeps the expired entry.
		g.MarkComplete(other)
		require.Equal(t, 1, len(g.complete))
		require.Equal(t, false, g.Allow(p, "t", idFor(other, 0, content)))
	})

	t.Run("committed and failed are inert", func(t *testing.T) {
		g.Committed(p, []string{idFor(other, 0, content)})
		g.Failed(p, []string{idFor(other, 0, content)})
		require.Equal(t, false, g.Allow(p, "t", idFor(other, 0, content)))
	})
}
