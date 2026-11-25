package pubsub

import (
	"strings"
	"testing"
	"time"
)

// TestSeacastStaleWaits is seacast-lite.md §4.2's drop_stale_waits at spec f6c19db, which the draft carries
// (seacast.md's on_heartbeat): what waits for a root that never installs goes when the slot its copy
// named leaves the live window, a kept copy by its own slot and a tie by the slot its sender's first copy
// named. A publishes a cell of a group whose header B never validates and whose slot B's application
// cannot place (SlotOf unknown); the copy names slot 5. B keeps it and ties A. With the live window of
// two slots, advancing B's slot to 6 keeps both; to 7 drops both.
func TestSeacastStaleWaits(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		root := strings.Repeat("w", haveRootLen)
		c := newSeacastTieCell(t, 2)
		for _, T := range c.types {
			T.chunkSlots = map[string]uint64{root: 5}
		}
		a, b := c.pubsubs[0], c.pubsubs[1]
		if err := a.Publish("seg/col/0", seacastCell(root, 3)); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Second)
		if b.SeacastPending() != 1 || b.SeacastTies() != 1 {
			t.Fatalf("after A's copy: pending %d ties %d, want 1 1", b.SeacastPending(), b.SeacastTies())
		}
		b.SeacastAdvanceSlot(6)
		time.Sleep(100 * time.Millisecond)
		if b.SeacastPending() != 1 || b.SeacastTies() != 1 {
			t.Fatalf("at slot 6 (slot 5 still live): pending %d ties %d, want 1 1", b.SeacastPending(), b.SeacastTies())
		}
		b.SeacastAdvanceSlot(7)
		time.Sleep(100 * time.Millisecond)
		if b.SeacastPending() != 0 || b.SeacastTies() != 0 {
			t.Fatalf("at slot 7 (slot 5 left the window): pending %d ties %d, want 0 0", b.SeacastPending(), b.SeacastTies())
		}
	})
}
