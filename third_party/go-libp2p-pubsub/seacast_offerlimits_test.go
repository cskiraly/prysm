package pubsub

import (
	"strings"
	"testing"
)

// TestSeacastOfferHeldToLimits is two rules of v0.10 that the draft carries into its own functions (spec
// f6c19db): an offer's bitmap is held to a holding's limits (seacast-lite.md §4.4's on_ihave, seacast.md
// §4.3's on_offer: one naming an index at or beyond the group's known count is ignored and counted against
// its sender), and an offer's plain id must be a valid one (seacast-lite.md §4.4's valid_id: an index at or
// beyond the count is ignored, without penalty). A offers B, on a path topic whose type counts 32 indices,
// a bitmap with bit 32 set and the id of index 32; and, as controls, a bitmap with bit 31 and the id of
// index 30. B asks for the controls only, and counts the bitmap as malformed, not the id.
func TestSeacastOfferHeldToLimits(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		root := strings.Repeat("o", haveRootLen)
		// The cells' offers carry bitmaps (HAVEMODE=both): newSeacastAskCell's options with B′ and bitmap offers.
		c := newSeacastParkMissCell(t, 2, &SpecV05{LastResort: true}, nil, WithHaveAnnounceBitmaps()) // A = 0, B = 1
		A := c.hosts[0].ID()
		ctr := c.gs(1).seacast.ctr
		var beyond, inside haveBits
		beyond.Set(32)
		inside.Set(31)
		m0 := ctr.Malformed.Load()
		c.offer(0, encodeHave(root, &beyond))
		if n := c.asksTo(encodeWant(root, 32), A); n != 0 {
			t.Fatalf("a bitmap naming index 32 of 32: %d asks of it, want 0", n)
		}
		if d := ctr.Malformed.Load() - m0; d != 1 {
			t.Fatalf("a bitmap naming index 32 of 32: malformed +%d, want +1", d)
		}
		c.offer(0, encodeHave(root, &inside))
		if n := c.asksTo(encodeWant(root, 31), A); n != 1 {
			t.Fatalf("the control, a bitmap naming index 31: %d asks of it, want 1", n)
		}
		m1 := ctr.Malformed.Load()
		c.offer(0, root+string(byte(32)))
		if n := c.asksTo(root+string(byte(32)), A); n != 0 {
			t.Fatalf("the id of index 32 of 32: %d asks of it, want 0", n)
		}
		if d := ctr.Malformed.Load() - m1; d != 0 {
			t.Fatalf("the id of index 32 of 32: malformed +%d, want +0 (ignored without penalty)", d)
		}
		c.offer(0, root+string(byte(30)))
		if n := c.asksTo(root+string(byte(30)), A); n != 1 {
			t.Fatalf("the control, the id of index 30: %d asks of it, want 1", n)
		}
	})
}
