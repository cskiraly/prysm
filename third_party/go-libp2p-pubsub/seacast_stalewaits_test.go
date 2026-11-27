package pubsub

import (
	"fmt"
	"strings"
	"testing"
	"testing/synctest"
	"time"
)

// TestSeacastStaleWaits is seacast-lite.md §4.2's drop_stale_waits at spec f6c19db, which the draft carries
// (seacast.md's on_heartbeat): what waits for a root that never installs goes when the slot its copy
// named leaves the live window, a kept copy by its own slot and a tie by the slot its sender's first copy
// named. A publishes a cell of a group whose header B never validates and whose slot B's application
// cannot place (SlotOf unknown); the copy names slot 5. B keeps it and ties A. With the live window of
// two slots, B's heartbeat after its slot advances to 6 keeps both; the advance to 7 drops neither,
// and the heartbeat after it drops both, the copy's message with them.
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
		time.Sleep(1100 * time.Millisecond) // a heartbeat
		if b.SeacastPending() != 1 || b.SeacastTies() != 1 {
			t.Fatalf("at slot 6 (slot 5 still live): pending %d ties %d, want 1 1", b.SeacastPending(), b.SeacastTies())
		}
		b.SeacastAdvanceSlot(7)
		synctest.Wait() // no time passes: the advance, without a heartbeat
		if b.SeacastPending() != 1 || b.SeacastTies() != 1 {
			t.Fatalf("at the advance to 7, before a heartbeat: pending %d ties %d, want 1 1 (the heartbeat drops them)", b.SeacastPending(), b.SeacastTies())
		}
		time.Sleep(1100 * time.Millisecond) // a heartbeat
		var bodies int
		c.read(1, func(gs *GossipSubRouter) { bodies = len(gs.seacast.waitMsg) })
		if b.SeacastPending() != 0 || b.SeacastTies() != 0 || bodies != 0 {
			t.Fatalf("at slot 7's heartbeat (slot 5 left the window): pending %d ties %d messages %d, want 0 0 0", b.SeacastPending(), b.SeacastTies(), bodies)
		}
	})
}

// TestSeacastResumeChecksCopySlot: at the gate's opening a kept copy resumes only while its own slot
// is live (seacast-lite.md §4.2's on_validated). B keeps A's copy naming slot 5 of a root its
// application cannot place (SlotOf unknown), registered at slot 6, so the group stays live by its
// registration at slot 7. The gate opens after the advance to 7 and before the heartbeat would drop
// the copy: it is dropped, not validated.
func TestSeacastResumeChecksCopySlot(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		root := strings.Repeat("v", haveRootLen)
		c := newSeacastTieCell(t, 2)
		for _, T := range c.types {
			T.chunkSlots = map[string]uint64{root: 5}
		}
		a, b := c.pubsubs[0], c.pubsubs[1]
		if err := a.Publish("seg/col/0", seacastCell(root, 3)); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Second)
		b.SeacastAdvanceSlot(6) // the root takes slot 6 as its registration's
		b.SeacastAdvanceSlot(7)
		synctest.Wait()
		if b.SeacastPending() != 1 {
			t.Fatalf("at slot 7, before a heartbeat: pending %d, want 1", b.SeacastPending())
		}
		c.read(1, func(*GossipSubRouter) { c.types[1].gateClosed = false })
		b.SeacastHeaderValidated([]string{root})
		synctest.Wait()
		var held int
		c.read(1, func(*GossipSubRouter) { held = c.stores[1].Len() })
		if b.SeacastPending() != 0 || c.validated.Load() != 0 || held != 0 {
			t.Fatalf("at the gate's opening: pending %d validated %d held %d, want 0 0 0", b.SeacastPending(), c.validated.Load(), held)
		}
	})
}

// TestSeacastCopySlotAfterValidation: a copy is filled only while its own slot is live when its
// validation finishes (seacast-lite.md §4.2's on_validated). B keeps A's copy naming slot 5 of a root
// registered at slot 6, and the gate opens at slot 6, where the copy's slot is live: the copy resumes.
// Its validation waits while the node advances to slot 7, where the group is live and the copy's slot
// is not: the copy is validated and not filled. A copy of the same root naming slot 6 is filled.
func TestSeacastCopySlotAfterValidation(t *testing.T) {
	for _, slot := range []uint64{5, 6} {
		t.Run(fmt.Sprint("slot ", slot), func(t *testing.T) {
			synctestTest(t, func(t *testing.T) {
				root := strings.Repeat("w", haveRootLen)
				c := newSeacastTieCell(t, 2)
				for _, T := range c.types {
					T.chunkSlots = map[string]uint64{root: slot}
				}
				a, b := c.pubsubs[0], c.pubsubs[1]
				if err := a.Publish("seg/col/0", seacastCell(root, 3)); err != nil {
					t.Fatal(err)
				}
				time.Sleep(time.Second)
				b.SeacastAdvanceSlot(6) // the root takes slot 6 as its registration's
				synctest.Wait()
				if b.SeacastPending() != 1 {
					t.Fatalf("at slot 6: pending %d, want 1", b.SeacastPending())
				}
				hold := make(chan struct{})
				c.hold.Store(&hold)
				c.read(1, func(*GossipSubRouter) { c.types[1].gateClosed = false })
				b.SeacastHeaderValidated([]string{root})
				synctest.Wait()
				if b.SeacastPending() != 0 || c.validated.Load() != 1 {
					t.Fatalf("at the gate's opening: pending %d validated %d, want 0 1 (resumed, in validation)", b.SeacastPending(), c.validated.Load())
				}
				b.SeacastAdvanceSlot(7)
				synctest.Wait()
				c.hold.Store(nil)
				close(hold)
				synctest.Wait()
				var held int
				c.read(1, func(*GossipSubRouter) { held = c.stores[1].Len() })
				if want := map[uint64]int{5: 0, 6: 1}[slot]; held != want {
					t.Fatalf("validated at slot 7: held %d, want %d", held, want)
				}
			})
		})
	}
}

// TestSeacastCopyOfLaterSlotIgnored: a copy naming a later slot than the node's is not live
// (seacast-lite.md §8.1: a chunk of a later slot is ignored, as a block from a future slot is). B,
// at slot 6, ignores A's copy of a fresh root naming slot 8 and keeps A's copy of another naming
// slot 6.
func TestSeacastCopyOfLaterSlotIgnored(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		later, now := strings.Repeat("y", haveRootLen), strings.Repeat("z", haveRootLen)
		c := newSeacastTieCell(t, 2)
		for _, T := range c.types {
			T.chunkSlots = map[string]uint64{later: 8, now: 6}
		}
		a, b := c.pubsubs[0], c.pubsubs[1]
		b.SeacastAdvanceSlot(6)
		synctest.Wait()
		for _, root := range []string{later, now} {
			if err := a.Publish("seg/col/0", seacastCell(root, 3)); err != nil {
				t.Fatal(err)
			}
		}
		time.Sleep(time.Second)
		if f := c.gs(1).seacast.ctr.Future.Load(); b.SeacastPending() != 1 || f != 1 {
			t.Fatalf("at slot 6: pending %d, later slots ignored %d; want 1 1", b.SeacastPending(), f)
		}
	})
}

// TestSeacastSeenCheckBeforeValidationOnly: the admission's test of a seen id (on_chunk's seen first)
// runs before validation only. Once validated, a copy's id is seen by its own validation, so the test
// there would take every copy through. As a test device (no type's gate closes again), B's gate is
// open when A's copy arrives and closes while the copy is in validation: once validated, the copy
// meets the closed gate and is kept, not filled.
func TestSeacastSeenCheckBeforeValidationOnly(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		root := strings.Repeat("s", haveRootLen)
		c := newSeacastTieCell(t, 2)
		a, b := c.pubsubs[0], c.pubsubs[1]
		hold := make(chan struct{})
		c.hold.Store(&hold)
		c.read(1, func(*GossipSubRouter) { c.types[1].gateClosed = false })
		if err := a.Publish("seg/col/0", seacastCell(root, 3)); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Second)
		if c.validated.Load() != 1 || b.SeacastPending() != 0 {
			t.Fatalf("A's copy at an open gate: validated %d pending %d, want 1 0 (in validation)", c.validated.Load(), b.SeacastPending())
		}
		c.read(1, func(*GossipSubRouter) { c.types[1].gateClosed = true })
		c.hold.Store(nil)
		close(hold)
		synctest.Wait()
		var held int
		c.read(1, func(*GossipSubRouter) { held = c.stores[1].Len() })
		if b.SeacastPending() != 1 || held != 0 {
			t.Fatalf("validated at a closed gate: pending %d held %d, want 1 0", b.SeacastPending(), held)
		}
	})
}
