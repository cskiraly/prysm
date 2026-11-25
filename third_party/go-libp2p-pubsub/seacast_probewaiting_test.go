package pubsub

import (
	"context"
	"testing"
	"time"
)

// TestSeacastProbeWaitingAtDecoder is seacast.md §4.6 and §4.5 at spec 43fe953 (on_want's probe_waiting,
// serve_waiting_probe from fill and decode_group): a claimant still decoding keeps the first marked ask per
// asker and group of a claimed index it has not stored, and answers it once, with its exemption, when the
// cell is stored. A is put in DECODING before B's probe of it arrives; A then publishes the probed cell, which
// its fill stores. A kept the probe and served it, once.
func TestSeacastProbeWaitingAtDecoder(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		s := newR5SceneCross(t, ctx, false, nil, func(s *r5Scene, psubs []*PubSub) {
			s.on(s.gsA, func() {
				g := s.gsA.seacast.groupOf(s.root)
				g.state = seacastDecoding // A claims every index while it decodes (holding_of's claim_all)
			})
		})
		time.Sleep(300 * time.Millisecond) // B's probe reaches A, which does not hold the cell
		ctrA := s.gsA.seacast.ctr
		if k, v := ctrA.ProbesKept.Load(), ctrA.ProbesServed.Load(); k != 1 || v != 0 {
			t.Fatalf("A decoding, the probed cell not stored: kept %d served %d; want 1 and 0", k, v)
		}
		s.publishA(byte(s.index)) // lint:ignore uintcast -- a small index.
		time.Sleep(300 * time.Millisecond)
		if k, v := ctrA.ProbesKept.Load(), ctrA.ProbesServed.Load(); k != 1 || v != 1 {
			t.Fatalf("after A stored the probed cell: kept %d served %d; want 1 and 1 (answered once, with its exemption)", k, v)
		}
	})
}
