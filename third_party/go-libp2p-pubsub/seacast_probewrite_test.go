package pubsub

import (
	"context"
	"testing"
	"time"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"
)

// TestSeacastProbeWaitAtWrite is seacast.md §4.6 at spec 43fe953 (defer_on_claim, reconsider,
// probe_settled, filter_entry): the probe's promise and the verdict's wait start when its WANT is written;
// a probe still unwritten when the duty fires gets no verdict, and the member's own repair goes on (no
// penalty); a probe settled while still queued is not written later. B's writes are held 3 s from just
// before its probe of A, past its reconsider (DecodeLater 2 s, DEFER_WINDOW 1 s).
func TestSeacastProbeWaitAtWrite(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		s := newR5SceneCross(t, ctx, false, nil, func(s *r5Scene, psubs []*PubSub) {
			psubs[1].sendDelay.Store(int64(3 * time.Second))
		})
		s.afterReconsider()
		ctr := s.gsB.seacast.ctr
		if p := ctr.Penalties.Load(); p != 0 {
			t.Errorf("a probe not written by the reconsider: penalties %d, want 0 (no verdict)", p)
		}
		if n := ctr.ProbesNoVerdict.Load(); n != 1 {
			t.Errorf("a probe not written by the reconsider: no-verdict count %d, want 1", n)
		}
		time.Sleep(5 * time.Second) // past the held write
		if n := s.recvA.got(s.mid); n != 0 {
			t.Errorf("a probe settled while queued: A received it %d times, want 0 (not written later)", n)
		}
	})
}

// TestSeacastProbeRefused is defer_on_claim's refused probe (seacast.md §4.6, spec 43fe953): a probe a
// full queue refuses is never written, so it gets no verdict, and the duty returns to its own time. B's queue
// to A is filled (its writes held) before B probes A.
func TestSeacastProbeRefused(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		s := newR5SceneCross(t, ctx, false, nil, func(s *r5Scene, psubs []*PubSub) {
			psubs[1].sendDelay.Store(int64(time.Minute))
			topic := r5Topic
			fill := func() { // until the queue refuses (its cap is 32)
				s.on(s.gsB, func() {
					for i := 0; i < 256; i++ {
						if !s.gsB.sendRPC(s.A, rpcWithControl(nil, []*pb.ControlIHave{{TopicID: &topic, MessageIDs: []string{"filler"}}}, nil, nil, nil, nil), false) {
							return
						}
					}
				})
			}
			// The writer pops one entry and holds its write a minute: fill, let it pop, fill again, so the
			// queue is full whichever came first (the fill raced the writer's first pop: 7 of 40 runs).
			fill()
			time.Sleep(50 * time.Millisecond)
			fill()
		})
		s.afterReconsider()
		ctr := s.gsB.seacast.ctr
		if p := ctr.Penalties.Load(); p != 0 {
			t.Errorf("a refused probe: penalties %d, want 0 (no verdict)", p)
		}
		if r, n := ctr.ProbesRefused.Load(), ctr.ProbesNoVerdict.Load(); r != 1 || n != 1 {
			t.Errorf("a refused probe: refused %d, no verdict %d; want 1 and 1", r, n)
		}
	})
}
