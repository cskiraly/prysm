package pubsub

import (
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
)

func TestPacedGossipRelease(t *testing.T) {
	hb := 700 * time.Millisecond
	t0 := time.Unix(1000, 0)
	a, b, gone := peer.ID("a"), peer.ID("b"), peer.ID("gone")
	newGS := func() *GossipSubRouter {
		gs := &GossipSubRouter{
			params:   GossipSubParams{HeartbeatInterval: hb},
			peers:    map[peer.ID]protocol.ID{a: GossipSubID_v11, b: GossipSubID_v11},
			unwanted: make(map[peer.ID]map[checksum]int),
			paced:    &pacedGossip{tick: hb / 16, minAge: 400 * time.Millisecond, born: make(map[string]time.Time)},
		}
		return gs
	}
	t.Run("one job per call, oldest first, held and young ids left out", func(t *testing.T) {
		gs := newGS()
		gs.paced.born["old"], gs.paced.born["young"], gs.paced.born["held"] = t0.Add(-time.Second), t0, t0.Add(-time.Second)
		gs.unwanted[a] = map[checksum]int{computeChecksum("held"): 3}
		gs.paced.queue = []pacedJob{{p: a, mids: []string{"old", "young", "held"}, at: t0}, {p: b, mids: []string{"old"}, at: t0}}
		j, mids, ok := gs.nextPaced(t0)
		if !ok || j.p != a || len(mids) != 1 || mids[0] != "old" {
			t.Fatalf("got %v %v %v, want peer a offered only the old id", j.p, mids, ok)
		}
		if len(gs.paced.queue) != 1 || gs.paced.queue[0].p != b {
			t.Fatal("the second job must wait for the next tick")
		}
	})
	t.Run("a job with only young ids waits at the back", func(t *testing.T) {
		gs := newGS()
		gs.paced.born["young"] = t0
		gs.paced.queue = []pacedJob{{p: a, mids: []string{"young"}, at: t0}}
		if _, _, ok := gs.nextPaced(t0.Add(100 * time.Millisecond)); ok {
			t.Fatal("nothing is old enough yet")
		}
		if gs.paced.deferred.Load() != 1 || len(gs.paced.queue) != 1 {
			t.Fatal("the job must be deferred, not dropped")
		}
		if _, mids, ok := gs.nextPaced(t0.Add(450 * time.Millisecond)); !ok || len(mids) != 1 {
			t.Fatal("at 450 ms the id is old enough")
		}
	})
	t.Run("stale jobs and departed peers are dropped", func(t *testing.T) {
		gs := newGS()
		gs.paced.queue = []pacedJob{{p: a, mids: []string{"x"}, at: t0.Add(-hb)}, {p: gone, mids: []string{"x"}, at: t0}}
		if _, _, ok := gs.nextPaced(t0); ok || gs.paced.dropped.Load() != 2 {
			t.Fatalf("dropped %d, want both jobs dropped", gs.paced.dropped.Load())
		}
	})
}

func TestPacedGossipUnderHose(t *testing.T) {
	ctx := t.Context()
	job := []pacedJob{{p: "a", mids: []string{"x"}, at: time.Now()}}
	newGS := func(passive, flag bool) (*GossipSubRouter, *hoseWindow) {
		h := newTestHose(passive) // window of 2
		gs := &GossipSubRouter{p: &PubSub{hose: h}, paced: &pacedGossip{hose: flag, queue: append([]pacedJob(nil), job...)}}
		return gs, h
	}
	t.Run("a full window holds the release, a slot back frees it", func(t *testing.T) {
		gs, h := newGS(false, true)
		_, _ = h.acquire(ctx, nil)
		_, _ = h.acquire(ctx, nil)
		if !gs.pacedHeld() || gs.paced.held.Load() != 1 {
			t.Fatal("a full window must hold the IHAVE")
		}
		h.release(20*time.Millisecond, true, false)
		if gs.pacedHeld() {
			t.Fatal("a free slot must let the IHAVE go")
		}
	})
	t.Run("never held without the flag, when passive, or with nothing queued", func(t *testing.T) {
		for _, c := range []struct {
			name          string
			passive, flag bool
			empty         bool
		}{{"flag off", false, false, false}, {"passive", true, true, false}, {"empty queue", false, true, true}} {
			gs, h := newGS(c.passive, c.flag)
			_, _ = h.acquire(ctx, nil)
			_, _ = h.acquire(ctx, nil)
			if c.empty {
				gs.paced.queue = nil
			}
			if gs.pacedHeld() {
				t.Fatalf("%s: held", c.name)
			}
		}
	})
}
