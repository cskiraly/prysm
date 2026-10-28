package pubsub

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

func newParkRouter(scoped bool) *GossipSubRouter {
	gs := &GossipSubRouter{
		parkK: 1, parkTTL: time.Minute,
		iwantPromised: make(map[string]map[peer.ID]time.Time),
		parkBreaks:    make(map[peer.ID]int), parkedUntil: make(map[peer.ID]time.Time),
		parkBreaksCtr: new(atomic.Int64), parksCtr: new(atomic.Int64),
	}
	if scoped {
		gs.parkGroups = make(map[peer.ID]map[string]*parkEntry)
	}
	return gs
}

func TestParkPerGroup(t *testing.T) {
	p := peer.ID("relay")
	rootA, rootB := strings.Repeat("a", 32), strings.Repeat("b", 32)
	a0, a1, b0 := encodeWant(rootA, 0), encodeWant(rootA, 1), encodeWant(rootB, 0)
	t.Run("unscoped, a break in one group parks the peer for every group", func(t *testing.T) {
		gs := newParkRouter(false)
		gs.notePromiseBreak(a0, p)
		if !gs.peerParkedFor(p, a1) || !gs.peerParkedFor(p, b0) {
			t.Fatal("unscoped park should cover every id")
		}
	})
	t.Run("scoped, the park covers the group it was earned in only", func(t *testing.T) {
		gs := newParkRouter(true)
		gs.notePromiseBreak(a0, p)
		if !gs.peerParkedFor(p, a1) {
			t.Fatal("a break in group A should park the peer for A")
		}
		if gs.peerParkedFor(p, b0) {
			t.Fatal("a break in group A parked the peer for group B")
		}
		if gs.parksCtr.Load() != 1 || gs.parkBreaksCtr.Load() != 1 || len(gs.parkBreaks) != 0 {
			t.Fatalf("parks %d breaks %d per-peer counts %d, want 1, 1 and none", gs.parksCtr.Load(), gs.parkBreaksCtr.Load(), len(gs.parkBreaks))
		}
		gs.notePromiseBreak(a1, p) // a re-park in the same group is not a new park
		if gs.parksCtr.Load() != 1 {
			t.Fatalf("parks %d after a second break in the same group, want 1", gs.parksCtr.Load())
		}
	})
	t.Run("an application scope names the group when there is no HAVE root", func(t *testing.T) {
		gs := newParkRouter(true)
		gs.parkScope = func(mid string) (string, bool) { return mid[:1], true }
		gs.notePromiseBreak("x1", p)
		if !gs.peerParkedFor(p, "x2") || gs.peerParkedFor(p, "y1") {
			t.Fatal("the park must follow the application's group")
		}
	})
	t.Run("the sweep forgets a group once its park and breaks are a TTL old", func(t *testing.T) {
		gs := newParkRouter(true)
		gs.notePromiseBreak(a0, p)
		gs.sweepGroupParks()
		if len(gs.parkGroups[p]) != 1 {
			t.Fatal("the sweep dropped a running park")
		}
		e := gs.parkGroups[p][rootA]
		e.until, e.last = time.Now().Add(-time.Second), time.Now().Add(-2*time.Minute)
		gs.sweepGroupParks()
		if len(gs.parkGroups) != 0 {
			t.Fatal("the sweep kept an expired group")
		}
	})
}

func TestPromiseKeepsEarliestDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		gs := newParkRouter(false)
		gs.p = &PubSub{eval: make(chan func(), 16), ctx: ctx}
		gs.iwantAsked = make(map[string]time.Time)
		gs.iwantWindow = 200 * time.Millisecond
		gs.parkPromise = 400 * time.Millisecond
		p, mid := peer.ID("silent"), "frame"
		if !gs.iwantAllowed(mid, p) {
			t.Fatal("first ask refused")
		}
		first := gs.iwantPromised[mid][p]
		time.Sleep(205 * time.Millisecond) // the discipline window has passed: a re-ask is allowed
		if !gs.iwantAllowed(mid, p) {
			t.Fatal("re-ask after the window refused")
		}
		if got := gs.iwantPromised[mid][p]; !got.Equal(first) {
			t.Fatalf("the re-ask moved the promise from %v to %v", first, got)
		}
		time.Sleep(200 * time.Millisecond) // 405 ms: the first promise is due
		synctest.Wait()
		for len(gs.p.eval) > 0 {
			(<-gs.p.eval)()
		}
		if gs.parkBreaksCtr.Load() != 1 || !gs.peerParked(p) {
			t.Fatalf("breaks %d parked %v at 405 ms, want the first promise broken and the peer parked", gs.parkBreaksCtr.Load(), gs.peerParked(p))
		}
	})
}

func TestPromiseRecheck(t *testing.T) {
	p, root := peer.ID("server"), strings.Repeat("r", 32)
	held, open := encodeWant(root, 1), testMid(root, 2) // an ask by index, and one by id
	setup := func(recheck bool) *GossipSubRouter {
		gs := newParkRouter(true)
		gs.have = newHaveState(testIndex)
		gs.have.mine[root] = &haveEntry{}
		gs.have.mine[root].bits.Set(1) // decoded or received since the ask
		gs.promiseRecheck = recheck
		at := time.Now()
		for _, mid := range []string{held, open} {
			gs.iwantPromised[mid] = map[peer.ID]time.Time{p: at}
		}
		gs.promiseDue(held, p, at)
		gs.promiseDue(open, p, at)
		return gs
	}
	t.Run("off, both promises break and the held one is counted", func(t *testing.T) {
		gs := setup(false)
		if gs.parkBreaksCtr.Load() != 2 || gs.promiseHeld.Load() != 1 {
			t.Fatalf("breaks %d held %d, want 2 and 1", gs.parkBreaksCtr.Load(), gs.promiseHeld.Load())
		}
	})
	t.Run("on, the held index is absolved and only the open one breaks", func(t *testing.T) {
		gs := setup(true)
		if gs.parkBreaksCtr.Load() != 1 || gs.promiseHeld.Load() != 1 || len(gs.iwantPromised) != 0 {
			t.Fatalf("breaks %d held %d promises left %d, want 1, 1 and none", gs.parkBreaksCtr.Load(), gs.promiseHeld.Load(), len(gs.iwantPromised))
		}
	})
}
