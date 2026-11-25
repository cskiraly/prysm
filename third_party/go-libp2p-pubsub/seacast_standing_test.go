package pubsub

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
)

// TestSeacastStandingAndServeLimit is the standing table and the serving limit on three routers: A subscribes
// to the column with a SERVE_LIMIT of 3, C subscribes and publishes, B subscribes to nothing and
// asks A. A standing WANT for a cell A lacks is kept and answered when the cell arrives from C
// (B admits it by its WANT); one for a cell A holds is answered at once; at the limit an ask is
// refused without penalty, held or not, and the connection stays; a standing request of another
// group goes with that group's expiry before its cell arrives; a cell A publishes itself answers
// the request waiting for it; and a plain WANT (not standing) for a cell A lacks is dropped as
// before, the control.
func TestSeacastStandingAndServeLimit(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		root1, root2, root3 := strings.Repeat("s", haveRootLen), strings.Repeat("t", haveRootLen), strings.Repeat("u", haveRootLen)
		stores := make([]*MapCellStore, 3)
		hosts := getDefaultHosts(t, 3)
		params := DefaultGossipSubParams()
		params.IDontWantMessageThreshold = 1
		psubs := getGossipsubsOptFn(ctx, hosts, func(i int, h host.Host) []Option {
			stores[i] = NewMapCellStore()
			tp := TypeParams{RNode: 2}
			if i == 0 {
				tp.ServeLimit = 3
			}
			T := &testObjectType{name: "col", params: tp, topics: []string{"seg/col/0"}, store: stores[i]}
			return []Option{
				WithGossipSubParams(params),
				WithMessageIdFn(seacastTestID),
				WithHaveBitmaps(seacastTestIndex),
				WithPhaseForwarding(func(topic string) bool { return topic != "seg/col/0" }, 2),
				WithSeacast([]ObjectType{T}, nil),
			}
		})
		seacastOrdZeroAtEnd(t, psubs)
		a, b, c := psubs[0], psubs[1], psubs[2]
		for _, ps := range []*PubSub{a, c} {
			if _, err := ps.Subscribe("seg/col/0"); err != nil {
				t.Fatal(err)
			}
		}
		gsA := a.rt.(*GossipSubRouter)
		ctrA, ctrB := gsA.seacast.ctr, b.rt.(*GossipSubRouter).seacast.ctr
		connect(t, hosts[0], hosts[1])
		connect(t, hosts[0], hosts[2])
		time.Sleep(2 * time.Second)
		read := func(gs *GossipSubRouter, f func()) {
			done := make(chan struct{})
			gs.p.eval <- func() { f(); close(done) }
			<-done
		}
		cell := func(root string, index byte) []byte {
			return append([]byte(root+string(index)), []byte(strings.Repeat("x", 64))...)
		}
		publish := func(ps *PubSub, root string, index byte) {
			if err := ps.Publish("seg/col/0", cell(root, index)); err != nil {
				t.Fatal(err)
			}
			time.Sleep(time.Second)
		}
		want := func(root string, index uint32, standing bool) {
			if !b.SeacastWant(hosts[0].ID(), root, index, standing, 0) {
				t.Fatalf("B's WANT for %s/%d was not sent", root[:1], index)
			}
			time.Sleep(time.Second)
		}
		heldB := func() int {
			var n int
			read(b.rt.(*GossipSubRouter), func() { n = stores[1].Len() })
			return n
		}
		servedAtA := func(root string) int {
			var n int
			read(gsA, func() { n = gsA.seacast.served[root][hosts[1].ID()] })
			return n
		}
		standingAtA := func() int {
			var n int
			read(gsA, func() {
				for _, g := range gsA.seacast.standing {
					for _, peers := range g {
						n += len(peers)
					}
				}
			})
			return n
		}
		// C publishes cells 6, 9 and 10 of group 1; A holds them.
		for _, i := range []byte{6, 9, 10} {
			publish(c, root1, i)
		}
		var held int
		read(gsA, func() { held = stores[0].Len() })
		if held != 3 {
			t.Fatalf("A holds %d cells, want 3", held)
		}

		// A standing WANT for cell 5, which A lacks: kept, nothing sent.
		want(root1, 5, true)
		if ctrA.StandingKept.Load() != 1 || ctrA.WantsStanding.Load() != 1 || standingAtA() != 1 || heldB() != 0 {
			t.Fatalf("after B's standing WANT for an unheld cell: kept %d, standing WANTs %d, entries %d, B holds %d; want 1 1 1 0", ctrA.StandingKept.Load(), ctrA.WantsStanding.Load(), standingAtA(), heldB())
		}
		// The cell arrives from C: A serves the standing request; B admits it by its WANT.
		publish(c, root1, 5)
		if ctrA.StandingServed.Load() != 1 || standingAtA() != 0 || heldB() != 1 || ctrB.AdmittedByWant.Load() != 1 || ctrB.Accepted.Load() != 1 || servedAtA(root1) != 1 {
			t.Fatalf("after the cell arrived: served %d, entries %d, B holds %d, admitted %d, accepted %d, served count %d; want 1 0 1 1 1 1", ctrA.StandingServed.Load(), standingAtA(), heldB(), ctrB.AdmittedByWant.Load(), ctrB.Accepted.Load(), servedAtA(root1))
		}
		// Standing WANTs for cells A holds: answered at once, counted against the limit.
		want(root1, 6, true)
		want(root1, 9, true)
		if heldB() != 3 || servedAtA(root1) != 3 || ctrA.StandingKept.Load() != 1 {
			t.Fatalf("after two held cells asked: B holds %d, served count %d, kept %d; want 3 3 1", heldB(), servedAtA(root1), ctrA.StandingKept.Load())
		}
		// At the limit: a held cell and an unheld one are refused without penalty, neither kept.
		want(root1, 10, true)
		want(root1, 11, true)
		if ctrA.ServeLimited.Load() != 2 || heldB() != 3 || ctrA.StandingKept.Load() != 1 || standingAtA() != 0 {
			t.Fatalf("at the limit: refused %d, B holds %d, kept %d, entries %d; want 2 3 1 0", ctrA.ServeLimited.Load(), heldB(), ctrA.StandingKept.Load(), standingAtA())
		}
		publish(c, root1, 11)
		if heldB() != 3 || len(hosts[0].Network().ConnsToPeer(hosts[1].ID())) == 0 {
			t.Fatalf("after the refused cell arrived: B holds %d (want 3), A–B connected %v (want true)", heldB(), len(hosts[0].Network().ConnsToPeer(hosts[1].ID())) > 0)
		}

		// Group 2: the request goes with the group's expiry before its cell arrives.
		want(root2, 1, true)
		if ctrA.StandingKept.Load() != 2 || standingAtA() != 1 {
			t.Fatalf("group 2's standing WANT: kept %d, entries %d; want 2 1", ctrA.StandingKept.Load(), standingAtA())
		}
		a.SeacastExpire([]string{root2})
		time.Sleep(100 * time.Millisecond)
		if ctrA.StandingExpired.Load() != 1 || standingAtA() != 0 {
			t.Fatalf("after group 2's expiry: expired %d, entries %d; want 1 0", ctrA.StandingExpired.Load(), standingAtA())
		}
		publish(c, root2, 1)
		if heldB() != 3 || ctrA.StandingServed.Load() != 1 {
			t.Fatalf("after the expired group's cell arrived: B holds %d, served %d; want 3 1", heldB(), ctrA.StandingServed.Load())
		}

		// Group 3: A's own publication answers the request waiting for it.
		want(root3, 1, true)
		publish(a, root3, 1)
		if ctrA.StandingServed.Load() != 2 || heldB() != 4 || ctrB.AdmittedByWant.Load() != 4 {
			t.Fatalf("after A published the waited-for cell: served %d, B holds %d, admitted %d; want 2 4 4", ctrA.StandingServed.Load(), heldB(), ctrB.AdmittedByWant.Load())
		}
		// The control: a plain WANT for a cell A lacks is dropped as before, nothing kept.
		want(root3, 2, false)
		publish(c, root3, 2)
		if ctrA.StandingKept.Load() != 3 || standingAtA() != 0 || heldB() != 4 || ctrA.WantsStanding.Load() != 7 || ctrA.WantsV4.Load() != 8 {
			t.Fatalf("the plain WANT: kept %d, entries %d, B holds %d, standing WANTs %d, v4 WANTs %d; want 3 0 4 7 8", ctrA.StandingKept.Load(), standingAtA(), heldB(), ctrA.WantsStanding.Load(), ctrA.WantsV4.Load())
		}
	})
}
