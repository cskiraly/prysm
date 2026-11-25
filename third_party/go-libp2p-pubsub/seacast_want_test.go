package pubsub

import (
	"context"
	"strings"
	"testing"
	"time"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
)

// TestSeacastAdmissionByWant is the sampler's admission on three routers: A and B subscribe to the
// column, C subscribes to nothing. B publishes two cells; A accepts them. C asks A for one by index
// with a v4 WANT: A serves it, C admits it on the unsubscribed topic, validates, accepts and files
// it, and A hears C's holding. A copy of the other cell from A, never asked, is dropped; a late
// duplicate of the asked cell from A is admitted and acknowledged; after the group's expiry the
// registration is gone and the same copy is dropped.
func TestSeacastAdmissionByWant(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		root := strings.Repeat("w", haveRootLen)
		stores := make([]*MapCellStore, 3)
		hosts := getDefaultHosts(t, 3)
		params := DefaultGossipSubParams()
		params.IDontWantMessageThreshold = 1
		psubs := getGossipsubsOptFn(ctx, hosts, func(i int, h host.Host) []Option {
			stores[i] = NewMapCellStore()
			T := &testObjectType{name: "col", params: TypeParams{RNode: 2}, topics: []string{"seg/col/0"}, store: stores[i]}
			return []Option{
				WithGossipSubParams(params),
				WithMessageIdFn(seacastTestID),
				WithHaveBitmaps(seacastTestIndex),
				WithPhaseForwarding(func(topic string) bool { return topic != "seg/col/0" }, 2),
				WithSeacast([]ObjectType{T}, nil),
			}
		})
		seacastOrdZeroAtEnd(t, psubs)
		for _, ps := range psubs[:2] {
			if _, err := ps.Subscribe("seg/col/0"); err != nil {
				t.Fatal(err)
			}
		}
		a, b, c := psubs[0], psubs[1], psubs[2]
		gsA, gsC := a.rt.(*GossipSubRouter), c.rt.(*GossipSubRouter)
		connect(t, hosts[0], hosts[1])
		connect(t, hosts[0], hosts[2])
		time.Sleep(2 * time.Second)
		read := func(gs *GossipSubRouter, f func()) {
			done := make(chan struct{})
			gs.p.eval <- func() { f(); close(done) }
			<-done
		}
		cell := func(index byte) []byte { return append([]byte(root+string(index)), []byte(strings.Repeat("x", 64))...) }
		for _, i := range []byte{5, 6} {
			if err := b.Publish("seg/col/0", cell(i)); err != nil {
				t.Fatal(err)
			}
		}
		time.Sleep(time.Second)
		var held int
		read(gsA, func() { held = stores[0].Len() })
		if held != 2 {
			t.Fatalf("A holds %d cells, want 2", held)
		}
		ctrC := gsC.seacast.ctr

		// C asks A for cell 5: served, admitted on the unsubscribed topic, accepted, filed.
		if !c.SeacastAsk(hosts[0].ID(), root, 5) {
			t.Fatal("C's ask was not sent")
		}
		time.Sleep(time.Second)
		read(gsC, func() { held = stores[2].Len() })
		if held != 1 || ctrC.AdmittedByWant.Load() != 1 || ctrC.Accepted.Load() != 1 {
			t.Fatalf("C after its ask: store %d, admitted by WANT %d, accepted %d, want 1 1 1 (A served from the store %d, v4 WANTs %d)", held, ctrC.AdmittedByWant.Load(), ctrC.Accepted.Load(), gsA.seacast.ctr.ServedFromStore.Load(), gsA.seacast.ctr.WantsV4.Load())
		}
		read(gsA, func() {
			if e := gsA.have.peers[hosts[2].ID()][root]; e == nil || !e.bits.Has(5) {
				t.Errorf("A did not hear C's holding of the cell it served")
			}
		})

		// A copy of cell 6 from A, never asked: dropped before admission.
		inject := func(index byte) {
			// A faithful copy, the message as B published it (signed), from A's store (its message
			// cache has let the cells go by now).
			read(gsA, func() {
				m, ok := stores[0].Get(root, uint32(index))
				if !ok {
					t.Errorf("A's store does not hold cell %d", index)
					return
				}
				gsA.sendRPC(hosts[2].ID(), rpcWithControl([]*pb.Message{m.Message}, nil, nil, nil, nil, nil), false)
			})
		}
		inject(6)
		time.Sleep(time.Second)
		read(gsC, func() { held = stores[2].Len() })
		if held != 1 || ctrC.AdmittedByWant.Load() != 1 || ctrC.Arrived.Load() != 1 {
			t.Fatalf("C after an unasked copy: store %d, admitted by WANT %d, arrived %d, want 1 1 1", held, ctrC.AdmittedByWant.Load(), ctrC.Arrived.Load())
		}

		// A late duplicate of cell 5 from A: admitted (the registration lives) and acknowledged with
		// the plain v4 holding (the hose is off here): counted at C's send, heard at A as one more
		// v4 holding absorbed.
		heard := gsA.seacast.ctr.HoldingsV4.Load()
		inject(5)
		time.Sleep(time.Second)
		if ctrC.AdmittedByWant.Load() != 2 || ctrC.DupUnacked.Load() != 0 || ctrC.DupAcked.Load() != 1 || ctrC.Accepted.Load() != 1 {
			t.Fatalf("C after a late duplicate: admitted by WANT %d, dup unacked %d, dup acked %d, accepted %d, want 2 0 1 1", ctrC.AdmittedByWant.Load(), ctrC.DupUnacked.Load(), ctrC.DupAcked.Load(), ctrC.Accepted.Load())
		}
		if n := gsA.seacast.ctr.HoldingsV4.Load(); n != heard+1 {
			t.Fatalf("A absorbed %d v4 holdings after the duplicate it sent, want %d: C's acknowledgement did not arrive", n, heard+1)
		}

		// The group expired: the registration is gone, the same copy is dropped.
		c.SeacastExpire([]string{root})
		time.Sleep(100 * time.Millisecond)
		inject(5)
		time.Sleep(time.Second)
		if ctrC.AdmittedByWant.Load() != 2 {
			t.Fatalf("C admitted a copy after the group's expiry: admitted by WANT %d, want 2", ctrC.AdmittedByWant.Load())
		}
		var asked int
		read(gsC, func() { asked = len(gsC.seacast.asked) })
		if asked != 0 {
			t.Fatalf("C keeps %d asked groups after the expiry, want 0", asked)
		}
		_ = peer.ID("")
	})
}
