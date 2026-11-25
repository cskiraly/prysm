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

// TestSeacastStoreServes is the type's store on the serve path: B publishes a cell that A accepts
// and files in its type's store (and B files its own); C asks A for it by index with a v4 WANT four
// times across A's message cache's lifetime: the cache answers twice, the store once (counted as
// an answer), and the fourth ask is refused at the retransmission bound, one count per (id, peer)
// whichever answered; an ask for an index nobody holds is answered by nothing; the expiry forgets
// the counts.
func TestSeacastStoreServes(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		root := strings.Repeat("s", haveRootLen)
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
		subs := make([]*Subscription, 3)
		for i, ps := range psubs {
			sub, err := ps.Subscribe("seg/col/0")
			if err != nil {
				t.Fatal(err)
			}
			subs[i] = sub
		}
		a, b, c := psubs[0], psubs[1], psubs[2]
		gsA, gsC := a.rt.(*GossipSubRouter), c.rt.(*GossipSubRouter)
		connect(t, hosts[0], hosts[1])
		time.Sleep(2 * time.Second)
		read := func(gs *GossipSubRouter, f func()) {
			done := make(chan struct{})
			gs.p.eval <- func() { f(); close(done) }
			<-done
		}
		cell := append([]byte(root+string(byte(5))), []byte(strings.Repeat("x", 64))...)

		// B publishes; A accepts and files the cell; B files its own.
		if err := b.Publish("seg/col/0", cell); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Second)
		var held int
		read(gsA, func() { held = stores[0].Len() })
		if held != 1 {
			t.Fatalf("A's store holds %d cells after the accept, want 1", held)
		}
		read(b.rt.(*GossipSubRouter), func() { held = stores[1].Len() })
		if held != 1 {
			t.Fatalf("B's store holds %d cells after its own publish, want 1", held)
		}
		if n := gsA.seacast.ctr.Stored.Load(); n != 1 {
			t.Fatalf("A stored %d, want 1", n)
		}

		// C joins A. Its validator ignores the cell, so C never holds it and never tells A it does
		// (a holder's later asks are dropped as stale, havePeerHolds); it asks A by index four times
		// across the cache's lifetime: the cache answers the first two, the store the third (counted
		// as an answer), the fourth is refused at the retransmission bound, one count per (id, peer)
		// whichever answered (draft §4.5: three answers per id per peer).
		if err := c.RegisterTopicValidator("seg/col/0", func(context.Context, peer.ID, *Message) ValidationResult { return ValidationIgnore }); err != nil {
			t.Fatal(err)
		}
		connect(t, hosts[0], hosts[2])
		time.Sleep(2 * time.Second)
		ctrA, ctrC := gsA.seacast.ctr, gsC.seacast.ctr
		ask := func(index uint32) {
			read(gsC, func() {
				gsC.sendRPC(hosts[0].ID(), rpcWithControl(nil, nil, []*pb.ControlIWant{{MessageIDs: []string{encodeWantV2(root, index, false, 0)}}}, nil, nil, nil), false)
			})
			time.Sleep(time.Second)
		}
		var count int
		countFor := func() int {
			read(gsA, func() { count = gsA.seacast.serveTx[root][5][hosts[2].ID()] })
			return count
		}
		ask(5)
		if ctrC.Arrived.Load() != 1 || ctrC.Ignored.Load() != 1 || countFor() != 1 || ctrA.ServedFromStore.Load() != 0 {
			t.Fatalf("after the first ask: C arrived %d ignored %d (want 1 1); A's count for C %d (want 1), served from the store %d (want 0: the cache answered)", ctrC.Arrived.Load(), ctrC.Ignored.Load(), count, ctrA.ServedFromStore.Load())
		}
		ask(5)
		if countFor() != 2 || ctrA.ServedFromStore.Load() != 0 || ctrC.Arrived.Load() != 1 {
			t.Fatalf("after the second ask: A's count for C %d (want 2), served from the store %d (want 0); C arrived %d (want 1: a duplicate)", count, ctrA.ServedFromStore.Load(), ctrC.Arrived.Load())
		}
		// A's message cache lets the cell go.
		read(gsA, func() {
			for range gsA.params.HistoryLength + 1 {
				gsA.mcache.Shift()
			}
			if _, ok := gsA.mcache.Get(seacastTestID(&pb.Message{Data: cell})); ok {
				t.Errorf("A's message cache still holds the cell after the shifts")
			}
		})
		ask(5)
		if countFor() != 3 || ctrA.ServedFromStore.Load() != 1 {
			t.Fatalf("after the third ask: A's count for C %d (want 3), served from the store %d (want 1)", count, ctrA.ServedFromStore.Load())
		}
		ask(5)
		if countFor() != 4 || ctrA.ServedFromStore.Load() != 1 {
			t.Fatalf("after the fourth ask: A's count for C %d (want 4), served from the store %d (want 1: the fourth refused at the bound)", count, ctrA.ServedFromStore.Load())
		}

		// An ask for an index nobody holds: nothing served, nothing counted.
		ask(6)
		if n := ctrA.ServedFromStore.Load(); n != 1 {
			t.Fatalf("A served from the store %d after an ask for a cell it does not hold, want 1", n)
		}

		// The group expired: the served counts are forgotten.
		a.SeacastExpire([]string{root})
		time.Sleep(100 * time.Millisecond)
		read(gsA, func() { count = len(gsA.seacast.serveTx[root]) })
		if count != 0 {
			t.Fatalf("A keeps %d served counts for the expired group, want 0", count)
		}
	})
}
