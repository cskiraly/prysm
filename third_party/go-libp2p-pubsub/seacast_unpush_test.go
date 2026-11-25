package pubsub

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
)

// TestSeacastUnpushSourcePool is seacast.md §4.7's unpush at a source (spec 43fe953): a chosen push the
// queue refuses is owed as an offer to a peer of the pool the push was planned from, the relation peers or,
// with none, the links of the type (source_pool's fallback, gossipsub's fanout), not to relation peers only.
// S (router 0) has joined the column topic without subscribing, so it has no mesh for it and publishes to
// its fanout, P (router 1); S's queue to P holds two entries, and S publishes a group of 8 at once under
// v0.5's A′. Before, a refused push to P (no mesh peer) was forgotten: P got only what fit. After, each is
// owed to P as an offer, P asks, and P gets all 8.
func TestSeacastUnpushSourcePool(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		const topic = "seg/col/0"
		ctr := &SpecV05Counters{}
		hosts := v05Hosts(t, 2, 20, 25*time.Millisecond)
		params := DefaultGossipSubParams()
		params.IDontWantMessageThreshold = 1
		params.Dlazy = 0
		params.GossipFactor = 0
		psubs := getGossipsubsOptFn(ctx, hosts, func(i int, _ host.Host) []Option {
			T := &testObjectType{name: "col", params: TypeParams{RNode: 2}, topics: []string{topic}, store: NewMapCellStore()}
			opts := []Option{
				WithGossipSubParams(params), WithMessageSignaturePolicy(StrictNoSign), WithNoAuthor(),
				WithMessageIdFn(seacastTestID), WithHaveBitmaps(seacastTestIndex), WithHaveAnnounceBitmaps(),
				WithPhaseForwarding(func(tp string) bool { return tp != topic }, 2),
				WithIWantDiscipline(200 * time.Millisecond),
				WithSeacast([]ObjectType{T}, nil),
				WithSpecV05(SpecV05{OfferSkipped: true}, ctr),
			}
			if i == 0 {
				opts = append(opts, WithPeerOutboundQueueSize(2))
			}
			return opts
		})
		var tps []*Topic
		for _, ps := range psubs {
			tp, err := ps.Join(topic)
			if err != nil {
				t.Fatal(err)
			}
			tps = append(tps, tp)
		}
		sub, err := tps[1].Subscribe() // P only: S publishes to the topic by its fanout
		if err != nil {
			t.Fatal(err)
		}
		var mu sync.Mutex
		got := map[uint32]bool{}
		go func() {
			for {
				m, err := sub.Next(ctx)
				if err != nil {
					return
				}
				if _, i, ok := seacastTestIndex(seacastTestID(m.Message)); ok {
					mu.Lock()
					got[i] = true
					mu.Unlock()
				}
			}
		}()
		connect(t, hosts[0], hosts[1])
		time.Sleep(2 * time.Second)
		gsS := psubs[0].rt.(*GossipSubRouter)
		var meshS int
		done := make(chan struct{})
		gsS.p.eval <- func() { meshS = len(gsS.mesh[topic]); close(done) }
		<-done
		if meshS != 0 {
			t.Fatalf("the scene: S has %d mesh peers on the topic, want 0", meshS)
		}
		root := strings.Repeat("u", haveRootLen)
		var batch MessageBatch
		for i := byte(0); i < 8; i++ {
			if err := tps[0].AddToBatch(ctx, &batch, seacastCell(root, i)); err != nil {
				t.Fatal(err)
			}
		}
		if err := psubs[0].PublishBatch(&batch); err != nil {
			t.Fatal(err)
		}
		time.Sleep(3 * time.Second)
		mu.Lock()
		n := len(got)
		mu.Unlock()
		t.Logf("P got %d of 8; unpushed %d, offers owed %d, owed sent %d", n, ctr.Unpushed.Load(), ctr.OffersOwed.Load(), ctr.OwedSent.Load())
		if ctr.Unpushed.Load() == 0 {
			t.Fatalf("the scene: no push was refused (unpushed 0)")
		}
		if ctr.OffersOwed.Load() == 0 || n != 8 {
			t.Errorf("a source's refused push to its fallback link: owed %d, P got %d of 8; want owed > 0 and 8", ctr.OffersOwed.Load(), n)
		}
	})
}
