package pubsub

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/host"
)

// seacastF13Cell is two routers, A (0) offering and B (1) asking, with the pull discipline, a
// 60 s promise (longer than the offer table's 30 s), parks, the offer table, HAVE dispatch on the
// queues (the writer's held-ask filter) and v0.5's retry.
type seacastF13Cell struct {
	pubsubs []*PubSub
	hosts   []host.Host
	breaks  atomic.Int64
	parks   atomic.Int64
}

func newSeacastF13Cell(t *testing.T) *seacastF13Cell {
	t.Helper()
	c := &seacastF13Cell{}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	c.hosts = getDefaultHosts(t, 2)
	params := DefaultGossipSubParams()
	params.IDontWantMessageThreshold = 1
	c.pubsubs = getGossipsubsOptFn(ctx, c.hosts, func(i int, h host.Host) []Option {
		T := &testObjectType{name: "col", params: TypeParams{RNode: 2}, topics: []string{"seg/col/0"}, store: NewMapCellStore()}
		return []Option{
			WithGossipSubParams(params),
			WithMessageIdFn(seacastTestID),
			WithHaveBitmaps(seacastTestIndex),
			WithHaveDispatch(true, false),
			WithPhaseForwarding(func(topic string) bool { return topic != "seg/col/0" }, 2),
			WithIWantDiscipline(200 * time.Millisecond),
			WithIHaveCommitmentPark(1, 60*time.Second, 30*time.Second, &c.breaks, &c.parks),
			WithIHaveParkPerGroup(nil),
			WithOfferTable(nil),
			WithSpecV05(SpecV05{LastResort: true, CloseWithConn: true}, nil),
			WithSeacast([]ObjectType{T}, nil),
		}
	})
	for _, ps := range c.pubsubs {
		if _, err := ps.Subscribe("seg/col/0"); err != nil {
			t.Fatal(err)
		}
	}
	connect(t, c.hosts[0], c.hosts[1])
	time.Sleep(2 * time.Second)
	return c
}

func (c *seacastF13Cell) gs(i int) *GossipSubRouter { return c.pubsubs[i].rt.(*GossipSubRouter) }

func (c *seacastF13Cell) read(i int, f func(gs *GossipSubRouter)) {
	gs := c.gs(i)
	done := make(chan struct{})
	gs.p.eval <- func() { f(gs); close(done) }
	<-done
}

func (c *seacastF13Cell) offer(mid string) {
	topic := "seg/col/0"
	to := c.hosts[1].ID()
	c.read(0, func(gs *GossipSubRouter) {
		gs.sendRPC(to, rpcWithControl(nil, []*pb.ControlIHave{{TopicID: &topic, MessageIDs: []string{mid}}}, nil, nil, nil, nil), false)
	})
	time.Sleep(50 * time.Millisecond)
}

// TestSeacastAskGoesWithOffer: B asks A for a cell A offered and never serves, with a 60 s promise.
// When the offer table drops A's offer thirty seconds on, A's promise closes with it, so no break
// falls due at 60 s. The control: A renews its offer at 20 s and 45 s, the offer lives past 60 s
// and the promise breaks.
func TestSeacastAskGoesWithOffer(t *testing.T) {
	for _, renew := range []bool{false, true} {
		synctestTest(t, func(t *testing.T) {
			root := strings.Repeat("d", haveRootLen)
			c := newSeacastF13Cell(t)
			mid := root + string(byte(2))
			A := c.hosts[0].ID()
			ctrB := c.gs(1).seacast.ctr
			c.offer(mid)
			var promised, offered bool
			c.read(1, func(gs *GossipSubRouter) {
				_, promised = gs.iwantPromised[mid][A]
				_, offered = gs.offerTable[mid][A]
			})
			if !promised || !offered {
				t.Fatalf("after A's offer: promised %v offered %v, want both", promised, offered)
			}
			if renew {
				time.Sleep(20 * time.Second)
				c.offer(mid)
				time.Sleep(25 * time.Second)
				c.offer(mid)
				time.Sleep(16 * time.Second) // 61 s
			} else {
				time.Sleep(31 * time.Second)
				c.read(1, func(gs *GossipSubRouter) {
					_, promised = gs.iwantPromised[mid][A]
					_, offered = gs.offerTable[mid][A]
				})
				if promised || offered || ctrB.F13Closed.Load() != 1 {
					t.Fatalf("31 s on, A's offer swept: promised %v offered %v closed-with-offer %d; want false false 1", promised, offered, ctrB.F13Closed.Load())
				}
				time.Sleep(30 * time.Second) // 61 s
			}
			if renew {
				if c.breaks.Load() != 1 || c.parks.Load() != 1 {
					t.Fatalf("control, the offer renewed: breaks %d parks %d, want 1 1", c.breaks.Load(), c.parks.Load())
				}
			} else if c.breaks.Load() != 0 || c.parks.Load() != 0 {
				t.Fatalf("the offer dropped at 30 s: breaks %d parks %d at 61 s, want 0 0", c.breaks.Load(), c.parks.Load())
			}
		})
	}
}

// TestSeacastDroppedAskLeavesWriter: an ask queued to A when A's offer is dropped is marked for A's
// writer, whose held-ask filter answers once for it and not again.
func TestSeacastDroppedAskLeavesWriter(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		root := strings.Repeat("e", haveRootLen)
		c := newSeacastF13Cell(t)
		mid := root + string(byte(4))
		A := c.hosts[0].ID()
		ctrB := c.gs(1).seacast.ctr
		var open, first, second bool
		c.read(1, func(gs *GossipSubRouter) {
			gs.seacastAskQueued(A, []string{mid}) // as a dispatch records it, before the writer's report
			open = gs.seacastOfferDropped(A, mid)
			q := gs.haveQueue(A)
			if q == nil || q.have == nil || q.have.ownHeld == nil {
				t.Errorf("A's queue has no held-ask filter")
				return
			}
			first = q.have.ownHeld(mid)
			second = q.have.ownHeld(mid)
		})
		if !open || !first || second || ctrB.F13AskDropped.Load() != 1 {
			t.Fatalf("open %v, filter first %v second %v, asks dropped %d; want true true false 1", open, first, second, ctrB.F13AskDropped.Load())
		}
	})
}
