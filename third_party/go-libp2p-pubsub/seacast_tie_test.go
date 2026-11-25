package pubsub

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
)

// seacastTieCell is n routers on one owned topic with every gate closed (HeaderKnown false), the
// pull discipline, promises and parks, the offer table and v0.5's retry, as the harness's cells run
// them; the last router's validator counts its calls. Every host is connected to the last one.
type seacastTieCell struct {
	pubsubs   []*PubSub
	hosts     []host.Host
	stores    []*MapCellStore
	types     []*testObjectType
	validated atomic.Int64
	breaks    atomic.Int64
	parks     atomic.Int64
}

func newSeacastTieCell(t *testing.T, n int, limits ...SeacastLimits) *seacastTieCell {
	t.Helper()
	c := &seacastTieCell{}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	c.stores = make([]*MapCellStore, n)
	c.types = make([]*testObjectType, n)
	c.hosts = getDefaultHosts(t, n)
	params := DefaultGossipSubParams()
	params.IDontWantMessageThreshold = 1
	c.pubsubs = getGossipsubsOptFn(ctx, c.hosts, func(i int, h host.Host) []Option {
		c.stores[i] = NewMapCellStore()
		T := &testObjectType{name: "col", params: TypeParams{RNode: 2}, topics: []string{"seg/col/0"}, store: c.stores[i], gateClosed: true}
		c.types[i] = T
		opts := []Option{
			WithGossipSubParams(params),
			WithMessageIdFn(seacastTestID),
			WithHaveBitmaps(seacastTestIndex),
			WithPhaseForwarding(func(topic string) bool { return topic != "seg/col/0" }, 2),
			WithIWantDiscipline(200 * time.Millisecond),
			WithIHaveCommitmentPark(1, 400*time.Millisecond, 30*time.Second, &c.breaks, &c.parks),
			WithIHaveParkPerGroup(nil),
			WithOfferTable(nil),
			WithSeacast([]ObjectType{T}, nil),
			WithSpecV05(SpecV05{LastResort: true, CloseWithConn: true}, nil),
		}
		if len(limits) > 0 {
			opts = append(opts, WithSeacastLimits(limits[0]))
		}
		return opts
	})
	last := c.pubsubs[n-1]
	if err := last.RegisterTopicValidator("seg/col/0", func(context.Context, peer.ID, *Message) ValidationResult {
		c.validated.Add(1)
		return ValidationAccept
	}); err != nil {
		t.Fatal(err)
	}
	for _, ps := range c.pubsubs {
		if _, err := ps.Subscribe("seg/col/0"); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < n-1; i++ {
		connect(t, c.hosts[i], c.hosts[n-1])
	}
	time.Sleep(2 * time.Second)
	return c
}

func (c *seacastTieCell) gs(i int) *GossipSubRouter { return c.pubsubs[i].rt.(*GossipSubRouter) }

func (c *seacastTieCell) read(i int, f func(gs *GossipSubRouter)) {
	gs := c.gs(i)
	done := make(chan struct{})
	gs.p.eval <- func() { f(gs); close(done) }
	<-done
}

// offer sends, from router i to router j, an IHAVE naming mid on the topic.
func (c *seacastTieCell) offer(i, j int, mid string) {
	topic := "seg/col/0"
	to := c.hosts[j].ID()
	c.read(i, func(gs *GossipSubRouter) {
		gs.sendRPC(to, rpcWithControl(nil, []*pb.ControlIHave{{TopicID: &topic, MessageIDs: []string{mid}}}, nil, nil, nil, nil), false)
	})
	time.Sleep(100 * time.Millisecond)
}

// TestSeacastTiePassesOverSender: A pushes a cell whose header B has not validated, so B keeps it
// pending and ties A for the index; A's offer of the index is recorded and not asked for, C's is
// asked for; at the header the pending copy fills and the ties go.
func TestSeacastTiePassesOverSender(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		root := strings.Repeat("t", haveRootLen)
		c := newSeacastTieCell(t, 3) // A = 0, C = 1, B = 2 (the validator's)
		a, b := c.pubsubs[0], c.pubsubs[2]
		ctrB := c.gs(2).seacast.ctr
		if err := a.Publish("seg/col/0", seacastCell(root, 5)); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Second)
		if b.SeacastPending() != 1 || ctrB.Tied.Load() != 1 || b.SeacastTies() != 1 {
			t.Fatalf("after A's push: pending %d tied %d ties %d, want 1 1 1", b.SeacastPending(), ctrB.Tied.Load(), b.SeacastTies())
		}
		mid := root + string(byte(5))
		c.offer(0, 2, mid) // A offers what B already holds from it, unchecked
		var asked, offeredByA bool
		c.read(2, func(gs *GossipSubRouter) {
			_, asked = gs.iwantAsked[mid]
			_, offeredByA = gs.offerTable[mid][c.hosts[0].ID()]
		})
		if asked || !offeredByA || ctrB.TiedPassedOver.Load() != 1 {
			t.Fatalf("after A's offer: asked %v, A in the offer table %v, passed over %d; want false true 1", asked, offeredByA, ctrB.TiedPassedOver.Load())
		}
		c.offer(1, 2, mid) // C offers it: asked
		var promisedByC bool
		c.read(2, func(gs *GossipSubRouter) {
			_, asked = gs.iwantAsked[mid]
			_, promisedByC = gs.iwantPromised[mid][c.hosts[1].ID()]
		})
		if !asked || !promisedByC {
			t.Fatalf("after C's offer: asked %v, promised by C %v; want true true", asked, promisedByC)
		}
		c.types[2].gateClosed = false // B's application validated the header
		b.SeacastHeaderValidated([]string{root})
		time.Sleep(time.Second)
		var held int
		c.read(2, func(*GossipSubRouter) { held = c.stores[2].Len() })
		if held != 1 || ctrB.Accepted.Load() != 1 || b.SeacastTies() != 0 || b.SeacastPending() != 0 {
			t.Fatalf("after the header: held %d accepted %d ties %d pending %d; want 1 1 0 0", held, ctrB.Accepted.Load(), b.SeacastTies(), b.SeacastPending())
		}
	})
}

// TestSeacastTieClosesPromise: B asks A for a cell A offered; A answers with a copy B cannot check
// yet (the gate closed), which ties A and closes its promise without a charge: no break, no park.
// The control, where A never answers: the promise breaks and A is parked.
func TestSeacastTieClosesPromise(t *testing.T) {
	for _, answers := range []bool{false, true} {
		synctestTest(t, func(t *testing.T) {
			root := strings.Repeat("u", haveRootLen)
			c := newSeacastTieCell(t, 2) // A = 0, B = 1
			a, b := c.pubsubs[0], c.pubsubs[1]
			mid := root + string(byte(6))
			c.offer(0, 1, mid)
			var promised bool
			c.read(1, func(gs *GossipSubRouter) { _, promised = gs.iwantPromised[mid][c.hosts[0].ID()] })
			if !promised {
				t.Fatal("B did not ask A after its offer")
			}
			if answers {
				if err := a.Publish("seg/col/0", seacastCell(root, 6)); err != nil {
					t.Fatal(err)
				}
			}
			time.Sleep(2 * time.Second) // past the promise and the retry
			ctrB := c.gs(1).seacast.ctr
			if answers {
				if c.breaks.Load() != 0 || c.parks.Load() != 0 || b.SeacastPending() != 1 || ctrB.Tied.Load() != 1 {
					t.Fatalf("A answered with a copy that waits: breaks %d parks %d pending %d tied %d; want 0 0 1 1", c.breaks.Load(), c.parks.Load(), b.SeacastPending(), ctrB.Tied.Load())
				}
			} else if c.breaks.Load() < 1 || c.parks.Load() != 1 {
				t.Fatalf("control, A never answers: breaks %d parks %d; want >= 1 and 1", c.breaks.Load(), c.parks.Load())
			}
		})
	}
}

// TestSeacastTieForgeryAtHeader: a pending copy naming an index beyond the type's count is dropped
// at the header as a forgery, never validated, with its sender's offer of the index; a copy with an
// index the header admits is validated and filled.
func TestSeacastTieForgeryAtHeader(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		root := strings.Repeat("f", haveRootLen)
		c := newSeacastTieCell(t, 2)
		a, b := c.pubsubs[0], c.pubsubs[1]
		ctrB := c.gs(1).seacast.ctr
		for _, i := range []byte{40, 3} { // the test type's IndexCount is 32
			if err := a.Publish("seg/col/0", seacastCell(root, i)); err != nil {
				t.Fatal(err)
			}
		}
		time.Sleep(time.Second)
		if b.SeacastPending() != 2 || ctrB.Tied.Load() != 2 {
			t.Fatalf("pending %d tied %d, want 2 2", b.SeacastPending(), ctrB.Tied.Load())
		}
		mid40 := root + string(byte(40))
		c.offer(0, 1, mid40)
		var offeredByA bool
		c.read(1, func(gs *GossipSubRouter) { _, offeredByA = gs.offerTable[mid40][c.hosts[0].ID()] })
		if !offeredByA {
			t.Fatal("A's offer of index 40 was not recorded")
		}
		c.types[1].gateClosed = false // B's application validated the header
		b.SeacastHeaderValidated([]string{root})
		time.Sleep(time.Second)
		var held int
		c.read(1, func(gs *GossipSubRouter) {
			held = c.stores[1].Len()
			_, offeredByA = gs.offerTable[mid40][c.hosts[0].ID()]
		})
		if held != 1 || c.validated.Load() != 1 || ctrB.Forged.Load() != 1 || ctrB.Accepted.Load() != 1 || offeredByA || b.SeacastTies() != 0 {
			t.Fatalf("after the header: held %d validated %d forged %d accepted %d, A still offers 40 %v, ties %d; want 1 1 1 1 false 0", held, c.validated.Load(), ctrB.Forged.Load(), ctrB.Accepted.Load(), offeredByA, b.SeacastTies())
		}
	})
}

// TestSeacastTieConnection: a copy refused at the per-peer bound still ties its sender; a sender's
// pending copies and ties go with its connection, its reservation released, so the same id from
// another sender afterwards is a fresh copy, not a duplicate of a pending one.
func TestSeacastTieConnection(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		root := strings.Repeat("c", haveRootLen)
		c := newSeacastTieCell(t, 3) // A = 0, C = 1, B = 2
		a, b := c.pubsubs[0], c.pubsubs[2]
		ctrB := c.gs(2).seacast.ctr
		c.read(2, func(gs *GossipSubRouter) { gs.seacast.limits.PendingPerPeer = 1 })
		for _, i := range []byte{1, 2} {
			if err := a.Publish("seg/col/0", seacastCell(root, i)); err != nil {
				t.Fatal(err)
			}
		}
		time.Sleep(time.Second)
		if b.SeacastPending() != 1 || ctrB.PendingRefused.Load() != 1 || ctrB.Tied.Load() != 2 {
			t.Fatalf("at the bound: pending %d refused %d tied %d, want 1 1 2", b.SeacastPending(), ctrB.PendingRefused.Load(), ctrB.Tied.Load())
		}
		if err := c.hosts[0].Network().ClosePeer(c.hosts[2].ID()); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Second)
		if b.SeacastPending() != 0 || ctrB.PendingGone.Load() != 1 || b.SeacastTies() != 0 {
			t.Fatalf("after A's connection closed: pending %d gone %d ties %d, want 0 1 0", b.SeacastPending(), ctrB.PendingGone.Load(), b.SeacastTies())
		}
		// C sends the id A's copy carried: B's pending entry for it went with A, so it is fresh.
		if err := c.pubsubs[1].Publish("seg/col/0", seacastCell(root, 1)); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Second)
		if b.SeacastPending() != 1 || ctrB.PendingDup.Load() != 0 || ctrB.Tied.Load() != 3 {
			t.Fatalf("C's copy of the id after A left: pending %d dup %d tied %d, want 1 0 3", b.SeacastPending(), ctrB.PendingDup.Load(), ctrB.Tied.Load())
		}
	})
}
