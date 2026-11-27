package pubsub

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
)

// seacastWindowCell is two routers on one owned topic, every root of slots placed by the type, B's
// validator counting its calls; A publishes, B is read.
type seacastWindowCell struct {
	a, b       *PubSub
	gsA, gsB   *GossipSubRouter
	stores     []*MapCellStore
	validated  atomic.Int64
	hosts      []host.Host
	breaks     atomic.Int64
	parks      atomic.Int64
	withPull   bool
	pubsubs    []*PubSub
	cancelFunc context.CancelFunc
}

func newSeacastWindowCell(t *testing.T, slots map[string]uint64, withPull bool) *seacastWindowCell {
	t.Helper()
	c := &seacastWindowCell{withPull: withPull}
	ctx, cancel := context.WithCancel(context.Background())
	c.cancelFunc = cancel
	t.Cleanup(cancel)
	c.stores = make([]*MapCellStore, 2)
	c.hosts = getDefaultHosts(t, 2)
	params := DefaultGossipSubParams()
	params.IDontWantMessageThreshold = 1
	c.pubsubs = getGossipsubsOptFn(ctx, c.hosts, func(i int, h host.Host) []Option {
		c.stores[i] = NewMapCellStore()
		T := &testObjectType{name: "col", params: TypeParams{RNode: 2}, topics: []string{"seg/col/0"}, store: c.stores[i], slots: slots}
		opts := []Option{
			WithGossipSubParams(params),
			WithMessageIdFn(seacastTestID),
			WithHaveBitmaps(seacastTestIndex),
			WithPhaseForwarding(func(topic string) bool { return topic != "seg/col/0" }, 2),
		}
		if withPull {
			// The pull discipline, promises and parks, the offer table and v0.5's retry and expiry,
			// as the harness's cells run them: what the window has to drop.
			opts = append(opts,
				WithIWantDiscipline(200*time.Millisecond),
				WithIHaveCommitmentPark(1, 400*time.Millisecond, 30*time.Second, &c.breaks, &c.parks),
				WithIHaveParkPerGroup(nil),
				WithOfferTable(nil),
			)
		}
		opts = append(opts, WithSeacast([]ObjectType{T}, nil))
		if withPull {
			opts = append(opts, WithSpecV05(SpecV05{}, nil))
		}
		return opts
	})
	c.a, c.b = c.pubsubs[0], c.pubsubs[1]
	c.gsA, c.gsB = c.a.rt.(*GossipSubRouter), c.b.rt.(*GossipSubRouter)
	if err := c.b.RegisterTopicValidator("seg/col/0", func(context.Context, peer.ID, *Message) ValidationResult {
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
	connect(t, c.hosts[0], c.hosts[1])
	time.Sleep(2 * time.Second)
	return c
}

func (c *seacastWindowCell) read(gs *GossipSubRouter, f func()) {
	done := make(chan struct{})
	gs.p.eval <- func() { f(); close(done) }
	<-done
}

func seacastCell(root string, index byte) []byte {
	return append([]byte(root+string(index)), []byte(strings.Repeat("x", 64))...)
}

// TestSeacastWindowRefusesBeforeValidation: at B's slot 5 with a window of 2, a chunk of a group at
// slot 3 is refused at admission (B's validator never runs, nothing is held); one at slot 4 is
// validated and filled; one at slot 9 is ignored as a chunk of a future slot and makes no state,
// and is admitted once B's slot reaches 9.
func TestSeacastWindowRefusesBeforeValidation(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		r3, r4, r9 := strings.Repeat("3", haveRootLen), strings.Repeat("4", haveRootLen), strings.Repeat("9", haveRootLen)
		c := newSeacastWindowCell(t, map[string]uint64{r3: 3, r4: 4, r9: 9}, false)
		ctrB := c.gsB.seacast.ctr
		c.b.SeacastAdvanceSlot(5)
		if s, ok := c.b.SeacastSlot(); !ok || s != 5 {
			t.Fatalf("B's slot %d %v, want 5 true", s, ok)
		}

		if err := c.a.Publish("seg/col/0", seacastCell(r3, 1)); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Second)
		var held int
		c.read(c.gsB, func() { held = c.stores[1].Len() })
		// Refused at admission, before Preprocess: not an arrival, never validated, nothing held.
		if ctrB.ExpiredDropped.Load() != 1 || c.validated.Load() != 0 || held != 0 || ctrB.Arrived.Load() != 0 {
			t.Fatalf("a chunk of slot 3 at slot 5: expired-dropped %d, validated %d, held %d, arrived %d; want 1 0 0 0", ctrB.ExpiredDropped.Load(), c.validated.Load(), held, ctrB.Arrived.Load())
		}
		c.read(c.gsB, func() {
			if m := c.gsB.have.mine[seacastGID(r3)]; m != nil {
				t.Errorf("B made holdings for a group out of the window")
			}
		})

		if err := c.a.Publish("seg/col/0", seacastCell(r4, 1)); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Second)
		c.read(c.gsB, func() { held = c.stores[1].Len() })
		if c.validated.Load() != 1 || held != 1 || ctrB.Accepted.Load() != 1 {
			t.Fatalf("a chunk of slot 4 at slot 5: validated %d, held %d, accepted %d; want 1 1 1", c.validated.Load(), held, ctrB.Accepted.Load())
		}

		if err := c.a.Publish("seg/col/0", seacastCell(r9, 1)); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Second)
		c.read(c.gsB, func() { held = c.stores[1].Len() })
		if ctrB.Future.Load() != 1 || c.validated.Load() != 1 || held != 1 {
			t.Fatalf("a chunk of slot 9 at slot 5: future %d, validated %d, held %d; want 1 1 1", ctrB.Future.Load(), c.validated.Load(), held)
		}
		c.read(c.gsB, func() {
			if m := c.gsB.have.mine[seacastGID(r9)]; m != nil {
				t.Errorf("B made holdings for a group of a future slot")
			}
		})

		c.b.SeacastAdvanceSlot(9)
		if err := c.a.Publish("seg/col/0", seacastCell(r9, 2)); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Second)
		c.read(c.gsB, func() { held = c.stores[1].Len() })
		if c.validated.Load() != 2 || held != 2 || ctrB.Accepted.Load() != 2 || ctrB.Future.Load() != 1 {
			t.Fatalf("a chunk of slot 9 at slot 9: validated %d, held %d, accepted %d, future %d; want 2 2 2 1", c.validated.Load(), held, ctrB.Accepted.Load(), ctrB.Future.Load())
		}
	})
}

// TestSeacastWindowTombstone: a group the application expires at slot 5 leaves a tombstone that keeps
// its later copies out; when B's slot reaches 6 the window has passed the group's slot 4, the
// tombstone goes, and the window keeps the group out from then on.
func TestSeacastWindowTombstone(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		r4 := strings.Repeat("4", haveRootLen)
		c := newSeacastWindowCell(t, map[string]uint64{r4: 4}, false)
		ctrB := c.gsB.seacast.ctr
		c.b.SeacastAdvanceSlot(5)
		if err := c.a.Publish("seg/col/0", seacastCell(r4, 1)); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Second)
		if ctrB.Accepted.Load() != 1 {
			t.Fatalf("accepted %d, want 1", ctrB.Accepted.Load())
		}
		c.b.SeacastExpire([]string{r4})
		time.Sleep(100 * time.Millisecond)
		if n := c.b.SeacastTombstones(); n != 1 {
			t.Fatalf("tombstones after the expiry %d, want 1", n)
		}
		if err := c.a.Publish("seg/col/0", seacastCell(r4, 2)); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Second)
		if ctrB.ExpiredDropped.Load() != 1 || c.validated.Load() != 1 {
			t.Fatalf("a copy after the expiry: expired-dropped %d, validated %d; want 1 1", ctrB.ExpiredDropped.Load(), c.validated.Load())
		}

		c.b.SeacastAdvanceSlot(6)
		time.Sleep(100 * time.Millisecond)
		if n := c.b.SeacastTombstones(); n != 0 || ctrB.TombstonesDropped.Load() != 1 {
			t.Fatalf("tombstones once the window passed slot 4: %d (dropped %d), want 0 (1)", n, ctrB.TombstonesDropped.Load())
		}
		if err := c.a.Publish("seg/col/0", seacastCell(r4, 3)); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Second)
		if ctrB.ExpiredDropped.Load() != 2 || c.validated.Load() != 1 || ctrB.WindowExpired.Load() != 0 {
			t.Fatalf("a copy after the tombstone went: expired-dropped %d, validated %d, window-expired %d; want 2 1 0", ctrB.ExpiredDropped.Load(), c.validated.Load(), ctrB.WindowExpired.Load())
		}
	})
}

// TestSeacastWindowForgetsTheRoot: once the window has passed the slot of a group the type places, the
// path's registry holds nothing of its root; a later chunk of the group is placed again by the type's
// slot and dropped as expired before validation.
func TestSeacastWindowForgetsTheRoot(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		r4 := strings.Repeat("4", haveRootLen)
		c := newSeacastWindowCell(t, map[string]uint64{r4: 4}, false)
		ctrB := c.gsB.seacast.ctr
		c.b.SeacastAdvanceSlot(5)
		if err := c.a.Publish("seg/col/0", seacastCell(r4, 1)); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Second)
		registered := func() (roots, topics, known bool) {
			c.read(c.gsB, func() {
				sc := c.gsB.seacast
				_, roots = sc.roots[r4]
				_, topics = sc.rootTopic[r4]
				_, known = sc.knownAt[r4]
			})
			return
		}
		if roots, topics, known := registered(); !roots || !topics || !known || c.validated.Load() != 1 {
			t.Fatalf("after a chunk at slot 5: registered %v %v %v, validated %d; want true true true 1", roots, topics, known, c.validated.Load())
		}
		c.b.SeacastAdvanceSlot(6)
		time.Sleep(100 * time.Millisecond)
		if roots, topics, known := registered(); roots || topics || known {
			t.Fatalf("once the window passed slot 4: registered %v %v %v, want false false false", roots, topics, known)
		}
		if err := c.a.Publish("seg/col/0", seacastCell(r4, 2)); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Second)
		if ctrB.ExpiredDropped.Load() != 1 || c.validated.Load() != 1 {
			t.Fatalf("a later chunk: expired-dropped %d, validated %d; want 1 1", ctrB.ExpiredDropped.Load(), c.validated.Load())
		}
	})
}

// TestSeacastWindowMonotone: the node's slot never goes back; and WithSeacastLimits sets the window
// after WithSeacast, refusing a zero window or an order without the path.
func TestSeacastWindowMonotone(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		c := newSeacastWindowCell(t, map[string]uint64{}, false)
		c.b.SeacastAdvanceSlot(7)
		c.b.SeacastAdvanceSlot(5)
		s, ok := c.b.SeacastSlot()
		if !ok || s != 7 || c.gsB.seacast.ctr.SlotRegress.Load() != 1 {
			t.Fatalf("slot %d %v, regressions %d; want 7 true 1", s, ok, c.gsB.seacast.ctr.SlotRegress.Load())
		}
		if err := WithSeacastLimits(SeacastLimits{PendingPerPeer: 8, LiveWindow: 3})(c.b); err != nil {
			t.Fatal(err)
		}
		var lim SeacastLimits
		c.read(c.gsB, func() { lim = c.gsB.seacast.limits })
		if lim.PendingPerPeer != 8 || lim.LiveWindow != 3 {
			t.Fatalf("limits %+v, want pending 8 window 3", lim)
		}
		if err := WithSeacastLimits(SeacastLimits{PendingPerPeer: 8})(c.b); err == nil {
			t.Fatal("a zero window was accepted")
		}
	})
}

// TestSeacastWindowDropsPullState: B has asked A for a chunk A announced and never serves (the
// promise open, the offer recorded). When B's slot moves so the group's slot leaves the window, the
// group's pull state goes at once: the promise's deadline and the retry fire on nothing (no break,
// no park, no new ask), and the offer is gone. The control, without the advance: the promise breaks.
func TestSeacastWindowDropsPullState(t *testing.T) {
	for _, advance := range []bool{false, true} {
		synctestTest(t, func(t *testing.T) {
			r4 := strings.Repeat("4", haveRootLen)
			c := newSeacastWindowCell(t, map[string]uint64{r4: 4}, true)
			ctrB := c.gsB.seacast.ctr
			c.b.SeacastAdvanceSlot(5)
			mid := r4 + string(byte(5))
			topic := "seg/col/0"
			peerB := c.hosts[1].ID()
			// A's offer of a cell it does not hold, as an IHAVE.
			c.read(c.gsA, func() {
				c.gsA.sendRPC(peerB, rpcWithControl(nil, []*pb.ControlIHave{{TopicID: &topic, MessageIDs: []string{mid}}}, nil, nil, nil, nil), false)
			})
			time.Sleep(100 * time.Millisecond)
			var asked, promised, offered bool
			c.read(c.gsB, func() {
				asked = c.gsB.pullAsked(mid)
				promised = c.gsB.pullPromised(c.hosts[0].ID(), mid)
				offered = c.gsB.pullOfferedByAny(mid)
			})
			if !asked || !promised || !offered {
				t.Fatalf("after A's offer: asked %v promised %v offered %v, want all true", asked, promised, offered)
			}
			if advance {
				c.b.SeacastAdvanceSlot(6) // slot 4 leaves the window
				time.Sleep(10 * time.Millisecond)
				if ctrB.WindowExpired.Load() != 1 {
					t.Fatalf("window-expired %d, want 1", ctrB.WindowExpired.Load())
				}
			}
			time.Sleep(2 * time.Second) // past the promise (400 ms), the retry and a heartbeat
			c.read(c.gsB, func() {
				asked = c.gsB.pullAsked(mid)
				promised = c.gsB.pullPromised(c.hosts[0].ID(), mid)
				offered = c.gsB.pullOfferedByAny(mid)
			})
			if advance {
				if c.breaks.Load() != 0 || c.parks.Load() != 0 || asked || promised || offered {
					t.Fatalf("after the window passed: breaks %d parks %d asked %v promised %v offered %v; want 0 0 false false false", c.breaks.Load(), c.parks.Load(), asked, promised, offered)
				}
			} else if c.breaks.Load() < 1 || c.parks.Load() != 1 {
				// The promise breaks and A is parked (k = 1); the last resort then asks the parked
				// announcer once more and that promise breaks too, so breaks are at least one.
				t.Fatalf("control, no advance: breaks %d parks %d, want >= 1 and 1", c.breaks.Load(), c.parks.Load())
			}
		})
	}
}

// TestSeacastAdvanceSlotWithRunsOnTheRouter: the hook of SeacastAdvanceSlotWith runs on the router's
// goroutine, after the router took the slot and before the call returns, so nothing else the router
// runs falls between the two advances.
func TestSeacastAdvanceSlotWithRunsOnTheRouter(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		c := newSeacastWindowCell(t, nil, false)
		var saw uint64
		var hookDone bool
		var probeRan atomic.Bool
		c.b.SeacastAdvanceSlotWith(5, func(slot uint64) {
			saw = c.gsB.seacast.slot
			go func() {
				c.b.eval <- func() { probeRan.Store(true) }
			}()
			synctest.Wait()
			if probeRan.Load() {
				t.Error("the router ran a probe while the hook ran: the hook is not on the router's goroutine")
			}
			hookDone = true
		})
		if !hookDone {
			t.Fatal("the call returned before its hook ran")
		}
		if saw != 5 {
			t.Fatalf("the hook saw the router's slot %d, want 5", saw)
		}
	})
}

// TestSeacastWindowForgetsAnUnplacedRoot: a root registered by an offer whose chunk never validates
// has no slot the type can give; the path keeps the slot it registered the root at, and the window
// forgets the root, and drops its pull state, once that slot has left it.
func TestSeacastWindowForgetsAnUnplacedRoot(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		r7 := strings.Repeat("7", haveRootLen)
		c := newSeacastWindowCell(t, map[string]uint64{}, true)
		c.b.SeacastAdvanceSlot(5)
		mid := r7 + string(byte(5))
		topic := "seg/col/0"
		peerB := c.hosts[1].ID()
		c.read(c.gsA, func() {
			c.gsA.sendRPC(peerB, rpcWithControl(nil, []*pb.ControlIHave{{TopicID: &topic, MessageIDs: []string{mid}}}, nil, nil, nil, nil), false)
		})
		time.Sleep(100 * time.Millisecond)
		state := func() (registered, tomb, offered bool) {
			c.read(c.gsB, func() {
				sc := c.gsB.seacast
				_, registered = sc.roots[r7]
				_, tomb = sc.expired[r7]
				offered = c.gsB.pullOfferedByAny(mid)
			})
			return
		}
		if registered, _, offered := state(); !registered || !offered {
			t.Fatalf("after A's offer at slot 5: registered %v, offered %v; want true true", registered, offered)
		}
		c.b.SeacastAdvanceSlot(6)
		time.Sleep(10 * time.Millisecond)
		if registered, _, _ := state(); !registered {
			t.Fatal("the root forgotten at slot 6, inside the window of its registration at 5")
		}
		c.b.SeacastAdvanceSlot(7)
		time.Sleep(10 * time.Millisecond)
		if registered, tomb, offered := state(); registered || tomb || offered {
			t.Fatalf("once slot 5 left the window: registered %v, tombstone %v, offered %v; want all false", registered, tomb, offered)
		}
	})
}

// TestSeacastWindowUnplacedRootValidatesLater: a root the window forgot by the slot it was registered
// at, which no type placed, may still validate as a new group, for instance when its header comes late.
// Its first chunk after that is held: the window's forgetting left no holdings tombstone in its way.
func TestSeacastWindowUnplacedRootValidatesLater(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		r7 := strings.Repeat("7", haveRootLen)
		c := newSeacastWindowCell(t, map[string]uint64{}, true)
		c.b.SeacastAdvanceSlot(5)
		mid := r7 + string(byte(5))
		topic := "seg/col/0"
		peerB := c.hosts[1].ID()
		c.read(c.gsA, func() {
			c.gsA.sendRPC(peerB, rpcWithControl(nil, []*pb.ControlIHave{{TopicID: &topic, MessageIDs: []string{mid}}}, nil, nil, nil, nil), false)
		})
		time.Sleep(100 * time.Millisecond)
		c.b.SeacastAdvanceSlot(6)
		c.b.SeacastAdvanceSlot(7) // slot 5 leaves the window: the unplaced root is forgotten
		time.Sleep(10 * time.Millisecond)
		var registered bool
		var tomb int
		var writerHeld bool
		c.read(c.gsB, func() {
			_, registered = c.gsB.seacast.roots[r7]
			tomb = c.gsB.have.tomb[r7]
			writerHeld = c.gsB.have.ownHeldID(r7 + "\x01")
		})
		if tomb != 0 || writerHeld {
			t.Fatalf("forgotten registration: holdings tombstone %d, writer sees held %v; want 0 false", tomb, writerHeld)
		}
		if registered {
			t.Fatal("the unplaced root still registered once slot 5 left the window")
		}
		// The group's first chunk validates now, and the type places it at the node's slot, 7.
		c.read(c.gsB, func() { c.gsB.seacast.types["seg/col/0"].(*testObjectType).slots = map[string]uint64{r7: 7} })
		if err := c.a.Publish("seg/col/0", seacastCell(r7, 1)); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Second)
		var held bool
		c.read(c.gsB, func() {
			m := c.gsB.have.mine[seacastGID(r7)]
			held = m != nil && m.Bits.Has(1)
		})
		if !held || c.validated.Load() != 1 {
			t.Fatalf("a chunk of the group placed at slot 7: held %v, validated %d (holdings tombstone after the window forgot the root: %d heartbeats); want held, 1 validated", held, c.validated.Load(), tomb)
		}
	})
}

type windowSlotType struct {
	ObjectType
	afterSlotOf func()
}

func (t *windowSlotType) SlotOf(root string) (uint64, bool) {
	slot, placed := t.ObjectType.SlotOf(root)
	if t.afterSlotOf != nil {
		f := t.afterSlotOf
		t.afterSlotOf = nil
		f()
	}
	return slot, placed
}

func TestSeacastWindowPlacementDuringSweep(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		root := strings.Repeat("r", haveRootLen)
		c := newSeacastWindowCell(t, nil, false)
		gs := c.gsB
		var expired bool
		var tomb int
		c.read(gs, func() {
			gs.seacastAdvanceSlot(5)
			T := gs.seacastRootType(root).(*testObjectType)
			gs.seacastAdvanceSlot(6)
			gs.seacast.roots[root] = &windowSlotType{ObjectType: T, afterSlotOf: func() {
				// The lookup saw no record; placement still sees slot 6.
				T.slots = map[string]uint64{root: 6}
			}}
			gs.seacastAdvanceSlot(7)
			_, expired = gs.seacast.expired[root]
			tomb = gs.have.tomb[root]
		})

		if err := c.a.Publish("seg/col/0", seacastCell(root, 1)); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Second)
		var held bool
		c.read(gs, func() {
			m := gs.have.mine[seacastGID(root)]
			held = m != nil && m.Bits.Has(1)
		})
		if expired || tomb != 0 || c.validated.Load() != 1 || !held {
			t.Fatalf("live placement: expired %v, holdings tombstone %d, validated %d, held %v; want false 0 1 true",
				expired, tomb, c.validated.Load(), held)
		}
	})
}

func TestSeacastWindowKeepsEarlierHoldingsTombstone(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		root := strings.Repeat("r", haveRootLen)
		c := newSeacastWindowCell(t, map[string]uint64{root: 4}, false)
		gs := c.gsB
		var before, after int
		var held, registered, expired bool
		c.read(gs, func() {
			gs.seacastAdvanceSlot(5)
			T := gs.seacastRootType(root).(*testObjectType)
			gs.seacastExpire([]string{root})
			gs.seacastAdvanceSlot(6)
			gs.haveHeartbeat()
			before = gs.have.tomb[root]
			T.slots = nil
			gs.seacastRootType(root)
			gs.seacastAdvanceSlot(8)
			after = gs.have.tomb[root]
			held = gs.have.ownHeldID(root + "\x01")
			_, registered = gs.seacast.roots[root]
			_, expired = gs.seacast.expired[root]
		})
		if before == 0 || after != before || !held || registered || expired {
			t.Fatalf("holdings TTL %d -> %d, writer sees held %v, registered %v, expired %v; want the same positive TTL and true false false",
				before, after, held, registered, expired)
		}
	})
}

func TestSeacastWindowForgetsHeldBackOffers(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		root := strings.Repeat("r", haveRootLen)
		mid := root + "\x01"
		c := newSeacastWindowCell(t, nil, false)
		gs := c.gsB
		var held, registered, heldAfterExpiry bool
		var retried int64
		c.read(gs, func() {
			gs.seacastAdvanceSlot(5)
			gs.seacastRootType(root)
			gs.seacastHoldBack([]string{mid}, c.hosts[0].ID())
			gs.seacastAdvanceSlot(7)
			_, held = gs.seacast.heldBack[mid]
			gs.seacastHeartbeatLimits()
			_, registered = gs.seacast.roots[root]
			retried = gs.seacast.ctr.HeldBackRetried.Load()
			gs.seacastRootType(root)
			gs.seacastHoldBack([]string{mid}, c.hosts[0].ID())
			gs.seacastExpire([]string{root})
			_, heldAfterExpiry = gs.seacast.heldBack[mid]
		})
		if held || registered || retried != 0 {
			t.Errorf("forgotten registration: held back %v, registered after retry %v, retried %d; want false false 0",
				held, registered, retried)
		}
		if heldAfterExpiry {
			t.Error("application expiry left a held-back offer")
		}
	})
}

// TestSeacastWindowAgedHoldingsAfterTheRecord: a group the window has forgotten, whose record the
// type has since dropped, is not taken up again when its holdings age out: no registration and no
// tombstone, which no slot could ever remove.
func TestSeacastWindowAgedHoldingsAfterTheRecord(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		r4 := strings.Repeat("4", haveRootLen)
		c := newSeacastWindowCell(t, map[string]uint64{r4: 4}, false)
		c.b.SeacastAdvanceSlot(5)
		if err := c.a.Publish("seg/col/0", seacastCell(r4, 1)); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Second)
		state := func() (registered, tomb, held bool) {
			c.read(c.gsB, func() {
				sc := c.gsB.seacast
				_, registered = sc.roots[r4]
				_, tomb = sc.expired[r4]
				_, held = c.gsB.have.mine[seacastGID(r4)]
			})
			return
		}
		if registered, _, held := state(); !registered || !held || c.validated.Load() != 1 {
			t.Fatalf("after a chunk at slot 5: registered %v, held %v, validated %d; want true true 1", registered, held, c.validated.Load())
		}
		c.b.SeacastAdvanceSlot(6) // slot 4 leaves the window: the root is forgotten
		time.Sleep(10 * time.Millisecond)
		// The type drops the group's record (RecordSlots later, in the node).
		c.read(c.gsB, func() { c.gsB.seacast.types["seg/col/0"].(*testObjectType).slots = map[string]uint64{} })
		time.Sleep(3 * time.Minute) // the holdings age out (holdTTL heartbeats)
		if registered, tomb, held := state(); registered || tomb || held {
			t.Fatalf("after the holdings aged out: registered %v, tombstone %v, held %v; want all false", registered, tomb, held)
		}
	})
}

// TestSeacastWindowForgetsHeldBackChunk: a chunk the IHAVE limits held back, whose group the window
// forgets before the heartbeat, goes with it: the heartbeat's retry would look its root up again,
// and a type that places roots it does not know would register it anew. Another group's chunk held
// back stays.
func TestSeacastWindowForgetsHeldBackChunk(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		root := strings.Repeat("r", haveRootLen)
		c := newSeacastWindowCell(t, nil, false)
		gs := c.gsB
		other := seacastGID(strings.Repeat("o", haveRootLen))
		var before, after, otherKept bool
		c.read(gs, func() {
			gs.seacastAdvanceSlot(5)
			gs.seacastRootType(root)
			gs.pull.held.Hold(seacastGID(root), 1)
			gs.pull.held.Hold(other, 1)
			before = gs.pull.held.Holds(seacastGID(root), 1)
			gs.seacastAdvanceSlot(7) // the window passes the root's slot
			after = gs.pull.held.Holds(seacastGID(root), 1)
			otherKept = gs.pull.held.Holds(other, 1)
		})
		if !before || after || !otherKept {
			t.Fatalf("held back before the window passed %v, after %v; the other group's %v; want true false true", before, after, otherKept)
		}
	})
}

// TestSeacastWindowForgetsTombstoneHeldBackChunk: a chunk the IHAVE limits held back, whose group
// expired before its slot left the window (a tombstone), goes when the window passes the tombstone, as
// a live group's does: the heartbeat's retry would look its root up again.
func TestSeacastWindowForgetsTombstoneHeldBackChunk(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		root := strings.Repeat("t", haveRootLen)
		c := newSeacastWindowCell(t, nil, false)
		gs := c.gsB
		var tomb, before, after, registered bool
		c.read(gs, func() {
			gs.seacastAdvanceSlot(5)
			gs.seacastRootType(root)
			gs.pull.held.Hold(seacastGID(root), 1)
			gs.expireGroup(root) // its group expires early: a tombstone; its held-back chunk stays
			_, tomb = gs.seacast.expired[root]
			before = gs.pull.held.Holds(seacastGID(root), 1)
			gs.seacastAdvanceSlot(7) // the window passes the tombstone
			after = gs.pull.held.Holds(seacastGID(root), 1)
			_, registered = gs.seacast.roots[root]
		})
		if !tomb || !before {
			t.Fatalf("the scenario: tombstone %v, held back after the expiry %v; want true true", tomb, before)
		}
		if after || registered {
			t.Fatalf("after the window passed the tombstone: held back %v, registered %v; want false false", after, registered)
		}
	})
}
