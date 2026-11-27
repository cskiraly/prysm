package pubsub

import (
	"context"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p-pubsub/timecache"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
)

// seacastTieCell is n routers on one owned topic with every gate closed (HeaderKnown false), the
// pull discipline, promises and parks, the offer table and v0.5's retry, as the harness's cells run
// them; the last router's validator counts its calls, and waits on hold while one is set. Every host
// is connected to the last one.
type seacastTieCell struct {
	pubsubs   []*PubSub
	hosts     []host.Host
	stores    []*MapCellStore
	types     []*testObjectType
	validated atomic.Int64
	hold      atomic.Pointer[chan struct{}]
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
			WithSpecV05(SpecV05{}, nil),
		}
		if len(limits) > 0 {
			opts = append(opts, WithSeacastLimits(limits[0]))
		}
		return opts
	})
	last := c.pubsubs[n-1]
	if err := last.RegisterTopicValidator("seg/col/0", func(_ context.Context, _ peer.ID, m *Message) ValidationResult {
		c.validated.Add(1)
		if h := c.hold.Load(); h != nil {
			<-*h
		}
		if _, i, ok := seacastTestIndex(seacastTestID(m.Message)); ok && i >= 32 {
			return ValidationReject // level 1: an index the group has not, by the test type's count
		}
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

// TestSeacastTiedOffersWaitForTheGate: A pushes a cell whose header B has not validated, so B keeps it
// and ties A for the index. While the gate is closed, A's offer of the index and C's are recorded and
// not asked (seacast-lite.md §4.4: the block first), the gate before the tie. At the header the kept
// copy fills and the ties go.
func TestSeacastTiedOffersWaitForTheGate(t *testing.T) {
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
		c.offer(0, 2, mid) // A offers what B already keeps from it, unchecked
		c.offer(1, 2, mid) // and so does C
		var asked, offeredByA, offeredByC bool
		c.read(2, func(gs *GossipSubRouter) {
			asked = gs.pullAsked(mid)
			offeredByA = gs.pullOffered(c.hosts[0].ID(), mid)
			offeredByC = gs.pullOffered(c.hosts[1].ID(), mid)
		})
		if asked || !offeredByA || !offeredByC || ctrB.TiedPassedOver.Load() != 0 {
			t.Fatalf("after A's and C's offers: asked %v, in the offer table A %v C %v, passed over %d; want false true true 0",
				asked, offeredByA, offeredByC, ctrB.TiedPassedOver.Load())
		}
		c.read(2, func(*GossipSubRouter) { c.types[2].gateClosed = false }) // B's application validated the header
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
// The control, where A never answers: the promise breaks and A is parked. Nothing is asked of a group
// whose gate is closed (seacast-lite.md §4.4), and in the fork a root is its group, so a promise and a
// tie meet only where the gate closes after the ask: the test type's gate is open for the offer and
// the ask, and closes before the answer. (In the spec they meet where the group's block installed and
// the copy names another root.)
func TestSeacastTieClosesPromise(t *testing.T) {
	for _, answers := range []bool{false, true} {
		synctestTest(t, func(t *testing.T) {
			root := strings.Repeat("u", haveRootLen)
			c := newSeacastTieCell(t, 2) // A = 0, B = 1
			a, b := c.pubsubs[0], c.pubsubs[1]
			mid := root + string(byte(6))
			c.read(1, func(*GossipSubRouter) { c.types[1].gateClosed = false })
			c.offer(0, 1, mid)
			var promised bool
			c.read(1, func(gs *GossipSubRouter) {
				promised = gs.pullPromised(c.hosts[0].ID(), mid)
				c.types[1].gateClosed = true
			})
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

// TestSeacastTieDropsQueuedAsks: at the tie of p's copy of a chunk, by either path that ties p (its
// copy kept at the closed gate, and its copy of a chunk another peer's copy of which waits), p's asks
// of that chunk still queued go, in either lane and in any form that names it, the probe's own among
// them, and each closes with the score's pending promise (seacast-lite.md §7.2's filter_entry,
// tied_to_root). p's ask of another chunk and r's asks of the chunk stay, though the chunk's cell was
// probed at r. A later ask of the chunk to p is refused where it is made, and one of another chunk is
// not. On a hand-built router, through the admission (seacastAdmit).
func TestSeacastTieDropsQueuedAsks(t *testing.T) {
	root := strings.Repeat("t", haveRootLen)
	topic := "seg/col/0"
	host := getDefaultHosts(t, 1)[0]
	p, r, s := peer.ID("p"), peer.ID("r"), peer.ID("s")
	six, seven := root+string(byte(6)), root+string(byte(7)) // seacastTestID's form
	sixByIndex, probe := encodeWantV2(root, 6, false, 0), encodeWantV2Probe(root, 6)
	for _, path := range []string{"kept", "another copy"} {
		T := &testObjectType{name: "col", params: TypeParams{RNode: 2}, topics: []string{topic}, gateClosed: true}
		ctr := &SeacastCounters{}
		qp, qr := newRpcQueue(16), newRpcQueue(16)
		gs := &GossipSubRouter{peers: map[peer.ID]protocol.ID{p: GossipSubID_v12, r: GossipSubID_v12}}
		gs.p = &PubSub{host: host, idGen: &msgIDGenerator{Default: seacastTestID, topicGens: map[string]MsgIdFunction{}},
			seenMessages: timecache.NewTimeCache(time.Minute), peers: map[peer.ID]*rpcQueue{p: qp, r: qr},
			maxMessageSize: DefaultMaxMessageSize, maxControlMessageSize: DefaultMaxControlMessageSize}
		gs.have = newHaveState(seacastTestIndex)
		probed := &haveBits{}
		probed.Set(6)
		gs.seacast = &seacast{types: map[string]ObjectType{topic: T}, ctr: ctr, limits: SeacastLimits{PendingPerPeer: 2},
			probeAsks: map[string]struct{}{probe: {}}, probeCells: map[string]*haveBits{root: probed}}
		gt := newGossipTracer()
		gt.atWrite = true
		gs.gossipTracer = gt
		ask := func(q *rpcQueue, urgent bool, ids ...string) {
			rpc := &RPC{RPC: pb.RPC{Control: &pb.ControlMessage{Iwant: []*pb.ControlIWant{{MessageIDs: ids}}}}}
			rpc.size = 10
			push := q.Push
			if urgent {
				push = q.UrgentPush
			}
			if err := push(rpc, false); err != nil {
				t.Fatal(err)
			}
		}
		ask(qp, false, six, seven)
		ask(qp, true, sixByIndex) // a plain WANT, on the urgent lane as seacastAsk sends it
		ask(qp, false, probe)
		ask(qr, false, six)
		ask(qr, false, probe) // the cell probed at r
		gs.seacastAskQueued(p, []string{six, seven, sixByIndex, probe})
		gs.seacastAskQueued(r, []string{six, probe})
		for _, id := range []string{six, seven, sixByIndex} {
			gt.AddPromise(p, []string{id})
		}
		gt.AddPromise(r, []string{six})

		arrive := func(from peer.ID) {
			m := &Message{Message: &pb.Message{Topic: &topic, Data: append([]byte(six), 'x')}, ReceivedFrom: from}
			if gs.seacastAdmit(m, false) {
				t.Fatalf("%s: a copy of a closed gate admitted", path)
			}
		}
		if path == "another copy" {
			arrive(s) // s's copy waits, so p's is another copy of it (seacastTie)
		}
		arrive(p)
		if !gs.seacast.ties.Tied(p, seacastGID(root), 6) || ctr.PendingDup.Load() != map[string]int64{"kept": 0, "another copy": 1}[path] {
			t.Fatalf("%s: p tied %v, another copy %d", path, gs.seacast.ties.Tied(p, seacastGID(root), 6), ctr.PendingDup.Load())
		}

		left := func(q *rpcQueue) (ids []string) {
			q.queueMu.Lock()
			defer q.queueMu.Unlock()
			for _, rpc := range append(slices.Clone(q.queue.priority), q.queue.normal...) {
				for _, w := range rpc.GetControl().GetIwant() {
					ids = append(ids, w.GetMessageIDs()...)
				}
			}
			return ids
		}
		if ids := left(qp); qp.queue.Len() != 1 || !slices.Equal(ids, []string{seven}) {
			t.Errorf("%s: p's queue after the tie: %d RPCs asking %q; want 1 asking %q", path, qp.queue.Len(), ids, []string{seven})
		}
		if ids := left(qr); qr.queue.Len() != 2 || !slices.Equal(ids, []string{six, probe}) {
			t.Errorf("%s: r's queue after p's tie: %d RPCs asking %q; want 2 asking %q", path, qr.queue.Len(), ids, []string{six, probe})
		}
		pending := func(mid string, q peer.ID) bool {
			gt.Lock()
			defer gt.Unlock()
			_, ok := gt.pending[mid][q]
			return ok
		}
		for _, c := range []struct {
			p    peer.ID
			id   string
			want bool
		}{{p, six, false}, {p, sixByIndex, false}, {p, probe, false}, {p, seven, true}, {r, six, true}, {r, probe, true}} {
			if got := gs.seacastAskIsQueued(c.p, c.id); got != c.want {
				t.Errorf("%s: %s's ask %q queued after the tie: %v, want %v", path, string(c.p), c.id, got, c.want)
			}
			if c.id == probe {
				continue // no score promise was made for it
			}
			if got := pending(c.id, c.p); got != c.want {
				t.Errorf("%s: %s's score promise for %q pending after the tie: %v, want %v", path, string(c.p), c.id, got, c.want)
			}
		}

		if gs.seacastAsk(p, root, 6, false, 0) || ctr.AskOutside.Load() != 0 || qp.queue.Len() != 1 {
			t.Errorf("%s: an ask of the tied chunk made after the tie: refused outside may_be_asked %d, queue %d; want refused, 0, 1", path, ctr.AskOutside.Load(), qp.queue.Len())
		}
		if !gs.seacastAsk(p, root, 7, false, 0) || qp.queue.Len() != 2 {
			t.Errorf("%s: an ask of another chunk after the tie not queued: queue %d, want 2", path, qp.queue.Len())
		}
	}
}

// TestSeacastGateOpening: while B's gate for a root is closed, A's copy of index 5 waits and ties A,
// A is parked for the group, and C's offer of index 7 is recorded and not asked (seacast-lite.md §4.4:
// the block first). At the header (core.OpenGate), A's park lifts as its ties release, and the ledger
// asks C for index 7, which no copy filled.
func TestSeacastGateOpening(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		root := strings.Repeat("o", haveRootLen)
		c := newSeacastTieCell(t, 3) // A = 0, C = 1, B = 2
		A, C := c.hosts[0].ID(), c.hosts[1].ID()
		b := c.pubsubs[2]
		mid7 := root + string(byte(7))
		if err := c.pubsubs[0].Publish("seg/col/0", seacastCell(root, 5)); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Second)
		c.offer(1, 2, mid7)
		var asked, offeredByC, parked, promisedByC bool
		c.read(2, func(gs *GossipSubRouter) {
			gs.pull.promises.Park(A, seacastGID(root), time.Now().Add(time.Minute)) // A parked for the group
			asked, offeredByC, parked = gs.pullAsked(mid7), gs.pullOffered(C, mid7), gs.peerParkedFor(A, mid7)
		})
		if b.SeacastTies() != 1 || asked || !offeredByC || !parked {
			t.Fatalf("before the header: ties %d, index 7 asked %v, offered by C %v, A parked %v; want 1 false true true",
				b.SeacastTies(), asked, offeredByC, parked)
		}
		c.read(2, func(*GossipSubRouter) { c.types[2].gateClosed = false }) // B's application validated the header
		b.SeacastHeaderValidated([]string{root})
		time.Sleep(100 * time.Millisecond)
		c.read(2, func(gs *GossipSubRouter) {
			asked, promisedByC, parked = gs.pullAsked(mid7), gs.pullPromised(C, mid7), gs.peerParkedFor(A, mid7)
		})
		if parked || !asked || !promisedByC || b.SeacastTies() != 0 {
			t.Fatalf("after the header: A parked %v, index 7 asked %v, promised by C %v, ties %d; want false true true 0",
				parked, asked, promisedByC, b.SeacastTies())
		}
	})
}

// TestSeacastOpenGateNotMet: a type whose gate never closes (Prysm's payload: the descriptor travels
// in each chunk) tells the path a group's count at its first chunk through SeacastHeaderValidated,
// which opens no gate: nothing met it closed, so its ledger is not retried, and an ask the IHAVE
// limits held back waits for the heartbeat, as it does without the call.
func TestSeacastOpenGateNotMet(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		root := strings.Repeat("n", haveRootLen)
		c := newSeacastTieCell(t, 2) // A = 0, B = 1
		b := c.pubsubs[1]
		mid7 := root + string(byte(7))
		c.read(1, func(gs *GossipSubRouter) {
			c.types[1].gateClosed = false // B's gate is open from the start
			gs.params.MaxIHaveLength = 0  // every ask held back to the heartbeat
		})
		// Start just past one of B's heartbeats, so that none falls between the offer and the count:
		// the heartbeat would ask the held-back offer whatever the gate does.
		ticks := func() (n uint64) { c.read(1, func(gs *GossipSubRouter) { n = gs.heartbeatTicks }); return n }
		for t0 := ticks(); ticks() == t0; {
			time.Sleep(10 * time.Millisecond)
		}
		t0 := ticks()
		c.offer(0, 1, mid7)
		var asked, held bool
		look := func() {
			c.read(1, func(gs *GossipSubRouter) { asked, held = gs.pullAsked(mid7), gs.pull.held.Holds(seacastGID(root), 7) })
		}
		look()
		if asked || !held {
			t.Fatalf("A's offer: asked %v, held back %v; want false true", asked, held)
		}
		b.SeacastHeaderValidated([]string{root})
		synctest.Wait()
		look()
		if asked || !held {
			t.Fatalf("after the count: asked %v, held back %v; want false true (no gate opened)", asked, held)
		}
		if n := ticks(); n != t0 {
			t.Fatalf("%d heartbeats between the offer and the count: the test's window is wrong", n-t0)
		}
		c.read(1, func(gs *GossipSubRouter) { gs.params.MaxIHaveLength = GossipSubMaxIHaveLength })
		time.Sleep(time.Second)
		look()
		if !asked || held {
			t.Fatalf("after the heartbeat: asked %v, held back %v; want true false", asked, held)
		}
	})
}

// TestSeacastGroupEndDropsWaits: a group's end drops its kept copies, their messages and their
// senders' ties at once, not at the next heartbeat: the heartbeat must not look a forgotten root up,
// which can register it again (the type here places any root on its topic, as Prysm's does). Each way
// a group ends on the path: cleared (the window's expiry, seacastClearGroup), aged (the holdings'
// expiry, seacastGroupAged) and forgotten (the window's forget, seacastForgetRoot), after which the
// next heartbeat leaves the root unregistered.
func TestSeacastGroupEndDropsWaits(t *testing.T) {
	for _, end := range []string{"cleared", "aged", "forgotten"} {
		t.Run(end, func(t *testing.T) {
			synctestTest(t, func(t *testing.T) {
				root := strings.Repeat("e", haveRootLen)
				c := newSeacastTieCell(t, 2) // A = 0, B = 1
				b := c.pubsubs[1]
				if err := c.pubsubs[0].Publish("seg/col/0", seacastCell(root, 3)); err != nil {
					t.Fatal(err)
				}
				time.Sleep(time.Second)
				if b.SeacastPending() != 1 || b.SeacastTies() != 1 {
					t.Fatalf("A's copy: pending %d ties %d, want 1 1", b.SeacastPending(), b.SeacastTies())
				}
				var pending, ties, bodies int
				var registered, met bool
				look := func(gs *GossipSubRouter) {
					pending, ties, bodies = gs.seacast.waits.Len(), gs.seacast.ties.Pairs(), len(gs.seacast.waitMsg)
					_, registered = gs.seacast.roots[root]
					_, met = gs.seacast.gateMet[root]
				}
				if c.read(1, look); !met {
					t.Fatal("A's copy kept: the gate not marked as met")
				}
				if end == "forgotten" {
					b.SeacastAdvanceSlot(1) // a lookup of the root's slot needs the node's slot
				}
				c.read(1, func(gs *GossipSubRouter) {
					switch end {
					case "cleared":
						gs.seacastClearGroup(root)
					case "aged":
						gs.seacastGroupAged(root)
					default:
						gs.seacastForgetRoot(root)
					}
					look(gs)
				})
				if end == "forgotten" {
					time.Sleep(1100 * time.Millisecond) // a heartbeat
					c.read(1, look)
					if registered {
						t.Fatal("the heartbeat after the forget registered the root again")
					}
				}
				if pending != 0 || ties != 0 || bodies != 0 || met {
					t.Fatalf("after the group's end (%s): pending %d ties %d messages %d gate met %v, want 0 0 0 false", end, pending, ties, bodies, met)
				}
			})
		})
	}
}

// TestSeacastGateMetByOfferAlone: an offer alone meets a closed gate (no copy kept, no tie): it is
// recorded and not asked, and marks the gate, so the count opens it and asks the offer from the
// ledger at once, with no time passing. The opening clears the mark.
func TestSeacastGateMetByOfferAlone(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		root := strings.Repeat("m", haveRootLen)
		c := newSeacastTieCell(t, 2) // A = 0, B = 1
		b := c.pubsubs[1]
		mid7 := root + string(byte(7))
		c.offer(0, 1, mid7)
		var asked, met bool
		look := func() {
			c.read(1, func(gs *GossipSubRouter) { asked = gs.pullAsked(mid7); _, met = gs.seacast.gateMet[root] })
		}
		look()
		if asked || !met || b.SeacastPending() != 0 || b.SeacastTies() != 0 {
			t.Fatalf("A's offer at a closed gate: asked %v, gate met %v, pending %d, ties %d; want false true 0 0",
				asked, met, b.SeacastPending(), b.SeacastTies())
		}
		c.read(1, func(*GossipSubRouter) { c.types[1].gateClosed = false })
		b.SeacastHeaderValidated([]string{root})
		synctest.Wait()
		look()
		if !asked || met {
			t.Fatalf("at the count, with no time passed: asked %v, gate met %v; want true false", asked, met)
		}
	})
}

// TestSeacastWaitExpiredRegistersNothing: the heartbeat's expiry test of a wait reads the owner and
// the tombstone only, never the types' TopicOf, so a root no wait could name any more (forgotten by
// the window) is not registered again. Waits get their owner before they are stored and go before it
// does, so no flow reaches this: the test calls the function on an unregistered root.
func TestSeacastWaitExpiredRegistersNothing(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		root := strings.Repeat("x", haveRootLen)
		c := newSeacastTieCell(t, 1)
		var expired, registered bool
		c.read(0, func(gs *GossipSubRouter) {
			gs.seacastAdvanceSlot(3)
			expired = gs.seacastWaitExpired(root)
			_, registered = gs.seacast.roots[root]
		})
		if expired || registered {
			t.Fatalf("an unregistered root: expired %v, registered %v; want false false", expired, registered)
		}
	})
}

// TestSeacastOwnPublishFreesReservation: B keeps A's copy of index 3 at A's bound of one, and then
// publishes that chunk itself, alone (Publish: the fill at ACCEPT) or in a batch (PublishBatch: the
// source's store): the fill takes the kept copy out of the store with its message, and A's
// reservation goes back. A's copy of 3, sent again, is a duplicate of an id B has seen and is not
// kept (on_chunk's seen first), so A's copy of index 4 is kept rather than refused at the bound.
func TestSeacastOwnPublishFreesReservation(t *testing.T) {
	for _, how := range []string{"publish", "batch"} {
		t.Run(how, func(t *testing.T) {
			synctestTest(t, func(t *testing.T) {
				root := strings.Repeat("u", haveRootLen)
				c := newSeacastTieCell(t, 2) // A = 0, B = 1
				a, b := c.pubsubs[0], c.pubsubs[1]
				ctrB := c.gs(1).seacast.ctr
				c.read(1, func(gs *GossipSubRouter) { gs.seacast.limits.PendingPerPeer = 1 })
				if err := a.Publish("seg/col/0", seacastCell(root, 3)); err != nil {
					t.Fatal(err)
				}
				time.Sleep(time.Second)
				if b.SeacastPending() != 1 {
					t.Fatalf("A's copy of 3: pending %d, want 1", b.SeacastPending())
				}
				if how == "publish" {
					if err := b.Publish("seg/col/0", seacastCell(root, 3)); err != nil {
						t.Fatal(err)
					}
				} else {
					var tp *Topic
					c.read(1, func(gs *GossipSubRouter) { tp = gs.p.myTopics["seg/col/0"] })
					var batch MessageBatch
					if err := tp.AddToBatch(context.Background(), &batch, seacastCell(root, 3)); err != nil {
						t.Fatal(err)
					}
					if err := b.PublishBatch(&batch); err != nil {
						t.Fatal(err)
					}
				}
				time.Sleep(100 * time.Millisecond)
				var bodies int
				c.read(1, func(gs *GossipSubRouter) { bodies = len(gs.seacast.waitMsg) })
				if b.SeacastPending() != 0 || bodies != 0 {
					t.Fatalf("after B published 3: pending %d messages %d, want 0 0", b.SeacastPending(), bodies)
				}
				// A's copy of 3 reaches B again, as A published it, straight into B's receive path (A's
				// writer would cancel it: B has claimed the index): B has seen the id (its own
				// publication), so the copy is a duplicate and takes no reservation.
				var m3 *Message
				c.read(0, func(gs *GossipSubRouter) { m3, _ = gs.mcache.Get(root + string(byte(3))) })
				if m3 == nil {
					t.Fatal("A's copy of 3 is not in its message cache")
				}
				c.read(1, func(gs *GossipSubRouter) {
					gs.p.handleIncomingRPC(&RPC{RPC: pb.RPC{Publish: []*pb.Message{m3.Message}}, from: c.hosts[0].ID()})
				})
				time.Sleep(100 * time.Millisecond)
				if b.SeacastPending() != 0 || ctrB.SeenAtGate.Load() != 1 {
					t.Fatalf("A's copy of 3 again: pending %d, seen at the gate %d; want 0, 1", b.SeacastPending(), ctrB.SeenAtGate.Load())
				}
				if err := a.Publish("seg/col/0", seacastCell(root, 4)); err != nil {
					t.Fatal(err)
				}
				time.Sleep(time.Second)
				if b.SeacastPending() != 1 || ctrB.PendingRefused.Load() != 0 {
					t.Fatalf("A's copy of 4: pending %d refused %d, want 1 0", b.SeacastPending(), ctrB.PendingRefused.Load())
				}
			})
		})
	}
}

// TestSeacastTieForgeryAtHeader: a pending copy naming an index beyond the type's count resumes level
// 1 at the header, which rejects it (seacast-lite.md §4.2's on_block_installed: the failure is its
// sender's), and its sender's offer of the index goes at drop_beyond; a copy with an index the header
// admits is validated and filled.
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
		c.read(1, func(gs *GossipSubRouter) { offeredByA = gs.pullOffered(c.hosts[0].ID(), mid40) })
		if !offeredByA {
			t.Fatal("A's offer of index 40 was not recorded")
		}
		c.types[1].gateClosed = false // B's application validated the header
		b.SeacastHeaderValidated([]string{root})
		time.Sleep(time.Second)
		var held int
		c.read(1, func(gs *GossipSubRouter) {
			held = c.stores[1].Len()
			offeredByA = gs.pullOffered(c.hosts[0].ID(), mid40)
		})
		if held != 1 || c.validated.Load() != 2 || ctrB.Rejected.Load() != 1 || ctrB.Accepted.Load() != 1 || offeredByA || b.SeacastTies() != 0 {
			t.Fatalf("after the header: held %d validated %d rejected %d accepted %d, A still offers 40 %v, ties %d; want 1 2 1 1 false 0", held, c.validated.Load(), ctrB.Rejected.Load(), ctrB.Accepted.Load(), offeredByA, b.SeacastTies())
		}
	})
}

// TestSeacastTieConnection: a copy refused at the per-peer bound still ties its sender; a sender's
// ties go with its connection, and its kept copies stay, by root (seacast-lite.md §8.1's table), so
// the same id from another sender afterwards is a duplicate of a kept one, which ties that sender.
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
		if b.SeacastPending() != 1 || b.SeacastTies() != 0 {
			t.Fatalf("after A's connection closed: pending %d ties %d, want 1 0", b.SeacastPending(), b.SeacastTies())
		}
		// C sends the id A's copy carried: B still keeps A's copy of it, so C's is a duplicate.
		if err := c.pubsubs[1].Publish("seg/col/0", seacastCell(root, 1)); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Second)
		if b.SeacastPending() != 1 || ctrB.PendingDup.Load() != 1 || ctrB.Tied.Load() != 3 || b.SeacastTies() != 1 {
			t.Fatalf("C's copy of the id after A left: pending %d dup %d tied %d ties %d, want 1 1 3 1", b.SeacastPending(), ctrB.PendingDup.Load(), ctrB.Tied.Load(), b.SeacastTies())
		}
	})
}

// TestSeacastStaleTieDropsOfferByAnyID: with the chunk pull off, an offer is kept under the id it
// named, and two copies of one index can carry different ids (Prysm's ids carry the content's hash).
// C offers X for index 3, which B records no id for (on the path an id is recorded only for a chunk
// the node stored, seacast-lite.md §4.4); B then keeps A's copy Y of index 3, waiting, and records
// A's offer of Y. When the tie goes stale (its slot left the live window), drop_tie drops A's offers
// of index 3 under every id: A's offer of Y goes, though Y is neither an id B recorded nor an ask's
// form.
func TestSeacastStaleTieDropsOfferByAnyID(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		const idLen = haveRootLen + 2 // the root, the index, a variant
		idOf := func(m *pb.Message) string { return string(m.GetData()[:idLen]) }
		index := func(mid string) (string, uint32, bool) {
			if len(mid) != idLen {
				return "", 0, false
			}
			return mid[:haveRootLen], uint32(mid[haveRootLen]), true
		}
		root := strings.Repeat("y", haveRootLen)
		cell := func(v byte) []byte { return append([]byte(root+"\x03"+string(v)), strings.Repeat("x", 64)...) }
		hosts := getDefaultHosts(t, 3) // A = 0, C = 1, B = 2
		types := make([]*testObjectType, len(hosts))
		params := DefaultGossipSubParams()
		params.IDontWantMessageThreshold = 1
		pubsubs := getGossipsubsOptFn(ctx, hosts, func(i int, _ host.Host) []Option {
			types[i] = &testObjectType{name: "col", params: TypeParams{RNode: 2}, topics: []string{"seg/col/0"}, store: NewMapCellStore(),
				gateClosed: true, chunkSlots: map[string]uint64{root: 5}}
			return []Option{ // no park per group: the chunk pull is off
				WithGossipSubParams(params),
				WithMessageIdFn(idOf),
				WithHaveBitmaps(index),
				WithPhaseForwarding(func(topic string) bool { return topic != "seg/col/0" }, 2),
				WithIWantDiscipline(200 * time.Millisecond),
				WithIHaveCommitmentPark(1, 400*time.Millisecond, 30*time.Second, nil, nil),
				WithOfferTable(nil),
				WithSeacast([]ObjectType{types[i]}, nil),
				WithSpecV05(SpecV05{}, nil),
			}
		})
		for _, ps := range pubsubs {
			if _, err := ps.Subscribe("seg/col/0"); err != nil {
				t.Fatal(err)
			}
		}
		connect(t, hosts[0], hosts[2])
		connect(t, hosts[1], hosts[2])
		time.Sleep(2 * time.Second)
		B := pubsubs[2]
		A := hosts[0].ID()
		read := func(f func(gs *GossipSubRouter)) { joinPushRun(B, f) }
		read(func(gs *GossipSubRouter) {
			if gs.pullOn() {
				t.Fatal("the chunk pull is on")
			}
		})
		offer := func(from int, mid string) {
			topic := "seg/col/0"
			joinPushRun(pubsubs[from], func(gs *GossipSubRouter) {
				gs.sendRPC(hosts[2].ID(), rpcWithControl(nil, []*pb.ControlIHave{{TopicID: &topic, MessageIDs: []string{mid}}}, nil, nil, nil, nil), false)
			})
			time.Sleep(100 * time.Millisecond)
		}
		X, Y := root+"\x03x", root+"\x03y"
		offer(1, X) // C's offer of X for index 3: no id recorded
		if err := pubsubs[0].Publish("seg/col/0", cell('y')); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Second)
		offer(0, Y) // A's offer of the copy B keeps from it
		var learned string
		var offered bool
		read(func(gs *GossipSubRouter) {
			if m := gs.have.mids[root]; m != nil {
				learned = m.ids[3]
			}
			_, offered = gs.offerTable[Y][A]
		})
		if B.SeacastPending() != 1 || B.SeacastTies() != 1 || learned != "" || !offered {
			t.Fatalf("before: pending %d ties %d, index 3 learned as %q, A offers Y %v; want 1 1 \"\" true", B.SeacastPending(), B.SeacastTies(), learned, offered)
		}
		B.SeacastAdvanceSlot(7)             // slot 5 leaves the window of two
		time.Sleep(1100 * time.Millisecond) // a heartbeat: drop_stale_waits
		read(func(gs *GossipSubRouter) { _, offered = gs.offerTable[Y][A] })
		if B.SeacastTies() != 0 || offered {
			t.Fatalf("after the stale drop: ties %d, A offers Y %v; want 0 false", B.SeacastTies(), offered)
		}
	})
}
