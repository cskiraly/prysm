package pubsub

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	core "github.com/cskiraly/seacast-go"
	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
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
			WithSpecV05(SpecV05{}, nil),
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
// falls due at 60 s. The controls: A renews its offer at 20 s and 45 s, of the same index or of
// another index of the same group (offers age per announcer and group, seacast_pull.go), the offer
// lives past 60 s and the promise breaks. The other index's own promise falls due at 80 s.
func TestSeacastAskGoesWithOffer(t *testing.T) {
	for _, renew := range []string{"", "same index", "other index"} {
		synctestTest(t, func(t *testing.T) {
			root := strings.Repeat("d", haveRootLen)
			c := newSeacastF13Cell(t)
			mid := root + string(byte(2))
			renewal := mid
			if renew == "other index" {
				renewal = root + string(byte(3))
			}
			A := c.hosts[0].ID()
			ctrB := c.gs(1).seacast.ctr
			c.offer(mid)
			var promised, offered bool
			c.read(1, func(gs *GossipSubRouter) {
				promised = gs.pullPromised(A, mid)
				offered = gs.pullOffered(A, mid)
			})
			if !promised || !offered {
				t.Fatalf("after A's offer: promised %v offered %v, want both", promised, offered)
			}
			if renew != "" {
				time.Sleep(20 * time.Second)
				c.offer(renewal)
				time.Sleep(11 * time.Second) // 31 s
				c.read(1, func(gs *GossipSubRouter) {
					promised = gs.pullPromised(A, mid)
					offered = gs.pullOffered(A, mid)
				})
				if !promised || !offered {
					t.Fatalf("control, renewed by the %s at 20 s: at 31 s promised %v offered %v, want both", renew, promised, offered)
				}
				time.Sleep(14 * time.Second)
				c.offer(renewal)
				time.Sleep(16 * time.Second) // 61 s
			} else {
				time.Sleep(31 * time.Second)
				c.read(1, func(gs *GossipSubRouter) {
					promised = gs.pullPromised(A, mid)
					offered = gs.pullOffered(A, mid)
				})
				if promised || offered || ctrB.F13Closed.Load() != 1 {
					t.Fatalf("31 s on, A's offer swept: promised %v offered %v closed-with-offer %d; want false false 1", promised, offered, ctrB.F13Closed.Load())
				}
				time.Sleep(30 * time.Second) // 61 s
			}
			if renew != "" {
				if c.breaks.Load() != 1 || c.parks.Load() != 1 {
					t.Fatalf("control, renewed by the %s: breaks %d parks %d, want 1 1", renew, c.breaks.Load(), c.parks.Load())
				}
			} else if c.breaks.Load() != 0 || c.parks.Load() != 0 {
				t.Fatalf("the offer dropped at 30 s: breaks %d parks %d at 61 s, want 0 0", c.breaks.Load(), c.parks.Load())
			}
		})
	}
}

// TestSeacastStockWriteKeepsRecord: without v0.5's E, a writer's report closes the queued record of a
// chunk's ask and opens its promise, and leaves a stock ask's record as it was before the core pulled
// chunks: its promise opened at the dispatch, and the record goes with the offer.
func TestSeacastStockWriteKeepsRecord(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		c := newSeacastF13Cell(t)
		A := c.hosts[0].ID()
		mid, stock := strings.Repeat("d", haveRootLen)+string(byte(2)), "a stock message id"
		var chunkQueued, stockQueued, promised, isChunk, stockChunk bool
		c.read(1, func(gs *GossipSubRouter) {
			_, _, _, isChunk = gs.pullChunk(mid)
			_, _, _, stockChunk = gs.pullChunk(stock)
			gs.seacastAskQueued(A, []string{mid, stock})
			now := time.Now()
			gs.promisesFromWrite(A, []string{mid, stock}, nextAskSeq(), now)
			chunkQueued, stockQueued = gs.seacastAskIsQueued(A, mid), gs.seacastAskIsQueued(A, stock)
			promised = gs.pullPromised(A, mid)
		})
		if !isChunk || stockChunk {
			t.Fatalf("fixture: %q a chunk %v, %q %v; want true, false", mid, isChunk, stock, stockChunk)
		}
		if chunkQueued || !promised || !stockQueued {
			t.Fatalf("after the write: the chunk's ask queued %v promised %v, the stock ask's queued %v; want false true true",
				chunkQueued, promised, stockQueued)
		}
	})
}

// TestSeacastDroppedAskLeavesQueue: an ask queued to A when A's offer is dropped leaves A's queue: an IWANT
// of it and another id stays with the other id only, and an IWANT of it alone goes. A's queue has no
// writer's hooks (A sent no holding), and the drop installs none. Nothing is left behind: a drop that
// finds the ask popped already, as when the writer took it first, leaves a fresh ask of the same id to
// A whole, through the writer's filter.
func TestSeacastDroppedAskLeavesQueue(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		root := strings.Repeat("e", haveRootLen)
		c := newSeacastF13Cell(t)
		mid := root + string(byte(4))
		other := root + string(byte(5))
		A := c.hosts[0].ID()
		ctrB := c.gs(1).seacast.ctr
		ask := func(ids ...string) *RPC {
			return rpcWithControl(nil, nil, []*pb.ControlIWant{{MessageIDs: ids}}, nil, nil, nil)
		}
		queued := func(q *rpcQueue) (ids []string, rpcs int) {
			q.queueMu.Lock()
			defer q.queueMu.Unlock()
			all := slices.Clone(q.queue.priority)
			for _, e := range q.queue.normal {
				all = append(all, e)
			}
			for _, rpc := range all {
				for _, w := range rpc.GetControl().GetIwant() {
					ids = append(ids, w.GetMessageIDs()...)
				}
			}
			return ids, len(all)
		}
		var open, hooks, fresh bool
		var left []string
		var rpcs int
		var written []string
		c.read(1, func(gs *GossipSubRouter) {
			q := newRpcQueue(16) // A's queue, with no writer: what the drop leaves stays to be read
			real := gs.p.peers[A]
			gs.p.peers[A] = q
			defer func() { gs.p.peers[A] = real }()
			// An ask by the chunk's id, as A's offer of it is asked: the queued ask's form finds it
			// (chunkForms), though on the path the offer teaches no id.
			if err := q.Push(ask(mid, other), false); err != nil {
				t.Errorf("push: %v", err)
				return
			}
			if err := q.Push(ask(mid), false); err != nil {
				t.Errorf("push: %v", err)
				return
			}
			gs.seacastAskQueued(A, []string{mid}) // as a dispatch records it, before the writer's report
			open = gs.seacastOfferDropped(A, mid)
			left, rpcs = queued(q)
			hooks = q.have != nil
			// The writer took the ask before the drop: the drop finds nothing queued. A fresh ask of the
			// same id, queued later, leaves whole through the writer's filter.
			q.queueMu.Lock()
			q.queue.priority, q.queue.normal = nil, nil
			q.queueMu.Unlock()
			gs.haveQueue(A) // the writer's hooks, as a holding from A installs them
			gs.seacastAskQueued(A, []string{mid})
			gs.seacastOfferDropped(A, mid)
			gs.seacastAskQueued(A, []string{mid})
			q.queueMu.Lock()
			out := q.dispatch(ask(mid))
			q.queueMu.Unlock()
			if out != nil {
				for _, w := range out.GetControl().GetIwant() {
					written = append(written, w.GetMessageIDs()...)
				}
			}
			fresh = gs.seacastAskIsQueued(A, mid)
		})
		if !open || ctrB.F13AskDropped.Load() != 2 {
			t.Fatalf("open %v, asks dropped %d; want true 2", open, ctrB.F13AskDropped.Load())
		}
		if len(left) != 1 || left[0] != other || rpcs != 1 || hooks {
			t.Fatalf("A's queue after the drop: asks %q in %d RPCs, writer's hooks %v; want the other id alone, 1, false",
				left, rpcs, hooks)
		}
		if len(written) != 1 || written[0] != mid || !fresh {
			t.Fatalf("a fresh ask after a drop that found nothing queued: written %q, still queued %v; want it, true", written, fresh)
		}
	})
}

// TestSeacastOfferPastGroupsKept: A offers B one chunk of each of 65 groups in one IHAVE, and an ask by
// index of the first group's index MAX_PLAIN_CHUNKS. B keeps the offers of the first 64 groups
// (offeredRootsPerPeer) and asks for those chunks. The 65th group's offer is ignored: neither kept nor
// asked, so no ask is left that the offer's aging could not close. B learns none of the offered ids:
// on the path an id is recorded only for a chunk the node stored (seacast-lite.md §4.4). The index no
// group can have is ignored too.
func TestSeacastOfferPastGroupsKept(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		c := newSeacastF13Cell(t)
		A := c.hosts[0].ID()
		mids := make([]string, offeredRootsPerPeer+1)
		for k := range mids {
			mids[k] = fmt.Sprintf("%0*d", haveRootLen, k) + string(byte(1))
		}
		beyond := encodeWant(fmt.Sprintf("%0*d", haveRootLen, 0), core.MaxPlainChunks)
		topic := "seg/col/0"
		to := c.hosts[1].ID()
		c.read(0, func(gs *GossipSubRouter) {
			gs.sendRPC(to, rpcWithControl(nil, []*pb.ControlIHave{{TopicID: &topic, MessageIDs: mids}}, nil, nil, nil, nil), false)
		})
		time.Sleep(50 * time.Millisecond)
		var renewedBefore, renewedAfter time.Time
		g0 := seacastGID(fmt.Sprintf("%0*d", haveRootLen, 0))
		c.read(1, func(gs *GossipSubRouter) { renewedBefore = gs.pull.lastOffer[A][g0] })
		c.offer(beyond) // on its own: not cut by the asks one IHAVE allows
		c.read(1, func(gs *GossipSubRouter) {
			// The pull's own check, as a router without SEACAST reaches it: no offer check before it.
			gs.pullAddOffer(A, g0, core.MaxPlainChunks)
			renewedAfter = gs.pull.lastOffer[A][g0]
		})
		if renewedBefore.IsZero() || !renewedAfter.Equal(renewedBefore) {
			t.Fatalf("A's offers of the first group: last renewed %v, then %v after the ask by index %d, offered and "+
				"taken by the pull itself; want the same, set", renewedBefore, renewedAfter, core.MaxPlainChunks)
		}
		var kept, asked, learned int
		var lastKept, lastAsked, lastLearned, beyondKept, beyondAsked bool
		c.read(1, func(gs *GossipSubRouter) {
			for k, mid := range mids {
				o, a := gs.pullOffered(A, mid), gs.pullTried(A, mid)
				l := gs.have.mids[mid[:haveRootLen]] != nil && gs.have.mids[mid[:haveRootLen]].ids[1] == mid
				if k < offeredRootsPerPeer {
					if o {
						kept++
					}
					if a {
						asked++
					}
					if l {
						learned++
					}
					continue
				}
				lastKept, lastAsked, lastLearned = o, a, l
			}
			beyondKept, beyondAsked = gs.pullOffered(A, beyond), gs.pullTried(A, beyond)
		})
		if kept != offeredRootsPerPeer || asked != offeredRootsPerPeer || learned != 0 || lastKept || lastAsked || lastLearned {
			t.Fatalf("first %d groups: %d kept, %d asked, %d learned; the next: kept %v asked %v learned %v; want %d %d 0 false false false",
				offeredRootsPerPeer, kept, asked, learned, lastKept, lastAsked, lastLearned, offeredRootsPerPeer, offeredRootsPerPeer)
		}
		if beyondKept || beyondAsked {
			t.Fatalf("the ask by index of index %d: kept %v asked %v; want neither", core.MaxPlainChunks, beyondKept, beyondAsked)
		}
	})
}

// TestSeacastRetryArmedOnce: a chunk's retry armed twice in one event (as a sweep that drops two
// announcers' offers of it does) is one retry, and it is gone once it ran. A retry armed a window later
// is a retry of its own.
func TestSeacastRetryArmedOnce(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		c := newSeacastF13Cell(t)
		mid := strings.Repeat("r", haveRootLen) + string(byte(3))
		var armed, armedLater, after int
		c.read(1, func(gs *GossipSubRouter) {
			_, g, i, _ := gs.pullChunk(mid)
			gs.pullArmRetry(g, i)
			gs.pullArmRetry(g, i)
			armed = len(gs.pull.retryDue[pullKey{g, i}])
		})
		time.Sleep(100 * time.Millisecond)
		c.read(1, func(gs *GossipSubRouter) {
			_, g, i, _ := gs.pullChunk(mid)
			gs.pullArmRetry(g, i)
			armedLater = len(gs.pull.retryDue[pullKey{g, i}])
		})
		time.Sleep(time.Second)
		c.read(1, func(gs *GossipSubRouter) {
			_, g, i, _ := gs.pullChunk(mid)
			after = len(gs.pull.retryDue[pullKey{g, i}])
		})
		if armed != 1 || armedLater != 2 || after != 0 {
			t.Fatalf("retries armed: %d after two at once, %d after a third 100 ms on, %d once they ran; want 1 2 0", armed, armedLater, after)
		}
	})
}

// TestSeacastQueuedAskFoundAfterIDAged: B's ask of a chunk, under the chunk's learned id, is queued to
// A when the id ages out of B's learned ids (haveAge). The ask is still outstanding to A, found by the
// id it carries, and a drop of A's offer still drops it; the form it was found by goes with its record.
func TestSeacastQueuedAskFoundAfterIDAged(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		root := strings.Repeat("d", haveRootLen)
		c := newSeacastF13Cell(t)
		A := c.hosts[0].ID()
		mid := root + string(byte(2))
		g := seacastGID(root)
		var learned, outstanding, dropped bool
		var formsBefore, formsAfter int
		c.read(1, func(gs *GossipSubRouter) {
			gs.haveLearnID(mid)
			learned = gs.chunkAskID(root, 2) == mid
			gs.seacastAskQueued(A, []string{mid})
			delete(gs.have.mids, root) // aged out
			outstanding = gs.pullOutstanding(A, root, g, 2)
			formsBefore = len(gs.pull.askForms)
			gs.pullOfferDropped(A, root, g, 2)
			dropped = !gs.seacastAskIsQueued(A, mid)
			formsAfter = len(gs.pull.askForms)
		})
		if !learned || !outstanding || !dropped || formsBefore != 1 || formsAfter != 0 {
			t.Fatalf("asked under the learned id %v; once it aged out, outstanding %v, dropped with the offer %v; chunks with "+
				"forms recorded %d, then %d; want true, true, true, 1, 0", learned, outstanding, dropped, formsBefore, formsAfter)
		}
	})
}

// TestSeacastLastResortForgottenTimer: a last resort armed for a chunk leaves its timer pending when the
// window forgets the chunk's group (pullForgetGroup). The group is taken up again and the chunk armed
// anew, with a later timer. The old timer, firing, does nothing: the new arm stays armed for its own.
// The control, a chunk of another group armed alike and not forgotten: its timer runs and clears it.
func TestSeacastLastResortForgottenTimer(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		root := strings.Repeat("d", haveRootLen)
		c := newSeacastF13Cell(t)
		A := c.hosts[0].ID()
		g := seacastGID(root)
		h := seacastGID(strings.Repeat("h", haveRootLen)) // the control: armed alike, not forgotten
		var old, renewed, after, control, controlAfter uint64
		c.read(1, func(gs *GossipSubRouter) {
			gs.pull.lr.Arm(lrTakesEnv{A}, g, 2, time.Second) // the core's mark; this env arms no timer
			old = gs.pull.lr.Armed(g, 2)
			gs.pullEnvOf(g, true).AskParkedAfter(g, 2, time.Second) // the binding's timer for that arm
			gs.pull.lr.Arm(lrTakesEnv{A}, h, 2, time.Second)
			control = gs.pull.lr.Armed(h, 2)
			gs.pullEnvOf(h, true).AskParkedAfter(h, 2, time.Second)
			gs.pullForgetGroup(g)
			gs.pull.lr.Arm(lrTakesEnv{A}, g, 2, 10*time.Second)
			renewed = gs.pull.lr.Armed(g, 2)
		})
		time.Sleep(1100 * time.Millisecond) // past the old timer
		c.read(1, func(gs *GossipSubRouter) { after, controlAfter = gs.pull.lr.Armed(g, 2), gs.pull.lr.Armed(h, 2) })
		if old == 0 || renewed == 0 || renewed == old || after != renewed {
			t.Fatalf("tokens: the first arm %d, the new %d, the chunk's after the old timer %d; want two distinct, the new kept",
				old, renewed, after)
		}
		if control == 0 || controlAfter != 0 {
			t.Fatalf("the control's token %d, after its timer %d; want one, then none: the timer ran", control, controlAfter)
		}
	})
}

// TestSeacastLostAskClosesRecord: without v0.5's E, an IWANT the injected loss removes at the dispatch is
// reported as written: the chunk's queued record closes and its promise opens, so the retry does not
// pass its announcer over as outstanding for ever.
func TestSeacastLostAskClosesRecord(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		root := strings.Repeat("d", haveRootLen)
		c := newSeacastF13Cell(t)
		A := c.hosts[0].ID()
		mid := root + string(byte(2))
		var out *RPC
		var queuedBefore, queued, promised bool
		c.read(1, func(gs *GossipSubRouter) {
			gs.lossClass, gs.lossPct = lossClassIWant, 100
			gs.seacastAskQueued(A, []string{mid})
			queuedBefore = gs.seacastAskIsQueued(A, mid)
			out = gs.applyClassLoss(rpcWithControl(nil, nil, []*pb.ControlIWant{{MessageIDs: []string{mid}}}, nil, nil, nil), A)
			queued, promised = gs.seacastAskIsQueued(A, mid), gs.pullPromised(A, mid)
			gs.lossClass, gs.lossPct = lossClassNone, 0
		})
		if out != nil || !queuedBefore || queued || !promised {
			t.Fatalf("the IWANT lost %v; queued before %v, after %v; promised %v; want true, true, false, true",
				out == nil, queuedBefore, queued, promised)
		}
	})
}

// TestSeacastRetryForgottenGroup: a retry armed for a chunk whose group the window then forgets
// (pullForgetGroup) does nothing when it fires, though the group is taken up again and A offers the
// chunk meanwhile: the new offer is asked when its own path says so, not by the old timer. The control,
// a chunk of another group armed and offered alike and not forgotten: its retry asks A.
func TestSeacastRetryForgottenGroup(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		c := newSeacastF13Cell(t)
		A := c.hosts[0].ID()
		g, h := seacastGID(strings.Repeat("d", haveRootLen)), seacastGID(strings.Repeat("h", haveRootLen))
		var asked, controlAsked bool
		c.read(1, func(gs *GossipSubRouter) {
			gs.pullArmRetry(g, 2)
			gs.pullArmRetry(h, 2)
			gs.pullForgetGroup(g)
			gs.pullAddOffer(A, g, 2) // taken up again: A's offer, not asked (held back, as over a limit)
			gs.pullAddOffer(A, h, 2)
		})
		time.Sleep(gs1Window(c) + 2*retrySlack)
		c.read(1, func(gs *GossipSubRouter) {
			_, asked = gs.pull.asks.Tried(g, 2, A)
			_, controlAsked = gs.pull.asks.Tried(h, 2, A)
		})
		if asked || !controlAsked {
			t.Fatalf("after the retries fell due: the forgotten group's chunk asked %v, the control's %v; want false, true", asked, controlAsked)
		}
	})
}

// gs1Window is the asker's IWANT window.
func gs1Window(c *seacastF13Cell) time.Duration { return c.gs(1).iwantWindow }

// TestSeacastAskFormsGoWithRecords: an id a chunk was asked under is kept while an ask under it is queued,
// and goes when the ask's record closes: at its last peer's report, at the chunk's delivery, and when its
// peer goes. An id still queued to another peer stays.
func TestSeacastAskFormsGoWithRecords(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		root := strings.Repeat("f", haveRootLen)
		c := newSeacastF13Cell(t)
		A, P := c.hosts[0].ID(), peer.ID("another")
		mid := func(i int) string { return root + string(byte(i)) }
		var got []int
		c.read(1, func(gs *GossipSubRouter) {
			forms := func() { got = append(got, len(gs.pull.askForms)) }
			for i := 1; i <= 3; i++ {
				gs.seacastAskQueued(A, []string{mid(i)})
			}
			gs.seacastAskQueued(P, []string{mid(1)})
			forms()
			gs.seacastAskClosed(A, mid(1)) // still queued to P
			forms()
			gs.seacastAskClosed(P, mid(1))
			forms()
			gs.seacastAsksClosed(mid(2))
			forms()
			gs.seacastPeerGone(A)
			forms()
		})
		if want := []int{3, 3, 2, 1, 0}; !slices.Equal(got, want) {
			t.Fatalf("chunks with forms recorded: %v; want %v", got, want)
		}
	})
}

// TestSeacastLateWriteLeavesNewerAsk: the writer pops A's ask of a chunk, A's offer of it is dropped
// before the write, and a newer ask of the chunk is dispatched to A, all at the same instant of the
// clock. The older RPC's write report, which carries its pop's number, leaves the newer ask's record and
// opens no promise: that RPC did not carry the newer ask. The newer ask's own write, popped after its
// dispatch, closes the record and opens the promise.
func TestSeacastLateWriteLeavesNewerAsk(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		root := strings.Repeat("w", haveRootLen)
		c := newSeacastF13Cell(t)
		mid := root + string(byte(4))
		A := c.hosts[0].ID()
		ask := func() *RPC {
			return rpcWithControl(nil, nil, []*pb.ControlIWant{{MessageIDs: []string{mid}}}, nil, nil, nil)
		}
		var queuedAfterOld, promisedAfterOld, queuedAfterNew, promisedAfterNew bool
		var fail string
		c.read(1, func(gs *GossipSubRouter) {
			_, g, i, _ := gs.pullChunk(mid)
			q := newRpcQueue(16) // A's queue, its writer played here
			real := gs.p.peers[A]
			gs.p.peers[A] = q
			defer func() { gs.p.peers[A] = real }()
			q.onAsks = func(mids []string, popped uint64, at time.Time) { gs.promisesFromWrite(A, mids, popped, at) }
			gs.haveLearnID(mid)
			pop := func() *RPC {
				if err := q.Push(ask(), false); err != nil {
					fail = fmt.Sprintf("push: %v", err)
					return nil
				}
				gs.seacastAskQueued(A, []string{mid}) // the dispatch's record
				rpc, err := q.Pop(context.Background())
				if err != nil {
					fail = fmt.Sprintf("pop: %v", err)
				}
				return rpc
			}
			start := time.Now()
			old := pop()
			gs.seacastOfferDropped(A, mid)        // the writer took the ask: nothing left to strip; the record closes
			gs.seacastAskQueued(A, []string{mid}) // the newer ask, dispatched after the older pop
			if err := q.Push(ask(), false); err != nil {
				fail = fmt.Sprintf("push: %v", err)
				return
			}
			if !time.Now().Equal(start) {
				fail = "the clock moved: the older pop and the newer dispatch must share an instant"
				return
			}
			q.noteAsksWritten(old) // the older write returns
			queuedAfterOld, promisedAfterOld = gs.seacastAskIsQueued(A, mid), gs.pull.promises.Promised(A, g, i)
			newer, err := q.Pop(context.Background())
			if err != nil {
				fail = fmt.Sprintf("pop: %v", err)
				return
			}
			q.noteAsksWritten(newer)
			queuedAfterNew, promisedAfterNew = gs.seacastAskIsQueued(A, mid), gs.pull.promises.Promised(A, g, i)
		})
		if fail != "" {
			t.Fatal(fail)
		}
		if !queuedAfterOld || promisedAfterOld {
			t.Fatalf("after the older write: the newer ask recorded %v, promise open %v; want true false", queuedAfterOld, promisedAfterOld)
		}
		if queuedAfterNew || !promisedAfterNew {
			t.Fatalf("after the newer write: recorded %v, promise open %v; want false true", queuedAfterNew, promisedAfterNew)
		}
	})
}

// TestSeacastDroppedAskLeavesScorePromise: under v0.5's E the score's promise of an ask waits for its
// write. An ask dropped from A's queue with A's offer never leaves, so that promise goes with it, as
// for an ask the queue refused, and a write reported later for the same id opens none.
func TestSeacastDroppedAskLeavesScorePromise(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		root := strings.Repeat("s", haveRootLen)
		c := newSeacastF13Cell(t)
		mid := root + string(byte(4))
		A := c.hosts[0].ID()
		var pendingBefore, pendingAfter, opened int
		c.read(1, func(gs *GossipSubRouter) {
			gt := newGossipTracer()
			gt.atWrite, gt.followUpTime = true, time.Second
			real, realQ := gs.gossipTracer, gs.p.peers[A]
			gs.gossipTracer, gs.p.peers[A] = gt, newRpcQueue(16)
			defer func() { gs.gossipTracer, gs.p.peers[A] = real, realQ }()
			gs.haveLearnID(mid)
			if err := gs.p.peers[A].Push(rpcWithControl(nil, nil, []*pb.ControlIWant{{MessageIDs: []string{mid}}}, nil, nil, nil), false); err != nil {
				t.Errorf("push: %v", err)
				return
			}
			gt.AddPromise(A, []string{mid})
			gs.seacastAskQueued(A, []string{mid})
			pendingBefore = len(gt.pending[mid])
			gs.seacastOfferDropped(A, mid)
			pendingAfter = len(gt.pending[mid])
			opened = gt.promisesWritten(A, []string{mid}, nextAskSeq(), time.Now())
		})
		if pendingBefore != 1 || pendingAfter != 0 || opened != 0 {
			t.Fatalf("the score's pending promise: %d before the drop, %d after, %d opened at a later write; want 1 0 0",
				pendingBefore, pendingAfter, opened)
		}
	})
}

// TestSeacastWriteReportThroughTheHooks: the write report as the queue's own hooks make it
// (v05QueueHooks), with the score's promise under E. The writer pops A's ask of a chunk, A's offer of
// it is dropped, and a newer ask of the chunk is dispatched to A, at the same instant of the clock: the
// older write opens neither the park's promise nor the score's, and leaves the newer ask queued and its
// score promise pending. The newer ask's write, its asks all lost on the wire (an RPC that carries only
// the report), counts as written: it closes the record and opens both promises.
func TestSeacastWriteReportThroughTheHooks(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		root := strings.Repeat("h", haveRootLen)
		c := newSeacastF13Cell(t)
		mid := root + string(byte(4))
		A := c.hosts[0].ID()
		var gt *gossipTracer
		var q, realQ *rpcQueue
		var realGT *gossipTracer
		var old *RPC
		var fail string
		state := func(gs *GossipSubRouter) (queued, parkPromised, scorePromised bool, pending int) {
			_, g, i, _ := gs.pullChunk(mid)
			gt.Lock()
			defer gt.Unlock()
			_, scorePromised = gt.promises[mid][A]
			return gs.seacastAskIsQueued(A, mid), gs.pull.promises.Promised(A, g, i), scorePromised, len(gt.pending[mid])
		}
		start := time.Now()
		c.read(1, func(gs *GossipSubRouter) {
			gt = newGossipTracer()
			gt.atWrite, gt.followUpTime = true, time.Minute
			realGT, realQ = gs.gossipTracer, gs.p.peers[A]
			q = newRpcQueue(16) // A's queue, its writer played here, with the production hooks
			q.peer = A
			gs.gossipTracer, gs.p.peers[A] = gt, q
			gs.v05QueueHooks(q)
			if q.onAsks == nil {
				fail = "the hooks installed no write report"
				return
			}
			gs.haveLearnID(mid)
			dispatch := func() { // as the router dispatches an ask: the promise, the record, the push
				gt.AddPromise(A, []string{mid})
				gs.noteIWantSent(A, []string{mid})
				if err := q.Push(rpcWithControl(nil, nil, []*pb.ControlIWant{{MessageIDs: []string{mid}}}, nil, nil, nil), false); err != nil {
					fail = fmt.Sprintf("push: %v", err)
				}
			}
			dispatch()
			var err error
			if old, err = q.Pop(context.Background()); err != nil {
				fail = fmt.Sprintf("pop: %v", err)
				return
			}
			gs.seacastOfferDropped(A, mid) // the writer took the ask: the record and the pending promise close
			dispatch()                     // the newer ask
			q.noteAsksWritten(old)         // the older write returns; its report reaches the router's loop
		})
		synctest.Wait()
		if fail != "" {
			t.Fatal(fail)
		}
		if !time.Now().Equal(start) {
			t.Fatal("the clock moved: the older pop and the newer dispatch must share an instant")
		}
		var queued, park, score bool
		var pending int
		c.read(1, func(gs *GossipSubRouter) {
			queued, park, score, pending = state(gs)
			q.askLoss = func(ids []string) (kept, lost []string) { return nil, ids } // the next asks lost on the wire
			newer, err := q.Pop(context.Background())
			if err != nil {
				fail = fmt.Sprintf("pop: %v", err)
				return
			}
			if !newer.lostOnly {
				fail = "the lost asks' RPC is not the report-only one"
				return
			}
			q.noteAsksWritten(newer)
		})
		synctest.Wait()
		if fail != "" {
			t.Fatal(fail)
		}
		if !queued || park || score || pending != 1 {
			t.Fatalf("after the older write: the newer ask queued %v, park promise %v, score promise %v, pending %d; want true false false 1",
				queued, park, score, pending)
		}
		c.read(1, func(gs *GossipSubRouter) {
			queued, park, score, pending = state(gs)
			gs.gossipTracer, gs.p.peers[A] = realGT, realQ
		})
		if queued || !park || !score || pending != 0 {
			t.Fatalf("after the newer write: queued %v, park promise %v, score promise %v, pending %d; want false true true 0",
				queued, park, score, pending)
		}
	})
}

// TestSeacastDropBeyondQueuedAskWithoutOffer: a router whose chunk pull is off (no group-scoped
// parks) has an ask of index 40 queued to A, whose offer its offer table did not keep. When the
// group's count, 32, becomes known, drop_beyond finds the ask by its queued record and takes it out
// of A's queue.
func TestSeacastDropBeyondQueuedAskWithoutOffer(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		root := strings.Repeat("q", haveRootLen)
		c := newSeacastTieCell(t, 2)
		A := c.hosts[0].ID()
		mid40 := root + string(byte(40))
		var pullOff, offered, before, recorded bool
		var left []string
		c.read(1, func(gs *GossipSubRouter) {
			parks := gs.parkGroups
			gs.parkGroups = nil // the chunk pull off
			defer func() { gs.parkGroups = parks }()
			pullOff = !gs.pullOn()
			q := newRpcQueue(16)
			real := gs.p.peers[A]
			gs.p.peers[A] = q
			defer func() { gs.p.peers[A] = real }()
			gs.seacastPlaceRoot("seg/col/0", root)
			if err := q.Push(rpcWithControl(nil, nil, []*pb.ControlIWant{{MessageIDs: []string{mid40}}}, nil, nil, nil), false); err != nil {
				t.Errorf("push: %v", err)
				return
			}
			gs.iwantAsked[mid40] = time.Now()
			gs.seacastAskQueued(A, []string{mid40})
			_, offered = gs.offerTable[mid40]
			before = gs.seacastAskIsQueued(A, mid40)
			c.types[1].gateClosed = false // the count is known now
			gs.seacastDropBeyond([]string{root})
			recorded = gs.seacastAskIsQueued(A, mid40)
			q.queueMu.Lock()
			for _, rpc := range q.queue.priority {
				for _, w := range rpc.GetControl().GetIwant() {
					left = append(left, w.GetMessageIDs()...)
				}
			}
			for _, e := range q.queue.normal {
				for _, w := range e.GetControl().GetIwant() {
					left = append(left, w.GetMessageIDs()...)
				}
			}
			q.queueMu.Unlock()
		})
		if !pullOff || offered || !before {
			t.Fatalf("fixture: pull off %v, offer kept %v, ask recorded %v; want true false true", pullOff, offered, before)
		}
		if recorded || len(left) != 0 {
			t.Fatalf("after drop_beyond: the ask recorded %v, still queued %q; want false, none", recorded, left)
		}
	})
}

// TestSeacastOfferGuardReadsAsThePull: a client's parser that reads an ask by index of group G's
// index 7 as group H's index 40 makes it H's for the pull (groupIndex), and the offer guard reads
// it the same way: H's count is 32, so the offer is ignored. Without that parser the guard reads
// G's index 7, within its count, and keeps it.
func TestSeacastOfferGuardReadsAsThePull(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		G, H := strings.Repeat("G", haveRootLen), strings.Repeat("H", haveRootLen)
		w := encodeWant(G, 7)
		c := newSeacastF13Cell(t)
		var plain, claimed bool
		var root string
		var index uint32
		c.read(1, func(gs *GossipSubRouter) {
			gs.seacastPlaceRoot("seg/col/0", G)
			gs.seacastPlaceRoot("seg/col/0", H)
			plain = gs.seacastOfferIgnored(w)
			parse := gs.have.index
			gs.have.index = func(mid string) (string, uint32, bool) {
				if mid == w {
					return H, 40, true
				}
				return parse(mid)
			}
			defer func() { gs.have.index = parse }()
			root, index, _ = gs.groupIndex(w)
			claimed = gs.seacastOfferIgnored(w)
		})
		if root != H || index != 40 {
			t.Fatalf("fixture: the pull reads the ask as %q/%d; want H/40", root[:1], index)
		}
		if plain || !claimed {
			t.Fatalf("the guard: ignored as G/7 %v, as H/40 %v; want false true", plain, claimed)
		}
	})
}
