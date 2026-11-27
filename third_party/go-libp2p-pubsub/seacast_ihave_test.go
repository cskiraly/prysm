package pubsub

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
)

// newSeacastIHaveCell is the ask cell (seacast_ask_test.go) with B's gossipsub parameters changed
// by mod: the two IHAVE limits set low enough to reach within a heartbeat, and B's heartbeat at
// 5 s, so that none falls between the offers of a case (the counters reset at the heartbeat).
func newSeacastIHaveCell(t *testing.T, mod func(*GossipSubParams), extra ...Option) *seacastAskCell {
	t.Helper()
	c := &seacastAskCell{}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	c.hosts = getDefaultHosts(t, 2)
	c.pubsubs = getGossipsubsOptFn(ctx, c.hosts, func(i int, h host.Host) []Option {
		params := DefaultGossipSubParams()
		params.IDontWantMessageThreshold = 1
		if i == 1 {
			params.HeartbeatInterval = 5 * time.Second
			mod(&params)
		}
		T := &testObjectType{name: "col", params: TypeParams{RNode: 2}, topics: []string{"seg/col/0"}, store: NewMapCellStore()}
		return append([]Option{
			WithGossipSubParams(params),
			WithMessageIdFn(seacastTestID),
			WithHaveBitmaps(seacastTestIndex),
			WithPhaseForwarding(func(topic string) bool { return topic != "seg/col/0" }, 2),
			WithIWantDiscipline(200 * time.Millisecond),
			WithIHaveCommitmentPark(1, 400*time.Millisecond, 30*time.Second, &c.breaks, &c.parks),
			WithIHaveParkPerGroup(nil),
			WithOfferTable(nil),
			WithRequestObservation(),
			WithSeacast([]ObjectType{T}, nil),
		}, extra...)
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

// read runs f on router i's goroutine and waits for it.
func (c *seacastAskCell) read(i int, f func(*GossipSubRouter)) {
	gs := c.gs(i)
	done := make(chan struct{})
	gs.p.eval <- func() { f(gs); close(done) }
	<-done
}

// control sends, from A to B, a control RPC with no IHAVE in it (an IDONTWANT).
func (c *seacastAskCell) control(mid string) {
	to := c.hosts[1].ID()
	gs := c.gs(0)
	done := make(chan struct{})
	gs.p.eval <- func() {
		gs.sendRPC(to, rpcWithControl(nil, nil, nil, nil, nil, []*pb.ControlIDontWant{{MessageIDs: []string{mid}}}), false)
		close(done)
	}
	<-done
	time.Sleep(50 * time.Millisecond)
}

// TestSeacastIHaveMessagesLimitHoldsBack: with MaxIHaveMessages 2 at B, A's third offer-bearing RPC
// within a heartbeat is recorded as an offer and not asked for until the heartbeat, where the retry
// asks it; control RPCs without an IHAVE do not count.
func TestSeacastIHaveMessagesLimitHoldsBack(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		root := strings.Repeat("h", haveRootLen)
		c := newSeacastIHaveCell(t, func(p *GossipSubParams) { p.MaxIHaveMessages = 2 },
			WithSpecV05(SpecV05{}, nil))
		A := c.hosts[0].ID()
		ctrB := c.gs(1).seacast.ctr
		mid := func(i byte) string { return root + string(i) }
		c.offer(0, mid(1))
		c.control(mid(50)) // two control RPCs without offers: not counted
		c.control(mid(51))
		c.offer(0, mid(2))
		c.offer(0, mid(3)) // the third offer-bearing RPC: over the limit
		var offered3 bool
		gs := c.gs(1)
		done := make(chan struct{})
		gs.p.eval <- func() { offered3 = gs.pullOffered(A, mid(3)); close(done) }
		<-done
		if a1, a2, a3 := c.asksTo(mid(1), A), c.asksTo(mid(2), A), c.asksTo(mid(3), A); a1 != 1 || a2 != 1 || a3 != 0 || !offered3 || ctrB.HeldBack.Load() != 1 {
			t.Fatalf("within the heartbeat: asks %d %d %d, third offered %v, held back %d; want 1 1 0 true 1", a1, a2, a3, offered3, ctrB.HeldBack.Load())
		}
		time.Sleep(5200 * time.Millisecond) // B's heartbeat: the limits start again, the held-back retried
		if a3 := c.asksTo(mid(3), A); a3 != 1 || ctrB.HeldBackRetried.Load() != 1 {
			t.Fatalf("after the heartbeat: asks for the third %d, retried %d; want 1 1", a3, ctrB.HeldBackRetried.Load())
		}
	})
}

// TestSeacastHeldBackOfAnInactiveGroupNotAsked: a chunk the limit held back, whose group expired or
// pulls no more by the heartbeat, is not asked for there, and leaves no tried ask behind. The cell
// runs the group machine without v0.5's options, as an application without them does, and puts A's
// offer of the chunk and the chunk held back in place just before the retry, so that the retry's own
// checks are what decide: it runs for the chunk, and counts, and asks nobody.
func TestSeacastHeldBackOfAnInactiveGroupNotAsked(t *testing.T) {
	for _, state := range []string{"expired", "stopped"} {
		t.Run(state, func(t *testing.T) {
			synctestTest(t, func(t *testing.T) {
				c := newSeacastIHaveCell(t, func(*GossipSubParams) {}, WithSeacastMachine())
				gs := c.gs(1)
				A := c.hosts[0].ID()
				root := strings.Repeat("h", haveRootLen)
				mid := root + "\x03"
				var tried bool
				done := make(chan struct{})
				gs.p.eval <- func() {
					defer close(done)
					gs.seacastRootType(root)
					if state == "expired" {
						gs.seacastExpire([]string{root})
					} else {
						gs.seacast.groupOf(root).StopPull = true
					}
					// Inject the offer and its held-back ask at the retry boundary to exercise its guards.
					gs.pullAddOffer(A, seacastGID(root), 3)
					gs.seacastHoldBack([]string{mid}, A)
					gs.seacastHeartbeatLimits()
					tried = gs.pullTried(A, mid)
				}
				<-done
				if asks, retried := c.asksTo(mid, A), gs.seacast.ctr.HeldBackRetried.Load(); asks != 0 || retried != 1 || tried {
					t.Fatalf("inactive group: asks %d, retried %d, tried %v; want 0 1 false", asks, retried, tried)
				}
			})
		})
	}
}

// TestSeacastIHaveLengthLimitRetryDoesNotCount: with MaxIHaveLength 1 at B, A's second offered chunk
// is held back; the heartbeat's retry asks it without counting against the limit.
func TestSeacastIHaveLengthLimitRetryDoesNotCount(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		root := strings.Repeat("i", haveRootLen)
		c := newSeacastIHaveCell(t, func(p *GossipSubParams) { p.MaxIHaveLength = 1 },
			WithSpecV05(SpecV05{}, nil))
		A := c.hosts[0].ID()
		ctrB := c.gs(1).seacast.ctr
		mid := func(i byte) string { return root + string(i) }
		c.offer(0, mid(1))
		c.offer(0, mid(2))
		gs := c.gs(1)
		var asked int
		read := func() {
			done := make(chan struct{})
			gs.p.eval <- func() { _, asked = gs.seacast.ihave.Counts(A); close(done) }
			<-done
		}
		read()
		if a1, a2 := c.asksTo(mid(1), A), c.asksTo(mid(2), A); a1 != 1 || a2 != 0 || asked != 1 || ctrB.HeldBack.Load() != 1 {
			t.Fatalf("within the heartbeat: asks %d %d, counted %d, held back %d; want 1 0 1 1", a1, a2, asked, ctrB.HeldBack.Load())
		}
		time.Sleep(5200 * time.Millisecond) // B's heartbeat
		read()
		if a2 := c.asksTo(mid(2), A); a2 != 1 || asked != 0 || ctrB.HeldBackRetried.Load() != 1 {
			t.Fatalf("after the heartbeat: asks for the second %d, counted %d, retried %d; want 1 0 1", a2, asked, ctrB.HeldBackRetried.Load())
		}
	})
}

// TestSeacastIHaveLimitPerCandidate: the ask limit is tested per candidate, and only the asks made
// count. With MaxIHaveLength 1 at B, each round A offers B, in one RPC, a chunk B has just asked of
// another peer, in flight, and a chunk of another group, each in an IHAVE entry of its own (an entry
// carries at most MaxIHaveLength ids). Drawn first, the chunk in flight is refused by may_ask and not
// counted, so the other is asked; drawn second, it is held back, as the spec tests the limit before
// may_ask. Either way the other chunk is asked at once, in every round: a refused candidate leaves
// the one ask allowed to the next. Cut to the allowance before the policies, as the limit was once
// applied, half the rounds would hold the other chunk back instead. The chunk in flight is held back
// in some rounds (drawn second) and not in others (drawn first, refused): skipping it once the
// allowance is used, instead of holding it back, would hold none. The rounds fall a heartbeat apart,
// so the limits start again for each, and their draws are independent.
func TestSeacastIHaveLimitPerCandidate(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		c := newSeacastIHaveCell(t, func(p *GossipSubParams) { p.MaxIHaveLength = 1 }, WithSpecV05(SpecV05{}, nil))
		A, B := c.hosts[0].ID(), c.hosts[1].ID()
		topic := "seg/col/0"
		const rounds = 32
		heldRounds := 0
		for k := range rounds {
			inFlight := fmt.Sprintf("f%-*d", haveRootLen-1, k) + "\x01"
			open := fmt.Sprintf("o%-*d", haveRootLen-1, k) + "\x01"
			c.read(1, func(gs *GossipSubRouter) {
				gs.pullMayAsk(peer.ID("another"), seacastGID(inFlight[:haveRootLen]), 1) // asked of another peer now
			})
			c.read(0, func(gsA *GossipSubRouter) {
				gsA.sendRPC(B, rpcWithControl(nil, []*pb.ControlIHave{{TopicID: &topic, MessageIDs: []string{inFlight}}, {TopicID: &topic, MessageIDs: []string{open}}}, nil, nil, nil, nil), false)
			})
			time.Sleep(50 * time.Millisecond)
			if a, f := c.asksTo(open, A), c.asksTo(inFlight, A); a != 1 || f != 0 {
				t.Fatalf("round %d: asks of the open chunk %d, of the one in flight %d; want 1 0", k, a, f)
			}
			c.read(1, func(gs *GossipSubRouter) {
				if gs.pull.held.Holds(seacastGID(inFlight[:haveRootLen]), 1) {
					heldRounds++
				}
			})
			time.Sleep(5200 * time.Millisecond) // B's heartbeat: the limits start again
		}
		if heldRounds == 0 || heldRounds == rounds {
			t.Fatalf("the chunk in flight held back in %d of %d rounds; want some, not all", heldRounds, rounds)
		}
	})
}

// TestSeacastHeldBackAsksAnnouncers: a chunk held back is retried at the heartbeat from the offer table
// alone. An offer that went meanwhile (its connection closed, or it aged out) leaves no one to ask:
// the peer that offered it is not asked on its strength. The control, a chunk held back whose offer
// is still in the table, is asked of its announcer.
func TestSeacastHeldBackAsksAnnouncers(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		c := newSeacastIHaveCell(t, func(*GossipSubParams) {}, WithSpecV05(SpecV05{}, nil))
		gs := c.gs(1)
		A := c.hosts[0].ID()
		gone := strings.Repeat("g", haveRootLen) + "\x02"
		kept := strings.Repeat("k", haveRootLen) + "\x02"
		var goneTried, keptTried bool
		c.read(1, func(gs *GossipSubRouter) {
			g := seacastGID(gone[:haveRootLen])
			gs.pullAddOffer(A, g, 2)
			gs.seacastHoldBack([]string{gone}, A)            // held back while A's offer of it stood
			gs.pullOfferDropped(A, gone[:haveRootLen], g, 2) // then the offer went (drop_offer: aged, or its tie dropped)
			gs.pullAddOffer(A, seacastGID(kept[:haveRootLen]), 2)
			gs.seacastHoldBack([]string{kept}, A)
			gs.seacastHeartbeatLimits()
			goneTried, keptTried = gs.pullTried(A, gone), gs.pullTried(A, kept)
		})
		if goneTried || !keptTried || gs.seacast.ctr.HeldBackRetried.Load() != 2 {
			t.Fatalf("retried %d; asked: the chunk whose offer went %v, the control %v; want 2, false, true",
				gs.seacast.ctr.HeldBackRetried.Load(), goneTried, keptTried)
		}
	})
}

// TestSeacastIHaveCountsGoWithConnection: a peer's IHAVE counts are its connection's
// (seacast-lite.md §8.1): when it goes, they go, and a peer reconnecting within the heartbeat starts
// from none. Another peer's stay. The connection closes for real, and its stream's teardown reaches
// the router.
func TestSeacastIHaveCountsGoWithConnection(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		c := newSeacastIHaveCell(t, func(*GossipSubParams) {}, WithSpecV05(SpecV05{}, nil))
		A, P := c.hosts[0].ID(), peer.ID("another")
		var af, aa, pf, pa int
		c.read(1, func(gs *GossipSubRouter) {
			for _, q := range []peer.ID{A, P} {
				gs.seacast.ihave.Frame(q)
				gs.seacast.ihave.Ask(q)
			}
		})
		if err := c.hosts[1].Network().ClosePeer(A); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Second) // within B's 5 s heartbeat, which would clear the counts anyway
		c.read(1, func(gs *GossipSubRouter) {
			af, aa = gs.seacast.ihave.Counts(A)
			pf, pa = gs.seacast.ihave.Counts(P)
		})
		if af != 0 || aa != 0 || pf != 1 || pa != 1 {
			t.Fatalf("after A's connection went: A's frames %d and asks %d, the other's %d and %d; want 0 0 1 1", af, aa, pf, pa)
		}
	})
}

// TestSeacastIHaveStockKeepsAllowanceCut: on a router with the path, ids outside any group keep the
// stock rule. With MaxIHaveLength 1 at B, A offers two such ids, x and y, in one IHAVE, and x is in
// flight from another peer (the pull discipline refuses it). The allowance is cut before the policies:
// drawn first, x takes the one ask allowed, is refused, and y is held back; drawn second, y is asked.
// So y is asked at once in some rounds and not in others. Asked per candidate, as a chunk is, y would
// be asked in every round. The rounds fall a heartbeat apart, so the limits start again for each. A
// holds neither id, so an ask of it breaks its promise and parks A, for every stock id: each round
// starts with A's park lifted, and drops what it held back, which the heartbeat would ask of A.
func TestSeacastIHaveStockKeepsAllowanceCut(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		c := newSeacastIHaveCell(t, func(p *GossipSubParams) { p.MaxIHaveLength = 1 }, WithSpecV05(SpecV05{}, nil))
		A, B := c.hosts[0].ID(), c.hosts[1].ID()
		topic := "seg/col/0"
		const rounds = 32
		asked, held := 0, 0
		for k := range rounds {
			x, y := fmt.Sprintf("stock x %02d", k), fmt.Sprintf("stock y %02d", k)
			c.read(1, func(gs *GossipSubRouter) {
				delete(gs.parkGroups, A)
				gs.iwantAsked[x] = time.Now() // asked of another peer now
			})
			c.read(0, func(gsA *GossipSubRouter) {
				gsA.sendRPC(B, rpcWithControl(nil, []*pb.ControlIHave{{TopicID: &topic, MessageIDs: []string{x}}, {TopicID: &topic, MessageIDs: []string{y}}}, nil, nil, nil, nil), false)
			})
			time.Sleep(50 * time.Millisecond)
			if n := c.asksTo(x, A); n != 0 {
				t.Fatalf("round %d: x, in flight, asked %d times", k, n)
			}
			n := c.asksTo(y, A)
			asked += n
			c.read(1, func(gs *GossipSubRouter) {
				_, hx := gs.seacast.heldBack[x]
				_, hy := gs.seacast.heldBack[y]
				if hy {
					held++
				}
				// x drawn first: refused, and y past the allowance, held back. y drawn first: asked,
				// and x held back.
				if !(n == 0 && !hx && hy) && !(n == 1 && hx && !hy) {
					t.Errorf("round %d: y asked %d, x held back %v, y held back %v; want 0 false true or 1 true false", k, n, hx, hy)
				}
				delete(gs.seacast.heldBack, x)
				delete(gs.seacast.heldBack, y)
			})
			time.Sleep(5200 * time.Millisecond) // B's heartbeat: the limits start again
		}
		if asked == 0 || held == 0 || asked+held != rounds {
			t.Fatalf("y asked at once in %d of %d rounds, held back in %d; want some of each, together all", asked, rounds, held)
		}
	})
}

// TestSeacastIHaveEntryKeepsEveryOffer: with MaxIHaveLength 1 at B, A's IHAVE names two chunks of a
// group in one entry. Both are recorded as A's offers (seacast-lite.md §4.4 records every offer; the
// limits hold back the asks): a path topic's entry is not cut at MaxIHaveLength.
func TestSeacastIHaveEntryKeepsEveryOffer(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		c := newSeacastIHaveCell(t, func(p *GossipSubParams) { p.MaxIHaveLength = 1 }, WithSpecV05(SpecV05{}, nil))
		A, B := c.hosts[0].ID(), c.hosts[1].ID()
		topic := "seg/col/0"
		root := strings.Repeat("e", haveRootLen)
		one, two := root+"\x01", root+"\x02"
		c.read(0, func(gsA *GossipSubRouter) {
			gsA.sendRPC(B, rpcWithControl(nil, []*pb.ControlIHave{{TopicID: &topic, MessageIDs: []string{one, two}}}, nil, nil, nil, nil), false)
		})
		time.Sleep(50 * time.Millisecond)
		var o1, o2 bool
		c.read(1, func(gs *GossipSubRouter) { o1, o2 = gs.pullOffered(A, one), gs.pullOffered(A, two) })
		if !o1 || !o2 {
			t.Fatalf("offers recorded from one entry of two: %v %v; want both", o1, o2)
		}
	})
}

// TestSeacastIHaveHeldBackDrawn: an IHAVE that comes over B's limits has every chunk it names held
// back, in a drawn order, as on_ihave's candidates are: the heartbeat's retry goes in the order held,
// and the sender's order doesn't choose it. Each round's IHAVE names a group's chunks 1 and 2 in that
// order; both orders are held.
func TestSeacastIHaveHeldBackDrawn(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		c := newSeacastIHaveCell(t, func(p *GossipSubParams) { p.MaxIHaveMessages = 0 }, WithSpecV05(SpecV05{}, nil))
		B := c.hosts[1].ID()
		topic := "seg/col/0"
		const rounds = 32
		reversed, held := 0, 0
		for k := range rounds {
			root := fmt.Sprintf("%-*d", haveRootLen, k)
			c.read(0, func(gsA *GossipSubRouter) {
				gsA.sendRPC(B, rpcWithControl(nil, []*pb.ControlIHave{{TopicID: &topic, MessageIDs: []string{root + "\x01", root + "\x02"}}}, nil, nil, nil, nil), false)
			})
			time.Sleep(50 * time.Millisecond)
			var order []uint32
			c.read(1, func(gs *GossipSubRouter) { order = gs.pull.held.Indices(seacastGID(root)) })
			if len(order) != 2 {
				continue // a heartbeat fell between the offer and the read, and retried them
			}
			held++
			if order[0] == 2 {
				reversed++
			}
		}
		if held < rounds/2 || reversed == 0 || reversed == held {
			t.Fatalf("held in %d rounds, in the reverse order in %d; want at least %d, some reversed, not all", held, reversed, rounds/2)
		}
	})
}

// TestSeacastIHaveGatedNotHeldBack: over B's limits, A offers a chunk of a group that pulls no more
// (stop_pull at its completion). The offer is recorded, but the chunk is not held back: it is no
// candidate (seacast-lite.md §4.4 tests gated before the limits). A chunk of another group is held.
func TestSeacastIHaveGatedNotHeldBack(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		c := newSeacastIHaveCell(t, func(p *GossipSubParams) { p.MaxIHaveMessages = 0 }, WithSpecV05(SpecV05{}, nil), WithSeacastMachine())
		A, B := c.hosts[0].ID(), c.hosts[1].ID()
		topic := "seg/col/0"
		stopped, open := strings.Repeat("s", haveRootLen), strings.Repeat("o", haveRootLen)
		c.read(1, func(gs *GossipSubRouter) {
			gs.seacastRootType(stopped)
			gs.seacast.groupOf(stopped).StopPull = true
		})
		c.read(0, func(gsA *GossipSubRouter) {
			gsA.sendRPC(B, rpcWithControl(nil, []*pb.ControlIHave{{TopicID: &topic, MessageIDs: []string{stopped + "\x03", open + "\x03"}}}, nil, nil, nil, nil), false)
		})
		time.Sleep(50 * time.Millisecond)
		var offered, heldStopped, heldOpen bool
		c.read(1, func(gs *GossipSubRouter) {
			offered = gs.pullOffered(A, stopped+"\x03")
			heldStopped = gs.pull.held.Holds(seacastGID(stopped), 3)
			heldOpen = gs.pull.held.Holds(seacastGID(open), 3)
		})
		if !offered || heldStopped || !heldOpen {
			t.Fatalf("the stopped group's chunk: offered %v, held back %v; the other's held back %v; want true false true", offered, heldStopped, heldOpen)
		}
	})
}

// TestSeacastHeldBackSkipsAgedOffer: a chunk held back is retried at the heartbeat from offers that
// still live. A's offers of one group are past their 30 s (seacast-lite.md §8.1), though the
// heartbeat's sweep has not dropped them yet: its chunk is asked of nobody. The control, a group A
// offered just now, is asked.
func TestSeacastHeldBackSkipsAgedOffer(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		c := newSeacastIHaveCell(t, func(*GossipSubParams) {}, WithSpecV05(SpecV05{}, nil))
		A := c.hosts[0].ID()
		aged := strings.Repeat("a", haveRootLen) + "\x02"
		fresh := strings.Repeat("f", haveRootLen) + "\x02"
		var agedTried, freshTried bool
		c.read(1, func(gs *GossipSubRouter) {
			ga := seacastGID(aged[:haveRootLen])
			gs.pullAddOffer(A, ga, 2)
			gs.pull.lastOffer[A][ga] = time.Now().Add(-offerTTL) // offered 30 s ago, not swept yet
			gs.pullAddOffer(A, seacastGID(fresh[:haveRootLen]), 2)
			gs.seacastHoldBack([]string{aged, fresh}, A)
			gs.seacastHeartbeatLimits()
			agedTried, freshTried = gs.pullTried(A, aged), gs.pullTried(A, fresh)
		})
		if agedTried || !freshTried {
			t.Fatalf("asked: the chunk of aged offers %v, the control %v; want false true", agedTried, freshTried)
		}
	})
}
