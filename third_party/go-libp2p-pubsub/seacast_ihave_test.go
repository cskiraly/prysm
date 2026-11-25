package pubsub

import (
	"context"
	"strings"
	"testing"
	"time"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/host"
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
			WithSpecV05(SpecV05{LastResort: true, CloseWithConn: true}, nil))
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
		gs.p.eval <- func() { _, offered3 = gs.offerTable[mid(3)][A]; close(done) }
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
// runs the group machine without v0.5's options, as an application without them does, and puts the
// held-back chunk in place just before the retry, so that the retry's own checks are what decide.
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
						gs.seacast.groupOf(root).stopPull = true
					}
					// Inject the held-back offer at the retry boundary to exercise its guards.
					gs.seacastHoldBack([]string{mid}, A)
					gs.seacastRetryHeldBack()
					_, tried = gs.offerTried[mid][A]
				}
				<-done
				if asks, retried := c.asksTo(mid, A), gs.seacast.ctr.HeldBackRetried.Load(); asks != 0 || retried != 0 || tried {
					t.Fatalf("inactive group: asks %d, retried %d, tried %v; want 0 0 false", asks, retried, tried)
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
			WithSpecV05(SpecV05{LastResort: true, CloseWithConn: true}, nil))
		A := c.hosts[0].ID()
		ctrB := c.gs(1).seacast.ctr
		mid := func(i byte) string { return root + string(i) }
		c.offer(0, mid(1))
		c.offer(0, mid(2))
		gs := c.gs(1)
		var asked int
		read := func() {
			done := make(chan struct{})
			gs.p.eval <- func() { asked = gs.iasked[A]; close(done) }
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
