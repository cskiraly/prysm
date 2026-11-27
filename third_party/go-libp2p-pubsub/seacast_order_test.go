package pubsub

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	core "github.com/cskiraly/seacast-go"
	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
)

// seacastOrdZeroAtEnd asserts, when the test ends, that no router of psubs counted an honest
// ordering violation (seacast_order.go): every SEACAST fixture's honest run is a zero case of the
// checks. A faulty node's acts count in the OrdF twins and are not asserted here.
func seacastOrdZeroAtEnd(t *testing.T, psubs []*PubSub, except ...int) {
	t.Helper()
	t.Cleanup(func() {
		for i, ps := range psubs {
			gs, ok := ps.rt.(*GossipSubRouter)
			if !ok || gs.seacast == nil {
				continue
			}
			c := gs.seacast.ctr
			for k := ordBeforeHeader; k <= ordRebuiltExpired; k++ {
				if slices.Contains(except, k) {
					continue
				}
				if n := c.ordCounter(k, false).Load(); n != 0 {
					t.Errorf("node %d: honest ordering check %d counted %d (want 0)", i, k, n)
				}
			}
		}
	})
}

// TestSeacastOrdering is the ordering checks' positive controls: each counter moves on the act it
// names, on an honest node in the honest counter and on a faulty node in its OrdF twin, and the
// conforming act moves nothing. One node, a row type with K = 2 of 4.
func TestSeacastOrdering(t *testing.T) {
	for _, faulty := range []bool{false, true} {
		name := "honest"
		if faulty {
			name = "faulty"
		}
		t.Run(name, func(t *testing.T) {
			synctestTest(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				root := strings.Repeat("R", haveRootLen)
				hosts := getDefaultHosts(t, 2)
				self, other := hosts[0].ID(), hosts[1].ID()
				table := &MapRoleTable{Joins: map[peer.ID]map[string]Join{
					self: {"row": {Interest: InterestAll, Duty: DutyForward}}, other: {"row": {Interest: InterestAll, Duty: DutyForward}}}}
				row := &testObjectType{name: "row", topics: []string{"seg/row"}, store: NewMapCellStore(), indexCount: 4, threshold: 2,
					params: TypeParams{}}
				psubs := getGossipsubsOptFn(ctx, hosts, func(i int, _ host.Host) []Option {
					o := []Option{WithMessageSignaturePolicy(StrictNoSign), WithMessageIdFn(seacastTestID), WithHaveBitmaps(seacastTestIndex),
						WithPhaseForwarding(func(tp string) bool { return !strings.HasPrefix(tp, "seg/") }, 2),
						WithSeacast([]ObjectType{row}, nil), WithSeacastRoles(table), WithSeacastMachine()}
					if i == 0 && faulty {
						o = append(o, WithSeacastFault("hoard"))
					}
					return o
				})
				for _, ps := range psubs {
					if _, err := ps.Subscribe("seg/row"); err != nil {
						t.Fatal(err)
					}
				}
				connect(t, hosts[0], hosts[1])
				time.Sleep(time.Second)
				gs := psubs[0].rt.(*GossipSubRouter)
				run := func(f func()) {
					done := make(chan struct{})
					gs.p.eval <- func() { f(); close(done) }
					<-done
				}
				cell := func(i byte) *pb.Message {
					tp := "seg/row"
					return &pb.Message{Topic: &tp, Data: append([]byte(root+string(rune(i))), 'x')}
				}
				hold := func(i uint32) {
					e := gs.have.mine[seacastGID(root)]
					if e == nil {
						e = &core.Holding{}
						gs.have.mine[seacastGID(root)] = e
					}
					e.Bits.Set(i)
				}
				c := gs.seacast.ctr
				got := func(k int) int64 { return c.ordCounter(k, faulty).Load() }
				if gs.seacast.roots == nil {
					gs.seacast.roots = map[string]ObjectType{}
				}
				gs.seacast.roots[root] = row // the root placed on the path, as a first sight does
				var steps []string
				expect := func(what string, k int, want int64) {
					if n := got(k); n != want {
						steps = append(steps, what)
						t.Errorf("%s: counter %d is %d, want %d", what, k, n, want)
					}
				}
				run(func() {
					// (ii) a holding of nothing held, then of one held cell (conforming).
					gs.seacastOrderHolding(root, false)
					expect("holding with nothing held", ordHoldingUnheld, 1)
					hold(0)
					gs.seacastOrderHolding(root, false)
					expect("holding with one held", ordHoldingUnheld, 1)
					// (ii) claim_all with 1 of K = 2 held and no decode; then with K held (conforming).
					gs.seacastOrderHolding(root, true)
					expect("claim_all short of K", ordClaimShort, 1)
					hold(1)
					gs.seacastOrderHolding(root, true)
					expect("claim_all at K", ordClaimShort, 1)
					// The per-event conditions: a decode short of K, one on a whole group, one at K.
					gs.seacastOrderDecode(row, root, 1)
					expect("decode short of K", ordDecodeShort, 1)
					gs.seacastOrderDecode(row, root, 4)
					expect("decode on a whole group", ordDecodeWhole, 1)
					gs.seacastOrderDecode(row, root, 2)
					expect("decode at K", ordDecodeShort, 1)
					// (iii) a rebuilt cell sent without its decode's success, then with it.
					gs.seacast.markRebuilt(root, 2)
					gs.seacastOrderSend(other, rpcWithMessages(cell(2)))
					expect("rebuilt cell before its decode", ordRebuiltEarly, 1)
					gs.seacast.markRebuiltOK(root, 2)
					gs.seacastOrderSend(other, rpcWithMessages(cell(2)))
					expect("rebuilt cell after its decode", ordRebuiltEarly, 1)
					// (iv) a probe asked of a peer whose claim was not received, then after the claim.
					gs.seacastOrderProbe(other, root)
					expect("probe before the claim", ordProbeBeforeClaim, 1)
					if gs.have.peers[other] == nil {
						gs.have.peers[other] = map[core.GroupID]*core.PeerHolding{}
					}
					gs.have.peers[other][seacastGID(root)] = &core.PeerHolding{ClaimAll: true} // the claim received
					gs.seacastOrderProbe(other, root)
					expect("probe after the claim", ordProbeBeforeClaim, 1)
					// (i) the header closed: a fill, a holding and a sent cell each count; open again, none.
					row.gateClosed = true
					gs.seacastOrderFill(row, root, 0)
					gs.seacastOrderHolding(root, false)
					gs.seacastOrderSend(other, rpcWithMessages(cell(0)))
					expect("fill, holding and send before the header", ordBeforeHeader, 3)
					row.gateClosed = false
					gs.seacastOrderFill(row, root, 0)
					gs.seacastOrderSend(other, rpcWithMessages(cell(0)))
					expect("after the header", ordBeforeHeader, 3)
					// Own data: the header closed, a holding and a send of an own root count nothing.
					row.gateClosed = true
					gs.seacast.ownRoots = map[string]struct{}{root: {}}
					gs.seacastOrderHolding(root, false)
					gs.seacastOrderSend(other, rpcWithMessages(cell(0)))
					expect("own data before the header", ordBeforeHeader, 3)
					row.gateClosed = false
					// The expiry: a rebuilt cell sent after its group expired.
					gs.seacast.expired = map[string]struct{}{root: {}}
					gs.seacastOrderSend(other, rpcWithMessages(cell(2)))
					expect("rebuilt cell after the expiry", ordRebuiltExpired, 1)
					// The group's state dropped (its rebuilt indices gone): a rebuilt chunk sent after is
					// still seen as one (rebuiltGone).
					gs.seacast.expired = nil
					gs.seacastExpire([]string{root}) // the window's own path: orderForget before the rebuilt indices go
					if gs.seacast.rebuilt[root] != nil {
						t.Fatal("the expiry left the group's rebuilt indices")
					}
					gs.seacastOrderSend(other, rpcWithMessages(cell(2)))
					expect("rebuilt cell after its group was dropped", ordRebuiltExpired, 2)
					// An ordinary chunk id as the acknowledgement: of a cell not held, then of a held one.
					ack := func(i byte) *RPC {
						return rpcWithControl(nil, nil, nil, nil, nil, []*pb.ControlIDontWant{{MessageIDs: []string{root + string(rune(i))}}})
					}
					gs.seacastOrderSend(other, ack(3))
					expect("ordinary-id acknowledgement of a cell not held", ordHoldingUnheld, 2)
					gs.seacastOrderSend(other, ack(0))
					expect("ordinary-id acknowledgement of a held cell", ordHoldingUnheld, 2)
					// Nothing in the other set of counters.
					for k := ordBeforeHeader; k <= ordRebuiltExpired; k++ {
						if n := c.ordCounter(k, !faulty).Load(); n != 0 {
							t.Errorf("the %v counters moved: kind %d is %d", !faulty, k, n)
						}
					}
				})
				_ = steps
			})
		})
	}
}
