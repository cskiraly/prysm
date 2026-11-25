package pubsub

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
)

// TestSeacastRepairModes is the review of part 4's build, finding 13's f9 case on seacast.md §4.6's
// push_rebuilt: B decodes a coded group (K = 4 of 8) and repairs A and C, both short of K in its
// view (cleared before the installation), with the four rebuilt chunks: each is pushed its
// shortfall (8 pushes). The decisions are counted; the dispatch may drop the pushes at A's and C's
// holdings, which is the decode fixture's.
func TestSeacastRepairModes(t *testing.T) {
	type mode struct {
		name           string
		rNode          int
		pushes, offers int64
	}
	for _, m := range []mode{
		{name: "push", rNode: 2, pushes: 8},
	} {
		t.Run(m.name, func(t *testing.T) {
			synctestTest(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				const topic = "seg/row/1"
				root := strings.Repeat("1", haveRootLen)
				cell := func(r string, i byte) []byte {
					return append([]byte(r+string(rune(i))), bytes.Repeat([]byte{'a' + i}, 64)...)
				}
				hosts := getDefaultHosts(t, 3)
				table := &MapRoleTable{Joins: map[peer.ID]map[string]Join{}}
				for _, h := range hosts {
					table.Joins[h.ID()] = map[string]Join{"row": {Interest: InterestAll, Duty: DutyForward}}
				}
				ran := make(chan struct{}, 4)
				params := DefaultGossipSubParams()
				params.IDontWantMessageThreshold = 1
				psubs := getGossipsubsOptFn(ctx, hosts, func(i int, h host.Host) []Option {
					tp := TypeParams{RNode: m.rNode}
					if i == 1 {
						tp.DecodeAtK = func(string) bool { return true }
					}
					base := &testObjectType{name: "row", params: tp, topics: []string{topic}, store: NewMapCellStore(),
						threshold: 4, indexCount: 8, topicOf: func(r string) (string, bool) { return topic, len(r) == haveRootLen }}
					T := &testDecodeType{testObjectType: base, cell: cell, wait: 100 * time.Millisecond}
					if i == 1 {
						T.ran = ran
					}
					return []Option{
						WithGossipSubParams(params), WithMessageSignaturePolicy(StrictNoSign), WithMessageIdFn(seacastTestID),
						WithHaveBitmaps(seacastTestIndex),
						WithPhaseForwarding(func(tp string) bool { return !strings.HasPrefix(tp, "seg/") }, 2),
						WithSeacast([]ObjectType{T}, nil), WithSeacastRoles(table), WithSeacastMachine(),
					}
				})
				seacastOrdZeroAtEnd(t, psubs)
				for _, ps := range psubs {
					if _, err := ps.Subscribe(topic); err != nil {
						t.Fatal(err)
					}
				}
				connect(t, hosts[0], hosts[1])
				connect(t, hosts[1], hosts[2])
				time.Sleep(2 * time.Second)
				a, b := psubs[0], psubs[1]
				gsB := b.rt.(*GossipSubRouter)
				for i := range 4 {
					if err := a.Publish(topic, cell(root, byte(i))); err != nil { // lint:ignore uintcast -- a small index.
						t.Fatal(err)
					}
				}
				time.Sleep(50 * time.Millisecond) // B at K: DECODING, its decode running (100 ms)
				done := make(chan struct{})
				gsB.p.eval <- func() {
					// A and C short by K in B's eyes: each is owed the rebuilt chunks.
					delete(gsB.have.peers[hosts[0].ID()], root)
					delete(gsB.have.peers[hosts[2].ID()], root)
					close(done)
				}
				<-done
				<-ran
				time.Sleep(200 * time.Millisecond)
				ctr := gsB.seacast.ctr
				if ctr.CellsRebuilt.Load() != 4 || ctr.RepairPushes.Load() != m.pushes || ctr.RepairOffers.Load() != m.offers {
					t.Fatalf("rebuilt %d, repair pushes %d offers %d; want 4, %d, %d", ctr.CellsRebuilt.Load(), ctr.RepairPushes.Load(), ctr.RepairOffers.Load(), m.pushes, m.offers)
				}
			})
		})
	}
}
