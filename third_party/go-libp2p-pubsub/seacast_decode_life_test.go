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

// forgetPeerViews clears gs's views of p's holding of root, the router's ledger and the writer's
// mirror, so that a copy gs sends p now is one in flight before p's holding came (a test's way to
// get a chunk to a peer projected at K). Router goroutine.
func forgetPeerViews(gs *GossipSubRouter, p peer.ID, root string) {
	delete(gs.have.peers[p], seacastGID(root))
	if q := gs.p.peers[p]; q != nil && q.have != nil {
		q.queueMu.Lock()
		delete(q.have.theirs, root)
		delete(q.have.popped, root)
		q.queueMu.Unlock()
	}
}

// TestSeacastDecodeLifecycle is the review of part 4's build, finding 13's f7 and f1 cases on
// seacast.md §4.6's decode_group: B decodes a coded group (K = 4 of 8) at K, the decode taking
// 100 ms. A chunk accepted while it runs is not rebuilt (installed: 3 rebuilt, 8 held); a group
// dropped while it runs leaves the result stale (nothing installed); a failed decode is terminal (a
// later chunk of the group is not taken up); and a network copy of a rebuilt chunk is accepted
// again (the fork has no rebuilt-seen, seacast.md §6.3's REBUILT_SEEN, an option off).
func TestSeacastDecodeLifecycle(t *testing.T) {
	const (
		during  = "a chunk accepted during the decode"
		dropped = "the group dropped during the decode"
		failed  = "a failed decode is terminal"
		seenOff = "a copy of a rebuilt chunk"
	)
	for _, name := range []string{during, dropped, failed, seenOff} {
		t.Run(name, func(t *testing.T) {
			synctestTest(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				const topic = "seg/row/1"
				root := strings.Repeat("1", haveRootLen)
				cell := func(r string, i byte) []byte {
					return append([]byte(r+string(rune(i))), bytes.Repeat([]byte{'a' + i}, 64)...)
				}
				hosts := getDefaultHosts(t, 2)
				table := &MapRoleTable{Joins: map[peer.ID]map[string]Join{}}
				for _, h := range hosts {
					table.Joins[h.ID()] = map[string]Join{"row": {Interest: InterestAll, Duty: DutyForward}}
				}
				ran := make(chan struct{}, 4)
				params := DefaultGossipSubParams()
				params.IDontWantMessageThreshold = 1
				psubs := getGossipsubsOptFn(ctx, hosts, func(i int, h host.Host) []Option {
					tp := TypeParams{RNode: 2}
					if i == 1 {
						tp.DecodeAtK = func(string) bool { return true }
					}
					base := &testObjectType{name: "row", params: tp, topics: []string{topic}, store: NewMapCellStore(),
						threshold: 4, indexCount: 8, topicOf: func(r string) (string, bool) { return topic, len(r) == haveRootLen }}
					T := &testDecodeType{testObjectType: base, cell: cell, wait: 100 * time.Millisecond, fail: i == 1 && name == failed}
					if i == 1 {
						T.ran = ran
					}
					opts := []Option{
						WithGossipSubParams(params), WithMessageSignaturePolicy(StrictNoSign), WithMessageIdFn(seacastTestID),
						WithHaveBitmaps(seacastTestIndex),
						WithPhaseForwarding(func(tp string) bool { return !strings.HasPrefix(tp, "seg/") }, 2),
						WithSeacast([]ObjectType{T}, nil), WithSeacastRoles(table), WithSeacastMachine(),
					}
					return opts
				})
				seacastOrdZeroAtEnd(t, psubs)
				for _, ps := range psubs {
					if _, err := ps.Subscribe(topic); err != nil {
						t.Fatal(err)
					}
				}
				connect(t, hosts[0], hosts[1])
				time.Sleep(2 * time.Second)
				a, b := psubs[0], psubs[1]
				gsA, gsB := a.rt.(*GossipSubRouter), b.rt.(*GossipSubRouter)
				B := hosts[1].ID()
				on := func(gs *GossipSubRouter, f func()) {
					done := make(chan struct{})
					gs.p.eval <- func() { f(); close(done) }
					<-done
				}
				// A copy from A that B's holding has not stopped: A's views of B forgotten first.
				late := func(i byte) {
					on(gsA, func() { forgetPeerViews(gsA, B, root) })
					if err := a.Publish(topic, cell(root, i)); err != nil {
						t.Fatal(err)
					}
				}
				for i := range 4 {
					if err := a.Publish(topic, cell(root, byte(i))); err != nil { // lint:ignore uintcast -- a small index.
						t.Fatal(err)
					}
				}
				time.Sleep(50 * time.Millisecond) // B at K: DECODING, its decode running (100 ms)
				ctr := gsB.seacast.ctr
				state := seacastCollecting
				switch name {
				case during:
					late(5)
				case dropped:
					on(gsB, func() { gsB.seacastDropGroup(root) })
				}
				<-ran
				time.Sleep(200 * time.Millisecond)
				var heldB int
				stored := func(i uint32) (held bool) {
					on(gsB, func() {
						if T := gsB.seacastRootType(root); T != nil && T.Store() != nil {
							_, held = T.Store().Get(root, i)
						}
					})
					return held
				}
				read := func() {
					on(gsB, func() {
						heldB, state = 0, 0
						if m := gsB.have.mine[seacastGID(root)]; m != nil {
							heldB = m.Bits.Count()
						}
						if g := gsB.seacast.group(root); g != nil {
							state = g.State
						}
					})
				}
				read()
				switch name {
				case during:
					if ctr.DecodesDone.Load() != 1 || ctr.CellsRebuilt.Load() != 3 || heldB != 8 || state != seacastComplete {
						t.Fatalf("done %d rebuilt %d held %d state %d; want 1, 3 (index 5 received meanwhile), 8, COMPLETE",
							ctr.DecodesDone.Load(), ctr.CellsRebuilt.Load(), heldB, state)
					}
				case dropped:
					// The holdings stay until the heartbeat ages them (seacast-lite.md's drop_group: HAVEs stay).
					if ctr.DecodesStale.Load() != 1 || ctr.DecodesDone.Load() != 0 || ctr.CellsRebuilt.Load() != 0 || heldB != 4 || stored(6) {
						t.Fatalf("stale %d done %d rebuilt %d held %d, index 6 stored %v; want 1, 0, 0, 4, false",
							ctr.DecodesStale.Load(), ctr.DecodesDone.Load(), ctr.CellsRebuilt.Load(), heldB, stored(6))
					}
				case failed:
					// drop_group (seacast-lite.md): INVALID, its holdings kept (HAVEs stay), nothing rebuilt.
					if ctr.DecodesFailed.Load() != 1 || state != seacastInvalid || heldB != 4 || ctr.CellsRebuilt.Load() != 0 {
						t.Fatalf("failed %d state %d held %d rebuilt %d; want 1, INVALID, 4, 0", ctr.DecodesFailed.Load(), state, heldB, ctr.CellsRebuilt.Load())
					}
					late(6)
					time.Sleep(200 * time.Millisecond)
					read()
					if heldB != 4 || stored(6) || state != seacastInvalid {
						t.Fatalf("after a later chunk: held %d, index 6 stored %v, state %d; want 4, false, INVALID", heldB, stored(6), state)
					}
				case seenOff:
					if ctr.CellsRebuilt.Load() != 4 || heldB != 8 {
						t.Fatalf("rebuilt %d held %d; want 4, 8", ctr.CellsRebuilt.Load(), heldB)
					}
					accepted := ctr.Accepted.Load()
					late(6) // a network copy of a rebuilt chunk
					time.Sleep(200 * time.Millisecond)
					again := ctr.Accepted.Load() - accepted
					if again != 1 {
						t.Fatalf("the copy accepted again %d times; want 1", again)
					}
				}
			})
		})
	}
}

// TestSeacastDecodedStanding is revision 2's f8 case "standing service after installation" (the
// review of part 4's build, finding 13): A's standing WANT for index 6 reaches B while B decodes; B
// keeps it (it lacks 6), and the decode's installation serves it with the rebuilt chunk, which A
// then holds.
func TestSeacastDecodedStanding(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		const topic = "seg/row/1"
		root := strings.Repeat("1", haveRootLen)
		cell := func(r string, i byte) []byte {
			return append([]byte(r+string(rune(i))), bytes.Repeat([]byte{'a' + i}, 64)...)
		}
		hosts := getDefaultHosts(t, 2)
		table := &MapRoleTable{Joins: map[peer.ID]map[string]Join{}}
		for _, h := range hosts {
			table.Joins[h.ID()] = map[string]Join{"row": {Interest: InterestAll, Duty: DutyForward}}
		}
		ran := make(chan struct{}, 4)
		params := DefaultGossipSubParams()
		params.IDontWantMessageThreshold = 1
		psubs := getGossipsubsOptFn(ctx, hosts, func(i int, h host.Host) []Option {
			tp := TypeParams{RNode: 2}
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
		time.Sleep(2 * time.Second)
		a, b := psubs[0], psubs[1]
		gsA, gsB := a.rt.(*GossipSubRouter), b.rt.(*GossipSubRouter)
		B := hosts[1].ID()
		for i := range 4 {
			if err := a.Publish(topic, cell(root, byte(i))); err != nil { // lint:ignore uintcast -- a small index.
				t.Fatal(err)
			}
		}
		time.Sleep(30 * time.Millisecond) // B at K: DECODING, its decode running (100 ms)
		var sent bool
		done := make(chan struct{})
		gsA.p.eval <- func() { sent = gsA.seacastAsk(B, root, 6, true, 0); close(done) }
		<-done
		time.Sleep(20 * time.Millisecond)
		ctrB := gsB.seacast.ctr
		if !sent || ctrB.StandingKept.Load() != 1 || ctrB.StandingServed.Load() != 0 {
			t.Fatalf("during the decode: sent %v, B kept %d served %d; want true, 1, 0", sent, ctrB.StandingKept.Load(), ctrB.StandingServed.Load())
		}
		<-ran
		time.Sleep(200 * time.Millisecond)
		var heldA bool
		done = make(chan struct{})
		gsA.p.eval <- func() {
			if m := gsA.have.mine[seacastGID(root)]; m != nil {
				heldA = m.Bits.Has(6)
			}
			close(done)
		}
		<-done
		if ctrB.CellsRebuilt.Load() != 4 || ctrB.StandingServed.Load() != 1 || !heldA {
			t.Fatalf("after the installation: rebuilt %d, standing served %d, A holds 6 %v; want 4, 1, true", ctrB.CellsRebuilt.Load(), ctrB.StandingServed.Load(), heldA)
		}
	})
}
