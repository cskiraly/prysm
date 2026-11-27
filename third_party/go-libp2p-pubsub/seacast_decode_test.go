package pubsub

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
)

// testDecodeType is a test type with a level 2: from K held cells it returns the others, built as
// the test builds every cell, after a wait standing for the decode's cost; fail makes it refuse.
type testDecodeType struct {
	*testObjectType
	cell func(root string, i byte) []byte
	wait time.Duration
	fail bool
	ran  chan struct{}
}

func (t *testDecodeType) Decode(root string, held []uint32) func() (map[uint32]*Message, bool) {
	return func() (map[uint32]*Message, bool) { return t.decode(root, held) }
}

func (t *testDecodeType) decode(root string, held []uint32) (map[uint32]*Message, bool) {
	time.Sleep(t.wait)
	if t.ran != nil {
		t.ran <- struct{}{}
	}
	if t.fail || len(held) < t.Threshold(root) {
		return nil, false
	}
	have := map[uint32]bool{}
	for _, i := range held {
		have[i] = true
	}
	out := map[uint32]*Message{}
	topic, _ := t.TopicOf(root)
	for i := range t.IndexCount(root) {
		if idx := uint32(i); !have[idx] { // lint:ignore uintcast -- a small index.
			tp := topic
			out[idx] = &Message{Message: &pb.Message{Data: t.cell(root, byte(i)), Topic: &tp}} // lint:ignore uintcast -- a small index.
		}
	}
	return out, true
}

// TestSeacastDecode is the decode (seacast.md §4.6's decode_group and push_rebuilt): B decodes
// a coded group (K = 4 of 8) the moment K are in; the result, installed on the router's goroutine,
// makes B hold the group whole and claim it; B's view of A cleared before the installation, B
// decides to push A the rebuilt cells up to A's shortfall, and its writer drops them at dispatch
// because A is at K (the coded type's projection). A decode that fails leaves the group INVALID with nothing
// rebuilt.
func TestSeacastDecode(t *testing.T) {
	for _, fail := range []bool{false, true} {
		name := "decoded: installed, claimed, repaired"
		if fail {
			name = "the decode fails: INVALID"
		}
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
					T := &testDecodeType{testObjectType: base, cell: cell, wait: 100 * time.Millisecond, fail: fail}
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
				read := func(gs *GossipSubRouter, f func()) {
					done := make(chan struct{})
					gs.p.eval <- func() { f(); close(done) }
					<-done
				}
				for i := range 4 {
					if err := a.Publish(topic, cell(root, byte(i))); err != nil { // lint:ignore uintcast -- a small index.
						t.Fatal(err)
					}
				}
				time.Sleep(50 * time.Millisecond) // B at K: DECODING, its decode running (100 ms)
				A := hosts[0].ID()
				read(gsB, func() {
					// B's view of A cleared: A, short by K in B's eyes, is owed the rebuilt cells.
					delete(gsB.have.peers[A], seacastGID(root))
				})
				<-ran
				time.Sleep(200 * time.Millisecond)
				state := seacastCollecting
				var heldB, heldA int
				var claimAll bool
				read(gsB, func() {
					if g := gsB.seacast.group(root); g != nil {
						state = g.State
					}
					if m := gsB.have.mine[seacastGID(root)]; m != nil {
						heldB = m.Bits.Count()
					}
				})
				read(gsA, func() {
					if m := gsA.have.mine[seacastGID(root)]; m != nil {
						heldA = m.Bits.Count()
					}
					if e := gsA.have.peers[hosts[1].ID()][seacastGID(root)]; e != nil {
						claimAll = e.ClaimAll
					}
				})
				ctr := gsB.seacast.ctr
				if fail {
					if state != seacastInvalid || ctr.DecodesFailed.Load() != 1 || ctr.CellsRebuilt.Load() != 0 || heldB != 4 {
						t.Fatalf("failed decode: state %d failed %d rebuilt %d held %d, want INVALID, 1, 0, 4", state, ctr.DecodesFailed.Load(), ctr.CellsRebuilt.Load(), heldB)
					}
					return
				}
				if state != seacastComplete || ctr.DecodesDone.Load() != 1 || ctr.CellsRebuilt.Load() != 4 || heldB != 8 || !claimAll {
					t.Fatalf("decoded: state %d done %d rebuilt %d held %d claim at A %v, want COMPLETE, 1, 4, 8, true",
						state, ctr.DecodesDone.Load(), ctr.CellsRebuilt.Load(), heldB, claimAll)
				}
				// A completed at K: B's writer drops the four pushes at dispatch, by the projection of a
				// coded type (§4.9: a peer projected to reach K is sent no more chunks of the group) or
				// by A's holding (the dispatch suppression). A delivered repair to a custodian short of
				// its column is the column cells' case (K = N, no projection).
				var cancelled int64
				read(gsB, func() {
					if q := gsB.p.peers[A]; q != nil {
						cancelled = q.cancelled.Load() + q.projected.Load()
					}
				})
				if ctr.RepairPushes.Load() != 4 || heldA != 4 || cancelled < 4 {
					ca := gsA.seacast.ctr
					t.Fatalf("repair: pushes %d, A holds %d, cancelled at B's dispatch %d; want 4, 4 and at least 4 (A: accepted %d rejected %d pending %d dup-acked %d)",
						ctr.RepairPushes.Load(), heldA, cancelled, ca.Accepted.Load(), ca.Rejected.Load(), ca.Pending.Load(), ca.DupAcked.Load())
				}
			})
		})
	}
}

// TestSeacastHoldingAfterDecode is the holding's targets after a fill (seacast-lite.md §4.2's
// on_validated): on a line A - B - C, a chunk new to B's holding goes to B's scope (its mesh, C)
// and to its sender A; once B has decoded the group, a copy of an index B rebuilt, accepted from
// A, is acknowledged to A alone, and C hears nothing from B.
func TestSeacastHoldingAfterDecode(t *testing.T) {
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
		connect(t, hosts[1], hosts[2])
		time.Sleep(2 * time.Second)
		a := psubs[0]
		gsA, gsB, gsC := psubs[0].rt.(*GossipSubRouter), psubs[1].rt.(*GossipSubRouter), psubs[2].rt.(*GossipSubRouter)
		read := func(gs *GossipSubRouter, f func()) {
			done := make(chan struct{})
			gs.p.eval <- func() { f(); close(done) }
			<-done
		}
		ctrA, ctrB, ctrC := gsA.seacast.ctr, gsB.seacast.ctr, gsC.seacast.ctr

		// A claim: cell 0, new to B, reaches B's scope (C) and its sender (A).
		heardA, heardC := ctrA.HoldingsV4.Load(), ctrC.HoldingsV4.Load()
		if err := a.Publish(topic, cell(root, 0)); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Second)
		if dA, dC := ctrA.HoldingsV4.Load()-heardA, ctrC.HoldingsV4.Load()-heardC; dA < 1 || dC < 1 {
			t.Fatalf("B's holding of a claim: A heard %d, C heard %d holdings; want at least 1 each", dA, dC)
		}

		// B at K decodes and rebuilds 4 to 7.
		for i := 1; i < 4; i++ {
			if err := a.Publish(topic, cell(root, byte(i))); err != nil { // lint:ignore uintcast -- a small index.
				t.Fatal(err)
			}
		}
		<-ran
		time.Sleep(time.Second)
		state := seacastCollecting
		read(gsB, func() {
			if g := gsB.seacast.group(root); g != nil {
				state = g.State
			}
		})
		if state != seacastComplete || ctrB.CellsRebuilt.Load() != 4 {
			t.Fatalf("B's decode: state %d rebuilt %d, want COMPLETE, 4", state, ctrB.CellsRebuilt.Load())
		}

		// A copy of 5, which B rebuilt, accepted from A: no claim, so the holding goes to A alone.
		// A's writer may drop its push (B claims the group), so A's copy, as A published it, also goes
		// straight into B's receive path; B accepts it once either way.
		accepted := ctrB.Accepted.Load()
		heardA, heardC = ctrA.HoldingsV4.Load(), ctrC.HoldingsV4.Load()
		if err := a.Publish(topic, cell(root, 5)); err != nil {
			t.Fatal(err)
		}
		time.Sleep(100 * time.Millisecond)
		var m5 *Message
		read(gsA, func() { m5, _ = gsA.mcache.Get(root + string(byte(5))) })
		if m5 == nil {
			t.Fatal("A's copy of 5 is not in its message cache")
		}
		read(gsB, func() {
			gsB.p.handleIncomingRPC(&RPC{RPC: pb.RPC{Publish: []*pb.Message{m5.Message}}, from: hosts[0].ID()})
		})
		time.Sleep(time.Second)
		if n := ctrB.Accepted.Load() - accepted; n != 1 {
			t.Fatalf("B accepted %d copies of 5, want 1", n)
		}
		if dA, dC := ctrA.HoldingsV4.Load()-heardA, ctrC.HoldingsV4.Load()-heardC; dA < 1 || dC != 0 {
			t.Fatalf("B's holding after a copy of a rebuilt index: A heard %d, C heard %d holdings; want at least 1 and 0", dA, dC)
		}
	})
}
