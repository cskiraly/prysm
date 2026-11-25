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

// TestSeacastPayloadSetting is seacast.md §6.1 (seacast-lite.md as a setting) on v2's path: the payload as an object
// type of one family whose chunks belong to one group (Cross false), every node joined with interest ALL and duty
// FORWARD, the push relation the mesh (EAGER, RNode the mesh degree), the announce relation the mesh plus the
// heartbeat's sample (ANNOUNCE_EXTRA SAMPLE, every type's in this fork), the decode claim at K. A coded group, K = 4 of N = 8, published whole by node 0
// on ten routers densely connected with a mesh of degree 4; another topic runs on the phase path beside it. Every
// node holds the group whole (received or rebuilt) and completes, the phase topic's message reaches every node, the
// type's heartbeat gossip offers ids, and no honest ordering check moves.
func TestSeacastPayloadSetting(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		const (
			n          = 10
			topic      = "seg/payload"
			phaseTopic = "ctl/phase"
		)
		root := strings.Repeat("p", haveRootLen)
		cell := func(r string, i byte) []byte {
			return append([]byte(r+string(rune(i))), bytes.Repeat([]byte{'a' + i}, 256)...)
		}
		hosts := getDefaultHosts(t, n)
		table := &MapRoleTable{Joins: map[peer.ID]map[string]Join{}}
		for _, h := range hosts {
			table.Joins[h.ID()] = map[string]Join{"payload": {Interest: InterestAll, Duty: DutyForward}}
		}
		params := DefaultGossipSubParams()
		params.D, params.Dlo, params.Dhi, params.Dout, params.Dscore = 4, 3, 6, 1, 2
		params.Dlazy, params.GossipFactor = 6, 0.25
		params.HeartbeatInterval = time.Second
		params.IDontWantMessageThreshold = 1
		psubs := getGossipsubsOptFn(ctx, hosts, func(i int, h host.Host) []Option {
			tp := TypeParams{RNode: params.D, DecodeAtK: func(string) bool { return true }}
			base := &testObjectType{name: "payload", params: tp, topics: []string{topic}, store: NewMapCellStore(),
				threshold: 4, indexCount: 8, topicOf: func(r string) (string, bool) { return topic, len(r) == haveRootLen }}
			T := &testDecodeType{testObjectType: base, cell: cell, wait: 10 * time.Millisecond}
			return []Option{
				WithGossipSubParams(params), WithMessageSignaturePolicy(StrictNoSign), WithMessageIdFn(seacastTestID),
				WithHaveBitmaps(seacastTestIndex),
				WithPhaseForwarding(func(tp string) bool { return !strings.HasPrefix(tp, "seg/") }, 2),
				WithPacedGossip(params.HeartbeatInterval/16, 0),
				WithSeacast([]ObjectType{T}, nil), WithSeacastRoles(table), WithSeacastMachine(),
			}
		})
		seacastOrdZeroAtEnd(t, psubs)
		subs := make([]*Subscription, n)
		for i, ps := range psubs {
			if _, err := ps.Subscribe(topic); err != nil {
				t.Fatal(err)
			}
			s, err := ps.Subscribe(phaseTopic)
			if err != nil {
				t.Fatal(err)
			}
			subs[i] = s
		}
		denseConnect(t, hosts)
		time.Sleep(3 * time.Second) // the meshes form
		src := psubs[0]
		for i := range 8 {
			if err := src.Publish(topic, cell(root, byte(i))); err != nil { // lint:ignore uintcast -- a small index.
				t.Fatal(err)
			}
		}
		phaseMsg := append([]byte(strings.Repeat("q", haveRootLen)+"\x00"), bytes.Repeat([]byte{'z'}, 64)...)
		if err := src.Publish(phaseTopic, phaseMsg); err != nil {
			t.Fatal(err)
		}
		time.Sleep(5 * time.Second) // past several heartbeats
		on := func(gs *GossipSubRouter, f func()) {
			done := make(chan struct{})
			gs.p.eval <- func() { f(); close(done) }
			<-done
		}
		var offered int64
		for i, ps := range psubs {
			gs := ps.rt.(*GossipSubRouter)
			var held int
			var state uint8
			var known bool
			on(gs, func() {
				if m := gs.have.mine[root]; m != nil {
					held = m.bits.Count()
				}
				if g := gs.seacast.groups[root]; g != nil {
					state, known = g.state, true
				}
			})
			if held != 8 {
				t.Errorf("node %d: holds %d of the payload's 8 chunks (received or rebuilt), want 8", i, held)
			}
			if i != 0 && (!known || state != seacastComplete) {
				t.Errorf("node %d: the payload's group state %d (known %v), want complete (%d)", i, state, known, seacastComplete)
			}
			offered += gs.seacast.ctr.GossipIdsOffered.Load()
		}
		for i, s := range subs {
			rctx, rcancel := context.WithTimeout(ctx, time.Second)
			m, err := s.Next(rctx)
			rcancel()
			if err != nil || !bytes.Equal(m.GetData(), phaseMsg) {
				t.Errorf("node %d: the phase topic's message not delivered (%v)", i, err)
			}
		}
		if offered == 0 {
			t.Errorf("the type's heartbeat gossip offered no id on any node (ANNOUNCE_EXTRA SAMPLE), want some")
		}
	})
}

// TestSeacastPayloadRepairTo is §6.3's REPAIR_TO on §4.6's push_rebuilt: a §6.1 payload type repairs into the push
// relation (the mesh), seacast-lite.md's "the mesh". Three routers in a line, A - B - C, with no mesh at all (D = 0): A
// publishes a coded group (K = 4 of 8) and B, decoding at K, rebuilds the rest, and pushes none to C, a joined link
// outside its empty mesh. The positive side, a repair pushed into the mesh, is
// TestSeacastRepairModes's.
func TestSeacastPayloadRepairTo(t *testing.T) {
	for _, tc := range []struct {
		name string
	}{{"relation"}} {
		t.Run(tc.name, func(t *testing.T) {
			synctestTest(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				const topic = "seg/payload"
				root := strings.Repeat("r", haveRootLen)
				cell := func(r string, i byte) []byte {
					return append([]byte(r+string(rune(i))), bytes.Repeat([]byte{'a' + i}, 256)...)
				}
				hosts := getDefaultHosts(t, 3)
				table := &MapRoleTable{Joins: map[peer.ID]map[string]Join{}}
				for _, h := range hosts {
					table.Joins[h.ID()] = map[string]Join{"payload": {Interest: InterestAll, Duty: DutyForward}}
				}
				params := DefaultGossipSubParams()
				params.D, params.Dlo, params.Dhi, params.Dout, params.Dscore = 0, 0, 0, 0, 0
				params.Dlazy, params.GossipFactor = 6, 0.25
				params.HeartbeatInterval = time.Second
				params.IDontWantMessageThreshold = 1
				psubs := getGossipsubsOptFn(ctx, hosts, func(i int, h host.Host) []Option {
					tp := TypeParams{RNode: 2,
						DecodeAtK: func(string) bool { return true }}
					base := &testObjectType{name: "payload", params: tp, topics: []string{topic}, store: NewMapCellStore(),
						threshold: 4, indexCount: 8, topicOf: func(r string) (string, bool) { return topic, len(r) == haveRootLen }}
					T := &testDecodeType{testObjectType: base, cell: cell}
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
				for i := range 4 { // K, the systematic half: B decodes at K and rebuilds 4
					if err := psubs[0].Publish(topic, cell(root, byte(i))); err != nil { // lint:ignore uintcast -- a small index.
						t.Fatal(err)
					}
				}
				time.Sleep(500 * time.Millisecond)
				gsB := psubs[1].rt.(*GossipSubRouter)
				ctr := gsB.seacast.ctr
				var meshC bool
				done := make(chan struct{})
				gsB.p.eval <- func() { _, meshC = gsB.mesh[topic][hosts[2].ID()]; close(done) }
				<-done
				if meshC {
					t.Fatalf("C is in B's mesh with D = 0: the fixture's premise fails")
				}
				if ctr.DecodesDone.Load() != 1 {
					t.Fatalf("B's decodes done %d, want 1 (at K)", ctr.DecodesDone.Load())
				}
				if pushes := ctr.RepairPushes.Load(); pushes != 0 {
					t.Errorf("REPAIR_TO the push relation: B pushed %d rebuilt chunks with an empty mesh, want 0 (C is outside it)", pushes)
				}
			})
		})
	}
}
