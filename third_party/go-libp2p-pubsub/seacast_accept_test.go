package pubsub

import (
	"context"
	"strings"
	"testing"
	"time"

	core "github.com/cskiraly/seacast-go"
	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p-pubsub/timecache"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
)

// seacastTestID is the test cells' message id: the data's first 33 bytes, root ‖ index.
func seacastTestID(m *pb.Message) string { return string(m.GetData()[:haveRootLen+1]) }

// seacastTestIndex reads the tests' chunk ids (seacastTestID): exactly a root and a one-byte index. Like
// Prysm's parser, which reads its own ids' length only, it reads no other id: an ask by index and a
// holding are not chunk ids.
func seacastTestIndex(mid string) (string, uint32, bool) {
	if len(mid) != haveRootLen+1 {
		return "", 0, false
	}
	return mid[:haveRootLen], uint32(mid[haveRootLen]), true
}

// TestSeacastAcceptedChunkGate is the accepted-chunk path on two routers: A publishes a cell of a
// group whose header B has not validated; B keeps the copy pending, notes no holding, serves and
// acknowledges nothing; when B's application validates the header, the copy is validated again,
// accepted, filled, and B's holding reaches A as a v4 with the flags; a second cell with the gate
// open is filled at once.
func TestSeacastAcceptedChunkGate(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		root := strings.Repeat("g", haveRootLen)
		T := &testObjectType{name: "col", params: TypeParams{RNode: 2}, topics: []string{"seg/col/0"}, gateClosed: true}
		hosts := getDefaultHosts(t, 2)
		params := DefaultGossipSubParams()
		params.IDontWantMessageThreshold = 1
		psubs := getGossipsubsOptFn(ctx, hosts, func(i int, h host.Host) []Option {
			return []Option{
				WithGossipSubParams(params),
				WithMessageIdFn(seacastTestID),
				WithHaveBitmaps(seacastTestIndex),
				WithPhaseForwarding(func(topic string) bool { return topic != "seg/col/0" }, 2),
				WithSeacast([]ObjectType{T}, nil),
			}
		})
		seacastOrdZeroAtEnd(t, psubs)
		for _, ps := range psubs {
			if _, err := ps.Subscribe("seg/col/0"); err != nil {
				t.Fatal(err)
			}
		}
		connect(t, hosts[0], hosts[1])
		time.Sleep(2 * time.Second)
		a, b := psubs[0], psubs[1]
		gsA, gsB := a.rt.(*GossipSubRouter), b.rt.(*GossipSubRouter)
		cell := func(index byte) []byte { return append([]byte(root+string(index)), []byte(strings.Repeat("x", 64))...) }
		read := func(gs *GossipSubRouter, f func()) {
			done := make(chan struct{})
			gs.p.eval <- func() { f(); close(done) }
			<-done
		}
		ctrB := gsB.seacast.ctr

		// The gate closed: B keeps the copy, holds nothing, tells A nothing.
		if err := a.Publish("seg/col/0", cell(3)); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Second)
		if n := b.SeacastPending(); n != 1 {
			ctrA := gsA.seacast.ctr
			t.Fatalf("pending copies at B %d, want 1 (B: arrived %d accepted %d ignored %d, store pending %d dup %d refused %d released %d; A: pending %d arrived %d accepted %d, A's pending copies %d; A's mesh %d; same ctr %v)", n, ctrB.Arrived.Load(), ctrB.Accepted.Load(), ctrB.Ignored.Load(), ctrB.Pending.Load(), ctrB.PendingDup.Load(), ctrB.PendingRefused.Load(), ctrB.Released.Load(), ctrA.Pending.Load(), ctrA.Arrived.Load(), ctrA.Accepted.Load(), a.SeacastPending(), len(gsA.mesh["seg/col/0"]), ctrB == gsB.seacast.ctr)
		}
		read(gsB, func() {
			if m := gsB.have.mine[seacastGID(root)]; m != nil && m.Bits.Has(3) {
				t.Errorf("B holds the cell before its header validated")
			}
		})
		read(gsA, func() {
			if e := gsA.have.peers[hosts[1].ID()][seacastGID(root)]; e != nil && e.Bits.Has(3) {
				t.Errorf("A learned B's holding before B's header validated")
			}
		})
		if ctrB.Pending.Load() != 1 || ctrB.Arrived.Load() != 0 || ctrB.Accepted.Load() != 0 {
			t.Fatalf("B's counters: pending %d arrived %d accepted %d, want 1 0 0", ctrB.Pending.Load(), ctrB.Arrived.Load(), ctrB.Accepted.Load())
		}

		// The header validated: the copy is validated again, accepted, filled; A hears the holding.
		T.gateClosed = false
		b.SeacastHeaderValidated([]string{root})
		time.Sleep(time.Second)
		if n := b.SeacastPending(); n != 0 {
			t.Fatalf("pending copies at B after the release %d, want 0", n)
		}
		read(gsB, func() {
			if m := gsB.have.mine[seacastGID(root)]; m == nil || !m.Bits.Has(3) {
				t.Errorf("B does not hold the cell after the release")
			}
		})
		read(gsA, func() {
			e := gsA.have.peers[hosts[1].ID()][seacastGID(root)]
			if e == nil || !e.Bits.Has(3) {
				t.Errorf("A did not learn B's holding after the release")
			} else if e.ClaimAll || e.Complete {
				t.Errorf("B's holding of one cell carries a claim")
			}
		})
		if ctrB.Released.Load() != 1 || ctrB.Accepted.Load() != 1 || ctrB.HoldingsSent.Load() < 1 {
			t.Fatalf("B's counters: released %d accepted %d holdings sent %d, want 1 1 >= 1", ctrB.Released.Load(), ctrB.Accepted.Load(), ctrB.HoldingsSent.Load())
		}

		// The gate open: a second cell is filled at once, nothing pends.
		if err := a.Publish("seg/col/0", cell(4)); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Second)
		if n := b.SeacastPending(); n != 0 || ctrB.Accepted.Load() != 2 {
			t.Fatalf("with the gate open: pending %d accepted %d, want 0 2", n, ctrB.Accepted.Load())
		}
		read(gsA, func() {
			if e := gsA.have.peers[hosts[1].ID()][seacastGID(root)]; e == nil || !e.Bits.Has(4) {
				t.Errorf("A did not learn B's second holding")
			}
		})
		if ctrB.Arrived.Load() != 2 || ctrB.Rejected.Load() != 0 || ctrB.DupUnacked.Load() != 0 {
			t.Fatalf("B's counters: arrived %d rejected %d dup unacked %d, want 2 0 0", ctrB.Arrived.Load(), ctrB.Rejected.Load(), ctrB.DupUnacked.Load())
		}

		// A reject through the pipeline: B's validator rejects cell 5; B holds nothing of it and
		// A hears no holding for it; a later copy of it from A is a duplicate at B (the id is seen
		// once validated) and goes unacknowledged: A hears nothing for it either.
		if err := b.RegisterTopicValidator("seg/col/0", func(_ context.Context, _ peer.ID, m *Message) ValidationResult {
			if m.GetData()[haveRootLen] == 5 {
				return ValidationReject
			}
			return ValidationAccept
		}); err != nil {
			t.Fatal(err)
		}
		if err := a.Publish("seg/col/0", cell(5)); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Second)
		if ctrB.Rejected.Load() != 1 || ctrB.Accepted.Load() != 2 || ctrB.Arrived.Load() != 3 {
			t.Fatalf("B after the reject: rejected %d accepted %d arrived %d, want 1 2 3", ctrB.Rejected.Load(), ctrB.Accepted.Load(), ctrB.Arrived.Load())
		}
		read(gsB, func() {
			if m := gsB.have.mine[seacastGID(root)]; m != nil && m.Bits.Has(5) {
				t.Errorf("B holds the rejected cell")
			}
		})
		heard := gsA.seacast.ctr.HoldingsV4.Load()
		read(gsA, func() {
			if e := gsA.have.peers[hosts[1].ID()][seacastGID(root)]; e != nil && e.Bits.Has(5) {
				t.Errorf("A learned a holding of the rejected cell")
			}
		})
		inject := func(index byte) {
			// A faithful copy, the message as A published it (signed), from A's message cache.
			read(gsA, func() {
				m, ok := gsA.mcache.Get(string(cell(index)[:haveRootLen+1]))
				if !ok {
					t.Errorf("A's message cache no longer holds cell %d", index)
					return
				}
				gsA.sendRPC(hosts[1].ID(), rpcWithControl([]*pb.Message{m.Message}, nil, nil, nil, nil, nil), false)
			})
			time.Sleep(time.Second)
		}
		inject(5)
		if ctrB.DupUnacked.Load() != 1 || ctrB.DupAcked.Load() != 0 || ctrB.Rejected.Load() != 1 {
			t.Fatalf("B after a duplicate of the rejected cell: dup unacked %d dup acked %d rejected %d, want 1 0 1", ctrB.DupUnacked.Load(), ctrB.DupAcked.Load(), ctrB.Rejected.Load())
		}
		if n := gsA.seacast.ctr.HoldingsV4.Load(); n != heard {
			t.Fatalf("A absorbed %d v4 holdings after the rejected cell's duplicate, want %d: B acknowledged a rejected id", n, heard)
		}

		// A duplicate of an accepted cell is acknowledged (the hose off: the plain v4 holding),
		// counted at B's send and heard at A.
		inject(4)
		if ctrB.DupAcked.Load() != 1 || ctrB.DupUnacked.Load() != 1 {
			t.Fatalf("B after a duplicate of an accepted cell: dup acked %d dup unacked %d, want 1 1", ctrB.DupAcked.Load(), ctrB.DupUnacked.Load())
		}
		if n := gsA.seacast.ctr.HoldingsV4.Load(); n != heard+1 {
			t.Fatalf("A absorbed %d v4 holdings after the accepted cell's duplicate, want %d: the acknowledgement did not arrive", n, heard+1)
		}
	})
}

// TestSeacastDuplicateAckHose is the duplicate's acknowledgement with the hose on (the positive
// control of the gate's configuration): A publishes a cell B accepts; a later copy from A is
// acknowledged with the hose's stamped acknowledgement, counted at B's send.
func TestSeacastDuplicateAckHose(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		root := strings.Repeat("h", haveRootLen)
		T := &testObjectType{name: "col", params: TypeParams{RNode: 2}, topics: []string{"seg/col/0"}}
		hosts := getDefaultHosts(t, 2)
		params := DefaultGossipSubParams()
		params.IDontWantMessageThreshold = 1
		psubs := getGossipsubsOptFn(ctx, hosts, func(i int, h host.Host) []Option {
			return []Option{
				WithGossipSubParams(params),
				WithMessageIdFn(seacastTestID),
				WithHaveBitmaps(seacastTestIndex),
				WithPhaseForwarding(func(topic string) bool { return topic != "seg/col/0" }, 2),
				WithHoseAck(),
				WithSeacast([]ObjectType{T}, nil),
			}
		})
		seacastOrdZeroAtEnd(t, psubs)
		for _, ps := range psubs {
			if _, err := ps.Subscribe("seg/col/0"); err != nil {
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
		cell := append([]byte(root+string(byte(2))), []byte(strings.Repeat("x", 64))...)
		if err := a.Publish("seg/col/0", cell); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Second)
		ctrB := gsB.seacast.ctr
		if ctrB.Accepted.Load() != 1 {
			t.Fatalf("B accepted %d, want 1", ctrB.Accepted.Load())
		}
		read(gsA, func() {
			// A faithful copy, the message as published (signed), from A's message cache.
			m, ok := gsA.mcache.Get(string(cell[:haveRootLen+1]))
			if !ok {
				t.Errorf("A's message cache no longer holds the cell")
				return
			}
			gsA.sendRPC(hosts[1].ID(), rpcWithControl([]*pb.Message{m.Message}, nil, nil, nil, nil, nil), false)
		})
		time.Sleep(time.Second)
		if ctrB.DupAcked.Load() != 1 || ctrB.DupUnacked.Load() != 0 {
			t.Fatalf("B after a duplicate with the hose on: dup acked %d dup unacked %d, want 1 0", ctrB.DupAcked.Load(), ctrB.DupUnacked.Load())
		}
	})
}

// TestSeacastPendingStore is the store's own contract on a hand-built router: the per-peer bound,
// one copy per id, a reject's dedup without acknowledgement, and the expiry. A reject of a kept id
// (only the node's own publication can reach validation while a copy waits) leaves the copy kept:
// the core keeps a copy by root until its gate opens or its group goes. A rejected id goes
// unacknowledged on its own too: one not kept, whose index the node holds.
func TestSeacastPendingStore(t *testing.T) {
	root := strings.Repeat("g", haveRootLen)
	T := &testObjectType{name: "col", params: TypeParams{RNode: 2}, topics: []string{"seg/col/0"}, gateClosed: true}
	ctr := &SeacastCounters{}
	gs := &GossipSubRouter{}
	gs.p = &PubSub{host: getDefaultHosts(t, 1)[0], idGen: &msgIDGenerator{Default: seacastTestID, topicGens: map[string]MsgIdFunction{}},
		seenMessages: timecache.NewTimeCache(time.Minute)}
	gs.have = newHaveState(seacastTestIndex)
	gs.seacast = &seacast{types: map[string]ObjectType{"seg/col/0": T}, ctr: ctr, limits: SeacastLimits{PendingPerPeer: 2}}
	topic := "seg/col/0"
	msg := func(index byte) *Message {
		return &Message{Message: &pb.Message{Topic: &topic, Data: append([]byte(root+string(index)), 'x')}, ReceivedFrom: peer.ID("p")}
	}
	for _, m := range []*Message{msg(1), msg(1), msg(2), msg(3)} {
		if gs.seacastAdmit(m, false) {
			t.Fatalf("a message of a closed gate admitted")
		}
	}
	if gs.seacast.waits.Len() != 2 || len(gs.seacast.waitMsg) != 2 || ctr.Pending.Load() != 2 || ctr.PendingDup.Load() != 1 || ctr.PendingRefused.Load() != 1 {
		t.Fatalf("pending %d, messages %d (counters pending %d dup %d refused %d), want 2, 2 (2 1 1)", gs.seacast.waits.Len(), len(gs.seacast.waitMsg), ctr.Pending.Load(), ctr.PendingDup.Load(), ctr.PendingRefused.Load())
	}
	mid1 := seacastTestID(msg(1).Message)
	if gs.seacastAckAllowed(mid1) {
		t.Fatalf("a pending id's duplicate acknowledged")
	}
	gs.seacastVerdict(peer.ID("p"), msg(1), ValidationReject)
	if pending := gs.seacast.waits.Has(mid1); !pending || ctr.Rejected.Load() != 1 || gs.seacastAckAllowed(mid1) {
		t.Fatalf("a rejected id: still pending %v, rejected %d, ack allowed %v; want true 1 false", pending, ctr.Rejected.Load(), gs.seacastAckAllowed(mid1))
	}
	mid5 := seacastTestID(msg(5).Message)
	var held core.Bits
	held.Set(5)
	gs.have.mine[seacastGID(root)] = &core.Holding{Bits: held} // index 5 held, so only the reject keeps its id unacknowledged
	if !gs.seacastAckAllowed(mid5) {
		t.Fatalf("a held index's id, neither kept nor rejected, not acknowledged")
	}
	gs.seacastVerdict(peer.ID("p"), msg(5), ValidationReject)
	if pending := gs.seacast.waits.Has(mid5); pending || gs.seacastAckAllowed(mid5) {
		t.Fatalf("a rejected id not kept: pending %v, ack allowed %v; want false false", pending, gs.seacastAckAllowed(mid5))
	}
	delete(gs.have.mine, seacastGID(root))
	gs.seacastExpire([]string{root})
	if gs.seacast.waits.Len() != 0 || len(gs.seacast.waitMsg) != 0 || ctr.Expired.Load() != 2 || gs.seacast.waits.Of(peer.ID("p")) != 0 {
		t.Fatalf("after expiry: pending %d, messages %d, expired %d, per-peer count %d; want 0 0 2 0", gs.seacast.waits.Len(), len(gs.seacast.waitMsg), ctr.Expired.Load(), gs.seacast.waits.Of(peer.ID("p")))
	}
	if ctr.DupUnacked.Load() != 3 {
		t.Fatalf("duplicates not acknowledged %d, want 3", ctr.DupUnacked.Load())
	}
}

// TestSeacastExpiryDuringValidation is an expiry during validation on two routers: B's validator
// takes half a second; A publishes a cell; B's application expires the group while the cell is in
// validation; the accepted verdict reaches the admission after validation and the chunk is dropped
// there, unfilled: no holding, no store entry, no acceptance; a later copy of the group from A is
// dropped at ingress.
func TestSeacastExpiryDuringValidation(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		root := strings.Repeat("e", haveRootLen)
		stores := make([]*MapCellStore, 2)
		hosts := getDefaultHosts(t, 2)
		params := DefaultGossipSubParams()
		params.IDontWantMessageThreshold = 1
		psubs := getGossipsubsOptFn(ctx, hosts, func(i int, h host.Host) []Option {
			stores[i] = NewMapCellStore()
			T := &testObjectType{name: "col", params: TypeParams{RNode: 2}, topics: []string{"seg/col/0"}, store: stores[i]}
			return []Option{
				WithGossipSubParams(params),
				WithMessageIdFn(seacastTestID),
				WithHaveBitmaps(seacastTestIndex),
				WithPhaseForwarding(func(topic string) bool { return topic != "seg/col/0" }, 2),
				WithSeacast([]ObjectType{T}, nil),
			}
		})
		seacastOrdZeroAtEnd(t, psubs)
		a, b := psubs[0], psubs[1]
		entered := make(chan struct{}, 1) // the validator's entry, so the expiry is known to fall inside it
		if err := b.RegisterTopicValidator("seg/col/0", func(context.Context, peer.ID, *Message) ValidationResult {
			select {
			case entered <- struct{}{}:
			default:
			}
			time.Sleep(500 * time.Millisecond)
			return ValidationAccept
		}); err != nil {
			t.Fatal(err)
		}
		for _, ps := range psubs {
			if _, err := ps.Subscribe("seg/col/0"); err != nil {
				t.Fatal(err)
			}
		}
		connect(t, hosts[0], hosts[1])
		time.Sleep(2 * time.Second)
		gsB := b.rt.(*GossipSubRouter)
		ctrB := gsB.seacast.ctr
		read := func(gs *GossipSubRouter, f func()) {
			done := make(chan struct{})
			gs.p.eval <- func() { f(); close(done) }
			<-done
		}
		cell := func(index byte) []byte { return append([]byte(root+string(index)), []byte(strings.Repeat("x", 64))...) }

		if err := a.Publish("seg/col/0", cell(7)); err != nil {
			t.Fatal(err)
		}
		<-entered // B's validator has the cell: the expiry falls inside its validation
		b.SeacastExpire([]string{root})
		time.Sleep(time.Second)
		var held int
		read(gsB, func() { held = stores[1].Len() })
		if held != 0 || ctrB.Accepted.Load() != 0 || ctrB.ExpiredDropped.Load() != 1 || ctrB.Arrived.Load() != 1 {
			t.Fatalf("B after an expiry during validation: store %d, accepted %d, expired-dropped %d, arrived %d, want 0 0 1 1", held, ctrB.Accepted.Load(), ctrB.ExpiredDropped.Load(), ctrB.Arrived.Load())
		}
		read(gsB, func() {
			if m := gsB.have.mine[seacastGID(root)]; m != nil && m.Bits.Has(7) {
				t.Errorf("B holds a cell of the expired group")
			}
		})

		// A later copy of the group: dropped at ingress, never an arrival.
		if err := a.Publish("seg/col/0", cell(8)); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Second)
		if ctrB.ExpiredDropped.Load() != 2 || ctrB.Arrived.Load() != 1 {
			t.Fatalf("B after a copy of the expired group: expired-dropped %d, arrived %d, want 2 1", ctrB.ExpiredDropped.Load(), ctrB.Arrived.Load())
		}
	})
}
