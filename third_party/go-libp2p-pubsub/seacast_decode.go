package pubsub

import (
	"math/rand/v2"
	"slices"
	"sort"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
)

// --- decode_group and push_rebuilt (seacast.md §4.6; part 4) ---
//
// A group in DECODING is decoded by its type's level 2 (Decoder) off the router's goroutine, on a
// snapshot of the indices held; the result comes back through eval and is installed only if the
// group is still the same operation's, still DECODING and not expired, leaving out what arrived
// meanwhile. Each rebuilt chunk is stored and held (servable from then), and the group becomes
// COMPLETE; then push_rebuilt, to the mesh peers whose interest holds the chunk and whose
// shortfall under K is open, a refused push offered.
// A failed decode leaves the group INVALID.

// Decoder is an object type's level 2 (§4.6's T.decode). Decode is called on the router's goroutine
// with the indices held, takes there whatever it needs of its own state (the block the group
// belongs to), and returns the work, run off the router's goroutine: from the held chunks, the
// missing ones, or ok false when the group cannot be decoded. The work prices itself (the harness on
// its verifier workers).
type Decoder interface {
	Decode(root string, held []uint32) func() (cells map[uint32]*Message, ok bool)
}

// seacastDecodeStart runs root's decode off the router's goroutine. Router goroutine.
func (gs *GossipSubRouter) seacastDecodeStart(T ObjectType, root string) {
	sc := gs.seacast
	D, ok := T.(Decoder)
	if !ok {
		return // a type without a level 2: the claim alone
	}
	g := sc.groupOf(root)
	g.decodeOp++
	op := g.decodeOp
	var held []uint32
	if m := gs.have.mine[root]; m != nil {
		for i := range T.IndexCount(root) {
			if idx := uint32(i); m.bits.Has(idx) { // lint:ignore uintcast -- an index below the group's count.
				held = append(held, idx)
			}
		}
	}
	g.decodeHeld = len(held)
	gs.seacastOrderDecode(T, root, len(held)) // the per-event conditions (seacast_order.go)
	work := D.Decode(root, held)              // the snapshot, on the router's goroutine
	go func() {
		cells, ok := work()
		select {
		case gs.p.eval <- func() { gs.seacastDecodeInstall(T, root, op, cells, ok) }:
		case <-gs.p.ctx.Done():
		}
	}()
}

// seacastDecodeInstall installs a decode's result. Router goroutine.
func (gs *GossipSubRouter) seacastDecodeInstall(T ObjectType, root string, op uint64, cells map[uint32]*Message, ok bool) {
	sc := gs.seacast
	g := sc.groups[root]
	if g == nil || g.decodeOp != op || g.state != seacastDecoding {
		sc.ctr.DecodesStale.Add(1)
		return
	}
	if expired, _ := gs.seacastRootState(root); expired {
		sc.ctr.DecodesStale.Add(1)
		return // the group passed its lifetime meanwhile (v0.5 §8.1)
	}
	if !ok {
		// drop_group (v0.3 §4.6): the group's queued traffic, offers, promises and store go; the
		// state stays INVALID, so a later chunk of it is not taken up (seacastAccepted).
		sc.ctr.DecodesFailed.Add(1)
		sc.event(SeacastEvDecodeFailed, root)
		gs.seacastDropGroup(root)
		gs.seacastClosePromises(root)
		sc.groups[root] = &seacastGroup{state: seacastInvalid}
		return
	}
	idxs := make([]uint32, 0, len(cells))
	for idx := range cells {
		idxs = append(idxs, idx)
	}
	slices.Sort(idxs)
	var rebuilt []uint32
	g.decodedOK = g.decodeHeld >= T.Threshold(root) // a decode from K held installed (seacast_order.go)
	for _, idx := range idxs {
		msg := cells[idx]
		if msg == nil || gs.seacastHeld(root, idx) {
			continue // received meanwhile: not rebuilt
		}
		mid := gs.p.idGen.ID(msg)
		gs.seacastStorePut(T, root, idx, msg)
		gs.seacastFillClose(root, idx, mid) // the fill's ledger: its promises, budget and offers closed (§4.2)
		gs.haveNoteArrival(mid)             // held, and servable from now on
		sc.markRebuilt(root, idx)
		if g.decodedOK {
			sc.markRebuiltOK(root, idx)
		}
		gs.seacastServeStanding(T, root, idx, msg)
		gs.seacastServeWaitingProbe(T, root, idx, msg) // (R5) a probe that reached us while we decoded
		rebuilt = append(rebuilt, idx)
	}
	g.state = seacastComplete
	sc.ctr.DecodesDone.Add(1)
	sc.event(SeacastEvDecodeDone, root)
	sc.ctr.CellsRebuilt.Add(int64(len(rebuilt)))
	gs.seacastSendHolding(T, root)
	switch sc.fault {
	case seacastFaultHoard: // pushes nothing it rebuilt (seacast_fault.go)
	default:
		gs.seacastPushRebuilt(T, root, rebuilt, cells) // [c10] repair: level 2 has passed
	}
}

// seacastFillClose closes what a fill closes for a cell the node rebuilt (§4.2's fill, :676): the
// asks of it under its own id and under the ask by index (encodeWant), their promises, budgets and
// offers, and the score's promises under both, as the network fill does through haveArrived (the
// review of part 4's gate, finding 6). Router goroutine.
func (gs *GossipSubRouter) seacastFillClose(root string, index uint32, mid string) {
	want := encodeWant(root, index)
	gs.iwantDelivered(mid)
	gs.iwantDelivered(want)
	if gs.gossipTracer != nil {
		gs.gossipTracer.fulfillPromiseID(mid)
		gs.gossipTracer.fulfillPromiseID(want)
	}
}

// seacastRepairType is the type of a group a rebuilt chunk fills: the root's registered type, or
// else any of the node's types that places the root (TopicOf), without registering it. A row's
// decoder fills and repairs columns outside its custody, whose topics it does not run and whose
// roots it may never have seen (a withheld column's). Router goroutine.
func (gs *GossipSubRouter) seacastRepairType(root string) ObjectType {
	if T := gs.seacastRootType(root); T != nil {
		return T
	}
	if len(root) != haveRootLen || gs.seacastRootKnownOff(root) {
		return nil
	}
	for _, T := range gs.seacast.types {
		if _, ok := T.TopicOf(root); ok {
			return T
		}
	}
	return nil
}

// markRebuilt records index of root as rebuilt here. Router goroutine.
func (sc *seacast) markRebuilt(root string, index uint32) {
	if sc.rebuilt == nil {
		sc.rebuilt = map[string]*haveBits{}
	}
	b := sc.rebuilt[root]
	if b == nil {
		b = &haveBits{}
		sc.rebuilt[root] = b
	}
	b.Set(index)
}

// seacastRepairItem is a rebuilt chunk in one group's form.
type seacastRepairItem struct {
	index uint32
	msg   *Message
}

// seacastRepairTo is REPAIR_TO(g) (§6.3, [D4]; §4.6's push_rebuilt) as the payload sets it (§6.1):
// the push relation, the mesh of the group's topic, before the shortfall and interest checks.
// Router goroutine.
func (gs *GossipSubRouter) seacastRepairTo(topic string) []peer.ID {
	links := make([]peer.ID, 0, len(gs.mesh[topic]))
	for q := range gs.mesh[topic] {
		links = append(links, q)
	}
	sort.Slice(links, func(a, b int) bool { return links[a] < links[b] }) // a fixed order before the caller's shuffle
	return links
}

// seacastPushRebuilt is push_rebuilt(g, new) (§4.6, [c10]): over the group the rebuilt chunks fill,
// to the mesh peers (REPAIR_TO, seacastRepairTo) whose interest holds the chunk and whose shortfall
// under the type's K is open, up to the shortfall, at random among the lacking chunks; a refused
// push is offered. No R_NODE bound (default repair).
// Router goroutine.
func (gs *GossipSubRouter) seacastPushRebuilt(T ObjectType, root string, rebuilt []uint32, cells map[uint32]*Message) {
	sc := gs.seacast
	byGroup := map[string][]seacastRepairItem{}
	for _, idx := range rebuilt {
		byGroup[root] = append(byGroup[root], seacastRepairItem{idx, cells[idx]})
	}
	groups := make([]string, 0, len(byGroup))
	for g2 := range byGroup {
		groups = append(groups, g2)
	}
	sort.Strings(groups)
	for _, g2 := range groups {
		if expired, _ := gs.seacastRootState(g2); expired {
			continue // an expired axis is not acted on (v0.3 §8.1)
		}
		T2 := gs.seacastRepairType(g2)
		if T2 == nil {
			sc.ctr.RepairNoType.Add(1)
			continue
		}
		topic, placed := T2.TopicOf(g2)
		if !placed {
			sc.ctr.RepairNoType.Add(1)
			continue
		}
		sc.ctr.RepairGroups.Add(1)
		k := T2.Threshold(g2)
		links := gs.seacastRepairTo(topic)
		rand.Shuffle(len(links), func(a, b int) { links[a], links[b] = links[b], links[a] })
		for _, q := range links {
			// The interest through the type resolved here: a decoder repairing a column outside its
			// custody runs none of that column's topics, so its own lookup by root finds no type.
			want := gs.seacastInterestOfPeerT(q, T2, g2, topic)
			if want == nil {
				sc.ctr.RepairNoInterest.Add(1)
				continue
			}
			known := &haveBits{}
			if e := gs.have.peers[q][g2]; e != nil {
				if e.complete {
					continue
				}
				known.Or(e.bits.Bytes())
			}
			if pe := gs.have.pushed[q][g2]; pe != nil {
				known.Or(pe.bits.Bytes())
			}
			short := k - known.Count()
			if short <= 0 {
				sc.ctr.RepairNotShort.Add(1)
				continue
			}
			var lacks []seacastRepairItem
			for _, it := range byGroup[g2] {
				if it.msg != nil && !known.Has(it.index) && want.Has(it.index) {
					lacks = append(lacks, it)
				}
			}
			rand.Shuffle(len(lacks), func(a, b int) { lacks[a], lacks[b] = lacks[b], lacks[a] })
			for _, it := range lacks[:min(short, len(lacks))] {
				mid := gs.p.idGen.ID(it.msg)
				if !gs.sendRPC(q, repairRPC(it.msg.Message), false) {
					gs.seacastOfferRepair(q, topic, mid) // a refused push: offered
					continue
				}
				gs.haveNotePushed(q, mid)
				sc.ctr.RepairPushes.Add(1)
			}
		}
	}
}

// repairRPC is a push of one rebuilt chunk, marked so the writer counts it when written.
func repairRPC(m *pb.Message) *RPC {
	out := rpcWithMessages(m)
	out.repair = true
	return out
}

// seacastOfferRepair offers one rebuilt chunk to q. Router goroutine.
func (gs *GossipSubRouter) seacastOfferRepair(q peer.ID, topic, mid string) {
	ids := gs.haveIHaveIDs(q, []string{mid})
	if len(ids) == 0 {
		return
	}
	t := topic
	gs.sendRPC(q, rpcWithControl(nil, []*pb.ControlIHave{{TopicID: &t, MessageIDs: ids}}, nil, nil, nil, nil), false)
	gs.seacast.ctr.RepairOffers.Add(1)
}
