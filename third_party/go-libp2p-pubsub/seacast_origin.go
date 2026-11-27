package pubsub

import (
	"sync"

	core "github.com/cskiraly/seacast-go"
	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
)

// --- The source at the acknowledgement (v0.9; spec 43fe953) ---
//
// seacast-lite.md §5.1's publish and origin_acked and §7.1's window_timeout, as seacast.md §4.7's publish,
// origin_acked and source_pool carry them for every object type: a source's holding names the chunks
// acknowledged so far, a chunk being acknowledged by the first HAVE naming it from any peer or else by its
// first copy's window timeout; each push target's first chunk is led by a holding naming what is
// acknowledged and that chunk; and the relation peers not chosen to be pushed a chunk are offered it at its
// acknowledgement, not at publish. On v0.3's wire the suppression scope and the relation are the topic's
// mesh; source_pool falls back to the topic's links when the mesh has none. Own publications on the path
// only (not rebuilt chunks, choose.go's ownPublication); the payload's phase path is unchanged.
//
// A refused push is not owed here: that is unpush, under v0.5's A′ (specv05.go), a switch; at the source it
// owes to the source_pool the push was planned from (seacastSourceOwes).

// seacastOrigin is the binding's part of the source's state for a group it publishes on the path: the
// topic and the chunks' ids. The core's part is in originAcks. Router goroutine.
type seacastOrigin struct {
	topic string
	ids   map[uint32]string // each own chunk's id, by index
}

// originAcks is the core's state of each group the node publishes on the path (core.Origin: the chunks
// acknowledged, the push targets chosen at dispatch and the targets whose lead holding has gone), under
// one mutex: haveBitsFor reads the chunks acknowledged for every holding of such a group, which may run
// off the router goroutine.
type originAcks struct {
	mu    sync.Mutex
	roots map[string]*core.Origin[peer.ID]
}

// bits is a copy of root's acknowledged indices; ok is false for a root the node does not publish on the
// path. A copy with nothing acknowledged yet is nil (no holding at all).
func (o *originAcks) bits(root string) (b *haveBits, ok bool) {
	if o == nil {
		return nil, false
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	a, ok := o.roots[root]
	if !ok {
		return nil, false
	}
	acked := a.Acked()
	if len(acked) == 0 {
		return nil, true
	}
	return &acked, true
}

// has says whether the node publishes root on the path: the group is in origin (seacast.md §1.5's
// holding_of).
func (o *originAcks) has(root string) bool {
	if o == nil {
		return false
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	_, ok := o.roots[root]
	return ok
}

func (o *originAcks) open(root string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if _, ok := o.roots[root]; !ok {
		o.roots[root] = &core.Origin[peer.ID]{}
	}
}

// dropBeyond is drop_beyond for root's source state once its count n is known (core.Origin.DropBeyond).
func (o *originAcks) dropBeyond(root string, n uint32) {
	if o == nil {
		return
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if a := o.roots[root]; a != nil {
		a.DropBeyond(n)
	}
}

// plan records chunk i of root's push targets (core.Origin.Plan).
func (o *originAcks) plan(root string, i uint32, targets []peer.ID) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if a := o.roots[root]; a != nil {
		a.Plan(i, targets)
	}
}

// lead is the holding to send p just before its first chunk of root, chunk i (core.Origin.Lead); ok
// is false once p has had its lead.
func (o *originAcks) lead(root string, p peer.ID, i uint32) (b haveBits, ok bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	a := o.roots[root]
	if a == nil {
		return nil, false
	}
	return a.Lead(p, i)
}

// ack is origin_acked(g, i) for root (core.Origin.Ack): newly is false when i was acknowledged
// already; offer is the peers of pool not planned to be pushed i.
func (o *originAcks) ack(root string, i uint32, pool []peer.ID) (offer []peer.ID, newly bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	a := o.roots[root]
	if a == nil {
		return nil, false
	}
	return a.Ack(i, pool)
}

func (o *originAcks) forget(root string) {
	if o == nil {
		return
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	delete(o.roots, root)
}

// seacastOriginOpen opens the source's state for msg's group, before anything about the publication is
// sent: an own, not rebuilt, chunk on the path. Router goroutine, from Publish and PublishBatch.
func (gs *GossipSubRouter) seacastOriginOpen(msg *Message) {
	sc := gs.seacast
	if sc == nil || gs.have == nil || !ownPublication(msg, gs.p.host.ID()) {
		return
	}
	if gs.seacastType(msg.GetTopic()) == nil {
		return
	}
	mid := gs.p.idGen.ID(msg)
	root, index, ok := gs.have.index(mid)
	if !ok || len(root) != haveRootLen {
		return
	}
	if sc.origin == nil {
		sc.origin = map[string]*seacastOrigin{}
	}
	o := sc.origin[root]
	if o == nil {
		o = &seacastOrigin{topic: msg.GetTopic(), ids: map[uint32]string{}}
		sc.origin[root] = o
	}
	o.ids[index] = mid
	if gs.have.originAcks == nil {
		gs.have.originAcks = &originAcks{roots: map[string]*core.Origin[peer.ID]{}}
	}
	gs.have.originAcks.open(root)
}

// seacastOriginPlan records, at dispatch, an own chunk's push targets, and reports whether the chunk is
// the source's: then no offer goes now (the caller empties its announce). Router goroutine, from rpcs.
func (gs *GossipSubRouter) seacastOriginPlan(mid string, tosend map[peer.ID]struct{}) bool {
	sc := gs.seacast
	if sc == nil || sc.origin == nil || gs.have == nil {
		return false
	}
	root, index, ok := gs.have.index(mid)
	if !ok {
		return false
	}
	o := sc.origin[root]
	if o == nil || o.ids[index] != mid {
		return false
	}
	targets := make([]peer.ID, 0, len(tosend))
	for p := range tosend {
		targets = append(targets, p)
	}
	gs.have.originAcks.plan(root, index, targets)
	return true
}

// seacastOriginLead sends p, before its first chunk of a group the node publishes, the lead holding:
// what is acknowledged and that chunk ([P4]). Router goroutine, before the chunk's RPC is queued.
func (gs *GossipSubRouter) seacastOriginLead(p peer.ID, rpc *RPC) {
	sc := gs.seacast
	if sc == nil || sc.origin == nil || gs.have == nil || len(rpc.Publish) == 0 {
		return
	}
	gs.haveQueue(p) // the queue's hooks, the window timeout's among them, before our first chunk to p
	if !gs.feature(GossipSubFeatureIdontwant, gs.peers[p]) {
		return
	}
	for _, m := range rpc.Publish {
		mid := gs.p.idGen.ID(&Message{Message: m})
		root, index, ok := gs.have.index(mid)
		if !ok {
			continue
		}
		if sc.origin[root] == nil {
			continue
		}
		bits, first := gs.have.originAcks.lead(root, p, index)
		if !first {
			continue
		}
		lead := rpcWithControl(nil, nil, nil, nil, nil, []*pb.ControlIDontWant{{MessageIDs: []string{gs.seacastEncodeHave(root, &bits, false)}}})
		gs.sendRPC(p, lead, true)
	}
}

// seacastOriginBelowCount is bits without the indices at or beyond root's count, for a group the node
// publishes whose count the type knows; otherwise bits itself. Such a group can hold an acknowledgement
// taken while its count was unknown, and the router takes it out (seacastDropBeyond) only at the
// header's validation, which the type may know before. A receiver that knows the count refuses a
// holding naming such an index whole.
func (gs *GossipSubRouter) seacastOriginBelowCount(root string, bits *haveBits) *haveBits {
	if bits == nil || gs.have == nil || !gs.have.originAcks.has(root) {
		return bits
	}
	T := gs.seacastRootType(root)
	if T == nil {
		return bits
	}
	n := T.IndexCount(root)
	if n <= 0 || len(*bits)*8 <= n {
		return bits // unknown, or no index at or beyond it
	}
	below := &haveBits{}
	for _, i := range bits.Indices() {
		if int64(i) < int64(n) {
			below.Set(i)
		}
	}
	return below
}

// seacastOriginAckIndices is a peer's HAVE newly naming indices of groups: an index of a group the node
// publishes is acknowledged. Router goroutine, from haveAbsorb.
func (gs *GossipSubRouter) seacastOriginAckIndices(root string, newly []uint32) {
	sc := gs.seacast
	if sc == nil || sc.origin[root] == nil {
		return
	}
	for _, i := range newly {
		gs.seacastOriginAck(root, i)
	}
}

// seacastOriginAck is origin_acked(g, i): once per index, the index joins the source's holding, which goes
// to the suppression scope as a forwarding node's does, and the source_pool peers not chosen to be pushed
// it are offered it. Router goroutine.
func (gs *GossipSubRouter) seacastOriginAck(root string, i uint32) {
	sc := gs.seacast
	if sc == nil {
		return
	}
	o := sc.origin[root]
	if o == nil {
		return
	}
	if T := gs.seacastRootType(root); T != nil {
		if n := T.IndexCount(root); n > 0 && int64(i) >= int64(n) {
			// An index the group has not, once its count is known: a window timeout of a copy sent
			// while it was unknown. drop_beyond took it, and it stays out.
			return
		}
	}
	offer, newly := gs.have.originAcks.ack(root, i, gs.seacastSourcePool(o.topic))
	if !newly {
		return
	}
	sc.ctr.OriginAcked.Add(1)
	mid, ok := o.ids[i]
	if !ok {
		return
	}
	gs.idontwantMesh(o.topic, []string{mid}, "") // the holding, now naming i, to the suppression scope
	for _, p := range offer {
		gs.seacastOriginOffer(p, o.topic, mid)
	}
}

// seacastSourcePool is source_pool (seacast.md §4.7): the relation peers, on v0.3's wire the topic's mesh,
// or, with none, the topic's links (gossipsub's fanout's candidates). Router goroutine.
func (gs *GossipSubRouter) seacastSourcePool(topic string) []peer.ID {
	var pool []peer.ID
	for p := range gs.mesh[topic] {
		pool = append(pool, p)
	}
	if len(pool) == 0 {
		for p := range gs.p.topics[topic] {
			pool = append(pool, p)
		}
	}
	return pool
}

// seacastSourceOwes reports whether p is in the pool the source planned root's push from (unpush, seacast.md
// §4.7, spec 43fe953): root is an own group on the path and p is in its topic's source_pool. Router goroutine.
func (gs *GossipSubRouter) seacastSourceOwes(p peer.ID, topic, root string) bool {
	sc := gs.seacast
	if sc == nil || sc.origin[root] == nil {
		return false
	}
	if _, inMesh := gs.mesh[topic][p]; inMesh {
		return true
	}
	if len(gs.mesh[topic]) > 0 {
		return false // the relation is the pool: a link outside it was a wide first hop's push
	}
	_, link := gs.p.topics[topic][p]
	return link
}

// seacastOriginOffer is the dispatch's offer of an own chunk, made at its acknowledgement.
func (gs *GossipSubRouter) seacastOriginOffer(p peer.ID, topic, mid string) {
	if gs.iSupportSendingPartial(topic) && gs.peerRequestsPartial(p, topic) {
		return
	}
	gs.seacast.ctr.Offered.Add(1)
	ids := gs.haveIHaveIDs(p, []string{mid})
	if len(ids) == 0 {
		return
	}
	ihave := rpcWithControl(nil, []*pb.ControlIHave{{TopicID: &topic, MessageIDs: ids}}, nil, nil, nil, nil)
	gs.sendRPC(p, ihave, false)
}

// seacastOriginForget drops the source's state for a group with the group.
func (gs *GossipSubRouter) seacastOriginForget(root string) {
	if sc := gs.seacast; sc != nil {
		delete(sc.origin, root)
	}
	if gs.have != nil {
		gs.have.originAcks.forget(root)
	}
}
