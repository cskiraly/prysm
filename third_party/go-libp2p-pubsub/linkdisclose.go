package pubsub

import (
	"fmt"
	"sort"
	"sync/atomic"
	"time"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
)

// --- Link-local disclosure (privacy) ---
//
// Cut-through makes the source the only node whose holding is complete before it forwards
// anything, and a HAVE bitmap tells every mesh peer the whole holding, so every honest per-frame
// announcement marks the source to its mesh. Under link-local disclosure every node, source and
// relay alike, limits what it reveals of a group to each peer until it completes the group. Per
// directed link and group it keeps a monotone disclosed set, and every channel that names held
// frames draws from it: HAVE bitmaps (alone, parked, attached, hose acknowledgements), phase and
// heartbeat IHAVEs, and the frames it serves that peer by IWANT. The frames exchanged with the
// peer (received from it, pushed to it) are always disclosed once held, and count; other held
// frames join, in index order as a message names them, while the union is below the budget, so a
// link's disclosure grows no faster than the node's own holding and passes the budget only by
// exchanged frames. From completion on (need frames held, or a decode claim) the node discloses
// normally.
//
// The source has no completion at publish. It counts its own frames as received when its writers
// pop them (WithOriginHavePaced, lead HAVE included), announces each frame to its phase-announce
// peers at that pop, as a relay announces at receipt, and completes at need pops, when its HAVE
// names the whole group, as a decoding relay's does.
//
// Origin dilution (WithOriginDilute) applies the same cut to the groups a node publishes only,
// with a budget that grows with time since publish and a completion at a set pace instead of at
// need pops, so the source's per-link HAVE follows a hop-one relay's curve.

// DisclosureStats counts what link-local disclosure did, for a harness's known answer. Safe for
// concurrent use; Reset clears it between measurements.
type DisclosureStats struct {
	// MaxShown is the largest disclosed union on one link when its node completed the group,
	// exchanged frames included; OverBudget counts the links where that passed the budget, which
	// only exchanged frames can do; MaxTraded is the most frames exchanged on one link by then and
	// MaxBeyond the most disclosed beyond them, at most the budget. Links is the links counted,
	// Completed the (node, group) completions.
	MaxShown, OverBudget, MaxTraded, MaxBeyond, Links, Completed atomic.Int64
	// Withheld is the indices cut from outgoing HAVE and IHAVE bitmaps before completion, Refused
	// the IWANT asks for undisclosed frames left unanswered, Deferred the source's phase announces
	// held back to their frame's pop.
	Withheld, Refused, Deferred atomic.Int64
}

// Reset clears every counter.
func (s *DisclosureStats) Reset() {
	for _, c := range []*atomic.Int64{&s.MaxShown, &s.OverBudget, &s.MaxTraded, &s.MaxBeyond, &s.Links, &s.Completed, &s.Withheld, &s.Refused, &s.Deferred} {
		c.Store(0)
	}
}

// storeMax raises c to v.
func storeMax(c *atomic.Int64, v int64) {
	for {
		cur := c.Load()
		if v <= cur || c.CompareAndSwap(cur, v) {
			return
		}
	}
}

// linkDisclosure is WithLinkDisclosure's state, router-owned.
type linkDisclosure struct {
	budget int
	need   int
	stats  *DisclosureStats
	// links is, per peer and group not yet completed, what we have disclosed to the peer and
	// exchanged with it; done is the groups we completed, each with the hold TTL in heartbeats, and
	// told those we publish whose whole-group HAVE has gone to the mesh.
	links map[peer.ID]map[string]*discloseLink
	done  map[string]int
	told  map[string]bool
	// announce is, for frames of a group we publish, the phase announces waiting for the pop.
	announce map[string]*discloseAnnounce
	// pace > 0 is WithOriginDilute: only groups we publish are limited, each from start, its
	// publish time, with a budget growing to need at pace, when the group completes.
	pace  time.Duration
	start map[string]time.Time
	// count > 0 is the group's frame count, which completion names whole, as a completed relay's
	// HAVE (HaveAll) does.
	count uint32
}

// diluteTick is how often a diluted group's links are offered what their budget has grown by.
const diluteTick = 5 * time.Millisecond

// discloseLink is one (peer, group): the indices disclosed to the peer and those exchanged with it.
type discloseLink struct {
	shown  haveBits
	traded haveBits
	ttl    int
}

// and is the frames of b that held has too; held may be nil.
func (b *haveBits) and(held *haveBits) *haveBits {
	out := &haveBits{b: append([]byte(nil), b.Bytes()...)}
	for i := range out.b {
		if held != nil && i < len(held.b) {
			out.b[i] &= held.b[i]
		} else {
			out.b[i] = 0
		}
	}
	return out
}

// union is what p has been told of the group: the disclosed frames and the held frames exchanged
// with it, named yet or not.
func (l *discloseLink) union(held *haveBits) *haveBits {
	out := l.traded.and(held)
	out.Or(l.shown.Bytes())
	return out
}

// discloseAnnounce is one frame's deferred phase announce.
type discloseAnnounce struct {
	topic string
	peers []peer.ID
	ttl   int
}

// WithLinkDisclosure limits what this node discloses of a group to each peer, until it holds need
// frames of the group, to budget frames beyond those exchanged with that peer (the section
// above). stats, when non-nil, counts what the limit did. Requires WithHaveBitmaps and
// WithOriginHavePaced: the source's holding is what its writers have popped.
func WithLinkDisclosure(budget, need int, stats *DisclosureStats) Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok {
			return fmt.Errorf("link disclosure requires a gossipsub router")
		}
		if gs.have == nil || gs.have.originPaced == nil || gs.have.originPaced.mode != OriginHavePaced {
			return fmt.Errorf("link disclosure requires WithHaveBitmaps and WithOriginHavePaced first")
		}
		if budget < 1 || need < 1 {
			return fmt.Errorf("link disclosure requires budget >= 1 and need >= 1")
		}
		if stats == nil {
			stats = &DisclosureStats{}
		}
		gs.have.disclose = &linkDisclosure{
			budget: budget, need: need, stats: stats,
			links:    make(map[peer.ID]map[string]*discloseLink),
			done:     make(map[string]int),
			told:     make(map[string]bool),
			announce: make(map[string]*discloseAnnounce),
		}
		return nil
	}
}

// WithOriginDilute limits, per peer, what this node discloses of each group it publishes to what
// a relay one hop out could hold: the frames exchanged with the peer, then held frames in one
// pseudo-random order for all links, up to a count that grows to need frames pace after publish,
// when the group completes and the HAVE names it whole, as a decoding relay's does. Relayed
// groups are disclosed normally. count > 0 is the group's frame count, which the completing HAVE
// names, for a network whose completed relays name every frame (HaveAll). Every channel is cut as
// under WithLinkDisclosure, which it excludes. Requires WithHaveBitmaps and WithOriginHavePaced.
func WithOriginDilute(pace time.Duration, need int, count uint32, stats *DisclosureStats) Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok {
			return fmt.Errorf("origin dilution requires a gossipsub router")
		}
		if gs.have == nil || gs.have.originPaced == nil || gs.have.originPaced.mode != OriginHavePaced {
			return fmt.Errorf("origin dilution requires WithHaveBitmaps and WithOriginHavePaced first")
		}
		if gs.have.disclose != nil {
			return fmt.Errorf("origin dilution excludes WithLinkDisclosure")
		}
		if pace <= 0 || need < 1 {
			return fmt.Errorf("origin dilution requires pace > 0 and need >= 1")
		}
		if stats == nil {
			stats = &DisclosureStats{}
		}
		gs.have.disclose = &linkDisclosure{
			budget: need, need: need, stats: stats, pace: pace, count: count,
			links:    make(map[peer.ID]map[string]*discloseLink),
			done:     make(map[string]int),
			told:     make(map[string]bool),
			announce: make(map[string]*discloseAnnounce),
			start:    make(map[string]time.Time),
		}
		return nil
	}
}

// scoped reports whether disclosure limits root: every group, or under dilution only ours.
func (d *linkDisclosure) scoped(root string) bool {
	if d.pace == 0 {
		return true
	}
	_, ok := d.start[root]
	return ok
}

// budgetFor is how many frames of root a link may be told beyond its exchanges: the fixed budget,
// or under dilution the share of need that the time since publish is of pace, below need until
// completion.
func (d *linkDisclosure) budgetFor(root string) int {
	if d.pace == 0 {
		return d.budget
	}
	n := int(int64(d.need) * int64(time.Since(d.start[root])) / int64(d.pace))
	return min(max(n, 0), d.need-1)
}

// owns reports whether root is a group we publish.
func (o *originPaced) owns(root string) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.roots[root] != nil
}

// isDone reports whether we completed root.
func (d *linkDisclosure) isDone(root string) bool {
	_, ok := d.done[root]
	return ok
}

// link is p's state for root, created with the hold TTL when missing.
func (d *linkDisclosure) link(p peer.ID, root string, ttl int) *discloseLink {
	roots := d.links[p]
	if roots == nil {
		roots = make(map[string]*discloseLink)
		d.links[p] = roots
	}
	l := roots[root]
	if l == nil {
		l = &discloseLink{}
		roots[root] = l
	}
	l.ttl = ttl
	return l
}

// discloseDone reports whether we completed root, recording the completion the first time what
// we hold of it reaches need: every link's disclosed set is counted and dropped. Router goroutine.
func (gs *GossipSubRouter) discloseDone(root string) bool {
	d := gs.have.disclose
	if d.isDone(root) || !d.scoped(root) {
		return true
	}
	if d.pace > 0 && time.Since(d.start[root]) < d.pace {
		return false
	}
	held := gs.have.haveBitsFor(root)
	if held == nil || held.Count() < d.need {
		return false
	}
	d.done[root] = gs.have.holdTTL
	s := d.stats
	s.Completed.Add(1)
	for p, roots := range d.links {
		l := roots[root]
		if l == nil {
			continue
		}
		n := l.union(held).Count()
		storeMax(&s.MaxShown, int64(n))
		storeMax(&s.MaxTraded, int64(l.traded.and(held).Count()))
		storeMax(&s.MaxBeyond, int64(l.shown.AndNot(&l.traded).Count()))
		s.Links.Add(1)
		if n > d.budget {
			s.OverBudget.Add(1)
		}
		delete(roots, root)
		if len(roots) == 0 {
			delete(d.links, p)
		}
	}
	return true
}

// discloseTells reports whether a tell of our pops of root, a group we publish, goes to the mesh:
// each until the first after completion, which names the whole group as a decoder's HAVE does;
// later pops add nothing to say. Router goroutine, from haveOriginTell.
func (gs *GossipSubRouter) discloseTells(root string) bool {
	d := gs.have.disclose
	if d.told[root] {
		return false
	}
	if gs.discloseDone(root) {
		d.told[root] = true
	}
	return true
}

// discloseBits is bits, a bitmap of root about to be named to p, cut to what p may be told: what
// was disclosed to it before, the held frames exchanged with it, and further held frames in index
// order while that union is below the budget, which grows by what it admits. bits itself when
// disclosure is off or we completed root. Router goroutine.
func (gs *GossipSubRouter) discloseBits(p peer.ID, root string, bits *haveBits) *haveBits {
	h := gs.have
	d := h.disclose
	if d == nil || bits == nil || gs.discloseDone(root) {
		return bits
	}
	held := h.haveBitsFor(root)
	l := d.links[p][root]
	switch {
	case l != nil:
		l.ttl = h.holdTTL
	case held == nil:
		d.stats.Withheld.Add(int64(bits.Count()))
		return &haveBits{} // nothing held and nothing disclosed yet
	default:
		l = d.link(p, root, h.holdTTL)
	}
	has := func(i uint32) bool { return bits.Has(i) && held != nil && held.Has(i) && !l.shown.Has(i) }
	union := l.union(held)
	n := union.Count()
	for i := uint32(0); i < uint32(len(bits.b))*8; i++ { // lint:ignore uintcast -- bounded by haveMaxBytes*8.
		if has(i) && l.traded.Has(i) {
			l.shown.Set(i) // in the union already
		}
	}
	budget := d.budgetFor(root)
	var admit []uint32
	for i := uint32(0); i < uint32(len(bits.b))*8; i++ { // lint:ignore uintcast -- bounded by haveMaxBytes*8.
		if has(i) && !union.Has(i) {
			admit = append(admit, i)
		}
	}
	if d.pace > 0 {
		// One order for every link, so each hears a prefix of it: a relay's one holding, which is
		// what reached it rather than a prefix of the index space.
		sort.Slice(admit, func(x, y int) bool { return pairRank(root, admit[x], "", "") < pairRank(root, admit[y], "", "") })
	}
	for _, i := range admit {
		if n >= budget {
			break
		}
		l.shown.Set(i)
		n++
	}
	out := bits.and(&l.shown)
	if cut := bits.Count() - out.Count(); cut > 0 {
		d.stats.Withheld.Add(int64(cut))
	}
	return out
}

// discloseTraded records that p and we exchanged mid: p sent it to us, or we push it to p.
// Router goroutine.
func (gs *GossipSubRouter) discloseTraded(p peer.ID, mid string) {
	h := gs.have
	if h == nil || h.disclose == nil || (gs.p != nil && p == gs.p.host.ID()) {
		return
	}
	root, index, ok := h.index(mid)
	if !ok || len(root) != haveRootLen || h.disclose.isDone(root) || !h.disclose.scoped(root) {
		return
	}
	h.disclose.link(p, root, h.holdTTL).traded.Set(index)
}

// discloseLead discloses to p the frame of a group we publish that our lead HAVE names ahead of
// pushing it: an exchanged frame, named just before it is popped. Router goroutine.
func (gs *GossipSubRouter) discloseLead(p peer.ID, root string, index uint32) {
	h := gs.have
	if h.disclose == nil || h.disclose.isDone(root) {
		return
	}
	h.disclose.link(p, root, h.holdTTL).shown.Set(index)
}

// discloseRefuses reports whether an IWANT from p for mid, a frame we hold, goes unanswered: the
// frame is of a group we have not completed and was neither disclosed to p nor exchanged with it.
// Serving it would disclose it. Router goroutine.
func (gs *GossipSubRouter) discloseRefuses(p peer.ID, mid string) bool {
	h := gs.have
	if h == nil || h.disclose == nil {
		return false
	}
	root, index, ok := h.index(mid)
	if !ok || len(root) != haveRootLen || gs.discloseDone(root) {
		return false
	}
	if l := h.disclose.links[p][root]; l != nil && (l.shown.Has(index) || l.traded.Has(index)) {
		return false
	}
	h.disclose.stats.Refused.Add(1)
	return true
}

// discloseDefer holds back, for a frame of a group we publish and have not completed, the phase
// announce to peers until our writers pop the frame; reports whether it did. Router goroutine.
func (gs *GossipSubRouter) discloseDefer(topic, mid string, peers []peer.ID) bool {
	h := gs.have
	if h == nil || h.disclose == nil || len(peers) == 0 {
		return false
	}
	root, _, ok := h.index(mid)
	if !ok || len(root) != haveRootLen || h.disclose.isDone(root) || !h.originPaced.owns(root) {
		return false
	}
	a := h.disclose.announce[mid]
	if a == nil {
		a = &discloseAnnounce{topic: topic}
		h.disclose.announce[mid] = a
	}
	a.peers = append(a.peers, peers...)
	a.ttl = h.holdTTL
	h.disclose.stats.Deferred.Add(int64(len(peers)))
	return true
}

// discloseAnnounce sends the phase announces held back for mids, frames our writers just popped,
// as rpcs sends a relay's. Router goroutine, from haveOriginTell.
func (gs *GossipSubRouter) discloseAnnounce(mids []string) {
	d := gs.have.disclose
	for _, mid := range mids {
		a := d.announce[mid]
		if a == nil {
			continue
		}
		delete(d.announce, mid)
		csum := computeChecksum(mid)
		for _, pid := range a.peers {
			if _, ok := gs.p.peers[pid]; !ok {
				continue
			}
			if _, ok := gs.unwanted[pid][csum]; ok {
				continue
			}
			if gs.hoseOffers != nil {
				gs.offerLater(pid, a.topic, mid)
				continue
			}
			ids := gs.haveIHaveIDs(pid, []string{mid})
			if len(ids) == 0 {
				continue
			}
			topic := a.topic
			ihave := rpcWithControl(nil, []*pb.ControlIHave{{TopicID: &topic, MessageIDs: ids}}, nil, nil, nil, nil)
			ihave.urgent = gs.haveAnnounceUrgent()
			gs.sendRPC(pid, ihave, ihave.urgent)
		}
	}
}

// discloseID is the control id mid as p may be told it: a HAVE or IHAVE bitmap cut by
// discloseBits, a per-id structured id kept or dropped, anything else (a claim, a receive stamp,
// an unstructured id) as it is; keep is false when nothing of it may go.
func (gs *GossipSubRouter) discloseID(p peer.ID, mid string) (out string, keep bool) {
	h := gs.have
	if _, _, _, ok := decodeHoseStamp(mid); ok {
		return mid, true // it names only what p just sent us
	}
	if root, load, enc, ok := decodeHaveLoad(mid); ok {
		if isHaveClaim(mid) || len(root) != haveRootLen {
			return mid, true // a claim says "stop sending", not "I hold"
		}
		var b haveBits
		b.Or(enc)
		cut := gs.discloseBits(p, root, &b)
		if cut == &b {
			return mid, true
		}
		if cut.Count() == 0 {
			return "", false
		}
		if mid[len(haveMagic)] == haveVersionLoad {
			return encodeHaveLoad(root, load, cut), true
		}
		return encodeHave(root, cut), true
	}
	if root, index, ok := h.index(mid); ok && len(root) == haveRootLen {
		var b haveBits
		b.Set(index)
		if cut := gs.discloseBits(p, root, &b); cut != &b && cut.Count() == 0 {
			return "", false
		}
	}
	return mid, true
}

// applyDisclosure returns rpc with every HAVE and IHAVE naming held frames to p cut to what p may
// be told (discloseID); nil when nothing else remains. rpc itself is never mutated, since one RPC
// can be yielded to several peers. Router goroutine, from doSendRPC.
func (gs *GossipSubRouter) applyDisclosure(rpc *RPC, p peer.ID) *RPC {
	if gs.have == nil || gs.have.disclose == nil || rpc == nil {
		return rpc
	}
	ctl := rpc.GetControl()
	if ctl == nil || (len(ctl.Idontwant) == 0 && len(ctl.Ihave) == 0) {
		return rpc
	}
	changed := false
	cutIDs := func(ids []string) []string {
		out := make([]string, 0, len(ids))
		for _, mid := range ids {
			id, keep := gs.discloseID(p, mid)
			if !keep || id != mid {
				changed = true
			}
			if keep {
				out = append(out, id)
			}
		}
		return out
	}
	var idontwant []*pb.ControlIDontWant
	for _, e := range ctl.Idontwant {
		if ids := cutIDs(e.MessageIDs); len(ids) > 0 {
			idontwant = append(idontwant, &pb.ControlIDontWant{MessageIDs: ids})
		}
	}
	var ihave []*pb.ControlIHave
	for _, e := range ctl.Ihave {
		if ids := cutIDs(e.MessageIDs); len(ids) > 0 {
			ihave = append(ihave, &pb.ControlIHave{TopicID: e.TopicID, MessageIDs: ids})
		}
	}
	if !changed {
		return rpc
	}
	out := shallowRPCCopy(rpc)
	out.urgent = rpc.urgent
	out.Control = nil
	if len(idontwant) > 0 || len(ihave) > 0 || len(ctl.Iwant) > 0 || len(ctl.Graft) > 0 || len(ctl.Prune) > 0 || ctl.Extensions != nil {
		out.Control = &pb.ControlMessage{Ihave: ihave, Iwant: ctl.Iwant, Graft: ctl.Graft, Prune: ctl.Prune, Idontwant: idontwant, Extensions: ctl.Extensions}
	}
	if len(out.Publish) == 0 && out.Control == nil && rpcOtherwiseEmpty(out) {
		return nil
	}
	return out
}

// discloseHeartbeat ages completed groups, link state and held-back announces on the hold TTL.
func (gs *GossipSubRouter) discloseHeartbeat() {
	d := gs.have.disclose
	if d == nil {
		return
	}
	for root, ttl := range d.done {
		if ttl <= 1 {
			delete(d.done, root)
			delete(d.told, root)
			delete(d.start, root)
		} else {
			d.done[root] = ttl - 1
		}
	}
	for p, roots := range d.links {
		for root, l := range roots {
			if l.ttl--; l.ttl <= 0 {
				delete(roots, root)
			}
		}
		if len(roots) == 0 {
			delete(d.links, p)
		}
	}
	for mid, a := range d.announce {
		if a.ttl--; a.ttl <= 0 {
			delete(d.announce, mid)
		}
	}
}

// discloseOriginStart begins dilution of root, a group we publish on topic: its budget runs from
// now, and a tick offers each mesh peer what the budget has grown by. Router goroutine, from
// haveOriginOwn.
func (gs *GossipSubRouter) discloseOriginStart(topic, root string) {
	d := gs.have.disclose
	if d == nil || d.pace == 0 || d.scoped(root) {
		return
	}
	d.start[root] = time.Now()
	gs.discloseOriginArm(topic, root)
}

// discloseOriginArm schedules the next dilution tick of root.
func (gs *GossipSubRouter) discloseOriginArm(topic, root string) {
	time.AfterFunc(diluteTick, func() {
		select {
		case gs.p.eval <- func() { gs.discloseOriginTick(topic, root) }:
		case <-gs.p.ctx.Done():
		}
	})
}

// discloseOriginTick sends each mesh peer our HAVE for root when what it may be told has grown,
// as a relay's HAVE grows with arrivals between our pops; at completion it names the whole group
// to the mesh once, as a decoding relay's does, and stops. Router goroutine.
func (gs *GossipSubRouter) discloseOriginTick(topic, root string) {
	h := gs.have
	d := h.disclose
	if !d.scoped(root) || d.told[root] {
		return
	}
	if gs.discloseDone(root) {
		d.told[root] = true
		if d.count > 0 {
			gs.haveAll(topic, root, d.count)
			return
		}
		if held := h.haveBitsFor(root); held != nil {
			for p := range gs.mesh[topic] {
				gs.discloseSendHave(p, root, held)
			}
		}
		return
	}
	if held := h.haveBitsFor(root); held != nil {
		for p := range gs.mesh[topic] {
			before := 0
			if l := d.links[p][root]; l != nil {
				before = l.shown.and(held).Count()
			}
			if cut := gs.discloseBits(p, root, held); cut.Count() > before {
				gs.discloseSendHave(p, root, cut)
			}
		}
	}
	gs.discloseOriginArm(topic, root)
}

// discloseSendHave sends p our HAVE bits for root, parked on its queue when something is queued.
func (gs *GossipSubRouter) discloseSendHave(p peer.ID, root string, bits *haveBits) {
	if !gs.feature(GossipSubFeatureIdontwant, gs.peers[p]) {
		return
	}
	mid := encodeHave(root, bits)
	if gs.havePendAll(p, []string{mid}) {
		return
	}
	gs.sendRPC(p, rpcWithControl(nil, nil, nil, nil, nil, []*pb.ControlIDontWant{{MessageIDs: []string{mid}}}), true)
}
