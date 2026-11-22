package pubsub

import (
	"fmt"
	"sort"
	"sync/atomic"
	"time"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
)

// --- Adversaries against the request rules (hedge-and-adaptivity plan, E7) ---
//
// Extensions of the F1 withholder for the screens the plan names, plus a slow reader. Each is a
// harness-only option on a hash-selected node set; none is a protocol feature.

// WithIWantWithholdingSelect restricts WithIWantWithholding to the ids the predicate accepts;
// every other id is served normally. The selective withholder of the last-piece screen serves
// everything but the piece the victims will need last. Requires WithIWantWithholding first.
func WithIWantWithholdingSelect(pred func(mid string) bool) Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok || !gs.withholdServe {
			return fmt.Errorf("selective withholding requires WithIWantWithholding first")
		}
		if pred == nil {
			return fmt.Errorf("selective withholding requires a predicate")
		}
		gs.withholdSelect = pred
		return nil
	}
}

// WithIWantWithholdingPrefix orders the injection in time by count. With fastFirst, the first n
// withholdable ids this node is asked for are served at once and the rest get the injection: a
// fast prefix then a slow or silent tail. Without it the first n get the injection and the rest
// are served at once. The median-poisoning screen's two timed strategies. Requires
// WithIWantWithholding first.
func WithIWantWithholdingPrefix(n int, fastFirst bool) Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok || !gs.withholdServe {
			return fmt.Errorf("withholding prefix requires WithIWantWithholding first")
		}
		if n <= 0 {
			return fmt.Errorf("withholding prefix must be positive")
		}
		gs.withholdPrefix = n
		gs.withholdPrefixFast = fastFirst
		return nil
	}
}

// withholdIWantServe applies the injection to a built reply and returns what is served normally:
// the ids the selection exempts and the fast side of a prefix. The rest is swallowed (silent) or
// sent whole after the delay, on pubsub's own goroutine like every router action.
func (gs *GossipSubRouter) withholdIWantServe(p peer.ID, mids []string, msgs []*pb.Message) []*pb.Message {
	var serve, held []*pb.Message
	for i, m := range msgs {
		if gs.withholdSelect != nil && !gs.withholdSelect(mids[i]) {
			serve = append(serve, m)
			continue
		}
		if gs.withholdPrefix > 0 {
			inPrefix := gs.withholdAsked < gs.withholdPrefix
			gs.withholdAsked++
			if inPrefix == gs.withholdPrefixFast {
				serve = append(serve, m)
				continue
			}
		}
		held = append(held, m)
	}
	if len(held) == 0 {
		return serve
	}
	if gs.withheldServes != nil {
		gs.withheldServes.Add(int64(len(held)))
	}
	if gs.withholdDelay > 0 {
		asked := gs.p.peers[p] // the connection the ask came on
		time.AfterFunc(gs.withholdDelay, func() {
			select {
			case gs.p.eval <- func() {
				cur, ok := gs.p.peers[p]
				if !ok {
					return
				}
				late := held
				if gs.v05 != nil {
					// Under v0.5 the late serve goes on the connection it was asked on, and only for
					// groups still alive; off v0.5 it leaves as the measured arms sent it.
					if cur != asked {
						return
					}
					late = nil
					for _, m := range held {
						if !gs.expiredID(gs.p.idGen.RawID(m)) {
							late = append(late, m)
						}
					}
					if len(late) == 0 {
						return
					}
				}
				// The late serve is an answer: under v0.5's A′ a full queue owes it as one, and the
				// writer never projects it away. Off v0.5 it leaves as the measured arms sent it.
				gs.sendingSolicited = gs.v05OfferSkipped()
				gs.sendRPC(p, rpcWithControl(late, nil, nil, nil, nil, nil), false)
				gs.sendingSolicited = false
			}:
			case <-gs.p.ctx.Done():
			}
		})
	}
	return serve
}

// WithIHaveHearsay makes this node re-announce every id it hears announced, whether or not it
// holds it: the offer-capture adversary. Its IHAVE reaches a victim before an honest relay has
// the message to announce, so it wins the first ask, and stacked on WithIWantWithholding the ask
// is lost to a move-on. Once per id.
//
// width caps how many peers each id is announced to, mesh peers first and then the rest in
// deterministic peer order; 0 means every peer of the topic. The distinction is the difference
// between an attacker that lies at honest fan-out and one that floods: an honest phase-forwarding
// relay announces to the mesh peers it did not push to plus the heartbeat's lazy subset, so a
// width near the mesh degree is the conformant liar. The unbounded form is only reachable because
// phase forwarding needs gossipsub's per-heartbeat IHAVE limit raised far above its default.
// The counter, when non-nil, records the (id, peer) announcements.
func WithIHaveHearsay(width int, announced *atomic.Int64) Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok {
			return fmt.Errorf("hearsay announcing requires a gossipsub router")
		}
		if width < 0 {
			return fmt.Errorf("hearsay width must be non-negative")
		}
		gs.hearsay = true
		gs.hearsayWidth = width
		gs.hearsaid = make(map[string]struct{})
		gs.hearsayCtr = announced
		return nil
	}
}

// WithRebuiltRefusal makes this node push none of the frames it rebuilds after decoding
// (RebuiltPush): the withholder that will not repair. Announcing them is untouched, and stacked on
// WithIWantWithholding they are never served either. The counter, when non-nil, records the
// rebuilt frames held back.
func WithRebuiltRefusal(refused *atomic.Int64) Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok {
			return fmt.Errorf("rebuilt refusal requires a gossipsub router")
		}
		gs.rebuiltRefuse = true
		gs.rebuiltRefused = refused
		return nil
	}
}

// hearsayAnnounce forwards the announced ids, first time each, to the peers hearsayTargets
// picks. Router goroutine.
func (gs *GossipSubRouter) hearsayAnnounce(from peer.ID, ctl *pb.ControlMessage) {
	for _, e := range ctl.GetIhave() {
		topic := e.GetTopicID()
		if _, ok := gs.p.topics[topic]; !ok {
			continue
		}
		ids := make([]string, 0, len(e.GetMessageIDs()))
		for _, mid := range e.GetMessageIDs() {
			if _, done := gs.hearsaid[mid]; done {
				continue
			}
			gs.hearsaid[mid] = struct{}{}
			ids = append(ids, mid)
		}
		if len(ids) == 0 {
			continue
		}
		for _, p2 := range gs.hearsayTargets(topic, from) {
			ihave := []*pb.ControlIHave{{TopicID: &topic, MessageIDs: ids}}
			gs.sendRPC(p2, rpcWithControl(nil, ihave, nil, nil, nil, nil), false)
			if gs.hearsayCtr != nil {
				gs.hearsayCtr.Add(int64(len(ids)))
			}
		}
	}
}

// hearsayTargets is the peer set one hearsay announcement reaches: mesh peers of the topic first,
// then its other peers, each in deterministic peer order, truncated to the configured width.
func (gs *GossipSubRouter) hearsayTargets(topic string, from peer.ID) []peer.ID {
	tmap, ok := gs.p.topics[topic]
	if !ok {
		return nil
	}
	mesh := gs.mesh[topic]
	var inMesh, rest []peer.ID
	for p2 := range tmap {
		if p2 == from {
			continue
		}
		if _, m := mesh[p2]; m {
			inMesh = append(inMesh, p2)
		} else {
			rest = append(rest, p2)
		}
	}
	sort.Slice(inMesh, func(i, j int) bool { return inMesh[i] < inMesh[j] })
	sort.Slice(rest, func(i, j int) bool { return rest[i] < rest[j] })
	out := append(inMesh, rest...)
	if gs.hearsayWidth > 0 && len(out) > gs.hearsayWidth {
		out = out[:gs.hearsayWidth]
	}
	return out
}

// WithReadDelay makes this node read its inbound streams slowly: d elapses before each read. The
// slow-reading peer of the queue-poisoning screen: its senders' writers block on the stream's
// flow control, and a node-wide maximum over per-peer queue signals reads that one peer as
// congestion everywhere.
func WithReadDelay(d time.Duration) Option {
	return func(ps *PubSub) error {
		if d <= 0 {
			return fmt.Errorf("read delay must be positive")
		}
		ps.readDelay = d
		return nil
	}
}

// WithControlMute makes this node send no IDONTWANT to any peer: no HAVE bitmap, no hose
// acknowledgement, no claim. It still receives, announces, asks and pushes. Against sel=fewest
// the mute peer looks furthest behind to every pusher; stack WithIWantWithholding and
// WithRelaySilence for the mute and silent free-rider. The counter, when non-nil, records the
// ids suppressed, so the exposure is verified.
func WithControlMute(muted *atomic.Int64) Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok {
			return fmt.Errorf("control mute requires a gossipsub router")
		}
		gs.controlMute = true
		gs.controlMuted = muted
		return nil
	}
}

// applyControlMute returns rpc without its IDONTWANT entries under the mute, nil when nothing
// else remains; rpc itself is never mutated, since one RPC can be yielded to several peers.
func (gs *GossipSubRouter) applyControlMute(rpc *RPC) *RPC {
	if !gs.controlMute || rpc == nil {
		return rpc
	}
	ctl := rpc.GetControl()
	if ctl == nil || len(ctl.Idontwant) == 0 {
		return rpc
	}
	n := 0
	for _, e := range ctl.Idontwant {
		n += len(e.MessageIDs)
	}
	if gs.controlMuted != nil {
		gs.controlMuted.Add(int64(n))
	}
	out := shallowRPCCopy(rpc)
	out.urgent = rpc.urgent
	out.Control = nil
	if len(ctl.Ihave) > 0 || len(ctl.Iwant) > 0 || len(ctl.Graft) > 0 || len(ctl.Prune) > 0 || ctl.Extensions != nil {
		out.Control = &pb.ControlMessage{Ihave: ctl.Ihave, Iwant: ctl.Iwant, Graft: ctl.Graft, Prune: ctl.Prune, Extensions: ctl.Extensions}
	}
	if len(out.Publish) == 0 && out.Control == nil && rpcOtherwiseEmpty(out) {
		return nil
	}
	return out
}

// WithHaveUnderReport makes this node tell each peer, in every HAVE bitmap it sends it, only the
// frames of the group that peer sent it -- pushed or answered, first copy or duplicate -- and
// nothing learned from anyone else; it sends no claim. The tailored under-reporter against
// sel=fewest: each pusher's view of it is consistent with what that pusher gave it and maximally
// behind, so it ranks first everywhere and passes any local proof, including the proof guard,
// whose HAVE arrives exactly when an honest node's would. The HAVE timing, the hose stamps and
// everything else are untouched. Requires WithHaveBitmaps. The counters, when non-nil, record the
// indices left out and the claims dropped, so the exposure is verified.
func WithHaveUnderReport(withheld, claims *atomic.Int64) Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok {
			return fmt.Errorf("HAVE under-reporting requires a gossipsub router")
		}
		if gs.have == nil {
			return fmt.Errorf("HAVE under-reporting requires WithHaveBitmaps first")
		}
		gs.under = &underReport{from: make(map[peer.ID]map[string]*haveEntry), withheld: withheld, claims: claims}
		return nil
	}
}

// underReport is the tailored under-reporter's state: what each peer sent us, per root.
type underReport struct {
	from             map[peer.ID]map[string]*haveEntry
	withheld, claims *atomic.Int64
}

// underReportNote records that from sent us mid. Router goroutine, from Preprocess and from the
// duplicate check, before either sends a HAVE or a hose ack.
func (gs *GossipSubRouter) underReportNote(from peer.ID, mid string) {
	if gs.under == nil || (gs.p != nil && from == gs.p.host.ID()) {
		return
	}
	root, index, ok := gs.have.index(mid)
	if !ok || len(root) != haveRootLen {
		return
	}
	roots := gs.under.from[from]
	if roots == nil {
		roots = make(map[string]*haveEntry)
		gs.under.from[from] = roots
	}
	e := roots[root]
	if e == nil {
		e = &haveEntry{}
		roots[root] = e
	}
	e.bits.Set(index)
	e.ttl = gs.have.holdTTL
}

// underReportBits is bits cut down to what p sent us of root, and how many indices that left
// out; bits itself is never mutated.
func (gs *GossipSubRouter) underReportBits(p peer.ID, root string, bits *haveBits) (*haveBits, int) {
	out := &haveBits{b: append([]byte(nil), bits.Bytes()...)}
	var from *haveBits
	if e := gs.under.from[p][root]; e != nil {
		from = &e.bits
	}
	for i := range out.b {
		if from != nil && i < len(from.b) {
			out.b[i] &= from.b[i]
		} else {
			out.b[i] = 0
		}
	}
	return out, bits.Count() - out.Count()
}

// underReportCount adds n left-out indices to the exposure counter; a no-op on an honest node.
func (gs *GossipSubRouter) underReportCount(n int) {
	if gs.under != nil && gs.under.withheld != nil && n > 0 {
		gs.under.withheld.Add(int64(n))
	}
}

// applyUnderReport returns rpc with every HAVE bitmap to p cut down to what p sent us, every
// structured per-id IDONTWANT p did not send us dropped, and every claim dropped; nil when nothing
// else remains. rpc itself is never mutated, since one RPC can be yielded to several peers.
func (gs *GossipSubRouter) applyUnderReport(rpc *RPC, p peer.ID) *RPC {
	if gs.under == nil || rpc == nil {
		return rpc
	}
	ctl := rpc.GetControl()
	if ctl == nil || len(ctl.Idontwant) == 0 {
		return rpc
	}
	entries := make([]*pb.ControlIDontWant, 0, len(ctl.Idontwant))
	for _, e := range ctl.Idontwant {
		ids := make([]string, 0, len(e.MessageIDs))
		for _, mid := range e.MessageIDs {
			if _, _, _, ok := decodeHoseStamp(mid); ok {
				ids = append(ids, mid) // it names only what p just sent us
				continue
			}
			if root, enc, ok := decodeHave(mid); ok {
				switch mid[len(haveMagic)] {
				case haveVersionClaim:
					if gs.under.claims != nil {
						gs.under.claims.Add(1)
					}
				case haveVersion:
					var b haveBits
					b.Or(enc)
					cut, n := gs.underReportBits(p, root, &b)
					gs.underReportCount(n)
					ids = append(ids, encodeHave(root, cut))
				default:
					ids = append(ids, mid)
				}
				continue
			}
			if root, index, ok := gs.have.index(mid); ok && len(root) == haveRootLen {
				if e := gs.under.from[p][root]; e == nil || !e.bits.Has(index) {
					gs.underReportCount(1)
					continue
				}
			}
			ids = append(ids, mid)
		}
		if len(ids) > 0 {
			entries = append(entries, &pb.ControlIDontWant{MessageIDs: ids})
		}
	}
	out := shallowRPCCopy(rpc)
	out.urgent = rpc.urgent
	out.Control = nil
	if len(entries) > 0 || len(ctl.Ihave) > 0 || len(ctl.Iwant) > 0 || len(ctl.Graft) > 0 || len(ctl.Prune) > 0 || ctl.Extensions != nil {
		out.Control = &pb.ControlMessage{Ihave: ctl.Ihave, Iwant: ctl.Iwant, Graft: ctl.Graft, Prune: ctl.Prune, Idontwant: entries, Extensions: ctl.Extensions}
	}
	if len(out.Publish) == 0 && out.Control == nil && rpcOtherwiseEmpty(out) {
		return nil
	}
	return out
}

// underReportHeartbeat ages what each peer sent us with the bitmaps it stands behind.
func (gs *GossipSubRouter) underReportHeartbeat() {
	if gs.under == nil {
		return
	}
	for p, roots := range gs.under.from {
		for root, e := range roots {
			if e.ttl--; e.ttl <= 0 {
				delete(roots, root)
			}
		}
		if len(roots) == 0 {
			delete(gs.under.from, p)
		}
	}
}
