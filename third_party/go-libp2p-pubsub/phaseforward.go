package pubsub

import (
	"fmt"
	"hash/fnv"
	"math/rand"
	"slices"
	"sort"
	"sync/atomic"
	"time"

	core "github.com/cskiraly/seacast-go"
	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
)

// Phase forwarding: a push-pull phase transition on gossipsub's eager-push path.
//
// Stock gossipsub forwards a validated message to every mesh peer that has not sent an
// IDONTWANT for it. Phase forwarding instead pushes it to at most
// max(0, degree - knownHolders) of them and sends the rest an immediate IHAVE, where
// knownHolders is the count of mesh peers that already announced the message via IDONTWANT.
// Early in a message's diffusion nobody is known to hold it and it is pushed degree-wide;
// late, when duplicates concentrate, it is announced instead, and peers that still lack it
// pull with their ordinary IWANT.
//
// This is the PPPT proposal (push-pull phase transition) with the IDONTWANT count standing
// in for the hop counter, so it needs no wire change: receivers already answer IHAVE with an
// immediate IWANT and serve IWANT from mcache. Two operational caveats: the estimator only
// sees mesh peers that have IDONTWANT'd, which requires messages at or above the router's
// IDONTWANT size threshold to be effective; and the per-heartbeat MaxIHaveMessages budget on
// the receiving side must accommodate one IHAVE per forwarded message rather than the
// heartbeat-batched cadence it was sized for.
type phaseForwarding struct {
	match  func(topic string) bool
	degree int

	// srcPushQuota > 0 caps the frames of one group the publisher pushes to each mesh peer; its
	// other frames are offered, each to the peers it has pushed and offered least, so each mesh
	// peer pulls a share as it pulls from a relay (WithPhaseSourcePushQuota). srcLoad counts, per
	// recent group, what each peer was pushed and offered.
	srcPushQuota int
	srcLoad      map[string]map[peer.ID]*srcPeerLoad
	srcLoadRoots []string

	// sel=fewest pushes to the mesh peers known to hold the fewest frames of the message's
	// group -- what they announced in HAVE bitmaps or we pushed them -- ties by the rank hash.
	selFewest bool
	// selGrace, when positive, ranks last a peer unproven for the group: pushed at least degree
	// frames, the first more than selGrace ago, and no HAVE or hose ack from it for the group.
	selGrace time.Duration
	// selDemoted, when set, hears each (peer, root) pair the first time the guard demotes it.
	selDemoted func(p peer.ID, root string)
	// selTolerance, when set, replaces sel=fewest's argmax on relayed messages by a draw that caps
	// what an under-reporting peer can take (WithPhaseSelectFewestTolerant); selFrames is the
	// group's frame count the proportional draw measures deficits against.
	selTolerance PhaseSelectTolerance
	selFrames    int

	// srcConnections widens the publisher's own push candidates from the mesh to every
	// connected topic peer, so a fast source spreads distinct frames over all its first-hop
	// uplinks; the announce stays mesh-scoped.
	srcConnections bool

	// senderInForward keeps a relayed message's sender a candidate of SEACAST's forward, where it
	// can take a place of the budget and is then skipped at the push (WithSeacastSenderInForward).
	senderInForward bool
}

// WithSeacastSenderInForward keeps the sender of a relayed chunk a candidate of SEACAST's forward:
// the choice can give it a place of the push budget, and the push to it is then skipped, so the
// chunk reaches one peer fewer. Without it, as seacast-lite.md §5.2's forward does, the sender is
// never a candidate. Must follow WithPhaseForwarding.
func WithSeacastSenderInForward() Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok || gs.phaseForward == nil {
			return fmt.Errorf("the sender in the forward requires WithPhaseForwarding first")
		}
		gs.phaseForward.senderInForward = true
		return nil
	}
}

// WithPhaseSourceConnections makes the publisher rank its own messages over every connected
// gossipsub topic peer instead of its mesh, pushing to the phase degree (or the source push
// degree) and announcing only to mesh peers not pushed. Relays keep mesh-scoped candidates.
// Must follow WithPhaseForwarding.
func WithPhaseSourceConnections() Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok {
			return fmt.Errorf("phase source connections requires a gossipsub router")
		}
		if gs.phaseForward == nil {
			return fmt.Errorf("phase source connections requires WithPhaseForwarding first")
		}
		gs.phaseForward.srcConnections = true
		return nil
	}
}

// WithPhaseSelectFewest switches phase forwarding's push-target selection to sel=fewest: the
// mesh peers known to hold the fewest frames of the message's group go first, so a push lands
// where the receiver can forward it at once (the Earliest-Latest peer rule of Abeni, Kiraly,
// Lo Cigno, NETWORKING 2009, on group state). Requires WithPhaseForwarding and WithHaveBitmaps;
// without the IHAVE side only the peers' HAVEs count, not our own pushes.
func WithPhaseSelectFewest() Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok {
			return fmt.Errorf("phase selection requires a gossipsub router")
		}
		if gs.phaseForward == nil {
			return fmt.Errorf("phase selection requires WithPhaseForwarding first")
		}
		if gs.have == nil {
			return fmt.Errorf("sel=fewest requires WithHaveBitmaps first")
		}
		gs.phaseForward.selFewest = true
		return nil
	}
}

// WithPhaseSelectFewestGuard is sel=fewest with a proof guard against the mute peer, which
// sends no HAVE and so looks furthest behind to every pusher: a mesh peer we pushed at least the
// phase degree's frames of a group, the first more than grace ago, that has sent no HAVE bitmap
// or hose ack for the group ranks with the backlogged class, last. Honest peers prove within a
// round trip. demoted, when non-nil, is called on the event loop the first time each (peer,
// root) pair is demoted. Requires WithPhaseSelectFewest and the IHAVE bitmap side, which records
// our pushes.
func WithPhaseSelectFewestGuard(grace time.Duration, demoted func(p peer.ID, root string)) Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok {
			return fmt.Errorf("phase selection requires a gossipsub router")
		}
		if gs.phaseForward == nil || !gs.phaseForward.selFewest {
			return fmt.Errorf("the proof guard requires WithPhaseSelectFewest first")
		}
		if !gs.have.ihaveOn {
			return fmt.Errorf("the proof guard requires WithHaveAnnounceBitmaps first")
		}
		if grace <= 0 {
			return fmt.Errorf("the proof guard needs a positive grace")
		}
		gs.phaseForward.selGrace = grace
		gs.phaseForward.selDemoted = demoted
		return nil
	}
}

// PhaseSelectTolerance is how sel=fewest picks its push targets among the mesh peers that appear
// furthest behind.
type PhaseSelectTolerance int

const (
	// PhaseSelectArgmax pushes to the peers that appear to hold the fewest frames: the default.
	PhaseSelectArgmax PhaseSelectTolerance = iota
	// PhaseSelectLowerHalf draws uniformly among the peers at or below the median apparent
	// holding.
	PhaseSelectLowerHalf
	// PhaseSelectProportional draws with probability proportional to the frames of the group a
	// peer appears to lack.
	PhaseSelectProportional
)

// WithPhaseSelectFewestTolerant replaces sel=fewest's argmax with a draw that bounds what a peer
// can take by under-reporting. What a pusher knows of a peer is what the peer said plus what the
// pusher sent it, so a peer that reports only the latter wins every argmax and passes every local
// proof; a draw caps its share near its fair share whatever it claims. PhaseSelectLowerHalf draws
// uniformly among the peers at or below the median apparent holding, so an under-reporter takes
// at most budget / ceil(half) of the draws; PhaseSelectProportional draws each target with
// probability proportional to frames less its apparent holding, which needs the group's frame
// count. Both draw only among the peers the proof guard leaves in the first class, in the degree
// the phase grants, and fill any budget the draw leaves from the fewest order. The publisher's own messages keep the
// argmax: it ranks its batch at publish time on its own pushes alone, before any peer has
// reported anything for the group. Requires WithPhaseSelectFewest.
func WithPhaseSelectFewestTolerant(mode PhaseSelectTolerance, frames int) Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok {
			return fmt.Errorf("phase selection requires a gossipsub router")
		}
		if gs.phaseForward == nil || !gs.phaseForward.selFewest {
			return fmt.Errorf("the tolerant draw requires WithPhaseSelectFewest first")
		}
		switch mode {
		case PhaseSelectLowerHalf:
		case PhaseSelectProportional:
			if frames <= 0 {
				return fmt.Errorf("the proportional draw needs the group's frame count")
			}
		default:
			return fmt.Errorf("unknown tolerant draw %d", mode)
		}
		gs.phaseForward.selTolerance = mode
		gs.phaseForward.selFrames = frames
		return nil
	}
}

// tolerantOrder reorders cands, in sel=fewest order with the slow class last, by the proportional
// draw (PhaseSelectProportional): its first budget entries are the draw, and whatever the draw
// leaves keeps its fewest order behind them, so a short draw fills from there. The lower-half
// draw is the core's (core.ChoosePush). Event loop.
func (pf *phaseForwarding) tolerantOrder(cands []peer.ID, known map[peer.ID]int, slow map[peer.ID]bool, budget int) []peer.ID {
	fast := 0
	for fast < len(cands) && !slow[cands[fast]] {
		fast++
	}
	pool := cands[:fast]
	if len(pool) == 0 || budget <= 0 {
		return cands
	}
	pool = append([]peer.ID(nil), pool...)
	k := min(budget, len(pool))
	chosen := make(map[peer.ID]bool, k)
	out := make([]peer.ID, 0, len(cands))
	w := make([]int, len(pool))
	total := 0
	for i, p := range pool {
		w[i] = max(0, pf.selFrames-known[p])
		total += w[i]
	}
	for range k {
		if total <= 0 {
			break // nobody appears to lack anything: the fewest order fills the rest
		}
		r, j := rand.Intn(total), 0
		for r >= w[j] {
			r -= w[j]
			j++
		}
		chosen[pool[j]] = true
		out = append(out, pool[j])
		total -= w[j]
		w[j] = 0
	}
	for _, p := range cands {
		if !chosen[p] {
			out = append(out, p)
		}
	}
	return out
}

// pushChoice is the core's choose_push settings for mid of root under this forwarding: the rank
// hash, the draw, and the proof guard at now.
func (pf *phaseForwarding) pushChoice(mid, root string, now time.Time) core.PushChoice[peer.ID] {
	c := core.PushChoice[peer.ID]{
		Rank:        func(p peer.ID) uint64 { return phaseRank(mid, p) },
		Intn:        rand.Intn,
		Now:         now,
		GuardGrace:  pf.selGrace,
		GuardPushes: pf.degree,
	}
	if pf.selDemoted != nil {
		c.Demoted = func(p peer.ID) { pf.selDemoted(p, root) }
	}
	return c
}

// fewestRoot is the group root sel=fewest ranks mid's candidates by; false when sel=fewest is
// off or mid is not a structured group member, which falls back to sel=rank.
func (pf *phaseForwarding) fewestRoot(gs *GossipSubRouter, mid string) (string, bool) {
	if !pf.selFewest || gs.have == nil {
		return "", false
	}
	root, _, ok := gs.have.index(mid)
	return root, ok
}

// WithPhaseSourcePushQuota caps what the publisher pushes of one group to each mesh peer at quota
// frames; a frame no peer under the quota takes is offered instead, to every mesh peer not pushed,
// least loaded first. Must follow WithPhaseForwarding and WithHaveBitmaps.
func WithPhaseSourcePushQuota(quota int) Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok {
			return fmt.Errorf("phase source push quota requires a gossipsub router")
		}
		if gs.phaseForward == nil || gs.have == nil {
			return fmt.Errorf("phase source push quota requires WithPhaseForwarding and WithHaveBitmaps first")
		}
		if quota <= 0 {
			return fmt.Errorf("phase source push quota must be positive")
		}
		gs.phaseForward.srcPushQuota = quota
		gs.phaseForward.srcLoad = make(map[string]map[peer.ID]*srcPeerLoad)
		return nil
	}
}

// srcPeerLoad is what the publisher has pushed and offered one mesh peer of one group.
type srcPeerLoad struct{ pushed, offered int }

// srcLoadMaxRoots bounds the groups whose per-peer load the publisher remembers.
const srcLoadMaxRoots = 8

// quotaTrim is trim for the publisher's own frame under a push quota: cands in order of load
// (pushed plus offered, then the rank hash), pushes to the first budget under the quota, removes the
// rest from tosend and returns the peers to offer the frame to.
func (pf *phaseForwarding) quotaTrim(root, mid string, cands []peer.ID, budget int, tosend map[peer.ID]struct{}) []peer.ID {
	load := pf.srcLoad[root]
	if load == nil {
		load = make(map[peer.ID]*srcPeerLoad)
		pf.srcLoad[root] = load
		pf.srcLoadRoots = append(pf.srcLoadRoots, root)
		if len(pf.srcLoadRoots) > srcLoadMaxRoots {
			delete(pf.srcLoad, pf.srcLoadRoots[0])
			pf.srcLoadRoots = pf.srcLoadRoots[1:]
		}
	}
	get := func(p peer.ID) *srcPeerLoad {
		l := load[p]
		if l == nil {
			l = &srcPeerLoad{}
			load[p] = l
		}
		return l
	}
	sort.Slice(cands, func(i, j int) bool {
		li, lj := get(cands[i]), get(cands[j])
		if a, b := li.pushed+li.offered, lj.pushed+lj.offered; a != b {
			return a < b
		}
		ri, rj := phaseRank(mid, cands[i]), phaseRank(mid, cands[j])
		if ri != rj {
			return ri > rj
		}
		return cands[i] > cands[j]
	})
	pushed := 0
	var offer []peer.ID
	for _, p := range cands {
		if pushed < budget && get(p).pushed < pf.srcPushQuota {
			get(p).pushed++
			pushed++
			continue
		}
		delete(tosend, p)
		offer = append(offer, p)
	}
	for _, p := range offer {
		get(p).offered++
	}
	return offer
}

// WithPhaseForwarding enables the phase transition on every topic the matcher accepts.
func WithPhaseForwarding(match func(topic string) bool, degree int) Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok {
			return fmt.Errorf("phase forwarding requires a gossipsub router")
		}
		if match == nil || degree < 0 {
			return fmt.Errorf("phase forwarding needs a topic matcher and a non-negative degree")
		}
		// Degree 0 is pull-only: every message is announced (immediate IHAVE) and nothing
		// is pushed unasked, on any node including the source. The gossipsub-native
		// counterpart of an announce-then-pull policy.
		gs.phaseForward = &phaseForwarding{match: match, degree: degree}
		return nil
	}
}

// trim caps the mesh members of tosend at max(0, degree - knownHolders), removing the
// over-budget ones and returning them, rank-ordered, so the caller can announce to them
// instead. Peers outside the mesh set (direct and floodsub peers) are never trimmed.
// fromSelf applies the source's push degree and push quota, when they are set.
func (pf *phaseForwarding) trim(gs *GossipSubRouter, mid string, csum checksum, gmap map[peer.ID]struct{}, tosend map[peer.ID]struct{}, fromSelf bool) []peer.ID {
	return pf.trimDegree(gs, mid, csum, gmap, tosend, fromSelf, pf.degree)
}

// trimDegree is trim with the base push degree given: the phase path's own (trim), or an object
// type's R_NODE on SEACAST's path (seacast.go); the source's overrides apply to either.
func (pf *phaseForwarding) trimDegree(gs *GossipSubRouter, mid string, csum checksum, gmap map[peer.ID]struct{}, tosend map[peer.ID]struct{}, fromSelf bool, base int) []peer.ID {
	holders := 0
	for p := range gmap {
		if _, ok := gs.unwanted[p][csum]; ok {
			holders++
		}
	}
	budget := base - holders
	if budget < 0 {
		budget = 0
	}
	cands := make([]peer.ID, 0, len(tosend))
	for p := range tosend {
		if _, inMesh := gmap[p]; inMesh {
			cands = append(cands, p)
		}
	}
	if fromSelf && pf.srcPushQuota > 0 {
		if root, _, ok := gs.have.index(mid); ok {
			return pf.quotaTrim(root, mid, cands, budget, tosend)
		}
	}
	if budget >= len(cands) {
		return nil
	}
	if root, ok := pf.fewestRoot(gs, mid); ok {
		var now time.Time
		if pf.selGrace > 0 {
			now = time.Now()
		}
		c := pf.pushChoice(mid, root, now)
		if pf.selTolerance == PhaseSelectProportional && !fromSelf {
			order, known, slow := core.RankPush(gs.have.peers, gs.have.pushed, seacastGID(root), cands, c)
			cands = pf.tolerantOrder(order, known, slow, budget)
		} else {
			// The publisher's own messages and the argmax keep the ranking; the lower half draws.
			atSource := fromSelf || pf.selTolerance == PhaseSelectArgmax
			cands = core.ChoosePush(gs.have.peers, gs.have.pushed, seacastGID(root), cands, budget, atSource, c)
		}
	} else {
		// sel=rank: per (message, peer) hash so different messages spread their pushes across
		// different peers, and the choice is stable rather than map-order dependent.
		sort.Slice(cands, func(i, j int) bool {
			ri, rj := phaseRank(mid, cands[i]), phaseRank(mid, cands[j])
			if ri != rj {
				return ri > rj
			}
			return cands[i] > cands[j]
		})
	}
	announce := make([]peer.ID, 0, max(0, len(cands)-budget))
	pushed := 0
	for _, p := range cands {
		if pushed < budget {
			pushed++
			continue
		}
		delete(tosend, p)
		announce = append(announce, p)
	}
	return announce
}

// seacastForwardedChunk is the group and index of a message SEACAST's forward handles: a chunk
// of a path root. Any other message on the path's topics keeps trimDegree.
func (gs *GossipSubRouter) seacastForwardedChunk(mid string) (root string, index uint32, ok bool) {
	if gs.have == nil {
		return "", 0, false
	}
	root, index, ok = gs.have.index(mid)
	return root, index, ok && len(root) == haveRootLen
}

// seacastTrim is trimDegree on SEACAST's path, for chunk index of root (seacast-lite.md §5.2 and
// §5.3, through the core). The core says who is a candidate and the budget; the fork's selection
// options order the candidates, as trimDegree does.
//   - At a relay, core.ForwardCandidates over cand, the mesh: each peer whose holding names the
//     chunk takes one from base (R_NODE), and the candidates are the others that want the chunk,
//     but the sender (unless WithSeacastSenderInForward) and the peers that claimed the group
//     complete. A peer wants the chunk when it has not asked us not to send it (IDONTWANT) and its
//     interest has the index.
//   - At the source, the candidates are cand's peers that want the chunk from the node
//     (core.WantsFromMe: not complete, and neither holding the chunk nor pushed it) and have not
//     asked us not to send it, and the budget is base, less no holder; the order is the ranking
//     (no draw).
//
// Every peer of cand not chosen leaves tosend. It returns the peers chosen, in order (the pushes
// leave in it, rankedPushOrder), and those past the budget, in order, to be offered. With the source's
// push quota (WithPhaseSourcePushQuota) the quota chooses, and the order is tosend's (push nil).
func (pf *phaseForwarding) seacastTrim(gs *GossipSubRouter, mid, root string, index uint32, csum checksum, from peer.ID, cand, tosend map[peer.ID]struct{}, fromSelf bool, base int) (push, announce []peer.ID) {
	h, g := gs.have, seacastGID(root)
	pushers := make([]peer.ID, 0, len(cand))
	for p := range cand {
		pushers = append(pushers, p)
	}
	slices.Sort(pushers) // the pool's order, fixed: the order a choice that covers every candidate keeps
	interest := func(p peer.ID) *haveBits {
		if gs.seacast == nil || gs.seacast.roles == nil {
			return nil
		}
		return gs.seacastInterestOfPeer(p, root)
	}
	wants := func(p peer.ID) bool {
		if _, unwanted := gs.unwanted[p][csum]; unwanted {
			return false
		}
		in := interest(p)
		return in == nil || in.Has(index)
	}
	var cands []peer.ID
	budget := max(0, base) // a type's negative R_NODE pushes nothing, as trimDegree's clamp did
	if fromSelf {
		for _, p := range pushers {
			if wants(p) && core.WantsFromMe(h.peers, h.pushed, p, g, index, nil) {
				cands = append(cands, p)
			}
		}
	} else {
		sender := from
		if pf.senderInForward {
			sender = ""
		}
		cands, budget = core.ForwardCandidates(h.peers, g, index, sender, pushers, base, wants)
	}
	for p := range cand {
		delete(tosend, p)
	}
	if fromSelf && pf.srcPushQuota > 0 {
		for _, p := range cands {
			tosend[p] = struct{}{}
		}
		return nil, pf.quotaTrim(root, mid, cands, budget, tosend)
	}
	if budget < len(cands) {
		var now time.Time
		if pf.selGrace > 0 {
			now = time.Now()
		}
		c := pf.pushChoice(mid, root, now)
		switch {
		case pf.selFewest && pf.selTolerance == PhaseSelectProportional && !fromSelf:
			order, known, slow := core.RankPush(h.peers, h.pushed, g, cands, c)
			cands = pf.tolerantOrder(order, known, slow, budget)
		case pf.selFewest:
			// The publisher's own messages and the argmax keep the ranking; the lower half draws.
			atSource := fromSelf || pf.selTolerance == PhaseSelectArgmax
			cands = core.ChoosePush(h.peers, h.pushed, g, cands, budget, atSource, c)
		default:
			sort.SliceStable(cands, func(i, j int) bool {
				ri, rj := phaseRank(mid, cands[i]), phaseRank(mid, cands[j])
				if ri != rj {
					return ri > rj
				}
				return cands[i] > cands[j]
			})
		}
	}
	n := min(budget, len(cands))
	for _, p := range cands[:n] {
		tosend[p] = struct{}{}
	}
	return cands[:n], cands[n:]
}

// --- Withholding injection ---

// WithIWantWithholding makes this node answer IWANTs never (delay 0 = silent) or whole
// after a fixed delay. Announcing is untouched -- IHAVE, IDONTWANT and mesh behaviour all
// continue -- which is the adversarial announce-without-serving case; a crashed peer is
// F3's job. The counter, when non-nil, records the ids withheld or delayed, so injected
// exposure is verified rather than assumed.
func WithIWantWithholding(delay time.Duration, withheld *atomic.Int64) Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok {
			return fmt.Errorf("iwant withholding requires a gossipsub router")
		}
		if delay < 0 {
			return fmt.Errorf("iwant withholding delay must be zero (silent) or positive (slow)")
		}
		gs.withholdServe = true
		gs.withholdDelay = delay
		gs.withheldServes = withheld
		return nil
	}
}

// --- Per-class admission loss ---
//
// Models gossipsub's real failure mode -- RPC content shed at queue admission, never
// retransmitted -- one control class at a time, so a run can attribute which mechanism a
// lost class breaks: a lost IDONTWANT costs duplicates, a lost IHAVE/IWANT or data message
// costs liveness. Entries are stripped at doSendRPC, after splitting and piggybacking.
// Decisions are stateless: hash(node seed, class, message id, destination's stable address)
// -- so the schedule is identical across configurations that share semantic units, and does
// not depend on a configuration's own call order (common random numbers).

const (
	lossClassNone uint8 = iota
	lossClassIHave
	lossClassIWant
	lossClassIDontWant
	lossClassData
)

// WithRPCClassLoss drops pct percent of the given class ("ihave", "iwant", "idontwant",
// "data") at queue admission. nodeSeed comes from the harness (fail seed x node index);
// dropped, when non-nil, counts stripped entries so exposure is verified.
func WithRPCClassLoss(class string, pct int, nodeSeed uint64, dropped *atomic.Int64) Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok {
			return fmt.Errorf("rpc class loss requires a gossipsub router")
		}
		if pct <= 0 || pct >= 100 {
			return fmt.Errorf("rpc class loss pct must be in (0,100)")
		}
		switch class {
		case "ihave":
			gs.lossClass = lossClassIHave
		case "iwant":
			gs.lossClass = lossClassIWant
		case "idontwant":
			gs.lossClass = lossClassIDontWant
		case "data":
			gs.lossClass = lossClassData
		default:
			return fmt.Errorf("unknown rpc loss class %q", class)
		}
		gs.lossPct = pct
		gs.lossSeed = nodeSeed
		gs.lossDropped = dropped
		return nil
	}
}

// stablePeerKey is a peer's run-stable key: its first known address, or its id.
func (gs *GossipSubRouter) stablePeerKey(p peer.ID) string {
	if addrs := gs.p.host.Peerstore().Addrs(p); len(addrs) > 0 {
		return addrs[0].String()
	}
	return string(p)
}

// lossDecide is the stateless drop decision for one semantic unit.
func (gs *GossipSubRouter) lossDecide(id string, dst peer.ID) bool {
	h := fnv.New64a()
	_, _ = h.Write([]byte{byte(gs.lossClass)})
	_, _ = h.Write([]byte(id))
	_, _ = h.Write([]byte(gs.stablePeerKey(dst)))
	x := h.Sum64() ^ gs.lossSeed
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return int(x%100) < gs.lossPct
}

// applyClassLoss returns the RPC to send toward dst with the configured class's losses
// applied, or nil when the loss consumed everything the RPC carried.
//
// The input RPC is never mutated. The forwarding path yields one *RPC to every push
// recipient, so an in-place filter would let the first peer's loss decision rewrite what
// every later peer receives -- per-peer loss would silently become correlated loss. When a
// drop occurs, a fresh RPC is built field by field rather than by copying the value:
// pb.RPC embeds protobuf state that carries a mutex.
func (gs *GossipSubRouter) applyClassLoss(rpc *RPC, dst peer.ID) *RPC {
	if gs.lossClass == lossClassNone || rpc == nil {
		return rpc
	}
	if gs.lossClass == lossClassIWant && gs.v05PromiseAtWrite() {
		return rpc // under v0.5's E the asks' loss is drawn at the writer (specv05.go, askLoss)
	}
	dropped := int64(0)
	out := rpc
	switch gs.lossClass {
	case lossClassData:
		if len(rpc.Publish) == 0 {
			return rpc
		}
		kept := make([]*pb.Message, 0, len(rpc.Publish))
		for _, m := range rpc.Publish {
			if gs.lossDecide(gs.p.idGen.ID(&Message{Message: m}), dst) {
				dropped++
				continue
			}
			kept = append(kept, m)
		}
		if dropped == 0 {
			return rpc
		}
		out = &RPC{from: rpc.from}
		out.Subscriptions = rpc.Subscriptions
		out.Control = rpc.Control
		out.Publish = kept
	case lossClassIHave, lossClassIWant, lossClassIDontWant:
		ctl := rpc.GetControl()
		if ctl == nil {
			return rpc
		}
		var lost []string
		filterIDs := func(ids []string) ([]string, int64) {
			var n int64
			kept := make([]string, 0, len(ids))
			for _, id := range ids {
				if gs.lossDecide(id, dst) {
					n++
					lost = append(lost, id)
					continue
				}
				kept = append(kept, id)
			}
			return kept, n
		}
		newCtl := &pb.ControlMessage{
			Ihave:     ctl.Ihave,
			Iwant:     ctl.Iwant,
			Graft:     ctl.Graft,
			Prune:     ctl.Prune,
			Idontwant: ctl.Idontwant,
		}
		switch gs.lossClass {
		case lossClassIHave:
			entries := make([]*pb.ControlIHave, len(ctl.Ihave))
			for i, e := range ctl.Ihave {
				kept, n := filterIDs(e.MessageIDs)
				if n == 0 {
					entries[i] = e
					continue
				}
				dropped += n
				entries[i] = &pb.ControlIHave{TopicID: e.TopicID, MessageIDs: kept}
			}
			newCtl.Ihave = entries
		case lossClassIWant:
			entries := make([]*pb.ControlIWant, len(ctl.Iwant))
			for i, e := range ctl.Iwant {
				kept, n := filterIDs(e.MessageIDs)
				if n == 0 {
					entries[i] = e
					continue
				}
				dropped += n
				entries[i] = &pb.ControlIWant{MessageIDs: kept}
			}
			newCtl.Iwant = entries
		case lossClassIDontWant:
			entries := make([]*pb.ControlIDontWant, len(ctl.Idontwant))
			for i, e := range ctl.Idontwant {
				kept, n := filterIDs(e.MessageIDs)
				if n == 0 {
					entries[i] = e
					continue
				}
				dropped += n
				entries[i] = &pb.ControlIDontWant{MessageIDs: kept}
			}
			newCtl.Idontwant = entries
		}
		if dropped == 0 {
			return rpc
		}
		out = &RPC{from: rpc.from}
		out.Subscriptions = rpc.Subscriptions
		out.Publish = rpc.Publish
		out.Control = newCtl
		if gs.lossClass == lossClassIWant && gs.pullOn() {
			// An ask lost on the wire was written: a chunk's promise opens and its queued record closes,
			// as at the writer under E (here E is off: under it the loss is drawn there). Router
			// goroutine: only doSendRPC draws this class.
			// Never queued: popped and written at once, after the asks were recorded.
			gs.promisesFromWrite(dst, lost, nextAskSeq(), time.Now())
		}
	}
	if gs.lossDropped != nil {
		gs.lossDropped.Add(dropped)
	}
	if len(out.Publish) == 0 && out.GetControl() != nil {
		ctl := out.GetControl()
		empty := true
		for _, e := range ctl.Ihave {
			empty = empty && len(e.MessageIDs) == 0
		}
		for _, e := range ctl.Iwant {
			empty = empty && len(e.MessageIDs) == 0
		}
		for _, e := range ctl.Idontwant {
			empty = empty && len(e.MessageIDs) == 0
		}
		if empty && len(ctl.Graft) == 0 && len(ctl.Prune) == 0 {
			return nil
		}
	}
	return out
}

// phaseRank scores one (message, peer) pair. FNV-1a avalanched with the splitmix64
// finalizer: FNV's low bits alone carry almost no mixing, so an unfinalized hash reduced or
// compared over small candidate sets correlates across permuted inputs.
func phaseRank(mid string, p peer.ID) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(mid))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(p))
	x := h.Sum64()
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return x
}

// --- IWANT discipline (independent of phase forwarding) ---
//
// Stock gossipsub re-requests a message id whenever another peer's IHAVE arrives while an
// earlier IWANT is still in flight: handleIHave only checks seenMessage, which is false until
// the payload lands. On announce-heavy paths that is the dominant duplicate source (measured
// 2.2x received bytes with pull only). The discipline: at most one outstanding IWANT per
// message id within a window; on expiry the next IHAVE may re-request, so a lost reply costs
// one window rather than liveness.

// WithIWantDiscipline enables one-outstanding-IWANT-per-id with the given expiry window.
// The window must cover a round trip plus the peer's transmission of what was asked -- the
// same sizing rule as any request timeout; sub-RTT values reintroduce the duplicates.
func WithIWantDiscipline(window time.Duration) Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok {
			return fmt.Errorf("iwant discipline requires a gossipsub router")
		}
		if window <= 0 {
			return fmt.Errorf("iwant discipline window must be positive")
		}
		gs.iwantWindow = window
		gs.iwantAsked = make(map[string]time.Time)
		return nil
	}
}

// WithAskUplink scales the ask window by this node's own uplink (seacast.md §4.13): IWANT_WINDOW
// times max(1, askUplink / uplink), so it is the discipline's window at askUplink and above and
// stretches in proportion below it (five times at a fifth). Every reader of the window is scaled; the
// promise is WithIHaveCommitmentPark's own figure and does not change. Requires WithIWantDiscipline
// first; uplink and askUplink in bits per second.
func WithAskUplink(askUplink, uplink int64) Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok || gs.iwantWindow <= 0 {
			return fmt.Errorf("ask scaling requires WithIWantDiscipline first")
		}
		if askUplink <= 0 || uplink <= 0 {
			return fmt.Errorf("ask scaling needs positive uplinks")
		}
		if scale := float64(askUplink) / float64(uplink); scale > 1 {
			gs.iwantWindow = time.Duration(float64(gs.iwantWindow) * scale)
		}
		return nil
	}
}

// AskWindow is the node's ask window (IWANT_WINDOW, after any ask scaling); 0 without the
// discipline. Set at construction, so safe to read from any goroutine afterwards.
func (p *PubSub) AskWindow() time.Duration {
	if gs, ok := p.rt.(*GossipSubRouter); ok {
		return gs.iwantWindow
	}
	return 0
}

// WithSendDelay stalls this node's outbound writer per RPC (real queue pressure).
// Applied to selected nodes by the harness; their rpcQueue
// then overflows naturally through doDropRPC as offered load outpaces the slowed drain.
// Test-only; a production node must never set it.
func WithSendDelay(d time.Duration) Option {
	return func(ps *PubSub) error {
		if d <= 0 {
			return fmt.Errorf("send delay must be positive")
		}
		ps.sendDelay.Store(int64(d))
		return nil
	}
}

// iwantDelivered clears an id's ask state on its arrival.
func (gs *GossipSubRouter) iwantDelivered(mid string) {
	if root, g, i, ok := gs.pullChunk(mid); ok {
		gs.pullClose(root, g, i) // a chunk: close_promises in the core's tables (seacast_pull.go)
		return
	}
	// Delivery keeps the promise: the asked peer served within the window, so the later
	// expiry of the iwantAsked entry (which delivery deliberately does not clear) must not
	// count as a break. Without this line every honest serve would eventually park its
	// server, because iwantAsked entries age out rather than clearing on arrival.
	if gs.iwantPromised != nil {
		delete(gs.iwantPromised, mid)
	}
	gs.seacastAsksClosed(mid) // delivered: no ask of it is open any more
	gs.clearOffers(mid)
}

// iwantAllowed reports whether an IWANT for mid may be issued now, recording it if so.
// Runs on pubsub's goroutine, like every router map.
func (gs *GossipSubRouter) iwantAllowed(mid string, from peer.ID) bool {
	if gs.iwantAsked == nil {
		return true
	}
	if _, g, i, ok := gs.pullChunk(mid); ok {
		return gs.pullMayAsk(from, g, i) // a chunk: may_ask in the core (seacast_pull.go)
	}
	if at, ok := gs.iwantAsked[mid]; ok && time.Since(at) < gs.iwantWindow {
		return false
	}
	gs.iwantAsked[mid] = time.Now()
	if gs.offerTable != nil {
		if gs.offerTried[mid] == nil {
			gs.offerTried[mid] = make(map[peer.ID]time.Time)
		}
		gs.offerTried[mid][from] = time.Now()
		m := mid
		time.AfterFunc(gs.iwantWindow+5*time.Millisecond, func() {
			select {
			case gs.p.eval <- func() { gs.retryOfferedPull(m) }:
			case <-gs.p.ctx.Done():
			}
		})
	}
	if gs.iwantPromised != nil && !gs.v05PromiseAtWrite() { // under v0.5's E the writer opens it
		// Two-timer design: the promise is per (id, peer) and survives the discipline's
		// move-on, so a fast window re-asks elsewhere while the slow/withholding peer's
		// own deadline (parkPromise, which may exceed the window) still runs to judgment.
		// Delivery of the id from anyone absolves every open promise for it.
		if gs.iwantPromised[mid] == nil {
			gs.iwantPromised[mid] = make(map[peer.ID]time.Time)
		}
		// A re-ask of a peer whose promise for this id is still open keeps the earlier
		// deadline: overwriting it let every retry postpone the break, so a silent sole
		// announcer asked every window was never judged.
		if _, open := gs.iwantPromised[mid][from]; open {
			return true
		}
		askedAt := gs.iwantAsked[mid]
		gs.iwantPromised[mid][from] = askedAt
		m, f := mid, from
		time.AfterFunc(gs.parkPromise, func() {
			select {
			case gs.p.eval <- func() { gs.promiseDue(m, f, askedAt, askedAt) }:
			case <-gs.p.ctx.Done():
			}
		})
	}
	return true
}

// promiseDue runs on the router goroutine at this peer's promise deadline. A promise already
// cleared (delivery of the id, from anyone) or re-made (a newer ask to the same peer) is not
// charged; an open one breaks: the asked peer is charged and possibly parked. The discipline
// slot is refunded only when this ask is still the current one (a shorter window has usually
// moved on already — then there is nothing left to refund). promisedAt identifies the promise;
// decidedAt is when the ask was decided, which differs from it when the promise starts at the
// write (v0.5's E).
func (gs *GossipSubRouter) promiseDue(mid string, from peer.ID, promisedAt, decidedAt time.Time) {
	if gs.iwantPromised == nil {
		return
	}
	peers := gs.iwantPromised[mid]
	if at, open := peers[from]; !open || !at.Equal(promisedAt) {
		return
	}
	gs.notePromiseBreak(mid, from)
	// The ask's record outlives a broken promise until its window has run: the asker moves on only by
	// the window (seacast.md §4.13, seacast-lite.md §6.1: promise_due leaves asked_at alone). With the
	// window shorter than the promise, as at every uplink without ask scaling, it has always run by
	// now.
	if at, ok := gs.iwantAsked[mid]; ok && at.Equal(decidedAt) && time.Since(at) >= gs.iwantWindow {
		delete(gs.iwantAsked, mid)
	}
}

// notePromiseBreak charges peer p for an unserved ask of mid (its promise deadline elapsed
// undelivered), parking it once its breaks reach parkK. No-op when enforcement is off.
func (gs *GossipSubRouter) notePromiseBreak(mid string, p peer.ID) {
	gs.seacastAskClosed(p, mid) // the ask closed unanswered: p may be asked again (seacast_ask.go)
	if gs.iwantPromised == nil {
		return
	}
	if g, ok := gs.pullParkGroup(mid); ok {
		// A chunk's or a direct ask's group: parked in the core's park, which a chunk's asks read.
		if _, _, i, chunk := gs.pullChunk(mid); chunk {
			gs.pull.promises.CloseFrom(g, i, p)
		}
		gs.pullPark(p, g)
		return
	}
	if peers := gs.iwantPromised[mid]; peers != nil {
		delete(peers, p)
		if len(peers) == 0 {
			delete(gs.iwantPromised, mid)
		}
	}
	if gs.parkBreaksCtr != nil {
		gs.parkBreaksCtr.Add(1)
	}
	if gs.parkGroups != nil {
		gs.noteGroupBreak(mid, p)
		return
	}
	gs.parkBreaks[p]++
	if gs.parkBreaks[p] >= gs.parkK {
		if _, already := gs.parkedUntil[p]; !already {
			if gs.parksCtr != nil {
				gs.parksCtr.Add(1)
			}
		}
		gs.parkedUntil[p] = time.Now().Add(gs.parkTTL)
	}
}

// peerParked reports whether a peer's announcements are currently ineligible for pull
// selection under commitment enforcement. Expired parks clear lazily.
func (gs *GossipSubRouter) peerParked(p peer.ID) bool {
	if gs.parkedUntil == nil {
		return false
	}
	until, ok := gs.parkedUntil[p]
	if !ok {
		return false
	}
	if time.Now().After(until) {
		delete(gs.parkedUntil, p)
		return false
	}
	return true
}

// clearIWantAsked drops expired outstanding-request records; called from the heartbeat.
func (gs *GossipSubRouter) clearIWantAsked() {
	if gs.iwantAsked == nil {
		return
	}
	for mid, at := range gs.iwantAsked {
		if time.Since(at) >= gs.iwantWindow {
			// Two-timer enforcement: expiry here just moves the discipline on; any open
			// promise keeps running to its own deadline (promiseDue charges it there).
			delete(gs.iwantAsked, mid)
		}
	}
	gs.sweepOffers()
	gs.pullSweep()
	gs.sweepGroupParks()
}

// WithIHaveCommitmentPark treats an IHAVE as a binding offer under the IWANT discipline:
// each ask opens a per-(id, peer) promise with its own deadline; a promise still unserved at
// the deadline is a break, and a peer reaching k breaks is parked — its announcements
// ineligible for pull selection — for ttl. Delivery of the id (from anyone) absolves every
// open promise for it. The promise deadline is independent of the discipline window (v3,
// two-timer): a short window moves on and re-asks elsewhere while slow peers still run to
// judgment. Requires WithIWantDiscipline first. Park gates pull selection only; pushes,
// scoring and holder evidence are untouched.
func WithIHaveCommitmentPark(k int, promise, ttl time.Duration, breaks, parks *atomic.Int64) Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok {
			return fmt.Errorf("commitment park requires a gossipsub router")
		}
		if gs.iwantAsked == nil {
			return fmt.Errorf("commitment park requires WithIWantDiscipline first")
		}
		if k < 1 || ttl <= 0 || promise <= 0 {
			return fmt.Errorf("commitment park needs k >= 1, a positive ttl, and a positive promise")
		}
		gs.parkPromise = promise
		gs.iwantPromised = make(map[string]map[peer.ID]time.Time)
		gs.parkBreaks = make(map[peer.ID]int)
		gs.parkedUntil = make(map[peer.ID]time.Time)
		gs.parkK = k
		gs.parkTTL = ttl
		gs.parkBreaksCtr = breaks
		gs.parksCtr = parks
		return nil
	}
}

// WithOfferTable makes the IWANT discipline remember every announcement instead of discarding
// the ones it cannot act on: offers for unseen ids go into a standing table (announcer, time,
// topic), every granted ask arms a retry timer one window later, and the retry re-asks the best
// untried announcer from the table — cycling to the least-recently-tried when all are spent, so
// a slow honest server gets a second lap. Parks and stop_pull filter at pick time;
// delivery prunes the id everywhere. This is state, not a queue: announcements are facts about
// who holds what, and in a protocol whose relays announce once, forgetting them is what turned
// the discipline into the withholding adversary's capture lever. Requires
// WithIWantDiscipline.
func WithOfferTable(retries *atomic.Int64) Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok {
			return fmt.Errorf("offer table requires a gossipsub router")
		}
		if gs.iwantAsked == nil {
			return fmt.Errorf("offer table requires WithIWantDiscipline first")
		}
		gs.offerTable = make(map[string]map[peer.ID]time.Time)
		gs.offerTried = make(map[string]map[peer.ID]time.Time)
		gs.offerTopic = make(map[string]string)
		gs.offerByPeer = make(map[peer.ID]int)
		gs.offerRetryCtr = retries
		return nil
	}
}

// Per-peer and per-id bounds on the offer table. A peer beyond its budget stops being
// recorded (in production that refusal is the scoreable anomaly — ~70 peers x 256 entries
// is ~1 MB worst case); a single id keeps at most offerPerID announcers. Entries also age
// out (offerTTL, swept from the heartbeat), so fabricated never-delivered ids cannot
// accumulate: without the TTL a junk flood would eventually trip a global guard and
// silently blind the table for honest offers — a policy attack, not a memory one.
const (
	offerPerPeer = 256
	offerPerID   = 16
	offerTTL     = 30 * time.Second
)

// recordOffer notes that peer p announced mid; a fact, kept until delivery or expiry.
func (gs *GossipSubRouter) recordOffer(mid, topic string, p peer.ID) {
	if gs.offerTable == nil || gs.haveSeen(mid) {
		return
	}
	if _, g, i, ok := gs.pullChunk(mid); ok {
		gs.pullAddOffer(p, g, i) // a chunk: add_offer in the core (seacast_pull.go)
		return
	}
	if gs.offerByPeer[p] >= offerPerPeer {
		return
	}
	peers := gs.offerTable[mid]
	if peers == nil {
		peers = make(map[peer.ID]time.Time)
		gs.offerTable[mid] = peers
		gs.offerTopic[mid] = topic
	} else if len(peers) >= offerPerID {
		if _, known := peers[p]; !known {
			return
		}
	}
	if _, known := peers[p]; !known {
		gs.offerByPeer[p]++
	}
	peers[p] = time.Now()
}

// clearOffers forgets a delivered id.
func (gs *GossipSubRouter) clearOffers(mid string) {
	if gs.offerTable == nil {
		return
	}
	for p := range gs.offerTable[mid] {
		if gs.offerByPeer[p] > 0 {
			gs.offerByPeer[p]--
		}
	}
	delete(gs.offerTable, mid)
	delete(gs.offerTried, mid)
	delete(gs.offerTopic, mid)
}

// sweepOffers ages out stale offers; called from the heartbeat sweep.
func (gs *GossipSubRouter) sweepOffers() {
	if gs.offerTable == nil {
		return
	}
	var retry []string
	for mid, peers := range gs.offerTable {
		for p, at := range peers {
			if time.Since(at) >= offerTTL {
				delete(peers, p)
				if gs.offerByPeer[p] > 0 {
					gs.offerByPeer[p]--
				}
				if gs.seacastOfferDropped(p, mid) {
					retry = append(retry, mid) // its ask went with the offer (F13): ask another announcer
				}
			}
		}
		if len(peers) == 0 {
			gs.clearOffers(mid)
		}
	}
	for _, mid := range retry {
		if !gs.haveSeen(mid) {
			gs.retryOfferedPull(mid)
		}
	}
}

// retryOfferedPull runs on the router goroutine one window after an ask was granted. If the id
// is still missing and no ask is in flight, it re-asks the best remembered announcer:
// untried first, then least-recently tried, in deterministic peer order.
// It returns the peer asked, and whether an ask went out (seacastHeartbeatLimits reads the latter).
func (gs *GossipSubRouter) retryOfferedPull(mid string) (pick peer.ID, sent bool) {
	if gs.offerTable == nil {
		return "", false
	}
	if _, g, i, ok := gs.pullChunk(mid); ok {
		return gs.pullRetry(g, i) // a chunk: retry_offer in the core (seacast_pull.go)
	}
	if gs.haveSeen(mid) {
		gs.clearOffers(mid)
		return "", false
	}
	if gs.expiredID(mid) {
		return "", false // the group passed its lifetime (v0.5's D′)
	}
	if gs.seacastStopPulled(mid) {
		return "", false // retry_offer returns at stop_pull, the probed cell's included (seacast_complete.go)
	}
	if at, ok := gs.iwantAsked[mid]; ok && time.Since(at) < gs.iwantWindow {
		return "", false // an ask is in flight; its own retry timer will fire
	}
	offers := gs.offerTable[mid]
	if len(offers) == 0 {
		return "", false
	}
	cands := make([]peer.ID, 0, len(offers))
	anyOutstanding := false
	for p := range offers {
		if gs.peerParkedFor(p, mid) || gs.seacastTiedTo(p, mid) {
			continue
		}
		if !gs.requestAllowed(p, gs.offerTopic[mid], mid) {
			continue
		}
		if gs.seacastAskOutstanding(p, mid) {
			anyOutstanding = true
			continue
		}
		cands = append(cands, p)
	}
	if len(cands) == 0 {
		if anyOutstanding {
			gs.seacastRetryLater(mid)
		}
		return "", false
	}
	sort.Slice(cands, func(i, j int) bool { return cands[i] < cands[j] })
	pick, found := gs.pickAnnouncer(mid, cands)
	if !found || !gs.iwantAllowed(mid, pick) {
		return "", false
	}
	if gs.offerRetryCtr != nil {
		gs.offerRetryCtr.Add(1)
	}
	if gs.seacast == nil {
		gs.iasked[pick]++ // with the path, only asks in reply to offers count (seacast.md §4.3)
	}
	gs.gossipTracer.AddPromise(pick, []string{mid})
	gs.noteIWantSent(pick, []string{mid})
	gs.sendRPC(pick, rpcWithControl(nil, nil, []*pb.ControlIWant{{MessageIDs: []string{mid}}}, nil, nil, nil), false)
	return pick, true
}

// --- Application request gate ---
//
// A receiver in announce-and-pull decides whether to ask, so it can decline what it cannot yet
// authenticate -- for segmented payloads bound to a consensus commitment, that means declining an
// announcement whose authorizing block has not arrived. The decision point already exists and is
// already policy-driven; what is missing is a place for the *application* to answer, since only it
// knows what a message id claims and whether that claim is currently satisfiable.
//
// Deliberately opaque: the gate sees the peer, the topic and the raw message id and nothing else.
// Every segment-specific notion -- how an id encodes its structural claim, which blocks are
// installed, what aggregate per-claim caps apply -- stays in the application, so the router carries
// one generic predicate rather than a protocol.
