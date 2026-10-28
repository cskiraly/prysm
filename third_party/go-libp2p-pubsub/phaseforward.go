package pubsub

import (
	"fmt"
	"hash/fnv"
	"math/rand"
	"sort"
	"sync/atomic"
	"time"

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

	// Source policy: what the *publisher* of a message does with the mesh peers it does not
	// push to. The default -- announce to all of them immediately -- turns the source into
	// the pull target of first resort: at the moment of publishing it is the only holder, so
	// every announced peer IWANTs from it at once, and its transmit balloons (measured 2.7x
	// variant A's publisher at degree 70). The three alternatives shape when and to whom the
	// source announces, so pulls land on first-hop holders instead.
	srcMode   PhaseSourceMode
	srcDelay  time.Duration
	srcDegree int
	// srcPushDegree > 0 replaces the push degree for the publisher's own messages; the peers
	// not pushed are announced as usual (the source's second copy of a
	// frame races only its own first copy, since nobody else holds the frame yet).
	srcPushDegree int
	// srcPushQuota > 0 caps the frames of one group the publisher pushes to each mesh peer; its
	// other frames are offered, each to the peers it has pushed and offered least, so each mesh
	// peer pulls a share as it pulls from a relay (WithPhaseSourcePushQuota). srcLoad counts, per
	// recent group, what each peer was pushed and offered.
	srcPushQuota int
	srcLoad      map[string]map[peer.ID]*srcPeerLoad
	srcLoadRoots []string
	// srcOfferPace > 0 holds the publisher's offers of its own frames until the frames it has named,
	// pushed or offered, are below srcOfferNeed times the time since publish over the pace, so its
	// HAVE, which names them, grows as a relay's does (WithPhaseSourceOfferPace).
	srcOfferPace  time.Duration
	srcOfferNeed  int
	srcOffers     map[string]*srcOfferQueue
	srcOfferRoots []string

	// Meshless mode dissolves the topic mesh's role in forwarding: push candidates are all
	// connected gossipsub-capable topic peers rather than the mesh, ranked per message as
	// usual, and the peers not pushed are announced up to announceWidth (0 = all of them).
	// The mesh still exists for gossipsub's own maintenance; it just stops deciding who gets
	// eager copies. Known v1 limitation: IDONTWANT is only emitted to mesh peers, so the
	// known-holders decay undercounts and the push degree stays near r.
	meshless      bool
	announceWidth int

	// Selection policy for the pushed subset (design-space §13 nomenclature). Default is
	// sel=rank: the per-(message, peer) hash, blind to need with no floor on a receiver's
	// per-segment push in-degree. sel=rr rotates a deterministic peer order by a per-router
	// message counter instead: each mesh peer receives a fair, deterministic share of this
	// sender's pushes across messages, at the price of predictability.
	selRR bool
	rrCtr atomic.Uint64
	// sel=fewest pushes to the mesh peers known to hold the fewest frames of the message's
	// group -- what they announced in HAVE bitmaps or we pushed them -- ties by the rank hash.
	selFewest bool
	// selBacklog, when positive, ranks a peer with more than this many bytes still queued from us
	// after every peer below it: a straggler looks furthest behind because it drains slowly.
	selBacklog int64
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
	// selKeys caches each peer's run-stable sort key for the seeded draw.
	selKeys map[peer.ID]string

	// Fixed budget switches the phase transition off and keeps only the split: push to at
	// most degree of the mesh peers not known to hold the message and announce to the rest,
	// without the known-holder count shrinking the push. Isolates the announce-instead-of-
	// push half of phase forwarding from the estimator half.
	fixedBudget bool

	// srcConnections widens the publisher's own push candidates from the mesh to every
	// connected topic peer, so a fast source spreads distinct frames over all its first-hop
	// uplinks; the announce stays mesh-scoped.
	srcConnections bool
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

// WithPhaseFixedBudget keeps phase forwarding's split (push r, announce the rest) but drops
// the IDONTWANT-driven decay of the push budget. Requires WithPhaseForwarding.
func WithPhaseFixedBudget() Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok {
			return fmt.Errorf("fixed phase budget requires a gossipsub router")
		}
		if gs.phaseForward == nil {
			return fmt.Errorf("fixed phase budget requires WithPhaseForwarding first")
		}
		gs.phaseForward.fixedBudget = true
		return nil
	}
}

// WithPhaseSelectRR switches phase forwarding's push-target selection from the per-message
// hash rank (sel=rank) to sender-side round-robin (sel=rr). Requires WithPhaseForwarding.
func WithPhaseSelectRR() Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok {
			return fmt.Errorf("phase selection requires a gossipsub router")
		}
		if gs.phaseForward == nil {
			return fmt.Errorf("phase selection requires WithPhaseForwarding first")
		}
		gs.phaseForward.selRR = true
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

// WithPhaseSelectFewestBacklog is sel=fewest with a backlog guard: a mesh peer with more than
// maxBytes still queued from us ranks after every peer at or below it, fewest-first inside each
// class. ELp pushes to the peer that can forward at once; a straggler looks furthest behind
// because it drains slowly, and every pusher would otherwise feed it. Requires
// WithPhaseSelectFewest.
func WithPhaseSelectFewestBacklog(maxBytes int64) Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok {
			return fmt.Errorf("phase selection requires a gossipsub router")
		}
		if gs.phaseForward == nil || !gs.phaseForward.selFewest {
			return fmt.Errorf("the backlog guard requires WithPhaseSelectFewest first")
		}
		if maxBytes <= 0 {
			return fmt.Errorf("the backlog guard needs a positive byte threshold")
		}
		gs.phaseForward.selBacklog = maxBytes
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
// count. Both draw only among the peers the backlog and proof guards leave in the first class, in
// the degree the phase grants, and fill any budget the draw leaves from the fewest order; the
// seeded source of WithDeterministicRand is used when set. The publisher's own messages keep the
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

// tolerantOrder reorders cands, in sel=fewest order with the slow class last, so its first budget
// entries are the tolerant draw; whatever the draw leaves keeps its fewest order behind them, so
// a short draw fills from there and the announce order is the fewest order. Event loop.
func (pf *phaseForwarding) tolerantOrder(gs *GossipSubRouter, cands []peer.ID, known map[peer.ID]int, slow map[peer.ID]bool, budget int) []peer.ID {
	fast := 0
	for fast < len(cands) && !slow[cands[fast]] {
		fast++
	}
	pool := cands[:fast]
	if pf.selTolerance == PhaseSelectLowerHalf && fast > 0 {
		median := known[cands[(fast-1)/2]]
		e := 0
		for e < fast && known[cands[e]] <= median {
			e++
		}
		pool = cands[:e]
	}
	if len(pool) == 0 || budget <= 0 {
		return cands
	}
	pool = append([]peer.ID(nil), pool...)
	if gs.rng != nil && gs.p != nil && gs.p.host != nil {
		// A function of the seed: peer ids are fresh every run, the address is not.
		if pf.selKeys == nil {
			pf.selKeys = make(map[peer.ID]string)
		}
		for _, p := range pool {
			if _, ok := pf.selKeys[p]; !ok {
				pf.selKeys[p] = gs.stablePeerKey(p)
			}
		}
		sort.Slice(pool, func(i, j int) bool { return pf.selKeys[pool[i]] < pf.selKeys[pool[j]] })
	}
	intn := rand.Intn
	if gs.rng != nil {
		intn = gs.rng.Intn
	}
	k := min(budget, len(pool))
	chosen := make(map[peer.ID]bool, k)
	out := make([]peer.ID, 0, len(cands))
	switch pf.selTolerance {
	case PhaseSelectLowerHalf:
		for i := range k {
			j := i + intn(len(pool)-i)
			pool[i], pool[j] = pool[j], pool[i]
			chosen[pool[i]] = true
			out = append(out, pool[i])
		}
	case PhaseSelectProportional:
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
			r, j := intn(total), 0
			for r >= w[j] {
				r -= w[j]
				j++
			}
			chosen[pool[j]] = true
			out = append(out, pool[j])
			total -= w[j]
			w[j] = 0
		}
	}
	for _, p := range cands {
		if !chosen[p] {
			out = append(out, p)
		}
	}
	return out
}

// unproven reports whether the proof guard demotes p for root at now. Event loop.
func (pf *phaseForwarding) unproven(gs *GossipSubRouter, p peer.ID, root string, now time.Time) bool {
	if pf.selGrace <= 0 {
		return false
	}
	pe := gs.have.pushed[p][root]
	if pe == nil || pe.bits.Count() < pf.degree || now.Sub(pe.first) <= pf.selGrace {
		return false
	}
	if gs.have.peers[p][root] != nil {
		return false // a HAVE, a claim or a hose ack for the group is the proof
	}
	if !pe.demoted {
		pe.demoted = true
		if pf.selDemoted != nil {
			pf.selDemoted(p, root)
		}
	}
	return true
}

// backlogged reports whether more than the guard's threshold is queued to p from us.
func (pf *phaseForwarding) backlogged(gs *GossipSubRouter, p peer.ID) bool {
	if pf.selBacklog <= 0 || gs.p == nil {
		return false
	}
	q, ok := gs.p.peers[p]
	return ok && q.QueuedBytes() > pf.selBacklog
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

// knownFrames is how many frames of root p is known to hold or to have in flight from us: the
// union of its HAVE bitmap and what we pushed it. Called on the event loop.
func (h *haveState) knownFrames(p peer.ID, root string) int {
	var u haveBits
	if pe := h.peers[p]; pe != nil && pe[root] != nil {
		u.Or(pe[root].bits.b)
	}
	if pe := h.pushed[p]; pe != nil && pe[root] != nil {
		u.Or(pe[root].bits.b)
	}
	return u.Count()
}

// WithPhaseMeshless widens phase forwarding's candidate set from the topic mesh to every
// connected gossipsub-capable topic peer, announcing the non-pushed candidates up to
// announceWidth per message (0 announces all of them). Requires WithPhaseForwarding.
func WithPhaseMeshless(announceWidth int) Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok {
			return fmt.Errorf("meshless phase requires a gossipsub router")
		}
		if gs.phaseForward == nil {
			return fmt.Errorf("meshless phase requires WithPhaseForwarding first")
		}
		if announceWidth < 0 {
			return fmt.Errorf("announce width must be non-negative")
		}
		gs.phaseForward.meshless = true
		gs.phaseForward.announceWidth = announceWidth
		return nil
	}
}

// PhaseSourceMode selects the source-side staggering policy.
type PhaseSourceMode int

const (
	// PhaseSourceImmediate announces every non-pushed mesh peer at publish time. The
	// default, and the mob-the-source baseline.
	PhaseSourceImmediate PhaseSourceMode = iota
	// PhaseSourceDelay pushes now and announces after srcDelay, rechecking IDONTWANT state
	// at fire time so peers that got the message meanwhile are not invited to pull it.
	PhaseSourceDelay
	// PhaseSourceRotate announces each message to only the next srcDegree peers in the same
	// per-message ranking that chose the push targets. Different messages announce to
	// different peers, so each mesh peer initially pulls a disjoint stripe and the rest
	// arrives by cross-pull from relays' announces.
	PhaseSourceRotate
	// PhaseSourceWidePush pushes each message to srcDegree peers and announces to nobody;
	// non-pushed peers learn of the message from relays. Trades the emergent pull mob for a
	// chosen, bounded upload.
	PhaseSourceWidePush
)

// WithPhaseSourcePushDegree sets the push degree the publisher uses for its own messages in
// place of the forwarding degree; the mesh peers not pushed are announced as usual. Ignored
// under PhaseSourceWidePush, which has its own width. Must follow WithPhaseForwarding.
func WithPhaseSourcePushDegree(r int) Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok {
			return fmt.Errorf("phase source push degree requires a gossipsub router")
		}
		if gs.phaseForward == nil {
			return fmt.Errorf("phase source push degree requires WithPhaseForwarding first")
		}
		if r <= 0 {
			return fmt.Errorf("phase source push degree must be positive")
		}
		gs.phaseForward.srcPushDegree = r
		return nil
	}
}

// WithPhaseSourcePushQuota caps what the publisher pushes of one group to each mesh peer at quota
// frames; a frame no peer under the quota takes is offered instead, to the peers pushed and offered
// least (all non-pushed mesh peers, or srcDegree of them under PhaseSourceRotate). Must follow
// WithPhaseForwarding and WithHaveBitmaps.
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
// (pushed plus offered, then the rank hash), pushes to the first budget under the quota and not
// held, removes the rest from tosend and returns the peers to offer the frame to, and how many held
// peers a push passed over.
func (pf *phaseForwarding) quotaTrim(root, mid string, cands []peer.ID, budget int, tosend map[peer.ID]struct{}, held map[peer.ID]bool) ([]peer.ID, int) {
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
	pushed, withheld := 0, 0
	var offer []peer.ID
	for _, p := range cands {
		if pushed < budget && get(p).pushed < pf.srcPushQuota {
			if !held[p] {
				get(p).pushed++
				pushed++
				continue
			}
			withheld++
		}
		delete(tosend, p)
		offer = append(offer, p)
	}
	if pf.srcMode == PhaseSourceRotate && len(offer) > pf.srcDegree {
		offer = offer[:pf.srcDegree]
	}
	for _, p := range offer {
		get(p).offered++
	}
	return offer, withheld
}

// WithPhaseSourceOfferPace holds the publisher's offers of its own frames so that the frames it has
// named reach need only pace after publish, as a relay's holding does: an offer of a frame not yet
// named waits while the named count is at the budget, need times the time since publish over pace.
// Pushes are not held. Must follow WithPhaseForwarding and WithHaveBitmaps; meant with the paced
// origin HAVE and WithPhaseSourcePushQuota.
func WithPhaseSourceOfferPace(pace time.Duration, need int) Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok {
			return fmt.Errorf("phase source offer pace requires a gossipsub router")
		}
		if gs.phaseForward == nil || gs.have == nil {
			return fmt.Errorf("phase source offer pace requires WithPhaseForwarding and WithHaveBitmaps first")
		}
		if pace <= 0 || need < 1 {
			return fmt.Errorf("phase source offer pace requires pace > 0 and need >= 1")
		}
		pf := gs.phaseForward
		pf.srcOfferPace, pf.srcOfferNeed = pace, need
		pf.srcOffers = make(map[string]*srcOfferQueue)
		return nil
	}
}

// srcOfferQueue is one group's held offers: its publish time, the offers in arrival order, the
// frames released so far, and whether a pump is scheduled.
type srcOfferQueue struct {
	start    time.Time
	items    []offerItem
	released haveBits
	armed    bool
}

// originOfferPaced takes over an offer of mid to p when mid is a frame of a group we publish and
// offers are paced, and reports whether it did. Router goroutine, from the announce path.
func (gs *GossipSubRouter) originOfferPaced(p peer.ID, topic, mid string) bool {
	pf := gs.phaseForward
	if pf == nil || pf.srcOfferPace == 0 || gs.have == nil || gs.have.originPaced == nil {
		return false
	}
	root, _, ok := gs.have.index(mid)
	if !ok || !gs.have.originPaced.owns(root) {
		return false
	}
	q := pf.srcOffers[root]
	if q == nil {
		q = &srcOfferQueue{start: time.Now()}
		pf.srcOffers[root] = q
		pf.srcOfferRoots = append(pf.srcOfferRoots, root)
		if len(pf.srcOfferRoots) > srcLoadMaxRoots {
			delete(pf.srcOffers, pf.srcOfferRoots[0])
			pf.srcOfferRoots = pf.srcOfferRoots[1:]
		}
	}
	q.items = append(q.items, offerItem{p: p, topic: topic, mid: mid})
	gs.originOfferPump(root)
	return true
}

// originOfferPump releases root's held offers while the named frames, what our writers popped and
// what was released, are under the budget; an offer of a frame already named goes at once. It
// reschedules itself while offers wait. Router goroutine.
func (gs *GossipSubRouter) originOfferPump(root string) {
	pf := gs.phaseForward
	q := pf.srcOffers[root]
	if q == nil {
		return
	}
	budget := 1 << 30
	if elapsed := time.Since(q.start); elapsed < pf.srcOfferPace {
		budget = int(int64(pf.srcOfferNeed) * int64(elapsed) / int64(pf.srcOfferPace))
	}
	named := &haveBits{}
	named.Or(q.released.Bytes())
	o := gs.have.originPaced
	o.mu.Lock()
	if r := o.roots[root]; r != nil {
		named.Or(r.popped.Bytes())
	}
	o.mu.Unlock()
	n := named.Count()
	for len(q.items) > 0 {
		it := q.items[0]
		if _, index, ok := gs.have.index(it.mid); ok && !named.Has(index) {
			if n >= budget {
				break
			}
			named.Set(index)
			q.released.Set(index)
			n++
		}
		q.items = q.items[1:]
		gs.originOfferRelease(it)
	}
	if len(q.items) > 0 && !q.armed {
		q.armed = true
		time.AfterFunc(diluteTick, func() {
			select {
			case gs.p.eval <- func() {
				q.armed = false
				gs.originOfferPump(root)
			}:
			case <-gs.p.ctx.Done():
			}
		})
	}
}

// originOfferRelease sends a held offer as the announce path would have: through the hose, or as
// an immediate IHAVE that joins the paced HAVE.
func (gs *GossipSubRouter) originOfferRelease(it offerItem) {
	if _, ok := gs.p.peers[it.p]; !ok {
		return
	}
	if gs.hoseOffers != nil {
		gs.offerLater(it.p, it.topic, it.mid)
		return
	}
	ids := gs.haveIHaveIDs(it.p, []string{it.mid})
	if len(ids) == 0 {
		return
	}
	ihave := rpcWithControl(nil, []*pb.ControlIHave{{TopicID: &it.topic, MessageIDs: ids}}, nil, nil, nil, nil)
	ihave.urgent = gs.haveAnnounceUrgent()
	gs.sendRPC(it.p, ihave, ihave.urgent)
	gs.haveOriginOffered(it.mid)
}

// originOfferRefuses reports whether an ask for mid goes unanswered under paced offers: a frame of
// a group we publish that we have not yet sent, told in our HAVE or offered, as a relay that has
// not received it would leave it. Router goroutine.
func (gs *GossipSubRouter) originOfferRefuses(mid string) bool {
	pf := gs.phaseForward
	if pf == nil || pf.srcOfferPace == 0 || gs.have == nil || gs.have.originPaced == nil {
		return false
	}
	root, index, ok := gs.have.index(mid)
	if !ok {
		return false
	}
	if q := pf.srcOffers[root]; q != nil && q.released.Has(index) {
		return false
	}
	o := gs.have.originPaced
	o.mu.Lock()
	defer o.mu.Unlock()
	r := o.roots[root]
	return r != nil && !r.popped.Has(index) && !r.told.Has(index)
}

// WithOriginDataHold holds the data of this node's own messages from some peers for a while: held
// reports whether data for mid toward p is held, and until when. While held, the publish path pushes
// mid to unheld peers only, announcing to held ones as to any peer not pushed, and an ask from a held
// peer is answered when the hold ends, as over a congested link. Relayed messages and announcements
// are untouched. pushes counts the (frame, peer) pushes withheld, answers the answers deferred; either
// may be nil.
func WithOriginDataHold(held func(p peer.ID, mid string) (until time.Time, ok bool), pushes, answers *atomic.Int64) Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok {
			return fmt.Errorf("origin data hold requires a gossipsub router")
		}
		if held == nil {
			return fmt.Errorf("origin data hold requires a predicate")
		}
		gs.originHold = &originHold{held: held, pushes: pushes, answers: answers, waiting: make(map[originHoldKey]*originHoldBatch)}
		return nil
	}
}

// originHold is WithOriginDataHold's predicate, its counters and the answers waiting for a hold to end.
type originHold struct {
	held            func(p peer.ID, mid string) (time.Time, bool)
	pushes, answers *atomic.Int64
	waiting         map[originHoldKey]*originHoldBatch
}

// originHoldKey is one peer's answers due at one instant, in Unix nanoseconds.
type originHoldKey struct {
	p     peer.ID
	until int64
}

// originHoldBatch is the answers of one key, and their ids so a repeated ask is folded in.
type originHoldBatch struct {
	msgs []*pb.Message
	mids map[string]struct{}
}

// originHeld reports whether data for mid, one of our own messages, is held from p now, and until
// when. Router goroutine.
func (gs *GossipSubRouter) originHeld(p peer.ID, mid string) (time.Time, bool) {
	if gs.originHold == nil {
		return time.Time{}, false
	}
	until, ok := gs.originHold.held(p, mid)
	if !ok || !time.Now().Before(until) {
		return time.Time{}, false
	}
	return until, true
}

// originHeldPeers is the peers of cands our own mid is held from; nil when none.
func (gs *GossipSubRouter) originHeldPeers(mid string, cands []peer.ID) map[peer.ID]bool {
	var held map[peer.ID]bool
	for _, p := range cands {
		if _, ok := gs.originHeld(p, mid); ok {
			if held == nil {
				held = make(map[peer.ID]bool)
			}
			held[p] = true
		}
	}
	return held
}

// originHoldCount adds n to the withheld pushes.
func (gs *GossipSubRouter) originHoldCount(n int) {
	if gs.originHold != nil && gs.originHold.pushes != nil && n > 0 {
		gs.originHold.pushes.Add(int64(n))
	}
}

// originHoldDefers takes over the answer of msg to p when msg is our own and held from p, and reports
// whether it did; the answer leaves on the event loop when the hold ends. Router goroutine.
func (gs *GossipSubRouter) originHoldDefers(p peer.ID, mid string, msg *Message) bool {
	h := gs.originHold
	if h == nil || msg.ReceivedFrom != gs.p.host.ID() {
		return false
	}
	until, ok := gs.originHeld(p, mid)
	if !ok {
		return false
	}
	key := originHoldKey{p: p, until: until.UnixNano()}
	b := h.waiting[key]
	if b == nil {
		b = &originHoldBatch{mids: make(map[string]struct{})}
		h.waiting[key] = b
		time.AfterFunc(time.Until(until), func() {
			select {
			case gs.p.eval <- func() { gs.originHoldRelease(key) }:
			case <-gs.p.ctx.Done():
			}
		})
	}
	if _, dup := b.mids[mid]; !dup {
		b.mids[mid] = struct{}{}
		b.msgs = append(b.msgs, msg.Message)
		if h.answers != nil {
			h.answers.Add(1)
		}
	}
	return true
}

// originHoldRelease sends the answers due at key as an ordinary answer. Router goroutine.
func (gs *GossipSubRouter) originHoldRelease(key originHoldKey) {
	b := gs.originHold.waiting[key]
	delete(gs.originHold.waiting, key)
	if b == nil {
		return
	}
	if _, ok := gs.p.peers[key.p]; !ok {
		return
	}
	gs.sendingSolicited = true
	gs.sendRPC(key.p, rpcWithControl(b.msgs, nil, nil, nil, nil, nil), false)
	gs.sendingSolicited = false
}

// WithPhaseSourcePolicy selects the source-side staggering policy for phase forwarding.
// Must be applied together with WithPhaseForwarding. delay is used by PhaseSourceDelay;
// degree by PhaseSourceRotate (announce width) and PhaseSourceWidePush (push width).
func WithPhaseSourcePolicy(mode PhaseSourceMode, delay time.Duration, degree int) Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok {
			return fmt.Errorf("phase source policy requires a gossipsub router")
		}
		if gs.phaseForward == nil {
			return fmt.Errorf("phase source policy requires WithPhaseForwarding first")
		}
		switch mode {
		case PhaseSourceImmediate:
		case PhaseSourceDelay:
			if delay <= 0 {
				return fmt.Errorf("phase source delay must be positive")
			}
		case PhaseSourceRotate, PhaseSourceWidePush:
			if degree <= 0 {
				return fmt.Errorf("phase source degree must be positive")
			}
		default:
			return fmt.Errorf("unknown phase source mode %d", mode)
		}
		gs.phaseForward.srcMode = mode
		gs.phaseForward.srcDelay = delay
		gs.phaseForward.srcDegree = degree
		return nil
	}
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
// fromSelf applies the source policy: it may widen the push (WidePush), and the caller
// narrows or defers the returned announce list per srcMode; a held peer is never pushed.
func (pf *phaseForwarding) trim(gs *GossipSubRouter, mid string, csum checksum, gmap map[peer.ID]struct{}, tosend map[peer.ID]struct{}, fromSelf bool) []peer.ID {
	holders := 0
	if !pf.fixedBudget {
		for p := range gmap {
			if _, ok := gs.unwanted[p][csum]; ok {
				holders++
			} else if gs.announcedHolder(mid, p) {
				holders++
			}
		}
	}
	degree := pf.degree
	if fromSelf && pf.srcMode == PhaseSourceWidePush {
		degree = pf.srcDegree
	} else if fromSelf && pf.srcPushDegree > 0 {
		degree = pf.srcPushDegree
	}
	budget := degree - holders
	if budget < 0 {
		budget = 0
	}
	cands := make([]peer.ID, 0, len(tosend))
	for p := range tosend {
		if _, inMesh := gmap[p]; inMesh {
			cands = append(cands, p)
		}
	}
	// Our own frame goes to no peer it is held from (WithOriginDataHold): the push skips them.
	var held map[peer.ID]bool
	if fromSelf {
		held = gs.originHeldPeers(mid, cands)
	}
	if fromSelf && pf.srcPushQuota > 0 && pf.srcMode != PhaseSourceWidePush {
		if root, _, ok := gs.have.index(mid); ok {
			offer, withheld := pf.quotaTrim(root, mid, cands, budget, tosend, held)
			gs.originHoldCount(withheld)
			return offer
		}
	}
	if budget >= len(cands) && len(held) == 0 {
		return nil
	}
	if pf.selRR {
		// sel=rr: deterministic peer order rotated by a per-router message counter, so each
		// mesh peer gets a fair share of this sender's pushes across messages. Senders do not
		// coordinate, so per-segment coverage across a receiver's neighbors remains
		// probabilistic — the same (1-r/D)^h expectation as sel=rank, minus the within-sender
		// binomial variance.
		sort.Slice(cands, func(i, j int) bool { return cands[i] < cands[j] })
		k := int((pf.rrCtr.Add(1) - 1) % uint64(len(cands)))
		cands = append(cands[k:], cands[:k]...)
	} else if root, ok := pf.fewestRoot(gs, mid); ok {
		known := make(map[peer.ID]int, len(cands))
		slow := make(map[peer.ID]bool, len(cands))
		var now time.Time
		if pf.selGrace > 0 {
			now = time.Now()
		}
		for _, p := range cands {
			known[p] = gs.have.knownFrames(p, root)
			slow[p] = pf.unproven(gs, p, root, now) || pf.backlogged(gs, p) // unproven first, so every demotion is heard
		}
		sort.Slice(cands, func(i, j int) bool {
			if si, sj := slow[cands[i]], slow[cands[j]]; si != sj {
				return sj
			}
			if ki, kj := known[cands[i]], known[cands[j]]; ki != kj {
				return ki < kj
			}
			ri, rj := phaseRank(mid, cands[i]), phaseRank(mid, cands[j])
			if ri != rj {
				return ri > rj
			}
			return cands[i] > cands[j]
		})
		if pf.selTolerance != PhaseSelectArgmax && !fromSelf {
			cands = pf.tolerantOrder(gs, cands, known, slow, budget)
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
	pushed, withheld := 0, 0
	for _, p := range cands {
		if pushed < budget {
			if !held[p] {
				pushed++
				continue
			}
			withheld++
		}
		delete(tosend, p)
		announce = append(announce, p)
	}
	gs.originHoldCount(withheld)
	if pf.meshless && pf.announceWidth > 0 && len(announce) > pf.announceWidth {
		// Rank-ordered, so the announce set rotates per message like the push set does.
		announce = announce[:pf.announceWidth]
	}
	if !fromSelf {
		return announce
	}
	switch pf.srcMode {
	case PhaseSourceWidePush:
		// The wide push replaces the announce entirely.
		return nil
	case PhaseSourceRotate:
		// Announce only the next srcDegree in the same per-message ranking the push used;
		// the rank rotates per message, so each mesh peer's initial pulls are a stripe.
		if len(announce) > pf.srcDegree {
			announce = announce[:pf.srcDegree]
		}
		return announce
	case PhaseSourceDelay:
		// Handled by the caller: the announce is scheduled, not yielded.
		return announce
	default:
		return announce
	}
}

// scheduleDeferredIHave announces mid to peers after the source delay, on pubsub's own
// goroutine, rechecking at fire time: a peer that IDONTWANT'd the message meanwhile has it
// and must not be invited to pull, and a peer that disconnected cannot be. This is what
// lets the first-hop holders absorb the pulls -- by the time the deferred announce lands,
// they have announced too, and an IWANT goes to whoever announced first.
func (pf *phaseForwarding) scheduleDeferredIHave(gs *GossipSubRouter, topic, mid string, peers []peer.ID) {
	if len(peers) == 0 {
		return
	}
	time.AfterFunc(pf.srcDelay, func() {
		select {
		case gs.p.eval <- func() {
			csum := computeChecksum(mid)
			for _, pid := range peers {
				if _, ok := gs.unwanted[pid][csum]; ok {
					continue
				}
				if _, ok := gs.p.peers[pid]; !ok {
					continue
				}
				ids := gs.haveIHaveIDs(pid, []string{mid})
				if len(ids) == 0 {
					continue
				}
				gs.sendRPC(pid, rpcWithControl(nil,
					[]*pb.ControlIHave{{TopicID: &topic, MessageIDs: ids}},
					nil, nil, nil, nil), gs.haveAnnounceUrgent())
			}
		}:
		case <-gs.p.ctx.Done():
		}
	})
}

// --- F1 withholding injection (measurement-plan section 15) ---

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

// WithRelaySilence makes this node forward nothing it receives from others: the fan-out for
// relayed messages yields no recipients. Own publishes, announcements and IWANT service are
// untouched; stack WithIWantWithholding for the bait-and-blackhole adversary, under which
// push supply through adversarial nodes disappears and the phi^D exposure arithmetic applies.
func WithRelaySilence(skipped *atomic.Int64) Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok {
			return fmt.Errorf("relay silence requires a gossipsub router")
		}
		gs.relaySilent = true
		gs.relaySkipped = skipped
		return nil
	}
}

// WithIDontWantSpoofing makes this node answer every IHAVE by claiming, to all mesh peers of
// the announced topic, that it already holds the announced ids — and by requesting nothing.
// The claims are wire-legal IDONTWANTs for messages the node does not hold; against phase
// forwarding they inflate the holder count that decays the push budget.
func WithIDontWantSpoofing(spoofed *atomic.Int64) Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok {
			return fmt.Errorf("idontwant spoofing requires a gossipsub router")
		}
		gs.idwSpoof = true
		gs.idwSpoofed = spoofed
		return nil
	}
}

// spoofIDontWant sends an IDONTWANT for every announced id to every mesh peer of the
// announcement's topic that supports the feature.
func (gs *GossipSubRouter) spoofIDontWant(ctl *pb.ControlMessage) {
	for _, e := range ctl.GetIhave() {
		topic := e.GetTopicID()
		ids := e.GetMessageIDs()
		if len(ids) == 0 {
			continue
		}
		for p2 := range gs.mesh[topic] {
			if !gs.feature(GossipSubFeatureIdontwant, gs.peers[p2]) {
				continue
			}
			idontwant := []*pb.ControlIDontWant{{MessageIDs: ids}}
			gs.sendRPC(p2, rpcWithControl(nil, nil, nil, nil, nil, idontwant), true)
			if gs.idwSpoofed != nil {
				gs.idwSpoofed.Add(int64(len(ids)))
			}
		}
	}
}

// --- F2a per-class admission loss (measurement-plan section 15) ---
//
// Models gossipsub's real failure mode -- RPC content shed at queue admission, never
// retransmitted -- one control class at a time, so a run can attribute which mechanism a
// lost class breaks: a lost IDONTWANT costs duplicates, a lost IHAVE/IWANT or data message
// costs liveness. Entries are stripped at doSendRPC, after splitting and piggybacking.
// Decisions are stateless: hash(node seed, class, message id, destination's stable address)
// -- so the schedule is identical across arms that share semantic units, and does not
// depend on arm-specific call order (the section 15 CRN rule).

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
		filterIDs := func(ids []string) ([]string, int64) {
			var n int64
			kept := make([]string, 0, len(ids))
			for _, id := range ids {
				if gs.lossDecide(id, dst) {
					n++
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
// 2.2x received bytes on a pull-only arm). The discipline: at most one outstanding IWANT per
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

// WithIWantHedge lets a second IWANT for the same id go out once `after` has elapsed since
// the first, capping at two outstanding. It is the cheap mitigation for pull-only's
// completion risk under withholding (measurement-plan section 15): a stalled first request
// no longer waits a full window for a fresh announcement to re-open the id. Requires the
// discipline; a later announcer's IHAVE, from a different peer, is what carries the hedge,
// so it naturally lands on a second holder rather than re-asking the silent one. `after`
// should be a round trip or two, well below the window.
func WithIWantHedge(after time.Duration) Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok {
			return fmt.Errorf("iwant hedge requires a gossipsub router")
		}
		if after <= 0 {
			return fmt.Errorf("iwant hedge delay must be positive")
		}
		gs.iwantHedge = after
		gs.iwantCount = make(map[string]int)
		return nil
	}
}

// WithIWantTailHedge is "ask k, take the first" restricted to the tail: once EnterTailHedge has
// been called (the application knows it is within a few segments of completing the group), the
// discipline permits up to k outstanding IWANTs per id, issued immediately to distinct
// announcers, instead of one. The duplicate bytes are bounded by (k - 1) times the ids still
// missing at that point, whatever the payload size, and are spent where the tail lives. On entry
// the ids already in flight are hedged from the offer table (untried announcers first); after
// entry, a fresh IHAVE for an outstanding id is answered up to the cap. Requires the discipline;
// the offer table is what makes entry proactive. Single-group form (one tail flag per router),
// like the harness's stop-pull; a production form keeps the flag per group.
func WithIWantTailHedge(k int, extra *atomic.Int64) Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok {
			return fmt.Errorf("tail hedge requires a gossipsub router")
		}
		if k < 2 {
			return fmt.Errorf("tail hedge fanout must be at least 2")
		}
		gs.tailK = k
		gs.tailExtraCtr = extra
		gs.tailAsks = make(map[string]int)
		gs.tailPeerExtra = make(map[peer.ID]int)
		if gs.iwantCount == nil {
			gs.iwantCount = make(map[string]int)
		}
		return nil
	}
}

// EnterTailHedge tells the router that this node is in the tail of the group it is receiving.
// Safe from any goroutine; the state change and the proactive hedging run on the router's.
func (p *PubSub) EnterTailHedge() {
	gs, ok := p.rt.(*GossipSubRouter)
	if !ok || gs.tailK == 0 {
		return
	}
	select {
	case p.eval <- func() { gs.enterTail() }:
	case <-p.ctx.Done():
	}
}

// enterTail runs on the router goroutine: flips the flag and hedges the asks already in flight
// from the offer table, up to the fan-out, untried announcers first in deterministic peer order
// (tailTopUp: oldest asks first, and no more ids than the deficit when it is known).
func (gs *GossipSubRouter) enterTail() {
	gs.tailOn = true
	gs.tailTopUp(nil)
}

// tailHedgeMid issues extra IWANTs for an outstanding id until the effective fan-out are in
// flight, each to an announcer not yet asked for it, within the bounds; honours parks and the
// request gate like every other pull. Returns the asks issued.
func (gs *GossipSubRouter) tailHedgeMid(mid string) int {
	issued := 0
	tried := gs.offerTried[mid]
	cands := make([]peer.ID, 0, len(gs.offerTable[mid]))
	for p := range gs.offerTable[mid] {
		if _, was := tried[p]; was {
			continue
		}
		if gs.peerParkedFor(p, mid) {
			continue
		}
		if gs.requestGate != nil && !gs.requestGate.Allow(p, gs.offerTopic[mid], mid) {
			continue
		}
		cands = append(cands, p)
	}
	sort.Slice(cands, func(i, j int) bool { return cands[i] < cands[j] })
	for _, pick := range cands {
		if gs.iwantCount[mid] >= gs.tailKNow() {
			return issued
		}
		if !gs.tailExtraAllowed(mid, pick) {
			return issued
		}
		issued++
		gs.iwantCount[mid]++
		gs.noteAsk(mid, pick)
		if gs.tailExtraCtr != nil {
			gs.tailExtraCtr.Add(1)
		}
		gs.iasked[pick]++
		gs.gossipTracer.AddPromise(pick, []string{mid})
		gs.noteIWantSent(pick, []string{mid})
		if gs.requestGate != nil {
			gs.requestGate.Committed(pick, []string{mid})
		}
		gs.sendRPC(pick, rpcWithControl(nil, nil, []*pb.ControlIWant{{MessageIDs: []string{mid}}}, nil, nil, nil), false)
	}
	return issued
}

// noteAsk records an ask of mid to from for the offer table (so a retry does not re-ask the same
// peer) and opens its promise for the park, when those are on. The first ask's retry timer is not
// armed here: it belongs to the id, not the peer.
func (gs *GossipSubRouter) noteAsk(mid string, from peer.ID) {
	if gs.offerTable != nil {
		if gs.offerTried[mid] == nil {
			gs.offerTried[mid] = make(map[peer.ID]time.Time)
		}
		gs.offerTried[mid][from] = time.Now()
	}
	if gs.iwantPromised != nil {
		if gs.iwantPromised[mid] == nil {
			gs.iwantPromised[mid] = make(map[peer.ID]time.Time)
		}
		if _, open := gs.iwantPromised[mid][from]; open {
			return // the earlier promise keeps its deadline (see iwantAllowed)
		}
		askedAt := time.Now()
		gs.iwantPromised[mid][from] = askedAt
		m, f := mid, from
		time.AfterFunc(gs.parkPromise, func() {
			select {
			case gs.p.eval <- func() { gs.promiseDue(m, f, askedAt) }:
			case <-gs.p.ctx.Done():
			}
		})
	}
}

// --- Deterministic randomness (common-random-numbers mode for the harness) ---
//
// Gossipsub's mesh draw runs the global math/rand over map-iteration-ordered candidate
// lists, so two runs on the same connectivity graph grow different meshes (Q23: pull-only
// completion spans 0.79-1.37 s across realizations at constant bytes). This mode makes the
// draw a function of the seed: candidate lists are sorted by a run-stable key before a
// Fisher-Yates from a router-local seeded source. Peer IDs are fresh keys every run, so the
// sort key is the peer's first known address -- stable in simnet, where addresses derive
// from the node index. Harness-only; upstream needs none of this.

// WithDeterministicRand replaces the router's shuffle randomness with a seeded source.
func WithDeterministicRand(seed int64) Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok {
			return fmt.Errorf("deterministic rand requires a gossipsub router")
		}
		gs.rng = rand.New(rand.NewSource(seed))
		return nil
	}
}

// stablePeerKey orders peers by first known address, falling back to the id.
func (gs *GossipSubRouter) stablePeerKey(p peer.ID) string {
	if addrs := gs.p.host.Peerstore().Addrs(p); len(addrs) > 0 {
		return addrs[0].String()
	}
	return string(p)
}

func (gs *GossipSubRouter) shufflePeers(peers []peer.ID) {
	if gs.rng == nil {
		shufflePeers(peers)
		return
	}
	// Compute each key once. stablePeerKey is an address-book lookup, and calling it inside the
	// comparator made the sort O(n log n) lookups per shuffle; the heartbeat runs a shuffle per
	// topic, so at 64 topics on a 500-node mesh one topology spent two wall-clock hours in this
	// sort without ever settling (experiments Q77, seed 13). Same keys, same order, so every
	// deterministic-rand cell keeps its outcome.
	keys := make(map[peer.ID]string, len(peers))
	for _, p := range peers {
		if _, ok := keys[p]; !ok {
			keys[p] = gs.stablePeerKey(p)
		}
	}
	sort.Slice(peers, func(i, j int) bool {
		return keys[peers[i]] < keys[peers[j]]
	})
	for i := range peers {
		j := gs.rng.Intn(i + 1)
		peers[i], peers[j] = peers[j], peers[i]
	}
}

func (gs *GossipSubRouter) shuffleStrings(lst []string) {
	if gs.rng == nil {
		shuffleStrings(lst)
		return
	}
	sort.Strings(lst)
	for i := range lst {
		j := gs.rng.Intn(i + 1)
		lst[i], lst[j] = lst[j], lst[i]
	}
}

func (gs *GossipSubRouter) shufflePeerInfo(peers []*pb.PeerInfo) {
	if gs.rng == nil {
		shufflePeerInfo(peers)
		return
	}
	sort.Slice(peers, func(i, j int) bool {
		return string(peers[i].GetPeerID()) < string(peers[j].GetPeerID())
	})
	for i := range peers {
		j := gs.rng.Intn(i + 1)
		peers[i], peers[j] = peers[j], peers[i]
	}
}

// WithSendDelay stalls this node's outbound writer per RPC (F2b real queue pressure,
// measurement-plan section 15). Applied to selected nodes by the harness; their rpcQueue
// then overflows naturally through doDropRPC as offered load outpaces the slowed drain.
// Test-only; a production node must never set it.
func WithSendDelay(d time.Duration) Option {
	return func(ps *PubSub) error {
		if d <= 0 {
			return fmt.Errorf("send delay must be positive")
		}
		ps.sendDelay = d
		return nil
	}
}

// --- (N, q, g) request kernel (design-space §10; measurement-plan §15) ---
//
// The inner per-message-ID launch scheduler: keep N *plausible* outstanding requests, each
// declared implausible (and replaceable) at a per-peer quantile F_p⁻¹(q) of that peer's
// response time, clamped to [floor, ceil] with ceil doubling as the hard deadline so a
// withholder times out. Estimation is online and warms across payloads (repeated harness),
// which is the whole reason this cannot be judged single-shot (Q30).
//
// Honest scope, per the external review: this is the *kernel*, not the deployable path. It
// is IHAVE-driven (a replacement is dispatched when the next candidate's IHAVE arrives, not
// from a timer callback — the "otherwise dispatch on IHAVE arrival" fallback used as the
// only trigger); the where-ranking is "a fresh, still-plausible candidate" (LCFS-ish by
// arrival, no reliability/load ranking); node/group admission limits and serve-probability
// are not modelled; sampling is win-biased and post-verification, as in variant B.

// peerQuantile is a Robbins-Monro online estimate of the q-quantile of a peer's response
// time. One float and a count per peer; the step is a fraction of the current estimate.
type peerQuantile struct {
	est time.Duration
	n   int
}

func (pq *peerQuantile) observe(sample time.Duration, q float64) {
	if pq.n == 0 {
		pq.est, pq.n = sample, 1
		return
	}
	step := time.Duration(float64(pq.est) * 0.10)
	if step <= 0 {
		step = time.Millisecond
	}
	if sample <= pq.est {
		pq.est -= time.Duration(float64(step) * (1 - q))
	} else {
		pq.est += time.Duration(float64(step) * q)
	}
	if pq.est < 0 {
		pq.est = 0
	}
	pq.n++
}

type nqgConfig struct {
	n           int
	q           float64
	floor, ceil time.Duration
	minSamples  int
}

// WithRequestNQG enables the (N, q, g) kernel. Distinct from the discipline/hedge modes;
// enable one. dInit seeds the global prior used for cold peers.
func WithRequestNQG(n int, q float64, floor, ceil, dInit time.Duration) Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok {
			return fmt.Errorf("request-nqg requires a gossipsub router")
		}
		if n < 1 || q <= 0 || q >= 1 || floor <= 0 || ceil < floor || dInit < floor || dInit > ceil {
			return fmt.Errorf("bad request-nqg parameters")
		}
		gs.nqg = &nqgConfig{n: n, q: q, floor: floor, ceil: ceil, minSamples: 8}
		gs.nqgAsked = make(map[string]map[peer.ID]time.Time)
		gs.rttPeer = make(map[peer.ID]*peerQuantile)
		gs.rttGlobal = &peerQuantile{est: dInit, n: 1}
		return nil
	}
}

// nqgTimer is peer p's implausibility timeout: its own quantile once warm, else the global
// prior, clamped to [floor, ceil]. ceil is the hard deadline that fires against silence.
func (gs *GossipSubRouter) nqgTimer(p peer.ID) time.Duration {
	est := gs.rttGlobal.est
	if pq, ok := gs.rttPeer[p]; ok && pq.n >= gs.nqg.minSamples {
		est = pq.est
	}
	if est < gs.nqg.floor {
		est = gs.nqg.floor
	}
	if est > gs.nqg.ceil {
		est = gs.nqg.ceil
	}
	return est
}

// nqgAllowed decides whether to dispatch a request for mid to announcer p: maintain N
// plausible (unexpired) asks, never re-ask a still-plausible peer, drop expired asks (they
// race on, no cancel). Runs on pubsub's goroutine.
func (gs *GossipSubRouter) nqgAllowed(mid string, p peer.ID) bool {
	asks := gs.nqgAsked[mid]
	if asks == nil {
		asks = make(map[peer.ID]time.Time)
		gs.nqgAsked[mid] = asks
	}
	now := time.Now()
	plausible := 0
	for q, at := range asks {
		if now.Sub(at) >= gs.nqgTimer(q) {
			delete(asks, q) // implausible: no longer counts toward N; still on the wire
			continue
		}
		plausible++
	}
	if plausible >= gs.nqg.n {
		return false
	}
	if _, ok := asks[p]; ok {
		return false // already have a plausible request to this peer
	}
	asks[p] = now
	return true
}

// nqgObserve feeds a claim->arrival sample for a served id, if we asked that peer for it.
func (gs *GossipSubRouter) nqgObserve(mid string, from peer.ID) {
	if gs.nqg == nil {
		return
	}
	asks := gs.nqgAsked[mid]
	if asks == nil {
		return
	}
	if at, ok := asks[from]; ok {
		sample := time.Since(at)
		if sample > 0 {
			pq := gs.rttPeer[from]
			if pq == nil {
				pq = &peerQuantile{}
				gs.rttPeer[from] = pq
			}
			pq.observe(sample, gs.nqg.q)
			gs.rttGlobal.observe(sample, gs.nqg.q)
		}
	}
	delete(gs.nqgAsked, mid) // satisfied
}

// clearNqgAsked drops fully-expired id entries at the heartbeat (rttPeer persists — it warms
// across payloads).
func (gs *GossipSubRouter) clearNqgAsked() {
	if gs.nqgAsked == nil {
		return
	}
	now := time.Now()
	for mid, asks := range gs.nqgAsked {
		allOld := true
		for _, at := range asks {
			if now.Sub(at) < gs.nqg.ceil {
				allOld = false
				break
			}
		}
		if allOld {
			delete(gs.nqgAsked, mid)
		}
	}
}

// --- Adaptive hedge: two decoupled controllers (measurement-plan section 15) ---
//
// Job 1 sets the hedge delay d by tracking a tail quantile of arrival latency (when to
// hedge); job 2 gates hedging with an AIMD rate e driven by local queue occupancy (how much
// under load). They are separate because d must RISE under heterogeneous latency while e
// must FALL under congestion -- one knob cannot do both. Both update at the heartbeat.
type adaptiveHedge struct {
	// Job 1: quantile-tracked delay.
	d      time.Duration // current hedge delay
	dMin   time.Duration
	dMax   time.Duration
	target float64       // desired hedge-launch rate (e.g. 0.05 => d tracks p95)
	eta    time.Duration // Robbins-Monro step
	// Job 2: AIMD hedge-firing probability.
	e     float64 // in (0,1]
	aiAdd float64 // additive increase per heartbeat
	aiMul float64 // multiplicative decrease factor (<1)
	qHigh float64 // queue-occupancy fraction that triggers the decrease
	// Per-epoch counters, reset each heartbeat.
	launched int // hedges fired this epoch (job-1 numerator)
	eligible int // ids that reached d this epoch (job-1 denominator)
	// Observability: did Job 2 ever engage, and how deep did the queue get? Scaled x1000
	// into the optional atomics so the harness can log them (min e, max occupancy seen).
	minE   *atomic.Int64
	maxOcc *atomic.Int64
}

// WithAdaptiveHedge turns the fixed hedge delay into the two-controller form. Requires the
// discipline. Sensible defaults; the harness overrides target/bounds by env.
func WithAdaptiveHedge(dInit, dMin, dMax time.Duration, target, qHigh float64, minE, maxOcc *atomic.Int64) Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok {
			return fmt.Errorf("adaptive hedge requires a gossipsub router")
		}
		if target <= 0 || target >= 1 || dMin <= 0 || dMax < dMin || dInit < dMin || dInit > dMax || qHigh <= 0 || qHigh >= 1 {
			return fmt.Errorf("bad adaptive hedge parameters")
		}
		gs.adaptHedge = &adaptiveHedge{
			d: dInit, dMin: dMin, dMax: dMax, target: target, eta: dMin / 4,
			e: 1.0, aiAdd: 0.1, aiMul: 0.5, qHigh: qHigh,
			minE: minE, maxOcc: maxOcc,
		}
		if minE != nil {
			minE.Store(1000) // e starts at 1.0
		}
		gs.iwantCount = make(map[string]int)
		return nil
	}
}

// hedgeAllowedAdaptive is the adaptive counterpart of the fixed-delay hedge check. Called
// from iwantAllowed when a request for mid is already outstanding.
func (gs *GossipSubRouter) hedgeAllowedAdaptive(mid string, elapsed time.Duration) bool {
	a := gs.adaptHedge
	if elapsed < a.d {
		return false // job 1: not yet at the delay
	}
	if gs.iwantCount[mid] >= 2 {
		return false // cap at two outstanding
	}
	a.eligible++
	// job 2: fire only with probability e (the AIMD budget). Use the seeded rng if present
	// so the decision is reproducible under SEGMENT_DET_RAND, else the global source.
	var r float64
	if gs.rng != nil {
		r = gs.rng.Float64()
	} else {
		r = rand.Float64()
	}
	if r >= a.e {
		return false
	}
	a.launched++
	gs.iwantCount[mid]++
	return true
}

// updateAdaptiveHedge runs both control loops once, at the heartbeat.
func (gs *GossipSubRouter) updateAdaptiveHedge() {
	a := gs.adaptHedge
	if a == nil {
		return
	}
	// Job 1: Robbins-Monro on the hedge-launch rate. H is the observed rate this epoch; nudge
	// d toward the delay at which H == target. Too many launches => d too small => raise it.
	if a.eligible > 0 {
		h := float64(a.launched) / float64(a.eligible)
		d := a.d + time.Duration((h-a.target)*float64(a.eta)*4)
		if d < a.dMin {
			d = a.dMin
		}
		if d > a.dMax {
			d = a.dMax
		}
		a.d = d
	}
	a.launched, a.eligible = 0, 0
	// Job 2: AIMD on e, signal = max outbound-queue occupancy across this node's peers.
	occ := 0.0
	for _, q := range gs.p.peers {
		l, c := q.Occupancy()
		if c > 0 {
			if f := float64(l) / float64(c); f > occ {
				occ = f
			}
		}
	}
	if occ >= a.qHigh {
		a.e *= a.aiMul // multiplicative decrease: back off fast
	} else {
		a.e += a.aiAdd // additive increase: creep back
		if a.e > 1.0 {
			a.e = 1.0
		}
	}
	// Observability: record how far e fell and how deep the queue ever got.
	if a.minE != nil {
		if v := int64(a.e * 1000); v < a.minE.Load() {
			a.minE.Store(v)
		}
	}
	if a.maxOcc != nil {
		if v := int64(occ * 1000); v > a.maxOcc.Load() {
			a.maxOcc.Store(v)
		}
	}
}

// WithIWantBudget enables the upstream-PR-625 rule instead: a per-message-id IWANT budget
// that the heartbeat resets wholesale and message arrival clears. Same intent as the
// discipline, different retry clock -- the budget re-opens at the next heartbeat *boundary*,
// the discipline's window a fixed interval after the request. Built for a measured
// head-to-head; enable one or the other, not both.
func WithIWantBudget(n int) Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok {
			return fmt.Errorf("iwant budget requires a gossipsub router")
		}
		if n <= 0 {
			return fmt.Errorf("iwant budget must be positive")
		}
		gs.iwantBudgetMax = n
		gs.iwantBudget = make(map[string]int)
		return nil
	}
}

// iwantBudgetAllowed mirrors PR 625's allowedIWantCount check, consuming one unit.
func (gs *GossipSubRouter) iwantBudgetAllowed(mid string) bool {
	if gs.iwantBudget == nil {
		return true
	}
	n, ok := gs.iwantBudget[mid]
	if !ok {
		n = gs.iwantBudgetMax
	}
	if n <= 0 {
		return false
	}
	gs.iwantBudget[mid] = n - 1
	return true
}

// resetIWantBudget discards all budget state; called from the heartbeat (PR 625 semantics).
func (gs *GossipSubRouter) resetIWantBudget() {
	if len(gs.iwantBudget) > 0 {
		gs.iwantBudget = make(map[string]int)
	}
}

// clearIWantBudgetFor drops the id on message arrival (PR 625's Preprocess delete). The pull
// budget's outstanding entry clears here too: delivery is what frees a slot.
func (gs *GossipSubRouter) clearIWantBudgetFor(mid string) {
	if gs.iwantBudget != nil {
		delete(gs.iwantBudget, mid)
	}
	// Delivery keeps the promise: the asked peer served within the window, so the later
	// expiry of the iwantAsked entry (which delivery deliberately does not clear) must not
	// count as a break. Without this line every honest serve would eventually park its
	// server, because iwantAsked entries age out rather than clearing on arrival.
	if gs.iwantPromised != nil {
		delete(gs.iwantPromised, mid)
	}
	if gs.pullOutstanding != nil {
		delete(gs.pullOutstanding, mid)
		gs.redrivePulls()
	}
	gs.clearOffers(mid)
}

// iwantAllowed reports whether an IWANT for mid may be issued now, recording it if so.
// Runs on pubsub's goroutine, like every router map.
func (gs *GossipSubRouter) iwantAllowed(mid string, from peer.ID) bool {
	if gs.iwantAsked == nil {
		return true
	}
	if at, ok := gs.iwantAsked[mid]; ok && time.Since(at) < gs.iwantWindow {
		elapsed := time.Since(at)
		// Tail hedge: in the tail, a fresh announcer of an outstanding id is asked at once, up
		// to the cap, never the same peer twice.
		if gs.tailOn && gs.tailK > 0 && gs.iwantCount[mid] < gs.tailKNow() {
			if tried := gs.offerTried[mid]; tried != nil {
				if _, was := tried[from]; was {
					return false
				}
			}
			if !gs.tailExtraAllowed(mid, from) {
				return false
			}
			gs.iwantCount[mid]++
			gs.noteAsk(mid, from)
			if gs.tailExtraCtr != nil {
				gs.tailExtraCtr.Add(1)
			}
			return true
		}
		// Adaptive hedge (two-controller form) takes precedence over the fixed one.
		if gs.adaptHedge != nil {
			return gs.hedgeAllowedAdaptive(mid, elapsed)
		}
		// Fixed hedge: once the first ask has been outstanding for iwantHedge, permit exactly
		// one more (total two) without extending the window. The elapsed check keeps a burst
		// of near-simultaneous announcers from firing two requests at once.
		if gs.iwantHedge > 0 && elapsed >= gs.iwantHedge && gs.iwantCount[mid] < 2 {
			gs.iwantCount[mid]++
			return true
		}
		return false
	}
	gs.iwantAsked[mid] = time.Now()
	if gs.iwantCount != nil {
		gs.iwantCount[mid] = 1
	}
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
	if gs.iwantPromised != nil {
		// Two-timer design: the promise is per (id, peer) and survives the discipline's
		// move-on, so a fast window re-asks elsewhere while the slow/withholding peer's
		// own deadline (parkPromise, which may exceed the window) still runs to judgment.
		// Delivery of the id from anyone absolves every open promise for it.
		if gs.iwantPromised[mid] == nil {
			gs.iwantPromised[mid] = make(map[peer.ID]time.Time)
		}
		// A re-ask of a peer whose promise for this id is still open keeps the earlier
		// deadline: overwriting it let every retry postpone the break, so a silent sole
		// announcer asked every window was never judged (Q131).
		if _, open := gs.iwantPromised[mid][from]; open {
			return true
		}
		askedAt := gs.iwantAsked[mid]
		gs.iwantPromised[mid][from] = askedAt
		m, f := mid, from
		time.AfterFunc(gs.parkPromise, func() {
			select {
			case gs.p.eval <- func() { gs.promiseDue(m, f, askedAt) }:
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
// moved on already — then there is nothing left to refund).
func (gs *GossipSubRouter) promiseDue(mid string, from peer.ID, askedAt time.Time) {
	if gs.iwantPromised == nil {
		return
	}
	peers := gs.iwantPromised[mid]
	if at, open := peers[from]; !open || !at.Equal(askedAt) {
		return
	}
	if gs.haveHolds(mid) {
		gs.promiseHeld.Add(1)
		if gs.promiseRecheck {
			gs.absolvePromise(mid, from)
			return
		}
	}
	gs.notePromiseBreak(mid, from)
	if at, ok := gs.iwantAsked[mid]; ok && at.Equal(askedAt) {
		delete(gs.iwantAsked, mid)
		if gs.iwantCount != nil {
			delete(gs.iwantCount, mid)
		}
		if gs.pullOutstanding != nil {
			delete(gs.pullOutstanding, mid)
			gs.redrivePulls()
		}
	}
}

// notePromiseBreak charges peer p for an unserved ask of mid (its promise deadline elapsed
// undelivered), parking it once its breaks reach parkK. No-op when enforcement is off.
func (gs *GossipSubRouter) notePromiseBreak(mid string, p peer.ID) {
	if gs.iwantPromised == nil {
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
			if gs.iwantCount != nil {
				delete(gs.iwantCount, mid)
			}
		}
	}
	// With pull memory, freed windows re-ask remembered announcers instead of waiting
	// for a fresh offer that may never come (relays announce once).
	gs.redrivePulls()
	gs.sweepOffers()
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

// --- Announce-fed holder tracking (for phase forwarding) ---
//
// The phase decay's holder estimate is IDONTWANT-fed, and IDONTWANT only reaches mesh peers
// -- which is exactly what broke the first meshless attempt (Q21): off-mesh candidates never
// registered as holders, so the decay never fired. Announces are the wider signal: a peer
// that sent us an IHAVE for a message id holds it. Opt-in, because it changes the measured
// behaviour of the existing phase arms.

// Group-aware push suppression: the sender-side half of the coded-group byte-suppression
// design (the receiver-side half is the application's stop-pull request gate). Messages that
// belong to a group where any `need` distinct members complete a receiver — erasure-coded
// segment groups — stop being pushed to a peer once that peer has evidenced `need` distinct
// members, and are announced to it instead. Evidence is IDONTWANT-fed, the same channel the
// per-message decay uses, with the same v1 limitation: only mesh peers emit it.
//
// Storage is prototype-grade: per (peer, group) distinct-mid sets, unpruned, sized for a
// measurement cell rather than a long-lived node. Runs entirely on the router's event loop,
// so it takes no locks, like the phaseHolders tracking above.
type groupPush struct {
	extract func(mid string) (string, bool)
	need    int
	held    map[peer.ID]map[string]map[string]struct{}
	vetoes  *atomic.Int64 // optional, application-supplied; counts (message, peer) push vetoes
	// project (WithGroupCompletionProjection) counts the members we pushed to p ourselves
	// together with the evidenced ones: what p will hold once our frames in flight land. The
	// evidence alone arrives a have-signal delay late, and by then the overrun has
	// already left.
	project bool
	pushed  map[peer.ID]map[string]map[string]struct{}
}

func groupPushAdd(m map[peer.ID]map[string]map[string]struct{}, p peer.ID, key, mid string) {
	byGroup, ok := m[p]
	if !ok {
		byGroup = make(map[string]map[string]struct{})
		m[p] = byGroup
	}
	mids, ok := byGroup[key]
	if !ok {
		mids = make(map[string]struct{})
		byGroup[key] = mids
	}
	mids[mid] = struct{}{}
}

// note records that p evidenced holding mid.
func (g *groupPush) note(p peer.ID, mid string) {
	key, ok := g.extract(mid)
	if !ok {
		return
	}
	groupPushAdd(g.held, p, key, mid)
}

// notePushed records that we pushed mid to p. Router goroutine, from the push loop.
func (g *groupPush) notePushed(p peer.ID, mid string) {
	key, ok := g.extract(mid)
	if !ok {
		return
	}
	if g.pushed == nil {
		g.pushed = make(map[peer.ID]map[string]map[string]struct{})
	}
	groupPushAdd(g.pushed, p, key, mid)
}

// complete reports whether p has evidenced enough distinct members of mid's group to
// complete without mid — with the projection, whether p will have once our own pushes land.
func (g *groupPush) complete(p peer.ID, mid string) bool {
	key, ok := g.extract(mid)
	if !ok {
		return false
	}
	held := g.held[p][key]
	if !g.project {
		return len(held) >= g.need
	}
	n := len(held)
	for m := range g.pushed[p][key] {
		if _, dup := held[m]; !dup {
			n++
		}
	}
	return n >= g.need
}

// WithGroupCompletionProjection makes the group-completion veto count the members this node
// has itself pushed to a peer along with the ones the peer evidenced, so the push stops when
// the frames in flight will complete the peer rather than when the peer says they did.
// Requires WithGroupCompletionPush.
func WithGroupCompletionProjection() Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok || gs.groupPush == nil {
			return fmt.Errorf("group completion projection requires WithGroupCompletionPush first")
		}
		gs.groupPush.project = true
		return nil
	}
}

// WithGroupCompletionPush stops pushing group members to peers evidenced to hold at least
// `need` distinct members of the same group, announcing to them instead. extract derives a
// group key from a message id, ok=false for ids that carry none — those are never vetoed.
// vetoes, if non-nil, is incremented once per suppressed (message, peer) push. Requires
// WithPhaseForwarding: the veto rides the phase path's announce machinery.
func WithGroupCompletionPush(extract func(mid string) (string, bool), need int, vetoes *atomic.Int64) Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok {
			return fmt.Errorf("group completion push requires a gossipsub router")
		}
		if gs.phaseForward == nil {
			return fmt.Errorf("group completion push requires WithPhaseForwarding first")
		}
		if extract == nil || need <= 0 {
			return fmt.Errorf("group completion push requires an extractor and need > 0")
		}
		gs.groupPush = &groupPush{
			extract: extract,
			need:    need,
			held:    make(map[peer.ID]map[string]map[string]struct{}),
			vetoes:  vetoes,
		}
		return nil
	}
}

// WithMessageLinkFilter installs a per-(message, link) activation predicate on the eager
// path: a mesh link that is inactive for a message carries neither the push, nor the phase
// announce, nor the IDONTWANT for it. The predicate must be symmetric — both endpoints
// compute the same answer from the unordered peer pair and the message id — so each message
// diffuses over a deterministic subgraph of the mesh that both sides agree on, and a wide
// mesh yields per-message topologies of smaller expected degree with disjoint path sets.
//
// The lazy channel follows the complement: heartbeat gossip, which normally excludes mesh
// peers because they were served eagerly, includes them under a filter — restricted to their
// inactive-link ids — since for those ids the eager service never happened. Non-mesh gossip
// and IWANT serving are unchanged, so a node isolated in one message's subgraph can still
// pull it. Non-message traffic never reaches the predicate.
func WithMessageLinkFilter(filter func(local, remote peer.ID, mid string) bool) Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok {
			return fmt.Errorf("message link filter requires a gossipsub router")
		}
		if filter == nil {
			return fmt.Errorf("message link filter requires a non-nil predicate")
		}
		gs.linkFilter = filter
		return nil
	}
}

// WithMessageLinkFilterEnforcement makes the link filter bilateral on the data channel. The
// predicate is deterministic and symmetric, so a receiver can verify inbound traffic against
// it — but only where the channel is unambiguous: an *unsolicited* full message over an
// inactive mesh link is a violation, counted (its bytes are already spent, so it is still
// used) with the sender attributable. Data whose id sits in the IWANT discipline's
// asked-table is solicited — a served pull of a lazily announced id — and never counted.
// Announcements are deliberately not checked: heartbeat gossip
// legitimately carries inactive-link ids to mesh peers (see WithMessageLinkFilter), and the
// wire cannot distinguish lazy from immediate IHAVEs. violations, if non-nil, is incremented
// once per violating push. Requires WithMessageLinkFilter first.
func WithMessageLinkFilterEnforcement(violations *atomic.Int64) Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok {
			return fmt.Errorf("link filter enforcement requires a gossipsub router")
		}
		if gs.linkFilter == nil {
			return fmt.Errorf("link filter enforcement requires WithMessageLinkFilter first")
		}
		gs.linkEnforce = true
		gs.linkViolations = violations
		return nil
	}
}

// WithPhaseAnnounceHolders feeds received IHAVEs into phase forwarding's holder estimate.
func WithPhaseAnnounceHolders() Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok {
			return fmt.Errorf("announce holders requires a gossipsub router")
		}
		if gs.phaseForward == nil {
			return fmt.Errorf("announce holders requires WithPhaseForwarding first")
		}
		gs.phaseHolders = make(map[string]*phaseHolderEntry)
		return nil
	}
}

type phaseHolderEntry struct {
	peers map[peer.ID]struct{}
	added time.Time
}

// noteAnnouncedHolder records that p announced mid. Bounded by the heartbeat pruning below
// and by gossipsub's own caps on how many IHAVE ids a peer may advertise per heartbeat.
func (gs *GossipSubRouter) noteAnnouncedHolder(mid string, p peer.ID) {
	if gs.phaseHolders == nil {
		return
	}
	e, ok := gs.phaseHolders[mid]
	if !ok {
		e = &phaseHolderEntry{peers: make(map[peer.ID]struct{}), added: time.Now()}
		gs.phaseHolders[mid] = e
	}
	e.peers[p] = struct{}{}
}

// announcedHolder reports whether p is known (via IHAVE) to hold mid.
func (gs *GossipSubRouter) announcedHolder(mid string, p peer.ID) bool {
	if gs.phaseHolders == nil {
		return false
	}
	e, ok := gs.phaseHolders[mid]
	if !ok {
		return false
	}
	_, held := e.peers[p]
	return held
}

// clearPhaseHolders drops holder entries older than the retention window; called from the
// heartbeat. Six heartbeats matches the mcache history a message stays serveable for. The
// rarest-first announcer table follows the same retention.
func (gs *GossipSubRouter) clearPhaseHolders() {
	cutoff := 6 * gs.params.HeartbeatInterval
	for mid, e := range gs.phaseHolders {
		if time.Since(e.added) >= cutoff {
			delete(gs.phaseHolders, mid)
		}
	}
	for mid, e := range gs.rarityAnnounce {
		if time.Since(e.added) >= cutoff {
			delete(gs.rarityAnnounce, mid)
		}
	}
	for mid, e := range gs.rarityHold {
		if time.Since(e.added) >= cutoff {
			delete(gs.rarityHold, mid)
		}
	}
}

// WithRarestFirstPull orders each IHAVE's pull candidates by how many distinct peers have
// announced them, fewest first — rarest-first chunk scheduling on the pull plane. The eager
// wavefront (pushes) is untouched: pulls happen where segment diffusions already collide in a
// node's schedule, which is exactly where balancing belongs. Rarity comes from a dedicated
// announcer table rather than the phase decay's holder estimate, so enabling this changes pull
// order and nothing else.
func WithRarestFirstPull() Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok {
			return fmt.Errorf("rarest-first pull requires a gossipsub router")
		}
		gs.rarestFirst = true
		gs.rarityAnnounce = make(map[string]*phaseHolderEntry)
		return nil
	}
}

// WithRarityOrderedDrain orders each peer's queued normal-lane RPCs by the rarity of their
// data, least-diffused first — sender-side rarest-first, at the plane where the uplink
// actually binds. Rank 0 (control flushes, non-data) drains ahead; a data RPC ranks 1 plus
// the fewest known announcers among its messages. The rank is stamped at enqueue on the event
// loop, where the rarity table lives, and may go stale while queued — bounded by queue
// residence time. With an empty or single-entry queue the drain is exact FIFO, so the
// uncontended wavefront is untouched by construction: the queue's own occupancy is the phase
// indicator, and no explicit collision detector exists or is needed.
func WithRarityOrderedDrain() Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok {
			return fmt.Errorf("rarity-ordered drain requires a gossipsub router")
		}
		gs.rarityDrain = true
		if gs.rarityAnnounce == nil {
			gs.rarityAnnounce = make(map[string]*phaseHolderEntry)
		}
		return nil
	}
}

// WithRarityDrainDelivered switches the drain's rank signal from announcers to delivered
// evidence: distinct peers that IDONTWANTed the mid — peers known to HOLD it, not merely to
// offer it. Q64's diagnosis of the announce signal: widely announced says the message is
// widely *offered*, and deferring its forwards converts them into pulls on the first
// announcer. Holding is the fact balance actually cares about. Requires
// WithRarityOrderedDrain first.
func WithRarityDrainDelivered() Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok {
			return fmt.Errorf("delivered rarity signal requires a gossipsub router")
		}
		if !gs.rarityDrain {
			return fmt.Errorf("delivered rarity signal requires WithRarityOrderedDrain first")
		}
		gs.rarityHold = make(map[string]*phaseHolderEntry)
		return nil
	}
}

// noteRarityHolder records that p evidenced holding mid (IDONTWANT-fed).
func (gs *GossipSubRouter) noteRarityHolder(mid string, p peer.ID) {
	if gs.rarityHold == nil {
		return
	}
	e, ok := gs.rarityHold[mid]
	if !ok {
		e = &phaseHolderEntry{peers: make(map[peer.ID]struct{}), added: time.Now()}
		gs.rarityHold[mid] = e
	}
	e.peers[p] = struct{}{}
}

// drainRank computes the enqueue-time rank for a normal-lane RPC: 1 + the fewest evidenced
// holders (delivered signal, when enabled) or announcers among its messages.
func (gs *GossipSubRouter) drainRank(rpc *RPC) int {
	msgs := rpc.GetPublish()
	if len(msgs) == 0 {
		return 0
	}
	best := int(^uint(0) >> 1)
	for _, m := range msgs {
		mid := gs.p.idGen.RawID(m)
		var c int
		if gs.rarityHold != nil {
			if e, ok := gs.rarityHold[mid]; ok {
				c = len(e.peers)
			}
		} else {
			c = gs.rarityCount(mid)
		}
		if c < best {
			best = c
		}
	}
	return 1 + best
}

// WithPullBudget caps the number of concurrently outstanding pulled ids at n, across all
// peers. Distinct from WithIWantBudget, which is upstream #625's per-id retry budget per
// heartbeat (Q23: a no-op at default) — this is BitTorrent-style pipeline depth: with a
// binding budget, which candidate gets a slot is a real allocation, which is what gives
// rarest-first pull something to decide. Outstanding entries clear on delivery and expire on
// the discipline window (1 s when no discipline is set), so a wedged sender cannot leak slots.
func WithPullBudget(n int) Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok {
			return fmt.Errorf("pull budget requires a gossipsub router")
		}
		if n <= 0 {
			return fmt.Errorf("pull budget must be positive")
		}
		gs.pullBudgetMax = n
		gs.pullOutstanding = make(map[string]time.Time)
		return nil
	}
}

// WithOfferTable makes the IWANT discipline remember every announcement instead of discarding
// the ones it cannot act on: offers for unseen ids go into a standing table (announcer, time,
// topic), every granted ask arms a retry timer one window later, and the retry re-asks the best
// untried announcer from the table — cycling to the least-recently-tried when all are spent, so
// a slow honest server gets a second lap. Parks and the request gate filter at pick time;
// delivery prunes the id everywhere. This is state, not a queue: announcements are facts about
// who holds what, and in a protocol whose relays announce once, forgetting them is what turned
// the discipline into the withholding adversary's capture lever (Q70/Q72). Requires
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
	for mid, peers := range gs.offerTable {
		for p, at := range peers {
			if time.Since(at) >= offerTTL {
				delete(peers, p)
				if gs.offerByPeer[p] > 0 {
					gs.offerByPeer[p]--
				}
			}
		}
		if len(peers) == 0 {
			gs.clearOffers(mid)
		}
	}
}

// retryOfferedPull runs on the router goroutine one window after an ask was granted. If the id
// is still missing and no ask is in flight, it re-asks the best remembered announcer:
// untried first, then least-recently tried, in deterministic peer order.
func (gs *GossipSubRouter) retryOfferedPull(mid string) {
	if gs.offerTable == nil {
		return
	}
	if gs.haveSeen(mid) {
		gs.clearOffers(mid)
		return
	}
	if at, ok := gs.iwantAsked[mid]; ok && time.Since(at) < gs.iwantWindow {
		return // an ask is in flight; its own retry timer will fire
	}
	offers := gs.offerTable[mid]
	if len(offers) == 0 {
		return
	}
	cands := make([]peer.ID, 0, len(offers))
	for p := range offers {
		if gs.peerParkedFor(p, mid) {
			continue
		}
		if gs.requestGate != nil && !gs.requestGate.Allow(p, gs.offerTopic[mid], mid) {
			continue
		}
		cands = append(cands, p)
	}
	if len(cands) == 0 {
		return
	}
	sort.Slice(cands, func(i, j int) bool { return cands[i] < cands[j] })
	tried := gs.offerTried[mid]
	pick, found := peer.ID(""), false
	var oldest time.Time
	for _, p := range cands {
		t, was := tried[p]
		if !was {
			pick, found = p, true
			break
		}
		if !found || t.Before(oldest) {
			pick, found, oldest = p, true, t
		}
	}
	if !found || !gs.iwantAllowed(mid, pick) {
		return
	}
	if gs.offerRetryCtr != nil {
		gs.offerRetryCtr.Add(1)
	}
	gs.iasked[pick]++
	gs.gossipTracer.AddPromise(pick, []string{mid})
	gs.noteIWantSent(pick, []string{mid})
	if gs.requestGate != nil {
		gs.requestGate.Committed(pick, []string{mid})
	}
	gs.sendRPC(pick, rpcWithControl(nil, nil, []*pb.ControlIWant{{MessageIDs: []string{mid}}}, nil, nil, nil), false)
}

// WithPullMemory keeps the offers the IWANT discipline could not act on (today they are
// dropped: relays announce once, so a blocked offer is usually gone for good) and re-drives
// them whenever a slot frees — delivery, window expiry (heartbeat sweep) or an enforcement
// refund. The re-drive path honors the request gate, the discipline window and parks. Distinct
// from WithPullBudget, which caps concurrency; memory changes only what is remembered.
func WithPullMemory(redrives *atomic.Int64) Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok {
			return fmt.Errorf("pull memory requires a gossipsub router")
		}
		gs.pullMemory = true
		gs.redriveCtr = redrives
		if gs.pullOutstanding == nil {
			gs.pullOutstanding = make(map[string]time.Time)
		}
		return nil
	}
}

// pullCandidate is a budget-declined pull, kept for re-drive.
type pullCandidate struct {
	mid   string
	topic string
	from  peer.ID
}

// pullBacklogCap bounds the backlog; oldest entries drop first.
const pullBacklogCap = 1024

// backlogPulls stores the candidates the budget could not admit, in the order given (rarity
// order when rarest-first is on), skipping anything already taken or seen.
func (gs *GossipSubRouter) backlogPulls(mids []string, topicOf map[string]string, from peer.ID, taken map[string]struct{}) {
	for _, mid := range mids {
		if _, ok := taken[mid]; ok {
			continue
		}
		if gs.haveSeen(mid) {
			continue
		}
		gs.pullBacklog = append(gs.pullBacklog, pullCandidate{mid: mid, topic: topicOf[mid], from: from})
	}
	if n := len(gs.pullBacklog) - pullBacklogCap; n > 0 {
		gs.pullBacklog = append(gs.pullBacklog[:0:0], gs.pullBacklog[n:]...)
	}
}

// redrivePulls issues IWANTs from the backlog while budget slots are free. Called on delivery
// (a delivery is what frees a slot) — this is the internal retry that keeps a binding budget
// from taxing the tail with a wait for an external re-offer. Prototype scope: it consults the
// request gate and the discipline window but bypasses the other request policies, and it asks
// the peer that announced the id when it was backlogged.
func (gs *GossipSubRouter) redrivePulls() {
	if (gs.pullBudgetMax <= 0 && !gs.pullMemory) || len(gs.pullBacklog) == 0 {
		return
	}
	remaining := 64 // memory-only: per-call work bound; iwantAllowed still gates per id
	if gs.pullBudgetMax > 0 {
		remaining = gs.pullBudgetMax - gs.pullOutstandingLive()
	}
	if remaining <= 0 {
		return
	}
	// Rarity is re-evaluated at re-drive, not frozen at backlog time: the estimate moves
	// while entries wait, and a freed slot should go to what is rare *now*. Stable, so FIFO
	// survives within a rarity class; gated on rarest-first so a budget alone keeps arrival
	// order rather than importing rarity policy through the back door. O(n log n) over a
	// capped backlog per re-drive — prototype-fine; a production form wants a lazy bucket
	// queue keyed on the announcer count.
	if gs.rarestFirst {
		sort.SliceStable(gs.pullBacklog, func(a, b int) bool {
			return gs.rarityCount(gs.pullBacklog[a].mid) < gs.rarityCount(gs.pullBacklog[b].mid)
		})
	}
	perPeer := make(map[peer.ID][]string)
	keep := gs.pullBacklog[:0:0]
	scanned := 0
	for remaining > 0 && scanned < len(gs.pullBacklog) {
		c := gs.pullBacklog[scanned]
		scanned++
		if gs.haveSeen(c.mid) {
			continue // delivered: drop for good
		}
		if gs.requestGate != nil && !gs.requestGate.Allow(c.from, c.topic, c.mid) {
			continue // gate refusal is final for this candidate
		}
		if gs.peerParkedFor(c.from, c.mid) {
			continue // parked announcer: never re-ask
		}
		if _, out := gs.pullOutstanding[c.mid]; out {
			keep = append(keep, c) // id in flight elsewhere: not eligible NOW, keep the offer
			continue
		}
		if !gs.iwantAllowed(c.mid, c.from) {
			keep = append(keep, c) // window still open: keep the offer for the next re-drive
			continue
		}
		perPeer[c.from] = append(perPeer[c.from], c.mid)
		gs.pullOutstanding[c.mid] = time.Now()
		if gs.redriveCtr != nil {
			gs.redriveCtr.Add(1)
		}
		remaining--
	}
	gs.pullBacklog = append(keep, gs.pullBacklog[scanned:]...)
	for p, mids := range perPeer {
		gs.iasked[p] += len(mids)
		gs.gossipTracer.AddPromise(p, mids)
		gs.noteIWantSent(p, mids)
		if gs.requestGate != nil {
			gs.requestGate.Committed(p, mids)
		}
		gs.sendRPC(p, rpcWithControl(nil, nil, []*pb.ControlIWant{{MessageIDs: mids}}, nil, nil, nil), false)
	}
}

// pullOutstandingLive sweeps expired entries and reports the live outstanding count.
func (gs *GossipSubRouter) pullOutstandingLive() int {
	window := gs.iwantWindow
	if window <= 0 {
		window = time.Second
	}
	for mid, at := range gs.pullOutstanding {
		if time.Since(at) >= window {
			delete(gs.pullOutstanding, mid)
		}
	}
	return len(gs.pullOutstanding)
}

// noteRarityAnnouncer records that p announced mid, for the rarest-first estimate.
func (gs *GossipSubRouter) noteRarityAnnouncer(mid string, p peer.ID) {
	if gs.rarityAnnounce == nil {
		return
	}
	e, ok := gs.rarityAnnounce[mid]
	if !ok {
		e = &phaseHolderEntry{peers: make(map[peer.ID]struct{}), added: time.Now()}
		gs.rarityAnnounce[mid] = e
	}
	e.peers[p] = struct{}{}
}

// rarityCount reports how many distinct peers have announced mid.
func (gs *GossipSubRouter) rarityCount(mid string) int {
	e, ok := gs.rarityAnnounce[mid]
	if !ok {
		return 0
	}
	return len(e.peers)
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

// RequestGate is an application veto on requesting announced message ids.
//
// The two methods exist because a one-shot boolean cannot both decide and account. Allow runs ahead
// of every request policy, and those policies may still refuse an id the gate approved; a gate that
// charged a budget inside Allow would spend it on ids no IWANT ever carried -- the same phantom-commit
// bug that made selection and dispatch one set in the first place. So Allow must be pure, and
// Committed reports exactly the ids that were dispatched.
type RequestGate interface {
	// Allow reports whether p's announcement of mid on topic may be requested now. It MUST NOT
	// record state: a false answer has to leave the id exactly as requestable as it was, since an
	// application that declines for want of authority will want the same id once authority arrives.
	Allow(p peer.ID, topic, mid string) bool
	// Committed reports the ids actually dispatched in an IWANT to p. This is the only place an
	// application may charge per-claim budgets or record provenance.
	Committed(p peer.ID, mids []string)
}

// RequestDeferral is the router-side ledger of announcements a gate declined, plus the trigger that
// replays them.
//
// The ledger lives in the router and the trigger belongs to the application, and the split is not
// arbitrary. Replaying a declined announcement needs the announcer's identity, the per-peer request
// allowance, the outstanding-request ledgers and the promise tracker -- all router state. Knowing
// *when* the answer changed needs the block, which only the application has. So the router
// remembers who offered what, and the application says when to ask again.
//
// Bounded on three axes, because a declined announcement is attacker-suppliable: total entries,
// announcers per entry, and age. Beyond the mcache's serve window an announcer could not serve the
// message anyway, so retention past it would be pure memory.
type RequestDeferral struct {
	max    int
	replay func()
}

// NewRequestDeferral returns a deferral ledger holding at most max declined ids.
func NewRequestDeferral(max int) *RequestDeferral {
	if max < 0 {
		max = 0
	}
	return &RequestDeferral{max: max}
}

// Replay asks the router to re-offer every deferred announcement to the gate and request those now
// permitted. Safe to call from any goroutine and from a timer; the work runs on the router's loop.
//
// This is the event the design turns on. Without it, recovery waits for heartbeat gossip, which
// fires once per heartbeat to a random subset of *non-mesh* peers -- it excludes mesh peers on the
// assumption that they are being pushed to, an assumption a receiver-side gate falsifies. Measured
// at n=30, that made the cost of gating flat in the authority delay and quantized to the heartbeat.
func (d *RequestDeferral) Replay() {
	if d == nil || d.replay == nil {
		return
	}
	d.replay()
}

// WithRequestGate installs a request gate, and optionally the deferral ledger that makes a decline
// recoverable. Returning false from Allow declines the id *without consuming any request state*, so
// the same id can be requested later when the answer changes.
//
// Declining is not the same as forgetting, and nothing re-delivers a declined announcement on its
// own: heartbeat gossip excludes mesh peers and the phase policy's immediate IHAVEs are one-shot.
// Passing a nil deferral is therefore a deliberate choice to measure the no-retry baseline.
func WithRequestGate(gate RequestGate, deferral *RequestDeferral) Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok {
			return fmt.Errorf("request gate requires a gossipsub router")
		}
		if gate == nil {
			return fmt.Errorf("request gate requires a non-nil gate")
		}
		gs.requestGate = gate
		if deferral == nil {
			return nil
		}
		gs.deferredMax = deferral.max
		gs.deferred = make(map[string]*deferredEntry)
		deferral.replay = func() {
			select {
			case gs.p.eval <- gs.replayDeferred:
			case <-gs.p.ctx.Done():
			}
		}
		return nil
	}
}

// deferredEntry is one declined announcement: who offered it, on what topic, and when it was first
// declined. `at` is set once and never refreshed, so a repeatedly re-announced id still expires.
type deferredEntry struct {
	topic string
	peers []peer.ID
	at    time.Time
}

// maxDeferredAnnouncers caps how many peers are remembered per declined id. Fallback across
// announcers matters (the first may disconnect or refuse to serve) but the tail is worthless.
const maxDeferredAnnouncers = 4

// noteDeclined records that p offered mid on topic and the gate said no. Called from the selection
// path, where the peer and topic are both in hand.
func (gs *GossipSubRouter) noteDeclined(p peer.ID, topic, mid string) {
	if gs.deferred == nil {
		return
	}
	e, ok := gs.deferred[mid]
	if !ok {
		if len(gs.deferred) >= gs.deferredMax {
			return
		}
		gs.deferred[mid] = &deferredEntry{topic: topic, peers: []peer.ID{p}, at: time.Now()}
		return
	}
	if len(e.peers) >= maxDeferredAnnouncers {
		return
	}
	for _, have := range e.peers {
		if have == p {
			return
		}
	}
	e.peers = append(e.peers, p)
}

// replayDeferred re-offers deferred ids to the gate and requests those now permitted. Runs on the
// router's loop.
//
// It goes through the ordinary selection path rather than around it, so per-peer allowances, the
// outstanding-request discipline and the promise tracker all apply exactly as they would to a fresh
// announcement. A replay that bypassed them would be a second, unaccounted request channel.
func (gs *GossipSubRouter) replayDeferred() {
	if gs.deferred == nil || gs.requestGate == nil {
		return
	}
	ttl := time.Duration(gs.params.HistoryLength) * gs.params.HeartbeatInterval
	announcersOf := make(map[string][]peer.ID, len(gs.deferred))
	byPeer := make(map[peer.ID]struct{})
	topicOf := make(map[string]string, len(gs.deferred))
	for mid, e := range gs.deferred {
		// Expired, or it actually arrived while we waited -- either way stop tracking it.
		//
		// "Arrived" means *delivered*, not *seen*. A message is marked seen before application
		// validation, so an id the application declined and did not retain is seen while having
		// reached nobody: testing seenMessage here discarded exactly the ids most in need of a
		// retry, and did so silently. deliveredMessages is the distinction the library already
		// keeps for this reason.
		if time.Since(e.at) >= ttl || gs.p.deliveredMessages.Has(mid) {
			delete(gs.deferred, mid)
			continue
		}
		topicOf[mid] = e.topic
		for _, pid := range e.peers {
			if _, connected := gs.p.peers[pid]; !connected {
				continue
			}
			announcersOf[mid] = append(announcersOf[mid], pid)
			byPeer[pid] = struct{}{}
		}
	}
	// Assign each id to ONE announcer, balancing across announcers, and only then request.
	//
	// The obvious implementation -- walk the peers and ask each for everything it announced -- is
	// strongly source-concentrating: the peers that announced during the closed window are exactly
	// the ones that get asked for the whole group, so the replay swamps a handful of uplinks. That
	// is worse than not retrying at all in a pull-only arm, where every byte moves by IWANT
	// (measured: pull-only completion 2.2s -> 2.7s across three seeds, with duplicate counts and
	// per-node bytes flat, so it was load placement rather than volume). Heartbeat rediscovery
	// happens to draw a fresh random subset each round and is therefore source-diversifying by
	// accident; a replay has to be so on purpose.
	ids := make([]string, 0, len(byPeer))
	for mid := range topicOf {
		if len(announcersOf[mid]) > 0 {
			ids = append(ids, mid)
		}
	}
	gs.shuffleStrings(ids)
	load := make(map[peer.ID]int, len(byPeer))
	assigned := make(map[peer.ID][]string, len(byPeer))
	for _, mid := range ids {
		cands := announcersOf[mid]
		best, bestLoad := peer.ID(""), -1
		for _, pid := range cands {
			if l := load[pid]; bestLoad < 0 || l < bestLoad {
				best, bestLoad = pid, l
			}
		}
		if bestLoad < 0 {
			continue
		}
		load[best]++
		assigned[best] = append(assigned[best], mid)
	}
	for pid, mids := range assigned {
		allowance := gs.params.MaxIHaveLength - gs.iasked[pid]
		if allowance <= 0 {
			continue
		}
		// Shuffle before selection, not after: the policies consulted inside record state as a side
		// effect of allowing an id, so selecting freely and truncating would leave phantom entries.
		gs.shuffleStrings(mids)
		lst := gs.selectIWants(mids, topicOf, pid, allowance)
		if len(lst) == 0 {
			continue
		}
		gs.iasked[pid] += len(lst)
		gs.gossipTracer.AddPromise(pid, lst)
		gs.noteIWantSent(pid, lst)
		gs.requestGate.Committed(pid, lst)
		gs.sendRPC(pid, rpcWithControl(nil, nil, []*pb.ControlIWant{{MessageIDs: lst}}, nil, nil, nil), false)
		for _, mid := range lst {
			delete(gs.deferred, mid)
		}
	}
}
