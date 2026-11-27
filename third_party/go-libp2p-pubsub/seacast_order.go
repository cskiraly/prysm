package pubsub

import (
	"fmt"
	"sync/atomic"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
)

// --- The ordering checks (seacast.md §4.2 and §4.6) ---
//
// Each check runs at the act's site against the node's own state at that instant and counts a
// violation; every count is want 0 on an honest node. A faulty node's acts (seacast_fault.go) count
// in the OrdF twins, so they are attributed to the fault and never to the honest counts.
//   (i)   no cell filed, sent or acknowledged before its group's header validated (HeaderKnown);
//   (ii)  no holding for a group the node holds nothing of; no claim_all with fewer than K held and
//         no successful decode of the group;
//   (iii) no rebuilt cell sent unless its decode succeeded from K held (the level-2 decode installed);
//   (iv)  no probe asked of a peer whose claim for the group the node had not received (the ledger's
//         claim_all for the peer and the group at the ask's instant).
// The header (i) is the block's: a validated cell has it, and a node's own publication has it by
// construction (the source made the header, §4.7).
// The rebuilders' per-event conditions: no decode started short of K or on a group already whole;
// no rebuilt cell sent after its group expired.
// The sends are checked where every RPC enters the writers (sendRPC) and where a holding is parked
// to ride the next RPC (havePendAll); the fill at seacastAccepted; the decode at seacastDecodeStart;
// the probe at its ask (seacastDeferOnClaim).

const (
	ordBeforeHeader = iota
	ordHoldingUnheld
	ordClaimShort
	ordRebuiltEarly
	ordProbeBeforeClaim
	ordDecodeShort
	ordDecodeWhole
	ordRebuiltExpired
)

// ordCounter is the counter of kind for this node: the OrdF twin on a faulty node.
func (c *SeacastCounters) ordCounter(kind int, faulty bool) *atomic.Int64 {
	honest := [...]*atomic.Int64{&c.OrdBeforeHeader, &c.OrdHoldingUnheld, &c.OrdClaimShort, &c.OrdRebuiltEarly,
		&c.OrdProbeBeforeClaim, &c.OrdDecodeShort, &c.OrdDecodeWhole, &c.OrdRebuiltExpired}
	twin := [...]*atomic.Int64{&c.OrdFBeforeHeader, &c.OrdFHoldingUnheld, &c.OrdFClaimShort, &c.OrdFRebuiltEarly,
		&c.OrdFProbeBeforeClaim, &c.OrdFDecodeShort, &c.OrdFDecodeWhole, &c.OrdFRebuiltExpired}
	if faulty {
		return twin[kind]
	}
	return honest[kind]
}

// ord counts one violation of kind. Router goroutine.
func (sc *seacast) ord(kind int) {
	sc.ctr.ordCounter(kind, sc.fault != seacastFaultNone).Add(1)
}

// seacastHeldCount is how many indices of root the node holds. Router goroutine.
func (gs *GossipSubRouter) seacastHeldCount(root string) int {
	if m := gs.have.mine[seacastGID(root)]; m != nil {
		return m.Bits.Count()
	}
	return 0
}

// seacastOrderSend checks an RPC entering p's writer: its chunks on the path's topics ((i), (iii),
// the expiry) and its holdings ((i), (ii)). Router goroutine.
func (gs *GossipSubRouter) seacastOrderSend(p peer.ID, out *RPC) {
	sc := gs.seacast
	if sc == nil || gs.have == nil || out == nil {
		return
	}
	for _, m := range out.GetPublish() {
		root, index, ok := gs.seacastOrderIndex(m)
		if !ok {
			continue
		}
		// The type: the topic's, or the one a rebuilt chunk's root resolves to (a decoder repairs
		// columns whose topics it does not run, seacastRepairType).
		T := gs.seacastType(m.GetTopic())
		if T == nil {
			if T = gs.seacastRepairType(root); T == nil {
				continue
			}
		}
		if !gs.seacastHeaderValid(T, root, index) {
			sc.ord(ordBeforeHeader)
		}
		if b := sc.rebuilt[root]; b != nil && b.Has(index) {
			if ok := sc.rebuiltOK[root]; ok == nil || !ok.Has(index) {
				sc.ord(ordRebuiltEarly)
			}
			if expired, _ := gs.seacastRootState(root); expired {
				sc.ord(ordRebuiltExpired)
			}
		} else if g := sc.rebuiltGone[root]; g != nil && g.Has(index) {
			sc.ord(ordRebuiltExpired) // rebuilt here, its group since expired or dropped
		}
	}
	ctl := out.GetControl()
	if ctl == nil {
		return
	}
	for _, dw := range ctl.GetIdontwant() {
		for _, id := range dw.GetMessageIDs() {
			root, claimAll, _, _, isV4, malformed := decodeHaveV2(id)
			if !isV4 || malformed {
				var ok bool
				if root, _, ok = decodeHave(id); !ok {
					// An ordinary chunk id as the acknowledgement (haveIDontWantIDs keeps it where
					// the node has no holding entry for the root): the cell itself held, and its
					// header. A hose stamp or another id indexes to no path root.
					if r, idx, ok := gs.have.index(id); ok && len(r) == haveRootLen {
						if T := gs.seacastRootType(r); T != nil {
							if !gs.seacastHeaderValid(T, r, idx) {
								sc.ord(ordBeforeHeader)
							}
							if m := gs.have.mine[seacastGID(r)]; m == nil || !m.Bits.Has(idx) {
								sc.ord(ordHoldingUnheld)
							}
						}
					}
					continue
				}
				claimAll = false
			}
			gs.seacastOrderHolding(root, claimAll)
		}
	}
}

// seacastOrderProbe checks a probe at its ask: (iv) the claimant's claim for root received. At the
// probe's own site (seacastDeferOnClaim): the probe's id is a WANT of the cell, the same id the
// node's ordinary asks of it carry. Router goroutine.
func (gs *GossipSubRouter) seacastOrderProbe(q peer.ID, root string) {
	if e := gs.have.peers.Of(q, seacastGID(root)); e == nil || !e.ClaimAll {
		gs.seacast.ord(ordProbeBeforeClaim)
	}
}

// seacastOrderHolding checks one holding the node sends for root: (i) the header, (ii) something
// held, a claim_all from K held or a successful decode. Roots outside the path are not checked.
// Router goroutine.
func (gs *GossipSubRouter) seacastOrderHolding(root string, claimAll bool) {
	sc := gs.seacast
	T := gs.seacastRootType(root)
	if T == nil {
		return
	}
	if !gs.seacastHeaderValidAny(T, root) {
		sc.ord(ordBeforeHeader)
	}
	held := gs.seacastHeldCount(root)
	if held == 0 {
		sc.ord(ordHoldingUnheld)
	}
	if claimAll && held < T.Threshold(root) {
		if g := sc.group(root); g == nil || !g.DecodedOK {
			sc.ord(ordClaimShort)
		}
	}
}

// seacastOrderFill checks a fill (seacastAccepted) against the header: (i). Router goroutine.
func (gs *GossipSubRouter) seacastOrderFill(T ObjectType, root string, index uint32) {
	if gs.seacast != nil && !gs.seacastHeaderValid(T, root, index) {
		gs.seacast.ord(ordBeforeHeader)
	}
}

// seacastHeaderValid is (i)'s gate for one cell: its group's header validated, or the group one of
// the node's own publications. Router goroutine.
func (gs *GossipSubRouter) seacastHeaderValid(T ObjectType, root string, index uint32) bool {
	if T.HeaderKnown(root) {
		return true
	}
	_, own := gs.seacast.ownRoots[root]
	return own
}

// seacastOrderIndex is a published chunk's (root, index), through a one-entry cache: a push of
// the same message to several peers enters the writers once per peer, back to back, and its id is
// computed once. Router goroutine.
func (gs *GossipSubRouter) seacastOrderIndex(m *pb.Message) (string, uint32, bool) {
	sc := gs.seacast
	if sc.ordLast != m {
		root, index, ok := gs.have.index(gs.p.idGen.RawID(m)) // as the writers' index does (seacast.go)
		sc.ordLast, sc.ordLastRoot, sc.ordLastIndex, sc.ordLastOK = m, root, index, ok && len(root) == haveRootLen
	}
	return sc.ordLastRoot, sc.ordLastIndex, sc.ordLastOK
}

// seacastHeaderValidAny is (i)'s gate for a holding of root: some held cell passes
// seacastHeaderValid. Router goroutine.
func (gs *GossipSubRouter) seacastHeaderValidAny(T ObjectType, root string) bool {
	if T.HeaderKnown(root) {
		return true
	}
	if m := gs.have.mine[seacastGID(root)]; m != nil {
		for i := range T.IndexCount(root) {
			if idx := uint32(i); m.Bits.Has(idx) && gs.seacastHeaderValid(T, root, idx) { // lint:ignore uintcast -- an index below the group's count.
				return true
			}
		}
	}
	_, own := gs.seacast.ownRoots[root]
	return own
}

// seacastNoteOwn records the root of one of the node's own publications. Router goroutine.
func (gs *GossipSubRouter) seacastNoteOwn(mid string) {
	if gs.have == nil {
		return
	}
	root, _, ok := gs.have.index(mid)
	if !ok || len(root) != haveRootLen {
		return
	}
	sc := gs.seacast
	if sc.ownRoots == nil {
		sc.ownRoots = map[string]struct{}{}
	}
	sc.ownRoots[root] = struct{}{}
}

// seacastOrderDecode checks a decode's start against the held count: never short of K, never on a
// group already whole. Router goroutine.
func (gs *GossipSubRouter) seacastOrderDecode(T ObjectType, root string, held int) {
	sc := gs.seacast
	if held < T.Threshold(root) {
		sc.ord(ordDecodeShort)
	}
	if held >= T.IndexCount(root) {
		sc.ord(ordDecodeWhole)
	}
}

// markRebuiltOK records index of root as rebuilt by a decode that succeeded from K held. Router
// goroutine.
func (sc *seacast) markRebuiltOK(root string, index uint32) {
	if sc.rebuiltOK == nil {
		sc.rebuiltOK = map[string]*haveBits{}
	}
	b := sc.rebuiltOK[root]
	if b == nil {
		b = &haveBits{}
		sc.rebuiltOK[root] = b
	}
	b.Set(index)
}

// orderForget drops root's ordering state with the group, keeping its rebuilt indices apart
// (rebuiltGone, bounded) so that a rebuilt chunk sent after the group expired or was dropped is still
// seen as one. Called before the group's rebuilt indices go. Router goroutine.
func (sc *seacast) orderForget(root string) {
	if b := sc.rebuilt[root]; b != nil {
		if sc.rebuiltGone == nil || len(sc.rebuiltGone) >= 4096 {
			sc.rebuiltGone = map[string]*haveBits{} // bounded: the oldest evidence goes first, wholesale
		}
		sc.rebuiltGone[root] = b
	}
	delete(sc.rebuiltOK, root)
	delete(sc.ownRoots, root)
}

// SeacastEvent names one of the group machine's per-group events, for an observer that compares
// them group by group: the probe asked, answered or broken, a claim
// verified (ending the duty), the claimant penalised, the late decode
// started, a decode installed or failed.
type SeacastEvent int

const (
	SeacastEvProbeAsked SeacastEvent = iota
	SeacastEvProbeAnswered
	SeacastEvProbeBroken
	SeacastEvVerified
	SeacastEvPenalty
	SeacastEvLateDecode
	SeacastEvDecodeDone
	SeacastEvDecodeFailed
	// SeacastEvVerdictPassed: the probe's verdict, taken at reconsider, passed on the claimant's own
	// copy (seacast.md §4.6, R5); a failed verdict is SeacastEvPenalty.
	SeacastEvVerdictPassed
)

// WithSeacastEvents installs an observer of the group machine's per-group events, called on the
// router's goroutine beside the counters; it must not block. Requires WithSeacastMachine first.
func WithSeacastEvents(fn func(kind SeacastEvent, root string)) Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok || gs.seacast == nil || !gs.seacast.machine {
			return fmt.Errorf("the machine's events require WithSeacastMachine first")
		}
		gs.seacast.events = fn
		return nil
	}
}

// event tells the observer, if any. Router goroutine.
func (sc *seacast) event(kind SeacastEvent, root string) {
	if sc.events != nil {
		sc.events(kind, root)
	}
}
