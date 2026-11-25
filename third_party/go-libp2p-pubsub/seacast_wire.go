package pubsub

// SEACAST's messages on v0.3's wire (seacast.md §7):
// a holding with its two flags and a WANT with its two fields get encodings of their
// own, version 4 of the HAVE and WANT pseudo ids, since the existing version byte holds exact tags
// (1 a bitmap, 2 a bitmap with a load byte, 3 a claim without holding). Offers keep version 1.
//
//	holding v4: "HAVE" ‖ 0x04 ‖ root (32) ‖ flags (1) ‖ bitmap     flags: bit 0 claim_all, bit 1 complete
//	WANT v4:    "WANT" ‖ 0x04 ‖ root (32) ‖ index (4, little-endian) ‖ flags (1) ‖ hops (1)   flags: bit 0 standing
//
// The bitmap is today's (least-significant bit first, trailing zero bytes trimmed, at most
// haveMaxBytes); the WANT's length is exact. A codec-malformed entry (the magic with a version no
// path recognises, a WANT of the wrong length, a holding over the cap, a reserved bit set, hops
// over the bound) is ignored, counted, and the rest of its RPC is kept, without penalty; the
// draft's semantic failures (valid_have) are the receiving path's and penalise. A control resolves
// to its root's owning path: a v4 entry for a root a type owns is the path's; one for a root the
// phase path owns is ignored and counted; one for a root nobody has seen yet is absorbed, since a
// holding may precede the data on a link.

import (
	"fmt"
	"math/bits"

	"github.com/libp2p/go-libp2p/core/peer"
)

const (
	seacastVersion = 4
	// The holding's flags.
	seacastFlagClaimAll = 1 << 0
	seacastFlagComplete = 1 << 1
	seacastHaveFlags    = seacastFlagClaimAll | seacastFlagComplete
	seacastHaveHeadLen  = haveHeaderLen + 1
	// The WANT's flags and the codec's cap on its hops. The receiver takes no routed request for a
	// root of its types (seacastWantIn: hops above zero is malformed once the root's type is known);
	// the cap only keeps a runaway byte for a root nobody has seen yet from counting as a WANT.
	seacastFlagStanding = 1 << 0
	// seacastFlagProbe marks the probe of a claim (seacast.md §2's probe field, §4.6's R5): the
	// claimant answers the first such ask per asker and group, of an index it claims, whatever the
	// asker's holdings say.
	seacastFlagProbe = 1 << 1
	seacastWantFlags = seacastFlagStanding | seacastFlagProbe
	seacastWantLen   = wantLen + 2 // 43: the magic, the version, the root, the index, the flags, the hops
	seacastMaxHops   = 8
)

// encodeHaveV2 is the holding pseudo id for root's bitmap with its two flags.
func encodeHaveV2(root string, bits *haveBits, claimAll, complete bool) string {
	var flags byte
	if claimAll {
		flags |= seacastFlagClaimAll
	}
	if complete {
		flags |= seacastFlagComplete
	}
	out := make([]byte, 0, seacastHaveHeadLen+len(bits.b))
	out = append(out, haveMagic...)
	out = append(out, seacastVersion)
	out = append(out, root...)
	out = append(out, flags)
	out = append(out, bits.Bytes()...)
	return string(out)
}

// decodeHaveV2 reads a v4 holding. ok is false for any other id; malformed is true for a v4
// holding that fails the codec (a reserved flag bit, a bitmap over the cap).
func decodeHaveV2(mid string) (root string, claimAll, complete bool, bitmap []byte, ok, malformed bool) {
	if len(mid) < seacastHaveHeadLen || mid[:len(haveMagic)] != haveMagic || mid[len(haveMagic)] != seacastVersion {
		return "", false, false, nil, false, false
	}
	root = mid[len(haveMagic)+1 : haveHeaderLen]
	flags := mid[haveHeaderLen]
	if flags&^seacastHaveFlags != 0 || len(mid)-seacastHaveHeadLen > haveMaxBytes {
		return root, false, false, nil, true, true
	}
	return root, flags&seacastFlagClaimAll != 0, flags&seacastFlagComplete != 0, []byte(mid[seacastHaveHeadLen:]), true, false
}

// encodeWantV2 is the WANT pseudo id for an ask by index with its two fields.
func encodeWantV2(root string, index uint32, standing bool, hops uint8) string {
	var flags byte
	if standing {
		flags |= seacastFlagStanding
	}
	return encodeWantV2Flags(root, index, flags, hops)
}

// encodeWantV2Probe is the probe's WANT: an ask by index with the probe flag, neither standing nor
// routed.
func encodeWantV2Probe(root string, index uint32) string {
	return encodeWantV2Flags(root, index, seacastFlagProbe, 0)
}

// wantV2Probe reports whether mid is a well-formed v4 WANT with the probe flag.
func wantV2Probe(mid string) bool {
	if _, _, _, _, ok, malformed := decodeWantV2(mid); !ok || malformed {
		return false
	}
	return mid[len(wantMagic)+1+haveRootLen+4]&seacastFlagProbe != 0
}

// encodeWantV2Flags is the v4 WANT with its flags byte as given.
func encodeWantV2Flags(root string, index uint32, flags byte, hops uint8) string {
	out := make([]byte, 0, seacastWantLen)
	out = append(out, wantMagic...)
	out = append(out, seacastVersion)
	out = append(out, root...)
	out = append(out, byte(index), byte(index>>8), byte(index>>16), byte(index>>24)) // little-endian, as the binding's
	out = append(out, flags, hops)
	return string(out)
}

// decodeWantV2 reads a v4 WANT. ok is false for any other id; malformed is true for a v4 WANT of
// the wrong length, with a reserved flag bit, or with hops over the bound.
func decodeWantV2(mid string) (root string, index uint32, standing bool, hops uint8, ok, malformed bool) {
	if len(mid) < len(wantMagic)+1 || mid[:len(wantMagic)] != wantMagic || mid[len(wantMagic)] != seacastVersion {
		return "", 0, false, 0, false, false
	}
	if len(mid) != seacastWantLen {
		return "", 0, false, 0, true, true
	}
	root = mid[len(wantMagic)+1 : len(wantMagic)+1+haveRootLen]
	b := []byte(mid[len(wantMagic)+1+haveRootLen:])
	index = uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24
	flags, hops := b[4], b[5]
	if flags&^seacastWantFlags != 0 || hops > seacastMaxHops {
		return root, index, false, 0, true, true
	}
	return root, index, flags&seacastFlagStanding != 0, hops, true, false
}

// seacastPseudoUnknown reports an id that carries one of the two magics with a version no path
// recognises: ignored by every path, counted, never treated as an ordinary id.
func seacastPseudoUnknown(mid string) bool {
	if len(mid) <= len(haveMagic) {
		return false
	}
	v := mid[len(haveMagic)]
	switch mid[:len(haveMagic)] {
	case haveMagic:
		return v != haveVersion && v != seacastVersion
	case wantMagic:
		return v != haveVersion && v != seacastVersion
	}
	return false
}

// seacastIsV4Have says whether mid carries the HAVE magic with version 4, whatever follows.
func seacastIsV4Have(mid string) bool {
	return len(mid) > len(haveMagic) && mid[:len(haveMagic)] == haveMagic && mid[len(haveMagic)] == seacastVersion
}

// seacastBitsBeyond says whether a holding's bitmap for root names an index at or beyond the
// root's type's IndexCount: malformed. False when the root's type is
// unknown or gives no count, and off the path. The bitmap is least-significant bit first.
func (gs *GossipSubRouter) seacastBitsBeyond(root string, enc []byte) bool {
	T := gs.seacastRootType(root)
	if T == nil {
		return false
	}
	n := T.IndexCount(root)
	if n <= 0 {
		return false
	}
	for i := len(enc) - 1; i >= 0; i-- {
		if enc[i] != 0 {
			return i*8+bits.Len8(enc[i])-1 >= n
		}
	}
	return false
}

// seacastOfferIgnored classifies an IHAVE entry for handleIHave: offers keep version 1,
// so a holding of version 4 or a pseudo id of a version no path recognises in an offer is
// ignored and counted, never an announce to ask for. Off the path nothing changes.
func (gs *GossipSubRouter) seacastOfferIgnored(mid string) bool {
	if gs.seacast == nil {
		return false
	}
	if seacastPseudoUnknown(mid) || seacastIsV4Have(mid) {
		gs.seacast.ctr.Malformed.Add(1)
		return true
	}
	// v0.10, carried by the draft (spec f6c19db): an offer's bitmap is held to a holding's limits
	// (seacast-lite.md §4.4's on_ihave, seacast.md §4.3's on_offer): one naming an index at or beyond
	// its group's known count is ignored and counted against its sender.
	// The count is known once the group's header is (the type's HeaderKnown); before that an offer is
	// recorded whatever it names, and what named an index beyond the count goes when it becomes known.
	if root, enc, ok := decodeHave(mid); ok {
		if gs.seacastCountKnown(root) && gs.seacastBitsBeyond(root, enc) {
			gs.seacast.ctr.Malformed.Add(1)
			return true
		}
		return false
	}
	// A chunk's id must be a valid one (seacast-lite.md §4.4's valid_id): an index at or beyond its group's
	// known count is ignored, without penalty. Only an ordinary id is read so: a pseudo id is the
	// bitmap's above or an ask's, never parsed as a chunk's.
	if _, _, isWant := decodeWant(mid); isWant || gs.have == nil {
		return false
	}
	if root, index, ok := gs.have.index(mid); ok && gs.seacastCountKnown(root) {
		if n := gs.seacastRootType(root).IndexCount(root); n > 0 && int64(index) >= int64(n) {
			return true
		}
	}
	return false
}

// seacastCountKnown says whether root's index count is known: a path root whose header is known.
func (gs *GossipSubRouter) seacastCountKnown(root string) bool {
	T := gs.seacastRootType(root)
	return T != nil && T.HeaderKnown(root)
}

// seacastHaveRoot is the root of a holding or offer pseudo id of any version the paths know,
// for the coalescer's keying; ok is false for an ordinary id.
func seacastHaveRoot(mid string) (string, bool) {
	if root, _, ok := decodeHave(mid); ok {
		return root, true
	}
	if root, _, _, _, ok, _ := decodeHaveV2(mid); ok {
		return root, true
	}
	return "", false
}

// --- the root's owning path

// seacastNoteRoot records that root belongs to the type owning topic, at the first message of the
// group the router sees on the topic (a publish or an arrival). Nothing for an unowned topic.
func (gs *GossipSubRouter) seacastNoteRoot(topic, mid string) {
	if gs.seacast == nil || gs.have == nil {
		return
	}
	T := gs.seacast.types[topic]
	if T == nil {
		return
	}
	root, _, ok := gs.have.index(mid)
	if !ok || len(root) != haveRootLen {
		return
	}
	if gs.seacast.roots == nil {
		gs.seacast.roots = map[string]ObjectType{}
		gs.seacast.rootTopic = map[string]string{}
	}
	if _, known := gs.seacast.roots[root]; !known {
		gs.seacast.roots[root] = T
		gs.seacast.rootTopic[root] = topic
		gs.seacast.noteRegSlot(root)
	}
	gs.seacast.noteKnown(root) // known_at: the first sight (seacast_known.go)
}

// noteRegSlot records the node's slot as a newly registered root's, once the node has one; the
// first advance gives it to the roots registered before (seacastAdvanceSlot).
func (sc *seacast) noteRegSlot(root string) {
	if !sc.slotKnown {
		return
	}
	if sc.regSlot == nil {
		sc.regSlot = map[string]uint64{}
	}
	sc.regSlot[root] = sc.slot
}

// seacastRootOwner is the type owning root if the path has it registered, nil otherwise. Unlike
// seacastRootType it never registers: what only clears a root's state (the holdings' expiry) must
// not take up again a root the window has forgotten.
func (gs *GossipSubRouter) seacastRootOwner(root string) ObjectType {
	if gs.seacast == nil {
		return nil
	}
	return gs.seacast.roots[root]
}

// seacastRootType is the type owning root, nil when the root is unknown or the phase path's. A
// root no chunk has reached the node yet (a WANT names it first: a sampler's at block arrival, a
// routed request at a forwarder, §4.10) is placed by the types' TopicOf and registered.
func (gs *GossipSubRouter) seacastRootType(root string) ObjectType {
	sc := gs.seacast
	if sc == nil {
		return nil
	}
	if T := sc.roots[root]; T != nil {
		return T
	}
	if len(root) != haveRootLen || gs.seacastRootKnownOff(root) {
		return nil
	}
	for topic, T := range sc.types {
		if t, ok := T.TopicOf(root); ok && t == topic {
			if sc.roots == nil {
				sc.roots = map[string]ObjectType{}
				sc.rootTopic = map[string]string{}
			}
			sc.roots[root] = T
			sc.rootTopic[root] = topic
			sc.noteRegSlot(root)
			sc.noteKnown(root) // known_at: a WANT or an offer naming it first (seacast_known.go)
			return T
		}
	}
	return nil
}

// seacastRootKnownOff says whether root is known and not the path's: a group the router has seen
// on an unowned topic (the phase path's). The router keeps the roots it learns on unowned topics
// in offRoots, so that a v4 entry for one is refused rather than absorbed.
func (gs *GossipSubRouter) seacastRootKnownOff(root string) bool {
	if gs.seacast == nil || gs.seacast.offRoots == nil {
		return false
	}
	return gs.seacast.offRoots[root]
}

// seacastNoteOffRoot records a root seen on an unowned topic.
func (gs *GossipSubRouter) seacastNoteOffRoot(topic, mid string) {
	if gs.seacast == nil || gs.have == nil {
		return
	}
	if _, owned := gs.seacast.types[topic]; owned {
		return
	}
	root, _, ok := gs.have.index(mid)
	if !ok || len(root) != haveRootLen {
		return
	}
	if gs.seacast.offRoots == nil {
		gs.seacast.offRoots = map[string]bool{}
	}
	gs.seacast.offRoots[root] = true
}

// seacastHaveFlags says how a holding of root is encoded on the wire: v4 with its flags on the
// path's roots, today's v1 otherwise (v4 false). claimAll says every index of the group is held
// (the decode claim in this step: the whole bitmap); complete, in this step, says the same (the
// interest's satisfaction per type comes with part 4's completion). Router goroutine: every writer
// of a holding (the fill's, the sender's acknowledgement, the queue's attachment at the pend, the
// source's batch holding) decides here, so a path root leaves as v4 from every one of them.
func (gs *GossipSubRouter) seacastHaveFlags(root string, bits *haveBits, claimAll bool) (v4 bool, flags byte) {
	T := gs.seacastRootType(root)
	if T == nil {
		return false, 0
	}
	if sc := gs.seacast; sc != nil && sc.machine {
		// The machine's two claims (seacast_complete.go, §4.2's holding_of): claim_all for a group
		// being decoded or held whole, complete for a group whose asks stopped.
		g := sc.groups[root]
		if claimAll || bits.Count() >= T.IndexCount(root) || (g != nil && g.state == seacastDecoding) {
			flags |= seacastFlagClaimAll
		}
		if g != nil && g.stopPull {
			flags |= seacastFlagComplete
		}
		return true, flags
	}
	if !claimAll && bits.Count() >= T.IndexCount(root) {
		claimAll = true // the whole bitmap held: the decode claim of this step
	}
	if claimAll {
		flags = seacastFlagClaimAll | seacastFlagComplete
	}
	return true, flags
}

// seacastEncodeHave is the holding's encoding for root (seacastHaveFlags).
func (gs *GossipSubRouter) seacastEncodeHave(root string, bits *haveBits, claimAll bool) string {
	if sc := gs.seacast; sc != nil && sc.fault == seacastFaultClaim {
		// The liar (seacast_fault.go): every index of a coded group it takes part in, claimed.
		if T := gs.seacastRootType(root); T != nil && T.Threshold(root) < T.IndexCount(root) {
			if want := gs.seacastInterestOfPeer(gs.p.host.ID(), root); want != nil && want.Count() > 0 {
				full := fullHaveBits(uint32(T.IndexCount(root))) // lint:ignore uintcast -- an index count.
				return encodeHaveV2(root, full, true, true)
			}
		}
	}
	v4, flags := gs.seacastHaveFlags(root, bits, claimAll)
	if !v4 {
		return encodeHave(root, bits)
	}
	return encodeHaveV2(root, bits, flags&seacastFlagClaimAll != 0, flags&seacastFlagComplete != 0)
}

// seacastAbsorbV2 absorbs a v4 holding from p into the ledger: the bits as a holding (as v1's),
// the flags into the entry. Called for an id carrying the HAVE magic with version 4; such an id is
// the path's whatever follows, so handled is true in every case (absorbed, refused, or malformed:
// too short for the header, a reserved bit, over the cap, a bit beyond the type's IndexCount), and
// the caller is done with it: it is never stored as an ordinary id.
func (gs *GossipSubRouter) seacastAbsorbV2(p peer.ID, mid string) (handled bool) {
	ctr := gs.seacastCounters()
	root, claimAll, complete, enc, ok, malformed := decodeHaveV2(mid)
	if !ok || malformed {
		ctr.Malformed.Add(1) // !ok here is a v4 id too short for its header
		return true
	}
	if gs.seacastRootKnownOff(root) {
		ctr.OffPath.Add(1) // a v4 holding for a group of the phase path: refused, counted
		return true
	}
	if gs.seacastBitsBeyond(root, enc) {
		ctr.Malformed.Add(1)
		return true
	}
	h := gs.have
	if h == nil {
		return true
	}
	roots := h.peers[p]
	if roots == nil {
		roots = make(map[string]*haveEntry)
		h.peers[p] = roots
	}
	e := roots[root]
	if e == nil {
		e = &haveEntry{}
		roots[root] = e
	}
	e.claimAll = e.claimAll || claimAll // [c10] a later holding never retracts a claim (draft §4.3)
	e.complete = e.complete || complete
	ctr.HoldingsV4.Add(1)
	// The bits as a v1 holding would be absorbed: through haveAbsorb's holding branch on the v1
	// encoding of the same bitmap, so every consumer (holders, the ledger, the writer's copy,
	// the known ids) sees it once.
	var bits haveBits
	bits.Or(enc)
	gs.haveAbsorb(p, encodeHave(root, &bits))
	if claimAll {
		// (R5) under the machine: a deferral on the claim. On every claiming holding, not only a peer's first
		// claim of the group (seacast.md §4.3's on_holding, spec 43fe953): a claim that came before our duty
		// was armed defers it when the next one comes.
		gs.seacastNoteClaim(p, root)
	}
	return true
}

// seacastCounters is the path's counters, a shared zero value off the path.
func (gs *GossipSubRouter) seacastCounters() *SeacastCounters {
	if gs.seacast == nil {
		return &seacastNoCounters
	}
	return gs.seacast.ctr
}

var seacastNoCounters SeacastCounters

// seacastWantIn classifies an ask's id for handleIWant: a v4 WANT is resolved as the indexed ask
// it carries, with its standing flag kept for the handler (answered when the chunk arrives,
// seacast_standing.go; counted), a malformed one or one for a root of the phase path is dropped
// (counted), an unknown-version pseudo id is dropped (counted). resolved is the v1 WANT id the rest
// of the handler resolves; drop says the entry is done with. A v1 ask carries no flag.
func (gs *GossipSubRouter) seacastWantIn(mid string) (resolved string, standing bool, probe bool, drop bool) {
	ctr := gs.seacastCounters()
	if seacastPseudoUnknown(mid) {
		ctr.Malformed.Add(1)
		return "", false, false, true
	}
	root, index, standing, hops, ok, malformed := decodeWantV2(mid)
	if !ok {
		return mid, false, false, false
	}
	if malformed {
		ctr.Malformed.Add(1)
		return "", false, false, true
	}
	if gs.seacastRootKnownOff(root) {
		ctr.OffPath.Add(1)
		return "", false, false, true
	}
	if T := gs.seacastRootType(root); T != nil && hops > 0 {
		// A routed request: the port's types take none (the payload's ROUTE_HOPS is 0, and no node
		// samples it), so hops above zero is malformed at the receiver.
		ctr.Malformed.Add(1)
		return "", false, false, true
	}
	ctr.WantsV4.Add(1)
	if standing {
		ctr.WantsStanding.Add(1) // the kinds apart (the reporting path)
	}
	return encodeWant(root, index), standing, wantV2Probe(mid), false
}

func (sc *seacast) String() string {
	return fmt.Sprintf("seacast{types %d, roots %d}", len(sc.types), len(sc.roots))
}
