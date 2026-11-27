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
	"strings"

	core "github.com/cskiraly/seacast-go"
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
	out := make([]byte, 0, seacastHaveHeadLen+len(*bits))
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
	return !core.BitsWithin(enc, T.IndexCount(root))
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
	// A chunk's id must be a valid one (seacast-lite.md §4.4's valid_id): an index at or beyond
	// MAX_PLAIN_CHUNKS, or at or beyond its group's known count, is ignored, without penalty. The id is
	// read as the pull reads it (groupIndex: the client's parser, then an ask by index's own form), so
	// the guard and the pull name the same chunk.
	root, index, ok := gs.groupIndex(mid)
	if !ok {
		return false
	}
	if index >= core.MaxPlainChunks {
		return true
	}
	if gs.seacastCountKnown(root) {
		if n := gs.seacastRootType(root).IndexCount(root); n > 0 && int64(index) >= int64(n) {
			return true
		}
	}
	return false
}

// seacastOfferedRoot is the root an offered id names, read as seacastNoteRoot reads it, when the id
// passes seacastOfferIgnored's form checks (not a pseudo id, its index below MAX_PLAIN_CHUNKS): the
// root handleIHave places before the checks that look it up. ok is false for any other id.
func (gs *GossipSubRouter) seacastOfferedRoot(mid string) (string, bool) {
	if gs.seacast == nil || gs.have == nil || seacastPseudoUnknown(mid) || seacastIsV4Have(mid) {
		return "", false
	}
	root, index, ok := gs.have.index(mid)
	if !ok || index >= core.MaxPlainChunks {
		return "", false
	}
	return root, true
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
	root, _, ok := gs.have.index(mid)
	if !ok {
		return
	}
	gs.seacastPlaceRoot(topic, root)
}

// seacastPlaceAccepted places root again on the topic its type answers for it (TopicOf), at an
// accepted chunk of the group: an offer or a copy on another of the type's topics (a fork's topics
// overlapping) may have placed it first, and the group's joins, ties and gossip follow its topic. A
// topic the type does not own, or a root placed under another type, keeps its placement. Router
// goroutine, from seacastAccepted.
func (gs *GossipSubRouter) seacastPlaceAccepted(T ObjectType, root string) {
	sc := gs.seacast
	topic, ok := T.TopicOf(root)
	if !ok || sc.types[topic] != T || sc.roots[root] != T || sc.rootTopic[root] == topic {
		return
	}
	sc.rootTopic[root] = topic
	sc.ctr.RootsReplaced.Add(1)
}

// seacastPlaceRoot records that root belongs to the type owning topic, where the router first meets
// the group on the topic: a message of it, or an offer of it, before any check looks the root up (a
// lookup would place it by the types' TopicOf instead, seacastRootType). A root already placed keeps
// its type and topic, until an accepted chunk places it on its type's (seacastPlaceAccepted). Nothing
// for an unowned topic.
func (gs *GossipSubRouter) seacastPlaceRoot(topic, root string) {
	if gs.seacast == nil || len(root) != haveRootLen {
		return
	}
	T := gs.seacast.types[topic]
	if T == nil {
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
// routed request at a forwarder, §4.10) is placed by the types' TopicOf and registered, when one
// type places it. Where two do, the root alone doesn't tell their groups apart (two types whose
// TopicOf answers for any root they haven't seen, say): the root stays unplaced until the router
// meets it on a topic (seacastPlaceRoot).
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
	T, topic := gs.seacastTopicOfPlaces(root)
	if T == nil {
		return nil
	}
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

// seacastTopicOfPlaces is the one type whose TopicOf places root on a topic the type owns, with that
// topic; nil when no type does, or when two do.
func (gs *GossipSubRouter) seacastTopicOfPlaces(root string) (ObjectType, string) {
	var placed ObjectType
	var placedOn string
	for topic, T := range gs.seacast.types {
		if t, ok := T.TopicOf(root); ok && t == topic {
			if placed != nil && placed != T {
				return nil, ""
			}
			placed, placedOn = T, topic
		}
	}
	return placed, placedOn
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
		if gs.have != nil && gs.have.originAcks.has(root) {
			// A group the node publishes claims only what has arrived (holding_of at the source,
			// core.OriginClaims): both once bits names every chunk, neither before, whatever its
			// machine state says (a single Publish accepts the source's own chunk).
			if all, complete := core.OriginClaims(*bits, T.IndexCount(root)); all && complete {
				flags = seacastFlagClaimAll | seacastFlagComplete
			}
			return true, flags
		}
		// The machine's two claims (seacast_complete.go, §4.2's holding_of): claim_all for a group
		// being decoded or held whole, complete for a group whose asks stopped.
		g := sc.group(root)
		if claimAll || bits.Count() >= T.IndexCount(root) || (g != nil && g.State == seacastDecoding) {
			flags |= seacastFlagClaimAll
		}
		if g != nil && g.StopPull {
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
	bits = gs.seacastOriginBelowCount(root, bits)
	v4, flags := gs.seacastHaveFlags(root, bits, claimAll)
	if !v4 {
		return encodeHave(root, bits)
	}
	return encodeHaveV2(root, bits, flags&seacastFlagClaimAll != 0, flags&seacastFlagComplete != 0)
}

// seacastAbsorbV2 absorbs a v4 holding from p into the ledger: the bits as a holding (as v1's),
// the flags into the entry. Called for an id carrying the HAVE magic with version 4; such an id is
// the path's whatever follows, so handled is true in every case (absorbed, refused, ignored for an
// expired group, or malformed: too short for the header, a reserved bit, over the cap, a bit beyond
// the type's IndexCount), and the caller is done with it: it is never stored as an ordinary id.
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
	if gs.have == nil {
		return true
	}
	if gs.expired(root) {
		return true // ignored as v1's (seacast-lite.md §4.3's on_have): neither its bits nor its claims kept
	}
	// The bits as a v1 holding's, with the claims: one absorption, so every consumer (the entry,
	// holders, the ledger, the writer's copy, the known ids) sees it once. [c10] a later holding
	// never retracts a claim (draft §4.3).
	gs.haveTake(p, root, enc, claimAll, complete)
	ctr.HoldingsV4.Add(1)
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

// --- the codec to the core's messages
//
// The binding's strings to the core's types (package seacast) and back. Bindings §4 names a group
// by 32 bytes, "seacast-lite.md §2's group key, which is the whole id of a payload group"; the core
// names it by its GroupId (§1.2), the payload's GroupId(0, key, 0). How the strings name a column's
// or a row's group is open there (§4's first open point), so the codec maps the payload's groups
// only. A holding travels in an IDONTWANT and an offer in an IHAVE, so the reader takes the carrier.

// seacastGroupID is the core's GroupID of a payload group the binding names by root.
func seacastGroupID(root string) (core.GroupID, bool) {
	if len(root) != haveRootLen {
		return "", false
	}
	var key [32]byte
	copy(key[:], root)
	return core.NewGroupID(core.TypePayload, key, 0), true
}

// seacastRootOf is the binding's name of g: the payload group's key.
func seacastRootOf(g core.GroupID) (string, bool) {
	if !g.Valid() || g.Type() != core.TypePayload || g.Index() != 0 {
		return "", false
	}
	key := g.Key()
	return string(key[:]), true
}

// seacastHaveOf reads a HAVE pseudo id as the core's Have. In an IDONTWANT (offer false) it is a
// holding, of version 1 or 4; in an IHAVE (offer true) an offer, of version 1 only ("An offer keeps
// version 1"). ok is false for an ordinary id, and for a version-1 entry over the cap, which the
// binding reads as one (decodeHave; seacast-bindings.md §4: any other entry "is an opaque id"); malformed is
// true, with ok, for what the binding ignores: a v4 holding the codec refuses, a version it
// doesn't write, and anything but version 1 in an offer (seacastOfferIgnored).
func seacastHaveOf(mid string, offer bool) (h core.Have, ok, malformed bool) {
	if strings.HasPrefix(mid, haveMagic) && seacastPseudoUnknown(mid) {
		return core.Have{}, true, true
	}
	if root, claimAll, complete, bitmap, isV4, mal := decodeHaveV2(mid); isV4 {
		g, mapped := seacastGroupID(root)
		if offer || mal || !mapped {
			return core.Have{}, true, true
		}
		return core.Have{Group: g, Bits: bitmap, ClaimAll: claimAll, Complete: complete}, true, false
	}
	root, bitmap, isV1 := decodeHave(mid)
	if !isV1 {
		return core.Have{}, false, false
	}
	g, mapped := seacastGroupID(root)
	if !mapped {
		return core.Have{}, true, true
	}
	return core.Have{Group: g, Bits: bitmap, Offer: offer}, true, false
}

// seacastHaveID writes the core's Have as the binding's string: an offer as version 1, a holding as
// version 4 with its flags, as seacastEncodeHave writes a holding of the path's roots. ok is false
// for a group the codec doesn't map, an offer with a holding's flag, or a bitmap over the cap.
func seacastHaveID(h core.Have) (string, bool) {
	root, mapped := seacastRootOf(h.Group)
	if !mapped || len(h.Bits.Trim()) > haveMaxBytes || (h.Offer && (h.ClaimAll || h.Complete)) {
		return "", false
	}
	bits := newHaveBits(h.Bits)
	if h.Offer {
		return encodeHave(root, bits), true
	}
	return encodeHaveV2(root, bits, h.ClaimAll, h.Complete), true
}

// seacastWantOf reads a WANT pseudo id as the core's Want: version 4 with its flags and hops, or
// version 1, the plain ask by index (which seacast-bindings.md §4 leaves open for a seacast.md node; the
// port reads it as an ask, as today). One id names one index. ok is false for an ordinary id, and
// for a version-1 WANT of the wrong length, which the binding reads as one (decodeWant); malformed
// is true, with ok, for a WANT the codec refuses and a version the binding doesn't write.
// The hops bound per type (ROUTE_HOPS) is the receiver's (seacastWantIn), not the codec's.
func seacastWantOf(mid string) (w core.Want, ok, malformed bool) {
	if strings.HasPrefix(mid, wantMagic) && seacastPseudoUnknown(mid) {
		return core.Want{}, true, true
	}
	if root, index, standing, hops, isV4, mal := decodeWantV2(mid); isV4 {
		g, mapped := seacastGroupID(root)
		if mal || !mapped {
			return core.Want{}, true, true
		}
		w = core.Want{Group: g, Standing: standing, Probe: wantV2Probe(mid), Hops: hops}
		if index >= core.MaxHaveBytes*8 {
			return core.Want{}, true, true // beyond any group's indices: the core's Bits can't name it
		}
		w.Bits.Set(index)
		return w, true, false
	}
	root, index, isV1 := decodeWant(mid)
	if !isV1 {
		return core.Want{}, false, false
	}
	g, mapped := seacastGroupID(root)
	if !mapped {
		return core.Want{}, true, true
	}
	if index >= core.MaxHaveBytes*8 {
		return core.Want{}, true, true
	}
	w = core.Want{Group: g}
	w.Bits.Set(index)
	return w, true, false
}

// seacastWantIDs writes the core's Want as the binding's strings: one version 4 id per index, in
// order, with the flags and hops. ok is false for a group the codec doesn't map, hops over the
// codec's bound, or no index.
func seacastWantIDs(w core.Want) ([]string, bool) {
	root, mapped := seacastRootOf(w.Group)
	idx := w.Bits.Indices()
	if !mapped || w.Hops > seacastMaxHops || len(idx) == 0 {
		return nil, false
	}
	var flags byte
	if w.Standing {
		flags |= seacastFlagStanding
	}
	if w.Probe {
		flags |= seacastFlagProbe
	}
	out := make([]string, len(idx))
	for k, i := range idx {
		out[k] = encodeWantV2Flags(root, i, flags, w.Hops)
	}
	return out, true
}

// seacastChunkOf is the core's Chunk for a message of the path: its group and index from the
// structured id, its bytes as Data, and the message itself as Ref, so that forwarding sends the
// same bytes under the same id. The binding doesn't split the bytes: the object type reads its
// proof, header and context from them. ok is false for a message whose id names no payload group.
// Router goroutine.
func (gs *GossipSubRouter) seacastChunkOf(msg *Message) (core.Chunk, bool) {
	if gs.have == nil || msg == nil {
		return core.Chunk{}, false
	}
	root, index, ok := gs.have.index(gs.p.idGen.ID(msg))
	if !ok {
		return core.Chunk{}, false
	}
	g, mapped := seacastGroupID(root)
	if !mapped {
		return core.Chunk{}, false
	}
	return core.Chunk{Group: g, Index: index, Data: msg.GetData(), Ref: msg}, true
}

// seacastMessageOf is the message a Chunk came in (its Ref); ok is false for a rebuilt chunk.
func seacastMessageOf(c core.Chunk) (*Message, bool) {
	m, ok := c.Ref.(*Message)
	return m, ok && m != nil
}
