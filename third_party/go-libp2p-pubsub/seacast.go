package pubsub

// SEACAST (seacast.md) in the fork: one
// option, one path beside the phase path, in the form of the v0.5 rules (specv05.go): a struct of
// what the application supplies, counters, and hooks guarded by gs.seacast. The path runs on the
// topics the registered object types own; every other topic runs as today, and
// nothing changes with the option absent. A topic owned by a type and matched by the phase path's
// matcher is refused at installation (the matcher is a predicate, so the registered topics are
// what can be checked), and a Join of such a topic is refused and counted.
//
// This first step carries the column proxy's cells on the path with the phase path's machinery
// (trimDegree, the offers, the HAVE bitmaps) under the type's parameters: R_NODE is the type's
// push budget. The parts that replace that machinery
// (the accepted-chunk path, the encodings, admission by WANT, the standing WANT, completion) come
// each behind its own default.

import (
	"fmt"
	"reflect"
	"sort"
	"sync/atomic"
	"time"

	core "github.com/cskiraly/seacast-go"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
)

// TypeParams is an object type's parameters (seacast.md §6.3): the core's.
type TypeParams = core.TypeParams

// ChunkSlotter is an ObjectType that reads, from a chunk's id, the slot its copy names (seacast-lite.md §4.2's
// f.slot; spec f6c19db). What waits for a root the application cannot place yet (SlotOf unknown) goes
// when that slot leaves the live window (drop_stale_waits, seacast_window.go). Optional: without it a
// wait goes with its group's slot only, as before.
type ChunkSlotter interface {
	ChunkSlot(mid string) (slot uint64, ok bool)
}

// ObjectType is what the application supplies per object type: its name, its parameters, and the
// topics it owns on v0.3's wire (the binding's rendezvous, seacast.md §7). The lookups the
// later parts need (IndexCount, Verify, Decode, Project, Cross, the store) are added with them.
type ObjectType interface {
	Name() string
	Params() TypeParams
	Topics() []string
	// HeaderKnown says whether the group's header is validated (the gate, §4.2): a chunk of a
	// group whose header is not yet validated waits in the pending store (seacast_accept.go). Once
	// true it stays true while the group lives (seacast-lite.md's authenticable: an installed
	// block); the path asks for nothing of a group before, and a gate that closed again would not
	// stop an ask the retry had already scheduled.
	HeaderKnown(root string) bool
	// IndexCount is the group's number of indices (the claim_all flag counts against it).
	IndexCount(root string) int
	// Store is the type's cell store (seacast_store.go): filled at ACCEPT and with the node's own
	// chunks, asked when the message cache has let a chunk go; nil for a type that keeps none.
	Store() CellStore
	// DerivedInterest is the type's rule for a DERIVED interest (§1.2): what a node with this id
	// and the Join's custody count is to receive of the group, as indices or as every index.
	DerivedInterest(root string, id NodeID, j Join) (indices []uint32, all bool)
	// SlotOf is the slot of the group's key (v0.3 §1, §8.1): the slot of the block whose bid
	// commits the group, fixed by its name. ok is false for a root the application cannot place
	// yet; the live window (seacast_window.go) then leaves the group alone.
	SlotOf(root string) (slot uint64, ok bool)
	// TopicOf is the topic the group root's chunks travel on, from the root alone (the binding's
	// rendezvous, §7): the path learns a root's type from the first chunk it sees of the group,
	// and a WANT may name a root before any chunk reached the node (a sampler's at block
	// arrival, a routed request at a forwarder, §4.10), so the type answers for it. ok is false
	// for a root the type does not place.
	TopicOf(root string) (topic string, ok bool)
	// Threshold is K, the chunks of a group from which the whole follows (§1.1's table): a
	// column's IndexCount, since nothing is coded across a column's cells, a row's 64. The
	// window's projection reads it (§4.9): a type with K = N projects nothing.
	Threshold(root string) int
	// Cross is the chunk's other (group, index) pair, from the ids alone (§1.1's cross, R4; the
	// fork acts on none), with
	// the message id of the chunk's wire form on that axis: the cell at index r of column c is
	// index c of row r. ok is false for a type whose chunks belong to one group (the payload), or
	// while the other axis is not placed. The draft's cross returns a list; the two-axis types
	// here have one other pair each, a stated specialization.
	Cross(root string, index uint32) (other GroupIndex, mid string, ok bool)
	// Family is the relation key of the group (§1.3): a column's index, a row's subnet.
	Family(root string) string
}

// GroupIndex names one (group, index) pair (§1.1's project and cross): the core's.
type GroupIndex = core.GroupIndex

// SeacastCounters counts what the path did, for the harness's lines and its positive controls.
type SeacastCounters struct {
	// Forwarded is the messages dispatched on the path's topics; Pushed and Offered the peers
	// pushed and offered per message, summed; Refused the Joins refused for an overlap.
	Forwarded, Pushed, Offered, Refused atomic.Int64
	// The wire (seacast_wire.go): v4 holdings absorbed and v4 WANTs received; entries ignored
	// as codec-malformed or as unknown pseudo ids; v4 entries refused for a root of the phase path.
	HoldingsV4, WantsV4, Malformed, OffPath atomic.Int64
	// The accepted-chunk path (seacast_accept.go): arrivals from a peer on the path's topics, and
	// apart the node's own publications passing Preprocess (Own: stored and seen without
	// reception, §4.7); accepted (the fill), rejected, deferred (the copy pending at its verdict),
	// ignored otherwise; copies put in the pending store, duplicates of a pending id dropped,
	// copies refused at the per-peer bound, released at the header, expired.
	Arrived, Own, Accepted, Rejected, Ignored              atomic.Int64
	Pending, PendingDup, PendingRefused, Released, Expired atomic.Int64
	UnknownRefused                                         atomic.Int64 // copies of a group beyond the sender's MAX_PENDING_GROUPS: neither kept nor tied
	SeenAtGate                                             atomic.Int64 // copies of a seen id meeting a closed gate: duplicates, not kept
	StaleWaits                                             atomic.Int64 // kept copies and ties dropped as their copy's slot left the live window
	DroppedBeyond                                          atomic.Int64 // ids beyond a group's count dropped once it is known (drop_beyond)
	OriginAcked                                            atomic.Int64 // own chunks acknowledged at the source (origin_acked): a peer's HAVE, or the first copy's window timeout
	ProbesNoVerdict                                        atomic.Int64 // probes given no verdict: refused by a full queue, or not written by the reconsider (§4.6)
	ProbesRefused                                          atomic.Int64 // probes a full queue refused at the decision
	ProbesKept                                             atomic.Int64 // as a claimant still decoding, marked asks kept until the cell is stored (probe_waiting)
	// ExpiredDropped counts chunks of an expired group refused at admission: a copy arriving after
	// the expiry, or one that was in validation when its group expired (dropped at the admission
	// that follows validation, as v0.5's D′ drops them).
	ExpiredDropped atomic.Int64
	// HoldingsSent counts the fills whose holding was attempted (the send to the scope and the
	// sender's acknowledgement are not counted apart); DupUnacked the later copies of a pending,
	// rejected or unaccepted id refused an acknowledgement; DupAcked the later copies of an
	// accepted id acknowledged, counted at the send ([c14], draft §4.2).
	HoldingsSent, DupUnacked, DupAcked atomic.Int64
	// The store (seacast_store.go): chunks filed in the types' stores; asks answered from them.
	Stored, ServedFromStore atomic.Int64
	// Admission by WANT (seacast_want.go): WANTs the path sent; chunks admitted on an unsubscribed
	// topic because their (group, index) was asked of the sender.
	WantsSent, AdmittedByWant atomic.Int64
	// The reporting path: the bytes of the RPCs dispatched on the path's topics (the
	// write, apart from the arrival and the acceptance), and the standing v4 WANTs received apart
	// from the plain indexed ones counted in WantsV4.
	WriteBytes, WantsStanding atomic.Int64
	// The live window (seacast_window.go): chunks of a later slot ignored; groups expired because
	// their slot left the window; tombstones dropped once the window passed their slot; advances to
	// a lower slot ignored. Roots placed again on their type's topic at an accepted chunk
	// (seacastPlaceAccepted).
	Future, WindowExpired, TombstonesDropped, SlotRegress, RootsReplaced atomic.Int64
	// Ties (seacast_tie.go): senders tied for an index by a copy that waits; offers passed over
	// for a tied sender; ties turned into offers at the gate's opening.
	Tied, TiedPassedOver, TiesToOffers atomic.Int64
	// No second ask of a peer at once (seacast_ask.go): offers passed over because an ask of the
	// chunk to the offering peer is outstanding; retries deferred because every announcer was.
	AskOutstanding, RetryDeferred atomic.Int64
	// An ask goes with its announcer's offers (seacast_f13.go): promises closed with the offer
	// that drew them; queued asks dropped at the writer with it.
	F13Closed, F13AskDropped atomic.Int64
	// The IHAVE limits as written (seacast_ihave.go): asks held back to the next heartbeat because
	// a limit was reached; held-back asks retried at the heartbeat.
	HeldBack, HeldBackRetried atomic.Int64
	// The standing table and the serving limit (seacast_standing.go, §4.5): asks refused at the
	// limit; standing requests kept for a chunk not held; served when the chunk arrived or was
	// published; refused by a full queue (owed under v0.5); skipped at the limit when the chunk
	// came; dropped with their group's expiry.
	ServeLimited, StandingKept, StandingServed, StandingRefused, StandingLimited, StandingExpired atomic.Int64
	// AskOutside counts asks outside may_be_asked (§4.5, P7; seacast_route.go): refused at the
	// sender on the path's own asks, counted on the ordinary ones.
	AskOutside atomic.Int64
	// Typed gossip (seacast_gossip.go, §4.8): per-peer heartbeat jobs queued over the path's types,
	// and the ids offered in them.
	GossipJobs, GossipIdsOffered atomic.Int64
	// The group machine's (seacast_complete.go, part 4): groups completed, later duties armed and
	// cancelled (the group whole by then), late decodes, decodes started, promises closed at a
	// completion, and asks declined by stop_pull.
	GroupsCompleted, DutiesArmed, DutiesCancelled, LateDecodes, DecodesStarted atomic.Int64
	PromisesClosed, StopPullDeclined                                           atomic.Int64
	// The claims and the probe (seacast_claims.go): claims noted, probes asked, answered, broken,
	// none possible (no claimant with allowance left), the penalties, and verified claims ending a
	// duty.
	ClaimsNoted, ProbesAsked, ProbesAnswered, ProbesBroken, ProbesNone atomic.Int64
	// Probes whose claimant's connection closed while their promise ran: cancelled, not charged.
	ProbesCancelled           atomic.Int64
	Penalties, ClaimsVerified atomic.Int64
	// The probe's verdict on the claimant (seacast.md §4.6, R5): only its own copy passes it.
	// ProbesOwnCopy counts the claimant's copies that passed a probe (first or a duplicate);
	// VerdictsPassed the verdicts taken at reconsider that passed (those that failed are Penalties);
	// ProbesServed our answers, as a claimant, to a peer's marked probe, counted when decided.
	ProbesOwnCopy, VerdictsPassed, ProbesServed atomic.Int64
	// decode_group and push_rebuilt (seacast_decode.go): decodes done, failed and stale, cells
	// rebuilt, repair pushes and offers.
	DecodesDone, DecodesFailed, DecodesStale, CellsRebuilt, RepairPushes, RepairOffers atomic.Int64
	// push_rebuilt's filters: groups reached, groups whose type no lookup placed, candidates with
	// no interest in the group, candidates not short of K.
	RepairGroups, RepairNoType, RepairNoInterest, RepairNotShort atomic.Int64
	// The faults (seacast_fault.go): a hoarder's refused and served asks for rebuilt chunks.
	HoardRefused, HoardServed atomic.Int64
	// The queued control entries of the v4 forms (holdings, WANTs) a group's expiry discarded from
	// the peers' queues (specv05.go's expireGroup, D′; the review of part 4's build, finding 2).
	ExpiredV4Discards atomic.Int64
	// The group's asks still queued at its completion, dropped from the writers' queues (stop_pull at
	// the writers; the review of part 4's gate, finding 1).
	StopPullDiscards atomic.Int64
	// Offers and gossip that named a group past its lifetime or dropped INVALID, left out at the
	// encoding (the review of part 4's gate, finding 5).
	ExpiredOffersSkipped atomic.Int64
	// join_push (seacast.md §4.1, part 6; WithSeacastJoinPush): the peers that entered the mesh of
	// an EAGER type's topic, by the entry (a GRAFT accepted, our own GRAFT at the heartbeat or at a
	// Join), and what was offered them: the groups and the ids. The entries that added nothing
	// (the peer already in the mesh) run nothing and are not counted.
	JoinPushAccepted, JoinPushOwn, JoinPushGroups, JoinPushIDs atomic.Int64
	// The source's offers to a peer it pushes the same chunk (v0.7's single offer, §4.7 publish):
	// want 0, counted at the dispatch whether or not join_push is on.
	SourceOfferOverlap atomic.Int64
	// The ordering checks (seacast_order.go), each counted at the act's site against the node's own
	// state at that instant; every one is want 0 on an honest node. (i) a cell filed, sent or
	// acknowledged before its group's header validated; (ii) a holding for a group the node holds
	// nothing of, and a claim_all with fewer than K held and no successful decode; (iii) a rebuilt
	// cell sent whose decode had not succeeded from K held; (iv) a probe asked of a peer whose claim
	// for the group was not noted first. The rebuilders' per-event conditions: a decode started short
	// of K, a decode started on a group already whole, a rebuilt cell sent after its group expired.
	// A faulty node's own acts (seacast_fault.go) count in the OrdF twins, never in the honest ones.
	OrdBeforeHeader, OrdHoldingUnheld, OrdClaimShort, OrdRebuiltEarly, OrdProbeBeforeClaim      atomic.Int64
	OrdDecodeShort, OrdDecodeWhole, OrdRebuiltExpired                                           atomic.Int64
	OrdFBeforeHeader, OrdFHoldingUnheld, OrdFClaimShort, OrdFRebuiltEarly, OrdFProbeBeforeClaim atomic.Int64
	OrdFDecodeShort, OrdFDecodeWhole, OrdFRebuiltExpired                                        atomic.Int64
}

// Reset stores zero into every counter, atomically each, so an owner may reset them (the harness
// between blocks) while the routers add. Any goroutine.
func (c *SeacastCounters) Reset() {
	v := reflect.ValueOf(c).Elem()
	for i := 0; i < v.NumField(); i++ {
		if f, ok := v.Field(i).Addr().Interface().(*atomic.Int64); ok {
			f.Store(0)
		}
	}
}

// seacast is the router's registry: each owned topic's type.
type seacast struct {
	types map[string]ObjectType
	ctr   *SeacastCounters
	// roots is the type owning each group root the router has seen on an owned topic; offRoots
	// the roots seen on unowned topics (seacast_wire.go: a control resolves to its root's path).
	roots    map[string]ObjectType
	offRoots map[string]bool
	// regSlot is the node's slot when each root was registered (the first advance's, for one
	// registered before it): the window's fallback for a root its type cannot place, so that the
	// registry holds no root beyond the window (seacast_window.go).
	regSlot map[string]uint64
	// The accepted-chunk path's state (seacast_accept.go).
	limits   SeacastLimits
	rejected map[string]struct{}
	// waits is the deferred store, the core's (seacast_tie.go): the copies kept while their gate is
	// closed, by root; waitMsg is each kept copy's message, by id, which the gate's opening resumes.
	waits   core.Pending[peer.ID]
	waitMsg map[string]*Message
	// gateMet is the roots whose closed gate something met: a copy kept or refused, a tie, an offer
	// recorded and not asked. Only their gate opens (seacastOpenGate); a type whose gate never closes
	// has none.
	gateMet map[string]struct{}
	// serveTx is the one count of the answers to a peer per (root, index), whichever of the message
	// cache and the type's store answered (seacast_store.go; the draft's serve_count, §4.5).
	serveTx map[string]map[uint32]map[peer.ID]int
	// The role table (seacast_roles.go), each root's topic, and the DECLARED interests (part 2).
	roles     RoleTable
	rootTopic map[string]string
	wantP     map[peer.ID]map[string]*haveBits
	// asked is the admission registry (seacast_want.go): root, index, the peers asked; askedOf
	// counts, per root and peer, the chunks asked of the peer (against the type's SERVE_LIMIT,
	// which the peer applies: an ask past it is refused without a word, seacast_claims.go's probe).
	asked   map[string]map[uint32]map[peer.ID]struct{}
	askedOf map[string]map[peer.ID]int
	// expired is the tombstones: the groups expired before their slot left the live window (the
	// application's SeacastExpire, or the ledger's aging of a path root), kept out by name until the
	// window has passed their slot (seacast_window.go). A chunk in validation at the expiry is
	// dropped at the admission after it.
	expired map[string]struct{}
	// slot is the node's current slot, once the application set it (SeacastAdvanceSlot); it only
	// moves forward. The window applies from the first advance.
	slot      uint64
	slotKnown bool
	// ties is the ties, the core's (seacast_tie.go): per root and sender, the indices whose copy waits
	// for the root, and the slot each sender's first copy under the root named (tie_slot).
	ties core.Ties[peer.ID]
	// origin is the source's state per group it publishes on the path (seacast_origin.go).
	origin map[string]*seacastOrigin
	// askQueued is the asks dispatched and not yet written or dropped (seacast_ask.go), per chunk
	// the peers asked, with when each was dispatched: a write reported later matches only an ask
	// dispatched before that write.
	askQueued map[string]map[peer.ID]uint64
	// ihave is ihave_frames and ihave_asked, the IHAVE limits' counts per peer this heartbeat
	// (seacast_ihave.go).
	ihave core.IHaveLimits[peer.ID]
	// heldBack is the ids outside the core's pull whose ask a limit held back to the next heartbeat,
	// with the peers that offered them (seacast_ihave.go): the retry asks one of them where the offer
	// table, bounded per peer, kept no entry. A chunk's are the pull's (chunkPull.held).
	heldBack map[string]map[peer.ID]struct{}
	// standing is the standing table (seacast_standing.go, §4.5's standing[g][i]): per root and
	// index, the peers whose request waits for the chunk; served is the core's served[p][g], keyed
	// by group first, the chunks answered to each peer per group against the type's ServeLimit.
	standing map[string]map[uint32]map[peer.ID]struct{}
	served   core.Served[peer.ID]
	// knownAt is known_at[g] (seacast_known.go): the earliest the node knew each root, by its
	// first sight on the path or the application's word.
	knownAt map[string]time.Time
	// machine is WithSeacastMachine (seacast_complete.go): groups the state per root, probeAsks
	// the probe's ask ids, exempt from stop_pull.
	machine   bool
	gm        *core.Machine[*seacastProbe]
	probeAsks map[string]struct{}
	// probeCells is the probe's ask exemption by cell (seacast.md §4.6's probe_asks, R5): per root,
	// the probed indices, whatever id an ask of them carries; kept until the group expires, or the
	// claimant's connection closes. probeServed is, as a claimant, a peer's marked probe of a group
	// answered (the cap).
	probeCells  map[string]*haveBits
	probeServed map[peer.ID]map[string]bool
	// probeWaiting is, as a claimant still decoding, a peer's first marked ask of a group, of a claimed index
	// not yet stored, by root and peer: answered once when the cell is (probe_waiting, seacast_probe.go).
	probeWaiting map[string]map[peer.ID]uint32
	// rebuilt is each group's rebuilt indices (seacast_decode.go).
	rebuilt map[string]*haveBits
	// joinPush is WithSeacastJoinPush (seacast.go's seacastJoinPush, part 6).
	joinPush bool
	// fault is the toy's fault on this node (seacast_fault.go); hoardProbed the hoarder's answered
	// probes, per peer and group.
	fault       int
	hoardProbed map[string]bool
	// The ordering checks' state (seacast_order.go): per root, the rebuilt indices whose decode
	// succeeded from K held (rebuiltOK, beside rebuilt); the roots of the node's own publications
	// (ownRoots: their header is the node's own, validated by construction).
	rebuiltOK map[string]*haveBits
	ownRoots  map[string]struct{}
	// rebuiltGone keeps the rebuilt indices of groups expired or dropped (bounded), for the expiry
	// check; ordLast* is seacastOrderIndex's one-entry cache.
	rebuiltGone  map[string]*haveBits
	ordLast      *pb.Message
	ordLastRoot  string
	ordLastIndex uint32
	ordLastOK    bool
	// events is WithSeacastEvents's observer (seacast_order.go), nil without it.
	events func(kind SeacastEvent, root string)
}

// WithSeacast installs the path for the types' topics. Two types on one topic, or a topic the
// installed phase matcher accepts, are refused.
func WithSeacast(types []ObjectType, ctr *SeacastCounters) Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok {
			return fmt.Errorf("seacast requires a gossipsub router")
		}
		if len(types) == 0 {
			return fmt.Errorf("seacast needs at least one object type")
		}
		// The path runs on the HAVE machinery (the gate at admission, the holdings, the ledger)
		// and the phase path's budget enforcement, so both must be installed before it: a
		// router without them would let the pending gate silently go unenforced.
		if gs.have == nil {
			return fmt.Errorf("seacast needs WithHaveBitmaps before it")
		}
		if gs.phaseForward == nil {
			return fmt.Errorf("seacast needs WithPhaseForwarding before it")
		}
		if ctr == nil {
			ctr = &SeacastCounters{}
		}
		sc := &seacast{types: map[string]ObjectType{}, ctr: ctr, limits: DefaultSeacastLimits}
		for _, T := range types {
			if T == nil {
				return fmt.Errorf("seacast: a nil object type")
			}
			for _, topic := range T.Topics() {
				if prev, dup := sc.types[topic]; dup {
					return fmt.Errorf("seacast: topic %q owned by types %q and %q", topic, prev.Name(), T.Name())
				}
				if gs.phaseForward != nil && gs.phaseForward.match(topic) {
					return fmt.Errorf("seacast: topic %q of type %q is also matched by the phase path", topic, T.Name())
				}
				sc.types[topic] = T
				if a, ok := T.(AttachedObject); ok {
					if ps.attachedTopics == nil {
						ps.attachedTopics = make(map[string]AttachedObject)
						ps.segmentForWhole = make(map[string]string)
					}
					ps.attachedTopics[topic] = a
					ps.segmentForWhole[a.WholeTopic(topic)] = topic
				}
			}
		}
		gs.seacast = sc
		// The writers' index over a queued message, for the projection per type (seacast_project.go),
		// as WithHaveDispatchProjection installs it.
		if gs.have.msgIndex == nil {
			gs.have.msgIndex = func(m *pb.Message) (string, uint32, bool) { return gs.have.index(ps.idGen.RawID(m)) }
		}
		// The gate at admission, ahead of any admission the application set.
		prevAdmit := ps.admitMsg
		ps.admitMsg = func(m *Message, validated bool) bool {
			if a := ps.attachedTopics[m.GetTopic()]; a != nil && !validated {
				verdict, opened := a.AdmitFrame(m)
				if opened != "" {
					gs.seacastPlaceRoot(m.GetTopic(), opened)
					gs.seacastDropBeyond([]string{opened})
				}
				switch verdict {
				case ValidationReject:
					ps.tracer.RejectMessage(m, RejectValidationFailed)
					return false
				case ValidationIgnore:
					return false
				}
			}
			if !gs.seacastAdmit(m, validated) {
				return false
			}
			return prevAdmit == nil || prevAdmit(m, validated)
		}
		// A chunk on a topic we have not subscribed to enters when its (group, index) was asked of
		// the sender (seacast_want.go).
		ps.admitUnsubscribed = func(m *pb.Message, from peer.ID) bool { return gs.seacastExpects(m, from) }
		ps.onVerdict = func(src peer.ID, msg *Message, result ValidationResult) {
			// Off the path's topics the verdict is nobody's business here: no hop to the router
			// goroutine for them (the types map is fixed at installation, so this read is safe
			// from the validation goroutine).
			if gs.seacastType(msg.GetTopic()) == nil {
				return
			}
			select {
			case ps.eval <- func() { gs.seacastVerdict(src, msg, result) }:
			case <-ps.ctx.Done():
			}
		}
		return nil
	}
}

// seacastType is the type that owns topic, nil for a topic the path does not run.
func (gs *GossipSubRouter) seacastType(topic string) ObjectType {
	if gs.seacast == nil {
		return nil
	}
	return gs.seacast.types[topic]
}

// WithSeacastJoinPush runs seacast.md §4.1's join_push on v0.3's wire (part 6, off by default):
// a peer that enters the mesh of an EAGER type's topic is offered what the node holds of the
// topic's live groups. Requires WithSeacast first.
func WithSeacastJoinPush() Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok || gs.seacast == nil {
			return fmt.Errorf("join push requires WithSeacast first")
		}
		gs.seacast.joinPush = true
		return nil
	}
}

// seacastJoinPush is join_push(p, T, fam, c) on v0.3's wire, where the relation for a column is its
// topic's mesh (M = 1). p entered the mesh of topic: at a GRAFT the node accepted (own false) or at
// its own GRAFT, which on gossipsub's wires puts it in the relation at once (own true;
// seacast.md §4.1 "On gossipsub's wires"). For each live group placed on the topic, it offers p
// the indices the node stores, received or rebuilt, minus what p holds, was pushed or does not want
// (haveIHaveIDs, the peer's interest): one holding per group, or one offer per id through the
// hose's offers when they are on. Not the decode claim, nothing of an expired or dropped group,
// Router goroutine.
func (gs *GossipSubRouter) seacastJoinPush(p peer.ID, topic string, own bool) {
	sc := gs.seacast
	if sc == nil || !sc.joinPush || gs.have == nil {
		return
	}
	if sc.types[topic] == nil {
		return
	}
	if own {
		sc.ctr.JoinPushOwn.Add(1)
	} else {
		sc.ctr.JoinPushAccepted.Add(1)
	}
	roots := make([]string, 0, len(gs.have.mine))
	for gid := range gs.have.mine {
		root := seacastGIDRoot(gid)
		if sc.rootTopic[root] == topic && !gs.expired(root) {
			roots = append(roots, root)
		}
	}
	sort.Strings(roots) // the order offers leave in, deterministic under a seeded run
	for _, root := range roots {
		e, ids := gs.have.mine[seacastGID(root)], gs.have.mids[root]
		if e == nil || ids == nil {
			continue
		}
		interest := gs.seacastInterestOfPeer(p, root) // nil: no JOIN known, the mesh is the relation
		theirs, pushed := gs.have.peers.Of(p, seacastGID(root)), gs.have.pushed.Of(p, seacastGID(root))
		if theirs != nil && theirs.Complete {
			continue // p's interest in the group is satisfied
		}
		var mids []string
		for i := uint32(0); i < uint32(len(e.Bits))*8; i++ { // lint:ignore uintcast -- a bitmap's length in bits.
			if !e.Bits.Has(i) || (interest != nil && !interest.Has(i)) {
				continue
			}
			if (theirs != nil && theirs.Bits.Has(i)) || (pushed != nil && pushed.Bits.Has(i)) {
				continue // wants_from_me: not what p holds or was pushed (§4.1), whichever side is a bitmap
			}
			if mid, ok := ids.ids[i]; ok && mid != "" {
				mids = append(mids, mid)
			}
		}
		if len(mids) == 0 {
			continue
		}
		out := gs.haveIHaveIDs(p, mids)
		if len(out) == 0 {
			continue
		}
		sc.ctr.JoinPushGroups.Add(1)
		sc.ctr.JoinPushIDs.Add(int64(seacastIDsNamed(out)))
		gs.sendRPC(p, rpcWithControl(nil, []*pb.ControlIHave{{TopicID: &topic, MessageIDs: out}}, nil, nil, nil, nil), false)
	}
}

// seacastJoinPushGrafted runs join_push for the heartbeat's own GRAFTs (tograft, by peer), once
// sendGraftPrune has queued them: each offer enters the peer's queue after the GRAFT that put the
// peer in the relation, so it never takes the queue's last place ahead of it. A peer no longer in
// the topic's mesh is skipped. Router goroutine.
func (gs *GossipSubRouter) seacastJoinPushGrafted(tograft map[peer.ID][]string) {
	if gs.seacast == nil || !gs.seacast.joinPush {
		return
	}
	for p, topics := range tograft {
		for _, topic := range topics {
			if _, in := gs.mesh[topic][p]; in {
				gs.seacastJoinPush(p, topic, true) // our own GRAFT, at the heartbeat
			}
		}
	}
}

// seacastIDsNamed is how many chunks an announce's ids name: a holding's indices, one for any
// other id.
func seacastIDsNamed(ids []string) int {
	n := 0
	for _, mid := range ids {
		if _, bitmap, ok := decodeHave(mid); ok {
			n += (newHaveBits(bitmap)).Count()
		} else {
			n++
		}
	}
	return n
}

// seacastSourceOverlap is how many of the source's offers of a chunk go to a peer it pushes the same
// chunk: v0.7's single offer wants none (§4.7 publish).
func seacastSourceOverlap(announce []peer.ID, tosend map[peer.ID]struct{}) int {
	n := 0
	for _, p := range announce {
		if _, pushed := tosend[p]; pushed {
			n++
		}
	}
	return n
}

// seacastDegree is the push budget the path gives a message on topic: the type's R_NODE; ok is
// false off the path.
func (gs *GossipSubRouter) seacastDegree(topic string) (degree int, ok bool) {
	T := gs.seacastType(topic)
	if T == nil {
		return 0, false
	}
	return T.Params().RNode, true
}

// seacastPushBudget is the push budget the path gives a message on topic that this node sends:
// for our own chunk (own: published here and not rebuilt, ownPublication) the type's RSource when
// set, else its R_NODE (seacastDegree); ok is false off the path. A chunk we rebuilt and publish is
// not our own: it gets R_NODE.
func (gs *GossipSubRouter) seacastPushBudget(topic string, own bool) (budget int, ok bool) {
	T := gs.seacastType(topic)
	if T == nil {
		return 0, false
	}
	p := T.Params()
	if own && p.RSource != 0 {
		return p.RSource, true
	}
	return p.RNode, true
}

// seacastJoinRefused says whether a Join of topic is refused: the topic is owned by a type and
// matched by the phase path too (a matcher installed after the types). Counted.
func (gs *GossipSubRouter) seacastJoinRefused(topic string) bool {
	if gs.seacast == nil || gs.phaseForward == nil {
		return false
	}
	if _, owned := gs.seacast.types[topic]; owned && gs.phaseForward.match(topic) {
		gs.seacast.ctr.Refused.Add(1)
		return true
	}
	return false
}

// seacastNoteDispatch counts a message dispatched on the path: the peers pushed and offered.
func (gs *GossipSubRouter) seacastNoteDispatch(pushed, offered int) {
	if gs.seacast == nil {
		return
	}
	gs.seacast.ctr.Forwarded.Add(1)
	gs.seacast.ctr.Pushed.Add(int64(pushed))
	gs.seacast.ctr.Offered.Add(int64(offered))
}
