// Package segmentauth decides whether a segment group is one the chain committed to.
//
// A segment's Merkle proof shows only that it belongs to some root; nothing inside the tree
// says the root is legitimate. This package supplies that outside fact, and it is a membership
// test: block processing gives a node the set of segment-group commitments the chain has
// accepted, and a descriptor is authentic exactly when its group id is in that set.
//
// # Nothing on the wire is trusted
//
// The group id is derived from the descriptor the sender supplied. Either it is committed or it
// is not, so a sender cannot assert authority, only fail to match. That is why a segment
// carries no authority field, and why two earlier designs were removed:
//
//   - A detached builder signature on every segment. It proved *a* registered key signed *a*
//     descriptor, and one key may sign any number, so an adversary could mint unboundedly many
//     groups inside the acceptance window. It bounded nothing and cost a BLS verification.
//   - A slot-and-block-root anchor on every segment. Redundant with the group id, and being
//     outside both the descriptor and the Merkle tree it could be rewritten without
//     invalidating any proof -- so it needed its own invariant, its own error and its own
//     tests just to be safe, and it gave a peer a key it could flood.
//
// Naming the block a segment belongs to is genuinely useful, but at *announcement* time, so a
// receiver can decline an id it cannot yet authenticate. That belongs in the message id.
//
// # What is real here and what is stubbed
//
// The commitment is meant to live in SignedExecutionPayloadBid, which the builder already
// signs and which rides in the gloas block body, arriving ahead of the segments it describes.
// Adding that field changes the bid's hash_tree_root, so it is a consensus change gated on a
// spec PR. Everything on this side of CommittedGroups is independent of that PR.
//
// # The coupling this creates
//
// Proof verification needs Count, Count comes from the committed descriptor, and the
// commitment comes from the block. So a segment cannot be authenticated before its block. That
// is the price of anchoring in consensus.
package segmentauth

import (
	"errors"
	"fmt"
	"sync"

	"github.com/OffchainLabs/prysm/v7/consensus-types/primitives"
	"github.com/OffchainLabs/prysm/v7/container/segments"
)

var (
	// ErrNotCommitted is returned when the group id is not one the chain committed to.
	//
	// Transient rather than attributable on its own: the commitment arrives with a block, so
	// a segment that outruns its block lands here through no fault of the sender.
	ErrNotCommitted = errors.New("segment group is not committed by any known block")
	// ErrFirstSeenConflict is returned when the interim authority has already admitted a
	// different group for this slot.
	//
	// Distinct from ErrNotCommitted because it must never cost a peer reputation: under
	// first-seen a conflict proves only that two peers disagree, and if an attacker won the
	// race it is the honest peer that conflicts.
	ErrFirstSeenConflict = errors.New("a different segment group was already admitted for this slot")
)

// CommittedGroups reports whether a group id is one the chain has committed to.
//
// Implementations must answer only for commitments carried by blocks the node has validated.
// This is the entire trust boundary.
type CommittedGroups func(groupID []byte) bool

// CurrentSlot reports the chain's current slot.
type CurrentSlot func() primitives.Slot

// Authenticator admits groups the chain committed to.
type Authenticator struct {
	committed CommittedGroups
}

// New builds an Authenticator over a commitment set.
func New(committed CommittedGroups) *Authenticator {
	return &Authenticator{committed: committed}
}

// AuthenticateDescriptor implements segments.Authenticator.
func (a *Authenticator) AuthenticateDescriptor(_ *segments.Descriptor, groupID []byte) error {
	if !a.committed(groupID) {
		return fmt.Errorf("%w: group %#x", ErrNotCommitted, groupID)
	}
	return nil
}

// FirstSeenAuthenticator is the interim authority, for use until ExecutionPayloadBid carries
// the commitment: the first group offered in a slot is the one that slot admits.
//
// # What this provides and what it does not
//
// It admits one segment group per slot, which is the right cardinality -- ePBS has one
// canonical payload per slot -- and it takes its key from the node's own clock, so a peer
// cannot choose it, cannot flood it, and cannot make the table grow. It does NOT provide
// authenticity: nothing here checks that the first group came from the winning builder, so a
// peer that wins the race is admitted and the honest group is then refused for that slot.
//
// In the study's terms this is the *policy* form of the committed-group route; the intended
// form is *cryptographic*. Once the bid carries the commitment, Authenticator replaces this
// and both properties hold -- at which point this type is deleted, not extended.
//
// It is therefore not a strict improvement on either scheme it replaced: it gives up the proof
// that a registered builder signed the group, and gains a bound they lacked. It must not ship
// as the real scheme on a public network; default-off is not a security boundary.
//
// One known liveness cost: a proposer that equivocates produces two blocks in a slot with two
// payloads, and only one group is admissible, so the second is refused until the next slot.
type FirstSeenAuthenticator struct {
	now CurrentSlot

	mu      sync.Mutex
	bySlot  map[primitives.Slot]string
	horizon primitives.Slot
}

// SlotRetention is how many past slots keep an admitted group.
//
// Two is enough to cover a payload whose segments straddle a slot boundary while keeping the
// table to a handful of entries.
const SlotRetention = primitives.Slot(2)

// NewFirstSeen builds the interim authority.
func NewFirstSeen(now CurrentSlot) *FirstSeenAuthenticator {
	return &FirstSeenAuthenticator{now: now, bySlot: make(map[primitives.Slot]string)}
}

// AuthenticateDescriptor implements segments.Authenticator.
func (f *FirstSeenAuthenticator) AuthenticateDescriptor(_ *segments.Descriptor, groupID []byte) error {
	slot := f.now()
	f.mu.Lock()
	defer f.mu.Unlock()
	// Drop slots that have fallen behind. Keyed on our own clock, so the table holds at most
	// SlotRetention+1 entries however much a peer sends.
	if slot > f.horizon {
		f.horizon = slot
		for s := range f.bySlot {
			if s+SlotRetention < slot {
				delete(f.bySlot, s)
			}
		}
	}
	admitted, ok := f.bySlot[slot]
	if !ok {
		f.bySlot[slot] = string(groupID)
		return nil
	}
	if admitted != string(groupID) {
		return fmt.Errorf("%w: slot %d", ErrFirstSeenConflict, slot)
	}
	return nil
}

// Len reports how many slots hold an admitted group. For tests and metrics.
func (f *FirstSeenAuthenticator) Len() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.bySlot)
}
