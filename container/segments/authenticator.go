package segments

import "errors"

var (
	// ErrUnauthenticatedDescriptor is returned when descriptor authentication fails.
	ErrUnauthenticatedDescriptor = errors.New("descriptor failed authentication")
	// ErrNoAuthenticator is returned when a Reassembler is built without an Authenticator
	// and without explicitly opting out.
	ErrNoAuthenticator = errors.New("no authenticator configured and AllowUnauthenticated is false")
)

// Why authentication is an interface rather than a fixed scheme.
//
// A Merkle proof shows only that a segment belongs to Descriptor.Root; nothing inside the
// tree says the root is legitimate. Without an Authenticator, a peer can build a flawless
// tree over arbitrary bytes and we would propagate it. This is the seam the design turns on.
//
// The intended end state is that the builder's segment commitment travels inside
// SignedExecutionPayloadBid, carried in the gloas beacon block body. That is the right home:
// the commitment arrives with the block, ahead of the segments it describes, already covered
// by a signature, and the block's own proposer signature means a proposer cannot forge it
// either. Adding that field changes the container's hash_tree_root and therefore the
// signature preimage, so it is a consensus change needing a spec PR.
//
// Given that, authentication reduces to a membership test. Block processing gives a node the
// set of group ids the chain has committed to; a segment is authentic exactly when its group
// id is in that set. Nothing on the wire has to say which block a segment belongs to, and
// nothing on the wire is trusted: the group id is derived from the descriptor the sender
// supplied, and either it is committed or it is not.
//
// Two earlier schemes are worth recording, because both were worse for instructive reasons.
// A detached builder signature on every segment bounded nothing -- it proved *a* registered
// key signed *a* descriptor, and one key may sign any number, so an adversary could mint
// unboundedly many groups. Then a slot-and-block-root "anchor" on every segment, which was
// redundant with the group id and, being outside both the descriptor and the Merkle tree,
// rewritable without invalidating any proof -- so it needed an invariant and an error of its
// own to be safe at all. Naming the block is genuinely useful before the body is fetched, to
// decline an announcement; that belongs in the message id, not in every segment body.
//
// The coupling worth naming: proof verification needs Count, Count comes from the committed
// descriptor, and the commitment comes from the block. So a segment cannot be authenticated
// before its block. That is the price of anchoring in consensus, not an artefact of this
// interface.
//
// Implementations must authenticate the *whole* descriptor, not just its root. groupID is
// the domain-separated hash of every field affecting reassembly (see Descriptor.GroupID),
// so a membership test on groupID pins K, segment size, total length and hash choice along
// with the root. Committing to the root alone would leave the framing unbound.
type Authenticator interface {
	// AuthenticateDescriptor reports whether d, identified by groupID, is a group the chain
	// has committed to.
	AuthenticateDescriptor(d *Descriptor, groupID []byte) error
}

// UnsafeAcceptAllDescriptors authenticates nothing.
//
// Deliberately verbose: it is for tests and local experiments only. Using it on a live
// network means accepting and forwarding attacker-chosen bytes.
type UnsafeAcceptAllDescriptors struct{}

// AuthenticateDescriptor always succeeds.
func (UnsafeAcceptAllDescriptors) AuthenticateDescriptor(*Descriptor, []byte) error {
	return nil
}
