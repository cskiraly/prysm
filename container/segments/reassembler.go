package segments

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

var (
	// ErrTooManyGroups is returned when the reassembler is at its group capacity.
	ErrTooManyGroups = errors.New("too many in-flight segment groups")
	// ErrBufferFull is returned when buffering a segment would exceed the byte budget.
	ErrBufferFull = errors.New("segment buffer budget exhausted")
	// ErrDescriptorConflict is returned when a group's descriptor does not match the pinned one.
	ErrDescriptorConflict = errors.New("descriptor conflicts with the pinned descriptor for this group")
)

// ReassemblerConfig bounds the memory a peer can make us hold.
type ReassemblerConfig struct {
	// MaxGroups caps concurrently tracked segmented messages.
	MaxGroups int
	// MaxBytes caps total buffered segment bytes across all groups.
	MaxBytes int
	// TTL is how long an incomplete group is kept.
	TTL time.Duration
	// Now is injected for tests; defaults to time.Now.
	Now func() time.Time
	// Auth authenticates a group's descriptor once, before any of its segments are
	// buffered. Required unless AllowUnauthenticated is set.
	Auth Authenticator
	// AllowUnauthenticated must be set explicitly to run without an Authenticator, so
	// that accepting unauthenticated descriptors is never the accidental default.
	AllowUnauthenticated bool
	// Retain keeps a group after it completes, and keeps each segment's proof, so the
	// segments can be served back to peers that are still missing them.
	//
	// Off by default because it costs memory for a whole TTL rather than until completion,
	// and only a node that relays segments to peers on demand needs it. A node that merely
	// consumes segments from gossip does not.
	Retain bool
}

// group holds the partially reassembled state for one segmented message.
type group struct {
	desc    *Descriptor
	segs    [][]byte
	have    int
	bytes   int
	created time.Time
	// delivered marks a group whose message has been returned. A coded group keeps
	// accepting segments past its threshold -- each one extends what we can serve -- so
	// "every index filled" stops being the natural once-only guard.
	delivered bool

	// Set only when the config asks for retention, so a consume-only reassembler carries
	// none of this.
	proofs [][][]byte
}

// Reassembler collects verified segments until a message is complete.
//
// It is safe for concurrent use. Verification happens before any buffering, so an
// unverifiable segment can never consume budget.
type Reassembler struct {
	mu     sync.Mutex
	cfg    ReassemblerConfig
	groups map[string]*group
	bytes  int
}

// NewReassembler builds a Reassembler, filling in defaults.
//
// It returns an error rather than silently running unauthenticated, because an unchecked
// descriptor is the difference between segmenting a message and relaying whatever a peer
// sends.
func NewReassembler(cfg ReassemblerConfig) (*Reassembler, error) {
	if cfg.Auth == nil && !cfg.AllowUnauthenticated {
		return nil, ErrNoAuthenticator
	}
	if cfg.MaxGroups <= 0 {
		cfg.MaxGroups = 64
	}
	if cfg.MaxBytes <= 0 {
		cfg.MaxBytes = 64 << 20
	}
	if cfg.TTL <= 0 {
		cfg.TTL = 30 * time.Second
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	return &Reassembler{cfg: cfg, groups: make(map[string]*group)}, nil
}

// Add verifies a segment and buffers it, returning the reassembled message once complete.
//
// The returned message is nil while the group is still incomplete. A duplicate segment is
// accepted idempotently and consumes no additional budget.
func (r *Reassembler) Add(h Hasher, m *SegmentMessage) ([]byte, error) {
	if err := m.Verify(h); err != nil {
		return nil, err
	}
	key := string(m.Descriptor.GroupID(h))

	r.mu.Lock()
	defer r.mu.Unlock()
	r.pruneExpiredLocked()

	g, ok := r.groups[key]
	if !ok {
		if len(r.groups) >= r.cfg.MaxGroups {
			return nil, fmt.Errorf("%w: %d groups", ErrTooManyGroups, len(r.groups))
		}
		// Authenticate once per group, before the first segment is buffered, so an
		// unauthenticated descriptor costs no memory and is never partially accepted.
		if r.cfg.Auth != nil {
			if err := r.cfg.Auth.AuthenticateDescriptor(m.Descriptor, []byte(key)); err != nil {
				return nil, fmt.Errorf("%w: %w", ErrUnauthenticatedDescriptor, err)
			}
		}
		g = &group{
			desc:    m.Descriptor,
			segs:    make([][]byte, m.Descriptor.Count),
			created: r.cfg.Now(),
		}
		if r.cfg.Retain {
			g.proofs = make([][][]byte, m.Descriptor.Count)
		}
		r.groups[key] = g
	} else if !descriptorsEqual(g.desc, m.Descriptor) {
		// GroupID is derived from the descriptor, so this should be unreachable; checked
		// anyway so a hash collision cannot silently mix two messages.
		return nil, ErrDescriptorConflict
	}

	idx := int(m.Index)
	if g.segs[idx] != nil {
		return nil, nil
	}
	if r.bytes+len(m.Data) > r.cfg.MaxBytes {
		return nil, fmt.Errorf("%w: %d + %d > %d", ErrBufferFull, r.bytes, len(m.Data), r.cfg.MaxBytes)
	}
	seg := make([]byte, len(m.Data))
	copy(seg, m.Data)
	g.segs[idx] = seg
	g.have++
	g.bytes += len(seg)
	r.bytes += len(seg)
	if g.proofs != nil {
		g.proofs[idx] = m.Proof
	}

	if g.delivered || uint32(g.have) < g.desc.Required() {
		return nil, nil
	}
	var msg []byte
	var err error
	if g.desc.Version == VersionCoded {
		// Recovered segments are deliberately not stored back into the group: they carry no
		// proofs, so they could never be served, and Held must only advertise what Segment
		// can actually produce.
		msg, err = RecoverAndVerify(g.desc, h, g.segs)
		if err != nil {
			// Every buffered segment proved itself against the pinned root, so an
			// inconsistent codeword is the builder's doing and no further segment can fix
			// it. Keeping the group would re-run recovery on every arrival.
			r.dropLocked(key, g)
			return nil, err
		}
	} else {
		msg, err = Join(g.desc, h, g.segs)
		if err != nil {
			return nil, err
		}
	}
	g.delivered = true
	if r.cfg.Retain {
		// Keep serving peers that are still missing segments. The group ages out on TTL
		// like any other; delivered stops the message being returned twice.
		return msg, nil
	}
	r.dropLocked(key, g)
	return msg, nil
}

// Held returns the set of segment indices held for a group.
//
// The second return is false when the group is not open, which is different from an open
// group holding nothing: the first says we cannot speak about the group at all, and only
// the second lets us announce a count.
func (r *Reassembler) Held(groupID []byte) (*Bitmap, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	g, ok := r.groups[string(groupID)]
	if !ok {
		return nil, false
	}
	b, err := NewBitmap(g.desc.Count)
	if err != nil {
		return nil, false
	}
	for i, seg := range g.segs {
		if seg == nil {
			continue
		}
		// The index cannot be out of range: the slice was sized by the count.
		if err := b.Set(uint32(i)); err != nil {
			return nil, false
		}
	}
	return b, true
}

// Complete reports whether a group's message has been reassembled and delivered.
//
// This, not Held().Full(), is the completeness test: a coded group is done at Required
// segments, and even an uncoded group is done the moment its message was returned.
func (r *Reassembler) Complete(groupID []byte) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	g, ok := r.groups[string(groupID)]
	return ok && g.delivered
}

// Descriptor returns a group's pinned descriptor and its hasher.
func (r *Reassembler) Descriptor(groupID []byte) (*Descriptor, Hasher, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	g, ok := r.groups[string(groupID)]
	if !ok {
		return nil, nil, false
	}
	h, err := HasherByID(g.desc.HashID)
	if err != nil {
		return nil, nil, false
	}
	return g.desc, h, true
}

// Segment rebuilds the wire message for one held segment, so it can be served to a peer.
//
// Only available when the reassembler was configured to Retain: without it the proof is
// discarded on receipt, and a proof cannot be recomputed from a partial group.
func (r *Reassembler) Segment(groupID []byte, index uint32) (*SegmentMessage, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	g, ok := r.groups[string(groupID)]
	if !ok || g.proofs == nil || index >= g.desc.Count {
		return nil, false
	}
	if g.segs[index] == nil || g.proofs[index] == nil {
		return nil, false
	}
	return &SegmentMessage{
		Descriptor: g.desc,
		Index:      index,
		Proof:      g.proofs[index],
		Data:       g.segs[index],
	}, true
}

// Drop discards a group, e.g. once the message has been handled elsewhere.
func (r *Reassembler) Drop(groupID []byte) {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := string(groupID)
	if g, ok := r.groups[key]; ok {
		r.dropLocked(key, g)
	}
}

// Has reports whether a group is already open.
//
// Lets a caller find out whether Add would need to authenticate a descriptor -- and so
// spend a signature verification -- before handing it the segment. An open group needs no
// authentication, because its descriptor was authenticated when it opened and a segment can
// only join it by proving against the pinned root.
func (r *Reassembler) Has(groupID []byte) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.groups[string(groupID)]
	return ok
}

// Prune removes groups past their TTL.
func (r *Reassembler) Prune() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.pruneExpiredLocked()
}

// Groups returns the number of tracked groups.
func (r *Reassembler) Groups() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.groups)
}

// Bytes returns total buffered segment bytes.
func (r *Reassembler) Bytes() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.bytes
}

// dropLocked removes a group and returns its bytes to the budget.
func (r *Reassembler) dropLocked(key string, g *group) {
	r.bytes -= g.bytes
	delete(r.groups, key)
}

// pruneExpiredLocked evicts groups older than the TTL.
func (r *Reassembler) pruneExpiredLocked() {
	cutoff := r.cfg.Now().Add(-r.cfg.TTL)
	for key, g := range r.groups {
		if g.created.Before(cutoff) {
			r.dropLocked(key, g)
		}
	}
}

// descriptorsEqual compares two descriptors field by field.
func descriptorsEqual(a, b *Descriptor) bool {
	if a.Version != b.Version || a.HashID != b.HashID || a.Count != b.Count ||
		a.SegmentSize != b.SegmentSize || a.TotalLength != b.TotalLength {
		return false
	}
	return constantTimeEqual(a.Root, b.Root)
}
