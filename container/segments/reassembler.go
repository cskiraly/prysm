package segments

import (
	"bytes"
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
}

// group holds the partially reassembled state for one segmented message.
type group struct {
	desc    *Descriptor
	segs    [][]byte
	have    int
	bytes   int
	created time.Time
	// delivered marks a group whose message has been returned. The entry stays until its
	// TTL, with its buffers released, so that a segment arriving after completion is
	// recognised as belonging to a finished group rather than reopening it: a coded group
	// keeps producing arrivals past its threshold, and a plain one still sees duplicates.
	delivered bool
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
// The returned message is nil while the group is still incomplete, and nil again for every
// segment of a group that has already been delivered: the message is returned exactly once.
// A duplicate segment is accepted idempotently and consumes no additional budget.
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
		r.groups[key] = g
	} else if !descriptorsEqual(g.desc, m.Descriptor) {
		// GroupID is derived from the descriptor, so this should be unreachable; checked
		// anyway so a hash collision cannot silently mix two messages.
		return nil, ErrDescriptorConflict
	}

	// Verified against the pinned root, so the segment is genuine; the group just has no
	// further use for it.
	if g.delivered {
		return nil, nil
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

	if uint32(g.have) < g.desc.Required() {
		return nil, nil
	}
	var msg []byte
	var err error
	if g.desc.Version == VersionCoded {
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
	r.releaseLocked(g)
	g.delivered = true
	return msg, nil
}

// Complete reports whether a group's message has been reassembled and returned. False for a
// group that is not open, which includes one that completed longer ago than the TTL.
func (r *Reassembler) Complete(groupID []byte) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	g, ok := r.groups[string(groupID)]
	return ok && g.delivered
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

// Has reports whether a group is already open, delivered or not.
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

// Groups returns the number of tracked groups, delivered ones included until their TTL.
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
	r.releaseLocked(g)
	delete(r.groups, key)
}

// releaseLocked returns a group's buffered bytes to the budget and drops the buffers, keeping
// the descriptor and the entry.
func (r *Reassembler) releaseLocked(g *group) {
	r.bytes -= g.bytes
	g.bytes = 0
	g.segs = nil
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

// descriptorsEqual compares two descriptors by their canonical encoding.
func descriptorsEqual(a, b *Descriptor) bool {
	return bytes.Equal(a.MarshalCanonical(), b.MarshalCanonical())
}
