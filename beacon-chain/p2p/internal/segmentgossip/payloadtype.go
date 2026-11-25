package segmentgossip

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/OffchainLabs/prysm/v7/beacon-chain/p2p/encoder"
	"github.com/OffchainLabs/prysm/v7/config/params"
	"github.com/OffchainLabs/prysm/v7/container/segments"
	ethpb "github.com/OffchainLabs/prysm/v7/proto/prysm/v1alpha1"
	"github.com/golang/snappy"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pubsubpb "github.com/libp2p/go-libp2p-pubsub/pb"
)

// PayloadTypeName is the payload's object type, and its one family (seacast.md §6.1).
const PayloadTypeName = "payload"

// MaxPlainChunks is the payload's MAX_PLAIN_CHUNKS (seacast-lite.md §8), the most indices a payload
// group can have. Before a group's descriptor is known, it stands for the group's count.
const MaxPlainChunks = 8192

// PayloadConfig is what the payload type needs from the node.
type PayloadConfig struct {
	// Topics is the segment topic at every fork digest the node can run, from Gloas on.
	Topics []string
	// CurrentTopic is the segment topic at the current fork digest.
	CurrentTopic func() string
	// CurrentSlot is the node's current slot.
	CurrentSlot func() uint64
	// UplinkMbps is the node's configured uplink, which sets the push budget.
	UplinkMbps int
}

// groupShape is what the node learned of a group when it published it or its first segment passed
// validation.
type groupShape struct {
	n, k  int
	slot  uint64
	topic string
	// dropped is set once the live window passed the group and its segments went.
	dropped bool
}

// DecodeCounts is what the payload type's decodes count, for the node's metrics.
type DecodeCounts struct {
	// DecodeRecoveries counts the decodes that recovered the group's committed bytes from its held
	// segments (segments.RecoverAndVerify).
	DecodeRecoveries atomic.Int64
}

// PayloadType is the execution payload on SEACAST's path (seacast.md §6.1): one group per
// payload, keyed by its group key (seacast-lite.md §1, the chunk's group field and the head of its
// message id), every peer with interest ALL and duty FORWARD.
// It implements pubsub.ObjectType and pubsub.Decoder.
//
// A group's shape (its N and K), the slot that keys it and its topic are recorded by NoteGroup: at
// the source when it publishes the group, elsewhere when the group's first segment passes
// validation. Until then the group reads as MaxPlainChunks indices with K of the same, so nothing
// completes or decodes on a count the node does not know.
type PayloadType struct {
	cfg    PayloadConfig
	params pubsub.TypeParams
	store  *pubsub.MapCellStore
	// recordSlots is RecordSlots at the configuration the type was made with.
	recordSlots uint64
	counts      DecodeCounts

	mu     sync.Mutex
	groups map[string]groupShape
	// slot is the slot last given to the router, once slotKnown.
	slot      uint64
	slotKnown bool
}

var (
	_ pubsub.ObjectType = (*PayloadType)(nil)
	_ pubsub.Decoder    = (*PayloadType)(nil)
)

// NewPayloadType makes the payload type for a node.
func NewPayloadType(cfg PayloadConfig) *PayloadType {
	t := &PayloadType{
		cfg:         cfg,
		params:      PayloadParams(cfg.UplinkMbps),
		store:       pubsub.NewMapCellStore(),
		recordSlots: 2 * uint64(params.BeaconConfig().SlotsPerEpoch),
		groups:      map[string]groupShape{},
	}
	t.params.DecodeAtK = t.coded
	return t
}

// Counts is what the type's decodes counted.
func (t *PayloadType) Counts() *DecodeCounts { return &t.counts }

// coded is the type's DecodeAtK: the node decodes a group at K when the group has parity segments,
// so that K of them rebuild the rest. A plain group (K = N) completes when every segment is in, with
// nothing to rebuild; a group not yet noted has no K.
func (t *PayloadType) coded(root string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	g, ok := t.groups[root]
	return ok && g.n > g.k
}

// NoteGroup records a group's count and threshold, the topic its segments travel on, and the node's
// slot as the group's: at the source before it publishes the group, elsewhere when the group's first
// segment passes validation. The node's slot is the one last given to the router (Advance), and the
// clock's before the first. It reports whether the group was new, so the caller can tell the router
// that the count is known (SeacastHeaderValidated, which drops offers beyond it). A group whose record
// still stands, expired or not, is not new.
func (t *PayloadType) NoteGroup(root, topic string, n, k int) bool {
	_, isNew := t.NoteGroupSlot(root, topic, n, k)
	return isNew
}

// NoteGroupSlot is NoteGroup that also returns the group's slot, read under the same lock: the slot the
// new group was keyed by, or the one its standing record holds.
func (t *PayloadType) NoteGroupSlot(root, topic string, n, k int) (uint64, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if g, ok := t.groups[root]; ok {
		return g.slot, false
	}
	slot := t.slot
	if !t.slotKnown {
		slot = t.cfg.CurrentSlot()
	}
	t.groups[root] = groupShape{n: n, k: k, slot: slot, topic: topic}
	return slot, true
}

// RecordSlots is how long the type keeps a group's record after the group's slot: two epochs of the
// configuration the type was made with (64 slots at 32 an epoch), the span of the node's seen cache,
// longer than any copy of the group is offered or relayed. While the record stands, a late segment,
// offer or ask of the group finds its slot, and the router drops it as expired; after it, the group
// could be taken up again as new.
func (t *PayloadType) RecordSlots() uint64 { return t.recordSlots }

// Advance takes the slot the service gave the router. A group noted from now on is keyed by it, so the
// type and the router read one clock. The groups the router expires at this slot (their slot plus
// window at most slot, the router's own test) lose their segments now; their record goes RecordSlots
// slots after their slot. A lower slot than the last is ignored, as the router ignores it.
func (t *PayloadType) Advance(slot, window uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.slotKnown && slot < t.slot {
		return
	}
	t.slot, t.slotKnown = slot, true
	for root, g := range t.groups {
		if g.slot+t.recordSlots <= slot {
			delete(t.groups, root)
			t.store.Drop(root)
			continue
		}
		if !g.dropped && g.slot+window <= slot {
			g.dropped = true
			t.groups[root] = g
			t.store.Drop(root)
		}
	}
}

func (*PayloadType) Name() string { return PayloadTypeName }

func (t *PayloadType) Params() pubsub.TypeParams { return t.params }

func (t *PayloadType) Topics() []string { return t.cfg.Topics }

// HeaderKnown is true for every group. The descriptor travels in each segment and the validator
// authenticates it there, so there is no header to wait for; a gate would hold a group's first
// segment for ever.
func (*PayloadType) HeaderKnown(string) bool { return true }

func (t *PayloadType) IndexCount(root string) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	if g, ok := t.groups[root]; ok {
		return g.n
	}
	return MaxPlainChunks
}

func (t *PayloadType) Threshold(root string) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	if g, ok := t.groups[root]; ok {
		return g.k
	}
	return MaxPlainChunks
}

func (t *PayloadType) Store() pubsub.CellStore { return t.store }

// SlotOf is the node's slot when it first saw the group (NoteGroup), not the slot its chunks name: the
// router's live window runs on the node's own clock, and the record keeps it for RecordSlots slots.
func (t *PayloadType) SlotOf(root string) (uint64, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	g, ok := t.groups[root]
	return g.slot, ok
}

// TopicOf is the topic the group's segments travel on: the one recorded for it, so that a segment the
// node rebuilds after a fork digest changes keeps its topic and with it its message id; for a group not
// yet recorded, the segment topic at the current digest, and none while the node doesn't know it.
func (t *PayloadType) TopicOf(root string) (string, bool) {
	if len(root) != 32 {
		return "", false
	}
	t.mu.Lock()
	g, ok := t.groups[root]
	t.mu.Unlock()
	if ok && g.topic != "" {
		return g.topic, true
	}
	topic := t.cfg.CurrentTopic()
	return topic, topic != ""
}

func (*PayloadType) DerivedInterest(string, pubsub.NodeID, pubsub.Join) ([]uint32, bool) {
	return nil, false
}

func (*PayloadType) Cross(string, uint32) (pubsub.GroupIndex, string, bool) {
	return pubsub.GroupIndex{}, "", false
}

func (*PayloadType) Family(string) string { return PayloadTypeName }

// Decode is the payload's decode (seacast.md §4.6's T.decode). On the router's goroutine it takes
// the held segments' wire bytes from the store. Off it, it recovers the committed bytes from them,
// cuts them again as the publisher cut them, gives the cut the held segments' slot, block root and
// group key, checks that the cut is the group's, and frames every missing index as the broadcaster
// frames a segment. So each rebuilt message is byte for byte the source's, and has the id the
// network already uses for it.
func (t *PayloadType) Decode(root string, held []uint32) func() (map[uint32]*pubsub.Message, bool) {
	chunks := make(map[uint32][]byte, len(held))
	var topic string
	for _, i := range held {
		m, ok := t.store.Get(root, i)
		if !ok || m == nil || m.Message == nil {
			continue
		}
		chunks[i] = m.GetData()
		if topic == "" {
			topic = m.GetTopic()
		}
	}
	return func() (map[uint32]*pubsub.Message, bool) {
		out, err := rebuildMissing(root, topic, chunks, &t.counts)
		if err != nil {
			return nil, false
		}
		return out, true
	}
}

var errDecode = errors.New("payload decode")

// rebuildMissing is Decode's work: the missing segments of the group whose key is group, from the
// held segments' wire bytes, as wire messages on topic. A recovery is counted in counts.
func rebuildMissing(group, topic string, chunks map[uint32][]byte, counts *DecodeCounts) (map[uint32]*pubsub.Message, error) {
	if topic == "" || len(chunks) == 0 {
		return nil, fmt.Errorf("%w: no held segment", errDecode)
	}
	var first *segments.SegmentMessage
	var d *segments.Descriptor
	var h segments.Hasher
	var segs [][]byte
	for i, wire := range chunks {
		m, mh, err := decodeWire(wire)
		if err != nil {
			return nil, err
		}
		if string(m.Group) != group {
			return nil, fmt.Errorf("%w: a held segment of another group", errDecode)
		}
		if first == nil {
			first, d, h = m, m.Descriptor, mh
			segs = make([][]byte, d.Count)
		} else if !sameDescriptor(d, m.Descriptor) {
			return nil, fmt.Errorf("%w: held segments disagree on the descriptor", errDecode)
		} else if m.Slot != first.Slot || !bytes.Equal(m.BeaconBlockRoot, first.BeaconBlockRoot) {
			// The key binds the slot, so held segments of one group name one slot; the block root is
			// not bound, and segments that disagree on it cannot all be rebuilt as the source's.
			return nil, fmt.Errorf("%w: held segments disagree on the block", errDecode)
		}
		if m.Index != i || int(i) >= len(segs) {
			return nil, fmt.Errorf("%w: segment %d held under index %d", errDecode, m.Index, i)
		}
		segs[i] = m.Data
	}
	k := int(d.Required()) // lint:ignore uintcast -- bounded by MaxSegments.
	parity := len(segs) - k
	var committed []byte
	var err error
	if d.Version == segments.VersionCoded {
		counts.DecodeRecoveries.Add(1)
		committed, err = segments.RecoverAndVerify(d, h, segs)
	} else {
		committed, err = segments.Join(d, h, segs)
	}
	if err != nil {
		return nil, fmt.Errorf("%w: %w", errDecode, err)
	}
	if d.Version != segments.VersionCoded {
		parity = 0
	}
	cut, err := segments.Build(committed, segments.Layout{
		SegmentSize: int(d.SegmentSize),
		Hasher:      h,
		Encoding:    d.Encoding,
		Parity:      parity,
	})
	if err != nil {
		return nil, fmt.Errorf("%w: %w", errDecode, err)
	}
	if len(cut) != len(segs) || !sameDescriptor(d, cut[0].Descriptor) {
		return nil, fmt.Errorf("%w: the rebuilt group is not the committed one", errDecode)
	}
	if err := segments.WithContext(cut, h, first.Slot, first.BeaconBlockRoot); err != nil {
		return nil, fmt.Errorf("%w: %w", errDecode, err)
	}
	if string(cut[0].Group) != group {
		return nil, fmt.Errorf("%w: the rebuilt group's key is not the group's", errDecode)
	}
	out := make(map[uint32]*pubsub.Message, len(segs)-len(chunks))
	for i, seg := range cut {
		if segs[i] != nil {
			continue
		}
		pb, err := seg.ToProto()
		if err != nil {
			return nil, fmt.Errorf("%w: %w", errDecode, err)
		}
		var buf bytes.Buffer
		if _, err := (encoder.SszNetworkEncoder{}).EncodeGossip(&buf, pb); err != nil {
			return nil, fmt.Errorf("%w: %w", errDecode, err)
		}
		tp := topic
		out[seg.Index] = &pubsub.Message{Message: &pubsubpb.Message{Data: buf.Bytes(), Topic: &tp}}
	}
	return out, nil
}

// decodeWire reads a segment from its gossip wire form.
func decodeWire(wire []byte) (*segments.SegmentMessage, segments.Hasher, error) {
	body, err := snappy.Decode(nil, wire)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: %w", errDecode, err)
	}
	pb := &ethpb.ExecutionPayloadSegment{}
	if err := pb.UnmarshalSSZ(body); err != nil {
		return nil, nil, fmt.Errorf("%w: %w", errDecode, err)
	}
	m, h, err := segments.FromProto(pb)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: %w", errDecode, err)
	}
	return m, h, nil
}

func sameDescriptor(a, b *segments.Descriptor) bool {
	return a.Version == b.Version && a.HashID == b.HashID && a.Encoding == b.Encoding && a.Count == b.Count &&
		a.SegmentSize == b.SegmentSize && a.TotalLength == b.TotalLength && bytes.Equal(a.Root, b.Root)
}
