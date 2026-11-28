package segmentgossip

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math/bits"
	"slices"
	"strings"
	"sync"

	"github.com/OffchainLabs/prysm/v7/beacon-chain/verification/segmentauth"
	"github.com/OffchainLabs/prysm/v7/config/params"
	"github.com/OffchainLabs/prysm/v7/container/segments"
	ethpb "github.com/OffchainLabs/prysm/v7/proto/prysm/v1alpha1"
	"github.com/golang/snappy"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
	"google.golang.org/protobuf/proto"
)

const AggregateTopicName = "beacon_aggregate_and_proof_segment"
const AggregateTypeName = "aggregate"
const aggregateGroupsPerSlot = 64
const maxDeferredAggregateWrites = 1024

var _ pubsub.AttachedObject = (*AggregateType)(nil)

type aggregateGroup struct {
	root, topic   string
	slot          uint64 // Local arrival slot, used for expiry.
	meta          []byte
	votes         []byte
	count         int
	source        peer.ID
	told          map[peer.ID]bool
	held, settled bool
	enqueued      bool // A source publication reached at least one peer queue.
	deferred      []pubsub.DeferredChunk
	whole         *pubsub.Message
}

func (g *aggregateGroup) ranksWith(o *aggregateGroup) bool {
	return g.count != 0 && o.count != 0 && bytes.Equal(g.meta[dataOffset:dataOffset+8], o.meta[dataOffset:dataOffset+8]) &&
		bytes.Equal(g.meta[committeeOffset:voteOffset], o.meta[committeeOffset:voteOffset])
}

func (g *aggregateGroup) covers(o *aggregateGroup) bool {
	if !g.ranksWith(o) {
		return false
	}
	for i, b := range o.votes {
		if b&^g.votes[i] != 0 {
			return false
		}
	}
	return true
}

// AggregateType shares the router's group shapes and cell store with the payload implementation.
type AggregateType struct {
	*PayloadType
	rankMu sync.Mutex
	live   map[string]*aggregateGroup
	order  []*aggregateGroup // Arrival order also resolves promotion ties.
}

func NewAggregateType(cfg PayloadConfig) *AggregateType {
	a := &AggregateType{PayloadType: NewPayloadType(cfg), live: make(map[string]*aggregateGroup)}
	a.params.DecodeAtK = func(string) bool { return true }
	return a
}

func (*AggregateType) Name() string { return AggregateTypeName }

func (*AggregateType) Family(string) string { return AggregateTypeName }

func (*AggregateType) WholeTopic(topic string) string {
	return strings.Replace(topic, "/"+AggregateTopicName+"/", "/beacon_aggregate_and_proof/", 1)
}

func (t *AggregateType) TopicOf(root string) (string, bool) {
	t.rankMu.Lock()
	defer t.rankMu.Unlock()
	if g := t.live[root]; g != nil {
		return g.topic, true
	}
	return "", false
}

func (t *AggregateType) Advance(slot uint64) {
	t.rankMu.Lock()
	defer t.rankMu.Unlock()
	t.PayloadType.Advance(slot, 1)
	t.order = slices.DeleteFunc(t.order, func(g *aggregateGroup) bool {
		if g.slot >= slot {
			return false
		}
		delete(t.live, g.root)
		return true
	})
}

// The rank compares the SSZ bitlist including its delimiter, as carried in the claim.
func newAggregateGroup(root, topic string, meta []byte) *aggregateGroup {
	g := &aggregateGroup{root: root, topic: topic, meta: bytes.Clone(meta), told: make(map[peer.ID]bool)}
	g.votes = g.meta[voteOffset:signatureOffset]
	for _, b := range g.votes {
		g.count += bits.OnesCount8(b)
	}
	return g
}

func (t *AggregateType) covered(g *aggregateGroup) bool {
	return slices.ContainsFunc(t.order, func(o *aggregateGroup) bool {
		return o != g && o.settled && !o.held && (o.source != "" || o.enqueued) && o.covers(g)
	})
}

func (t *AggregateType) open(g *aggregateGroup, d *segments.Descriptor) bool {
	// Expired group records prevent a late copy from reopening a group.
	slot, fresh := t.NoteGroupSlot(g.root, g.topic, int(d.Count), int(d.Required()))
	if !fresh {
		return false
	}
	g.slot = slot
	t.live[g.root] = g
	t.order = append(t.order, g)
	return true
}

func (t *AggregateType) AdmitFrame(m *pubsub.Message) (pubsub.ValidationResult, string) {
	meta, seg, err := readAggregateChunk(m.Data)
	if err != nil || seg.Verify(aggregateHasher) != nil {
		return pubsub.ValidationReject, ""
	}
	root := string(seg.Group)
	verdict, opened := t.admitGroup(root, m.GetTopic(), m.ReceivedFrom, meta, seg.Descriptor)
	if verdict == pubsub.ValidationAccept && len(meta) > 0 {
		// Keep one bare copy in the store. Attachments belong to a link, even on IWANT replies.
		m.Message = proto.Clone(m.Message).(*pb.Message)
		m.Data = aggregateWire([32]byte(seg.Group), seg, nil)
	}
	return verdict, opened
}

func (t *AggregateType) admitGroup(root, topic string, from peer.ID, meta []byte, d *segments.Descriptor) (pubsub.ValidationResult, string) {
	t.rankMu.Lock()
	defer t.rankMu.Unlock()
	if g := t.live[root]; g != nil {
		if g.topic != topic {
			return pubsub.ValidationIgnore, ""
		}
		g.told[from] = true
		return pubsub.ValidationAccept, ""
	}
	if len(meta) != metaSize || meta[0] != 1 {
		return pubsub.ValidationIgnore, ""
	}
	var remote int
	for _, o := range t.order {
		if o.source != "" {
			remote++
		}
	}
	if remote >= aggregateGroupsPerSlot || len(t.order) >= aggregateGroupsPerSlot+1 {
		return pubsub.ValidationIgnore, ""
	}
	g := newAggregateGroup(root, topic, meta)
	g.source = from
	if !t.open(g, d) {
		return pubsub.ValidationIgnore, ""
	}
	g.told[from] = true
	t.rank(g)
	return pubsub.ValidationAccept, root
}

// Before decode there is one primary per slot and committee. Only a strict superset displaces it.
func (t *AggregateType) rank(g *aggregateGroup) {
	for _, o := range t.order {
		if o == g || o.held || o.settled || !o.ranksWith(g) {
			continue
		}
		if g.count > o.count && g.covers(o) {
			o.held = true
		} else {
			g.held = true
		}
		return
	}
	g.held = t.covered(g)
}

// Publish builds the canonical frames. The binding adds the claim at each peer's writer.
func (t *AggregateType) Publish(s *ethpb.SignedAggregateAttestationAndProofPQ, topic string, enqueue func([][]byte) error) error {
	meta, err := aggregateMeta(s)
	if err != nil {
		return err
	}
	g := newAggregateGroup("", topic, meta)
	t.rankMu.Lock()
	covered := t.covered(g)
	t.rankMu.Unlock()
	if covered {
		return nil
	}
	body, err := s.MarshalSSZ()
	if err != nil {
		return err
	}
	if uint64(len(body)) > params.BeaconConfig().MaxSignedAggregateAndProofSizePQ {
		return errors.New("aggregate exceeds gossip size limit")
	}
	chunks, err := segmentauth.SegmentAggregate(body)
	if err != nil {
		return err
	}
	d := chunks[0].Descriptor
	key := aggregateKey(d)
	g.root = string(key[:])
	g, err = t.openLocal(g, d)
	if err != nil || g == nil {
		return err
	}
	out := make([][]byte, len(chunks))
	for i, m := range chunks {
		out[i] = aggregateWire(key, m, nil)
	}
	if err := enqueue(out); err != nil {
		return err
	}
	t.rankMu.Lock()
	g.enqueued = true
	t.rankMu.Unlock()
	return nil
}

func (t *AggregateType) openLocal(g *aggregateGroup, d *segments.Descriptor) (*aggregateGroup, error) {
	t.rankMu.Lock()
	defer t.rankMu.Unlock()
	if t.covered(g) {
		return nil, nil
	}
	if existing := t.live[g.root]; existing != nil {
		if existing.source == "" && !existing.enqueued {
			return existing, nil
		}
		return nil, nil
	}
	if len(t.order) >= aggregateGroupsPerSlot+1 {
		return nil, errors.New("aggregate group limit reached")
	}
	if !t.open(g, d) {
		return nil, nil
	}
	g.settled = true
	return g, nil
}

func (t *AggregateType) NoteHolding(root string, p peer.ID) {
	t.rankMu.Lock()
	defer t.rankMu.Unlock()
	if g := t.live[root]; g != nil {
		g.told[p] = true
	}
}

func (t *AggregateType) ForgetPeer(p peer.ID) {
	t.rankMu.Lock()
	defer t.rankMu.Unlock()
	for _, g := range t.order {
		delete(g.told, p)
		g.deferred = slices.DeleteFunc(g.deferred, func(w pubsub.DeferredChunk) bool { return w.Peer == p })
	}
}

func (t *AggregateType) AllowOffer(root string) bool {
	t.rankMu.Lock()
	defer t.rankMu.Unlock()
	g := t.live[root]
	return g != nil && !g.held
}

func (t *AggregateType) PrepareWrite(w pubsub.DeferredChunk, data []byte) ([]byte, bool) {
	meta, send := t.claimForWrite(w)
	if !send {
		return nil, false
	}
	if meta == nil {
		return data, true
	}
	// Every stored frame is bare and verified; only its link-local prefix changes.
	body, err := snappy.Decode(nil, data)
	if err != nil || len(body) < 2 {
		return nil, false
	}
	attached := make([]byte, 2, len(body)+len(meta))
	binary.LittleEndian.PutUint16(attached, uint16(len(meta)))
	attached = append(attached, meta...)
	return snappy.Encode(nil, append(attached, body[2:]...)), true
}

func (t *AggregateType) claimForWrite(w pubsub.DeferredChunk) ([]byte, bool) {
	t.rankMu.Lock()
	defer t.rankMu.Unlock()
	g := t.live[w.Root]
	if g == nil {
		return nil, false
	}
	if g.held {
		if !g.settled && len(g.deferred) < maxDeferredAggregateWrites {
			g.deferred = append(g.deferred, w)
		}
		return nil, false
	}
	if g.told[w.Peer] {
		return nil, true
	}
	g.told[w.Peer] = true
	return g.meta, true
}

// Settle decides forwarding at codeword verification. A covered group still delivers its
// aggregate locally. A failed primary releases the largest uncovered waiting claim.
func (t *AggregateType) Settle(root string, passed bool) (whole *pubsub.Message, writes []pubsub.DeferredChunk) {
	t.rankMu.Lock()
	defer t.rankMu.Unlock()
	g := t.live[root]
	if g == nil || g.settled {
		return
	}
	g.held = !passed || t.covered(g)
	g.settled = true
	if passed {
		whole = g.whole
	}
	g.whole = nil
	if !g.held {
		writes = g.deferred
	}
	g.deferred = nil
	if passed {
		return
	}
	var best *aggregateGroup
	for _, o := range t.order {
		if o.settled || !o.ranksWith(g) {
			continue
		}
		if !o.held {
			return
		}
		if !t.covered(o) && (best == nil || o.count >= best.count) {
			best = o
		}
	}
	if best != nil {
		best.held = false
		writes = best.deferred
		best.deferred = nil
	}
	return
}

// Decode snapshots immutable frames before the worker reconstructs and verifies the codeword.
func (t *AggregateType) Decode(root string, held []uint32) func() (map[uint32]*pubsub.Message, bool) {
	t.rankMu.Lock()
	g := t.live[root]
	t.rankMu.Unlock()
	frames := make([][]byte, 0, len(held))
	for _, i := range held {
		if m, ok := t.Store().Get(root, i); ok {
			frames = append(frames, m.Data)
		}
	}
	return func() (map[uint32]*pubsub.Message, bool) {
		if g == nil || len(frames) == 0 {
			return nil, false
		}
		var d *segments.Descriptor
		var shards [][]byte
		for i, f := range frames {
			_, m, err := readAggregateChunk(f)
			if err != nil || string(m.Group) != root {
				return nil, false
			}
			if d == nil {
				d = m.Descriptor
				shards = make([][]byte, d.Count)
			}
			if m.Index >= uint32(len(shards)) {
				return nil, false
			}
			shards[m.Index] = m.Data
			// Exactly K leaves determine the codeword, even if additional parity arrived.
			if i+1 == int(d.Required()) {
				break
			}
		}
		var committed []byte
		var err error
		if d.Version == segments.VersionCoded {
			committed, err = segments.RecoverAndVerify(d, aggregateHasher, shards)
		} else {
			committed, err = segments.Join(d, aggregateHasher, shards)
		}
		if err != nil {
			return nil, false
		}
		rebuilt := make(map[uint32]*pubsub.Message)
		if len(frames) < len(shards) {
			cut, err := segments.Build(committed, segments.Layout{Hasher: aggregateHasher, SegmentSize: int(d.SegmentSize), Parity: int(d.Count - d.Required()), Encoding: d.Encoding})
			if err != nil || !sameDescriptor(d, cut[0].Descriptor) {
				return nil, false
			}
			for i, m := range cut {
				if shards[i] == nil {
					rebuilt[m.Index] = &pubsub.Message{Message: &pb.Message{Topic: &g.topic, Data: aggregateWire([32]byte([]byte(root)), m, nil)}}
				}
			}
		}
		wire := committed
		if d.Encoding == segments.EncodingRaw {
			wire = snappy.Encode(nil, committed)
		}
		topic := t.WholeTopic(g.topic)
		n, err := snappy.DecodedLen(wire)
		if err == nil && uint64(n) <= params.BeaconConfig().MaxSignedAggregateAndProofSizePQ {
			t.rankMu.Lock()
			if t.live[root] == g && g.source != "" {
				g.whole = &pubsub.Message{Message: &pb.Message{Topic: &topic, Data: wire}, ReceivedFrom: g.source, Local: true}
			}
			t.rankMu.Unlock()
		}
		return rebuilt, true
	}
}
