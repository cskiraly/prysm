package segmentgossip

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"

	fieldparams "github.com/OffchainLabs/prysm/v7/config/fieldparams"
	"github.com/OffchainLabs/prysm/v7/config/params"
	"github.com/OffchainLabs/prysm/v7/container/segments"
	ethpb "github.com/OffchainLabs/prysm/v7/proto/prysm/v1alpha1"
	"github.com/golang/snappy"
)

const (
	indexOffset     = 32 + 51 // group key followed by the canonical descriptor
	proofOffset     = indexOffset + 4
	payloadOffset   = proofOffset + 4
	aggregateFixed  = payloadOffset + 4
	voteBytes       = 257 // SSZ bitlist of one committee, including its delimiter.
	dataOffset      = 9
	committeeOffset = dataOffset + 128
)

var committeeWidth = len(ethpb.NewCommitteeBitsPQ())
var voteOffset = committeeOffset + committeeWidth
var signatureOffset = voteOffset + voteBytes
var metaSize = signatureOffset + fieldparams.XMSSSignatureLength
var aggregateHasher, _ = segments.HasherByID(segments.HashSHA256)
var errAggregateFrame = errors.New("invalid aggregate segment frame")

func aggregateMeta(s *ethpb.SignedAggregateAttestationAndProofPQ) ([]byte, error) {
	if s == nil || s.Message == nil || s.Message.Aggregate == nil || s.Message.Aggregate.Data == nil {
		return nil, errors.New("missing aggregate data")
	}
	a := s.Message.Aggregate
	if len(a.CommitteeBits) != committeeWidth || len(a.AggregationBits) > voteBytes || len(s.Signature) != fieldparams.XMSSSignatureLength {
		return nil, errors.New("aggregate does not fit a v1 claim")
	}
	data, err := a.Data.MarshalSSZ()
	if err != nil {
		return nil, err
	}
	m := make([]byte, metaSize)
	m[0] = 1
	binary.LittleEndian.PutUint64(m[1:], uint64(s.Message.AggregatorIndex))
	copy(m[dataOffset:], data)
	copy(m[committeeOffset:], a.CommitteeBits)
	copy(m[voteOffset:], a.AggregationBits)
	copy(m[signatureOffset:], s.Signature)
	return m, nil
}

func aggregateKey(d *segments.Descriptor) [32]byte {
	return sha256.Sum256(append([]byte{3}, d.GroupID(aggregateHasher)...))
}

// AggregateFrame splits a decompressed attached chunk for the topic's message ID.
func AggregateFrame(body []byte) (meta, frame []byte, ok bool) {
	if len(body) < 2 {
		return
	}
	n := int(binary.LittleEndian.Uint16(body))
	if len(body) < 2+n+aggregateFixed {
		return
	}
	f := body[2+n:]
	if binary.LittleEndian.Uint32(f[proofOffset:]) != aggregateFixed {
		return
	}
	return body[2 : 2+n], f, true
}

// AggregateMessageID prefixes the content ID of a parsed frame with its group and index.
func AggregateMessageID(frame []byte, contentID string) string {
	id := make([]byte, 0, MessageIDLen)
	id = append(id, frame[:32]...)
	id = append(id, frame[indexOffset:proofOffset]...)
	return string(append(id, contentID...))
}

func readAggregateChunk(wire []byte) ([]byte, *segments.SegmentMessage, error) {
	n, err := snappy.DecodedLen(wire)
	if err != nil || n > 2+metaSize+aggregateFixed+segments.MaxProofDepth*32+segments.MaxWireSegmentSize {
		return nil, nil, errAggregateFrame
	}
	body, err := snappy.Decode(nil, wire)
	if err != nil {
		return nil, nil, err
	}
	meta, f, ok := AggregateFrame(body)
	if !ok {
		return nil, nil, errAggregateFrame
	}
	at := binary.LittleEndian.Uint32(f[payloadOffset:])
	if uint64(at) > uint64(len(f)) || at < aggregateFixed || (at-aggregateFixed)%32 != 0 || at-aggregateFixed > segments.MaxProofDepth*32 {
		return nil, nil, errAggregateFrame
	}
	d := &segments.Descriptor{
		Version: f[32], HashID: segments.HashID(f[33]), Encoding: segments.Encoding(f[34]),
		Count: binary.LittleEndian.Uint32(f[35:]), SegmentSize: binary.LittleEndian.Uint32(f[39:]),
		TotalLength: binary.LittleEndian.Uint64(f[43:]), Root: f[51:indexOffset],
	}
	maxBody := params.BeaconConfig().MaxSignedAggregateAndProofSizePQ
	if d.Count > MaxPlainChunks || d.SegmentSize > segments.MaxWireSegmentSize ||
		d.TotalLength > uint64(snappy.MaxEncodedLen(int(maxBody))) || d.Validate(aggregateHasher) != nil {
		return nil, nil, errAggregateFrame
	}
	if d.Version == segments.VersionCoded && d.Count != 2*d.Required() {
		return nil, nil, errAggregateFrame
	}
	key := aggregateKey(d)
	if !bytes.Equal(f[:32], key[:]) {
		return nil, nil, errAggregateFrame
	}
	m := &segments.SegmentMessage{Descriptor: d, Group: f[:32], Index: binary.LittleEndian.Uint32(f[indexOffset:]), Data: f[at:]}
	for proof := f[aggregateFixed:at]; len(proof) > 0; proof = proof[32:] {
		m.Proof = append(m.Proof, proof[:32])
	}
	return meta, m, nil
}

func aggregateWire(key [32]byte, m *segments.SegmentMessage, meta []byte) []byte {
	body := make([]byte, 2, 2+len(meta)+aggregateFixed+32*len(m.Proof)+len(m.Data))
	binary.LittleEndian.PutUint16(body, uint16(len(meta)))
	body = append(body, meta...)
	body = append(body, key[:]...)
	body = append(body, m.Descriptor.MarshalCanonical()...)
	body = binary.LittleEndian.AppendUint32(body, m.Index)
	body = binary.LittleEndian.AppendUint32(body, aggregateFixed)
	body = binary.LittleEndian.AppendUint32(body, uint32(aggregateFixed+32*len(m.Proof)))
	for _, p := range m.Proof {
		body = append(body, p...)
	}
	return snappy.Encode(nil, append(body, m.Data...))
}
