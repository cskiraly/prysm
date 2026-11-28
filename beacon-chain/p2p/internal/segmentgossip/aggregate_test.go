package segmentgossip

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"testing"

	fieldparams "github.com/OffchainLabs/prysm/v7/config/fieldparams"
	"github.com/OffchainLabs/prysm/v7/container/segments"
	ethpb "github.com/OffchainLabs/prysm/v7/proto/prysm/v1alpha1"
	"github.com/OffchainLabs/prysm/v7/testing/require"
	"github.com/golang/snappy"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pb "github.com/libp2p/go-libp2p-pubsub/pb"
)

const testAggregateTopic = "/eth2/07070707/beacon_aggregate_and_proof_segment/ssz_snappy"

func testAggregateType() *AggregateType {
	return NewAggregateType(PayloadConfig{Topics: []string{testAggregateTopic}, CurrentSlot: func() uint64 { return 9 }, UplinkMbps: 100})
}

func signedAggregate(bits, id byte) *ethpb.SignedAggregateAttestationAndProofPQ {
	a := &ethpb.AttestationPQ{
		Data: &ethpb.AttestationData{
			Slot: 9, BeaconBlockRoot: bytes.Repeat([]byte{0xb1}, 32),
			Source: &ethpb.Checkpoint{Root: make([]byte, 32)}, Target: &ethpb.Checkpoint{Root: make([]byte, 32)},
		},
		CommitteeBits: ethpb.NewCommitteeBitsPQ(), AggregationBits: []byte{bits},
	}
	a.CommitteeBits[0] = 1
	return &ethpb.SignedAggregateAttestationAndProofPQ{
		Signature: bytes.Repeat([]byte{id}, fieldparams.XMSSSignatureLength),
		Message: &ethpb.AggregateAttestationAndProofPQ{
			AggregatorIndex: 123, Aggregate: a, Proof: bytes.Repeat([]byte{id}, 50000),
		},
	}
}

func captureFrames(src *AggregateType, s *ethpb.SignedAggregateAttestationAndProofPQ, topic string) (frames [][]byte, err error) {
	err = src.Publish(s, topic, func(rows [][]byte) error { frames = rows; return nil })
	return
}

func admit(t *AggregateType, m *pubsub.Message) pubsub.ValidationResult {
	result, _ := t.AdmitFrame(m)
	return result
}

func testMessage(data []byte) *pubsub.Message {
	topic := testAggregateTopic
	return &pubsub.Message{Message: &pb.Message{Data: data, Topic: &topic}, ReceivedFrom: "upstream"}
}

func TestAggregateClaimAndReconstruction(t *testing.T) {
	src, dst := testAggregateType(), testAggregateType()
	signed := signedAggregate(7, 4)
	frames, err := captureFrames(src, signed, testAggregateTopic)
	require.NoError(t, err)
	_, first, err := readAggregateChunk(frames[0])
	require.NoError(t, err)
	key := sha256.Sum256(append([]byte{2}, first.Descriptor.MarshalCanonical()...))
	key = sha256.Sum256(append([]byte{3}, key[:]...))
	require.DeepEqual(t, key[:], first.Group)
	root := string(key[:])
	require.Equal(t, pubsub.ValidationIgnore, admit(dst, testMessage(frames[0])))
	var held []uint32
	for _, wire := range frames[first.Descriptor.Required():] { // Recover from parity alone.
		_, m, e := readAggregateChunk(wire)
		require.NoError(t, e)
		w := pubsub.DeferredChunk{Root: root, Index: m.Index, Peer: "receiver"}
		outgoing, ok := src.PrepareWrite(w, wire)
		require.Equal(t, true, ok)
		meta, _, e := readAggregateChunk(outgoing)
		require.NoError(t, e)
		if len(held) == 0 {
			require.Equal(t, 1+8+128+committeeWidth+257+2404, len(meta))
			require.Equal(t, byte(1), meta[0])
			require.Equal(t, uint64(123), binary.LittleEndian.Uint64(meta[1:]))
			att, e := signed.Message.Aggregate.Data.MarshalSSZ()
			require.NoError(t, e)
			require.DeepEqual(t, att, meta[9:137])
			require.DeepEqual(t, []byte(signed.Message.Aggregate.CommitteeBits), meta[137:137+committeeWidth])
			require.DeepEqual(t, append([]byte{7}, make([]byte, 256)...), meta[137+committeeWidth:len(meta)-2404])
			require.DeepEqual(t, signed.Signature, meta[len(meta)-2404:])
		} else {
			require.Equal(t, 0, len(meta))
		}
		msg := testMessage(outgoing)
		require.Equal(t, pubsub.ValidationAccept, admit(dst, msg))
		dst.Store().Put(root, m.Index, msg)
		held = append(held, m.Index)
	}
	rebuilt, ok := dst.Decode(root, held)()
	require.Equal(t, true, ok)
	require.Equal(t, len(frames)-len(held), len(rebuilt))
	for i, m := range rebuilt {
		require.DeepEqual(t, frames[i], m.Data)
	}
	whole, _ := dst.Settle(root, true)
	require.NotNil(t, whole)
	require.Equal(t, true, whole.Local)
	require.Equal(t, "upstream", string(whole.ReceivedFrom))
	body, err := snappy.Decode(nil, whole.Data)
	require.NoError(t, err)
	want, err := signed.MarshalSSZ()
	require.NoError(t, err)
	require.DeepEqual(t, want, body)
	// A holding proves that a peer already knows the group. A reconnect loses that knowledge.
	w := pubsub.DeferredChunk{Root: root, Peer: "third"}
	dst.NoteHolding(root, w.Peer)
	out, ok := dst.PrepareWrite(w, frames[0])
	require.Equal(t, true, ok)
	require.DeepEqual(t, frames[0], out)
	dst.ForgetPeer(w.Peer)
	out, ok = dst.PrepareWrite(w, frames[0])
	require.Equal(t, true, ok)
	meta, _, err := readAggregateChunk(out)
	require.NoError(t, err)
	require.Equal(t, metaSize, len(meta))
	// A corrupt proof cannot open a group, even with the exact metadata of a correct copy.
	bad := *first
	bad.Data = bytes.Clone(bad.Data)
	bad.Data[0] ^= 1
	fresh := testAggregateType()
	require.Equal(t, pubsub.ValidationReject, admit(fresh, testMessage(aggregateWire(key, &bad, meta))))
	meta[0]++
	require.Equal(t, pubsub.ValidationIgnore, admit(fresh, testMessage(aggregateWire(key, first, meta))))
	dst.Advance(10)
	_, ok = dst.Store().Get(root, held[0])
	require.Equal(t, false, ok)
	require.Equal(t, pubsub.ValidationIgnore, admit(dst, testMessage(out)))
}

func TestAggregateForwarding(t *testing.T) {
	for _, pass := range []bool{true, false} {
		dst := testAggregateType()
		open := func(bits, id, committee byte) (pubsub.DeferredChunk, []byte) {
			src := testAggregateType()
			signed := signedAggregate(bits, id)
			signed.Message.Aggregate.CommitteeBits[0] = committee
			rows, err := captureFrames(src, signed, testAggregateTopic)
			require.NoError(t, err)
			_, m, err := readAggregateChunk(rows[0])
			require.NoError(t, err)
			w := pubsub.DeferredChunk{Root: string(m.Group), Peer: "next", Solicited: true}
			wire, ok := src.PrepareWrite(w, rows[0])
			require.Equal(t, true, ok)
			require.Equal(t, pubsub.ValidationAccept, admit(dst, testMessage(wire)))
			return w, rows[0]
		}
		first, a := open(3, 1, 1)
		primary, _ := open(7, 2, 1)
		other, b := open(9, 3, 1)
		equal, c := open(7, 4, 1)
		separate, _ := open(3, 5, 2)
		require.Equal(t, true, dst.AllowOffer(primary.Root))
		require.Equal(t, true, dst.AllowOffer(separate.Root))
		for _, p := range []struct {
			w    pubsub.DeferredChunk
			wire []byte
		}{{first, a}, {other, b}, {equal, c}} {
			require.Equal(t, false, dst.AllowOffer(p.w.Root))
			_, send := dst.PrepareWrite(p.w, p.wire)
			require.Equal(t, false, send)
		}
		_, writes := dst.Settle(primary.Root, pass)
		if pass {
			require.Equal(t, 0, len(writes))
			_, writes = dst.Settle(first.Root, true)
			require.Equal(t, 0, len(writes))
			require.Equal(t, false, dst.AllowOffer(first.Root))
			_, writes = dst.Settle(other.Root, true)
			require.DeepEqual(t, []pubsub.DeferredChunk{other}, writes)
			_, writes = dst.Settle(equal.Root, true)
			require.Equal(t, 0, len(writes))
			covered, err := captureFrames(dst, signedAggregate(3, 99), testAggregateTopic)
			require.NoError(t, err)
			require.Equal(t, 0, len(covered))
		} else {
			// The equal claimant has the most votes of those waiting.
			require.DeepEqual(t, []pubsub.DeferredChunk{equal}, writes)
			require.Equal(t, true, dst.AllowOffer(equal.Root))
			require.Equal(t, false, dst.AllowOffer(primary.Root))
			_, writes = dst.Settle(other.Root, false)
			require.Equal(t, 0, len(writes))
		}
	}
}

func TestAggregateDecodeRejectsInconsistentCodeword(t *testing.T) {
	src, dst := testAggregateType(), testAggregateType()
	signed := signedAggregate(7, 8)
	rows, err := captureFrames(src, signed, testAggregateTopic)
	require.NoError(t, err)
	meta, err := aggregateMeta(signed)
	require.NoError(t, err)
	var values [][]byte
	var chunks []*segments.SegmentMessage
	for _, wire := range rows {
		_, m, err := readAggregateChunk(wire)
		require.NoError(t, err)
		values = append(values, m.Data)
		chunks = append(chunks, m)
	}
	values[len(values)-1][0] ^= 1 // The Merkle tree authenticates each leaf, including this inconsistent parity leaf.
	tree, err := segments.BuildTree(aggregateHasher, values)
	require.NoError(t, err)
	var held []uint32
	var root string
	for _, m := range chunks[:len(chunks)/2] {
		m.Descriptor.Root = tree.Root()
		key := aggregateKey(m.Descriptor)
		root = string(key[:])
		m.Proof, err = tree.Proof(int(m.Index))
		require.NoError(t, err)
		msg := testMessage(aggregateWire(key, m, meta))
		require.Equal(t, pubsub.ValidationAccept, admit(dst, msg))
		dst.Store().Put(root, m.Index, msg)
		held = append(held, m.Index)
	}
	_, ok := dst.Decode(root, held)()
	require.Equal(t, false, ok)
	whole, writes := dst.Settle(root, false)
	require.Equal(t, (*pubsub.Message)(nil), whole)
	require.Equal(t, 0, len(writes))
	require.Equal(t, false, dst.AllowOffer(root))
}
