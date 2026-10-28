package segmentintegrationtest

// Structured segment message ids.
//
// Today's ids are content hashes, so an announcement does not say which block would authorize
// the message it advertises. That is what stops a receiver from declining to request what it
// cannot yet authenticate. The message-id function is application-installed and both ends here
// are ours, so the convention can be adopted unilaterally in simulation: nothing about measuring
// the gated-pull design needs a spec change first.
//
// The id is a hybrid, not a bare structural key: gossipsub deduplicates by id, so a purely
// structural id would let one bad first arrival poison that id for every later honest copy.
//
//	prefix (81 B) || inherited 20-byte id = 101-byte id
//
// The same 81-byte prefix rides at the front of the opaque segment bytes, so every receiver derives
// the id from the wire rather than from a side table. The digest half is Prysm's own message id
// rather than a fresh hash, which is what keeps the id bound to the *decompressed* data and to the
// topic. Enabled by SEGMENT_STRUCTURED_IDS; off, the harness computes Prysm's MsgID exactly as
// before.

import (
	"crypto/sha256"
	"encoding/binary"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/OffchainLabs/prysm/v7/beacon-chain/p2p"
	"github.com/OffchainLabs/prysm/v7/beacon-chain/p2p/encoder"
	"github.com/OffchainLabs/prysm/v7/config/params"
	"github.com/OffchainLabs/prysm/v7/consensus-types/primitives"
	"github.com/OffchainLabs/prysm/v7/container/segments"
	"github.com/OffchainLabs/prysm/v7/crypto/bls"
	ethpb "github.com/OffchainLabs/prysm/v7/proto/prysm/v1alpha1"
	"github.com/OffchainLabs/prysm/v7/testing/require"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pubsubpb "github.com/libp2p/go-libp2p-pubsub/pb"
)

// Prefix layout, little-endian throughout.
//
// The magic is load-bearing rather than decorative. Without it the version byte alone is not a
// discriminator: an ordinary unprefixed SegmentMessage also begins with 0x01, because that is
// segments.Version at the head of the canonical descriptor -- so every long unprefixed segment
// would be misread as a structural claim with garbage fields. In production the right answer is not
// a magic at all but explicit fields on an outer SSZ container; this is a harness shim that lets
// the opaque byte field carry a claim without a proto change.
const (
	segIDMagic     = "SGID"
	segIDVersion   = 1
	segIDMagicAt   = 0
	segIDVersionAt = 4
	segIDSlotAt    = 5
	segIDRootAt    = 13
	segIDGroupAt   = 45
	segIDIndexAt   = 77
	segIDPrefixLen = 81
	segIDDigestLen = 20 // the inherited id width, kept as the digest half
	segIDLen       = segIDPrefixLen + segIDDigestLen
)

// segKey is the structural half of a segment id: what the message claims to be, as opposed to
// what its bytes hash to.
type segKey struct {
	slot      uint64
	blockRoot [32]byte
	group     [32]byte
	index     uint32
}

// authorityKey is the part a receiver needs a block for. The gate keys on the block root alone;
// slot travels with it so a pre-block freshness check is possible, as the data-column sidecar
// rule requires.
func (k segKey) authorityKey() [32]byte { return k.blockRoot }

// structuralKey identifies the (block, group, index) slot a message claims to fill, ignoring its
// content digest. Aggregate caps key on this: without it an attacker announces unlimited
// content-hash variants of one slot and walks past every per-id cap.
func (k segKey) structuralKey() segKey {
	k.slot = 0
	return k
}

func (k segKey) encode() []byte {
	b := make([]byte, segIDPrefixLen)
	copy(b[segIDMagicAt:], segIDMagic)
	b[segIDVersionAt] = segIDVersion
	binary.LittleEndian.PutUint64(b[segIDSlotAt:], k.slot)
	copy(b[segIDRootAt:], k.blockRoot[:])
	copy(b[segIDGroupAt:], k.group[:])
	binary.LittleEndian.PutUint32(b[segIDIndexAt:], k.index)
	return b
}

// parseSegPrefix decodes a prefix. The malformed branch is explicit: a wrong version or a short
// buffer is not an error to propagate but a message that simply has no structural claim, and it
// must fall back to a content-hash id rather than being dropped.
func parseSegPrefix(b []byte) (segKey, bool) {
	if len(b) < segIDPrefixLen || string(b[segIDMagicAt:segIDVersionAt]) != segIDMagic ||
		b[segIDVersionAt] != segIDVersion {
		return segKey{}, false
	}
	var k segKey
	k.slot = binary.LittleEndian.Uint64(b[segIDSlotAt:])
	copy(k.blockRoot[:], b[segIDRootAt:segIDGroupAt])
	copy(k.group[:], b[segIDGroupAt:segIDIndexAt])
	k.index = binary.LittleEndian.Uint32(b[segIDIndexAt:])
	return k, true
}

// segIDKey recovers the structural claim from a message id produced by structuredMsgID.
func segIDKey(mid string) (segKey, bool) {
	if len(mid) != segIDLen {
		return segKey{}, false
	}
	return parseSegPrefix([]byte(mid[:segIDPrefixLen]))
}

// prependSegPrefix puts the prefix in front of the opaque segment bytes so the id is derivable
// from the wire. Returns a fresh slice; the caller's buffer is untouched.
func prependSegPrefix(k segKey, segment []byte) []byte {
	out := make([]byte, 0, segIDPrefixLen+len(segment))
	out = append(out, k.encode()...)
	return append(out, segment...)
}

// forceStructuredIDs lets a test that cannot work without structured ids turn them on for itself.
// The two-publisher driver is the case: its gate reads a structural claim out of the message id, so
// running it with content-hash ids would measure a gate that silently passes everything. Set before
// arms are built and the network is constructed; package tests here are sequential (synctest
// forbids parallel), so a package-level switch is safe.
var forceStructuredIDs bool

// structuredIDsEnabled reports whether this cell runs structured ids.
func structuredIDsEnabled() bool {
	return forceStructuredIDs || os.Getenv("SEGMENT_STRUCTURED_IDS") != ""
}

// sszBytesFieldOffset is the SSZ offset a single variable-size `bytes` field carries: a
// four-byte offset pointing just past itself. ExecutionPayloadSegment is exactly that shape, so
// the prefix sits at a fixed position and the id function can read it without unmarshalling the
// 32 KiB segment body. TestSegmentPrefixAtFixedOffset pins the assumption against a real decode.
const sszBytesFieldOffset = 4

// segmentPrefixFromGossip extracts the structural prefix from an encoded gossip message body.
func segmentPrefixFromGossip(data []byte) (segKey, bool) {
	decoded, err := encoder.DecodeSnappy(data, params.BeaconConfig().MaxPayloadSize)
	if err != nil {
		return segKey{}, false
	}
	if len(decoded) < sszBytesFieldOffset+segIDPrefixLen {
		return segKey{}, false
	}
	if binary.LittleEndian.Uint32(decoded[:sszBytesFieldOffset]) != sszBytesFieldOffset {
		return segKey{}, false
	}
	return parseSegPrefix(decoded[sszBytesFieldOffset:])
}

// structuredMsgID is the message-id function for a cell running structured ids: a 101-byte
// self-describing id on the segment topics, Prysm's 20-byte MsgID everywhere else (background
// traffic included, which is why the topic is checked as well as the payload).
func structuredMsgID(genesisValidatorsRoot []byte) func(*pubsubpb.Message) string {
	segPrefix := segmentTopic()
	return func(pmsg *pubsubpb.Message) string {
		if pmsg == nil || pmsg.Data == nil || pmsg.Topic == nil {
			return p2p.MsgID(genesisValidatorsRoot, pmsg)
		}
		if !strings.HasPrefix(*pmsg.Topic, segPrefix) {
			return p2p.MsgID(genesisValidatorsRoot, pmsg)
		}
		k, ok := segmentPrefixFromGossip(pmsg.Data)
		if !ok {
			return p2p.MsgID(genesisValidatorsRoot, pmsg)
		}
		// The digest half is the inherited id itself, not a hash of the wire bytes. Two things
		// hang on that. It is computed over the *decompressed* data, so the several valid snappy
		// encodings of one object cannot become several ids and slip past deduplication; and it is
		// domain-separated over the topic, so the same bytes replayed on another indexed segment
		// topic get a different id -- otherwise a wrong-topic replay could occupy the seen cache
		// and suppress the correct-topic copy, since seen state and the mcache are keyed by id
		// alone. Cost: pmsg.Data is decompressed twice, once here and once inside MsgID.
		id := make([]byte, 0, segIDLen)
		id = append(id, k.encode()...)
		return string(append(id, p2p.MsgID(genesisValidatorsRoot, pmsg)...))
	}
}

// syntheticSlot is the slot every arm in this package segments at. Named because the gate has to
// derive the same block root the arms claim, and a bare literal in two places is how those drift.
const syntheticSlot = primitives.Slot(2048)

// cellBlockRoot is the beacon block root a cell's segments claim to be authorized by.
//
// Derived from the slot rather than from a real block, and the two-publisher driver carries this
// value *in* the block it publishes so that authority is granted for exactly the root the segments
// name. That keeps the root real to the experiment -- a node opens the root it received, not "the
// root", which is what makes competing-fork and hostile-root behaviour expressible at all -- while
// keeping the harness free of block SSZ.
func cellBlockRoot(slot primitives.Slot) [32]byte {
	var b [40]byte
	copy(b[:], "SEGMENT_CELL_BLOCK_ROOT_V1")
	binary.LittleEndian.PutUint64(b[32:], uint64(slot))
	return sha256.Sum256(b[:])
}

// applyStructuredPrefixes puts a structural prefix on every segment of a group, in place.
//
// No-op unless the cell enables structured ids, so every existing arm keeps its exact wire bytes
// and its measured byte totals. When enabled, each segment grows by the 77-byte prefix -- 0.23%
// of a 32 KiB segment, but it is real wire cost and lands in the arm's byte accounting.
func applyStructuredPrefixes(msgs []*ethpb.ExecutionPayloadSegment, slot primitives.Slot, group []byte) {
	if !structuredIDsEnabled() {
		return
	}
	root := cellBlockRoot(slot)
	var g [32]byte
	copy(g[:], group)
	for i, m := range msgs {
		k := segKey{slot: uint64(slot), blockRoot: root, group: g, index: uint32(i)} // lint:ignore uintcast -- bounded by MaxSegments.
		m.Segment = prependSegPrefix(k, m.Segment)
	}
}

// groupIDFor recomputes a payload's group id, which the arm builders need for the prefix but
// which signedSegments keeps inside the descriptor.
func groupIDFor(t *testing.T, payload []byte, segmentSize int) []byte {
	t.Helper()
	h, err := segments.HasherByID(segments.HashSHA256)
	require.NoError(t, err)
	d, _, err := segments.Commit(payload, segmentSize, h)
	require.NoError(t, err)
	return d.GroupID(h)
}

// msgIDFn is the message-id function every node in a cell installs: Prysm's by default, the
// structured one when the cell enables it.
func msgIDFn(genesisValidatorsRoot []byte) func(*pubsubpb.Message) string {
	if structuredIDsEnabled() {
		return structuredMsgID(genesisValidatorsRoot)
	}
	return func(pmsg *pubsubpb.Message) string { return p2p.MsgID(genesisValidatorsRoot, pmsg) }
}

// structuredIDOpts installs msgIDFn over the one gossipsim's production options set, and is empty
// when the cell runs content-hash ids.
//
// It exists because the id function is the seam the whole authority layer hangs from: the gate and
// the admission queue read a structural claim out of the id, so a cell running Prysm's 20-byte id
// has a gate that passes everything and a queue that never sees a claim. gossipsim installs its own
// (correctly -- it is the generic harness and knows nothing about segment ids), and the promoted
// options are appended before per-node ones, so overriding here is the intended layering rather
// than a fight with it.
//
// The genesis root is zero on both sides, matching gossipsim, so the digest halves agree.
func structuredIDOpts() []pubsub.Option {
	if !structuredIDsEnabled() {
		return nil
	}
	var genesisValidatorsRoot [32]byte
	opts := []pubsub.Option{pubsub.WithMessageIdFn(msgIDFn(genesisValidatorsRoot[:]))}
	// SEGMENT_HAVE_BITMAP=1: one HAVE bitmap per group in place of one IDONTWANT per segment
	// (scheduled-diffusion-plan.md §7c, D1). The group root keys the bitmap, the index is its bit.
	// SEGMENT_HAVE_BITMAP=1|idontwant (D1: the IDONTWANT bitmap), ihave (D1b: the IHAVE bitmap,
	// IDONTWANT kept per id) or both. Each bitmap inherits its message's semantics and lane.
	if mode := os.Getenv("SEGMENT_HAVE_BITMAP"); mode != "" {
		opts = append(opts, pubsub.WithHaveBitmaps(haveIndexOf))
		switch mode {
		case "1", "idontwant":
		case "ihave":
			opts = append(opts, pubsub.WithHaveAnnounceBitmaps(), pubsub.WithHaveIDontWantPerID())
		case "both":
			opts = append(opts, pubsub.WithHaveAnnounceBitmaps())
		default:
			panic("bad SEGMENT_HAVE_BITMAP " + mode + " (want 1, idontwant, ihave or both)")
		}
		// SEGMENT_ORIGIN_HAVE=1: the publisher sends its HAVE for each group it batch-publishes, as a receiver's goes.
		if os.Getenv("SEGMENT_ORIGIN_HAVE") != "" {
			opts = append(opts, pubsub.WithOriginHave())
		}
		// SEGMENT_SRC_HAVE: what the publisher's HAVE says of its own group instead of every frame at publish (privacy card P4):
		// paced grows it as its writers pop the frames; none sends none; sent tells each peer the frames sent it;
		// dilute:<ms> is paced, cut per peer to a relay's curve by the Q6 cell (SEGMENT_SRC_HAVE there).
		switch v := os.Getenv("SEGMENT_SRC_HAVE"); {
		case v == "":
		case v == "paced" || strings.HasPrefix(v, "dilute:"):
			opts = append(opts, pubsub.WithOriginHavePaced())
		case v == "none":
			opts = append(opts, pubsub.WithOriginHaveMode(pubsub.OriginHaveNone))
		case v == "sent":
			opts = append(opts, pubsub.WithOriginHaveMode(pubsub.OriginHaveSent))
		default:
			panic("bad SEGMENT_SRC_HAVE " + v + " (want paced, none, sent or dilute:<ms>)")
		}
		// SEGMENT_DECODE_CLEARS_PROMISES=1: the decode claim fulfils the group's gossip-tracer promises (implies SEGMENT_PROMISE_COUNT).
		if os.Getenv("SEGMENT_DECODE_CLEARS_PROMISES") != "" {
			opts = append(opts, pubsub.WithDecodeClearsPromises())
		}
		// D1c: SEGMENT_HAVE_FAST_IHAVE=1 puts the announce bitmap in the urgent lane carrying the
		// announcer's queued bytes to the peer; SEGMENT_HAVE_ASK_DELAY_MBPS makes the asker hold
		// a loaded announce for the time that load takes at this rate (cap
		// SEGMENT_HAVE_ASK_DELAY_CAP_MS, default 500). Unset: IHAVE in the normal lane.
		if mode == "ihave" || mode == "both" {
			if os.Getenv("SEGMENT_HAVE_FAST_IHAVE") != "" {
				opts = append(opts, pubsub.WithHaveAnnounceUrgent(true))
			}
			if v := os.Getenv("SEGMENT_HAVE_ASK_DELAY_MBPS"); v != "" {
				mbps, err := strconv.Atoi(v)
				if err != nil || mbps <= 0 {
					panic("bad SEGMENT_HAVE_ASK_DELAY_MBPS " + v)
				}
				capMs := 500
				if c := os.Getenv("SEGMENT_HAVE_ASK_DELAY_CAP_MS"); c != "" {
					if capMs, err = strconv.Atoi(c); err != nil || capMs <= 0 {
						panic("bad SEGMENT_HAVE_ASK_DELAY_CAP_MS " + c)
					}
				}
				opts = append(opts, pubsub.WithHaveAskDelay(int64(mbps)*1_000_000, time.Duration(capMs)*time.Millisecond))
			}
		}
		// SEGMENT_DISPATCH_CREDITS=<A>[,sticky]: a node feeds at most A peers at a time; a slot is
		// kept while the peer's queue has data (sticky) or rotates to the longest waiter (§7c D5).
		if v := os.Getenv("SEGMENT_DISPATCH_CREDITS"); v != "" {
			a, sticky := strings.CutSuffix(v, ",sticky")
			n, err := strconv.Atoi(a)
			if err != nil || n < 1 {
				panic("bad SEGMENT_DISPATCH_CREDITS " + v + " (want <A>[,sticky], A >= 1)")
			}
			opts = append(opts, pubsub.WithDispatchCredits(n, sticky))
		}
		// SEGMENT_HAVE_DISPATCH=cancel|attach|both: the writer drops a queued frame the peer has
		// since announced, and/or carries a pending HAVE on the next RPC out (§7c, D2).
		if v := os.Getenv("SEGMENT_HAVE_DISPATCH"); v != "" {
			cancel, attach := v == "cancel" || v == "both", v == "attach" || v == "both"
			if !cancel && !attach {
				panic("bad SEGMENT_HAVE_DISPATCH " + v + " (want cancel, attach or both)")
			}
			opts = append(opts, pubsub.WithHaveDispatch(cancel, attach))
			// The dispatch ledger (pop and HAVE times per peer and frame) rides along: the horizon
			// classes each duplicate by whether its sender could have cancelled it (§7c, after Q116).
			opts = append(opts, pubsub.WithHaveLedger())
			// SEGMENT_HAVE_RAREST=1: the writer pops the rarest queued frame first (§7c D3).
			if os.Getenv("SEGMENT_HAVE_RAREST") != "" {
				opts = append(opts, pubsub.WithHaveRarestDispatch())
			}
		}
	}
	return opts
}

// haveIndexOf is the HAVE bitmap's view of a structured id: the group root and the index.
func haveIndexOf(mid string) (string, uint32, bool) {
	k, ok := segIDKey(mid)
	if !ok {
		return "", 0, false
	}
	return string(k.group[:]), k.index, true
}

func TestSegmentIDPrefixRoundTrip(t *testing.T) {
	k := segKey{slot: 0x0102030405060708, index: 0xdeadbeef}
	for i := range k.blockRoot {
		k.blockRoot[i] = byte(i)
		k.group[i] = byte(0xa0 + i)
	}

	t.Run("encode then parse", func(t *testing.T) {
		enc := k.encode()
		require.Equal(t, segIDPrefixLen, len(enc))
		got, ok := parseSegPrefix(enc)
		require.Equal(t, true, ok)
		require.DeepEqual(t, k, got)
	})

	t.Run("id carries the prefix verbatim", func(t *testing.T) {
		mid := string(k.encode()) + strings.Repeat("x", segIDDigestLen)
		got, ok := segIDKey(mid)
		require.Equal(t, true, ok)
		require.DeepEqual(t, k, got)
	})

	t.Run("wrong version is not a structural claim", func(t *testing.T) {
		enc := k.encode()
		enc[segIDVersionAt] = segIDVersion + 1
		_, ok := parseSegPrefix(enc)
		require.Equal(t, false, ok)
	})

	t.Run("short buffer is not a structural claim", func(t *testing.T) {
		_, ok := parseSegPrefix(k.encode()[:segIDPrefixLen-1])
		require.Equal(t, false, ok)
	})

	t.Run("a 20-byte content id has no structural claim", func(t *testing.T) {
		_, ok := segIDKey(strings.Repeat("z", 20))
		require.Equal(t, false, ok)
	})

	t.Run("structural key drops the slot but nothing else", func(t *testing.T) {
		other := k
		other.slot++
		require.DeepEqual(t, k.structuralKey(), other.structuralKey())
		other.index++
		require.Equal(t, false, k.structuralKey() == other.structuralKey())
	})
}

// TestSegmentPrefixAtFixedOffset pins the fixed-offset read against a real SSZ decode. If the
// wire type ever gains a field the offset shifts and the id function would read garbage, so this
// is the assumption's alarm rather than a restatement of it.
func TestSegmentPrefixAtFixedOffset(t *testing.T) {
	k := segKey{slot: 42, index: 7}
	k.blockRoot[0] = 0x11
	k.group[0] = 0x22
	body := prependSegPrefix(k, []byte("segment payload bytes"))

	enc := encoder.SszNetworkEncoder{}
	var buf strings.Builder
	_, err := enc.EncodeGossip(&buf, &ethpb.ExecutionPayloadSegment{Segment: body})
	require.NoError(t, err)
	wire := []byte(buf.String())

	t.Run("fixed-offset read", func(t *testing.T) {
		got, ok := segmentPrefixFromGossip(wire)
		require.Equal(t, true, ok)
		require.DeepEqual(t, k, got)
	})

	t.Run("agrees with a full decode", func(t *testing.T) {
		var msg ethpb.ExecutionPayloadSegment
		require.NoError(t, enc.DecodeGossip(wire, &msg))
		got, ok := parseSegPrefix(msg.Segment)
		require.Equal(t, true, ok)
		require.DeepEqual(t, k, got)
	})

	t.Run("unprefixed bytes fall through", func(t *testing.T) {
		var buf2 strings.Builder
		_, err := enc.EncodeGossip(&buf2, &ethpb.ExecutionPayloadSegment{Segment: []byte("no prefix here")})
		require.NoError(t, err)
		_, ok := segmentPrefixFromGossip([]byte(buf2.String()))
		require.Equal(t, false, ok)
	})

	// The version byte alone is not a discriminator: segments.Version is also 1 and sits at the
	// head of the canonical descriptor, so a real unprefixed segment starts with the same byte a
	// prefix does. Short synthetic bodies pass a version-only check for the wrong reason, which is
	// how the first version of this test missed it -- so use the genuine article.
	t.Run("a real unprefixed segment is not a structural claim", func(t *testing.T) {
		params.SetupTestConfigCleanup(t)
		sk, err := bls.RandKey()
		require.NoError(t, err)
		payload := highEntropyPayload(64<<10, 7)
		for _, tc := range []struct {
			name string
			arm  wireArm
		}{
			{"segmented", segmentedArm(t, payload, 32<<10, sk, primitives.Slot(2048))},
			{"coded", codedArm(t, payload, 32<<10, 2, sk, primitives.Slot(2048))},
			{"whole", wholeArm(t, payload)},
		} {
			t.Run(tc.name, func(t *testing.T) {
				for i, m := range tc.arm.msgs {
					if _, ok := segmentPrefixFromGossip(m); ok {
						t.Fatalf("message %d read as a structural claim it does not carry", i)
					}
				}
			})
		}
	})
}

// TestStructuredMsgIDDispatch checks the id function's three branches: a prefixed segment on a
// segment topic gets a structured id, the same bytes on another topic do not, and an unprefixed
// message on a segment topic falls back rather than being rejected.
func TestStructuredMsgIDDispatch(t *testing.T) {
	var genesis [32]byte
	idFn := structuredMsgID(genesis[:])
	enc := encoder.SszNetworkEncoder{}
	wireFor := func(t *testing.T, body []byte) []byte {
		t.Helper()
		var buf strings.Builder
		_, err := enc.EncodeGossip(&buf, &ethpb.ExecutionPayloadSegment{Segment: body})
		require.NoError(t, err)
		return []byte(buf.String())
	}

	k := segKey{slot: 99, index: 3}
	k.blockRoot[31] = 0xff
	prefixed := wireFor(t, prependSegPrefix(k, []byte("data")))
	plain := wireFor(t, []byte("data"))

	segTopic := segmentTopic()
	idxTopic := segmentTopicIndexed(3)
	otherTopic := "/eth2/00000000/some_other_topic/ssz_snappy"

	t.Run("segment topic, prefixed", func(t *testing.T) {
		mid := idFn(&pubsubpb.Message{Data: prefixed, Topic: &segTopic})
		require.Equal(t, segIDLen, len(mid))
		got, ok := segIDKey(mid)
		require.Equal(t, true, ok)
		require.DeepEqual(t, k, got)
	})

	t.Run("indexed segment topic, prefixed", func(t *testing.T) {
		mid := idFn(&pubsubpb.Message{Data: prefixed, Topic: &idxTopic})
		require.Equal(t, segIDLen, len(mid))
	})

	t.Run("other topic keeps the inherited id", func(t *testing.T) {
		mid := idFn(&pubsubpb.Message{Data: prefixed, Topic: &otherTopic})
		require.Equal(t, 20, len(mid))
	})

	t.Run("unprefixed on a segment topic falls back", func(t *testing.T) {
		mid := idFn(&pubsubpb.Message{Data: plain, Topic: &segTopic})
		require.Equal(t, 20, len(mid))
		require.Equal(t, p2p.MsgID(genesis[:], &pubsubpb.Message{Data: plain, Topic: &segTopic}), mid)
	})

	t.Run("the same bytes on two indexed topics get different ids", func(t *testing.T) {
		// Seen state and the mcache are keyed by id alone, and a message is marked seen before
		// application validation -- so if a wrong-topic replay shared an id with the correct-topic
		// copy it could occupy the cache and suppress it. Topic binding is what forecloses that.
		three := segmentTopicIndexed(3)
		four := segmentTopicIndexed(4)
		a := idFn(&pubsubpb.Message{Data: prefixed, Topic: &three})
		b := idFn(&pubsubpb.Message{Data: prefixed, Topic: &four})
		require.Equal(t, false, a == b)
		ka, okA := segIDKey(a)
		kb, okB := segIDKey(b)
		require.Equal(t, true, okA && okB)
		require.DeepEqual(t, ka, kb) // identical structural claim, different digest half
	})

	t.Run("distinct content, same structural slot, distinct ids", func(t *testing.T) {
		a := idFn(&pubsubpb.Message{Data: prefixed, Topic: &segTopic})
		other := wireFor(t, prependSegPrefix(k, []byte("different data")))
		b := idFn(&pubsubpb.Message{Data: other, Topic: &segTopic})
		require.Equal(t, false, a == b)
		ka, _ := segIDKey(a)
		kb, _ := segIDKey(b)
		require.DeepEqual(t, ka.structuralKey(), kb.structuralKey())
	})
}
