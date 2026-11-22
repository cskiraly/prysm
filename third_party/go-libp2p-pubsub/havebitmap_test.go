package pubsub

import (
	"context"
	"crypto/sha256"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p-pubsub/timecache"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
)

// testIndex reads the test id layout: 32-byte root, one index byte, then anything.
func testIndex(mid string) (string, uint32, bool) {
	if len(mid) < 34 || mid[33] != '#' {
		return "", 0, false
	}
	return mid[:32], uint32(mid[32]), true
}

func testMid(root string, index byte) string {
	return root + string([]byte{index}) + "#digest"
}

func testHaveRouter() *GossipSubRouter {
	h := newHaveState(testIndex)
	h.holdTTL, h.pushTTL = 3, 3 // the TTL subtest counts heartbeats against these
	return &GossipSubRouter{
		params:   DefaultGossipSubParams(),
		unwanted: make(map[peer.ID]map[checksum]int),
		have:     h,
	}
}

func TestHaveBitsEncoding(t *testing.T) {
	var b haveBits
	b.Set(0)
	b.Set(9)
	b.Set(63)
	if !b.Has(0) || !b.Has(9) || !b.Has(63) || b.Has(1) || b.Has(64) || b.Has(1000) {
		t.Fatal("Set/Has disagree")
	}
	if got := len(b.Bytes()); got != 8 {
		t.Fatalf("Bytes() = %d bytes, want 8 (index 63 lives in byte 7)", got)
	}
	var c haveBits
	c.Set(80)
	c.b[10] = 0 // clear again by hand: trailing zeros must trim
	if got := len(c.Bytes()); got != 0 {
		t.Fatalf("all-zero bitmap encodes as %d bytes, want 0", got)
	}

	root := strings.Repeat("r", 32)
	mid := encodeHave(root, &b)
	gotRoot, enc, ok := decodeHave(mid)
	if !ok || gotRoot != root || len(enc) != 8 {
		t.Fatalf("round trip failed: ok=%v root=%q enc=%d", ok, gotRoot, len(enc))
	}
	var d haveBits
	newly := d.Or(enc)
	if len(newly) != 3 || newly[0] != 0 || newly[1] != 9 || newly[2] != 63 {
		t.Fatalf("Or reported %v as newly set, want [0 9 63]", newly)
	}
	if again := d.Or(enc); len(again) != 0 {
		t.Fatalf("a second Or reported %v as new", again)
	}

	for name, bad := range map[string]string{
		"ordinary id":   testMid(root, 3),
		"short":         "HAVE\x01" + root[:10],
		"wrong magic":   "have\x01" + root,
		"wrong version": "HAVE\x02" + root,
		"too long":      "HAVE\x01" + root + strings.Repeat("x", haveMaxBytes+1),
	} {
		if _, _, ok := decodeHave(bad); ok {
			t.Fatalf("%s decoded as a HAVE", name)
		}
	}
}

func TestHaveExpandsIntoUnwanted(t *testing.T) {
	root := strings.Repeat("g", 32)
	p := peer.ID("peer")

	t.Run("HAVE first, then the message arrives", func(t *testing.T) {
		gs := testHaveRouter()
		var b haveBits
		b.Set(2)
		if !gs.haveAbsorb(p, encodeHave(root, &b)) {
			t.Fatal("pseudo id not recognised")
		}
		if len(gs.unwanted[p]) != 0 {
			t.Fatal("nothing known yet, so nothing should be unwanted")
		}
		gs.haveNoteArrival(testMid(root, 2))
		gs.haveNoteArrival(testMid(root, 3))
		if _, ok := gs.unwanted[p][computeChecksum(testMid(root, 2))]; !ok {
			t.Fatal("index 2 should be unwanted by the peer that announced it")
		}
		if _, ok := gs.unwanted[p][computeChecksum(testMid(root, 3))]; ok {
			t.Fatal("index 3 was never announced")
		}
	})

	t.Run("the message first, then the HAVE", func(t *testing.T) {
		gs := testHaveRouter()
		gs.haveNoteArrival(testMid(root, 5))
		var b haveBits
		b.Set(5)
		b.Set(6)
		gs.haveAbsorb(p, encodeHave(root, &b))
		if _, ok := gs.unwanted[p][computeChecksum(testMid(root, 5))]; !ok {
			t.Fatal("index 5 known and announced, should be unwanted")
		}
		// 6 becomes unwanted the moment it arrives.
		gs.haveNoteArrival(testMid(root, 6))
		if _, ok := gs.unwanted[p][computeChecksum(testMid(root, 6))]; !ok {
			t.Fatal("index 6 announced before arrival, should be unwanted on arrival")
		}
	})

	t.Run("an ordinary id is not a HAVE", func(t *testing.T) {
		gs := testHaveRouter()
		if gs.haveAbsorb(p, testMid(root, 1)) {
			t.Fatal("ordinary id absorbed as a HAVE")
		}
	})

	t.Run("the TTL ages both sides out", func(t *testing.T) {
		gs := testHaveRouter()
		gs.haveNoteArrival(testMid(root, 1))
		var b haveBits
		b.Set(1)
		gs.haveAbsorb(p, encodeHave(root, &b))
		for i := 0; i < gs.params.IDontWantMessageTTL; i++ {
			gs.haveHeartbeat()
		}
		if len(gs.have.mine) != 0 || len(gs.have.mids) != 0 || len(gs.have.peers) != 0 {
			t.Fatal("HAVE state survived its TTL")
		}
	})

	t.Run("an announced id counts the HAVE's holder before the message arrives", func(t *testing.T) {
		gs := testHaveRouter()
		var b haveBits
		b.Set(7)
		gs.haveAbsorb(p, encodeHave(root, &b))
		// IHAVE names the id; we still hold nothing of the root.
		gs.haveLearnID(testMid(root, 7))
		if _, ok := gs.unwanted[p][computeChecksum(testMid(root, 7))]; !ok {
			t.Fatal("learning the id must expand the peer's HAVE for it")
		}
		if gs.have.mine[root] != nil {
			t.Fatal("learning an id must not claim we hold it")
		}
		// And the other way round: id learned first, HAVE second.
		gs2 := testHaveRouter()
		gs2.haveLearnID(testMid(root, 8))
		var c haveBits
		c.Set(8)
		gs2.haveAbsorb(p, encodeHave(root, &c))
		if _, ok := gs2.unwanted[p][computeChecksum(testMid(root, 8))]; !ok {
			t.Fatal("a HAVE for a learned id must expand")
		}
	})
}

func TestHaveReplacesIDontWantIDs(t *testing.T) {
	root, other := strings.Repeat("g", 32), strings.Repeat("h", 32)
	gs := testHaveRouter()
	gs.haveNoteArrival(testMid(root, 2))
	gs.haveNoteArrival(testMid(root, 5))
	gs.haveNoteArrival(testMid(other, 0))

	in := []string{"plain-id", testMid(root, 2), testMid(root, 5), testMid(other, 0), "another"}
	out := gs.haveIDontWantIDs(in)
	if len(out) != 4 {
		t.Fatalf("got %d ids, want 4 (plain, HAVE root, HAVE other, another): %q", len(out), out)
	}
	if out[0] != "plain-id" || out[3] != "another" {
		t.Fatalf("ordinary ids moved: %q", out)
	}
	r, enc, ok := decodeHave(out[1])
	if !ok || r != root {
		t.Fatalf("second id is not root's HAVE")
	}
	var b haveBits
	b.Or(enc)
	if !b.Has(2) || !b.Has(5) || b.Has(3) {
		t.Fatalf("root's HAVE carries the wrong bits")
	}
	if r, _, ok := decodeHave(out[2]); !ok || r != other {
		t.Fatalf("third id is not other's HAVE")
	}
	// Nothing structured: the input comes back as is.
	plain := []string{"a", "b"}
	if got := gs.haveIDontWantIDs(plain); len(got) != 2 || &got[0] != &plain[0] {
		t.Fatal("an input with no structured ids must be returned untouched")
	}
}

func TestDedupeHaveKeepsTheNewest(t *testing.T) {
	root := strings.Repeat("g", 32)
	var old, cur haveBits
	old.Set(1)
	cur.Set(1)
	cur.Set(2)
	lists := []*pb.ControlIDontWant{
		{MessageIDs: []string{"x", encodeHave(root, &old)}},
		{MessageIDs: []string{encodeHave(root, &cur), "y"}},
	}
	out := dedupeHave(lists)
	if len(out) != 1 || len(out[0].MessageIDs) != 3 {
		t.Fatalf("got %v", out)
	}
	if out[0].MessageIDs[0] != "x" || out[0].MessageIDs[2] != "y" {
		t.Fatalf("ordinary ids moved: %q", out[0].MessageIDs)
	}
	if _, enc, _ := decodeHave(out[0].MessageIDs[1]); len(enc) != 1 || enc[0] != 0b110 {
		t.Fatalf("kept the wrong HAVE: %v", enc)
	}
	// Inputs untouched, and no duplicate means the same slice back.
	if len(lists[0].MessageIDs) != 2 || len(lists[1].MessageIDs) != 2 {
		t.Fatal("dedupeHave mutated its input")
	}
	single := []*pb.ControlIDontWant{{MessageIDs: []string{"x", encodeHave(root, &cur)}}}
	if got := dedupeHave(single); &got[0] != &single[0] {
		t.Fatal("no repeat, but a new slice came back")
	}
}

// testQueueIndex reads the test id layout off a message's data: the message id is the data.
func testQueueIndex(m *pb.Message) (string, uint32, bool) { return testIndex(string(m.Data)) }

func testDataRPC(root string, index byte) *RPC {
	rpc := &RPC{RPC: pb.RPC{Publish: []*pb.Message{{Data: []byte(testMid(root, index))}}}}
	rpc.size = 100
	return rpc
}

func TestHaveDispatchOnTheQueue(t *testing.T) {
	root := strings.Repeat("g", 32)
	ctx := t.Context()

	t.Run("a frame the peer announced is dropped at dispatch, the rest are written", func(t *testing.T) {
		q := newRpcQueue(16)
		q.enableHave(testQueueIndex, true, true)
		for _, i := range []byte{0, 1, 2} {
			if err := q.Push(testDataRPC(root, i), false); err != nil {
				t.Fatal(err)
			}
		}
		var theirs haveBits
		theirs.Set(1)
		q.haveTheirs(root, theirs.Bytes())
		var got []byte
		for len(got) < 2 {
			rpc, err := q.Pop(ctx)
			if err != nil {
				t.Fatal(err)
			}
			got = append(got, rpc.Publish[0].Data[32])
		}
		if got[0] != 0 || got[1] != 2 {
			t.Fatalf("wrote indices %v, want [0 2]", got)
		}
		if q.outboundStat().Cancelled != 1 {
			t.Fatalf("cancelled = %d, want 1", q.outboundStat().Cancelled)
		}
		if n, _ := q.Occupancy(); n != 0 {
			t.Fatalf("%d left in the queue", n)
		}
	})

	t.Run("a pending HAVE rides the next RPC out, once", func(t *testing.T) {
		q := newRpcQueue(16)
		q.enableHave(testQueueIndex, true, true)
		var mine haveBits
		mine.Set(3)
		if q.havePend([]string{root}, []*haveBits{&mine}) {
			t.Fatal("nothing queued, so nothing to ride: havePend must refuse")
		}
		if err := q.Push(testDataRPC(root, 0), false); err != nil {
			t.Fatal(err)
		}
		if err := q.Push(testDataRPC(root, 1), false); err != nil {
			t.Fatal(err)
		}
		if !q.havePend([]string{root}, []*haveBits{&mine}) {
			t.Fatal("something is queued: havePend must accept")
		}
		first, err := q.Pop(ctx)
		if err != nil {
			t.Fatal(err)
		}
		ids := first.Control.GetIdontwant()
		if len(ids) != 1 || len(ids[0].MessageIDs) != 1 {
			t.Fatalf("first RPC out carries %v, want one HAVE", ids)
		}
		if r, enc, ok := decodeHave(ids[0].MessageIDs[0]); !ok || r != root || len(enc) != 1 || enc[0] != 1<<3 {
			t.Fatalf("attached the wrong HAVE: %v", ids[0].MessageIDs)
		}
		if len(first.Publish) != 1 {
			t.Fatal("the data frame must still be there")
		}
		second, err := q.Pop(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if second.Control != nil {
			t.Fatal("the HAVE rode once; the second RPC must be clean")
		}
		if q.outboundStat().Attached != 1 {
			t.Fatalf("attached = %d, want 1", q.outboundStat().Attached)
		}
	})

	t.Run("cancel off leaves announced frames alone", func(t *testing.T) {
		q := newRpcQueue(16)
		q.enableHave(testQueueIndex, false, true)
		if err := q.Push(testDataRPC(root, 1), false); err != nil {
			t.Fatal(err)
		}
		var theirs haveBits
		theirs.Set(1)
		q.haveTheirs(root, theirs.Bytes())
		if _, err := q.Pop(ctx); err != nil {
			t.Fatal(err)
		}
		if q.outboundStat().Cancelled != 0 {
			t.Fatal("cancelled with cancel off")
		}
	})
}

func TestHaveKnowsOwnMessages(t *testing.T) {
	// A publisher never sees its own messages in Preprocess; rpcs() must record them, or a
	// peer's HAVE for them expands into nothing and phase forwarding sees no holders.
	root := strings.Repeat("g", 32)
	gs := testHaveRouter()
	gs.haveNoteArrival(testMid(root, 4)) // stands in for the rpcs() hook on an own message
	var b haveBits
	b.Set(4)
	gs.haveAbsorb(peer.ID("peer"), encodeHave(root, &b))
	if _, ok := gs.unwanted[peer.ID("peer")][computeChecksum(testMid(root, 4))]; !ok {
		t.Fatal("a HAVE for an own message must mark the peer as holding it")
	}
}

func TestWantEncoding(t *testing.T) {
	root := strings.Repeat("g", 32)
	mid := encodeWant(root, 0x01020304)
	if len(mid) != wantLen {
		t.Fatalf("len %d, want %d", len(mid), wantLen)
	}
	r, i, ok := decodeWant(mid)
	if !ok || r != root || i != 0x01020304 {
		t.Fatalf("round trip: ok=%v root=%q index=%#x", ok, r, i)
	}
	if _, _, ok := decodeWant(encodeHave(root, &haveBits{})); ok {
		t.Fatal("a HAVE decoded as a WANT")
	}
	if _, _, ok := decodeWant(testMid(root, 1)); ok {
		t.Fatal("an ordinary id decoded as a WANT")
	}
}

func TestIHaveBitmapSide(t *testing.T) {
	root := strings.Repeat("g", 32)
	p := peer.ID("peer")
	on := func() *GossipSubRouter {
		gs := testHaveRouter()
		gs.have.ihaveOn = true
		return gs
	}

	t.Run("the bitmap encodes exactly the announced ids, and only when the side is on", func(t *testing.T) {
		gs := on()
		gs.haveNoteServable(testMid(root, 1))
		gs.haveNoteServable(testMid(root, 2))
		gs.haveNoteServable(testMid(root, 3))
		// A phase announce names one message: one bit, whatever else we hold.
		out := gs.haveIHaveIDs(p, []string{testMid(root, 2), "plain"})
		if len(out) != 2 {
			t.Fatalf("got %q", out)
		}
		// Ordinary ids keep their place; the bitmaps follow them.
		have := out[1]
		if out[0] != "plain" {
			t.Fatalf("plain id moved: %q", out)
		}
		_, enc, ok := decodeHave(have)
		var b haveBits
		b.Or(enc)
		if !ok || b.Has(1) || !b.Has(2) || b.Has(3) {
			t.Fatalf("announce must carry index 2 alone: ok=%v bits=%v", ok, enc)
		}
		// A gossip list names several: one bitmap per root with all of them.
		out = gs.haveIHaveIDs(p, []string{testMid(root, 1), testMid(root, 3)})
		_, enc, _ = decodeHave(out[0])
		var c haveBits
		c.Or(enc)
		if len(out) != 1 || !c.Has(1) || !c.Has(3) || c.Has(2) {
			t.Fatalf("gossip bitmap wrong: %q %v", out, enc)
		}
		off := testHaveRouter()
		off.haveNoteServable(testMid(root, 2))
		in := []string{testMid(root, 2)}
		if got := off.haveIHaveIDs(p, in); &got[0] != &in[0] {
			t.Fatal("IHAVE side off must return the input untouched")
		}
	})

	t.Run("the announce to a peer leaves out what we pushed to it and what it holds", func(t *testing.T) {
		gs := on()
		for _, i := range []byte{1, 2, 3, 4} {
			gs.haveNoteServable(testMid(root, i))
		}
		gs.haveNotePushed(p, testMid(root, 1))
		var theirs haveBits
		theirs.Set(2)
		gs.haveAbsorb(p, encodeHave(root, &theirs))
		all := []string{testMid(root, 1), testMid(root, 2), testMid(root, 3), testMid(root, 4)}
		out := gs.haveIHaveIDs(p, all)
		if len(out) != 1 {
			t.Fatalf("got %q", out)
		}
		_, enc, _ := decodeHave(out[0])
		var b haveBits
		b.Or(enc)
		if b.Has(1) || b.Has(2) || !b.Has(3) || !b.Has(4) {
			t.Fatalf("announce bitmap wrong: %v", enc)
		}
		// Nothing left to announce: no id at all rather than an empty bitmap.
		if out := gs.haveIHaveIDs(p, []string{testMid(root, 1), testMid(root, 2)}); len(out) != 0 {
			t.Fatalf("expected no announce, got %q", out)
		}
	})

	t.Run("an announced bitmap expands to known ids, asks by index, and skips what we hold", func(t *testing.T) {
		gs := on()
		gs.haveNoteArrival(testMid(root, 0)) // held: skipped
		gs.haveLearnID(testMid(root, 1))     // known, not held: real id
		var theirs haveBits
		theirs.Set(0)
		theirs.Set(1)
		theirs.Set(5) // unknown: ask by index
		out := gs.haveExpandIHave(root, theirs.Bytes())
		if len(out) != 2 || out[0] != testMid(root, 1) {
			t.Fatalf("got %q", out)
		}
		if r, i, ok := decodeWant(out[1]); !ok || r != root || i != 5 {
			t.Fatalf("second id is not an ask for index 5: %q", out[1])
		}
	})

	t.Run("an ask by index resolves to the held id, or is skipped", func(t *testing.T) {
		gs := on()
		gs.haveNoteServable(testMid(root, 3))
		if real, skip := gs.haveResolveWant(encodeWant(root, 3)); skip || real != testMid(root, 3) {
			t.Fatalf("resolve: real=%q skip=%v", real, skip)
		}
		if _, skip := gs.haveResolveWant(encodeWant(root, 4)); !skip {
			t.Fatal("an index we do not hold must be skipped")
		}
		if real, skip := gs.haveResolveWant("plain"); skip || real != "plain" {
			t.Fatal("an ordinary id must pass through")
		}
	})

	t.Run("an index we hold is seen under its ask id", func(t *testing.T) {
		gs := on()
		gs.p = &PubSub{seenMessages: timecache.NewTimeCache(time.Minute)}
		if gs.haveSeen(encodeWant(root, 6)) {
			t.Fatal("not held yet")
		}
		gs.haveNoteArrival(testMid(root, 6))
		if !gs.haveSeen(encodeWant(root, 6)) {
			t.Fatal("held, so the ask id must read as seen")
		}
	})

	t.Run("pending gossip keeps only the newest bitmap per root", func(t *testing.T) {
		gs := on()
		gs.gossip = make(map[peer.ID][]*pb.ControlIHave)
		var old, cur haveBits
		old.Set(1)
		cur.Set(1)
		cur.Set(2)
		topic := "t"
		gs.enqueueGossip(p, &pb.ControlIHave{TopicID: &topic, MessageIDs: []string{"x", encodeHave(root, &old)}})
		gs.enqueueGossip(p, &pb.ControlIHave{TopicID: &topic, MessageIDs: []string{encodeHave(root, &cur)}})
		pending := gs.gossip[p]
		if len(pending) != 2 || len(pending[0].MessageIDs) != 1 || pending[0].MessageIDs[0] != "x" {
			t.Fatalf("older bitmap not removed: %v", pending[0].MessageIDs)
		}
	})
}

func TestHaveLoadByte(t *testing.T) {
	for _, tc := range []struct {
		queued int64
		bucket byte
	}{{0, 0}, {512, 1}, {1 << 10, 2}, {16 << 10, 6}, {1 << 20, 12}, {1 << 40, 32}} {
		if got := loadBucket(tc.queued); got != tc.bucket {
			t.Fatalf("loadBucket(%d) = %d, want %d", tc.queued, got, tc.bucket)
		}
	}
	if loadBytes(0) != 0 || loadBytes(1) != 0 || loadBytes(2) != 1<<10 || loadBytes(12) != 1<<20 {
		t.Fatalf("loadBytes inverse wrong: %d %d %d %d", loadBytes(0), loadBytes(1), loadBytes(2), loadBytes(12))
	}
	root := strings.Repeat("g", 32)
	var b haveBits
	b.Set(3)
	mid := encodeHaveLoad(root, 12, &b)
	r, load, enc, ok := decodeHaveLoad(mid)
	if !ok || r != root || load != 12 || len(enc) != 1 || enc[0] != 1<<3 {
		t.Fatalf("v2 round trip: ok=%v root=%q load=%d enc=%v", ok, r, load, enc)
	}
	// A v1 id reads with load 0 through the same decoder, and decodeHave accepts both.
	if _, load, _, ok := decodeHaveLoad(encodeHave(root, &b)); !ok || load != 0 {
		t.Fatal("v1 must decode with load 0")
	}
	if _, _, ok := decodeHave(mid); !ok {
		t.Fatal("decodeHave must accept v2")
	}

	gs := testHaveRouter()
	gs.have.askRate = 50_000_000
	gs.have.askCap = 500 * time.Millisecond
	if d := gs.haveAskDelay(0); d != 0 {
		t.Fatalf("no load, no delay: %v", d)
	}
	// 1 MiB at 50 Mbps is 167.8 ms.
	if d := gs.haveAskDelay(12); d < 160*time.Millisecond || d > 175*time.Millisecond {
		t.Fatalf("1 MiB at 50 Mbps = %v, want ~168 ms", d)
	}
	if d := gs.haveAskDelay(40); d != 500*time.Millisecond {
		t.Fatalf("cap not applied: %v", d)
	}
	gs.have.askRate = 0
	if d := gs.haveAskDelay(12); d != 0 {
		t.Fatalf("delay off must be zero: %v", d)
	}
}

func TestRarestDispatchOnTheQueue(t *testing.T) {
	root := strings.Repeat("g", 32)
	ctx := t.Context()
	counts := &holderCounts{}
	counts.add(root, []uint32{0, 0, 0, 0, 0}) // index 0: five holders
	counts.add(root, []uint32{1})             // index 1: one holder
	counts.add(root, []uint32{2, 2, 2})       // index 2: three holders
	holders := func(m *pb.Message) (int, bool) {
		r, i, ok := testQueueIndex(m)
		if !ok {
			return 0, false
		}
		return counts.get(r, i), true
	}
	newQ := func() *rpcQueue {
		q := newRpcQueue(16)
		q.enableHave(testQueueIndex, false, false)
		q.enableRarest(holders)
		return q
	}
	pop := func(t *testing.T, q *rpcQueue) *RPC {
		rpc, err := q.Pop(ctx)
		if err != nil {
			t.Fatal(err)
		}
		return rpc
	}

	t.Run("the rarest queued frame goes first, FIFO within a tie", func(t *testing.T) {
		q := newQ()
		for _, i := range []byte{0, 1, 2} {
			if err := q.Push(testDataRPC(root, i), false); err != nil {
				t.Fatal(err)
			}
		}
		var got []byte
		for range 3 {
			got = append(got, pop(t, q).Publish[0].Data[32])
		}
		if got[0] != 1 || got[1] != 2 || got[2] != 0 {
			t.Fatalf("popped %v, want [1 2 0]", got)
		}
		if q.outboundStat().Reordered != 2 {
			t.Fatalf("reordered = %d, want 2", q.outboundStat().Reordered)
		}
	})

	t.Run("a control RPC keeps its place: nothing behind it overtakes it", func(t *testing.T) {
		q := newQ()
		if err := q.Push(testDataRPC(root, 0), false); err != nil {
			t.Fatal(err)
		}
		ctl := &RPC{RPC: pb.RPC{Control: &pb.ControlMessage{Ihave: []*pb.ControlIHave{{}}}}}
		ctl.size = 10
		if err := q.Push(ctl, false); err != nil {
			t.Fatal(err)
		}
		if err := q.Push(testDataRPC(root, 1), false); err != nil {
			t.Fatal(err)
		}
		// Index 1 is rarer than 0, but it sits behind the control RPC, so 0 goes first, then
		// the control, then 1.
		if first := pop(t, q); len(first.Publish) != 1 || first.Publish[0].Data[32] != 0 {
			t.Fatalf("first pop was not index 0")
		}
		if second := pop(t, q); second.Control == nil || len(second.Publish) != 0 {
			t.Fatal("second pop was not the control RPC")
		}
		if third := pop(t, q); third.Publish[0].Data[32] != 1 {
			t.Fatal("third pop was not index 1")
		}
		if q.outboundStat().Reordered != 0 {
			t.Fatal("nothing should have been reordered")
		}
	})

	t.Run("the urgent lane is untouched", func(t *testing.T) {
		q := newQ()
		if err := q.Push(testDataRPC(root, 1), false); err != nil {
			t.Fatal(err)
		}
		if err := q.UrgentPush(testDataRPC(root, 0), false); err != nil {
			t.Fatal(err)
		}
		if first := pop(t, q); first.Publish[0].Data[32] != 0 {
			t.Fatal("urgent must pop first whatever its rarity")
		}
	})
}

// The dispatch ledger sees every frame popped to a peer and every HAVE the peer sends, so a
// duplicate can later be classed by whether its sender could have cancelled it: a frame the
// peer announced before dispatch is dropped and never popped; one announced after its pop
// carries both instants with the HAVE later; one never announced carries the pop alone.
func TestHaveLedgerOnTheQueue(t *testing.T) {
	root := strings.Repeat("g", 32)
	ctx := t.Context()
	p := peer.ID("peer")
	led := &haveLedger{entries: make(map[peer.ID]map[string]map[uint32]*HaveLedgerEntry)}
	q := newRpcQueue(16)
	q.enableHave(testQueueIndex, true, true)
	q.enableDispatchHook(func(m *pb.Message) {
		if r, i, ok := testQueueIndex(m); ok {
			led.pop(p, r, i, sha256.Sum256(m.Data), time.Now())
		}
	})
	for _, i := range []byte{0, 1, 2} {
		if err := q.Push(testDataRPC(root, i), false); err != nil {
			t.Fatal(err)
		}
	}
	var theirs haveBits
	theirs.Set(1)
	q.haveTheirs(root, theirs.Bytes())
	led.have(p, root, []uint32{1}, time.Now())
	for range 2 {
		if _, err := q.Pop(ctx); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(time.Millisecond)
	led.have(p, root, []uint32{0}, time.Now())

	byIndex := map[uint32]HaveLedgerEntry{}
	for _, e := range led.snapshot() {
		if e.Peer != p || e.Root != root {
			t.Fatalf("entry for %s/%q, want %s/%q", e.Peer, e.Root, p, root)
		}
		byIndex[e.Index] = e
	}
	if len(byIndex) != 3 {
		t.Fatalf("%d entries, want 3", len(byIndex))
	}
	e0, e1, e2 := byIndex[0], byIndex[1], byIndex[2]
	if e0.PopAt.IsZero() || e0.HaveAt.IsZero() || !e0.HaveAt.After(e0.PopAt) {
		t.Fatalf("frame 0 should be popped then announced: %+v", e0)
	}
	if e0.Digest != sha256.Sum256(testDataRPC(root, 0).Publish[0].Data) {
		t.Fatal("frame 0's digest is not its data's")
	}
	if !e1.PopAt.IsZero() || e1.HaveAt.IsZero() {
		t.Fatalf("frame 1 was announced before dispatch and must never pop: %+v", e1)
	}
	if e2.PopAt.IsZero() || !e2.HaveAt.IsZero() {
		t.Fatalf("frame 2 was popped and never announced: %+v", e2)
	}
}

// TestHaveAllCancelsEveryQueuedFrame is D6 step 2's unit known answer: a full bitmap of a
// coded group's frame count sets exactly those indices, flips every one of them on a peer
// that had none, and once the peer has said it, every frame queued to it cancels at pop.
func TestHaveAllCancelsEveryQueuedFrame(t *testing.T) {
	root := strings.Repeat("g", 32)
	full := fullHaveBits(128)
	if !full.Has(0) || !full.Has(127) || full.Has(128) {
		t.Fatal("a full bitmap of 128 must set exactly 0..127")
	}
	if got := len(full.Bytes()); got != 16 {
		t.Fatalf("128 indices are 16 bytes, got %d", got)
	}
	var theirs haveBits
	if newly := theirs.Or(full.Bytes()); len(newly) != 128 {
		t.Fatalf("a full bitmap flips 128 indices on an empty peer, got %d", len(newly))
	}
	q := newRpcQueue(16)
	q.enableHave(testQueueIndex, true, false)
	for _, i := range []byte{0, 1, 2} {
		if err := q.Push(testDataRPC(root, i), false); err != nil {
			t.Fatal(err)
		}
	}
	q.haveTheirs(root, full.Bytes())
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	if rpc, err := q.Pop(ctx); err == nil {
		t.Fatalf("every queued frame is held by the peer and must cancel, popped %v", rpc)
	}
}

// TestGroupPushProjection is D6 step 4's unit known answer: with need 2, a peer that evidenced
// one frame of a group and was pushed another is complete under the projection and not without.
func TestGroupPushProjection(t *testing.T) {
	extract := func(mid string) (string, bool) {
		if len(mid) < 2 {
			return "", false
		}
		return mid[:1], true
	}
	p := peer.ID("peer")
	g := &groupPush{extract: extract, need: 2, held: map[peer.ID]map[string]map[string]struct{}{}}
	g.note(p, "g0")
	g.notePushed(p, "g1")
	if g.complete(p, "g2") {
		t.Fatal("without the projection one evidenced frame does not complete need 2")
	}
	g.project = true
	if !g.complete(p, "g2") {
		t.Fatal("evidenced g0 plus pushed g1 reach need 2 under the projection")
	}
	g.notePushed(p, "g0") // the same frame both ways counts once
	g.need = 3
	if g.complete(p, "g2") {
		t.Fatal("g0 evidenced and pushed is one frame, not two")
	}
	if g.complete(p, "x") {
		t.Fatal("an id without a group is never vetoed")
	}
}

// TestDispatchProjectionDropsAtPop is D6 step 4b's unit known answer: need 4, fan-in 2, the peer
// holds {0, 1}; frame 2 pops (2 + 2·0 < 4), then frames 3 and 4 drop (2 + 2·1 ≥ 4), projected = 2.
func TestDispatchProjectionDropsAtPop(t *testing.T) {
	root := strings.Repeat("g", 32)
	q := newRpcQueue(16)
	q.enableHave(testQueueIndex, true, false)
	q.enableProjection(4, 2)
	var theirs haveBits
	theirs.Set(0)
	theirs.Set(1)
	q.haveTheirs(root, theirs.Bytes())
	for _, i := range []byte{2, 3, 4} {
		if err := q.Push(testDataRPC(root, i), false); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	rpc, err := q.Pop(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, i, _ := testQueueIndex(rpc.Publish[0]); i != 2 {
		t.Fatalf("first pop is frame 2, got %d", i)
	}
	if rpc, err := q.Pop(ctx); err == nil {
		t.Fatalf("frames 3 and 4 are projected complete and must drop, popped %v", rpc)
	}
	if got := q.projected.Load(); got != 2 {
		t.Fatalf("projected drops = %d, want 2", got)
	}
	if got := q.cancelled.Load(); got != 0 {
		t.Fatalf("nothing was held by the peer, cancelled = %d", got)
	}
}

// TestPairOrderAtPop is D7 idea 6's unit known answer: with equal holder counts the tie form
// pops the frame with the smallest pair hash, not the head; the first form does so even when a
// later frame has more holders; and the same frames toward a different peer pop in another order.
func TestPairOrderAtPop(t *testing.T) {
	root := strings.Repeat("g", 32)
	local, remote, other := peer.ID("local"), peer.ID("remote"), peer.ID("other")
	frames := []byte{0, 1, 2, 3}
	order := func(first bool, to peer.ID, holders func(uint32) int) []uint32 {
		q := newRpcQueue(16)
		q.enableHave(testQueueIndex, true, false)
		q.enableRarest(func(m *pb.Message) (int, bool) {
			_, i, ok := testQueueIndex(m)
			if !ok {
				return 0, false
			}
			return holders(i), true
		})
		key := func(m *pb.Message) (uint64, bool) {
			r, i, ok := testQueueIndex(m)
			if !ok {
				return 0, false
			}
			return pairRank(r, i, local, to), true
		}
		q.enablePairOrder(key, first)
		for _, i := range frames {
			if err := q.Push(testDataRPC(root, i), false); err != nil {
				t.Fatal(err)
			}
		}
		ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
		defer cancel()
		var got []uint32
		for range frames {
			rpc, err := q.Pop(ctx)
			if err != nil {
				t.Fatal(err)
			}
			_, i, _ := testQueueIndex(rpc.Publish[0])
			got = append(got, i)
		}
		return got
	}
	flat := func(uint32) int { return 0 }
	byKey := func(to peer.ID) []uint32 {
		idx := []uint32{0, 1, 2, 3}
		sort.Slice(idx, func(a, b int) bool { return pairRank(root, idx[a], local, to) < pairRank(root, idx[b], local, to) })
		return idx
	}
	if got, want := order(false, remote, flat), byKey(remote); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("tie form, equal holders: popped %v, want the pair-hash order %v", got, want)
	}
	if got, want := order(false, other, flat), byKey(other); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("tie form toward another peer: popped %v, want %v", got, want)
	}
	if fmt.Sprint(byKey(remote)) == fmt.Sprint(byKey(other)) {
		t.Fatal("two peers must not share an order (a hash collision on 4 frames is astronomically unlikely)")
	}
	// Tie form with holders: frame 3 is rarest (0 holders) and pops first whatever its hash;
	// the rest (1 holder each) follow the hash order.
	rare3 := func(i uint32) int {
		if i == 3 {
			return 0
		}
		return 1
	}
	got := order(false, remote, rare3)
	if got[0] != 3 {
		t.Fatalf("tie form: the rarest frame pops first, got %v", got)
	}
	// First form: the hash leads even against a rarer frame.
	if got, want := order(true, remote, rare3), byKey(remote); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("first form: popped %v, want the pair-hash order %v regardless of holders", got, want)
	}
}

// TestPopsBeforeRules pins the comparator: no key is D3's strict-less with FIFO ties.
func TestPopsBeforeRules(t *testing.T) {
	h := &queueHave{}
	if h.popsBefore(1, 0, false, 1, 0) {
		t.Fatal("no key: an equal holder count keeps FIFO")
	}
	if !h.popsBefore(0, 0, false, 1, 0) {
		t.Fatal("no key: fewer holders pops first")
	}
	if !h.popsBefore(1, 5, true, 1, 9) || h.popsBefore(1, 9, true, 1, 5) {
		t.Fatal("tie form: within equal holders the smaller key pops first")
	}
	if h.popsBefore(2, 1, true, 1, 9) {
		t.Fatal("tie form: more holders never pops first")
	}
	h.pairFirst = true
	if !h.popsBefore(2, 1, true, 1, 9) || h.popsBefore(1, 9, true, 2, 1) {
		t.Fatal("first form: the smaller key pops first regardless of holders")
	}
	if !h.popsBefore(0, 4, true, 1, 4) {
		t.Fatal("first form: equal keys fall back to fewer holders")
	}
}

// TestSolicitedFramesAtDispatch is D7 idea 2's second unit known answer. A solicited frame the
// peer has since announced is stale and cancels like any other; a solicited frame the peer asked
// for after a full claim is written once the ask revoked its claimed bit; an ask never revokes what
// the peer said it holds; and a solicited frame is never projected away.
func TestSolicitedFramesAtDispatch(t *testing.T) {
	root := strings.Repeat("g", 32)
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()

	t.Run("stale answer cancels", func(t *testing.T) {
		q := newRpcQueue(16)
		q.enableHave(testQueueIndex, true, false)
		q.haveTheirs(root, fullHaveBits(128).Bytes())
		answer := testDataRPC(root, 6)
		answer.solicited = true
		if err := q.Push(answer, false); err != nil {
			t.Fatal(err)
		}
		if rpc, err := q.Pop(ctx); err == nil {
			t.Fatalf("the peer announced frame 6, the answer is stale and must cancel, popped %v", rpc)
		}
	})
	t.Run("an ask revokes the claimed bit", func(t *testing.T) {
		q := newRpcQueue(16)
		q.enableHave(testQueueIndex, true, false)
		q.haveClaim(root, fullHaveBits(128).Bytes())
		q.haveRevoke(root, 6)
		pushed := testDataRPC(root, 5)
		if err := q.Push(pushed, false); err != nil {
			t.Fatal(err)
		}
		answer := testDataRPC(root, 6)
		answer.solicited = true
		if err := q.Push(answer, false); err != nil {
			t.Fatal(err)
		}
		rpc, err := q.Pop(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, i, _ := testQueueIndex(rpc.Publish[0]); i != 6 {
			t.Fatalf("the pushed frame 5 cancels and the asked-for frame 6 is written, got %d", i)
		}
		if got := q.cancelled.Load(); got != 1 {
			t.Fatalf("cancelled = %d, want 1", got)
		}
	})
	t.Run("an ask never revokes a HAVE", func(t *testing.T) {
		q := newRpcQueue(16)
		q.enableHave(testQueueIndex, true, false)
		q.haveTheirs(root, fullHaveBits(128).Bytes())
		q.haveRevoke(root, 6)
		answer := testDataRPC(root, 6)
		answer.solicited = true
		if err := q.Push(answer, false); err != nil {
			t.Fatal(err)
		}
		if rpc, err := q.Pop(ctx); err == nil {
			t.Fatalf("the peer holds frame 6 by its HAVE, which no ask revokes, so the answer cancels; popped %v", rpc)
		}
	})
	t.Run("never projected away", func(t *testing.T) {
		q := newRpcQueue(16)
		q.enableHave(testQueueIndex, true, false)
		q.enableProjection(4, 2)
		var theirs haveBits
		theirs.Set(0)
		theirs.Set(1)
		q.haveTheirs(root, theirs.Bytes())
		answer := testDataRPC(root, 2)
		answer.solicited = true
		if err := q.Push(answer, false); err != nil {
			t.Fatal(err)
		}
		if err := q.Push(testDataRPC(root, 3), false); err != nil {
			t.Fatal(err)
		}
		rpc, err := q.Pop(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, i, _ := testQueueIndex(rpc.Publish[0]); i != 2 {
			t.Fatalf("the solicited frame 2 is written, got %d", i)
		}
		if rpc, err := q.Pop(ctx); err == nil {
			t.Fatalf("frame 3 is projected complete (2 held + 2·1 popped ≥ 4) and must drop, popped %v", rpc)
		}
		if got := q.projected.Load(); got != 1 {
			t.Fatalf("projected = %d, want 1", got)
		}
	})
	t.Run("written when the projection already says complete, with no claim", func(t *testing.T) {
		q := newRpcQueue(16)
		q.enableHave(testQueueIndex, true, false)
		q.enableProjection(4, 2)
		var theirs haveBits
		theirs.Set(0)
		theirs.Set(1)
		q.haveTheirs(root, theirs.Bytes())
		if err := q.Push(testDataRPC(root, 3), false); err != nil {
			t.Fatal(err)
		}
		if _, err := q.Pop(ctx); err != nil { // 2 held + 2·1 popped = 4: projected complete from here
			t.Fatal(err)
		}
		answer := testDataRPC(root, 5)
		answer.solicited = true
		if err := q.Push(answer, false); err != nil {
			t.Fatal(err)
		}
		rpc, err := q.Pop(ctx)
		if err != nil {
			t.Fatalf("the peer asked for frame 5; the projection must not drop the answer: %v", err)
		}
		if _, i, _ := testQueueIndex(rpc.Publish[0]); i != 5 {
			t.Fatalf("the solicited frame 5 is written, got %d", i)
		}
	})
}

// TestNodeOrderIsPeerIndependent pins the per-node modes: the key ignores the remote peer.
func TestNodeOrderIsPeerIndependent(t *testing.T) {
	h := &haveState{pairOrder: haveNodeTie, msgIndex: testQueueIndex}
	root := strings.Repeat("g", 32)
	m := testDataRPC(root, 3).Publish[0]
	a, _ := h.havePairKey("local", "peerA")(m)
	b, _ := h.havePairKey("local", "peerB")(m)
	if a != b {
		t.Fatal("per-node order must give the same key toward every peer")
	}
	h.pairOrder = havePairTie
	a, _ = h.havePairKey("local", "peerA")(m)
	b, _ = h.havePairKey("local", "peerB")(m)
	if a == b {
		t.Fatal("per-pair order must differ between peers")
	}
}

// TestClaimAtDispatch is D7 idea 2's third unit known answer, the claim kind: a claim cancels
// queued pushes like a HAVE, is not counted by the projection, and a solicited answer to a
// claiming peer is written after the ask revoked its bit.
func TestClaimAtDispatch(t *testing.T) {
	root := strings.Repeat("g", 32)
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	claim := encodeHaveClaim(root, fullHaveBits(128))
	if !isHaveClaim(claim) || isHaveClaim(encodeHave(root, fullHaveBits(128))) {
		t.Fatal("isHaveClaim must tell the two kinds apart")
	}
	if r, enc, ok := decodeHave(claim); !ok || r != root || len(enc) != 16 {
		t.Fatal("a claim decodes as a HAVE bitmap of the same root")
	}
	q := newRpcQueue(16)
	q.enableHave(testQueueIndex, true, false)
	q.enableProjection(64, 2)
	_, enc, _ := decodeHave(claim)
	q.haveClaim(root, enc)
	if err := q.Push(testDataRPC(root, 5), false); err != nil {
		t.Fatal(err)
	}
	if rpc, err := q.Pop(ctx); err == nil {
		t.Fatalf("a claimed frame cancels at pop, popped %v", rpc)
	}
	// The peer asks for 6 by index: the bit is revoked, the answer is neither cancelled nor
	// projected away even though the claim covers 127 indices.
	q.haveRevoke(root, 6)
	answer := testDataRPC(root, 6)
	answer.solicited = true
	if err := q.Push(answer, false); err != nil {
		t.Fatal(err)
	}
	rpc, err := q.Pop(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, i, _ := testQueueIndex(rpc.Publish[0]); i != 6 {
		t.Fatalf("the asked-for frame 6 is written, got %d", i)
	}
	if q.projected.Load() != 0 {
		t.Fatalf("a claim must not feed the projection, projected = %d", q.projected.Load())
	}
	// The router side: a claim absorbed from a peer adds no holder.
	gs := testHaveRouter()
	gs.have.holders = &holderCounts{}
	p := peer.ID("peer")
	if !gs.haveAbsorb(p, claim) {
		t.Fatal("claim not absorbed")
	}
	if gs.have.holders.get(root, 3) != 0 {
		t.Fatal("a claim counted as a holder")
	}
	if e := gs.have.peers[p][root]; !e.claim.Has(3) || e.bits.Has(3) {
		t.Fatal("the claim lands in the entry's claim bits, not its HAVE bits")
	}
	if !gs.haveClaimedPeer(p, testMid(root, 3)) {
		t.Fatal("haveClaimedPeer must see the claim")
	}
	var real haveBits
	real.Set(1)
	gs.haveAbsorb(p, encodeHave(root, &real))
	if gs.have.holders.get(root, 1) != 1 {
		t.Fatal("a real HAVE after the claim still counts a holder")
	}
}

// TestPhaseSelectFewest is sel=fewest's known answer: with a budget of two, the two mesh peers
// known to hold the fewest frames of the group are pushed -- a HAVE and our own pushes count as
// one union -- and the rest are announced; outside a group the rank order is kept.
func TestPhaseSelectFewest(t *testing.T) {
	gs := testHaveRouter()
	gs.have.ihaveOn = true
	pf := &phaseForwarding{degree: 2, fixedBudget: true, selFewest: true}
	root := strings.Repeat("g", 32)
	mid := testMid(root, 9)
	have := func(p peer.ID, idx ...uint32) {
		if gs.have.peers[p] == nil {
			gs.have.peers[p] = make(map[string]*haveEntry)
		}
		e := &haveEntry{}
		for _, i := range idx {
			e.bits.Set(i)
		}
		gs.have.peers[p][root] = e
	}
	have("a", 0, 1, 2, 3)
	have("b", 0, 1)
	have("c", 0)
	gs.haveNotePushed("c", testMid(root, 0)) // already in c's HAVE: the union stays one
	gs.haveNotePushed("d", testMid(root, 1))
	gs.haveNotePushed("d", testMid(root, 2))
	gs.haveNotePushed("d", testMid(root, 3))
	mesh := map[peer.ID]struct{}{"a": {}, "b": {}, "c": {}, "d": {}, "e": {}}
	tosend := map[peer.ID]struct{}{"a": {}, "b": {}, "c": {}, "d": {}, "e": {}}
	if got := gs.have.knownFrames("c", root); got != 1 {
		t.Fatalf("c knows %d frames, want 1 (HAVE and push are one union)", got)
	}
	announce := pf.trim(gs, mid, computeChecksum(mid), mesh, tosend, false)
	if _, ok := tosend["e"]; !ok || len(tosend) != 2 {
		t.Fatalf("pushed %v, want e (0 frames) and c (1)", tosend)
	}
	if _, ok := tosend["c"]; !ok {
		t.Fatalf("pushed %v, want e and c", tosend)
	}
	if len(announce) != 3 {
		t.Fatalf("announced %v, want a, b, d", announce)
	}
	t.Run("a backlogged peer ranks after the rest", func(t *testing.T) {
		gs.p = &PubSub{peers: map[peer.ID]*rpcQueue{
			"a": {}, "b": {}, "c": {}, "d": {}, "e": {queuedBytes: 1 << 20},
		}}
		defer func() { gs.p = nil }()
		guarded := &phaseForwarding{degree: 2, fixedBudget: true, selFewest: true, selBacklog: 32 << 10}
		tosend := map[peer.ID]struct{}{"a": {}, "b": {}, "c": {}, "d": {}, "e": {}}
		guarded.trim(gs, mid, computeChecksum(mid), mesh, tosend, false)
		_, c := tosend["c"]
		_, b := tosend["b"]
		if len(tosend) != 2 || !c || !b {
			t.Fatalf("pushed %v, want c (1 frame) and b (2): e holds 0 but has 1 MiB queued", tosend)
		}
	})
	t.Run("the proof guard", func(t *testing.T) {
		gs := testHaveRouter()
		gs.have.ihaveOn = true
		var demoted []peer.ID
		guarded := &phaseForwarding{degree: 2, fixedBudget: true, selFewest: true, selGrace: 200 * time.Millisecond,
			selDemoted: func(p peer.ID, r string) {
				if r != root {
					t.Fatalf("demoted root %q, want %q", r, root)
				}
				demoted = append(demoted, p)
			}}
		push := func(p peer.ID, ago time.Duration, idx ...byte) {
			for _, i := range idx {
				gs.haveNotePushed(p, testMid(root, i))
			}
			gs.have.pushed[p][root].first = time.Now().Add(-ago)
		}
		hv := func(p peer.ID, idx ...byte) {
			var b haveBits
			for _, i := range idx {
				b.Set(uint32(i))
			}
			if !gs.haveAbsorb(p, encodeHave(root, &b)) {
				t.Fatal("HAVE not absorbed")
			}
		}
		// m is mute: pushed two frames 300 ms ago, no HAVE; it holds the fewest, so ranks first
		// without the guard. h was pushed as long ago and sent a HAVE; x and y only sent HAVEs.
		push("m", 300*time.Millisecond, 0, 1)
		push("h", 300*time.Millisecond, 0, 1)
		hv("h", 2)
		hv("x", 0, 1, 2)
		hv("y", 0, 1, 2, 3)
		mesh := map[peer.ID]struct{}{"m": {}, "h": {}, "x": {}, "y": {}}
		t.Run("a peer silent past grace ranks after the rest", func(t *testing.T) {
			tosend := map[peer.ID]struct{}{"m": {}, "h": {}, "x": {}, "y": {}}
			guarded.trim(gs, mid, computeChecksum(mid), mesh, tosend, false)
			_, h := tosend["h"]
			_, x := tosend["x"]
			if len(tosend) != 2 || !h || !x {
				t.Fatalf("pushed %v, want h and x (3 frames each): m holds 2 but is unproven past grace", tosend)
			}
			if len(demoted) != 1 || demoted[0] != "m" {
				t.Fatalf("demoted %v, want m alone", demoted)
			}
		})
		t.Run("one that sent a HAVE is not demoted", func(t *testing.T) {
			if guarded.unproven(gs, "h", root, time.Now()) {
				t.Fatal("h sent a HAVE for the group and was demoted")
			}
		})
		t.Run("a pair is heard once", func(t *testing.T) {
			tosend := map[peer.ID]struct{}{"m": {}, "h": {}, "x": {}, "y": {}}
			guarded.trim(gs, mid, computeChecksum(mid), mesh, tosend, false)
			if len(demoted) != 1 {
				t.Fatalf("demoted %v after a second trim, want m once", demoted)
			}
		})
		t.Run("within grace or below the degree it is not demoted", func(t *testing.T) {
			push("w", 50*time.Millisecond, 0, 1)
			push("s", 300*time.Millisecond, 0)
			if guarded.unproven(gs, "w", root, time.Now()) {
				t.Fatal("w was demoted 50 ms after our first push, inside the 200 ms grace")
			}
			if guarded.unproven(gs, "s", root, time.Now()) {
				t.Fatal("s was demoted after one push, below the degree of two")
			}
		})
		t.Run("without the guard the mute peer ranks first", func(t *testing.T) {
			tosend := map[peer.ID]struct{}{"m": {}, "h": {}, "x": {}, "y": {}}
			(&phaseForwarding{degree: 2, fixedBudget: true, selFewest: true}).trim(gs, mid, computeChecksum(mid), mesh, tosend, false)
			if _, ok := tosend["m"]; !ok {
				t.Fatalf("pushed %v, want m among them", tosend)
			}
		})
	})
	t.Run("the tolerant draw", func(t *testing.T) {
		gs := testHaveRouter()
		gs.have.ihaveOn = true
		// u under-reports: we pushed it one frame and it said nothing more; the rest told us.
		gs.haveNotePushed("u", testMid(root, 0))
		for p, n := range map[peer.ID]int{"a": 2, "b": 3, "c": 6, "d": 7, "e": 8} {
			e := &haveEntry{}
			for i := range n {
				e.bits.Set(uint32(i))
			}
			gs.have.peers[p] = map[string]*haveEntry{root: e}
		}
		mesh := map[peer.ID]struct{}{"u": {}, "a": {}, "b": {}, "c": {}, "d": {}, "e": {}}
		const trims = 3000
		draw := func(pf *phaseForwarding, seed int64, fromSelf bool) (map[peer.ID]int, []peer.ID) {
			gs.rng = rand.New(rand.NewSource(seed))
			counts := map[peer.ID]int{}
			var seq []peer.ID
			for range trims {
				tosend := map[peer.ID]struct{}{"u": {}, "a": {}, "b": {}, "c": {}, "d": {}, "e": {}}
				pf.trim(gs, mid, computeChecksum(mid), mesh, tosend, fromSelf)
				for p := range tosend {
					counts[p]++
				}
				pushed := make([]peer.ID, 0, len(tosend))
				for p := range tosend {
					pushed = append(pushed, p)
				}
				sort.Slice(pushed, func(i, j int) bool { return pushed[i] < pushed[j] })
				seq = append(seq, pushed...)
			}
			return counts, seq
		}
		near := func(got, want int) bool { return got > want-150 && got < want+150 }
		half := func() *phaseForwarding {
			return &phaseForwarding{degree: 2, fixedBudget: true, selFewest: true, selTolerance: PhaseSelectLowerHalf}
		}
		t.Run("argmax gives the under-reporter every push", func(t *testing.T) {
			counts, _ := draw(&phaseForwarding{degree: 2, fixedBudget: true, selFewest: true}, 1, false)
			if counts["u"] != trims || counts["a"] != trims {
				t.Fatalf("pushes %v, want u and a every time", counts)
			}
		})
		t.Run("lower half draws uniformly at or below the median", func(t *testing.T) {
			// Holdings u 1, a 2, b 3 | c 6, d 7, e 8: the median is 3, so u, a and b take 2/3 each.
			counts, _ := draw(half(), 1, false)
			for _, p := range []peer.ID{"u", "a", "b"} {
				if !near(counts[p], 2*trims/3) {
					t.Fatalf("pushes %v, want u, a, b near %d each", counts, 2*trims/3)
				}
			}
			if counts["c"]+counts["d"]+counts["e"] != 0 {
				t.Fatalf("pushes %v, want none above the median", counts)
			}
		})
		t.Run("a seed repeats its draw", func(t *testing.T) {
			_, s1 := draw(half(), 5, false)
			_, s2 := draw(half(), 5, false)
			_, s3 := draw(half(), 6, false)
			if fmt.Sprint(s1) != fmt.Sprint(s2) || fmt.Sprint(s1) == fmt.Sprint(s3) {
				t.Fatal("the seeded draw is not a function of the seed")
			}
		})
		t.Run("the publisher keeps the argmax", func(t *testing.T) {
			counts, _ := draw(half(), 1, true)
			if counts["u"] != trims || counts["a"] != trims {
				t.Fatalf("pushes %v, want u and a every time", counts)
			}
		})
		t.Run("the slow class stays out of the draw", func(t *testing.T) {
			gs.p = &PubSub{peers: map[peer.ID]*rpcQueue{
				"u": {queuedBytes: 1 << 20}, "a": {}, "b": {}, "c": {}, "d": {}, "e": {},
			}}
			defer func() { gs.p = nil }()
			pf := half()
			pf.selBacklog = 32 << 10
			// u is backlogged; among a 2, b 3, c 6, d 7, e 8 the median is 6: a, b, c take 2/3 each.
			counts, _ := draw(pf, 1, false)
			for _, p := range []peer.ID{"a", "b", "c"} {
				if !near(counts[p], 2*trims/3) {
					t.Fatalf("pushes %v, want a, b, c near %d each", counts, 2*trims/3)
				}
			}
			if counts["u"] != 0 {
				t.Fatalf("pushes %v, want none to the backlogged u", counts)
			}
		})
		t.Run("proportional draws by the frames a peer lacks", func(t *testing.T) {
			// Of 16 frames: u lacks 15, a 14, b 13, c 10, d 9, e 8; 69 in all, one push per trim.
			pf := &phaseForwarding{degree: 1, fixedBudget: true, selFewest: true, selTolerance: PhaseSelectProportional, selFrames: 16}
			counts, _ := draw(pf, 1, false)
			for p, lack := range map[peer.ID]int{"u": 15, "a": 14, "b": 13, "c": 10, "d": 9, "e": 8} {
				if !near(counts[p], trims*lack/69) {
					t.Fatalf("pushes %v, want %s near %d", counts, p, trims*lack/69)
				}
			}
		})
	})
	t.Run("not a group member falls back to rank", func(t *testing.T) {
		plain := "plain-id"
		tosend := map[peer.ID]struct{}{"a": {}, "b": {}, "c": {}, "d": {}, "e": {}}
		pf.trim(gs, plain, computeChecksum(plain), mesh, tosend, false)
		rank := (&phaseForwarding{degree: 2, fixedBudget: true})
		want := map[peer.ID]struct{}{"a": {}, "b": {}, "c": {}, "d": {}, "e": {}}
		rank.trim(gs, plain, computeChecksum(plain), mesh, want, false)
		for p := range want {
			if _, ok := tosend[p]; !ok {
				t.Fatalf("sel=fewest outside a group pushed %v, sel=rank %v", tosend, want)
			}
		}
	})
}

// TestControlMute is the mute's known answer: IDONTWANT entries (HAVEs, hose acks) are stripped
// from what a mute node sends, the rest is kept, the input is not touched, and a HAVE is never
// parked where the queue would attach it.
func TestControlMute(t *testing.T) {
	root := strings.Repeat("g", 32)
	var b haveBits
	b.Set(3)
	have := encodeHave(root, &b)
	mixed := func() *RPC {
		return &RPC{RPC: pb.RPC{
			Publish: []*pb.Message{{Data: []byte("frame")}},
			Control: &pb.ControlMessage{
				Iwant:     []*pb.ControlIWant{{MessageIDs: []string{"want"}}},
				Idontwant: []*pb.ControlIDontWant{{MessageIDs: []string{have, "plain"}}},
			},
		}}
	}
	t.Run("off is the identity", func(t *testing.T) {
		gs := testHaveRouter()
		in := mixed()
		if gs.applyControlMute(in) != in {
			t.Fatal("an unmuted router rewrote the RPC")
		}
	})
	var muted atomic.Int64
	gs := testHaveRouter()
	if err := WithControlMute(&muted)(&PubSub{rt: gs}); err != nil {
		t.Fatal(err)
	}
	t.Run("IDONTWANT stripped, the rest kept", func(t *testing.T) {
		in := mixed()
		out := gs.applyControlMute(in)
		if out == nil || len(out.Publish) != 1 || len(out.GetControl().GetIwant()) != 1 || len(out.GetControl().GetIdontwant()) != 0 {
			t.Fatalf("got %v, want the frame and the IWANT without the IDONTWANT", out)
		}
		if len(in.GetControl().GetIdontwant()) != 1 {
			t.Fatal("the input RPC was mutated")
		}
		if muted.Load() != 2 {
			t.Fatalf("counted %d ids, want 2", muted.Load())
		}
	})
	t.Run("an IDONTWANT-only RPC is consumed", func(t *testing.T) {
		ack := rpcWithControl(nil, nil, nil, nil, nil, []*pb.ControlIDontWant{{MessageIDs: []string{have}}})
		if out := gs.applyControlMute(ack); out != nil {
			t.Fatalf("got %v, want nothing sent", out)
		}
	})
	t.Run("a HAVE is not parked", func(t *testing.T) {
		gs.have.attach = true
		gs.have.mine[root] = &haveEntry{bits: b}
		if gs.havePendAll("peer", []string{have}) {
			t.Fatal("a mute node parked a HAVE for the queue to attach")
		}
	})
}

// TestHaveUnderReport is the tailored under-reporter's known answer: each peer's HAVE carries
// only the frames that peer sent us, claims are dropped, hose stamps and other control are kept,
// the input is not touched, a parked HAVE is cut the same way, and the pusher's proof guard
// passes while its sel=fewest view of us stays at its own pushes.
func TestHaveUnderReport(t *testing.T) {
	root := strings.Repeat("g", 32)
	full := fullHaveBits(8)
	have := encodeHave(root, full)
	claim := encodeHaveClaim(root, full)
	stamp := encodeHoseStamp(time.Now(), root, []uint32{1})
	mixed := func() *RPC {
		return &RPC{RPC: pb.RPC{
			Publish: []*pb.Message{{Data: []byte("frame")}},
			Control: &pb.ControlMessage{
				Iwant:     []*pb.ControlIWant{{MessageIDs: []string{"want"}}},
				Idontwant: []*pb.ControlIDontWant{{MessageIDs: []string{have, claim, stamp, testMid(root, 1), testMid(root, 2), "plain"}}},
			},
		}}
	}
	t.Run("off is the identity", func(t *testing.T) {
		gs := testHaveRouter()
		in := mixed()
		if gs.applyUnderReport(in, "a") != in {
			t.Fatal("a router without the under-report rewrote the RPC")
		}
	})
	t.Run("an honest node parks its whole HAVE", func(t *testing.T) {
		gs := testHaveRouter()
		gs.have.attach = true
		gs.have.mine[root] = &haveEntry{bits: *full}
		gs.p = &PubSub{peers: map[peer.ID]*rpcQueue{"a": newRpcQueue(4)}}
		q := gs.p.peers["a"]
		q.enableHave(testQueueIndex, true, true)
		if err := q.Push(&RPC{RPC: pb.RPC{Publish: []*pb.Message{{Data: []byte("x")}}}}, false); err != nil {
			t.Fatal(err)
		}
		if !gs.havePendAll("a", []string{have}) {
			t.Fatal("the HAVE was not parked")
		}
		if got := q.have.pending[root]; got == nil || got.Count() != 8 {
			t.Fatalf("parked %v, want all 8 frames", got)
		}
	})
	var withheld, claims atomic.Int64
	gs := testHaveRouter()
	if err := WithHaveUnderReport(&withheld, &claims)(&PubSub{rt: gs}); err != nil {
		t.Fatal(err)
	}
	gs.underReportNote("a", testMid(root, 1))
	gs.underReportNote("a", testMid(root, 5))
	gs.underReportNote("b", testMid(root, 2))
	bitsTo := func(out *RPC) (got []uint32, ids []string) {
		for _, e := range out.GetControl().GetIdontwant() {
			for _, mid := range e.GetMessageIDs() {
				ids = append(ids, mid)
				if r, enc, ok := decodeHave(mid); ok && r == root && !isHaveClaim(mid) {
					var b haveBits
					b.Or(enc)
					for i := uint32(0); i < 8; i++ {
						if b.Has(i) {
							got = append(got, i)
						}
					}
				}
			}
		}
		return got, ids
	}
	t.Run("each peer hears only what it sent", func(t *testing.T) {
		in := mixed()
		outA := gs.applyUnderReport(in, "a")
		gotA, idsA := bitsTo(outA)
		if fmt.Sprint(gotA) != "[1 5]" {
			t.Fatalf("a's HAVE covers %v, want [1 5]", gotA)
		}
		gotB, _ := bitsTo(gs.applyUnderReport(in, "b"))
		if fmt.Sprint(gotB) != "[2]" {
			t.Fatalf("b's HAVE covers %v, want [2]", gotB)
		}
		gotC, idsC := bitsTo(gs.applyUnderReport(in, "c"))
		if len(gotC) != 0 || len(idsC) == 0 || idsC[0] != encodeHave(root, &haveBits{}) {
			t.Fatalf("c's HAVE covers %v (ids %q), want an empty bitmap still sent", gotC, idsC)
		}
		for _, id := range idsA {
			if isHaveClaim(id) {
				t.Fatal("a claim went out")
			}
		}
		// a keeps the stamp, its own per-id frame 1 and the plain id; b's frame 2 is not a's.
		want := []string{encodeHave(root, &haveBits{b: []byte{1<<1 | 1<<5}}), stamp, testMid(root, 1), "plain"}
		if fmt.Sprintf("%q", idsA) != fmt.Sprintf("%q", want) {
			t.Fatalf("a got ids %q, want %q", idsA, want)
		}
		if len(outA.Publish) != 1 || len(outA.GetControl().GetIwant()) != 1 {
			t.Fatalf("got %v, want the frame and the IWANT kept", outA)
		}
		if len(in.GetControl().GetIdontwant()[0].GetMessageIDs()) != 6 {
			t.Fatal("the input RPC was mutated")
		}
		if claims.Load() != 3 {
			t.Fatalf("counted %d claims, want 3 (one per peer)", claims.Load())
		}
		// a: 6 bits of the HAVE and per-id frame 2; b: 7 bits and frame 1; c: 8 bits and both.
		if withheld.Load() != 7+8+10 {
			t.Fatalf("counted %d indices withheld, want 25", withheld.Load())
		}
	})
	t.Run("a claim-only RPC is consumed", func(t *testing.T) {
		out := gs.applyUnderReport(rpcWithControl(nil, nil, nil, nil, nil, []*pb.ControlIDontWant{{MessageIDs: []string{claim}}}), "a")
		if out != nil {
			t.Fatalf("got %v, want nothing sent", out)
		}
	})
	t.Run("a parked HAVE is cut too", func(t *testing.T) {
		gs.have.attach = true
		gs.have.mine[root] = &haveEntry{bits: *full}
		gs.p = &PubSub{peers: map[peer.ID]*rpcQueue{"a": newRpcQueue(4)}}
		defer func() { gs.p = nil }()
		q := gs.p.peers["a"]
		q.enableHave(testQueueIndex, true, true)
		if err := q.Push(&RPC{RPC: pb.RPC{Publish: []*pb.Message{{Data: []byte("x")}}}}, false); err != nil {
			t.Fatal(err)
		}
		if !gs.havePendAll("a", []string{have}) {
			t.Fatal("the HAVE was not parked")
		}
		if got := q.have.pending[root]; got == nil || got.Count() != 2 || !got.Has(1) || !got.Has(5) {
			t.Fatalf("parked %v, want a's frames 1 and 5", got)
		}
	})
	t.Run("the pusher's guard passes and its view stays at its pushes", func(t *testing.T) {
		pusher := testHaveRouter()
		pusher.have.ihaveOn = true
		pf := &phaseForwarding{degree: 2, fixedBudget: true, selFewest: true, selGrace: 200 * time.Millisecond}
		for _, i := range []byte{1, 5} {
			pusher.haveNotePushed("u", testMid(root, i))
		}
		pusher.have.pushed["u"][root].first = time.Now().Add(-time.Second)
		cut, _ := bitsTo(gs.applyUnderReport(mixed(), "a"))
		var b haveBits
		for _, i := range cut {
			b.Set(i)
		}
		if !pusher.haveAbsorb("u", encodeHave(root, &b)) {
			t.Fatal("HAVE not absorbed")
		}
		if pf.unproven(pusher, "u", root, time.Now()) {
			t.Fatal("the under-reporter was demoted by the proof guard")
		}
		if got := pusher.have.knownFrames("u", root); got != 2 {
			t.Fatalf("the pusher knows %d frames of it, want its own 2 pushes", got)
		}
	})
	t.Run("what a peer sent ages out and leaves with it", func(t *testing.T) {
		gs.underReportNote("d", testMid(root, 0))
		gs.haveForget("d")
		if gs.under.from["d"] != nil {
			t.Fatal("a departed peer's frames were kept")
		}
		for range gs.have.holdTTL {
			gs.haveHeartbeat()
		}
		if len(gs.under.from) != 0 {
			t.Fatalf("kept %d peers after the TTL", len(gs.under.from))
		}
	})
}

// TestFrameClassAtPop is the priority class's unit known answer: a lower class pops first even
// when the higher class's frames are rarer and queued ahead; rarest-first orders within a class;
// without a class the rarer frames lead.
func TestFrameClassAtPop(t *testing.T) {
	rootA, rootB := strings.Repeat("a", 32), strings.Repeat("b", 32)
	order := func(withClass bool, releaseMargin int) []string {
		q := newRpcQueue(16)
		q.enableHave(testQueueIndex, true, false)
		if releaseMargin > 0 {
			q.enableProjection(4, 1)
			var b haveBits
			b.Set(5) // the peer announced one frame of a's group, not one queued here
			q.have.theirs[rootA] = &b
		}
		q.enableRarest(func(m *pb.Message) (int, bool) {
			r, i, ok := testQueueIndex(m)
			if !ok {
				return 0, false
			}
			if r == rootB {
				return int(i), true // b is rarer than every a frame
			}
			return 10 - int(i), true
		})
		if withClass {
			q.enableClass(func(m *pb.Message) int {
				if r, _, _ := testQueueIndex(m); r == rootB {
					return 1
				}
				return 0
			}, false, releaseMargin, 1)
		}
		for _, f := range []struct {
			root string
			i    byte
		}{{rootB, 0}, {rootB, 1}, {rootA, 0}, {rootA, 1}} {
			if err := q.Push(testDataRPC(f.root, f.i), false); err != nil {
				t.Fatal(err)
			}
		}
		ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
		defer cancel()
		var got []string
		for range 4 {
			rpc, err := q.Pop(ctx)
			if err != nil {
				t.Fatal(err)
			}
			r, i, _ := testQueueIndex(rpc.Publish[0])
			got = append(got, fmt.Sprintf("%c%d", r[0], i))
		}
		return got
	}
	if got, want := fmt.Sprint(order(true, 0)), "[a1 a0 b0 b1]"; got != want {
		t.Fatalf("with class: popped %s, want %s (class first, rarest within)", got, want)
	}
	if got, want := fmt.Sprint(order(false, 0)), "[b0 b1 a1 a0]"; got != want {
		t.Fatalf("without class: popped %s, want %s (rarest first)", got, want)
	}
	// Projected at 1 of 4 with a margin of 3: the peer is within the margin of completing a, so
	// a's frames pop at b's class and rarest-first orders the lot.
	if got, want := fmt.Sprint(order(true, 3)), "[b0 b1 a1 a0]"; got != want {
		t.Fatalf("released: popped %s, want %s", got, want)
	}
	// A margin of 2 leaves the peer outside it (1 + 2 < 4): a1 keeps its priority, and popping it
	// brings the projection to 2, inside the margin, so a0 falls back among b's frames.
	if got, want := fmt.Sprint(order(true, 2)), "[a1 b0 b1 a0]"; got != want {
		t.Fatalf("outside the margin: popped %s, want %s", got, want)
	}
}

// TestFrameClassPullOnly: under the pull-only scope a pushed frame pops at class 0 whatever its
// class, so pushes order by rarity alone; an answered ask keeps its class.
func TestFrameClassPullOnly(t *testing.T) {
	rootA, rootB := strings.Repeat("a", 32), strings.Repeat("b", 32)
	order := func(solicited bool) string {
		q := newRpcQueue(16)
		q.enableHave(testQueueIndex, true, false)
		q.enableRarest(func(m *pb.Message) (int, bool) {
			r, i, ok := testQueueIndex(m)
			if !ok {
				return 0, false
			}
			if r == rootB {
				return int(i), true
			}
			return 10 - int(i), true
		})
		q.enableClass(func(m *pb.Message) int {
			if r, _, _ := testQueueIndex(m); r == rootB {
				return 1
			}
			return 0
		}, false, 0, 0)
		q.have.classPullOnly = true
		for _, f := range []struct {
			root string
			i    byte
		}{{rootB, 0}, {rootB, 1}, {rootA, 0}, {rootA, 1}} {
			rpc := testDataRPC(f.root, f.i)
			rpc.solicited = solicited
			if err := q.Push(rpc, false); err != nil {
				t.Fatal(err)
			}
		}
		ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
		defer cancel()
		var got []string
		for range 4 {
			rpc, err := q.Pop(ctx)
			if err != nil {
				t.Fatal(err)
			}
			r, i, _ := testQueueIndex(rpc.Publish[0])
			got = append(got, fmt.Sprintf("%c%d", r[0], i))
		}
		return fmt.Sprint(got)
	}
	if got, want := order(false), "[b0 b1 a1 a0]"; got != want {
		t.Fatalf("pushes: popped %s, want %s (rarity alone)", got, want)
	}
	if got, want := order(true), "[a1 a0 b0 b1]"; got != want {
		t.Fatalf("answers: popped %s, want %s (class first)", got, want)
	}
}

// TestDecodeClearsPromises is WithDecodeClearsPromises's known answer: the decode claim fulfils the group's promises, by index and by id, and no other group's.
func TestDecodeClearsPromises(t *testing.T) {
	root, other := strings.Repeat("g", 32), strings.Repeat("h", 32)
	for _, on := range []bool{false, true} {
		t.Run(fmt.Sprintf("clears %v", on), func(t *testing.T) {
			gs := testHaveRouter()
			gs.have.decodeClears = on
			gs.gossipTracer = newGossipTracer()
			gs.gossipTracer.followUpTime = time.Minute
			gs.gossipTracer.AddPromise("A", []string{encodeWant(root, 1)})
			gs.gossipTracer.AddPromise("B", []string{testMid(root, 2)})
			gs.gossipTracer.AddPromise("C", []string{encodeWant(other, 1)})
			gs.gossipTracer.AddPromise("D", []string{testMid(other, 3)})
			gs.haveAll("t", root, 4)
			left := make(map[peer.ID]bool)
			for _, ps := range gs.gossipTracer.promises {
				for p := range ps {
					left[p] = true
				}
			}
			want := map[peer.ID]bool{"A": !on, "B": !on, "C": true, "D": true}
			for p, w := range want {
				if left[p] != w {
					t.Fatalf("promise to %s outstanding %v, want %v", p, left[p], w)
				}
			}
			if on && (len(gs.gossipTracer.peerPromises["A"]) != 0 || len(gs.gossipTracer.peerPromises["B"]) != 0) {
				t.Fatal("a cleared promise must leave the per-peer index too")
			}
		})
	}
}

// TestOriginHave is WithOriginHave's known answer: a batch publisher's mesh peer gets one HAVE with every published index; without it, none.
func TestOriginHave(t *testing.T) {
	root := strings.Repeat("g", 32)
	for _, on := range []bool{false, true} {
		t.Run(fmt.Sprintf("origin HAVE %v", on), func(t *testing.T) {
			synctestTest(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				hosts := getDefaultHosts(t, 2)
				var mu sync.Mutex
				var got []haveBits
				tr := &mockRawTracer{onRecvRPC: func(rpc *RPC) {
					mu.Lock()
					defer mu.Unlock()
					for _, idw := range rpc.GetControl().GetIdontwant() {
						for _, mid := range idw.GetMessageIDs() {
							if r, enc, ok := decodeHave(mid); ok && r == root {
								var b haveBits
								b.Or(enc)
								got = append(got, b)
							}
						}
					}
				}}
				psubs := getGossipsubsOptFn(ctx, hosts, func(i int, _ host.Host) []Option {
					opts := []Option{
						WithMessageSignaturePolicy(StrictNoSign), WithNoAuthor(),
						WithMessageIdFn(func(m *pb.Message) string { return string(m.Data) }),
						WithHaveBitmaps(testIndex),
					}
					if on && i == 0 {
						opts = append(opts, WithOriginHave())
					}
					if i == 1 {
						opts = append(opts, WithRawTracer(tr))
					}
					return opts
				})
				var topics []*Topic
				for _, ps := range psubs {
					tp, err := ps.Join("t")
					if err != nil {
						t.Fatal(err)
					}
					if _, err := tp.Subscribe(); err != nil {
						t.Fatal(err)
					}
					topics = append(topics, tp)
				}
				connectAll(t, hosts)
				time.Sleep(2 * time.Second)
				var batch MessageBatch
				for i := byte(0); i < 3; i++ {
					if err := topics[0].AddToBatch(ctx, &batch, []byte(testMid(root, i))); err != nil {
						t.Fatal(err)
					}
				}
				if err := psubs[0].PublishBatch(&batch); err != nil {
					t.Fatal(err)
				}
				time.Sleep(time.Second)
				mu.Lock()
				defer mu.Unlock()
				if !on {
					if len(got) != 0 {
						t.Fatalf("no origin HAVE without the option, got %d", len(got))
					}
					return
				}
				if len(got) != 1 || got[0].Count() != 3 || !got[0].Has(0) || !got[0].Has(2) {
					t.Fatalf("want one HAVE with indices 0..2, got %v", got)
				}
			})
		})
	}
}

// TestOriginHaveModes is WithOriginHaveMode's known answer. Frames above the IDONTWANT threshold
// make a publisher's Preprocess send its mesh a HAVE per frame it publishes, all before its first
// frame leaves. Paced, the first HAVE leads the first frame and names it alone, every later one
// names only frames the peer already got, and the last names all; per peer (sent), every HAVE
// rides a frame and names exactly the frames received; none, the peer gets no HAVE at all.
func TestOriginHaveModes(t *testing.T) {
	root := strings.Repeat("p", 32)
	const frames = 8
	idLen := len(testMid(root, 0))
	for _, arm := range []struct {
		name     string
		mode     OriginHaveMode // 0: no option
		dispatch bool
	}{
		{"at publish", 0, false},
		{"paced", OriginHavePaced, false},
		{"paced with dispatch cancel", OriginHavePaced, true},
		{"none", OriginHaveNone, true},
		{"sent", OriginHaveSent, true},
	} {
		t.Run(arm.name, func(t *testing.T) {
			synctestTest(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				hosts := getDefaultHosts(t, 2)
				var mu sync.Mutex
				// got and named: the frames the peer received and the publisher's HAVEs so far; ahead:
				// HAVEs after the first that name a frame not yet received; exact: HAVEs naming exactly got.
				var got, named haveBits
				namedAtFirst, ahead, exact, haves := -1, 0, 0, 0
				tr := &mockRawTracer{onRecvRPC: func(rpc *RPC) {
					mu.Lock()
					defer mu.Unlock()
					for _, m := range rpc.GetPublish() {
						if _, i, ok := testIndex(string(m.GetData()[:idLen])); ok {
							if got.Count() == 0 {
								namedAtFirst = named.Count()
							}
							got.Set(i)
						}
					}
					for _, idw := range rpc.GetControl().GetIdontwant() {
						for _, mid := range idw.GetMessageIDs() {
							if r, enc, ok := decodeHave(mid); ok && r == root {
								haves++
								var b haveBits
								b.Or(enc)
								if haves > 1 && len(b.AndNot(&got).Bytes()) > 0 {
									ahead++
								}
								if len(b.AndNot(&got).Bytes()) == 0 && len(got.AndNot(&b).Bytes()) == 0 {
									exact++
								}
								named.Or(enc)
							}
						}
					}
				}}
				psubs := getGossipsubsOptFn(ctx, hosts, func(i int, _ host.Host) []Option {
					opts := []Option{
						WithMessageSignaturePolicy(StrictNoSign), WithNoAuthor(),
						WithMessageIdFn(func(m *pb.Message) string { return string(m.Data[:idLen]) }),
						WithHaveBitmaps(testIndex),
					}
					if arm.dispatch {
						opts = append(opts, WithHaveDispatch(true, false))
					}
					if arm.mode != 0 && i == 0 {
						opts = append(opts, WithOriginHaveMode(arm.mode))
					}
					if i == 1 {
						opts = append(opts, WithRawTracer(tr))
					}
					return opts
				})
				var topics []*Topic
				for _, ps := range psubs {
					tp, err := ps.Join("t")
					if err != nil {
						t.Fatal(err)
					}
					if _, err := tp.Subscribe(); err != nil {
						t.Fatal(err)
					}
					topics = append(topics, tp)
				}
				connectAll(t, hosts)
				time.Sleep(2 * time.Second)
				pad := strings.Repeat("x", 2*GossipSubIDontWantMessageThreshold)
				var batch MessageBatch
				for i := byte(0); i < frames; i++ {
					if err := topics[0].AddToBatch(ctx, &batch, []byte(testMid(root, i)+pad)); err != nil {
						t.Fatal(err)
					}
				}
				if err := psubs[0].PublishBatch(&batch); err != nil {
					t.Fatal(err)
				}
				time.Sleep(time.Second)
				mu.Lock()
				defer mu.Unlock()
				wantNamed := frames
				if arm.mode == OriginHaveNone {
					wantNamed = 0
				}
				if got.Count() != frames || named.Count() != wantNamed {
					t.Fatalf("peer got %d of %d frames and was named %d, want %d", got.Count(), frames, named.Count(), wantNamed)
				}
				switch arm.mode {
				case 0:
					if namedAtFirst != frames {
						t.Fatalf("at publish: %d of %d frames named before the first arrived, want all", namedAtFirst, frames)
					}
				case OriginHavePaced:
					if namedAtFirst != 1 || ahead != 0 {
						t.Fatalf("paced: %d frames named before the first arrived (want the lead's 1), %d of %d later HAVEs ahead of their frames (want 0)", namedAtFirst, ahead, haves)
					}
				case OriginHaveNone:
					if haves != 0 {
						t.Fatalf("none: %d HAVEs, want 0", haves)
					}
				case OriginHaveSent:
					if namedAtFirst != 0 || haves == 0 || exact != haves {
						t.Fatalf("sent: %d frames named before the first arrived (want 0), %d of %d HAVEs naming exactly the frames received (want all)", namedAtFirst, exact, haves)
					}
				}
			})
		})
	}
	t.Run("exclusive with WithOriginHave", func(t *testing.T) {
		for i, order := range [][2]Option{{WithOriginHave(), WithOriginHavePaced()}, {WithOriginHaveMode(OriginHaveNone), WithOriginHave()}} {
			ps := &PubSub{rt: &GossipSubRouter{have: newHaveState(testIndex)}}
			if err := order[0](ps); err != nil {
				t.Fatalf("order %d: first option: %v", i, err)
			}
			if err := order[1](ps); err == nil {
				t.Fatalf("order %d: the second option must refuse the first", i)
			}
		}
	})
}

// TestStaleAsksAtDispatch is the spec's §7.2 known answer for asks: a queued ask for a frame we
// have received since leaves no more, an ask for one we still lack does, and an RPC left with
// nothing to say is not written.
func TestStaleAsksAtDispatch(t *testing.T) {
	root := strings.Repeat("s", 32)
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	held := map[string]bool{encodeWant(root, 1): true}
	q := newRpcQueue(16)
	q.enableHave(testQueueIndex, true, false)
	q.enableStaleAsks(func(mid string) bool { return held[mid] })
	ask := &RPC{RPC: pb.RPC{Control: &pb.ControlMessage{Iwant: []*pb.ControlIWant{{MessageIDs: []string{encodeWant(root, 1), encodeWant(root, 2)}}}}}}
	only := &RPC{RPC: pb.RPC{Control: &pb.ControlMessage{Iwant: []*pb.ControlIWant{{MessageIDs: []string{encodeWant(root, 1)}}}}}}
	before := StaleAsksDropped.Load()
	for _, r := range []*RPC{ask, only} {
		if err := q.Push(r, false); err != nil {
			t.Fatal(err)
		}
	}
	rpc, err := q.Pop(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if ids := rpc.Control.GetIwant()[0].GetMessageIDs(); len(ids) != 1 || ids[0] != encodeWant(root, 2) {
		t.Fatalf("the ask for frame 1, held by now, drops and frame 2's stays: got %v", ids)
	}
	if len(ask.Control.Iwant[0].MessageIDs) != 2 {
		t.Fatal("the queued RPC was mutated")
	}
	if rpc, err := q.Pop(ctx); err == nil {
		t.Fatalf("an RPC whose only ask is stale is not written, popped %v", rpc)
	}
	if got := StaleAsksDropped.Load() - before; got != 2 {
		t.Fatalf("stale asks dropped = %d, want 2", got)
	}
}

// TestOwnViewAndPeerHolds: the writers' mirror of what we hold follows arrivals, a full claim and
// expiry, and havePeerHolds reads a peer's HAVE.
func TestOwnViewAndPeerHolds(t *testing.T) {
	root := strings.Repeat("o", 32)
	h := newHaveState(func(mid string) (string, uint32, bool) { return testIndex(mid) })
	h.own.set(root, 3)
	if !h.ownHeldID(encodeWant(root, 3)) || h.ownHeldID(encodeWant(root, 4)) {
		t.Fatal("own view: index 3 held, 4 not")
	}
	h.own.or(root, fullHaveBits(8).Bytes())
	if !h.ownHeldID(encodeWant(root, 7)) {
		t.Fatal("own view: a full claim holds index 7")
	}
	h.own.forget(root)
	if h.ownHeldID(encodeWant(root, 3)) {
		t.Fatal("own view: a forgotten root holds nothing")
	}
	gs := &GossipSubRouter{have: h}
	var b haveBits
	b.Set(5)
	h.peers["p"] = map[string]*haveEntry{root: {bits: b}}
	if !gs.havePeerHolds("p", root, 5) || gs.havePeerHolds("p", root, 6) || gs.havePeerHolds("q", root, 5) {
		t.Fatal("havePeerHolds: p holds 5 only, q nothing")
	}
}
