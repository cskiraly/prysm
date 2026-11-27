package pubsub

import (
	"context"
	"crypto/sha256"
	"slices"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	core "github.com/cskiraly/seacast-go"
	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p-pubsub/timecache"
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
	c[10] = 0 // clear again by hand: trailing zeros must trim
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
		if gs.have.mine[seacastGID(root)] != nil {
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
		if q.havePend([]string{root}, []*pendHave{{bits: &mine}}) {
			t.Fatal("nothing queued, so nothing to ride: havePend must refuse")
		}
		if err := q.Push(testDataRPC(root, 0), false); err != nil {
			t.Fatal(err)
		}
		if err := q.Push(testDataRPC(root, 1), false); err != nil {
			t.Fatal(err)
		}
		if !q.havePend([]string{root}, []*pendHave{{bits: &mine}}) {
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

// TestRarestHoldersAreLive: rarest's holder counts are the peers' live holdings (seacast-lite.md
// §4.3's holders). A peer leaves them when its holding of the group ages out, and counts once when
// it announces the group again; it leaves them when its connection closes; the node's own holding of
// the group expiring leaves them as they are; and the root goes with its last holder, to be counted
// anew. The writer's order follows: once p's holding ages out, index 1 ties with index 3 and goes
// first, behind index 4's two holders. With p still counted, index 3 would go first; without rarest,
// index 4.
func TestRarestHoldersAreLive(t *testing.T) {
	root := strings.Repeat("h", 32)
	gs := testHaveRouter()
	gs.have.holders = &holderCounts{}
	have := func(p peer.ID, indices ...uint32) {
		var b haveBits
		for _, i := range indices {
			b.Set(i)
		}
		if !gs.haveAbsorb(p, encodeHave(root, &b)) {
			t.Fatalf("%s's HAVE not read as one", string(p))
		}
	}
	check := func(when string, want ...int) {
		t.Helper()
		got := make([]int, len(want))
		for i := range got {
			got[i] = gs.have.holders.get(root, uint32(i))
		}
		if !slices.Equal(got, want) {
			t.Fatalf("%s: holders of indices 0 to %d %v, want %v", when, len(want)-1, got, want)
		}
	}
	p, q, r, s, u := peer.ID("p"), peer.ID("q"), peer.ID("r"), peer.ID("s"), peer.ID("u")
	have(p, 1, 2)
	have(q, 1)
	have(r, 2, 3)
	have(s, 4)
	have(u, 4)
	check("five holdings", 0, 2, 2, 1, 2)

	ttl := gs.params.IDontWantMessageTTL
	for range ttl - 1 {
		gs.haveHeartbeat()
	}
	for _, again := range []peer.ID{q, r, s, u} { // all but p announce again, so only p's holding ages out
		have(again, gs.have.peers.Of(again, seacastGID(root)).Bits.Indices()...)
	}
	check("all but p again", 0, 2, 2, 1, 2)
	gs.haveHeartbeat()
	if gs.have.peers.Of(p, seacastGID(root)) != nil {
		t.Fatalf("p's holding did not age out after %d heartbeats", ttl)
	}
	check("p's holding aged out", 0, 1, 1, 1, 2)

	wq := newRpcQueue(16)
	wq.enableHave(testQueueIndex, false, false)
	wq.enableRarest(func(m *pb.Message) (int, bool) {
		rt, i, ok := testQueueIndex(m)
		return gs.have.holders.get(rt, i), ok
	})
	for _, i := range []byte{4, 1, 3} {
		if err := wq.Push(testDataRPC(root, i), false); err != nil {
			t.Fatal(err)
		}
	}
	if rpc, err := wq.Pop(t.Context()); err != nil || rpc.Publish[0].Data[32] != 1 {
		t.Fatalf("after p's holding aged out the writer did not send index 1 first: %v", err)
	}

	have(p, 2)
	check("p again", 0, 1, 2, 1, 2)
	gs.haveForget(r)
	check("r gone", 0, 1, 1, 0, 2)
	gs.have.mine[seacastGID(root)] = &core.Holding{TTL: 1}
	gs.haveHeartbeat()
	if gs.have.mine[seacastGID(root)] != nil {
		t.Fatal("the node's own holding did not expire")
	}
	check("the node's own holding expired", 0, 1, 1, 0, 2)
	for _, gone := range []peer.ID{p, q, s, u} {
		gs.haveForget(gone)
	}
	gs.have.holders.mu.Lock()
	_, kept := gs.have.holders.counts[root]
	gs.have.holders.mu.Unlock()
	if kept {
		t.Fatal("the root's counts stayed after its last holder went")
	}
	have(q, 1)
	check("q back", 0, 1, 0, 0, 0)
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

	t.Run("a stock message is not chosen, and the rarer chunk behind it still goes first", func(t *testing.T) {
		q := newQ()
		stock := &RPC{RPC: pb.RPC{Publish: []*pb.Message{{Data: []byte("a block")}}}}
		stock.size = 100
		for _, rpc := range []*RPC{testDataRPC(root, 0), stock, testDataRPC(root, 1)} {
			if err := q.Push(rpc, false); err != nil {
				t.Fatal(err)
			}
		}
		// The stock message's holders are unknown, so it is no candidate: index 1 (one holder)
		// goes first, then index 0 (five) at the head, then the stock message.
		var got []string
		for range 3 {
			got = append(got, string(pop(t, q).Publish[0].Data))
		}
		if want := []string{testMid(root, 1), testMid(root, 0), "a block"}; !slices.Equal(got, want) {
			t.Fatalf("popped %q, want %q", got, want)
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

// TestHaveAllCancelsEveryQueuedFrame: a full bitmap of a
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

// TestDispatchProjectionDropsAtPop: need 4, fan-in 2, the peer
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

// TestSolicitedFramesAtDispatch: a solicited frame the peer has since announced is stale and
// cancels like any other, and a solicited frame is never projected away.
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
	t.Run("written when the projection already says complete", func(t *testing.T) {
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

// TestPhaseSelectFewest is sel=fewest's known answer: with a budget of two, the two mesh peers
// known to hold the fewest frames of the group are pushed -- a HAVE and our own pushes count as
// one union -- and the rest are announced; outside a group the rank order is kept.
func TestPhaseSelectFewest(t *testing.T) {
	gs := testHaveRouter()
	gs.have.ihaveOn = true
	pf := &phaseForwarding{degree: 2, selFewest: true}
	root := strings.Repeat("g", 32)
	mid := testMid(root, 9)
	have := func(p peer.ID, idx ...uint32) {
		if gs.have.peers[p] == nil {
			gs.have.peers[p] = make(map[core.GroupID]*core.PeerHolding)
		}
		e := &core.PeerHolding{}
		for _, i := range idx {
			e.Bits.Set(i)
		}
		gs.have.peers[p][seacastGID(root)] = e
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
	t.Run("the proof guard", func(t *testing.T) {
		gs := testHaveRouter()
		gs.have.ihaveOn = true
		var demoted []peer.ID
		guarded := &phaseForwarding{degree: 2, selFewest: true, selGrace: 200 * time.Millisecond,
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
			gs.have.pushed[p][seacastGID(root)].First = time.Now().Add(-ago)
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
			(&phaseForwarding{degree: 2, selFewest: true}).trim(gs, mid, computeChecksum(mid), mesh, tosend, false)
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
			e := &core.PeerHolding{}
			for i := range n {
				e.Bits.Set(uint32(i))
			}
			gs.have.peers[p] = map[core.GroupID]*core.PeerHolding{seacastGID(root): e}
		}
		mesh := map[peer.ID]struct{}{"u": {}, "a": {}, "b": {}, "c": {}, "d": {}, "e": {}}
		const trims = 3000
		draw := func(pf *phaseForwarding, _ int64, fromSelf bool) (map[peer.ID]int, []peer.ID) {
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
			return &phaseForwarding{degree: 2, selFewest: true, selTolerance: PhaseSelectLowerHalf}
		}
		t.Run("argmax gives the under-reporter every push", func(t *testing.T) {
			counts, _ := draw(&phaseForwarding{degree: 2, selFewest: true}, 1, false)
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
		t.Run("the publisher keeps the argmax", func(t *testing.T) {
			counts, _ := draw(half(), 1, true)
			if counts["u"] != trims || counts["a"] != trims {
				t.Fatalf("pushes %v, want u and a every time", counts)
			}
		})
		t.Run("proportional draws by the frames a peer lacks", func(t *testing.T) {
			// Of 16 frames: u lacks 15, a 14, b 13, c 10, d 9, e 8; 69 in all, one push per trim.
			pf := &phaseForwarding{degree: 1, selFewest: true, selTolerance: PhaseSelectProportional, selFrames: 16}
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
		rank := (&phaseForwarding{degree: 2})
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
		gs.have.mine[seacastGID(root)] = &core.Holding{Bits: b}
		if gs.havePendAll("peer", []string{have}) {
			t.Fatal("a mute node parked a HAVE for the queue to attach")
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

// TestOwnViewAndPeerHolds: the writers' mirror of what we hold follows arrivals and expiry, and
// havePeerHolds reads a peer's HAVE.
func TestOwnViewAndPeerHolds(t *testing.T) {
	root := strings.Repeat("o", 32)
	h := newHaveState(func(mid string) (string, uint32, bool) { return testIndex(mid) })
	h.own.set(root, 3)
	if !h.ownHeldID(encodeWant(root, 3)) || h.ownHeldID(encodeWant(root, 4)) {
		t.Fatal("own view: index 3 held, 4 not")
	}
	h.own.forget(root)
	if h.ownHeldID(encodeWant(root, 3)) {
		t.Fatal("own view: a forgotten root holds nothing")
	}
	gs := &GossipSubRouter{have: h}
	var b haveBits
	b.Set(5)
	h.peers["p"] = map[core.GroupID]*core.PeerHolding{seacastGID(root): {Bits: b}}
	if !gs.havePeerHolds("p", root, 5) || gs.havePeerHolds("p", root, 6) || gs.havePeerHolds("q", root, 5) {
		t.Fatal("havePeerHolds: p holds 5 only, q nothing")
	}
}
