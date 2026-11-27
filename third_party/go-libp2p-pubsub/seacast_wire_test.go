package pubsub

import (
	"context"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	core "github.com/cskiraly/seacast-go"
	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
)

// TestSeacastWireCodec is the v4 codec's contract: the holding's and the WANT's layouts round
// trip, the WANT is 43 bytes, a reserved bit, a wrong length, hops over the bound and a bitmap
// over the cap are malformed, and an unknown version of either magic is recognised as a pseudo id.
func TestSeacastWireCodec(t *testing.T) {
	root := strings.Repeat("r", haveRootLen)
	var bits haveBits
	bits.Set(0)
	bits.Set(5)
	bits.Set(31)
	h := encodeHaveV2(root, &bits, true, false)
	if len(h) != seacastHaveHeadLen+4 {
		t.Fatalf("holding length %d, want header %d + 4 bitmap bytes", len(h), seacastHaveHeadLen)
	}
	r, ca, co, enc, ok, mal := decodeHaveV2(h)
	if !ok || mal || r != root || !ca || co || len(enc) != 4 || enc[0] != 0b100001 || enc[3] != 0x80 {
		t.Fatalf("holding round trip: ok %v mal %v claimAll %v complete %v enc %x", ok, mal, ca, co, enc)
	}
	if _, _, _, _, ok, _ := decodeHaveV2(encodeHave(root, &bits)); ok {
		t.Fatalf("a v1 holding decoded as v4")
	}
	bad := []byte(h)
	bad[haveHeaderLen] |= 0x80 // a reserved flag bit
	if _, _, _, _, ok, mal := decodeHaveV2(string(bad)); !ok || !mal {
		t.Fatalf("a reserved flag bit not malformed")
	}
	over := h + strings.Repeat("\x01", haveMaxBytes)
	if _, _, _, _, ok, mal := decodeHaveV2(over); !ok || !mal {
		t.Fatalf("a bitmap over the cap not malformed")
	}

	w := encodeWantV2(root, 0x01020304, true, 1)
	if len(w) != seacastWantLen || seacastWantLen != 43 {
		t.Fatalf("WANT length %d, want 43", len(w))
	}
	r, idx, standing, hops, ok, mal := decodeWantV2(w)
	if !ok || mal || r != root || idx != 0x01020304 || !standing || hops != 1 {
		t.Fatalf("WANT round trip: ok %v mal %v root eq %v idx %x standing %v hops %d", ok, mal, r == root, idx, standing, hops)
	}
	if _, _, _, _, ok, _ := decodeWantV2(encodeWant(root, 7)); ok {
		t.Fatalf("a v1 WANT decoded as v4")
	}
	if _, _, _, _, ok, mal := decodeWantV2(w[:len(w)-1]); !ok || !mal {
		t.Fatalf("a short WANT not malformed")
	}
	if _, _, _, _, ok, mal := decodeWantV2(encodeWantV2(root, 1, false, seacastMaxHops+1)); !ok || !mal {
		t.Fatalf("hops over the bound not malformed")
	}
	badW := []byte(w)
	badW[len(w)-2] |= 0x04 // 0x01 standing, 0x02 the probe; 0x04 is reserved
	if _, _, _, _, ok, mal := decodeWantV2(string(badW)); !ok || !mal {
		t.Fatalf("a reserved WANT flag not malformed")
	}
	for _, mid := range []string{haveMagic + "\x09" + root, wantMagic + "\x07" + root, haveMagic + "\x03" + root} { // v3, the claim, is an unknown version
		if !seacastPseudoUnknown(mid) {
			t.Fatalf("an unknown version not recognised as a pseudo id: %q", mid[:5])
		}
	}
	for _, mid := range []string{h, w, encodeHave(root, &bits), encodeWant(root, 1), "an ordinary id"} {
		if seacastPseudoUnknown(mid) {
			t.Fatalf("a known id taken for an unknown version")
		}
	}
	if r, ok := seacastHaveRoot(h); !ok || r != root {
		t.Fatalf("the coalescer's root of a v4 holding")
	}
}

// TestWantIndexLittleEndian is the WANT's index on the wire, from the spec's layouts built by hand:
// the v4 WANT is "WANT" ‖ 0x04 ‖ the group (32) ‖ the index (4) ‖ flags (1) ‖ hops (1), and "The
// index is a little-endian uint32" (seacast-bindings.md, the extra fields); the v1 WANT is
// seacast-lite.md §2's WantIndex, "WANT" ‖ 0x01 ‖ the group ‖ the index, whose "index [is] a uint32 in
// little-endian byte order". Both encoders write it so, and both decoders read it back; each of the
// four is checked on its own.
func TestWantIndexLittleEndian(t *testing.T) {
	root := strings.Repeat("r", haveRootLen)
	index := []byte{0x04, 0x03, 0x02, 0x01}                        // 0x01020304, least significant byte first
	v4 := "WANT" + "\x04" + root + string(index) + "\x01" + "\x02" // standing, two hops
	v1 := "WANT" + "\x01" + root + string(index)

	if got := encodeWantV2(root, 0x01020304, true, 2); got != v4 {
		t.Errorf("v4 WANT encoded\n got %x\nwant %x", got, v4)
	}
	if r, idx, standing, hops, ok, mal := decodeWantV2(v4); !ok || mal || r != root || idx != 0x01020304 || !standing || hops != 2 {
		t.Errorf("v4 WANT decoded: ok %v mal %v root eq %v idx %#x standing %v hops %d", ok, mal, r == root, idx, standing, hops)
	}
	if got := encodeWant(root, 0x01020304); got != v1 {
		t.Errorf("v1 WANT encoded\n got %x\nwant %x", got, v1)
	}
	if r, idx, ok := decodeWant(v1); !ok || r != root || idx != 0x01020304 {
		t.Errorf("v1 WANT decoded: ok %v root eq %v idx %#x", ok, r == root, idx)
	}
}

// TestSeacastAbsorbMixed is the mixed-router contract at the absorb: on one router with a type
// owning one group and the phase path another, an IDONTWANT list carrying a v1 holding, a v3
// claim, a v4 holding for the path's group, a v4 holding for the phase path's group, and an
// unknown version is absorbed entry by entry: the v1 and v3 as today, the v4 with its flags, the
// v4 off the path refused, the unknown ignored, and the counters say so.
func TestSeacastAbsorbMixed(t *testing.T) {
	v2root := strings.Repeat("a", haveRootLen)
	payroot := strings.Repeat("b", haveRootLen)
	index := func(mid string) (string, uint32, bool) {
		if len(mid) < haveRootLen+1 {
			return "", 0, false
		}
		return mid[:haveRootLen], uint32(mid[haveRootLen]), true
	}
	ctr := &SeacastCounters{}
	T := &testObjectType{name: "col", params: TypeParams{RNode: 2}, topics: []string{"seg/col/0"}}
	gs := &GossipSubRouter{peers: map[peer.ID]protocol.ID{}, unwanted: map[peer.ID]map[checksum]int{}}
	gs.have = newHaveState(index)
	gs.seacast = &seacast{types: map[string]ObjectType{"seg/col/0": T}, ctr: ctr}
	gs.seacastNoteRoot("seg/col/0", v2root+"\x00")
	gs.seacastNoteOffRoot("seg/payload", payroot+"\x00")
	p := peer.ID("p")

	var b1, b4 haveBits
	b1.Set(1)
	b4.Set(2)
	b4.Set(3)
	entries := []string{
		encodeHave(v2root, &b1),               // v1 on the path's root: absorbed as today
		haveMagic + "\x03" + payroot + "\x02", // v3, the claim this binding never writes: an unknown version
		encodeHaveV2(v2root, &b4, true, true), // v4 on the path's root: the flags
		encodeHaveV2(payroot, &b4, false, false),
		haveMagic + "\x09" + v2root + "\x01", // an unknown version
	}
	for _, mid := range entries {
		if !gs.haveAbsorb(p, mid) {
			t.Fatalf("an entry fell through as an ordinary id: %q", mid[:5])
		}
	}
	e := gs.have.peers[p][seacastGID(v2root)]
	if e == nil || !e.Bits.Has(1) || !e.Bits.Has(2) || !e.Bits.Has(3) || !e.ClaimAll || !e.Complete {
		t.Fatalf("the path's root: bits or flags not absorbed: %+v", e)
	}
	pe := gs.have.peers[p][seacastGID(payroot)]
	if pe != nil && (pe.Bits.Count() != 0 || pe.ClaimAll) {
		t.Fatalf("the phase path's root: the v3 or the v4 absorbed: %+v", pe)
	}
	if ctr.HoldingsV4.Load() != 1 || ctr.OffPath.Load() != 1 || ctr.Malformed.Load() != 2 {
		t.Fatalf("counters: v4 %d off-path %d malformed %d, want 1 1 2", ctr.HoldingsV4.Load(), ctr.OffPath.Load(), ctr.Malformed.Load())
	}
	// A v4 WANT resolves as the indexed ask; a malformed one and one off the path drop.
	if got, standing, _, drop := gs.seacastWantIn(encodeWantV2(v2root, 3, true, 0)); drop || got != encodeWant(v2root, 3) || !standing {
		t.Fatalf("a v4 WANT on the path not resolved to its indexed ask with its flag kept")
	}
	if _, _, _, drop := gs.seacastWantIn(encodeWantV2(payroot, 3, false, 0)); !drop {
		t.Fatalf("a v4 WANT for the phase path's root not dropped")
	}
	if _, _, _, drop := gs.seacastWantIn(wantMagic + "\x07" + v2root); !drop {
		t.Fatalf("an unknown WANT version not dropped")
	}
	if got, standing, _, drop := gs.seacastWantIn(encodeWant(v2root, 3)); drop || got != encodeWant(v2root, 3) || standing {
		t.Fatalf("a v1 WANT changed")
	}
	if _, _, _, drop := gs.seacastWantIn(encodeWantV2(v2root, 3, false, 1)); !drop {
		t.Fatalf("a v4 WANT with hops not dropped: the port's types take no routed request")
	}
	// The counters run on from the holdings: malformed 2 there (the v3 and the unknown version), 2 here.
	if ctr.WantsV4.Load() != 1 || ctr.OffPath.Load() != 2 || ctr.Malformed.Load() != 4 {
		t.Fatalf("WANT counters: v4 %d off-path %d malformed %d, want 1 2 4", ctr.WantsV4.Load(), ctr.OffPath.Load(), ctr.Malformed.Load())
	}
}

// TestSeacastMalformedControls is the receiver's rule for the path's controls through the real
// handlers on two routers (a codec-malformed entry is ignored, counted, and the
// rest of its RPC is kept). A sends B an IDONTWANT carrying an id shorter than the magic and one
// exactly the magic (ordinary ids, no panic), a v4 holding too short for its header, a v4 and a v1
// holding naming a bit beyond the type's IndexCount, a valid v4 holding and an ordinary id; then an
// IHAVE carrying a v4 holding, an unknown-version pseudo id and an ordinary id; then an IWANT
// carrying a v4 WANT with hops (the port's types take no routed request) and a plain v4 WANT. B's ledger, unwanted
// set, announced holders and counters say what each entry became.
func TestSeacastMalformedControls(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		root := strings.Repeat("m", haveRootLen)
		topic := "seg/col/0"
		hosts := getDefaultHosts(t, 2)
		params := DefaultGossipSubParams()
		params.IDontWantMessageThreshold = 1
		wantsB := &wantsSent{seq: map[peer.ID][]string{}}
		psubs := getGossipsubsOptFn(ctx, hosts, func(i int, h host.Host) []Option {
			T := &testObjectType{name: "col", params: TypeParams{RNode: 2}, topics: []string{topic}}
			opts := []Option{
				WithGossipSubParams(params),
				WithMessageIdFn(seacastTestID),
				WithHaveBitmaps(seacastTestIndex),
				WithPhaseForwarding(func(t string) bool { return t != topic }, 2),
				WithSeacast([]ObjectType{T}, nil),
			}
			if i == 1 {
				opts = append(opts, WithRawTracer(wantsB)) // what B asks, as it queues it
			}
			return opts
		})
		// A hand-sends controls (a holding of an index it does not hold among them) to test B's parsing, an
		// act the ordering checks count on purpose: every other honest count is asserted 0.
		seacastOrdZeroAtEnd(t, psubs, ordHoldingUnheld)
		for _, ps := range psubs {
			if _, err := ps.Subscribe(topic); err != nil {
				t.Fatal(err)
			}
		}
		connect(t, hosts[0], hosts[1])
		time.Sleep(2 * time.Second)
		a, b := psubs[0], psubs[1]
		gsA, gsB := a.rt.(*GossipSubRouter), b.rt.(*GossipSubRouter)
		pA := hosts[0].ID()
		read := func(gs *GossipSubRouter, f func()) {
			done := make(chan struct{})
			gs.p.eval <- func() { f(); close(done) }
			<-done
		}
		cell := func(index byte) []byte { return append([]byte(root+string(index)), []byte(strings.Repeat("x", 64))...) }
		// B learns the group's type (and IndexCount 32) from the first cell.
		if err := a.Publish(topic, cell(0)); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Second)
		ctrB := gsB.seacast.ctr
		base := ctrB.Malformed.Load()
		var unwantedBefore int
		read(gsB, func() { unwantedBefore = len(gsB.unwanted[pA]) })
		send := func(ctl *pb.ControlMessage) {
			read(gsA, func() { gsA.sendRPC(hosts[1].ID(), &RPC{RPC: pb.RPC{Control: ctl}}, true) })
			time.Sleep(time.Second)
		}

		// The IDONTWANT.
		var beyond, two haveBits
		beyond.Set(32)
		two.Set(1)
		two.Set(2)
		ordinary := root + "\x09" // an id of the group B does not hold
		send(&pb.ControlMessage{Idontwant: []*pb.ControlIDontWant{{MessageIDs: []string{
			"HAV", haveMagic, haveMagic + "\x04short",
			encodeHaveV2(root, &beyond, false, false), encodeHave(root, &beyond),
			encodeHaveV2(root, &two, false, false), ordinary}}}})
		if n := ctrB.Malformed.Load() - base; n != 3 {
			t.Fatalf("malformed after the IDONTWANT: %d, want 3 (a short v4, a v4 and a v1 bit beyond IndexCount)", n)
		}
		read(gsB, func() {
			e := gsB.have.peers[pA][seacastGID(root)]
			if e == nil || !e.Bits.Has(1) || !e.Bits.Has(2) || e.Bits.Has(32) {
				t.Errorf("B's record of A's holding: %+v, want bits 1 and 2 and not 32", e)
			}
			if n := len(gsB.unwanted[pA]) - unwantedBefore; n != 3 {
				t.Errorf("B's unwanted set of A grew by %d, want 3 (the two short ordinary ids and the ordinary one)", n)
			}
		})

		// The IHAVE: only the ordinary id is an announce, and B asks A for it. On the path an announce
		// records no id (seacast-lite.md §4.4). A pseudo id taken for an announce would be asked too,
		// and A counts an ask by an unknown pseudo id as malformed.
		unknown := haveMagic + "\x09" + root + "\x01"
		v4 := encodeHaveV2(root, &two, false, false)
		malformedA := gsA.seacast.ctr.Malformed.Load()
		send(&pb.ControlMessage{Ihave: []*pb.ControlIHave{{TopicID: &topic, MessageIDs: []string{v4, unknown, ordinary}}}})
		if n := ctrB.Malformed.Load() - base; n != 5 {
			t.Fatalf("malformed after the IHAVE: %d, want 5 (a v4 holding and an unknown version in an offer)", n)
		}
		read(gsB, func() {
			if _, asked := gsB.seacast.asked[root][9][pA]; !asked {
				t.Errorf("B did not take the ordinary id's announce: no ask of index 9 to A")
			}
			if m := gsB.have.mids[root]; m != nil && m.ids[9] != "" {
				t.Errorf("B recorded %q for index 9 from the announce, want none", m.ids[9])
			}
		})
		if n := gsA.seacast.ctr.Malformed.Load() - malformedA; n != 0 {
			t.Fatalf("A counted %d malformed asks after B's reply to the IHAVE, want 0: a pseudo id in an offer was taken as an announce", n)
		}
		// A counts an ask by an unknown version, not one by a v4 holding: what B queued to A says it.
		asked := wantsB.of(pA)
		if len(asked) == 0 {
			t.Fatal("B queued no ask to A after the IHAVE; want the ordinary id's")
		}
		for _, id := range asked {
			if strings.HasPrefix(id, haveMagic) {
				t.Fatalf("B asked A for a holding's pseudo id %x from the IHAVE; want only the ordinary id's ask", id)
			}
		}

		// The IWANT: a WANT with hops is malformed (no routed request); the plain v4 WANT resolves.
		// Of a cell A does not hold: A's writer drops its own asks for cells it holds, v4's as v1's
		// (seacast-lite.md §7.2's filter_entry).
		send(&pb.ControlMessage{Iwant: []*pb.ControlIWant{{MessageIDs: []string{encodeWantV2(root, 3, false, 1), encodeWantV2(root, 3, false, 0)}}}})
		if n := ctrB.Malformed.Load() - base; n != 6 || ctrB.WantsV4.Load() != 1 {
			t.Fatalf("after the IWANT: malformed %d (want 6), v4 WANTs %d (want 1)", n, ctrB.WantsV4.Load())
		}
	})
}

// TestSeacastHoldingWriters is the flags' and the writers' contract on a hand-built router with a
// type owning one group and the phase path another: a later holding without the flags never
// retracts them ([c10], draft §4.3); the queue's attachment writes a pend on the path's root as a
// v4 holding with its flags (the whole bitmap held: claim_all and complete) and a pend on the phase
// path's root as v1; the origin holding's encoder (haveOrigin's) does the same by root.
func TestSeacastHoldingWriters(t *testing.T) {
	root := strings.Repeat("w", haveRootLen)
	payroot := strings.Repeat("p", haveRootLen)
	index := func(mid string) (string, uint32, bool) {
		if len(mid) < haveRootLen+1 {
			return "", 0, false
		}
		return mid[:haveRootLen], uint32(mid[haveRootLen]), true
	}
	T := &testObjectType{name: "col", params: TypeParams{RNode: 2}, topics: []string{"seg/col/0"}}
	gs := &GossipSubRouter{peers: map[peer.ID]protocol.ID{}, unwanted: map[peer.ID]map[checksum]int{}}
	gs.have = newHaveState(index)
	gs.seacast = &seacast{types: map[string]ObjectType{"seg/col/0": T}, ctr: &SeacastCounters{}}
	gs.seacastNoteRoot("seg/col/0", root+"\x00")
	gs.seacastNoteOffRoot("seg/payload", payroot+"\x00")
	p := peer.ID("p")

	// The flags are never retracted.
	var one haveBits
	one.Set(1)
	gs.haveAbsorb(p, encodeHaveV2(root, &one, true, true))
	gs.haveAbsorb(p, encodeHaveV2(root, &one, false, false))
	if e := gs.have.peers[p][seacastGID(root)]; e == nil || !e.ClaimAll || !e.Complete {
		t.Fatalf("a later holding without the flags retracted them: %+v", e)
	}

	// The queue's attachment: the encoding is decided at the pend by root, written out by attach.
	var all haveBits
	for i := 0; i < 32; i++ {
		all.Set(uint32(i))
	}
	q := &queueHave{attachOn: true, pending: map[string]*pendHave{}}
	for _, c := range []struct {
		root string
		bits *haveBits
	}{{root, &all}, {payroot, &one}} {
		v4, flags := gs.seacastHaveFlags(c.root, c.bits, false)
		q.pending[c.root] = &pendHave{bits: c.bits, v4: v4, flags: flags}
	}
	out := q.attach(&RPC{})
	if out == nil || out.Control == nil || len(out.Control.Idontwant) != 1 || len(out.Control.Idontwant[0].MessageIDs) != 2 {
		t.Fatalf("the attachment did not carry the two holdings: %+v", out)
	}
	ids := out.Control.Idontwant[0].MessageIDs // sorted by root: the phase path's first
	if r, enc, ok := decodeHave(ids[0]); !ok || r != payroot || len(enc) != 1 {
		t.Fatalf("the phase path's pend did not leave as v1: ok %v root eq %v", ok, r == payroot)
	}
	if r, ca, co, enc, ok, mal := decodeHaveV2(ids[1]); !ok || mal || r != root || !ca || !co || len(enc) != 4 {
		t.Fatalf("the path's pend did not leave as v4 with the flags: ok %v mal %v claimAll %v complete %v enc %x", ok, mal, ca, co, enc)
	}
	if len(q.pending) != 0 {
		t.Fatalf("the pends were not cleared by the attachment")
	}

	// The origin holding's encoder, by root.
	if _, _, _, _, ok, _ := decodeHaveV2(gs.seacastEncodeHave(root, &one, false)); !ok {
		t.Fatalf("the origin holding on the path's root is not v4")
	}
	if _, _, ok := decodeHave(gs.seacastEncodeHave(payroot, &one, false)); !ok {
		t.Fatalf("the origin holding on the phase path's root is not v1")
	}
}

// TestSeacastWireCore is the codec to the core's messages, both ways against today's strings: a payload group's root is
// GroupId(0, root, 0); a holding of either version, an offer, a v4 WANT with each flag and a v1
// WANT read as the core's messages and, written back, give the strings today's encoders write; what
// the binding ignores reads as malformed; an ordinary id is no message; a chunk's message comes
// back as its Ref.
func TestSeacastWireCore(t *testing.T) {
	root := strings.Repeat("k", haveRootLen)
	g, ok := seacastGroupID(root)
	if !ok || string(g) != "\x00"+root+"\x00\x00" {
		t.Fatalf("the payload's GroupId(0, root, 0): %q", g)
	}
	if back, ok := seacastRootOf(g); !ok || back != root {
		t.Fatalf("the root back from the GroupID")
	}
	var key [32]byte
	if _, ok := seacastRootOf(core.NewGroupID(core.TypeColumn, key, 3)); ok {
		t.Fatalf("a column's group mapped: the strings for cell groups are open (bindings §4)")
	}
	var bits haveBits
	bits.Set(0)
	bits.Set(5)
	bits.Set(17)

	// Holdings, version 4, every flag pair: read, then written back byte for byte.
	for _, f := range []struct{ claimAll, complete bool }{{false, false}, {true, false}, {false, true}, {true, true}} {
		mid := encodeHaveV2(root, &bits, f.claimAll, f.complete)
		h, ok, mal := seacastHaveOf(mid, false)
		if !ok || mal || h.Group != g || h.Offer || h.ClaimAll != f.claimAll || h.Complete != f.complete ||
			!slices.Equal(h.Bits.Indices(), []uint32{0, 5, 17}) {
			t.Fatalf("v4 holding %+v read as %+v (ok %v, malformed %v)", f, h, ok, mal)
		}
		if back, ok := seacastHaveID(h); !ok || back != mid {
			t.Fatalf("v4 holding %+v written back as %q, want %q", f, back, mid)
		}
	}
	// A holding of version 1 reads as a holding without flags; the codec writes holdings as
	// version 4 (seacastEncodeHave on the path's roots), so it comes back as that.
	v1 := encodeHave(root, &bits)
	h, ok, mal := seacastHaveOf(v1, false)
	if !ok || mal || h.Offer || h.ClaimAll || h.Complete || !slices.Equal(h.Bits.Indices(), []uint32{0, 5, 17}) {
		t.Fatalf("v1 holding read as %+v", h)
	}
	if back, _ := seacastHaveID(h); back != encodeHaveV2(root, &bits, false, false) {
		t.Fatalf("v1 holding not written back as v4 without flags")
	}
	// An offer: version 1 in an IHAVE, both ways.
	o, ok, mal := seacastHaveOf(v1, true)
	if !ok || mal || !o.Offer || o.Group != g {
		t.Fatalf("v1 offer read as %+v", o)
	}
	if back, ok := seacastHaveID(o); !ok || back != v1 {
		t.Fatalf("offer written back as %q, want today's v1", back)
	}
	if _, ok := seacastHaveID(core.Have{Group: g, Bits: core.Bits{1}, Offer: true, ClaimAll: true}); ok {
		t.Fatalf("an offer with a holding's flag written")
	}
	// What the binding ignores: a v4 holding in an IHAVE, a reserved flag bit, an unknown version.
	badFlag := []byte(encodeHaveV2(root, &bits, false, false))
	badFlag[haveHeaderLen] |= 0x04
	for _, c := range []struct {
		mid   string
		offer bool
	}{{encodeHaveV2(root, &bits, true, false), true}, {string(badFlag), false}, {haveMagic + "\x03" + root, false}} {
		if _, ok, mal := seacastHaveOf(c.mid, c.offer); !ok || !mal {
			t.Fatalf("an entry the binding ignores read as a message: %q (offer %v)", c.mid[:5], c.offer)
		}
	}
	if _, ok, _ := seacastHaveOf(encodeHave(root, &bits)+strings.Repeat("\xff", haveMaxBytes), false); ok {
		t.Fatalf("a v1 holding over the cap read as a HAVE, not as the opaque id the binding reads")
	}
	if _, ok, _ := seacastHaveOf("an ordinary id", false); ok {
		t.Fatalf("an ordinary id read as a HAVE")
	}
	if _, ok, _ := seacastHaveOf(wantMagic+"\x07"+root, false); ok {
		t.Fatalf("a WANT of an unknown version read as a HAVE")
	}

	// WANTs, version 4: each flag, and hops; read, then written back.
	for _, mid := range []string{
		encodeWantV2(root, 9, false, 0),
		encodeWantV2(root, 9, true, 0),
		encodeWantV2(root, 9, true, 2),
		encodeWantV2Probe(root, 9),
	} {
		w, ok, mal := seacastWantOf(mid)
		if !ok || mal || w.Group != g || !slices.Equal(w.Bits.Indices(), []uint32{9}) {
			t.Fatalf("v4 WANT read as %+v", w)
		}
		if back, ok := seacastWantIDs(w); !ok || len(back) != 1 || back[0] != mid {
			t.Fatalf("v4 WANT %+v written back as %q, want %q", w, back, mid)
		}
	}
	if w, _, _ := seacastWantOf(encodeWantV2Probe(root, 9)); !w.Probe || w.Standing || w.Hops != 0 {
		t.Fatalf("the probe's WANT read as %+v", w)
	}
	// A WANT of several indices is one id per index, in order.
	var many core.Want
	many.Group, many.Standing = g, true
	for _, i := range []uint32{12, 3, 40} {
		many.Bits.Set(i)
	}
	ids, ok := seacastWantIDs(many)
	if !ok || !slices.Equal(ids, []string{encodeWantV2(root, 3, true, 0), encodeWantV2(root, 12, true, 0), encodeWantV2(root, 40, true, 0)}) {
		t.Fatalf("a WANT of three indices: %q", ids)
	}
	// Version 1, the plain ask by index, reads as a WANT without fields.
	if w, ok, mal := seacastWantOf(encodeWant(root, 7)); !ok || mal || w.Standing || w.Probe || w.Hops != 0 ||
		!slices.Equal(w.Bits.Indices(), []uint32{7}) {
		t.Fatalf("v1 WANT read as %+v", w)
	}
	badW := []byte(encodeWantV2(root, 1, false, 0))
	badW[len(badW)-2] |= 0x04
	for _, mid := range []string{string(badW), encodeWantV2(root, 1, false, seacastMaxHops+1), wantMagic + "\x07" + root,
		encodeWantV2(root, core.MaxHaveBytes*8, false, 0), encodeWant(root, 1<<31)} {
		if _, ok, mal := seacastWantOf(mid); !ok || !mal {
			t.Fatalf("a WANT the binding ignores read as a message: %x", mid)
		}
	}
	if _, ok, _ := seacastWantOf(encodeWant(root, 7) + "x"); ok {
		t.Fatalf("a v1 WANT of the wrong length read as a WANT, not as the opaque id the binding reads")
	}
	if _, ok, _ := seacastWantOf(haveMagic + "\x09" + root); ok {
		t.Fatalf("a HAVE of an unknown version read as a WANT")
	}
	if _, ok := seacastWantIDs(core.Want{Group: g}); ok {
		t.Fatalf("a WANT of no index written")
	}

	// A chunk: its group and index from the structured id, its bytes, and its message as Ref.
	gs := &GossipSubRouter{p: &PubSub{idGen: newMsgIdGenerator()}}
	gs.p.idGen.Default = func(m *pb.Message) string { return string(m.Data[:haveRootLen+1]) }
	gs.have = newHaveState(func(mid string) (string, uint32, bool) {
		if len(mid) <= haveRootLen {
			return "", 0, false
		}
		return mid[:haveRootLen], uint32(mid[haveRootLen]), true
	})
	msg := &Message{Message: &pb.Message{Data: []byte(root + "\x06payload bytes")}}
	c, ok := gs.seacastChunkOf(msg)
	if !ok || c.Group != g || c.Index != 6 || string(c.Data) != root+"\x06payload bytes" {
		t.Fatalf("the chunk read as %+v", c)
	}
	if m, ok := seacastMessageOf(c); !ok || m != msg {
		t.Fatalf("the chunk's message not its Ref")
	}
	if _, ok := seacastMessageOf(core.Chunk{Group: g, Index: 6}); ok {
		t.Fatalf("a rebuilt chunk (no Ref) gave a message")
	}
}

// wantsSent records the ids of the IWANTs a router queues to each peer, in order.
type wantsSent struct {
	mockRawTracer
	mu  sync.Mutex
	seq map[peer.ID][]string
}

func (w *wantsSent) SendRPC(rpc *RPC, p peer.ID) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, iw := range rpc.GetControl().GetIwant() {
		w.seq[p] = append(w.seq[p], iw.GetMessageIDs()...)
	}
}

// of is what was asked of p so far.
func (w *wantsSent) of(p peer.ID) []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return slices.Clone(w.seq[p])
}

// TestSeacastHoldingPaddingTrimmed: a holding padded with trailing zero bytes, of either version,
// is kept as its bits alone. The peer's entry and the writer's copy of it grow no further than its
// highest index, as for the same holding unpadded.
func TestSeacastHoldingPaddingTrimmed(t *testing.T) {
	root1, root4 := strings.Repeat("a", haveRootLen), strings.Repeat("b", haveRootLen)
	p := peer.ID("p")
	gs := testHaveRouter()
	gs.seacast = &seacast{ctr: &SeacastCounters{}, expired: map[string]struct{}{}}
	q := newRpcQueue(8)
	gs.p = &PubSub{peers: map[peer.ID]*rpcQueue{p: q}}
	var b3 haveBits
	b3.Set(3)
	pad := strings.Repeat("\x00", 1000)
	if !gs.haveAbsorb(p, encodeHave(root1, &b3)+pad) || !gs.haveAbsorb(p, encodeHaveV2(root4, &b3, false, false)+pad) {
		t.Fatal("a padded holding was not handled")
	}
	for _, root := range []string{root1, root4} {
		e := gs.have.peers.Of(p, seacastGID(root))
		if e == nil || len(e.Bits) != 1 || !slices.Equal(e.Bits.Indices(), []uint32{3}) {
			if e == nil {
				t.Fatalf("no entry of %q for the peer", root[:1])
			}
			t.Fatalf("the peer's entry of %q: indices %v in %d bytes; want index 3 in one byte", root[:1], e.Bits.Indices(), len(e.Bits))
		}
		q.queueMu.Lock()
		theirs := q.have.theirs[root]
		q.queueMu.Unlock()
		if theirs == nil || len(*theirs) != 1 || !theirs.Has(3) {
			if theirs == nil {
				t.Fatalf("no writer's copy of %q", root[:1])
			}
			t.Fatalf("the writer's copy of %q: indices %v in %d bytes; want index 3 in one byte", root[:1], theirs.Indices(), len(*theirs))
		}
	}
}

// TestSeacastV4HoldingOfExpiredGroup is seacast-lite.md §4.3's on_have for a holding of either
// version: one for an expired group is ignored. A v4 holding then makes no entry, sets no claim on
// an entry the peer already has, leaves that entry's bits and lifetime, counts no holder, is not
// counted as absorbed, and its claim is not noted. A live group's v4 holding, the control, is
// absorbed with its two claims, its holder and its lifetime, and its claim is noted.
func TestSeacastV4HoldingOfExpiredGroup(t *testing.T) {
	dead, held, live := strings.Repeat("d", haveRootLen), strings.Repeat("h", haveRootLen), strings.Repeat("l", haveRootLen)
	p := peer.ID("p")
	gs := testHaveRouter()
	gs.have.holders = &holderCounts{}
	ctr := &SeacastCounters{}
	gs.seacast = &seacast{ctr: ctr, machine: true, expired: map[string]struct{}{}}
	var b3, b5 haveBits
	b3.Set(3)
	b5.Set(5)

	// p's holding of held, made while its group was live.
	gs.haveAbsorb(p, encodeHave(held, &b3))
	gs.have.peers.Of(p, seacastGID(held)).TTL = 1
	gs.seacast.expired[dead] = struct{}{}
	gs.seacast.expired[held] = struct{}{}

	if !gs.haveAbsorb(p, encodeHave(dead, &b5)) || gs.have.peers.Of(p, seacastGID(dead)) != nil {
		t.Fatal("a v1 holding of an expired group made the peer's entry")
	}
	for _, root := range []string{dead, held, live} {
		if !gs.haveAbsorb(p, encodeHaveV2(root, &b5, true, true)) {
			t.Fatal("a v4 holding was not handled")
		}
	}
	if gs.have.peers.Of(p, seacastGID(dead)) != nil {
		t.Fatal("a v4 holding of an expired group made the peer's entry")
	}
	if e := gs.have.peers.Of(p, seacastGID(held)); e == nil || e.ClaimAll || e.Complete || e.TTL != 1 ||
		!slices.Equal(e.Bits.Indices(), []uint32{3}) {
		t.Fatalf("a v4 holding of an expired group changed the peer's entry: %+v", e)
	}
	for _, root := range []string{dead, held} {
		if c := gs.have.peers.Claimants(seacastGID(root)); len(c) != 0 {
			t.Fatalf("claimants of an expired group %v, want none", c)
		}
		if n := gs.have.holders.get(root, 5); n != 0 {
			t.Fatalf("an expired group's index 5 has %d holders, want 0", n)
		}
	}

	e := gs.have.peers.Of(p, seacastGID(live))
	if e == nil || !e.ClaimAll || !e.Complete || e.TTL != gs.params.IDontWantMessageTTL ||
		!slices.Equal(e.Bits.Indices(), []uint32{5}) {
		t.Fatalf("a live group's v4 holding: entry %+v, want index 5 claimed whole and complete, TTL %d",
			e, gs.params.IDontWantMessageTTL)
	}
	if c := gs.have.peers.Claimants(seacastGID(live)); !slices.Equal(c, []peer.ID{p}) {
		t.Fatalf("claimants of the live group %v, want [p]", c)
	}
	if n := gs.have.holders.get(live, 5); n != 1 {
		t.Fatalf("the live group's index 5 has %d holders, want 1", n)
	}
	if n := ctr.HoldingsV4.Load(); n != 1 {
		t.Fatalf("v4 holdings absorbed %d, want 1 (the live group's)", n)
	}
	if n := ctr.ClaimsNoted.Load(); n != 1 {
		t.Fatalf("claims noted %d, want 1 (the live group's)", n)
	}
}
