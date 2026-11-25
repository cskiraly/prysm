package pubsub

import (
	"context"
	"strings"
	"testing"
	"time"

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
	e := gs.have.peers[p][v2root]
	if e == nil || !e.bits.Has(1) || !e.bits.Has(2) || !e.bits.Has(3) || !e.claimAll || !e.complete {
		t.Fatalf("the path's root: bits or flags not absorbed: %+v", e)
	}
	pe := gs.have.peers[p][payroot]
	if pe != nil && (pe.bits.Count() != 0 || pe.claimAll) {
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
		psubs := getGossipsubsOptFn(ctx, hosts, func(i int, h host.Host) []Option {
			T := &testObjectType{name: "col", params: TypeParams{RNode: 2}, topics: []string{topic}}
			return []Option{
				WithGossipSubParams(params),
				WithMessageIdFn(seacastTestID),
				WithHaveBitmaps(seacastTestIndex),
				WithPhaseForwarding(func(t string) bool { return t != topic }, 2),
				WithSeacast([]ObjectType{T}, nil),
			}
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
			e := gsB.have.peers[pA][root]
			if e == nil || !e.bits.Has(1) || !e.bits.Has(2) || e.bits.Has(32) {
				t.Errorf("B's record of A's holding: %+v, want bits 1 and 2 and not 32", e)
			}
			if n := len(gsB.unwanted[pA]) - unwantedBefore; n != 3 {
				t.Errorf("B's unwanted set of A grew by %d, want 3 (the two short ordinary ids and the ordinary one)", n)
			}
		})

		// The IHAVE: only the ordinary id is an announce. An announce learns its id under its root
		// (haveLearnID); a pseudo id taken for one would learn a garbage root from its first bytes.
		unknown := haveMagic + "\x09" + root + "\x01"
		v4 := encodeHaveV2(root, &two, false, false)
		var rootsBefore int
		read(gsB, func() { rootsBefore = len(gsB.have.mids) })
		send(&pb.ControlMessage{Ihave: []*pb.ControlIHave{{TopicID: &topic, MessageIDs: []string{v4, unknown, ordinary}}}})
		if n := ctrB.Malformed.Load() - base; n != 5 {
			t.Fatalf("malformed after the IHAVE: %d, want 5 (a v4 holding and an unknown version in an offer)", n)
		}
		read(gsB, func() {
			if m := gsB.have.mids[root]; m == nil || m.ids[9] != ordinary {
				t.Errorf("B did not take the ordinary id's announce (learned ids of the group: %v)", m)
			}
			if n := len(gsB.have.mids); n != rootsBefore {
				t.Errorf("B learned %d roots from the IHAVE, want 0: a pseudo id in an offer was taken as an announce", n-rootsBefore)
			}
		})

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
	if e := gs.have.peers[p][root]; e == nil || !e.claimAll || !e.complete {
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
