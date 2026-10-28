package pubsub

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
)

// indicesOf is the frame indices of root that rpc's IDONTWANT and IHAVE bitmaps name, per side.
func indicesOf(rpc *RPC, root string) (idw, ihave []uint32) {
	read := func(ids []string, into *[]uint32) {
		for _, mid := range ids {
			if r, _, enc, ok := decodeHaveLoad(mid); ok && r == root && !isHaveClaim(mid) {
				var b haveBits
				b.Or(enc)
				for i := uint32(0); i < uint32(len(b.b))*8; i++ {
					if b.Has(i) {
						*into = append(*into, i)
					}
				}
			}
		}
	}
	for _, e := range rpc.GetControl().GetIdontwant() {
		read(e.GetMessageIDs(), &idw)
	}
	for _, e := range rpc.GetControl().GetIhave() {
		read(e.GetMessageIDs(), &ihave)
	}
	return idw, ihave
}

// TestLinkDisclosure is the per-link disclosure's unit known answer: each link hears its exchanged
// frames and the budget's first held frames, the IHAVE side draws from the same set, nothing
// unheld is named, an ask for an undisclosed frame is refused, and completion lifts the limit.
func TestLinkDisclosure(t *testing.T) {
	root := strings.Repeat("d", 32)
	held := func(n int) *haveEntry {
		e := &haveEntry{}
		for i := 0; i < n; i++ {
			e.bits.Set(uint32(i)) // lint:ignore uintcast -- a test index below 8.
		}
		return e
	}
	bitmap := func(idx ...uint32) *haveBits {
		var b haveBits
		for _, i := range idx {
			b.Set(i)
		}
		return &b
	}
	idw := func(ids ...string) *RPC {
		return rpcWithControl(nil, nil, []*pb.ControlIWant{{MessageIDs: []string{"want"}}}, nil, nil, []*pb.ControlIDontWant{{MessageIDs: ids}})
	}
	topic := "t"
	ihave := func(ids ...string) *RPC {
		return rpcWithControl(nil, []*pb.ControlIHave{{TopicID: &topic, MessageIDs: ids}}, []*pb.ControlIWant{{MessageIDs: []string{"want"}}}, nil, nil, nil)
	}
	t.Run("requires the paced origin HAVE", func(t *testing.T) {
		if err := WithLinkDisclosure(3, 6, nil)(&PubSub{rt: testHaveRouter()}); err == nil {
			t.Fatal("link disclosure installed without WithOriginHavePaced")
		}
	})
	gs := testHaveRouter()
	gs.have.ihaveOn = true
	in := idw(encodeHave(root, bitmap(0, 1, 2)))
	if gs.applyDisclosure(in, "a") != in {
		t.Fatal("a router without link disclosure rewrote the RPC")
	}
	var st DisclosureStats
	ps := &PubSub{rt: gs}
	if err := WithOriginHavePaced()(ps); err != nil {
		t.Fatal(err)
	}
	if err := WithLinkDisclosure(3, 6, &st)(ps); err != nil {
		t.Fatal(err)
	}
	gs.have.mine[root] = held(5)
	all := encodeHave(root, &gs.have.mine[root].bits)
	gs.discloseTraded("a", testMid(root, 4))
	t.Run("each link hears its exchanged frames and the budget's first held frames", func(t *testing.T) {
		gotA, _ := indicesOf(gs.applyDisclosure(idw(all), "a"), root)
		gotB, _ := indicesOf(gs.applyDisclosure(idw(all), "b"), root)
		if fmt.Sprint(gotA) != "[0 1 4]" || fmt.Sprint(gotB) != "[0 1 2]" {
			t.Fatalf("a hears %v (want [0 1 4]), b hears %v (want [0 1 2])", gotA, gotB)
		}
		if len(in.GetControl().GetIdontwant()[0].GetMessageIDs()) != 1 {
			t.Fatal("the input RPC was mutated")
		}
	})
	t.Run("the IHAVE side draws from the same set", func(t *testing.T) {
		out := gs.applyDisclosure(ihave(encodeHave(root, bitmap(2, 3))), "a")
		if _, got := indicesOf(out, root); len(got) != 0 || len(out.GetControl().GetIhave()) != 0 || len(out.GetControl().GetIwant()) != 1 {
			t.Fatalf("a was announced %v (entries %d), want nothing and the IWANT kept", got, len(out.GetControl().GetIhave()))
		}
		out = gs.applyDisclosure(ihave(encodeHaveLoad(root, 7, bitmap(2, 3))), "b")
		_, got := indicesOf(out, root)
		_, load, _, _ := decodeHaveLoad(out.GetControl().GetIhave()[0].GetMessageIDs()[0])
		if fmt.Sprint(got) != "[2]" || load != 7 {
			t.Fatalf("b was announced %v with load %d, want [2] with load 7", got, load)
		}
	})
	t.Run("nothing unheld is named", func(t *testing.T) {
		got, _ := indicesOf(gs.applyDisclosure(idw(encodeHave(root, bitmap(0, 7))), "c"), root)
		if fmt.Sprint(got) != "[0]" {
			t.Fatalf("c hears %v, want [0]", got)
		}
		if out := gs.applyDisclosure(rpcWithControl(nil, nil, nil, nil, nil, []*pb.ControlIDontWant{{MessageIDs: []string{encodeHave(strings.Repeat("u", 32), bitmap(1))}}}), "c"); out != nil {
			t.Fatalf("a HAVE of a group not held went out: %v", out)
		}
	})
	t.Run("claims, stamps and plain ids pass; a per-id frame past the budget does not", func(t *testing.T) {
		claim := encodeHaveClaim(root, fullHaveBits(8))
		stamp := encodeHoseStamp(time.Now(), root, []uint32{3})
		out := gs.applyDisclosure(idw(claim, stamp, "plain", testMid(root, 3), testMid(root, 4)), "a")
		want := []string{claim, stamp, "plain", testMid(root, 4)}
		if got := out.GetControl().GetIdontwant()[0].GetMessageIDs(); fmt.Sprintf("%q", got) != fmt.Sprintf("%q", want) {
			t.Fatalf("a got %q, want %q", got, want)
		}
	})
	t.Run("an ask for an undisclosed frame is refused", func(t *testing.T) {
		if !gs.discloseRefuses("a", testMid(root, 2)) || gs.discloseRefuses("a", testMid(root, 0)) || gs.discloseRefuses("a", testMid(root, 4)) {
			t.Fatal("a: want frame 2 refused, 0 (disclosed) and 4 (exchanged) served")
		}
		if gs.discloseRefuses("b", "plain") || st.Refused.Load() != 1 {
			t.Fatalf("refused %d, want 1 and plain ids served", st.Refused.Load())
		}
	})
	t.Run("exchanged frames count against the budget, named or not", func(t *testing.T) {
		gs.discloseTraded("h", testMid(root, 3))
		gs.discloseTraded("h", testMid(root, 4))
		if got, _ := indicesOf(gs.applyDisclosure(idw(encodeHave(root, bitmap(0, 1, 2))), "h"), root); fmt.Sprint(got) != "[0]" {
			t.Fatalf("h hears %v, want [0]: its two exchanged frames leave room for one more", got)
		}
		delete(gs.have.disclose.links, "h")
	})
	t.Run("exchanged frames pass the budget", func(t *testing.T) {
		for _, i := range []byte{0, 1, 2, 3} {
			gs.discloseTraded("e", testMid(root, i))
		}
		if got, _ := indicesOf(gs.applyDisclosure(idw(all), "e"), root); fmt.Sprint(got) != "[0 1 2 3]" {
			t.Fatalf("e hears %v, want its four exchanged frames", got)
		}
	})
	t.Run("the parked HAVE is cut too", func(t *testing.T) {
		gs.have.attach = true
		defer func() { gs.have.attach = false }()
		gs.p = &PubSub{peers: map[peer.ID]*rpcQueue{"f": newRpcQueue(4)}}
		defer func() { gs.p = nil }()
		q := gs.p.peers["f"]
		q.enableHave(testQueueIndex, true, true)
		if err := q.Push(&RPC{RPC: pb.RPC{Publish: []*pb.Message{{Data: []byte("x")}}}}, false); err != nil {
			t.Fatal(err)
		}
		if !gs.havePendAll("f", []string{all}) {
			t.Fatal("the HAVE was not parked")
		}
		if got := q.have.pending[root]; got == nil || fmt.Sprint(got.Bytes()) != fmt.Sprint(bitmap(0, 1, 2).Bytes()) {
			t.Fatalf("parked %v, want f's budget: frames 0 1 2", got)
		}
	})
	t.Run("completion lifts the limit and counts the links", func(t *testing.T) {
		gs.have.mine[root] = held(6)
		got, _ := indicesOf(gs.applyDisclosure(idw(encodeHave(root, &gs.have.mine[root].bits)), "b"), root)
		if len(got) != 6 || gs.discloseRefuses("b", testMid(root, 5)) {
			t.Fatalf("b hears %v after completion, want all 6 and every ask served", got)
		}
		// a, b, c, e and f: the largest union is e's four exchanged frames, the one link over budget;
		// b and f were told the budget's three beyond their exchanges.
		if st.Completed.Load() != 1 || st.Links.Load() != 5 || st.MaxShown.Load() != 4 || st.OverBudget.Load() != 1 || st.MaxTraded.Load() != 4 || st.MaxBeyond.Load() != 3 {
			t.Fatalf("completed %d links %d max shown %d over budget %d max traded %d max beyond %d, want 1 5 4 1 4 3",
				st.Completed.Load(), st.Links.Load(), st.MaxShown.Load(), st.OverBudget.Load(), st.MaxTraded.Load(), st.MaxBeyond.Load())
		}
		if len(gs.have.disclose.links) != 0 {
			t.Fatalf("kept link state for %d peers after completion", len(gs.have.disclose.links))
		}
		gs.discloseTraded("a", testMid(root, 5))
		if len(gs.have.disclose.links) != 0 {
			t.Fatal("an exchange after completion made link state")
		}
	})
	t.Run("state ages out on the hold TTL", func(t *testing.T) {
		other := strings.Repeat("o", 32)
		gs.have.mine[other] = held(1)
		gs.discloseTraded("g", testMid(other, 0))
		for range gs.have.holdTTL {
			gs.haveHeartbeat()
		}
		if len(gs.have.disclose.links) != 0 || len(gs.have.disclose.done) != 0 {
			t.Fatalf("kept %d links and %d completions after the TTL", len(gs.have.disclose.links), len(gs.have.disclose.done))
		}
	})
}

// discloseView is the publisher's side of TestLinkDisclosureNetwork: per peer, what its HAVEs and
// IHAVEs named to the peer before it completed, and the frames it and the peer exchanged.
type discloseView struct {
	*mockRawTracer
	mu            sync.Mutex
	root          string
	need, idLen   int
	full          bool
	worst         int
	named, traded map[peer.ID]*haveBits
	final         map[peer.ID]int
}

func (v *discloseView) bits(m map[peer.ID]*haveBits, p peer.ID) *haveBits {
	if m[p] == nil {
		m[p] = &haveBits{}
	}
	return m[p]
}

func (v *discloseView) frames(rpc *RPC, p peer.ID) {
	for _, m := range rpc.GetPublish() {
		if _, idx, ok := testIndex(string(m.GetData()[:v.idLen])); ok {
			v.bits(v.traded, p).Set(idx)
		}
	}
}

func (v *discloseView) RecvRPC(rpc *RPC) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.frames(rpc, rpc.from)
}

func (v *discloseView) SendRPC(rpc *RPC, p peer.ID) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.frames(rpc, p)
	a, b := indicesOf(rpc, v.root)
	if len(a) >= v.need {
		v.full = true
	}
	if len(a) > v.final[p] {
		v.final[p] = len(a)
	}
	if v.full {
		return
	}
	named := v.bits(v.named, p)
	for _, idx := range append(a, b...) {
		named.Set(idx)
	}
	if n := named.AndNot(v.bits(v.traded, p)).Count(); n > v.worst {
		v.worst = n
	}
}

// TestLinkDisclosureNetwork runs a publisher that pushes each frame to one peer and announces it
// to the rest: before it completes, what it tells a peer of the group is at most the budget beyond
// the frames it and the peer exchanged, its announces wait for the pops, its HAVE names the whole
// group in the end, and every node receives the group.
func TestLinkDisclosureNetwork(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		root := strings.Repeat("n", 32)
		const frames, need, budget, peers = 8, 6, 1, 4
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		hosts := getDefaultHosts(t, peers)
		idLen := len(testMid(root, 0))
		view := &discloseView{mockRawTracer: &mockRawTracer{}, root: root, need: need, idLen: idLen,
			named: map[peer.ID]*haveBits{}, traded: map[peer.ID]*haveBits{}, final: map[peer.ID]int{}}
		var stats DisclosureStats
		psubs := getGossipsubsOptFn(ctx, hosts, func(i int, _ host.Host) []Option {
			opts := []Option{
				WithMessageSignaturePolicy(StrictNoSign), WithNoAuthor(),
				WithMessageIdFn(func(m *pb.Message) string { return string(m.Data[:idLen]) }),
				WithHaveBitmaps(testIndex), WithHaveAnnounceBitmaps(), WithOriginHavePaced(),
				WithLinkDisclosure(budget, need, &stats),
			}
			if i == 0 {
				// A slow writer spreads the pops, so HAVEs and announces go out before completion.
				opts = append(opts, WithPhaseForwarding(func(string) bool { return true }, 1), WithSendDelay(20*time.Millisecond), WithRawTracer(view))
			}
			return opts
		})
		var topics []*Topic
		var subs []*Subscription
		for _, ps := range psubs {
			tp, err := ps.Join("t")
			if err != nil {
				t.Fatal(err)
			}
			sub, err := tp.Subscribe()
			if err != nil {
				t.Fatal(err)
			}
			topics = append(topics, tp)
			subs = append(subs, sub)
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
		for i := 1; i < peers; i++ {
			seen := map[string]bool{}
			for len(seen) < frames {
				sctx, scancel := context.WithTimeout(ctx, 5*time.Second)
				m, err := subs[i].Next(sctx)
				scancel()
				if err != nil {
					t.Fatalf("peer %d received %d of %d frames: %v (refused %d deferred %d completed %d withheld %d)", i, len(seen), frames, err, stats.Refused.Load(), stats.Deferred.Load(), stats.Completed.Load(), stats.Withheld.Load())
				}
				seen[string(m.Data[:idLen])] = true
			}
		}
		time.Sleep(time.Second)
		view.mu.Lock()
		defer view.mu.Unlock()
		if !view.full {
			t.Fatal("the publisher never named the whole group")
		}
		if view.worst > budget {
			t.Fatalf("a peer was told %d frames beyond those exchanged before completion, budget %d", view.worst, budget)
		}
		named := 0
		for i := 1; i < peers; i++ {
			p := hosts[i].ID()
			named += view.bits(view.named, p).Count()
			if view.final[p] != frames {
				t.Fatalf("peer %d: the publisher's largest HAVE named %d of %d frames", i, view.final[p], frames)
			}
		}
		if named < peers-1 {
			t.Fatalf("%d frames named before completion over %d peers: the window the test reads is empty", named, peers-1)
		}
		if stats.Refused.Load() != 0 || stats.Deferred.Load() == 0 || stats.Completed.Load() != peers {
			t.Fatalf("refused %d (want 0), deferred %d (want > 0), completed %d (want %d)", stats.Refused.Load(), stats.Deferred.Load(), stats.Completed.Load(), peers)
		}
		cancel()
	})
}

// TestOriginDilute is origin dilution's unit known answer: relayed groups pass untouched, every
// link of a group we publish hears one admitted set, grown with time since publish and short of
// need until pace, plus its own exchanges, and completion at pace lifts the limit.
func TestOriginDilute(t *testing.T) {
	root := strings.Repeat("s", 32)
	relayed := strings.Repeat("r", 32)
	idw := func(ids ...string) *RPC {
		return rpcWithControl(nil, nil, nil, nil, nil, []*pb.ControlIDontWant{{MessageIDs: ids}})
	}
	full := func(n int) *haveBits {
		var b haveBits
		for i := 0; i < n; i++ {
			b.Set(uint32(i)) // lint:ignore uintcast -- a test index below 16.
		}
		return &b
	}
	t.Run("requires the paced origin HAVE and excludes link disclosure", func(t *testing.T) {
		if err := WithOriginDilute(time.Second, 6, 0, nil)(&PubSub{rt: testHaveRouter()}); err == nil {
			t.Fatal("dilution installed without WithOriginHavePaced")
		}
		ps := &PubSub{rt: testHaveRouter()}
		if err := WithOriginHavePaced()(ps); err != nil {
			t.Fatal(err)
		}
		if err := WithLinkDisclosure(3, 6, nil)(ps); err != nil {
			t.Fatal(err)
		}
		if err := WithOriginDilute(time.Second, 6, 0, nil)(ps); err == nil {
			t.Fatal("dilution installed over link disclosure")
		}
	})
	gs := testHaveRouter()
	ps := &PubSub{rt: gs}
	if err := WithOriginHavePaced()(ps); err != nil {
		t.Fatal(err)
	}
	var st DisclosureStats
	const pace = 400 * time.Millisecond
	if err := WithOriginDilute(pace, 12, 16, &st)(ps); err != nil {
		t.Fatal(err)
	}
	d := gs.have.disclose
	gs.have.mine[relayed] = &haveEntry{bits: *full(16)}
	gs.have.originPaced.roots[root] = &originPacedRoot{popped: *full(16)}
	d.start[root] = time.Now().Add(-pace / 4) // a budget of 12/4 = 3
	gs.discloseTraded("a", testMid(root, 15))
	gs.discloseTraded("b", testMid(root, 14))
	gs.discloseTraded("a", testMid(relayed, 3))
	t.Run("a relayed group passes untouched", func(t *testing.T) {
		in := idw(encodeHave(relayed, full(16)))
		if gs.applyDisclosure(in, "a") != in || gs.discloseRefuses("a", testMid(relayed, 9)) || len(d.links["a"]) != 1 {
			t.Fatal("a relayed group was cut, refused or tracked")
		}
	})
	t.Run("every link hears its exchanges, then a prefix of one order, to the budget", func(t *testing.T) {
		gotA, _ := indicesOf(gs.applyDisclosure(idw(encodeHave(root, full(16))), "a"), root)
		gotB, _ := indicesOf(gs.applyDisclosure(idw(encodeHave(root, full(16))), "b"), root)
		order := make([]uint32, 16)
		for i := range order {
			order[i] = uint32(i) // lint:ignore uintcast -- a test index below 16.
		}
		sort.Slice(order, func(x, y int) bool { return pairRank(root, order[x], "", "") < pairRank(root, order[y], "", "") })
		want := func(own uint32) string {
			out := []uint32{own}
			for _, i := range order {
				if len(out) == 3 {
					break
				}
				if i != own {
					out = append(out, i)
				}
			}
			slices.Sort(out)
			return fmt.Sprint(out)
		}
		if fmt.Sprint(gotA) != want(15) || fmt.Sprint(gotB) != want(14) {
			t.Fatalf("a hears %v, want %s; b hears %v, want %s", gotA, want(15), gotB, want(14))
		}
		if !slices.Contains(gotB, 15) && !gs.discloseRefuses("b", testMid(root, 15)) {
			t.Fatal("b was served a's exchanged frame")
		}
	})
	t.Run("the budget grows with time and stays short of need before pace", func(t *testing.T) {
		d.start[root] = time.Now().Add(-pace * 99 / 100)
		got, _ := indicesOf(gs.applyDisclosure(idw(encodeHave(root, full(16))), "c"), root)
		if len(got) != 11 || gs.discloseDone(root) {
			t.Fatalf("c hears %d frames at 99%% of pace, want 11 (need - 1) and the group open", len(got))
		}
	})
	t.Run("completion at pace lifts the limit", func(t *testing.T) {
		d.start[root] = time.Now().Add(-pace)
		in := idw(encodeHave(root, full(16)))
		if gs.applyDisclosure(in, "c") != in || gs.discloseRefuses("b", testMid(root, 15)) || st.Completed.Load() != 1 {
			t.Fatalf("after pace: cut, refused, or completed %d (want 1)", st.Completed.Load())
		}
	})
}
