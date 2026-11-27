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
	"github.com/libp2p/go-libp2p-pubsub/timecache"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
)

// seacastForwardRouter is a hand-built router for SEACAST's forward of chunk index of root: a's
// holding names the chunk, b claimed the group complete without it, and c asked us not to send the
// message (IDONTWANT). mesh is a to f and s; tosend is the mesh less c, as rpcs builds it.
func seacastForwardRouter(root string, index uint32, csum checksum) (gs *GossipSubRouter, mesh, tosend map[peer.ID]struct{}) {
	gs = testHaveRouter()
	g := seacastGID(root)
	hold := func(p peer.ID, complete bool, idx ...uint32) {
		if gs.have.peers[p] == nil {
			gs.have.peers[p] = map[core.GroupID]*core.PeerHolding{}
		}
		e := &core.PeerHolding{Complete: complete}
		for _, i := range idx {
			e.Bits.Set(i)
		}
		gs.have.peers[p][g] = e
	}
	hold("a", false, index)
	hold("b", true, 0, 1)
	gs.unwanted["c"] = map[checksum]int{csum: 1}
	mesh = map[peer.ID]struct{}{"a": {}, "b": {}, "c": {}, "d": {}, "e": {}, "f": {}, "s": {}}
	tosend = map[peer.ID]struct{}{"a": {}, "b": {}, "d": {}, "e": {}, "f": {}, "s": {}}
	return gs, mesh, tosend
}

func sortedPeers(set map[peer.ID]struct{}) []peer.ID {
	out := make([]peer.ID, 0, len(set))
	for p := range set {
		out = append(out, p)
	}
	slices.Sort(out)
	return out
}

// TestSeacastForwardCandidates: a relay's forward of a chunk through the core (seacastTrim,
// seacast-lite.md §5.2's forward) with R_NODE 4 over the mesh a to f and s, the sender. a's holding
// names the chunk: it takes one place and is pushed nothing, whether or not an IDONTWANT of it is
// still recorded. b claimed the group complete: no candidate, no place, not offered. c asked us
// not to send it: no candidate, no place. The sender s is no candidate. So the budget is 3 over d,
// e and f: all three pushed, nobody offered. With WithSeacastSenderInForward the sender is a
// candidate too: with R_NODE 5 the budget is 4 over d, e, f and s, all chosen (the push to s is
// skipped later, at the send).
func TestSeacastForwardCandidates(t *testing.T) {
	root := strings.Repeat("f", 32)
	const index = 4
	mid := testMid(root, index)
	csum := computeChecksum(mid)

	gs, mesh, tosend := seacastForwardRouter(root, index, csum)
	pf := &phaseForwarding{degree: 4, selFewest: true}
	push, offer := pf.seacastTrim(gs, mid, root, index, csum, "s", mesh, tosend, false, 4)
	if want := []peer.ID{"d", "e", "f"}; !slices.Equal(push, want) || len(offer) != 0 || !slices.Equal(sortedPeers(tosend), want) {
		t.Fatalf("pushed %v (to send %v), offered %v; want %v, %v, none", push, sortedPeers(tosend), offer, want, want)
	}

	t.Run("the sender kept", func(t *testing.T) {
		gs, mesh, tosend := seacastForwardRouter(root, index, csum)
		pf := &phaseForwarding{degree: 5, selFewest: true, senderInForward: true}
		push, offer := pf.seacastTrim(gs, mid, root, index, csum, "s", mesh, tosend, false, 5)
		if want := []peer.ID{"d", "e", "f", "s"}; !slices.Equal(push, want) || len(offer) != 0 {
			t.Fatalf("pushed %v, offered %v; want %v, none", push, offer, want)
		}
	})
}

// TestSeacastForwardAtSource: the publisher's own chunk (seacastTrim at the source, seacast-lite.md
// §5.3): the candidates are the peers that want the chunk from it. a, whose holding names the
// chunk, and b, complete, are left out, and the budget stays R_NODE: a holder takes no place at
// the source. c asked us not to send it. With R_NODE 6 over d to k, the six chosen are the
// ranking's first (no draw at the source), in its order, and the pushes leave in that order
// (rankedPushOrder), a direct peer after them: the first copy of each chunk goes to the target
// ranked first.
func TestSeacastForwardAtSource(t *testing.T) {
	root := strings.Repeat("s", 32)
	const index = 2
	mid := testMid(root, index)
	csum := computeChecksum(mid)
	gs, mesh, tosend := seacastForwardRouter(root, index, csum)
	for _, p := range []peer.ID{"g", "h", "i", "j", "k"} {
		mesh[p], tosend[p] = struct{}{}, struct{}{}
	}
	delete(mesh, "s")
	delete(tosend, "s")
	for k, p := range []peer.ID{"d", "e", "f", "g"} { // known counts 1 to 4: the ranking's last
		for i := range k + 1 {
			gs.haveNotePushed(p, testMid(root, byte(10+i)))
		}
	}
	pf := &phaseForwarding{degree: 6, selFewest: true}
	push, offer := pf.seacastTrim(gs, mid, root, index, csum, "me", mesh, tosend, true, 6)
	cands := []peer.ID{"d", "e", "f", "g", "h", "i", "j", "k"}
	order, _, _ := core.RankPush(gs.have.peers, gs.have.pushed, seacastGID(root), cands, pf.pushChoice(mid, root, time.Time{}))
	if !slices.Equal(push, order[:6]) || !slices.Equal(offer, order[6:]) {
		t.Fatalf("pushed %v, offered %v; want the ranking's %v, then %v", push, offer, order[:6], order[6:])
	}
	tosend["x"] = struct{}{} // a direct peer, outside the forward
	sends := gs.rankedPushOrder(tosend, push)
	if !slices.Equal(sends[:6], push) || len(sends) != 7 || sends[6] != "x" {
		t.Fatalf("the pushes leave in %v; want %v, then x", sends, push)
	}

	t.Run("a negative budget", func(t *testing.T) { // a type's R_NODE below zero pushes nothing
		gs, mesh, tosend := seacastForwardRouter(root, index, csum)
		delete(mesh, "s")
		delete(tosend, "s")
		push, offer := pf.seacastTrim(gs, mid, root, index, csum, "me", mesh, tosend, true, -1)
		if want := []peer.ID{"d", "e", "f"}; len(push) != 0 || !slices.Equal(slices.Sorted(slices.Values(offer)), want) || len(tosend) != 0 {
			t.Fatalf("pushed %v (to send %v), offered %v; want none, none, %v", push, sortedPeers(tosend), offer, want)
		}
	})
}

// TestSeacastSourceHoldingComplete: the holding of a group the node publishes (in origin) claims
// complete beside claim_all once it names every chunk, and neither before (holding_of at the
// source, seacast.md §1.5, core.OriginClaims), whatever the group's machine state says. A single
// Publish accepts the source's own chunk, so the source can have that state: a group whose asks
// stopped claims nothing more while its holding names one chunk, and a group still collecting
// claims both once every chunk is named. A chunk the node publishes rebuilt is its own but not in
// origin: its holding claims what the machine says, claim_all alone here.
func TestSeacastSourceHoldingComplete(t *testing.T) {
	root := strings.Repeat("o", haveRootLen)
	T := &testObjectType{name: "col", topics: []string{"seg/col/0"}, indexCount: 4}
	router := func(origin bool) *GossipSubRouter {
		gs := testHaveRouter()
		gs.seacast = &seacast{types: map[string]ObjectType{"seg/col/0": T}, ctr: &SeacastCounters{}, machine: true,
			roots: map[string]ObjectType{root: T}, rootTopic: map[string]string{root: "seg/col/0"},
			ownRoots: map[string]struct{}{root: {}}, gm: core.NewMachine[*seacastProbe]()}
		gs.have.originAcks = &originAcks{roots: map[string]*core.Origin[peer.ID]{strings.Repeat("p", haveRootLen): {}}}
		if origin {
			gs.have.originAcks.roots[root] = &core.Origin[peer.ID]{}
		}
		return gs
	}
	bitsOf := func(idx ...uint32) *haveBits {
		var b haveBits
		for _, i := range idx {
			b.Set(i)
		}
		return &b
	}
	const both = seacastFlagClaimAll | seacastFlagComplete
	gs := router(true)
	if _, flags := gs.seacastHaveFlags(root, bitsOf(0, 1, 2), false); flags != 0 {
		t.Fatalf("three of four chunks: flags %b, want none", flags)
	}
	if v4, flags := gs.seacastHaveFlags(root, bitsOf(0, 1, 2, 3), false); !v4 || flags != both {
		t.Fatalf("every chunk: v4 %v, flags %b; want true, claim_all and complete", v4, flags)
	}

	gs.seacast.gm.Put(seacastGID(root), &seacastGroup{State: seacastCollecting, StopPull: true})
	if _, flags := gs.seacastHaveFlags(root, bitsOf(0), false); flags != 0 {
		t.Errorf("machine state whose asks stopped, one chunk named: flags %b, want none", flags)
	}
	gs.seacast.gm.Put(seacastGID(root), &seacastGroup{State: seacastCollecting})
	if _, flags := gs.seacastHaveFlags(root, bitsOf(0, 1, 2, 3), false); flags != both {
		t.Errorf("machine state collecting, every chunk named: flags %b, want claim_all and complete", flags)
	}

	if _, flags := router(false).seacastHaveFlags(root, bitsOf(0, 1, 2, 3), false); flags != seacastFlagClaimAll {
		t.Errorf("own but not in origin (rebuilt), every chunk held: flags %b, want claim_all alone", flags)
	}
}

// TestSeacastSenderInForwardOption: the option needs the phase path, and sets the forward's switch.
func TestSeacastSenderInForwardOption(t *testing.T) {
	gs := &GossipSubRouter{}
	ps := &PubSub{rt: gs}
	if err := WithSeacastSenderInForward()(ps); err == nil {
		t.Fatal("without WithPhaseForwarding: no error")
	}
	gs.phaseForward = &phaseForwarding{}
	if err := WithSeacastSenderInForward()(ps); err != nil || !gs.phaseForward.senderInForward {
		t.Fatalf("after WithPhaseForwarding: error %v, switch %v; want none, on", err, gs.phaseForward.senderInForward)
	}
}

// TestSeacastForwardInterest: a peer whose interest in the group leaves out the chunk is no
// candidate of its forward, and takes no place (wants, seacast-lite.md §5.2). d2's interest is
// indices 3 and 4 (DERIVED), all's is every index (ALL). Chunk 0 goes to all alone; chunk 3 to both.
func TestSeacastForwardInterest(t *testing.T) {
	root := strings.Repeat("r", haveRootLen)
	topic := "seg/col/0"
	T := &testObjectType{name: "col", topics: []string{topic},
		derived: func(string, NodeID, Join) ([]uint32, bool) { return []uint32{3, 4}, false }}
	all, d2 := peer.ID("all"), peer.ID("d2")
	table := &MapRoleTable{
		Joins: map[peer.ID]map[string]Join{
			all: {"col": {Interest: InterestAll, Duty: DutyForward}},
			d2:  {"col": {Interest: InterestDerived, CustodyCount: 8, Duty: DutyForward}},
		},
		IDs: map[peer.ID]NodeID{d2: {2}},
	}
	gs := &GossipSubRouter{}
	gs.p = &PubSub{host: getDefaultHosts(t, 1)[0], idGen: &msgIDGenerator{Default: seacastTestID, topicGens: map[string]MsgIdFunction{}},
		topics: map[string]map[peer.ID]peerTopicState{topic: {all: {}, d2: {}}}}
	gs.have = newHaveState(seacastTestIndex)
	gs.seacast = &seacast{types: map[string]ObjectType{topic: T}, ctr: &SeacastCounters{}, roles: table}
	gs.seacastNoteRoot(topic, seacastTestID(&pb.Message{Data: append([]byte(root+string(byte(0))), 'x')}))
	pf := &phaseForwarding{degree: 2, selFewest: true}
	for _, c := range []struct {
		index uint32
		want  []peer.ID
	}{{0, []peer.ID{all}}, {3, []peer.ID{all, d2}}} {
		mid := seacastTestID(&pb.Message{Data: append([]byte(root+string(byte(c.index))), 'x')})
		mesh := map[peer.ID]struct{}{all: {}, d2: {}}
		tosend := map[peer.ID]struct{}{all: {}, d2: {}}
		push, offer := pf.seacastTrim(gs, mid, root, c.index, computeChecksum(mid), "s", mesh, tosend, false, 2)
		if !slices.Equal(push, c.want) || len(offer) != 0 {
			t.Errorf("chunk %d: pushed %v, offered %v; want %v, none", c.index, push, offer, c.want)
		}
	}
}

// pushRecorder is a batch's scheduler that records the order the router hands it each message's
// pushes in, before the round robin.
type pushRecorder struct {
	RoundRobinMessageIDScheduler
	mu    sync.Mutex
	order map[string][]peer.ID
}

func (r *pushRecorder) AddRPC(p peer.ID, msgID string, rpc *RPC) {
	if len(rpc.Publish) > 0 {
		r.mu.Lock()
		r.order[msgID] = append(r.order[msgID], p)
		r.mu.Unlock()
	}
	r.RoundRobinMessageIDScheduler.AddRPC(p, msgID, rpc)
}

// TestSeacastSourcePushOrder: a source's batch hands each chunk's pushes to the scheduler in the
// order its forward chose them (rpcs through rankedPushOrder), so the first copy of each chunk
// goes to the target ranked first (seacast-lite.md §5.3's targets[copy]), not in Go's map order.
// S (router 0) has ten peers, each only connected to S, all in its mesh on the column topic;
// R_NODE 6. Each chunk's six pushes are the first six of RankPush's order over S's
// mesh, in that order. The batch plans every chunk before any is sent, so no peer's holding is
// known yet, and the counts are S's pushes of the earlier chunks alone (recorded with the IHAVE
// side, WithHaveAnnounceBitmaps, as Prysm runs).
func TestSeacastSourcePushOrder(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		const topic = "seg/col/0"
		hosts := getDefaultHosts(t, 11)
		params := DefaultGossipSubParams()
		params.D, params.Dlo, params.Dhi = 8, 8, 12
		psubs := getGossipsubsOptFn(ctx, hosts, func(int, host.Host) []Option {
			T := &testObjectType{name: "col", params: TypeParams{RNode: 6}, topics: []string{topic}}
			return []Option{
				WithGossipSubParams(params), WithMessageSignaturePolicy(StrictNoSign), WithNoAuthor(),
				WithMessageIdFn(seacastTestID), WithHaveBitmaps(seacastTestIndex), WithHaveAnnounceBitmaps(),
				WithPhaseForwarding(func(string) bool { return false }, 2), WithPhaseSelectFewest(),
				WithSeacast([]ObjectType{T}, nil),
			}
		})
		var tps []*Topic
		for _, ps := range psubs {
			tp, err := ps.Join(topic)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := tp.Subscribe(); err != nil {
				t.Fatal(err)
			}
			tps = append(tps, tp)
		}
		for _, h := range hosts[1:] {
			connect(t, hosts[0], h)
		}
		time.Sleep(2 * time.Second)
		gsS := psubs[0].rt.(*GossipSubRouter)
		var meshS []peer.ID
		done := make(chan struct{})
		gsS.p.eval <- func() { meshS = sortedPeers(gsS.mesh[topic]); close(done) }
		<-done
		if len(meshS) != 10 {
			// Every leaf in S's mesh: none can graft later and change the candidates under the
			// batch, and with 10 at most Dhi none is pruned.
			t.Fatalf("the scene: S has %d mesh peers on the topic, want all 10", len(meshS))
		}

		root := strings.Repeat("o", haveRootLen)
		var batch MessageBatch
		for i := byte(0); i < 4; i++ {
			if err := tps[0].AddToBatch(ctx, &batch, seacastCell(root, i)); err != nil {
				t.Fatal(err)
			}
		}
		rec := &pushRecorder{order: map[string][]peer.ID{}}
		strategy := func(o *BatchPublishOptions) error { o.Strategy = rec; return nil }
		if err := psubs[0].PublishBatch(&batch, strategy); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Second)
		g := seacastGID(root)
		pushes := core.Pushes[peer.ID]{} // S's pushes of the chunks before, as the ranking counts them
		for i := byte(0); i < 4; i++ {
			mid := seacastTestID(&pb.Message{Data: seacastCell(root, i)})
			rec.mu.Lock()
			got := slices.Clone(rec.order[mid])
			rec.mu.Unlock()
			order, _, _ := core.RankPush(core.PeerHoldings[peer.ID]{}, pushes, g, meshS, gsS.phaseForward.pushChoice(mid, root, time.Time{}))
			if !slices.Equal(got, order[:6]) {
				t.Fatalf("chunk %d's pushes handed over in %v; want the ranking's first six, %v", i, got, order[:6])
			}
			for _, p := range got {
				if pushes[p] == nil {
					pushes[p] = map[core.GroupID]*core.Push{}
				}
				if pushes[p][g] == nil {
					pushes[p][g] = &core.Push{}
				}
				pushes[p][g].Bits.Set(uint32(i))
			}
		}
	})
}

// sourceHoldingScene is a hand-built router that publishes root and only holds other, both whole at
// four chunks, with one mesh peer m on the topic: sent pops the holdings the router queued to m since
// the last call, each as its indices and claims.
func sourceHoldingScene(t *testing.T) (gs *GossipSubRouter, T *testObjectType, root, other string, sent func() []sentHolding) {
	t.Helper()
	root, other = strings.Repeat("o", haveRootLen), strings.Repeat("q", haveRootLen)
	const topic = "seg/col/0"
	T = &testObjectType{name: "col", topics: []string{topic}, indexCount: 4}
	gs = testHaveRouter()
	gs.seacast = &seacast{types: map[string]ObjectType{topic: T}, ctr: &SeacastCounters{}, machine: true,
		roots: map[string]ObjectType{root: T, other: T}, rootTopic: map[string]string{root: topic, other: topic},
		gm:     core.NewMachine[*seacastProbe](),
		origin: map[string]*seacastOrigin{root: {topic: topic, ids: map[uint32]string{}}}}
	for i := range byte(10) {
		gs.seacast.origin[root].ids[uint32(i)] = testMid(root, i) // testHaveRouter's index reads these
	}
	gs.have.originAcks = &originAcks{roots: map[string]*core.Origin[peer.ID]{root: {}}}
	for _, r := range []string{root, other} {
		e := &core.Holding{}
		gs.have.mine[seacastGID(r)] = e
		for i := range uint32(4) {
			e.Bits.Set(i)
		}
	}
	m := peer.ID("m")
	q := newRpcQueue(16)
	gs.peers = map[peer.ID]protocol.ID{m: GossipSubID_v12}
	gs.feature = GossipSubDefaultFeatures
	gs.mesh = map[string]map[peer.ID]struct{}{topic: {m: {}}}
	gs.p = &PubSub{seenMessages: timecache.NewTimeCache(time.Minute), idGen: newMsgIdGenerator(),
		peers: map[peer.ID]*rpcQueue{m: q}, maxMessageSize: DefaultMaxMessageSize, maxControlMessageSize: DefaultMaxControlMessageSize}
	sent = func() []sentHolding {
		var out []sentHolding
		for {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
			rpc, err := q.Pop(ctx)
			cancel()
			if err != nil {
				return out
			}
			for _, idw := range rpc.GetControl().GetIdontwant() {
				for _, id := range idw.GetMessageIDs() {
					if r, ca, co, enc, ok, _ := decodeHaveV2(id); ok {
						var b haveBits
						b.Or(enc)
						out = append(out, sentHolding{root: r, bits: b.Indices(), claimAll: ca, complete: co})
					}
				}
			}
		}
	}
	return gs, T, root, other, sent
}

type sentHolding struct {
	root               string
	bits               []uint32
	claimAll, complete bool
}

// TestSeacastSourceCompletionHolding: the holding a completion sends (seacastSendHolding, the
// machine's SendHolding) for a group the node publishes names the chunks acknowledged, not those it
// holds (have_bits, seacast.md §4.7): with all four held and none acknowledged, nothing goes; with
// one acknowledged, a holding of that one, without claims; with all four, both claims. A group the
// node doesn't publish sends what it holds. Once the count is known, drop_beyond takes an index
// acknowledged at or beyond it (seacastDropBeyond), so the holding names no bit its receivers refuse.
func TestSeacastSourceCompletionHolding(t *testing.T) {
	gs, T, root, other, sent := sourceHoldingScene(t)
	send := func(r string) []sentHolding {
		gs.seacastSendHolding(T, r)
		return sent()
	}
	if got := send(root); len(got) != 0 {
		t.Fatalf("published, every chunk held, none acknowledged: holdings %+v sent, want none", got)
	}
	gs.have.originAcks.ack(root, 0, nil)
	if got := send(root); len(got) != 1 || !slices.Equal(got[0].bits, []uint32{0}) || got[0].claimAll || got[0].complete {
		t.Fatalf("published, chunk 0 acknowledged: holdings %+v; want one of [0], without claims", got)
	}
	for i := range uint32(4) {
		gs.have.originAcks.ack(root, i, nil)
	}
	if got := send(root); len(got) != 1 || len(got[0].bits) != 4 || !got[0].claimAll || !got[0].complete {
		t.Fatalf("published, every chunk acknowledged: holdings %+v; want one of four, both claims", got)
	}
	if got := send(other); len(got) != 1 || len(got[0].bits) != 4 || !got[0].claimAll || got[0].complete {
		t.Fatalf("not published, every chunk held: holdings %+v; want one of four, claim_all alone", got)
	}

	gs.have.originAcks.ack(root, 9, nil) // acknowledged while the count was unknown
	gs.seacastDropBeyond([]string{root})
	if b, _ := gs.have.originAcks.bits(root); !slices.Equal(b.Indices(), []uint32{0, 1, 2, 3}) {
		t.Fatalf("after drop_beyond at 4: acknowledged %v, want [0 1 2 3]", b.Indices())
	}
	if got := send(root); len(got) != 1 || !slices.Equal(got[0].bits, []uint32{0, 1, 2, 3}) {
		t.Fatalf("after drop_beyond at 4: holdings %+v, want one of [0 1 2 3]", got)
	}
}

// TestSeacastSourceHoldingBelowCount: a group the node publishes, with an index acknowledged while
// its count was unknown, never has a holding naming that index once the type knows the count, even
// before drop_beyond reaches the router (the header's validation): the acknowledgement's holding, the
// completion's and the lead's name only indices below the count. And a window timeout of a copy sent
// while the count was unknown, after drop_beyond, acknowledges nothing (seacastOriginAck).
func TestSeacastSourceHoldingBelowCount(t *testing.T) {
	gs, T, root, _, sent := sourceHoldingScene(t)
	T.indexCount = -1 // not known yet
	gs.seacastOriginAck(root, 9)
	if got := sent(); len(got) != 1 || !slices.Equal(got[0].bits, []uint32{9}) {
		t.Fatalf("count unknown, 9 acknowledged: holdings %+v, want one of [9]", got)
	}

	T.indexCount = 4 // the type knows the count; the router hasn't run drop_beyond yet
	gs.seacastOriginAck(root, 0)
	if got := sent(); len(got) != 1 || !slices.Equal(got[0].bits, []uint32{0}) {
		t.Fatalf("count 4 known to the type, 0 acknowledged: holdings %+v, want one of [0]", got)
	}
	gs.seacastSendHolding(T, root)
	if got := sent(); len(got) != 1 || !slices.Equal(got[0].bits, []uint32{0}) {
		t.Fatalf("count 4 known to the type, completion: holdings %+v, want one of [0]", got)
	}
	bits, first := gs.have.originAcks.lead(root, "m", 2)
	if !first {
		t.Fatal("the lead to m: already sent")
	}
	_, _, _, enc, _, _ := decodeHaveV2(gs.seacastEncodeHave(root, &bits, false)) // the lead's encoding
	var lead haveBits
	lead.Or(enc)
	if !slices.Equal(lead.Indices(), []uint32{0, 2}) {
		t.Fatalf("count 4 known to the type, the lead naming 2: %v, want [0 2]", lead.Indices())
	}

	gs.seacastDropBeyond([]string{root})
	gs.seacastOriginAck(root, 9) // a copy of 9's window slot timing out (onHoseExpire)
	if b, _ := gs.have.originAcks.bits(root); !slices.Equal(b.Indices(), []uint32{0}) {
		t.Fatalf("after drop_beyond, 9's window timeout: acknowledged %v, want [0]", b.Indices())
	}
	if got := sent(); len(got) != 0 {
		t.Fatalf("after drop_beyond, 9's window timeout: holdings %+v sent, want none", got)
	}
}

// TestSeacastSourceLeadNamesFirstSent: a source's lead holding to a peer names the chunk the peer is
// sent first (seacast-lite.md §5.1: the lead goes in the copy loop, just before the peer's first
// copy), not the first one planned to it. S (router 0) publishes a batch of 8 chunks to its mesh of
// 10 leaves, R_NODE 6. The scheduler sends one copy of each chunk per pass, so a peer ranked low
// for chunk 0 and first for a later chunk is sent that later chunk first. Each leaf records the
// first holding of the group it hears from S and the first chunk it receives: the holding names
// that chunk alone, and comes before it. The scene must have such a peer, read from the batch's plan.
func TestSeacastSourceLeadNamesFirstSent(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		const topic = "seg/col/0"
		const chunks = 8
		root := strings.Repeat("l", haveRootLen)
		hosts := getDefaultHosts(t, 11)
		params := DefaultGossipSubParams()
		params.D, params.Dlo, params.Dhi = 8, 8, 12
		type first struct {
			lead       []uint32
			chunk      int
			leadBefore bool // the lead had come when the first chunk did
		}
		var mu sync.Mutex
		seen := map[peer.ID]*first{}
		psubs := getGossipsubsOptFn(ctx, hosts, func(i int, h host.Host) []Option {
			T := &testObjectType{name: "col", params: TypeParams{RNode: 6}, topics: []string{topic}}
			opts := []Option{
				WithGossipSubParams(params), WithMessageSignaturePolicy(StrictNoSign), WithNoAuthor(),
				WithMessageIdFn(seacastTestID), WithHaveBitmaps(seacastTestIndex), WithHaveAnnounceBitmaps(),
				WithPhaseForwarding(func(string) bool { return false }, 2), WithPhaseSelectFewest(),
				WithSeacast([]ObjectType{T}, nil),
			}
			if i == 0 {
				return opts
			}
			me := h.ID()
			return append(opts, WithRawTracer(&mockRawTracer{onRecvRPC: func(rpc *RPC) {
				mu.Lock()
				defer mu.Unlock()
				f := seen[me]
				if f == nil {
					f = &first{chunk: -1}
					seen[me] = f
				}
				for _, idw := range rpc.GetControl().GetIdontwant() {
					for _, id := range idw.GetMessageIDs() {
						if r, _, _, enc, ok, _ := decodeHaveV2(id); ok && r == root && f.lead == nil {
							var b haveBits
							b.Or(enc)
							f.lead = b.Indices()
						}
					}
				}
				for _, m := range rpc.GetPublish() {
					if r, i, ok := seacastTestIndex(seacastTestID(m)); ok && r == root && f.chunk < 0 {
						f.chunk = int(i)
						f.leadBefore = f.lead != nil
					}
				}
			}}))
		})
		var tps []*Topic
		for _, ps := range psubs {
			tp, err := ps.Join(topic)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := tp.Subscribe(); err != nil {
				t.Fatal(err)
			}
			tps = append(tps, tp)
		}
		for _, h := range hosts[1:] {
			connect(t, hosts[0], h)
		}
		time.Sleep(2 * time.Second)
		gsS := psubs[0].rt.(*GossipSubRouter)
		var meshS int
		done := make(chan struct{})
		gsS.p.eval <- func() { meshS = len(gsS.mesh[topic]); close(done) }
		<-done
		if meshS != 10 {
			t.Fatalf("the scene: S has %d mesh peers on the topic, want all 10", meshS)
		}

		var batch MessageBatch
		for i := byte(0); i < chunks; i++ {
			if err := tps[0].AddToBatch(ctx, &batch, seacastCell(root, i)); err != nil {
				t.Fatal(err)
			}
		}
		rec := &pushRecorder{order: map[string][]peer.ID{}}
		if err := psubs[0].PublishBatch(&batch, func(o *BatchPublishOptions) error { o.Strategy = rec; return nil }); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Second)

		// The plan: each peer's first planned chunk, and the first sent, pass by pass as the
		// scheduler sends (one copy of each chunk per pass).
		rec.mu.Lock()
		planned, sent := map[peer.ID]int{}, map[peer.ID]int{}
		for pass := 0; pass < 10; pass++ {
			for i := 0; i < chunks; i++ {
				targets := rec.order[seacastTestID(&pb.Message{Data: seacastCell(root, byte(i))})]
				if pass == 0 {
					for _, p := range targets {
						if _, ok := planned[p]; !ok {
							planned[p] = i
						}
					}
				}
				if pass < len(targets) {
					if _, ok := sent[targets[pass]]; !ok {
						sent[targets[pass]] = i
					}
				}
			}
		}
		rec.mu.Unlock()
		differ := 0
		for p, i := range sent {
			if planned[p] != i {
				differ++
			}
		}
		if differ == 0 {
			t.Fatalf("the scene: every peer's first planned chunk is its first sent; want one that differs")
		}

		mu.Lock()
		defer mu.Unlock()
		for p, i := range sent {
			f := seen[p]
			if f == nil || f.chunk != i {
				t.Fatalf("peer %s: first chunk received %v, want %d (the batch's first sent)", p, f, i)
			}
			if !slices.Equal(f.lead, []uint32{uint32(i)}) {
				t.Errorf("peer %s: lead names %v, first chunk sent %d (first planned %d)", p, f.lead, i, planned[p])
			}
			if !f.leadBefore {
				t.Errorf("peer %s: first chunk %d came before the lead", p, i)
			}
		}
		t.Logf("%d of %d peers are sent another chunk before their first planned one", differ, len(sent))
	})
}

// TestSeacastSourceBudget: the type's RSource is the source's per-chunk push budget
// (seacast-lite.md §5.1's R at the source), and unset the source pushes R_NODE. S (router 0) has
// ten peers, each only connected to S, all in its mesh on the column topic, with R_NODE 6. With
// RSource 2 each of S's chunks is handed to the scheduler for two peers, the ranking's first two;
// with RSource unset, for six. A relay's budget stays R_NODE either way, and so does that of a chunk
// S rebuilt and publishes (WithRebuiltPublication): it is not S's own.
func TestSeacastSourceBudget(t *testing.T) {
	for _, rs := range []int{2, 0} {
		want := rs
		if rs == 0 {
			want = 6
		}
		t.Run(map[int]string{2: "RSource 2", 0: "RSource unset"}[rs], func(t *testing.T) {
			synctestTest(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				const topic = "seg/col/0"
				hosts := getDefaultHosts(t, 11)
				params := DefaultGossipSubParams()
				params.D, params.Dlo, params.Dhi = 8, 8, 12
				psubs := getGossipsubsOptFn(ctx, hosts, func(int, host.Host) []Option {
					T := &testObjectType{name: "col", params: TypeParams{RNode: 6, RSource: rs}, topics: []string{topic}}
					return []Option{
						WithGossipSubParams(params), WithMessageSignaturePolicy(StrictNoSign), WithNoAuthor(),
						WithMessageIdFn(seacastTestID), WithHaveBitmaps(seacastTestIndex), WithHaveAnnounceBitmaps(),
						WithPhaseForwarding(func(string) bool { return false }, 2), WithPhaseSelectFewest(),
						WithSeacast([]ObjectType{T}, nil),
					}
				})
				var tps []*Topic
				for _, ps := range psubs {
					tp, err := ps.Join(topic)
					if err != nil {
						t.Fatal(err)
					}
					if _, err := tp.Subscribe(); err != nil {
						t.Fatal(err)
					}
					tps = append(tps, tp)
				}
				for _, h := range hosts[1:] {
					connect(t, hosts[0], h)
				}
				time.Sleep(2 * time.Second)
				gsS := psubs[0].rt.(*GossipSubRouter)
				var meshS []peer.ID
				var relay, source int
				done := make(chan struct{})
				gsS.p.eval <- func() {
					meshS = sortedPeers(gsS.mesh[topic])
					relay, _ = gsS.seacastPushBudget(topic, false)
					source, _ = gsS.seacastPushBudget(topic, true)
					close(done)
				}
				<-done
				if len(meshS) != 10 {
					t.Fatalf("the scene: S has %d mesh peers on the topic, want all 10", len(meshS))
				}
				if relay != 6 || source != want {
					t.Fatalf("budgets: relay %d, source %d; want 6, %d", relay, source, want)
				}

				root := strings.Repeat("b", haveRootLen)
				var batch MessageBatch
				for i := byte(0); i < 5; i++ {
					var opts []PubOpt
					if i == 4 {
						opts = append(opts, WithRebuiltPublication()) // a chunk S rebuilt: R_NODE
					}
					if err := tps[0].AddToBatch(ctx, &batch, seacastCell(root, i), opts...); err != nil {
						t.Fatal(err)
					}
				}
				rec := &pushRecorder{order: map[string][]peer.ID{}}
				strategy := func(o *BatchPublishOptions) error { o.Strategy = rec; return nil }
				if err := psubs[0].PublishBatch(&batch, strategy); err != nil {
					t.Fatal(err)
				}
				time.Sleep(time.Second)
				g := seacastGID(root)
				pushes := core.Pushes[peer.ID]{}
				for i := byte(0); i < 5; i++ {
					mid := seacastTestID(&pb.Message{Data: seacastCell(root, i)})
					rec.mu.Lock()
					got := slices.Clone(rec.order[mid])
					rec.mu.Unlock()
					wantI := want
					if i == 4 {
						wantI = 6 // rebuilt: R_NODE
					}
					order, _, _ := core.RankPush(core.PeerHoldings[peer.ID]{}, pushes, g, meshS, gsS.phaseForward.pushChoice(mid, root, time.Time{}))
					if !slices.Equal(got, order[:wantI]) {
						t.Fatalf("chunk %d's pushes handed over to %v; want the ranking's first %d, %v", i, got, wantI, order[:wantI])
					}
					for _, p := range got {
						if pushes[p] == nil {
							pushes[p] = map[core.GroupID]*core.Push{}
						}
						if pushes[p][g] == nil {
							pushes[p][g] = &core.Push{}
						}
						pushes[p][g].Bits.Set(uint32(i))
					}
				}
			})
		})
	}
}
