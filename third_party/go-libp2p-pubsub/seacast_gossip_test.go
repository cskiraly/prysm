package pubsub

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	core "github.com/cskiraly/seacast-go"
	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
)

// newGossipFixture: two routers, A the publisher and B joined to every topic but kept out of A's
// meshes (D = 0), so B is gossip-eligible for each topic; the pacer releases one job per tick,
// sixteen ticks a heartbeat. A's path type owns nPath topics; nPhase topics run on the phase path
// as today's control.
func newGossipFixture(t *testing.T, ctx context.Context, nPath, nPhase int) (psubs []*PubSub, pathTopics, phaseTopics []string) {
	hosts := getDefaultHosts(t, 2)
	for i := range nPath {
		pathTopics = append(pathTopics, fmt.Sprintf("seg/col/%d", i))
	}
	for i := range nPhase {
		phaseTopics = append(phaseTopics, fmt.Sprintf("seg/pay/%d", i))
	}
	params := DefaultGossipSubParams()
	params.D, params.Dlo, params.Dhi, params.Dout, params.Dscore = 0, 0, 0, 0, 0
	params.Dlazy, params.GossipFactor = 6, 0.25
	params.HeartbeatInterval = time.Second
	params.IDontWantMessageThreshold = 1
	psubs = getGossipsubsOptFn(ctx, hosts, func(i int, h host.Host) []Option {
		T := &testObjectType{name: "col", params: TypeParams{RNode: 2}, topics: pathTopics, store: NewMapCellStore(),
			topicOf: func(root string) (string, bool) {
				// The root's last byte names the column topic.
				if len(root) != haveRootLen {
					return "", false
				}
				return fmt.Sprintf("seg/col/%d", int(root[haveRootLen-1])), true
			}}
		return []Option{
			WithGossipSubParams(params),
			WithMessageIdFn(seacastTestID),
			WithHaveBitmaps(seacastTestIndex),
			WithPhaseForwarding(func(topic string) bool { return strings.HasPrefix(topic, "seg/pay/") }, 2),
			WithPacedGossip(params.HeartbeatInterval/16, 0),
			WithSeacast([]ObjectType{T}, nil),
		}
	})
	for _, ps := range psubs {
		for _, topic := range append(append([]string(nil), pathTopics...), phaseTopics...) {
			if _, err := ps.Subscribe(topic); err != nil {
				t.Fatal(err)
			}
		}
	}
	connect(t, hosts[0], hosts[1])
	time.Sleep(2 * time.Second)
	return psubs, pathTopics, phaseTopics
}

// pathCell is a cell of column i of a distinct group root per column.
func pathCell(i int) []byte {
	root := strings.Repeat("g", haveRootLen-1) + string(rune(i))
	return append([]byte(root+string(byte(1))), []byte(strings.Repeat("x", 64))...)
}

// TestSeacastTypedGossip: with 64 path topics under SAMPLE and 32 phase topics, one heartbeat's
// gossip queues one typed job for B over the path's topics (released within the heartbeat, with
// the ids of every topic) and 32 per-topic jobs for the phase topics, of which the pacer's sixteen
// ticks release half and the next heartbeat drops the rest as stale: the starvation the per-topic
// jobs would suffer on 128 columns, and the control that the other topics run as today.
func TestSeacastTypedGossip(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		psubs, pathTopics, phaseTopics := newGossipFixture(t, ctx, 64, 32)
		a := psubs[0]
		gsA := a.rt.(*GossipSubRouter)
		for i, topic := range pathTopics {
			if err := a.Publish(topic, pathCell(i)); err != nil {
				t.Fatal(err)
			}
		}
		for i, topic := range phaseTopics {
			if err := a.Publish(topic, []byte(fmt.Sprintf("pay-%d-%s", i, strings.Repeat("y", 64)))); err != nil {
				t.Fatal(err)
			}
		}
		time.Sleep(100 * time.Millisecond)
		ctr := gsA.seacast.ctr
		read := func(f func()) {
			done := make(chan struct{})
			gsA.p.eval <- func() { f(); close(done) }
			<-done
		}
		var released, dropped int64
		// Two heartbeats and their ticks.
		time.Sleep(2*time.Second + 200*time.Millisecond)
		read(func() { released, dropped = gsA.paced.released.Load(), gsA.paced.dropped.Load() })
		if ctr.GossipJobs.Load() < 1 || ctr.GossipIdsOffered.Load() < 64 {
			t.Fatalf("typed jobs %d, ids offered %d; want at least 1 and 64", ctr.GossipJobs.Load(), ctr.GossipIdsOffered.Load())
		}
		if dropped == 0 {
			t.Fatalf("the phase topics' per-topic jobs: released %d, dropped %d; want some dropped stale (32 jobs against 16 ticks)", released, dropped)
		}
		if released < ctr.GossipJobs.Load() {
			t.Fatalf("released %d jobs in all, fewer than the %d typed ones queued: a typed job was not released within its heartbeat", released, ctr.GossipJobs.Load())
		}
	})
}

// joinPushCell is a cell of column 0's group at index i.
func joinPushCell(i int) []byte {
	root := strings.Repeat("g", haveRootLen-1) + string(rune(0))
	return append([]byte(root+string(byte(i))), []byte(strings.Repeat("x", 64))...)
}

// newJoinPushFixture: two routers on one path topic, seg/col/0. Heartbeat gossip is off (Dlazy 0,
// GossipFactor 0) and the type announces at the forward to the mesh alone, so a peer outside the
// mesh learns of a cell only from join_push. A's mesh degree is aD (0: B stays out until a GRAFT),
// B's bD (0: B never grafts A itself). At a mesh
// degree of 0 A accepts a GRAFT only from an outbound peer (the stock rule at Dhi): the callers mark
// B so.
func newJoinPushFixture(t *testing.T, ctx context.Context, joinPush bool, aD, bD int, extraA ...Option) (psubs []*PubSub, topic string) {
	hosts := getDefaultHosts(t, 2)
	topic = "seg/col/0"
	psubs = getGossipsubsOptFn(ctx, hosts, func(i int, h host.Host) []Option {
		params := DefaultGossipSubParams()
		params.D, params.Dlo, params.Dhi, params.Dout, params.Dscore = 0, 0, 0, 0, 0
		if d := []int{aD, bD}[i]; d > 0 {
			params.D, params.Dlo, params.Dhi = d, d, d+1 // own GRAFTs at the heartbeat
		}
		params.Dlazy, params.GossipFactor = 0, 0
		params.HeartbeatInterval = time.Second
		T := &testObjectType{name: "col", params: TypeParams{RNode: 2}, topics: []string{topic}, store: NewMapCellStore(),
			topicOf: func(root string) (string, bool) { return topic, len(root) == haveRootLen }}
		opts := []Option{
			WithGossipSubParams(params),
			WithMessageIdFn(seacastTestID),
			WithHaveBitmaps(seacastTestIndex),
			WithPhaseForwarding(func(string) bool { return false }, 2),
			WithSeacast([]ObjectType{T}, nil),
		}
		if joinPush {
			opts = append(opts, WithSeacastJoinPush())
		}
		if i == 0 {
			opts = append(opts, extraA...)
		}
		return opts
	})
	if _, err := psubs[0].Subscribe(topic); err != nil {
		t.Fatal(err)
	}
	return psubs, topic
}

// joinPushRun runs f on ps's router goroutine and waits for it.
func joinPushRun(ps *PubSub, f func(gs *GossipSubRouter)) {
	done := make(chan struct{})
	ps.eval <- func() { f(ps.rt.(*GossipSubRouter)); close(done) }
	<-done
}

// joinPushHeld is what B stores of column 0's group, by index.
func joinPushHeld(ps *PubSub) (held []uint32) {
	joinPushRun(ps, func(gs *GossipSubRouter) {
		if e := gs.have.mine[seacastGID(strings.Repeat("g", haveRootLen-1)+string(rune(0)))]; e != nil {
			for i := uint32(0); i < 8; i++ {
				if e.Bits.Has(i) {
					held = append(held, i)
				}
			}
		}
	})
	return held
}

// TestSeacastJoinPush: part 6's join_push at a GRAFT A accepts (seacast.md §4.1). A holds
// indices 1, 2 and 3 of a group, B joined the topic outside A's mesh and learned of none of them.
// A is told B holds 2 and has pushed B 3; B's GRAFT then makes A offer B index 1 alone, and B pulls
// it (the positive). A second GRAFT adds nothing and runs nothing. Off (the control), B is offered
// nothing and holds nothing.
func TestSeacastJoinPush(t *testing.T) {
	for _, on := range []bool{true, false} {
		t.Run(fmt.Sprintf("on=%v", on), func(t *testing.T) {
			synctestTest(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				psubs, topic := newJoinPushFixture(t, ctx, on, 0, 0)
				a, b := psubs[0], psubs[1]
				for i := 1; i <= 3; i++ {
					if err := a.Publish(topic, joinPushCell(i)); err != nil {
						t.Fatal(err)
					}
				}
				if _, err := b.Subscribe(topic); err != nil {
					t.Fatal(err)
				}
				connect(t, a.host, b.host)
				time.Sleep(500 * time.Millisecond)
				if h := joinPushHeld(b); len(h) != 0 {
					t.Fatalf("B holds %v before any GRAFT; want nothing (no gossip, no announce outside the mesh)", h)
				}
				root := strings.Repeat("g", haveRootLen-1) + string(rune(0))
				bid := b.host.ID()
				graft := &pb.ControlMessage{Graft: []*pb.ControlGraft{{TopicID: &topic}}}
				joinPushRun(a, func(gs *GossipSubRouter) {
					if gs.have.peers[bid] == nil {
						gs.have.peers[bid] = map[core.GroupID]*core.PeerHolding{}
					}
					he := &core.PeerHolding{TTL: 100}
					he.Bits.Set(2)
					gs.have.peers[bid][seacastGID(root)] = he
					if gs.have.pushed[bid] == nil {
						gs.have.pushed[bid] = map[core.GroupID]*core.Push{}
					}
					pe := &core.Push{TTL: 100, First: time.Now()}
					pe.Bits.Set(3)
					gs.have.pushed[bid][seacastGID(root)] = pe
					gs.outbound[bid] = true
					if pr := gs.handleGraft(bid, graft); pr != nil {
						t.Errorf("A pruned B's GRAFT: %v", pr)
					}
				})
				time.Sleep(300 * time.Millisecond)
				joinPushRun(a, func(gs *GossipSubRouter) {
					if pr := gs.handleGraft(bid, graft); pr != nil {
						t.Errorf("A pruned B's second GRAFT: %v", pr)
					}
				})
				time.Sleep(100 * time.Millisecond)
				c := a.rt.(*GossipSubRouter).seacast.ctr
				acc, groups, ids := c.JoinPushAccepted.Load(), c.JoinPushGroups.Load(), c.JoinPushIDs.Load()
				held := joinPushHeld(b)
				if on {
					if acc != 1 || groups != 1 || ids != 1 {
						t.Fatalf("join_push accepted %d, groups %d, ids %d; want 1, 1, 1 (index 1 alone: B holds 2, was pushed 3; the second GRAFT adds nothing)", acc, groups, ids)
					}
					if fmt.Sprint(held) != "[1]" {
						t.Fatalf("B holds %v after the GRAFT; want [1], pulled on join_push's offer", held)
					}
				} else if acc != 0 || ids != 0 || len(held) != 0 {
					t.Fatalf("off: join_push accepted %d, ids %d, B holds %v; want 0, 0, nothing", acc, ids, held)
				}
			})
		})
	}
}

// TestSeacastJoinPushNothing: join_push offers nothing of a group past its lifetime.
func TestSeacastJoinPushNothing(t *testing.T) {
	for _, tc := range []struct {
		name    string
		expired bool
	}{{"expired", true}} {
		t.Run(tc.name, func(t *testing.T) {
			synctestTest(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				psubs, topic := newJoinPushFixture(t, ctx, true, 0, 0)
				a, b := psubs[0], psubs[1]
				if err := a.Publish(topic, joinPushCell(1)); err != nil {
					t.Fatal(err)
				}
				if _, err := b.Subscribe(topic); err != nil {
					t.Fatal(err)
				}
				connect(t, a.host, b.host)
				time.Sleep(500 * time.Millisecond)
				root := strings.Repeat("g", haveRootLen-1) + string(rune(0))
				bid := b.host.ID()
				joinPushRun(a, func(gs *GossipSubRouter) {
					if tc.expired {
						if gs.seacast.expired == nil {
							gs.seacast.expired = map[string]struct{}{}
						}
						gs.seacast.expired[root] = struct{}{}
					}
					gs.outbound[bid] = true
					gs.handleGraft(bid, &pb.ControlMessage{Graft: []*pb.ControlGraft{{TopicID: &topic}}})
				})
				time.Sleep(300 * time.Millisecond)
				c := a.rt.(*GossipSubRouter).seacast.ctr
				want := int64(1)
				if c.JoinPushAccepted.Load() != want || c.JoinPushIDs.Load() != 0 || len(joinPushHeld(b)) != 0 {
					t.Fatalf("accepted %d (want %d), ids %d, B holds %v; want nothing offered or held", c.JoinPushAccepted.Load(), want, c.JoinPushIDs.Load(), joinPushHeld(b))
				}
			})
		})
	}
}

// joinPushOrder records, in the order a router enqueues them to each peer, the RPCs that carry a
// GRAFT ("G") or an IHAVE ("I") of one topic.
type joinPushOrder struct {
	mockRawTracer
	topic string
	mu    sync.Mutex
	seq   map[peer.ID][]string
}

func (o *joinPushOrder) SendRPC(rpc *RPC, p peer.ID) {
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, g := range rpc.GetControl().GetGraft() {
		if g.GetTopicID() == o.topic {
			o.seq[p] = append(o.seq[p], "G")
		}
	}
	for _, ih := range rpc.GetControl().GetIhave() {
		if ih.GetTopicID() == o.topic {
			o.seq[p] = append(o.seq[p], "I")
		}
	}
}

// TestSeacastJoinPushOwnGraft: A's own GRAFT at its heartbeat runs join_push at once, as gossipsub's
// wires have it (no answer comes): A holds a cell before B joins; B joins later and never grafts A
// (its degree is 0); A's heartbeat grafts B, offers it the cell, and B pulls it. The offer enters B's
// queue after the GRAFT, so it never takes the queue's last place ahead of it.
func TestSeacastJoinPushOwnGraft(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		order := &joinPushOrder{topic: "seg/col/0", seq: map[peer.ID][]string{}}
		psubs, topic := newJoinPushFixture(t, ctx, true, 2, 0, WithRawTracer(order))
		a, b := psubs[0], psubs[1]
		if err := a.Publish(topic, joinPushCell(1)); err != nil {
			t.Fatal(err)
		}
		if _, err := b.Subscribe(topic); err != nil {
			t.Fatal(err)
		}
		connect(t, a.host, b.host)
		time.Sleep(2500 * time.Millisecond) // past A's heartbeat
		c := a.rt.(*GossipSubRouter).seacast.ctr
		if c.JoinPushOwn.Load() < 1 || c.JoinPushIDs.Load() != 1 {
			t.Fatalf("own %d, ids %d; want at least one own GRAFT and the one id offered", c.JoinPushOwn.Load(), c.JoinPushIDs.Load())
		}
		if h := joinPushHeld(b); fmt.Sprint(h) != "[1]" {
			t.Fatalf("B holds %v; want [1], pulled on the offer at A's own GRAFT", h)
		}
		order.mu.Lock()
		seq := slices.Clone(order.seq[b.host.ID()])
		order.mu.Unlock()
		if len(seq) < 2 || seq[0] != "G" || seq[1] != "I" {
			t.Fatalf("A's GRAFTs and IHAVEs of the topic to B, in queue order: %v; want the GRAFT first, then the offer", seq)
		}
	})
}

// TestSeacastSourceSingleOffer: with a mesh between A and B (join_push off), A's cell is pushed to
// B and not also offered it: the source's overlap counter stays 0 on a live dispatch.
func TestSeacastSourceSingleOffer(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		psubs, topic := newJoinPushFixture(t, ctx, false, 2, 2)
		a, b := psubs[0], psubs[1]
		if _, err := b.Subscribe(topic); err != nil {
			t.Fatal(err)
		}
		connect(t, a.host, b.host)
		time.Sleep(2500 * time.Millisecond) // the mesh forms at the heartbeats
		var inMesh bool
		joinPushRun(a, func(gs *GossipSubRouter) { _, inMesh = gs.mesh[topic][b.host.ID()] })
		if !inMesh {
			t.Fatal("B is not in A's mesh after the heartbeats")
		}
		if err := a.Publish(topic, joinPushCell(1)); err != nil {
			t.Fatal(err)
		}
		time.Sleep(300 * time.Millisecond)
		if h := joinPushHeld(b); fmt.Sprint(h) != "[1]" {
			t.Fatalf("B holds %v; want [1], pushed", h)
		}
		if n := a.rt.(*GossipSubRouter).seacast.ctr.SourceOfferOverlap.Load(); n != 0 {
			t.Fatalf("the source offered %d chunks to a peer it pushed them; want 0", n)
		}
	})
}

// TestSeacastSourceOverlapCounts is the overlap counter's positive control: a doubled announce,
// a peer both pushed and offered, is counted; the single offer is not.
func TestSeacastSourceOverlapCounts(t *testing.T) {
	p1, p2 := peer.ID("p1"), peer.ID("p2")
	tosend := map[peer.ID]struct{}{p2: {}}
	if n := seacastSourceOverlap([]peer.ID{p1, p2}, tosend); n != 1 {
		t.Fatalf("doubled announce counted %d; want 1", n)
	}
	if n := seacastSourceOverlap([]peer.ID{p1}, tosend); n != 0 {
		t.Fatalf("single offer counted %d; want 0", n)
	}
}

// TestSeacastGossipCap: a typed job's ids of a topic are cut to MaxIHaveLength, drawn. With HAVE
// bitmaps announced, a group's ids go out as one bitmap and none is cut, while the ids outside any
// group are; without them, every id counts. The ids handed in are left as they were.
func TestSeacastGossipCap(t *testing.T) {
	gs := &GossipSubRouter{params: DefaultGossipSubParams(), have: newHaveState(seacastTestIndex)}
	gs.params.MaxIHaveLength = 4
	root := strings.Repeat("g", haveRootLen)
	var mids []string
	for i := range 10 {
		mids = append(mids, root+string(byte(i))) // chunk ids of one group
	}
	for i := range 6 {
		mids = append(mids, fmt.Sprintf("plain-%d", i)) // ids outside any group
	}
	in := append([]string(nil), mids...)
	handed := map[string]bool{}
	for _, mid := range in {
		handed[mid] = true
	}
	// kept counts out's grouped and plain ids, each handed in and none repeated.
	kept := func(out []string) (grouped, plain int) {
		t.Helper()
		seen := map[string]bool{}
		for _, mid := range out {
			if seen[mid] || !handed[mid] {
				t.Fatalf("id %q repeated or not handed in: %q", mid, out)
			}
			seen[mid] = true
			if strings.HasPrefix(mid, "plain-") {
				plain++
			} else {
				grouped++
			}
		}
		return grouped, plain
	}
	gs.have.ihaveOn = true
	if g, p := kept(gs.seacastGossipCap(mids)); g != 10 || p != 4 {
		t.Fatalf("with bitmaps, kept %d grouped and %d plain ids; want 10 and 4", g, p)
	}
	// The plain ids kept are drawn: over 32 cuts, each of the six is kept at least once (a fixed cut
	// keeps the same four; a draw leaves one out of all 32 with odds of 6·(1/3)^32).
	drawn := map[string]bool{}
	for range 32 {
		for _, mid := range gs.seacastGossipCap(mids) {
			if strings.HasPrefix(mid, "plain-") {
				drawn[mid] = true
			}
		}
	}
	if len(drawn) != 6 {
		t.Fatalf("over 32 cuts, %d of the 6 plain ids were ever kept; want all 6, drawn", len(drawn))
	}
	gs.have.ihaveOn = false
	for range 32 {
		if g, p := kept(gs.seacastGossipCap(mids)); g+p != 4 {
			t.Fatalf("without bitmaps, kept %d grouped and %d plain ids; want 4 in all", g, p)
		}
	}
	for i := range mids {
		if mids[i] != in[i] {
			t.Fatalf("the ids handed in were changed at %d: %q", i, mids)
		}
	}
}

// TestSeacastGossipCapBeyondBitmap: with HAVE bitmaps announced, an id whose index is MAX_PLAIN_CHUNKS
// or more is not offered: no bitmap carries it, and a receiver's offer guard drops it as an id. Its
// group's ids below the bound are kept, and so is the id at the bound's last index.
func TestSeacastGossipCapBeyondBitmap(t *testing.T) {
	root := strings.Repeat("b", haveRootLen)
	byIndex := map[string]uint32{}
	id := func(i uint32) string {
		mid := fmt.Sprintf("%s/%d", root, i)
		byIndex[mid] = i
		return mid
	}
	index := func(mid string) (string, uint32, bool) {
		i, ok := byIndex[mid]
		return root, i, ok
	}
	gs := &GossipSubRouter{params: DefaultGossipSubParams(), have: newHaveState(index)}
	gs.have.ihaveOn = true
	last, at, past := id(core.MaxPlainChunks-1), id(core.MaxPlainChunks), id(core.MaxPlainChunks+808)
	out := gs.seacastGossipCap([]string{id(0), last, at, past, id(1)})
	if want := []string{id(0), last, id(1)}; fmt.Sprint(out) != fmt.Sprint(want) {
		t.Fatalf("kept %q; want %q", out, want)
	}
	var b haveBits
	for _, mid := range out {
		_, i, _ := index(mid)
		b.Set(i)
	}
	if b.Count() != len(out) {
		t.Fatalf("a bitmap of the kept ids holds %d indices; want %d, every kept id", b.Count(), len(out))
	}
}

// TestSeacastGossipJobBitmapOnTheWire: the heartbeat's typed job, released by the pacer, reaches the
// peer as an IHAVE whose bitmap names every index of the group past MaxIHaveLength: ten cells of one
// group, against a limit of four. What A's router writes to B is read from the queue it writes into.
func TestSeacastGossipJobBitmapOnTheWire(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		psubs, pathTopics, _ := newGossipFixture(t, ctx, 1, 0)
		a, b := psubs[0], psubs[1].host.ID()
		root := strings.Repeat("g", haveRootLen-1) + string(rune(0)) // column 0's topic
		q := newRpcQueue(256)
		var real *rpcQueue
		joinPushRun(a, func(gs *GossipSubRouter) {
			gs.have.ihaveOn = true // as WithHaveAnnounceBitmaps sets it
			gs.params.MaxIHaveLength = 4
			real = gs.p.peers[b]
			gs.p.peers[b] = q // what A writes to B, kept here
		})
		for i := range 10 {
			if err := a.Publish(pathTopics[0], append([]byte(root+string(byte(i))), []byte(strings.Repeat("x", 64))...)); err != nil {
				t.Fatal(err)
			}
		}
		time.Sleep(time.Second + 100*time.Millisecond) // a heartbeat, and its job released
		joinPushRun(a, func(gs *GossipSubRouter) { gs.p.peers[b] = real })
		bitmaps, most := 0, 0
		for {
			popCtx, popCancel := context.WithTimeout(ctx, time.Millisecond)
			rpc, err := q.Pop(popCtx)
			popCancel()
			if err != nil {
				break
			}
			for _, ih := range rpc.GetControl().GetIhave() {
				for _, mid := range ih.GetMessageIDs() {
					if r, bm, ok := decodeHave(mid); ok && r == root {
						bitmaps++
						most = max(most, core.Bits(bm).Count())
					}
				}
			}
		}
		if bitmaps < 1 || most != 10 {
			t.Fatalf("A wrote %d bitmaps of the group to B, the largest naming %d indices; want at least one, naming all 10", bitmaps, most)
		}
	})
}

// TestSeacastGossipJobKeepsGroups: with HAVE bitmaps announced, the heartbeat's typed job carries
// every id of a group past MaxIHaveLength, since the group goes out as one bitmap: ten cells of one
// group, against a limit of four, give each job ten ids.
func TestSeacastGossipJobKeepsGroups(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		psubs, pathTopics, _ := newGossipFixture(t, ctx, 1, 0)
		a := psubs[0]
		joinPushRun(a, func(gs *GossipSubRouter) {
			gs.have.ihaveOn = true // as WithHaveAnnounceBitmaps sets it
			gs.params.MaxIHaveLength = 4
		})
		ctr := a.rt.(*GossipSubRouter).seacast.ctr
		root := strings.Repeat("g", haveRootLen-1) + string(rune(0)) // column 0's topic
		for i := range 10 {
			if err := a.Publish(pathTopics[0], append([]byte(root+string(byte(i))), []byte(strings.Repeat("x", 64))...)); err != nil {
				t.Fatal(err)
			}
		}
		time.Sleep(100 * time.Millisecond)
		jobs, offered := ctr.GossipJobs.Load(), ctr.GossipIdsOffered.Load()
		time.Sleep(time.Second + 100*time.Millisecond) // a heartbeat
		jobs, offered = ctr.GossipJobs.Load()-jobs, ctr.GossipIdsOffered.Load()-offered
		if jobs < 1 || offered != 10*jobs {
			t.Fatalf("%d typed jobs offered %d ids; want at least one job, each with the group's 10", jobs, offered)
		}
	})
}
