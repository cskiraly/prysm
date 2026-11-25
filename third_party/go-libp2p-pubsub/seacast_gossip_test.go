package pubsub

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
)

// gossipFixture: two routers, A the publisher and B joined to every topic but kept out of A's
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
func newJoinPushFixture(t *testing.T, ctx context.Context, joinPush bool, aD, bD int) (psubs []*PubSub, topic string) {
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
		if e := gs.have.mine[strings.Repeat("g", haveRootLen-1)+string(rune(0))]; e != nil {
			for i := uint32(0); i < 8; i++ {
				if e.bits.Has(i) {
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
						gs.have.peers[bid] = map[string]*haveEntry{}
					}
					he := &haveEntry{ttl: 100}
					he.bits.Set(2)
					gs.have.peers[bid][root] = he
					if gs.have.pushed[bid] == nil {
						gs.have.pushed[bid] = map[string]*haveEntry{}
					}
					pe := &haveEntry{ttl: 100, first: time.Now()}
					pe.bits.Set(3)
					gs.have.pushed[bid][root] = pe
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

// TestSeacastJoinPushOwnGraft: A's own GRAFT at its heartbeat runs join_push at once, as gossipsub's
// wires have it (no answer comes): A holds a cell before B joins; B joins later and never grafts A
// (its degree is 0); A's heartbeat grafts B, offers it the cell, and B pulls it.
func TestSeacastJoinPushOwnGraft(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		psubs, topic := newJoinPushFixture(t, ctx, true, 2, 0)
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
