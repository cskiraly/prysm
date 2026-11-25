package pubsub

import (
	"context"
	"strings"
	"testing"
	"time"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
)

// originAckedCell: a source S (router 0) and three leaves X, Y, Z (1..3), each linked to S only, all in
// S's mesh for one column topic on the path, R_NODE = rnode. The leaves are control-muted: they send no
// holding at all, so no peer's HAVE acknowledges S's chunk unless the test injects one. hose adds S's
// window (timeout 500 ms), the other acknowledgement.
type originAckedCell struct {
	pubsubs []*PubSub
	hosts   []host.Host
	topic   *Topic
}

func newOriginAckedCell(t *testing.T, hose bool, rnode int) *originAckedCell {
	t.Helper()
	c := &originAckedCell{}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	c.hosts = getDefaultHosts(t, 4)
	params := DefaultGossipSubParams()
	params.IDontWantMessageThreshold = 1
	c.pubsubs = getGossipsubsOptFn(ctx, c.hosts, func(i int, h host.Host) []Option {
		T := &testObjectType{name: "col", params: TypeParams{RNode: rnode}, topics: []string{"seg/col/0"}, store: NewMapCellStore()}
		opts := []Option{
			WithGossipSubParams(params),
			WithMessageIdFn(seacastTestID),
			WithHaveBitmaps(seacastTestIndex),
			WithHaveAnnounceBitmaps(),
			WithPhaseForwarding(func(topic string) bool { return topic != "seg/col/0" }, 2),
			WithSeacast([]ObjectType{T}, nil),
		}
		if i != 0 {
			opts = append(opts, WithControlMute(nil))
		} else if hose {
			opts = append(opts, WithHoseWindow(HoseConfig{Init: 8, Min: 1, Max: 8, Kp: 1, Timeout: 500 * time.Millisecond}))
		}
		return opts
	})
	for i, ps := range c.pubsubs {
		tp, err := ps.Join("seg/col/0")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tp.Subscribe(); err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			c.topic = tp
		}
	}
	for i := 1; i < 4; i++ {
		connect(t, c.hosts[0], c.hosts[i])
	}
	time.Sleep(2 * time.Second)
	return c
}

func (c *originAckedCell) read(i int, f func(gs *GossipSubRouter)) {
	gs := c.pubsubs[i].rt.(*GossipSubRouter)
	done := make(chan struct{})
	gs.p.eval <- func() { f(gs); close(done) }
	<-done
}

// state reads, at leaf i, whether it holds index ix of root and whether S's holding names it.
func (c *originAckedCell) state(i int, root string, ix uint32) (holds, srcNames bool) {
	S := c.hosts[0].ID()
	c.read(i, func(gs *GossipSubRouter) {
		if e := gs.have.mine[root]; e != nil {
			holds = e.bits.Has(ix)
		}
		if e := gs.have.peers[S][root]; e != nil {
			srcNames = e.bits.Has(ix)
		}
	})
	return
}

// TestSeacastSourceOffersAtAck is v0.9's source at the acknowledgement (seacast-lite.md §5.1's publish and
// origin_acked, §7.1's window_timeout; seacast.md §4.7's publish, origin_acked and source_pool; spec
// 43fe953): the source's holding names a chunk only once it is acknowledged, by a peer's HAVE naming it or
// else by its first copy's window timeout; a push target's first chunk is led by a holding naming it; and
// the mesh peers not chosen to be pushed the chunk are offered it at its acknowledgement, not at publish.
// Leg peer-have (one push): the acknowledgement is an injected HAVE from the push target. Leg
// window-timeout (two pushes, the hose's 500 ms timeout): the first copy's timeout acknowledges the chunk
// and the second copy's timeout acknowledges nothing new (OriginAcked 1).
func TestSeacastSourceOffersAtAck(t *testing.T) {
	for _, leg := range []string{"peer-have", "window-timeout"} {
		t.Run(leg, func(t *testing.T) {
			synctestTest(t, func(t *testing.T) {
				rnode := 1
				if leg == "window-timeout" {
					rnode = 2
				}
				c := newOriginAckedCell(t, leg == "window-timeout", rnode)
				root := strings.Repeat("o", haveRootLen)
				const ix = 5
				var inMesh int
				c.read(0, func(gs *GossipSubRouter) { inMesh = len(gs.mesh["seg/col/0"]) })
				if inMesh != 3 {
					t.Fatalf("S's mesh has %d peers; want 3", inMesh)
				}
				var batch MessageBatch
				if err := c.topic.AddToBatch(context.Background(), &batch, seacastCell(root, ix)); err != nil {
					t.Fatal(err)
				}
				if err := c.pubsubs[0].PublishBatch(&batch); err != nil {
					t.Fatal(err)
				}
				time.Sleep(100 * time.Millisecond) // well inside the window timeout; no peer has acknowledged
				ctrS := c.pubsubs[0].rt.(*GossipSubRouter).seacast.ctr
				t.Logf("S at dispatch: pushed %d, offered %d", ctrS.Pushed.Load(), ctrS.Offered.Load())
				// The push target: the leaf S's dispatch pushed (S's pushed set for the chunk).
				pushedSet := map[peer.ID]bool{}
				var pushed peer.ID
				c.read(0, func(gs *GossipSubRouter) {
					for p, roots := range gs.have.pushed {
						if e := roots[root]; e != nil && e.bits.Has(ix) {
							pushedSet[p] = true
							pushed = p
						}
					}
				})
				if len(pushedSet) != rnode {
					t.Fatalf("S pushed the chunk to %d leaves; want %d", len(pushedSet), rnode)
				}
				var unpushed []int
				bad := false
				for i := 1; i < 4; i++ {
					holds, names := c.state(i, root, ix)
					t.Logf("t+100ms leaf %d (pushed %v): holds %v, S's holding names it %v", i, pushedSet[c.hosts[i].ID()], holds, names)
					if pushedSet[c.hosts[i].ID()] {
						if !holds || !names {
							bad = true
							t.Errorf("the push target: holds %v, S's lead holding names the chunk %v; want true true", holds, names)
						}
						continue
					}
					unpushed = append(unpushed, i)
					if holds || names {
						bad = true
						t.Errorf("before any acknowledgement, unpushed leaf %d: holds %v (offered and pulled), S's holding names the chunk %v; want false false", i, holds, names)
					}
				}
				if bad {
					return
				}
				if leg == "peer-have" {
					c.read(0, func(gs *GossipSubRouter) {
						var b haveBits
						b.Set(ix)
						gs.handleIDontWant(pushed, &pb.ControlMessage{Idontwant: []*pb.ControlIDontWant{{MessageIDs: []string{encodeHave(root, &b)}}}})
					})
					time.Sleep(100 * time.Millisecond)
				} else {
					time.Sleep(600 * time.Millisecond) // past the pushed copy's 500 ms window timeout
				}
				for _, i := range unpushed {
					holds, names := c.state(i, root, ix)
					if !holds || !names {
						t.Errorf("after the acknowledgement, unpushed leaf %d: holds %v, S's holding names it %v; want true true", i, holds, names)
					}
				}
				if leg == "window-timeout" {
					time.Sleep(600 * time.Millisecond) // every copy's timeout has run
				}
				if n := ctrS.OriginAcked.Load(); n != 1 {
					t.Errorf("acknowledgements at S: %d; want 1 (a later copy's timeout acknowledges nothing new)", n)
				}
			})
		})
	}
}
