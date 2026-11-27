package pubsub

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p-pubsub/timecache"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
)

// The score's promise under v0.5's E and CloseWithConn. The gossip tracer's promise is the one the
// heartbeat charges as the behaviour penalty. Stock gossipsub opens it when an ask is dispatched,
// so an ask that waits in our own queue past the follow-up time, or that the queue refuses,
// charges the peer asked, which never received it; and a departed peer's promises stay to be
// charged. Under E the promise opens at the write, as the park's does (specv05.go,
// promisesFromWrite). Under CloseWithConn a grouped ask's promise closes with the connection, as
// the park's does (v05PeerGone); a stock ask's stays, as upstream keeps it.

// scoreParams is v05Base's gossipsub parameters with a follow-up time of 500 ms, shorter than the
// asker's window timeout in TestSpecV05ScorePromiseAtWrite (1.5 s).
func scoreParams() GossipSubParams {
	params := DefaultGossipSubParams()
	params.Dlazy = 0
	params.GossipFactor = 0
	params.IWantFollowupTime = 500 * time.Millisecond
	return params
}

// TestSpecV05ScorePromiseAtWrite is TestSpecV05PromiseAtWrite for the score: the asker's asks wait
// behind its own frame for the window's timeout (1.5 s), and the muted source answers each ask it
// receives at once. A promise from the dispatch breaks at 500 ms and the heartbeat charges the
// source; under E nothing is charged. (A queue that refuses the ask is
// TestSpecV05ScorePromiseRefused's: over a network, which ask's promise upstream's sampling
// tracks, and whether a heartbeat falls before the retry's answer, decide whether the refusal
// shows.)
func TestSpecV05ScorePromiseAtWrite(t *testing.T) {
	asker, source := strings.Repeat("c", 32), strings.Repeat("d", 32)
	run := func(t *testing.T, on, silent bool) (got, charged int, ctr *SpecV05Counters) {
		const own = 2 // frame 0 takes the window's one slot, frame 1 waits behind it
		ctr = &SpecV05Counters{}
		synctestTest(t, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			hosts := v05Hosts(t, 2, 20, time.Millisecond)
			psubs := getGossipsubsOptFn(ctx, hosts, func(i int, _ host.Host) []Option {
				opts := append(v05Base(nil), WithGossipSubParams(scoreParams()))
				if i == 0 { // the asker
					opts = append(opts, WithPromiseTracking(), WithHoseWindow(HoseConfig{Target: 20 * time.Millisecond,
						Kp: 1, Init: 1, Min: 1, Max: 1, Timeout: 1500 * time.Millisecond, FrameSlots: true}))
				} else { // the muted source: never acknowledges the asker's frames
					opts = append(opts, WithPhaseSourcePushQuota(1), WithControlMute(nil))
					if silent { // nor answers an ask
						opts = append(opts, WithIWantWithholding(0, nil))
					}
				}
				if on {
					opts = append(opts, WithSpecV05(SpecV05{PromiseAtWrite: true}, ctr))
				}
				return opts
			})
			topics := v05Join(t, psubs)
			c := newV05Collect(ctx, t, topics)
			connectAll(t, hosts)
			time.Sleep(2 * time.Second)
			// The asker's pushes first.
			var mine MessageBatch
			for i := byte(0); i < byte(own); i++ {
				if err := topics[0].AddToBatch(ctx, &mine, v05Frame(asker, i)); err != nil {
					t.Fatal(err)
				}
			}
			if err := psubs[0].PublishBatch(&mine); err != nil {
				t.Fatal(err)
			}
			time.Sleep(10 * time.Millisecond)
			// Then the source offers the asker its group: the asks wait behind the asker's frame 1.
			var theirs MessageBatch
			for i := byte(0); i < 8; i++ {
				if err := topics[1].AddToBatch(ctx, &theirs, v05Frame(source, i)); err != nil {
					t.Fatal(err)
				}
			}
			if err := psubs[1].PublishBatch(&theirs); err != nil {
				t.Fatal(err)
			}
			time.Sleep(4 * time.Second)
			got = c.count(0, source)
			ch, _ := psubs[0].BrokenPromises()
			charged = ch[hosts[1].ID()]
		})
		return got, charged, ctr
	}
	offGot, off, _ := run(t, false, false)
	onGot, on, _ := run(t, true, false)
	t.Logf("from the dispatch: %d frames, %d promises charged to the source; with E: %d frames, %d charged",
		offGot, off, onGot, on)
	if off == 0 {
		t.Fatal("known answer: a promise from the dispatch should break while the ask waits, and charge the source")
	}
	if on != 0 {
		t.Fatalf("with E no promise should be charged for an ask that had not left, got %d", on)
	}
	if onGot != 8 {
		t.Fatalf("with E the asker should still get the source's 8 frames, got %d", onGot)
	}
	// No promise opens at the write above, in the runs seen so far; why was not established, and
	// the verdict is the same either way, since a delivery clears a pending promise and an open one
	// alike. A source that answers nothing shows that one does open, and that under E a peer that
	// does break its promise is still charged.
	_, silent, ctr := run(t, true, true)
	t.Logf("with E, a source that answers no ask: %d charged, %d opened at the write", silent, ctr.ScorePromisesAtWrite.Load())
	if ctr.ScorePromisesAtWrite.Load() == 0 || silent == 0 {
		t.Fatalf("with E the written asks to a silent source should open promises and be charged, got %d opened and %d charged",
			ctr.ScorePromisesAtWrite.Load(), silent)
	}
}

// TestSpecV05ScorePromiseRefused: an ask dispatched to a peer whose queue is full is refused and
// dropped (sendRPC, doDropRPC). A promise from the dispatch is charged after the follow-up time,
// though the peer never received the ask; under E nothing is charged, and nothing is left pending.
func TestSpecV05ScorePromiseRefused(t *testing.T) {
	p := peer.ID("server")
	mid := testMid(strings.Repeat("j", 32), 3)
	run := func(t *testing.T, rules SpecV05) (charged int, gs *GossipSubRouter) {
		synctest.Test(t, func(t *testing.T) {
			gs = scoreRouter(rules)
			gs.logger = slog.New(slog.DiscardHandler)
			q := newRpcQueue(1)
			if err := q.Push(&RPC{RPC: pb.RPC{Publish: []*pb.Message{{Data: []byte("a frame")}}}}, false); err != nil {
				t.Fatal(err)
			}
			gs.p.peers = map[peer.ID]*rpcQueue{p: q}
			gs.p.maxMessageSize, gs.p.maxControlMessageSize = DefaultMaxMessageSize, DefaultMaxControlMessageSize
			gs.gossipTracer.AddPromise(p, []string{mid}) // the dispatch, as handleIHave and the retries do
			ask := &RPC{RPC: pb.RPC{Control: &pb.ControlMessage{Iwant: []*pb.ControlIWant{{MessageIDs: []string{mid}}}}}}
			if gs.sendRPC(p, ask, false) {
				t.Fatal("fixture: the full queue should refuse the ask")
			}
			time.Sleep(150 * time.Millisecond) // past the follow-up time
			charged = gs.gossipTracer.GetBrokenPromises()[p]
		})
		return charged, gs
	}
	if n, _ := run(t, SpecV05{}); n != 1 {
		t.Fatalf("known answer: from the dispatch the refused ask's promise is charged, got %d", n)
	}
	n, gs := run(t, SpecV05{PromiseAtWrite: true})
	if n != 0 {
		t.Fatalf("with E the refused ask should charge nothing, got %d", n)
	}
	if b, o := gs.p.BrokenPromises(); len(b) != 0 || len(o) != 0 {
		t.Fatalf("with E nothing should be charged or outstanding, got %v and %v", b, o)
	}
	if len(gs.gossipTracer.pending) != 0 {
		t.Fatalf("with E the refused ask should leave nothing pending, got %v", gs.gossipTracer.pending)
	}
}

// scoreRouter is a router with a gossip tracer of follow-up time 100 ms, the HAVE index of the
// test ids (so testMid's ids are grouped) and the given v0.5 rules, its tracer started as Attach
// starts it.
func scoreRouter(rules SpecV05) *GossipSubRouter {
	gs := newParkRouter(true)
	gs.p = &PubSub{idGen: newMsgIdGenerator()}
	gs.params.IWantFollowupTime = 100 * time.Millisecond
	gs.have = newHaveState(testIndex)
	gs.v05 = &specV05{SpecV05: rules, ctr: &SpecV05Counters{}}
	gs.gossipTracer = newGossipTracer()
	gs.gossipTracer.Start(gs)
	return gs
}

// TestSpecV05ScorePromiseClosesWithConn: an ask of a chunk and a stock ask, each from the same
// peer, whose connection then closes. Under the chunk pull the chunk's promise closes with it; the
// stock ask's stays to be charged, as upstream keeps it. Under E an ask not yet written goes with
// the queue, whatever its kind.
func TestSpecV05ScorePromiseClosesWithConn(t *testing.T) {
	p := peer.ID("server")
	chunk, stock := testMid(strings.Repeat("g", 32), 1), "a stock message id"
	charged := func(gs *GossipSubRouter) int {
		time.Sleep(150 * time.Millisecond) // past the follow-up time
		return gs.gossipTracer.GetBrokenPromises()[p]
	}
	// left says which of the two asks' promises are still open to p, before the follow-up time.
	left := func(gs *GossipSubRouter) (chunkLeft, stockLeft bool) {
		gs.gossipTracer.Lock()
		defer gs.gossipTracer.Unlock()
		_, chunkLeft = gs.gossipTracer.promises[chunk][p]
		_, stockLeft = gs.gossipTracer.promises[stock][p]
		return chunkLeft, stockLeft
	}
	t.Run("the chunk pull: the chunk's written ask closes, the stock ask's stays", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			gs := scoreRouter(SpecV05{})
			gs.offerTable = make(map[string]map[peer.ID]time.Time) // with the park per group: the core pulls chunks
			if gs.parkScopeOf(chunk) == "" || gs.parkScopeOf(stock) != "" {
				t.Fatal("fixture: the chunk's id should be grouped and the stock id not")
			}
			gs.gossipTracer.AddPromise(p, []string{chunk})
			gs.gossipTracer.AddPromise(p, []string{stock})
			gs.v05PeerGone(p)
			if c, st := left(gs); c || !st {
				t.Fatalf("open after the close: the chunk's %v, the stock ask's %v; want false, true", c, st)
			}
			if n := charged(gs); n != 1 || gs.v05.ctr.ClosedScorePromises.Load() != 1 {
				t.Fatalf("charged %d and closed %d, want 1 and 1: the stock ask's charged, the chunk's closed",
					n, gs.v05.ctr.ClosedScorePromises.Load())
			}
		})
	})
	t.Run("the chunk pull without WithSpecV05: the same", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			gs := scoreRouter(SpecV05{})
			gs.v05 = nil
			gs.offerTable = make(map[string]map[peer.ID]time.Time)
			gs.gossipTracer.AddPromise(p, []string{chunk})
			gs.gossipTracer.AddPromise(p, []string{stock})
			gs.v05PeerGone(p)
			if c, st := left(gs); c || !st {
				t.Fatalf("open after the close: the chunk's %v, the stock ask's %v; want false, true", c, st)
			}
			if n := charged(gs); n != 1 {
				t.Fatalf("charged %d, want 1: the stock ask's charged, the chunk's closed", n)
			}
		})
	})
	t.Run("E: asks not yet written go with the queue", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			gs := scoreRouter(SpecV05{PromiseAtWrite: true})
			gs.gossipTracer.AddPromise(p, []string{chunk})
			gs.gossipTracer.AddPromise(p, []string{stock})
			gs.v05PeerGone(p)
			if len(gs.gossipTracer.pending) != 0 {
				t.Fatalf("pending left after the connection closed: %v", gs.gossipTracer.pending)
			}
			if n := charged(gs); n != 0 {
				t.Fatalf("charged %d, want 0: neither ask was written", n)
			}
		})
	})
}

// TestSeacastRepeatOfferWithoutPath: a router that pulls chunks through the core without the
// SEACAST path (no WithSeacast) gets a second offer of a chunk whose ask to the same peer is open.
// The offer is recorded and not asked, and nothing reads the path's counters.
func TestSeacastRepeatOfferWithoutPath(t *testing.T) {
	seen := timecache.NewTimeCache(time.Minute) // its sweeper runs outside the bubble
	synctest.Test(t, func(t *testing.T) {
		p := peer.ID("server")
		mid := testMid(strings.Repeat("g", 32), 1)
		gs := scoreRouter(SpecV05{})
		gs.v05 = nil
		gs.offerTable = make(map[string]map[peer.ID]time.Time)
		gs.p.seenMessages = seen
		_, g, i, ok := gs.pullChunk(mid)
		if !ok || gs.seacast != nil {
			t.Fatalf("fixture: chunk %v, the path %v; want a chunk without the path", ok, gs.seacast != nil)
		}
		gs.pull.promises.Open(g, i, p, time.Now())
		iwant := map[string]string{}
		var candidates []string
		gs.considerAnnounced(p, "seg/col/0", mid, iwant, &candidates)
		if !gs.pullOffered(p, mid) || len(iwant) != 0 || len(candidates) != 0 {
			t.Fatalf("offered %v, asks %v %v; want true and none", gs.pullOffered(p, mid), iwant, candidates)
		}
	})
}

// TestScorePromisePending is the tracer's side of E: a dispatched ask's promise waits for its
// write, opens with the follow-up time from the write, and goes uncharged when the ask is refused,
// delivered, ended with its group, throttled or never reported.
func TestScorePromisePending(t *testing.T) {
	p, q := peer.ID("server"), peer.ID("other")
	root := strings.Repeat("h", 32)
	mid, other := testMid(root, 1), testMid(strings.Repeat("i", 32), 2)
	empty := func(t *testing.T, gt *gossipTracer) {
		t.Helper()
		if len(gt.pending) != 0 || len(gt.peerPending) != 0 {
			t.Fatalf("pending left: %v %v", gt.pending, gt.peerPending)
		}
	}
	t.Run("the deadline runs from the write", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			gt := scoreRouter(SpecV05{PromiseAtWrite: true}).gossipTracer
			gt.AddPromise(p, []string{mid})
			time.Sleep(time.Second) // held in the queue ten follow-up times
			if n := gt.GetBrokenPromises()[p]; n != 0 {
				t.Fatalf("charged %d before the write", n)
			}
			if n := gt.promisesWritten(p, []string{mid, other}, nextAskSeq(), time.Now()); n != 1 {
				t.Fatalf("opened %d at the write, want 1 (the id not dispatched opens nothing)", n)
			}
			time.Sleep(90 * time.Millisecond)
			if n := gt.GetBrokenPromises()[p]; n != 0 {
				t.Fatalf("charged %d within the follow-up time of the write", n)
			}
			time.Sleep(20 * time.Millisecond)
			if n := gt.GetBrokenPromises()[p]; n != 1 {
				t.Fatalf("charged %d past the follow-up time of the write, want 1", n)
			}
			empty(t, gt)
		})
	})
	t.Run("an ask refused, delivered, ended with its group or throttled opens nothing", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			gt := scoreRouter(SpecV05{PromiseAtWrite: true}).gossipTracer
			gt.AddPromise(p, []string{mid})
			gt.asksRefused(p, []string{mid})
			gt.AddPromise(q, []string{other})
			gt.fulfillPromiseID(other)
			empty(t, gt)
			gt.AddPromise(p, []string{mid})
			gt.fulfillPromisesWhere(func(id string) bool { r, _, _ := testIndex(id); return r == root })
			empty(t, gt)
			gt.AddPromise(p, []string{mid})
			gt.ThrottlePeer(p)
			empty(t, gt)
			if n := gt.promisesWritten(p, []string{mid}, nextAskSeq(), time.Now()) + gt.promisesWritten(q, []string{other}, nextAskSeq(), time.Now()); n != 0 {
				t.Fatalf("opened %d at the write after the asks closed", n)
			}
			time.Sleep(150 * time.Millisecond)
			if b := gt.GetBrokenPromises(); len(b) != 0 {
				t.Fatalf("charged %v", b)
			}
		})
	})
	t.Run("a write never reported goes uncharged past its age limit", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			gt := scoreRouter(SpecV05{PromiseAtWrite: true}).gossipTracer
			gt.AddPromise(p, []string{mid})
			time.Sleep(promisePendingTTL - time.Second)
			gt.GetBrokenPromises()
			if len(gt.pending) != 1 {
				t.Fatal("a pending promise went before its age limit")
			}
			time.Sleep(2 * time.Second)
			if b := gt.GetBrokenPromises(); len(b) != 0 {
				t.Fatalf("charged %v", b)
			}
			empty(t, gt)
		})
	})
	t.Run("without E the promise is upstream's, from the dispatch", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			gt := scoreRouter(SpecV05{}).gossipTracer
			gt.AddPromise(p, []string{mid})
			gt.asksRefused(p, []string{mid})
			time.Sleep(150 * time.Millisecond)
			if n := gt.GetBrokenPromises()[p]; n != 1 {
				t.Fatalf("charged %d, want 1 as upstream", n)
			}
			empty(t, gt)
		})
	})
}

// heldStream is a stream whose every write waits for the test: Write takes its result from release.
type heldStream struct {
	network.Stream
	release chan error
}

func (s *heldStream) Write(b []byte) (int, error) {
	if err := <-s.release; err != nil {
		return 0, err
	}
	return len(b), nil
}
func (s *heldStream) SetWriteDeadline(time.Time) error { return nil }
func (s *heldStream) Conn() network.Conn               { return heldConn{} }
func (s *heldStream) Close() error                     { return nil }
func (s *heldStream) Reset() error                     { return nil }

type heldConn struct{ network.Conn }

func (heldConn) RemotePeer() peer.ID { return peer.ID("asked") }

// TestSpecV05AsksReportedAfterTheWrite: under E an ask's promise starts at its write. The writer
// reports an RPC's asks once the stream has taken it: while the write blocks nothing is reported,
// and a write that fails reports nothing at all (the peer is then declared dead, and what waits for
// the report goes with it, v05PeerGone and seacastPeerGone).
func TestSpecV05AsksReportedAfterTheWrite(t *testing.T) {
	run := func(t *testing.T, writeErr error) (during, after int32, late time.Duration) {
		synctest.Test(t, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			logger := slog.New(slog.DiscardHandler)
			p := &PubSub{idGen: newMsgIdGenerator(), logger: logger, rpcLogger: logger}
			q := newRpcQueue(8)
			var reports atomic.Int32
			var at atomic.Int64
			q.onAsks = func(_ []string, _ uint64, when time.Time) {
				reports.Add(1)
				at.Store(when.UnixNano())
			}
			s := &heldStream{release: make(chan error)}
			first := make(chan *RPC, 1)
			first <- &RPC{}
			done := make(chan struct{})
			go func() {
				p.handleSendingMessages(ctx, s, q, first)
				close(done)
			}()
			ask := &RPC{RPC: pb.RPC{Control: &pb.ControlMessage{Iwant: []*pb.ControlIWant{{MessageIDs: []string{"a", "b"}}}}}}
			if err := q.Push(ask, false); err != nil {
				t.Fatal(err)
			}
			time.Sleep(5 * time.Second) // the write blocks for five seconds
			synctest.Wait()
			during = reports.Load()
			released := time.Now()
			s.release <- writeErr
			synctest.Wait()
			after = reports.Load()
			if after > 0 {
				late = time.Unix(0, at.Load()).Sub(released)
			}
			cancel()
			<-done
		})
		return
	}
	t.Run("a write that blocks reports its asks when it returns", func(t *testing.T) {
		during, after, late := run(t, nil)
		if during != 0 {
			t.Fatalf("the asks were reported while the write blocked (%d reports)", during)
		}
		if after != 1 || late < 0 {
			t.Fatalf("want one report at the write's return, got %d, %v from it", after, late)
		}
	})
	t.Run("a write that fails reports nothing", func(t *testing.T) {
		if during, after, _ := run(t, errors.New("stream reset")); during != 0 || after != 0 {
			t.Fatalf("the asks of a failed write were reported: %d while blocked, %d after", during, after)
		}
	})
}

// TestScorePromiseByIndexFulfilled: an ask by index is fulfilled at the points where upstream
// fulfils an ask of the same chunk by its id. A reply that begins validation fulfils it, so one that
// validation then ignores breaks no promise; a rejection does too, but not one for the message's
// signature; a delivery does; and so does a copy already seen, which can answer an ask by index (B′'s
// last resort asks again for a chunk whose earlier copy was ignored). A reply of another index does not.
func TestScorePromiseByIndexFulfilled(t *testing.T) {
	p := peer.ID("server")
	root := strings.Repeat("j", 32)
	want := encodeWant(root, 3)
	reply := func(index byte) *Message { return &Message{Message: &pb.Message{}, ID: testMid(root, index)} }
	cases := []struct {
		name    string
		atWrite bool
		event   func(gt *gossipTracer)
		charged int
	}{
		{"the reply begins validation", false, func(gt *gossipTracer) { gt.ValidateMessage(reply(3)) }, 0},
		{"the reply is rejected", false, func(gt *gossipTracer) { gt.RejectMessage(reply(3), RejectValidationFailed) }, 0},
		{"the reply's signature is rejected", false, func(gt *gossipTracer) { gt.RejectMessage(reply(3), RejectInvalidSignature) }, 1},
		{"the reply is delivered", false, func(gt *gossipTracer) { gt.DeliverMessage(reply(3)) }, 0},
		{"a copy already seen answers it", false, func(gt *gossipTracer) { gt.DuplicateMessage(reply(3)) }, 0},
		{"another index begins validation", false, func(gt *gossipTracer) { gt.ValidateMessage(reply(4)) }, 1},
		{"a copy of another index already seen", false, func(gt *gossipTracer) { gt.DuplicateMessage(reply(4)) }, 1},
		{"under E, the reply begins validation before the write's report", true, func(gt *gossipTracer) {
			gt.ValidateMessage(reply(3))
			gt.promisesWritten(p, []string{want}, nextAskSeq(), time.Now())
		}, 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				gt := scoreRouter(SpecV05{PromiseAtWrite: c.atWrite}).gossipTracer
				gt.AddPromise(p, []string{want})
				c.event(gt)
				time.Sleep(time.Second) // ten follow-up times
				if n := gt.GetBrokenPromises()[p]; n != c.charged {
					t.Fatalf("charged %d, want %d", n, c.charged)
				}
			})
		})
	}
}

// TestSpecV05ScoreIgnoredReplyByIndex: the source pushes one frame of its group and offers the rest
// by bitmap; the asker asks by index, and its validator ignores every frame, as Prysm's does for a
// segment it has no room for or whose slot another group took first, neither the sender's fault.
// The group never completes, so nothing but the replies closes the asks' promises. Off SEACAST's
// path an arrival's effects run at Preprocess, before validation, so an ask by index is fulfilled
// there (haveArrived) and nothing is charged, with E or without, as an ask by id is fulfilled when
// its reply begins validation upstream. It is the control of TestSeacastScoreIgnoredReplyByIndex,
// on the path, where the effects wait for the verdict and the tracer's wantOf fulfils the ask.
func TestSpecV05ScoreIgnoredReplyByIndex(t *testing.T) {
	source := strings.Repeat("e", 32)
	for _, on := range []bool{false, true} {
		var charged int
		var seen atomic.Int32
		asks := &askCounter{}
		synctestTest(t, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			hosts := v05Hosts(t, 2, 20, time.Millisecond)
			psubs := getGossipsubsOptFn(ctx, hosts, func(i int, _ host.Host) []Option {
				opts := append(v05Base(nil), WithGossipSubParams(scoreParams()))
				if i == 0 { // the asker
					opts = append(opts, WithPromiseTracking(), WithRawTracer(asks))
					if on {
						opts = append(opts, WithSpecV05(SpecV05{PromiseAtWrite: true}, &SpecV05Counters{}))
					}
				} else { // the source
					opts = append(opts, WithPhaseSourcePushQuota(1))
				}
				return opts
			})
			if err := psubs[0].RegisterTopicValidator("t", func(context.Context, peer.ID, *Message) ValidationResult {
				seen.Add(1)
				return ValidationIgnore
			}); err != nil {
				t.Fatal(err)
			}
			topics := v05Join(t, psubs)
			newV05Collect(ctx, t, topics)
			connectAll(t, hosts)
			time.Sleep(2 * time.Second)
			var theirs MessageBatch
			for i := byte(0); i < 8; i++ {
				if err := topics[1].AddToBatch(ctx, &theirs, v05Frame(source, i)); err != nil {
					t.Fatal(err)
				}
			}
			if err := psubs[1].PublishBatch(&theirs); err != nil {
				t.Fatal(err)
			}
			time.Sleep(4 * time.Second)
			ch, _ := psubs[0].BrokenPromises()
			charged = ch[hosts[1].ID()]
		})
		t.Logf("E %v: %d asks by index sent, %d frames validated, %d promises charged to the source",
			on, asks.byIndex.Load(), seen.Load(), charged)
		if asks.byIndex.Load() == 0 {
			t.Fatalf("E %v: the asker should ask by index", on)
		}
		if seen.Load() < 2 {
			t.Fatalf("E %v: the asker should validate the push and at least one answer, got %d", on, seen.Load())
		}
		if charged != 0 {
			t.Fatalf("E %v: an honest source whose replies were ignored was charged %d promises", on, charged)
		}
	}
}

// askCounter counts the asks by index a node sends.
type askCounter struct {
	mockRawTracer
	byIndex atomic.Int32
}

func (a *askCounter) SendRPC(rpc *RPC, _ peer.ID) {
	for _, w := range rpc.GetControl().GetIwant() {
		for _, mid := range w.GetMessageIDs() {
			if _, _, ok := decodeWant(mid); ok {
				a.byIndex.Add(1)
			}
		}
	}
}

// TestSeacastScoreIgnoredReplyByIndex is TestSpecV05ScoreIgnoredReplyByIndex on SEACAST's path,
// where an arrival's effects wait for its verdict (seacastPreprocess): A publishes two cells of a
// group before B is connected, then offers them to B by bitmap; B asks by index, A serves them from
// its cache, and B's validator ignores them, as Prysm's does for a segment it has no room for or
// whose slot another group took first. The group never completes at B. Each ask is fulfilled when
// its reply begins validation, as an ask by id is upstream, so A is charged nothing; with the v0.5
// rules Prysm runs and without.
func TestSeacastScoreIgnoredReplyByIndex(t *testing.T) {
	for _, rules := range []*SpecV05{nil, {PromiseAtWrite: true}} {
		var charged int
		var seen atomic.Int32
		asks := &askCounter{}
		synctestTest(t, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			root := strings.Repeat("s", haveRootLen)
			topic := "seg/col/0"
			hosts := getDefaultHosts(t, 2)
			params := DefaultGossipSubParams()
			params.IDontWantMessageThreshold = 1
			params.IWantFollowupTime = 500 * time.Millisecond
			psubs := getGossipsubsOptFn(ctx, hosts, func(i int, h host.Host) []Option {
				T := &testObjectType{name: "col", params: TypeParams{RNode: 2}, topics: []string{topic}, store: NewMapCellStore()}
				opts := []Option{
					WithGossipSubParams(params),
					WithMessageIdFn(seacastTestID),
					WithHaveBitmaps(seacastTestIndex),
					WithPhaseForwarding(func(tp string) bool { return tp != topic }, 2),
					WithIWantDiscipline(200 * time.Millisecond),
					WithIHaveCommitmentPark(1, 400*time.Millisecond, 30*time.Second, nil, nil),
					WithIHaveParkPerGroup(nil),
					WithOfferTable(nil),
					WithRequestObservation(),
					WithPromiseTracking(),
				}
				if i == 1 { // B, the asker
					opts = append(opts, WithRawTracer(asks))
				}
				if rules != nil {
					opts = append(opts, WithSpecV05(*rules, nil))
				}
				return append(opts, WithHaveAnnounceBitmaps(), WithSeacast([]ObjectType{T}, nil))
			})
			if err := psubs[1].RegisterTopicValidator(topic, func(context.Context, peer.ID, *Message) ValidationResult {
				seen.Add(1)
				return ValidationIgnore
			}); err != nil {
				t.Fatal(err)
			}
			for _, ps := range psubs {
				if _, err := ps.Subscribe(topic); err != nil {
					t.Fatal(err)
				}
			}
			cell := func(index byte) []byte { return append([]byte(root+string(index)), []byte(strings.Repeat("x", 64))...) }
			for _, i := range []byte{5, 6} {
				if err := psubs[0].Publish(topic, cell(i)); err != nil {
					t.Fatal(err)
				}
			}
			time.Sleep(time.Second)
			connect(t, hosts[0], hosts[1])
			time.Sleep(2 * time.Second)
			gsA := psubs[0].rt.(*GossipSubRouter)
			var bits haveBits
			bits.Set(5)
			bits.Set(6)
			done := make(chan struct{})
			gsA.p.eval <- func() {
				gsA.sendRPC(hosts[1].ID(), rpcWithControl(nil, []*pb.ControlIHave{{TopicID: &topic, MessageIDs: []string{encodeHave(root, &bits)}}}, nil, nil, nil, nil), false)
				close(done)
			}
			<-done
			time.Sleep(3 * time.Second) // six follow-up times, three heartbeats
			ch, _ := psubs[1].BrokenPromises()
			charged = ch[hosts[0].ID()]
		})
		t.Logf("rules %+v: %d asks by index, %d answers validated, %d promises charged to A", rules, asks.byIndex.Load(), seen.Load(), charged)
		if asks.byIndex.Load() == 0 || seen.Load() == 0 {
			t.Fatalf("rules %+v: B should ask by index and validate A's answers: %d asks, %d validated", rules, asks.byIndex.Load(), seen.Load())
		}
		if charged != 0 {
			t.Fatalf("rules %+v: A, whose answers B ignored, was charged %d promises", rules, charged)
		}
	}
}

// TestScorePromiseWrittenAfterNewerDispatch: under E, an older RPC's ask of an id is popped, the ask
// is refused (its offer dropped), and a newer ask of the id is dispatched, all at the same instant of
// the clock. The older write's report, which carries its pop's number, opens no promise: the pending one
// is the newer ask's, which that RPC did not carry. The newer ask's own write opens it.
func TestScorePromiseWrittenAfterNewerDispatch(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := peer.ID("server")
		mid := testMid(strings.Repeat("n", 32), 1)
		gt := scoreRouter(SpecV05{PromiseAtWrite: true}).gossipTracer
		start := time.Now()
		gt.AddPromise(p, []string{mid})
		popped := nextAskSeq() // the writer pops the older ask
		gt.asksRefused(p, []string{mid})
		gt.AddPromise(p, []string{mid}) // the newer ask
		if !time.Now().Equal(start) {
			t.Fatal("the clock moved: the pop and the newer dispatch must share an instant")
		}
		if n := gt.promisesWritten(p, []string{mid}, popped, time.Now()); n != 0 || len(gt.pending[mid]) != 1 {
			t.Fatalf("the older write: opened %d, pending %d; want 0 1", n, len(gt.pending[mid]))
		}
		if n := gt.promisesWritten(p, []string{mid}, nextAskSeq(), time.Now()); n != 1 {
			t.Fatalf("the newer write: opened %d; want 1", n)
		}
	})
}

// TestSeacastHeldIndexNotOffered: a router on the path holds index 7 of a group under one id; another
// id of the same group and index (another content) is offered. It is held (seacast-lite.md §4.4's held:
// the chunk is in mine), so it is neither recorded as an offer nor a candidate to ask, whose ask the
// writer would drop with nothing to close its record. An id of an index not held is both. A router with
// the same HAVE bitmaps and no path keeps its intake: the other id is offered and a candidate.
func TestSeacastHeldIndexNotOffered(t *testing.T) {
	p := peer.ID("server")
	root := strings.Repeat("v", 32)
	held, other, free := testMid(root, 7), root+string([]byte{7})+"#another", testMid(root, 8)
	router := func(path bool, seen timecache.TimeCache) *GossipSubRouter {
		gs := scoreRouter(SpecV05{})
		gs.v05 = nil
		gs.offerTable = make(map[string]map[peer.ID]time.Time)
		gs.p.seenMessages = seen
		if path {
			gs.seacast = &seacast{ctr: &SeacastCounters{}}
		}
		gs.haveNoteArrival(held)
		return gs
	}
	t.Run("on the path", func(t *testing.T) {
		seen := timecache.NewTimeCache(time.Minute) // its sweeper runs outside the bubble
		synctest.Test(t, func(t *testing.T) {
			gs := router(true, seen)
			iwant := map[string]string{}
			var candidates []string
			gs.considerAnnounced(p, "seg/col/0", other, iwant, &candidates)
			if gs.pullOffered(p, other) || len(candidates) != 0 {
				t.Fatalf("another id of a held index: offered %v, candidates %q; want false, none", gs.pullOffered(p, other), candidates)
			}
			gs.considerAnnounced(p, "seg/col/0", free, iwant, &candidates)
			if !gs.pullOffered(p, free) || len(candidates) != 1 {
				t.Fatalf("an index not held: offered %v, candidates %d; want true 1", gs.pullOffered(p, free), len(candidates))
			}
		})
	})
	t.Run("HAVE bitmaps without the path", func(t *testing.T) {
		seen := timecache.NewTimeCache(time.Minute)
		synctest.Test(t, func(t *testing.T) {
			gs := router(false, seen)
			iwant := map[string]string{}
			var candidates []string
			gs.considerAnnounced(p, "seg/col/0", other, iwant, &candidates)
			if !gs.pullOffered(p, other) || len(candidates) != 1 {
				t.Fatalf("another id of a held index: offered %v, candidates %q; want true, the id", gs.pullOffered(p, other), candidates)
			}
		})
	})
}
