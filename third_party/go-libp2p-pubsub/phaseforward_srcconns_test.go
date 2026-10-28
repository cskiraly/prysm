package pubsub

import (
	"context"
	"fmt"
	"maps"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
)

// TestPhaseSourceConnections checks that the publisher's pushes range over every connection,
// not only its mesh. The topology is a star around the publisher with heartbeat gossip off, so
// a peer outside the publisher's mesh can only receive a message as a source push.
func TestPhaseSourceConnections(t *testing.T) {
	const (
		leaves = 20
		msgs   = 40
	)
	run := func(t *testing.T, wide bool) int {
		reached := 0
		synctestTest(t, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			hosts := getDefaultHosts(t, leaves+1)
			params := DefaultGossipSubParams()
			params.Dlazy = 0
			params.GossipFactor = 0
			psubs := getGossipsubsOptFn(ctx, hosts, func(i int, h host.Host) []Option {
				opts := []Option{WithGossipSubParams(params), WithPhaseForwarding(func(string) bool { return true }, 2)}
				if wide && i == 0 {
					opts = append(opts, WithPhaseSourceConnections())
				}
				return opts
			})
			subs := make([]*Subscription, len(psubs))
			for i, ps := range psubs {
				s, err := ps.Subscribe("t")
				if err != nil {
					t.Fatal(err)
				}
				subs[i] = s
			}
			for _, h := range hosts[1:] {
				connect(t, hosts[0], h)
			}
			time.Sleep(2 * time.Second)

			for i := range msgs {
				if err := psubs[0].Publish("t", fmt.Appendf(nil, "msg %d", i)); err != nil {
					t.Fatal(err)
				}
			}
			time.Sleep(2 * time.Second)

			got := make(map[peer.ID]bool)
			for i, s := range subs[1:] {
				rctx, rcancel := context.WithTimeout(ctx, 10*time.Millisecond)
				if _, err := s.Next(rctx); err == nil {
					got[hosts[i+1].ID()] = true
				}
				rcancel()
			}
			reached = len(got)
		})
		return reached
	}

	dhi := DefaultGossipSubParams().Dhi
	mesh, wideN := run(t, false), run(t, true)
	t.Logf("leaves reached: mesh-scoped %d, connection-wide %d (Dhi %d)", mesh, wideN, dhi)
	if mesh > dhi {
		t.Fatalf("mesh-scoped source reached %d leaves, more than Dhi=%d", mesh, dhi)
	}
	if wideN <= dhi {
		t.Fatalf("connection-wide source reached %d leaves, want more than Dhi=%d", wideN, dhi)
	}
}

// TestPhaseSourcePushQuota is the push quota's unit known answer: each mesh peer is pushed at most
// quota frames of a group, the rest are offered to the least-loaded peers (one per frame under
// rotate), and every peer's pushes plus offers stay within one of the others'.
func TestPhaseSourcePushQuota(t *testing.T) {
	pf := &phaseForwarding{degree: 2, srcPushDegree: 1, srcMode: PhaseSourceRotate, srcDegree: 1,
		srcPushQuota: 2, srcLoad: make(map[string]map[peer.ID]*srcPeerLoad)}
	peers := []peer.ID{"a", "b", "c", "d"}
	pushed := map[peer.ID]int{}
	offered := map[peer.ID]int{}
	for i := 0; i < 24; i++ {
		tosend := map[peer.ID]struct{}{}
		for _, p := range peers {
			tosend[p] = struct{}{}
		}
		offer, _ := pf.quotaTrim("root", fmt.Sprintf("mid%d", i), append([]peer.ID(nil), peers...), 1, tosend, nil)
		for p := range tosend {
			pushed[p]++
		}
		for _, p := range offer {
			offered[p]++
		}
		if len(tosend) > 1 || len(offer) != 1 {
			t.Fatalf("frame %d: pushed to %d peers and offered to %d, want at most 1 and exactly 1", i, len(tosend), len(offer))
		}
	}
	lo, hi := 1<<30, 0
	for _, p := range peers {
		if pushed[p] > 2 {
			t.Fatalf("%s pushed %d frames, want at most the quota 2", p, pushed[p])
		}
		n := pushed[p] + offered[p]
		lo, hi = min(lo, n), max(hi, n)
	}
	if hi-lo > 1 {
		t.Fatalf("pushes plus offers per peer range %d..%d, want within one", lo, hi)
	}
}

func TestOriginOfferRefuses(t *testing.T) {
	const root, relayed = "rootrootrootrootrootrootrootroot", "relayedrelayedrelayedrelayedrela"
	gs := testHaveRouter()
	gs.phaseForward = &phaseForwarding{srcOfferPace: 500 * time.Millisecond, srcOfferNeed: 8,
		srcOffers: make(map[string]*srcOfferQueue)}
	if err := WithOriginHavePaced()(&PubSub{rt: gs}); err != nil {
		t.Fatal(err)
	}
	r := &originPacedRoot{}
	r.popped.Set(0)
	r.told.Set(1)
	gs.have.originPaced.roots[root] = r
	q := &srcOfferQueue{start: time.Now()}
	q.released.Set(2)
	gs.phaseForward.srcOffers[root] = q
	for i, want := range []bool{false, false, false, true} {
		if got := gs.originOfferRefuses(testMid(root, byte(i))); got != want {
			t.Fatalf("frame %d: refused %v, want %v (sent 0, told 1, offered 2, none of these 3)", i, got, want)
		}
	}
	if gs.originOfferRefuses(testMid(relayed, 3)) || gs.originOfferRefuses("plain") {
		t.Fatal("refused a frame of a group we relay, or a plain message")
	}
	gs.phaseForward.srcOfferPace = 0
	if gs.originOfferRefuses(testMid(root, 3)) {
		t.Fatal("refused with offers unpaced")
	}
}

// TestOriginDataHoldTrim is the hold's selection known answer: our own frame is pushed among the
// unheld peers only, a held peer is announced instead, nothing falls back to a held peer, and a
// relayed frame or an ended hold pushes as without the hold.
func TestOriginDataHoldTrim(t *testing.T) {
	const root = "rootrootrootrootrootrootrootroot"
	mesh := map[peer.ID]struct{}{"a": {}, "b": {}, "c": {}, "d": {}, "e": {}, "f": {}}
	unheld := map[peer.ID]bool{"e": true, "f": true}
	later := time.Now().Add(time.Hour)
	setup := func(until time.Time) (*GossipSubRouter, *atomic.Int64, *int) {
		gs := testHaveRouter()
		var pushes atomic.Int64
		asked := new(int)
		gs.originHold = &originHold{held: func(p peer.ID, _ string) (time.Time, bool) {
			*asked++
			return until, !unheld[p]
		}, pushes: &pushes, waiting: make(map[originHoldKey]*originHoldBatch)}
		return gs, &pushes, asked
	}
	all := func(except ...peer.ID) map[peer.ID]struct{} {
		out := maps.Clone(mesh)
		for _, p := range except {
			delete(out, p)
		}
		return out
	}
	t.Run("pushes among the unheld only, the held announced", func(t *testing.T) {
		for i := range byte(20) {
			gs, pushes, _ := setup(later)
			mid := testMid(root, i)
			for _, degree := range []int{1, 2, 6} {
				tosend := all()
				announce := (&phaseForwarding{degree: degree, fixedBudget: true}).trim(gs, mid, computeChecksum(mid), mesh, tosend, true)
				for p := range tosend {
					if !unheld[p] {
						t.Fatalf("frame %d, degree %d: pushed to held peer %s", i, degree, p)
					}
				}
				if len(tosend) != min(degree, 2) || len(tosend)+len(announce) != len(mesh) {
					t.Fatalf("frame %d, degree %d: pushed %v, announced %v", i, degree, tosend, announce)
				}
			}
			if pushes.Load() == 0 {
				t.Fatalf("frame %d: no withheld push counted", i)
			}
		}
	})
	t.Run("no fallback to a held peer", func(t *testing.T) {
		gs, _, _ := setup(later)
		mid := testMid(root, 0)
		tosend := all("e", "f")
		announce := (&phaseForwarding{degree: 2, fixedBudget: true}).trim(gs, mid, computeChecksum(mid), mesh, tosend, true)
		if len(tosend) != 0 || len(announce) != 4 {
			t.Fatalf("pushed %v and announced %v, want nothing pushed and the four held announced", tosend, announce)
		}
	})
	t.Run("the quota path pushes no held peer", func(t *testing.T) {
		gs, pushes, _ := setup(later)
		pf := &phaseForwarding{degree: 2, srcPushQuota: 100, srcLoad: make(map[string]map[peer.ID]*srcPeerLoad)}
		for i := range byte(8) {
			mid := testMid(root, i)
			tosend := all()
			offer := pf.trim(gs, mid, computeChecksum(mid), mesh, tosend, true)
			if len(tosend) != 2 || len(offer) != 4 {
				t.Fatalf("frame %d: pushed %v, offered %v; want e and f pushed, the rest offered", i, tosend, offer)
			}
			for p := range tosend {
				if !unheld[p] {
					t.Fatalf("frame %d: pushed to held peer %s", i, p)
				}
			}
		}
		if pushes.Load() == 0 {
			t.Fatal("no withheld push counted")
		}
	})
	t.Run("an ended hold and a relayed frame push as without the hold", func(t *testing.T) {
		for _, c := range []struct {
			until    time.Time
			fromSelf bool
		}{{time.Now().Add(-time.Second), true}, {later, false}} {
			gs, pushes, asked := setup(c.until)
			mid := testMid(root, 3)
			got, want := all(), all()
			(&phaseForwarding{degree: 2, fixedBudget: true}).trim(gs, mid, computeChecksum(mid), mesh, got, c.fromSelf)
			(&phaseForwarding{degree: 2, fixedBudget: true}).trim(testHaveRouter(), mid, computeChecksum(mid), mesh, want, c.fromSelf)
			if !maps.Equal(got, want) || pushes.Load() != 0 {
				t.Fatalf("fromSelf %v: pushed %v, want %v as without the hold; %d withheld", c.fromSelf, got, want, pushes.Load())
			}
			if !c.fromSelf && *asked != 0 {
				t.Fatalf("a relayed frame consulted the hold %d times", *asked)
			}
		}
	})
}

// TestOriginDataHold is the hold's end-to-end known answer on a star around the publisher, whose
// leaves can get data only from it: during the hold the unheld leaf is pushed at once, the held ones
// are announced to, ask, and are answered when the hold ends; a message the hub relays is not held.
func TestOriginDataHold(t *testing.T) {
	const (
		leaves = 6
		hold   = 500 * time.Millisecond
	)
	synctestTest(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		hosts := getDefaultHosts(t, leaves+1)
		params := DefaultGossipSubParams()
		params.Dlazy = 0
		params.GossipFactor = 0
		var pushes, answers atomic.Int64
		var mu sync.Mutex
		var until time.Time
		consulted := map[string]bool{}
		unheld := hosts[1].ID()
		held := func(p peer.ID, mid string) (time.Time, bool) {
			mu.Lock()
			defer mu.Unlock()
			consulted[mid] = true
			return until, p != unheld
		}
		psubs := getGossipsubsOptFn(ctx, hosts, func(i int, h host.Host) []Option {
			opts := []Option{WithGossipSubParams(params)}
			if i == 0 {
				opts = append(opts, WithPhaseForwarding(func(string) bool { return true }, 2), WithOriginDataHold(held, &pushes, &answers))
			}
			return opts
		})
		subs := make([]*Subscription, len(psubs))
		for i, ps := range psubs {
			s, err := ps.Subscribe("t")
			if err != nil {
				t.Fatal(err)
			}
			subs[i] = s
		}
		for _, h := range hosts[1:] {
			connect(t, hosts[0], h)
		}
		time.Sleep(2 * time.Second)

		start := time.Now()
		mu.Lock()
		until = start.Add(hold)
		mu.Unlock()
		got := make([]map[string]time.Duration, len(subs))
		var wg sync.WaitGroup
		for i := 1; i < len(subs); i++ {
			got[i] = map[string]time.Duration{}
			wg.Add(1)
			go func() {
				defer wg.Done()
				for range 2 {
					m, err := subs[i].Next(ctx)
					if err != nil {
						return
					}
					got[i][string(m.Data)] = time.Since(start)
				}
			}()
		}
		if err := psubs[0].Publish("t", []byte("origin")); err != nil {
			t.Fatal(err)
		}
		if err := psubs[leaves].Publish("t", []byte("relayed")); err != nil {
			t.Fatal(err)
		}
		time.Sleep(2 * time.Second)
		cancel()
		wg.Wait()

		for i := 1; i < len(subs); i++ {
			at, ok := got[i]["origin"]
			switch {
			case !ok:
				t.Fatalf("leaf %d never received the origin message", i)
			case hosts[i].ID() == unheld && at >= hold:
				t.Fatalf("the unheld leaf received the origin message at %v, want before the hold ends at %v", at, hold)
			case hosts[i].ID() != unheld && (at < hold || at > hold+100*time.Millisecond):
				t.Fatalf("held leaf %d received the origin message at %v, want just after the hold ends at %v", i, at, hold)
			}
			if i == leaves {
				continue // the relayed message's author
			}
			if at, ok := got[i]["relayed"]; !ok || at >= hold {
				t.Fatalf("leaf %d received the relayed message at %v (%v), want before %v", i, at, ok, hold)
			}
		}
		mu.Lock()
		defer mu.Unlock()
		if pushes.Load() != leaves-1 || answers.Load() != leaves-1 || len(consulted) != 1 {
			t.Fatalf("withheld pushes %d, deferred answers %d, ids consulted %d; want %d, %d and only the origin message's",
				pushes.Load(), answers.Load(), len(consulted), leaves-1, leaves-1)
		}
	})
}
