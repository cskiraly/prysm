package pubsub

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

// Ask scaling (seacast.md §4.13).
func TestAskScaling(t *testing.T) {
	t.Run("the window follows the uplink below ASK_UPLINK, the promise stays", func(t *testing.T) {
		for _, c := range []struct {
			uplink int64
			want   time.Duration
		}{{25e6, 400 * time.Millisecond}, {50e6, 200 * time.Millisecond}, {1e9, 200 * time.Millisecond}, {10e6, time.Second}} {
			gs := &GossipSubRouter{}
			ps := &PubSub{rt: gs}
			for _, o := range []Option{WithIWantDiscipline(200 * time.Millisecond), WithIHaveCommitmentPark(1, 400*time.Millisecond, time.Minute, nil, nil), WithAskUplink(50e6, c.uplink)} {
				if err := o(ps); err != nil {
					t.Fatal(err)
				}
			}
			if gs.iwantWindow != c.want || ps.AskWindow() != c.want || gs.parkPromise != 400*time.Millisecond {
				t.Fatalf("uplink %d: window %v promise %v, want %v and 400ms", c.uplink, gs.iwantWindow, gs.parkPromise, c.want)
			}
		}
		if err := WithAskUplink(50e6, 25e6)(&PubSub{rt: &GossipSubRouter{}}); err == nil {
			t.Fatal("ask scaling without the discipline must be refused")
		}
	})
	// At 10 Mbps the window is 1 s and the promise 400 ms. The silent peer's promise breaks at 400 ms
	// and parks it; a second announcer at 500 ms must not be asked before the window ends, and the
	// offer table's retry fires at the window plus 5 ms. With a window shorter than the promise (300
	// ms), the broken promise finds the window run and the record goes, as before.
	ask := func(t *testing.T, window time.Duration) (refusedAt500 bool, evalsAt time.Duration, retryAt time.Duration) {
		synctest.Test(t, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			gs := newParkRouter(false)
			gs.p = &PubSub{eval: make(chan func(), 16), ctx: ctx}
			gs.iwantAsked = make(map[string]time.Time)
			gs.offerTable = make(map[string]map[peer.ID]time.Time)
			gs.offerTried = make(map[string]map[peer.ID]time.Time)
			gs.iwantWindow = window
			gs.parkPromise = 400 * time.Millisecond
			silent, second, mid := peer.ID("silent"), peer.ID("second"), "frame"
			start := time.Now()
			if !gs.iwantAllowed(mid, silent) {
				t.Fatal("first ask refused")
			}
			retries := 0
			drain := func() {
				synctest.Wait()
				for len(gs.p.eval) > 0 {
					f := <-gs.p.eval
					if time.Since(start) >= window+5*time.Millisecond && retryAt == 0 && time.Since(start) < 400*time.Millisecond+window {
						retryAt = time.Since(start) // the retry's func: counted, not run (it needs the offers)
						retries++
						continue
					}
					f() // the promise
				}
			}
			time.Sleep(405 * time.Millisecond)
			drain()
			if !gs.peerParked(silent) {
				t.Fatal("the silent peer is parked at its promise, 400 ms")
			}
			time.Sleep(95 * time.Millisecond) // 500 ms
			refusedAt500 = !gs.iwantAllowed(mid, second)
			if refusedAt500 {
				time.Sleep(window - 500*time.Millisecond + 4*time.Millisecond)
				drain()
				if retries != 0 {
					t.Fatalf("a retry before the window plus 5 ms, at %v", time.Since(start))
				}
				time.Sleep(time.Millisecond) // the window plus 5 ms
				drain()
				evalsAt = time.Since(start)
			}
		})
		return
	}
	t.Run("a broken promise does not end a scaled window", func(t *testing.T) {
		refused, _, retryAt := ask(t, time.Second)
		if !refused {
			t.Fatal("the second announcer was asked at 500 ms, inside the 1 s window")
		}
		if retryAt != time.Second+5*time.Millisecond {
			t.Fatalf("the offer table's retry at %v, want 1.005 s", retryAt)
		}
	})
	t.Run("positive control: a window shorter than the promise has run by the break", func(t *testing.T) {
		if refused, _, _ := ask(t, 300*time.Millisecond); refused {
			t.Fatal("with a 300 ms window the second announcer is asked at 500 ms")
		}
	})
}
