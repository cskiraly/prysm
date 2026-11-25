package pubsub

import (
	"context"
	"fmt"
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
// quota frames of a group, the rest are offered, and every peer's pushes plus offers stay within one
// of the others'.
func TestPhaseSourcePushQuota(t *testing.T) {
	pf := &phaseForwarding{degree: 2, srcPushQuota: 2,
		srcLoad: make(map[string]map[peer.ID]*srcPeerLoad)}
	peers := []peer.ID{"a", "b", "c", "d"}
	pushed := map[peer.ID]int{}
	offered := map[peer.ID]int{}
	for i := 0; i < 24; i++ {
		tosend := map[peer.ID]struct{}{}
		for _, p := range peers {
			tosend[p] = struct{}{}
		}
		offer := pf.quotaTrim("root", fmt.Sprintf("mid%d", i), append([]peer.ID(nil), peers...), 1, tosend)
		for p := range tosend {
			pushed[p]++
		}
		for _, p := range offer {
			offered[p]++
		}
		if len(tosend) > 1 || len(tosend)+len(offer) != len(peers) {
			t.Fatalf("frame %d: pushed to %d peers and offered to %d, want at most 1 pushed and the rest offered", i, len(tosend), len(offer))
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
