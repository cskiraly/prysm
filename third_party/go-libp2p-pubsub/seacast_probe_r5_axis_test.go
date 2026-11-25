package pubsub

import (
	"context"
	"testing"
	"time"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
)

// TestSeacastR5StateGoesAtExpiry is the probe's tables going with the group (seacast.md §3): at
// the group's expiry, the asker's waiting probe at its writer, its exemption by cell, and, at the
// claimant, a marked answer not yet written and the cap all go.
func TestSeacastR5StateGoesAtExpiry(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		s := newR5Scene(t, ctx, false)
		s.on(s.gsA, func() {
			s.gsA.HandleRPC(&RPC{RPC: pb.RPC{Control: &pb.ControlMessage{Iwant: []*pb.ControlIWant{{MessageIDs: []string{r5FlaggedWant(s.root, 1)}}}}}, from: s.B})
		})
		time.Sleep(100 * time.Millisecond) // the answer written, its exemption spent
		// An answer not yet written: the cap's answer stands in for one whose write never came
		// (withheld, or cancel and projection off, where nothing spends it).
		s.on(s.gsA, func() {
			q := s.gsA.p.peers[s.B]
			q.queueMu.Lock()
			if q.have.probeAnswers == nil {
				q.have.probeAnswers = map[string]*haveBits{}
			}
			q.have.probeAnswers[s.root] = &haveBits{}
			q.have.probeAnswers[s.root].Set(1)
			q.queueMu.Unlock()
		})
		state := func(gs *GossipSubRouter, p peer.ID) (answers, waits int, cells, served bool) {
			s.on(gs, func() {
				if q := gs.p.peers[p]; q != nil && q.have != nil {
					q.queueMu.Lock()
					if b := q.have.probeAnswers[s.root]; b != nil {
						answers = b.Count()
					}
					waits = len(q.have.probeWaits)
					q.queueMu.Unlock()
				}
				cells = gs.seacast.probeCells[s.root] != nil
				served = gs.seacast.probeServed[s.B][s.root]
			})
			return
		}
		ans, _, _, served := state(s.gsA, s.B)
		_, waits, cells, _ := state(s.gsB, s.A)
		if ans != 1 || !served || waits != 1 || !cells {
			t.Fatalf("before the expiry: A's unwritten answers %d, cap %v; B's waiting probes %d, exemption %v; want 1, true, 1, true", ans, served, waits, cells)
		}
		s.on(s.gsA, func() { s.gsA.expireGroup(s.root) })
		s.on(s.gsB, func() { s.gsB.expireGroup(s.root) })
		ans, _, _, served = state(s.gsA, s.B)
		_, waits, cells, _ = state(s.gsB, s.A)
		if ans != 0 || served || waits != 0 || cells {
			t.Fatalf("after the expiry: A's unwritten answers %d, cap %v; B's waiting probes %d, exemption %v; want 0, false, 0, false", ans, served, waits, cells)
		}
	})
}
