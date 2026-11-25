package pubsub

import (
	"context"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

// TestSeacastRetryStopsAtStopPull is seacast-lite.md §6.1's retry_offer and ask_parked at spec 43fe953 (:1281-1283,
// :1357-1360): a retry returns once its group's stop_pull is set, the probed cell included. The probe's exemption
// (probe_asks) passes gated, which on_offer and the writer read, not the retry. B, complete at K, has probed A for
// a cell it lacks; C's earlier offer of that cell is in B's offer table. Before, B's retry asked C for it past
// stop_pull; after, it asks nobody. The control: an offer of the probed cell that arrives after the probe is still
// asked (on_offer's gated, exempt).
func TestSeacastRetryStopsAtStopPull(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		s := newR5Scene(t, ctx, false)
		mid := string(s.cell(byte(s.index))[:haveRootLen+1]) // lint:ignore uintcast -- a small index.
		var sent, allowed bool
		var pick peer.ID
		s.on(s.gsB, func() {
			gs := s.gsB
			if gs.offerTable == nil {
				gs.offerTable = map[string]map[peer.ID]time.Time{}
				gs.offerTried = map[string]map[peer.ID]time.Time{}
				gs.offerTopic = map[string]string{}
				gs.offerByPeer = map[peer.ID]int{}
			}
			gs.recordOffer(mid, r5Topic, s.C)
			delete(gs.iwantAsked, mid)
			pick, sent = gs.retryOfferedPull(mid)
			allowed = gs.requestAllowed(s.C, r5Topic, mid) // on_offer's gate: exempt for the probed cell
		})
		if !allowed {
			t.Fatalf("the control: an offer of the probed cell is gated past stop_pull, want exempt")
		}
		if sent {
			t.Errorf("a retry of the probed cell past stop_pull: asked %s, want no ask (retry_offer returns at stop_pull)", pick)
		}
	})
}
