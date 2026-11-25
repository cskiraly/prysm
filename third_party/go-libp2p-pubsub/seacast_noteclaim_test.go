package pubsub

import (
	"context"
	"testing"
)

// TestSeacastNoteClaimEveryHolding is seacast.md §4.3's on_holding at spec 43fe953 (:1149): note_claim runs on
// every holding that claims, not only on a peer's first claim of the group. B's ledger entry for A already
// carries A's claim (as when A claimed before B reached K); B, now complete at K with its duty armed, absorbs
// another of A's claiming holdings. Before, the claim was not new, so B did not defer: no probe until its duty
// fired. After, B defers on it at once: a probe of A.
func TestSeacastNoteClaimEveryHolding(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var armed, probed bool
		newR5SceneCross(t, ctx, false, nil, func(s *r5Scene, _ []*PubSub) {
			s.on(s.gsB, func() {
				e := s.gsB.have.peers[s.A][s.root]
				if e == nil {
					return
				}
				e.claimAll = true // A's earlier claim, noted before B's duty was armed
				g := s.gsB.seacast.groups[s.root]
				armed = g != nil && !g.reconsiderAt.IsZero() && g.state == seacastComplete && g.probe == nil
				s.gsB.seacastAbsorbV2(s.A, encodeHaveV2(s.root, fullHaveBits(8), true, true))
				probed = g != nil && g.probe != nil
			})
		})
		if !armed {
			t.Fatalf("the scene: B not complete with its duty armed and no probe before A's later claim")
		}
		if !probed {
			t.Errorf("a later claiming holding from A, B complete with its duty armed: no probe, want one (note_claim on every claim)")
		}
	})
}
