package pubsub

import (
	"context"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

// The close rules of the probe (seacast.md §3's lifetimes and §4.6's claims_close, spec 43fe953): a
// probe of a claimant whose connection closes before the verdict gets none, and the duty returns to its
// own time (duty_at), at once if that has passed; as a claimant, a peer's probe tables go with its
// connection.

// TestSeacastClaimsCloseDutyAt is claims_close's return to duty_at: B's duty is re-armed to 100 ms from
// just before A's claim, so the probe arms the reconsider at the probe's DEFER_WINDOW (1 s), past the
// duty's own time. A's connection closes 300 ms after the probe: B reconsiders at once (a late decode),
// with no verdict.
func TestSeacastClaimsCloseDutyAt(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		s := newR5SceneCross(t, ctx, false, nil, func(s *r5Scene, _ []*PubSub) {
			s.on(s.gsB, func() { s.gsB.seacastArm(s.root, time.Now().Add(100*time.Millisecond)) })
		})
		time.Sleep(time.Until(s.asked.Add(300 * time.Millisecond)))
		if err := s.hosts[1].Network().ClosePeer(s.A); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Until(s.asked.Add(600 * time.Millisecond)))
		ctr := s.gsB.seacast.ctr
		if n := ctr.LateDecodes.Load(); n != 1 {
			t.Errorf("the claimant gone after the duty's own time: late decodes %d at 600 ms, want 1 (reconsidered at once)", n)
		}
		s.afterReconsider()
		if p, n := ctr.Penalties.Load(), ctr.ProbesNoVerdict.Load(); p != 0 || n != 1 {
			t.Errorf("the claimant gone before the verdict: penalties %d, no-verdict count %d; want 0 and 1", p, n)
		}
	})
}

// TestSeacastClaimsCloseAfterDue is claims_close after the probe's promise came due but before the
// verdict: A does not hold the probed cell, so its promise breaks at DEFER_WINDOW (1 s from the write);
// A's connection closes at 1.3 s, before B's duty (DecodeLater 2 s from K). The verdict is reconsider's,
// so A gets none; the promise's charge stands (not cancelled).
func TestSeacastClaimsCloseAfterDue(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		s := newR5Scene(t, ctx, false)
		time.Sleep(time.Until(s.asked.Add(1300 * time.Millisecond)))
		ctr := s.gsB.seacast.ctr
		if b := ctr.ProbesBroken.Load(); b != 1 {
			t.Fatalf("the scene: broken promises %d at 1.3 s, want 1", b)
		}
		if err := s.hosts[1].Network().ClosePeer(s.A); err != nil {
			t.Fatal(err)
		}
		s.afterReconsider()
		if p, n := ctr.Penalties.Load(), ctr.ProbesNoVerdict.Load(); p != 0 || n != 1 {
			t.Errorf("the claimant gone after its promise's due, before the verdict: penalties %d, no-verdict count %d; want 0 and 1", p, n)
		}
		if c, l := ctr.ProbesCancelled.Load(), ctr.LateDecodes.Load(); c != 0 || l != 1 {
			t.Errorf("cancelled promises %d, late decodes %d; want 0 (judged already) and 1 (at the duty's own time)", c, l)
		}
	})
}

// TestSeacastProbeTablesClose is §3's lifetimes for a claimant's probe tables (probe_served,
// probe_waiting): B's entries at A go with B's connection, another group's peer's stay. A control: it
// passes on the fork before this commit too.
func TestSeacastProbeTablesClose(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		s := newR5Scene(t, ctx, false)
		other := "2" + s.root[1:]
		s.on(s.gsA, func() {
			sc := s.gsA.seacast
			sc.probeServed = map[peer.ID]map[string]bool{s.B: {s.root: true}, s.C: {other: true}}
			sc.probeWaiting = map[string]map[peer.ID]uint32{s.root: {s.B: 5}, other: {s.C: 6}}
		})
		if err := s.hosts[0].Network().ClosePeer(s.B); err != nil {
			t.Fatal(err)
		}
		time.Sleep(200 * time.Millisecond)
		var servedB, waitingB, keptC bool
		s.on(s.gsA, func() {
			sc := s.gsA.seacast
			_, servedB = sc.probeServed[s.B]
			_, waitingB = sc.probeWaiting[s.root][s.B]
			keptC = sc.probeServed[s.C][other] && sc.probeWaiting[other][s.C] == 6
		})
		if servedB || waitingB {
			t.Errorf("B's probe tables at A after B's close: probe_served kept %v, probe_waiting kept %v; want both gone", servedB, waitingB)
		}
		if !keptC {
			t.Errorf("another peer's probe tables at B's close: gone, want kept")
		}
	})
}
