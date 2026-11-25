package pubsub

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
)

// TestSeacastCompletion is a group's completion (seacast.md §4.6's check_completion, arm, reconsider):
// on two routers, A publishes cells of a coded group (K = 4 of 8) that B, of interest every index,
// receives pushed. B completes at K: its asks for the group stop, its holding says complete, its
// later duty is armed from known_at; the duty fires a late decode when the group is still short, is
// cancelled when it became whole meanwhile, and a type that decodes at K enters DECODING at once with
// the claim. With the machine off nothing of this runs (the control).
func TestSeacastCompletion(t *testing.T) {
	for _, c := range []struct {
		name            string
		machine, atK    bool
		publish         int // cells of group 1 A publishes before the duty
		wantLate, wantC int64
	}{
		{"short at the duty: a late decode", true, false, 4, 1, 0},
		// Since v0.9 (seacast_origin.go) the source claims only what is acknowledged: its pushes of indices
		// 4-7 are dropped at its writer once B holds K, are never acknowledged, and so are never claimed; B
		// is short at the duty with no claim to cover it, and decodes late. The cancelled duty this case
		// showed before needs a topology where B receives all eight (open, step2/build-order.md).
		{"eight published, four acknowledged: a late decode", true, false, 8, 1, 0},
		{"decode at K", true, true, 4, 0, 0},
		{"the machine off (control)", false, false, 4, 0, 0},
	} {
		t.Run(c.name, func(t *testing.T) {
			synctestTest(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				const topic = "seg/row/1"
				root := strings.Repeat("1", haveRootLen)
				hosts := getDefaultHosts(t, 2)
				table := &MapRoleTable{Joins: map[peer.ID]map[string]Join{}}
				for _, h := range hosts {
					table.Joins[h.ID()] = map[string]Join{"row": {Interest: InterestAll, Duty: DutyForward}}
				}
				params := DefaultGossipSubParams()
				params.IDontWantMessageThreshold = 1
				psubs := getGossipsubsOptFn(ctx, hosts, func(i int, h host.Host) []Option {
					tp := TypeParams{RNode: 2}
					if i == 1 {
						// B alone has the later duty: A's own late decode would claim the group and
						// defer B's duty on that claim, which is the draft's rule but not this case.
						tp.DecodeLater = func(string) (time.Duration, bool) { return time.Second, true }
					}
					if c.atK {
						tp.DecodeAtK = func(string) bool { return true }
					}
					T := &testObjectType{name: "row", params: tp, topics: []string{topic}, store: NewMapCellStore(),
						threshold: 4, indexCount: 8, topicOf: func(r string) (string, bool) { return topic, len(r) == haveRootLen }}
					opts := []Option{
						WithGossipSubParams(params), WithMessageSignaturePolicy(StrictNoSign), WithMessageIdFn(seacastTestID),
						WithHaveBitmaps(seacastTestIndex),
						WithPhaseForwarding(func(tp string) bool { return !strings.HasPrefix(tp, "seg/") }, 2),
						WithSeacast([]ObjectType{T}, nil), WithSeacastRoles(table),
					}
					if c.machine {
						opts = append(opts, WithSeacastMachine())
					}
					return opts
				})
				seacastOrdZeroAtEnd(t, psubs)
				for _, ps := range psubs {
					if _, err := ps.Subscribe(topic); err != nil {
						t.Fatal(err)
					}
				}
				connect(t, hosts[0], hosts[1])
				time.Sleep(2 * time.Second)
				a, b := psubs[0], psubs[1]
				gsA, gsB := a.rt.(*GossipSubRouter), b.rt.(*GossipSubRouter)
				read := func(gs *GossipSubRouter, f func()) {
					done := make(chan struct{})
					gs.p.eval <- func() { f(); close(done) }
					<-done
				}
				cell := func(i byte) []byte { return append([]byte(root+string(rune(i))), bytes.Repeat([]byte{'a' + i}, 64)...) }
				for i := range c.publish {
					if err := a.Publish(topic, cell(byte(i))); err != nil { // lint:ignore uintcast -- a small index.
						t.Fatal(err)
					}
				}
				time.Sleep(300 * time.Millisecond)
				var state uint8
				var stopPull, gated, complete, claimAll bool
				read(gsB, func() {
					if g := gsB.seacast.groups[root]; g != nil {
						state, stopPull = g.state, g.stopPull
					}
					// An ask of a cell B lacks and does not probe: a claim's probe draws its cell at
					// random (seacast.md §4.6, R5), and an ask of the probed cell passes stop_pull.
					idx := byte(7)
					if g := gsB.seacast.groups[root]; g != nil && g.probe != nil && g.probe.index == 7 {
						idx = 6
					}
					gated = !gsB.requestAllowed(hosts[0].ID(), topic, string(cell(idx)[:haveRootLen+1]))
				})
				read(gsA, func() {
					if e := gsA.have.peers[hosts[1].ID()][root]; e != nil {
						complete, claimAll = e.complete, e.claimAll
					}
				})
				if !c.machine {
					if state != 0 || stopPull || gated || gsB.seacast.ctr.GroupsCompleted.Load() != 0 {
						t.Fatalf("machine off: state %d stop %v gated %v completed %d, want nothing", state, stopPull, gated, gsB.seacast.ctr.GroupsCompleted.Load())
					}
					return
				}
				if !stopPull || !gated || !complete || gsB.seacast.ctr.GroupsCompleted.Load() != 1 {
					t.Fatalf("at K: stop %v gated %v complete at A %v completed %d, want true, true, true, 1", stopPull, gated, complete, gsB.seacast.ctr.GroupsCompleted.Load())
				}
				if c.atK {
					if state != seacastDecoding || !claimAll || gsB.seacast.ctr.DecodesStarted.Load() != 1 || gsB.seacast.ctr.DutiesArmed.Load() != 0 {
						t.Fatalf("decode at K: state %d claim_all at A %v decodes %d armed %d", state, claimAll, gsB.seacast.ctr.DecodesStarted.Load(), gsB.seacast.ctr.DutiesArmed.Load())
					}
					return
				}
				if state != seacastComplete || gsB.seacast.ctr.DutiesArmed.Load() < 1 {
					t.Fatalf("at K: state %d armed %d, want COMPLETE and armed", state, gsB.seacast.ctr.DutiesArmed.Load())
				}
				var knownAt time.Time
				read(gsB, func() { knownAt = gsB.seacast.knownAt[root] })
				if knownAt.IsZero() {
					t.Fatal("B has no known_at for the group")
				}
				ctr := gsB.seacast.ctr
				if c.publish == 8 {
					// A, the source, holds the group whole, but since v0.9 its holding names a cell only once it
					// is acknowledged: the four pushes its writer dropped once B held K never are, so A claims
					// none of them and B notes no claim. B's duty finds it short and decodes late.
					time.Sleep(time.Until(knownAt.Add(2500 * time.Millisecond)))
					if ctr.ClaimsNoted.Load() != 0 || ctr.ProbesAsked.Load() != 0 {
						t.Fatalf("the source's claim: noted %d, probes %d; want 0 and 0", ctr.ClaimsNoted.Load(), ctr.ProbesAsked.Load())
					}
					if ctr.DutiesCancelled.Load() != c.wantC || ctr.LateDecodes.Load() != c.wantLate {
						t.Fatalf("%s: cancelled %d late %d, want %d and %d", c.name, ctr.DutiesCancelled.Load(), ctr.LateDecodes.Load(), c.wantC, c.wantLate)
					}
					return
				}
				time.Sleep(time.Until(knownAt.Add(time.Second)) - time.Millisecond)
				if n := ctr.LateDecodes.Load() + ctr.DutiesCancelled.Load(); n != 0 {
					t.Fatalf("the duty acted %d times before its instant", n)
				}
				time.Sleep(2 * time.Millisecond)
				if late, cancelled := ctr.LateDecodes.Load(), ctr.DutiesCancelled.Load(); late != c.wantLate || cancelled != c.wantC {
					t.Fatalf("at known_at + 1 s: late decodes %d cancelled %d, want %d and %d", late, cancelled, c.wantLate, c.wantC)
				}
			})
		})
	}
}
