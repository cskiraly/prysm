package pubsub

import (
	"bytes"
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	core "github.com/cskiraly/seacast-go"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
)

// TestSeacastProbe is the probe (seacast.md §4.6's
// defer_on_claim, gated, known_in_scope, and R5's verdict): B, complete at K = 4 of 8 with its duty
// armed, notes A's claim and probes A for an index it lacks, drawn at random. Unanswered, the probe's
// promise breaks at DEFER_WINDOW exactly, charges A once and parks A in the group's scope only;
// answered by a copy from a third node C (not the claimant), the cell counts for B but A's promise
// still breaks, since only the claimant's own copy keeps it (R5). The probe's ask passes stop_pull by its exemption (without
// it, the same id is declined: the positive control), another ask of the group does not; its write
// opens no writer's promise though writer-time promises are on (the review of part 4's build,
// findings 2, 3 and 13); a claimant's holding counts for nothing in known_in_scope.
func TestSeacastProbe(t *testing.T) {
	for _, mode := range []string{"unanswered", "answered", "gone"} {
		answered, gone := mode == "answered", mode == "gone"
		name := map[string]string{
			"unanswered": "unanswered: broken at DEFER_WINDOW, the claimant parked in the group",
			"answered":   "answered by a third node's copy: the cell counts, the claimant's promise breaks (R5)",
			"gone":       "the claimant's connection closes: the probe cancelled, nothing charged",
		}[mode]
		t.Run(name, func(t *testing.T) {
			synctestTest(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				const topic = "seg/row/1"
				root := strings.Repeat("1", haveRootLen)
				otherRoot := strings.Repeat("2", haveRootLen)
				hosts := getDefaultHosts(t, 3)
				table := &MapRoleTable{Joins: map[peer.ID]map[string]Join{}}
				for _, h := range hosts {
					table.Joins[h.ID()] = map[string]Join{"row": {Interest: InterestAll, Duty: DutyForward}}
				}
				params := DefaultGossipSubParams()
				params.IDontWantMessageThreshold = 1
				v05 := &SpecV05Counters{}
				var breaks atomic.Int64 // B's park breaks: the charges
				var evMu sync.Mutex
				events := map[string]map[SeacastEvent]int{} // B's machine events per group (WithSeacastEvents)
				evOf := func(r string, k SeacastEvent) int { evMu.Lock(); defer evMu.Unlock(); return events[r][k] }
				psubs := getGossipsubsOptFn(ctx, hosts, func(i int, h host.Host) []Option {
					tp := TypeParams{RNode: 2, DeferWindow: time.Second}
					if i == 1 {
						tp.DecodeLater = func(string) (time.Duration, bool) { return 2 * time.Second, true }
					}
					T := &testObjectType{name: "row", params: tp, topics: []string{topic}, store: NewMapCellStore(),
						threshold: 4, indexCount: 8, topicOf: func(r string) (string, bool) { return topic, len(r) == haveRootLen }}
					opts := []Option{
						WithGossipSubParams(params), WithMessageSignaturePolicy(StrictNoSign), WithMessageIdFn(seacastTestID),
						WithHaveBitmaps(seacastTestIndex),
						WithPhaseForwarding(func(tp string) bool { return !strings.HasPrefix(tp, "seg/") }, 2),
						WithIWantDiscipline(200 * time.Millisecond),
					}
					if i == 1 {
						opts = append(opts, WithIHaveCommitmentPark(1, 400*time.Millisecond, time.Minute, &breaks, nil))
					} else {
						opts = append(opts, WithIHaveCommitmentPark(1, 400*time.Millisecond, time.Minute, nil, nil))
					}
					opts = append(opts, WithIHaveParkPerGroup(nil))
					c := (*SpecV05Counters)(nil)
					if i == 1 {
						c = v05
					}
					opts = append(opts, WithSpecV05(SpecV05{PromiseAtWrite: true}, c))
					opts = append(opts, WithSeacast([]ObjectType{T}, nil), WithSeacastRoles(table), WithSeacastMachine())
					if i == 1 {
						opts = append(opts, WithSeacastEvents(func(k SeacastEvent, r string) {
							evMu.Lock()
							defer evMu.Unlock()
							if events[r] == nil {
								events[r] = map[SeacastEvent]int{}
							}
							events[r][k]++
						}))
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
				connect(t, hosts[1], hosts[2])
				time.Sleep(2 * time.Second)
				a, b, c := psubs[0], psubs[1], psubs[2]
				gsB := b.rt.(*GossipSubRouter)
				read := func(f func()) {
					done := make(chan struct{})
					gsB.p.eval <- func() { f(); close(done) }
					<-done
				}
				cell := func(i byte) []byte { return append([]byte(root+string(rune(i))), bytes.Repeat([]byte{'a' + i}, 64)...) }
				for i := range 4 {
					if err := a.Publish(topic, cell(byte(i))); err != nil { // lint:ignore uintcast -- a small index.
						t.Fatal(err)
					}
				}
				time.Sleep(300 * time.Millisecond)
				A := hosts[0].ID()
				var asked time.Time
				var viewWith, viewWithout int
				var probeAllowed, probeWithoutExemption, otherAllowed bool
				var probeMid string
				var probeIndex uint32
				read(func() {
					// A's claim, as a v4 holding with claim_all would bring it: every index.
					e := gsB.have.peers[A][seacastGID(root)]
					if e == nil {
						t.Fatal("B has no ledger entry for A")
					}
					e.Bits.Or(fullHaveBits(8).Bytes())
					T := gsB.seacastRootType(root)
					viewWithout = gsB.seacastKnownInScope(T, root).Count()
					e.ClaimAll = true
					viewWith = gsB.seacastKnownInScope(T, root).Count()
					gsB.seacastNoteClaim(A, root)
					asked = time.Now()
					other := byte(6)
					if g := gsB.seacast.group(root); g != nil && g.Ext != nil {
						probeMid, probeIndex = g.Ext.mid, g.Ext.index
						probeAllowed = gsB.requestAllowed(A, topic, probeMid)
						// The positive control: the same id with neither exemption, its id's nor its cell's.
						delete(gsB.seacast.probeAsks, probeMid)
						gsB.seacast.probeCells[root].Clear(probeIndex)
						probeWithoutExemption = gsB.requestAllowed(A, topic, probeMid)
						gsB.seacast.probeAsks[probeMid] = struct{}{}
						gsB.seacast.probeCells[root].Set(probeIndex)
						if probeIndex == 6 {
							other = 7 // another cell B lacks: its ask is gated
						}
					}
					otherAllowed = gsB.requestAllowed(A, topic, string(cell(other)[:haveRootLen+1]))
				})
				if viewWithout != 8 || viewWith != 4 {
					t.Fatalf("known_in_scope: %d with A's holding as a plain holding, %d as a claim; want 8 and 4", viewWithout, viewWith)
				}
				ctr := gsB.seacast.ctr
				if ctr.ProbesAsked.Load() != 1 || !probeAllowed || probeWithoutExemption || otherAllowed {
					t.Fatalf("probes %d, the probe past stop_pull %v (without its exemption %v), another ask %v; want 1, true, false, false",
						ctr.ProbesAsked.Load(), probeAllowed, probeWithoutExemption, otherAllowed)
				}
				time.Sleep(100 * time.Millisecond) // the probe written
				var writerPromise bool
				read(func() { _, writerPromise = gsB.iwantPromised[probeMid][A] })
				if writerPromise {
					t.Fatal("the probe's write opened a writer's promise: it keeps its own of DEFER_WINDOW only")
				}
				if gone {
					if err := hosts[1].Network().ClosePeer(A); err != nil {
						t.Fatal(err)
					}
				}
				if answered {
					// The probed index from C (B lacks 4 and up), a node that is not the claimant, as a
					// copy in flight before B's holdings reached C: C's views of B (the router's ledger
					// and its writer's mirror) cleared, else the writer projects B at K and sends nothing.
					gsC, B := c.rt.(*GossipSubRouter), hosts[1].ID()
					done := make(chan struct{})
					gsC.p.eval <- func() {
						delete(gsC.have.peers[B], seacastGID(root))
						if q := gsC.p.peers[B]; q != nil && q.have != nil {
							q.queueMu.Lock()
							delete(q.have.theirs, root)
							delete(q.have.popped, root)
							q.queueMu.Unlock()
						}
						close(done)
					}
					<-done
					if err := c.Publish(topic, cell(byte(probeIndex))); err != nil { // lint:ignore uintcast -- a small index.
						t.Fatal(err)
					}
				}
				time.Sleep(time.Until(asked.Add(time.Second)) - time.Millisecond)
				if n := ctr.ProbesBroken.Load() + ctr.ProbesAnswered.Load(); n != 0 {
					t.Fatalf("the probe judged %d times before DEFER_WINDOW", n)
				}
				time.Sleep(2 * time.Millisecond)
				var parkedHere, parkedElsewhere bool
				read(func() {
					parkedHere = gsB.peerParkedFor(A, probeMid)
					parkedElsewhere = gsB.peerParkedFor(A, encodeWantV2(otherRoot, 4, false, 0))
				})
				if gone {
					if ctr.ProbesCancelled.Load() != 1 || ctr.ProbesBroken.Load() != 0 || ctr.ProbesAnswered.Load() != 0 || breaks.Load() != 0 {
						t.Fatalf("gone: cancelled %d broken %d answered %d charges %d; want 1, 0, 0, 0", ctr.ProbesCancelled.Load(), ctr.ProbesBroken.Load(), ctr.ProbesAnswered.Load(), breaks.Load())
					}
					return
				}
				if a, ans, br := evOf(root, SeacastEvProbeAsked), evOf(root, SeacastEvProbeAnswered), evOf(root, SeacastEvProbeBroken); a != 1 || ans != int(ctr.ProbesAnswered.Load()) || br != int(ctr.ProbesBroken.Load()) {
					t.Fatalf("the events for the group: probe asked %d answered %d broken %d; want 1 and the counters' %d, %d", a, ans, br, ctr.ProbesAnswered.Load(), ctr.ProbesBroken.Load())
				}
				if answered {
					// (R5) the third node's copy counts for the member (answered, as the duty reads the
					// cell from anyone), but it does not keep the claimant's promise: only its own copy
					// does, so the claimant is charged and parked at DEFER_WINDOW.
					if ctr.ProbesAnswered.Load() != 1 || ctr.ProbesBroken.Load() != 0 || breaks.Load() != 1 || !parkedHere || ctr.ProbesOwnCopy.Load() != 0 {
						t.Fatalf("answered by a third node: answered %d broken %d charges %d parked %v own copies %d; want 1, 0, 1, true, 0",
							ctr.ProbesAnswered.Load(), ctr.ProbesBroken.Load(), breaks.Load(), parkedHere, ctr.ProbesOwnCopy.Load())
					}
					return
				}
				if ctr.ProbesBroken.Load() != 1 || breaks.Load() != 1 || !parkedHere || parkedElsewhere {
					t.Fatalf("unanswered: broken %d charges %d, parked in the group %v, in another %v at DEFER_WINDOW; want 1, 1, true, false",
						ctr.ProbesBroken.Load(), breaks.Load(), parkedHere, parkedElsewhere)
				}
				time.Sleep(time.Second) // past the writer's PROMISE: no second charge for the probe
				if breaks.Load() != 1 || v05.PromisesAtWrite.Load() != 0 {
					t.Fatalf("after the writer's PROMISE: charges %d, writer's promises %d; want 1, 0", breaks.Load(), v05.PromisesAtWrite.Load())
				}
				// A disconnection after the judgment cancels nothing (the second check of part 4's gate,
				// finding 23).
				if err := hosts[1].Network().ClosePeer(A); err != nil {
					t.Fatal(err)
				}
				time.Sleep(100 * time.Millisecond)
				if n := ctr.ProbesCancelled.Load(); n != 0 {
					t.Fatalf("a judged probe counted cancelled at a later disconnection: %d", n)
				}
			})
		})
	}
}

// TestSeacastProbeAllowance is revision 2's f5 (the review of part 4's build, finding 13's f4–f6
// cases; seacast.md §4.6's defer_on_claim): the probe goes to a claimant with serving allowance
// left by the asker's own count against SERVE_LIMIT (64): A, asked 63 times, is probed; A, asked 64
// times, is passed over for another claimant C; with nobody else claiming, there is no probe and
// the duty goes on as armed.
func TestSeacastProbeAllowance(t *testing.T) {
	for _, c := range []struct {
		name    string
		askedA  int
		cClaims bool
		want    int // 0 A, 2 C, -1 none
	}{
		{"A asked 63 times, the only claimant: A probed", 63, false, 0},
		{"A asked 64 times, C claims too: C probed", 64, true, 2},
		{"A asked 64 times, nobody else: no probe", 64, false, -1},
	} {
		t.Run(c.name, func(t *testing.T) {
			synctestTest(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				const topic = "seg/row/1"
				root := strings.Repeat("1", haveRootLen)
				hosts := getDefaultHosts(t, 3)
				table := &MapRoleTable{Joins: map[peer.ID]map[string]Join{}}
				for _, h := range hosts {
					table.Joins[h.ID()] = map[string]Join{"row": {Interest: InterestAll, Duty: DutyForward}}
				}
				params := DefaultGossipSubParams()
				params.IDontWantMessageThreshold = 1
				psubs := getGossipsubsOptFn(ctx, hosts, func(i int, h host.Host) []Option {
					tp := TypeParams{RNode: 2, DeferWindow: time.Second, ServeLimit: 64}
					if i == 1 {
						tp.DecodeLater = func(string) (time.Duration, bool) { return 2 * time.Second, true }
					}
					T := &testObjectType{name: "row", params: tp, topics: []string{topic}, store: NewMapCellStore(),
						threshold: 4, indexCount: 8, topicOf: func(r string) (string, bool) { return topic, len(r) == haveRootLen }}
					return []Option{
						WithGossipSubParams(params), WithMessageSignaturePolicy(StrictNoSign), WithMessageIdFn(seacastTestID),
						WithHaveBitmaps(seacastTestIndex),
						WithPhaseForwarding(func(tp string) bool { return !strings.HasPrefix(tp, "seg/") }, 2),
						WithSeacast([]ObjectType{T}, nil), WithSeacastRoles(table), WithSeacastMachine(),
					}
				})
				seacastOrdZeroAtEnd(t, psubs)
				for _, ps := range psubs {
					if _, err := ps.Subscribe(topic); err != nil {
						t.Fatal(err)
					}
				}
				connect(t, hosts[0], hosts[1])
				connect(t, hosts[1], hosts[2])
				time.Sleep(2 * time.Second)
				a, b := psubs[0], psubs[1]
				gsB := b.rt.(*GossipSubRouter)
				cell := func(i byte) []byte { return append([]byte(root+string(rune(i))), bytes.Repeat([]byte{'a' + i}, 64)...) }
				for i := range 4 {
					if err := a.Publish(topic, cell(byte(i))); err != nil { // lint:ignore uintcast -- a small index.
						t.Fatal(err)
					}
				}
				time.Sleep(300 * time.Millisecond)
				A, C := hosts[0].ID(), hosts[2].ID()
				var asked peer.ID
				var probed bool
				done := make(chan struct{})
				gsB.p.eval <- func() {
					defer close(done)
					claim := func(q peer.ID) {
						if gsB.have.peers[q] == nil {
							gsB.have.peers[q] = map[core.GroupID]*core.PeerHolding{}
						}
						e := gsB.have.peers[q][seacastGID(root)]
						if e == nil {
							e = &core.PeerHolding{}
							gsB.have.peers[q][seacastGID(root)] = e
						}
						e.Bits.Or(fullHaveBits(8).Bytes())
						e.ClaimAll = true
					}
					claim(A)
					if c.cClaims {
						claim(C)
					}
					if gsB.seacast.askedOf == nil {
						gsB.seacast.askedOf = map[string]map[peer.ID]int{}
					}
					gsB.seacast.askedOf[root] = map[peer.ID]int{A: c.askedA}
					gsB.seacastNoteClaim(A, root)
					if g := gsB.seacast.group(root); g != nil && g.Ext != nil {
						probed, asked = true, g.Ext.asked
					}
				}
				<-done
				ctr := gsB.seacast.ctr
				switch c.want {
				case -1:
					if probed || ctr.ProbesNone.Load() != 1 {
						t.Fatalf("probed %v (of %s), none possible %d; want no probe, 1", probed, asked, ctr.ProbesNone.Load())
					}
				default:
					if want := hosts[c.want].ID(); !probed || asked != want {
						t.Fatalf("probed %v of %s; want %s", probed, asked, want)
					}
				}
			})
		})
	}
}

// TestSeacastClaimBeforeArming is revision 2's f3 case "a claim before arming" (the review of part
// 4's build, finding 13; seacast.md §4.6's note_claim and reconsider): C, a liar, claims the group
// with its first chunk, while B holds 3 of K = 4 and has no duty armed; the claim is noted and
// nothing is probed. When B completes at K its duty is armed (the third phase, 2 s); at the
// reconsideration the claim defers it once with a probe of C; C cannot answer, so the probe breaks
// at DEFER_WINDOW, and the duty decodes (a late decode) with the claimant penalised.
func TestSeacastClaimBeforeArming(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		const topic = "seg/row/1"
		root := strings.Repeat("1", haveRootLen)
		hosts := getDefaultHosts(t, 3)
		table := &MapRoleTable{Joins: map[peer.ID]map[string]Join{}}
		for _, h := range hosts {
			table.Joins[h.ID()] = map[string]Join{"row": {Interest: InterestAll, Duty: DutyForward}}
		}
		params := DefaultGossipSubParams()
		params.IDontWantMessageThreshold = 1
		cell := func(i byte) []byte { return append([]byte(root+string(rune(i))), bytes.Repeat([]byte{'a' + i}, 64)...) }
		ran := make(chan struct{}, 4)
		psubs := getGossipsubsOptFn(ctx, hosts, func(i int, h host.Host) []Option {
			tp := TypeParams{RNode: 2, DeferWindow: time.Second}
			if i == 1 {
				tp.DecodeLater = func(string) (time.Duration, bool) { return 2 * time.Second, true }
			}
			base := &testObjectType{name: "row", params: tp, topics: []string{topic}, store: NewMapCellStore(),
				threshold: 4, indexCount: 8, topicOf: func(r string) (string, bool) { return topic, len(r) == haveRootLen }}
			T := &testDecodeType{testObjectType: base, cell: func(_ string, i byte) []byte { return cell(i) }, wait: 10 * time.Millisecond}
			if i == 1 {
				T.ran = ran
			}
			opts := []Option{
				WithGossipSubParams(params), WithMessageSignaturePolicy(StrictNoSign), WithMessageIdFn(seacastTestID),
				WithHaveBitmaps(seacastTestIndex),
				WithPhaseForwarding(func(tp string) bool { return !strings.HasPrefix(tp, "seg/") }, 2),
				WithIWantDiscipline(200 * time.Millisecond),
				WithIHaveCommitmentPark(1, 400*time.Millisecond, time.Minute, nil, nil),
				WithSeacast([]ObjectType{T}, nil), WithSeacastRoles(table), WithSeacastMachine(),
			}
			if i == 2 {
				opts = append(opts, WithSeacastFault("claim"))
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
		connect(t, hosts[1], hosts[2])
		time.Sleep(2 * time.Second)
		a, b := psubs[0], psubs[1]
		gsB := b.rt.(*GossipSubRouter)
		ctr := gsB.seacast.ctr
		C := hosts[2].ID()
		for i := range 3 {
			if err := a.Publish(topic, cell(byte(i))); err != nil { // lint:ignore uintcast -- a small index.
				t.Fatal(err)
			}
		}
		time.Sleep(300 * time.Millisecond)
		var claimed, armed bool
		read := func(f func()) {
			done := make(chan struct{})
			gsB.p.eval <- func() { f(); close(done) }
			<-done
		}
		read(func() {
			e := gsB.have.peers[C][seacastGID(root)]
			claimed = e != nil && e.ClaimAll
			g := gsB.seacast.group(root)
			armed = g != nil && !g.ReconsiderAt.IsZero()
		})
		if !claimed || armed || ctr.ClaimsNoted.Load() == 0 || ctr.ProbesAsked.Load() != 0 {
			t.Fatalf("before K: C's claim seen %v, B armed %v, claims noted %d, probes %d; want true, false, at least 1, 0",
				claimed, armed, ctr.ClaimsNoted.Load(), ctr.ProbesAsked.Load())
		}
		if err := a.Publish(topic, cell(3)); err != nil {
			t.Fatal(err)
		}
		time.Sleep(300 * time.Millisecond)
		var reconsiderAt time.Time
		read(func() {
			if g := gsB.seacast.group(root); g != nil {
				reconsiderAt = g.ReconsiderAt
			}
		})
		if reconsiderAt.IsZero() || ctr.ProbesAsked.Load() != 0 {
			t.Fatalf("at K: armed %v, probes %d; want armed and no probe yet (the claim came before arming)", !reconsiderAt.IsZero(), ctr.ProbesAsked.Load())
		}
		time.Sleep(time.Until(reconsiderAt) + 10*time.Millisecond)
		var probed peer.ID
		read(func() {
			if g := gsB.seacast.group(root); g != nil && g.Ext != nil {
				probed = g.Ext.asked
			}
		})
		if ctr.ProbesAsked.Load() != 1 || probed != C || ctr.LateDecodes.Load() != 0 {
			t.Fatalf("at the reconsideration: probes %d of %s, late decodes %d; want 1 of C, 0 (deferred once)", ctr.ProbesAsked.Load(), probed, ctr.LateDecodes.Load())
		}
		time.Sleep(time.Second + 100*time.Millisecond) // DEFER_WINDOW: the probe breaks, the duty decodes
		select {
		case <-ran:
		case <-time.After(time.Second):
			t.Fatal("no decode after the deferral")
		}
		if ctr.ProbesBroken.Load() != 1 || ctr.LateDecodes.Load() != 1 || ctr.Penalties.Load() != 1 {
			t.Fatalf("after DEFER_WINDOW: broken %d, late decodes %d, penalties %d; want 1, 1, 1", ctr.ProbesBroken.Load(), ctr.LateDecodes.Load(), ctr.Penalties.Load())
		}
	})
}
