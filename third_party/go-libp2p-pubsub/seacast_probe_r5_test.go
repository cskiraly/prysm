package pubsub

import (
	"bytes"
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
)

// The probe of a claim, marked, and its verdict on the claimant (seacast.md §4.6, R5). These
// fixtures read the package's behaviour through surfaces it had before R5 (the counters, the
// events, the router's handlers and the writer), so each one's distinction from the earlier rule
// shows as a failed assertion, not a missing symbol.

const r5Topic = "seg/row/1"

// r5FlaggedWant is the probe's WANT for root's index: the v4 WANT with the probe flag (0x02).
func r5FlaggedWant(root string, index uint32) string {
	b := []byte(encodeWantV2(root, index, false, 0))
	b[len(b)-2] |= 0x02
	return string(b)
}

// r5Scene is the probe's setting, as TestSeacastProbe's: A publishes 4 of 8 cells of root, B is
// complete at K = 4 with its duty armed (DecodeLater 2 s), C is B's other neighbour; A claims every
// index and B probes A for an index it lacks, which A does not hold either.
type r5Scene struct {
	t                 *testing.T
	root              string
	hosts             []host.Host
	gsA, gsB, gsC     *GossipSubRouter
	A, B, C           peer.ID
	breaks            *atomic.Int64
	evMu              *sync.Mutex
	events            map[SeacastEvent]int
	recvA             *r5Recv // what A received from B
	recvB             *r5Recv // what B received from A
	index             uint32
	mid               string
	asked             time.Time
	cell              func(i byte) []byte
	publishA, publish func(i byte) // A's, C's
}

// r5Recv records one host's frames from one peer: the cells of root by index, and the ask ids.
type r5Recv struct {
	*mockRawTracer
	mu    sync.Mutex
	from  peer.ID
	root  string
	cells map[uint32]int
	asks  map[string]int
}

func (v *r5Recv) RecvRPC(rpc *RPC) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.from == "" || rpc.from != v.from {
		return
	}
	for _, m := range rpc.GetPublish() {
		if r, i, ok := seacastTestIndex(string(m.GetData()[:haveRootLen+1])); ok && r == v.root {
			v.cells[i]++
		}
	}
	for _, w := range rpc.GetControl().GetIwant() {
		for _, mid := range w.GetMessageIDs() {
			v.asks[mid]++
		}
	}
}

func (v *r5Recv) got(mid string) int { v.mu.Lock(); defer v.mu.Unlock(); return v.asks[mid] }
func (v *r5Recv) cell(i uint32) int  { v.mu.Lock(); defer v.mu.Unlock(); return v.cells[i] }

func (s *r5Scene) on(gs *GossipSubRouter, f func()) {
	done := make(chan struct{})
	gs.p.eval <- func() { f(); close(done) }
	<-done
}

func (s *r5Scene) ev(k SeacastEvent) int {
	s.evMu.Lock()
	defer s.evMu.Unlock()
	return s.events[k]
}

// newR5Scene builds the setting and runs it to B's probe. dispatch turns on the writer's cancel
// (WithHaveDispatch), as the harness's cells run it.
func newR5Scene(t *testing.T, ctx context.Context, dispatch bool) *r5Scene {
	return newR5SceneCross(t, ctx, dispatch, nil)
}

// newR5SceneCross is newR5Scene with the type's Cross as given (the other axis's lookup).
// beforeClaim, when given, runs just before B notes A's claim (and so probes it).
func newR5SceneCross(t *testing.T, ctx context.Context, dispatch bool, cross func(root string, index uint32) (GroupIndex, string, bool), beforeClaim ...func(s *r5Scene, psubs []*PubSub)) *r5Scene {
	s := &r5Scene{t: t, root: strings.Repeat("1", haveRootLen), breaks: &atomic.Int64{}, evMu: &sync.Mutex{},
		events: map[SeacastEvent]int{}}
	s.hosts = getDefaultHosts(t, 3)
	table := &MapRoleTable{Joins: map[peer.ID]map[string]Join{}}
	for _, h := range s.hosts {
		table.Joins[h.ID()] = map[string]Join{"row": {Interest: InterestAll, Duty: DutyForward}}
	}
	s.A, s.B, s.C = s.hosts[0].ID(), s.hosts[1].ID(), s.hosts[2].ID()
	s.recvA = &r5Recv{mockRawTracer: &mockRawTracer{}, root: s.root, cells: map[uint32]int{}, asks: map[string]int{}}
	s.recvB = &r5Recv{mockRawTracer: &mockRawTracer{}, root: s.root, cells: map[uint32]int{}, asks: map[string]int{}}
	params := DefaultGossipSubParams()
	params.IDontWantMessageThreshold = 1
	psubs := getGossipsubsOptFn(ctx, s.hosts, func(i int, h host.Host) []Option {
		tp := TypeParams{RNode: 2, DeferWindow: time.Second}
		if i == 1 {
			tp.DecodeLater = func(string) (time.Duration, bool) { return 2 * time.Second, true }
		}
		T := &testObjectType{name: "row", params: tp, topics: []string{r5Topic}, store: NewMapCellStore(),
			threshold: 4, indexCount: 8, topicOf: func(r string) (string, bool) { return r5Topic, len(r) == haveRootLen },
			cross: cross}
		opts := []Option{
			WithGossipSubParams(params), WithMessageSignaturePolicy(StrictNoSign), WithMessageIdFn(seacastTestID),
			WithHaveBitmaps(probeCellIndex),
			WithPhaseForwarding(func(tp string) bool { return !strings.HasPrefix(tp, "seg/") }, 2),
			WithIWantDiscipline(200 * time.Millisecond),
		}
		if dispatch {
			opts = append(opts, WithHaveDispatch(true, false))
		}
		if i == 1 {
			opts = append(opts, WithIHaveCommitmentPark(1, 400*time.Millisecond, time.Minute, s.breaks, nil))
		} else {
			opts = append(opts, WithIHaveCommitmentPark(1, 400*time.Millisecond, time.Minute, nil, nil))
		}
		opts = append(opts, WithIHaveParkPerGroup(nil), WithSpecV05(SpecV05{PromiseAtWrite: true}, nil),
			WithSeacast([]ObjectType{T}, nil), WithSeacastRoles(table), WithSeacastMachine())
		switch i {
		case 0:
			opts = append(opts, WithRawTracer(s.recvA))
		case 1:
			opts = append(opts, WithRawTracer(s.recvB), WithSeacastEvents(func(k SeacastEvent, r string) {
				s.evMu.Lock()
				defer s.evMu.Unlock()
				if r == s.root {
					s.events[k]++
				}
			}))
		}
		return opts
	})
	for _, ps := range psubs {
		if _, err := ps.Subscribe(r5Topic); err != nil {
			t.Fatal(err)
		}
	}
	connect(t, s.hosts[0], s.hosts[1])
	connect(t, s.hosts[1], s.hosts[2])
	time.Sleep(2 * time.Second)
	s.gsA, s.gsB, s.gsC = psubs[0].rt.(*GossipSubRouter), psubs[1].rt.(*GossipSubRouter), psubs[2].rt.(*GossipSubRouter)
	s.cell = func(i byte) []byte {
		return append([]byte(s.root+string(rune(i))), bytes.Repeat([]byte{'a' + i}, 64)...)
	}
	// A pushes to B a cell it publishes only after A's and C's views of B are cleared (as
	// TestSeacastProbe's third node): else the writer projects B at K and sends nothing.
	clear := func(gs *GossipSubRouter) {
		s.on(gs, func() {
			delete(gs.have.peers[s.B], s.root)
			if q := gs.p.peers[s.B]; q != nil && q.have != nil {
				q.queueMu.Lock()
				delete(q.have.theirs, s.root)
				delete(q.have.popped, s.root)
				q.queueMu.Unlock()
			}
		})
	}
	s.publishA = func(i byte) {
		clear(s.gsA)
		if err := psubs[0].Publish(r5Topic, s.cell(i)); err != nil {
			t.Fatal(err)
		}
	}
	s.publish = func(i byte) {
		clear(s.gsC)
		if err := psubs[2].Publish(r5Topic, s.cell(i)); err != nil {
			t.Fatal(err)
		}
	}
	for i := range 4 {
		if err := psubs[0].Publish(r5Topic, s.cell(byte(i))); err != nil { // lint:ignore uintcast -- a small index.
			t.Fatal(err)
		}
	}
	time.Sleep(300 * time.Millisecond)
	for _, r := range []struct {
		v    *r5Recv
		from peer.ID
	}{{s.recvA, s.B}, {s.recvB, s.A}} {
		r.v.mu.Lock() // the tracer reads it on the router's goroutine
		r.v.from = r.from
		r.v.mu.Unlock()
	}
	for _, f := range beforeClaim {
		f(s, psubs)
	}
	s.on(s.gsB, func() {
		e := s.gsB.have.peers[s.A][s.root]
		if e == nil {
			t.Fatal("B has no ledger entry for A")
		}
		e.bits.Or(fullHaveBits(8).Bytes())
		e.claimAll = true
		s.gsB.seacastNoteClaim(s.A, s.root)
		s.asked = time.Now()
		if g := s.gsB.seacast.groups[s.root]; g != nil && g.probe != nil {
			s.index, s.mid = g.probe.index, g.probe.mid
		}
	})
	if s.mid == "" || s.index < 4 {
		t.Fatalf("B did not probe A for a cell it lacks (index %d, id %q)", s.index, s.mid)
	}
	return s
}

// afterReconsider sleeps past B's reconsider: DecodeLater's 2 s from K, or DEFER_WINDOW from the
// probe, whichever is later.
func (s *r5Scene) afterReconsider() { time.Sleep(time.Until(s.asked.Add(2500 * time.Millisecond))) }

// TestSeacastR5Verdict is R5's verdict (seacast.md §4.6's reconsider and probe_answer): only the
// claimant's own copy passes its probe, first or a duplicate; another peer's copy fills the cell
// for the member but does not pass the claimant, who is penalised once, at reconsider; the verdict
// is taken before reconsider returns on a group whole by then; a probe cancelled by the claimant's
// close gets no verdict (this package's close rule, beside the text's open case).
func TestSeacastR5Verdict(t *testing.T) {
	cases := []struct {
		name    string
		run     func(s *r5Scene)
		penalty int
	}{
		// F2's negative case, failing under the earlier rule (any peer's copy passed).
		{"another peer's copy alone: penalised", func(s *r5Scene) { s.publish(byte(s.index)) }, 1}, // lint:ignore uintcast -- a small index.
		// F2's positives, controls: they pass under either rule.
		{"the claimant's own copy: passes", func(s *r5Scene) { s.publishA(byte(s.index)) }, 0}, // lint:ignore uintcast -- a small index.
		{"the claimant's duplicate after another's copy: passes", func(s *r5Scene) {
			s.publish(byte(s.index)) // lint:ignore uintcast -- a small index.
			time.Sleep(200 * time.Millisecond)
			s.on(s.gsB, func() { s.gsB.seacastAckDuplicate(string(s.cell(byte(s.index))[:haveRootLen+1]), s.A) }) // lint:ignore uintcast -- a small index.
		}, 0},
		// The verdict on a group whole at reconsider, failing under the earlier rule (it returned first).
		{"a group whole by reconsider, no copy from the claimant: penalised", func(s *r5Scene) {
			for i := byte(4); i < 8; i++ {
				s.publish(i)
			}
		}, 1},
		// No verdict on a cancelled probe, failing under the earlier rule (it penalised it).
		{"the claimant's connection closes before the due: no verdict", func(s *r5Scene) {
			if err := s.hosts[1].Network().ClosePeer(s.A); err != nil {
				s.t.Fatal(err)
			}
		}, 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			synctestTest(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				s := newR5Scene(t, ctx, false)
				c.run(s)
				s.afterReconsider()
				ctr := s.gsB.seacast.ctr
				if p, e := ctr.Penalties.Load(), s.ev(SeacastEvPenalty); p != int64(c.penalty) || e != c.penalty {
					t.Fatalf("%s: penalties %d (events %d), want %d", c.name, p, e, c.penalty)
				}
			})
		})
	}
}

// TestSeacastR5ExemptByCell is R5's probe_asks by (group, index): an ask of the probed cell passes
// stop_pull whatever id it carries, and still after the probe's due (the exemption lasts until the
// group expires); an ask of another cell the member lacks is gated (the control). Under the earlier
// rule only the probe's own WANT id passed, and only until the due.
func TestSeacastR5ExemptByCell(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		s := newR5Scene(t, ctx, false)
		other := uint32(4)
		if s.index == 4 {
			other = 5
		}
		check := func(when string) {
			var v1, chunk, otherV1 bool
			s.on(s.gsB, func() {
				v1 = s.gsB.requestAllowed(s.C, r5Topic, encodeWant(s.root, s.index))
				chunk = s.gsB.requestAllowed(s.C, r5Topic, string(s.cell(byte(s.index))[:haveRootLen+1])) // lint:ignore uintcast -- a small index.
				otherV1 = s.gsB.requestAllowed(s.C, r5Topic, encodeWant(s.root, other))
			})
			if !v1 || !chunk || otherV1 {
				t.Fatalf("%s: the probed cell by index %v, by chunk id %v; another cell %v; want true, true, false", when, v1, chunk, otherV1)
			}
		}
		check("within the window")
		time.Sleep(time.Until(s.asked.Add(1100 * time.Millisecond)))
		check("past the due")
	})
}

// TestSeacastR5Flagged is R5's mark at the claimant (probe_of_claim, serve_probe and card 8): the
// first marked ask per asker and group, of an index the claimant holds, is answered though the
// asker's holding names the cell, and the answer passes the writer's duplicate cancel once; a
// later marked ask of the group is a plain one; an unmarked v4 ask of a held cell is dropped and
// a push of a cell the peer holds is cancelled (the controls). Under the earlier rule the mark was
// a reserved bit: the WANT was malformed and nothing answered.
func TestSeacastR5Flagged(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		s := newR5Scene(t, ctx, true)
		// A holds and B holds cells 0 to 3; A's ledger says so. B's marked ask of cell 1 goes
		// through A's handler and writer as B's RPC would.
		time.Sleep(200 * time.Millisecond)
		var holds bool
		s.on(s.gsA, func() {
			holds = s.gsA.havePeerHolds(s.B, s.root, 1)
			s.gsA.HandleRPC(&RPC{RPC: pb.RPC{Control: &pb.ControlMessage{Iwant: []*pb.ControlIWant{{MessageIDs: []string{r5FlaggedWant(s.root, 1)}}}}}, from: s.B})
		})
		if !holds {
			t.Fatal("A's ledger does not say B holds cell 1: the setting is wrong")
		}
		time.Sleep(300 * time.Millisecond)
		if n := s.recvB.cell(1); n != 1 {
			t.Fatalf("B received cell 1 from A %d times after its marked ask, want 1 (answered, through the cancel once)", n)
		}
		// The cap and the controls: a second marked ask of the group, an unmarked v4 ask, and a
		// push of a cell B holds; none reaches B.
		var second, plain []*pb.Message
		s.on(s.gsA, func() {
			second = s.gsA.handleIWant(s.B, &pb.ControlMessage{Iwant: []*pb.ControlIWant{{MessageIDs: []string{r5FlaggedWant(s.root, 2)}}}})
			plain = s.gsA.handleIWant(s.B, &pb.ControlMessage{Iwant: []*pb.ControlIWant{{MessageIDs: []string{encodeWantV2(s.root, 3, false, 0)}}}})
		})
		if len(second) != 0 || len(plain) != 0 {
			t.Fatalf("a second marked ask answered %d, an unmarked v4 ask %d; want 0, 0", len(second), len(plain))
		}
		var msg *pb.Message
		s.on(s.gsA, func() {
			if m, _, ok := s.gsA.mcache.GetForPeer(string(s.cell(1)[:haveRootLen+1]), s.B); ok {
				msg = m.Message
			}
			if msg != nil {
				s.gsA.sendRPC(s.B, &RPC{RPC: pb.RPC{Publish: []*pb.Message{msg}}}, false)
			}
		})
		if msg == nil {
			t.Fatal("A's cache has no cell 1 to push")
		}
		time.Sleep(300 * time.Millisecond)
		if n := s.recvB.cell(1); n != 1 {
			t.Fatalf("B received cell 1 from A %d times after a push of it, want still 1 (the push cancelled)", n)
		}
	})
}

// TestSeacastR5AskerKeepsProbe is R5's kept WANT at the asker (probe_waits, seacast-lite.md §7.2's
// filter_entry): while its probe waits, the member's writer writes its marked probe though it
// holds the cell by then; an unmarked ask of the held cell, v1 or v4, is dropped; once the verdict
// is taken the marked WANT is a held ask like any other. Under the earlier rule the writer kept
// every v4 ask, held or not.
func TestSeacastR5AskerKeepsProbe(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		s := newR5Scene(t, ctx, false)
		s.publish(byte(s.index)) // lint:ignore uintcast -- a small index.: B holds the probed cell by C's copy
		time.Sleep(200 * time.Millisecond)
		ordinary := encodeWantV2(s.root, s.index, false, 0)
		push := func(ids ...string) {
			s.on(s.gsB, func() {
				q := s.gsB.haveQueue(s.A)
				if q == nil {
					t.Fatal("B has no dispatch on its queue to A")
				}
				rpc := rpcWithControl(nil, nil, []*pb.ControlIWant{{MessageIDs: ids}}, nil, nil, nil)
				rpc.size = 1
				if err := q.Push(rpc, false); err != nil {
					t.Fatal(err)
				}
			})
			time.Sleep(200 * time.Millisecond)
		}
		before := s.recvA.got(s.mid)
		push(s.mid, ordinary, encodeWant(s.root, s.index))
		if n, o, v1 := s.recvA.got(s.mid)-before, s.recvA.got(ordinary), s.recvA.got(encodeWant(s.root, s.index)); n != 1 || o != 0 || v1 != 0 {
			t.Fatalf("while the probe waits: A got the marked probe %d, an unmarked v4 ask %d, a v1 ask %d; want 1, 0, 0", n, o, v1)
		}
		s.afterReconsider()
		before = s.recvA.got(s.mid)
		push(s.mid)
		if n := s.recvA.got(s.mid) - before; n != 0 {
			t.Fatalf("after the verdict: A got the marked probe of a held cell %d times, want 0", n)
		}
	})
}

// TestSeacastR5RandomIndex is R5's cell at random: 24 probes of a group whose lacking cells are 4 to
// 7 do not all take the same index. Under the earlier rule every probe took index 4.
func TestSeacastR5RandomIndex(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		hosts := getDefaultHosts(t, 2)
		table := &MapRoleTable{Joins: map[peer.ID]map[string]Join{}}
		for _, h := range hosts {
			table.Joins[h.ID()] = map[string]Join{"row": {Interest: InterestAll, Duty: DutyForward}}
		}
		psubs := getGossipsubsOptFn(ctx, hosts, func(i int, h host.Host) []Option {
			tp := TypeParams{RNode: 2, DeferWindow: time.Second}
			if i == 1 {
				tp.DecodeLater = func(string) (time.Duration, bool) { return 2 * time.Second, true }
			}
			T := &testObjectType{name: "row", params: tp, topics: []string{r5Topic}, store: NewMapCellStore(),
				threshold: 4, indexCount: 8, topicOf: func(r string) (string, bool) { return r5Topic, len(r) == haveRootLen }}
			return []Option{WithMessageSignaturePolicy(StrictNoSign), WithMessageIdFn(seacastTestID),
				WithHaveBitmaps(probeCellIndex),
				WithPhaseForwarding(func(tp string) bool { return !strings.HasPrefix(tp, "seg/") }, 2),
				WithSeacast([]ObjectType{T}, nil), WithSeacastRoles(table), WithSeacastMachine()}
		})
		for _, ps := range psubs {
			if _, err := ps.Subscribe(r5Topic); err != nil {
				t.Fatal(err)
			}
		}
		connect(t, hosts[0], hosts[1])
		time.Sleep(2 * time.Second)
		const groups = 12
		var roots []string
		for k := range groups {
			root := strings.Repeat(string(rune('a'+k)), haveRootLen)
			roots = append(roots, root)
			for i := range 4 {
				data := append([]byte(root+string(rune(i))), bytes.Repeat([]byte{'x'}, 64)...) // lint:ignore uintcast -- a small index.
				if err := psubs[0].Publish(r5Topic, data); err != nil {
					t.Fatal(err)
				}
			}
		}
		time.Sleep(1500 * time.Millisecond) // every group complete at K with its duty armed (DecodeLater 2 s)
		gsB, A := psubs[1].rt.(*GossipSubRouter), hosts[0].ID()
		// One group complete at K with its duty armed, probed again and again: each probe is a
		// fresh draw (the group's probe cleared, its duty re-armed, A's claim noted again).
		const draws = 24
		seen := map[uint32]int{}
		var ready string
		done := make(chan struct{})
		gsB.p.eval <- func() {
			defer close(done)
			for _, root := range roots {
				e, g := gsB.have.peers[A][root], gsB.seacast.groups[root]
				if e != nil && g != nil && g.state == seacastComplete && !g.reconsiderAt.IsZero() {
					ready = root
					break
				}
			}
			if ready == "" {
				return
			}
			e, g := gsB.have.peers[A][ready], gsB.seacast.groups[ready]
			e.bits.Or(fullHaveBits(8).Bytes())
			e.claimAll = true
			for range draws {
				g.probe = nil
				g.reconsiderAt = time.Now().Add(time.Hour)
				gsB.seacastNoteClaim(A, ready)
				if g.probe != nil {
					seen[g.probe.index]++
				}
			}
		}
		<-done
		if ready == "" {
			t.Fatal("no group complete at K with its duty armed: the setting is wrong")
		}
		n := 0
		for i, c := range seen {
			if i < 4 || i > 7 {
				t.Fatalf("a probe of index %d, which B holds", i)
			}
			n += c
		}
		if n != draws || len(seen) < 2 {
			t.Fatalf("probes %d of %d draws, indices drawn %v; want %d, and more than one index", n, draws, seen, draws)
		}
	})
}

// TestSeacastR5WireFlag is R5's mark on the wire: the v4 WANT's flags byte carries the probe bit
// beside standing, so a marked WANT is well formed and keeps its cell; a bit beyond the two is
// still malformed (the control). Under the earlier rule the probe bit was reserved: malformed.
func TestSeacastR5WireFlag(t *testing.T) {
	root := strings.Repeat("z", haveRootLen)
	w := r5FlaggedWant(root, 9)
	r, i, standing, hops, ok, malformed := decodeWantV2(w)
	if !ok || malformed || r != root || i != 9 || standing || hops != 0 {
		t.Fatalf("a marked WANT: ok %v malformed %v root ok %v index %d standing %v hops %d; want true, false, true, 9, false, 0",
			ok, malformed, r == root, i, standing, hops)
	}
	if w[len(w)-2]&0x02 == 0 {
		t.Fatal("the probe bit is not in the flags byte")
	}
	bad := []byte(w)
	bad[len(bad)-2] |= 0x04
	if _, _, _, _, ok, malformed := decodeWantV2(string(bad)); !ok || !malformed {
		t.Fatal("a reserved bit beyond standing and the probe is not malformed")
	}
}
