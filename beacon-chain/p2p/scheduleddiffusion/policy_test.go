package scheduleddiffusion

import (
	"iter"
	"testing"
	"time"

	"github.com/OffchainLabs/prysm/v7/container/segments"
	"github.com/OffchainLabs/prysm/v7/testing/require"
	"github.com/libp2p/go-libp2p/core/peer"
)

// The publish pass is a pure function of what we hold and what each peer has told us, so it
// can be exercised without a network. The two things worth getting right are the arithmetic
// -- held ∧ wanted ∧ ¬theirs -- and where the state is written, which is only ever inside
// OnSent(true).

var passNow = time.Date(2026, 9, 20, 12, 0, 0, 0, time.UTC)

// testGroup builds a real segmented message, so the segments in an action are genuine wire
// messages that verify rather than stubs.
type testGroup struct {
	msgs   []*segments.SegmentMessage
	hasher segments.Hasher
	count  uint32
}

func newTestGroup(t *testing.T, count int) *testGroup {
	t.Helper()
	h, err := segments.HasherByID(segments.HashSHA256)
	require.NoError(t, err)
	payload := make([]byte, count*64)
	for i := range payload {
		payload[i] = byte(i*31 + 7)
	}
	msgs, err := segments.BuildSegmentMessages(payload, 64, h)
	require.NoError(t, err)
	require.Equal(t, count, len(msgs))
	// lint:ignore uintcast -- a test group's segment count is a small literal.
	return &testGroup{msgs: msgs, hasher: h, count: uint32(count)}
}

// bits returns a bitmap over the group with exactly these indices set.
func (g *testGroup) bits(t *testing.T, idx ...uint32) *segments.Bitmap {
	t.Helper()
	b, err := segments.NewBitmap(g.count)
	require.NoError(t, err)
	for _, i := range idx {
		require.NoError(t, b.Set(i))
	}
	return b
}

// all returns a bitmap with every index set.
func (g *testGroup) all(t *testing.T) *segments.Bitmap {
	t.Helper()
	b, err := segments.NewBitmap(g.count)
	require.NoError(t, err)
	for i := uint32(0); i < g.count; i++ {
		require.NoError(t, b.Set(i))
	}
	return b
}

// input builds a publishInput serving held, counting into c.
func (g *testGroup) input(held *segments.Bitmap, c *Counters) publishInput {
	return publishInput{
		groupID: []byte("test-group"),
		held:    held,
		segment: func(index uint32) (*segments.SegmentMessage, bool) {
			if index >= g.count {
				return nil, false
			}
			return g.msgs[index], true
		},
		bump:          func(f func(*Counters)) { f(c) },
		now:           passNow,
		urgentUpdates: true,
	}
}

// yielded is one RPC the pass handed to the extension.
type yielded struct {
	peer   peer.ID
	meta   *segments.PartsMetadata
	segs   []uint32
	urgent bool
}

// runPassWith runs one publish pass, deciding admission per action with admit. It calls
// OnSent exactly where the extension does: after the action is handed over and before the
// next one is built, which is what makes the incremental latching observable.
func runPassWith(t *testing.T, in publishInput, states map[peer.ID]PeerState, partial map[peer.ID]bool,
	admit func(n int, a yielded) bool) []yielded {
	t.Helper()
	var out []yielded
	for id, action := range publishActions(in)(states, func(p peer.ID) bool { return partial[p] }) {
		require.NoError(t, action.Err)
		a := yielded{peer: id, urgent: action.Urgent}
		if len(action.EncodedPartialMessage) > 0 {
			msgs, hashers, err := segments.UnmarshalPartialMessage(action.EncodedPartialMessage)
			require.NoError(t, err)
			for i, m := range msgs {
				// Anything we send must be independently verifiable by the receiver.
				require.NoError(t, m.Verify(hashers[i]))
				a.segs = append(a.segs, m.Index)
			}
		}
		if len(action.EncodedPartsMetadata) > 0 {
			parsed, err := segments.UnmarshalPartsMetadata(action.EncodedPartsMetadata)
			require.NoError(t, err)
			a.meta = parsed
		}
		ok := true
		if admit != nil {
			ok = admit(len(out), a)
		}
		out = append(out, a)
		if action.OnSent != nil {
			action.OnSent(ok)
		}
	}
	return out
}

// runPassAdmitted runs a pass in which every action is admitted.
func runPassAdmitted(t *testing.T, in publishInput, states map[peer.ID]PeerState, partial map[peer.ID]bool) []yielded {
	t.Helper()
	return runPassWith(t, in, states, partial, nil)
}

// segsOf flattens the segments a peer was sent across the whole pass, in order.
func segsOf(actions []yielded, id peer.ID) []uint32 {
	var out []uint32
	for _, a := range actions {
		if a.peer == id {
			out = append(out, a.segs...)
		}
	}
	return out
}

func peers(n int) []peer.ID {
	out := make([]peer.ID, n)
	for i := range out {
		out[i] = peer.ID([]byte{byte('a' + i)})
	}
	return out
}

func TestSendableArithmetic(t *testing.T) {
	g := newTestGroup(t, 4)
	p := peers(1)[0]
	partial := map[peer.ID]bool{p: true}

	none := func(*testing.T) PeerState { return PeerState{} }
	for _, tc := range []struct {
		name       string
		state      func(*testing.T) PeerState
		held       func(*testing.T) *segments.Bitmap
		want       []uint32
		wantSilent bool
	}{
		{
			// Nothing heard from the peer: the standing want is the whole group, which is
			// what removes the round trip.
			name:  "nil wanted is everything",
			state: none,
			held:  g.all,
			want:  []uint32{0, 1, 2, 3},
		},
		{
			name:  "wanted follows their requests",
			state: func(t *testing.T) PeerState { return PeerState{Wanted: g.bits(t, 1, 3)} },
			held:  g.all,
			want:  []uint32{1, 3},
		},
		{
			name:  "theirs excludes what they hold",
			state: func(t *testing.T) PeerState { return PeerState{Theirs: g.bits(t, 0, 1)} },
			held:  g.all,
			want:  []uint32{2, 3},
		},
		{
			// What we pushed is believed held until the peer says otherwise.
			name:  "pushed excludes what we already sent",
			state: func(t *testing.T) PeerState { return PeerState{Pushed: g.bits(t, 0, 1)} },
			held:  g.all,
			want:  []uint32{2, 3},
		},
		{
			// A complete peer requests nothing, and an empty want set is a full stop, not
			// the "tell me nothing yet" that a nil one is.
			name:  "complete peer gets nothing",
			state: func(t *testing.T) PeerState { return PeerState{Wanted: g.bits(t)} },
			held:  g.all,
			want:  nil,
		},
		{
			name:       "held empty yields no action at all",
			state:      none,
			held:       func(t *testing.T) *segments.Bitmap { return g.bits(t) },
			want:       nil,
			wantSilent: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var c Counters
			states := map[peer.ID]PeerState{p: tc.state(t)}
			actions := runPassAdmitted(t, g.input(tc.held(t), &c), states, partial)
			require.DeepEqual(t, tc.want, segsOf(actions, p))
			if tc.wantSilent {
				require.Equal(t, 0, len(actions))
				require.Equal(t, 0, c.PeerActions)
			}
		})
	}
}

func TestOnSentLatchesOnlyOnAdmission(t *testing.T) {
	g := newTestGroup(t, 4)
	p := peers(1)[0]
	partial := map[peer.ID]bool{p: true}

	t.Run("not admitted leaves no trace and the next pass re-yields", func(t *testing.T) {
		var c Counters
		states := map[peer.ID]PeerState{p: {}}
		first := runPassWith(t, g.input(g.all(t), &c), states, partial, func(int, yielded) bool { return false })
		require.DeepEqual(t, []uint32{0, 1, 2, 3}, segsOf(first, p))
		// The update and the four frames.
		require.Equal(t, 5, c.SendsNotAdmitted)
		require.Equal(t, 0, c.SegmentsSent)
		require.Equal(t, 0, c.MetadataSent)
		// Nothing was written, so nothing is remembered.
		require.IsNil(t, states[p].Pushed)
		require.IsNil(t, states[p].Sent)

		second := runPassWith(t, g.input(g.all(t), &c), states, partial, func(int, yielded) bool { return false })
		require.DeepEqual(t, []uint32{0, 1, 2, 3}, segsOf(second, p))
		require.NotNil(t, second[0].meta)
	})

	t.Run("admitted latches theirs and sent", func(t *testing.T) {
		var c Counters
		states := map[peer.ID]PeerState{p: {}}
		first := runPassAdmitted(t, g.input(g.all(t), &c), states, partial)
		require.DeepEqual(t, []uint32{0, 1, 2, 3}, segsOf(first, p))
		require.Equal(t, 4, c.SegmentsSent)
		require.Equal(t, 0, c.SendsNotAdmitted)
		require.Equal(t, 4, states[p].Pushed.Len())
		require.Equal(t, passNow, states[p].PushedAt)
		// Evidence is theirs to give; a push is only our belief.
		require.IsNil(t, states[p].Theirs)
		require.NotNil(t, states[p].Sent)
		require.Equal(t, passNow, states[p].SentAt)
		require.Equal(t, 1, c.MetadataSent)

		second := runPassAdmitted(t, g.input(g.all(t), &c), states, partial)
		require.Equal(t, 0, len(second))
	})

	t.Run("a refused round leaves the rounds before it latched", func(t *testing.T) {
		var c Counters
		states := map[peer.ID]PeerState{p: {}}
		// One peer: the update, then round n carries index n. Refuse the middle frame only.
		first := runPassWith(t, g.input(g.bits(t, 0, 1, 2), &c), states, partial,
			func(n int, _ yielded) bool { return n != 2 })
		require.DeepEqual(t, []uint32{0, 1, 2}, segsOf(first, p))
		require.Equal(t, 1, c.SendsNotAdmitted)
		require.Equal(t, true, states[p].Pushed.Has(0))
		require.Equal(t, false, states[p].Pushed.Has(1))
		require.Equal(t, true, states[p].Pushed.Has(2))

		// Only the refused index is still sendable.
		second := runPassAdmitted(t, g.input(g.bits(t, 0, 1, 2), &c), states, partial)
		require.DeepEqual(t, []uint32{1}, segsOf(second, p))
	})
}

func TestOneSegmentPerRPCRoundRobin(t *testing.T) {
	g := newTestGroup(t, 4)
	ps := peers(2)
	partial := map[peer.ID]bool{ps[0]: true, ps[1]: true}

	t.Run("first segment of every peer before the second of any", func(t *testing.T) {
		var c Counters
		states := map[peer.ID]PeerState{
			ps[0]: {},
			// The second peer wants one index, so it drops out after round 0.
			ps[1]: {Wanted: g.bits(t, 0)},
		}
		actions := runPassAdmitted(t, g.input(g.bits(t, 0, 1, 2), &c), states, partial)

		// Every update first and alone, then the frames round-robin.
		want := []yielded{
			{peer: ps[0]},
			{peer: ps[1]},
			{peer: ps[0], segs: []uint32{0}},
			{peer: ps[1], segs: []uint32{0}},
			{peer: ps[0], segs: []uint32{1}},
			{peer: ps[0], segs: []uint32{2}},
		}
		require.Equal(t, len(want), len(actions))
		for i := range want {
			require.Equal(t, want[i].peer, actions[i].peer, "action %d", i)
			require.DeepEqual(t, want[i].segs, actions[i].segs, "action %d", i)
			// Metadata travels alone, ahead of every frame, and only it is urgent.
			require.Equal(t, i < 2, actions[i].meta != nil, "action %d metadata", i)
			require.Equal(t, i < 2, actions[i].urgent, "action %d urgency", i)
		}
		require.Equal(t, 6, c.PeerActions)
		require.Equal(t, 2, c.MetadataSent)
	})

	for _, urgent := range []bool{true, false} {
		name := "standalone update is urgent"
		if !urgent {
			name = "standalone update is not urgent"
		}
		t.Run(name, func(t *testing.T) {
			var c Counters
			// Both peers want nothing, so both updates travel alone.
			states := map[peer.ID]PeerState{ps[0]: {Wanted: g.bits(t)}, ps[1]: {Wanted: g.bits(t)}}
			in := g.input(g.all(t), &c)
			in.urgentUpdates = urgent
			actions := runPassAdmitted(t, in, states, partial)
			require.Equal(t, 2, len(actions))
			for i, a := range actions {
				require.Equal(t, 0, len(a.segs), "action %d", i)
				require.NotNil(t, a.meta)
				require.Equal(t, urgent, a.urgent, "action %d", i)
			}
			require.Equal(t, 2, c.MetadataSent)
		})
	}
}

func TestForwardingSetIsTheMesh(t *testing.T) {
	g := newTestGroup(t, 2)
	ps := peers(3)
	partial := map[peer.ID]bool{ps[0]: true, ps[1]: true, ps[2]: false}
	fresh := func() map[peer.ID]PeerState { return map[peer.ID]PeerState{ps[0]: {}, ps[1]: {}, ps[2]: {}} }

	t.Run("bounded to the mesh, only mesh peers are served", func(t *testing.T) {
		var c Counters
		in := g.input(g.all(t), &c)
		in.meshPeers = func() iter.Seq[peer.ID] {
			return func(yield func(peer.ID) bool) { yield(ps[1]) }
		}
		actions := runPassAdmitted(t, in, fresh(), partial)
		for i, a := range actions {
			require.Equal(t, ps[1], a.peer, "action %d", i)
		}
		require.DeepEqual(t, []uint32{0, 1}, segsOf(actions, ps[1]))
	})
	t.Run("unbounded, every negotiated peer in the map is served", func(t *testing.T) {
		var c Counters
		actions := runPassAdmitted(t, g.input(g.all(t), &c), fresh(), partial)
		require.DeepEqual(t, []uint32{0, 1}, segsOf(actions, ps[0]))
		require.DeepEqual(t, []uint32{0, 1}, segsOf(actions, ps[1]))
		// The peer that never negotiated partial messages gets nothing either way.
		require.Equal(t, 0, len(segsOf(actions, ps[2])))
	})
}

func TestAnnounceWindowHoldsBackUpdates(t *testing.T) {
	g := newTestGroup(t, 4)
	p := peers(1)[0]
	partial := map[peer.ID]bool{p: true}

	// A peer that wants nothing from us, so any update to it travels alone. Its last update
	// said we held nothing, so the pass has something new to say.
	stale := func(t *testing.T) PeerState {
		t.Helper()
		sent, err := segments.NewPartsMetadata(g.count)
		require.NoError(t, err)
		return PeerState{Wanted: g.bits(t), Sent: sent, SentAt: passNow.Add(-10 * time.Millisecond)}
	}

	t.Run("a non-completion update waits out the window", func(t *testing.T) {
		var c Counters
		var deferred int
		states := map[peer.ID]PeerState{p: stale(t)}
		in := g.input(g.bits(t, 0, 1), &c)
		in.announceWindow = 100 * time.Millisecond
		in.deferAnnounce = func() { deferred++ }
		require.Equal(t, 0, len(runPassAdmitted(t, in, states, partial)))
		require.Equal(t, 1, c.MetadataHeldBack)
		require.Equal(t, 1, deferred)
		require.Equal(t, 0, c.MetadataSent)
	})

	t.Run("a completion update never waits", func(t *testing.T) {
		var c Counters
		var deferred int
		states := map[peer.ID]PeerState{p: stale(t)}
		in := g.input(g.all(t), &c)
		in.complete = true
		in.announceWindow = 100 * time.Millisecond
		in.deferAnnounce = func() { deferred++ }
		actions := runPassAdmitted(t, in, states, partial)
		require.Equal(t, 1, len(actions))
		require.Equal(t, 0, actions[0].meta.Requests.Len())
		require.Equal(t, 0, c.MetadataHeldBack)
		require.Equal(t, 0, deferred)
		require.Equal(t, 1, c.MetadataSent)
	})

	t.Run("the window only delays, it does not suppress", func(t *testing.T) {
		var c Counters
		states := map[peer.ID]PeerState{p: stale(t)}
		in := g.input(g.bits(t, 0, 1), &c)
		in.announceWindow = 5 * time.Millisecond
		actions := runPassAdmitted(t, in, states, partial)
		require.Equal(t, 1, len(actions))
		require.Equal(t, 0, c.MetadataHeldBack)
	})

	t.Run("data goes while its update waits", func(t *testing.T) {
		var c Counters
		st := stale(t)
		// Now the peer wants something: the frame goes, the update still waits.
		st.Wanted = g.bits(t, 0)
		states := map[peer.ID]PeerState{p: st}
		in := g.input(g.bits(t, 0, 1), &c)
		in.announceWindow = 100 * time.Millisecond
		actions := runPassAdmitted(t, in, states, partial)
		require.Equal(t, 1, len(actions))
		require.DeepEqual(t, []uint32{0}, actions[0].segs)
		require.IsNil(t, actions[0].meta)
		require.Equal(t, 1, c.MetadataHeldBack)
		require.Equal(t, 0, c.MetadataSent)
	})
}
