package segmentbroadcaster

import (
	"sort"
	"testing"

	"github.com/OffchainLabs/prysm/v7/container/segments"
	"github.com/OffchainLabs/prysm/v7/testing/require"
	"github.com/libp2p/go-libp2p/core/peer"
)

// The publish decision is a pure function of what we hold, what each peer has told us, and
// the arm in force, so it can be exercised without a network. That is the point of keeping it
// separate from the loop: the interesting behaviour -- who gets which segment, and how many
// copies leave the node -- is the part that is cheap to get wrong and cheap to test.

// testGroup builds a real segmented message so the segments in an action are genuine wire
// messages that verify, not stubs.
type testGroup struct {
	msgs   []*segments.SegmentMessage
	hasher segments.Hasher
	held   *segments.Bitmap
}

func newTestGroup(t *testing.T, count int) *testGroup {
	t.Helper()
	h, err := segments.HasherByID(segments.HashSHA256)
	require.NoError(t, err)
	// count segments of 64 bytes each.
	payload := make([]byte, count*64)
	for i := range payload {
		payload[i] = byte(i*31 + 7)
	}
	msgs, err := segments.BuildSegmentMessages(payload, 64, h)
	require.NoError(t, err)
	require.Equal(t, count, len(msgs))
	held, err := segments.NewBitmap(uint32(count))
	require.NoError(t, err)
	for i := range msgs {
		require.NoError(t, held.Set(uint32(i)))
	}
	return &testGroup{msgs: msgs, hasher: h, held: held}
}

// input builds a publishInput serving everything in the group, at replication 1.
func (g *testGroup) input(policy Policy) publishInput {
	return g.inputAt(policy, 1)
}

// inputAt builds a publishInput at a given replication factor, so the r of dimension 9 can be
// swept in a unit test rather than only over a simulated network.
func (g *testGroup) inputAt(policy Policy, replication int) publishInput {
	return publishInput{
		held:        g.held,
		policy:      policy,
		groupID:     []byte("test-group"),
		replication: replication,
		segment: func(index uint32) (*segments.SegmentMessage, bool) {
			if index >= uint32(len(g.msgs)) {
				return nil, false
			}
			return g.msgs[index], true
		},
	}
}

// run executes one publish pass and returns, per peer, the segment indices sent.
func run(t *testing.T, in publishInput, states map[peer.ID]PeerState, partial map[peer.ID]bool) (map[peer.ID][]uint32, map[peer.ID]*segments.PartsMetadata) {
	t.Helper()
	fn := publishActions(in)
	sent := map[peer.ID][]uint32{}
	meta := map[peer.ID]*segments.PartsMetadata{}
	for id, action := range fn(states, func(p peer.ID) bool { return partial[p] }) {
		require.NoError(t, action.Err)
		if len(action.EncodedPartialMessage) > 0 {
			msgs, hashers, err := segments.UnmarshalPartialMessage(action.EncodedPartialMessage)
			require.NoError(t, err)
			for i, m := range msgs {
				// Anything we send must be independently verifiable by the receiver.
				require.NoError(t, m.Verify(hashers[i]))
				sent[id] = append(sent[id], m.Index)
			}
			sort.Slice(sent[id], func(a, b int) bool { return sent[id][a] < sent[id][b] })
		}
		if len(action.EncodedPartsMetadata) > 0 {
			parsed, err := segments.UnmarshalPartsMetadata(action.EncodedPartsMetadata)
			require.NoError(t, err)
			meta[id] = parsed
		}
	}
	return sent, meta
}

func peers(n int) []peer.ID {
	out := make([]peer.ID, n)
	for i := range out {
		out[i] = peer.ID([]byte{byte('a' + i)})
	}
	return out
}

func TestPolicyPushAll(t *testing.T) {
	g := newTestGroup(t, 8)
	ps := peers(3)
	states := map[peer.ID]PeerState{ps[0]: {}, ps[1]: {}, ps[2]: {}}
	partial := map[peer.ID]bool{ps[0]: true, ps[1]: true, ps[2]: true}

	sent, meta := run(t, g.input(PushAll), states, partial)

	// Every peer gets every segment: this is variant A's duplication, reproduced on the new
	// substrate as the control arm.
	for _, p := range ps {
		require.Equal(t, 8, len(sent[p]), "peer %s", p)
		require.Equal(t, uint32(8), meta[p].Count)
		require.Equal(t, true, meta[p].Available.Full())
	}
}

func TestPolicyPushSplit(t *testing.T) {
	g := newTestGroup(t, 8)
	ps := peers(4)
	states := map[peer.ID]PeerState{}
	partial := map[peer.ID]bool{}
	for _, p := range ps {
		states[p] = PeerState{}
		partial[p] = true
	}

	sent, meta := run(t, g.input(PushSplit), states, partial)

	// One copy of the payload leaves the node: every segment goes to exactly one peer, and
	// between them the peers receive all of it. This is the property the whole variant rests
	// on -- Q12 showed duplicate reception is what binds completion.
	seen := map[uint32]peer.ID{}
	total := 0
	for _, p := range ps {
		for _, idx := range sent[p] {
			prior, dup := seen[idx]
			require.Equal(t, false, dup, "segment %d sent to both %s and %s", idx, prior, p)
			seen[idx] = p
			total++
		}
	}
	require.Equal(t, 8, total)
	require.Equal(t, 8, len(seen))

	// Balance is *not* asserted, and that is the trade rendezvous hashing makes. The modulo it
	// replaced split 8 over 4 as exactly 2/2/2/2 -- and reshuffled all of it whenever a peer
	// appeared. Scores are random, so shares are uneven; the sender's total upload is unchanged
	// (K x r segments) and it is only the distribution across peers that varies, which averages
	// out across the senders feeding any given receiver.
	for _, p := range ps {
		// Metadata still advertises everything we hold, which is how the peers learn about
		// the segments they were not pushed.
		require.Equal(t, true, meta[p].Available.Full())
	}
}

// TestPolicySplitIsStableAsPeersAppear is the regression test for the bug this replaced.
//
// `idx % len(peers)` moved every assignment whenever the peer set grew -- and it grows on every
// heartbeat, as EmitGossip adds non-mesh peers. A segment already pushed to one peer was
// reassigned to another and pushed again, and Pushed is per peer so nothing suppressed it. That
// was measured as the whole of PushSplit's duplication: at a 3 s request timeout it reissued no
// requests at all and still took 2.37 copies per node.
func TestPolicySplitIsStableAsPeersAppear(t *testing.T) {
	g := newTestGroup(t, 32)
	all := peers(12)

	// Who owns what, over the first 4 peers, then 8, then all 12.
	ownerAt := func(n int) map[uint32]peer.ID {
		set := all[:n]
		out := map[uint32]peer.ID{}
		for _, idx := range g.held.Indices() {
			for _, p := range set {
				if assignedTo([]byte("g"), idx, p, set, 1) {
					out[idx] = p
				}
			}
		}
		return out
	}

	four, eight, twelve := ownerAt(4), ownerAt(8), ownerAt(12)
	require.Equal(t, 32, len(four))
	require.Equal(t, 32, len(eight))
	require.Equal(t, 32, len(twelve))

	// Growing the set may only move a segment *to one of the new peers*. An owner that is still
	// present must keep everything it had. Under the old modulo essentially every assignment
	// moved, and moved to peers that were already there.
	assertOnlyMovedToNewcomers := func(before, after map[uint32]peer.ID, n int) {
		t.Helper()
		existing := map[peer.ID]bool{}
		for _, p := range all[:n] {
			existing[p] = true
		}
		moved := 0
		for idx, was := range before {
			now := after[idx]
			if now == was {
				continue
			}
			moved++
			require.Equal(t, false, existing[now],
				"segment %d moved from %s to %s, which was already present", idx, was, now)
		}
		// Some movement is expected and correct -- the newcomers have to earn a share.
		require.Equal(t, true, moved > 0)
	}
	assertOnlyMovedToNewcomers(four, eight, 4)
	assertOnlyMovedToNewcomers(eight, twelve, 8)
}

// TestPolicySplitReplication sweeps r, which is dimension 9's open parameter.
func TestPolicySplitReplication(t *testing.T) {
	g := newTestGroup(t, 32)
	ps := peers(8)
	for _, r := range []int{1, 2, 3, 8, 99} {
		states := map[peer.ID]PeerState{}
		partial := map[peer.ID]bool{}
		for _, p := range ps {
			states[p] = PeerState{}
			partial[p] = true
		}
		sent, _ := run(t, g.inputAt(PushSplit, r), states, partial)

		owners := map[uint32]int{}
		total := 0
		for _, p := range ps {
			for _, idx := range sent[p] {
				owners[idx]++
				total++
			}
		}
		// Every segment is owned by exactly min(r, peers) peers, so the node's upload is
		// r copies of the payload rather than one per peer.
		want := r
		if want > len(ps) {
			want = len(ps)
		}
		require.Equal(t, 32*want, total, "r=%d", r)
		for idx, n := range owners {
			require.Equal(t, want, n, "r=%d segment %d", r, idx)
		}
		require.Equal(t, 32, len(owners), "r=%d", r)
	}
}

func TestPolicyPushNone(t *testing.T) {
	g := newTestGroup(t, 8)
	ps := peers(2)
	states := map[peer.ID]PeerState{ps[0]: {}, ps[1]: {}}
	partial := map[peer.ID]bool{ps[0]: true, ps[1]: true}

	sent, meta := run(t, g.input(PushNone), states, partial)

	// Announce only. Nothing moves until a peer asks, which is the arm that isolates the
	// round-trip cost of pulling.
	for _, p := range ps {
		require.Equal(t, 0, len(sent[p]))
		require.Equal(t, true, meta[p].Available.Full())
	}
}

func TestPolicyServesRequestsUnderEveryArm(t *testing.T) {
	for _, policy := range []Policy{PushNone, PushSplit, PushAll} {
		t.Run(policy.String(), func(t *testing.T) {
			g := newTestGroup(t, 8)
			ps := peers(2)

			// ps[0] has everything but segment 5, and asks for it. Under PushNone nothing
			// else would move, so what arrives is exactly the request.
			recvd, err := segments.NewPartsMetadata(8)
			require.NoError(t, err)
			for i := uint32(0); i < 8; i++ {
				if i != 5 {
					require.NoError(t, recvd.Available.Set(i))
				}
			}
			require.NoError(t, recvd.Requests.Set(5))

			states := map[peer.ID]PeerState{ps[0]: {Recvd: recvd}, ps[1]: {}}
			partial := map[peer.ID]bool{ps[0]: true, ps[1]: true}
			sent, _ := run(t, g.input(policy), states, partial)

			require.DeepEqual(t, []uint32{5}, sent[ps[0]])
			// The request is cleared once served, so it is not served again.
			require.Equal(t, false, states[ps[0]].Recvd.Requests.Has(5))
			require.Equal(t, true, states[ps[0]].Pushed.Has(5))
		})
	}
}

func TestPolicySkipsWhatThePeerAlreadyHolds(t *testing.T) {
	g := newTestGroup(t, 8)
	p := peers(1)[0]

	recvd, err := segments.NewPartsMetadata(8)
	require.NoError(t, err)
	for _, i := range []uint32{0, 1, 2, 3} {
		require.NoError(t, recvd.Available.Set(i))
	}
	states := map[peer.ID]PeerState{p: {Recvd: recvd}}
	partial := map[peer.ID]bool{p: true}

	sent, _ := run(t, g.input(PushAll), states, partial)
	require.DeepEqual(t, []uint32{4, 5, 6, 7}, sent[p])
}

func TestPolicyDoesNotResend(t *testing.T) {
	g := newTestGroup(t, 8)
	p := peers(1)[0]
	states := map[peer.ID]PeerState{p: {}}
	partial := map[peer.ID]bool{p: true}

	first, firstMeta := run(t, g.input(PushAll), states, partial)
	require.Equal(t, 8, len(first[p]))
	require.Equal(t, true, firstMeta[p] != nil)

	// A second pass with nothing new to say sends nothing at all: no segments, and no
	// metadata either, because the extension asks implementations to suppress a repeat.
	second, secondMeta := run(t, g.input(PushAll), states, partial)
	require.Equal(t, 0, len(second[p]))
	require.Equal(t, true, secondMeta[p] == nil)
}

func TestPolicyNonPartialPeerGetsMetadataOnly(t *testing.T) {
	g := newTestGroup(t, 8)
	ps := peers(2)
	states := map[peer.ID]PeerState{ps[0]: {}, ps[1]: {}}
	// ps[1] speaks only whole messages. It must never be sent a partial message -- the
	// extension would drop it anyway, and gossipsub delivers it the whole envelope instead.
	partial := map[peer.ID]bool{ps[0]: true, ps[1]: false}

	sent, meta := run(t, g.input(PushAll), states, partial)
	require.Equal(t, 8, len(sent[ps[0]]))
	require.Equal(t, 0, len(sent[ps[1]]))
	// Metadata still goes out, so the peer can tell us if it later wants segments.
	require.Equal(t, true, meta[ps[1]] != nil)
}

func TestPolicySplitIgnoresNonPartialPeersWhenAssigning(t *testing.T) {
	g := newTestGroup(t, 4)
	ps := peers(4)
	states := map[peer.ID]PeerState{}
	partial := map[peer.ID]bool{}
	for i, p := range ps {
		states[p] = PeerState{}
		// Only half the peers speak segments.
		partial[p] = i%2 == 0
	}

	sent, _ := run(t, g.input(PushSplit), states, partial)

	// The split is over the peers that can receive segments, so all four segments still go
	// out. A partition over all peers would have silently dropped half the payload.
	total := 0
	for _, p := range ps {
		total += len(sent[p])
	}
	require.Equal(t, 4, total)
}

func TestPolicyRequestsGoToOnePeer(t *testing.T) {
	g := newTestGroup(t, 4)
	// We hold segments 0 and 1 only, and want 2 and 3.
	held, err := segments.NewBitmap(4)
	require.NoError(t, err)
	require.NoError(t, held.Set(0))
	require.NoError(t, held.Set(1))
	wanted, err := segments.NewBitmap(4)
	require.NoError(t, err)
	require.NoError(t, wanted.Set(2))
	require.NoError(t, wanted.Set(3))

	ps := peers(3)
	// All three peers advertise both missing segments.
	states := map[peer.ID]PeerState{}
	partial := map[peer.ID]bool{}
	for _, p := range ps {
		recvd, err := segments.NewPartsMetadata(4)
		require.NoError(t, err)
		require.NoError(t, recvd.Available.Set(2))
		require.NoError(t, recvd.Available.Set(3))
		states[p] = PeerState{Recvd: recvd}
		partial[p] = true
	}

	// The ledger stand-in: first caller for an index wins it.
	claimed := map[uint32]peer.ID{}
	in := publishInput{
		held:   held,
		policy: PushSplit,
		wanted: wanted,
		segment: func(index uint32) (*segments.SegmentMessage, bool) {
			return g.msgs[index], true
		},
		requestFrom: func(index uint32, p peer.ID, _ *int) bool {
			if prior, ok := claimed[index]; ok {
				return prior == p
			}
			claimed[index] = p
			return true
		},
	}

	_, meta := run(t, in, states, partial)

	// Each missing segment is requested from exactly one peer. Asking all three would put
	// three copies on our own downlink, which is the cost the variant exists to remove.
	requests := map[uint32]int{}
	for _, m := range meta {
		for _, idx := range m.Requests.Indices() {
			requests[idx]++
		}
	}
	require.Equal(t, 2, len(requests))
	require.Equal(t, 1, requests[2])
	require.Equal(t, 1, requests[3])
}

func TestPolicyDoesNotRequestFromAPeerThatLacksIt(t *testing.T) {
	g := newTestGroup(t, 4)
	held, err := segments.NewBitmap(4)
	require.NoError(t, err)
	require.NoError(t, held.Set(0))
	wanted, err := segments.NewBitmap(4)
	require.NoError(t, err)
	for _, i := range []uint32{1, 2, 3} {
		require.NoError(t, wanted.Set(i))
	}

	p := peers(1)[0]
	recvd, err := segments.NewPartsMetadata(4)
	require.NoError(t, err)
	// The peer holds only segment 1.
	require.NoError(t, recvd.Available.Set(1))
	states := map[peer.ID]PeerState{p: {Recvd: recvd}}
	partial := map[peer.ID]bool{p: true}

	in := publishInput{
		held:        held,
		policy:      PushNone,
		wanted:      wanted,
		segment:     func(index uint32) (*segments.SegmentMessage, bool) { return g.msgs[index], true },
		requestFrom: func(uint32, peer.ID, *int) bool { return true },
	}
	_, meta := run(t, in, states, partial)

	// Only the segment the peer actually advertises is requested from it.
	require.DeepEqual(t, []uint32{1}, meta[p].Requests.Indices())
}

func TestPolicyReportsWhatItSent(t *testing.T) {
	g := newTestGroup(t, 8)
	ps := peers(2)

	recvd, err := segments.NewPartsMetadata(8)
	require.NoError(t, err)
	require.NoError(t, recvd.Requests.Set(3))
	states := map[peer.ID]PeerState{ps[0]: {Recvd: recvd}, ps[1]: {}}
	partial := map[peer.ID]bool{ps[0]: true, ps[1]: true}

	var pushed, served int
	in := g.input(PushNone)
	in.onSend = func(_ peer.ID, p, s int, _ bool) {
		pushed += p
		served += s
	}
	_, _ = run(t, in, states, partial)

	// Under PushNone every byte that moves is a byte someone asked for, so the split between
	// volunteered and requested is what distinguishes the arms.
	require.Equal(t, 0, pushed)
	require.Equal(t, 1, served)
}

func TestPolicyStateUnchangedOnError(t *testing.T) {
	g := newTestGroup(t, 8)
	p := peers(1)[0]
	states := map[peer.ID]PeerState{p: {}}
	partial := map[peer.ID]bool{p: true}

	in := g.input(PushAll)
	// A segment that cannot be marshalled: a descriptor-free message fails Marshal.
	in.segment = func(uint32) (*segments.SegmentMessage, bool) {
		return &segments.SegmentMessage{}, true
	}

	var actions int
	for _, action := range publishActions(in)(states, func(p peer.ID) bool { return partial[p] }) {
		actions++
		require.NotNil(t, action.Err)
	}
	require.Equal(t, 1, actions)
	// The failed action left no trace, so a later pass starts clean rather than believing
	// segments were sent that never were.
	require.Equal(t, true, states[p].Pushed == nil)
	require.Equal(t, true, states[p].Sent == nil)
}

// TestWillPushIsSymmetricallyComputable is the property the coordinated push rests on: sender
// and receiver, running the same predicate over the same four inputs, must agree. If they can
// disagree, a receiver either asks for what is coming (the bug this fixes) or waits for
// something that was never sent.
func TestWillPushIsSymmetricallyComputable(t *testing.T) {
	group := []byte("group-1")
	a, b := peer.ID("alice"), peer.ID("bob")

	t.Run("both ends of a link compute the same answer", func(t *testing.T) {
		for idx := uint32(0); idx < 64; idx++ {
			// What the sender decides, and what the receiver predicts of it, are one call.
			require.Equal(t,
				willPush(group, idx, a, b, 8),
				willPush(group, idx, a, b, 8),
				"idx %d", idx)
		}
	})

	t.Run("direction matters, so a link is not symmetric", func(t *testing.T) {
		// a->b and b->a are independent decisions. If they were the same, every push would be
		// mirrored and the two directions could not carry different shares.
		differs := 0
		for idx := uint32(0); idx < 256; idx++ {
			if willPush(group, idx, a, b, 4) != willPush(group, idx, b, a, 4) {
				differs++
			}
		}
		require.Equal(t, true, differs > 0)
	})

	t.Run("sender identity changes the choice", func(t *testing.T) {
		// The bug in the rendezvous rule this replaces: its score ignored the sender, so every
		// sender ranked receivers identically and their collisions were correlated rather than
		// independent. Two senders must disagree about a shared receiver at least sometimes.
		c := peer.ID("carol")
		differs := 0
		for idx := uint32(0); idx < 256; idx++ {
			if willPush(group, idx, a, c, 4) != willPush(group, idx, b, c, 4) {
				differs++
			}
		}
		require.Equal(t, true, differs > 50, "only %d of 256 differed", differs)
	})

	t.Run("density tracks the divisor", func(t *testing.T) {
		// Each sender pushes with probability 1/divisor, so a receiver with in-degree P expects
		// P/divisor copies. Checked loosely -- this is a hash, not a counter.
		for _, divisor := range []int{2, 4, 8, 20} {
			hits := 0
			const trials = 4000
			for idx := uint32(0); idx < trials; idx++ {
				if willPush(group, idx, a, b, divisor) {
					hits++
				}
			}
			want := float64(trials) / float64(divisor)
			require.Equal(t, true, float64(hits) > want*0.7 && float64(hits) < want*1.4,
				"divisor %d: %d hits, want about %.0f", divisor, hits, want)
		}
	})

	t.Run("group id separates concurrent groups", func(t *testing.T) {
		differs := 0
		for idx := uint32(0); idx < 256; idx++ {
			if willPush([]byte("g1"), idx, a, b, 4) != willPush([]byte("g2"), idx, a, b, 4) {
				differs++
			}
		}
		require.Equal(t, true, differs > 50, "only %d of 256 differed", differs)
	})

	t.Run("divisor zero pushes nothing, so the mode is off by default", func(t *testing.T) {
		for idx := uint32(0); idx < 32; idx++ {
			require.Equal(t, false, willPush(group, idx, a, b, 0))
		}
	})
}

// TestPolicyCoordinatedPushSuppressesTheRequest is the behaviour the fix exists for: a peer that
// holds a segment and is predicted to push it must not also be asked for it.
func TestPolicyCoordinatedPushSuppressesTheRequest(t *testing.T) {
	g := newTestGroup(t, 64)
	self := peer.ID("me")
	p := peers(1)[0]
	const divisor = 4

	// We hold nothing; the peer holds everything.
	held, err := segments.NewBitmap(64)
	require.NoError(t, err)
	wanted, err := segments.NewBitmap(64)
	require.NoError(t, err)
	for i := uint32(0); i < 64; i++ {
		require.NoError(t, wanted.Set(i))
	}
	recvd, err := segments.NewPartsMetadata(64)
	require.NoError(t, err)
	for i := uint32(0); i < 64; i++ {
		require.NoError(t, recvd.Available.Set(i))
	}

	base := publishInput{
		held:        held,
		policy:      PushSplit,
		wanted:      wanted,
		groupID:     []byte("test-group"),
		self:        self,
		pushDivisor: divisor,
		segment:     func(i uint32) (*segments.SegmentMessage, bool) { return g.msgs[i], true },
		requestFrom: func(uint32, peer.ID, *int) bool { return true },
	}

	// Which indices this peer is predicted to push to us.
	var predicted []uint32
	for i := uint32(0); i < 64; i++ {
		if willPush(base.groupID, i, p, self, divisor) {
			predicted = append(predicted, i)
		}
	}
	require.Equal(t, true, len(predicted) > 0, "no pushes predicted, test proves nothing")

	t.Run("within the grace window they are not requested", func(t *testing.T) {
		in := base
		in.pushGrace = true
		states := map[peer.ID]PeerState{p: {Recvd: recvd.Clone()}}
		_, meta := run(t, in, states, map[peer.ID]bool{p: true})
		for _, i := range predicted {
			require.Equal(t, false, meta[p].Requests.Has(i),
				"segment %d was requested even though the peer is going to push it", i)
		}
		// Everything else still is -- suppression must be narrow.
		require.Equal(t, 64-len(predicted), meta[p].Requests.Len())
	})

	t.Run("past the grace window everything missing is requested", func(t *testing.T) {
		// A predicted push that never arrives must not strand the segment. This is the reason
		// the suppression is time-bounded rather than permanent.
		in := base
		in.pushGrace = false
		states := map[peer.ID]PeerState{p: {Recvd: recvd.Clone()}}
		_, meta := run(t, in, states, map[peer.ID]bool{p: true})
		require.Equal(t, 64, meta[p].Requests.Len())
	})
}

func TestPolicyPushPhase(t *testing.T) {
	g := newTestGroup(t, 8)
	ps := peers(3)
	partial := map[peer.ID]bool{ps[0]: true, ps[1]: true, ps[2]: true}

	holdingState := func(t *testing.T, indices ...uint32) PeerState {
		t.Helper()
		m, err := segments.NewPartsMetadata(8)
		require.NoError(t, err)
		for _, idx := range indices {
			require.NoError(t, m.Available.Set(idx))
		}
		return PeerState{Recvd: m}
	}

	t.Run("early diffusion pushes replication-wide", func(t *testing.T) {
		states := map[peer.ID]PeerState{ps[0]: {}, ps[1]: {}, ps[2]: {}}
		sent, _ := run(t, g.inputAt(PushPhase, 2), states, partial)
		// Nobody is known to hold anything, so every segment goes to its top-2 rendezvous
		// peers: 16 sends over 8 segments, no segment more than twice.
		perSegment := map[uint32]int{}
		total := 0
		for _, idxs := range sent {
			total += len(idxs)
			for _, idx := range idxs {
				perSegment[idx]++
			}
		}
		require.Equal(t, 16, total)
		for idx, n := range perSegment {
			require.Equal(t, 2, n, "segment %d", idx)
		}
	})

	t.Run("known holders decay the push degree", func(t *testing.T) {
		// One peer holds segment 3: its push degree drops to 1, everything else stays at 2.
		states := map[peer.ID]PeerState{ps[0]: holdingState(t, 3), ps[1]: {}, ps[2]: {}}
		sent, _ := run(t, g.inputAt(PushPhase, 2), states, partial)
		perSegment := map[uint32]int{}
		for _, idxs := range sent {
			for _, idx := range idxs {
				perSegment[idx]++
			}
		}
		require.Equal(t, 1, perSegment[3])
		for idx, n := range perSegment {
			if idx == 3 {
				continue
			}
			require.Equal(t, 2, n, "segment %d", idx)
		}
	})

	t.Run("late diffusion pushes nothing", func(t *testing.T) {
		// Two of three peers hold everything: with replication 2 the push degree is zero
		// for every segment, so the third peer -- who holds nothing -- gets announce only.
		all := []uint32{0, 1, 2, 3, 4, 5, 6, 7}
		states := map[peer.ID]PeerState{
			ps[0]: holdingState(t, all...),
			ps[1]: holdingState(t, all...),
			ps[2]: {},
		}
		sent, meta := run(t, g.inputAt(PushPhase, 2), states, partial)
		require.Equal(t, 0, len(sent[ps[2]]))
		require.Equal(t, true, meta[ps[2]].Available.Full())
	})
}
