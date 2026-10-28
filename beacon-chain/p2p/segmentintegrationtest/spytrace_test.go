package segmentintegrationtest

// The spy tracer (SEGMENT_SPY_TRACE=<path>): every node's first delivery of each frame per link,
// what each link sender has named to it of the message, as that grows, and the node's mesh, with
// the link sender (RPC.From, never the message's From), in publish-relative virtual time. Spies
// are passive, so one all-node log is subsampled offline into any spy set.
//
// The file is gzip'd CSV, one gzip member per message (multi-member gzip reads as one stream):
//
//	H,msg,publisher,n,frames,need                one per message
//	E,msg,receiver,sender,index,t_ns,kind,count  push|answer: a frame (answer = node had asked sender), count 1
//	                                             have|idw|claim: index -1, count = distinct frames the sender has
//	                                             named on the link so far by IHAVE (have), by IDONTWANT ids or HAVE
//	                                             bitmap (idw), or by the fork's no-hold claim bitmap (claim, isHaveClaim)
//	                                             stem: a frame of the source's hand-off to its stem (SEGMENT_SRC_STEM), count 1
//	M,msg,node,peer                              the node's mesh at publish
//	G,msg,node,peer,t_ns / P,msg,node,peer,t_ns  a graft / prune by the node after publish
//	S,msg,receiver,sender,t_ns,kind,i;j;...      SEGMENT_SPY_TRACE_SETS=1 only: the indices one RPC newly named
//	                                             on the link's have|idw|claim channel, one row per RPC and kind
//
// An announce kind gets a row per link when the link first names the message and whenever its
// count crosses a power of four, need or frames: 5 rows at most per link and kind at 128 frames.
// The S rows carry the exact sets the counts summarise: at most frames indices per link and kind
// in all, one row per announcing RPC (measured at n = 100: 95 k rows and 0.7 MB gzip'd per message,
// five times the trace without them).
// Format 1 (e4ec09aaae) had no count column, one have and one idw row per link, claims among the
// idw rows, and M at report time; a reader of each kind's first row per link reads both.

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"maps"
	"math/bits"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/OffchainLabs/prysm/v7/testing/require"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pubsubpb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
)

const (
	spyPush uint8 = iota
	spyAnswer
	spyHave
	spyIDW
	spyClaim
	spyStem
)

var spyKindName = [...]string{"push", "answer", "have", "idw", "claim", "stem"}

// The fork's HAVE pseudo id (havebitmap.go): magic, version, 32-byte root, then the bitmap, behind a load byte in version 2.
const (
	spyHaveHeaderLen    = 4 + 1 + 32
	spyHaveVersion      = 1
	spyHaveVersionLoad  = 2
	spyHaveVersionClaim = 3
)

// spyLink keys a first delivery: the sender and the frame index.
type spyLink struct {
	from peer.ID
	idx  int32
}

// spyAnnKey is one announce channel of a link: the sender and the kind.
type spyAnnKey struct {
	from peer.ID
	kind uint8
}

// spyAnn is what a sender has named on one announce channel, and the growth step of its last row.
type spyAnn struct {
	named []bool
	count int
	step  int
}

type spyEvent struct {
	from  peer.ID
	at    time.Time
	idx   int32
	kind  uint8
	count int
}

// spySet is the indices one RPC newly named on an announce channel (SEGMENT_SPY_TRACE_SETS).
type spySet struct {
	from peer.ID
	at   time.Time
	kind uint8
	idx  []int32
}

// spyChurn is one change of the node's mesh.
type spyChurn struct {
	peer  peer.ID
	at    time.Time
	graft bool
}

// spyTracer is one node's spy log for the message in flight; a RawTracer next to the recording one.
type spyTracer struct {
	mu     sync.Mutex
	known  map[digest]int
	ids    map[string]int
	group  string
	frames int
	need   int
	asked  map[peer.ID]map[int32]bool
	seen   map[spyLink]bool
	ann    map[spyAnnKey]*spyAnn
	events []spyEvent
	// withSets keeps sets, the exact indices each announcing RPC added (SEGMENT_SPY_TRACE_SETS).
	withSets bool
	sets     []spySet
	// mesh is the node's graft count per peer, kept from its start; churn is its changes since setMessage.
	mesh  map[peer.ID]int
	churn []spyChurn
}

// spyTraceOut is the trace path, empty when the instrument is off.
func spyTraceOut() string { return os.Getenv("SEGMENT_SPY_TRACE") }

// spyTraceSets reports whether the trace carries the announced index sets (S rows).
func spyTraceSets() bool { return os.Getenv("SEGMENT_SPY_TRACE_SETS") != "" }

// setMessage starts the log for the message whose frames are arm; ids is its id to index map.
func (s *spyTracer) setMessage(arm wireArm, ids map[string]int, group string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.known, s.ids, s.group = arm.known, ids, group
	s.frames, s.need = len(arm.msgs), arm.completeAt()
	s.asked = make(map[peer.ID]map[int32]bool)
	s.seen = make(map[spyLink]bool)
	s.ann = make(map[spyAnnKey]*spyAnn)
	s.events = nil
	s.sets = nil
	s.churn = nil
}

// spyMessageIDs is the id to frame index map and the group root of arm, shared read-only by every node.
func spyMessageIDs(arm wireArm) (map[string]int, string) {
	ids := make(map[string]int, len(arm.msgs))
	group := ""
	for i, id := range segmentIDs(arm) {
		ids[id] = i
		if k, ok := segIDKey(id); ok && group == "" {
			group = string(k.group[:])
		}
	}
	return ids, group
}

// spyHaveBitmap reads mid as a HAVE pseudo id for group: its bitmap, and whether it is the no-hold claim.
func spyHaveBitmap(mid, group string) (bitmap string, claim, ok bool) {
	if group == "" || len(mid) < spyHaveHeaderLen || mid[:4] != "HAVE" || mid[5:spyHaveHeaderLen] != group {
		return "", false, false
	}
	switch mid[4] {
	case spyHaveVersion, spyHaveVersionClaim:
		return mid[spyHaveHeaderLen:], mid[4] == spyHaveVersionClaim, true
	case spyHaveVersionLoad:
		if len(mid) > spyHaveHeaderLen {
			return mid[spyHaveHeaderLen+1:], false, true
		}
	}
	return "", false, false
}

// spyStep is the growth step of a count: its power of four, then whether it reached need and all frames.
func spyStep(count, need, frames int) int {
	st := 4 * ((bits.Len(uint(count)) + 1) / 2)
	if count >= need {
		st++
	}
	if count >= frames {
		st += 2
	}
	return st
}

// name folds control id mid from sender into its channel of kind (claim for a no-hold claim) and
// returns the channel and its kind, or nil when mid names another message; with sets kept, the
// indices it newly names join fresh[kind]. Under s.mu.
func (s *spyTracer) name(from peer.ID, kind uint8, mid string, fresh map[uint8][]int32) (*spyAnn, uint8) {
	i, perID := s.ids[mid]
	bitmap, claim, isHave := "", false, false
	if !perID {
		if bitmap, claim, isHave = spyHaveBitmap(mid, s.group); !isHave {
			return nil, kind
		}
		if claim {
			kind = spyClaim
		}
	}
	k := spyAnnKey{from: from, kind: kind}
	a := s.ann[k]
	if a == nil {
		a = &spyAnn{named: make([]bool, s.frames), step: -1}
		s.ann[k] = a
	}
	mark := func(j int) {
		if j < len(a.named) && !a.named[j] {
			a.named[j] = true
			a.count++
			if fresh != nil {
				fresh[kind] = append(fresh[kind], int32(j))
			}
		}
	}
	if perID {
		mark(i)
		return a, kind
	}
	for at := 0; at < len(bitmap); at++ {
		for b := range 8 {
			if bitmap[at]&(1<<b) != 0 {
				mark(at*8 + b)
			}
		}
	}
	return a, kind
}

func (s *spyTracer) RecvRPC(rpc *pubsub.RPC) {
	if rpc == nil {
		return
	}
	from := rpc.From()
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.seen == nil {
		return
	}
	for _, m := range rpc.GetPublish() {
		i, ok := s.known[digestOf(m.GetData())]
		if !ok {
			continue
		}
		l := spyLink{from: from, idx: int32(i)}
		if s.seen[l] {
			continue
		}
		s.seen[l] = true
		kind := spyPush
		if s.asked[from][l.idx] {
			kind = spyAnswer
		}
		s.events = append(s.events, spyEvent{from: from, at: now, idx: l.idx, kind: kind, count: 1})
	}
	s.announce(from, now, rpc.GetControl())
}

// announce logs what ctl from sender names of the message: a row per channel whose count
// stepped, at most one per channel, as the whole RPC is folded first. Under s.mu.
func (s *spyTracer) announce(from peer.ID, now time.Time, ctl *pubsubpb.ControlMessage) {
	if ctl == nil {
		return
	}
	touched := make(map[uint8]*spyAnn, 3)
	var fresh map[uint8][]int32
	if s.withSets {
		fresh = make(map[uint8][]int32, 3)
	}
	for _, ih := range ctl.GetIhave() {
		for _, mid := range ih.GetMessageIDs() {
			if a, kind := s.name(from, spyHave, mid, fresh); a != nil {
				touched[kind] = a
			}
		}
	}
	for _, d := range ctl.GetIdontwant() {
		for _, mid := range d.GetMessageIDs() {
			if a, kind := s.name(from, spyIDW, mid, fresh); a != nil {
				touched[kind] = a
			}
		}
	}
	for _, kind := range []uint8{spyHave, spyIDW, spyClaim} {
		a := touched[kind]
		if a == nil {
			continue
		}
		if idx := fresh[kind]; len(idx) > 0 {
			s.sets = append(s.sets, spySet{from: from, at: now, kind: kind, idx: idx})
		}
		if st := spyStep(a.count, s.need, s.frames); st > a.step {
			a.step = st
			s.events = append(s.events, spyEvent{from: from, at: now, idx: -1, kind: kind, count: a.count})
		}
	}
}

// stemFrame records frame idx of the source's hand-off, from the source, as the link's first delivery.
func (s *spyTracer) stemFrame(from peer.ID, idx int, at time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.seen == nil {
		return
	}
	l := spyLink{from: from, idx: int32(idx)}
	if s.seen[l] {
		return
	}
	s.seen[l] = true
	s.events = append(s.events, spyEvent{from: from, at: at, idx: l.idx, kind: spyStem, count: 1})
}

func (s *spyTracer) SendRPC(rpc *pubsub.RPC, p peer.ID) {
	if rpc == nil || rpc.GetControl() == nil || len(rpc.GetControl().GetIwant()) == 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.asked == nil {
		return
	}
	for _, iw := range rpc.GetControl().GetIwant() {
		for _, mid := range iw.GetMessageIDs() {
			idx := int32(-1)
			if i, ok := s.ids[mid]; ok {
				idx = int32(i)
			} else if root, i, ok := pubsub.DecodeHaveWant(mid); ok && root == s.group {
				idx = int32(i) // lint:ignore uintcast -- a frame index, bounded by the arm.
			}
			if idx < 0 {
				continue
			}
			if s.asked[p] == nil {
				s.asked[p] = make(map[int32]bool)
			}
			s.asked[p][idx] = true
		}
	}
}

func (s *spyTracer) Graft(p peer.ID, _ string) { s.meshChange(p, true) }
func (s *spyTracer) Prune(p peer.ID, _ string) { s.meshChange(p, false) }

// meshChange counts a graft or prune of p, as the recording tracer does, and logs it.
func (s *spyTracer) meshChange(p peer.ID, graft bool) {
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.mesh == nil {
		s.mesh = make(map[peer.ID]int)
	}
	if graft {
		s.mesh[p]++
	} else {
		s.mesh[p]--
	}
	s.churn = append(s.churn, spyChurn{peer: p, at: now, graft: graft})
}

// meshAt is the node's mesh just before start, the current one with the changes from start on
// undone, and those changes in order. Under s.mu.
func (s *spyTracer) meshAt(start time.Time) ([]peer.ID, []spyChurn) {
	count := maps.Clone(s.mesh)
	i := len(s.churn)
	for i > 0 && !s.churn[i-1].at.Before(start) {
		i--
		if c := s.churn[i]; c.graft {
			count[c.peer]--
		} else {
			count[c.peer]++
		}
	}
	mesh := make([]peer.ID, 0, len(count))
	for p, c := range count {
		if c > 0 {
			mesh = append(mesh, p)
		}
	}
	sort.Slice(mesh, func(a, b int) bool { return mesh[a] < mesh[b] })
	return mesh, append([]spyChurn(nil), s.churn[i:]...)
}

func (*spyTracer) OnNewOutboundStream(peer.ID, protocol.ID) {}
func (*spyTracer) OnClosedOutboundStream(peer.ID)           {}
func (*spyTracer) Join(string)                              {}
func (*spyTracer) Leave(string)                             {}
func (*spyTracer) ValidateMessage(*pubsub.Message)          {}
func (*spyTracer) DeliverMessage(*pubsub.Message)           {}
func (*spyTracer) RejectMessage(*pubsub.Message, string)    {}
func (*spyTracer) DuplicateMessage(*pubsub.Message)         {}
func (*spyTracer) ThrottlePeer(peer.ID)                     {}
func (*spyTracer) DropRPC(*pubsub.RPC, peer.ID)             {}
func (*spyTracer) UndeliverableMessage(*pubsub.Message)     {}

// writeSpyTrace appends message msg's rows as one gzip member (the first message truncates) and
// returns the E rows, the mesh changes and the S rows written; spies[i] is node i's log, nil where absent.
func writeSpyTrace(path string, msg, pub int, arm wireArm, spies []*spyTracer, hosts []peer.ID, start time.Time) (int, int, int, error) {
	flag := os.O_CREATE | os.O_WRONLY | os.O_APPEND
	if msg <= 1 {
		flag = os.O_CREATE | os.O_WRONLY | os.O_TRUNC
	}
	f, err := os.OpenFile(path, flag, 0o644)
	if err != nil {
		return 0, 0, 0, err
	}
	zw := gzip.NewWriter(f)
	w := bufio.NewWriterSize(zw, 1<<20)
	idx := make(map[peer.ID]int, len(hosts))
	for i, h := range hosts {
		idx[h] = i
	}
	node := func(p peer.ID) int {
		if i, ok := idx[p]; ok {
			return i
		}
		return -1
	}
	// A write error sticks to the bufio.Writer, so Flush reports it for every row.
	row := func(format string, a ...any) { _, _ = fmt.Fprintf(w, format, a...) }
	row("H,%d,%d,%d,%d,%d\n", msg, pub, len(hosts), len(arm.msgs), arm.completeAt())
	rows, changes, setRows := 0, 0, 0
	var line []byte
	for i, s := range spies {
		if s == nil {
			continue
		}
		s.mu.Lock()
		evs := append([]spyEvent(nil), s.events...)
		sets := append([]spySet(nil), s.sets...)
		mesh, churn := s.meshAt(start)
		s.mu.Unlock()
		sort.SliceStable(evs, func(a, b int) bool { return evs[a].at.Before(evs[b].at) })
		for _, e := range evs {
			row("E,%d,%d,%d,%d,%d,%s,%d\n", msg, i, node(e.from), e.idx, e.at.Sub(start).Nanoseconds(), spyKindName[e.kind], e.count)
			rows++
		}
		for _, p := range mesh {
			row("M,%d,%d,%d\n", msg, i, node(p))
		}
		for _, c := range churn {
			kind := "P"
			if c.graft {
				kind = "G"
			}
			row("%s,%d,%d,%d,%d\n", kind, msg, i, node(c.peer), c.at.Sub(start).Nanoseconds())
			changes++
		}
		sort.SliceStable(sets, func(a, b int) bool { return sets[a].at.Before(sets[b].at) })
		for _, st := range sets {
			line = fmt.Appendf(line[:0], "S,%d,%d,%d,%d,%s,", msg, i, node(st.from), st.at.Sub(start).Nanoseconds(), spyKindName[st.kind])
			for k, j := range st.idx {
				if k > 0 {
					line = append(line, ';')
				}
				line = strconv.AppendInt(line, int64(j), 10)
			}
			_, _ = w.Write(append(line, '\n'))
			setRows++
		}
	}
	if err := w.Flush(); err != nil {
		return rows, changes, setRows, err
	}
	if err := zw.Close(); err != nil {
		return rows, changes, setRows, err
	}
	return rows, changes, setRows, f.Close()
}

// TestSpyTracer is the trace's known answer: rows at each growth step, claims apart, the mesh as it stood at publish.
func TestSpyTracer(t *testing.T) {
	root := strings.Repeat("r", 32)
	have := func(version byte, n int) string {
		b := make([]byte, (n+7)/8)
		for i := range n {
			b[i/8] |= 1 << (i % 8)
		}
		if version == spyHaveVersionLoad {
			b = append([]byte{7}, b...) // the load byte
		}
		return "HAVE" + string([]byte{version}) + root + string(b)
	}
	fresh := func() *spyTracer {
		s := &spyTracer{}
		s.setMessage(wireArm{msgs: make([][]byte, 128), need: 64}, map[string]int{"id0": 0, "id1": 1}, root)
		return s
	}
	idw := func(mids ...string) *pubsubpb.ControlMessage {
		return &pubsubpb.ControlMessage{Idontwant: []*pubsubpb.ControlIDontWant{{MessageIDs: mids}}}
	}
	rows := func(s *spyTracer) (out []string) {
		for _, e := range s.events {
			out = append(out, fmt.Sprintf("%s %d", spyKindName[e.kind], e.count))
		}
		return out
	}
	t.Run("a growing bitmap steps at powers of four, need and frames", func(t *testing.T) {
		s := fresh()
		for n := 1; n <= 128; n++ {
			s.announce("a", time.Now(), idw(have(spyHaveVersion, n)))
		}
		require.DeepEqual(t, []string{"idw 1", "idw 4", "idw 16", "idw 64", "idw 128"}, rows(s))
	})
	t.Run("need and frames step off the powers of four", func(t *testing.T) {
		s := &spyTracer{}
		s.setMessage(wireArm{msgs: make([][]byte, 100), need: 50}, nil, root)
		for _, n := range []int{40, 49, 50, 63, 64, 99, 100} {
			s.announce("a", time.Now(), idw(have(spyHaveVersion, n)))
		}
		require.DeepEqual(t, []string{"idw 40", "idw 50", "idw 64", "idw 100"}, rows(s))
	})
	t.Run("a claim is its own kind, and an empty first HAVE still counts", func(t *testing.T) {
		s := fresh()
		s.announce("a", time.Now(), idw(have(spyHaveVersion, 0)))
		s.announce("a", time.Now(), idw(have(spyHaveVersionClaim, 128)))
		s.announce("a", time.Now(), idw("HTS\x02 not a HAVE", "other message"))
		require.DeepEqual(t, []string{"idw 0", "claim 128"}, rows(s))
	})
	t.Run("per-id and announce channels fold per RPC and per sender", func(t *testing.T) {
		s := fresh()
		s.announce("a", time.Now(), &pubsubpb.ControlMessage{Ihave: []*pubsubpb.ControlIHave{{MessageIDs: []string{"id0", "id1", have(spyHaveVersionLoad, 3)}}}})
		s.announce("b", time.Now(), idw("id1"))
		require.DeepEqual(t, []string{"have 3", "idw 1"}, rows(s))
		require.Equal(t, 0, len(s.sets), "sets are kept only under SEGMENT_SPY_TRACE_SETS")
	})
	t.Run("with sets, each RPC's newly named indices, per kind", func(t *testing.T) {
		s := fresh()
		s.withSets = true
		s.announce("a", time.Now(), idw(have(spyHaveVersion, 2), have(spyHaveVersionClaim, 3)))
		s.announce("a", time.Now(), idw(have(spyHaveVersion, 5)))
		s.announce("a", time.Now(), idw(have(spyHaveVersion, 5), "id1"))
		s.announce("b", time.Now(), &pubsubpb.ControlMessage{Ihave: []*pubsubpb.ControlIHave{{MessageIDs: []string{"id1"}}}})
		var got []string
		for _, st := range s.sets {
			got = append(got, fmt.Sprintf("%s %s %v", string(st.from), spyKindName[st.kind], st.idx))
		}
		require.Equal(t, fmt.Sprint([]string{"a idw [0 1]", "a claim [0 1 2]", "a idw [2 3 4]", "b have [1]"}), fmt.Sprint(got))
	})
	t.Run("the mesh at publish undoes the changes after it", func(t *testing.T) {
		s := fresh()
		s.Graft("a", "t")
		s.Graft("b", "t")
		time.Sleep(time.Millisecond)
		start := time.Now()
		s.Prune("a", "t")
		s.Graft("c", "t")
		s.mu.Lock()
		mesh, after := s.meshAt(start)
		s.mu.Unlock()
		require.DeepEqual(t, []peer.ID{"a", "b"}, mesh)
		require.Equal(t, 2, len(after))
		require.DeepEqual(t, spyChurn{peer: "a", at: after[0].at, graft: false}, after[0])
		require.Equal(t, peer.ID("c"), after[1].peer)
	})
}
