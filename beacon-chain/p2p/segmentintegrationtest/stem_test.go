package segmentintegrationtest

// Card P6, the one-hop stem (SEGMENT_SRC_STEM=msg|epoch|<node>): the source hands the payload to
// one connected peer, the stem, which publishes it as its own through the batch path the source
// would have used, with the source's knobs; the source sends nothing else for the message. It is
// Dandelion++ (POMACS 2018) with a stem of one hop and no fluff coin.
//
//	msg     each message's stem is drawn anew from the publisher's connections outside its mesh
//	epoch   one stem per publisher for the whole cell, Dandelion++'s per-epoch relay: repeated
//	        messages name the same stem, so intersecting them narrows nothing; replaced if it
//	        joins the publisher's mesh
//	<node>  that node is every message's stem: the known answer is a cell published from it
//	        (SEGMENT_PUBLISHER=<node>), one hand-off later
//
// The hand-off rides a harness stream protocol, and its id is how the stem knows it is one; a
// production form needs a protocol of its own or a flag on the RPC. It carries the first K frames,
// enough to decode; the stem publishes every frame, which re-encoding the decoded payload would
// reproduce byte for byte. The stem stands in for the source in every role: no watch goroutine,
// its completion taken when it publishes. The source's router never learns the message, so the
// frames come back to it as to any relay and it acts as one, forwarding, announcing and claiming at
// decode, but is not waited for; the push-back it costs is the publisher rx line.
//
// SEGMENT_SRC_STEM_PIPE=decode|all pipelines the hand-off: the stem publishes each frame as it
// arrives instead of waiting for K. decode streams the K data frames and publishes the parity as
// one batch once the stem can decode; all streams every frame, the parity included, and publishes
// each on arrival. Either way the stem completes at its K-th frame. A frame published alone is
// announced while the stem is its only holder, so its mesh pulls it from the stem; the source's
// deferred announce (SEGMENT_PHASE_SRC=delay) applies to the stem's publishes and prevents that.
//
// SEGMENT_SRC_STEM_STREAM=keep holds each (publisher, stem) stream open from one message to the
// next, as a per-epoch stem's channel would be, so a hand-off after the first meets a flow-control
// window already grown instead of a fresh stream's 512 KiB.
//
// SEGMENT_SRC_STEM_SELECT=uniform|capable draws the stem under a policy (stemselect_test.go).
//
// SEGMENT_SRC_STEM_WITHHOLD=1 makes the stem an adversary that takes the hand-off and publishes
// nothing. It still completes at its K-th frame, as it holds the payload; its router relays
// whatever reaches it by gossip, as any node's.
//
// SEGMENT_SRC_STEM_FAILSAFE_MS=<T> is the source's fail-safe: if, T after the hand-off starts, the
// source has had fewer than SEGMENT_SRC_STEM_FAILSAFE_FRAMES distinct frames delivered by gossip
// (default K, the payload), it publishes the message itself through the batch path, as the
// private-A source would; its router skips the frames it already holds.
//
// SEGMENT_SRC_STEM_HOPS=2 makes the stem two hops: the first stem publishes nothing and writes each
// hand-off frame on to a second stem as it arrives, over a stream kept like the first under
// SEGMENT_SRC_STEM_STREAM=keep; the second publishes. The second is drawn by the same rule from
// the first's connections, the publisher and the publisher's mesh left out, or fixed by
// SEGMENT_SRC_STEM_HOP2=<node>. Both stems complete at their K-th frame, and the spy trace records
// both hand-offs as stem rows, each from its sender. SEGMENT_SRC_STEM_WITHHOLD=2 makes the second
// stem the withholder; =1 makes the first forward nothing.

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"slices"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	simlibp2p "github.com/libp2p/go-libp2p/x/simlibp2p"
)

// stemProtocol is deliberately not an Ethereum protocol id: only the harness speaks it.
const stemProtocol = protocol.ID("/prysm/harness/stem/1.0.0")

// stemMaxFrame bounds one hand-off frame, far above any arm's.
const stemMaxFrame = 16 << 20

type stemMode uint8

const (
	stemPerMessage stemMode = iota + 1
	stemPerEpoch
	stemFixed
)

var stemModeName = map[stemMode]string{stemPerMessage: "per message", stemPerEpoch: "per epoch", stemFixed: "fixed"}

// stemPipe is SEGMENT_SRC_STEM_PIPE: off, or which frames the stream carries while the stem
// publishes each on arrival.
type stemPipe uint8

const (
	stemPipeOff stemPipe = iota
	stemPipeDecode
	stemPipeAll
)

var stemPipeName = map[stemPipe]string{stemPipeOff: "", stemPipeDecode: ", pipelined, parity at decode", stemPipeAll: ", pipelined, every frame streamed"}

// stemState is a cell's stem rule and the message in flight; its mutex guards the latter.
type stemState struct {
	mode stemMode
	pipe stemPipe
	// keep holds each (publisher, stem) stream open across messages (SEGMENT_SRC_STEM_STREAM=keep);
	// kept is those streams and their writers, touched by the driver goroutine only.
	keep bool
	// redrawn marks a message whose first draw fell in the publisher's mesh.
	redrawn bool
	fixed   int
	seed    uint64
	epoch   map[int]int // publisher -> its per-epoch stem
	kept    map[[2]int]*stemStream
	// sel is the stem policy (SEGMENT_SRC_STEM_SELECT), nil for the draw outside the mesh alone;
	// uplinks and geo are what it reads of the cell.
	sel     *stemPolicy
	uplinks []int
	geo     cellGeo
	// withhold is the hop whose stem takes the hand-off and publishes nothing
	// (SEGMENT_SRC_STEM_WITHHOLD), 0 for none.
	withhold int
	// failsafe is the source's timer (SEGMENT_SRC_STEM_FAILSAFE_MS), 0 for none; failsafeFrames the
	// frames it must have seen by then (SEGMENT_SRC_STEM_FAILSAFE_FRAMES), 0 for K.
	failsafe       time.Duration
	failsafeFrames int
	// fs is the message's fail-safe, nil before its hand-off or without one.
	fs *stemFailsafe
	// hops is the stem's length (SEGMENT_SRC_STEM_HOPS), 1 or 2; hop2 the fixed second stem
	// (SEGMENT_SRC_STEM_HOP2), -1 to draw it; epoch2 each first stem's per-epoch second stem.
	hops   int
	hop2   int
	epoch2 map[int]int

	mu  sync.Mutex
	msg int
	pub int
	// hop is the message's hand-offs, the publisher's first; need the frames a hand-off stream
	// carries, of which K complete the stem.
	hop     []*stemHop
	need    int
	arm     wireArm
	publish func(idx []int) error
	signal  func(int, time.Duration)
	start   time.Time
	err     error
}

// stemHop is one hand-off of the message in flight, from its sender to node, the stem; the
// stemState's mutex guards it.
type stemHop struct {
	// n is the hop's number, the publisher's hand-off 1.
	n    int
	node int
	from peer.ID
	spy  *spyTracer
	// opened and written are the sender's stream open and last write; first and last the
	// hand-off frames' arrivals at the stem; published and lastPub the stem's first and last
	// publish; decoded its K-th frame's arrival.
	opened, written, first, last time.Time
	published, lastPub, decoded  time.Time
	frames, bytes, sent          int
	draw                         stemDraw
	// fwd is a first stem's stream to the second, which it writes each frame to as it arrives.
	fwd *stemStream
}

// stemFromEnv reads SEGMENT_SRC_STEM and its companions for an n-node cell whose graph seed is
// seed, uplinks and latency model as given; nil when off.
func stemFromEnv(t *testing.T, n int, seed uint64, uplinks []int, geo cellGeo) *stemState {
	t.Helper()
	var s *stemState
	sel := stemPolicyFromEnv(t)
	switch v := os.Getenv("SEGMENT_SRC_STEM"); v {
	case "":
		for _, k := range []string{"SEGMENT_SRC_STEM_PIPE", "SEGMENT_SRC_STEM_STREAM", "SEGMENT_SRC_STEM_WITHHOLD", "SEGMENT_SRC_STEM_FAILSAFE_MS", "SEGMENT_SRC_STEM_FAILSAFE_FRAMES", "SEGMENT_SRC_STEM_HOPS", "SEGMENT_SRC_STEM_HOP2"} {
			if os.Getenv(k) != "" {
				t.Fatalf("%s needs SEGMENT_SRC_STEM", k)
			}
		}
		if sel != nil {
			t.Fatal("SEGMENT_SRC_STEM_SELECT needs SEGMENT_SRC_STEM")
		}
		return nil
	case "msg":
		s = &stemState{mode: stemPerMessage, seed: seed}
	case "epoch":
		s = &stemState{mode: stemPerEpoch, seed: seed, epoch: map[int]int{}}
	default:
		k, err := strconv.Atoi(v)
		if err != nil || k < 0 || k >= n {
			t.Fatalf("bad SEGMENT_SRC_STEM %q (want msg, epoch or a node index below %d)", v, n)
		}
		s = &stemState{mode: stemFixed, fixed: k, seed: seed}
		if sel != nil {
			t.Fatal("SEGMENT_SRC_STEM_SELECT draws the stem: it needs SEGMENT_SRC_STEM=msg or epoch")
		}
	}
	s.sel, s.uplinks, s.geo = sel, uplinks, geo
	switch v := os.Getenv("SEGMENT_SRC_STEM_PIPE"); v {
	case "":
	case "decode":
		s.pipe = stemPipeDecode
	case "all":
		s.pipe = stemPipeAll
	default:
		t.Fatalf("bad SEGMENT_SRC_STEM_PIPE %q (want decode or all)", v)
	}
	switch v := os.Getenv("SEGMENT_SRC_STEM_STREAM"); v {
	case "":
	case "keep":
		s.keep, s.kept = true, map[[2]int]*stemStream{}
	default:
		t.Fatalf("bad SEGMENT_SRC_STEM_STREAM %q (want keep)", v)
	}
	s.hops, s.hop2 = 1, -1
	switch v := os.Getenv("SEGMENT_SRC_STEM_HOPS"); v {
	case "", "1":
	case "2":
		s.hops = 2
		if s.mode != stemPerMessage {
			s.epoch2 = map[int]int{}
		}
	default:
		t.Fatalf("bad SEGMENT_SRC_STEM_HOPS %q (want 1 or 2)", v)
	}
	if v := os.Getenv("SEGMENT_SRC_STEM_HOP2"); v != "" {
		k, err := strconv.Atoi(v)
		if err != nil || k < 0 || k >= n || s.hops != 2 {
			t.Fatalf("bad SEGMENT_SRC_STEM_HOP2 %q (want a node index below %d, with SEGMENT_SRC_STEM_HOPS=2)", v, n)
		}
		s.hop2 = k
	}
	switch v := os.Getenv("SEGMENT_SRC_STEM_WITHHOLD"); v {
	case "":
	case "1", "2":
		s.withhold, _ = strconv.Atoi(v)
		if s.withhold > s.hops {
			t.Fatalf("SEGMENT_SRC_STEM_WITHHOLD=%s: the stem has %d hop(s)", v, s.hops)
		}
	default:
		t.Fatalf("bad SEGMENT_SRC_STEM_WITHHOLD %q (want the withholding hop, 1 or 2)", v)
	}
	if v := os.Getenv("SEGMENT_SRC_STEM_FAILSAFE_MS"); v != "" {
		k, err := strconv.Atoi(v)
		if err != nil || k <= 0 {
			t.Fatalf("bad SEGMENT_SRC_STEM_FAILSAFE_MS %q (want a positive number of ms)", v)
		}
		s.failsafe = time.Duration(k) * time.Millisecond
	}
	if v := os.Getenv("SEGMENT_SRC_STEM_FAILSAFE_FRAMES"); v != "" {
		k, err := strconv.Atoi(v)
		if err != nil || k <= 0 || s.failsafe == 0 {
			t.Fatalf("bad SEGMENT_SRC_STEM_FAILSAFE_FRAMES %q (want a positive count, with SEGMENT_SRC_STEM_FAILSAFE_MS)", v)
		}
		s.failsafeFrames = k
	}
	return s
}

// stemFailsafe is one message's fail-safe: armed at the hand-off, it fires if the source has
// seen fewer than want frames by gossip when its timer runs out.
type stemFailsafe struct {
	done  chan struct{}
	at    time.Time
	want  int
	seen  int
	fired bool
	err   error
}

// armFailsafe starts the message's fail-safe: after s.failsafe, seen counts the frames the source
// has had by gossip, and publish, if they are fewer than the threshold, sends the message from
// the source as the private-A source would; the frames it already holds its router skips.
func (s *stemState) armFailsafe(ctx context.Context, k int, seen func() int, publish func() error) {
	want := k
	if s.failsafeFrames > 0 {
		want = s.failsafeFrames
	}
	f := &stemFailsafe{done: make(chan struct{}), at: time.Now(), want: want}
	s.mu.Lock()
	s.fs = f
	s.mu.Unlock()
	go func() {
		defer close(f.done)
		select {
		case <-time.After(s.failsafe):
		case <-ctx.Done():
			return
		}
		n := seen()
		var err error
		if n < want {
			err = publish()
		}
		s.mu.Lock()
		f.seen, f.fired, f.err = n, n < want, err
		s.mu.Unlock()
	}()
}

// failsafeLine waits for the message's fail-safe and reports it; empty without one.
func (s *stemState) failsafeLine(start time.Time) (string, error) {
	s.mu.Lock()
	f := s.fs
	s.mu.Unlock()
	if f == nil {
		return "", nil
	}
	<-f.done
	s.mu.Lock()
	defer s.mu.Unlock()
	armed := float64(f.at.Sub(start).Microseconds()) / 1000
	if !f.fired {
		return fmt.Sprintf("stem fail-safe (armed at the hand-off +%.1fms, timer %v, want %d frames by gossip): not fired, %d frames seen", armed, s.failsafe, f.want, f.seen), f.err
	}
	return fmt.Sprintf("stem fail-safe (armed at the hand-off +%.1fms, timer %v, want %d frames by gossip): fired at +%.1fms, %d frames seen; the source published the message itself", armed, s.failsafe, f.want, armed+float64(s.failsafe.Microseconds())/1000, f.seen), f.err
}

// gossipFrames counts the distinct frames of arm node self's tracer has had delivered by a peer.
func gossipFrames(tr *recordingTracer, arm wireArm, self peer.ID) int {
	tr.Mu.Lock()
	defer tr.Mu.Unlock()
	got := map[digest]bool{}
	for _, e := range tr.Recvs {
		if _, ok := arm.known[e.What]; ok && e.From != self {
			got[e.What] = true
		}
	}
	return len(got)
}

// stemStream is a kept hand-off stream and its writer.
type stemStream struct {
	str network.Stream
	w   *bufio.Writer
}

// next picks message m's stem among publisher pub's connections outside its mesh, and starts the
// message's record. A stem in the mesh would push the frames straight back to the publisher, whom
// sel=fewest ranks first for holding nothing, and make it the second hop; so a draw that lands in
// the mesh is drawn again among the rest, and a per-epoch stem that joins the mesh is replaced.
// A fixed stem is kept wherever it is. Under SEGMENT_SRC_STEM_HOPS=2 the stem's own stem is drawn
// the same way among its connections, the publisher and the publisher's mesh left out. meshOf is
// a node's mesh.
func (s *stemState) next(t *testing.T, nw *simNetwork, m, pub int, meshOf func(int) []peer.ID) {
	t.Helper()
	idx := make(map[peer.ID]int, len(nw.Hosts))
	for i, h := range nw.Hosts {
		idx[h.ID()] = i
	}
	meshSet := func(i int) map[int]bool {
		in := map[int]bool{}
		for _, p := range meshOf(i) {
			in[idx[p]] = true
		}
		return in
	}
	conns := func(i int, skip map[int]bool) []int {
		var out []int
		for _, p := range nw.Hosts[i].Network().Peers() {
			if j, ok := idx[p]; ok && j != i && !skip[j] {
				out = append(out, j)
			}
		}
		sort.Ints(out)
		return out
	}
	pubMesh := meshSet(pub)
	// A hash of the graph seed and the sender, and the message unless per epoch.
	key := s.seed ^ uint64(pub+1)<<24 ^ 0x5354454d
	if s.mode == stemPerMessage {
		key ^= uint64(m) << 44
	}
	fixed := -1
	if s.mode == stemFixed {
		fixed = s.fixed
	}
	stem, d, redrawn := s.draw(t, nw, pub, conns(pub, nil), pubMesh, fixed, s.epoch, key)
	hops := []*stemHop{{n: 1, node: stem, from: nw.Hosts[pub].ID(), draw: d}}
	if s.hops == 2 {
		skip := map[int]bool{pub: true}
		for i := range pubMesh {
			skip[i] = true
		}
		key2 := s.seed ^ uint64(stem+1)<<24 ^ 0x5354454d32
		if s.mode == stemPerMessage {
			key2 ^= uint64(m) << 44
		}
		stem2, d2, _ := s.draw(t, nw, stem, conns(stem, skip), meshSet(stem), s.hop2, s.epoch2, key2)
		d2.levels[0].name = "connections off the publisher and its mesh"
		hops = append(hops, &stemHop{n: 2, node: stem2, from: nw.Hosts[stem].ID(), draw: d2})
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.msg, s.pub, s.redrawn = m, pub, redrawn
	s.hop = hops
	s.publish, s.signal, s.err, s.fs = nil, nil, nil, nil
}

// draw picks sender from's stem among base, its candidates, where inMesh is from's mesh: fixed
// when not negative, else the per-epoch stem in epoch (nil per message) while it stays eligible,
// else drawn from key. redrawn marks a first draw in the mesh when no policy is set.
func (s *stemState) draw(t *testing.T, nw *simNetwork, from int, base []int, inMesh map[int]bool, fixed int, epoch map[int]int, key uint64) (int, stemDraw, bool) {
	t.Helper()
	lv, dropped := stemLevels(nw, s.sel, s.uplinks, s.geo, from, base, inMesh)
	if fixed < 0 && len(lv[len(lv)-1].nodes) == 0 {
		t.Fatalf("SEGMENT_SRC_STEM: node %d has no candidate for its stem outside its mesh", from)
	}
	d := stemDraw{levels: lv, used: lastLevel(lv), dropped: dropped}
	stem, redrawn := -1, false
	switch {
	case fixed >= 0:
		if !slices.Contains(nw.Hosts[from].Network().Peers(), nw.Hosts[fixed].ID()) {
			t.Fatalf("SEGMENT_SRC_STEM: the fixed stem %d is not connected to node %d", fixed, from)
		}
		stem, d.fixed = fixed, true
	case epoch != nil:
		if e, ok := epoch[from]; ok && d.used >= 0 && slices.Contains(lv[d.used].nodes, e) {
			stem, d.kept = e, true
		}
	}
	if stem < 0 {
		stem, d.at = stemChain(key, lv)
		redrawn = s.sel == nil && d.at > 0
		if epoch != nil {
			epoch[from] = stem
		}
	}
	if stem < len(s.uplinks) {
		d.up = s.uplinks[stem]
	}
	d.oneWay = quicMinRTT(nodeHost(nw, from), nw.Hosts[stem].ID()) / 2
	return stem, d, redrawn
}

// stemNode is the message's stem.
func (s *stemState) stemNode() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.hop[0].node
}

// hopAt is the message's hand-off to node j, nil when j is not a stem of it.
func (s *stemState) hopAt(j int) *stemHop {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, h := range s.hop {
		if h.node == j {
			return h
		}
	}
	return nil
}

// armSignal hands the stem the driver's completion signal and the message's start: the stem
// completes when it publishes.
func (s *stemState) armSignal(signal func(int, time.Duration), start time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.signal, s.start = signal, start
}

// register installs the stem handler on every host of the cell.
func (s *stemState) register(nw *simNetwork) {
	for j, h := range nw.Hosts {
		h.SetStreamHandler(stemProtocol, func(str network.Stream) { s.serve(j, str) })
	}
}

// msLazyHeader is multistream-select's header and proto as a lazy client writes them ahead of its
// data: each token uvarint-length-prefixed and newline-terminated.
func msLazyHeader(proto protocol.ID) []byte {
	var b []byte
	for _, tok := range []string{"/multistream/1.0.0", string(proto)} {
		b = binary.AppendUvarint(b, uint64(len(tok)+1))
		b = append(b, tok...)
		b = append(b, '\n')
	}
	return b
}

// handOff writes the message's first K frames (every frame under SEGMENT_SRC_STEM_PIPE=all) from the
// publisher to the stem, whose last hop publishes frames through publish; spyOf is a node's spy
// log. It returns once the frames are written.
func (s *stemState) handOff(ctx context.Context, nw *simNetwork, arm wireArm, spyOf func(int) *spyTracer, publish func(idx []int) error) error {
	s.mu.Lock()
	h := s.hop[0]
	pub, stem := s.pub, h.node
	s.need, s.arm, s.publish = arm.completeAt(), arm, publish
	if s.pipe == stemPipeAll {
		s.need = len(arm.msgs)
	}
	for _, hh := range s.hop {
		hh.spy = spyOf(hh.node)
	}
	need := s.need
	hops := slices.Clone(s.hop)
	s.mu.Unlock()
	if len(hops) == 2 && s.withhold != 1 {
		// The first stem's channel to the second, opened ahead so its handler only writes.
		fwd, err := s.openStream(ctx, nw, stem, hops[1].node)
		if err != nil {
			return err
		}
		s.mu.Lock()
		h.fwd = fwd
		s.mu.Unlock()
	}
	ks, err := s.openStream(ctx, nw, pub, stem)
	if err != nil {
		return err
	}
	if !s.keep {
		defer func() { _ = ks.str.Close() }()
	}
	s.mu.Lock()
	h.opened = time.Now()
	s.mu.Unlock()
	w := ks.w
	var hdr [binary.MaxVarintLen64]byte
	sent := 0
	if s.keep {
		// A kept stream carries one message after another, each behind its frame count.
		k := binary.PutUvarint(hdr[:], uint64(need))
		if _, err := w.Write(hdr[:k]); err != nil {
			return fmt.Errorf("stem hand-off: %w", err)
		}
		sent += k
	}
	for _, d := range arm.msgs[:need] {
		k := binary.PutUvarint(hdr[:], uint64(len(d)))
		if _, err := w.Write(hdr[:k]); err != nil {
			return fmt.Errorf("stem hand-off: %w", err)
		}
		if _, err := w.Write(d); err != nil {
			return fmt.Errorf("stem hand-off: %w", err)
		}
		sent += k + len(d)
	}
	if err := w.Flush(); err != nil {
		return fmt.Errorf("stem hand-off: %w", err)
	}
	s.mu.Lock()
	h.sent, h.written = sent, time.Now()
	s.mu.Unlock()
	return nil
}

// openStream is the hand-off stream from node from to node to: the kept one under
// SEGMENT_SRC_STEM_STREAM=keep, else a new one; driver goroutine only.
func (s *stemState) openStream(ctx context.Context, nw *simNetwork, from, to int) (*stemStream, error) {
	if ks := s.kept[[2]int{from, to}]; ks != nil {
		return ks, nil
	}
	// The harness's blank hosts negotiate every stream with a round trip; a production host that
	// knows the protocol from identify sends multistream-select's header ahead of the data, so
	// the hand-off does that by hand.
	str, err := nw.Hosts[from].Network().NewStream(ctx, nw.Hosts[to].ID())
	if err != nil {
		return nil, fmt.Errorf("stem hand-off from node %d to node %d: %w", from, to, err)
	}
	if err := str.SetProtocol(stemProtocol); err != nil {
		_ = str.Reset()
		return nil, fmt.Errorf("stem hand-off: %w", err)
	}
	ks := &stemStream{str: str, w: bufio.NewWriterSize(str, 64<<10)}
	if _, err := ks.w.Write(msLazyHeader(stemProtocol)); err != nil {
		_ = str.Reset()
		return nil, fmt.Errorf("stem hand-off: %w", err)
	}
	if s.keep {
		s.kept[[2]int{from, to}] = ks
	}
	return ks, nil
}

// publisherNode is the message's publishing stem, its last hop.
func (s *stemState) publisherNode() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.hop[len(s.hop)-1].node
}

// handOffNodes are the stems a hand-off reaches: every hop, or up to a withholder that forwards
// nothing.
func (s *stemState) handOffNodes() []int {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []int
	for _, h := range s.hop {
		out = append(out, h.node)
		if s.withhold == h.n {
			break
		}
	}
	return out
}

// serve is node j's end of a hand-off stream: one message, or under SEGMENT_SRC_STEM_STREAM=keep
// one message after another, each behind its frame count, until the stream closes.
func (s *stemState) serve(j int, str network.Stream) {
	defer func() { _ = str.Close() }()
	br := bufio.NewReader(str)
	for {
		var count uint64
		if s.keep {
			c, err := binary.ReadUvarint(br)
			if err != nil {
				return // the stream closed with the cell
			}
			count = c
		}
		if !s.receive(j, str, br, count) || !s.keep {
			return
		}
	}
}

// receive reads one message's hand-off frames and publishes the message as its own, at the K-th
// frame or, pipelined, frame by frame; count, when not zero, is the frames the source announced.
// A first stem of two publishes nothing and writes each frame on to the second as it arrives.
func (s *stemState) receive(j int, str network.Stream, br *bufio.Reader, count uint64) bool {
	h := s.hopAt(j)
	if h == nil {
		_ = str.Reset()
		return false
	}
	s.mu.Lock()
	need, arm, from, spy, publish, pipe := s.need, s.arm, h.from, h.spy, s.publish, s.pipe
	var next *stemHop
	if h.n < len(s.hop) {
		next = s.hop[h.n]
	}
	fwd := h.fwd
	s.mu.Unlock()
	k := arm.completeAt()
	var hdr [binary.MaxVarintLen64]byte
	started := false
	forward := func(buf []byte) bool {
		if fwd == nil || s.withhold == h.n {
			return true
		}
		now := time.Now()
		sent := 0
		if !started && s.keep {
			sent += binary.PutUvarint(hdr[:], uint64(need))
			if _, err := fwd.w.Write(hdr[:sent]); err != nil {
				s.fail(fmt.Errorf("stem node %d: forward: %w", j, err))
				return false
			}
		}
		m := binary.PutUvarint(hdr[:], uint64(len(buf)))
		_, err := fwd.w.Write(hdr[:m])
		if err == nil {
			_, err = fwd.w.Write(buf)
		}
		if err == nil {
			err = fwd.w.Flush()
		}
		if err != nil {
			s.fail(fmt.Errorf("stem node %d: forward: %w", j, err))
			return false
		}
		s.mu.Lock()
		if !started {
			next.opened, started = now, true
		}
		next.written = time.Now()
		next.sent += sent + m + len(buf)
		s.mu.Unlock()
		return true
	}
	if fwd != nil && !s.keep {
		defer func() { _ = fwd.str.Close() }()
	}
	pub := func(idx []int) bool {
		if s.withhold == h.n || next != nil {
			return true // it holds the frames and publishes none
		}
		now := time.Now()
		s.mu.Lock()
		if h.published.IsZero() {
			h.published = now
		}
		h.lastPub = now
		s.mu.Unlock()
		if err := publish(idx); err != nil {
			s.fail(fmt.Errorf("stem node %d: publish: %w", j, err))
			return false
		}
		return true
	}
	if publish == nil || str.Conn().RemotePeer() != from {
		_ = str.Reset()
		return false
	}
	if count != 0 && count != uint64(need) {
		s.fail(fmt.Errorf("stem node %d: the hand-off announces %d frames, want %d", j, count, need))
		return false
	}
	got := make(map[int]bool, need)
	for len(got) < need {
		n, err := binary.ReadUvarint(br)
		if err != nil || n == 0 || n > stemMaxFrame {
			s.fail(fmt.Errorf("stem node %d: bad hand-off frame header after %d frames (length %d): %v", j, len(got), n, err))
			return false
		}
		buf := make([]byte, n)
		if _, err := io.ReadFull(br, buf); err != nil {
			s.fail(fmt.Errorf("stem node %d: short hand-off frame after %d frames: %w", j, len(got), err))
			return false
		}
		now := time.Now()
		i, ok := arm.known[digestOf(buf)]
		if !ok {
			s.fail(fmt.Errorf("stem node %d: hand-off frame %d is not the message's", j, len(got)))
			return false
		}
		got[i] = true
		if spy != nil {
			spy.stemFrame(from, i, now)
		}
		s.mu.Lock()
		if h.first.IsZero() {
			h.first = now
		}
		h.last = now
		h.frames++
		h.bytes += len(buf)
		s.mu.Unlock()
		if !forward(buf) {
			return false
		}
		if pipe != stemPipeOff && !pub([]int{i}) {
			return false
		}
		if len(got) != k {
			continue
		}
		// The K-th frame: the stem can decode, so it holds the message and every frame of it.
		s.mu.Lock()
		h.decoded = now
		signal, at := s.signal, now.Sub(s.start)
		s.mu.Unlock()
		var rest []int
		if pipe != stemPipeAll {
			for mi := range arm.msgs {
				if !got[mi] || pipe == stemPipeOff {
					rest = append(rest, mi)
				}
			}
		}
		if len(rest) > 0 && !pub(rest) {
			return false
		}
		if signal != nil {
			signal(j, at)
		}
	}
	return true
}

// fail records the message's first hand-off error.
func (s *stemState) fail(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err == nil {
		s.err = err
	}
}

// lines reports the message's stem: who, the hand-off's timing against its floor, the stem's first
// hop and its upload; the error, if the hand-off failed.
func (s *stemState) lines(v *q6Variant, st diffusionStats) ([]string, error) {
	s.mu.Lock()
	hops := make([]stemHop, len(s.hop))
	for i, h := range s.hop {
		hops[i] = *h
	}
	need, redrawn, err := s.need, s.redrawn, s.err
	s.mu.Unlock()
	ms := func(t time.Time) string {
		if t.IsZero() {
			return "never"
		}
		return fmt.Sprintf("+%.1fms", float64(t.Sub(st.startAt).Microseconds())/1000)
	}
	var out []string
	for _, h := range hops {
		// The first hop keeps the one-hop stem's line; a second is the first stem's stem.
		label, sender, from := "stem", "publisher", st.pub
		if h.n > 1 {
			label, sender, from = fmt.Sprintf("stem%d", h.n), "stem", hops[h.n-2].node
		}
		stemID := v.nw.Hosts[h.node].ID()
		inMesh := "no"
		if st.tracers != nil && slices.Contains(st.tracers[from].MeshPeerIDs(), stemID) {
			inMesh = "yes"
		}
		if redrawn && h.n == 1 {
			inMesh += " (first draw in the mesh, drawn again outside it)"
		}
		mode := stemModeName[s.mode] + stemPipeName[s.pipe] + map[bool]string{true: ", stream kept"}[s.keep] + map[bool]string{true: ", withholding: publishes nothing"}[s.withhold == h.n]
		if len(hops) > 1 {
			mode += fmt.Sprintf(", hop %d of %d", h.n, len(hops))
			if h.n < len(hops) && s.withhold != h.n {
				mode += fmt.Sprintf(", forwards each frame to node %d", hops[h.n].node)
			}
		}
		oneWay := quicMinRTT(nodeHost(v.nw, from), stemID) / 2
		floor := time.Duration(float64(h.sent*8) / float64(v.rate) * float64(time.Second))
		out = append(out, fmt.Sprintf("%s (%s): %s %d -> node %d, in its mesh at report %s; hand-off %d of %d frames, %d B written (%d B of frames; not in the publisher's upload line); stream open %s, last write %s; first frame at the stem %s, last %s, the stem's publish %s, last publish %s, decodable %s; one-way %v (QUIC min RTT / 2), serialisation of the hand-off at %d Mbps %v",
			label, mode, sender, from, h.node, inMesh, h.frames, need, h.sent, h.bytes, ms(h.opened), ms(h.written), ms(h.first), ms(h.last),
			ms(h.published), ms(h.lastPub), ms(h.decoded), oneWay.Round(100*time.Microsecond), v.rate/simlibp2p.OneMbps, floor.Round(100*time.Microsecond)))
		if s.sel != nil || len(hops) > 1 {
			policy := "outside the mesh"
			if s.sel != nil {
				policy = s.sel.name
			}
			out = append(out, h.draw.line(label, policy, from, h.node, s.geo))
		}
		if st.tracers != nil {
			out = append(out, label+" "+firstHopLine(v.arm, st, stemID, label))
		}
		out = append(out, uploadLine(v.nw, label, []int{h.node}))
	}
	fs, fsErr := s.failsafeLine(st.startAt)
	if fs != "" {
		out = append(out, fs)
	}
	if err == nil && fsErr != nil {
		err = fmt.Errorf("stem fail-safe: the source's publish: %w", fsErr)
	}
	return out, err
}
