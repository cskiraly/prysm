package segmentintegrationtest

// Pair cells (Q147): each slot publishes two payloads of the configured size at once, a and b,
// in one batch, a's frames first. SEGMENT_PAIR=one puts both on the segment topic (two groups on
// one mesh); SEGMENT_PAIR=topics puts b on a second topic every node joins (two meshes over one
// connection graph, one rpcQueue per peer). SEGMENT_PAIR_PRIO=1 gives a's frames the lower
// priority class at every writer's pop (the fork's WithFrameClass); SEGMENT_PAIR_PRIO=hose also
// grants each freed hose slot to the waiting writer with the lowest class (WithFrameClassGrants).
// SEGMENT_PAIR_RELEASE=<m> makes the class completion-aware per peer: a's frames lose their
// priority toward a peer the dispatch projection takes to within m frames of completing a
// (WithFrameClassRelease); it needs the priority and SEGMENT_HAVE_PROJECT.
//
// SEGMENT_PAIR=joint codes the pair as one group (q6Cell.buildJoint): a's systematic frames, then
// b's, then parity. SEGMENT_PAIR_UEP=u takes the first u parity frames as a's own (unequal
// protection: an expanding-window code, a's window inside the whole's); the rest stay over both.
// The harness counts frames rather than decoding, so the code is taken generic per window: a
// decodes from Ka of its window (its systematic frames and its own parity), or once it holds every
// systematic frame of a, or with the whole; the whole decodes once it holds K in all, Ka of a's
// window and the joint parity together, and Kb of b's. The priority class is a's window: frames
// 0 in it, 1 outside. SEGMENT_PAIR_PRIO_SCOPE=pull applies it to pull answers alone;
// SEGMENT_PAIR_PRIO_SCOPE=source installs it at the publisher alone, relays neutral.
// SEGMENT_PAIR_ORDER=window publishes a joint group window-first: a's systematic frames and a's own
// parity, then b's systematic frames and the joint parity (index order otherwise). A node's completion, as the
// driver counts it, is the later of its two groups; each group's own completion is reported in
// the pair line.

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/OffchainLabs/prysm/v7/testing/gossipsim"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
)

// pairState is a pair cell's per-variant state.
type pairState struct {
	mode       string // "one" or "topics"
	prio       bool
	grants     bool
	pullOnly   bool
	sourceOnly bool
	// windowFirst publishes a joint group's a-window (systematic and own parity) before anything else.
	windowFirst bool
	release     int
	// uep is the parity frames that are a's own under SEGMENT_PAIR=joint; ka and k are a's
	// systematic count and the group's K there.
	uep, ka, k int
	// atSys is each node's instant of holding every systematic frame of a (joint only).
	atSys map[int]time.Time
	// split is a's frame count: arm indices below it are a's; need is one group's completion.
	split, need int
	// rootB is b's group root this message, which the one-topic class reads.
	rootB atomic.Pointer[string]
	mu    sync.Mutex
	// at is each node's completion of a and of b this message, absolute; zero when not reached.
	at [2]map[int]time.Time
}

func pairTopicB() string { return segmentTopic() + "/b" }

// pairFromEnv reads SEGMENT_PAIR and SEGMENT_PAIR_PRIO; nil when the cell is not a pair.
func pairFromEnv(t *testing.T) *pairState {
	t.Helper()
	mode := os.Getenv("SEGMENT_PAIR")
	if mode == "" {
		if os.Getenv("SEGMENT_PAIR_PRIO") != "" {
			t.Fatal("SEGMENT_PAIR_PRIO needs SEGMENT_PAIR")
		}
		return nil
	}
	if mode != "one" && mode != "topics" && mode != "joint" {
		t.Fatalf("bad SEGMENT_PAIR %q (want one, topics or joint)", mode)
	}
	for _, k := range []string{"SEGMENT_WARMUP", "SEGMENT_HAVE_PREDICT_MS", "SEGMENT_HAVE_PREDICT_PEER", "SEGMENT_TAIL_HEDGE_K",
		"SEGMENT_STOP_PULL_H", "SEGMENT_FAIL_PUBLISH_NARROW", "SEGMENT_FAIL_PUBLISH_WITHHOLD", "SEGMENT_PUBLISH_MODE", "SEGMENT_GROUP_PUSH"} {
		if os.Getenv(k) != "" {
			t.Fatalf("SEGMENT_PAIR does not run with %s", k)
		}
	}
	p := &pairState{mode: mode}
	switch v := os.Getenv("SEGMENT_PAIR_PRIO"); v {
	case "":
	case "1":
		p.prio = true
	case "hose":
		p.prio, p.grants = true, true
	default:
		t.Fatalf("bad SEGMENT_PAIR_PRIO %q (want 1 or hose)", v)
	}
	if v := os.Getenv("SEGMENT_PAIR_RELEASE"); v != "" {
		m, err := strconv.Atoi(v)
		if err != nil || m < 1 || !p.prio || os.Getenv("SEGMENT_HAVE_PROJECT") == "" {
			t.Fatalf("bad SEGMENT_PAIR_RELEASE %q (want >= 1, with SEGMENT_PAIR_PRIO and SEGMENT_HAVE_PROJECT)", v)
		}
		p.release = m
	}
	if v := os.Getenv("SEGMENT_PAIR_UEP"); v != "" {
		u, err := strconv.Atoi(v)
		if err != nil || u < 1 || mode != "joint" {
			t.Fatalf("bad SEGMENT_PAIR_UEP %q (want >= 1, with SEGMENT_PAIR=joint)", v)
		}
		p.uep = u
	}
	switch v := os.Getenv("SEGMENT_PAIR_ORDER"); v {
	case "":
	case "window":
		if mode != "joint" {
			t.Fatal("SEGMENT_PAIR_ORDER=window needs SEGMENT_PAIR=joint")
		}
		p.windowFirst = true
	default:
		t.Fatalf("bad SEGMENT_PAIR_ORDER %q (want window)", v)
	}
	switch v := os.Getenv("SEGMENT_PAIR_PRIO_SCOPE"); v {
	case "":
	case "pull":
		if !p.prio {
			t.Fatal("SEGMENT_PAIR_PRIO_SCOPE needs SEGMENT_PAIR_PRIO")
		}
		p.pullOnly = true
	case "source":
		if !p.prio {
			t.Fatal("SEGMENT_PAIR_PRIO_SCOPE needs SEGMENT_PAIR_PRIO")
		}
		if os.Getenv("SEGMENT_PUBLISHER") == "rotate" {
			t.Fatal("SEGMENT_PAIR_PRIO_SCOPE=source is about node 0 as the source")
		}
		p.sourceOnly = true
	default:
		t.Fatalf("bad SEGMENT_PAIR_PRIO_SCOPE %q (want pull or source)", v)
	}
	return p
}

// pairArm is a and b as one arm: b's indices follow a's.
func pairArm(a, b wireArm) wireArm {
	u := wireArm{name: a.name + "+pair", total: a.total + b.total, max: max(a.max, b.max), need: a.completeAt() + b.completeAt()}
	u.msgs = append(append(u.msgs, a.msgs...), b.msgs...)
	u.known = make(map[digest]int, len(u.msgs))
	for d, i := range a.known {
		u.known[d] = i
	}
	for d, i := range b.known {
		u.known[d] = len(a.msgs) + i
	}
	return u
}

// set installs message m's pair as the variant's arm: a is the cell's message m, b the same
// build at m+100 (its own content and slot).
func (p *pairState) set(t *testing.T, v *q6Variant, a wireArm, m int) {
	if p.mode == "joint" {
		arm, ka := v.buildJoint(t, m)
		if p.uep > len(arm.msgs)-arm.completeAt() {
			t.Fatalf("SEGMENT_PAIR_UEP %d exceeds the group's %d parity frames", p.uep, len(arm.msgs)-arm.completeAt())
		}
		v.arm = arm
		p.ka, p.k, p.split, p.need = ka, arm.completeAt(), ka, arm.completeAt()
		p.mu.Lock()
		p.at = [2]map[int]time.Time{{}, {}}
		p.atSys = map[int]time.Time{}
		p.mu.Unlock()
		return
	}
	wb, b, _ := v.build(t, m+100)
	if v.armName == "whole" {
		b = wb
	}
	if a.completeAt() != b.completeAt() || len(a.msgs) != len(b.msgs) {
		t.Fatalf("pair halves differ: %d/%d vs %d/%d frames", a.completeAt(), len(a.msgs), b.completeAt(), len(b.msgs))
	}
	v.arm = pairArm(a, b)
	p.split, p.need = len(a.msgs), a.completeAt()
	if ids := segmentIDs(b); len(ids) > 0 {
		if root, _, ok := haveIndexOf(ids[0]); ok {
			p.rootB.Store(&root)
		}
	}
	p.mu.Lock()
	p.at = [2]map[int]time.Time{{}, {}}
	p.mu.Unlock()
}

// classOpts are the priority class, a's frames 0 and b's 1, and with grants the hose's class order.
func (p *pairState) classOpts() []pubsub.Option {
	tb := pairTopicB()
	opts := []pubsub.Option{p.classOpt(tb)}
	if p.pullOnly {
		opts = append(opts, pubsub.WithFrameClassPullOnly())
	}
	if p.grants {
		opts = append(opts, pubsub.WithFrameClassGrants())
	}
	if p.release > 0 {
		opts = append(opts, pubsub.WithFrameClassRelease(p.release, 1))
	}
	return opts
}

func (p *pairState) classOpt(tb string) pubsub.Option {
	return pubsub.WithFrameClass(func(topic, root string, index uint32) int {
		if p.mode == "joint" {
			if p.inWindowA(int(index)) { // lint:ignore uintcast -- a frame index below MaxSegments.
				return 0
			}
			return 1
		}
		if p.mode == "topics" {
			if topic == tb {
				return 1
			}
			return 0
		}
		if rb := p.rootB.Load(); rb != nil && root == *rb {
			return 1
		}
		return 0
	})
}

// inWindowA says whether joint frame i is in a's window: a systematic frame of a, or a's own parity.
func (p *pairState) inWindowA(i int) bool {
	return i < p.ka || (i >= p.k && i < p.k+p.uep)
}

// jointDone reads the counting rule: a's completion (by its systematic frames, its window, or the
// whole) and the whole's, from the frames held in each part.
func (p *pairState) jointDone(sa, sb, pa, pj int) (sys, a, whole bool) {
	kb := p.k - p.ka
	whole = sa+pa+sb+pj >= p.k && sa+pa+pj >= p.ka && sb+pj >= kb
	sys = sa == p.ka
	return sys, sys || sa+pa >= p.ka || whole, whole
}

// watchJoint is watchNode for a joint pair: one subscription, one group.
func (p *pairState) watchJoint(ctx context.Context, v *q6Variant, idx int, ps *pubsub.PubSub, sub *pubsub.Subscription) bool {
	arm := v.arm
	seen := make(map[digest]bool, len(arm.msgs))
	var sa, sb, pa, pj int
	var sysDone, aDone bool
	lastMid := ""
	for {
		msg, err := sub.Next(ctx)
		if err != nil {
			return false
		}
		d := digestOf(msg.Data)
		i, ok := arm.known[d]
		if !ok || seen[d] {
			continue
		}
		seen[d] = true
		lastMid = msg.ID
		switch {
		case i < p.ka:
			sa++
		case i < p.k:
			sb++
		case i < p.k+p.uep:
			pa++
		default:
			pj++
		}
		if v.stopPull {
			v.gate(idx).delivered(msg.ID, len(seen))
		}
		sys, a, whole := p.jointDone(sa, sb, pa, pj)
		now := time.Now()
		p.mu.Lock()
		if sys && !sysDone {
			sysDone = true
			p.atSys[idx] = now
		}
		if a && !aDone {
			aDone = true
			p.at[0][idx] = now
		}
		if whole {
			p.at[1][idx] = now
		}
		p.mu.Unlock()
		if whole {
			break
		}
	}
	if v.stopPull {
		v.gate(idx).done.Store(true)
	}
	if v.haveComplete {
		if root, _, ok := haveIndexOf(lastMid); ok {
			ps.HaveAll(sub.Topic(), root, uint32(len(arm.msgs))) // lint:ignore uintcast -- a frame count below MaxSegments.
		}
	}
	return true
}

// joinB joins and subscribes every node to the second topic.
func (p *pairState) joinB(t *testing.T, nw *simNetwork) ([]*pubsub.Topic, []*pubsub.Subscription, func()) {
	return gossipsim.JoinAndSubscribe(t, nw, pairTopicB(), false)
}

// watchNode reads one node's subscriptions until both groups complete. The stop-pull gate files
// each group as past when it completes, so the node declines its late announcements while the
// other group is still pulled; the completed group's HAVE of every index goes out at once.
func (p *pairState) watchNode(ctx context.Context, v *q6Variant, idx int, ps *pubsub.PubSub, subs []*pubsub.Subscription) bool {
	arm := v.arm
	type got struct {
		msg   *pubsub.Message
		topic string
	}
	ch := make(chan got, 64)
	rctx, cancel := context.WithCancel(ctx)
	defer cancel()
	for _, sub := range subs {
		go func(sub *pubsub.Subscription) {
			for {
				msg, err := sub.Next(rctx)
				if err != nil {
					return
				}
				select {
				case ch <- got{msg, sub.Topic()}:
				case <-rctx.Done():
					return
				}
			}
		}(sub)
	}
	seen := make(map[digest]bool, len(arm.msgs))
	var count [2]int
	var done [2]bool
	for !done[0] || !done[1] {
		var g got
		select {
		case g = <-ch:
		case <-ctx.Done():
			return false
		}
		d := digestOf(g.msg.Data)
		i, ok := arm.known[d]
		if !ok || seen[d] {
			continue
		}
		seen[d] = true
		h := 0
		if i >= p.split {
			h = 1
		}
		count[h]++
		if v.stopPull {
			v.gate(idx).delivered(g.msg.ID, len(seen))
		}
		if done[h] || count[h] < p.need {
			continue
		}
		done[h] = true
		p.mu.Lock()
		p.at[h][idx] = time.Now()
		p.mu.Unlock()
		root, _, rok := haveIndexOf(g.msg.ID)
		if v.stopPull && rok {
			v.gate(idx).completeGroup(root)
		}
		if v.haveComplete && rok {
			ps.HaveAll(g.topic, root, uint32(p.split))
		}
	}
	if v.stopPull {
		v.gate(idx).done.Store(true)
	}
	return true
}

// publish sends a and b from the publisher in one batch, a first; b on the second topic under
// SEGMENT_PAIR=topics.
func (p *pairState) publish(ctx context.Context, v *q6Variant, topicA, topicB *pubsub.Topic, ps *pubsub.PubSub) error {
	var batch pubsub.MessageBatch
	if p.windowFirst {
		var order []int
		for i := range v.arm.msgs {
			if p.inWindowA(i) {
				order = append(order, i)
			}
		}
		for i := range v.arm.msgs {
			if !p.inWindowA(i) {
				order = append(order, i)
			}
		}
		for _, i := range order {
			if err := topicA.AddToBatch(ctx, &batch, v.arm.msgs[i]); err != nil {
				return err
			}
		}
		return ps.PublishBatch(&batch)
	}
	for mi, m := range v.arm.msgs {
		topic := topicA
		if mi >= p.split && p.mode == "topics" {
			topic = topicB
		}
		if err := topic.AddToBatch(ctx, &batch, m); err != nil {
			return err
		}
	}
	return ps.PublishBatch(&batch)
}

// line is the pair's report: each group's p50/p99 over the receivers, and the gap b - a.
func (p *pairState) line(start time.Time, pub, n int) string {
	p.mu.Lock()
	defer p.mu.Unlock()
	q := func(h int) string {
		var xs []time.Duration
		for i := range n {
			if i == pub {
				continue
			}
			if at, ok := p.at[h][i]; ok {
				xs = append(xs, at.Sub(start))
			} else {
				xs = append(xs, time.Duration(1<<62))
			}
		}
		sort.Slice(xs, func(a, b int) bool { return xs[a] < xs[b] })
		pick := func(f float64) string {
			x := xs[int(f*float64(len(xs)-1))]
			if x == time.Duration(1<<62) {
				return "cens"
			}
			return fmt.Sprint(x.Milliseconds())
		}
		return fmt.Sprintf("p50 %s p90 %s p99 %s", pick(0.5), pick(0.9), pick(0.99))
	}
	sysLine := ""
	if p.mode == "joint" {
		var xs []time.Duration
		for i := range n {
			if i == pub {
				continue
			}
			if at, ok := p.atSys[i]; ok {
				xs = append(xs, at.Sub(start))
			} else {
				xs = append(xs, time.Duration(1<<62))
			}
		}
		sort.Slice(xs, func(a, b int) bool { return xs[a] < xs[b] })
		pick := func(f float64) string {
			x := xs[int(f*float64(len(xs)-1))]
			if x == time.Duration(1<<62) {
				return "cens"
			}
			return fmt.Sprint(x.Milliseconds())
		}
		sysLine = fmt.Sprintf("; a by its systematic frames p50 %s p90 %s p99 %s ms (uep %d)", pick(0.5), pick(0.9), pick(0.99), p.uep)
	}
	prio := ""
	if p.grants {
		prio = " prio+grants"
	} else if p.prio {
		prio = " prio"
	}
	if p.pullOnly {
		prio += "-pull"
	}
	if p.sourceOnly {
		prio += "-source"
	}
	if p.windowFirst {
		prio += " window-first"
	}
	if p.release > 0 {
		prio += fmt.Sprintf("+release%d", p.release)
	}
	return fmt.Sprintf("pair %s%s: a %s ms; b %s ms%s", p.mode, prio, q(0), q(1), sysLine)
}

// TestJointDone is the counting rule's known answer at Ka = Kb = 4 (K = 8).
func TestJointDone(t *testing.T) {
	p := &pairState{ka: 4, k: 8, uep: 2}
	for _, c := range []struct {
		name           string
		sa, sb, pa, pj int
		sys, a, whole  bool
	}{
		{"all of a's systematic frames", 4, 0, 0, 0, true, true, false},
		{"a's window: 2 systematic + 2 own parity", 2, 0, 2, 0, false, true, false},
		{"joint parity does not decode a alone", 3, 0, 0, 3, false, false, false},
		{"whole from joint parity", 3, 2, 0, 3, false, true, true},
		{"b short: joint parity must cover b", 4, 1, 2, 1, true, true, false},
		{"b covered by joint parity", 2, 2, 2, 2, false, true, true},
		{"8 frames but a's window over-full does not decode b", 4, 2, 2, 0, true, true, false},
	} {
		sys, a, whole := p.jointDone(c.sa, c.sb, c.pa, c.pj)
		if sys != c.sys || a != c.a || whole != c.whole {
			t.Errorf("%s: sys %v a %v whole %v, want %v %v %v", c.name, sys, a, whole, c.sys, c.a, c.whole)
		}
	}
}
