package segmentintegrationtest

// The observation horizon: every node read at one instant, the deadline after publish, on the
// happy path and the censored one alike. Before it, a cell's byte counters were read when its
// last node completed (so what arrived afterwards was never counted) or not at all (the timeout
// path returned before aggregating), and no cell had ever reported how long its requests took
// to be answered, how many were still open, or how much of its writers' time the transport held.
// The snapshot is passive: it reads the tracers, the routers' request ledgers and their outbound
// queues, and decides nothing (hedge-and-adaptivity plan, E0).

import (
	"fmt"
	"sort"
	"testing"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
)

type horizonSnapshot struct {
	at     time.Duration // the horizon, relative to publish
	takeAt time.Time     // the instant it was taken
	taken  bool
	// per node, at the horizon
	rx, ctrl, dups []int
	// dupStats reads each node's duplicate arrivals against its own first deliveries; segments
	// is K, the distinct frames anyone was delivered by the horizon.
	dupStats []nodeDups
	segments int
	ledgers  []*pubsub.RequestLedger
	outbound []map[peer.ID]pubsub.OutboundStat
	// CoDel's own drops on each node's access link, uplink and downlink summed, with the
	// enqueued count as the denominator. A drop rate with no denominator cannot be acted on,
	// and a cell that never congests must be distinguishable from one whose CoDel was
	// configured never to drop (SIMNET_CODEL_TARGET_MS does exactly that).
	linkDropped, linkEnqueued []int
	// the populations reported separately, in report order; attacker is the injected set, so
	// take can class the honest nodes by how many attackers sit in their mesh
	popNames []string
	pops     map[string][]int
	attacker map[int]bool
	// pub is the message's publisher. base, in a multi-message cell's later messages, is the
	// routers' cumulative counters at the message's publish, so the snapshot reads this message
	// alone: request records from base.at on, pushes and outbound counters as differences.
	pub  int
	base *horizonBase
	// pubID and pubMesh identify the publisher and its mesh at the horizon, for sourcePullsLine.
	pubID   peer.ID
	pubMesh map[peer.ID]bool
	ids     []peer.ID
	// The cross-check with the specification's toy (xcheck_test.go): per node its reading, the
	// push degree each node's frames are checked against, and the pull discipline's window.
	xc        []xcNode
	degree    func(int) int
	askWindow time.Duration
	// revoked, asksHeld and staleAsks are pubsub.HaveRevoked, AsksForHeldDropped and
	// StaleAsksDropped at the horizon, zeroed at the publish: network-wide.
	revoked, asksHeld, staleAsks int64
	// startAt is the publish instant, for the source's timeline; askFirst and askRetry are the asks
	// to the publisher that were their asker's first ask for the id, and later ones.
	startAt            time.Time
	askFirst, askRetry int
}

// horizonBase is what the routers had counted before a message was published.
type horizonBase struct {
	at                        time.Time
	unasked                   []int
	outbound                  []map[peer.ID]pubsub.OutboundStat
	linkDropped, linkEnqueued []int
}

// takeHorizonBase reads the routers' cumulative request and outbound counters at publish.
func takeHorizonBase(nw *simNetwork) *horizonBase {
	b := &horizonBase{at: time.Now(), unasked: make([]int, len(nw.Pubsubs)), outbound: make([]map[peer.ID]pubsub.OutboundStat, len(nw.Pubsubs))}
	for i, ps := range nw.Pubsubs {
		if l := ps.RequestLedger(); l != nil {
			b.unasked[i] = l.Unasked
		}
		b.outbound[i] = ps.OutboundStats()
	}
	b.linkDropped, b.linkEnqueued = make([]int, len(nw.Pubsubs)), make([]int, len(nw.Pubsubs))
	if nw.Sim != nil {
		for i, ls := range nw.Sim.LinkStats() {
			if i >= len(b.linkDropped) {
				break
			}
			b.linkDropped[i] = int(ls.Uplink.PacketsDropped + ls.Downlink.PacketsDropped)    // lint:ignore uintcast -- packet counts.
			b.linkEnqueued[i] = int(ls.Uplink.PacketsEnqueued + ls.Downlink.PacketsEnqueued) // lint:ignore uintcast -- packet counts.
		}
	}
	return b
}

// since is node i's ledger and outbound counters with the base taken out. Peaks and the longest
// write stay cumulative over the cell: a difference of maxima means nothing.
func (b *horizonBase) since(i int, l *pubsub.RequestLedger, out map[peer.ID]pubsub.OutboundStat) (*pubsub.RequestLedger, map[peer.ID]pubsub.OutboundStat) {
	if b == nil {
		return l, out
	}
	if l != nil {
		w := &pubsub.RequestLedger{Unasked: l.Unasked - b.unasked[i]}
		for _, r := range l.Records {
			t0 := r.Arrived
			if len(r.Asks) > 0 {
				t0 = r.Asks[0].At
			}
			if !t0.Before(b.at) {
				w.Records = append(w.Records, r)
			}
		}
		l = w
	}
	for p, s := range out {
		o, ok := b.outbound[i][p]
		if !ok {
			continue
		}
		s.WriteBlocked -= o.WriteBlocked
		s.Writes -= o.Writes
		s.Cancelled -= o.Cancelled
		s.Attached -= o.Attached
		s.Reordered -= o.Reordered
		s.Projected -= o.Projected
		s.CreditWait -= o.CreditWait
		s.CreditAcquires -= o.CreditAcquires
		out[p] = s
	}
	return l, out
}

func newHorizonSnapshot(t *testing.T, n int, at time.Duration, pub int, base *horizonBase) *horizonSnapshot {
	h := &horizonSnapshot{at: at, startAt: time.Now(), rx: make([]int, n), ctrl: make([]int, n), dups: make([]int, n),
		dupStats: make([]nodeDups, n),
		ledgers:  make([]*pubsub.RequestLedger, n), outbound: make([]map[peer.ID]pubsub.OutboundStat, n),
		linkDropped: make([]int, n), linkEnqueued: make([]int, n), pub: pub, base: base}
	h.popNames, h.pops = populations(t, n, pub)
	h.attacker = map[int]bool{}
	for _, i := range h.pops["attacker"] {
		h.attacker[i] = true
	}
	return h
}

// payloadRx is a node's payload bytes received on either path: variant B's ride in partial
// messages, A's and C's never do.
func payloadRx(tr *recordingTracer) int {
	_, _, rx, _ := tr.Counts()
	prx, _, _, _ := tr.PartialCounts()
	return rx + prx
}

// take reads every node. Runs in the bubble at the horizon; the routers are alive.
func (h *horizonSnapshot) take(nw *simNetwork, tracers []*recordingTracer) {
	for i := range tracers {
		d, _, _, _ := tracers[i].Counts()
		h.rx[i], h.ctrl[i], h.dups[i] = payloadRx(tracers[i]), tracers[i].ControlSeen().BytesRecv, d
		h.ledgers[i], h.outbound[i] = h.base.since(i, nw.Pubsubs[i].RequestLedger(), nw.Pubsubs[i].OutboundStats())
	}
	h.takeDups(nw, tracers)
	h.xcTake(nw, tracers)
	h.pubID = nw.Hosts[h.pub].ID()
	for _, hst := range nw.Hosts {
		h.ids = append(h.ids, hst.ID())
	}
	h.pubMesh = map[peer.ID]bool{}
	for _, p := range tracers[h.pub].MeshPeerIDs() {
		h.pubMesh[p] = true
	}
	// LinkStats is indexed the way the hosts are: simlibp2p builds them in index order and each
	// calls NewEndpoint once.
	if nw.Sim != nil {
		for i, ls := range nw.Sim.LinkStats() {
			if i >= len(h.linkDropped) {
				break
			}
			h.linkDropped[i] = int(ls.Uplink.PacketsDropped + ls.Downlink.PacketsDropped)    // lint:ignore uintcast -- packet counts.
			h.linkEnqueued[i] = int(ls.Uplink.PacketsEnqueued + ls.Downlink.PacketsEnqueued) // lint:ignore uintcast -- packet counts.
			if h.base != nil {
				h.linkDropped[i] -= h.base.linkDropped[i]
				h.linkEnqueued[i] -= h.base.linkEnqueued[i]
			}
		}
	}
	// Exposure classes: honest nodes by the attackers in their mesh at the horizon (the capture
	// screen's victims are the exposed, not a placed set).
	if len(h.attacker) > 0 {
		idx := make(map[peer.ID]int, len(nw.Hosts))
		for i, hst := range nw.Hosts {
			idx[hst.ID()] = i
		}
		for _, i := range h.pops["honest"] {
			c := 0
			for _, p := range tracers[i].MeshPeerIDs() {
				if h.attacker[idx[p]] {
					c++
				}
			}
			cls := "x0"
			switch {
			case c >= 5:
				cls = "x5p"
			case c >= 3:
				cls = "x3_4"
			case c >= 1:
				cls = "x1_2"
			}
			h.pops[cls] = append(h.pops[cls], i)
		}
		for _, cls := range []string{"x0", "x1_2", "x3_4", "x5p"} {
			if len(h.pops[cls]) > 0 {
				h.popNames = append(h.popNames, cls)
			}
		}
	}
	h.takeAt = time.Now()
	h.taken = true
}

// populations names the node sets the horizon reports separately: honest (every non-publisher
// node not injected as an adversary), attacker (withholders and spoofers), and among the honest
// the link classes when the cell has them — slow (thin links), dc (datacenter) and other. The
// sets follow the injectors' and meshLinks' own selection, so a node is in exactly the class it
// was built with.
func populations(t *testing.T, n, pub int) ([]string, map[string][]int) {
	attacker := map[int]bool{}
	for i := range failWithholdSet(t, n) {
		attacker[i] = true
	}
	for i := range failSpoofSet(t, n) {
		attacker[i] = true
	}
	dcCount, slowCount := meshLinkCounts(t, n)
	pops := map[string][]int{}
	add := func(name string, i int) { pops[name] = append(pops[name], i) }
	// Under the heterogeneity sweep there are no index blocks -- every node is its own class --
	// so the classes are read off the same draw meshLinks used. Both call the generator rather
	// than one of them re-deriving the boundaries: a per-class table that does not describe the
	// cell is the same failure as the wire basis, and it looks like a result.
	sigma := sigmaSweepOn(t, n)
	slowSet, dcSet := sigmaNodeClasses(t, n)
	inClass := map[int]string{}
	if sigma {
		slowCount, dcCount = len(slowSet), len(dcSet)
		for _, i := range slowSet {
			inClass[i] = "slow"
		}
		for _, i := range dcSet {
			inClass[i] = "dc"
		}
	}
	for i := range n {
		if i == pub {
			continue
		}
		if attacker[i] {
			add("attacker", i)
			continue
		}
		add("honest", i)
		switch {
		case i == 0:
			// Node 0 is a receiver only when another node publishes (multi-message cells); it
			// has the base link, whatever the index blocks after it are.
			add("other", i)
		case sigma:
			if c, ok := inClass[i]; ok {
				add(c, i)
			} else {
				add("other", i)
			}
		case i < 1+dcCount:
			add("dc", i)
		case i < 1+dcCount+slowCount:
			add("slow", i)
		default:
			add("other", i)
		}
	}
	names := []string{"honest"}
	if len(pops["attacker"]) > 0 {
		names = append(names, "attacker")
	}
	if dcCount > 0 || slowCount > 0 {
		for _, c := range []string{"slow", "dc", "other"} {
			if len(pops[c]) > 0 {
				names = append(names, c)
			}
		}
	} else {
		delete(pops, "other")
	}
	if _, holders := narrowHolders(t, n); len(holders) > 0 {
		pops["holder"] = holders
		names = append(names, "holder")
	}
	return names, pops
}

// quantile of a sorted slice, nearest rank.
func quantile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	i := int(float64(len(sorted)-1)*p + 0.5)
	return sorted[i]
}

func msOf(v float64) string { return fmt.Sprintf("%dms", int64(v/1e6)) }

// lines renders the horizon: per population its bytes, its requests and its outbound queues,
// then the publisher's queues alone. compAt and rxAtComp come from the driver's completion loop.
func (h *horizonSnapshot) lines(compAt []time.Duration, rxAtComp []int) []string {
	if h == nil || !h.taken {
		return nil
	}
	ms := h.at.Milliseconds()
	var out []string
	for _, name := range h.popNames {
		set := h.pops[name]
		if len(set) == 0 {
			continue
		}
		tag := fmt.Sprintf("@%dms [%s %d]", ms, name, len(set))
		// bytes
		var rx, ctrl, dups, done, post int
		for _, i := range set {
			rx += h.rx[i]
			ctrl += h.ctrl[i]
			dups += h.dups[i]
			if compAt[i] > 0 && compAt[i] <= h.at {
				done++
				post += h.rx[i] - rxAtComp[i]
			}
		}
		postPer := 0
		if done > 0 {
			postPer = post / done
		}
		out = append(out, fmt.Sprintf("bytes%s: completed %d/%d rx/node %dB ctrl rx/node %dB dups %d post-completion rx/node %dB",
			tag, done, len(set), rx/len(set), ctrl/len(set), dups, postPer))
		if h.segments > 0 {
			out = append(out, "dups"+tag+": "+h.dupLine(set, dups))
			out = append(out, "cancel"+tag+": "+h.cancelLine(set))
		}
		// CoDel drops, with their denominator. Zero drops over zero enqueued means the snapshot
		// could not read the link, which is a different fact from an uncongested cell.
		var codelDropped, codelEnqueued int
		for _, i := range set {
			codelDropped += h.linkDropped[i]
			codelEnqueued += h.linkEnqueued[i]
		}
		if codelEnqueued > 0 {
			out = append(out, fmt.Sprintf("codel%s: dropped %d of %d packets (%.3f%%), %.1f drops/node",
				tag, codelDropped, codelEnqueued, 100*float64(codelDropped)/float64(codelEnqueued),
				float64(codelDropped)/float64(len(set))))
		}
		// requests
		var asked, answered, overtaken, open, abandoned, dropped, pushes, asks, adups int
		var service, ages, topShare, answerers []float64
		for _, i := range set {
			l := h.ledgers[i]
			if l == nil {
				continue
			}
			pushes += l.Unasked
			byPeer := map[peer.ID]int{}
			nodeAnswered := 0
			done := compAt[i] > 0 && compAt[i] <= h.at
			for _, r := range l.Records {
				asked++
				asks += len(r.Asks)
				adups += r.Dups
				for _, a := range r.Asks {
					if a.Dropped {
						dropped++
					}
				}
				if r.Arrived.IsZero() {
					// An unanswered ask on a node that has completed was abandoned, not failed: the
					// node stopped needing it (a code's spare ids, a group's tail after the fact).
					if done {
						abandoned++
						continue
					}
					open++
					ages = append(ages, float64(h.takeAt.Sub(r.Asks[0].At)))
					continue
				}
				if !r.Solicited {
					overtaken++
					continue
				}
				answered++
				nodeAnswered++
				byPeer[r.From]++
				var askAt time.Time
				for _, a := range r.Asks {
					if a.Peer == r.From && !a.At.After(r.Arrived) {
						askAt = a.At
					}
				}
				service = append(service, float64(r.Arrived.Sub(askAt)))
			}
			if nodeAnswered > 0 {
				top := 0
				for _, c := range byPeer {
					if c > top {
						top = c
					}
				}
				topShare = append(topShare, float64(top)/float64(nodeAnswered))
				answerers = append(answerers, float64(len(byPeer)))
			}
		}
		for _, v := range [][]float64{service, ages, topShare, answerers} {
			sort.Float64s(v)
		}
		asksPerID := 0.0
		if asked > 0 {
			asksPerID = float64(asks) / float64(asked)
		}
		out = append(out, fmt.Sprintf("requests%s: ids asked %d answered %d overtaken %d open %d (age p50 %s) abandoned %d dropped-asks %d pushes %d; asks/id %.2f answered-dups %d; service p50 %s p90 %s p99 %s; top answerer p50 %.2f answerers p50 %.0f",
			tag, asked, answered, overtaken, open, msOf(quantile(ages, 0.5)), abandoned, dropped, pushes, asksPerID, adups,
			msOf(quantile(service, 0.5)), msOf(quantile(service, 0.9)), msOf(quantile(service, 0.99)),
			quantile(topShare, 0.5), quantile(answerers, 0.5)))
		// outbound
		out = append(out, "outbound"+tag+": "+h.outboundLine(set))
		if l := h.xcheckLine(set); l != "" {
			out = append(out, "xcheck"+tag+": "+l)
		}
	}
	out = append(out, fmt.Sprintf("outbound@%dms [publisher]: %s", ms, h.outboundLine([]int{h.pub})))
	if l := h.xcheckLine([]int{h.pub}); l != "" {
		out = append(out, fmt.Sprintf("xcheck@%dms [publisher]: %s", ms, l))
	}
	out = append(out, fmt.Sprintf("source-pulls@%dms: %s", ms, h.sourcePullsLine()))
	return out
}

// sourcePullsLine reads, from the receivers' request ledgers, the pulls the publisher answered:
// how many ids and from how many askers, how many of those askers sit in its mesh, and whether
// the publisher was the id's first ask or a later one (a retry or a hedge after another
// announcer). Asks to it that another peer's copy overtook are counted apart.
func (h *horizonSnapshot) sourcePullsLine() string {
	var answered, first, later, overtaken, asks, meshAskers int
	askers := map[int]bool{}
	for i, l := range h.ledgers {
		if l == nil || i == h.pub {
			continue
		}
		for _, r := range l.Records {
			askedPub := false
			for _, a := range r.Asks {
				if a.Peer == h.pubID {
					asks++
					askedPub = true
				}
			}
			if !askedPub {
				continue
			}
			if r.Arrived.IsZero() || r.From != h.pubID || !r.Solicited {
				overtaken++
				continue
			}
			answered++
			askers[i] = true
			if len(r.Asks) > 0 && r.Asks[0].Peer == h.pubID {
				first++
			} else {
				later++
			}
		}
	}
	for i := range askers {
		if i < len(h.ids) && h.pubMesh[h.ids[i]] {
			meshAskers++
		}
	}
	return fmt.Sprintf("asks to the publisher %d, ids it answered %d (first ask %d, after another announcer %d), overtaken or unanswered %d; askers %d, in its mesh %d",
		asks, answered, first, later, overtaken, len(askers), meshAskers)
}

// nodeDups is one node's duplicate arrivals by the horizon, each read against the node's own
// first delivery of the same frame: how far behind it landed, and the share of K the node still
// lacked at that instant (0 once complete). The D3 second form's instrument: the
// first-arrival matrix cannot say when the copies that are not first arrive, and that timing is
// what decides whether a dispatch-time cancel could have reached them.
type nodeDups struct {
	events  int
	offsets []time.Duration
	wants   []float64
	// The sender's side of each duplicate, joined through the dispatch ledger: joined is the
	// duplicates whose sender recorded popping that frame to this node; of those, failed were
	// popped after this node's HAVE had reached the sender (a cancel that did not fire), blind
	// were popped before this node had its first copy (no signal could have helped), signal
	// were popped in between (the have-signal's latency window), and pull came from a sender
	// outside this node's mesh — an IWANT answer, which no have-signal reaches under the
	// protocol. haveDelay is HAVE arrival at the sender less this node's first delivery; flight
	// is arrival less pop; signalGap is pop less first delivery for the signal class — the speed
	// a have-signal would need.
	joined, failed, blind, signal, pull int
	// failedStrict is the failed duplicates whose HAVE reached the sender strictly before the
	// pop; the rest of failed share the pop's virtual instant, a goroutine-order race.
	failedStrict                 int
	haveDelay, flight, signalGap []time.Duration
	// lateGap is pop less HAVE arrival for the failedStrict class: how long the sender had the
	// HAVE before it popped the frame anyway.
	lateGap []time.Duration
}

// takeDups reads every tracer's duplicate log against its first deliveries, and each duplicate
// against its sender's dispatch ledger. K is the distinct frames anyone was delivered by the
// horizon, so no arm has to say how many it published.
func (h *horizonSnapshot) takeDups(nw *simNetwork, tracers []*recordingTracer) {
	firsts := make([]map[digest]time.Time, len(tracers))
	events := make([][]recvEvent, len(tracers))
	all := map[digest]struct{}{}
	for i, tr := range tracers {
		tr.Mu.Lock()
		f := make(map[digest]time.Time, len(tr.Recvs))
		for _, e := range tr.Recvs {
			if _, seen := f[e.What]; !seen {
				f[e.What] = e.At
			}
		}
		events[i] = append([]recvEvent(nil), tr.Dups...)
		tr.Mu.Unlock()
		for d := range f {
			all[d] = struct{}{}
		}
		firsts[i] = f
	}
	h.segments = len(all)
	// Each sender's ledger, by the receiving peer and the frame's digest: what it popped to whom
	// and when, and when that peer's HAVE covering the frame reached it.
	idx := make(map[peer.ID]int, len(nw.Hosts))
	for i, hst := range nw.Hosts {
		idx[hst.ID()] = i
	}
	ledgers := make([]map[peer.ID]map[digest]pubsub.HaveLedgerEntry, len(tracers))
	for i := range tracers {
		if i >= len(nw.Pubsubs) {
			break
		}
		for _, e := range nw.Pubsubs[i].HaveLedger() {
			if e.PopAt.IsZero() {
				continue
			}
			if ledgers[i] == nil {
				ledgers[i] = make(map[peer.ID]map[digest]pubsub.HaveLedgerEntry)
			}
			byDigest := ledgers[i][e.Peer]
			if byDigest == nil {
				byDigest = make(map[digest]pubsub.HaveLedgerEntry)
				ledgers[i][e.Peer] = byDigest
			}
			byDigest[digest(e.Digest)] = e
		}
	}
	for i := range tracers {
		f := firsts[i]
		order := make([]time.Time, 0, len(f))
		for _, at := range f {
			order = append(order, at)
		}
		sort.Slice(order, func(a, b int) bool { return order[a].Before(order[b]) })
		var me peer.ID
		if i < len(nw.Hosts) {
			me = nw.Hosts[i].ID()
		}
		inMesh := map[peer.ID]bool{}
		for _, p := range tracers[i].MeshPeerIDs() {
			inMesh[p] = true
		}
		nd := nodeDups{events: len(events[i])}
		for _, e := range events[i] {
			first, ok := f[e.What]
			if !ok {
				continue
			}
			nd.offsets = append(nd.offsets, e.At.Sub(first))
			held := sort.Search(len(order), func(j int) bool { return !order[j].Before(e.At) })
			nd.wants = append(nd.wants, float64(h.segments-held)/float64(h.segments))
			s, known := idx[e.From]
			if !known || s >= len(ledgers) || ledgers[s] == nil {
				continue
			}
			le, ok := ledgers[s][me][e.What]
			if !ok {
				continue
			}
			nd.joined++
			nd.flight = append(nd.flight, e.At.Sub(le.PopAt))
			if !inMesh[e.From] {
				nd.pull++
				continue
			}
			if !le.HaveAt.IsZero() {
				nd.haveDelay = append(nd.haveDelay, le.HaveAt.Sub(first))
			}
			switch {
			case !le.HaveAt.IsZero() && !le.HaveAt.After(le.PopAt):
				nd.failed++
				if le.HaveAt.Before(le.PopAt) {
					nd.failedStrict++
					nd.lateGap = append(nd.lateGap, le.PopAt.Sub(le.HaveAt))
				}
			case le.PopAt.Before(first):
				nd.blind++
			default:
				nd.signal++
				nd.signalGap = append(nd.signalGap, le.PopAt.Sub(first))
			}
		}
		h.dupStats[i] = nd
	}
}

// cancelLine is the class's duplicates read from their senders' side: the share whose sender
// popped them after the receiver's HAVE had arrived (a cancel that failed), before the receiver
// had its first copy (blind), or in between (the have-signal's window); the have-signal's delay
// as the sender saw it; the pop-to-arrival flight; and, for the window class, how long after
// the first copy the pop came — the latency a have-signal would need to have caught it.
func (h *horizonSnapshot) cancelLine(set []int) string {
	var events, joined, failed, blind, signal, pull int
	var haveDelay, flight, gap []float64
	for _, i := range set {
		nd := h.dupStats[i]
		events += nd.events
		joined += nd.joined
		failed += nd.failed
		blind += nd.blind
		signal += nd.signal
		pull += nd.pull
		for _, d := range nd.haveDelay {
			haveDelay = append(haveDelay, float64(d))
		}
		for _, d := range nd.flight {
			flight = append(flight, float64(d))
		}
		for _, d := range nd.signalGap {
			gap = append(gap, float64(d))
		}
	}
	for _, v := range [][]float64{haveDelay, flight, gap} {
		sort.Float64s(v)
	}
	pct := func(c, n int) string {
		if n == 0 {
			return "-"
		}
		return fmt.Sprintf("%.0f%%", 100*float64(c)/float64(n))
	}
	return fmt.Sprintf("dups %d joined %s: failed %s blind %s signal %s pull %s; have-signal delay p50 %s p90 %s; pop-to-arrival p50 %s p90 %s; signal-class pop behind first p50 %s p90 %s",
		events, pct(joined, events), pct(failed, joined), pct(blind, joined), pct(signal, joined), pct(pull, joined),
		msOf(quantile(haveDelay, 0.5)), msOf(quantile(haveDelay, 0.9)),
		msOf(quantile(flight, 0.5)), msOf(quantile(flight, 0.9)),
		msOf(quantile(gap, 0.5)), msOf(quantile(gap, 0.9)))
}

// dupLine is the class's duplicate arrivals: the count against the tracer counter (they must
// agree), copies per frame per receiver from events, the offset behind the first copy, and the
// receiver's remaining want set when the copy landed -- §3.2's curve, from per-arrival events.
func (h *horizonSnapshot) dupLine(set []int, counter int) string {
	var offs []float64
	var events int
	offB := make([]int, 5)  // <50, 50-100, 100-200, 200-500, >=500 ms
	wantB := make([]int, 6) // complete, <=1/8, 1/8-1/4, 1/4-1/2, 1/2-3/4, >3/4 of K still wanted
	for _, i := range set {
		nd := h.dupStats[i]
		events += nd.events
		for _, o := range nd.offsets {
			ms := o.Milliseconds()
			switch {
			case ms < 50:
				offB[0]++
			case ms < 100:
				offB[1]++
			case ms < 200:
				offB[2]++
			case ms < 500:
				offB[3]++
			default:
				offB[4]++
			}
			offs = append(offs, float64(o))
		}
		for _, w := range nd.wants {
			switch {
			case w <= 0:
				wantB[0]++
			case w <= 0.125:
				wantB[1]++
			case w <= 0.25:
				wantB[2]++
			case w <= 0.5:
				wantB[3]++
			case w <= 0.75:
				wantB[4]++
			default:
				wantB[5]++
			}
		}
	}
	sort.Float64s(offs)
	n := len(offs)
	pct := func(c int) string {
		if n == 0 {
			return "-"
		}
		return fmt.Sprintf("%.0f%%", 100*float64(c)/float64(n))
	}
	copies := 1 + float64(events)/float64(len(set)*h.segments)
	return fmt.Sprintf("events %d (counter %d) copies/frame %.2f over K=%d; behind first <50ms %s 50-100 %s 100-200 %s 200-500 %s >=500 %s, p50 %s p90 %s; want-set at arrival >3/4 %s 1/2-3/4 %s 1/4-1/2 %s 1/8-1/4 %s <=1/8 %s complete %s",
		events, counter, copies, h.segments, pct(offB[0]), pct(offB[1]), pct(offB[2]), pct(offB[3]), pct(offB[4]),
		msOf(quantile(offs, 0.5)), msOf(quantile(offs, 0.9)),
		pct(wantB[5]), pct(wantB[4]), pct(wantB[3]), pct(wantB[2]), pct(wantB[1]), pct(wantB[0]))
}

func (h *horizonSnapshot) outboundLine(set []int) string {
	var peak, blocked, busiest, creditWait []float64
	var cancelled, attached, reordered, projected, acquires int64
	for _, i := range set {
		m := h.outbound[i]
		if m == nil {
			continue
		}
		var pk, bl, bs, cw int64
		for _, s := range m {
			if s.PeakQueuedBytes > pk {
				pk = s.PeakQueuedBytes
			}
			bl += int64(s.WriteBlocked)
			if int64(s.WriteBlocked) > bs {
				bs = int64(s.WriteBlocked)
			}
			cancelled += s.Cancelled
			attached += s.Attached
			reordered += s.Reordered
			projected += s.Projected
			cw += int64(s.CreditWait)
			acquires += s.CreditAcquires
		}
		peak = append(peak, float64(pk))
		blocked = append(blocked, float64(bl))
		busiest = append(busiest, float64(bs))
		creditWait = append(creditWait, float64(cw))
	}
	for _, v := range [][]float64{peak, blocked, busiest, creditWait} {
		sort.Float64s(v)
	}
	if len(set) == 1 {
		return fmt.Sprintf("peak queued/peer %dKB; writer blocked %s, busiest peer %s",
			int(quantile(peak, 0.5))>>10, msOf(quantile(blocked, 0.5)), msOf(quantile(busiest, 0.5)))
	}
	line := fmt.Sprintf("peak queued/peer p50 %dKB p90 %dKB; writer blocked/node p50 %s p90 %s, busiest peer p50 %s; cancelled at dispatch %d, HAVEs attached %d, reordered %d, projected %d",
		int(quantile(peak, 0.5))>>10, int(quantile(peak, 0.9))>>10,
		msOf(quantile(blocked, 0.5)), msOf(quantile(blocked, 0.9)), msOf(quantile(busiest, 0.5)), cancelled, attached, reordered, projected)
	if acquires > 0 {
		// Dispatch credits (D5): time the writers spent waiting for a slot, summed per node.
		line += fmt.Sprintf("; credit waited/node p50 %s p90 %s, slots taken %d",
			msOf(quantile(creditWait, 0.5)), msOf(quantile(creditWait, 0.9)), acquires)
	}
	return line
}
