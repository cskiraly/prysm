package segmentintegrationtest

// Shared diffusion driver for the mesh variants (A = q6, C with or without custody).
//
// Every variant runs the same experiment: bring up a random-regular mesh with a per-node
// tracer, settle it, publish a payload's worth of gossip messages from node 0, wait for every
// other node to complete, then aggregate the tracers into one summary line. Only the transport
// strategy differs -- how the payload is split into units, which topics a node joins, how a
// node decides it is complete, and where failure is injected. runDiffusion owns everything
// common (config wiring, the network, the synctest cell, the completion fan-in, and the metric
// aggregation); each variant supplies the rest through the variant/diffusionRun interfaces.
//
// The completion-signal callback is the key unifier: every variant's receive goroutines call
// signal(node, at) when their node completes, so the driver's fan-in is identical whether a
// variant scans one subscription or many.

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/marcopolo/simnet"
)

// diffusionParams is the resolved configuration for one cell.
type diffusionParams struct {
	n        int
	rate     int
	latency  time.Duration
	deadline time.Duration
	seed     uint64
}

// diffusionStats holds the metrics the driver aggregates from the tracers. The union of every
// variant's fields; each variant's report reads the subset it prints.
type diffusionStats struct {
	// Virtual time spent before the measured publish, which every figure excludes:
	// tPeer connections up, tMesh gossipsub meshes settled, tPre warm-up and pre-publish hooks.
	tPeer, tMesh, tPre time.Duration
	n, completed       int
	virt, wall         time.Duration
	compTimes          []time.Duration
	drops, dups        int
	rxBytes, txBytes   int
	linkRx             int // bytes the downlinks took in less their drops, all protocols, publish to completion
	socketRx           int // what the downlinks handed the sockets over the same interval (link rx counts at the queue's intake)
	idwSent, idwRecv   int
	ctrlRx             int
	publisherTx        int
	meshSlots          int
	meshSummary        string
	gateSummary        string // receiver authority gate decisions, empty when no gate ran
	pubControl         controlCounts
	deadline           time.Duration
	// Variant B's partial-message traffic. Union fields, summed for every variant and harmlessly
	// zero for A and C, which never move partial-message RPCs.
	partialRx, partialTx       int // partial-message PAYLOAD bytes received / sent, all nodes
	partialRPCs, partialTxRPCs int // partial-message RPCs received / sent, all nodes
	// Bitmap signalling, counted apart from the payload above. This is the partial path's
	// analogue of ctrlRx: it is what makes a bitmap variant's signalling cost comparable with an
	// IHAVE/IWANT variant's, where the control bytes have always been reported separately and
	// excluded from the copies figure. Summed into the payload it put the two families on
	// different bases and hid B's signalling inside B's copies.
	partialMetaRx, partialMetaTx int
	// The raw event log and the publish instant, for reports that need per-message shapes
	// (e.g. per-segment diffusion spread) rather than the aggregates above. Read under each
	// tracer's Mu.
	tracers            []*recordingTracer
	startAt            time.Time
	partialPublisherTx int // partial-message bytes sent by node 0
	// The observation horizon: every node read at the deadline after publish, on the happy
	// path and the censored one alike (horizon_test.go). compAt is each node's completion
	// (zero: not by the harness timeout) and rxAtComp the payload bytes it had received then,
	// so what arrived afterwards is post-completion traffic.
	horizon  *horizonSnapshot
	compAt   []time.Duration
	rxAtComp []int
	// pub is the message's publisher; msg and msgs place it in a multi-message cell (1 of 1
	// otherwise).
	pub, msg, msgs int
}

// completionEvent is one node reaching its completion predicate, at a virtual time relative to
// publish start.
type completionEvent struct {
	node int
	at   time.Duration
}

// variant is a transport strategy: it names itself, contributes per-node pubsub options, builds
// the run for a cell, and formats the variant-specific log output.
type variant interface {
	name() string
	// pubsubOpts returns per-node options; the driver adds the tracer itself.
	pubsubOpts(t *testing.T, i int) []pubsub.Option
	// setup joins/subscribes topology and builds publishable units, returning the run.
	setup(t *testing.T, nw *simNetwork, p diffusionParams) diffusionRun
	// onTimeout logs the variant's timeout message, inside the bubble.
	onTimeout(t *testing.T, completed, n int)
	// report emits the variant-specific summary line(s), outside the bubble.
	report(t *testing.T, s diffusionStats)
}

// diffusionRun is one variant's live state for a cell.
type diffusionRun interface {
	// settle waits for the variant's meshes to form and returns a summary for the log line.
	settle(t *testing.T, tracers []*recordingTracer) string
	// cancel cancels the variant's subscriptions; deferred inside the bubble.
	cancel()
	// watch spawns receive goroutines on wg; each calls signal(node, at) once its node completes.
	watch(ctx context.Context, wg *sync.WaitGroup, start time.Time, signal func(node int, at time.Duration))
	// publish sends the payload's units from node 0.
	publish(ctx context.Context, t *testing.T) error
}

// gateDriven is an optional variant hook: a variant that models block arrival itself takes the
// cell's gates and opens them on its own schedule, rather than letting the driver run the synthetic
// offset. It is what makes the authority gate testable in its real ordering.
type gateDriven interface {
	setGates(*segmentGates)
}

// partialDiffusion is an optional variant hook for the diffusion instrument: a variant whose
// segments travel in rpc.Partial rather than rpc.Publish supplies the decoder that makes them
// visible, and the key list that reads the curves back. Without it both curves are empty for the
// whole variant -- see RecordingTracer.TrackPartialSegments.
type partialDiffusion interface {
	partialObserve() partialObserveFn
	segKeys() []string
}

// prePublisher is an optional run hook run after mesh settle and before publish, used for A's
// co-resident background traffic. It may advance virtual time (mesh formation), which is why it
// runs before the driver captures the completion start.
type prePublisher interface {
	prePublish(t *testing.T, ctx context.Context, wg *sync.WaitGroup)
}

// announceRetryCeiling is one tick past the longest sleep pubsub's announceRetry can take
// (`1+rand.Intn(1000)` ms, upstream pubsub.go). See the drain in runDiffusion.
const announceRetryCeiling = 1001 * time.Millisecond

// runDiffusion runs one cell of the shared diffusion experiment for the given variant.
func runDiffusion(t *testing.T, p diffusionParams, v variant) {
	t.Helper()
	wall := time.Now()
	// Before the bubble: the window is read when each connection builds its sender.
	applyInitialCWND(t)
	stats := diffusionStats{n: p.n, deadline: p.deadline}
	warmOnly := false
	plan := messagePlanFromEnv(t, p.n, p.deadline)
	// IDONTWANT spoofers claim every id and so are never sent it: the wait is for everyone else.
	spoofers := failSpoofSet(t, p.n)
	need := func(pub int) int {
		k := p.n - 1
		for i := range spoofers {
			if i != pub {
				k--
			}
		}
		return k
	}
	if len(spoofers) > 0 {
		t.Logf("      completion waits for %d nodes: %d IDONTWANT spoofers are reported by class, not waited for", need(0), len(spoofers))
	}
	mm, multi := v.(multiMessage)
	if len(plan.pubs) > 1 && !multi {
		t.Fatalf("SEGMENT_MESSAGES: the %s variant has no multi-message support", v.name())
	}
	if plan.pubs[0] != 0 {
		fp, ok := v.(firstPublisher)
		if !ok {
			t.Fatalf("the %s variant publishes the first message from node 0 only", v.name())
		}
		fp.setFirstPublisher(plan.pubs[0])
	}
	var sums []messageSummary
	synctest.Test(t, func(t *testing.T) {
		bubbleStart := time.Now()
		// Let pubsub's untracked retry goroutines finish before the bubble closes.
		//
		// announce() spawns announceRetry as a bare goroutine whenever a peer's outbound queue is
		// full: it sleeps up to a second with no context, then exits through p.ctx, which the
		// network's stop() cancels. Nothing waits for it. Any cell that congests queues on purpose
		// -- an adversary, competing traffic -- therefore leaves sleepers behind, and because
		// synctest.Wait returns *without* advancing the clock, teardown cannot drain them: the
		// bubble returns with goroutines still asleep and panics as a deadlock, reported far from
		// its cause. Advancing past the retry ceiling wakes each one onto a cancelled context.
		//
		// Registered before stop's own defer so that it runs after it, and virtual, so it costs
		// nothing and happens entirely after the last measurement.
		defer func() {
			time.Sleep(announceRetryCeiling)
			synctest.Wait()
		}()
		tracers := make([]*recordingTracer, p.n)
		pd, onPartialPath := v.(partialDiffusion)
		for i := range tracers {
			tracers[i] = newRecordingTracer()
			if diffusionCurves() {
				tracers[i].TrackAnnounce()
				if onPartialPath {
					tracers[i].TrackPartialSegments(pd.partialObserve())
				}
			}
		}
		// The authority gate is installed by the driver rather than by each variant: it is a
		// property of the cell, not of the transport strategy, and every variant that pulls is
		// subject to it. nil unless the cell asks for one.
		// A gate-driven variant supplies authority from its own model of block arrival; everything
		// else runs the synthetic schedule.
		gd, driven := v.(gateDriven)
		gates := newSegmentGates(t, p.n, p.seed, driven)
		if driven {
			gd.setGates(gates)
		}
		// Built once: the network and the cross-check's dump read the same links and graph.
		links, edges := meshLinks(t, p.n, p.rate), meshGraph(t, p.n)
		nw, stop := newSimNetwork(t, networkConfig{
			links:   links,
			edges:   edges,
			latency: simnet.StaticLatency(p.latency),
			perNodeOpts: func(i int) []pubsub.Option {
				opts := []pubsub.Option{pubsub.WithRawTracer(tracers[i]), pubsub.WithRequestObservation()}
				opts = append(opts, structuredIDOpts()...)
				opts = append(opts, gossipPaceOpts(t, i, p.seed)...)
				if gates != nil {
					opts = append(opts, pubsub.WithRequestGate(gates.nodes[i], gates.deferrals[i]))
				}
				return append(opts, v.pubsubOpts(t, i)...)
			},
		})
		var wg sync.WaitGroup
		defer stop()
		defer wg.Wait()

		// Virtual-time checkpoints. Every figure in the study starts the clock at publish, so
		// what the network spent getting there is invisible unless it is recorded here.
		tPeered := time.Now()

		r := v.setup(t, nw, p)
		defer r.cancel()
		stats.meshSummary = r.settle(t, tracers)
		tMeshed := time.Now()
		xc := xcDumpOpen(t)
		defer xc.close(t)
		xc.net(t, p, links, edges, pushDegreeOf(v))

		ctx, cancel := context.WithTimeout(context.Background(), networkOpTimeout)
		defer cancel()

		// Link-level transport warm-up, before any variant hook: it must not interleave with
		// background load, and it is the only warm-up that leaves protocol state untouched.
		if n := warmupBytes(t); n > 0 {
			ws := warmConnections(t, ctx, nw, n)
			// Let the warm-up fully drain before the clock starts. A stream Close only closes
			// the write side, and synctest.Wait returns without advancing the clock, so
			// neither alone empties the links: the measured publish would contend with
			// warm-up bytes still in flight and read as the warm-up making things slower.
			// Virtual time, so the sleep is free, and quic-go does not shrink the congestion
			// window when a connection idles.
			time.Sleep(warmupDrain)
			settle(false)
			// SEGMENT_WARMUP_ONLY stops here, before anything is published. With QLOGDIR set,
			// each connection's final qlog state is then exactly its state at publish time,
			// which is the only clean way to read back the congestion window and smoothed RTT
			// a warm-up actually left behind.
			if os.Getenv("SEGMENT_WARMUP_ONLY") != "" {
				warmOnly = true
			}
			t.Logf("      transport warm-up: %d links x %dKiB, first half %.1f Mbps, second half %.1f Mbps, %v",
				ws.links, ws.bytes>>10, ws.firstHalfRate()/1e6, ws.secondHalfRate()/1e6, ws.elapsed)
		}

		if pp, ok := r.(prePublisher); ok {
			pp.prePublish(t, ctx, &wg)
		}
		// A warm-up arm moves a whole payload's worth of bytes before the measured one, so its
		// traffic has to leave the counters or every byte metric doubles. Mesh membership is
		// kept: the mesh really is formed.
		if os.Getenv("SEGMENT_WARMUP") != "" {
			for _, tr := range tracers {
				tr.ResetCounters()
			}
		}
		if warmOnly {
			return
		}
		tReady := time.Now()
		stats.tPeer = tPeered.Sub(bubbleStart)
		stats.tMesh = tMeshed.Sub(tPeered)
		stats.tPre = tReady.Sub(tMeshed)

		var prevStart time.Time
		for m := 1; m <= len(plan.pubs); m++ {
			var base *horizonBase
			if m > 1 {
				if gates != nil {
					t.Fatal("multi-message cells do not run the authority gate")
				}
				// A gap after the previous publish; the network idles on heartbeats meanwhile.
				time.Sleep(time.Until(prevStart.Add(plan.gap)))
				mm.nextMessage(t, m, plan.pubs[m-1])
				for _, tr := range tracers {
					tr.ResetCounters()
				}
				stats = diffusionStats{n: p.n, deadline: p.deadline}
				// The mesh as it stands at this publish: it has had a gap of heartbeats to churn.
				stats.meshSummary = summariseMesh(meshSizes(tracers))
				base = takeHorizonBase(nw)
				var cancelM context.CancelFunc
				ctx, cancelM = context.WithTimeout(context.Background(), networkOpTimeout)
				defer cancelM()
			}
			stats.pub, stats.msg, stats.msgs = plan.pubs[m-1], m, len(plan.pubs)
			breaks0, parks0, retries0 := parkBreaks.Load(), parks.Load(), offerRetries.Load()
			held0 := promisesHeld(nw)
			v050 := v05Now()

			// The announce-rate histogram describes the measured diffusion, not the settle heartbeats.
			announceStats.Reset()
			done := make(chan completionEvent, p.n)
			uploadNewWindow(nw) // the settle heartbeats and the previous gap are not this message's
			promiseNewWindow(nw)
			linkBase, socketBase := downlinkBytes(nw), socketBytes(nw)
			start := time.Now()
			prevStart = start
			// The horizon is taken on this goroutine at the deadline, while every subscription is
			// still up: as a timer case of the completion loop, or as a wait after the last
			// completion. A timer goroutine would race the deferred unsubscribe and read pruned
			// meshes and a network that has left the topic.
			stats.compAt = make([]time.Duration, p.n)
			stats.rxAtComp = make([]int, p.n)
			stats.horizon = newHorizonSnapshot(t, p.n, p.deadline, stats.pub, base)
			stats.horizon.degree, stats.horizon.askWindow = pushDegreeOf(v), xcAskWindow()
			horizonTimer := time.NewTimer(p.deadline)
			defer horizonTimer.Stop()
			takeHorizon := func() {
				if !stats.horizon.taken {
					stats.horizon.take(nw, tracers)
				}
			}
			// Authority is dated from publish on the synthetic schedule: before the payload exists
			// there is nothing a block could authorize, so a gate with no authority is closed. A
			// gate-driven variant opens its own gates from real block arrival instead.
			if !driven {
				gates.armAll()
			}
			signal := func(node int, at time.Duration) { done <- completionEvent{node: node, at: at} }
			r.watch(ctx, &wg, start, signal)
			xc.mesh(t, m, stats.pub, nw, tracers)
			pubsub.HaveRevoked.Store(0)
			pubsub.AsksForHeldDropped.Store(0)
			pubsub.StaleAsksDropped.Store(0)
			if err := r.publish(ctx, t); err != nil {
				t.Fatal(err)
			}

			timedOut := false
			for !timedOut && stats.completed < need(stats.pub) {
				select {
				case ev := <-done:
					if spoofers[ev.node] {
						continue // a spoofer's claim starves itself; it is reported by class, not waited for
					}
					stats.completed++
					stats.compTimes = append(stats.compTimes, ev.at)
					stats.compAt[ev.node] = ev.at
					stats.rxAtComp[ev.node] = payloadRx(tracers[ev.node])
				case <-ctx.Done():
					stats.virt = time.Since(start)
					if gates != nil {
						// A censored cell is exactly where the gate's own ledger matters, so record
						// it on the timeout path too rather than only on the happy one.
						stats.gateSummary = gates.stats.line() + "\n        " + gates.admitted.line()
					}
					// Aggregate here too: a censored cell without byte counters reads as "nothing
					// moved"; these are the counters at the harness timeout, the horizon's the ones at
					// the deadline.
					aggregateTracers(tracers, &stats)
					stats.linkRx = linkRxSince(nw, linkBase)
					stats.socketRx = socketRxSince(nw, socketBase)
					stats.tracers = tracers
					stats.startAt = start
					takeHorizon()
					v.onTimeout(t, stats.completed, p.n)
					timedOut = true
				case <-horizonTimer.C:
					takeHorizon()
				}
			}
			if !timedOut {
				stats.virt = time.Since(start)
				synctest.Wait()
				aggregateTracers(tracers, &stats)
				stats.linkRx = linkRxSince(nw, linkBase)
				stats.socketRx = socketRxSince(nw, socketBase)
				stats.tracers = tracers
				stats.startAt = start
				if gates != nil {
					stats.gateSummary = gates.stats.line() + "\n        " + gates.admitted.line()
				}
				// Bytes to completion are aggregated above, as every figure reads them; the horizon, if
				// still ahead, is waited for here, before the deferred unsubscribe.
				if !stats.horizon.taken {
					<-horizonTimer.C
					takeHorizon()
				}
			}
			if len(plan.pubs) > 1 {
				// Each message is reported inside the bubble, while the routers it reads are alive;
				// a single-message cell reports after it, as it always has.
				cold := ""
				if m == 1 {
					cold = ", cold"
				}
				t.Logf("=== message %d/%d, publisher node %d%s", m, len(plan.pubs), stats.pub, cold)
				reportCell(t, v, stats)
				t.Logf("        commitment enforcement this message: %d broken promises, %d peers parked; offer-table retries %d; promises due with the index held %d", parkBreaks.Load()-breaks0, parks.Load()-parks0, offerRetries.Load()-retries0, promisesHeld(nw)-held0)
				if os.Getenv("SEGMENT_V05") != "" {
					t.Logf("        %s", v050.line(v05Now()))
				}
				sums = append(sums, summarise(stats))
			}
			if timedOut {
				return
			}
		}
	})
	stats.wall = time.Since(wall)
	if warmOnly {
		t.Logf("      warm-up only: stopped before publish (SEGMENT_WARMUP_ONLY)")
		return
	}
	if len(plan.pubs) > 1 {
		t.Logf("%s; wall %v", messagesLine(plan, sums), stats.wall.Round(time.Millisecond))
	} else {
		reportCell(t, v, stats)
	}
	// Fail after reporting, outside the bubble: a t.Errorf inside synctest.Test propagates as
	// FailNow when the bubble exits, which would skip the censored report entirely.
	if stats.completed < need(stats.pub) {
		t.Errorf("TIMEOUT: message %d: only %d/%d completed", stats.msg, stats.completed, need(stats.pub))
	}
}

// reportCell emits one message's report: the variant's lines, then the driver's own.
func reportCell(t *testing.T, v variant, stats diffusionStats) {
	v.report(t, stats)
	if stats.linkRx > 0 && stats.n > 0 {
		t.Logf("        link rx/node %dB (each downlink's intake less its drops, publish to completion: UDP payloads, every protocol)", stats.linkRx/stats.n)
		t.Logf("        socket rx/node %dB (what the downlinks handed the sockets from the publish to completion; link rx counts bytes as a downlink queue takes them in, less its drops)", stats.socketRx/stats.n)
	}
	if w, ok := v.(wireFormer); ok {
		t.Logf("        %s", w.wireForm())
	}
	if stats.gateSummary != "" {
		t.Logf("        %s", stats.gateSummary)
	}
	for _, line := range stats.horizon.lines(stats.compAt, stats.rxAtComp) {
		t.Logf("        %s", line)
	}
}

// aggregateTracers sums the per-node tracer counters into stats. It computes the union of every
// variant's fields; harmless extras (e.g. txBytes or meshSlots for a variant that ignores them)
// cost only the addition.
func aggregateTracers(tracers []*recordingTracer, s *diffusionStats) {
	for _, tr := range tracers {
		d, u := tr.Losses()
		s.drops += d + u
		dup, _, rx, tx := tr.Counts()
		s.dups += dup
		s.rxBytes += rx
		s.txBytes += tx
		c := tr.ControlSeen()
		s.idwSent += c.IdontwantSent
		s.idwRecv += c.IdontwantRecv
		s.ctrlRx += c.BytesRecv
		s.meshSlots += tr.MeshSlots()
		prx, ptx, prpcs, ptxrpcs := tr.PartialCounts()
		s.partialRx += prx
		s.partialTx += ptx
		s.partialRPCs += prpcs
		s.partialTxRPCs += ptxrpcs
		mrx, mtx, _, _ := tr.PartialMetaCounts()
		s.partialMetaRx += mrx
		s.partialMetaTx += mtx
	}
	s.pubControl = tracers[s.pub].ControlSeen()
	_, _, _, s.publisherTx = tracers[s.pub].Counts()
	_, s.partialPublisherTx, _, _ = tracers[s.pub].PartialCounts()
}

// promisesHeld sums, over the network, the promises that fell due with their index already held.
func promisesHeld(nw *simNetwork) int64 {
	if nw == nil {
		return 0
	}
	var n int64
	for _, ps := range nw.Pubsubs {
		n += ps.PromisesHeldAtDeadline()
	}
	return n
}

// promiseCountOn is SEGMENT_PROMISE_COUNT, implied by SEGMENT_DECODE_CLEARS_PROMISES: the gossip tracer runs, and its promises are reported.
func promiseCountOn() bool {
	return os.Getenv("SEGMENT_PROMISE_COUNT") != "" || os.Getenv("SEGMENT_DECODE_CLEARS_PROMISES") != ""
}

// promiseNewWindow starts every node's broken-promise count afresh for the next message.
func promiseNewWindow(nw *simNetwork) {
	if nw == nil || !promiseCountOn() {
		return
	}
	for _, ps := range nw.Pubsubs {
		ps.ResetBrokenPromises()
	}
}

// promiseLine sums the gossip tracer's promises over every node: charged since publish (P7's input) and still outstanding at the report.
func promiseLine(nw *simNetwork) string {
	var charged, outstanding, pairs, maxPair, maxPeer int
	perPeer := make(map[peer.ID]int)
	for _, ps := range nw.Pubsubs {
		c, o := ps.BrokenPromises()
		both := make(map[peer.ID]int, len(c)+len(o))
		for p, k := range c {
			charged += k
			both[p] += k
		}
		for p, k := range o {
			outstanding += k
			both[p] += k
		}
		for p, k := range both {
			pairs++
			maxPair = max(maxPair, k)
			perPeer[p] += k
		}
	}
	for _, k := range perPeer {
		maxPeer = max(maxPeer, k)
	}
	return fmt.Sprintf("broken promises: %d charged since publish + %d outstanding at the report, over %d (node, peer) pairs; max per (node, peer) %d, max per peer over nodes %d",
		charged, outstanding, pairs, maxPair, maxPeer)
}
