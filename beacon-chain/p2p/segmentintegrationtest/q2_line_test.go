package segmentintegrationtest

// Q2: does segmentation reduce store-and-forward delay?
//
// This is the transport-only arm. No topic validator is registered, so gossipsub forwards
// immediately after reading an RPC, and the per-hop CPU cost of Prysm's real validator -- two
// snappy decompressions, a segment unmarshal, Merkle verification twice, and one BLS
// authentication per group -- is absent. Virtual time would hide that cost anyway. Treat what
// follows as the network term of the model and nothing more.
//
// What the model says. Simnet drives uplink and downlink independently, so a relay can receive
// and send at once, but pubsub writes one framed RPC at a time per peer and the reader consumes
// a whole RPC before handing it on. Forwarding is therefore store-and-forward at one-RPC
// granularity -- per segment, not per byte:
//
//	whole:      h*(Wwhole/R) + h*L
//	segmented:  Wsegments/R + (h-1)*(wmax/R) + h*L
//
// where Wsegments is the summed compressed size of every segment RPC and wmax the largest. The
// marginal cost of a hop is the whole message for one arm and one segment for the other, which
// is the entire claim.
//
// Why absolute times and not the ratio. A ratio over two hop counts cannot separate a slope
// from two different intercepts, and publisher-side encoding, connection warm-up and reassembly
// are fixed costs that amortise as h grows -- they would fake a slope on their own. So fit
//
//	T(h) = alpha + beta*h
//
// over several hop counts and compare the *slopes*: beta_seg should be about wmax/R + L while
// beta_whole should be about Wwhole/R + L. That is a prediction about the marginal hop, which
// nothing but pipelining explains.

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/OffchainLabs/prysm/v7/beacon-chain/p2p/encoder"
	"github.com/OffchainLabs/prysm/v7/config/params"
	"github.com/OffchainLabs/prysm/v7/consensus-types/primitives"
	"github.com/OffchainLabs/prysm/v7/container/segments"
	"github.com/OffchainLabs/prysm/v7/crypto/bls"
	ethpb "github.com/OffchainLabs/prysm/v7/proto/prysm/v1alpha1"
	"github.com/OffchainLabs/prysm/v7/testing/require"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	simlibp2p "github.com/libp2p/go-libp2p/x/simlibp2p"
	"github.com/marcopolo/simnet"
)

const (
	// q2PayloadLen is an execution payload envelope's order of magnitude.
	q2PayloadLen = 1 << 20
)

// wireArm is one encoded message set ready to publish, plus the sizes the model needs.
type wireArm struct {
	name  string
	msgs  [][]byte
	known map[digest]int
	total int // summed encoded bytes actually put on the wire
	max   int // largest single message, the marginal-hop cost
	// need is how many distinct messages complete a node: len(msgs) for plain arms, K for a
	// Reed-Solomon coded arm whose msgs carry K systematic plus parity segments.
	need int
}

// completeAt returns the arm's completion threshold.
func (a wireArm) completeAt() int {
	if a.need > 0 {
		return a.need
	}
	return len(a.msgs)
}

// rawBlob is an opaque SSZ byte-list with no length cap, wire-identical to a one-variable-field
// container: a 4-byte offset then the bytes.
//
// Only the whole-message baseline uses it, and only so the payload-size sweep can run above 1 MiB.
// ExecutionPayloadSegment caps `segment` at segments.MaxSegmentMessageSize, which is dominated by
// MaxSegmentSize = 1 MiB, so a 1 MiB payload fits only by the per-message overhead. Raising that
// bound would relax a deserialization limit on the production segments/p2p/encoder paths for the
// sake of a measurement. Nothing in the mesh driver decodes these bytes; only their length and
// snappy framing matter. encoder.MaxPayloadSize still applies, which is the real ceiling.
type rawBlob []byte

func (b rawBlob) SizeSSZ() int { return 4 + len(b) }

func (b rawBlob) MarshalSSZ() ([]byte, error) {
	return b.MarshalSSZTo(make([]byte, 0, b.SizeSSZ()))
}

func (b rawBlob) MarshalSSZTo(dst []byte) ([]byte, error) {
	var off [4]byte
	binary.LittleEndian.PutUint32(off[:], 4)
	return append(append(dst, off[:]...), b...), nil
}

// wholeArm encodes the payload as a single gossip message: the baseline.
func wholeArm(t *testing.T, payload []byte) wireArm {
	t.Helper()
	var buf bytes.Buffer
	_, err := encoder.SszNetworkEncoder{}.EncodeGossip(&buf, rawBlob(payload))
	require.NoError(t, err)
	b := bytes.Clone(buf.Bytes())
	return wireArm{
		name:  "whole",
		msgs:  [][]byte{b},
		known: map[digest]int{digestOf(b): 0},
		total: len(b),
		max:   len(b),
	}
}

// segmentedArm encodes the payload as K authenticated segments, exactly as the publisher would.
func segmentedArm(t *testing.T, payload []byte, segmentSize int, sk bls.SecretKey, slot primitives.Slot) wireArm {
	t.Helper()
	segs := signedSegments(t, sk, slot, 3, payload, segmentSize)
	applyStructuredPrefixes(segs, slot, groupIDFor(t, payload, segmentSize))
	enc := encoder.SszNetworkEncoder{}
	out := wireArm{name: fmt.Sprintf("segmented/K=%d", len(segs)), known: map[digest]int{}}
	for i, s := range segs {
		var buf bytes.Buffer
		_, err := enc.EncodeGossip(&buf, s)
		require.NoError(t, err)
		b := bytes.Clone(buf.Bytes())
		out.msgs = append(out.msgs, b)
		out.known[digestOf(b)] = i
		out.total += len(b)
		if len(b) > out.max {
			out.max = len(b)
		}
	}
	return out
}

// codedArm is segmentedArm with Reed-Solomon parity: K systematic plus parity segments on
// the wire, any K completing a node. Encoded through the same gossip encoder so the wire
// byte basis (snappy-compressed) matches the plain segmented arm.
func codedArm(t *testing.T, payload []byte, segmentSize, parity int, sk bls.SecretKey, slot primitives.Slot) wireArm {
	t.Helper()
	h, err := segments.HasherByID(segments.HashSHA256)
	require.NoError(t, err)
	msgs, err := segments.BuildCodedSegmentMessages(payload, segmentSize, parity, h)
	require.NoError(t, err)

	enc := encoder.SszNetworkEncoder{}
	k := int(msgs[0].Descriptor.Required()) // lint:ignore uintcast -- bounded by MaxSegments via Validate.
	out := wireArm{name: fmt.Sprintf("rs/K=%d/N=%d", k, len(msgs)), known: map[digest]int{}, need: k}
	wire := make([]*ethpb.ExecutionPayloadSegment, len(msgs))
	for i, m := range msgs {
		raw, err := m.Marshal()
		require.NoError(t, err)
		wire[i] = &ethpb.ExecutionPayloadSegment{Segment: raw}
	}
	applyStructuredPrefixes(wire, slot, msgs[0].Descriptor.GroupID(h))
	for i, m := range wire {
		var buf bytes.Buffer
		_, err = enc.EncodeGossip(&buf, m)
		require.NoError(t, err)
		b := bytes.Clone(buf.Bytes())
		out.msgs = append(out.msgs, b)
		out.known[digestOf(b)] = i
		out.total += len(b)
		if len(b) > out.max {
			out.max = len(b)
		}
	}
	return out
}

// runLine publishes one arm across a line of h+1 nodes and returns the time until the far end
// holds every message, plus the interior relay's time-to-first-forward.
func runLine(t *testing.T, hops int, arm wireArm, warmup []byte) (complete, firstForward time.Duration, drops int, ctl controlCounts, relayRecvBytes int) {
	const wallClock = false
	t.Helper()
	nodes := hops + 1
	tracers := make([]*recordingTracer, nodes)
	for i := range tracers {
		tracers[i] = newRecordingTracer()
	}
	nw, stop := newSimNetwork(t, networkConfig{
		links: uniformLinks(nodes, defaultRate),
		edges: line(nodes),
		perNodeOpts: func(i int) []pubsub.Option {
			return []pubsub.Option{pubsub.WithRawTracer(tracers[i])}
		},
	})
	topics, subs, cancelSubs := joinAndSubscribeAll(t, nw, wallClock)
	// Drain every node except the one being measured. A subscription whose consumer does not
	// read fills its buffer and pubsub then drops messages (UndeliverableMessage), which looks
	// exactly like a slow network. A real node consumes what it is delivered, so the harness
	// must too. Defer order matters: cancel the subscriptions, wait for the drainers to notice,
	// then tear the network down.
	var drainers sync.WaitGroup
	defer stop()
	defer drainers.Wait()
	defer cancelSubs()
	for i := 0; i < nodes-1; i++ {
		drainers.Add(1)
		go func(sub *pubsub.Subscription) {
			defer drainers.Done()
			for {
				if _, err := sub.Next(context.Background()); err != nil {
					return
				}
			}
		}(subs[i])
	}
	settle(wallClock)

	// Optional warm-up: push an unrelated payload the whole length of the chain first, so every
	// hop's QUIC connection has already left slow start before the measured publish. If the
	// per-hop floor is transport ramp-up rather than protocol overhead, this is what removes it.
	if warmup != nil {
		wd := digestOf(warmup)
		require.NoError(t, topics[0].Publish(context.Background(), warmup))
		for {
			msg, err := subs[nodes-1].Next(context.Background())
			require.NoError(t, err)
			if digestOf(msg.Data) == wd {
				break
			}
		}
		settle(wallClock)
	}

	start := time.Now()
	for _, m := range arm.msgs {
		require.NoError(t, topics[0].Publish(context.Background(), m))
	}
	// Drain the far end until it has seen every message once.
	seen := make(map[digest]bool, len(arm.msgs))
	for len(seen) < len(arm.msgs) {
		msg, err := subs[nodes-1].Next(context.Background())
		require.NoError(t, err)
		if _, ok := arm.known[digestOf(msg.Data)]; ok {
			seen[digestOf(msg.Data)] = true
		}
	}
	complete = time.Since(start)
	settle(wallClock)

	// The first interior node is the only place store-and-forward can be observed directly.
	if nodes > 2 {
		if at, _, ok := tracers[1].FirstForward(arm.known); ok {
			firstForward = at.Sub(start)
		}
	}
	for _, tr := range tracers {
		d, u := tr.Losses()
		drops += d + u
	}
	// Control traffic and byte totals at the first relay: the place a per-hop cost that the
	// model does not predict would have to show up.
	relay := tracers[0]
	if nodes > 2 {
		relay = tracers[1]
	}
	ctl = relay.ControlSeen()
	_, _, relayRecvBytes, _ = relay.Counts()
	return complete, firstForward, drops, ctl, relayRecvBytes
}

// TestQ2StoreAndForward measures both arms across several hop counts and fits the marginal cost
// of a hop. The prediction under test is about the slope, not the headline speedup.
func TestQ2StoreAndForward(t *testing.T) {
	// Run the whole grid twice: cold connections, then with every hop pre-warmed. The
	// difference isolates transport ramp-up from protocol cost.
	for _, warm := range []bool{false, true} {
		name := "cold"
		if warm {
			name = "warmed"
		}
		t.Run(name, func(t *testing.T) { runQ2Grid(t, warm) })
	}
}

func runQ2Grid(t *testing.T, warm bool) {
	hopCounts := []int{1, 2, 3, 4, 5}

	type row struct {
		hops      int
		complete  time.Duration
		firstFwd  time.Duration
		ctl       controlCounts
		recvBytes int
		drops     int
	}
	results := map[string][]row{}
	var arms []wireArm
	var warmupMsg []byte

	// Build the arms once, outside any bubble: encoding is a fixed publisher-side cost and
	// including it would put an h-independent term in every measurement.
	synctest.Test(t, func(t *testing.T) {
		params.SetupTestConfigCleanup(t)
		sk, err := bls.RandKey()
		require.NoError(t, err)
		payload := highEntropyPayload(q2PayloadLen, 11)
		if warm {
			// A distinct payload, so the seen-cache does not dedup it against the measured one.
			warmupMsg = wholeArm(t, highEntropyPayload(q2PayloadLen, 99)).msgs[0]
		}
		arms = append(arms, wholeArm(t, payload))
		// K=1 is the negative control: one segment carries the whole payload, so the only
		// difference from the whole arm is segment framing. It must NOT show a pipelining slope.
		for _, size := range []int{q2PayloadLen, 128 << 10, 32 << 10, 8 << 10} {
			arms = append(arms, segmentedArm(t, payload, size, sk, primitives.Slot(2048)))
		}
	})

	for _, arm := range arms {
		for _, hops := range hopCounts {
			synctest.Test(t, func(t *testing.T) {
				params.SetupTestConfigCleanup(t)
				complete, firstFwd, drops, ctl, recvBytes := runLine(t, hops, arm, warmupMsg)
				results[arm.name] = append(results[arm.name], row{hops, complete, firstFwd, ctl, recvBytes, drops})
			})
		}
	}

	// Report. The slope is (T(hmax) - T(hmin)) / (hmax - hmin): the marginal cost of one hop.
	t.Logf("payload %d KiB, link %d Mbps, latency %v one-way",
		q2PayloadLen>>10, defaultRate/simlibp2p.OneMbps, defaultLatency)
	for _, arm := range arms {
		rows := results[arm.name]
		first, last := rows[0], rows[len(rows)-1]
		slope := (last.complete - first.complete) / time.Duration(last.hops-first.hops)
		// Predicted marginal hop: the largest single message's wire time, plus one latency.
		predicted := time.Duration(float64(arm.max*8)/float64(defaultRate)*float64(time.Second)) + defaultLatency
		t.Logf("%-18s msgs=%3d total=%7dB max=%7dB | slope/hop %8v (predicted %8v)",
			arm.name, len(arm.msgs), arm.total, arm.max, slope.Round(time.Microsecond), predicted.Round(time.Microsecond))
		for _, r := range rows {
			flag := ""
			if r.drops > 0 {
				flag = fmt.Sprintf("  *** %d DROPS -- this cell is not a valid timing", r.drops)
			}
			t.Logf("    h=%d complete=%10v firstForward=%10v relayRecv=%8dB idontwant tx/rx=%d/%d%s",
				r.hops, r.complete.Round(time.Microsecond), r.firstFwd.Round(time.Microsecond), r.recvBytes,
				r.ctl.IdontwantSent, r.ctl.IdontwantRecv, flag)
		}
	}

	// Assertions are deliberately weak: this run exists to produce numbers, and the design
	// decision belongs to a human reading them. What is asserted is that the measurement is
	// well-formed -- every cell completed, and no cell lost a message to a queue. Drops are
	// checked after the whole grid is printed rather than aborting on the first one, so a
	// failure shows which cell broke and what the rest did.
	totalDrops := 0
	for _, arm := range arms {
		require.Equal(t, len(hopCounts), len(results[arm.name]), arm.name)
		for _, r := range results[arm.name] {
			require.Equal(t, true, r.complete > 0, fmt.Sprintf("%s h=%d did not complete", arm.name, r.hops))
			totalDrops += r.drops
		}
	}
	require.Equal(t, 0, totalDrops, "queue drops invalidate the affected timings")
}

// TestQ2PerHopResidual asks what the per-hop cost is made of.
//
// The main grid shows a marginal-hop cost that stays well above wmax/R + L once K is large, and
// warming the connections removes only part of it. Two candidates remain: a latency-proportional
// term (the model may undercount how many one-way delays a forward actually costs) or a
// size-independent processing term. Sweeping L separates them: if the residual scales with L it
// is the former, and the model's h*L term is simply wrong.
// Runs on the real clock, not in a synctest bubble, and that is the point.
//
// Under synctest this sweep was unaffordable: a 1 MiB payload over five hops at 20 ms one-way
// did not finish in fifteen minutes while pegging a core, for roughly 2.5 s of virtual time.
// The cost is (timer events) x (goroutines in the bubble), and higher latency means more
// slow-start round trips, so it grows exactly where the measurement is most interesting.
// Tuning QUIC's timers would be the wrong fix -- the round trips are inherent, and changing
// congestion control would change the thing being measured. Leaving the bubble costs the real
// seconds it simulates instead, which here is three orders of magnitude cheaper.
func TestQ2PerHopResidual(t *testing.T) {
	requireSlowTests(t)
	hopCounts := []int{1, 3, 5}
	const segmentSize = 8 << 10
	const payloadLen = 256 << 10

	params.SetupTestConfigCleanup(t)
	sk, err := bls.RandKey()
	require.NoError(t, err)
	arm := segmentedArm(t, highEntropyPayload(payloadLen, 11), segmentSize, sk, primitives.Slot(2048))
	warm := wholeArm(t, highEntropyPayload(payloadLen, 99)).msgs[0]

	transmit := time.Duration(float64(arm.max*8) / float64(defaultRate) * float64(time.Second))
	t.Logf("%s: wmax=%dB, transmission per hop %v", arm.name, arm.max, transmit.Round(time.Microsecond))

	for _, oneWay := range []time.Duration{time.Millisecond, 5 * time.Millisecond, 20 * time.Millisecond} {
		var times []time.Duration
		for _, hops := range hopCounts {
			complete, _, drops := runLineAtLatency(t, hops, arm, warm, oneWay, true)
			require.Equal(t, 0, drops, "queue drops invalidate a timing measurement")
			times = append(times, complete)
		}
		slope := (times[len(times)-1] - times[0]) / time.Duration(hopCounts[len(hopCounts)-1]-hopCounts[0])
		residual := slope - transmit
		t.Logf("L=%-6v slope/hop %9v  residual %9v  = %.1f x L",
			oneWay, slope.Round(time.Microsecond), residual.Round(time.Microsecond),
			float64(residual)/float64(oneWay))
	}
}

// runLineAtLatency is runLine with an explicit one-way latency.
func runLineAtLatency(t *testing.T, hops int, arm wireArm, warmup []byte, oneWay time.Duration, wallClock bool) (complete time.Duration, firstForward time.Duration, drops int) {
	t.Helper()
	nodes := hops + 1
	tracers := make([]*recordingTracer, nodes)
	for i := range tracers {
		tracers[i] = newRecordingTracer()
	}
	nw, stop := newSimNetwork(t, networkConfig{
		links:     uniformLinks(nodes, defaultRate),
		edges:     line(nodes),
		latency:   simnet.StaticLatency(oneWay),
		wallClock: wallClock,
		perNodeOpts: func(i int) []pubsub.Option {
			return []pubsub.Option{pubsub.WithRawTracer(tracers[i])}
		},
	})
	topics, subs, cancelSubs := joinAndSubscribeAll(t, nw, wallClock)
	var drainers sync.WaitGroup
	defer stop()
	defer drainers.Wait()
	defer cancelSubs()
	for i := 0; i < nodes-1; i++ {
		drainers.Add(1)
		go func(sub *pubsub.Subscription) {
			defer drainers.Done()
			for {
				if _, err := sub.Next(context.Background()); err != nil {
					return
				}
			}
		}(subs[i])
	}
	settle(wallClock)

	// Every wait is bounded. An unbounded Next on a message that never arrives turns a harness
	// bug into a twenty-minute hang that reports nothing; a deadline names the failure.
	ctx, cancelCtx := context.WithTimeout(context.Background(), networkOpTimeout)
	defer cancelCtx()

	wd := digestOf(warmup)
	require.NoError(t, topics[0].Publish(ctx, warmup))
	for {
		msg, err := subs[nodes-1].Next(ctx)
		require.NoError(t, err, "warm-up never reached the far end: mesh probably did not form")
		if digestOf(msg.Data) == wd {
			break
		}
	}
	settle(wallClock)

	start := time.Now()
	for _, m := range arm.msgs {
		require.NoError(t, topics[0].Publish(ctx, m))
	}
	seen := make(map[digest]bool, len(arm.msgs))
	for len(seen) < len(arm.msgs) {
		msg, err := subs[nodes-1].Next(ctx)
		require.NoError(t, err, fmt.Sprintf("only %d of %d messages arrived", len(seen), len(arm.msgs)))
		if _, ok := arm.known[digestOf(msg.Data)]; ok {
			seen[digestOf(msg.Data)] = true
		}
	}
	complete = time.Since(start)
	settle(wallClock)
	if nodes > 2 {
		if at, _, ok := tracers[1].FirstForward(arm.known); ok {
			firstForward = at.Sub(start)
		}
	}
	for _, tr := range tracers {
		d, u := tr.Losses()
		drops += d + u
	}
	return complete, firstForward, drops
}

// TestSegmentationOverheadVsCompressibility answers "at what compressibility?", which every
// overhead figure needs and none of ours carried.
//
// Segments are snappy-compressed one at a time, so segmentation forfeits whatever redundancy
// spanned a segment boundary. On incompressible input there is nothing to forfeit and the
// overhead is just framing; on compressible input the forfeit dominates. Reporting a single
// number without saying which end of that range it came from is how +0.73% and +21% both got
// quoted for the same feature.
func TestSegmentationOverheadVsCompressibility(t *testing.T) {
	const n = 1 << 20
	enc := encoder.SszNetworkEncoder{}
	encodedLen := func(b []byte) int {
		var buf bytes.Buffer
		_, err := enc.EncodeGossip(&buf, &ethpb.ExecutionPayloadSegment{Segment: b})
		require.NoError(t, err)
		return buf.Len()
	}

	// Two payloads only: the conservative case and the real one. Sweeping up to 20:1 produced a
	// "+21% overhead" figure that described nothing on any network -- a pathological synthetic,
	// not a payload.
	t.Logf("%-22s %-10s %8s %8s %8s", "payload", "ratio", "K=8", "K=32", "K=128")
	for _, tc := range []struct {
		name   string
		padded float64
	}{
		{"incompressible", 0.0},
		{"mainnet-like", mainnetPaddedFraction},
	} {
		payload := mixedEntropyPayload(n, tc.padded, 5)
		whole := encodedLen(payload)

		var cells []string
		for _, k := range []int{8, 32, 128} {
			size := n / k
			total := 0
			for off := 0; off < n; off += size {
				total += encodedLen(payload[off:min(off+size, n)])
			}
			cells = append(cells, fmt.Sprintf("%7.2f%%", 100*float64(total-whole)/float64(whole)))
		}
		t.Logf("%-22s %6.2f:1   %s %s %s  whole=%dB",
			tc.name, float64(n)/float64(whole), cells[0], cells[1], cells[2], whole)
	}

	// Pin the calibration itself: if mainnetLikePayload drifts off 1.40:1 every overhead figure
	// quoted from it silently becomes about a different network.
	mainnet := mainnetLikePayload(n, 5)
	ratio := float64(n) / float64(encodedLen(mainnet))
	require.Equal(t, true, ratio > 1.37 && ratio < 1.43,
		fmt.Sprintf("mainnet-like payload should compress about 1.40:1, got %.3f:1", ratio))

	// And the mechanism: per-segment compression forfeits cross-segment redundancy, so the
	// realistic payload must pay more than the incompressible one.
	segTotal := func(b []byte, k int) int {
		size := len(b) / k
		total := 0
		for off := 0; off < len(b); off += size {
			total += encodedLen(b[off:min(off+size, len(b))])
		}
		return total
	}
	inc := mixedEntropyPayload(n, 0.0, 5)
	incOverhead := float64(segTotal(inc, 32)-encodedLen(inc)) / float64(encodedLen(inc))
	mainOverhead := float64(segTotal(mainnet, 32)-encodedLen(mainnet)) / float64(encodedLen(mainnet))
	require.Equal(t, true, mainOverhead > incOverhead,
		"a compressible payload must pay more for segmentation than an incompressible one")
}

// TestQ2ClockComparison runs one identical configuration both ways.
//
// This exists because the per-hop residual measured under synctest (a coefficient near 3.7 on
// one-way latency) and the one measured on the real clock (near 1.0, which is what the model
// predicts) came from different payload and segment sizes. Clock and configuration were
// confounded, so neither number could be trusted. If the virtual clock inflates the marginal
// hop, every slope in TestQ2StoreAndForward is inflated with it and the residual finding is an
// artifact rather than a property of the transport.
//
// Same payload, same segment size, same hop counts, same warm-up. Only the clock differs.
func TestQ2ClockComparison(t *testing.T) {
	requireSlowTests(t)
	params.SetupTestConfigCleanup(t)
	hopCounts := []int{1, 3, 5}
	const segmentSize = 32 << 10
	const oneWay = 5 * time.Millisecond

	sk, err := bls.RandKey()
	require.NoError(t, err)
	arm := segmentedArm(t, highEntropyPayload(q2PayloadLen, 11), segmentSize, sk, primitives.Slot(2048))
	warm := wholeArm(t, highEntropyPayload(q2PayloadLen, 99)).msgs[0]
	transmit := time.Duration(float64(arm.max*8) / float64(defaultRate) * float64(time.Second))
	predicted := transmit + oneWay
	t.Logf("%s wmax=%dB: predicted marginal hop %v (transmission %v + L %v)",
		arm.name, arm.max, predicted.Round(time.Microsecond), transmit.Round(time.Microsecond), oneWay)

	measure := func(wallClock bool) time.Duration {
		var times []time.Duration
		for _, hops := range hopCounts {
			if wallClock {
				complete, _, drops := runLineAtLatency(t, hops, arm, warm, oneWay, true)
				require.Equal(t, 0, drops, "queue drops invalidate a timing measurement")
				times = append(times, complete)
				continue
			}
			synctest.Test(t, func(t *testing.T) {
				complete, _, drops := runLineAtLatency(t, hops, arm, warm, oneWay, false)
				require.Equal(t, 0, drops, "queue drops invalidate a timing measurement")
				times = append(times, complete)
			})
		}
		for i, h := range hopCounts {
			t.Logf("    wallClock=%-5v h=%d complete=%v", wallClock, h, times[i].Round(time.Microsecond))
		}
		return (times[len(times)-1] - times[0]) / time.Duration(hopCounts[len(hopCounts)-1]-hopCounts[0])
	}

	virtual := measure(false)
	wall := measure(true)
	t.Logf("marginal hop: virtual clock %v, real clock %v, predicted %v",
		virtual.Round(time.Microsecond), wall.Round(time.Microsecond), predicted.Round(time.Microsecond))
	t.Logf("virtual/real = %.2f, virtual/predicted = %.2f, real/predicted = %.2f",
		float64(virtual)/float64(wall), float64(virtual)/float64(predicted), float64(wall)/float64(predicted))
}

// TestSegmentSizeSpread checks whether compressibility is spread evenly or clustered, and what
// that does to the number the timing model actually depends on.
//
// The marginal cost of a hop is wmax/R, where wmax is the largest compressed segment. A payload
// whose compressibility is uniform gives every segment nearly the same size, so wmax is barely
// above the mean. A real block clusters -- large ABI-padded calldata in a few transactions --
// which raises wmax while leaving the total unchanged. If the gap is significant, a uniform
// generator makes segmentation look better than it is.
func TestSegmentSizeSpread(t *testing.T) {
	const n = 1 << 20
	enc := encoder.SszNetworkEncoder{}
	encodedLen := func(b []byte) int {
		var buf bytes.Buffer
		_, err := enc.EncodeGossip(&buf, &ethpb.ExecutionPayloadSegment{Segment: b})
		require.NoError(t, err)
		return buf.Len()
	}
	stats := func(payload []byte, k int) (total, mean, wmax int) {
		size := n / k
		for off := 0; off < n; off += size {
			l := encodedLen(payload[off:min(off+size, n)])
			total += l
			if l > wmax {
				wmax = l
			}
		}
		return total, total / k, wmax
	}

	for _, k := range []int{8, 32} {
		t.Logf("K=%d", k)
		for _, tc := range []struct {
			name    string
			payload []byte
		}{
			{"uniform (i.i.d. words)", mainnetLikePayload(n, 5)},
			{"clustered, runs of ~8", clusteredEntropyPayload(n, mainnetPaddedFraction, 8, 5)},
			{"clustered, runs of ~64", clusteredEntropyPayload(n, mainnetPaddedFraction, 64, 5)},
			{"clustered, runs of ~512", clusteredEntropyPayload(n, mainnetPaddedFraction, 512, 5)},
		} {
			whole := encodedLen(tc.payload)
			total, mean, wmax := stats(tc.payload, k)
			t.Logf("  %-24s ratio %5.2f:1  mean %7dB  wmax %7dB  wmax/mean %5.2f  seg total %+.2f%%",
				tc.name, float64(n)/float64(whole), mean, wmax, float64(wmax)/float64(mean),
				100*float64(total-whole)/float64(whole))
		}
	}

	// The marginal-hop cost scales with wmax, so a generator that holds wmax near the mean
	// understates it. Pin the direction so this cannot regress silently.
	_, uMean, uMax := stats(mainnetLikePayload(n, 5), 32)
	_, cMean, cMax := stats(clusteredEntropyPayload(n, mainnetPaddedFraction, 512, 5), 32)
	t.Logf("wmax/mean: uniform %.2f, clustered %.2f", float64(uMax)/float64(uMean), float64(cMax)/float64(cMean))
	require.Equal(t, true, float64(cMax)/float64(cMean) > float64(uMax)/float64(uMean),
		"clustered compressibility must widen the segment-size spread")
}
