package segmentintegrationtest

// The ecrs arm: ethp2p's erasure-coded broadcast (ECBroadcast) with its Reed-Solomon strategy, run
// unmodified (vendored in ./ethp2p) on the harness's own network through the ecbridge adapter. The
// cell is the tier-4 cell in every other respect: the same simnet links, latency model, bandwidth
// classes, connection graph and message schedule, the same driver, the same report line. Only the
// diffusion protocol changes: every node runs ethp2p's engine on every one of its connections, and
// no gossipsub topic is joined.
//
// Settings are those of ethp2p's benchmarks (sim/configs/500n-2mb-rs.yaml at 78452f02c9):
// RS-ChunkLen with 16 KiB chunks and as many parity as data chunks, ForwardMultiplier 4, routing
// bitmaps on from the first chunk (threshold 0). SEGMENT_ECRS_PAYLOAD picks what is broadcast:
// "compressed" (the default) is the whole message as gossip carries it, SSZ then snappy, the same
// bytes the segmented arms code; "random" is SEGMENT_PAYLOAD_BYTES of random bytes, as ethp2p's own
// simulator broadcasts. A node completes when the engine delivers the decoded message; under
// synctest decoding takes no virtual time, so that is the moment it holds k chunks.
//
// Bytes are counted by the adapter above the libp2p stream, as the gossipsub arms' tracer counts
// above pubsub's: rx/node is the chunk payload bytes received, ctrl rx/node everything else read on
// the engine's streams (selectors, session opens with their preambles, routing bitmaps, chunk
// headers, multistream lines).

import (
	"context"
	"crypto/rand"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/OffchainLabs/prysm/v7/beacon-chain/p2p/segmentintegrationtest/ecbridge"
	"github.com/OffchainLabs/prysm/v7/beacon-chain/p2p/segmentintegrationtest/ethp2p/broadcast"
	"github.com/OffchainLabs/prysm/v7/beacon-chain/p2p/segmentintegrationtest/ethp2p/broadcast/rs"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	simlibp2p "github.com/libp2p/go-libp2p/x/simlibp2p"
)

type ecrsChannel = broadcast.Channel[*rs.ChunkIdent, *rs.Bitmap, *rs.Preamble]

// ecrsVariant is one ecrs cell: the payload schedule and, once set up, one engine per node.
type ecrsVariant struct {
	rate    int
	oneWay  time.Duration
	pub     int
	msgNum  int
	mode    string
	build   func(t *testing.T, m int) (whole, segs wireArm, unit int)
	payload []byte
	// payloadLen is the cell's payload size (SEGMENT_PAYLOAD_BYTES), the random mode's length.
	payloadLen int

	nw      *simNetwork
	nodes   []*ecbridge.Node
	engines []*broadcast.Engine
	chans   []*ecrsChannel
	recv    []chan broadcast.FullMessage
	links   int
}

func newEcrsVariant(t *testing.T, c q6Cell, rate int, oneWay time.Duration) *ecrsVariant {
	t.Helper()
	mode := os.Getenv("SEGMENT_ECRS_PAYLOAD")
	switch mode {
	case "":
		mode = "compressed"
	case "compressed", "random":
	default:
		t.Fatalf("bad SEGMENT_ECRS_PAYLOAD %q (compressed or random)", mode)
	}
	v := &ecrsVariant{rate: rate, oneWay: oneWay, mode: mode, build: c.build, msgNum: 1, payloadLen: c.payloadLen}
	v.payload = v.payloadFor(t, 1)
	return v
}

// payloadFor returns message m's bytes: the whole message as gossip carries it, or random bytes of
// the configured payload size.
func (v *ecrsVariant) payloadFor(t *testing.T, m int) []byte {
	t.Helper()
	if v.mode == "random" {
		b := make([]byte, v.payloadLen)
		_, _ = rand.Read(b)
		return b
	}
	whole, _, _ := v.build(t, m)
	return whole.msgs[0]
}

func (v *ecrsVariant) name() string { return "ecrs" }

// pubsubOpts adds nothing: the routers run, as in every cell, but join no topic.
func (v *ecrsVariant) pubsubOpts(*testing.T, int) []pubsub.Option { return nil }

func (v *ecrsVariant) setFirstPublisher(pub int) { v.pub = pub }

func (v *ecrsVariant) setup(t *testing.T, nw *simNetwork, _ diffusionParams) diffusionRun {
	t.Helper()
	v.nw = nw
	n := nw.Len()
	scheme := rs.NewScheme(rs.Config{ChunkLen: 16384, ForwardMultiplier: 4, BitmapThreshold: 0})
	v.nodes = make([]*ecbridge.Node, n)
	v.engines = make([]*broadcast.Engine, n)
	v.chans = make([]*ecrsChannel, n)
	v.recv = make([]chan broadcast.FullMessage, n)
	for i := range n {
		v.nodes[i] = ecbridge.NewNode(nw.Hosts[i])
		v.engines[i] = broadcast.NewEngine(broadcast.EngineConfig{})
		v.chans[i] = broadcast.AttachChannel(v.engines[i], "payload", scheme)
		v.recv[i] = make(chan broadcast.FullMessage, 16)
		if err := v.chans[i].Subscribe(v.recv[i]); err != nil {
			t.Fatal(err)
		}
	}
	for i := range n {
		for _, p := range nw.Hosts[i].Network().Peers() {
			v.engines[i].NotifyPeerConnected(v.nodes[i].Conn(p))
			v.links++
		}
	}
	return &ecrsRun{v: v}
}

// nextMessage switches to message m, published by pub; sessions of earlier messages stay until
// their peers are done, as the engine disposes them.
func (v *ecrsVariant) nextMessage(t *testing.T, m, pub int) {
	v.msgNum, v.pub = m, pub
	v.payload = v.payloadFor(t, m)
}

func (v *ecrsVariant) onTimeout(*testing.T, int, int) {}

func (v *ecrsVariant) report(t *testing.T, s diffusionStats) {
	var data, ctrl, opened, accepted, cw, cr, oe, refused, ow, ww int64
	for _, nd := range v.nodes {
		c := &nd.Count
		data += c.Data.Load()
		ctrl += c.Control.Load()
		opened, accepted, cw, cr, oe, refused = opened+c.Opened.Load(), accepted+c.Accepted.Load(),
			cw+c.CancelWrites.Load(), cr+c.CancelReads.Load(), oe+c.OpenErrors.Load(), refused+c.Refused.Load()
		ow, ww = ow+c.OpenWait.Load(), ww+c.WriteWait.Load()
	}
	t.Logf("      ecrs sender waits: open %v per stream, write %v per stream", time.Duration(ow/max(opened, 1)), time.Duration(ww/max(opened, 1)))
	t.Logf("      ecrs streams: opened %d, accepted %d, cancelled by sender %d, by receiver %d, open errors %d, refused %d",
		opened, accepted, cw, cr, oe, refused)
	floor := time.Duration(float64(8*len(v.payload)*8) / float64(v.rate) * float64(time.Second))
	t.Logf("      ecrs: ethp2p RS-ChunkLen 16384, fm 4, bitmaps from 0 %%; payload %s, %d B; %d engine links",
		v.mode, len(v.payload), v.links)
	t.Logf("%4dMbps L=%-5v n=%-3d %-10s %3d/%d  virtual %9v (D-copy floor %8v)  %s  dups %6d  IDONTWANT tx %6d  dup-per-idw %5.2f  publisherTx %8dB  rx/node %8dB  ctrl rx/node %6dB  wall %v  [degree %d, mesh %s]",
		v.rate/simlibp2p.OneMbps, v.oneWay, s.n, "ecrs", s.completed, s.n-1,
		s.virt.Round(time.Millisecond), floor.Round(time.Millisecond),
		completionStats(s.compTimes, s.n-1, s.deadline),
		0, 0, 0.0, 0, data/int64(s.n), ctrl/int64(s.n), s.wall.Round(time.Millisecond),
		connectivityDegree(t, s.n), s.meshSummary)
}

// ecrsRun is the live state of an ecrs cell.
type ecrsRun struct {
	v *ecrsVariant
}

// settle lets every engine finish its handshakes and channel subscriptions, which run in the
// background after NotifyPeerConnected; a publish before then reaches only part of the network.
func (r *ecrsRun) settle(*testing.T, []*recordingTracer) string {
	time.Sleep(ecrsSettle)
	return fmt.Sprintf("none (ethp2p engines on %d links)", r.v.links)
}

// ecrsSettle is far above any handshake: two one-way trips at the model's largest latency.
const ecrsSettle = 3 * time.Second

func (r *ecrsRun) cancel() {
	for i := range r.v.chans {
		r.v.chans[i].Stop()
		_ = r.v.engines[i].Close()
	}
}

// watch waits, on every node but the publisher, for the engine to deliver the current message.
func (r *ecrsRun) watch(ctx context.Context, wg *sync.WaitGroup, start time.Time, signal func(node int, at time.Duration)) {
	want := broadcast.MessageID(fmt.Sprintf("m%d", r.v.msgNum))
	for i := range r.v.recv {
		if i == r.v.pub {
			continue
		}
		ch := r.v.recv[i]
		wg.Go(func() {
			for {
				select {
				case m, ok := <-ch:
					if !ok {
						return
					}
					if m.MessageID == want {
						signal(i, time.Since(start))
						return
					}
				case <-ctx.Done():
					return
				}
			}
		})
	}
}

// publish starts the message window's byte counters and publishes from the publisher's engine.
func (r *ecrsRun) publish(context.Context, *testing.T) error {
	for _, nd := range r.v.nodes {
		nd.Count.Reset()
	}
	return r.v.chans[r.v.pub].Publish(broadcast.MessageID(fmt.Sprintf("m%d", r.v.msgNum)), r.v.payload)
}
