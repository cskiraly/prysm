package segmentintegrationtest

// Link-level transport warm-up: grow every QUIC connection's congestion window before the
// measured publish, without touching anything above the transport.
//
// Why this exists. quic-go starts a connection with a 40 KiB congestion window
// (initialCongestionWindow=32 packets x InitialPacketSize=1280), so a sender that puts more than
// that on a link in one burst pays slow start. Our figures start their clock at publish, on
// connections that have carried only control traffic -- which is application-limited and so does
// not grow the window. 40 KiB is 3.2x what RFC 9002 recommends, which is why the threshold is
// worth being able to move.
//
// Why a raw stream rather than a warm-up topic. A decoy publish warms the transport *and* fills
// the seen cache, message cache and IDONTWANT state, and the second effect can outweigh the first
// -- measured at K=128, where warming made the arm 62% slower. A stream on a dummy protocol is
// invisible to gossipsub: the pubsub tracers never fire, so no counter reset is needed and no
// protocol state is disturbed. Coverage is exact rather than a mesh subset, because the congestion
// window is a property of the sender, so every node warming toward all of its peers warms every
// connection in both directions.
//
// What this cannot do. The congestion window is internal to quic-go, so this warms *blind*: there
// is no way to read back the window achieved. warmupStats therefore reports the second-half
// throughput of each transfer, which is the best available proxy -- a flow that finishes near line
// rate has a window at least the bandwidth-delay product. The quic-go override (option B) sets the
// window to a known value instead, and the two are meant to check each other.

import (
	"context"
	"io"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
)

// warmupProtocol is deliberately not an Ethereum protocol id: nothing should ever route this.
const warmupProtocol = protocol.ID("/prysm/harness/transport-warmup/1.0.0")

// warmupChunk is one initial congestion window, so the first chunk is the one that fits without
// waiting for an acknowledgement and every later one measures growth.
const warmupChunk = 40 << 10

// warmupDrain is how long to let the links empty after warming, in virtual time. Generous on
// purpose: it costs nothing, and an incompletely drained warm-up is indistinguishable from a
// warm-up that made things worse.
const warmupDrain = 5 * time.Second

// applyInitialCWND sets QUIC's initial congestion window, in packets, from
// SEGMENT_INITIAL_CWND_PACKETS. Unset leaves quic-go's own value.
//
// This is the alternative to warming: instead of sending traffic to grow the window past its
// starting point, start it where you want. It sends nothing, so unlike every traffic-based
// warm-up it perturbs no other layer -- no protocol state, no queues, no RTT estimates.
//
// It sets the *default* rather than a Config field because go-libp2p's quicreuse builds its own
// quic.Config from an unexported package variable, so the field is unreachable from here. Needs
// the quic-go fork (a local `replace github.com/quic-go/quic-go => ...` in go.mod, deliberately
// not committed) and the `quicfork` build tag; without the tag the sweep fails loudly instead of
// silently measuring the default window. The tagged implementations live in
// quicwindow_fork_test.go / quicwindow_default_test.go.
//
// Reference points, at quic-go's 1280-byte packet size: 10 packets = 12.5 KiB is what RFC 9002
// §7.2 recommends, 32 = 40 KiB is quic-go's default, and the per-connection fair-share
// bandwidth-delay product at D=8 on a 50 Mbps uplink is ~38 KiB -- so the default sits almost
// exactly on the BDP for this workload, apparently by coincidence.
func applyInitialCWND(t *testing.T) {
	v := os.Getenv("SEGMENT_INITIAL_CWND_PACKETS")
	if v == "" {
		return
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		t.Fatalf("bad SEGMENT_INITIAL_CWND_PACKETS %q", v)
	}
	setInitialCWND(t, uint32(n))
	t.Logf("      initial congestion window: %d packets (%d KiB at 1280 B/packet)", n, n*1280>>10)
}

// warmupBytes reports how many bytes to push per connection, 0 when disabled.
//
// Sizing: in slow start the window grows by roughly one byte per byte acknowledged, so the target
// is about the window wanted. The bandwidth-delay product is the point past which more buys
// nothing -- 50 Mbps x 50 ms RTT is ~305 KiB -- so 320 KiB is a sensible default once enabled.
func warmupBytes(t *testing.T) int {
	v := os.Getenv("SEGMENT_WARMUP_BYTES")
	if v == "" {
		return 0
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 0 {
		t.Fatalf("bad SEGMENT_WARMUP_BYTES %q", v)
	}
	return n
}

// warmupStats is the self-check. Without it a warm-up that self-congested and backed off looks
// exactly like one that worked.
type warmupStats struct {
	links      int
	bytes      int
	elapsed    time.Duration
	firstHalf  time.Duration // summed across links
	secondHalf time.Duration
}

// secondHalfRate returns the mean throughput of the back half of the transfers, in bits/sec.
//
// This is the number to read. If it approaches the link rate the windows grew; if it sits near the
// first-half rate the warm-up achieved little; if it is *below* it, the warm-up congested the
// network and the windows may have backed off past where they started.
func (w warmupStats) secondHalfRate() float64 {
	if w.secondHalf <= 0 || w.links == 0 {
		return 0
	}
	return float64(w.bytes/2*8) * float64(w.links) / w.secondHalf.Seconds()
}

func (w warmupStats) firstHalfRate() float64 {
	if w.firstHalf <= 0 || w.links == 0 {
		return 0
	}
	return float64(w.bytes/2*8) * float64(w.links) / w.firstHalf.Seconds()
}

// registerWarmupHandlers installs a read-and-discard handler on every host.
func registerWarmupHandlers(nw *simNetwork) {
	for _, h := range nw.Hosts {
		h.SetStreamHandler(warmupProtocol, func(s network.Stream) {
			_, _ = io.Copy(io.Discard, s)
			_ = s.Close()
		})
	}
}

// warmConnections pushes bytes down every connection, one peer at a time per node.
//
// Sequential per node on purpose. Warming all of a node's peers at once puts 20 flows on one
// uplink, which congests the link, drops packets and backs the windows off -- the warm-up would
// then leave the network *worse* than cold. One flow at a time still saturates the uplink, which
// is what grows the window fastest, and peers are visited in a per-node rotation so arrivals
// spread across receivers instead of converging on whoever sorts first.
func warmConnections(t *testing.T, ctx context.Context, nw *simNetwork, perLink int) warmupStats {
	t.Helper()
	if perLink <= 0 {
		return warmupStats{}
	}
	registerWarmupHandlers(nw)

	buf := make([]byte, warmupChunk)
	for i := range buf {
		buf[i] = byte(i * 31)
	}

	var mu sync.Mutex
	stats := warmupStats{bytes: perLink}
	start := time.Now()

	var wg sync.WaitGroup
	for i, h := range nw.Hosts {
		wg.Add(1)
		go func(i int, h hostWithNetwork) {
			defer wg.Done()
			peers := h.Network().Peers()
			for k := range peers {
				// Rotate the starting point per node so every node does not begin with the
				// same peer, which would pile every first flow onto one receiver.
				p := peers[(k+i)%len(peers)]
				first, second, ok := warmOnePeer(ctx, h, p, buf, perLink)
				if !ok {
					continue
				}
				mu.Lock()
				stats.links++
				stats.firstHalf += first
				stats.secondHalf += second
				mu.Unlock()
			}
		}(i, h)
	}
	wg.Wait()
	stats.elapsed = time.Since(start)
	return stats
}

// hostWithNetwork is the slice of host.Host this file needs, named so the loop above reads.
type hostWithNetwork interface {
	Network() network.Network
	NewStream(context.Context, peer.ID, ...protocol.ID) (network.Stream, error)
	SetStreamHandler(protocol.ID, network.StreamHandler)
}

// warmOnePeer writes perLink bytes to one peer, timing each half of the transfer.
//
// Writes block once the congestion and flow-control windows are full, which is what makes the
// halves comparable: the second half is faster exactly to the extent the window grew.
func warmOnePeer(ctx context.Context, h hostWithNetwork, p peer.ID, buf []byte, perLink int) (first, second time.Duration, ok bool) {
	s, err := h.NewStream(ctx, p, warmupProtocol)
	if err != nil {
		// A peer that will not accept the stream is not a failure of the cell: it warms
		// nothing and is counted by its absence from links.
		return 0, 0, false
	}
	defer func() { _ = s.Close() }()

	half := perLink / 2
	t0 := time.Now()
	if !writeN(s, buf, half) {
		return 0, 0, false
	}
	t1 := time.Now()
	if !writeN(s, buf, perLink-half) {
		return 0, 0, false
	}
	return t1.Sub(t0), time.Since(t1), true
}

func writeN(w io.Writer, buf []byte, n int) bool {
	for n > 0 {
		c := min(n, len(buf))
		if _, err := w.Write(buf[:c]); err != nil {
			return false
		}
		n -= c
	}
	return true
}
