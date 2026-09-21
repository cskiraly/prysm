package gossipsim

// Link-model calibration.
//
// Every latency figure the RowDAS harness has produced rests on the assumption that a simulated
// "50 Mbps, 25 ms" node behaves like one. Nothing had checked it. These three measurements do,
// in the order that isolates each layer: one raw simulated flow, so the rate link is characterised
// without QUIC in the way; one QUIC/libp2p flow, which adds congestion control, ACKs and
// retransmission on top; then the fanout the experiments actually run -- one uplink shared by
// several concurrent receivers, which is where FQ-CoDel's per-flow queues and the per-node (not
// per-edge) link model both matter.
//
// What each reports, separately, because conflating them is how the earlier write-ups went
// wrong: application goodput; bytes on the (simulated) wire; packets lost. Wire bytes and losses
// for QUIC come from quic-go's qlog, which libp2p's transport enables per host, so no patch to
// simnet is needed -- its CoDel drop path is a `// TODO add stats`.
//
// They log rather than assert tight bounds, since the point is to *know* the operating point.
// The loose assertions that remain are the ones whose failure would mean the model is not what
// the experiments assume at all.

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/OffchainLabs/prysm/v7/testing/require"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	simlibp2p "github.com/libp2p/go-libp2p/x/simlibp2p"
	"github.com/marcopolo/simnet"
)

const (
	calibrationProtocol = protocol.ID("/gossipsim/calibration/1.0.0")
)

func mbps(bytes int, d time.Duration) float64 {
	if d <= 0 {
		return 0
	}
	return float64(bytes) * 8 / d.Seconds() / 1e6
}

// rawFlow pushes total bytes from one simnet endpoint to another, paced at paceBps (0 = as fast
// as the sender can enqueue, which is a flood), and reports what the receiver saw.
func rawFlow(t *testing.T, rateBps, packetSize, total, paceBps int) (goodputMbps float64, sent, rcvd simnet.ConnStats) {
	t.Helper()

	sim := &simnet.Simnet{LatencyFunc: simnet.StaticLatency(DefaultLatency)}
	link := simnet.NodeBiDiLinkSettings{
		Uplink:   simnet.LinkSettings{BitsPerSecond: rateBps, BurstWindow: RealClockBurstWindow},
		Downlink: simnet.LinkSettings{BitsPerSecond: rateBps, BurstWindow: RealClockBurstWindow},
	}
	srcAddr := &net.UDPAddr{IP: net.IPv4(10, 0, 0, 1), Port: 4000}
	dstAddr := &net.UDPAddr{IP: net.IPv4(10, 0, 0, 2), Port: 4000}
	src := sim.NewEndpoint(srcAddr, link)
	dst := sim.NewEndpoint(dstAddr, link)
	sim.Start()
	defer sim.Close()

	var first, last time.Time
	var got int
	done := make(chan struct{})
	go func() {
		defer close(done)
		buf := make([]byte, 2048)
		for got < total {
			// Quiet for a while after the first packet means the flow is over: the link drained
			// or dropped the rest.
			require.NoError(t, dst.SetReadDeadline(time.Now().Add(time.Second)))
			n, _, err := dst.ReadFrom(buf)
			if err != nil {
				return
			}
			now := time.Now()
			if first.IsZero() {
				first = now
			}
			last = now
			got += n
		}
	}()

	pkt := make([]byte, packetSize)
	var interval time.Duration
	if paceBps > 0 {
		interval = time.Duration(float64(packetSize*8) / float64(paceBps) * float64(time.Second))
	}
	next := time.Now()
	for off := 0; off < total; off += packetSize {
		if interval > 0 {
			time.Sleep(time.Until(next))
			next = next.Add(interval)
		}
		_, err := src.WriteTo(pkt, dstAddr)
		require.NoError(t, err)
	}
	<-done

	return mbps(got, last.Sub(first)), src.Stats(), dst.Stats()
}

// TestCalibrationRawFlow characterises the rate link alone, across configured rates and packet
// sizes. What the first run of it found: at 50 Mbps a paced 1400 B flow gets ~21 Mbps and loses
// packets, because the link driver sends one packet per timer wake-up with a token bucket whose
// burst is one MTU (simlink.go processQueue, ratelink.go) -- so on the real clock the deliverable
// rate is one packet per (timer period + overshoot), whatever the configured bits per second. The
// sweep is how that shows: goodput tracks the label at low rates and flattens into a
// packets-per-second ceiling at high ones, and the ceiling scales with packet size.
//
// Logged, not asserted, because the point is to know the operating point. The one assertion is
// that goodput never exceeds the label, which would mean the accounting is wrong.
func TestCalibrationRawFlow(t *testing.T) {
	if testing.Short() {
		t.Skip("measurement")
	}
	const total = 2 << 20

	t.Logf("%9s %6s %6s | %10s %8s", "rate", "pkt B", "pacing", "goodput", "dropped")
	for _, c := range []struct {
		rateMbps, packet int
		pace             float64 // fraction of the rate; 0 floods
	}{
		{10, 1400, 0.9}, {20, 1400, 0.9}, {50, 1400, 0.9}, {100, 1400, 0.9},
		{50, 700, 0.9}, {50, 1400, 0}, {100, 1400, 0},
	} {
		rate := c.rateMbps * simlibp2p.OneMbps
		goodput, sent, rcvd := rawFlow(t, rate, c.packet, total, int(float64(rate)*c.pace))
		pacing := "flood"
		if c.pace > 0 {
			pacing = "90%"
		}
		t.Logf("%4d Mbps %6d %6s | %6.1f Mbps %8d", c.rateMbps, c.packet, pacing, goodput, sent.PacketsSent-rcvd.PacketsRcvd)
		require.Equal(t, true, goodput < float64(c.rateMbps)*1.05, "goodput above the configured rate: %.1f", goodput)
	}
}

// qlogStats is what the calibration reads out of quic-go's qlog: packets and bytes sent on the
// wire, and packets declared lost by loss recovery -- retransmission is the sender's response to
// the latter, so lost packets are the number to compare against application bytes.
type qlogStats struct {
	sentPackets, sentBytes, lostPackets int
}

// parseQlogDir totals the qlog traces in dir. quic-go writes JSON-SEQ (RFC 7464): records
// prefixed with 0x1E. Only the two event names the calibration needs are read; everything else
// is skipped without decoding it further.
func parseQlogDir(t *testing.T, dir, perspective string) qlogStats {
	t.Helper()

	var stats qlogStats
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), "_"+perspective+".sqlog") {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		require.NoError(t, err)
		for _, rec := range bytes.Split(raw, []byte{0x1e}) {
			rec = bytes.TrimSpace(rec)
			if len(rec) == 0 {
				continue
			}
			var ev struct {
				Name string `json:"name"`
				Data struct {
					Raw struct {
						Length int `json:"length"`
					} `json:"raw"`
				} `json:"data"`
			}
			if json.Unmarshal(rec, &ev) != nil {
				continue
			}
			switch ev.Name {
			case "transport:packet_sent":
				stats.sentPackets++
				stats.sentBytes += ev.Data.Raw.Length
			case "recovery:packet_lost":
				stats.lostPackets++
			}
		}
	}

	return stats
}

// calibrationHosts brings up n hosts on the default link model with qlog enabled, node 0
// connected to every other node. It returns the hosts and the qlog directory.
//
// qlog is enabled through the QLOGDIR environment variable, which quic-go's default tracer
// reads per connection. go-libp2p v0.48.0's quicreuse.WithQlogTracerDir stores the directory
// and never reads it back, so the option is a no-op there. One directory for the process: the
// file names carry the perspective (`_client` / `_server`), which is how the sender's traces are
// told apart.
func calibrationHosts(t *testing.T, n int) (hosts []host.Host, qlogDir string, stop func()) {
	t.Helper()

	qlogDir = t.TempDir()
	t.Setenv("QLOGDIR", qlogDir)
	settings := simlibp2p.NetworkSettings{UseBlankHost: true}
	// Calibration runs on the real clock, so the links need the real-clock burst window --
	// measuring the very defect it repairs is what this file is for.
	links := stampBurstWindow(UniformLinks(n, DefaultRate), true)
	sim, meta, err := simlibp2p.SimpleLibp2pNetwork(links, simnet.StaticLatency(DefaultLatency), settings)
	require.NoError(t, err)
	sim.Start()
	Settle(true)

	for i := 1; i < n; i++ {
		to := meta.Nodes[i]
		require.NoError(t, meta.Nodes[0].Connect(context.Background(), peer.AddrInfo{ID: to.ID(), Addrs: to.Addrs()}))
	}
	Settle(true)

	var once sync.Once
	stop = func() {
		// Called explicitly, to flush qlog before reading it, and again by the deferred cleanup.
		once.Do(func() {
			for _, h := range meta.Nodes {
				_ = h.Close()
			}
			sim.Close()
			Settle(true)
		})
	}

	return meta.Nodes, qlogDir, stop
}

// sink installs the calibration protocol on h: it drains every stream and reports, per stream,
// how many bytes arrived and when the last one did.
type sink struct {
	mu   sync.Mutex
	got  map[peer.ID]int
	last map[peer.ID]time.Time
	done chan peer.ID
}

func newSink(h host.Host) *sink {
	s := &sink{got: map[peer.ID]int{}, last: map[peer.ID]time.Time{}, done: make(chan peer.ID, 64)}
	h.SetStreamHandler(calibrationProtocol, func(str network.Stream) {
		from := str.Conn().RemotePeer()
		buf := make([]byte, 64<<10)
		for {
			n, err := str.Read(buf)
			s.mu.Lock()
			s.got[from] += n
			s.last[from] = time.Now()
			s.mu.Unlock()
			if err != nil {
				_ = str.Close()
				s.done <- from
				return
			}
		}
	})

	return s
}

// send writes total bytes to peer over one stream and half-closes it.
func send(t *testing.T, from host.Host, to peer.ID, total int) {
	t.Helper()

	str, err := from.NewStream(context.Background(), to, calibrationProtocol)
	require.NoError(t, err)
	chunk := make([]byte, 32<<10)
	for off := 0; off < total; off += len(chunk) {
		n := min(len(chunk), total-off)
		_, err := str.Write(chunk[:n])
		require.NoError(t, err)
	}
	require.NoError(t, str.CloseWrite())
	// Wait for the peer to close its side, so the stream's lifetime covers the delivery.
	_, _ = io.Copy(io.Discard, str)
}

// TestCalibrationQUICFlow is one libp2p stream over the default link model: the raw flow plus
// QUIC's congestion control, acknowledgements and loss recovery. The gap between this and the
// raw flow is the transport's cost; the gap between this and the configured rate is what a
// single-peer transfer in the experiments can hope for.
func TestCalibrationQUICFlow(t *testing.T) {
	if testing.Short() {
		t.Skip("measurement")
	}
	const total = 8 << 20

	hosts, qlogDir, stop := calibrationHosts(t, 2)
	defer stop()
	receiver := newSink(hosts[1])

	start := time.Now()
	send(t, hosts[0], hosts[1].ID(), total)
	<-receiver.done
	elapsed := receiver.last[hosts[0].ID()].Sub(start)
	require.Equal(t, total, receiver.got[hosts[0].ID()])

	// qlog is flushed on connection close.
	stop()
	sender := parseQlogDir(t, qlogDir, "client")

	t.Logf("QUIC flow: %d B in %v = %.1f Mbps goodput; wire %d pkts / %d B (%.3fx payload); lost %d pkts (%.2f%%)",
		total, elapsed.Round(time.Millisecond), mbps(total, elapsed),
		sender.sentPackets, sender.sentBytes, float64(sender.sentBytes)/float64(total),
		sender.lostPackets, 100*float64(sender.lostPackets)/float64(max(sender.sentPackets, 1)))

	require.Equal(t, true, sender.sentPackets > 0, "qlog produced no packet_sent events; the trace is not wired")
	require.Equal(t, true, mbps(total, elapsed) < float64(DefaultRate)/1e6*1.05, "goodput above the configured rate")
}

// TestCalibrationFanout is the shape the experiments run: one uplink, several concurrent
// receivers. What it checks is that the *per-node* link model is what it says -- the sender's
// uplink is shared, so the aggregate cannot exceed the rate and each receiver gets a share --
// and what it reports is how evenly FQ-CoDel shares it and what the concurrency costs in loss.
func TestCalibrationFanout(t *testing.T) {
	if testing.Short() {
		t.Skip("measurement")
	}
	const (
		receivers = 8
		perFlow   = 2 << 20
	)

	hosts, qlogDir, stop := calibrationHosts(t, receivers+1)
	defer stop()
	sinks := make([]*sink, receivers)
	for i := range sinks {
		sinks[i] = newSink(hosts[i+1])
	}

	start := time.Now()
	var wg sync.WaitGroup
	for i := range receivers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			send(t, hosts[0], hosts[i+1].ID(), perFlow)
		}()
	}
	wg.Wait()
	for i := range receivers {
		<-sinks[i].done
	}

	var lastAll time.Time
	var firstDone, lastDone time.Duration
	for i, s := range sinks {
		require.Equal(t, perFlow, s.got[hosts[0].ID()])
		done := s.last[hosts[0].ID()].Sub(start)
		if i == 0 || done < firstDone {
			firstDone = done
		}
		if done > lastDone {
			lastDone = done
		}
		if s.last[hosts[0].ID()].After(lastAll) {
			lastAll = s.last[hosts[0].ID()]
		}
	}
	elapsed := lastAll.Sub(start)

	stop()
	sender := parseQlogDir(t, qlogDir, "client")

	t.Logf("fanout x%d: %d B each in %v = %.1f Mbps aggregate; flows finished between %v and %v; wire %d pkts / %d B (%.3fx payload); lost %d pkts (%.2f%%)",
		receivers, perFlow, elapsed.Round(time.Millisecond), mbps(receivers*perFlow, elapsed),
		firstDone.Round(time.Millisecond), lastDone.Round(time.Millisecond),
		sender.sentPackets, sender.sentBytes, float64(sender.sentBytes)/float64(receivers*perFlow),
		sender.lostPackets, 100*float64(sender.lostPackets)/float64(max(sender.sentPackets, 1)))

	require.Equal(t, true, mbps(receivers*perFlow, elapsed) < float64(DefaultRate)/1e6*1.05,
		"aggregate above the configured uplink: the link model is not per node")
}
