package ecbridge

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/OffchainLabs/prysm/v7/beacon-chain/p2p/segmentintegrationtest/ethp2p/broadcast"
	"github.com/OffchainLabs/prysm/v7/beacon-chain/p2p/segmentintegrationtest/ethp2p/broadcast/rs"
	"github.com/OffchainLabs/prysm/v7/beacon-chain/p2p/segmentintegrationtest/ethp2p/legacy/transport"
	"github.com/OffchainLabs/prysm/v7/beacon-chain/p2p/segmentintegrationtest/ethp2p/protocol"
	protopb "github.com/OffchainLabs/prysm/v7/beacon-chain/p2p/segmentintegrationtest/ethp2p/protocol/pb"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
)

func newHosts(t *testing.T, n int) []host.Host {
	t.Helper()
	hs := make([]host.Host, n)
	for i := range hs {
		h, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/udp/0/quic-v1"))
		require.NoError(t, err)
		t.Cleanup(func() { _ = h.Close() })
		hs[i] = h
	}
	return hs
}

func connect(t *testing.T, a, b host.Host) {
	t.Helper()
	require.NoError(t, a.Connect(context.Background(), peer.AddrInfo{ID: b.ID(), Addrs: b.Addrs()}))
}

// A CHUNK stream's selector, length and header count as control and its payload as data, at the
// receiver; the opener reads the receiver's multistream echo as control.
func TestChunkAccounting(t *testing.T) {
	hs := newHosts(t, 2)
	connect(t, hs[0], hs[1])
	na, nb := NewNode(hs[0]), NewNode(hs[1])

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	w, err := na.Conn(hs[1].ID()).OpenUniStream(ctx)
	require.NoError(t, err)
	header := []byte("a chunk header of 27 bytes.")
	payload := make([]byte, 16384)
	_, _ = rand.Read(payload)
	var msg bytes.Buffer
	require.NoError(t, protocol.WriteSelector(&msg, protopb.Protocol_PROTOCOL_CHUNK))
	msg.Write([]byte{0, 0, 0, byte(len(header))})
	msg.Write(header)
	msg.Write(payload)
	_, err = w.Write(msg.Bytes())
	require.NoError(t, err)
	require.NoError(t, w.Close())

	r, err := nb.Conn(hs[0].ID()).AcceptUniStream(ctx)
	require.NoError(t, err)
	got, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, msg.Bytes(), got)
	require.Equal(t, int64(len(payload)), nb.Count.Data.Load())
	require.Equal(t, int64(1+4+len(header))+multistreamBytes, nb.Count.Control.Load())
	require.Eventually(t, func() bool { return na.Count.Control.Load() > 0 }, 5*time.Second, 10*time.Millisecond,
		"the opener reads the receiver's multistream echo")
	require.Equal(t, int64(0), na.Count.Data.Load())
}

// A stream reset with an application code reaches the reader as the engine's StreamResetError.
func TestResetCode(t *testing.T) {
	hs := newHosts(t, 2)
	connect(t, hs[0], hs[1])
	na, nb := NewNode(hs[0]), NewNode(hs[1])

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	w, err := na.Conn(hs[1].ID()).OpenUniStream(ctx)
	require.NoError(t, err)
	_, err = w.Write([]byte{byte(protopb.Protocol_PROTOCOL_SESS), 1, 2, 3})
	require.NoError(t, err)
	r, err := nb.Conn(hs[0].ID()).AcceptUniStream(ctx)
	require.NoError(t, err)
	buf := make([]byte, 4)
	_, err = io.ReadFull(r, buf)
	require.NoError(t, err)
	w.CancelWrite(0x01)
	_, err = r.Read(buf)
	var re *transport.StreamResetError
	require.True(t, errors.As(err, &re), "got %v", err)
	require.Equal(t, uint64(0x01), re.Code)
}

// Three engines in a line, A-B-C, each with ethp2p's RS channel as their benchmarks configure it:
// a 1 MiB message published at A reaches C through B, byte for byte.
func TestEngineLine(t *testing.T) {
	hs := newHosts(t, 3)
	connect(t, hs[0], hs[1])
	connect(t, hs[1], hs[2])
	scheme := rs.NewScheme(rs.Config{ChunkLen: 16384, ForwardMultiplier: 4, BitmapThreshold: 0})
	engines := make([]*broadcast.Engine, len(hs))
	chans := make([]*broadcast.Channel[*rs.ChunkIdent, *rs.Bitmap, *rs.Preamble], len(hs))
	recv := make([]chan broadcast.FullMessage, len(hs))
	nodes := make([]*Node, len(hs))
	for i, h := range hs {
		engines[i] = broadcast.NewEngine(broadcast.EngineConfig{})
		chans[i] = broadcast.AttachChannel(engines[i], "broadcast", scheme)
		t.Cleanup(func() {
			chans[i].Stop()
			_ = engines[i].Close()
		})
		recv[i] = make(chan broadcast.FullMessage, 4)
		require.NoError(t, chans[i].Subscribe(recv[i]))
		nodes[i] = NewNode(h)
	}
	for i, h := range hs {
		for _, p := range h.Network().Peers() {
			engines[i].NotifyPeerConnected(nodes[i].Conn(p))
		}
	}
	payload := make([]byte, 1<<20)
	_, _ = rand.Read(payload)
	// The channel handshakes run in the background; a publish before they finish reaches nobody.
	time.Sleep(500 * time.Millisecond)
	require.NoError(t, chans[0].Publish("m1", payload))
	for _, i := range []int{1, 2} {
		select {
		case m := <-recv[i]:
			require.Equal(t, broadcast.MessageID("m1"), m.MessageID)
			require.True(t, bytes.Equal(payload, m.Data), "node %d decoded a different payload", i)
		case <-time.After(20 * time.Second):
			t.Fatalf("node %d did not deliver", i)
		}
	}
	for i := 1; i < 3; i++ {
		require.GreaterOrEqual(t, nodes[i].Count.Data.Load(), int64(len(payload)), "node %d received at least k shards of data", i)
	}
	t.Logf("data/control bytes received: B %d/%d, C %d/%d", nodes[1].Count.Data.Load(), nodes[1].Count.Control.Load(),
		nodes[2].Count.Data.Load(), nodes[2].Count.Control.Load())
}
