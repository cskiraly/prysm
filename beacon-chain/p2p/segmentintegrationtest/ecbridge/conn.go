// Package ecbridge runs ethp2p's erasure-coded broadcast engine (vendored, unmodified, in
// ../ethp2p) over the harness's libp2p hosts. It implements the engine's transport.Conn with
// libp2p streams: each of the engine's unidirectional streams is one libp2p stream on UniProtocol,
// written by the opener only, and the engine's stream resets with an application code map onto
// ResetWithError. It also counts the bytes each node reads, data apart from control, at the same
// layer as the gossipsub arms: above the libp2p stream.
package ecbridge

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"time"

	bpb "github.com/OffchainLabs/prysm/v7/beacon-chain/p2p/segmentintegrationtest/ethp2p/broadcast/pb"
	"github.com/OffchainLabs/prysm/v7/beacon-chain/p2p/segmentintegrationtest/ethp2p/legacy/transport"
	protopb "github.com/OffchainLabs/prysm/v7/beacon-chain/p2p/segmentintegrationtest/ethp2p/protocol/pb"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	msmux "github.com/multiformats/go-multistream"
	"google.golang.org/protobuf/proto"
)

// UniProtocol carries every unidirectional stream of the engine (BCAST, SESS and CHUNK alike; the
// engine's own one-byte selector tells them apart).
const UniProtocol = protocol.ID("/ethp2p/uni/1")

// multistreamBytes is what lazy multistream-select puts on a stream in each direction: the
// "/multistream/1.0.0\n" line and the protocol line, each behind a one-byte length. The opener's
// pair arrives at the receiver and the receiver's echo at the opener, so each side reads it once.
const multistreamBytes = int64(1 + len("/multistream/1.0.0\n") + 1 + len(string(UniProtocol)) + 1)

// Counters are one node's received bytes. Data is the payload of CHUNK streams (the shard bytes);
// control is everything else read on the engine's streams, multistream lines included.
type Counters struct {
	Data    atomic.Int64
	Control atomic.Int64

	// Stream events, for diagnosis: opened and accepted streams, the engine's cancels on each
	// side, open failures, and streams refused by the adapter (closed conn or full queue).
	Opened, Accepted, CancelWrites, CancelReads, OpenErrors, Refused atomic.Int64
	// OpenWait and WriteWait sum the time spent blocked in opening streams and in writes.
	OpenWait, WriteWait atomic.Int64
}

// Reset zeroes both counters (a new message window).
func (c *Counters) Reset() {
	for _, x := range []*atomic.Int64{&c.Data, &c.Control, &c.Opened, &c.Accepted, &c.CancelWrites, &c.CancelReads, &c.OpenErrors, &c.Refused, &c.OpenWait, &c.WriteWait} {
		x.Store(0)
	}
}

// Node is one host's side of the bridge: it accepts UniProtocol streams and hands each to the
// Conn of the peer that opened it.
type Node struct {
	host  host.Host
	Count Counters

	mu    sync.Mutex
	conns map[peer.ID]*Conn
}

// NewNode registers the stream handler on h.
func NewNode(h host.Host) *Node {
	n := &Node{host: h, conns: make(map[peer.ID]*Conn)}
	h.SetStreamHandler(UniProtocol, n.handle)
	return n
}

// Conn returns the bridge connection to p, creating it on first use. Inbound streams that arrive
// before the engine is told about p wait in its accept queue.
func (n *Node) Conn(p peer.ID) *Conn {
	n.mu.Lock()
	defer n.mu.Unlock()
	c, ok := n.conns[p]
	if !ok {
		c = &Conn{node: n, remote: p, accept: make(chan *recvStream, acceptQueue), closed: make(chan struct{}),
			live: make(map[network.Stream]struct{})}
		n.conns[p] = c
	}
	return c
}

// acceptQueue bounds the inbound streams waiting for the engine's AcceptUniStream. The engine
// accepts in a loop and caps concurrent chunk streams itself, so this only absorbs bursts.
const acceptQueue = 4096

func (n *Node) handle(s network.Stream) {
	// The receiver never writes: close its write side at once so the stream can finish when the
	// opener closes its own.
	_ = s.CloseWrite()
	c := n.Conn(s.Conn().RemotePeer())
	if !c.track(s) {
		n.Count.Refused.Add(1)
		_ = s.Reset()
		return
	}
	n.Count.Accepted.Add(1)
	n.Count.Control.Add(multistreamBytes)
	rs := &recvStream{s: s, count: &n.Count, conn: c}
	select {
	case c.accept <- rs:
	case <-c.closed:
		n.Count.Refused.Add(1)
		_ = s.Reset()
	default:
		n.Count.Refused.Add(1)
		_ = s.Reset() // queue full: the engine is not keeping up; refuse rather than block the host
	}
}

// Conn is transport.Conn to one remote peer.
type Conn struct {
	node   *Node
	remote peer.ID
	accept chan *recvStream

	closeOnce sync.Once
	closed    chan struct{}

	// live holds the streams in use, so that Close can abort them as closing a QUIC connection
	// would: the engine's PeerConn.Close relies on it to unblock its readers.
	liveMu sync.Mutex
	live   map[network.Stream]struct{}
}

// track adds s to the live set; false once the Conn is closed.
func (c *Conn) track(s network.Stream) bool {
	c.liveMu.Lock()
	defer c.liveMu.Unlock()
	if c.live == nil {
		return false
	}
	c.live[s] = struct{}{}
	return true
}

func (c *Conn) untrack(s network.Stream) {
	c.liveMu.Lock()
	defer c.liveMu.Unlock()
	delete(c.live, s)
}

var _ transport.Conn = (*Conn)(nil)

var errBidiUnsupported = errors.New("ecbridge: bidirectional streams are not used by the broadcast engine")

func (c *Conn) OpenStream(context.Context) (transport.Stream, error) { return nil, errBidiUnsupported }
func (c *Conn) AcceptStream(context.Context) (transport.Stream, error) {
	return nil, errBidiUnsupported
}

// OpenUniStream opens a libp2p stream on UniProtocol with lazy negotiation: the protocol lines go
// out with the first write and no round trip is spent before it, as a BasicHost does for a
// protocol it knows the peer speaks. The harness's hosts are BlankHosts, whose NewStream always
// negotiates first: one round trip before every chunk, which would throttle a protocol that opens
// a stream per chunk.
func (c *Conn) OpenUniStream(ctx context.Context) (transport.SendStream, error) {
	t0 := time.Now()
	s, err := c.node.host.Network().NewStream(network.WithAllowLimitedConn(ctx, "ecbridge"), c.remote)
	if err == nil {
		if err = s.SetProtocol(UniProtocol); err != nil {
			_ = s.Reset()
		}
	}
	c.node.Count.OpenWait.Add(int64(time.Since(t0)))
	if err != nil {
		c.node.Count.OpenErrors.Add(1)
		return nil, err
	}
	c.node.Count.Opened.Add(1)
	if !c.track(s) {
		_ = s.Reset()
		return nil, io.EOF
	}
	return &sendStream{s: s, rw: msmux.NewMSSelect(s, UniProtocol), count: &c.node.Count, conn: c}, nil
}

// AcceptUniStream returns the next stream the remote peer opened.
func (c *Conn) AcceptUniStream(ctx context.Context) (transport.ReceiveStream, error) {
	select {
	case rs := <-c.accept:
		return rs, nil
	case <-c.closed:
		return nil, io.EOF
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (c *Conn) SendDatagram(context.Context, []byte) error { return transport.ErrDatagramsUnsupported }
func (c *Conn) RecvDatagram(context.Context) ([]byte, error) {
	return nil, transport.ErrDatagramsUnsupported
}

// Close stops accepting streams for the engine and resets every stream still in use, as closing
// the connection would; the libp2p connection itself belongs to the harness.
func (c *Conn) Close() error {
	c.closeOnce.Do(func() {
		close(c.closed)
		c.liveMu.Lock()
		live := c.live
		c.live = nil
		c.liveMu.Unlock()
		for s := range live {
			_ = s.Reset()
		}
	})
	return nil
}

func (c *Conn) SupportsStreams() bool   { return true }
func (c *Conn) SupportsDatagrams() bool { return false }

// ConnectionStats is unused by the broadcast engine; the bridge counts per node in Counters.
func (c *Conn) ConnectionStats() (bytesSent, bytesReceived uint64) { return 0, 0 }

// Direction reports the lower peer id as the dialer, so both ends agree; the engine only uses it
// for logging and tie-breaks.
func (c *Conn) Direction() transport.ConnDirection {
	if c.node.host.ID() < c.remote {
		return transport.Outbound
	}
	return transport.Inbound
}

func (c *Conn) AuthInfo() transport.AuthInfo {
	return transport.AuthInfo{Local: transport.PeerID(c.node.host.ID().String()), Remote: transport.PeerID(c.remote.String())}
}

// sendStream is the opener's side. It reads to the end of the stream in the background, so the
// libp2p stream is fully closed once both sides are done. The receiver's multistream echo is
// consumed inside libp2p's lazy negotiation and never surfaces in Read, so it is counted as a
// constant when the drain ends.
type sendStream struct {
	s     network.Stream
	rw    msmux.LazyConn // s behind lazy negotiation: reads and writes go through it
	count *Counters
	conn  *Conn
	once  sync.Once
}

func (w *sendStream) Write(p []byte) (int, error) {
	t0 := time.Now()
	n, err := w.rw.Write(p)
	w.count.WriteWait.Add(int64(time.Since(t0)))
	return n, err
}

func (w *sendStream) Close() error {
	_ = w.rw.Flush()
	err := w.s.CloseWrite()
	w.drain()
	return err
}

func (w *sendStream) CancelWrite(code uint64) {
	w.count.CancelWrites.Add(1)
	_ = w.s.ResetWithError(network.StreamErrorCode(code))
	w.conn.untrack(w.s)
}

func (w *sendStream) SetWriteDeadline(t time.Time) error { return w.s.SetWriteDeadline(t) }

func (w *sendStream) drain() {
	w.once.Do(func() {
		go func() {
			n, _ := io.Copy(io.Discard, w.rw)
			w.count.Control.Add(n + multistreamBytes)
			_ = w.s.Close()
			w.conn.untrack(w.s)
		}()
	})
}

// recvStream is the receiver's side. It classifies what it reads: on a CHUNK stream the selector
// byte, the 4-byte header length and the header are control, and everything after is data; on
// every other stream all of it is control.
//
// It also finishes CHUNK streams. The engine reads exactly a chunk's data and stops, but a QUIC
// stream only completes once its end is read, and until then it holds one of the connection's
// incoming-stream credits (256 in go-libp2p; ethp2p raises its own QUIC transport's to 16384 for
// this reason). So once the data named in the header has been read, the stream's end is read in
// the background, which returns the credit.
type recvStream struct {
	s     network.Stream
	count *Counters
	conn  *Conn

	pos      int64  // bytes read so far
	chunk    bool   // the selector named a CHUNK stream
	lenBuf   []byte // the header length, as it arrives
	dataFrom int64  // position where the chunk data starts, once known (0: not yet)
	hdr      []byte // the header, as it arrives
	dataEnd  int64  // position where the chunk data ends, once the header is parsed (0: not yet)
	finished bool   // the background read of the end has started
}

func (r *recvStream) Read(p []byte) (int, error) {
	n, err := r.s.Read(p)
	if n > 0 {
		r.account(p[:n])
	}
	if err == nil && r.dataEnd > 0 && r.pos >= r.dataEnd && !r.finished {
		r.finished = true
		go r.readEnd()
	}
	if err != nil {
		r.conn.untrack(r.s)
		var se *network.StreamError
		if errors.As(err, &se) && se.Remote {
			return n, &transport.StreamResetError{Code: uint64(se.ErrorCode)}
		}
	}
	return n, err
}

func (r *recvStream) account(b []byte) {
	for len(b) > 0 {
		switch {
		case r.pos == 0:
			r.chunk = protopb.Protocol(b[0]) == protopb.Protocol_PROTOCOL_CHUNK
			r.count.Control.Add(1)
			r.pos++
			b = b[1:]
		case !r.chunk:
			r.count.Control.Add(int64(len(b)))
			r.pos += int64(len(b))
			return
		case r.dataFrom == 0:
			if len(r.lenBuf) < 4 {
				k := min(4-len(r.lenBuf), len(b))
				r.lenBuf = append(r.lenBuf, b[:k]...)
				r.count.Control.Add(int64(k))
				r.pos += int64(k)
				b = b[k:]
				if len(r.lenBuf) == 4 {
					hdr := int64(r.lenBuf[0])<<24 | int64(r.lenBuf[1])<<16 | int64(r.lenBuf[2])<<8 | int64(r.lenBuf[3])
					r.dataFrom = r.pos + hdr
				}
				continue
			}
		default:
			if r.pos < r.dataFrom {
				k := min(r.dataFrom-r.pos, int64(len(b)))
				r.count.Control.Add(k)
				r.hdr = append(r.hdr, b[:k]...)
				r.pos += k
				b = b[k:]
				if r.pos == r.dataFrom {
					var h bpb.Chunk_Header
					if proto.Unmarshal(r.hdr, &h) == nil {
						r.dataEnd = r.dataFrom + int64(h.GetDataLength())
					}
				}
				continue
			}
			r.count.Data.Add(int64(len(b)))
			r.pos += int64(len(b))
			return
		}
	}
}

// readEnd reads past the chunk's data to the stream's end, so the stream completes.
func (r *recvStream) readEnd() {
	var buf [64]byte
	for {
		n, err := r.s.Read(buf[:])
		r.count.Control.Add(int64(n))
		if err != nil {
			r.conn.untrack(r.s)
			return
		}
	}
}

func (r *recvStream) CancelRead(code uint64) {
	r.count.CancelReads.Add(1)
	_ = r.s.ResetWithError(network.StreamErrorCode(code))
	r.conn.untrack(r.s)
}

func (r *recvStream) SetReadDeadline(t time.Time) error { return r.s.SetReadDeadline(t) }
