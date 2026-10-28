package pubsub

import (
	"sync"
	"time"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"

	"github.com/libp2p/go-libp2p/core/peer"
	"google.golang.org/protobuf/proto"
)

// --- Upload by class: the bytes a node wrote to its streams, split by what they carried ---
//
// Counted at the stream write, after every dispatch-time cancel and filter, so a frame dropped
// from a queue never counts; what the write hands the transport may still wait in its buffers.
// A field's bytes include its protobuf tag and length; Other is the rest of the write: the
// length prefix, subscriptions, partial messages and the RPC's own control framing.

// UploadStat is one node's written bytes since the last reset.
type UploadStat struct {
	PushBytes, AnswerBytes                        int64 // frames sent unasked, and IWANT answers
	PushFrames, AnswerFrames                      int64
	PushRPCs, AnswerRPCs                          int64 // writes carrying at least one frame of the class
	MaxPushFrames, MaxAnswerFrames                int   // the most frames of the class in one write
	IHaveBytes, IWantBytes, IDontWantBytes        int64 // IDONTWANT carries the HAVE bitmaps and hose acks
	GraftPruneBytes, OtherBytes, Writes, AllBytes int64
	// The ids written in each control kind, and those that are HAVE bitmaps (a holding, an
	// acknowledgement, a decode claim) or asks by index (WANT ids): the per-rule counts the
	// harness sets beside the spec's toy (xcheck_test.go).
	IHaveIDs, IHaveBitmaps, IWantIDs, IWantAsks, IDontWantIDs, IDontWantBitmaps int64
}

type uploadCounter struct {
	mu sync.Mutex
	s  UploadStat
	// pushTo is the pushed frames written to each peer, so a harness can tell who drew the pushes.
	pushTo map[peer.ID]int64
	// pushPerFrame is how many writes pushed each frame (by message id), unasked: the push budget
	// a harness checks against the spec's (xcheck_test.go). Filled only when note has an id function.
	pushPerFrame map[string]int32
	// When writes left, for a harness's timeline of a node's sends (the source's): the writes that
	// carried IHAVE ids, pushed frames and answers, each list capped at uploadTimesCap.
	ihaveAt, pushAt, answerAt []time.Time
	// asksIn and asksInMesh count the ask ids this node received (handleIWant), all and from its
	// mesh peers.
	asksIn, asksInMesh int64
}

const uploadTimesCap = 1 << 14

func appendCapped(ts []time.Time, t time.Time) []time.Time {
	if len(ts) >= uploadTimesCap {
		return ts
	}
	return append(ts, t)
}

// noteAsksIn adds n ask ids received, inMesh of them from mesh peers. Router goroutine.
func (u *uploadCounter) noteAsksIn(n, inMesh int64) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.asksIn += n
	u.asksInMesh += inMesh
}

// fieldSize is a length-delimited field of n bytes with a one-byte tag.
func fieldSize(n int) int64 {
	l := 1
	for v := n; v >= 0x80; v >>= 7 {
		l++
	}
	return int64(1 + l + n)
}

func (u *uploadCounter) note(rpc *RPC, written int, to peer.ID) {
	u.noteWithID(rpc, written, to, nil)
}

// noteWithID is note with the node's message-id function, which keys the per-frame push counts.
func (u *uploadCounter) noteWithID(rpc *RPC, written int, to peer.ID, id func(*pb.Message) string) {
	var frames, fbytes int64
	for _, m := range rpc.Publish {
		frames++
		fbytes += fieldSize(proto.Size(m))
	}
	var ihave, iwant, idw, gp int64
	var ihaveIDs, ihaveBits, iwantIDs, iwantAsks, idwIDs, idwBits int64
	if c := rpc.Control; c != nil {
		for _, x := range c.Ihave {
			ihave += fieldSize(proto.Size(x))
			for _, mid := range x.GetMessageIDs() {
				ihaveIDs++
				if _, _, ok := decodeHave(mid); ok {
					ihaveBits++
				}
			}
		}
		for _, x := range c.Iwant {
			iwant += fieldSize(proto.Size(x))
			for _, mid := range x.GetMessageIDs() {
				iwantIDs++
				if _, _, ok := decodeWant(mid); ok {
					iwantAsks++
				}
			}
		}
		for _, x := range c.Idontwant {
			idw += fieldSize(proto.Size(x))
			for _, mid := range x.GetMessageIDs() {
				idwIDs++
				if _, _, ok := decodeHave(mid); ok {
					idwBits++
				}
			}
		}
		for _, x := range c.Graft {
			gp += fieldSize(proto.Size(x))
		}
		for _, x := range c.Prune {
			gp += fieldSize(proto.Size(x))
		}
	}
	now := time.Now()
	u.mu.Lock()
	defer u.mu.Unlock()
	if ihaveIDs > 0 {
		u.ihaveAt = appendCapped(u.ihaveAt, now)
	}
	if frames > 0 {
		if rpc.solicited {
			u.answerAt = appendCapped(u.answerAt, now)
		} else {
			u.pushAt = appendCapped(u.pushAt, now)
		}
	}
	s := &u.s
	s.Writes++
	s.AllBytes += int64(written)
	if frames > 0 {
		if rpc.solicited {
			s.AnswerBytes, s.AnswerFrames, s.AnswerRPCs = s.AnswerBytes+fbytes, s.AnswerFrames+frames, s.AnswerRPCs+1
			s.MaxAnswerFrames = max(s.MaxAnswerFrames, len(rpc.Publish))
		} else {
			s.PushBytes, s.PushFrames, s.PushRPCs = s.PushBytes+fbytes, s.PushFrames+frames, s.PushRPCs+1
			s.MaxPushFrames = max(s.MaxPushFrames, len(rpc.Publish))
			if u.pushTo == nil {
				u.pushTo = make(map[peer.ID]int64)
			}
			u.pushTo[to] += frames
			if id != nil {
				if u.pushPerFrame == nil {
					u.pushPerFrame = make(map[string]int32)
				}
				for _, m := range rpc.Publish {
					u.pushPerFrame[id(m)]++
				}
			}
		}
	}
	s.IHaveIDs += ihaveIDs
	s.IHaveBitmaps += ihaveBits
	s.IWantIDs += iwantIDs
	s.IWantAsks += iwantAsks
	s.IDontWantIDs += idwIDs
	s.IDontWantBitmaps += idwBits
	s.IHaveBytes += ihave
	s.IWantBytes += iwant
	s.IDontWantBytes += idw
	s.GraftPruneBytes += gp
	s.OtherBytes += int64(written) - fbytes - ihave - iwant - idw - gp
}

// UploadStats is the node's written bytes by class since the last ResetUploadStats.
func (p *PubSub) UploadStats() UploadStat {
	p.upload.mu.Lock()
	defer p.upload.mu.Unlock()
	return p.upload.s
}

// UploadPushFramesTo is the pushed frames written to each peer since the last ResetUploadStats.
func (p *PubSub) UploadPushFramesTo() map[peer.ID]int64 {
	p.upload.mu.Lock()
	defer p.upload.mu.Unlock()
	out := make(map[peer.ID]int64, len(p.upload.pushTo))
	for k, v := range p.upload.pushTo {
		out[k] = v
	}
	return out
}

// UploadPushPerFrame is, per frame id, how many writes pushed it unasked since the last
// ResetUploadStats.
func (p *PubSub) UploadPushPerFrame() map[string]int32 {
	p.upload.mu.Lock()
	defer p.upload.mu.Unlock()
	out := make(map[string]int32, len(p.upload.pushPerFrame))
	for k, v := range p.upload.pushPerFrame {
		out[k] = v
	}
	return out
}

// UploadTimes is when the node's writes carrying IHAVE ids, pushed frames and answers left,
// since the last ResetUploadStats (each list capped).
func (p *PubSub) UploadTimes() (ihave, push, answer []time.Time) {
	p.upload.mu.Lock()
	defer p.upload.mu.Unlock()
	return append([]time.Time(nil), p.upload.ihaveAt...), append([]time.Time(nil), p.upload.pushAt...),
		append([]time.Time(nil), p.upload.answerAt...)
}

// AsksReceived is the ask ids the node received since the last ResetUploadStats, all and from
// its mesh peers.
func (p *PubSub) AsksReceived() (all, fromMesh int64) {
	p.upload.mu.Lock()
	defer p.upload.mu.Unlock()
	return p.upload.asksIn, p.upload.asksInMesh
}

// ResetUploadStats starts the count afresh (per message of a multi-message cell).
func (p *PubSub) ResetUploadStats() {
	p.upload.mu.Lock()
	defer p.upload.mu.Unlock()
	p.upload.s = UploadStat{}
	p.upload.pushTo = nil
	p.upload.pushPerFrame = nil
	p.upload.ihaveAt, p.upload.pushAt, p.upload.answerAt = nil, nil, nil
	p.upload.asksIn, p.upload.asksInMesh = 0, 0
}
