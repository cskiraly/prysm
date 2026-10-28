package pubsub

import (
	"sync"

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
}

type uploadCounter struct {
	mu sync.Mutex
	s  UploadStat
	// pushTo is the pushed frames written to each peer, so a harness can tell who drew the pushes.
	pushTo map[peer.ID]int64
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
	var frames, fbytes int64
	for _, m := range rpc.Publish {
		frames++
		fbytes += fieldSize(proto.Size(m))
	}
	var ihave, iwant, idw, gp int64
	if c := rpc.Control; c != nil {
		for _, x := range c.Ihave {
			ihave += fieldSize(proto.Size(x))
		}
		for _, x := range c.Iwant {
			iwant += fieldSize(proto.Size(x))
		}
		for _, x := range c.Idontwant {
			idw += fieldSize(proto.Size(x))
		}
		for _, x := range c.Graft {
			gp += fieldSize(proto.Size(x))
		}
		for _, x := range c.Prune {
			gp += fieldSize(proto.Size(x))
		}
	}
	u.mu.Lock()
	defer u.mu.Unlock()
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
		}
	}
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

// ResetUploadStats starts the count afresh (per message of a multi-message cell).
func (p *PubSub) ResetUploadStats() {
	p.upload.mu.Lock()
	defer p.upload.mu.Unlock()
	p.upload.s = UploadStat{}
	p.upload.pushTo = nil
}
