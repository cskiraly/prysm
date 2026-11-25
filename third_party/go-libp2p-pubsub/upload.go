package pubsub

import (
	"sync"

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

// TopicUpload is the frames of one topic the node wrote since the last reset, pushed and answered,
// their bytes counted as UploadStat's.
type TopicUpload struct {
	PushBytes, AnswerBytes   int64
	PushFrames, AnswerFrames int64
}

type uploadCounter struct {
	mu sync.Mutex
	s  UploadStat
	// byTopic splits the frames by topic. Its topics are those the node wrote frames on: published
	// on, joined or relayed. A frame is a message the node accepted or published, so peers can't
	// add topics to it.
	byTopic map[string]*TopicUpload
	// asksIn and asksInMesh count the ask ids this node received (handleIWant), all and from its
	// mesh peers.
	asksIn, asksInMesh int64
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

// note counts one write of rpc, written bytes long, by class.
func (u *uploadCounter) note(rpc *RPC, written int) {
	var frames, fbytes int64
	var small [8]int64
	sizes := small[:0]
	for _, m := range rpc.Publish {
		frames++
		n := fieldSize(proto.Size(m))
		sizes = append(sizes, n)
		fbytes += n
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
	u.mu.Lock()
	defer u.mu.Unlock()
	s := &u.s
	s.Writes++
	s.AllBytes += int64(written)
	for i, m := range rpc.Publish {
		tu := u.byTopic[m.GetTopic()]
		if tu == nil {
			if u.byTopic == nil {
				u.byTopic = make(map[string]*TopicUpload)
			}
			tu = &TopicUpload{}
			u.byTopic[m.GetTopic()] = tu
		}
		if rpc.solicited {
			tu.AnswerBytes += sizes[i]
			tu.AnswerFrames++
		} else {
			tu.PushBytes += sizes[i]
			tu.PushFrames++
		}
	}
	if frames > 0 {
		if rpc.solicited {
			s.AnswerBytes, s.AnswerFrames, s.AnswerRPCs = s.AnswerBytes+fbytes, s.AnswerFrames+frames, s.AnswerRPCs+1
			s.MaxAnswerFrames = max(s.MaxAnswerFrames, len(rpc.Publish))
		} else {
			s.PushBytes, s.PushFrames, s.PushRPCs = s.PushBytes+fbytes, s.PushFrames+frames, s.PushRPCs+1
			s.MaxPushFrames = max(s.MaxPushFrames, len(rpc.Publish))
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

// UploadByTopic is the frames the node wrote since the last ResetUploadStats, by topic: pushed and
// answered, their bytes counted as UploadStats counts them, so a class's bytes over the topics are
// UploadStats' bytes of that class.
func (p *PubSub) UploadByTopic() map[string]TopicUpload {
	return p.upload.byTopicCopy()
}

// UploadSnapshot is UploadStats and UploadByTopic read together, under one lock: no write falls
// between them, so the frames by topic add up to the totals' frames.
func (p *PubSub) UploadSnapshot() (UploadStat, map[string]TopicUpload) {
	p.upload.mu.Lock()
	defer p.upload.mu.Unlock()
	return p.upload.s, p.upload.byTopicLocked()
}

func (u *uploadCounter) byTopicCopy() map[string]TopicUpload {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.byTopicLocked()
}

// byTopicLocked copies the counts by topic. The caller holds u.mu.
func (u *uploadCounter) byTopicLocked() map[string]TopicUpload {
	out := make(map[string]TopicUpload, len(u.byTopic))
	for k, v := range u.byTopic {
		out[k] = *v
	}
	return out
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
	p.upload.byTopic = nil
	p.upload.asksIn, p.upload.asksInMesh = 0, 0
}
