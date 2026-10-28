package segmentintegrationtest

// The ask tracer (SEGMENT_ASK_TRACE=1): every IWANT id a node receives, classed against the
// message in flight. Built to find why the publisher of a multi-message cell's later messages is
// asked more and more (Q131): whether the asks name the current group at all (a scoping fault
// would ask for an earlier group's frames), whether the asker asks again for what it already
// asked, whether the frame was already queued to the asker, and whether the asker had completed.

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
)

type askEvent struct {
	from    peer.ID
	at      time.Time
	byIndex bool // named by HAVE index, not by message id
	current bool // the id is a frame of the message in flight
	repeat  bool // the asker had asked this node for the same frame already
	queued  bool // this node had queued the frame to the asker before the ask
}

// askTracer is one node's ask log; a RawTracer next to the recording one.
type askTracer struct {
	mu    sync.Mutex
	group string
	known map[digest]int
	sent  map[peer.ID]map[uint32]bool
	asked map[peer.ID]map[string]bool
	asks  []askEvent
}

func newAskTracer() *askTracer { return &askTracer{} }

// setMessage starts the log for the message whose frames are arm, in group root.
func (a *askTracer) setMessage(arm wireArm) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.group, a.known = "", arm.known
	if ids := segmentIDs(arm); len(ids) > 0 {
		if k, ok := segIDKey(ids[0]); ok {
			a.group = string(k.group[:])
		}
	}
	a.sent = make(map[peer.ID]map[uint32]bool)
	a.asked = make(map[peer.ID]map[string]bool)
	a.asks = nil
}

func (a *askTracer) RecvRPC(rpc *pubsub.RPC) {
	if rpc == nil || rpc.GetControl() == nil || len(rpc.GetControl().GetIwant()) == 0 {
		return
	}
	from := rpc.From()
	now := time.Now()
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.asked == nil {
		return
	}
	for _, iw := range rpc.GetControl().GetIwant() {
		for _, mid := range iw.GetMessageIDs() {
			e := askEvent{from: from, at: now}
			var root string
			var idx uint32
			if k, ok := segIDKey(mid); ok {
				root, idx = string(k.group[:]), k.index
			} else if r, i, ok := pubsub.DecodeHaveWant(mid); ok {
				root, idx, e.byIndex = r, i, true
			} else {
				continue
			}
			e.current = root == a.group
			key := fmt.Sprintf("%x/%d", root, idx)
			if a.asked[from] == nil {
				a.asked[from] = make(map[string]bool)
			}
			e.repeat = a.asked[from][key]
			a.asked[from][key] = true
			e.queued = e.current && a.sent[from][idx]
			a.asks = append(a.asks, e)
		}
	}
}

func (a *askTracer) SendRPC(rpc *pubsub.RPC, p peer.ID) {
	if rpc == nil || len(rpc.GetPublish()) == 0 {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.sent == nil {
		return
	}
	for _, m := range rpc.GetPublish() {
		if idx, ok := a.known[digestOf(m.GetData())]; ok {
			if a.sent[p] == nil {
				a.sent[p] = make(map[uint32]bool)
			}
			a.sent[p][uint32(idx)] = true // lint:ignore uintcast -- a frame index, bounded by the arm.
		}
	}
}

func (*askTracer) OnNewOutboundStream(peer.ID, protocol.ID) {}
func (*askTracer) OnClosedOutboundStream(peer.ID)           {}
func (*askTracer) Join(string)                              {}
func (*askTracer) Leave(string)                             {}
func (*askTracer) Graft(peer.ID, string)                    {}
func (*askTracer) Prune(peer.ID, string)                    {}
func (*askTracer) ValidateMessage(*pubsub.Message)          {}
func (*askTracer) DeliverMessage(*pubsub.Message)           {}
func (*askTracer) RejectMessage(*pubsub.Message, string)    {}
func (*askTracer) DuplicateMessage(*pubsub.Message)         {}
func (*askTracer) ThrottlePeer(peer.ID)                     {}
func (*askTracer) DropRPC(*pubsub.RPC, peer.ID)             {}
func (*askTracer) UndeliverableMessage(*pubsub.Message)     {}

// askLine summarises the asks the nodes in set received: how many name the current message,
// by index or by id, repeats, frames already queued to the asker, asks from askers that had
// completed (compAt, relative to start), askers in the node's mesh, and the ask's age.
func askLine(name string, set []int, ats []*askTracer, tracers []*recordingTracer, idx map[peer.ID]int, compAt []time.Duration, start time.Time) string {
	var n, byIndex, stale, repeat, queued, done, inMesh int
	var ages []float64
	askers := map[peer.ID]bool{}
	for _, i := range set {
		a := ats[i]
		if a == nil {
			continue
		}
		mesh := map[peer.ID]bool{}
		for _, p := range tracers[i].MeshPeerIDs() {
			mesh[p] = true
		}
		a.mu.Lock()
		for _, e := range a.asks {
			n++
			if e.byIndex {
				byIndex++
			}
			if !e.current {
				stale++
			}
			if e.repeat {
				repeat++
			}
			if e.queued {
				queued++
			}
			if j, ok := idx[e.from]; ok && compAt[j] > 0 && e.at.Sub(start) >= compAt[j] {
				done++
			}
			if mesh[e.from] {
				inMesh++
			}
			askers[e.from] = true
			ages = append(ages, float64(e.at.Sub(start)))
		}
		a.mu.Unlock()
	}
	sort.Float64s(ages)
	pct := func(c int) string {
		if n == 0 {
			return "-"
		}
		return fmt.Sprintf("%.0f%%", 100*float64(c)/float64(n))
	}
	var b strings.Builder
	fmt.Fprintf(&b, "asks [%s %d]: ids %d from %d askers; by index %s; other group %s; repeat %s; already queued to the asker %s; asker complete %s; asker in mesh %s; age p50 %s p90 %s",
		name, len(set), n, len(askers), pct(byIndex), pct(stale), pct(repeat), pct(queued), pct(done), pct(inMesh),
		msOf(quantile(ages, 0.5)), msOf(quantile(ages, 0.9)))
	return b.String()
}
