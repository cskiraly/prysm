package gossipsim

import (
	"crypto/sha256"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"google.golang.org/protobuf/proto"
)

// Digest keys a message by its bytes, which is how a traced event is tied back to the segment
// index that produced it. The publisher knows exactly what it published, so a digest table is
// built up front rather than decoding inside the tracer.
type Digest [32]byte

func DigestOf(b []byte) Digest { return sha256.Sum256(b) }

// SendEvent is one outbound message to one peer.
type SendEvent struct {
	At   time.Time
	To   peer.ID
	What Digest
}

// RecvEvent is one inbound message delivered to the application. from matters: the
// path-diversity question is exactly "how many distinct neighbours fed this node", which cannot
// be answered without it.
type RecvEvent struct {
	At   time.Time
	From peer.ID
	What Digest
}

// CompletionStats summarises per-node completion under right-censoring (measurement-plan
// section 15, phase 0). times holds the durations of the nodes that completed; population
// is every non-publisher node, so censored nodes rank above every finite time. A quantile
// is reported only when it is actually reached — never computed over completers alone —
// and the rate is against the experiment deadline, not the harness timeout. max is
// deliberately absent: it is undefined under censoring.
func CompletionStats(times []time.Duration, population int, deadline time.Duration) string {
	sorted := append([]time.Duration(nil), times...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	atDeadline := 0
	for _, d := range sorted {
		if d <= deadline {
			atDeadline++
		}
	}
	q := func(p float64) string {
		idx := int(math.Ceil(p*float64(population))) - 1
		if idx >= 0 && idx < len(sorted) {
			return sorted[idx].Round(time.Millisecond).String()
		}
		return "cens"
	}
	return fmt.Sprintf("p50 %s p90 %s p99 %s rate@%v %d/%d",
		q(0.50), q(0.90), q(0.99), deadline, atDeadline, population)
}

// ControlCounts tallies the control traffic that decides whether recovery is even possible.
// SendRPC and RecvRPC carry control messages alongside data, and a tracer that only looks at
// rpc.Publish cannot see an IHAVE or an IWANT at all.
type ControlCounts struct {
	IhaveSent, IhaveRecv int
	IwantSent, IwantRecv int
	// IWANT entries batch message ids, so entry counts alone cannot decompose an upload
	// into served copies — the flat-entries-growing-bytes puzzle of the mesh-width sweep.
	// These count the ids inside the entries.
	IwantIdsSent, IwantIdsRecv   int
	IdontwantSent, IdontwantRecv int
	GraftSent, PruneSent         int
	// Encoded bytes of the whole control section (IHAVE/IWANT/IDONTWANT/GRAFT/PRUNE),
	// measurement-plan section 13 layer 3. Payload equivalents exclude these; this counter
	// is what lets a report say by how much.
	BytesSent, BytesRecv int
}

// RecordingTracer captures what one node sent and received, with timestamps.
//
// Timestamps come from time.Now inside a synctest bubble, so they are virtual-clock readings and
// comparable across nodes. Counters are guarded because pubsub calls tracer hooks from several
// goroutines.
type RecordingTracer struct {
	// Mu, Sends and Recvs are exported because the raw event log is the measurement: an
	// experiment that needs a shape this file does not compute reads the events directly,
	// under Mu.
	Mu         sync.Mutex
	Sends      []SendEvent
	Recvs      []RecvEvent
	duplicates int
	rejects    int
	recvBytes  int
	sentBytes  int
	// Partial-message bytes are counted separately from rpc.Publish bytes. Variant B's
	// payload never appears in rpc.Publish at all -- it travels in rpc.Partial -- so a tracer
	// that only sums Publish reports zero wire bytes for the whole variant, which reads as
	// "nothing moved" rather than "the tracer cannot see it".
	partialRecvBytes int
	partialSentBytes int
	partialRPCsRecv  int
	partialRPCsSent  int
	// Partial-message bytes split by topic. An experiment running both DAS axes at once cannot
	// read the totals above at all: they sum across every topic the node speaks, so the row
	// share had to be inferred by differencing arms. rpc.Partial carries a TopicID, so the
	// information was always there.
	partialByTopic map[string]partialTopicCounts
	droppedRPCs    int
	undeliverable  int
	control        ControlCounts
	grafted        map[peer.ID]int
	// graftedInTopic is the same bookkeeping split by topic. A multi-topic experiment cannot
	// read grafted at all: it counts distinct peers, so it saturates at the connectivity
	// degree the moment a peer is grafted in any one topic, and a per-topic precondition
	// ("this topic's mesh has formed") becomes unassertable.
	graftedInTopic map[string]map[peer.ID]int
}

// partialTopicCounts is one topic's share of the partial-message traffic.
type partialTopicCounts struct {
	RecvBytes, SentBytes int
	RecvRPCs, SentRPCs   int
}

func NewRecordingTracer() *RecordingTracer {
	return &RecordingTracer{
		grafted:        make(map[peer.ID]int),
		graftedInTopic: make(map[string]map[peer.ID]int),
		partialByTopic: make(map[string]partialTopicCounts),
	}
}

// ResetCounters zeroes everything the run measures, keeping the mesh membership.
//
// For a warm-up arm: traffic sent to grow congestion windows must not be counted as the
// measured payload's cost. grafted and graftedInTopic are deliberately preserved -- the mesh
// really is formed, and re-deriving it would report a settled mesh as empty.
func (r *RecordingTracer) ResetCounters() {
	r.Mu.Lock()
	defer r.Mu.Unlock()
	r.Sends = nil
	r.Recvs = nil
	r.duplicates = 0
	r.rejects = 0
	r.recvBytes = 0
	r.sentBytes = 0
	r.partialRecvBytes = 0
	r.partialSentBytes = 0
	r.partialRPCsRecv = 0
	r.partialRPCsSent = 0
	r.partialByTopic = make(map[string]partialTopicCounts)
	r.droppedRPCs = 0
	r.undeliverable = 0
	r.control = ControlCounts{}
}

// FirstForward is the time this node first passed any traced message on, which is the direct
// pipelining probe: under store-and-forward it cannot precede having the whole payload.
func (r *RecordingTracer) FirstForward(known map[Digest]int) (time.Time, int, bool) {
	r.Mu.Lock()
	defer r.Mu.Unlock()
	for _, s := range r.Sends {
		if idx, ok := known[s.What]; ok {
			return s.At, idx, true
		}
	}
	return time.Time{}, 0, false
}

// ArrivalOrder is the sequence of segment indices as this node first saw each one. Duplicates
// and unknown messages are skipped, so the result is a permutation prefix.
func (r *RecordingTracer) ArrivalOrder(known map[Digest]int) []int {
	r.Mu.Lock()
	defer r.Mu.Unlock()
	seen := make(map[Digest]bool, len(r.Recvs))
	out := make([]int, 0, len(known))
	for _, e := range r.Recvs {
		idx, ok := known[e.What]
		if !ok || seen[e.What] {
			continue
		}
		seen[e.What] = true
		out = append(out, idx)
	}
	return out
}

// SendOrderTo is the sequence of segment indices this node sent to one peer, which is what a
// send-ordering question actually asks about.
func (r *RecordingTracer) SendOrderTo(p peer.ID, known map[Digest]int) []int {
	r.Mu.Lock()
	defer r.Mu.Unlock()
	out := make([]int, 0, len(known))
	seen := make(map[Digest]bool, len(known))
	for _, s := range r.Sends {
		if s.To != p || seen[s.What] {
			continue
		}
		if idx, ok := known[s.What]; ok {
			seen[s.What] = true
			out = append(out, idx)
		}
	}
	return out
}

// Counts returns the message and byte tallies.
//
// The byte counters sum Message.Data only. They exclude protobuf framing, control traffic, QUIC
// headers, ACKs and retransmissions, so they are "gossip payload bytes" and not wire bytes. Any
// claim about total traffic needs a link-level counter instead.
// PartialCounts returns the partial-message traffic: bytes and RPCs, each way.
func (r *RecordingTracer) PartialCounts() (recvBytes, sentBytes, recvRPCs, sentRPCs int) {
	r.Mu.Lock()
	defer r.Mu.Unlock()
	return r.partialRecvBytes, r.partialSentBytes, r.partialRPCsRecv, r.partialRPCsSent
}

func (r *RecordingTracer) Counts() (duplicates, rejects, recvBytes, sentBytes int) {
	r.Mu.Lock()
	defer r.Mu.Unlock()
	return r.duplicates, r.rejects, r.recvBytes, r.sentBytes
}

func (r *RecordingTracer) MeshPeers() int {
	r.Mu.Lock()
	defer r.Mu.Unlock()
	n := 0
	for _, v := range r.grafted {
		if v > 0 {
			n++
		}
	}
	return n
}

// MeshPeerIDs returns the peers currently grafted in any topic.
func (r *RecordingTracer) MeshPeerIDs() []peer.ID {
	r.Mu.Lock()
	defer r.Mu.Unlock()
	out := make([]peer.ID, 0, len(r.grafted))
	for p, v := range r.grafted {
		if v > 0 {
			out = append(out, p)
		}
	}
	return out
}

// MeshSlots returns the total (peer, topic) mesh membership count, which is what a
// multi-topic experiment must assert on: MeshPeers saturates at the connectivity degree the
// moment a peer is grafted in any topic.
func (r *RecordingTracer) MeshSlots() int {
	r.Mu.Lock()
	defer r.Mu.Unlock()
	n := 0
	for _, v := range r.grafted {
		if v > 0 {
			n += v
		}
	}
	return n
}

// PartialCountsForTopic is PartialCounts for one topic. This is what a two-axis experiment has to
// read: the totals sum across every topic, so a row-versus-column split taken from them is a
// difference of arms rather than a measurement.
func (r *RecordingTracer) PartialCountsForTopic(topic string) (recvBytes, sentBytes, recvRPCs, sentRPCs int) {
	r.Mu.Lock()
	defer r.Mu.Unlock()
	counts := r.partialByTopic[topic]

	return counts.RecvBytes, counts.SentBytes, counts.RecvRPCs, counts.SentRPCs
}

// PartialCountsByTopicPrefix sums the partial-message counters over every topic whose name
// contains the given substring, which is how an experiment separates the DAS axes: the row topics
// share `data_row_` and the column topics `data_column_sidecar_`.
func (r *RecordingTracer) PartialCountsByTopicPrefix(substr string) (recvBytes, sentBytes, recvRPCs, sentRPCs int) {
	r.Mu.Lock()
	defer r.Mu.Unlock()
	for topic, counts := range r.partialByTopic {
		if !strings.Contains(topic, substr) {
			continue
		}
		recvBytes += counts.RecvBytes
		sentBytes += counts.SentBytes
		recvRPCs += counts.RecvRPCs
		sentRPCs += counts.SentRPCs
	}

	return recvBytes, sentBytes, recvRPCs, sentRPCs
}

// MeshPeersInTopic is the mesh size for one topic. This is what a multi-topic experiment must
// assert on; see graftedInTopic.
func (r *RecordingTracer) MeshPeersInTopic(topic string) int {
	r.Mu.Lock()
	defer r.Mu.Unlock()
	n := 0
	for _, v := range r.graftedInTopic[topic] {
		if v > 0 {
			n++
		}
	}

	return n
}

func (r *RecordingTracer) SendRPC(rpc *pubsub.RPC, p peer.ID) {
	if rpc == nil {
		return
	}
	now := time.Now()
	r.Mu.Lock()
	defer r.Mu.Unlock()
	r.countControl(rpc, true)
	for _, m := range rpc.Publish {
		r.Sends = append(r.Sends, SendEvent{At: now, To: p, What: DigestOf(m.Data)})
		r.sentBytes += len(m.Data)
	}
	if rpc.Partial != nil {
		bytes := len(rpc.Partial.PartialMessage) + len(rpc.Partial.PartsMetadata)
		r.partialRPCsSent++
		r.partialSentBytes += bytes
		counts := r.partialByTopic[rpc.Partial.GetTopicID()]
		counts.SentBytes += bytes
		counts.SentRPCs++
		r.partialByTopic[rpc.Partial.GetTopicID()] = counts
	}
}

func (r *RecordingTracer) RecvRPC(rpc *pubsub.RPC) {
	if rpc == nil {
		return
	}
	r.Mu.Lock()
	defer r.Mu.Unlock()
	r.countControl(rpc, false)
	for _, m := range rpc.Publish {
		r.recvBytes += len(m.Data)
	}
	if rpc.Partial != nil {
		bytes := len(rpc.Partial.PartialMessage) + len(rpc.Partial.PartsMetadata)
		r.partialRPCsRecv++
		r.partialRecvBytes += bytes
		counts := r.partialByTopic[rpc.Partial.GetTopicID()]
		counts.RecvBytes += bytes
		counts.RecvRPCs++
		r.partialByTopic[rpc.Partial.GetTopicID()] = counts
	}
}

func (r *RecordingTracer) DeliverMessage(msg *pubsub.Message) {
	now := time.Now()
	r.Mu.Lock()
	defer r.Mu.Unlock()
	r.Recvs = append(r.Recvs, RecvEvent{At: now, From: msg.ReceivedFrom, What: DigestOf(msg.Data)})
}

// Contributors Counts how many distinct neighbours delivered at least one traced message to this
// node, and how many each supplied. This is the path-diversity metric.
func (r *RecordingTracer) Contributors(known map[Digest]int) map[peer.ID]int {
	r.Mu.Lock()
	defer r.Mu.Unlock()
	out := make(map[peer.ID]int)
	seen := make(map[Digest]bool, len(known))
	for _, e := range r.Recvs {
		if _, ok := known[e.What]; !ok || seen[e.What] {
			continue
		}
		seen[e.What] = true
		out[e.From]++
	}
	return out
}

// ControlSeen returns the control-message tallies.
func (r *RecordingTracer) ControlSeen() ControlCounts {
	r.Mu.Lock()
	defer r.Mu.Unlock()
	return r.control
}

// Losses returns queue-drop counters, which would otherwise hide a dropped segment behind a
// result that merely looks slow.
func (r *RecordingTracer) Losses() (droppedRPCs, undeliverable int) {
	r.Mu.Lock()
	defer r.Mu.Unlock()
	return r.droppedRPCs, r.undeliverable
}

// countControl tallies control messages on an RPC in either direction.
func (r *RecordingTracer) countControl(rpc *pubsub.RPC, outbound bool) {
	if rpc == nil || rpc.Control == nil {
		return
	}
	c := rpc.Control
	add := func(sent *int, recv *int, n int) {
		if n == 0 {
			return
		}
		if outbound {
			*sent += n
		} else {
			*recv += n
		}
	}
	add(&r.control.IhaveSent, &r.control.IhaveRecv, len(c.Ihave))
	add(&r.control.IwantSent, &r.control.IwantRecv, len(c.Iwant))
	iwantIds := 0
	for _, iw := range c.Iwant {
		iwantIds += len(iw.GetMessageIDs())
	}
	add(&r.control.IwantIdsSent, &r.control.IwantIdsRecv, iwantIds)
	add(&r.control.IdontwantSent, &r.control.IdontwantRecv, len(c.Idontwant))
	add(&r.control.BytesSent, &r.control.BytesRecv, proto.Size(c))
	if outbound {
		r.control.GraftSent += len(c.Graft)
		r.control.PruneSent += len(c.Prune)
	}
}

func (r *RecordingTracer) DuplicateMessage(*pubsub.Message) {
	r.Mu.Lock()
	defer r.Mu.Unlock()
	r.duplicates++
}

func (r *RecordingTracer) RejectMessage(*pubsub.Message, string) {
	r.Mu.Lock()
	defer r.Mu.Unlock()
	r.rejects++
}

func (r *RecordingTracer) Graft(p peer.ID, topic string) {
	r.Mu.Lock()
	defer r.Mu.Unlock()
	r.grafted[p]++
	inTopic, ok := r.graftedInTopic[topic]
	if !ok {
		inTopic = make(map[peer.ID]int)
		r.graftedInTopic[topic] = inTopic
	}
	inTopic[p]++
}

func (r *RecordingTracer) Prune(p peer.ID, topic string) {
	r.Mu.Lock()
	defer r.Mu.Unlock()
	r.grafted[p]--
	if inTopic, ok := r.graftedInTopic[topic]; ok {
		inTopic[p]--
	}
}

func (r *RecordingTracer) OnNewOutboundStream(peer.ID, protocol.ID) {}

func (r *RecordingTracer) OnClosedOutboundStream(peer.ID) {}

func (r *RecordingTracer) Join(string) {}

func (r *RecordingTracer) Leave(string) {}

func (r *RecordingTracer) ValidateMessage(*pubsub.Message) {}

func (r *RecordingTracer) ThrottlePeer(peer.ID) {}

// DropRPC and UndeliverableMessage are queue Losses. Leaving them empty hides a dropped segment
// behind a result that merely looks slow, so both are counted.
func (r *RecordingTracer) DropRPC(*pubsub.RPC, peer.ID) {
	r.Mu.Lock()
	defer r.Mu.Unlock()
	r.droppedRPCs++
}

func (r *RecordingTracer) UndeliverableMessage(*pubsub.Message) {
	r.Mu.Lock()
	defer r.Mu.Unlock()
	r.undeliverable++
}
