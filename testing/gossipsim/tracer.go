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
	pubsub_pb "github.com/libp2p/go-libp2p-pubsub/pb"
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

// CompletionStats summarises per-node completion under right-censoring. times holds the
// durations of the nodes that completed; population
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
	// Encoded bytes of the whole control section (IHAVE/IWANT/IDONTWANT/GRAFT/PRUNE).
	// Payload equivalents exclude these; this counter
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
	Mu    sync.Mutex
	Sends []SendEvent
	Recvs []RecvEvent
	// Dups is every later sighting of a message already delivered, with the sender and the
	// instant, so that a duplicate can be read against the first copy: how far behind it
	// landed, and how much of the payload the receiver still lacked when it did. The
	// duplicates counter and this log move together and reset together.
	Dups       []RecvEvent
	duplicates int
	rejects    int
	recvBytes  int
	recvFrames int
	sentBytes  int
	// Partial-message bytes are counted separately from rpc.Publish bytes. Variant B's
	// payload never appears in rpc.Publish at all -- it travels in rpc.Partial -- so a tracer
	// that only sums Publish reports zero wire bytes for the whole variant, which reads as
	// "nothing moved" rather than "the tracer cannot see it".
	//
	// Payload and signalling are counted apart. rpc.Partial carries both a PartialMessage
	// (the data) and a PartsMetadata (the have/want bitmaps), and summing them into one
	// figure put the two families on different bases: a variant whose signalling rides in
	// PartsMetadata reported zero control bytes and hid its signalling inside its copies,
	// while a variant signalling through IHAVE/IWANT had that cost reported separately and
	// excluded from its copies. Any claim that signalling collapsed is unreadable until the
	// two are split, so they are.
	partialRecvBytes     int
	partialSentBytes     int
	partialMetaRecvBytes int
	partialMetaSentBytes int
	partialRPCsRecv      int
	partialRPCsSent      int
	// Partial RPCs carrying a PartsMetadata, either direction. A metadata byte count alone
	// cannot say whether signalling piggybacked on data frames or paid for its own.
	partialMetaRPCsRecv int
	partialMetaRPCsSent int
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
	// firstAnnounce is the first time each message id was named by an inbound control message:
	// an IHAVE, an IWANT or an IDONTWANT. Hearing that a message exists is a different event
	// from receiving it, and on a topic whose ids carry the segment index in the clear it is
	// the earlier one -- which is what a diffusion curve of "knows about" rather than "holds"
	// is made of. Only the first mention is kept, so the map is bounded by the number of
	// distinct messages, not by the control traffic.
	//
	// nil unless TrackAnnounce is called: a cell that does not ask for the curve must not pay
	// a map write per announced id, so that its scheduling stays exactly what it was.
	firstAnnounce map[string]time.Time
	// partialObserve decodes one inbound rpc.Partial into the segments it names. nil unless
	// TrackPartialSegments is called.
	partialObserve PartialObserveFn
	// firstPartialGot is first delivery for segments that arrive inside rpc.Partial. They
	// never reach DeliverMessage, so FirstDelivery cannot see them at all.
	firstPartialGot map[string]time.Time
}

// PartialObserveFn decodes one inbound partial-message extension into the segments it names:
// got is what the frame carried, heard is what its bitmap says the sender holds. Either may be
// empty. The tracer is generic -- the encoding of a partial message and of PartsMetadata, and
// the mapping from a bit index to a segment's identity, belong to the experiment -- so this is
// supplied rather than decoded here. Called under the tracer's lock, so it must not call back
// into the tracer.
type PartialObserveFn func(topicID string, groupID, partialMessage, partsMetadata []byte) (got, heard []string)

// partialTopicCounts is one topic's share of the partial-message traffic. Payload bytes and
// metadata (bitmap) bytes are held apart for the reason given on partialMetaRecvBytes.
type partialTopicCounts struct {
	RecvBytes, SentBytes         int
	RecvMetaBytes, SentMetaBytes int
	RecvRPCs, SentRPCs           int
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
	r.Dups = nil
	r.duplicates = 0
	r.rejects = 0
	r.recvBytes = 0
	r.recvFrames = 0
	r.sentBytes = 0
	r.partialRecvBytes = 0
	r.partialSentBytes = 0
	r.partialMetaRecvBytes = 0
	r.partialMetaSentBytes = 0
	r.partialRPCsRecv = 0
	r.partialRPCsSent = 0
	r.partialMetaRPCsRecv = 0
	r.partialMetaRPCsSent = 0
	r.partialByTopic = make(map[string]partialTopicCounts)
	r.droppedRPCs = 0
	r.undeliverable = 0
	r.control = ControlCounts{}
	if r.firstAnnounce != nil {
		r.firstAnnounce = make(map[string]time.Time)
	}
	if r.firstPartialGot != nil {
		r.firstPartialGot = make(map[string]time.Time)
	}
}

// TrackAnnounce starts recording the first inbound control mention of each message id. Call it
// before the run; it costs one map write per id first seen and nothing per id after that.
func (r *RecordingTracer) TrackAnnounce() {
	r.Mu.Lock()
	defer r.Mu.Unlock()
	if r.firstAnnounce == nil {
		r.firstAnnounce = make(map[string]time.Time)
	}
}

// TrackPartialSegments extends the diffusion instrument to the partial-message path, which is
// invisible to everything TrackAnnounce and FirstDelivery read. Implies TrackAnnounce.
//
// Two separate blind spots, not one. A segment carried in rpc.Partial never reaches
// DeliverMessage, so *first got* is missing; and a variant signalling with bitmaps sends no
// IHAVE, so *first heard* is missing too. A curve built without this reports that nobody ever
// heard of anything and nobody ever received anything, which reads as a triumph and is an
// artefact.
//
// On the heard side, only the *have* bitmap should count, and it is the supplied fn's job to
// honour that. A want bitmap is not evidence that a segment exists: a standing want defaults to
// everything on first contact, so counting want bits would mark every segment heard the moment
// a peer connects -- the same artefact with the sign reversed, which is why the rule differs
// from the control path's, where an inbound IWANT does count.
func (r *RecordingTracer) TrackPartialSegments(fn PartialObserveFn) {
	r.Mu.Lock()
	defer r.Mu.Unlock()
	if r.firstAnnounce == nil {
		r.firstAnnounce = make(map[string]time.Time)
	}
	if r.firstPartialGot == nil {
		r.firstPartialGot = make(map[string]time.Time)
	}
	r.partialObserve = fn
}

// FirstPartialGot returns, per segment identity, when a frame carrying it first arrived. Empty
// unless TrackPartialSegments was called.
func (r *RecordingTracer) FirstPartialGot() map[string]time.Time {
	r.Mu.Lock()
	defer r.Mu.Unlock()
	out := make(map[string]time.Time, len(r.firstPartialGot))
	for id, at := range r.firstPartialGot {
		out[id] = at
	}
	return out
}

// FirstAnnounce returns a copy of the first inbound control mention of each message id. It is
// empty unless TrackAnnounce was called.
func (r *RecordingTracer) FirstAnnounce() map[string]time.Time {
	r.Mu.Lock()
	defer r.Mu.Unlock()
	out := make(map[string]time.Time, len(r.firstAnnounce))
	for id, at := range r.firstAnnounce {
		out[id] = at
	}
	return out
}

// FirstDelivery returns, per message digest, when this node was first delivered it. Duplicates
// do not appear: pubsub delivers a message to the application once, and later sightings arrive
// through DuplicateMessage, which records them in Dups.
func (r *RecordingTracer) FirstDelivery() map[Digest]time.Time {
	r.Mu.Lock()
	defer r.Mu.Unlock()
	out := make(map[Digest]time.Time, len(r.Recvs))
	for _, e := range r.Recvs {
		if _, seen := out[e.What]; !seen {
			out[e.What] = e.At
		}
	}
	return out
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

// PartialMetaCounts returns the bitmap-signalling traffic carried in rpc.Partial: bytes and
// the RPCs that carried any, each way. This is the partial path's analogue of
// ControlCounts.Bytes*, and it is what makes a bitmap variant's signalling cost comparable with
// an IHAVE/IWANT variant's. PartialCounts excludes every byte counted here.
func (r *RecordingTracer) PartialMetaCounts() (recvBytes, sentBytes, recvRPCs, sentRPCs int) {
	r.Mu.Lock()
	defer r.Mu.Unlock()
	return r.partialMetaRecvBytes, r.partialMetaSentBytes, r.partialMetaRPCsRecv, r.partialMetaRPCsSent
}

// PartialMetaCountsByTopicPrefix is PartialMetaCounts over every topic whose name contains
// substr, matching PartialCountsByTopicPrefix.
func (r *RecordingTracer) PartialMetaCountsByTopicPrefix(substr string) (recvBytes, sentBytes int) {
	r.Mu.Lock()
	defer r.Mu.Unlock()
	for topic, counts := range r.partialByTopic {
		if !strings.Contains(topic, substr) {
			continue
		}
		recvBytes += counts.RecvMetaBytes
		sentBytes += counts.SentMetaBytes
	}

	return recvBytes, sentBytes
}

func (r *RecordingTracer) Counts() (duplicates, rejects, recvBytes, sentBytes int) {
	r.Mu.Lock()
	defer r.Mu.Unlock()
	return r.duplicates, r.rejects, r.recvBytes, r.sentBytes
}

// RecvFrames is the data frames received in rpc.Publish, first copies and duplicates alike, and their data bytes.
func (r *RecordingTracer) RecvFrames() (frames, bytes int) {
	r.Mu.Lock()
	defer r.Mu.Unlock()
	return r.recvFrames, r.recvBytes
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
		payload := len(rpc.Partial.PartialMessage)
		meta := len(rpc.Partial.PartsMetadata)
		r.partialRPCsSent++
		r.partialSentBytes += payload
		r.partialMetaSentBytes += meta
		counts := r.partialByTopic[rpc.Partial.GetTopicID()]
		counts.SentBytes += payload
		counts.SentMetaBytes += meta
		counts.SentRPCs++
		r.partialByTopic[rpc.Partial.GetTopicID()] = counts
		if meta > 0 {
			r.partialMetaRPCsSent++
		}
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
		r.recvFrames++
	}
	if rpc.Partial != nil {
		payload := len(rpc.Partial.PartialMessage)
		meta := len(rpc.Partial.PartsMetadata)
		r.partialRPCsRecv++
		r.partialRecvBytes += payload
		r.partialMetaRecvBytes += meta
		counts := r.partialByTopic[rpc.Partial.GetTopicID()]
		counts.RecvBytes += payload
		counts.RecvMetaBytes += meta
		counts.RecvRPCs++
		r.partialByTopic[rpc.Partial.GetTopicID()] = counts
		if meta > 0 {
			r.partialMetaRPCsRecv++
		}
		r.observePartial(rpc.Partial)
	}
}

// observePartial records what an inbound partial frame carried and what its bitmap named.
// Caller holds Mu.
func (r *RecordingTracer) observePartial(ext *pubsub_pb.PartialMessagesExtension) {
	if r.partialObserve == nil {
		return
	}
	got, heard := r.partialObserve(ext.GetTopicID(), ext.GetGroupID(), ext.GetPartialMessage(), ext.GetPartsMetadata())
	if len(got) == 0 && len(heard) == 0 {
		return
	}
	now := time.Now()
	// A frame that carries a segment is also the strongest possible announcement of it, so a
	// got is a heard as well. Without this a segment that was pushed unasked -- which is the
	// whole point of a standing want -- would have a delivery time and no hearing time.
	for _, id := range got {
		if _, seen := r.firstPartialGot[id]; !seen {
			r.firstPartialGot[id] = now
		}
		if _, seen := r.firstAnnounce[id]; !seen {
			r.firstAnnounce[id] = now
		}
	}
	for _, id := range heard {
		if _, seen := r.firstAnnounce[id]; !seen {
			r.firstAnnounce[id] = now
		}
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
	if !outbound && r.firstAnnounce != nil {
		now := time.Now()
		note := func(ids []string) {
			for _, id := range ids {
				if _, seen := r.firstAnnounce[id]; !seen {
					r.firstAnnounce[id] = now
				}
			}
		}
		for _, h := range c.Ihave {
			note(h.MessageIDs)
		}
		for _, w := range c.Iwant {
			note(w.MessageIDs)
		}
		for _, d := range c.Idontwant {
			note(d.MessageIDs)
		}
	}
	if outbound {
		r.control.GraftSent += len(c.Graft)
		r.control.PruneSent += len(c.Prune)
	}
}

func (r *RecordingTracer) DuplicateMessage(msg *pubsub.Message) {
	now := time.Now()
	r.Mu.Lock()
	defer r.Mu.Unlock()
	r.duplicates++
	r.Dups = append(r.Dups, RecvEvent{At: now, From: msg.ReceivedFrom, What: DigestOf(msg.Data)})
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
