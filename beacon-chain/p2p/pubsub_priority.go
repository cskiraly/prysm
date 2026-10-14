package p2p

import (
	"strconv"
	"strings"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Outbound gossip priorities, used with --gossip-topic-priority: lower values
// are sent first. Messages on the critical path of block import come first,
// then the aggregated votes, then the unaggregated ones, then the rest.
const (
	prioBlock = iota
	prioBlockData
	prioAggregate
	prioVote
	prioOther
)

var topicFamilyPriority = map[string]int{
	GossipBlockMessage:                     prioBlock,
	GossipExecutionPayloadEnvelopeMessage:  prioBlock,
	GossipExecutionPayloadBidMessage:       prioBlock,
	GossipDataColumnSidecarMessage:         prioBlockData,
	GossipBlobSidecarMessage:               prioBlockData,
	GossipAggregateAndProofMessage:         prioAggregate,
	GossipContributionAndProofMessage:      prioAggregate,
	GossipPayloadAttestationMessageMessage: prioAggregate,
	GossipAttestationMessage:               prioVote,
	GossipSyncCommitteeMessage:             prioVote,
}

var (
	outboundQueueWait = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "p2p_gossip_outbound_queue_wait_seconds",
		Help:    "Time gossipsub RPCs wait in a peer's outbound queue, by topic family and queue class.",
		Buckets: []float64{0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 4, 8},
	}, []string{"topic", "class"})
	outboundWriteTime = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "p2p_gossip_outbound_write_seconds",
		Help:    "Time a gossipsub RPC stream write takes, by topic family.",
		Buckets: []float64{0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 4, 8},
	}, []string{"topic"})
	outboundBytes = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "p2p_gossip_outbound_bytes_total",
		Help: "Bytes of gossipsub RPCs written to peers, by topic family.",
	}, []string{"topic"})
)

// topicFamily returns the message name of a gossip topic without its fork
// digest, encoding and subnet, e.g. "data_column_sidecar" for
// /eth2/<digest>/data_column_sidecar_12/ssz_snappy, or "control" for "".
func topicFamily(topic string) string {
	if topic == "" {
		return "control"
	}
	parts := strings.Split(topic, "/")
	if len(parts) < 4 {
		return "unknown"
	}
	name := parts[3]
	if i := strings.LastIndexByte(name, '_'); i >= 0 && isDigits(name[i+1:]) {
		name = name[:i]
	}
	return name
}

func isDigits(s string) bool {
	if s == "" {
		return false
	}
	for _, c := range s {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

// gossipTopicPriority is the pubsub.TopicPriorityFn of --gossip-topic-priority.
func gossipTopicPriority(topic string) int {
	if prio, ok := topicFamilyPriority[topicFamily(topic)]; ok {
		return prio
	}
	return prioOther
}

var queueClassLabels = func() []string {
	labels := make([]string, pubsub.MaxTopicPriority+2)
	for i := range labels {
		labels[i] = strconv.Itoa(i)
	}
	return labels
}()

func observeOutboundRPC(info pubsub.OutboundRPCInfo) {
	family := topicFamily(info.Topic)
	class := "other"
	if info.Class >= 0 && info.Class < len(queueClassLabels) {
		class = queueClassLabels[info.Class]
	}
	outboundQueueWait.WithLabelValues(family, class).Observe(info.QueueWait.Seconds())
	outboundWriteTime.WithLabelValues(family).Observe(info.WriteTime.Seconds())
	outboundBytes.WithLabelValues(family).Add(float64(info.Size))
}
