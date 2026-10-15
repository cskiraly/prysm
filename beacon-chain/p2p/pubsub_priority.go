package p2p

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Outbound gossip priority orders for --gossip-topic-priority(-order): groups
// of topic families separated by ">", highest priority first; families not
// listed come after the last group.
var gossipPriorityPresets = map[string]string{
	// Messages on the critical path of block import first, then the aggregated
	// votes, then the unaggregated ones, then the rest.
	"blocks-first": GossipBlockMessage + "," + GossipExecutionPayloadEnvelopeMessage + "," + GossipExecutionPayloadBidMessage +
		">" + GossipDataColumnSidecarMessage + "," + GossipBlobSidecarMessage +
		">" + GossipAggregateAndProofMessage + "," + GossipContributionAndProofMessage + "," + GossipPayloadAttestationMessageMessage +
		">" + GossipAttestationMessage + "," + GossipSyncCommitteeMessage,
	// Votes (small, deadline bound) first, then blocks, then block data.
	"votes-first": GossipAttestationMessage + "," + GossipSyncCommitteeMessage + "," + GossipAggregateAndProofMessage + "," +
		GossipContributionAndProofMessage + "," + GossipPayloadAttestationMessageMessage +
		">" + GossipBlockMessage + "," + GossipExecutionPayloadEnvelopeMessage + "," + GossipExecutionPayloadBidMessage +
		">" + GossipDataColumnSidecarMessage + "," + GossipBlobSidecarMessage,
}

// uplinkPriorityMaxWait bounds how long --gossip-uplink-priority holds back
// gossip to one peer for higher priority gossip to others.
const uplinkPriorityMaxWait = 100 * time.Millisecond

// DefaultGossipPriorityOrder is the order used by --gossip-topic-priority.
const DefaultGossipPriorityOrder = "blocks-first"

// parseGossipPriorityOrder parses a preset name or an order spec into a
// pubsub.TopicPriorityFn.
func parseGossipPriorityOrder(spec string) (pubsub.TopicPriorityFn, error) {
	if preset, ok := gossipPriorityPresets[spec]; ok {
		spec = preset
	}
	prios := make(map[string]int)
	groups := strings.Split(spec, ">")
	if len(groups) > pubsub.MaxTopicPriority {
		return nil, fmt.Errorf("gossip priority order %q has more than %d groups", spec, pubsub.MaxTopicPriority)
	}
	for i, group := range groups {
		for _, family := range strings.Split(group, ",") {
			family = strings.TrimSpace(family)
			if family == "" {
				return nil, fmt.Errorf("empty topic in gossip priority order %q", spec)
			}
			if _, dup := prios[family]; dup {
				return nil, fmt.Errorf("topic %q listed twice in gossip priority order %q", family, spec)
			}
			prios[family] = i
		}
	}
	rest := len(groups)
	return func(topic string) int {
		if prio, ok := prios[topicFamily(topic)]; ok {
			return prio
		}
		return rest
	}, nil
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
