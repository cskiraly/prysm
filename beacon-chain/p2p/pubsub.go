package p2p

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/OffchainLabs/prysm/v7/beacon-chain/p2p/encoder"
	"github.com/OffchainLabs/prysm/v7/beacon-chain/p2p/internal/segmentgossip"
	p2ptypes "github.com/OffchainLabs/prysm/v7/beacon-chain/p2p/types"
	"github.com/OffchainLabs/prysm/v7/cmd/beacon-chain/flags"
	"github.com/OffchainLabs/prysm/v7/config/features"
	"github.com/OffchainLabs/prysm/v7/config/params"
	pbrpc "github.com/OffchainLabs/prysm/v7/proto/prysm/v1alpha1"
	"github.com/OffchainLabs/prysm/v7/time/slots"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pubsubpb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/pkg/errors"
)

const (
	// overlay parameters
	gossipSubD   = 8  // topic stable mesh target count
	gossipSubDlo = 6  // topic stable mesh low watermark
	gossipSubDhi = 12 // topic stable mesh high watermark

	// gossip parameters
	gossipSubMcacheLen    = 6   // number of windows to retain full messages in cache for `IWANT` responses
	gossipSubMcacheGossip = 3   // number of windows to gossip about
	gossipSubSeenTTL      = 768 // number of seconds to retain message IDs ( 2 epochs)

	// fanout ttl
	gossipSubFanoutTTL = 60000000000 // TTL for fanout maps for topics we are not subscribed to but have published to, in nano seconds

	// heartbeat interval
	gossipSubHeartbeatInterval = 700 * time.Millisecond // frequency of heartbeat, milliseconds

	// misc
	rSubD = 8 // random gossip target
)

// ErrInvalidTopic aliases p2ptypes.ErrInvalidTopic, which lives in the leaf types
// package so p2p subpackages can use it without an import cycle.
var ErrInvalidTopic = p2ptypes.ErrInvalidTopic

// JoinTopic will join PubSub topic, if not already joined.
func (s *Service) JoinTopic(topic string, opts ...pubsub.TopicOpt) (*pubsub.Topic, error) {
	s.joinedTopicsLock.Lock()
	defer s.joinedTopicsLock.Unlock()

	if _, ok := s.joinedTopics[topic]; !ok {
		if strings.Contains(topic, GossipDataColumnSidecarMessage) && s.partialColumnBroadcaster != nil {
			opts = append(opts, pubsub.RequestPartialMessages())
			log.WithField("topic", topic).Debug("Joining data column sidecar topic with partial messages")
		}

		topicHandle, err := s.pubsub.Join(topic, opts...)
		if err != nil {
			return nil, err
		}
		s.joinedTopics[topic] = topicHandle
	}

	return s.joinedTopics[topic], nil
}

// LeaveTopic closes topic and removes corresponding handler from list of joined topics.
// This method will return error if there are outstanding event handlers or subscriptions.
func (s *Service) LeaveTopic(topic string) error {
	s.joinedTopicsLock.Lock()
	defer s.joinedTopicsLock.Unlock()

	if t, ok := s.joinedTopics[topic]; ok {
		if err := t.Close(); err != nil {
			return err
		}
		delete(s.joinedTopics, topic)
	}
	return nil
}

// PublishToTopic joins (if necessary) and publishes a message to a PubSub topic.
func (s *Service) PublishToTopic(ctx context.Context, topic string, data []byte, opts ...pubsub.PubOpt) error {
	topicHandle, err := s.JoinTopic(topic)
	if err != nil {
		return err
	}

	// Wait for at least 1 peer to be available to receive the published message.
	for {
		if len(topicHandle.ListPeers()) > 0 || flags.Get().MinimumSyncPeers == 0 {
			return topicHandle.Publish(ctx, data, opts...)
		}
		select {
		case <-ctx.Done():
			return errors.Wrapf(ctx.Err(), "unable to find requisite number of peers for topic %s, 0 peers found to publish to", topic)
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// addToBatch joins (if necessary) a topic and adds the message to a message batch.
func (s *Service) addToBatch(ctx context.Context, batch *pubsub.MessageBatch, topic string, data []byte, opts ...pubsub.PubOpt) error {
	topicHandle, err := s.JoinTopic(topic)
	if err != nil {
		return fmt.Errorf("joining topic: %w", err)
	}

	// Wait for at least 1 peer to be available to receive the published message.
	for {
		if flags.Get().MinimumSyncPeers == 0 || len(topicHandle.ListPeers()) > 0 {
			return topicHandle.AddToBatch(ctx, batch, data, opts...)
		}
		select {
		case <-ctx.Done():
			return errors.Wrapf(ctx.Err(), "unable to find requisite number of peers for topic %s, 0 peers found to publish to", topic)
		case <-time.After(100 * time.Millisecond):
			// reenter the for loop after 100ms
		}
	}
}

// SubscribeToTopic joins (if necessary) and subscribes to PubSub topic.
func (s *Service) SubscribeToTopic(topic string, opts ...pubsub.SubOpt) (*pubsub.Subscription, error) {
	s.awaitStateInitialized() // Genesis time and genesis validators root are required to subscribe.

	topicHandle, err := s.JoinTopic(topic)
	if err != nil {
		return nil, err
	}
	scoringParams, err := s.topicScoreParams(topic)
	if err != nil {
		return nil, err
	}

	if scoringParams != nil {
		if err := topicHandle.SetScoreParams(scoringParams); err != nil {
			return nil, err
		}
		logGossipParameters(topic, scoringParams)
	}
	return topicHandle.Subscribe(opts...)
}

// peerInspector will scrape all the relevant scoring data and add it to our
// peer handler.
func (s *Service) peerInspector(peerMap map[peer.ID]*pubsub.PeerScoreSnapshot) {
	// Iterate through all the connected peers and through any of their
	// relevant topics.
	for pid, snap := range peerMap {
		s.peers.Scorers().GossipScorer().SetGossipData(pid, snap.Score,
			snap.BehaviourPenalty, convertTopicScores(snap.Topics))
	}
}

// pubsubOptions creates a list of options to configure our router with. It fails when segmented
// payload gossip is on and the node's setting breaks one of the segment path's conditions.
func (s *Service) pubsubOptions() ([]pubsub.Option, error) {
	filt := pubsub.NewAllowlistSubscriptionFilter(s.allTopicStrings()...)
	filt = pubsub.WrapLimitSubscriptionFilter(filt, pubsubSubscriptionRequestLimit)
	psOpts := []pubsub.Option{
		pubsub.WithMessageSignaturePolicy(pubsub.StrictNoSign),
		pubsub.WithNoAuthor(),
		pubsub.WithMessageIdFn(func(pmsg *pubsubpb.Message) string {
			return MsgID(pmsg)
		}),
		pubsub.WithSubscriptionFilter(filt),
		pubsub.WithPeerOutboundQueueSize(int(s.cfg.QueueSize)),
		pubsub.WithMaxMessageSize(int(MaxMessageSize())), // lint:ignore uintcast -- Max Message Size is a config value and is naturally bounded by networking limitations.
		pubsub.WithValidateQueueSize(int(s.cfg.QueueSize)),
		pubsub.WithPeerScore(peerScoringParams(s.cfg.IPColocationWhitelist)),
		pubsub.WithPeerScoreInspect(s.peerInspector, time.Minute),
		pubsub.WithGossipSubParams(pubsubGossipParam()),
		pubsub.WithRawTracer(&gossipTracer{host: s.host, allowedTopics: filt}),
	}

	if len(s.cfg.StaticPeers) > 0 {
		directPeersAddrInfos, err := parsePeersEnr(s.cfg.StaticPeers)
		if err != nil {
			log.WithError(err).Error("Could not add direct peer option")
			if features.Get().EnableSegmentedPayloadGossip {
				// Returning here would start the node without the segment path and its checks.
				return nil, errors.Wrap(err, "static peers")
			}
			return psOpts, nil
		}
		psOpts = append(psOpts, pubsub.WithDirectPeers(directPeersAddrInfos))
	}
	if s.partialColumnBroadcaster != nil {
		psOpts = s.partialColumnBroadcaster.AppendPubSubOpts(psOpts)
	}
	if features.Get().EnableSegmentedPayloadGossip {
		seacastOpts, err := s.seacastOptions(filt)
		if err != nil {
			return nil, err
		}
		psOpts = append(psOpts, seacastOpts...)
	}

	return psOpts, nil
}

// seacastOptions are the router's options for SEACAST's payload path on the segment topic. They
// are built only from a setting that segmentgossip.CheckSetting passes, so a node whose setting
// breaks one of the path's conditions does not start.
func (s *Service) seacastOptions(filt pubsub.SubscriptionFilter) ([]pubsub.Option, error) {
	setting, typ := s.seacastSetting(filt)
	if err := segmentgossip.CheckSetting(setting); err != nil {
		return nil, err
	}
	s.segmentPayloadType = typ
	s.seacastMetrics = &segmentgossip.PathMetrics{} // exported once the router is built (NewService)
	return segmentgossip.RouterOptions(setting, typ, gossipSubHeartbeatInterval, s.seacastMetrics), nil
}

// seacastSetting is the node's setting for the segment path, and the payload type it runs: the
// segment topic at every fork digest of the schedule, the subscription filter that must admit them,
// cancel on, and the fork's default limits, which bound no group a peer names.
func (s *Service) seacastSetting(filt pubsub.SubscriptionFilter) (segmentgossip.Setting, *segmentgossip.PayloadType) {
	var topics []string
	for _, t := range s.allTopicStrings() {
		if isSegmentTopic(t) {
			topics = append(topics, t)
		}
	}
	uplink := features.Get().SeacastUplinkMbps
	typ := segmentgossip.NewPayloadType(segmentgossip.PayloadConfig{
		Topics:       topics,
		CurrentTopic: s.currentSegmentTopic,
		CurrentSlot:  s.seacastCurrentSlot,
		UplinkMbps:   uplink,
	})
	return segmentgossip.Setting{
		SegmentTopics: topics,
		Allowed:       filt.CanSubscribe,
		TypeTopics:    typ.Topics(),
		Params:        typ.Params(),
		Limits:        pubsub.DefaultSeacastLimits,
		Cancel:        true,
		UplinkMbps:    uplink,
	}, typ
}

// currentSegmentTopic is the segment topic at the current fork digest, empty before the node knows
// its genesis.
func (s *Service) currentSegmentTopic() string {
	digest, err := s.currentForkDigest()
	if err != nil {
		return ""
	}
	return fmt.Sprintf(ExecutionPayloadSegmentTopicFormat, digest) + sszEnc.ProtocolSuffix()
}

// seacastCurrentSlot is the node's current slot, 0 before it knows its genesis.
func (s *Service) seacastCurrentSlot() uint64 {
	if !s.isInitialized() {
		return 0
	}
	return uint64(slots.CurrentSlot(s.genesisTime))
}

// parsePeersEnr takes a list of raw ENRs and converts them into a list of AddrInfos.
func parsePeersEnr(peers []string) ([]peer.AddrInfo, error) {
	addrs, err := PeersFromStringAddrs(peers)
	if err != nil {
		return nil, fmt.Errorf("cannot convert peers raw ENRs into multiaddresses: %w", err)
	}
	if len(addrs) == 0 {
		return nil, fmt.Errorf("converting peers raw ENRs into multiaddresses resulted in an empty list")
	}
	directAddrInfos, err := peer.AddrInfosFromP2pAddrs(addrs...)
	if err != nil {
		return nil, fmt.Errorf("cannot convert peers multiaddresses into AddrInfos: %w", err)
	}
	return directAddrInfos, nil
}

// creates a custom gossipsub parameter set.
func pubsubGossipParam() pubsub.GossipSubParams {
	gParams := pubsub.DefaultGossipSubParams()
	gParams.Dlo = gossipSubDlo
	gParams.D = gossipSubD
	gParams.Dhi = gossipSubDhi
	gParams.HeartbeatInterval = gossipSubHeartbeatInterval
	gParams.HistoryLength = gossipSubMcacheLen
	gParams.HistoryGossip = gossipSubMcacheGossip
	if features.Get().EnableSegmentedPayloadGossip {
		gParams.MaxIHaveMessages = segmentgossip.MaxIHaveMessages
	}
	return gParams
}

// We have to unfortunately set this globally in order
// to configure our message id time-cache rather than instantiating
// it with a router instance.
func setPubSubParameters() {
	seenTtl := 2 * params.EpochsDuration(1, params.BeaconConfig())
	pubsub.TimeCacheDuration = seenTtl
}

// convert from libp2p's internal schema to a compatible prysm protobuf format.
func convertTopicScores(topicMap map[string]*pubsub.TopicScoreSnapshot) map[string]*pbrpc.TopicScoreSnapshot {
	newMap := make(map[string]*pbrpc.TopicScoreSnapshot, len(topicMap))
	for t, s := range topicMap {
		newMap[t] = &pbrpc.TopicScoreSnapshot{
			TimeInMesh:               uint64(s.TimeInMesh.Milliseconds()),
			FirstMessageDeliveries:   float32(s.FirstMessageDeliveries),
			MeshMessageDeliveries:    float32(s.MeshMessageDeliveries),
			InvalidMessageDeliveries: float32(s.InvalidMessageDeliveries),
		}
	}
	return newMap
}

// ExtractGossipDigest extracts the relevant fork digest from the gossip topic.
func ExtractGossipDigest(topic string) ([4]byte, error) {
	return p2ptypes.ExtractGossipDigest(topic)
}

// MaxMessageSize returns the maximum allowed compressed message size.
//
// Spec pseudocode definition:
// def max_message_size() -> uint64:
//
//	# Allow 1024 bytes for framing and encoding overhead but at least 1MiB in case MAX_PAYLOAD_SIZE is small.
//	return max(max_compressed_len(MAX_PAYLOAD_SIZE) + 1024, 1024 * 1024)
func MaxMessageSize() uint64 {
	return max(encoder.MaxCompressedLen(params.BeaconConfig().MaxPayloadSize)+1024, 1024*1024)
}
