package p2p

// The row topics are registered in allTopics but deliberately absent from gossipTopicMappings,
// because they carry no full messages. That means the mapping-driven tests in
// pubsub_filter_test.go do not cover them, so the coverage they would have given is here instead.

import (
	"fmt"
	"strings"
	"testing"

	"github.com/OffchainLabs/prysm/v7/beacon-chain/p2p/partialdatacolumnbroadcaster"
	"github.com/OffchainLabs/prysm/v7/config/params"
	"github.com/OffchainLabs/prysm/v7/testing/require"
)

// TestDataRowTopicPassesTheSubscriptionFilter is the check that matters most: allTopics feeds
// NewAllowlistSubscriptionFilter, so a row topic missing from it is rejected outright and no
// amount of correct broadcaster code helps.
func TestDataRowTopicPassesTheSubscriptionFilter(t *testing.T) {
	params.SetupTestConfigCleanup(t)
	cfg := params.BeaconConfig().Copy()
	cfg.FuluForkEpoch = 0
	params.OverrideBeaconConfig(cfg)

	// Collect the row subnets that appear, across every fork-schedule entry rather than for one
	// digest: BPOs multiply the topic set, so pinning a single digest would test the schedule
	// rather than the registration.
	s := &Service{cfg: &Config{}}
	subnets := make(map[uint64]bool)
	rowTopics := 0
	for _, topic := range s.allTopicStrings() {
		if !strings.Contains(topic, GossipDataRowMessage+"_") {
			continue
		}
		rowTopics++
		var digest uint32
		var subnet uint64
		_, err := fmt.Sscanf(topic, "/eth2/%x/"+GossipDataRowMessage+"_%d/ssz_snappy", &digest, &subnet)
		require.NoError(t, err, fmt.Sprintf("row topic %q does not match the expected shape", topic))
		subnets[subnet] = true
	}

	rowSubnetCount := params.BeaconConfig().RowSubnetCount
	require.Equal(t, true, rowTopics > 0, "no row topics registered at all")
	require.Equal(t, int(rowSubnetCount), len(subnets),
		fmt.Sprintf("expected subnets 0..%d, got %d distinct", rowSubnetCount-1, len(subnets)))
	for subnet := range rowSubnetCount {
		require.Equal(t, true, subnets[subnet], fmt.Sprintf("row subnet %d is not in the allow-list", subnet))
	}
	// Nothing past the end, or the allow-list is not bounding anything.
	require.Equal(t, false, subnets[rowSubnetCount], "a row subnet past ROW_SUBNET_COUNT is registered")
}

// TestDataRowTopicsAbsentWhenRowSubnetCountIsZero covers the configuration switch: zero means
// RowDAS is off for this chain, and newSubnetTopic would otherwise emit an unindexed "data_row".
func TestDataRowTopicsAbsentWhenRowSubnetCountIsZero(t *testing.T) {
	params.SetupTestConfigCleanup(t)
	cfg := params.BeaconConfig().Copy()
	cfg.FuluForkEpoch = 0
	cfg.RowSubnetCount = 0
	params.OverrideBeaconConfig(cfg)

	s := &Service{cfg: &Config{}}
	for _, topic := range s.allTopicStrings() {
		require.Equal(t, false, strings.Contains(topic, GossipDataRowMessage),
			fmt.Sprintf("row topic %q registered with ROW_SUBNET_COUNT=0", topic))
	}
}

// TestDataRowTopicHasScoringParams covers the trap that topicScoreParams' default branch returns
// an error, so an unmapped topic fails SubscribeToTopic rather than defaulting.
func TestDataRowTopicHasScoringParams(t *testing.T) {
	params.SetupTestConfigCleanup(t)

	s := &Service{cfg: &Config{}}
	topic := fmt.Sprintf(DataRowSubnetTopicFormat, params.ForkDigest(0), 7)
	scoreParams, err := s.topicScoreParams(topic)
	require.NoError(t, err)
	require.NotNil(t, scoreParams)
}

// TestDataRowPrefixMatchesTheBroadcaster pins the duplicated string. The broadcaster classifies
// topics by prefix and cannot import this package, so the two definitions have to agree by hand.
func TestDataRowPrefixMatchesTheBroadcaster(t *testing.T) {
	topic := fmt.Sprintf(DataRowSubnetTopicFormat, params.ForkDigest(0), 3)
	require.Equal(t, true, strings.Contains(topic, partialdatacolumnbroadcaster.DataRowPrefix),
		fmt.Sprintf("topic %q does not contain the broadcaster's prefix %q", topic, partialdatacolumnbroadcaster.DataRowPrefix))

	// And the column prefix must not appear in a row topic, or classifyTopic would call it
	// ambiguous and refuse it.
	require.Equal(t, false, strings.Contains(topic, GossipDataColumnSidecarMessage))
}
