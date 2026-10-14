package p2p

import (
	"testing"

	"github.com/OffchainLabs/prysm/v7/testing/assert"
)

func TestTopicFamily(t *testing.T) {
	tests := map[string]string{
		"/eth2/f5a5fd42/beacon_block/ssz_snappy":                          "beacon_block",
		"/eth2/f5a5fd42/data_column_sidecar_12/ssz_snappy":                "data_column_sidecar",
		"/eth2/f5a5fd42/beacon_attestation_3/ssz_snappy":                  "beacon_attestation",
		"/eth2/f5a5fd42/sync_committee_contribution_and_proof/ssz_snappy": "sync_committee_contribution_and_proof",
		"/eth2/f5a5fd42/sync_committee_1/ssz_snappy":                      "sync_committee",
		"":      "control",
		"bogus": "unknown",
	}
	for topic, want := range tests {
		assert.Equal(t, want, topicFamily(topic), topic)
	}
}

func TestGossipTopicPriority(t *testing.T) {
	block := gossipTopicPriority("/eth2/f5a5fd42/beacon_block/ssz_snappy")
	column := gossipTopicPriority("/eth2/f5a5fd42/data_column_sidecar_7/ssz_snappy")
	aggregate := gossipTopicPriority("/eth2/f5a5fd42/beacon_aggregate_and_proof/ssz_snappy")
	vote := gossipTopicPriority("/eth2/f5a5fd42/beacon_attestation_9/ssz_snappy")
	exit := gossipTopicPriority("/eth2/f5a5fd42/voluntary_exit/ssz_snappy")
	assert.Equal(t, true, block < column && column < aggregate && aggregate < vote && vote < exit)
}
