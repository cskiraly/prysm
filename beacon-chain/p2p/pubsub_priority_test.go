package p2p

import (
	"testing"

	"github.com/OffchainLabs/prysm/v7/testing/assert"
	"github.com/OffchainLabs/prysm/v7/testing/require"
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

func TestGossipPriorityOrder(t *testing.T) {
	topic := func(name string) string { return "/eth2/f5a5fd42/" + name + "/ssz_snappy" }

	prio, err := parseGossipPriorityOrder(DefaultGossipPriorityOrder)
	require.NoError(t, err)
	block, column := prio(topic("beacon_block")), prio(topic("data_column_sidecar_7"))
	aggregate, vote := prio(topic("beacon_aggregate_and_proof")), prio(topic("beacon_attestation_9"))
	exit := prio(topic("voluntary_exit"))
	assert.Equal(t, true, block < column && column < aggregate && aggregate < vote && vote < exit)

	prio, err = parseGossipPriorityOrder("votes-first")
	require.NoError(t, err)
	assert.Equal(t, true, prio(topic("beacon_attestation_1")) < prio(topic("beacon_block")))
	assert.Equal(t, true, prio(topic("beacon_block")) < prio(topic("data_column_sidecar_3")))
	assert.Equal(t, true, prio(topic("data_column_sidecar_3")) < prio(topic("voluntary_exit")))

	prio, err = parseGossipPriorityOrder("beacon_block > beacon_attestation, sync_committee")
	require.NoError(t, err)
	assert.Equal(t, 0, prio(topic("beacon_block")))
	assert.Equal(t, 1, prio(topic("sync_committee_2")))
	assert.Equal(t, 2, prio(topic("data_column_sidecar_0")))

	_, err = parseGossipPriorityOrder("beacon_block>beacon_block")
	require.ErrorContains(t, "listed twice", err)
	_, err = parseGossipPriorityOrder("beacon_block>>sync_committee")
	require.ErrorContains(t, "empty topic", err)
}
