package partialdatacolumnbroadcaster

import (
	"testing"

	"github.com/OffchainLabs/prysm/v7/config/params"
	"github.com/OffchainLabs/prysm/v7/testing/require"
)

func TestClassifyTopic(t *testing.T) {
	cases := []struct {
		name       string
		topic      string
		wantSubnet uint64
		wantKind   topicKind
		wantErr    bool
	}{
		{
			name:       "column topic",
			topic:      "/eth2/abcd1234/data_column_sidecar_12/ssz_snappy",
			wantKind:   topicKindColumn,
			wantSubnet: 12,
		},
		{
			name:       "column subnet zero",
			topic:      "/eth2/abcd1234/data_column_sidecar_0/ssz_snappy",
			wantKind:   topicKindColumn,
			wantSubnet: 0,
		},
		{
			name:       "row topic",
			topic:      "/eth2/abcd1234/data_row_7/ssz_snappy",
			wantKind:   topicKindRow,
			wantSubnet: 7,
		},
		{
			name:       "row topic without an encoding suffix",
			topic:      "/eth2/abcd1234/data_row_127",
			wantKind:   topicKindRow,
			wantSubnet: 127,
		},
		{
			name:    "column subnet out of range",
			topic:   "/eth2/abcd1234/data_column_sidecar_128/ssz_snappy",
			wantErr: true,
		},
		{
			name:    "row subnet out of range",
			topic:   "/eth2/abcd1234/data_row_128/ssz_snappy",
			wantErr: true,
		},
		{
			name:    "not a DAS topic",
			topic:   "/eth2/abcd1234/beacon_block/ssz_snappy",
			wantErr: true,
		},
		{
			name:    "no subnet index",
			topic:   "/eth2/abcd1234/data_row_/ssz_snappy",
			wantErr: true,
		},
		{
			name:    "negative subnet index",
			topic:   "/eth2/abcd1234/data_row_-1/ssz_snappy",
			wantErr: true,
		},
		{
			// The topic string is peer-controlled, so a name carrying both prefixes must not
			// be silently resolved to whichever one is checked first.
			name:    "both prefixes is ambiguous, not column-wins",
			topic:   "/eth2/abcd1234/data_column_sidecar_1/data_row_2/ssz_snappy",
			wantErr: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			kind, subnet, err := classifyTopic(tc.topic)
			if tc.wantErr {
				require.NotNil(t, err)
				require.Equal(t, topicKindUnknown, kind)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.wantKind, kind)
			require.Equal(t, tc.wantSubnet, subnet)
		})
	}
}

func TestClassifyTopicHonoursReducedRowSubnetCount(t *testing.T) {
	// Test and devnet configurations shrink ROW_SUBNET_COUNT so a small network still puts
	// several nodes on each row subnet. The bound has to follow it, or a peer could open
	// state for subnets that cannot exist.
	params.SetupTestConfigCleanup(t)
	cfg := params.BeaconConfig().Copy()
	cfg.RowSubnetCount = 4
	params.OverrideBeaconConfig(cfg)

	kind, subnet, err := classifyTopic("/eth2/abcd1234/data_row_3/ssz_snappy")
	require.NoError(t, err)
	require.Equal(t, topicKindRow, kind)
	require.Equal(t, uint64(3), subnet)

	_, _, err = classifyTopic("/eth2/abcd1234/data_row_4/ssz_snappy")
	require.NotNil(t, err)
}

func TestClassifyTopicRejectsRowsWhenDisabled(t *testing.T) {
	// ROW_SUBNET_COUNT of zero means RowDAS is off. No row topic can be valid then.
	params.SetupTestConfigCleanup(t)
	cfg := params.BeaconConfig().Copy()
	cfg.RowSubnetCount = 0
	params.OverrideBeaconConfig(cfg)

	_, _, err := classifyTopic("/eth2/abcd1234/data_row_0/ssz_snappy")
	require.NotNil(t, err)

	// Columns are unaffected.
	kind, _, err := classifyTopic("/eth2/abcd1234/data_column_sidecar_0/ssz_snappy")
	require.NoError(t, err)
	require.Equal(t, topicKindColumn, kind)
}

func TestExtractColumnIndexFromTopicRefusesRowTopics(t *testing.T) {
	_, err := extractColumnIndexFromTopic("/eth2/abcd1234/data_row_1/ssz_snappy")
	require.NotNil(t, err)
}
