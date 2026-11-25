package segmentgossip

import (
	"strings"
	"testing"

	"github.com/OffchainLabs/prysm/v7/testing/require"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
)

// nodeSetting is the setting the node builds: two digests from Gloas on, both allowed and both the
// type's, cancel on and the path's default limits.
func nodeSetting() Setting {
	topics := []string{
		"/eth2/aaaaaaaa/execution_payload_segment/ssz_snappy",
		"/eth2/bbbbbbbb/execution_payload_segment/ssz_snappy",
	}
	allowed := map[string]bool{topics[0]: true, topics[1]: true, "/eth2/aaaaaaaa/beacon_block/ssz_snappy": true}
	return Setting{
		SegmentTopics: topics,
		Allowed:       func(t string) bool { return allowed[t] },
		TypeTopics:    append([]string(nil), topics...),
		Params:        PayloadParams(100),
		Limits:        pubsub.DefaultSeacastLimits,
		Cancel:        true,
		UplinkMbps:    100,
	}
}

// TestCheckSetting: each of the path's four conditions, flipped, makes the check fail with an error
// that names it, and the node's setting passes.
func TestCheckSetting(t *testing.T) {
	require.NoError(t, CheckSetting(nodeSetting()))

	cases := []struct {
		name string
		flip func(s *Setting)
		want string
	}{
		{"no segment topic", func(s *Setting) { s.SegmentTopics = nil }, "condition 1"},
		{"the allowlist refuses a digest's topic", func(s *Setting) {
			first := s.SegmentTopics[0]
			s.Allowed = func(t string) bool { return t != first }
		}, "condition 1"},
		{"the type lacks a digest's topic", func(s *Setting) { s.TypeTopics = s.TypeTopics[:1] }, "condition 1"},
		{"cancel off", func(s *Setting) { s.Cancel = false }, "condition 2"},
		{"a serve limit", func(s *Setting) { s.Params.ServeLimit = 4 }, "condition 3"},
		{"unknown groups bounded", func(s *Setting) { s.Limits.UnknownGroupsPerPeer = 8 }, "condition 3"},
		{"no uplink", func(s *Setting) { s.UplinkMbps = 0 }, "--seacast-uplink-mbps"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := nodeSetting()
			c.flip(&s)
			require.ErrorContains(t, c.want, CheckSetting(s))
		})
	}
}

// TestPayloadDecodesOnlyCodedGroups: the node decodes at K only a group with parity segments. A plain
// group (K = N) completes when every segment is in and has nothing to rebuild, and a group the node
// has not noted has no K to decode at.
func TestPayloadDecodesOnlyCodedGroups(t *testing.T) {
	typ := NewPayloadType(PayloadConfig{UplinkMbps: 100, CurrentSlot: func() uint64 { return 1 }})
	decode := typ.Params().DecodeAtK
	require.NotNil(t, decode)
	coded, plain, unknown := strings.Repeat("c", 32), strings.Repeat("p", 32), strings.Repeat("u", 32)
	require.Equal(t, true, typ.NoteGroup(coded, "topic", 128, 64))
	require.Equal(t, true, typ.NoteGroup(plain, "topic", 11, 11))
	require.Equal(t, true, decode(coded), "a coded group")
	require.Equal(t, false, decode(plain), "a plain group")
	require.Equal(t, false, decode(unknown), "a group not noted")
}

func TestPayloadParamsPushBudget(t *testing.T) {
	require.Equal(t, 2, PayloadParams(100).RNode)
	require.Equal(t, 2, PayloadParams(499).RNode)
	require.Equal(t, 6, PayloadParams(500).RNode)
	require.Equal(t, 6, PayloadParams(1000).RNode)
}
