package features

import (
	"flag"
	"testing"

	"github.com/OffchainLabs/prysm/v7/testing/assert"
	"github.com/OffchainLabs/prysm/v7/testing/require"
	"github.com/urfave/cli/v2"
)

func TestParseSegmentedPayloadMode(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want SegmentedPayloadMode
	}{
		{"off", SegmentedPayloadOff},
		{"messages", SegmentedPayloadMessages},
		{"partial", SegmentedPayloadPartial},
		// Tolerant about presentation, strict about meaning.
		{"  PARTIAL ", SegmentedPayloadPartial},
	} {
		got, err := ParseSegmentedPayloadMode(tc.in)
		require.NoError(t, err)
		assert.Equal(t, tc.want, got)
		// Round trip, so the flag value and the mode name cannot drift apart.
		assert.Equal(t, tc.want.String(), got.String())
	}

	// A typo must be refused rather than treated as off: silently disabling the feature under
	// measurement is the worst available failure.
	for _, bad := range []string{"", "on", "true", "partials", "message"} {
		_, err := ParseSegmentedPayloadMode(bad)
		require.NotNil(t, err)
	}
}

func TestSegmentedPayloadModeEnabled(t *testing.T) {
	assert.Equal(t, false, SegmentedPayloadOff.Enabled())
	assert.Equal(t, true, SegmentedPayloadMessages.Enabled())
	assert.Equal(t, true, SegmentedPayloadPartial.Enabled())
}

func TestConfigureBeaconChain_SegmentedPayloadGossip(t *testing.T) {
	defer Init(&Flags{})
	app := cli.App{}

	newSet := func() *flag.FlagSet {
		set := flag.NewFlagSet("test", 0)
		set.String(SegmentedPayloadGossip.Name, "off", "test")
		set.Bool(EnableSegmentedPayloadGossip.Name, false, "test")
		return set
	}

	t.Run("default is off", func(t *testing.T) {
		require.NoError(t, ConfigureBeaconChain(cli.NewContext(&app, newSet(), nil)))
		assert.Equal(t, SegmentedPayloadOff, Get().SegmentedPayloadGossip)
	})

	for _, mode := range []SegmentedPayloadMode{SegmentedPayloadMessages, SegmentedPayloadPartial} {
		t.Run(mode.String(), func(t *testing.T) {
			set := newSet()
			require.NoError(t, set.Set(SegmentedPayloadGossip.Name, mode.String()))
			require.NoError(t, ConfigureBeaconChain(cli.NewContext(&app, set, nil)))
			assert.Equal(t, mode, Get().SegmentedPayloadGossip)
		})
	}

	t.Run("deprecated boolean selects variant A", func(t *testing.T) {
		set := newSet()
		require.NoError(t, set.Set(EnableSegmentedPayloadGossip.Name, "true"))
		require.NoError(t, ConfigureBeaconChain(cli.NewContext(&app, set, nil)))
		assert.Equal(t, SegmentedPayloadMessages, Get().SegmentedPayloadGossip)
	})

	t.Run("the explicit mode wins over the deprecated boolean", func(t *testing.T) {
		// Both set, disagreeing. The mode flag is the newer and more specific statement of
		// intent, so it has to be the one that decides -- including when it says off.
		set := newSet()
		require.NoError(t, set.Set(EnableSegmentedPayloadGossip.Name, "true"))
		require.NoError(t, set.Set(SegmentedPayloadGossip.Name, "partial"))
		require.NoError(t, ConfigureBeaconChain(cli.NewContext(&app, set, nil)))
		assert.Equal(t, SegmentedPayloadPartial, Get().SegmentedPayloadGossip)

		set = newSet()
		require.NoError(t, set.Set(EnableSegmentedPayloadGossip.Name, "true"))
		require.NoError(t, set.Set(SegmentedPayloadGossip.Name, "off"))
		require.NoError(t, ConfigureBeaconChain(cli.NewContext(&app, set, nil)))
		assert.Equal(t, SegmentedPayloadOff, Get().SegmentedPayloadGossip)
	})

	t.Run("an unknown mode is an error, not a silent off", func(t *testing.T) {
		set := newSet()
		require.NoError(t, set.Set(SegmentedPayloadGossip.Name, "partials"))
		require.NotNil(t, ConfigureBeaconChain(cli.NewContext(&app, set, nil)))
	})
}
