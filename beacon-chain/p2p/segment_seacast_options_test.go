package p2p

import (
	"bytes"
	"context"
	"net"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	mock "github.com/OffchainLabs/prysm/v7/beacon-chain/blockchain/testing"
	testDB "github.com/OffchainLabs/prysm/v7/beacon-chain/db/testing"
	"github.com/OffchainLabs/prysm/v7/beacon-chain/p2p/internal/segmentgossip"
	"github.com/OffchainLabs/prysm/v7/config/features"
	fieldparams "github.com/OffchainLabs/prysm/v7/config/fieldparams"
	"github.com/OffchainLabs/prysm/v7/config/params"
	leakybucket "github.com/OffchainLabs/prysm/v7/container/leaky-bucket"
	prysmnetwork "github.com/OffchainLabs/prysm/v7/network"
	"github.com/OffchainLabs/prysm/v7/testing/require"
	"github.com/libp2p/go-libp2p"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pubsubpb "github.com/libp2p/go-libp2p-pubsub/pb"
)

// seacastGloasConfig schedules every fork up to Gloas in the first epochs, so the fork schedule has
// a segment topic.
func seacastGloasConfig(t *testing.T) {
	cfg := params.MainnetTestConfig()
	if fieldparams.Preset == "minimal" {
		cfg = params.MinimalSpecConfig()
	}
	cfg.AltairForkEpoch, cfg.BellatrixForkEpoch, cfg.CapellaForkEpoch = 0, 1, 2
	cfg.DenebForkEpoch, cfg.ElectraForkEpoch, cfg.FuluForkEpoch, cfg.GloasForkEpoch = 3, 4, 5, 6
	params.SetActiveTestCleanup(t, cfg)
}

// newRouter builds a gossipsub router on a fresh host with opts, and returns NewGossipSub's error.
func newRouter(t *testing.T, opts []pubsub.Option) error {
	_, err := newPubSub(t, opts)
	return err
}

func newPubSub(t *testing.T, opts []pubsub.Option) (*pubsub.PubSub, error) {
	h, err := libp2p.New(libp2p.NoListenAddrs)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, h.Close()) })
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	return pubsub.NewGossipSub(ctx, h, opts...)
}

// TestSeacastWindowFollowsTheSlot checks that the node moves SEACAST's live window with the beacon
// slot: the router learns the slot it is given, the payload type drops a group's segments at the advance
// at which the router expires the group and keeps its slot, and a group is noted at the router's slot.
func TestSeacastWindowFollowsTheSlot(t *testing.T) {
	seacastGloasConfig(t)
	reset := features.InitWithReset(&features.Flags{EnableSegmentedPayloadGossip: true, SeacastUplinkMbps: 100})
	defer reset()
	slot := params.BeaconConfig().SlotDuration()
	s := &Service{
		cfg:                   &Config{QueueSize: 600},
		genesisTime:           time.Now().Add(-10*slot - slot/2),
		genesisValidatorsRoot: make([]byte, 32),
	}
	opts, err := s.pubsubOptions()
	require.NoError(t, err)
	s.pubsub, err = newPubSub(t, opts)
	require.NoError(t, err)

	root := string(bytes.Repeat([]byte{7}, 32))
	topic, ok := s.segmentPayloadType.TopicOf(root)
	require.Equal(t, true, ok)
	require.Equal(t, true, isSegmentTopic(topic), topic)
	require.Equal(t, true, s.segmentPayloadType.NoteGroup(root, topic, 128, 64))
	seen, ok := s.segmentPayloadType.SlotOf(root)
	require.Equal(t, true, ok)
	require.Equal(t, uint64(10), seen)

	_, known := s.pubsub.SeacastSlot()
	require.Equal(t, false, known)
	s.segmentPayloadType.Store().Put(root, 0, &pubsub.Message{Message: &pubsubpb.Message{}})
	s.seacastAdvance(11)
	now, known := s.pubsub.SeacastSlot()
	require.Equal(t, true, known)
	require.Equal(t, uint64(11), now)
	_, held := s.segmentPayloadType.Store().Get(root, 0)
	require.Equal(t, true, held, "a live group's segment dropped")
	// At 12 the router expires the group of slot 10 (10 + 2 <= 12), and the type drops its segments
	// at the same advance, keeping its slot for the router's later drops.
	s.seacastAdvance(12)
	_, held = s.segmentPayloadType.Store().Get(root, 0)
	require.Equal(t, false, held, "an expired group's segment kept")
	s.seacastAdvance(13)
	seen, ok = s.segmentPayloadType.SlotOf(root)
	require.Equal(t, true, ok, "the expired group's slot forgotten")
	require.Equal(t, uint64(10), seen)

	// The clock moves into slot 16 before the ticker moves the router: a group whose first segment
	// passes now is the router's slot's (13), not the clock's, so the router doesn't take it for a
	// group of a future slot.
	s.genesisTime = s.genesisTime.Add(-6 * slot)
	require.Equal(t, uint64(16), s.seacastCurrentSlot())
	fresh := string(bytes.Repeat([]byte{8}, 32))
	require.Equal(t, true, s.segmentPayloadType.NoteGroup(fresh, topic, 4, 2))
	seen, ok = s.segmentPayloadType.SlotOf(fresh)
	require.Equal(t, true, ok)
	require.Equal(t, uint64(13), seen)
}

// TestSeacastRouterOptions: with the segment flag on, the node's options build a router with the payload type on SEACAST's path; a setting that breaks a
// condition stops startup; and the path's options fail when their order or their phase matcher is wrong.
func TestSeacastRouterOptions(t *testing.T) {
	seacastGloasConfig(t)

	t.Run("the node's options build a router", func(t *testing.T) {
		reset := features.InitWithReset(&features.Flags{EnableSegmentedPayloadGossip: true, SeacastUplinkMbps: 100})
		defer reset()
		s := &Service{cfg: &Config{QueueSize: 600}}
		opts, err := s.pubsubOptions()
		require.NoError(t, err)
		require.NotNil(t, s.segmentPayloadType)
		require.NotNil(t, s.seacastMetrics)
		require.Equal(t, true, len(s.segmentPayloadType.Topics()) > 0)
		for _, topic := range s.segmentPayloadType.Topics() {
			require.Equal(t, true, isSegmentTopic(topic), topic)
		}
		require.NoError(t, newRouter(t, opts))
	})

	t.Run("no uplink stops startup", func(t *testing.T) {
		reset := features.InitWithReset(&features.Flags{EnableSegmentedPayloadGossip: true})
		defer reset()
		s := &Service{cfg: &Config{QueueSize: 600}}
		_, err := s.pubsubOptions()
		require.ErrorContains(t, "seacast-uplink-mbps", err)
		require.Equal(t, (*segmentgossip.PayloadType)(nil), s.segmentPayloadType)
	})

	t.Run("flag off, no path", func(t *testing.T) {
		s := &Service{cfg: &Config{QueueSize: 600}}
		opts, err := s.pubsubOptions()
		require.NoError(t, err)
		require.Equal(t, (*segmentgossip.PayloadType)(nil), s.segmentPayloadType)
		require.NoError(t, newRouter(t, opts))
	})

	// The node's options without the path (the flag off), then the path's: RouterOptions puts
	// WithPhaseForwarding first, and the two cases below replace it.
	s := &Service{cfg: &Config{QueueSize: 600}}
	base, err := s.pubsubOptions()
	require.NoError(t, err)
	reset := features.InitWithReset(&features.Flags{EnableSegmentedPayloadGossip: true, SeacastUplinkMbps: 100})
	defer reset()
	setting, typ := s.seacastSetting(pubsub.NewAllowlistSubscriptionFilter(s.allTopicStrings()...))
	require.NoError(t, segmentgossip.CheckSetting(setting))
	path := segmentgossip.RouterOptions(setting, typ, gossipSubHeartbeatInterval, &segmentgossip.PathMetrics{})

	t.Run("the ask window follows the uplink", func(t *testing.T) {
		// seacast.md §4.13: IWANT_WINDOW (200 ms) times max(1, ASK_UPLINK / UPLINK), ASK_UPLINK 50 Mbps.
		for _, c := range []struct {
			mbps int
			want time.Duration
		}{{10, time.Second}, {50, 200 * time.Millisecond}, {100, 200 * time.Millisecond}} {
			at := setting
			at.UplinkMbps = c.mbps
			opts := append(append([]pubsub.Option{}, base...), segmentgossip.RouterOptions(at, typ, gossipSubHeartbeatInterval, &segmentgossip.PathMetrics{})...)
			ps, err := newPubSub(t, opts)
			require.NoError(t, err)
			require.Equal(t, c.want, ps.AskWindow(), "at %d Mbps", c.mbps)
		}
	})

	t.Run("own chunks first", func(t *testing.T) {
		// seacast.md §4.12's choose [X3]: the writer sends the node's own chunks first.
		opts := append(append([]pubsub.Option{}, base...), segmentgossip.RouterOptions(setting, typ, gossipSubHeartbeatInterval, &segmentgossip.PathMetrics{})...)
		ps, err := newPubSub(t, opts)
		require.NoError(t, err)
		require.Equal(t, true, ps.OwnChunksFirst(), "own chunks first")
	})

	t.Run("phase forwarding left out", func(t *testing.T) {
		err := newRouter(t, append(append([]pubsub.Option{}, base...), path[1:]...))
		require.ErrorContains(t, "WithPhaseForwarding", err)
	})

	t.Run("a phase matcher that takes the segment topic", func(t *testing.T) {
		topic := typ.Topics()[0]
		opts := append(append([]pubsub.Option{}, base...),
			pubsub.WithPhaseForwarding(func(t string) bool { return t == topic }, setting.Params.RNode))
		opts = append(opts, path[1:]...)
		err := newRouter(t, opts)
		require.ErrorContains(t, "is also matched by the phase path", err)
		require.Equal(t, true, strings.Contains(err.Error(), topic))
	})
}

// TestSeacastSettingErrorClosesTheHost: a node whose segment setting stops startup closes the host it
// built before the check, so the host's port is free again.
func TestSeacastSettingErrorClosesTheHost(t *testing.T) {
	seacastGloasConfig(t)
	reset := features.InitWithReset(&features.Flags{EnableSegmentedPayloadGossip: true})
	defer reset()
	ip := prysmnetwork.IPAddr().String()
	l, err := net.Listen("tcp", net.JoinHostPort(ip, "0"))
	require.NoError(t, err)
	port := l.Addr().(*net.TCPAddr).Port
	require.NoError(t, l.Close())

	_, err = NewService(t.Context(), &Config{
		StateNotifier: &mock.MockStateNotifier{}, DB: testDB.SetupDB(t), TCPPort: uint(port), // lint:ignore uintcast -- a port.
	})
	require.ErrorContains(t, "seacast-uplink-mbps", err)
	l, err = net.Listen("tcp", net.JoinHostPort(ip, strconv.Itoa(port)))
	require.NoError(t, err, "the stopped node's host still holds its port")
	require.NoError(t, l.Close())
}

// TestSeacastSettingErrorStopsTheLimiter: a node whose segment setting stops startup also stops the IP
// limiter's prune goroutine, so repeated refusals leave none behind.
func TestSeacastSettingErrorStopsTheLimiter(t *testing.T) {
	seacastGloasConfig(t)
	reset := features.InitWithReset(&features.Flags{EnableSegmentedPayloadGossip: true})
	defer reset()
	db := testDB.SetupDB(t)
	prunes := func() int {
		buf := make([]byte, 1<<22)
		return strings.Count(string(buf[:runtime.Stack(buf, true)]), "(*Collector).PeriodicPrune.func1(")
	}
	before := prunes()
	// The count sees a limiter's goroutine.
	c := leakybucket.NewCollector(1, 1, time.Second, true)
	require.Equal(t, before+1, prunes(), "a new limiter's prune goroutine not counted")
	c.Free()
	for range 5 {
		_, err := NewService(t.Context(), &Config{StateNotifier: &mock.MockStateNotifier{}, DB: db})
		require.ErrorContains(t, "seacast-uplink-mbps", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for prunes() > before && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	require.Equal(t, before, prunes(), "prune goroutines left by refused starts")
}
