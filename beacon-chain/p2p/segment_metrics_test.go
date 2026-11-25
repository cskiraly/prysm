package p2p

import (
	"testing"

	mock "github.com/OffchainLabs/prysm/v7/beacon-chain/blockchain/testing"
	testDB "github.com/OffchainLabs/prysm/v7/beacon-chain/db/testing"
	"github.com/OffchainLabs/prysm/v7/beacon-chain/p2p/internal/segmentgossip"
	"github.com/OffchainLabs/prysm/v7/config/features"
	"github.com/OffchainLabs/prysm/v7/testing/require"
	"github.com/prometheus/client_golang/prometheus"
)

// seacastSeries returns the node's exported path counters by name.
func seacastSeries(t *testing.T) map[string]float64 {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)
	got := make(map[string]float64)
	for _, f := range families {
		if f.GetName() != "seacast_path_events_total" {
			continue
		}
		for _, m := range f.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == "counter" {
					got[l.GetValue()] = m.GetCounter().GetValue()
				}
			}
		}
	}
	return got
}

// TestSeacastPathMetricsExported: a node on the segment path exports what its router and its path
// options count, the router's counters and the guard's demotions among them, one series per counter.
func TestSeacastPathMetricsExported(t *testing.T) {
	seacastGloasConfig(t)
	reset := features.InitWithReset(&features.Flags{EnableSegmentedPayloadGossip: true, SeacastUplinkMbps: 100})
	defer reset()
	s, err := NewService(t.Context(), &Config{StateNotifier: &mock.MockStateNotifier{}, DB: testDB.SetupDB(t)})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, s.Stop())
		require.NoError(t, s.host.Close())
	})

	s.seacastMetrics.Seacast.Pushed.Add(3)
	s.seacastMetrics.Seacast.DecodesDone.Add(5)
	s.seacastMetrics.Seacast.RepairPushes.Add(7)
	s.seacastMetrics.GuardDemotions.Add(2)
	s.seacastMetrics.V05.PromisesAtWrite.Add(11)
	s.segmentPayloadType.Counts().DecodeRecoveries.Add(13)
	got := seacastSeries(t)
	require.Equal(t, float64(13), got["DecodeRecoveries"])
	require.Equal(t, float64(3), got["Pushed"])
	require.Equal(t, float64(5), got["DecodesDone"])
	require.Equal(t, float64(7), got["RepairPushes"])
	require.Equal(t, float64(2), got["GuardDemotions"])
	require.Equal(t, float64(11), got["V05PromisesAtWrite"])
	require.Equal(t, float64(0), got["Offered"])
	// Every counter the router keeps, not a chosen few: the struct has well over a hundred.
	require.Equal(t, true, len(got) > 100, "%d series", len(got))
}

// TestSeacastCollectorFollowsTheService: the exported counters are those of the service whose router
// was built, from NewService until its Stop. Building the options alone doesn't point the collector at
// them (the router may still fail to build), a second service takes the collector over, the first's
// Stop leaves the second's counters exported, and the second's Stop leaves none.
func TestSeacastCollectorFollowsTheService(t *testing.T) {
	seacastGloasConfig(t)
	reset := features.InitWithReset(&features.Flags{EnableSegmentedPayloadGossip: true, SeacastUplinkMbps: 100})
	defer reset()
	seacastPathCollector.m.Store(nil)
	t.Cleanup(func() { seacastPathCollector.m.Store(nil) })
	none := (*segmentgossip.PathMetrics)(nil)

	opts := &Service{cfg: &Config{QueueSize: 600}}
	_, err := opts.pubsubOptions()
	require.NoError(t, err)
	require.Equal(t, none, seacastPathCollector.m.Load(), "the options alone pointed the collector")

	db := testDB.SetupDB(t) // one for both: a second registers its collector again
	service := func() *Service {
		s, err := NewService(t.Context(), &Config{StateNotifier: &mock.MockStateNotifier{}, DB: db})
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, s.host.Close()) })
		require.NotNil(t, s.seacastMetrics)
		return s
	}
	a := service()
	require.Equal(t, a.seacastMetrics, seacastPathCollector.m.Load(), "the service's counters not exported")
	b := service()
	require.Equal(t, b.seacastMetrics, seacastPathCollector.m.Load(), "the second service's counters not exported")
	require.NoError(t, a.Stop())
	require.Equal(t, b.seacastMetrics, seacastPathCollector.m.Load(), "the first service's stop took the second's counters")
	require.NoError(t, b.Stop())
	require.Equal(t, none, seacastPathCollector.m.Load(), "a stopped service's counters still exported")
}

// TestSeacastCollectorDescribes: the collector describes the one metric it collects, so the registry
// checks what it sends.
func TestSeacastCollectorDescribes(t *testing.T) {
	ch := make(chan *prometheus.Desc, 4)
	seacastPathCollector.Describe(ch)
	close(ch)
	var got []*prometheus.Desc
	for d := range ch {
		got = append(got, d)
	}
	require.Equal(t, 1, len(got))
	require.Equal(t, seacastPathEvents, got[0])
}
