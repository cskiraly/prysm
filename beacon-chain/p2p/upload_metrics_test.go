package p2p

import (
	"fmt"
	"testing"

	mock "github.com/OffchainLabs/prysm/v7/beacon-chain/blockchain/testing"
	testDB "github.com/OffchainLabs/prysm/v7/beacon-chain/db/testing"
	"github.com/OffchainLabs/prysm/v7/config/features"
	"github.com/OffchainLabs/prysm/v7/testing/require"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// uploadSeries returns the exported series of the metric name, keyed by their label values joined
// with "/" ("" for a series without labels); found is false if the registry has no such metric.
func uploadSeries(t *testing.T, name string) (got map[string]float64, found bool) {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)
	got = make(map[string]float64)
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		found = true
		for _, m := range f.GetMetric() {
			key := ""
			for i, l := range m.GetLabel() {
				if i > 0 {
					key += "/"
				}
				key += l.GetValue()
			}
			got[key] = metricValue(m)
		}
	}
	return got, found
}

func metricValue(m *dto.Metric) float64 {
	if c := m.GetCounter(); c != nil {
		return c.GetValue()
	}
	return m.GetGauge().GetValue()
}

// TestUploadSeriesExported: a node exports the bytes its gossipsub streams wrote, with the segment
// path on or off: both arms of a comparison read them.
func TestUploadSeriesExported(t *testing.T) {
	for _, on := range []bool{false, true} {
		t.Run(fmt.Sprintf("segment path %v", on), func(t *testing.T) {
			seacastGloasConfig(t) // a config with its fork schedule, whatever an earlier test left
			reset := features.InitWithReset(&features.Flags{EnableSegmentedPayloadGossip: on, SeacastUplinkMbps: 100})
			defer reset()
			s, err := NewService(t.Context(), &Config{StateNotifier: &mock.MockStateNotifier{}, DB: testDB.SetupDB(t)})
			require.NoError(t, err)
			t.Cleanup(func() {
				require.NoError(t, s.Stop())
				require.NoError(t, s.host.Close())
			})
			for _, name := range []string{"p2p_pubsub_writes_total", "p2p_pubsub_written_bytes_total", "p2p_pubsub_written_framing_bytes_total"} {
				got, found := uploadSeries(t, name)
				require.Equal(t, true, found, "%s not exported", name)
				require.Equal(t, 1, len(got), "%s: %v", name, got)
			}
			got, found := uploadSeries(t, "p2p_pubsub_written_control_bytes_total")
			require.Equal(t, true, found, "control bytes not exported")
			require.Equal(t, 4, len(got), "control bytes by kind: %v", got)
		})
	}
}

// uploadStub is a router's written bytes, given.
type uploadStub struct {
	stat    pubsub.UploadStat
	byTopic map[string]pubsub.TopicUpload
}

func (u *uploadStub) UploadStats() pubsub.UploadStat               { return u.stat }
func (u *uploadStub) UploadByTopic() map[string]pubsub.TopicUpload { return u.byTopic }
func (u *uploadStub) UploadSnapshot() (pubsub.UploadStat, map[string]pubsub.TopicUpload) {
	return u.stat, u.byTopic
}

// TestUploadCollectorExportsTheRouters: every count the router keeps of its written bytes is
// exported under its series, frames by topic and class, control by kind.
func TestUploadCollectorExportsTheRouters(t *testing.T) {
	prev := uploadBytesCollector.source()
	t.Cleanup(func() { uploadBytesCollector.set(prev) })
	stub := &uploadStub{
		stat: pubsub.UploadStat{
			PushBytes: 1100, AnswerBytes: 70, PushFrames: 11, AnswerFrames: 1,
			IHaveBytes: 21, IWantBytes: 22, IDontWantBytes: 23, GraftPruneBytes: 24, OtherBytes: 25,
			Writes: 9, AllBytes: 1100 + 70 + 21 + 22 + 23 + 24 + 25,
		},
		byTopic: map[string]pubsub.TopicUpload{
			"/eth2/t": {PushBytes: 1000, PushFrames: 10, AnswerBytes: 70, AnswerFrames: 1},
			"/eth2/u": {PushBytes: 100, PushFrames: 1},
		},
	}
	uploadBytesCollector.set(stub)

	frameBytes, _ := uploadSeries(t, "p2p_pubsub_written_frame_bytes_total")
	require.DeepEqual(t, map[string]float64{
		"push/" + "/eth2/t": 1000, "answer/" + "/eth2/t": 70, "push/" + "/eth2/u": 100, "answer/" + "/eth2/u": 0,
	}, frameBytes)
	frames, _ := uploadSeries(t, "p2p_pubsub_written_frames_total")
	require.DeepEqual(t, map[string]float64{
		"push/" + "/eth2/t": 10, "answer/" + "/eth2/t": 1, "push/" + "/eth2/u": 1, "answer/" + "/eth2/u": 0,
	}, frames)
	control, _ := uploadSeries(t, "p2p_pubsub_written_control_bytes_total")
	require.DeepEqual(t, map[string]float64{"ihave": 21, "iwant": 22, "idontwant": 23, "graft_prune": 24}, control)
	framing, _ := uploadSeries(t, "p2p_pubsub_written_framing_bytes_total")
	require.DeepEqual(t, map[string]float64{"": 25}, framing)
	writes, _ := uploadSeries(t, "p2p_pubsub_writes_total")
	require.DeepEqual(t, map[string]float64{"": 9}, writes)
	all, _ := uploadSeries(t, "p2p_pubsub_written_bytes_total")
	require.DeepEqual(t, map[string]float64{"": float64(stub.stat.AllBytes)}, all)
}

// writingStub is a router that writes a 100-byte frame on topic t right after each read of its counts.
type writingStub struct{ n int64 }

func (w *writingStub) counts() (pubsub.UploadStat, map[string]pubsub.TopicUpload) {
	s := pubsub.UploadStat{PushBytes: w.n, PushFrames: w.n / 100, AllBytes: w.n}
	m := map[string]pubsub.TopicUpload{"t": {PushBytes: w.n, PushFrames: w.n / 100}}
	w.n += 100
	return s, m
}

func (w *writingStub) UploadStats() pubsub.UploadStat {
	s, _ := w.counts()
	return s
}

func (w *writingStub) UploadByTopic() map[string]pubsub.TopicUpload {
	_, m := w.counts()
	return m
}

func (w *writingStub) UploadSnapshot() (pubsub.UploadStat, map[string]pubsub.TopicUpload) {
	return w.counts()
}

// TestUploadCollectorOneSnapshot: a scrape's frame bytes add up to its total even when the router
// writes a frame while the scrape reads its counts: the counts by topic and the totals are read in
// one snapshot.
func TestUploadCollectorOneSnapshot(t *testing.T) {
	prev := uploadBytesCollector.source()
	t.Cleanup(func() { uploadBytesCollector.set(prev) })
	uploadBytesCollector.set(&writingStub{n: 1000})
	families, err := prometheus.DefaultGatherer.Gather() // one scrape
	require.NoError(t, err)
	var frames, all float64
	for _, f := range families {
		for _, m := range f.GetMetric() {
			switch f.GetName() {
			case "p2p_pubsub_written_frame_bytes_total":
				frames += metricValue(m)
			case "p2p_pubsub_written_bytes_total":
				all += metricValue(m)
			}
		}
	}
	require.Equal(t, float64(1000), all)
	require.Equal(t, all, frames, "the frames by topic don't add up to the bytes written in one scrape")
}

// TestUploadCollectorFollowsTheService: the exported bytes are those of the service whose router was
// built, until its Stop. A second service takes the collector over, the first's Stop leaves the
// second's exported, and the second's Stop leaves none.
func TestUploadCollectorFollowsTheService(t *testing.T) {
	seacastGloasConfig(t)
	uploadBytesCollector.set(nil)
	t.Cleanup(func() { uploadBytesCollector.set(nil) })
	db := testDB.SetupDB(t) // one for both: a second registers its collector again
	service := func() *Service {
		s, err := NewService(t.Context(), &Config{StateNotifier: &mock.MockStateNotifier{}, DB: db})
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, s.host.Close()) })
		return s
	}
	a := service()
	require.Equal(t, uploadSource(a.pubsub), uploadBytesCollector.source(), "the service's router not exported")
	b := service()
	require.Equal(t, uploadSource(b.pubsub), uploadBytesCollector.source(), "the second service's router not exported")
	require.NoError(t, a.Stop())
	require.Equal(t, uploadSource(b.pubsub), uploadBytesCollector.source(), "the first service's stop took the second's router")
	require.NoError(t, b.Stop())
	require.Equal(t, nil, uploadBytesCollector.source(), "a stopped service's router still exported")
	_, found := uploadSeries(t, "p2p_pubsub_writes_total")
	require.Equal(t, false, found, "series exported with no router")
}

// TestUploadCollectorDescribes: the collector describes every metric it collects, so the registry
// checks what it sends.
func TestUploadCollectorDescribes(t *testing.T) {
	ch := make(chan *prometheus.Desc, 16)
	uploadBytesCollector.Describe(ch)
	close(ch)
	got := make(map[*prometheus.Desc]bool)
	for d := range ch {
		got[d] = true
	}
	want := []*prometheus.Desc{uploadFrameBytes, uploadFrames, uploadControlBytes, uploadFramingBytes, uploadWrites, uploadBytes}
	require.Equal(t, len(want), len(got))
	for _, d := range want {
		require.Equal(t, true, got[d], "%v not described", d)
	}
}
