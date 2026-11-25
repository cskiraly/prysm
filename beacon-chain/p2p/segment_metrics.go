package p2p

import (
	"reflect"
	"sync/atomic"

	"github.com/OffchainLabs/prysm/v7/beacon-chain/p2p/internal/segmentgossip"
	"github.com/prometheus/client_golang/prometheus"
)

// seacastPathEvents is the metric the payload path's counters are exported under, one series per
// counter, labelled by the counter's name in the router (pubsub.SeacastCounters) or in the node's
// options (segmentgossip.PathMetrics).
var seacastPathEvents = prometheus.NewDesc(
	"seacast_path_events_total",
	"SEACAST's payload path: what the router and the node's path options counted, by counter.",
	[]string{"counter"}, nil,
)

// seacastCollector exports the path's counters of the node's running router. It is registered once;
// a service on the segment path points it at its own counters once its router is built, and its Stop
// clears them if they are still the ones exported.
type seacastCollector struct {
	m atomic.Pointer[segmentgossip.PathMetrics]
}

var seacastPathCollector = &seacastCollector{}

func init() {
	prometheus.MustRegister(seacastPathCollector)
}

// Describe sends the one metric the collector sends; its series are the counters' names, read at
// collection.
func (*seacastCollector) Describe(ch chan<- *prometheus.Desc) { ch <- seacastPathEvents }

// Collect sends one series per counter of the path's metrics, none before a router is on the path.
func (c *seacastCollector) Collect(ch chan<- prometheus.Metric) {
	m := c.m.Load()
	if m == nil {
		return
	}
	collectCounters(ch, reflect.ValueOf(&m.Seacast).Elem(), "")
	collectCounters(ch, reflect.ValueOf(m).Elem(), "")
	// The v0.5 rules' counters share names with the path's (Expired), so theirs carry a prefix.
	collectCounters(ch, reflect.ValueOf(&m.V05).Elem(), "V05")
	if m.Payload != nil {
		collectCounters(ch, reflect.ValueOf(m.Payload).Elem(), "")
	}
}

// collectCounters sends every atomic.Int64 field of the struct v as a series under its name, after
// prefix.
func collectCounters(ch chan<- prometheus.Metric, v reflect.Value, prefix string) {
	t := v.Type()
	for i := range t.NumField() {
		f := t.Field(i)
		if !f.IsExported() {
			continue
		}
		c, ok := v.Field(i).Addr().Interface().(*atomic.Int64)
		if !ok {
			continue
		}
		ch <- prometheus.MustNewConstMetric(seacastPathEvents, prometheus.CounterValue, float64(c.Load()), prefix+f.Name)
	}
}
