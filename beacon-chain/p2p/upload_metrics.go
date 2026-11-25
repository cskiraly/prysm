package p2p

import (
	"sync"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/prometheus/client_golang/prometheus"
)

// The bytes the node wrote to its gossipsub streams, as the router counts them at each write, after
// every queue filter and cancel: a frame dropped from a queue never counts. Frames are messages,
// pushed unasked or answering an IWANT; a frame's bytes include its field's tag and length. The
// frames, the control and the framing add up to the bytes written.
var (
	uploadFrameBytes = prometheus.NewDesc(
		"p2p_pubsub_written_frame_bytes_total",
		"Bytes of the message frames the node wrote to its gossipsub streams, by topic and class (push: sent unasked; answer: answering an IWANT).",
		[]string{"topic", "class"}, nil,
	)
	uploadFrames = prometheus.NewDesc(
		"p2p_pubsub_written_frames_total",
		"Message frames the node wrote to its gossipsub streams, by topic and class (push: sent unasked; answer: answering an IWANT).",
		[]string{"topic", "class"}, nil,
	)
	uploadControlBytes = prometheus.NewDesc(
		"p2p_pubsub_written_control_bytes_total",
		"Bytes of gossipsub control the node wrote, over all topics, by kind (ihave, iwant, idontwant, graft_prune). With segmented payload gossip on, idontwant also carries the HAVE holdings and the acknowledgements of pushed segments.",
		[]string{"kind"}, nil,
	)
	uploadFramingBytes = prometheus.NewDesc(
		"p2p_pubsub_written_framing_bytes_total",
		"The rest of the node's gossipsub writes: length prefixes, subscriptions, partial messages and the RPCs' own framing.",
		nil, nil,
	)
	uploadWrites = prometheus.NewDesc(
		"p2p_pubsub_writes_total",
		"Writes to the node's gossipsub streams.",
		nil, nil,
	)
	uploadBytes = prometheus.NewDesc(
		"p2p_pubsub_written_bytes_total",
		"All bytes the node wrote to its gossipsub streams.",
		nil, nil,
	)
)

// uploadSource is a router's count of its written bytes, its totals and its frames by topic read
// together.
type uploadSource interface {
	UploadSnapshot() (pubsub.UploadStat, map[string]pubsub.TopicUpload)
}

// uploadCollector exports the written bytes of the node's running router, with the segment path on
// or off. It is registered once; a service points it at its router once the router is built, and its
// Stop clears it if it is still the one exported.
type uploadCollector struct {
	mu  sync.Mutex
	src uploadSource
}

var uploadBytesCollector = &uploadCollector{}

func init() {
	prometheus.MustRegister(uploadBytesCollector)
}

func (c *uploadCollector) set(src uploadSource) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.src = src
}

// clear stops exporting src, if it is the one exported.
func (c *uploadCollector) clear(src uploadSource) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.src == src {
		c.src = nil
	}
}

func (c *uploadCollector) source() uploadSource {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.src
}

// Describe sends every metric the collector sends.
func (*uploadCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range []*prometheus.Desc{uploadFrameBytes, uploadFrames, uploadControlBytes, uploadFramingBytes, uploadWrites, uploadBytes} {
		ch <- d
	}
}

// Collect reads the router's counts and sends them, none before a router is built.
func (c *uploadCollector) Collect(ch chan<- prometheus.Metric) {
	src := c.source()
	if src == nil {
		return
	}
	counter := func(d *prometheus.Desc, v int64, labels ...string) {
		ch <- prometheus.MustNewConstMetric(d, prometheus.CounterValue, float64(v), labels...)
	}
	s, byTopic := src.UploadSnapshot() // one snapshot, so the series add up within a scrape
	for topic, tu := range byTopic {
		counter(uploadFrameBytes, tu.PushBytes, topic, "push")
		counter(uploadFrameBytes, tu.AnswerBytes, topic, "answer")
		counter(uploadFrames, tu.PushFrames, topic, "push")
		counter(uploadFrames, tu.AnswerFrames, topic, "answer")
	}
	counter(uploadControlBytes, s.IHaveBytes, "ihave")
	counter(uploadControlBytes, s.IWantBytes, "iwant")
	counter(uploadControlBytes, s.IDontWantBytes, "idontwant")
	counter(uploadControlBytes, s.GraftPruneBytes, "graft_prune")
	counter(uploadFramingBytes, s.OtherBytes)
	counter(uploadWrites, s.Writes)
	counter(uploadBytes, s.AllBytes)
}
