package p2p

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/OffchainLabs/prysm/v7/beacon-chain/p2p/internal/segmentgossip"
	p2ptest "github.com/OffchainLabs/prysm/v7/beacon-chain/p2p/testing"
	"github.com/OffchainLabs/prysm/v7/config/features"
	"github.com/OffchainLabs/prysm/v7/config/params"
	"github.com/OffchainLabs/prysm/v7/testing/require"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/sirupsen/logrus"
	logTest "github.com/sirupsen/logrus/hooks/test"
)

func TestSegmentGossipParams(t *testing.T) {
	stock := pubsubGossipParam()
	require.Equal(t, pubsub.DefaultGossipSubParams().MaxIHaveMessages, stock.MaxIHaveMessages)

	reset := features.InitWithReset(&features.Flags{EnableSegmentedPayloadGossip: true})
	defer reset()
	on := pubsubGossipParam()
	require.Equal(t, segmentgossip.MaxIHaveMessages, on.MaxIHaveMessages)
}

const segmentGroupNoted = "Payload segment group noted"

// segmentGroupLines returns the hook's lines of a group's first noting.
func segmentGroupLines(hook *logTest.Hook) []*logrus.Entry {
	var lines []*logrus.Entry
	for _, e := range hook.AllEntries() {
		if e.Message == segmentGroupNoted {
			lines = append(lines, e)
		}
	}
	return lines
}

// TestSegmentGroupSeenLogsTheFirstNoting: with the payload's path on, a group's first noting logs
// one Info line with its root, the slot the type keyed it by (the router's, here 9, not the clock's
// 7), its threshold, count and topic. A second noting of the group logs none, and with the path off
// nothing is logged.
func TestSegmentGroupSeenLogsTheFirstNoting(t *testing.T) {
	const topic = "/eth2/ffffffff/execution_payload_segment/ssz_snappy"
	root := [32]byte{0xab, 0x01}
	hook := logTest.NewGlobal()

	off := &Service{}
	off.SegmentGroupSeen(root, topic, 64, 32)
	require.Equal(t, 0, len(segmentGroupLines(hook)), "lines with the path off")

	svc := &Service{
		pubsub: p2ptest.NewTestP2P(t).PubSub(),
		segmentPayloadType: segmentgossip.NewPayloadType(segmentgossip.PayloadConfig{
			Topics:       []string{topic},
			CurrentTopic: func() string { return topic },
			CurrentSlot:  func() uint64 { return 7 },
			UplinkMbps:   100,
		}),
	}
	svc.segmentPayloadType.Advance(9, pubsub.DefaultSeacastLimits.LiveWindow)
	svc.SegmentGroupSeen(root, topic, 64, 32)
	require.Equal(t, 1, len(segmentGroupLines(hook)), "lines after the first noting")
	svc.SegmentGroupSeen(root, topic, 64, 32)
	lines := segmentGroupLines(hook)
	require.Equal(t, 1, len(lines), "lines after the second noting of the group")
	e := lines[0]
	require.Equal(t, logrus.InfoLevel, e.Level)
	require.Equal(t, "0xab01"+strings.Repeat("0", 60), e.Data["group"])
	require.Equal(t, uint64(9), e.Data["slot"])
	require.Equal(t, 32, e.Data["k"])
	require.Equal(t, 64, e.Data["n"])
	require.Equal(t, topic, e.Data["topic"])
}

// TestSegmentGroupSeenLogsBeforeTheRouter: the line is logged before the group is handed to the
// router, which waits until the router's loop takes the event, so the line's time is the noting's and
// not the noting's plus that wait. Here the loop is held inside a window advance whose callback blocks;
// the line comes while it is held, with the slot the type keyed the group by (12, while the clock reads
// 10), and SegmentGroupSeen returns only once the loop is let go.
func TestSegmentGroupSeenLogsBeforeTheRouter(t *testing.T) {
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
	require.Equal(t, uint64(10), s.seacastCurrentSlot())
	s.seacastAdvance(12)
	root := [32]byte{0xcd, 0x02}
	topic, ok := s.segmentPayloadType.TopicOf(string(root[:]))
	require.Equal(t, true, ok)
	hook := logTest.NewGlobal()

	entered, release, advanced := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once sync.Once
	letGo := func() { once.Do(func() { close(release) }) }
	defer letGo()
	go func() {
		s.pubsub.SeacastAdvanceSlotWith(13, func(uint64) { close(entered); <-release })
		close(advanced)
	}()
	<-entered
	seen := make(chan struct{})
	go func() {
		s.SegmentGroupSeen(root, topic, 64, 32)
		close(seen)
	}()
	var lines []*logrus.Entry
	for deadline := time.Now().Add(2 * time.Second); time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
		if lines = segmentGroupLines(hook); len(lines) > 0 {
			break
		}
	}
	select {
	case <-seen:
		t.Fatal("SegmentGroupSeen returned while the router's loop was held")
	default:
	}
	letGo()
	<-seen
	<-advanced
	require.Equal(t, 1, len(lines), "lines logged while the router's loop was held")
	require.Equal(t, "0xcd02"+strings.Repeat("0", 60), lines[0].Data["group"])
	require.Equal(t, uint64(12), lines[0].Data["slot"])
	require.Equal(t, 1, len(segmentGroupLines(hook)), "lines after the router took the group")
}
