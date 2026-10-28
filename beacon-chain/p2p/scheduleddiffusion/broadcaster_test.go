package scheduleddiffusion

import (
	"context"
	"testing"
	"time"

	"github.com/OffchainLabs/prysm/v7/container/segments"
	"github.com/OffchainLabs/prysm/v7/testing/require"
	pubsub_pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/sirupsen/logrus"
)

const testTopic = "test-topic"

// newTestBroadcaster returns a broadcaster serving testTopic. It is not started: the inbound
// callbacks run on pubsub's goroutine and touch nothing the loop owns.
func newTestBroadcaster(t *testing.T) *Broadcaster {
	t.Helper()
	b := New(context.Background(), logrus.New(), Config{})
	b.ServeTopic(testTopic)
	return b
}

// metaRPC builds an incoming RPC carrying only metadata.
func metaRPC(t *testing.T, groupID []byte, m *segments.PartsMetadata) *pubsub_pb.PartialMessagesExtension {
	t.Helper()
	encoded, err := m.Marshal()
	require.NoError(t, err)
	topic := testTopic
	return &pubsub_pb.PartialMessagesExtension{TopicID: &topic, GroupID: groupID, PartsMetadata: encoded}
}

func TestOnIncomingRPC(t *testing.T) {
	g := newTestGroup(t, 4)
	from := peer.ID("a")
	groupID := g.msgs[0].Descriptor.GroupID(g.hasher)

	t.Run("metadata unions into theirs and replaces wanted", func(t *testing.T) {
		b := newTestBroadcaster(t)
		states := map[peer.ID]PeerState{}

		first, err := segments.NewPartsMetadata(g.count)
		require.NoError(t, err)
		require.NoError(t, first.Available.Set(0))
		require.NoError(t, first.Requests.Set(2))
		require.NoError(t, first.Requests.Set(3))
		require.NoError(t, b.onIncomingRPC(from, states, metaRPC(t, groupID, first)))
		require.DeepEqual(t, []uint32{0}, states[from].Theirs.Indices())
		require.DeepEqual(t, []uint32{2, 3}, states[from].Wanted.Indices())

		// A second announce that drops a bit must not shrink what we believe they hold --
		// what a peer once had it still has -- but its want set replaces the old one
		// outright, because a want bitmap is a standing order and not a delta.
		second, err := segments.NewPartsMetadata(g.count)
		require.NoError(t, err)
		require.NoError(t, second.Available.Set(1))
		require.NoError(t, second.Requests.Set(3))
		require.NoError(t, b.onIncomingRPC(from, states, metaRPC(t, groupID, second)))
		require.DeepEqual(t, []uint32{0, 1}, states[from].Theirs.Indices())
		require.DeepEqual(t, []uint32{3}, states[from].Wanted.Indices())
		require.Equal(t, 2, b.Counters().MetadataReceived)
	})

	t.Run("a data frame records the holding and clears the want", func(t *testing.T) {
		b := newTestBroadcaster(t)
		states := map[peer.ID]PeerState{}

		meta, err := segments.NewPartsMetadata(g.count)
		require.NoError(t, err)
		for i := uint32(0); i < g.count; i++ {
			require.NoError(t, meta.Requests.Set(i))
		}
		require.NoError(t, b.onIncomingRPC(from, states, metaRPC(t, groupID, meta)))
		require.Equal(t, 4, states[from].Wanted.Len())

		encoded, err := segments.MarshalPartialMessage([]*segments.SegmentMessage{g.msgs[2]})
		require.NoError(t, err)
		topic := testTopic
		require.NoError(t, b.onIncomingRPC(from, states, &pubsub_pb.PartialMessagesExtension{
			TopicID: &topic, GroupID: groupID, PartialMessage: encoded,
		}))
		// They demonstrably hold what they sent us, and cannot still be waiting for it.
		require.Equal(t, true, states[from].Theirs.Has(2))
		require.Equal(t, false, states[from].Wanted.Has(2))
		require.Equal(t, 3, states[from].Wanted.Len())
		// The batch is queued for verification rather than hashed on pubsub's goroutine.
		require.Equal(t, 1, len(b.verifyQueue()))
	})

	t.Run("a want that outlives the grace clears the push", func(t *testing.T) {
		b := newTestBroadcaster(t)
		b.now = func() time.Time { return passNow }
		states := map[peer.ID]PeerState{from: {Pushed: g.bits(t, 1, 2), PushedAt: passNow.Add(-2 * time.Second)}}

		meta, err := segments.NewPartsMetadata(g.count)
		require.NoError(t, err)
		require.NoError(t, meta.Requests.Set(1))
		require.NoError(t, b.onIncomingRPC(from, states, metaRPC(t, groupID, meta)))
		// Still asked for two seconds after we pushed it: lost. The other push stands.
		require.Equal(t, false, states[from].Pushed.Has(1))
		require.Equal(t, true, states[from].Pushed.Has(2))
		require.Equal(t, 1, b.Counters().PushesExpired)
	})

	t.Run("within the grace a want does not clear the push", func(t *testing.T) {
		b := newTestBroadcaster(t)
		b.now = func() time.Time { return passNow }
		states := map[peer.ID]PeerState{from: {Pushed: g.bits(t, 1), PushedAt: passNow.Add(-100 * time.Millisecond)}}

		meta, err := segments.NewPartsMetadata(g.count)
		require.NoError(t, err)
		require.NoError(t, meta.Requests.Set(1))
		require.NoError(t, b.onIncomingRPC(from, states, metaRPC(t, groupID, meta)))
		// The frame is merely queued; forgetting it now would send it twice.
		require.Equal(t, true, states[from].Pushed.Has(1))
		require.Equal(t, 0, b.Counters().PushesExpired)
	})

	t.Run("a peer may not change the group's segment count", func(t *testing.T) {
		b := newTestBroadcaster(t)
		states := map[peer.ID]PeerState{}

		first, err := segments.NewPartsMetadata(g.count)
		require.NoError(t, err)
		require.NoError(t, b.onIncomingRPC(from, states, metaRPC(t, groupID, first)))

		wider, err := segments.NewPartsMetadata(g.count * 2)
		require.NoError(t, err)
		require.ErrorIs(t, b.onIncomingRPC(from, states, metaRPC(t, groupID, wider)), segments.ErrCountMismatch)
	})

	t.Run("segments filed under the wrong group are refused", func(t *testing.T) {
		b := newTestBroadcaster(t)
		states := map[peer.ID]PeerState{}

		encoded, err := segments.MarshalPartialMessage([]*segments.SegmentMessage{g.msgs[0]})
		require.NoError(t, err)
		topic := testTopic
		err = b.onIncomingRPC(from, states, &pubsub_pb.PartialMessagesExtension{
			TopicID: &topic, GroupID: []byte("not-the-group"), PartialMessage: encoded,
		})
		require.ErrorIs(t, err, ErrGroupMismatch)
		require.Equal(t, 1, b.Counters().SegmentsRejected)
	})

	t.Run("a topic we do not serve is ignored", func(t *testing.T) {
		b := newTestBroadcaster(t)
		states := map[peer.ID]PeerState{}

		meta, err := segments.NewPartsMetadata(g.count)
		require.NoError(t, err)
		rpc := metaRPC(t, groupID, meta)
		other := "some-other-topic"
		rpc.TopicID = &other
		require.NoError(t, b.onIncomingRPC(from, states, rpc))
		require.Equal(t, 0, len(states))
		require.Equal(t, 0, b.Counters().MetadataReceived)
	})
}

func TestNewAppliesDefaults(t *testing.T) {
	t.Run("updates take the urgent lane unless told otherwise", func(t *testing.T) {
		require.Equal(t, false, New(context.Background(), logrus.New(), Config{}).cfg.NormalLaneUpdates)
		on := New(context.Background(), logrus.New(), Config{NormalLaneUpdates: true})
		require.Equal(t, true, on.cfg.NormalLaneUpdates)
	})
}
