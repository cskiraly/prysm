package segmentbroadcaster

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/OffchainLabs/prysm/v7/container/segments"
	"github.com/OffchainLabs/prysm/v7/testing/require"
	"github.com/libp2p/go-libp2p-pubsub/partialmessages"
	pubsub_pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/sirupsen/logrus"
)

// allowAll admits every descriptor.
type allowAll struct{}

func (allowAll) AuthenticateDescriptor(*segments.Descriptor, []byte) error { return nil }

// windowCell is a started broadcaster whose extension is stubbed at the one seam it publishes
// through: each pass runs against per-group peer states the test sets, as the extension's map
// would be. Everything from the extension's hooks inward -- loop, reassembler, ledger, window,
// policy -- is the production path.
type windowCell struct {
	b      *Broadcaster
	mu     sync.Mutex
	states map[string]map[peer.ID]PeerState
	sent   map[string]map[peer.ID][]uint32
}

const windowTopic = "t"

func newWindowCell(t *testing.T) (*windowCell, func()) {
	t.Helper()
	c := &windowCell{
		b:      New(context.Background(), logrus.New(), Config{Policy: PushPhase, Replication: 2}),
		states: map[string]map[peer.ID]PeerState{},
		sent:   map[string]map[peer.ID][]uint32{},
	}
	c.b.publishPartial = func(_ string, groupID []byte, fn partialmessages.PublishActionsFn[PeerState]) error {
		c.mu.Lock()
		defer c.mu.Unlock()
		g := string(groupID)
		for id, action := range fn(c.states[g], func(peer.ID) bool { return true }) {
			require.NoError(t, action.Err)
			if len(action.EncodedPartialMessage) == 0 {
				continue
			}
			msgs, _, err := segments.UnmarshalPartialMessage(action.EncodedPartialMessage)
			require.NoError(t, err)
			for _, m := range msgs {
				c.sent[g][id] = append(c.sent[g][id], m.Index)
			}
		}
		return nil
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		require.NoError(t, c.b.Start(Callbacks{Auth: allowAll{}, OnEnvelope: func([]byte) error { return nil }}))
	}()
	synctest.Wait()
	return c, func() {
		c.b.Stop()
		wg.Wait()
	}
}

// group builds an 8-segment group whose payload is seeded, so two seeds are two groups.
func windowGroup(t *testing.T, seed byte) ([]*segments.SegmentMessage, []byte) {
	t.Helper()
	h, err := segments.HasherByID(segments.HashSHA256)
	require.NoError(t, err)
	payload := make([]byte, 8*64)
	for i := range payload {
		payload[i] = byte(i*31) + seed
	}
	msgs, err := segments.BuildSegmentMessages(payload, 64, h)
	require.NoError(t, err)
	return msgs, msgs[0].Descriptor.GroupID(h)
}

// blank gives a group three peers the extension knows nothing about, as after it forgot them,
// and clears what was sent.
func (c *windowCell) blank(groupID []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.states[string(groupID)] = map[peer.ID]PeerState{"a": {}, "b": {}, "c": {}}
	c.sent[string(groupID)] = map[peer.ID][]uint32{}
}

// pass runs a publish pass for a group through the extension's gossip hook.
func (c *windowCell) pass(groupID []byte) {
	c.b.onEmitGossip(windowTopic, groupID, nil, nil)
	synctest.Wait()
}

func (c *windowCell) sentTotal(groupID []byte) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, idxs := range c.sent[string(groupID)] {
		n += len(idxs)
	}
	return n
}

// TestPushWindow is Config.PushWindow on the broadcaster's own path: a late pass over peer state
// the extension has forgotten must not push a group again, but must still serve what is asked.
func TestPushWindow(t *testing.T) {
	t.Run("a fresh group is pushed replication-wide", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			c, stop := newWindowCell(t)
			defer stop()
			msgs, g := windowGroup(t, 1)
			c.blank(g)
			require.NoError(t, c.b.Publish(windowTopic, msgs))
			synctest.Wait()
			require.Equal(t, 16, c.sentTotal(g))
			require.Equal(t, 16, c.b.Counters().SegmentsPushed)
			require.Equal(t, 0, c.b.Counters().PushesPastWindow)
		})
	})

	t.Run("inside the window a blank pass still pushes", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			c, stop := newWindowCell(t)
			defer stop()
			msgs, g := windowGroup(t, 1)
			c.blank(g)
			require.NoError(t, c.b.Publish(windowTopic, msgs))
			synctest.Wait()
			time.Sleep(defaultPushWindow - 100*time.Millisecond)
			c.blank(g)
			c.pass(g)
			require.Equal(t, 16, c.sentTotal(g))
		})
	})

	t.Run("past the window a blank pass pushes nothing and still announces", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			c, stop := newWindowCell(t)
			defer stop()
			msgs, g := windowGroup(t, 1)
			c.blank(g)
			require.NoError(t, c.b.Publish(windowTopic, msgs))
			synctest.Wait()
			time.Sleep(defaultPushWindow)
			c.blank(g)
			before := c.b.Counters()
			c.pass(g)
			after := c.b.Counters()
			require.Equal(t, 0, c.sentTotal(g))
			require.Equal(t, before.SegmentsPushed, after.SegmentsPushed)
			require.Equal(t, 16, after.PushesPastWindow)
			// Every blank peer was still told what we hold, so it can ask.
			require.Equal(t, before.MetadataSent+3, after.MetadataSent)
		})
	})

	t.Run("past the window a request is served and nothing else sent", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			c, stop := newWindowCell(t)
			defer stop()
			msgs, g := windowGroup(t, 1)
			c.blank(g)
			require.NoError(t, c.b.Publish(windowTopic, msgs))
			synctest.Wait()
			time.Sleep(defaultPushWindow)
			c.blank(g)
			// A late contact asks for two segments, through the extension's incoming-RPC hook.
			want, err := segments.NewPartsMetadata(8)
			require.NoError(t, err)
			require.NoError(t, want.Requests.Set(1))
			require.NoError(t, want.Requests.Set(4))
			enc, err := want.Marshal()
			require.NoError(t, err)
			topic := windowTopic
			before := c.b.Counters()
			c.mu.Lock()
			require.NoError(t, c.b.onIncomingRPC("a", c.states[string(g)], &pubsub_pb.PartialMessagesExtension{
				TopicID: &topic, GroupID: g, PartsMetadata: enc,
			}))
			c.mu.Unlock()
			synctest.Wait()
			after := c.b.Counters()
			c.mu.Lock()
			got := append([]uint32(nil), c.sent[string(g)]["a"]...)
			c.mu.Unlock()
			sort.Slice(got, func(i, j int) bool { return got[i] < got[j] })
			require.Equal(t, fmt.Sprint([]uint32{1, 4}), fmt.Sprint(got))
			require.Equal(t, 2, c.sentTotal(g))
			require.Equal(t, before.SegmentsServed+2, after.SegmentsServed)
			require.Equal(t, before.SegmentsPushed, after.SegmentsPushed)
		})
	})

	t.Run("a group first held later has its own window", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			c, stop := newWindowCell(t)
			defer stop()
			old, g1 := windowGroup(t, 1)
			c.blank(g1)
			require.NoError(t, c.b.Publish(windowTopic, old))
			synctest.Wait()
			time.Sleep(defaultPushWindow)
			fresh, g2 := windowGroup(t, 2)
			c.blank(g1)
			c.blank(g2)
			require.NoError(t, c.b.Publish(windowTopic, fresh))
			synctest.Wait()
			c.pass(g1)
			require.Equal(t, 16, c.sentTotal(g2))
			require.Equal(t, 0, c.sentTotal(g1))
		})
	})
}
