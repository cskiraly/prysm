package segmentintegrationtest

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"os"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/OffchainLabs/prysm/v7/beacon-chain/p2p"
	"github.com/OffchainLabs/prysm/v7/beacon-chain/p2p/internal/segmentgossip"
	"github.com/OffchainLabs/prysm/v7/beacon-chain/verification/segmentauth"
	"github.com/OffchainLabs/prysm/v7/config/params"
	"github.com/OffchainLabs/prysm/v7/testing/require"
	"github.com/golang/snappy"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pubsubpb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	simlibp2p "github.com/libp2p/go-libp2p/x/simlibp2p"
	"github.com/marcopolo/simnet"
	"google.golang.org/protobuf/proto"
)

// injector is a peer that speaks gossipsub's wire by hand: it subscribes, sends the IHAVEs a test
// writes, records when each id is asked of it, and answers an ask for a message it was given.
type injector struct {
	h  host.Host
	mu sync.Mutex
	// asked is when each id was first asked of the injector by IWANT; asks how many times it was;
	// total every id asked, in any form (an ask by index too, which names no chunk id).
	asked map[string]time.Time
	asks  map[string]int
	total int
	// serve is the message to answer an ask with, by id.
	serve map[string]*pubsubpb.Message
	// got counts the frames the injector received, by message id.
	got map[string]int
	out network.Stream
}

func newInjector(h host.Host) *injector {
	in := &injector{h: h, asked: map[string]time.Time{}, asks: map[string]int{}, serve: map[string]*pubsubpb.Message{}, got: map[string]int{}}
	h.SetStreamHandler(pubsub.GossipSubID_v11, in.read)
	return in
}

func (in *injector) read(s network.Stream) {
	r := bufio.NewReader(s)
	for {
		size, err := binary.ReadUvarint(r)
		if err != nil || size > 1<<22 {
			_ = s.Reset()
			return
		}
		b := make([]byte, size)
		if _, err := io.ReadFull(r, b); err != nil {
			_ = s.Reset()
			return
		}
		rpc := &pubsubpb.RPC{}
		if err := proto.Unmarshal(b, rpc); err != nil {
			continue
		}
		var answer []*pubsubpb.Message
		in.mu.Lock()
		for _, m := range rpc.GetPublish() {
			in.got[p2p.MsgID(m)]++
		}
		for _, iw := range rpc.GetControl().GetIwant() {
			for _, id := range iw.GetMessageIDs() {
				if _, ok := in.asked[id]; !ok {
					in.asked[id] = time.Now()
				}
				in.asks[id]++
				in.total++
				if m, ok := in.serve[id]; ok {
					answer = append(answer, m)
				}
			}
		}
		in.mu.Unlock()
		if len(answer) > 0 {
			in.send(&pubsubpb.RPC{Publish: answer})
		}
	}
}

// open makes the injector's stream to to and subscribes it to topics.
func (in *injector) open(t *testing.T, to peer.ID, topics ...string) {
	t.Helper()
	s, err := in.h.NewStream(context.Background(), to, pubsub.GossipSubID_v11)
	require.NoError(t, err)
	in.out = s
	subs := make([]*pubsubpb.RPC_SubOpts, 0, len(topics))
	for _, topic := range topics {
		subscribe := true
		subs = append(subs, &pubsubpb.RPC_SubOpts{Subscribe: &subscribe, Topicid: &topic})
	}
	in.send(&pubsubpb.RPC{Subscriptions: subs})
}

func (in *injector) send(rpc *pubsubpb.RPC) {
	b, err := proto.Marshal(rpc)
	if err != nil {
		panic(err)
	}
	buf := binary.AppendUvarint(nil, uint64(len(b)))
	if _, err := in.out.Write(append(buf, b...)); err != nil {
		panic(err)
	}
}

// offer sends one IHAVE RPC naming ids on topic.
func (in *injector) offer(topic string, ids ...string) {
	in.send(&pubsubpb.RPC{Control: &pubsubpb.ControlMessage{
		Ihave: []*pubsubpb.ControlIHave{{TopicID: &topic, MessageIDs: ids}},
	}})
}

func (in *injector) askedAt(id string) (time.Time, bool) {
	in.mu.Lock()
	defer in.mu.Unlock()
	at, ok := in.asked[id]
	return at, ok
}

// ordinaryMessage is a message on an ordinary topic, as prysm frames one, and its 20-byte id.
func ordinaryMessage(topic string, body string) (*pubsubpb.Message, string) {
	m := &pubsubpb.Message{Data: snappy.Encode(nil, []byte(body)), Topic: &topic}
	return m, p2p.MsgID(m)
}

// TestSeacastInjectedOffers is the rest of TestSeacastPayloadPath's ground: an ordinary topic beside the
// payload's, under one IHAVE limit shared by both, and stop-pull. Node A runs the node's
// router options with MaxIHaveMessages at 1; node B is a source on the path; the injector offers by hand.
//   - An ordinary offer alone is asked at once.
//   - A payload offer and then an ordinary offer in the same heartbeat: the payload's RPC takes the one
//     place, and the ordinary ask is held back and written after the next heartbeat; the message arrives.
//   - After A completes B's group, an offer of one of its indices under another content id is declined
//     by stop-pull and counted, and no IWANT for it is written.
func TestSeacastInjectedOffers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		params.SetupTestConfigCleanup(t)
		net, meta, err := simlibp2p.SimpleLibp2pNetwork([]simlibp2p.NodeLinkSettingsAndCount{
			{LinkSettings: simnet.NodeBiDiLinkSettings{
				Downlink: simnet.LinkSettings{BitsPerSecond: pathDownMbps * simlibp2p.OneMbps},
				Uplink:   simnet.LinkSettings{BitsPerSecond: pathUpMbps * simlibp2p.OneMbps},
			}, Count: 3},
		}, simnet.StaticLatency(pathLatency), simlibp2p.NetworkSettings{UseBlankHost: true})
		require.NoError(t, err)
		net.Start()
		synctest.Wait()

		segTopic := segmentTopic()
		ordTopic := strings.Replace(segTopic, p2p.GossipExecutionPayloadSegmentMessage, p2p.GossipBlockMessage, 1)
		ctx, cancel := context.WithCancel(context.Background())
		mk := func(h host.Host, maxIHave int) *pathNode {
			typ := segmentgossip.NewPayloadType(segmentgossip.PayloadConfig{
				Topics:       []string{segTopic},
				CurrentTopic: func() string { return segTopic },
				CurrentSlot:  func() uint64 { return pathSlot },
				UplinkMbps:   pathUpMbps,
			})
			setting := segmentgossip.Setting{
				SegmentTopics: []string{segTopic},
				Allowed:       func(string) bool { return true },
				TypeTopics:    typ.Topics(),
				Params:        typ.Params(),
				Limits:        pubsub.DefaultSeacastLimits,
				Cancel:        true,
				UplinkMbps:    pathUpMbps,
			}
			require.NoError(t, segmentgossip.CheckSetting(setting))
			gp := pathGossipParams()
			gp.MaxIHaveMessages = maxIHave
			m := &segmentgossip.PathMetrics{}
			opts := append([]pubsub.Option{
				pubsub.WithMessageSignaturePolicy(pubsub.StrictNoSign),
				pubsub.WithNoAuthor(),
				pubsub.WithPeerOutboundQueueSize(pubsubQueueSize),
				pubsub.WithMessageIdFn(func(pmsg *pubsubpb.Message) string { return p2p.MsgID(pmsg) }),
				pubsub.WithGossipSubParams(gp),
			}, segmentgossip.RouterOptions(setting, typ, pathHeartbeat, m)...)
			ps, err := pubsub.NewGossipSub(ctx, h, opts...)
			require.NoError(t, err)
			n := &pathNode{self: h.ID(), ps: ps, typ: typ, m: m}
			require.NoError(t, ps.RegisterTopicValidator(segTopic, n.validate))
			n.topic, err = ps.Join(segTopic)
			require.NoError(t, err)
			n.sub, err = n.topic.Subscribe()
			require.NoError(t, err)
			go func() {
				for {
					if _, err := n.sub.Next(ctx); err != nil {
						return
					}
				}
			}()
			ps.SeacastAdvanceSlot(pathSlot)
			return n
		}
		// SEACAST_TEST_NODE_IHAVE_LIMIT=1 gives A the node's limit, under which nothing is held
		// back: the shared-limit case must then fail.
		limitA := 1
		if os.Getenv("SEACAST_TEST_NODE_IHAVE_LIMIT") != "" {
			limitA = segmentgossip.MaxIHaveMessages
		}
		a := mk(meta.Nodes[0], limitA)
		b := mk(meta.Nodes[1], segmentgossip.MaxIHaveMessages)
		ordA, err := a.ps.Join(ordTopic)
		require.NoError(t, err)
		ordSub, err := ordA.Subscribe()
		require.NoError(t, err)
		got := make(chan string, 16)
		go func() {
			for {
				msg, err := ordSub.Next(ctx)
				if err != nil {
					return
				}
				got <- string(msg.Data)
			}
		}()
		in := newInjector(meta.Nodes[2])
		defer func() {
			cancel()
			a.sub.Cancel()
			b.sub.Cancel()
			ordSub.Cancel()
			closeHosts(meta.Nodes)
			net.Close()
			synctest.Wait()
		}()

		ha, hb, hc := meta.Nodes[0], meta.Nodes[1], meta.Nodes[2]
		require.NoError(t, hb.Connect(context.Background(), peer.AddrInfo{ID: ha.ID(), Addrs: ha.Addrs()}))
		require.NoError(t, hc.Connect(context.Background(), peer.AddrInfo{ID: ha.ID(), Addrs: ha.Addrs()}))
		in.open(t, ha.ID(), segTopic, ordTopic)
		time.Sleep(3 * time.Second)
		synctest.Wait()

		// An ordinary offer alone: asked at once, and the message arrives.
		m0, id0 := ordinaryMessage(ordTopic, "alone")
		in.mu.Lock()
		in.serve[id0] = m0
		in.mu.Unlock()
		sent := time.Now()
		in.offer(ordTopic, id0)
		time.Sleep(pathHeartbeat / 4)
		synctest.Wait()
		at, ok := in.askedAt(id0)
		require.Equal(t, true, ok, "the lone ordinary offer was not asked")
		require.Equal(t, true, at.Sub(sent) < pathHeartbeat/4, "the lone ordinary offer was asked after %v", at.Sub(sent))
		time.Sleep(time.Second)
		synctest.Wait()
		require.Equal(t, string(m0.Data), <-got)

		// A payload offer, then an ordinary one in the same heartbeat.
		heldBack := a.m.Seacast.HeldBack.Load()
		other := bytes.Repeat([]byte{0x5a}, 32)
		pid := string(other) + string(make([]byte, 4)) + string(bytes.Repeat([]byte{1}, 20))
		m1, id1 := ordinaryMessage(ordTopic, "behind a payload offer")
		in.mu.Lock()
		in.serve[id1] = m1
		in.mu.Unlock()
		sent = time.Now()
		in.offer(segTopic, pid)
		in.offer(ordTopic, id1)
		time.Sleep(3 * pathHeartbeat)
		synctest.Wait()
		// The payload offer of a group A has not completed is asked: stop-pull's control below.
		_, ok = in.askedAt(pid)
		require.Equal(t, true, ok, "the payload offer of an open group was not asked")
		at, ok = in.askedAt(id1)
		require.Equal(t, true, ok, "the held-back ordinary ask was never written")
		t.Logf("the ordinary ask behind a payload offer went out %v after the offer; held back %d, retried %d",
			at.Sub(sent), a.m.Seacast.HeldBack.Load()-heldBack, a.m.Seacast.HeldBackRetried.Load())
		require.Equal(t, true, a.m.Seacast.HeldBack.Load() > heldBack, "no ask was held back")
		require.Equal(t, true, a.m.Seacast.HeldBackRetried.Load() > 0, "no held-back ask was retried")
		require.Equal(t, true, at.Sub(sent) >= pathHeartbeat/2, "the ordinary ask went out at once (%v)", at.Sub(sent))
		time.Sleep(time.Second)
		synctest.Wait()
		require.Equal(t, string(m1.Data), <-got)

		// After completion: B publishes a group, A completes it, and the injector offers two of its
		// indices under content ids A has not seen. Neither is asked, and neither reaches stop-pull's
		// gate: A holds every index below the group's count, and held answers first; an index at
		// the count is no valid id once the count is known, and is ignored.
		env := pathEnvelope(t, 32, 4096)
		segs, err := segmentauth.SegmentMessagesForEnvelope(env, segmentauth.DefaultParams())
		require.NoError(t, err)
		key := string(segs[0].Group)
		publishGroup(t, b, segTopic, segs)
		time.Sleep(pathSettle)
		synctest.Wait()
		require.Equal(t, int64(1), a.m.Seacast.GroupsCompleted.Load(), "A did not complete B's group")
		offerLate := func(index uint32) (asked bool, declined int64) {
			before := a.m.Seacast.StopPullDeclined.Load()
			idx := make([]byte, 4)
			binary.LittleEndian.PutUint32(idx, index)
			late := key + string(idx) + string(bytes.Repeat([]byte{2}, 20))
			in.offer(segTopic, late)
			time.Sleep(3 * pathHeartbeat)
			synctest.Wait()
			_, asked = in.askedAt(late)
			return asked, a.m.Seacast.StopPullDeclined.Load() - before
		}
		asked, declined := offerLate(1)
		t.Logf("a held index after completion: stop-pull declined %d more", declined)
		require.Equal(t, false, asked, "a held index of a completed group was asked")
		require.Equal(t, int64(0), declined, "a held index reached the gate")
		count := a.typ.IndexCount(key)
		asked, declined = offerLate(uint32(count))
		t.Logf("index %d, at the group's count: stop-pull declined %d more", count, declined)
		require.Equal(t, false, asked, "an index at a completed group's count was asked")
		require.Equal(t, int64(0), declined, "an index at the group's count reached the gate")
	})
}
