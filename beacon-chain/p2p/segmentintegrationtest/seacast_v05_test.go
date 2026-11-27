package segmentintegrationtest

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/OffchainLabs/prysm/v7/beacon-chain/p2p"
	"github.com/OffchainLabs/prysm/v7/beacon-chain/p2p/encoder"
	"github.com/OffchainLabs/prysm/v7/beacon-chain/p2p/internal/segmentgossip"
	"github.com/OffchainLabs/prysm/v7/beacon-chain/verification/segmentauth"
	"github.com/OffchainLabs/prysm/v7/config/params"
	"github.com/OffchainLabs/prysm/v7/testing/require"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pubsubpb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	simlibp2p "github.com/libp2p/go-libp2p/x/simlibp2p"
	"github.com/marcopolo/simnet"
)

// The v0.5 rules beside the promise at the write, at the node's router options: each test below is
// one rule's case, and fails with the options as they were before the rule was installed.

// v05Network is count hosts on the path's links, started.
func v05Network(t *testing.T, count int) (*simnet.Simnet, []host.Host) {
	t.Helper()
	net, meta, err := simlibp2p.SimpleLibp2pNetwork([]simlibp2p.NodeLinkSettingsAndCount{
		{LinkSettings: simnet.NodeBiDiLinkSettings{
			Downlink: simnet.LinkSettings{BitsPerSecond: pathDownMbps * simlibp2p.OneMbps},
			Uplink:   simnet.LinkSettings{BitsPerSecond: pathUpMbps * simlibp2p.OneMbps},
		}, Count: count},
	}, simnet.StaticLatency(pathLatency), simlibp2p.NetworkSettings{UseBlankHost: true})
	require.NoError(t, err)
	net.Start()
	synctest.Wait()
	return net, meta.Nodes
}

// v05Node is a node on h with the node's payload type and router options, and an outbound queue
// of queue RPCs per peer; extra options go after the node's, and replace what they set again.
func v05Node(t *testing.T, ctx context.Context, h host.Host, topic string, queue int, extra ...pubsub.Option) *pathNode {
	t.Helper()
	typ := segmentgossip.NewPayloadType(segmentgossip.PayloadConfig{
		Topics:       []string{topic},
		CurrentTopic: func() string { return topic },
		CurrentSlot:  func() uint64 { return pathSlot },
		UplinkMbps:   pathUpMbps,
	})
	setting := segmentgossip.Setting{
		SegmentTopics: []string{topic},
		Allowed:       func(string) bool { return true },
		TypeTopics:    typ.Topics(),
		Params:        typ.Params(),
		Limits:        pubsub.DefaultSeacastLimits,
		Cancel:        true,
		UplinkMbps:    pathUpMbps,
	}
	require.NoError(t, segmentgossip.CheckSetting(setting))
	n := &pathNode{self: h.ID(), typ: typ, m: &segmentgossip.PathMetrics{}, lowest: map[peer.ID]float64{}}
	opts := append([]pubsub.Option{
		pubsub.WithMessageSignaturePolicy(pubsub.StrictNoSign),
		pubsub.WithNoAuthor(),
		pubsub.WithPeerOutboundQueueSize(queue),
		pubsub.WithMessageIdFn(func(pmsg *pubsubpb.Message) string { return p2p.MsgID(pmsg) }),
		pubsub.WithGossipSubParams(pathGossipParams()),
	}, segmentgossip.RouterOptions(setting, typ, pathHeartbeat, n.m)...)
	opts = append(opts, extra...)
	ps, err := pubsub.NewGossipSub(ctx, h, opts...)
	require.NoError(t, err)
	n.ps = ps
	require.NoError(t, ps.RegisterTopicValidator(topic, n.validate))
	n.topic, err = ps.Join(topic)
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

// v05Injected is node A with the node's options and an injector beside it, subscribed to the
// segment topic, after the meshes have settled.
func v05Injected(t *testing.T) (*pathNode, *injector, string, func()) {
	t.Helper()
	net, hs := v05Network(t, 2)
	topic := segmentTopic()
	ctx, cancel := context.WithCancel(context.Background())
	a := v05Node(t, ctx, hs[0], topic, pubsubQueueSize)
	in := newInjector(hs[1])
	require.NoError(t, hs[1].Connect(context.Background(), peer.AddrInfo{ID: hs[0].ID(), Addrs: hs[0].Addrs()}))
	in.open(t, hs[0].ID(), topic)
	time.Sleep(3 * time.Second)
	synctest.Wait()
	return a, in, topic, func() {
		cancel()
		a.sub.Cancel()
		closeHosts(hs)
		net.Close()
		synctest.Wait()
	}
}

// chunkOfOpenGroup is a payload chunk's structured id, root ‖ index ‖ hash, of a group the node has
// not seen: an offer of it is asked.
func chunkOfOpenGroup(fill byte, index byte) string {
	return string(bytes.Repeat([]byte{fill}, 32)) + string([]byte{index, 0, 0, 0}) + string(bytes.Repeat([]byte{1}, 20))
}

// askedWithin waits up to d for the injector to be asked id.
func askedWithin(in *injector, id string, d time.Duration) bool {
	for waited := time.Duration(0); waited < d; waited += 10 * time.Millisecond {
		if _, ok := in.askedAt(id); ok {
			return true
		}
		time.Sleep(10 * time.Millisecond)
		synctest.Wait()
	}
	_, ok := in.askedAt(id)
	return ok
}

// TestSeacastCloseWithConn: a promise closes, and parks nobody, when "the peer's
// connection closes. The ask or its answer went with the connection, and the retry, whose timer is
// per ask, moves on to the other announcers." (seacast-lite.md §6.1). The injector offers a chunk, A
// writes its ask, and the injector closes the connection before the promise falls due. Without the
// rule the promise falls due 400 ms after the write and parks the injector for the group, though the
// ask went with the connection.
func TestSeacastCloseWithConn(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		params.SetupTestConfigCleanup(t)
		a, in, topic, stop := v05Injected(t)
		defer stop()

		id := chunkOfOpenGroup(0x31, 1)
		in.offer(topic, id)
		require.Equal(t, true, askedWithin(in, id, 3*pathHeartbeat), "the offer was not asked")
		require.NoError(t, in.h.Network().ClosePeer(a.self))
		time.Sleep(2 * time.Second)
		synctest.Wait()

		closed, breaks, parks := a.m.V05.ClosedPromises.Load(), a.m.ParkBreaks.Load(), a.m.Parks.Load()
		t.Logf("after the close: %d promises closed with the connection, %d broken, %d parks", closed, breaks, parks)
		require.Equal(t, int64(0), breaks, "the closed connection's promise fell due and broke")
		require.Equal(t, int64(0), parks, "the closed connection's peer was parked")
		require.Equal(t, true, closed > 0, "no promise closed with the connection")
	})
}

// TestSeacastLastResort: "When every announcer of a chunk is parked, the node asks each of them
// once more, after waits that double (the last resort)." (seacast-lite.md §6.1). The injector is the
// chunk's only announcer and doesn't answer: A asks, the promise breaks and parks it for the group.
// Without the rule A never asks it again, and the chunk is out of reach for the park's 30 s, longer
// than the group's live window.
func TestSeacastLastResort(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		params.SetupTestConfigCleanup(t)
		a, in, topic, stop := v05Injected(t)
		defer stop()

		id := chunkOfOpenGroup(0x32, 1)
		in.offer(topic, id)
		time.Sleep(6 * time.Second)
		synctest.Wait()

		in.mu.Lock()
		asks, total := in.asks[id], in.total
		in.mu.Unlock()
		parks, lastResort, parkedAsked := a.m.Parks.Load(), a.m.V05.LastResortAsks.Load(), a.m.V05.ParkedAsked.Load()
		t.Logf("the only announcer, never answering: asked %d times, %d under the offered id; %d parks; last-resort asks %d, to a parked peer %d",
			total, asks, parks, lastResort, parkedAsked)
		require.Equal(t, true, parks > 0, "the case needs the announcer parked")
		// Once more, and only once: "at most one ask per announcer and chunk" by the last resort. The
		// first ask names the offered id; the last resort's asks by index, since on the path an offer
		// records no id (seacast-lite.md §4.4). The injector offers nothing else, so every ask it
		// receives is of this chunk.
		require.Equal(t, 1, asks, "the first ask, under the offered id")
		require.Equal(t, 2, total, "the parked announcer was not asked exactly once more")
		// The last resort's ask is by the offered chunk's index: WANT ‖ version ‖ root ‖ index,
		// little-endian (seacast-lite.md §2's WantIndex), with v4's flags and hops after it if any.
		// The id is the root's 32 bytes, then the index's 4.
		var other []string
		in.mu.Lock()
		for asked := range in.asks {
			if asked != id {
				other = append(other, asked)
			}
		}
		in.mu.Unlock()
		require.Equal(t, 1, len(other), "asks under another id than the offered one")
		w := other[0]
		require.Equal(t, true, len(w) >= 41 && strings.HasPrefix(w, "WANT"), fmt.Sprintf("the last resort's ask is not an ask by index: %x", w))
		require.Equal(t, fmt.Sprintf("%x", id[:32]), fmt.Sprintf("%x", w[5:37]), "the last resort's ask names another group")
		require.Equal(t, fmt.Sprintf("%x", id[32:36]), fmt.Sprintf("%x", w[37:41]), "the last resort's ask names another index")
		require.Equal(t, int64(1), lastResort, "last-resort asks")
		require.Equal(t, int64(1), parkedAsked, "last-resort asks to a parked announcer")
	})
}

// TestSeacastOwedAtFullQueue: "A chosen push that ... a full queue refuses (§8.2), is no longer
// counted as pushed, and the chunk is offered to the peer instead ... An answer that a full queue
// refuses is owed" (seacast-lite.md §5.2, §4.1). The source's queue to the receiver holds two RPCs, and
// it publishes a group at once: the pushes past the queue are refused. Without the rule they stay
// counted as pushed, nobody offers them, and an answer the queue refuses is lost.
func TestSeacastOwedAtFullQueue(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		params.SetupTestConfigCleanup(t)
		net, hs := v05Network(t, 2)
		topic := segmentTopic()
		ctx, cancel := context.WithCancel(context.Background())
		src := v05Node(t, ctx, hs[0], topic, 2)
		dst := v05Node(t, ctx, hs[1], topic, pubsubQueueSize)
		defer func() {
			cancel()
			src.sub.Cancel()
			dst.sub.Cancel()
			closeHosts(hs)
			net.Close()
			synctest.Wait()
		}()
		require.NoError(t, hs[1].Connect(context.Background(), peer.AddrInfo{ID: hs[0].ID(), Addrs: hs[0].Addrs()}))
		time.Sleep(pathSettle)
		synctest.Wait()

		env := pathEnvelope(t, 32, 4096)
		want, err := env.MarshalSSZ()
		require.NoError(t, err)
		segs, err := segmentauth.SegmentMessagesForEnvelope(env, segmentauth.DefaultParams())
		require.NoError(t, err)
		key, count := string(segs[0].Group), int(segs[0].Descriptor.Count) // lint:ignore uintcast -- bounded by segments.MaxSegments.
		publishGroup(t, src, topic, segs)
		time.Sleep(pathSettle)
		synctest.Wait()

		got, held := heldEnvelope(t, dst, key, count)
		v := &src.m.V05
		t.Logf("a group of %d at once through a queue of 2: the receiver holds %d; unpushed %d, offers owed %d, owed offer indices attached %d, answers owed %d",
			count, held, v.Unpushed.Load(), v.OffersOwed.Load(), v.OwedSent.Load(), v.AnswersOwed.Load())
		t.Logf("the receiver: %d broken promises, %d parks", dst.m.ParkBreaks.Load(), dst.m.Parks.Load())
		require.Equal(t, true, bytes.Equal(want, got), "the receiver does not hold the source's envelope")
		require.Equal(t, true, v.Unpushed.Load() > 0, "no refused push was uncounted")
		require.Equal(t, true, v.OwedSent.Load() > 0, "no owed offer index was attached")
		// The answers half: an answer the full queue refused is owed and sent, so the receiver's
		// asks of the source break no promise and park nobody (§4.1's reason for owing it).
		require.Equal(t, true, v.AnswersOwed.Load() > 0, "no refused answer was owed")
		require.Equal(t, int64(0), dst.m.ParkBreaks.Load(), "the receiver broke a promise of the honest source")
		require.Equal(t, int64(0), dst.m.Parks.Load(), "the receiver parked the honest source")
	})
}

// TestSeacastFanIn (FAN_IN): the payload's projection assumes "one other pusher assumed as fast as
// us", FAN_IN = 2 (seacast-lite.md §8), so a push is skipped once what the peer holds plus twice what
// was sent to it and not yet confirmed reaches K (seacast-lite.md §7.2's projected). The payload type
// left it unset, and the router took 1. This is the type the node installs, whose Params the
// router's writers read at each pop; that they skip at 2 where they push at 1 is the fork's
// TestSeacastTypeProjectionFanIn.
func TestSeacastFanIn(t *testing.T) {
	topic := segmentTopic()
	typ := segmentgossip.NewPayloadType(segmentgossip.PayloadConfig{
		Topics:       []string{topic},
		CurrentTopic: func() string { return topic },
		CurrentSlot:  func() uint64 { return pathSlot },
		UplinkMbps:   pathUpMbps,
	})
	require.Equal(t, 2, typ.Params().ProjectFanIn)
}

// TestSeacastV05StockUnchanged: the v0.5 rules are SEACAST's, and leave the topics that share the
// node's router as they were. The same script of stock-topic traffic runs at the base's options
// (the promise at the write alone) and at the node's (OfferSkipped too), and every outcome on A must
// agree: the asks each announcer got, the messages delivered, the broken promises and parks, and
// every v0.5 counter. The script reaches the paths a chunk's pull takes that a stock id must not:
// the retry past an announcer that doesn't answer, a parked announcer's offer again (the last
// resort's parked branch), and a connection closing with an ask outstanding (the close with the
// connection). Then a full queue (OfferSkipped's owed answers): the v0.5 counters are compared
// before this step, which owes chunk answers at the node by design, and the step compares the stock
// message's copies at X. P, Q and X are injectors on the stock topic only, so A's payload publish
// pushes to nobody; every stock id is a block's, 20 bytes, as prysm makes one. A's queue to each
// peer holds two RPCs.
func TestSeacastV05StockUnchanged(t *testing.T) {
	type outcome struct {
		asks      map[string][2]int // per id: asks to P, to Q
		delivered map[string]bool
		breaks    int64
		parks     int64
		v05       string
		stockAtX  int   // copies of the full queue's stock message X received
		chunksAtX int   // chunks of the group X received (differs by design: logged only)
		owed      int64 // chunk answers A owed X (the node's; the step's precondition)
	}
	run := func(t *testing.T, base bool) (out outcome) {
		synctest.Test(t, func(t *testing.T) {
			params.SetupTestConfigCleanup(t)
			net, hs := v05Network(t, 4)
			topic := segmentTopic()
			stock := strings.Replace(topic, p2p.GossipExecutionPayloadSegmentMessage, p2p.GossipBlockMessage, 1)
			ctx, cancel := context.WithCancel(context.Background())
			var extra []pubsub.Option
			ctr := &pubsub.SpecV05Counters{}
			if base {
				extra = append(extra, pubsub.WithSpecV05(pubsub.SpecV05{PromiseAtWrite: true}, ctr))
			}
			a := v05Node(t, ctx, hs[0], topic, 2, extra...)
			if !base {
				ctr = &a.m.V05
			}
			st, err := a.ps.Join(stock)
			require.NoError(t, err)
			sub, err := st.Subscribe()
			require.NoError(t, err)
			var mu sync.Mutex
			got := map[string]bool{}
			go func() {
				for {
					msg, err := sub.Next(ctx)
					if err != nil {
						return
					}
					mu.Lock()
					got[msg.ID] = true
					mu.Unlock()
				}
			}()
			p, q, x := newInjector(hs[1]), newInjector(hs[2]), newInjector(hs[3])
			defer func() {
				cancel()
				a.sub.Cancel()
				sub.Cancel()
				closeHosts(hs)
				net.Close()
				synctest.Wait()
			}()
			for _, in := range []*injector{p, q, x} {
				require.NoError(t, in.h.Connect(context.Background(), peer.AddrInfo{ID: hs[0].ID(), Addrs: hs[0].Addrs()}))
			}
			p.open(t, hs[0].ID(), stock)
			q.open(t, hs[0].ID(), stock)
			// X is on the payload's topic too: the router keeps a peer's HAVE state, and with it the
			// hose's hold on its writer, only for a peer it sends the payload to.
			x.open(t, hs[0].ID(), topic, stock)
			time.Sleep(3 * time.Second)
			synctest.Wait()
			wait := func(d time.Duration) {
				time.Sleep(d)
				synctest.Wait()
			}

			// The retry: P offers s1 and doesn't answer; Q offers it inside the ask window and serves it.
			m1, s1 := ordinaryMessage(stock, "the retry")
			q.mu.Lock()
			q.serve[s1] = m1
			q.mu.Unlock()
			p.offer(stock, s1)
			wait(50 * time.Millisecond)
			q.offer(stock, s1)
			wait(time.Second)

			// A parked announcer: P alone offers s2 and doesn't answer, so its promise breaks and parks
			// it for the scope every stock id shares; then it offers s2 again.
			_, s2 := ordinaryMessage(stock, "the park")
			p.offer(stock, s2)
			wait(time.Second)
			p.offer(stock, s2)
			wait(time.Second)

			// The connection's close: Q offers s3, is asked, and closes the connection before the
			// promise falls due.
			_, s3 := ordinaryMessage(stock, "the close")
			q.offer(stock, s3)
			require.Equal(t, true, askedWithin(q, s3, time.Second), "s3 was not asked")
			wait(50 * time.Millisecond)
			require.NoError(t, q.h.Network().ClosePeer(hs[0].ID()))
			wait(2 * time.Second)

			out = outcome{asks: map[string][2]int{}, delivered: map[string]bool{}}
			for name, id := range map[string]string{"s1": s1, "s2": s2, "s3": s3} {
				p.mu.Lock()
				q.mu.Lock()
				out.asks[name] = [2]int{p.asks[id], q.asks[id]}
				q.mu.Unlock()
				p.mu.Unlock()
				mu.Lock()
				out.delivered[name] = got[id]
				mu.Unlock()
			}
			out.v05 = v05Counters(ctr)

			// The full queue: A publishes a group of more than 71 chunks and X asks for all of them. A's
			// queue to X holds two RPCs: the base drops the answers past them; the node owes them and
			// moves them back in as room appears. X never acknowledges a frame, so after 69 in flight
			// (A's hose window at 50 Mbps), about 330 ms after X's ask, A's writers wait until the first
			// frames time out (WINDOW_TIMEOUT, 500 ms after their pop), and the node's queue to X is
			// full of owed answers through the wait (between pops, a write under way, it has room).
			// Then, inside the wait, A publishes a stock message and X asks for it: X must get both
			// copies at the node as at the base.
			segs, err := segmentauth.SegmentMessagesForEnvelope(pathEnvelope(t, 160, 4096), segmentauth.DefaultParams())
			require.NoError(t, err)
			require.Equal(t, true, len(segs) > 71, "the group must outrun the hose window of 69")
			ids := make([]string, 0, len(segs))
			for _, sg := range segs {
				pbm, err := sg.ToProto()
				require.NoError(t, err)
				buf := new(bytes.Buffer)
				_, err = encoder.SszNetworkEncoder{}.EncodeGossip(buf, pbm)
				require.NoError(t, err)
				ids = append(ids, p2p.MsgID(&pubsubpb.Message{Data: buf.Bytes(), Topic: &topic}))
			}
			publishGroup(t, a, topic, segs)
			wait(100 * time.Millisecond)
			x.send(&pubsubpb.RPC{Control: &pubsubpb.ControlMessage{Iwant: []*pubsubpb.ControlIWant{{MessageIDs: ids}}}})
			wait(400 * time.Millisecond)
			m4, s4 := ordinaryMessage(stock, "the full queue")
			require.NoError(t, st.Publish(ctx, m4.Data))
			wait(30 * time.Millisecond)
			x.send(&pubsubpb.RPC{Control: &pubsubpb.ControlMessage{Iwant: []*pubsubpb.ControlIWant{{MessageIDs: []string{s4}}}}})
			wait(2 * time.Second)
			x.mu.Lock()
			out.stockAtX = x.got[s4]
			for _, id := range ids {
				if x.got[id] > 0 {
					out.chunksAtX++
				}
			}
			x.mu.Unlock()
			out.owed = ctr.AnswersOwed.Load()
			out.breaks, out.parks = a.m.ParkBreaks.Load(), a.m.Parks.Load()
		})
		return out
	}
	var b, n outcome
	t.Run("base", func(t *testing.T) { b = run(t, true) })
	t.Run("node", func(t *testing.T) { n = run(t, false) })
	for _, o := range []struct {
		name string
		o    outcome
	}{{"base", b}, {"node", n}} {
		t.Logf("%s: asks (P, Q) %v; delivered %v; %d broken promises, %d parks; v0.5 counters before the full queue %s; "+
			"the full queue: the stock message's copies at X %d, chunks at X %d, chunk answers owed %d",
			o.name, o.o.asks, o.o.delivered, o.o.breaks, o.o.parks, o.o.v05, o.o.stockAtX, o.o.chunksAtX, o.o.owed)
	}
	// The script's expected outcomes at the base's options: the retry asks Q, which serves s1; P alone is asked
	// s2 once, breaks and is parked, and is not asked again; Q is asked s3 once.
	require.Equal(t, [2]int{1, 1}, b.asks["s1"], "s1's asks at the base")
	require.Equal(t, true, b.delivered["s1"], "s1 delivered at the base")
	require.Equal(t, [2]int{1, 0}, b.asks["s2"], "s2's asks at the base")
	require.Equal(t, [2]int{0, 1}, b.asks["s3"], "s3's asks at the base")
	require.Equal(t, true, b.parks > 0, "the script needs P parked")
	require.Equal(t, 2, b.stockAtX, "the full queue's stock message at X at the base: the forward and the answer")
	require.Equal(t, true, n.owed > 0, "the full queue owed no chunk answer at the node: the step did not stage an owed answer")
	// The node's rules leave all of it as it was.
	require.DeepEqual(t, b.asks, n.asks, "asks")
	require.DeepEqual(t, b.delivered, n.delivered, "deliveries")
	require.Equal(t, b.breaks, n.breaks, "broken promises")
	require.Equal(t, b.parks, n.parks, "parks")
	require.Equal(t, b.v05, n.v05, "v0.5 counters")
	require.Equal(t, b.stockAtX, n.stockAtX, "the full queue's stock message at X")
}

// v05Counters is every v0.5 counter, by name.
func v05Counters(c *pubsub.SpecV05Counters) string {
	v := reflect.ValueOf(c).Elem()
	parts := make([]string, 0, v.NumField())
	for i := 0; i < v.NumField(); i++ {
		parts = append(parts, fmt.Sprintf("%s=%d", v.Type().Field(i).Name, v.Field(i).Addr().Interface().(*atomic.Int64).Load()))
	}
	return strings.Join(parts, " ")
}
