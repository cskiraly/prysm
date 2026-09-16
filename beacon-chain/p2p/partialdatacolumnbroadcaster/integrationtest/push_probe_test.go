package integrationtest

// What does it actually take to deliver a cell into a column subnet you are not subscribed to?
//
// R9 measured zero deliveries from 64 pushes and this is the probe that isolates why. Two nodes:
// one joined-but-not-subscribed pushing a single-cell column, one subscribed and expected to
// complete it. The two subtests differ only in whether the pusher declares partial interest.

import (
	"context"
	"crypto/rand"
	"fmt"
	"testing"
	"testing/synctest"
	"time"

	"github.com/OffchainLabs/go-bitfield"
	"github.com/OffchainLabs/prysm/v7/beacon-chain/core/peerdas"
	"github.com/OffchainLabs/prysm/v7/beacon-chain/p2p"
	"github.com/OffchainLabs/prysm/v7/beacon-chain/p2p/encoder"
	"github.com/OffchainLabs/prysm/v7/beacon-chain/p2p/partialdatacolumnbroadcaster"
	"github.com/OffchainLabs/prysm/v7/beacon-chain/verification"
	fieldparams "github.com/OffchainLabs/prysm/v7/config/fieldparams"
	"github.com/OffchainLabs/prysm/v7/config/params"
	"github.com/OffchainLabs/prysm/v7/consensus-types/blocks"
	"github.com/OffchainLabs/prysm/v7/testing/require"
	"github.com/OffchainLabs/prysm/v7/testing/util"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
	simlibp2p "github.com/libp2p/go-libp2p/x/simlibp2p"
	"github.com/marcopolo/simnet"
	"github.com/sirupsen/logrus"
)

// TestPushIntoUnsubscribedColumnTopicNeedsPartialInterest is the mechanism R9 forced into the
// open, and it took two corrections to state properly.
//
// The reading going in was that joining a topic is enough to push on it: MeshPeers falls back to
// fanout peers, and the send predicate is iSupportSendingPartial && peerRequestsPartial, the first
// satisfied by joining and the second by the subscriber's own announcement. All true, and all
// insufficient, because the partial protocol is request-driven -- an eager push carries the block
// header and a bitmap, not cells. The cells go out only in answer to the peer's request.
//
// So two things have to hold, and the first correction was assuming only one of them did:
//
//	the subscriber has to ask.   A node that received a header and nothing else does not
//	                             republish -- `Published` is false and both the republish and the
//	                             heartbeat-gossip paths check it. What makes a real node ask is
//	                             `emptyPartialColumnsRequestingAll` on block arrival, which
//	                             publishes an empty column requesting every cell. A probe that
//	                             omits that measures the harness, not the protocol.
//	the pusher has to be         The subscriber's own MeshPeers is built from the peers that
//	addressable.                 announced a subscription to the topic, and the pusher announced
//	                             nothing. Its request goes nowhere. Declaring partial interest is
//	                             what puts the pusher in that set.
//
// The result, over the 2x2: the subscriber's request is the only thing that matters. The interest
// announcement changes nothing in either row.
//
// That falsified the prediction this probe was written to check. The prediction was that the
// subscriber's request could not *reach* a pusher that announced nothing, since the subscriber's
// own MeshPeers is built from the peers that announced a subscription to the topic. It reaches it
// anyway -- the extension addresses the peers it already holds group state for, and the pusher's
// eager push created exactly that state at the subscriber. Recording the wrong prediction rather
// than deleting it, because a push arm had already been changed to declare interest on the strength
// of it, and the change had to be reverted.
func TestPushIntoUnsubscribedColumnTopicNeedsPartialInterest(t *testing.T) {
	t.Run("no request from the subscriber: nothing arrives either way", func(t *testing.T) {
		require.Equal(t, false, runPushProbe(t, false, false),
			"a subscriber that never asks cannot be served")
		require.Equal(t, false, runPushProbe(t, true, false),
			"and the interest announcement does not change that")
	})

	t.Run("the subscriber's request is what delivers, with or without interest", func(t *testing.T) {
		require.Equal(t, true, runPushProbe(t, false, true),
			"a push into an unsubscribed topic delivers once the subscriber asks -- no announcement needed")
		require.Equal(t, true, runPushProbe(t, true, true),
			"and declaring interest neither helps nor hurts")
	})
}

// runPushProbe returns whether the subscriber completed its column. subscriberRequests makes the
// subscriber publish an empty request-all column, which is what a real node does on block arrival.
func runPushProbe(t *testing.T, declareInterest, subscriberRequests bool) bool {
	t.Helper()

	var completed bool
	synctest.Test(t, func(t *testing.T) {
		latency := 10 * time.Millisecond
		network, meta, err := simlibp2p.SimpleLibp2pNetwork([]simlibp2p.NodeLinkSettingsAndCount{
			{LinkSettings: simnet.NodeBiDiLinkSettings{
				Downlink: simnet.LinkSettings{BitsPerSecond: 20 * simlibp2p.OneMbps},
				Uplink:   simnet.LinkSettings{BitsPerSecond: 20 * simlibp2p.OneMbps},
			}, Count: 2},
		}, simnet.StaticLatency(latency/2), simlibp2p.NetworkSettings{UseBlankHost: true})
		require.NoError(t, err)
		network.Start()
		defer network.Close()
		defer func() {
			for _, node := range meta.Nodes {
				if err := node.Close(); err != nil {
					panic(err)
				}
			}
		}()

		pusherHost, subscriberHost := meta.Nodes[0], meta.Nodes[1]
		synctest.Wait()

		logger := logrus.New()
		logger.SetLevel(logrus.ErrorLevel)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		pusher := partialdatacolumnbroadcaster.NewBroadcaster(ctx, logger)
		subscriber := partialdatacolumnbroadcaster.NewBroadcaster(ctx, logger)

		baseOpts := []pubsub.Option{
			pubsub.WithMessageSigning(false),
			pubsub.WithStrictSignatureVerification(false),
		}
		pusherPS, err := pubsub.NewGossipSub(ctx, pusherHost, pusher.AppendPubSubOpts(baseOpts)...)
		require.NoError(t, err)
		subscriberPS, err := pubsub.NewGossipSub(ctx, subscriberHost, subscriber.AppendPubSubOpts(baseOpts)...)
		require.NoError(t, err)

		// One cell, so a single pushed cell is the whole column and completion is unambiguous.
		const numCells = 1
		var blockRoot [fieldparams.RootLength]byte
		copy(blockRoot[:], []byte("push-probe-root"))
		commitments := make([][]byte, numCells)
		cells := make([][]byte, numCells)
		proofs := make([][]byte, numCells)
		for i := range numCells {
			commitments[i] = make([]byte, 48)
			cells[i] = make([]byte, 2048)
			_, err := rand.Read(cells[i])
			require.NoError(t, err)
			proofs[i] = make([]byte, 48)
			_ = fmt.Appendf(proofs[i][:0], "proof %d", i)
		}
		roDC, _ := util.CreateTestVerifiedRoDataColumnSidecars(t, []util.DataColumnParam{{
			BodyRoot:       blockRoot[:],
			KzgCommitments: commitments,
			Column:         cells,
			KzgProofs:      proofs,
		}})
		header := roDC[0].DataColumnSidecar().SignedBlockHeader
		headerRoot, err := header.Header.HashTreeRoot()
		require.NoError(t, err)
		kcs, err := roDC[0].KzgCommitments()
		require.NoError(t, err)
		incp, err := roDC[0].KzgCommitmentsInclusionProof()
		require.NoError(t, err)

		pushColumn, err := blocks.NewPartialDataColumn(headerRoot, header, roDC[0].Index(), kcs, incp)
		require.NoError(t, err)
		require.Equal(t, true, pushColumn.ExtendFromVerifiedCell(0, roDC[0].Column()[0], roDC[0].KzgProofs()[0]))

		digest := params.ForkDigest(0)
		subnet := peerdas.ComputeSubnetForDataColumnSidecar(roDC[0].Index())
		topicStr := fmt.Sprintf(p2p.DataColumnSubnetTopicFormat, digest, subnet) +
			encoder.SszNetworkEncoder{}.ProtocolSuffix()

		time.Sleep(100 * time.Millisecond)

		pusherTopic, err := pusherPS.Join(topicStr, pubsub.RequestPartialMessages())
		require.NoError(t, err)
		subscriberTopic, err := subscriberPS.Join(topicStr, pubsub.RequestPartialMessages())
		require.NoError(t, err)

		newVerifier := func(col *blocks.PartialDataColumn) (*verification.PartialColumnVerifier, error) {
			mock := &verification.MockDataColumnsVerifier{}
			mock.AppendRODataColumns(col.RODataColumn)

			return verification.NewPartialColumnVerifier(mock, col), nil
		}
		pusherDone := make(chan blocks.VerifiedRODataColumn, 4)
		subscriberDone := make(chan blocks.VerifiedRODataColumn, 4)
		go pusher.Start(&testColumnCallbacks{t: t, newVerifier: newVerifier, completeCh: pusherDone, label: "pusher"}, nil)
		go subscriber.Start(&testColumnCallbacks{t: t, newVerifier: newVerifier, completeCh: subscriberDone, label: "subscriber"}, nil)

		require.NoError(t, pusherHost.Connect(context.Background(), peer.AddrInfo{
			ID:    subscriberHost.ID(),
			Addrs: subscriberHost.Addrs(),
		}))
		time.Sleep(300 * time.Millisecond)

		// Only the subscriber subscribes. The pusher stays joined-but-unsubscribed, which is the
		// state cross-forwarding puts it in for a column it does not custody.
		sub, err := subscriberTopic.Subscribe()
		require.NoError(t, err)
		defer sub.Cancel()
		require.NoError(t, subscriber.Subscribe(ctx, subscriberTopic))

		time.Sleep(2 * time.Second)

		if declareInterest {
			require.NoError(t, pusherTopic.SetPartialInterest(context.Background(), true))
			time.Sleep(500 * time.Millisecond)
		}

		require.NoError(t, pusher.Publish(ctx, func(yield func(string, blocks.PartialDataColumn) bool) {
			yield(topicStr, pushColumn)
		}))
		time.Sleep(500 * time.Millisecond)

		if subscriberRequests {
			// What a real node does on block arrival: an empty column asking for every cell.
			// See emptyPartialColumnsRequestingAll in beacon-chain/sync.
			requestColumn, err := blocks.NewPartialDataColumn(headerRoot, header, roDC[0].Index(), kcs, incp)
			require.NoError(t, err)
			requests := bitfield.NewBitlist(uint64(numCells)).Not()
			require.NoError(t, requestColumn.SetPartsRequests(requests))
			require.NoError(t, subscriber.Publish(ctx, func(yield func(string, blocks.PartialDataColumn) bool) {
				yield(topicStr, requestColumn)
			}))
		}

		select {
		case <-subscriberDone:
			completed = true
		case <-time.After(10 * time.Second):
			completed = false
		}
	})

	return completed
}
