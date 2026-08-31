package sync

// RowDAS (EIP-8371) row subnet subscription.
//
// A row topic is unlike every other gossip topic in this file: it carries no full messages at
// all. Everything that travels on it is a gossipsub partial message, handled by the partial
// broadcaster outside the normal validate-then-handle pipeline. What this file provides is the
// subscription itself -- which is what makes gossipsub graft a mesh, without which no partial
// exchange happens either -- plus a validator that rejects anything arriving as a full message.

import (
	"context"

	"github.com/OffchainLabs/prysm/v7/beacon-chain/core/peerdas"
	"github.com/OffchainLabs/prysm/v7/config/params"
	"github.com/OffchainLabs/prysm/v7/consensus-types/primitives"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"
)

// rowSubnetIndices is the single row subnet this node participates in.
//
// Membership is a pure function of the node ID, so it needs no ENR entry, no MetaData field and
// no per-epoch rotation -- unlike every other subnet in this file. It is also deliberately
// stable across forks: EIP-8371 fixes the subnet count so that an upgrade does not force every
// row mesh to re-form at once.
func (s *Service) rowSubnetIndices(primitives.Slot) map[uint64]bool {
	subnet, err := peerdas.RowSubnetForNode(s.cfg.p2p.NodeID())
	if err != nil {
		log.WithError(err).Error("Could not compute row subnet")
		return nil
	}

	return map[uint64]bool{subnet: true}
}

// allRowSubnets is every row subnet, which is what peer discovery searches over. Unlike the
// subscription, which is one subnet, finding peers is worth doing across all of them: a
// reconstructor pushing recovered cells into column subnets it does not custody needs peers
// there too.
func (s *Service) allRowSubnets(primitives.Slot) map[uint64]bool {
	return mapFromCount(params.BeaconConfig().RowSubnetCount)
}

// validateDataRow rejects anything that arrives on a row topic as a full gossip message.
//
// Row topics carry partial messages only. A full message here is not a protocol variation we do
// not support yet -- there is no encoding for one -- so a peer sending it is either broken or
// probing, and REJECT is the honest answer. The partial messages themselves never reach this
// function: gossipsub hands them to the broadcaster's extension callback instead.
func (s *Service) validateDataRow(_ context.Context, pid peer.ID, msg *pubsub.Message) (pubsub.ValidationResult, error) {
	// Our own messages are not rejected, only ignored: we never publish a full message on a row
	// topic, so this can only be a harness or a bug, and downscoring ourselves is pointless.
	if pid == s.cfg.p2p.PeerID() {
		return pubsub.ValidationIgnore, nil
	}

	log.WithFields(logrus.Fields{
		"peer":  pid,
		"topic": msg.GetTopic(),
		"bytes": len(msg.Data),
	}).Debug("Rejecting full message on a row topic, which carries partial messages only")

	return pubsub.ValidationReject, errors.New("row topics carry partial messages only")
}

// dataRowSubscriber is never called: validateDataRow rejects every full message, so nothing
// reaches the handler. It exists because the subscription machinery requires one, and because
// the subscription is what makes gossipsub graft a mesh for the partial exchange to use.
func (s *Service) dataRowSubscriber(context.Context, proto.Message) error {
	return errors.New("row topics deliver no full messages")
}
