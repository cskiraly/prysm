package pubsub

import (
	"iter"
	"sync"

	"github.com/libp2p/go-libp2p/core/peer"
)

// MessageBatch allows a user to batch related messages and then publish them at
// once. This allows the Scheduler to define an order for outgoing RPCs.
// This helps bandwidth constrained peers.
type MessageBatch struct {
	mu       sync.Mutex
	messages []*Message
}

func (mb *MessageBatch) add(msg *Message) {
	mb.mu.Lock()
	defer mb.mu.Unlock()
	mb.messages = append(mb.messages, msg)
}

func (mb *MessageBatch) take() []*Message {
	mb.mu.Lock()
	defer mb.mu.Unlock()
	messages := mb.messages
	mb.messages = nil
	return messages
}

type messageBatchAndPublishOptions struct {
	messages []*Message
	opts     *BatchPublishOptions
}

// RPCScheduler schedules outgoing RPCs.
type RPCScheduler interface {
	// AddRPC adds an RPC to the scheduler.
	AddRPC(peer peer.ID, msgID string, rpc *RPC)
	// All returns an ordered iterator of RPCs.
	All() iter.Seq2[peer.ID, *RPC]
}

type pendingRPC struct {
	peer peer.ID
	rpc  *RPC
}

// RoundRobinMessageIDScheduler schedules outgoing RPCs in round-robin order of message IDs:
// each pass visits the messages in the order they were first added and yields one pending RPC
// of each. The pass order used to be Go's map order, which re-randomises per pass, so the order
// each peer received a batch in differed between two runs of one seed; with the per-message
// peer order also pinned (rpcs, under a seeded source), the publisher's whole send order is now
// a function of the seed (Q116, the repeat spread).
type RoundRobinMessageIDScheduler struct {
	rpcs  map[string][]pendingRPC
	order []string
}

func (s *RoundRobinMessageIDScheduler) AddRPC(peer peer.ID, msgID string, rpc *RPC) {
	if s.rpcs == nil {
		s.rpcs = make(map[string][]pendingRPC)
	}
	if _, seen := s.rpcs[msgID]; !seen {
		s.order = append(s.order, msgID)
	}
	s.rpcs[msgID] = append(s.rpcs[msgID], pendingRPC{peer: peer, rpc: rpc})
}

func (s *RoundRobinMessageIDScheduler) All() iter.Seq2[peer.ID, *RPC] {
	return func(yield func(peer.ID, *RPC) bool) {
		for len(s.rpcs) > 0 {
			for _, msgID := range s.order {
				rpcs, ok := s.rpcs[msgID]
				if !ok {
					continue
				}
				if len(rpcs) == 0 {
					delete(s.rpcs, msgID)
					continue
				}
				if !yield(rpcs[0].peer, rpcs[0].rpc) {
					return
				}

				s.rpcs[msgID] = rpcs[1:]
			}
		}
	}
}
