package pubsub

import (
	"slices"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
	"google.golang.org/protobuf/proto"
)

// DeferredChunk references the contents of the object store until a group's writes resume.
type DeferredChunk struct {
	Root      string
	Index     uint32
	Peer      peer.ID
	Solicited bool
}

// AttachedObject controls link-local metadata and outbound eligibility. PrepareWrite and
// AllowOffer run on peer writer goroutines. The other methods run on the router goroutine.
type AttachedObject interface {
	ObjectType
	WholeTopic(chunkTopic string) string
	// AdmitFrame returns the key of a newly opened group so the router can trim early offers.
	AdmitFrame(*Message) (ValidationResult, string)
	PrepareWrite(DeferredChunk, []byte) ([]byte, bool)
	AllowOffer(root string) bool
	NoteHolding(root string, p peer.ID)
	ForgetPeer(peer.ID)
	Settle(root string, passed bool) (whole *Message, writes []DeferredChunk)
}

// prepareAttachedData runs after cancellation, before a chunk counts toward the send window.
func (p *PubSub) prepareAttachedData(to peer.ID, rpc *RPC) *RPC {
	if len(p.attachedTopics) == 0 {
		return rpc
	}
	var out *RPC
	for i, m := range rpc.Publish {
		a := p.attachedTopics[m.GetTopic()]
		if a == nil {
			if out != nil {
				out.Publish = append(out.Publish, m)
			}
			continue
		}
		if out == nil {
			out = shallowRPCCopy(rpc)
			out.Publish = append([]*pb.Message(nil), rpc.Publish[:i]...)
		}
		mid := p.idGen.RawID(m)
		root, index, ok := p.rt.(*GossipSubRouter).have.index(mid)
		if !ok {
			continue
		}
		data, send := a.PrepareWrite(DeferredChunk{root, index, to, rpc.solicited}, m.Data)
		if send {
			if len(data) != len(m.Data) || (len(data) > 0 && &data[0] != &m.Data[0]) {
				m = proto.Clone(m).(*pb.Message)
				m.Data = data
			}
			out.Publish = append(out.Publish, m)
		}
	}
	if out == nil {
		return rpc
	}
	if len(out.Publish) == 0 && rpcOtherwiseEmpty(out) {
		return nil
	}
	return out
}

// filterAttachedOffers also checks offers piggybacked on the last pop from the queue.
func (p *PubSub) filterAttachedOffers(rpc *RPC) *RPC {
	if len(p.attachedTopics) == 0 {
		return rpc
	}
	var out *RPC
	for i, h := range rpc.GetControl().GetIhave() {
		a := p.attachedTopics[h.GetTopicID()]
		if a == nil {
			continue
		}
		var ids []string
		for j, id := range h.MessageIDs {
			root, ok := seacastHaveRoot(id)
			if !ok {
				root, _, ok = p.rt.(*GossipSubRouter).have.index(id)
			}
			if ok && !a.AllowOffer(root) {
				if ids == nil {
					ids = append(make([]string, 0, len(h.MessageIDs)), h.MessageIDs[:j]...)
				}
			} else if ids != nil {
				ids = append(ids, id)
			}
		}
		if ids == nil {
			continue
		}
		if out == nil {
			out = shallowRPCCopy(rpc)
		}
		if out.Control == rpc.Control {
			out.Control = proto.Clone(rpc.Control).(*pb.ControlMessage)
		}
		out.Control.Ihave[i].MessageIDs = ids
	}
	if out != nil && out.Control != nil && out.Control != rpc.Control {
		out.Control.Ihave = slices.DeleteFunc(out.Control.Ihave, func(h *pb.ControlIHave) bool { return len(h.MessageIDs) == 0 })
		if proto.Size(out.Control) == 0 {
			out.Control = nil
		}
	}
	if out != nil {
		out.asks, out.popped = rpc.asks, rpc.popped
		return out
	}
	return rpc
}

func (gs *GossipSubRouter) settleAttached(t AttachedObject, root string, passed bool) {
	whole, writes := t.Settle(root, passed)
	for _, w := range writes {
		if msg, ok := t.Store().Get(w.Root, w.Index); ok {
			save := gs.sendingSolicited
			gs.sendingSolicited = w.Solicited
			gs.sendRPC(w.Peer, rpcWithMessages(msg.Message), false)
			gs.sendingSolicited = save
		}
	}
	if whole != nil && whole.ReceivedFrom != gs.p.host.ID() && gs.p.subscribedToMsg(whole.Message) && gs.p.shouldPush(whole) {
		// Retain the remote sender for the ordinary whole-topic validation and pending queue.
		whole.Local = true
		gs.p.pushMsg(whole)
	}
}
