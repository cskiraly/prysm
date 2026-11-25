package pubsub

import (
	"fmt"
	"sync/atomic"
	"time"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
)

// --- Adversaries the tests use ---
//
// The F1 withholder and the control mute. Neither is a protocol feature: the v0.5 rules' tests
// use them to make a slow, silent or muted peer.

// withholdIWantServe applies the injection to a built reply: it is swallowed (silent) or sent
// whole after the delay, on pubsub's own goroutine like every router action. Nothing is served
// at once, so it returns nil.
func (gs *GossipSubRouter) withholdIWantServe(p peer.ID, msgs []*pb.Message) []*pb.Message {
	held := msgs
	if len(held) == 0 {
		return nil
	}
	if gs.withheldServes != nil {
		gs.withheldServes.Add(int64(len(held)))
	}
	if gs.withholdDelay > 0 {
		asked := gs.p.peers[p] // the connection the ask came on
		time.AfterFunc(gs.withholdDelay, func() {
			select {
			case gs.p.eval <- func() {
				cur, ok := gs.p.peers[p]
				if !ok {
					return
				}
				late := held
				if gs.v05 != nil {
					// Under v0.5 the late serve goes on the connection it was asked on, and only for
					// groups still alive; off v0.5 it leaves as before.
					if cur != asked {
						return
					}
					late = nil
					for _, m := range held {
						if !gs.expiredID(gs.p.idGen.RawID(m)) {
							late = append(late, m)
						}
					}
					if len(late) == 0 {
						return
					}
				}
				// The late serve is an answer: under v0.5's A′ a full queue owes it as one, and the
				// writer never projects it away. Off v0.5 it leaves as before.
				gs.sendingSolicited = gs.v05OfferSkipped()
				gs.sendRPC(p, rpcWithControl(late, nil, nil, nil, nil, nil), false)
				gs.sendingSolicited = false
			}:
			case <-gs.p.ctx.Done():
			}
		})
	}
	return nil
}

// WithControlMute makes this node send no IDONTWANT to any peer: no HAVE bitmap, no hose
// acknowledgement, no claim. It still receives, announces, asks and pushes. Against sel=fewest
// the mute peer looks furthest behind to every pusher. The counter, when non-nil, records the
// ids suppressed, so the exposure is verified.
func WithControlMute(muted *atomic.Int64) Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok {
			return fmt.Errorf("control mute requires a gossipsub router")
		}
		gs.controlMute = true
		gs.controlMuted = muted
		return nil
	}
}

// applyControlMute returns rpc without its IDONTWANT entries under the mute, nil when nothing
// else remains; rpc itself is never mutated, since one RPC can be yielded to several peers.
func (gs *GossipSubRouter) applyControlMute(rpc *RPC) *RPC {
	if !gs.controlMute || rpc == nil {
		return rpc
	}
	ctl := rpc.GetControl()
	if ctl == nil || len(ctl.Idontwant) == 0 {
		return rpc
	}
	n := 0
	for _, e := range ctl.Idontwant {
		n += len(e.MessageIDs)
	}
	if gs.controlMuted != nil {
		gs.controlMuted.Add(int64(n))
	}
	out := shallowRPCCopy(rpc)
	out.Control = nil
	if len(ctl.Ihave) > 0 || len(ctl.Iwant) > 0 || len(ctl.Graft) > 0 || len(ctl.Prune) > 0 || ctl.Extensions != nil {
		out.Control = &pb.ControlMessage{Ihave: ctl.Ihave, Iwant: ctl.Iwant, Graft: ctl.Graft, Prune: ctl.Prune, Extensions: ctl.Extensions}
	}
	if len(out.Publish) == 0 && out.Control == nil && rpcOtherwiseEmpty(out) {
		return nil
	}
	return out
}
