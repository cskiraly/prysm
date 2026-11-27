package pubsub

import (
	"time"

	core "github.com/cskiraly/seacast-go"
)

// seacastEnv is the core group machine's Env (seacast/machine.go) on the gossipsub binding, for one
// call: T is the group's object type, nil where the call needs none (arm) or the binding knows
// none (a later duty of a root no type names). The machine's groups are named by seacastGID; every
// method reads and acts on the router's state as the fork's code did before the move. Router
// goroutine.
type seacastEnv struct {
	gs *GossipSubRouter
	T  ObjectType
}

func (gs *GossipSubRouter) seacastEnv(T ObjectType) seacastEnv { return seacastEnv{gs: gs, T: T} }

func (e seacastEnv) Now() time.Time { return time.Now() }

// After brings the later duty back through eval, on the router's goroutine.
func (e seacastEnv) After(d time.Duration, t core.Timer) {
	gs := e.gs
	time.AfterFunc(d, func() {
		select {
		case gs.p.eval <- func() { gs.seacastReconsider(seacastGIDRoot(t.Group), t.At) }:
		case <-gs.p.ctx.Done():
		}
	})
}

// Event keeps the path's counters and event log.
func (e seacastEnv) Event(g core.GroupID, ev core.Event) {
	sc := e.gs.seacast
	switch ev {
	case core.EvCompleted:
		sc.ctr.GroupsCompleted.Add(1)
	case core.EvArmed:
		sc.ctr.DutiesArmed.Add(1)
	case core.EvDutyCancelled:
		sc.ctr.DutiesCancelled.Add(1)
	case core.EvLateDecode:
		root := seacastGIDRoot(g)
		if e.gs.seacastKnownInScope(e.T, root).Count() >= e.T.IndexCount(root) {
			sc.ord(ordDecodeWhole) // a late decode of a group whole in the subnet's view (seacast_order.go)
		}
		sc.ctr.LateDecodes.Add(1)
		sc.event(SeacastEvLateDecode, root)
	case core.EvDecodeStarted:
		sc.ctr.DecodesStarted.Add(1)
	case core.EvDecodeStale:
		sc.ctr.DecodesStale.Add(1)
	case core.EvDecodeFailed:
		sc.ctr.DecodesFailed.Add(1)
		sc.event(SeacastEvDecodeFailed, seacastGIDRoot(g))
	case core.EvDecodeDone:
		sc.ctr.DecodesDone.Add(1)
		sc.event(SeacastEvDecodeDone, seacastGIDRoot(g))
	}
}

func (e seacastEnv) Known(core.GroupID) bool { return e.T != nil }

// Holdings is the node's holdings, which the HAVE layer keeps by group (havebitmap.go).
func (e seacastEnv) Holdings() core.Holdings { return e.gs.have.mine }

// Interest is the node's own interest in g.
func (e seacastEnv) Interest(g core.GroupID) *core.Bits {
	return e.gs.seacastInterestOfPeer(e.gs.p.host.ID(), seacastGIDRoot(g))
}

func (e seacastEnv) IndexCount(g core.GroupID) int { return e.T.IndexCount(seacastGIDRoot(g)) }

func (e seacastEnv) Threshold(g core.GroupID) int { return e.T.Threshold(seacastGIDRoot(g)) }

func (e seacastEnv) DecodeAtK(g core.GroupID) bool {
	p := e.T.Params()
	return p.DecodeAtK != nil && p.DecodeAtK(seacastGIDRoot(g))
}

func (e seacastEnv) DecodeLater(g core.GroupID) (time.Duration, bool) {
	p := e.T.Params()
	if p.DecodeLater == nil {
		return 0, false
	}
	return p.DecodeLater(seacastGIDRoot(g))
}

func (e seacastEnv) KnownAt(g core.GroupID) (time.Time, bool) {
	at, ok := e.gs.seacast.knownAt[seacastGIDRoot(g)]
	return at, ok
}

func (e seacastEnv) Expired(g core.GroupID) bool {
	expired, _ := e.gs.seacastRootState(seacastGIDRoot(g))
	return expired
}

func (e seacastEnv) DropQueuedAsks(g core.GroupID) { e.gs.seacastDropQueuedAsks(seacastGIDRoot(g)) }
func (e seacastEnv) ClosePromises(g core.GroupID)  { e.gs.seacastClosePromises(seacastGIDRoot(g)) }
func (e seacastEnv) SendHolding(g core.GroupID)    { e.gs.seacastSendHolding(e.T, seacastGIDRoot(g)) }
func (e seacastEnv) DropGroup(g core.GroupID)      { e.gs.seacastDropGroup(seacastGIDRoot(g)) }

func (e seacastEnv) StartDecode(g core.GroupID, op uint64) (int, bool) {
	return e.gs.seacastDecodeStart(e.T, seacastGIDRoot(g), op)
}

// JudgeProbe is the claimant's verdict, once (R5, seacast_probe.go).
func (e seacastEnv) JudgeProbe(g core.GroupID) {
	root := seacastGIDRoot(g)
	e.gs.seacastProbeVerdict(root, e.gs.seacast.group(root))
}

// SettleByClaims is the views and the claims at a later duty (seacast_claims.go).
func (e seacastEnv) SettleByClaims(g core.GroupID) bool {
	root := seacastGIDRoot(g)
	return e.gs.seacastReconsiderVerdict(e.T, root, e.gs.seacast.group(root))
}
