package gossipsim

import (
	"fmt"
	"time"

	"github.com/OffchainLabs/prysm/v7/beacon-chain/p2p"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pubsub_pb "github.com/libp2p/go-libp2p-pubsub/pb"
	simlibp2p "github.com/libp2p/go-libp2p/x/simlibp2p"
)

const (
	// DefaultLatency and DefaultRate are the standing operating point for every experiment:
	// **25 ms one-way (50 ms RTT) on a 50 Mbps symmetric per-node access link.**
	//
	// Chosen to be plausible for a globally distributed network rather than convenient. Earlier
	// results were taken at 5 ms / 20 Mbps, which flattered segmentation twice over -- a 10 ms RTT
	// understates latency, and 20 Mbps saturated the publisher so badly that serving D=8 copies of
	// a 746 KB envelope took 2.39 s all by itself. Any figure quoted from before this change is at
	// the old point and says so.
	DefaultLatency = 25 * time.Millisecond
	DefaultRate    = 50 * simlibp2p.OneMbps
	// MeshFormation is how long to let gossipsub graft before publishing. A publish before the
	// mesh exists reaches nobody at all.
	//
	// Prysm's heartbeat interval is 700 ms after a 100 ms initial delay, so heartbeats land at
	// 100 ms, 800 ms, 1500 ms. Subscriptions are announced *after* connections are dialled, so
	// the 100 ms heartbeat fires before there is anything to graft and the first useful one is at
	// 800 ms. This has to clear several of them.
	//
	// This applies **in a synctest bubble too**, which an earlier revision got wrong: it used
	// 500 ms in the bubble on the reasoning that time.Sleep jumps the virtual clock and
	// synctest.Wait drains the consequences. Jumping the clock 500 ms does not make an 800 ms
	// timer fire. The symptom was a degree-8 mesh of 20 nodes where a publish reached zero peers
	// while 10 and 30 nodes happened to work, because whether an ungrafted publish lands at all
	// depends on fanout state and therefore on the graph.
	//
	// Virtual time is nearly free, so both modes use the same generous value.
	MeshFormation = 2500 * time.Millisecond
	// SubscriptionBuffer must exceed the largest number of messages any experiment publishes in
	// one batch, with room for a whole-message arm alongside it.
	SubscriptionBuffer = 1024
	// NetworkOpTimeout bounds any wait for a network event. A test must never block forever on
	// a message that is not coming: the failure has to name itself.
	NetworkOpTimeout = 60 * time.Second
	// ProductionQueueSize matches beacon-chain/p2p's defaultPubsubQueueSize.
	ProductionQueueSize = 600
)

// MeshParams returns the gossipsub parameters every cell runs on: Prysm's production values,
// with the mesh degree D overridden by SEGMENT_MESH_D.
//
// D matters most where a node holds many meshes at once. Variant C at 64 topics wants
// 64*D mesh memberships -- 512 at the production D=8 -- multiplexed over a connectivity degree of
// only ~70, so every connection carries ~7 mesh roles and each eager push is amplified
// accordingly. Halving D halves that pressure, which is why low D is worth testing precisely at
// high topic counts.
//
// Watermarks scale with D at production's ratios (Dlo = 3D/4, Dhi = 3D/2) and Dout/Dscore are
// clamped to gossipsub's invariants (Dout < Dlo, Dout <= D/2, Dscore <= Dhi). **Dlazy is left at
// its default on purpose**: it sets the IHAVE fan-out, so holding it fixed isolates a change in
// push width from a change in announce width -- and the announce plane is what the pull arms feed
// on.
// idontWantThreshold moves the message size above which IDONTWANT is emitted; zero keeps the
// library's value.
//
// The constant's documented safe minimum is Dhi * message-id size, so that an attacker cannot send
// small messages and draw larger IDONTWANT replies. At Dhi=12 and 20-byte ids that is 240 bytes,
// comfortably under the 1 KiB default; at 101-byte structured ids it is 1,212 bytes, above it. So
// the threshold does have to rise with the id width -- for the amplification bound, not for honest
// behaviour: 32 KiB segments are above every value at or below the segment size, and 1 KiB and 8 KiB
// measure identically. Raising it past the segment size disables IDONTWANT entirely, which this knob
// also makes measurable (one cell: dups x1.8, bytes +48%).
//
// A negative value for either parameter means "leave Prysm's value alone", so
// MeshParams(-1, -1) is the production configuration exactly. Zero is a meaningful setting for
// the threshold -- it disables the size floor on IDONTWANT entirely -- so it cannot double as
// "unset".
//
// lint:nopanic -- the panic below guards an invariant on caller input (only d=1 can violate it after
// the clamps). Returning an error instead would ripple into package-level initialisers in the
// harnesses, and silently clamping would change an experiment's meaning without telling anyone.
func MeshParams(d, idontWantThreshold int) pubsub.GossipSubParams {
	gsp := p2p.GossipSubParams()
	if idontWantThreshold >= 0 {
		gsp.IDontWantMessageThreshold = idontWantThreshold
	}
	if d < 1 {
		return gsp
	}
	gsp.D = d
	gsp.Dlo = max(1, d*3/4)
	gsp.Dhi = max(d+1, d*3/2)
	// Keep production's Dout wherever the invariants allow, so d=8 reproduces the default
	// configuration exactly and can serve as the sweep's baseline. gossipsub requires
	// Dout < Dlo AND Dout < D/2 -- strictly less, though its error message reads "must not
	// exceed D/2". Violating it surfaces as a deadlock panic from the NewGossipSub error inside
	// the synctest bubble rather than a readable failure, so clamp explicitly.
	gsp.Dout = max(0, min(gsp.Dout, min(gsp.Dlo-1, d/2-1)))
	gsp.Dscore = min(gsp.Dscore, gsp.Dhi)
	if !(gsp.Dlo <= gsp.D && gsp.D <= gsp.Dhi) || !(gsp.Dout < gsp.Dlo && gsp.Dout < gsp.D/2) {
		panic(fmt.Sprintf("mesh degree %d yields invalid gossipsub params D=%d Dlo=%d Dhi=%d Dout=%d",
			d, gsp.D, gsp.Dlo, gsp.Dhi, gsp.Dout))
	}
	return gsp
}

// ProductionPubsubOpts mirrors the pubsub configuration a real Prysm node runs.
//
// Library defaults are not merely less faithful, they silently break experiments. The default
// max message size is 1 MiB (pubsub.DefaultMaxMessageSize), and a 1 MiB high-entropy payload
// encodes to slightly MORE than that -- see TestPayloadEntropy -- so a whole-message baseline
// arm would be dropped rather than measured, with no error at the publisher. The default
// per-peer outbound queue is 32, exactly K for a 1 MiB payload at 32 KiB segments, leaving no
// slack for the control messages a batch publish needs alongside the data.
//
// Prysm's own values: max message size max(MaxCompressedLen(MaxPayloadSize)+1024, 1 MiB), queue
// 600, and the overlay parameters from p2p.GossipSubParams (D=8, Dlo=6, HistoryLength=6,
// 700 ms heartbeat) rather than the library's D=6, Dlo=5, HistoryLength=5, 1 s.
//
// MsgID is Prysm's, because message identity decides what counts as a duplicate and therefore
// what IHAVE and IDONTWANT suppress.
// Overrides carries the few production values an experiment sweeps.
//
// MeshDegree and PeerOutboundQueueSize take "production" from a value below 1, since neither has
// a meaningful zero. IDontWantThreshold does have a meaningful zero, so it takes production from
// a negative -- and therefore has to be built with NewOverrides rather than a bare literal.
type Overrides struct {
	// MeshDegree replaces D, deriving Dlo, Dhi, Dout and Dscore from it.
	MeshDegree int
	// IDontWantThreshold is the message size above which IDONTWANT is emitted. Negative means
	// production.
	IDontWantThreshold int
	// PeerOutboundQueueSize lowers the outbound queue depth so slowed writers can drive
	// genuine admission overflow (doDropRPC), not just backpressure latency: the 600
	// production default is hard to overflow via a slow drain alone.
	PeerOutboundQueueSize int
}

// NewOverrides returns an Overrides that changes nothing. Use it rather than a bare literal, so
// IDontWantThreshold starts at its "production" sentinel rather than at a meaningful zero.
func NewOverrides() Overrides {
	return Overrides{MeshDegree: -1, IDontWantThreshold: -1}
}

// ProductionPubsubOpts is ProductionPubsubOptsWith at production values.
func ProductionPubsubOpts() []pubsub.Option {
	return ProductionPubsubOptsWith(NewOverrides())
}

// ProductionPubsubOptsWith is ProductionPubsubOpts with the named values replaced.
func ProductionPubsubOptsWith(o Overrides) []pubsub.Option {
	var genesisValidatorsRoot [32]byte
	queueSize := ProductionQueueSize
	if o.PeerOutboundQueueSize > 0 {
		queueSize = o.PeerOutboundQueueSize
	}
	return []pubsub.Option{
		pubsub.WithMaxMessageSize(int(p2p.MaxMessageSize())), // lint:ignore uintcast -- bounded config value
		pubsub.WithPeerOutboundQueueSize(queueSize),
		pubsub.WithValidateQueueSize(ProductionQueueSize),
		pubsub.WithGossipSubParams(MeshParams(o.MeshDegree, o.IDontWantThreshold)),
		pubsub.WithMessageIdFn(MsgIDFn(genesisValidatorsRoot[:])),
	}
}

// MsgIDFn is Prysm's gossip message-id function, which decides what counts as a duplicate and
// therefore what IHAVE and IDONTWANT suppress.
func MsgIDFn(genesisValidatorsRoot []byte) func(*pubsub_pb.Message) string {
	return func(pmsg *pubsub_pb.Message) string {
		return p2p.MsgID(genesisValidatorsRoot, pmsg)
	}
}
