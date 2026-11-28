package segmentgossip

import (
	"math"
	"sync/atomic"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
)

// The payload path's values from seacast-lite.md §8's defaults, and its two uplink thresholds.
const (
	iwantWindow      = 200 * time.Millisecond // IWANT_WINDOW: one ask in flight per id, then move on
	promiseWait      = 400 * time.Millisecond // PROMISE: an unanswered ask past this parks the peer
	parkTTL          = 30 * time.Second       // PARK_TTL, per group
	parkStrikes      = 1                      // one broken promise parks a peer, for its group only
	guardGrace       = 200 * time.Millisecond // GUARD_GRACE
	paceTicks        = 16                     // the pace tick: a heartbeat over 16
	rttRef           = 112 * time.Millisecond // RTT_REF
	queueTarget      = 20 * time.Millisecond  // TARGET
	chunkBytes       = 12000                  // CHUNK_BYTES, ≈ 12 KB: a coded chunk at 1 MiB
	windowTimeout    = 500 * time.Millisecond // WINDOW_TIMEOUT: a lost acknowledgement frees its slot
	windowGain       = 0.05                   // the window's gain, inert while min = max
	stripeUplinkMbps = 1000                   // STRIPE_UPLINK: a source this fast pushes to every topic peer
	askUplinkMbps    = 50                     // ASK_UPLINK (seacast.md §4.13): below it the ask window stretches
)

// Window is seacast-lite.md §7.1's W for an uplink of mbps: what the uplink sends in RTT_REF + TARGET,
// in chunks of CHUNK_BYTES, one window per node: 69 at 50 Mbps.
func Window(mbps int) int {
	bytesPerSecond := float64(mbps) * 1e6 / 8
	return int(math.Ceil(bytesPerSecond * (rttRef + queueTarget).Seconds() / chunkBytes))
}

// PathMetrics is what the router's options count, for the node's metrics.
type PathMetrics struct {
	Seacast pubsub.SeacastCounters
	// ParkBreaks and Parks are the commitment park's broken promises and parked peers; OfferRetries
	// the offer table's retries; GuardDemotions the peers the push guard demoted.
	ParkBreaks, Parks, OfferRetries, GuardDemotions atomic.Int64
	// V05 is what the router's v0.5 rules count (PromisesAtWrite: the promises a written ask opened).
	V05 pubsub.SpecV05Counters
	// Payload is what the payload type's decodes count (PayloadType.Counts), nil where the node runs
	// no payload type.
	Payload *DecodeCounts
}

// payloadRoles is §6.1's JOIN for every peer: interest ALL and duty FORWARD on the payload, no node id.
type payloadRoles struct{}

func (payloadRoles) JoinOf(_ peer.ID, t pubsub.ObjectType) (pubsub.Join, bool) {
	if t.Name() != PayloadTypeName && t.Name() != AggregateTypeName {
		return pubsub.Join{}, false
	}
	return pubsub.Join{Interest: pubsub.InterestAll, Duty: pubsub.DutyForward}, true
}

func (payloadRoles) NodeIDOf(peer.ID) (pubsub.NodeID, bool) { return pubsub.NodeID{}, false }

// RouterOptions are the router's options for SEACAST's payload path, built from a setting that
// CheckSetting passed, in the order the options require. heartbeat is the router's heartbeat.
//
// Phase forwarding is installed with a matcher that accepts no topic: SEACAST needs it first, and on
// the path's own topics it puts its sub-settings (the fewest-held selection, its guard and the
// tolerant draw, the source's connections) in force, while every other topic keeps stock
// forwarding. The HAVE bitmaps read a segment's group and index from its structured id. No request
// gate is installed: the group machine stops a completed group's asks itself.
//
// Some of these act on every topic, not only the payload's: the window holds every data frame
// until a slot frees, and under SEACAST the IHAVE limit counts offer-bearing RPCs and holds asks
// back rather than ignoring the IHAVE.
func RouterOptions(s Setting, t pubsub.ObjectType, heartbeat time.Duration, m *PathMetrics, additional ...pubsub.ObjectType) []pubsub.Option {
	index := func(mid string) (string, uint32, bool) {
		c, ok := ParseMessageID(mid)
		if !ok {
			return "", 0, false
		}
		return string(c.Group[:]), c.Index, true
	}
	scope := func(mid string) (string, bool) {
		root, _, ok := index(mid)
		return root, ok
	}
	w := float64(Window(s.UplinkMbps))
	opts := []pubsub.Option{
		pubsub.WithPhaseForwarding(func(string) bool { return false }, s.Params.RNode),
		// A relay's sender stays a candidate of the forward and can take a place of the push budget;
		// seacast-lite.md §5.2's forward leaves it out.
		pubsub.WithSeacastSenderInForward(),
		pubsub.WithHaveBitmaps(index),
		pubsub.WithHaveAnnounceBitmaps(),
		pubsub.WithHaveDispatch(s.Cancel, false),
		pubsub.WithHaveRarestDispatch(),
		// seacast.md §4.12's choose: the writer sends this node's own chunks first, over the whole
		// queue ([X3]).
		pubsub.WithOwnChunksFirst(),
		pubsub.WithIWantDiscipline(iwantWindow),
		// seacast.md §4.13: IWANT_WINDOW times max(1, ASK_UPLINK / UPLINK), so a node below 50 Mbps
		// waits longer for an answer before it asks elsewhere.
		pubsub.WithAskUplink(askUplinkMbps*1_000_000, int64(s.UplinkMbps)*1_000_000),
		pubsub.WithIHaveCommitmentPark(parkStrikes, promiseWait, parkTTL, &m.ParkBreaks, &m.Parks),
		pubsub.WithIHaveParkPerGroup(scope),
		pubsub.WithOfferTable(&m.OfferRetries),
		// The v0.5 rules the router's own defaults miss, after the park, the park per group and the
		// offer table, which they need. A chunk's pull keeps its promise from the write, asks parked
		// announcers once more (the last resort) and closes promises with the connection without
		// these; the switches are for the router's other asks:
		//   - §6.1: "A promise starts at the write, not at the decision to ask, so that no peer is
		//     parked for an ask it never received." Started at the decision, an ask waiting behind
		//     the window broke its promise and parked an honest peer.
		//   - §5.2 and §4.1 (OfferSkipped): a push a full queue refuses is offered instead, and an answer it
		//     refuses is owed. Dropped, a refused answer broke the asker's promise against this node.
		pubsub.WithSpecV05(pubsub.SpecV05{PromiseAtWrite: true, OfferSkipped: true}, &m.V05),
		pubsub.WithPacedGossip(heartbeat/paceTicks, 0),
		// A paced job whose every id the peer holds by its tick is passed over within that tick, as
		// measured; seacast-lite.md §6.4's on_pace_tick spends the tick on it instead.
		pubsub.WithPacedGossipSkipHeld(),
		pubsub.WithPhaseSelectFewest(),
		pubsub.WithPhaseSelectFewestGuard(guardGrace, func(peer.ID, string) { m.GuardDemotions.Add(1) }),
		pubsub.WithPhaseSelectFewestTolerant(pubsub.PhaseSelectLowerHalf, 0),
	}
	if s.UplinkMbps >= stripeUplinkMbps {
		opts = append(opts, pubsub.WithPhaseSourceConnections())
	}
	opts = append(opts,
		pubsub.WithHoseAck(),
		pubsub.WithHoseWindow(pubsub.HoseConfig{
			Target:     queueTarget,
			Kp:         windowGain,
			Init:       w,
			Min:        w,
			Max:        w,
			Timeout:    windowTimeout,
			FrameSlots: true,
		}),
		pubsub.WithSeacast(append([]pubsub.ObjectType{t}, additional...), &m.Seacast),
		pubsub.WithSeacastRoles(payloadRoles{}),
		pubsub.WithSeacastMachine(),
		pubsub.WithSeacastLimits(s.Limits),
		// seacast.md §4.1's join_push: a peer that joins the payload topic's mesh is offered what the
		// node holds of the live groups.
		pubsub.WithSeacastJoinPush(),
	)
	return opts
}
