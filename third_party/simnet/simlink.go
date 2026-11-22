package simnet

import (
	"os"
	"strconv"
	"sync"
	"time"
)

const Mibps = 1_000_000

const DefaultFlowBucketCount = 128

// LinkSettings defines the network characteristics for a simulated link direction
type LinkSettings struct {
	// BitsPerSecond specifies the bandwidth limit in bits per second
	BitsPerSecond int

	// MTU (Maximum Transmission Unit) specifies the maximum packet size in bytes
	MTU int

	// BurstWindow is how much line time the link's token bucket may accumulate while the
	// driver sleeps. Zero keeps the burst at one MTU, which is exact under a virtual clock
	// (testing/synctest) and the faithful model there. On the real clock every timer wake-up
	// overshoots, so one-MTU pacing loses capacity to the overshoot -- a few milliseconds
	// recovers the line rate. See newRateLimiter.
	BurstWindow time.Duration

	// FlowBucketCount sets the number of flow buckets for FQ-CoDel. If zero
	// defaults to DefaultFlowBucketCount
	FlowBucketCount int
}

// Simlink simulates a bidirectional network link with variable latency,
// bandwidth limiting, and CoDel-based bufferbloat mitigation
type Simlink struct {
	up   *linkDriver
	down *linkDriver
}

func NewSimlink(
	closeSignal chan struct{},
	linkSettings NodeBiDiLinkSettings,
	upPacketReceiver PacketReceiver,
	downPacketReceiver PacketReceiver,
) *Simlink {
	const defaultMTU = 1500
	// CoDel's target and interval, Linux's defaults, overridable through SIMNET_CODEL_TARGET_MS
	// and SIMNET_CODEL_INTERVAL_MS (local change; upstream has them as constants).
	//
	// They are a knob because active queue management is one of the few things that genuinely
	// differs between this simulator and Shadow, which has none: setting the target far above
	// any sojourn a run reaches leaves CoDel installed but never dropping, which is Shadow's
	// behaviour, and makes the difference measurable rather than argued. Unset, nothing changes.
	target := envDuration("SIMNET_CODEL_TARGET_MS", 5*time.Millisecond)
	interval := envDuration("SIMNET_CODEL_INTERVAL_MS", 100*time.Millisecond)

	if linkSettings.Uplink.MTU == 0 {
		linkSettings.Uplink.MTU = defaultMTU
	}
	if linkSettings.Downlink.MTU == 0 {
		linkSettings.Downlink.MTU = defaultMTU
	}

	if linkSettings.Uplink.FlowBucketCount == 0 {
		linkSettings.Uplink.FlowBucketCount = DefaultFlowBucketCount
	}
	if linkSettings.Downlink.FlowBucketCount == 0 {
		linkSettings.Downlink.FlowBucketCount = DefaultFlowBucketCount
	}

	return &Simlink{
		up: newLinkDriver(
			target, interval,
			linkSettings.Uplink.MTU,
			linkSettings.Uplink.FlowBucketCount,
			linkSettings.Uplink.MTU, linkSettings.Uplink.BitsPerSecond,
			linkSettings.Uplink.BurstWindow,
			upPacketReceiver,
			closeSignal,
		),
		down: newLinkDriver(
			target, interval,
			linkSettings.Downlink.MTU,
			linkSettings.Downlink.FlowBucketCount,
			linkSettings.Downlink.MTU, linkSettings.Downlink.BitsPerSecond,
			linkSettings.Downlink.BurstWindow,
			downPacketReceiver,
			closeSignal,
		),
	}
}

func (l *Simlink) Start(wg *sync.WaitGroup) {
	l.up.Start(wg)
	l.down.Start(wg)
}

// LinkStats reports each direction's queue separately, since an asymmetric link congests in
// one of them long before the other.
type LinkStats struct {
	Uplink   QueueStats
	Downlink QueueStats
}

func (l *Simlink) Stats() LinkStats {
	return LinkStats{Uplink: l.up.q.Stats(), Downlink: l.down.q.Stats()}
}

type linkDriver struct {
	newPacket     chan *Packet
	q             fqCoDel
	rateLink      *RateLink
	closeSignal   chan struct{}
	pendingPacket *Packet
}

func newLinkDriver(
	target, interval time.Duration,
	quantum int,
	flowCount int,
	mtu int, bandwidth int,
	burstWindow time.Duration,
	receiver PacketReceiver,
	closeSignal chan struct{}) *linkDriver {
	return &linkDriver{
		newPacket:   make(chan *Packet, 32),
		q:           newFqCoDel(target, interval, quantum, flowCount),
		closeSignal: closeSignal,
		rateLink:    NewRateLink(bandwidth, mtu, burstWindow, receiver),
	}
}

func (d *linkDriver) RecvPacket(p *Packet) {
	select {
	case d.newPacket <- p:
	case <-d.closeSignal:
	}
}

// processQueue sends the pending packet and dequeues more.
// Returns the delay before next send, or 0 if nothing pending.
func (d *linkDriver) processQueue() time.Duration {
	for {
		if d.pendingPacket != nil {
			d.rateLink.RecvPacket(d.pendingPacket)
			d.pendingPacket = nil
		}

		p, ok := d.q.Dequeue()
		if !ok {
			return 0 // queue empty
		}
		d.pendingPacket = p
		now := time.Now()
		if d.rateLink.AllowN(now, len(p.buf)) {
			continue // can send immediately
		}
		return d.rateLink.Reserve(now, len(p.buf))
	}
}

func (d *linkDriver) Start(wg *sync.WaitGroup) {
	wgGo(wg, func() {
		deqTimer := time.NewTimer(0)
		deqTimer.Stop()

		for {
			select {
			case <-d.closeSignal:
				return
			case packet := <-d.newPacket:
				d.q.Enqueue(packet)
				if d.pendingPacket == nil {
					if delay := d.processQueue(); delay > 0 {
						deqTimer.Reset(delay)
					}
				}
			case <-deqTimer.C:
				if delay := d.processQueue(); delay > 0 {
					deqTimer.Reset(delay)
				}
			}
		}
	})
}

// envDuration reads a millisecond count from the environment, falling back when unset or unusable.
func envDuration(name string, fallback time.Duration) time.Duration {
	v, err := strconv.ParseFloat(os.Getenv(name), 64)
	if err != nil || v <= 0 {
		return fallback
	}
	return time.Duration(v * float64(time.Millisecond))
}
