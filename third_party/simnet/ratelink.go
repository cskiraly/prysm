package simnet

import (
	"time"

	"golang.org/x/time/rate"
)

type RateLink struct {
	*rate.Limiter
	BitsPerSecond int
	Receiver      PacketReceiver
}

// Creates a new RateLimiter with the following parameters:
// bandwidth (in bits/sec).
// burstSize is in Bytes, and is raised to burstWindow's worth of line rate when that is larger.
//
// burstWindow is how much line time the token bucket may accumulate while the link driver
// sleeps, and the right value depends on the clock the simulation runs on. The driver sends one
// timer wake-up at a time. On the real clock a wake-up overshoots its deadline by a few hundred
// microseconds; with a burst of one MTU every microsecond of overshoot is lost capacity, and a
// 50 Mbps link delivered ~21 Mbps to a 1400 B flow. A burst of a few milliseconds' worth lets
// the wake-up send what accumulated while it slept (measured in
// testing/gossipsim/calibration_test.go). Under a virtual clock (testing/synctest) wake-ups fire
// at exact instants, so one-MTU pacing already delivers the full rate -- and it is the more
// faithful model, since a window's worth of accumulated tokens is released in zero virtual time,
// which no physical link does. Zero keeps the burst at one MTU.
func newRateLimiter(bandwidth int, burstSize int, burstWindow time.Duration) *rate.Limiter {
	// Convert bandwidth from bits/sec to bytes/sec
	bytesPerSecond := rate.Limit(float64(bandwidth) / 8.0)
	if windowed := int(float64(bytesPerSecond) * burstWindow.Seconds()); windowed > burstSize {
		burstSize = windowed
	}
	return rate.NewLimiter(bytesPerSecond, burstSize)
}

func NewRateLink(bandwidth int, burstSize int, burstWindow time.Duration, receiver PacketReceiver) *RateLink {
	return &RateLink{
		Limiter:  newRateLimiter(bandwidth, burstSize, burstWindow),
		Receiver: receiver,
	}
}

func (l *RateLink) Reserve(now time.Time, packetSize int) time.Duration {
	r := l.Limiter.ReserveN(now, packetSize)
	return r.DelayFrom(now)
}

func (l *RateLink) RecvPacket(p *Packet) {
	l.Receiver.RecvPacket(p)
}
