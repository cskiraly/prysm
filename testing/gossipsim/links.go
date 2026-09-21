package gossipsim

import (
	"net"
	"time"

	simlibp2p "github.com/libp2p/go-libp2p/x/simlibp2p"
	"github.com/marcopolo/simnet"
)

// UniformLinks gives every node the same symmetric access link.
func UniformLinks(n, bitsPerSecond int) []simlibp2p.NodeLinkSettingsAndCount {
	return []simlibp2p.NodeLinkSettingsAndCount{{
		LinkSettings: simnet.NodeBiDiLinkSettings{
			Downlink: simnet.LinkSettings{BitsPerSecond: bitsPerSecond},
			Uplink:   simnet.LinkSettings{BitsPerSecond: bitsPerSecond},
		},
		Count: n,
	}}
}

// AsymmetricLinks constrains the uplink and leaves the downlink generous, so a sender's uplink
// is the scarce resource. Experiments about send ordering or path diversity need this: with
// symmetric links the receiver's own downlink binds first and there is nothing to observe.
func AsymmetricLinks(n, upBitsPerSecond, downBitsPerSecond int) []simlibp2p.NodeLinkSettingsAndCount {
	return []simlibp2p.NodeLinkSettingsAndCount{{
		LinkSettings: simnet.NodeBiDiLinkSettings{
			Downlink: simnet.LinkSettings{BitsPerSecond: downBitsPerSecond},
			Uplink:   simnet.LinkSettings{BitsPerSecond: upBitsPerSecond},
		},
		Count: n,
	}}
}

// RealClockBurstWindow is the token-bucket burst, in line time, for links driven by the real
// clock. On the real clock every timer wake-up overshoots its deadline by a few hundred
// microseconds, and with a one-MTU burst that overshoot is lost capacity: a "50 Mbps" link
// delivered ~21 Mbps (R14, calibration_test.go). Five milliseconds of accumulation recovers the
// line rate and matches CoDel's target, so the burstiness stays under the queueing delay the
// link already tolerates.
//
// Virtual-clock (synctest) links must NOT get this window: their wake-ups fire at exact virtual
// instants, so one-MTU pacing already delivers the full rate -- and a window's worth of tokens
// would be released in zero virtual time, a burst no physical link produces. Measured on the
// segment study's variant B, the 5 ms window under synctest inflated reissued requests by 46%
// and last-node completion by 25% (Q52) -- instrument artifact, not protocol behaviour.
const RealClockBurstWindow = 5 * time.Millisecond

// stampBurstWindow applies the clock-appropriate burst window to every link that has not chosen
// one explicitly: RealClockBurstWindow on the real clock, one-MTU pacing (zero) under synctest.
func stampBurstWindow(links []simlibp2p.NodeLinkSettingsAndCount, wallClock bool) []simlibp2p.NodeLinkSettingsAndCount {
	if !wallClock {
		return links
	}
	out := make([]simlibp2p.NodeLinkSettingsAndCount, len(links))
	for i, l := range links {
		if l.LinkSettings.Uplink.BurstWindow == 0 {
			l.LinkSettings.Uplink.BurstWindow = RealClockBurstWindow
		}
		if l.LinkSettings.Downlink.BurstWindow == 0 {
			l.LinkSettings.Downlink.BurstWindow = RealClockBurstWindow
		}
		out[i] = l
	}
	return out
}

// LatencyMatrix returns a per-pair latency function keyed on node index.
//
// Node addresses are derived from the index by simnet itself, so the mapping can be rebuilt
// here without waiting for the network to exist -- which matters because the latency function
// has to be supplied when the network is constructed. Packets whose endpoints are not recognised
// fall back to DefaultLatency rather than zero, so a mismatch shows up as uniform latency rather
// than an instantaneous network.
func LatencyMatrix(n int, oneWay func(from, to int) time.Duration) simlibp2p.LatencyFunc {
	byIP := make(map[string]int, n)
	for i := range n {
		byIP[simnet.IntToPublicIPv4(i).String()] = i
	}
	return func(p *simnet.Packet) time.Duration {
		from, okFrom := byIP[addrIP(p.From)]
		to, okTo := byIP[addrIP(p.To)]
		if !okFrom || !okTo {
			return DefaultLatency
		}
		return oneWay(from, to)
	}
}

func addrIP(a net.Addr) string {
	if u, ok := a.(*net.UDPAddr); ok {
		return u.IP.String()
	}
	host, _, err := net.SplitHostPort(a.String())
	if err != nil {
		return a.String()
	}
	return host
}
