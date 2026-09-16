package gossipsim

import (
	"fmt"
	"net"
	"sort"
	"testing"
	"time"

	simlibp2p "github.com/libp2p/go-libp2p/x/simlibp2p"
	"github.com/marcopolo/simnet"
)

// Synthetic geography for heterogeneous latency. Each node lands in a region by a seeded
// weighted draw; a packet's one-way latency is the inter-region base plus a deterministic
// per-pair jitter of up to +/-20%. The point is not cartographic accuracy but the two
// properties uniform latency destroys: geographic correlation (nearby pairs are fast, which
// is what shapes push/pull races) and heavy spread. Region weights lean EU/NA/AS like the
// observed node distribution; the base matrix is real-world-ish one-way milliseconds,
// calibrated so the pair mean lands near the ~62 ms RIPE-Atlas mean the PPPT study used.
var geoRegionWeights = []int{35, 20, 10, 25, 10} // eu, na-east, na-west, asia, rest

var geoBaseOneWayMs = [5][5]int{
	{8, 40, 70, 90, 110},
	{40, 8, 30, 80, 60},
	{70, 30, 8, 60, 70},
	{90, 80, 60, 10, 70},
	{110, 60, 70, 70, 15},
}

// GeoMix is the avalanche step shared by the region draw and the pair jitter.
func GeoMix(x uint64) uint64 {
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return x
}

// GeoLatency builds the packet-latency function over n nodes and returns it with a summary
// of the realized pair distribution. Node identity is recovered from simnet's deterministic
// index-to-IP mapping (IntToPublicIPv4: the low three octets carry index+1).
func GeoLatency(t *testing.T, seed uint64, n int) (simlibp2p.LatencyFunc, string) {
	fn, summary, _ := GeoLatencyPair(t, seed, n)
	return fn, summary
}

// GeoLatencyPair is GeoLatency plus the raw per-pair function.
//
// The two-publisher driver needs the modelled one-way latency between two specific nodes -- to
// place the proposer at a chosen distance from the builder, and to model a direct proposer-builder
// channel as exactly one hop rather than as a guess.
func GeoLatencyPair(t *testing.T, seed uint64, n int) (simlibp2p.LatencyFunc, string, func(i, j int) time.Duration) {
	t.Helper()
	total := 0
	for _, w := range geoRegionWeights {
		total += w
	}
	region := make([]int, n)
	for i := range region {
		r := int(GeoMix(seed^0x9e0_1a7e^uint64(i)) % uint64(total)) // lint:ignore uintcast -- bounded by total.
		for k, w := range geoRegionWeights {
			if r < w {
				region[i] = k
				break
			}
			r -= w
		}
	}
	pair := func(i, j int) time.Duration {
		if i == j {
			return time.Millisecond
		}
		lo, hi := i, j
		if lo > hi {
			lo, hi = hi, lo
		}
		base := geoBaseOneWayMs[region[i]][region[j]]
		// Deterministic symmetric jitter in [0.8, 1.2).
		f := 0.8 + 0.4*float64(GeoMix(seed^uint64(lo)<<20^uint64(hi))%1024)/1024
		return time.Duration(float64(base) * f * float64(time.Millisecond))
	}
	idxOf := func(a net.Addr) (int, bool) {
		var ip net.IP
		switch v := a.(type) {
		case *net.UDPAddr:
			ip = v.IP.To4()
		default:
			host, _, err := net.SplitHostPort(a.String())
			if err != nil {
				return 0, false
			}
			ip = net.ParseIP(host).To4()
		}
		if ip == nil {
			return 0, false
		}
		idx := int(ip[1])<<16 | int(ip[2])<<8 | int(ip[3])
		idx--
		if idx < 0 || idx >= n {
			return 0, false
		}
		return idx, true
	}
	fn := func(p *simnet.Packet) time.Duration {
		i, iok := idxOf(p.From)
		j, jok := idxOf(p.To)
		if !iok || !jok {
			return DefaultLatency
		}
		return pair(i, j)
	}
	// Realized distribution over a deterministic sample of pairs, for the log line.
	var samples []time.Duration
	for i := 0; i < n; i++ {
		for k := 1; k <= 3; k++ {
			samples = append(samples, pair(i, (i+k*7919)%n))
		}
	}
	sort.Slice(samples, func(a, b int) bool { return samples[a] < samples[b] })
	var sum time.Duration
	for _, s := range samples {
		sum += s
	}
	q := func(f float64) time.Duration { return samples[int(f*float64(len(samples)-1))] }
	summary := fmt.Sprintf("one-way mean %v, p10 %v, p50 %v, p90 %v over %d sampled pairs",
		(sum / time.Duration(len(samples))).Round(time.Millisecond),
		q(0.10).Round(time.Millisecond), q(0.50).Round(time.Millisecond), q(0.90).Round(time.Millisecond), len(samples))
	return fn, summary, pair
}
