package segmentintegrationtest

// SEGMENT_TOPO_DUMP=<path>: the cell's connection graph, written once when the mesh has settled,
// for offline analysis of what a graph-aware adversary can intersect. gzip'd CSV, a header, then
// one row per connection end (each connection twice, once from either node):
//
//	node,peer,dir,owd_ms,rtt_ms,node_up_mbps,peer_up_mbps,node_region,peer_region
//
// dir is the libp2p connection's direction at node (out: node dialled it); owd_ms the latency
// model's one-way delay; rtt_ms the QUIC connection's min RTT at the dump; up the node's uplink in
// Mbps as meshLinks built it; region the geo model's (empty under uniform latency).

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/OffchainLabs/prysm/v7/testing/gossipsim"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	simlibp2p "github.com/libp2p/go-libp2p/x/simlibp2p"
	"github.com/quic-go/quic-go"
)

// topoDumpOut is the dump's path, empty when the instrument is off.
func topoDumpOut() string { return os.Getenv("SEGMENT_TOPO_DUMP") }

// cellGeo is the cell's latency model as a stem policy and the dump read it: each node's region,
// nil when the model has none, and the modelled one-way delay of a pair.
type cellGeo struct {
	region []int
	oneWay func(i, j int) time.Duration
}

// cellGeoFor reads the latency model newSimNetwork builds for n nodes; uniform is the one-way
// delay of the uniform model.
func cellGeoFor(t *testing.T, n int, uniform time.Duration) cellGeo {
	t.Helper()
	if os.Getenv("SEGMENT_LATENCY_MODEL") != "geo" {
		return cellGeo{oneWay: func(int, int) time.Duration { return uniform }}
	}
	seed := meshGraphSeed(t)
	_, _, pair := geoLatencyPair(t, seed, n)
	return cellGeo{region: gossipsim.GeoRegions(seed, n), oneWay: pair}
}

// regionName is node i's region, empty without regions.
func (g cellGeo) regionName(i int) string {
	if g.region == nil {
		return ""
	}
	return gossipsim.GeoRegionNames[g.region[i]]
}

// nodeUplinks is each node's uplink in bits per second, in node order.
func nodeUplinks(links []simlibp2p.NodeLinkSettingsAndCount) []int {
	var up []int
	for _, l := range links {
		for range l.Count {
			up = append(up, l.LinkSettings.Uplink.BitsPerSecond)
		}
	}
	return up
}

// connDir names a connection's direction at its local end.
func connDir(c network.Conn) string {
	switch c.Stat().Direction {
	case network.DirOutbound:
		return "out"
	case network.DirInbound:
		return "in"
	default:
		return "unknown"
	}
}

// connMinRTT is a connection's QUIC min RTT, 0 before a sample or off QUIC.
func connMinRTT(c network.Conn) time.Duration {
	var qc *quic.Conn
	if !c.As(&qc) || qc == nil {
		return 0
	}
	return qc.ConnectionStats().MinRTT
}

// writeTopoDump writes the dump and returns its summary line.
func writeTopoDump(path string, nw *simNetwork, up []int, geo cellGeo) (string, error) {
	f, err := os.Create(path)
	if err != nil {
		return "", err
	}
	zw := gzip.NewWriter(f)
	w := bufio.NewWriterSize(zw, 1<<20)
	idx := make(map[peer.ID]int, len(nw.Hosts))
	for i, h := range nw.Hosts {
		idx[h.ID()] = i
	}
	mbps := func(i int) float64 {
		if i >= len(up) {
			return 0
		}
		return float64(up[i]) / float64(simlibp2p.OneMbps)
	}
	ms := func(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }
	_, _ = fmt.Fprintln(w, "node,peer,dir,owd_ms,rtt_ms,node_up_mbps,peer_up_mbps,node_region,peer_region")
	rows := 0
	dirs := map[string]int{}
	// against counts the ends whose direction disagrees with the harness's dial order (lower index dials).
	against := 0
	for i, h := range nw.Hosts {
		type end struct {
			j int
			c network.Conn
		}
		var ends []end
		for _, c := range h.Network().Conns() {
			if j, ok := idx[c.RemotePeer()]; ok {
				ends = append(ends, end{j, c})
			}
		}
		sort.SliceStable(ends, func(a, b int) bool { return ends[a].j < ends[b].j })
		for _, e := range ends {
			d := connDir(e.c)
			dirs[d]++
			if (d == "out") != (i < e.j) {
				against++
			}
			_, _ = fmt.Fprintf(w, "%d,%d,%s,%.3f,%.3f,%g,%g,%s,%s\n", i, e.j, d, ms(geo.oneWay(i, e.j)), ms(connMinRTT(e.c)),
				mbps(i), mbps(e.j), geo.regionName(i), geo.regionName(e.j))
			rows++
		}
	}
	if err := w.Flush(); err != nil {
		return "", err
	}
	if err := zw.Close(); err != nil {
		return "", err
	}
	if err := f.Close(); err != nil {
		return "", err
	}
	return fmt.Sprintf("topology dump: %d connection ends (out %d, in %d, unknown %d; %d against the dial order, lower index dials) -> %s",
		rows, dirs["out"], dirs["in"], dirs["unknown"], against, path), nil
}
