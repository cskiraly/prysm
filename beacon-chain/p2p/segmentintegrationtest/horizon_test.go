package segmentintegrationtest

// The observation horizon: every node read at one instant, the deadline after publish, on the
// happy path and the censored one alike. Before it, a cell's byte counters were read when its
// last node completed (so what arrived afterwards was never counted) or not at all (the timeout
// path returned before aggregating), and no cell had ever reported how long its requests took
// to be answered, how many were still open, or how much of its writers' time the transport held.
// The snapshot is passive: it reads the tracers, the routers' request ledgers and their outbound
// queues, and decides nothing (E0).

import (
	"fmt"
	"sort"
	"testing"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
)

type horizonSnapshot struct {
	at     time.Duration // the horizon, relative to publish
	takeAt time.Time     // the instant it was taken
	taken  bool
	// per node, at the horizon
	rx, ctrl, dups []int
	ledgers        []*pubsub.RequestLedger
	outbound       []map[peer.ID]pubsub.OutboundStat
	// the populations reported separately, in report order; attacker is the injected set, so
	// take can class the honest nodes by how many attackers sit in their mesh
	popNames []string
	pops     map[string][]int
	attacker map[int]bool
}

func newHorizonSnapshot(t *testing.T, n int, at time.Duration) *horizonSnapshot {
	h := &horizonSnapshot{at: at, rx: make([]int, n), ctrl: make([]int, n), dups: make([]int, n),
		ledgers: make([]*pubsub.RequestLedger, n), outbound: make([]map[peer.ID]pubsub.OutboundStat, n)}
	h.popNames, h.pops = populations(t, n)
	h.attacker = map[int]bool{}
	for _, i := range h.pops["attacker"] {
		h.attacker[i] = true
	}
	return h
}

// take reads every node. Runs in the bubble at the horizon; the routers are alive.
func (h *horizonSnapshot) take(nw *simNetwork, tracers []*recordingTracer) {
	for i := range tracers {
		d, _, rx, _ := tracers[i].Counts()
		h.rx[i], h.ctrl[i], h.dups[i] = rx, tracers[i].ControlSeen().BytesRecv, d
		h.ledgers[i] = nw.Pubsubs[i].RequestLedger()
		h.outbound[i] = nw.Pubsubs[i].OutboundStats()
	}
	// Exposure classes: honest nodes by the attackers in their mesh at the horizon (the capture
	// screen's victims are the exposed, not a placed set).
	if len(h.attacker) > 0 {
		idx := make(map[peer.ID]int, len(nw.Hosts))
		for i, hst := range nw.Hosts {
			idx[hst.ID()] = i
		}
		for _, i := range h.pops["honest"] {
			c := 0
			for _, p := range tracers[i].MeshPeerIDs() {
				if h.attacker[idx[p]] {
					c++
				}
			}
			cls := "x0"
			switch {
			case c >= 5:
				cls = "x5p"
			case c >= 3:
				cls = "x3_4"
			case c >= 1:
				cls = "x1_2"
			}
			h.pops[cls] = append(h.pops[cls], i)
		}
		for _, cls := range []string{"x0", "x1_2", "x3_4", "x5p"} {
			if len(h.pops[cls]) > 0 {
				h.popNames = append(h.popNames, cls)
			}
		}
	}
	h.takeAt = time.Now()
	h.taken = true
}

// populations names the node sets the horizon reports separately: honest (every non-publisher
// node not injected as an adversary), attacker (withholders and spoofers), and among the honest
// the link classes when the cell has them — slow (thin links), dc (datacenter) and other. The
// sets follow the injectors' and meshLinks' own selection, so a node is in exactly the class it
// was built with.
func populations(t *testing.T, n int) ([]string, map[string][]int) {
	attacker := map[int]bool{}
	for i := range failWithholdSet(t, n) {
		attacker[i] = true
	}
	for i := range failSpoofSet(t, n) {
		attacker[i] = true
	}
	dcCount, slowCount := meshLinkCounts(t, n)
	pops := map[string][]int{}
	add := func(name string, i int) { pops[name] = append(pops[name], i) }
	for i := 1; i < n; i++ {
		if attacker[i] {
			add("attacker", i)
			continue
		}
		add("honest", i)
		switch {
		case i < 1+dcCount:
			add("dc", i)
		case i < 1+dcCount+slowCount:
			add("slow", i)
		default:
			add("other", i)
		}
	}
	names := []string{"honest"}
	if len(pops["attacker"]) > 0 {
		names = append(names, "attacker")
	}
	if dcCount > 0 || slowCount > 0 {
		for _, c := range []string{"slow", "dc", "other"} {
			if len(pops[c]) > 0 {
				names = append(names, c)
			}
		}
	} else {
		delete(pops, "other")
	}
	if _, holders := narrowHolders(t, n); len(holders) > 0 {
		pops["holder"] = holders
		names = append(names, "holder")
	}
	return names, pops
}

// quantile of a sorted slice, nearest rank.
func quantile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	i := int(float64(len(sorted)-1)*p + 0.5)
	return sorted[i]
}

func msOf(v float64) string { return fmt.Sprintf("%dms", int64(v/1e6)) }

// lines renders the horizon: per population its bytes, its requests and its outbound queues,
// then the publisher's queues alone. compAt and rxAtComp come from the driver's completion loop.
func (h *horizonSnapshot) lines(compAt []time.Duration, rxAtComp []int) []string {
	if h == nil || !h.taken {
		return nil
	}
	ms := h.at.Milliseconds()
	var out []string
	for _, name := range h.popNames {
		set := h.pops[name]
		if len(set) == 0 {
			continue
		}
		tag := fmt.Sprintf("@%dms [%s %d]", ms, name, len(set))
		// bytes
		var rx, ctrl, dups, done, post int
		for _, i := range set {
			rx += h.rx[i]
			ctrl += h.ctrl[i]
			dups += h.dups[i]
			if compAt[i] > 0 && compAt[i] <= h.at {
				done++
				post += h.rx[i] - rxAtComp[i]
			}
		}
		postPer := 0
		if done > 0 {
			postPer = post / done
		}
		out = append(out, fmt.Sprintf("bytes%s: completed %d/%d rx/node %dB ctrl rx/node %dB dups %d post-completion rx/node %dB",
			tag, done, len(set), rx/len(set), ctrl/len(set), dups, postPer))
		// requests
		var asked, answered, overtaken, open, abandoned, dropped, pushes, asks, adups int
		var service, ages, topShare, answerers []float64
		for _, i := range set {
			l := h.ledgers[i]
			if l == nil {
				continue
			}
			pushes += l.Unasked
			byPeer := map[peer.ID]int{}
			nodeAnswered := 0
			done := compAt[i] > 0 && compAt[i] <= h.at
			for _, r := range l.Records {
				asked++
				asks += len(r.Asks)
				adups += r.Dups
				for _, a := range r.Asks {
					if a.Dropped {
						dropped++
					}
				}
				if r.Arrived.IsZero() {
					// An unanswered ask on a node that has completed was abandoned, not failed: the
					// node stopped needing it (a code's spare ids, a group's tail after the fact).
					if done {
						abandoned++
						continue
					}
					open++
					ages = append(ages, float64(h.takeAt.Sub(r.Asks[0].At)))
					continue
				}
				if !r.Solicited {
					overtaken++
					continue
				}
				answered++
				nodeAnswered++
				byPeer[r.From]++
				var askAt time.Time
				for _, a := range r.Asks {
					if a.Peer == r.From && !a.At.After(r.Arrived) {
						askAt = a.At
					}
				}
				service = append(service, float64(r.Arrived.Sub(askAt)))
			}
			if nodeAnswered > 0 {
				top := 0
				for _, c := range byPeer {
					if c > top {
						top = c
					}
				}
				topShare = append(topShare, float64(top)/float64(nodeAnswered))
				answerers = append(answerers, float64(len(byPeer)))
			}
		}
		for _, v := range [][]float64{service, ages, topShare, answerers} {
			sort.Float64s(v)
		}
		asksPerID := 0.0
		if asked > 0 {
			asksPerID = float64(asks) / float64(asked)
		}
		out = append(out, fmt.Sprintf("requests%s: ids asked %d answered %d overtaken %d open %d (age p50 %s) abandoned %d dropped-asks %d pushes %d; asks/id %.2f answered-dups %d; service p50 %s p90 %s p99 %s; top answerer p50 %.2f answerers p50 %.0f",
			tag, asked, answered, overtaken, open, msOf(quantile(ages, 0.5)), abandoned, dropped, pushes, asksPerID, adups,
			msOf(quantile(service, 0.5)), msOf(quantile(service, 0.9)), msOf(quantile(service, 0.99)),
			quantile(topShare, 0.5), quantile(answerers, 0.5)))
		// outbound
		out = append(out, "outbound"+tag+": "+h.outboundLine(set))
	}
	out = append(out, fmt.Sprintf("outbound@%dms [publisher]: %s", ms, h.outboundLine([]int{0})))
	return out
}

func (h *horizonSnapshot) outboundLine(set []int) string {
	var peak, blocked, busiest []float64
	for _, i := range set {
		m := h.outbound[i]
		if m == nil {
			continue
		}
		var pk, bl, bs int64
		for _, s := range m {
			if s.PeakQueuedBytes > pk {
				pk = s.PeakQueuedBytes
			}
			bl += int64(s.WriteBlocked)
			if int64(s.WriteBlocked) > bs {
				bs = int64(s.WriteBlocked)
			}
		}
		peak = append(peak, float64(pk))
		blocked = append(blocked, float64(bl))
		busiest = append(busiest, float64(bs))
	}
	for _, v := range [][]float64{peak, blocked, busiest} {
		sort.Float64s(v)
	}
	if len(set) == 1 {
		return fmt.Sprintf("peak queued/peer %dKB; writer blocked %s, busiest peer %s",
			int(quantile(peak, 0.5))>>10, msOf(quantile(blocked, 0.5)), msOf(quantile(busiest, 0.5)))
	}
	return fmt.Sprintf("peak queued/peer p50 %dKB p90 %dKB; writer blocked/node p50 %s p90 %s, busiest peer p50 %s",
		int(quantile(peak, 0.5))>>10, int(quantile(peak, 0.9))>>10,
		msOf(quantile(blocked, 0.5)), msOf(quantile(blocked, 0.9)), msOf(quantile(busiest, 0.5)))
}
