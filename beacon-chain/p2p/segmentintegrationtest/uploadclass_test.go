package segmentintegrationtest

// Upload by class (Q132): what each node wrote to its streams during a message, split into
// pushed frames, IWANT answers, IHAVE, IWANT, IDONTWANT (the HAVE bitmaps and the hose acks),
// GRAFT/PRUNE and framing. Counted at the stream write, so dispatch-time cancels never count.
// Built to find what fills the publisher's uplink while its hose window is at the floor.

import (
	"fmt"
	"math"
	"sort"
	"testing"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
)

// uploadNewWindow starts every node's count afresh for the next message.
func uploadNewWindow(nw *simNetwork) {
	if nw == nil {
		return
	}
	for _, ps := range nw.Pubsubs {
		ps.ResetUploadStats()
	}
}

// uploadLines reports the publisher pub and each population's median node, in KB.
func uploadLines(t *testing.T, nw *simNetwork, pub int) []string {
	if nw == nil {
		return nil
	}
	lines := []string{uploadLine(nw, "publisher", []int{pub})}
	names, pops := populations(t, len(nw.Pubsubs), pub)
	for _, name := range names {
		if l := uploadLine(nw, name, pops[name]); l != "" {
			lines = append(lines, l)
		}
	}
	return lines
}

// uploadLine reports the median node of set, named name, in KB; empty for an empty set.
func uploadLine(nw *simNetwork, name string, set []int) string {
	if len(set) == 0 {
		return ""
	}
	stats := make([]pubsub.UploadStat, 0, len(set))
	for _, i := range set {
		stats = append(stats, nw.Pubsubs[i].UploadStats())
	}
	med := func(f func(pubsub.UploadStat) int64) float64 {
		v := make([]int64, len(stats))
		for i, s := range stats {
			v[i] = f(s)
		}
		sort.Slice(v, func(a, b int) bool { return v[a] < v[b] })
		return float64(v[len(v)/2]) / 1000
	}
	var ansF, ansR int64
	maxAns, maxPush := 0, 0
	for _, s := range stats {
		ansF, ansR = ansF+s.AnswerFrames, ansR+s.AnswerRPCs
		maxAns, maxPush = max(maxAns, s.MaxAnswerFrames), max(maxPush, s.MaxPushFrames)
	}
	perRPC := 0.0
	if ansR > 0 {
		perRPC = float64(ansF) / float64(ansR)
	}
	return fmt.Sprintf("upload [%s %d] median KB: all %.0f = push %.0f + answers %.0f + IHAVE %.1f + IWANT %.1f + IDONTWANT/HAVE %.1f + GRAFT/PRUNE %.1f + other %.1f; answer frames per write %.1f (max %d), push max %d",
		name, len(set), med(func(s pubsub.UploadStat) int64 { return s.AllBytes }),
		med(func(s pubsub.UploadStat) int64 { return s.PushBytes }), med(func(s pubsub.UploadStat) int64 { return s.AnswerBytes }),
		med(func(s pubsub.UploadStat) int64 { return s.IHaveBytes }), med(func(s pubsub.UploadStat) int64 { return s.IWantBytes }),
		med(func(s pubsub.UploadStat) int64 { return s.IDontWantBytes }), med(func(s pubsub.UploadStat) int64 { return s.GraftPruneBytes }),
		med(func(s pubsub.UploadStat) int64 { return s.OtherBytes }), perRPC, maxAns, maxPush)
}

// pushShareLine is where the message's pushed frames went: the share of the honest relays' and
// the publisher's pushes that the attacker set drew, beside its share of the receivers, and the
// median node's pushes received from honest relays by class. Zero on a clean cell by construction.
func pushShareLine(t *testing.T, nw *simNetwork, pub int) string {
	if nw == nil {
		return ""
	}
	n := len(nw.Pubsubs)
	idx := make(map[peer.ID]int, n)
	for i, h := range nw.Hosts {
		idx[h.ID()] = i
	}
	_, pops := populations(t, n, pub)
	attacker := make(map[int]bool, len(pops["attacker"]))
	for _, i := range pops["attacker"] {
		attacker[i] = true
	}
	var relayAll, relayAtt, pubAll, pubAtt int64
	recv := make([]int64, n)
	for i, ps := range nw.Pubsubs {
		if attacker[i] {
			continue
		}
		for p, f := range ps.UploadPushFramesTo() {
			j, ok := idx[p]
			if !ok {
				continue
			}
			if i == pub {
				pubAll += f
				if attacker[j] {
					pubAtt += f
				}
				continue
			}
			relayAll += f
			if attacker[j] {
				relayAtt += f
			}
			recv[j] += f
		}
	}
	pct := func(a, b int64) float64 { return 100 * float64(a) / math.Max(1, float64(b)) }
	med := func(set []int) int64 {
		if len(set) == 0 {
			return 0
		}
		v := make([]int64, len(set))
		for k, i := range set {
			v[k] = recv[i]
		}
		sort.Slice(v, func(a, b int) bool { return v[a] < v[b] })
		return v[len(v)/2]
	}
	receivers := len(pops["attacker"]) + len(pops["honest"])
	return fmt.Sprintf("push share: honest relays pushed %d frames, %d to attackers (%.1f%%); publisher %d, %d to attackers (%.1f%%); attackers %d of %d receivers (%.1f%%); from honest relays per node median: attacker %d honest %d",
		relayAll, relayAtt, pct(relayAtt, relayAll), pubAll, pubAtt, pct(pubAtt, pubAll),
		len(pops["attacker"]), receivers, pct(int64(len(pops["attacker"])), int64(receivers)),
		med(pops["attacker"]), med(pops["honest"]))
}
