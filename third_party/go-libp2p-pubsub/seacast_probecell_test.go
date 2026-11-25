package pubsub

import (
	"bytes"
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
)

// probeCellIndex is seacastTestIndex held to the chunk id's exact length, as the harness's parser
// (haveIndexOf: magic, version and length) is: seacastTestIndex reads any longer id, a WANT pseudo
// id included, as a chunk of another group, and seacastGated, which asks the parser before
// decodeWant, then finds no group and lets the ask through.
func probeCellIndex(mid string) (string, uint32, bool) {
	if len(mid) != haveRootLen+1 {
		return "", 0, false
	}
	return seacastTestIndex(mid)
}

// probeCellView is B's side of TestSeacastProbeCellOffer: what C offered and pushed of the probed
// cell, and what B asked C for it, read off the frames.
type probeCellView struct {
	*mockRawTracer
	mu                  sync.Mutex
	root                string
	index               uint32
	from                peer.ID
	offers, pushes, ask int
	asked               []string
}

func (v *probeCellView) probed(mid string) bool {
	if r, i, _, _, ok, _ := decodeWantV2(mid); ok {
		return r == v.root && i == v.index
	}
	if r, i, ok := decodeWant(mid); ok {
		return r == v.root && i == v.index
	}
	if r, enc, ok := decodeHave(mid); ok {
		var b haveBits
		b.Or(enc)
		return r == v.root && b.Has(v.index)
	}
	if r, i, ok := seacastTestIndex(mid); ok && len(mid) == haveRootLen+1 {
		return r == v.root && i == v.index
	}
	return false
}

func (v *probeCellView) RecvRPC(rpc *RPC) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.root == "" || rpc.from != v.from {
		return
	}
	for _, m := range rpc.GetPublish() {
		if r, i, ok := seacastTestIndex(string(m.GetData()[:haveRootLen+1])); ok && r == v.root && i == v.index {
			v.pushes++
		}
	}
	for _, ih := range rpc.GetControl().GetIhave() {
		for _, mid := range ih.GetMessageIDs() {
			if v.probed(mid) {
				v.offers++
			}
		}
	}
}

func (v *probeCellView) SendRPC(rpc *RPC, p peer.ID) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.root == "" || p != v.from {
		return
	}
	for _, iw := range rpc.GetControl().GetIwant() {
		for _, mid := range iw.GetMessageIDs() {
			if v.probed(mid) {
				v.ask++
				v.asked = append(v.asked, mid)
			}
		}
	}
}

// TestSeacastProbeCellOffer is the fixture of the probed cell offered at stop_pull (seacast.md
// §4.6's probe_asks and R5's verdict): does a member past stop_pull, with its probe of A's claim
// outstanding, ask a third node C that offers it the probed cell, and does that copy pass the
// claimant? B is complete at K = 4 of 8 with its duty armed and probes A, which claims every index
// and does not hold the probed one. C then publishes the probed cell. Holding B's complete, C neither
// pushes nor offers it to B (checked); a peer outside B's suppression scope would offer it, so the
// fixture hands B's router two such offers from C, as they arrive on the wire: by bitmap (asked by
// index, the v1 WANT) and by chunk id. Bitmap offers are on, as in the harness's cells.
//
//   - "as built": the exemption is by the probed cell, whatever id the ask carries. B asks C, C's
//     copy arrives and counts for B (answered from anyone), and the claimant's own copy never came.
//   - "without the cell's exemption", the positive control: the test removes the probed cell from
//     the exemption, as the package had it before (the probe's own WANT id alone). Both offers'
//     asks are declined, nothing is asked, and the probe breaks.
func TestSeacastProbeCellOffer(t *testing.T) {
	for _, mode := range []string{"as built", "without the cell's exemption"} {
		narrow := mode != "as built"
		t.Run(mode, func(t *testing.T) {
			synctestTest(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				const topic = "seg/row/1"
				root := strings.Repeat("1", haveRootLen)
				hosts := getDefaultHosts(t, 3)
				table := &MapRoleTable{Joins: map[peer.ID]map[string]Join{}}
				for _, h := range hosts {
					table.Joins[h.ID()] = map[string]Join{"row": {Interest: InterestAll, Duty: DutyForward}}
				}
				params := DefaultGossipSubParams()
				params.IDontWantMessageThreshold = 1
				view := &probeCellView{mockRawTracer: &mockRawTracer{}}
				psubs := getGossipsubsOptFn(ctx, hosts, func(i int, h host.Host) []Option {
					tp := TypeParams{RNode: 2, DeferWindow: time.Second}
					if i == 1 {
						tp.DecodeLater = func(string) (time.Duration, bool) { return 2 * time.Second, true }
					}
					T := &testObjectType{name: "row", params: tp, topics: []string{topic}, store: NewMapCellStore(),
						threshold: 4, indexCount: 8, topicOf: func(r string) (string, bool) { return topic, len(r) == haveRootLen }}
					opts := []Option{
						WithGossipSubParams(params), WithMessageSignaturePolicy(StrictNoSign), WithMessageIdFn(seacastTestID),
						WithHaveBitmaps(probeCellIndex), WithHaveAnnounceBitmaps(), // both HAVE bitmaps
						WithPhaseForwarding(func(tp string) bool { return !strings.HasPrefix(tp, "seg/") }, 2),
						WithIWantDiscipline(200 * time.Millisecond),
						WithIHaveCommitmentPark(1, 400*time.Millisecond, time.Minute, nil, nil),
						WithIHaveParkPerGroup(nil),
						WithSpecV05(SpecV05{PromiseAtWrite: true}, nil),
						WithSeacast([]ObjectType{T}, nil), WithSeacastRoles(table), WithSeacastMachine(),
					}
					if i == 1 {
						opts = append(opts, WithRawTracer(view))
					}
					return opts
				})
				for _, ps := range psubs {
					if _, err := ps.Subscribe(topic); err != nil {
						t.Fatal(err)
					}
				}
				connect(t, hosts[0], hosts[1])
				connect(t, hosts[1], hosts[2])
				time.Sleep(2 * time.Second)
				a, b, c := psubs[0], psubs[1], psubs[2]
				gsB := b.rt.(*GossipSubRouter)
				read := func(f func()) {
					done := make(chan struct{})
					gsB.p.eval <- func() { f(); close(done) }
					<-done
				}
				cell := func(i byte) []byte { return append([]byte(root+string(rune(i))), bytes.Repeat([]byte{'a' + i}, 64)...) }
				for i := range 4 {
					if err := a.Publish(topic, cell(byte(i))); err != nil { // lint:ignore uintcast -- a small index.
						t.Fatal(err)
					}
				}
				time.Sleep(300 * time.Millisecond)
				A, C := hosts[0].ID(), hosts[2].ID()
				var asked time.Time
				var index uint32
				var stopPull, probed bool
				var declined0 int64
				read(func() {
					e := gsB.have.peers[A][root]
					if e == nil {
						t.Fatal("B has no ledger entry for A")
					}
					e.bits.Or(fullHaveBits(8).Bytes())
					e.claimAll = true
					gsB.seacastNoteClaim(A, root)
					asked = time.Now()
					g := gsB.seacast.groups[root]
					stopPull = g != nil && g.stopPull
					if g != nil && g.probe != nil {
						probed, index = true, g.probe.index
						if narrow {
							gsB.seacast.probeCells[root].Clear(index) // the package's earlier rule: the probe's id alone
						}
					}
					declined0 = gsB.seacast.ctr.StopPullDeclined.Load()
					view.mu.Lock()
					view.root, view.index, view.from = root, index, C
					view.mu.Unlock()
				})
				if !stopPull || !probed || gsB.seacast.ctr.ProbesAsked.Load() != 1 {
					t.Fatalf("B past stop_pull %v, probing %v (probes %d); want true, true, 1", stopPull, probed, gsB.seacast.ctr.ProbesAsked.Load())
				}
				if err := c.Publish(topic, cell(byte(index))); err != nil { // lint:ignore uintcast -- a small index.
					t.Fatal(err)
				}
				time.Sleep(100 * time.Millisecond) // C's own frames to B, if any
				view.mu.Lock()
				offers, pushes := view.offers, view.pushes
				view.mu.Unlock()
				if offers != 0 || pushes != 0 {
					t.Fatalf("C, holding B's complete, offered the probed cell %d times and pushed it %d; want 0 and 0", offers, pushes)
				}
				// The offers of a peer outside B's suppression scope, as they reach B's router: by
				// bitmap (asked by index, the v1 WANT) and by chunk id.
				var bits haveBits
				bits.Set(index)
				topicID := topic
				forms := []string{encodeHave(root, &bits), string(cell(byte(index))[:haveRootLen+1])} // lint:ignore uintcast -- a small index.
				perForm := make([]int64, len(forms))
				for k, mid := range forms {
					read(func() {
						d0 := gsB.seacast.ctr.StopPullDeclined.Load()
						ctl := &pb.ControlMessage{Ihave: []*pb.ControlIHave{{TopicID: &topicID, MessageIDs: []string{mid}}}}
						gsB.HandleRPC(&RPC{RPC: pb.RPC{Control: ctl}, from: C})
						perForm[k] = gsB.seacast.ctr.StopPullDeclined.Load() - d0
					})
					time.Sleep(50 * time.Millisecond)
				}
				time.Sleep(time.Until(asked.Add(time.Second)) + time.Millisecond)
				ctr := gsB.seacast.ctr
				declined := ctr.StopPullDeclined.Load() - declined0
				view.mu.Lock()
				asks := view.ask
				for _, mid := range view.asked {
					_, wi, w1 := decodeWant(mid)
					t.Logf("ask sent to C: %d bytes, a v1 WANT %v (index %d), the probe's own id %v", len(mid), w1, wi, mid == encodeWantV2(root, index, false, 0))
				}
				view.mu.Unlock()
				t.Logf("%s: probed index %d; offers of it to B %d (bitmap, chunk id); declined at stop_pull %v; B's asks of it to C %d; probe answered %d broken %d",
					mode, index, len(forms), perForm, asks, ctr.ProbesAnswered.Load(), ctr.ProbesBroken.Load())
				if narrow {
					if asks != 0 || declined != int64(len(forms)) || perForm[0] != 1 || perForm[1] != 1 || ctr.ProbesBroken.Load() != 1 || ctr.ProbesAnswered.Load() != 0 {
						t.Fatalf("without the cell's exemption: asks %d, declined %d, broken %d, answered %d; want 0, %d, 1, 0", asks, declined, ctr.ProbesBroken.Load(), ctr.ProbesAnswered.Load(), len(forms))
					}
					return
				}
				if asks == 0 || declined != 0 || ctr.ProbesAnswered.Load() != 1 || ctr.ProbesBroken.Load() != 0 || ctr.ProbesOwnCopy.Load() != 0 {
					t.Fatalf("as built: asks %d, declined %d, answered %d, broken %d, the claimant's own copies %d; want at least 1, 0, 1, 0, 0",
						asks, declined, ctr.ProbesAnswered.Load(), ctr.ProbesBroken.Load(), ctr.ProbesOwnCopy.Load())
				}
			})
		})
	}
}
