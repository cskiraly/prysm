package pubsub

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
)

// TestSeacastHoardAsks is the toy's hoarder on asks by chunk id (node.py:1508–1523; the review of
// part 4's build, findings 8 and 13's f10): B hoards, decodes a coded group (K = 4 of 8) and
// rebuilds 4 to 7. A's lone ask for rebuilt index 5 by chunk id is what a probe looks like: served
// once (before the fix, uncounted, it was refused); a second lone ask of the group by A is refused;
// two asks of the group in one RPC are both refused. (A's writer drops an ask for a chunk A holds,
// so each step asks for one A lacks.)
func TestSeacastHoardAsks(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		const topic = "seg/row/1"
		root := strings.Repeat("1", haveRootLen)
		cell := func(r string, i byte) []byte {
			return append([]byte(r+string(rune(i))), bytes.Repeat([]byte{'a' + i}, 64)...)
		}
		hosts := getDefaultHosts(t, 2)
		table := &MapRoleTable{Joins: map[peer.ID]map[string]Join{}}
		for _, h := range hosts {
			table.Joins[h.ID()] = map[string]Join{"row": {Interest: InterestAll, Duty: DutyForward}}
		}
		ran := make(chan struct{}, 4)
		params := DefaultGossipSubParams()
		params.IDontWantMessageThreshold = 1
		psubs := getGossipsubsOptFn(ctx, hosts, func(i int, h host.Host) []Option {
			tp := TypeParams{RNode: 2}
			if i == 1 {
				tp.DecodeAtK = func(string) bool { return true }
			}
			base := &testObjectType{name: "row", params: tp, topics: []string{topic}, store: NewMapCellStore(),
				threshold: 4, indexCount: 8, topicOf: func(r string) (string, bool) { return topic, len(r) == haveRootLen }}
			T := &testDecodeType{testObjectType: base, cell: cell, wait: 100 * time.Millisecond}
			opts := []Option{
				WithGossipSubParams(params), WithMessageSignaturePolicy(StrictNoSign), WithMessageIdFn(seacastTestID),
				WithHaveBitmaps(seacastTestIndex),
				WithPhaseForwarding(func(tp string) bool { return !strings.HasPrefix(tp, "seg/") }, 2),
				WithSeacast([]ObjectType{T}, nil), WithSeacastRoles(table), WithSeacastMachine(),
			}
			if i == 1 {
				T.ran = ran
				opts = append(opts, WithSeacastFault("hoard"))
			}
			return opts
		})
		seacastOrdZeroAtEnd(t, psubs)
		for _, ps := range psubs {
			if _, err := ps.Subscribe(topic); err != nil {
				t.Fatal(err)
			}
		}
		connect(t, hosts[0], hosts[1])
		time.Sleep(2 * time.Second)
		a, b := psubs[0], psubs[1]
		gsA, gsB := a.rt.(*GossipSubRouter), b.rt.(*GossipSubRouter)
		B := hosts[1].ID()
		for i := range 4 {
			if err := a.Publish(topic, cell(root, byte(i))); err != nil { // lint:ignore uintcast -- a small index.
				t.Fatal(err)
			}
		}
		<-ran
		time.Sleep(200 * time.Millisecond)
		ctr := gsB.seacast.ctr
		if ctr.CellsRebuilt.Load() != 4 {
			t.Fatalf("rebuilt %d, want 4", ctr.CellsRebuilt.Load())
		}
		id := func(i byte) string { return string(cell(root, i)[:haveRootLen+1]) }
		ask := func(ids ...string) {
			done := make(chan struct{})
			gsA.p.eval <- func() {
				gsA.sendRPC(B, rpcWithControl(nil, nil, []*pb.ControlIWant{{MessageIDs: ids}}, nil, nil, nil), false)
				close(done)
			}
			<-done
			time.Sleep(100 * time.Millisecond)
		}
		for _, step := range []struct {
			ids             []string
			served, refused int64
			description     string
		}{
			{[]string{id(5)}, 1, 0, "a lone ask by chunk id: served once"},
			{[]string{id(6)}, 1, 1, "a second lone ask of the group by the same peer: refused"},
			{[]string{id(6), id(7)}, 1, 3, "two asks of the group in one RPC: both refused"},
		} {
			ask(step.ids...)
			if ctr.HoardServed.Load() != step.served || ctr.HoardRefused.Load() != step.refused {
				t.Fatalf("%s: served %d refused %d, want %d and %d", step.description, ctr.HoardServed.Load(), ctr.HoardRefused.Load(), step.served, step.refused)
			}
		}
	})
}
