package segmentintegrationtest

// Multi-message cells under Shadow (multimessage_test.go's SEGMENT_MESSAGES): M payloads a gap
// apart on one standing network, node 0 publishing each. At message m's publish instant every node
// switches to that message's payload, as the driver switches the whole network at once, and watches
// for it afresh. Each node reports every message on its own SHADOWMSG line: completion dated from
// that message's publish, and counters windowed to it -- the tracer's, reset at the publish as the
// driver resets them, and the bytes the node's QUIC connections received (link rx: UDP payloads with
// every QUIC header, every protocol on the host, the quantity of the driver's link rx/node).
// Snapshots every msgSnapshotInterval up to the deadline let the extractor cut the counters where
// the driver does, at the last node's completion (testing/shadowstudy/msgextract.py).
//
// SHADOW_HOLD_MS keeps its meaning: the node counts until the first publish plus the hold, so a cell
// of M messages needs a hold beyond (M-1) gaps plus the deadline. Message m's window ends at the next
// publish, the last one's at the end of the hold.

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cskiraly/eth-networking-lab/shadowsim"
	"github.com/libp2p/go-libp2p/core/host"
)

// msgStatPrefix marks a node's report line for one message of a multi-message cell.
const msgStatPrefix = "SHADOWMSG "

// msgSnapshotInterval is how often the counters are sampled after each publish, up to the deadline.
const msgSnapshotInterval = 25 * time.Millisecond

// msgCounters is one node's counters for the message in flight, T seconds after its publish.
type msgCounters struct {
	T       float64 `json:"t"`
	Dups    int     `json:"dups"`
	RxBytes int     `json:"rx_bytes"`
	TxBytes int     `json:"tx_bytes"`
	CtrlRx  int     `json:"ctrl_rx"`
	IdwSent int     `json:"idw_sent"`
	Drops   int     `json:"drops"`
	LinkRx  int64   `json:"link_rx"`
	LinkTx  int64   `json:"link_tx"`
}

// msgStat is one node's report for one message.
type msgStat struct {
	Msg            int     `json:"msg"`
	Msgs           int     `json:"msgs"`
	Index          int     `json:"index"`
	Transport      string  `json:"transport"`
	Arm            string  `json:"arm"`
	ArmName        string  `json:"arm_name"`
	ArmMsgs        int     `json:"arm_msgs"`
	ArmNeed        int     `json:"arm_need"`
	ArmTotalBytes  int     `json:"arm_total_bytes"`
	UnitBytes      int     `json:"unit_bytes"`
	IDBytes        int     `json:"id_bytes"`
	Completed      bool    `json:"completed"`
	CompS          float64 `json:"comp_s"` // seconds after this message's publish
	MeshAtPublish  int     `json:"mesh_at_publish"`
	PeersAtPublish int     `json:"peers_at_publish"`
	StartS         float64 `json:"start_s"`
	PublishS       float64 `json:"publish_s"`
	EndS           float64 `json:"end_s"`
	// Declined is the stop-pull gate's declines so far in the cell, cumulative over messages.
	Declined  int64         `json:"declined"`
	Snapshots []msgCounters `json:"snapshots"`
	End       msgCounters   `json:"end"` // at the end of the message's window
	// V05 is what the v0.5 rules did on this node over the message's window (SEGMENT_V05), keyed
	// as the driver's "v0.5 this message" line names them; absent when the rules are off.
	V05 map[string]int64 `json:"v05,omitempty"`
}

// v05Deltas is what the v0.5 rules did between a and now (specv05_test.go's counters, which in a
// Shadow node's process are this node's alone).
func v05Deltas(a, now v05Snap) map[string]int64 {
	return map[string]int64{
		"promises_at_write": now.atWrite - a.atWrite, "asks_lost": now.lost - a.lost,
		"unpushed": now.unpushed - a.unpushed, "offers_owed": now.owed - a.owed, "owed_sent": now.owedSent - a.owedSent,
		"answers_owed": now.answers - a.answers, "last_resorts_armed": now.armed - a.armed,
		"last_resort_asks": now.lrAsks - a.lrAsks, "parked_asked": now.parked - a.parked,
		"beyond_list": now.beyond - a.beyond, "closed_promises": now.closed - a.closed,
		"ledger_asks": now.ledger - a.ledger, "expired": now.expired - a.expired, "expired_discards": now.discards - a.discards,
	}
}

// linkBytes is what the host's QUIC connections have received and sent so far: UDP payloads, QUIC
// headers and acknowledgements included. Zero over TCP, whose bytes this does not see.
func linkBytes(h host.Host) (rx, tx int64) {
	for _, qc := range quicConns(h) {
		s := qc.ConnectionStats()
		rx, tx = rx+int64(s.BytesReceived), tx+int64(s.BytesSent) // lint:ignore uintcast -- byte counts of one cell.
	}
	return rx, tx
}

// shadowMultiRefuse fails on what a multi-message Shadow cell cannot honour: a publisher other than
// node 0 (the schedule publishes from node 0), variants without a next-message hook, and the
// per-segment curves, which the single-message report carries.
func shadowMultiRefuse(t *testing.T, plan messagePlan, sc shadowCell) {
	t.Helper()
	if plan.rotate || plan.node != 0 {
		t.Fatal("a multi-message Shadow cell publishes every message from node 0 (SEGMENT_PUBLISHER)")
	}
	if sc.next == nil {
		t.Fatalf("SEGMENT_MESSAGES: the %s Shadow cell has no multi-message support", sc.label)
	}
	if diffusionCurves() {
		t.Fatal("SEGMENT_DIFFUSION is not supported on a multi-message Shadow cell")
	}
}

// shadowMessages runs a multi-message cell from the first publish on and prints one SHADOWMSG line
// per message. The node has joined; run's watch has not been started.
func shadowMessages(t *testing.T, e shadowsim.Schedule, plan messagePlan, sc shadowCell, run shadowRun, h host.Host, tracer *recordingTracer, start time.Time) {
	t.Helper()
	deadline := failureDeadline(t)
	m := len(plan.pubs)
	holdEnd := e.PublishAt.Add(e.Hold)
	if last := e.PublishAt.Add(time.Duration(m-1) * plan.gap); !holdEnd.After(last.Add(deadline)) {
		t.Fatalf("SHADOW_HOLD_MS %v ends before message %d's deadline: %d messages %v apart need more than %v",
			e.Hold, m, m, plan.gap, time.Duration(m-1)*plan.gap+deadline)
	}
	arm := sc.arm
	stopWatch := func() {}
	defer func() { stopWatch() }()
	for k := 1; k <= m; k++ {
		at := e.PublishAt.Add(time.Duration(k-1) * plan.gap)
		end := holdEnd
		if k < m {
			end = at.Add(plan.gap)
		}
		shadowsim.SleepUntil(at)
		// A node still watching the previous message has missed it; the new one starts afresh.
		stopWatch()
		if k > 1 {
			arm = sc.next(t, k)
			tracer.ResetCounters()
		}
		linkRx0, linkTx0 := linkBytes(h)
		v050 := v05Now()
		counters := func(since time.Duration) msgCounters {
			dups, _, rx, tx := tracer.Counts()
			d, u := tracer.Losses()
			c := tracer.ControlSeen()
			lrx, ltx := linkBytes(h)
			return msgCounters{T: since.Seconds(), Dups: dups, RxBytes: rx, TxBytes: tx, CtrlRx: c.BytesRecv,
				IdwSent: c.IdontwantSent, Drops: d + u, LinkRx: lrx - linkRx0, LinkTx: ltx - linkTx0}
		}
		st := msgStat{
			Msg: k, Msgs: m, Index: e.Index, Transport: e.Transport, Arm: sc.label, ArmName: arm.name,
			MeshAtPublish: tracer.MeshPeers(), PeersAtPublish: len(h.Network().Peers()),
			StartS: shadowsim.SimSeconds(start), PublishS: shadowsim.SimSeconds(at),
		}
		if w, ok := sc.v.(wireFormer); ok {
			wf := w.wireForm()
			st.ArmMsgs, st.ArmNeed, st.ArmTotalBytes, st.UnitBytes, st.IDBytes = wf.msgs, wf.need, wf.total, wf.unit, wf.idBytes
		}
		var compAt atomic.Int64
		var completed atomic.Bool
		wctx, cancel := context.WithCancel(context.Background())
		stopWatch = cancel
		run.watch(wctx, func() {
			compAt.Store(int64(time.Since(at)))
			completed.Store(true)
		})
		if e.Index == 0 {
			if err := run.publish(wctx, t); err != nil {
				t.Fatalf("message %d: publish: %v", k, err)
			}
			t.Logf("published message %d/%d %s: %d frames, %d bytes", k, m, arm.name, len(arm.msgs), arm.total)
		}
		for j := 1; ; j++ {
			snapAt := at.Add(time.Duration(j) * msgSnapshotInterval)
			if snapAt.After(at.Add(deadline)) || snapAt.After(end) {
				break
			}
			shadowsim.SleepUntil(snapAt)
			st.Snapshots = append(st.Snapshots, counters(time.Since(at)))
		}
		shadowsim.SleepUntil(end)
		st.End = counters(time.Since(at))
		st.EndS = shadowsim.SimSeconds(time.Now())
		st.Completed, st.CompS = completed.Load(), float64(compAt.Load())/1e9
		st.Declined = sc.declined(e.Index)
		if os.Getenv("SEGMENT_V05") != "" {
			st.V05 = v05Deltas(v050, v05Now())
		}
		b, err := json.Marshal(st)
		if err != nil {
			t.Fatalf("message %d: %v", k, err)
		}
		if _, err := fmt.Fprintf(os.Stdout, "%s%s\n", msgStatPrefix, b); err != nil {
			t.Fatalf("message %d: %v", k, err)
		}
	}
}
