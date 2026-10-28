package segmentintegrationtest

// Bytes on the wire, per node: what each node's downlink delivered, UDP payloads with every QUIC
// header and acknowledgement in them, for all protocols on the host. It is the one byte count that
// does not depend on where an arm's own counters sit, so it compares arms whose application
// counters differ in kind: a gossipsub message is counted whole when it arrives, while ethp2p's
// broadcast cancels chunk streams in flight and their data never reaches an application read.

// downlinkBytes is each node's downlink byte counter now: bytes enqueued minus bytes dropped.
func downlinkBytes(nw *simNetwork) []uint64 {
	if nw == nil || nw.Sim == nil {
		return nil
	}
	stats := nw.Sim.LinkStats()
	out := make([]uint64, len(stats))
	for i, s := range stats {
		out[i] = s.Downlink.BytesEnqueued - s.Downlink.BytesDropped
	}
	return out
}

// linkRxSince sums, over the network, what the downlinks delivered since base.
func linkRxSince(nw *simNetwork, base []uint64) int {
	now := downlinkBytes(nw)
	if now == nil || len(base) != len(now) {
		return 0
	}
	var sum uint64
	for i := range now {
		sum += now[i] - base[i]
	}
	return int(sum)
}

// socketBytes is each node's socket receive counter now: what its downlink handed it. downlinkBytes
// counts at the queue's intake instead, so over an interval the two differ by the change in what
// waits in the queue (more at the end than the start makes link rx the larger).
func socketBytes(nw *simNetwork) []uint64 {
	if nw == nil || nw.Sim == nil {
		return nil
	}
	stats := nw.Sim.ConnStats()
	out := make([]uint64, len(stats))
	for i, s := range stats {
		out[i] = uint64(s.BytesRcvd)
	}
	return out
}

// socketRxSince sums, over the network, what the sockets received since base.
func socketRxSince(nw *simNetwork, base []uint64) int {
	now := socketBytes(nw)
	if now == nil || len(base) != len(now) {
		return 0
	}
	var sum uint64
	for i := range now {
		sum += now[i] - base[i]
	}
	return int(sum)
}
