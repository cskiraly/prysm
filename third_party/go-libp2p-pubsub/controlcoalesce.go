package pubsub

import (
	pb "github.com/libp2p/go-libp2p-pubsub/pb"
)

// --- Folding control onto a queued RPC ---
//
// The writer attaches our pending HAVE to the RPC it pops for a peer (havebitmap.go's attach), so
// the HAVE leaves with the next frame instead of in an RPC of its own. mergedControl builds that RPC,
// and dedupeHave keeps one HAVE per group in it.

// mergedControl returns a fresh RPC carrying dst's payload untouched and dst's control followed by
// src's. It never mutates either: an RPC already in a queue may be observed by the writer goroutine
// the moment the queue mutex is released, and the control objects are shared with the router's
// pending maps. The clone is shallow on purpose — the published messages are shared pointers that
// nothing mutates, so folding control onto a queued *data* RPC costs one small allocation and not a
// copy of the payload. That case is the one that matters: on a link that keeps up, the RPC already
// waiting is usually a segment push rather than another control RPC.
func mergedControl(dst, src *RPC) *RPC {
	out := &RPC{RPC: pb.RPC{Publish: dst.Publish, Subscriptions: dst.Subscriptions, Control: &pb.ControlMessage{}}, from: dst.from}
	for _, c := range []*pb.ControlMessage{dst.GetControl(), src.GetControl()} {
		if c == nil {
			continue
		}
		out.Control.Ihave = append(out.Control.Ihave, c.GetIhave()...)
		out.Control.Iwant = append(out.Control.Iwant, c.GetIwant()...)
		out.Control.Graft = append(out.Control.Graft, c.GetGraft()...)
		out.Control.Prune = append(out.Control.Prune, c.GetPrune()...)
		out.Control.Idontwant = append(out.Control.Idontwant, c.GetIdontwant()...)
	}
	out.Control.Idontwant = dedupeHave(out.Control.Idontwant)
	return out
}

// dedupeHave keeps only the newest HAVE bitmap per group root across merged IDONTWANT lists:
// a bitmap is idempotent state, so the older one says nothing the newer does not, and this is
// where a HAVE saves bytes over N ids. Ordinary ids are kept in order. Nothing is mutated; the
// lists come back untouched when no root repeats.
func dedupeHave(lists []*pb.ControlIDontWant) []*pb.ControlIDontWant {
	last := make(map[string]int)
	n, dup := 0, false
	for _, l := range lists {
		for _, mid := range l.GetMessageIDs() {
			if root, ok := seacastHaveRoot(mid); ok {
				if _, seen := last[root]; seen {
					dup = true
				}
				last[root] = n
			}
			n++
		}
	}
	if !dup {
		return lists
	}
	kept := make([]string, 0, n)
	i := 0
	for _, l := range lists {
		for _, mid := range l.GetMessageIDs() {
			if root, ok := seacastHaveRoot(mid); !ok || last[root] == i {
				kept = append(kept, mid)
			}
			i++
		}
	}
	return []*pb.ControlIDontWant{{MessageIDs: kept}}
}
