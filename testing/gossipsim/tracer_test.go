package gossipsim

import (
	"testing"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pubsub_pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
)

// A duplicate arrival is an event with a sender and an instant, like a first delivery, so that
// a horizon can read how far behind the first copy it landed. The counter and the event log
// move together and reset together.
func TestRecordingTracerDuplicatesAreEvents(t *testing.T) {
	r := NewRecordingTracer()
	from := peer.ID("sender")
	msg := &pubsub.Message{Message: &pubsub_pb.Message{Data: []byte("frame")}, ReceivedFrom: from}
	r.DeliverMessage(msg)
	r.DuplicateMessage(msg)
	r.DuplicateMessage(msg)
	d, _, _, _ := r.Counts()
	if d != 2 || len(r.Dups) != 2 {
		t.Fatalf("counter %d, events %d; want 2 and 2", d, len(r.Dups))
	}
	if r.Dups[0].From != from || r.Dups[0].What != DigestOf(msg.Data) || r.Dups[0].At.Before(r.Recvs[0].At) {
		t.Fatalf("event %+v does not describe a later copy of %+v", r.Dups[0], r.Recvs[0])
	}
	r.ResetCounters()
	d, _, _, _ = r.Counts()
	if d != 0 || r.Dups != nil {
		t.Fatalf("after reset: counter %d, events %v", d, r.Dups)
	}
}
