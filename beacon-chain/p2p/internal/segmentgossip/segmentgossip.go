package segmentgossip

import (
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
)

// The push/pull policy the execution payload segment topic runs under.
//
// Stock forwarding pushes every message to every mesh peer, so a node receives the payload
// several times over and the repeats, not the first copies, bound completion. On the segment
// topic each node instead pushes a segment to a few mesh peers not known to hold it, as many
// as the payload's size warrants (see PushWidth), and announces it to the rest, and pulls are
// disciplined: one outstanding request per segment, a short move-on window, a remembered list
// of announcers to fall back to, and a park for announcers that do not serve. Measured on a
// 500-node simulated mesh with 1 MiB payloads this cut median completion from 4.9 s to 0.73 s
// and received bytes from 4.4 to 1.4 payload copies per node. Every other topic keeps stock
// forwarding and stock requests; the one router-wide knob touched is the IHAVE limit, see
// MaxIHaveMessages.

const (
	// iwantWindow is how long one outstanding request for a segment blocks another; a round
	// trip plus the segment's transmission at home-link rates.
	iwantWindow = 200 * time.Millisecond
	// promiseDeadline is how long an announcer has to serve a request before the request
	// counts as broken.
	promiseDeadline = 400 * time.Millisecond
	// parkBreaks is how many broken promises park an announcer; one strike.
	parkBreaks = 1
	// parkTTL is how long a parked announcer's segment announcements are ignored.
	parkTTL = 30 * time.Second
)

// MaxIHaveMessages replaces gossipsub's per-heartbeat IHAVE flood limit while segments are
// on. Phase forwarding announces each forwarded segment in its own IHAVE, so a peer may
// legitimately send one per segment per heartbeat: a 4 MiB payload in 32 KiB segments is 128,
// against a stock limit of 10 sized for heartbeat gossip. The parameter is router-wide, so the
// raise applies to every topic while the feature is on; a per-topic limit in the fork is open
// work.
const MaxIHaveMessages = 1024

// Options returns the pubsub options that install the policy on the topic named topicName.
// Order matters: the park and the offer table require the discipline to be installed first.
func Options(topicName string) []pubsub.Option {
	isSegmentTopic := TopicMatcher(topicName)
	return []pubsub.Option{
		pubsub.WithPhaseForwardingByMessage(isSegmentTopic, PushWidthOf),
		pubsub.WithIWantDiscipline(isSegmentTopic, iwantWindow),
		pubsub.WithIHaveCommitmentPark(parkBreaks, promiseDeadline, parkTTL),
		pubsub.WithOfferTable(),
	}
}
