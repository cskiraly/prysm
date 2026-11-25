package p2p

import (
	"testing"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
)

// TestSeacastForkGuard compiles only against the SEACAST fork of go-libp2p-pubsub, vendored in
// third_party/go-libp2p-pubsub and selected by go.mod's replace. It names what the fork has and
// upstream lacks: SEACAST's option and the repair counter. It fails if the replace is lost.
func TestSeacastForkGuard(t *testing.T) {
	var _ func([]pubsub.ObjectType, *pubsub.SeacastCounters) pubsub.Option = pubsub.WithSeacast
	var st pubsub.OutboundStat
	if st.RepairFrames != 0 || st.RepairBytes != 0 {
		t.Fatal("a zero OutboundStat counts repair frames")
	}
}
