package segmentintegrationtest

// Heartbeat phase and paced gossip (Q131). Every node's heartbeat ticker used to start at the
// same virtual instant, so the whole network gossiped in lockstep and a publish's phase in the
// cycle was the same everywhere; with messages 12 s apart that phase drifted 100 ms per message
// and the publisher's heartbeat gossip caught more receivers mid-diffusion every time. Real nodes'
// phases are independent, so each node now starts at its own.
//
//	SEGMENT_HEARTBEAT_PHASE=random   (default) each node's first heartbeat at a phase in
//	                                 [0, heartbeat), drawn from the seed and the node index
//	SEGMENT_HEARTBEAT_PHASE=lockstep every node at the library's initial delay, as every cell
//	                                 before seg-d25 ran
//	SEGMENT_GOSSIP_PACE=<N>          heartbeat gossip released one IHAVE per heartbeat/N tick
//	SEGMENT_GOSSIP_MIN_AGE_MS (0)    ids younger than this are not gossiped yet
//	SEGMENT_GOSSIP_PACE_HOSE=1       a paced IHAVE leaves only while the hose window has a free
//	                                 slot (needs SEGMENT_HOSE=on to bind)

import (
	"os"
	"strconv"
	"testing"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
)

func gossipPaceOpts(t *testing.T, i int, seed uint64) []pubsub.Option {
	t.Helper()
	var opts []pubsub.Option
	hb := meshParams().HeartbeatInterval
	switch v := os.Getenv("SEGMENT_HEARTBEAT_PHASE"); v {
	case "lockstep":
	case "", "random":
		// splitmix64 over (seed, node): a phase per node, the same in every arm of a seed.
		z := seed*0x9E3779B97F4A7C15 + uint64(i+1)*0xBF58476D1CE4E5B9 // lint:ignore uintcast -- a node index.
		z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9
		z = (z ^ (z >> 27)) * 0x94D049BB133111EB
		z ^= z >> 31
		opts = append(opts, pubsub.WithHeartbeatInitialDelay(time.Duration(z%uint64(hb)))) // lint:ignore uintcast -- a positive interval.
	default:
		t.Fatalf("bad SEGMENT_HEARTBEAT_PHASE %q (want random or lockstep)", v)
	}
	if v := os.Getenv("SEGMENT_GOSSIP_PACE"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			t.Fatalf("bad SEGMENT_GOSSIP_PACE %q (want ticks per heartbeat >= 1)", v)
		}
		opts = append(opts, pubsub.WithPacedGossip(hb/time.Duration(n), envDuration("SEGMENT_GOSSIP_MIN_AGE_MS", 0)))
	}
	if os.Getenv("SEGMENT_GOSSIP_PACE_HOSE") != "" {
		if os.Getenv("SEGMENT_GOSSIP_PACE") == "" || os.Getenv("SEGMENT_HOSE") == "" {
			t.Fatal("SEGMENT_GOSSIP_PACE_HOSE needs SEGMENT_GOSSIP_PACE and SEGMENT_HOSE")
		}
		opts = append(opts, pubsub.WithPacedGossipHose())
	}
	return opts
}

// pacedGossipLine sums the nodes' paced-gossip counters; empty when gossip is not paced.
func pacedGossipLine(nw *simNetwork) string {
	var s pubsub.PacedGossipStat
	on := false
	for _, ps := range nw.Pubsubs {
		if x, ok := ps.PacedGossipStats(); ok {
			on = true
			s.Released += x.Released
			s.Dropped += x.Dropped
			s.Deferred += x.Deferred
			s.Emptied += x.Emptied
			s.Held += x.Held
		}
	}
	if !on {
		return ""
	}
	return "paced gossip (cell so far): released " + strconv.FormatInt(s.Released, 10) + ", dropped stale " + strconv.FormatInt(s.Dropped, 10) +
		", deferred young " + strconv.FormatInt(s.Deferred, 10) + ", nothing left to offer " + strconv.FormatInt(s.Emptied, 10) + ", ticks held by the hose window " + strconv.FormatInt(s.Held, 10)
}
