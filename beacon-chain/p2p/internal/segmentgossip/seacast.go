package segmentgossip

import (
	"errors"
	"fmt"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
)

// The payload's push budget (seacast-lite.md §8's R and R_FAST): a forwarding node pushes each
// chunk to R peers of the mesh, and a node of the fast class, at FAST_UPLINK or more, to R_FAST.
const (
	pushBudget     = 2
	pushBudgetFast = 6
	fastUplinkMbps = 500
	projectFanIn   = 2 // FAN_IN
)

// PayloadParams is the payload type's parameters on SEACAST's path (seacast.md §6.1). The
// fork runs the payload's values of EAGER, REPAIR_TO (the push relation) and ANNOUNCE_EXTRA (the
// heartbeat's gossip) for every type, so they have no field here. No serve limit: the zero value,
// which condition 3 of CheckSetting requires. The decode at K depends on the group, so the type sets
// DecodeAtK (NewPayloadType).
func PayloadParams(uplinkMbps int) pubsub.TypeParams {
	r := pushBudget
	if uplinkMbps >= fastUplinkMbps {
		r = pushBudgetFast
	}
	return pubsub.TypeParams{
		RNode: r,
		// FAN_IN (seacast-lite.md §8): one other pusher assumed as fast as this node, so the projection
		// counts what was sent to a peer and not yet confirmed twice (§7.2). A push it skips is
		// offered instead (OfferSkipped, RouterOptions).
		ProjectFanIn: projectFanIn,
	}
}

// Setting is the node's SEACAST setting for the payload: what the router's options are built from,
// and what CheckSetting reads before the router is built.
type Setting struct {
	// SegmentTopics is the segment topic at every fork digest the node can run, from Gloas on.
	SegmentTopics []string
	// Allowed is the subscription allowlist's verdict on a topic.
	Allowed func(topic string) bool
	// TypeTopics and Params are the payload type's Topics and Params.
	TypeTopics []string
	Params     pubsub.TypeParams
	// Limits is what WithSeacastLimits installs.
	Limits pubsub.SeacastLimits
	// Cancel is WithHaveDispatch's cancel.
	Cancel bool
	// UplinkMbps is the node's configured uplink (--seacast-uplink-mbps).
	UplinkMbps int
}

// CheckSetting returns an error naming the first condition the setting breaks, nil if it keeps them all.
//
// The fork carried here has none of the bounds SEACAST added in v0.10.12. Leaving them out changes
// nothing an all-honest network does with the payload, but only under four conditions, found by
// reading every rule of v0.10.12 against the payload type:
//  1. every link has joined the payload type: the segment topic of every digest is allowed and is the
//     type's (the sync service subscribes to it whenever segments are on);
//  2. HAVE dispatch cancels, so that a bit a peer repeats never stands behind a copy written to it;
//  3. no serve or relation bounds and no limit on a peer's unknown groups;
//  4. no sampler and no routed requests on the payload.
//
// Outside them the fork's behaviour was not read against v0.10.12, so a node whose setting breaks
// one doesn't start.
func CheckSetting(s Setting) error {
	if s.UplinkMbps <= 0 {
		return fmt.Errorf("seacast: --seacast-uplink-mbps is %d; the window and the push budget need the node's uplink", s.UplinkMbps)
	}
	if len(s.SegmentTopics) == 0 {
		return errors.New("seacast condition 1 (every link joined): no segment topic in the fork schedule")
	}
	typeTopics := make(map[string]bool, len(s.TypeTopics))
	for _, t := range s.TypeTopics {
		typeTopics[t] = true
	}
	for _, t := range s.SegmentTopics {
		if s.Allowed == nil || !s.Allowed(t) {
			return fmt.Errorf("seacast condition 1 (every link joined): the subscription allowlist refuses %s", t)
		}
		if !typeTopics[t] {
			return fmt.Errorf("seacast condition 1 (every link joined): the payload type does not own %s", t)
		}
	}
	if !s.Cancel {
		return errors.New("seacast condition 2 (cancel on): HAVE dispatch does not cancel copies the peer holds")
	}
	// SERVE_BUDGET and MAX_RELATIONS have no setting in this fork, so for them condition 3 holds by
	// their absence. ServeLimit is SERVE_LIMIT, per peer and group, a different bound from
	// SERVE_BUDGET's per heartbeat; the payload sets none.
	if s.Params.ServeLimit != 0 {
		return fmt.Errorf("seacast condition 3 (no serve or relation bounds): SERVE_LIMIT is %d per peer and group; the payload sets none", s.Params.ServeLimit)
	}
	if s.Limits.UnknownGroupsPerPeer != 0 {
		return fmt.Errorf("seacast condition 3 (no serve or relation bounds): MAX_PENDING_GROUPS is %d; the payload sets none", s.Limits.UnknownGroupsPerPeer)
	}
	// The fork carried here has no sampler and no repair sample, passes no routed request on, and
	// takes a WANT with hops as malformed, so condition 4 holds by their absence.
	return nil
}
