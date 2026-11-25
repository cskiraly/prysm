package p2p

import (
	"fmt"

	"github.com/OffchainLabs/prysm/v7/config/params"
	"github.com/OffchainLabs/prysm/v7/time/slots"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/sirupsen/logrus"
)

// SegmentGroupSeen tells the payload's path the shape of a group and the topic its segments travel
// on, once a segment of it passed validation or this node is about to publish it. The first time
// while the path's record of the group stands, the group is logged with its key (seacast-lite.md §1's
// group_key, which every chunk of the group carries; neither the segments' root nor the block's),
// the slot the path keyed it by, its threshold and its count, and then the
// router learns that the group's count is known, and drops what named an index beyond it. The line
// comes first because the router's hand-off waits until the router's loop takes it, so the line's
// time is the noting's. At the publisher, the line comes before its segments are queued. A no-op
// while segmented payload gossip is off.
func (s *Service) SegmentGroupSeen(group [32]byte, topic string, count, threshold int) {
	if s.segmentPayloadType == nil {
		return
	}
	slot, isNew := s.segmentPayloadType.NoteGroupSlot(string(group[:]), topic, count, threshold)
	if !isNew {
		return
	}
	log.WithFields(logrus.Fields{
		"group": fmt.Sprintf("%#x", group),
		"slot":  slot,
		"k":     threshold,
		"n":     count,
		"topic": topic,
	}).Info("Payload segment group noted")
	s.pubsub.SeacastHeaderValidated([]string{string(group[:])})
}

// SegmentGroupCommitted hands the payload's path the bytes a coded group's root commits to, once the
// node's reassembler recovered them, so that the router's decode of the group cuts them again for the
// missing segments without recovering them a second time. The path keeps the slice until the decode
// takes it or the live window passes the group, so the caller must not write to it. A no-op while
// segmented payload gossip is off.
func (s *Service) SegmentGroupCommitted(group [32]byte, committed []byte) {
	if s.segmentPayloadType == nil {
		return
	}
	s.segmentPayloadType.NoteCommitted(string(group[:]), committed)
}

// seacastWindow moves SEACAST's live window with the beacon slot, from now and then at the start of
// every slot, until the service stops. A no-op while segmented payload gossip is off.
func (s *Service) seacastWindow() {
	if s.segmentPayloadType == nil {
		return
	}
	s.seacastAdvance(uint64(slots.CurrentSlot(s.genesisTime)))
	ticker := slots.NewSlotTicker(s.genesisTime, params.BeaconConfig().SlotDuration())
	defer ticker.Done()
	for {
		select {
		case slot := <-ticker.C():
			s.seacastAdvance(uint64(slot))
		case <-s.ctx.Done():
			return
		}
	}
}

// seacastAdvance tells the router the node's slot, and has the payload type drop the segments of the
// groups whose slot the live window has passed. The type keeps such a group's record, which places
// its root in a slot, for RecordSlots slots after the group's slot. The type advances on the router's
// goroutine right after the router, in the same router event, so the router's expiry reads every
// record it needs, and every later router event sees both clocks moved. A group a validator or the
// publisher notes between the two (NoteGroup runs on their goroutines) takes the type's earlier slot:
// after an advance by one slot, it leaves the window one slot early. Before the type's first advance,
// NoteGroup takes the clock's slot instead.
func (s *Service) seacastAdvance(slot uint64) {
	s.pubsub.SeacastAdvanceSlotWith(slot, func(slot uint64) {
		s.segmentPayloadType.Advance(slot, pubsub.DefaultSeacastLimits.LiveWindow)
	})
}
