package p2p

// SegmentGroupComplete tells the segment topic's pull gate that this node has reassembled
// the group committed to by root, so its remaining segments are no longer requested. A
// no-op while segmented payload gossip is off.
func (s *Service) SegmentGroupComplete(root [32]byte) {
	if s.segmentPullGate != nil {
		s.segmentPullGate.MarkComplete(root)
	}
}
