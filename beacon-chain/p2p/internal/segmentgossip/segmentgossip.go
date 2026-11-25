package segmentgossip

// MaxIHaveMessages replaces gossipsub's per-heartbeat IHAVE flood limit while segments are
// on: SEACAST's MAX_IHAVE_MESSAGES (4096 frames a heartbeat; its MAX_IHAVE_LENGTH, 5000 ids, is
// gossipsub's default already). The segment path offers what it holds in many small frames,
// against a stock limit of 10 sized for heartbeat gossip.
//
// The parameter is router-wide, and under SEACAST it counts a peer's offer-bearing RPCs on every
// topic; over it, the asks are held back to the next heartbeat rather than the IHAVE ignored. So
// while the feature is on, ordinary topics accept far more IHAVEs than stock does. A per-topic
// limit in the fork is open work.
const MaxIHaveMessages = 4096
