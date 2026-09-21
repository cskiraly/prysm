package segmentintegrationtest

// The wire form of a cell: the part of an arm's identity its name can hide. A policy name such as
// "A-tuned" says which rules run; how many messages there are, how many complete a node, the unit
// they were cut at and the width of their ids are what the bytes and the control plane follow, and
// two cells that differ in any of them are not a pair. Every variant reports its wire form on the
// in-process driver's log and in the Shadow node's record, and the comparison pairs on it.

import (
	"fmt"
	"os"

	pubsubpb "github.com/libp2p/go-libp2p-pubsub/pb"
)

type wireForm struct {
	msgs    int // messages on the wire (K, or K+parity for a coded group)
	need    int // distinct messages that complete a node
	unit    int // the segment size the payload was cut at; 0 for the whole message
	total   int // encoded bytes on the wire, all messages
	idBytes int // the message id width the nodes compute; 0 where ids do not apply
}

func (w wireForm) String() string {
	return fmt.Sprintf("wire: msgs=%d need=%d unit=%d total=%d id=%d", w.msgs, w.need, w.unit, w.total, w.idBytes)
}

// wireFormer is implemented by the variants whose wire form the driver reports.
type wireFormer interface {
	wireForm() wireForm
}

// idWidth is the byte length of the message id the cell's nodes compute for one of the arm's
// messages: Prysm's 20-byte id, or the structured id when SEGMENT_STRUCTURED_IDS is on, through
// the same function the nodes install.
func idWidth(data []byte) int {
	var genesisValidatorsRoot [32]byte
	topic := segmentTopic()
	return len(msgIDFn(genesisValidatorsRoot[:])(&pubsubpb.Message{Data: data, Topic: &topic}))
}

// strictCells reports whether SEGMENT_STRICT is set: a measurement run, where a wire-affecting
// knob left to a code default is a mistake rather than a convenience, so segmentSizeBytes fails
// instead of defaulting. Tests and exploratory runs leave it unset.
func strictCells() bool { return os.Getenv("SEGMENT_STRICT") != "" }
