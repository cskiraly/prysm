package segmentintegrationtest

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/OffchainLabs/prysm/v7/container/segments"
	pubsubpb "github.com/libp2p/go-libp2p-pubsub/pb"
)

// Per-segment diffusion curves, on both substrates.
//
// A cell's record carries one completion time per node, which is the time its last needed
// segment arrived. That answers how long the payload took and nothing about how it got there:
// whether every segment spread alike, whether a node knew of a segment long before it held it,
// or how the two differ between a tight feedback loop and a loose one. The curves here are the
// raw material for those questions -- per node, per segment, when it was first heard of and
// when it was first held -- from which the diffusion of one segment, and "how many nodes hold
// at least l segments by t", are both arithmetic.
//
// Off unless SEGMENT_DIFFUSION is set. A cell that does not ask for them records nothing extra
// and does no extra work on the announcement path, so its scheduling is what it was before.

// diffusionCurves reports whether this cell records per-segment curves.
func diffusionCurves() bool { return os.Getenv("SEGMENT_DIFFUSION") != "" }

// diffusionOut is where the in-process harness writes its matrix; empty means do not write.
// Under Shadow each node carries its own rows out in its report line instead.
func diffusionOut() string { return os.Getenv("SEGMENT_DIFFUSION_OUT") }

// segmentIDs is the router's message id for each of the arm's messages, in index order.
//
// Every node builds the arm deterministically, so it can label an announcement by segment index
// without asking anyone: the id of a body it has not yet received is still computable. The id
// function and the zero genesis root are the ones structuredIDOpts installs, or the run's ids
// would not be the ids computed here.
func segmentIDs(arm wireArm) []string {
	var genesisValidatorsRoot [32]byte
	topic := segmentTopic()
	idOf := msgIDFn(genesisValidatorsRoot[:])
	out := make([]string, len(arm.msgs))
	for i, data := range arm.msgs {
		out[i] = idOf(&pubsubpb.Message{Data: data, Topic: &topic})
	}
	return out
}

// segmentDiffusion is one node's two curves, in publish-relative seconds, with -1 for a segment
// the node never saw.
//
// got is the delivery, taken from the tracer's event log rather than from the completion
// watcher: the watcher stops reading at the arm's threshold, so a coded arm's later segments
// would be missing from a curve built there, and a node that never completes would have no
// curve at all. heard is the earliest of the delivery and any inbound control message naming
// the segment's id -- an IHAVE offering it, an IWANT asking for it, an IDONTWANT saying it is
// already held elsewhere. It is never later than got.
//
// ids is the key list, and it is what makes this basis-agnostic: gossip message ids for a
// variant whose segments ride in rpc.Publish, partialSegKeys for one whose segments ride in
// rpc.Partial. Both sources are consulted with it; the one the variant does not use is empty.
func segmentDiffusion(tr *recordingTracer, arm wireArm, ids []string, publishAt time.Time) (got, heard []float64) {
	m := len(arm.msgs)
	if m == 0 {
		// A partial-path variant has no encoded gossip bodies to count; its width is the key list.
		m = len(ids)
	}
	if m == 0 {
		return nil, nil
	}
	got = make([]float64, m)
	heard = make([]float64, m)
	for i := range got {
		got[i], heard[i] = -1, -1
	}
	for d, at := range tr.FirstDelivery() {
		i, ok := arm.known[d]
		if !ok || i < 0 || i >= m {
			continue
		}
		got[i] = at.Sub(publishAt).Seconds()
		heard[i] = got[i]
	}
	partialGot := tr.FirstPartialGot()
	for i, id := range ids {
		if i >= m {
			break
		}
		at, ok := partialGot[id]
		if !ok {
			continue
		}
		if s := at.Sub(publishAt).Seconds(); got[i] < 0 || s < got[i] {
			got[i] = s
			if heard[i] < 0 || s < heard[i] {
				heard[i] = s
			}
		}
	}
	announced := tr.FirstAnnounce()
	for i, id := range ids {
		if i >= m {
			break
		}
		at, ok := announced[id]
		if !ok {
			continue
		}
		if s := at.Sub(publishAt).Seconds(); heard[i] < 0 || s < heard[i] {
			heard[i] = s
		}
	}
	return got, heard
}

// diffusionMatrix is a whole cell's curves, one row per node, as the analysis reads them. Row 0
// is the publisher, which holds every segment from the start.
type diffusionMatrix struct {
	Arm      string `json:"arm"`
	N        int    `json:"n"`
	Segments int    `json:"segments"`
	Need     int    `json:"need"`
	// Publisher is the node that published, whose row is not a diffusion.
	Publisher int         `json:"publisher"`
	FirstGot  [][]float64 `json:"first_got_s"`
	Heard     [][]float64 `json:"first_heard_s"`
}

// writeDiffusionMatrix writes the cell's curves to path.
func writeDiffusionMatrix(path string, mx diffusionMatrix) error {
	b, err := json.Marshal(mx)
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0o600)
}

// The partial-message path is invisible to everything above.
//
// segmentIDs keys on the router's message id, and FirstDelivery on the digest of a delivered
// message body. A segment that travels in rpc.Partial has neither: it carries no message id, and
// it is reassembled by the extension rather than delivered to the application, so
// DeliverMessage never fires for it. A curve built from those two sources alone reports, for
// every bitmap variant, that nobody received anything and nobody heard anything -- which reads
// as a triumph and is an artefact. What follows gives the partial path its own key and its own
// decoder.

// partialSegKey identifies one segment of one group where there is no message id to key on.
func partialSegKey(groupID []byte, index int) string {
	return fmt.Sprintf("%x#%d", groupID, index)
}

// partialSegKeys is the key list for one group's segments, in index order -- the partial path's
// segmentIDs, and what segmentDiffusion reads the curves back with.
func partialSegKeys(groupID []byte, count int) []string {
	out := make([]string, count)
	for i := range out {
		out[i] = partialSegKey(groupID, i)
	}
	return out
}

// partialSegmentObserver decodes an inbound partial frame into the segments it carried and the
// segments its bitmap says the sender holds.
//
// Only the have bitmap counts as heard. A want bitmap is not evidence that a segment exists: a
// standing want defaults to everything on first contact, so counting want bits would mark every
// segment heard the moment a peer connects -- the same artefact as counting no bitmaps at all,
// with the sign reversed.
//
// Installed only when SEGMENT_DIFFUSION is set. It unmarshals every inbound partial frame, which
// is real work at every node; under synctest that is wall time and not one tick of virtual time,
// so the cell's numbers are unchanged, but a cell that has not asked for curves must not pay it.
// Decode failures are ignored rather than fatal: this is an observer, and a frame the node itself
// will reject must not fail the cell here first.
func partialSegmentObserver() partialObserveFn {
	return func(_ string, groupID, partialMessage, partsMetadata []byte) (got, heard []string) {
		if len(partialMessage) > 0 {
			if msgs, _, err := segments.UnmarshalPartialMessage(partialMessage); err == nil {
				for _, m := range msgs {
					got = append(got, partialSegKey(groupID, int(m.Index))) // lint:ignore uintcast -- bounded by MaxSegments.
				}
			}
		}
		if len(partsMetadata) > 0 {
			if meta, err := segments.UnmarshalPartsMetadata(partsMetadata); err == nil {
				for _, i := range meta.Available.Indices() {
					heard = append(heard, partialSegKey(groupID, int(i))) // lint:ignore uintcast -- bounded by MaxSegments.
				}
			}
		}
		return got, heard
	}
}
