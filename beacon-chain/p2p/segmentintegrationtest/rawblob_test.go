package segmentintegrationtest

import (
	"bytes"
	"testing"

	"github.com/OffchainLabs/prysm/v7/beacon-chain/p2p/encoder"
	ethpb "github.com/OffchainLabs/prysm/v7/proto/prysm/v1alpha1"
	"github.com/OffchainLabs/prysm/v7/testing/require"
)

// rawBlob must stay wire-identical to the container it replaces at sizes the container
// still accepts, or the sweep's 1 MiB point is not comparable to prior runs.
func TestRawBlobWireIdentical(t *testing.T) {
	for _, n := range []int{1, 1024, 262144, 1 << 20} {
		payload := mainnetLikePayload(n, 11)
		var oldBuf, newBuf bytes.Buffer
		_, err := encoder.SszNetworkEncoder{}.EncodeGossip(&oldBuf, &ethpb.ExecutionPayloadSegment{Segment: payload})
		require.NoError(t, err)
		_, err = encoder.SszNetworkEncoder{}.EncodeGossip(&newBuf, rawBlob(payload))
		require.NoError(t, err)
		require.DeepEqual(t, oldBuf.Bytes(), newBuf.Bytes())
	}
}
