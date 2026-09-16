package segmentintegrationtest

// Wire-size probe: what one segment of each arm costs on the wire, so that byte figures
// normalised to "payload equivalents" can be read with the right denominator.
//
// Every arm's rx/node figure in the notes is divided by the compressed wire size of one
// plain segmented copy (~746 KB for the 1 MiB mainnet-like payload). That is exact for the
// plain arm and only approximate for a coded one: the systematic half compresses like data,
// the parity half is high-entropy and does not. This test prints both so the notes can say
// how far apart they are.

import (
	"os"
	"slices"
	"strconv"
	"testing"

	"github.com/OffchainLabs/prysm/v7/consensus-types/primitives"
	"github.com/OffchainLabs/prysm/v7/crypto/bls"
	"github.com/OffchainLabs/prysm/v7/testing/require"
)

// TestWireArmSizes logs the per-segment wire sizes of the plain and coded arms. Run with -v.
func TestWireArmSizes(t *testing.T) {
	payloadLen := 1 << 20
	if v := os.Getenv("SEGMENT_PAYLOAD_BYTES"); v != "" {
		var err error
		payloadLen, err = strconv.Atoi(v)
		require.NoError(t, err)
	}
	sk, err := bls.RandKey()
	require.NoError(t, err)
	payload := mainnetLikePayload(payloadLen, 11)
	segSize := segmentSizeBytes(t)
	k := (payloadLen + segSize - 1) / segSize
	slot := primitives.Slot(2048)

	report := func(name string, arm wireArm) {
		sizes := make([]int, len(arm.msgs))
		for i, m := range arm.msgs {
			sizes[i] = len(m)
		}
		sys, par := sizes[:min(k, len(sizes))], sizes[min(k, len(sizes)):]
		t.Logf("%-22s msgs %3d total %8d B max %6d B  systematic mean %6d B [%d..%d]  parity mean %6d B [%d..%d]  raw payload %d B (%.3f of raw)",
			name, len(arm.msgs), arm.total, arm.max,
			mean(sys), minOf(sys), maxOf(sys), mean(par), minOf(par), maxOf(par),
			payloadLen, float64(arm.total)/float64(payloadLen))
	}
	plain := segmentedArm(t, payload, segSize, sk, slot)
	report(plain.name, plain)
	coded := codedArm(t, payload, segSize, k, sk, slot)
	report(coded.name, coded)

	// The variant D harness (removed 2026-09-07, now C's custody knob) published
	// segments.SegmentMessage.Marshal() bytes directly, with neither the gossip envelope nor
	// snappy, so every D figure before that date sits on this uncompressed basis. Kept so the
	// two bases can be read side by side.
	msgs := segmentMessagesFor(t, sk, slot, payload, segSize, k)
	var raw wireArm
	raw.name = "d-harness/raw"
	for _, m := range msgs {
		b, err := m.Marshal()
		require.NoError(t, err)
		raw.msgs = append(raw.msgs, b)
		raw.total += len(b)
		raw.max = max(raw.max, len(b))
	}
	report(raw.name, raw)
}

func mean(xs []int) int {
	if len(xs) == 0 {
		return 0
	}
	s := 0
	for _, x := range xs {
		s += x
	}
	return s / len(xs)
}

func minOf(xs []int) int {
	if len(xs) == 0 {
		return 0
	}
	return slices.Min(xs)
}

func maxOf(xs []int) int {
	if len(xs) == 0 {
		return 0
	}
	return slices.Max(xs)
}
