//go:build !quicfork

package segmentintegrationtest

// The no-fork half of applyInitialCWND (transportwarmup_test.go). Vanilla quic-go cannot set the
// initial congestion window from here, so asking for the sweep without the fork fails loudly --
// silently measuring the default window would be worse than not measuring.

import "testing"

func setInitialCWND(t *testing.T, packets uint32) {
	t.Fatalf("SEGMENT_INITIAL_CWND_PACKETS=%d needs the quic-go fork: add the local go.mod "+
		"replace and build with -tags quicfork (see quicwindow_fork_test.go)", packets)
}
