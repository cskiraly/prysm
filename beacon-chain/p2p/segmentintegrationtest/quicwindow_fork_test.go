//go:build quicfork

package segmentintegrationtest

// The fork half of applyInitialCWND (transportwarmup_test.go). Compiles only against the local
// quic-go fork, which adds SetDefaultInitialCongestionWindow -- see the LOCAL ONLY commit there.
// Run with:
//
//	go mod edit -replace github.com/quic-go/quic-go=/path/to/quic-go-fork
//	go test -tags quicfork ...
//
// The replace stays out of the committed go.mod on purpose: the fork's setter is not
// upstreamable (it mutates a package-level default), so the committed tree must not depend on it.

import (
	"testing"

	"github.com/quic-go/quic-go"
)

func setInitialCWND(t *testing.T, packets uint32) {
	quic.SetDefaultInitialCongestionWindow(packets)
	t.Cleanup(func() { quic.SetDefaultInitialCongestionWindow(0) })
}
