package segmentauth

import (
	"testing"

	"github.com/OffchainLabs/prysm/v7/consensus-types/primitives"
	"github.com/OffchainLabs/prysm/v7/container/segments"
)

// BenchmarkAuthenticateDescriptor measures what a peer can make us spend to open a group.
//
// Under the detached-signature scheme this was a BLS verification at ~0.70 ms, which was the
// entire justification for charging a per-peer budget for opening a group rather than for
// every segment. A membership test replaces it, so the budget argument weakens by whatever
// ratio this shows; keep both numbers in the same run so the change is visible rather than
// asserted.
func BenchmarkAuthenticateDescriptor(b *testing.B) {
	const slot = primitives.Slot(1000)
	h, err := segments.HasherByID(segments.HashSHA256)
	if err != nil {
		b.Fatal(err)
	}
	d, _, err := segments.Commit(make([]byte, 1<<20), segments.DefaultSegmentSize, h)
	if err != nil {
		b.Fatal(err)
	}
	groupID := d.GroupID(h)
	a := New(func(g []byte) bool { return string(g) == string(groupID) })

	b.Run("matching commitment", func(b *testing.B) {
		for b.Loop() {
			if err := a.AuthenticateDescriptor(d, groupID); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("first seen pins", func(b *testing.B) {
		f := NewFirstSeen(func() primitives.Slot { return slot })
		for b.Loop() {
			if err := f.AuthenticateDescriptor(d, groupID); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkSegmentVerifyForContrast measures the per-segment cost, so the ratio against
// group authentication is visible in one benchmark run.
func BenchmarkSegmentVerifyForContrast(b *testing.B) {
	h, err := segments.HasherByID(segments.HashSHA256)
	if err != nil {
		b.Fatal(err)
	}
	msgs, err := segments.BuildSegmentMessages(make([]byte, 1<<20), segments.DefaultSegmentSize, h)
	if err != nil {
		b.Fatal(err)
	}
	m := msgs[0]
	for b.Loop() {
		if err := m.Verify(h); err != nil {
			b.Fatal(err)
		}
	}
}
