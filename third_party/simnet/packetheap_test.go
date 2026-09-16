package simnet

import (
	"container/heap"
	"testing"
	"time"
)

// stdlibPacketHeap wraps packetHeap to implement heap.Interface,
// used as the baseline to measure how much the direct implementation saves.
type stdlibPacketHeap []packetWithDeliveryTimeAndOrder

func (h stdlibPacketHeap) Len() int { return len(h) }
func (h stdlibPacketHeap) Less(i, j int) bool {
	return (h[i].deliveryTime.Before(h[j].deliveryTime) ||
		h[i].deliveryTime.Equal(h[j].deliveryTime) && h[i].order < h[j].order)
}
func (h stdlibPacketHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *stdlibPacketHeap) Push(x any)   { *h = append(*h, x.(packetWithDeliveryTimeAndOrder)) }
func (h *stdlibPacketHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

func makePacket(order int, deliveryTime time.Time) packetWithDeliveryTimeAndOrder {
	return packetWithDeliveryTimeAndOrder{
		Packet:       &Packet{buf: []byte{byte(order)}},
		order:        order,
		deliveryTime: deliveryTime,
	}
}

func BenchmarkHeapPushPop(b *testing.B) {
	// Pre-generate delivery times to avoid measuring time.Now in the loop.
	base := time.Now()
	const batchSize = 100

	b.Run("container/heap", func(b *testing.B) {
		b.ReportAllocs()
		var h stdlibPacketHeap
		for b.Loop() {
			// Push a batch then pop them all, simulating real usage.
			for j := range batchSize {
				heap.Push(&h, makePacket(j, base.Add(time.Duration(batchSize-j)*time.Millisecond)))
			}
			for h.Len() > 0 {
				heap.Pop(&h)
			}
		}
	})

	b.Run("direct", func(b *testing.B) {
		b.ReportAllocs()
		var h packetHeap
		for b.Loop() {
			for j := range batchSize {
				h.push(makePacket(j, base.Add(time.Duration(batchSize-j)*time.Millisecond)))
			}
			for len(h) > 0 {
				h.pop()
			}
		}
	})
}
