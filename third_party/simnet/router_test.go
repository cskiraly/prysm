package simnet

import (
	"net"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

func TestVariableLatencyRouterDelaysPackets(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const wantLatency = 25 * time.Millisecond
		receiver := newRouterRecordingReceiver(1)
		router, from, to := startVariableLatencyRouter(t, func(*Packet) time.Duration {
			return wantLatency
		}, receiver)

		sendTime := time.Now()
		router.RecvPacket(&Packet{From: from, To: to, buf: []byte{0x1}})

		delivery := receiver.waitFor(t)
		delay := delivery.arrival.Sub(sendTime)
		const slop = time.Millisecond
		if delay < wantLatency {
			t.Fatalf("packet delivered too early: got %v want at least %v", delay, wantLatency)
		}
		if delay > wantLatency+slop {
			t.Fatalf("packet delivered too late: got %v want no more than %v", delay, wantLatency+slop)
		}
	})
}

func TestVariableLatencyRouterAllowsReordering(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		receiver := newRouterRecordingReceiver(2)
		router, from, to := startVariableLatencyRouter(t, func(p *Packet) time.Duration {
			if len(p.buf) == 0 {
				return 0
			}
			if p.buf[0] == 1 {
				return 60 * time.Millisecond
			}
			return 5 * time.Millisecond
		}, receiver)

		router.RecvPacket(&Packet{From: from, To: to, buf: []byte{1}})
		time.Sleep(10 * time.Millisecond)
		router.RecvPacket(&Packet{From: from, To: to, buf: []byte{2}})

		first := receiver.waitFor(t)
		second := receiver.waitFor(t)
		if first.packet.buf[0] != 2 {
			t.Fatalf("expected packet 2 to arrive first, got %d", first.packet.buf[0])
		}
		if second.packet.buf[0] != 1 {
			t.Fatalf("expected packet 1 to arrive second, got %d", second.packet.buf[0])
		}
	})
}

func TestVariableLatencyRouterKeepsOrderWithEqualLatency(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const latency = 15 * time.Millisecond
		receiver := newRouterRecordingReceiver(2)
		router, from, to := startVariableLatencyRouter(t, func(*Packet) time.Duration {
			return latency
		}, receiver)

		router.RecvPacket(&Packet{From: from, To: to, buf: []byte{1}})
		router.RecvPacket(&Packet{From: from, To: to, buf: []byte{2}})

		first := receiver.waitFor(t)
		second := receiver.waitFor(t)
		if first.packet.buf[0] != 1 {
			t.Fatalf("expected packet 1 to arrive first, got %d", first.packet.buf[0])
		}
		if second.packet.buf[0] != 2 {
			t.Fatalf("expected packet 2 to arrive second, got %d", second.packet.buf[0])
		}
		if second.arrival.Before(first.arrival) {
			t.Fatalf("expected packets with same latency to preserve order, got first=%v second=%v", first.arrival, second.arrival)
		}
	})
}

type deliveredPacket struct {
	packet  *Packet
	arrival time.Time
}

type routerRecordingReceiver struct {
	deliveries chan deliveredPacket
}

func newRouterRecordingReceiver(buffer int) *routerRecordingReceiver {
	return &routerRecordingReceiver{
		deliveries: make(chan deliveredPacket, buffer),
	}
}

func (r *routerRecordingReceiver) RecvPacket(p *Packet) {
	r.deliveries <- deliveredPacket{
		packet:  p,
		arrival: time.Now(),
	}
}

func (r *routerRecordingReceiver) waitFor(t *testing.T) deliveredPacket {
	t.Helper()
	select {
	case delivery := <-r.deliveries:
		return delivery
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for packet delivery")
		return deliveredPacket{}
	}
}

func BenchmarkAddrMapGet(b *testing.B) {
	var m addrMap[int]
	addrs := []*net.UDPAddr{
		{IP: net.IPv4(10, 0, 0, 1), Port: 1234},
		{IP: net.IPv4(10, 0, 0, 2), Port: 5678},
		{IP: net.IPv4(192, 168, 1, 1), Port: 80},
		{IP: net.IPv4(203, 0, 113, 5), Port: 443},
	}
	for i, a := range addrs {
		m.Set(a, i)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		m.Get(addrs[i%len(addrs)])
	}
}

func BenchmarkAddrMapSet(b *testing.B) {
	var m addrMap[int]
	addrs := []*net.UDPAddr{
		{IP: net.IPv4(10, 0, 0, 1), Port: 1234},
		{IP: net.IPv4(10, 0, 0, 2), Port: 5678},
		{IP: net.IPv4(192, 168, 1, 1), Port: 80},
		{IP: net.IPv4(203, 0, 113, 5), Port: 443},
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		m.Set(addrs[i%len(addrs)], i)
	}
}

func BenchmarkIPPortKeyFromNetAddr(b *testing.B) {
	addr := &net.UDPAddr{IP: net.IPv4(10, 0, 0, 1), Port: 1234}
	var k ipPortKey

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		k.FromNetAddr(addr)
	}
	_ = k
}

func startVariableLatencyRouter(t *testing.T, latency func(*Packet) time.Duration, receiver PacketReceiver) (*VariableLatencyRouter, net.Addr, net.Addr) {
	t.Helper()
	router := &VariableLatencyRouter{
		LatencyFunc: latency,
		CloseSignal: make(chan struct{}),
	}

	from := &net.UDPAddr{IP: net.IPv4(203, 0, 113, 1), Port: 40000}
	to := &net.UDPAddr{IP: net.IPv4(203, 0, 113, 2), Port: 40001}
	router.AddNode(to, receiver)

	var wg sync.WaitGroup
	router.Start(&wg)
	t.Cleanup(func() {
		close(router.CloseSignal)
		wg.Wait()
	})

	return router, from, to
}
