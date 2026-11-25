package pubsub

import (
	"crypto/sha256"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"
)

func cacheLen(m *msgIDGenerator) int {
	m.idsLk.Lock()
	defer m.idsLk.Unlock()
	return len(m.ids)
}

// TestMsgIDCache: the generator computes a message object's id once and returns it again,
// equal to the function's own; a copy of a message is computed again; a topic's new function
// clears the cache; and once the messages are collected the cache is empty, so it does not grow.
func TestMsgIDCache(t *testing.T) {
	var calls atomic.Int64
	gen := newMsgIdGenerator()
	gen.Default = func(m *pb.Message) string {
		calls.Add(1)
		h := sha256.Sum256(m.Data)
		return string(h[:])
	}
	want := func(m *pb.Message) string { h := sha256.Sum256(m.Data); return string(h[:]) }

	const n = 1000
	msgs := make([]*pb.Message, n)
	for i := range msgs {
		topic := "t"
		msgs[i] = &pb.Message{Data: []byte(fmt.Sprintf("message %d", i)), Topic: &topic}
	}
	for _, m := range msgs {
		for range 3 {
			if got := gen.RawID(m); got != want(m) {
				t.Fatalf("RawID %x, want %x", got, want(m))
			}
		}
		if got := gen.ID(&Message{Message: m}); got != want(m) {
			t.Fatalf("ID %x, want %x", got, want(m))
		}
	}
	if c := calls.Load(); c != n {
		t.Fatalf("the id function ran %d times on %d messages, want %d", c, n, n)
	}
	if l := cacheLen(gen); l != n {
		t.Fatalf("the cache holds %d, want %d", l, n)
	}

	cp := &pb.Message{Data: msgs[0].Data, Topic: msgs[0].Topic} // the same message, another object
	if got := gen.RawID(cp); got != want(msgs[0]) {
		t.Fatalf("the copy's id %x, want %x", got, want(msgs[0]))
	}
	if c := calls.Load(); c != n+1 {
		t.Fatalf("a copy was not computed again: %d calls, want %d", c, n+1)
	}

	gen.Set("other", gen.Default)
	if l := cacheLen(gen); l != 0 {
		t.Fatalf("Set left %d ids cached", l)
	}
	for _, m := range msgs {
		gen.RawID(m)
	}
	if l := cacheLen(gen); l != n {
		t.Fatalf("the cache holds %d after Set and a second pass, want %d", l, n)
	}

	msgs = nil
	for i := 0; i < 50 && cacheLen(gen) > 0; i++ {
		runtime.GC()
		time.Sleep(10 * time.Millisecond) // the cleanups run on their own goroutine
	}
	if l := cacheLen(gen); l != 0 {
		t.Fatalf("the cache holds %d ids after the messages were collected", l)
	}
}

// TestMsgIDCacheConcurrent: goroutines asking at once for the same messages
// compute each id once, and a message whose id is being computed does not hold up another's: the
// first message's id function waits until the second message's id has been returned.
func TestMsgIDCacheConcurrent(t *testing.T) {
	var calls atomic.Int64
	gen := newMsgIdGenerator()
	gen.Default = func(m *pb.Message) string {
		calls.Add(1)
		h := sha256.Sum256(m.Data)
		return string(h[:])
	}
	const n, workers = 200, 8
	msgs := make([]*pb.Message, n)
	for i := range msgs {
		msgs[i] = &pb.Message{Data: []byte(fmt.Sprintf("message %d", i))}
	}
	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			for _, m := range msgs {
				gen.RawID(m)
			}
		})
	}
	wg.Wait()
	if c := calls.Load(); c != n {
		t.Fatalf("%d goroutines on %d messages ran the id function %d times, want %d", workers, n, c, n)
	}

	slow, fast := &pb.Message{Data: []byte("slow")}, &pb.Message{Data: []byte("fast")}
	slowStarted, fastDone := make(chan struct{}), make(chan struct{})
	gen2 := newMsgIdGenerator()
	gen2.Default = func(m *pb.Message) string {
		if m == slow {
			close(slowStarted)
			select {
			case <-fastDone:
			case <-time.After(5 * time.Second):
				return "timed out"
			}
		}
		return string(m.Data)
	}
	slowID := make(chan string, 1)
	go func() { slowID <- gen2.RawID(slow) }()
	<-slowStarted
	if got := gen2.RawID(fast); got != "fast" {
		t.Fatalf("fast id %q", got)
	}
	close(fastDone)
	if got := <-slowID; got != "slow" {
		t.Fatalf("the slow message's id %q: the fast one waited behind it", got)
	}
}
