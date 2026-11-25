package pubsub

import (
	"context"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
)

// TestPublishBatchClosedRouter checks that PublishBatch on a router that has stopped returns an error
// instead of blocking on the batch channel, which no loop drains any more.
func TestPublishBatchClosedRouter(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	ps := getGossipsub(ctx, getDefaultHosts(t, 1)[0])
	cancel()
	errs := make(chan error, 3)
	go func() {
		for range 3 {
			errs <- ps.PublishBatch(&MessageBatch{})
		}
	}()
	for i := range 3 {
		select {
		case err := <-errs:
			if err == nil {
				t.Fatalf("publish %d on a stopped router returned nil", i)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("publish %d on a stopped router blocked", i)
		}
	}
}

// TestBatchEnqueued checks WithBatchEnqueued's report: a batch to a connected peer reports it, and
// a batch when every peer's outbound queue is full reports zero, though PublishBatch returns nil in
// both cases.
func TestBatchEnqueued(t *testing.T) {
	run := func(t *testing.T, full bool) int {
		var got int
		synctestTest(t, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			hosts := v05Hosts(t, 2, 20, 25*time.Millisecond)
			psubs := getGossipsubsOptFn(ctx, hosts, func(int, host.Host) []Option {
				return []Option{WithMessageSignaturePolicy(StrictNoSign), WithNoAuthor()}
			})
			topics := v05Join(t, psubs)
			for _, tp := range topics {
				if _, err := tp.Subscribe(); err != nil {
					t.Fatal(err)
				}
			}
			connectAll(t, hosts)
			time.Sleep(2 * time.Second)
			if full {
				// Every peer's queue full, and no writer draining it.
				done := make(chan struct{})
				psubs[0].eval <- func() {
					for p := range psubs[0].peers {
						q := newRpcQueue(1)
						if err := q.Push(&RPC{}, false); err != nil {
							t.Error(err)
						}
						psubs[0].peers[p] = q
					}
					close(done)
				}
				<-done
			}
			var batch MessageBatch
			for i := byte(0); i < 4; i++ {
				if err := topics[0].AddToBatch(ctx, &batch, v05Frame("batch-enqueued-root-0123456789ab", i)); err != nil {
					t.Fatal(err)
				}
			}
			report := make(chan int, 1)
			if err := psubs[0].PublishBatch(&batch, WithBatchEnqueued(func(n int) { report <- n })); err != nil {
				t.Fatal(err)
			}
			select {
			case got = <-report:
			case <-time.After(time.Second):
				t.Fatal("no report within a second")
			}
		})
		return got
	}
	if got := run(t, false); got != 1 {
		t.Fatalf("a batch to one connected peer reported %d peers, want 1", got)
	}
	if got := run(t, true); got != 0 {
		t.Fatalf("a batch with every peer queue full reported %d peers, want 0", got)
	}
}

// TestBatchEnqueuedOnce checks that an empty batch, and a batch the seen cache empties, each report
// zero exactly once.
func TestBatchEnqueuedOnce(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		hosts := v05Hosts(t, 2, 20, 25*time.Millisecond)
		psubs := getGossipsubsOptFn(ctx, hosts, func(int, host.Host) []Option {
			return []Option{WithMessageSignaturePolicy(StrictNoSign), WithNoAuthor()}
		})
		topics := v05Join(t, psubs)
		for _, tp := range topics {
			if _, err := tp.Subscribe(); err != nil {
				t.Fatal(err)
			}
		}
		connectAll(t, hosts)
		time.Sleep(2 * time.Second)

		publish := func(frames int) []int {
			var batch MessageBatch
			for i := byte(0); i < byte(frames); i++ {
				if err := topics[0].AddToBatch(ctx, &batch, v05Frame("batch-enqueued-once-0123456789ab", i)); err != nil {
					t.Fatal(err)
				}
			}
			report := make(chan int, 4)
			if err := psubs[0].PublishBatch(&batch, WithBatchEnqueued(func(n int) { report <- n })); err != nil {
				t.Fatal(err)
			}
			time.Sleep(time.Second)
			// Drained, not closed: the router's goroutine sends the report, and closing the channel
			// here would race with that send.
			var got []int
			for {
				select {
				case n := <-report:
					got = append(got, n)
				default:
					return got
				}
			}
		}
		if got := publish(0); len(got) != 1 || got[0] != 0 {
			t.Fatalf("an empty batch reported %v, want [0]", got)
		}
		if got := publish(4); len(got) != 1 || got[0] != 1 {
			t.Fatalf("a new batch reported %v, want [1]", got)
		}
		if got := publish(4); len(got) != 1 || got[0] != 0 {
			t.Fatalf("a batch the seen cache empties reported %v, want [0]", got)
		}
	})
}
