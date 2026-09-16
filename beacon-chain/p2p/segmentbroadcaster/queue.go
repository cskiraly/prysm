package segmentbroadcaster

import (
	"log/slog"

	"github.com/OffchainLabs/prysm/v7/internal/logrusadapter"
	"github.com/sirupsen/logrus"
)

// verifyQueue returns the channel batches wait on, allocating it once.
//
// Lazily created so New stays allocation-light and the zero Broadcaster is still usable in a
// test that never publishes.
func (b *Broadcaster) verifyQueue() chan verifyWork {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.verify == nil {
		b.verify = make(chan verifyWork, verifyQueueSize)
	}
	return b.verify
}

// enqueueVerify hands a batch to the loop, dropping it if the queue is full.
//
// Dropping is the only option that does not block pubsub's goroutine, and it is safe: the
// peer still advertises the segments, so the next publish pass asks again.
func (b *Broadcaster) enqueueVerify(work verifyWork) {
	q := b.verifyQueue()
	select {
	case q <- work:
	default:
		b.bump(func(c *Counters) { c.VerifyQueueDropped++ })
		b.logger.WithField("peer", work.from.String()).Debug("Dropped segment batch, verify queue full")
	}
}

// newSlogger adapts a logrus entry to the slog handle the extension wants.
func newSlogger(entry *logrus.Entry) *slog.Logger {
	return slog.New(logrusadapter.Handler{Logger: entry.Logger}).With("package", logPackage)
}
