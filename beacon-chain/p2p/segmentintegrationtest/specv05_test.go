package segmentintegrationtest

import (
	"fmt"
	"os"
	"strings"
	"testing"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
)

// SEGMENT_V05: the rules version 0.5 of the protocol text added (the fork's specv05.go), all of
// them or a comma-separated subset: e (the promise starts at the write), a (a push that does not
// happen is offered instead; offers and answers a full queue refuses are owed), b (every offer
// kept, announcement order, the last resort for parked announcers), conn (promises and pull
// tables close with the connection), c (the offer table is the gate's ledger; needs b), d (a group
// past its lifetime leaves a tombstone and its pull state goes). Unset, the arms run as measured
// before v0.5.

// v05Ctr is the cell's v0.5 counters, package-level so the driver can read them per message;
// zeroed when a network is built.
var v05Ctr pubsub.SpecV05Counters

// specV05FromEnv parses SEGMENT_V05; on is false when it is unset.
func specV05FromEnv(t *testing.T) (rules pubsub.SpecV05, on bool) {
	t.Helper()
	v := os.Getenv("SEGMENT_V05")
	if v == "" {
		return rules, false
	}
	if v == "all" || v == "1" {
		return pubsub.SpecV05All, true
	}
	for _, f := range strings.Split(v, ",") {
		switch strings.TrimSpace(f) {
		case "e":
			rules.PromiseAtWrite = true
		case "a":
			rules.OfferSkipped = true
		case "b":
			rules.LastResort = true
		case "conn":
			rules.CloseWithConn = true
		case "c":
			rules.LedgerFromOffers = true
		case "d":
			rules.Expiry = true
		default:
			t.Fatalf("bad SEGMENT_V05 entry %q (want all, or a list of e, a, b, c, d, conn)", f)
		}
	}
	return rules, true
}

// v05Snap is a reading of v05Ctr.
type v05Snap struct {
	atWrite, lost, unpushed, owed, owedSent, answers, armed, lrAsks, parked, beyond, closed, ledger, expired, discards int64
}

func v05Now() v05Snap {
	c := &v05Ctr
	return v05Snap{c.PromisesAtWrite.Load(), c.AsksLost.Load(), c.Unpushed.Load(), c.OffersOwed.Load(), c.OwedSent.Load(),
		c.AnswersOwed.Load(), c.LastResortsArmed.Load(), c.LastResortAsks.Load(), c.ParkedAsked.Load(), c.BeyondList.Load(),
		c.ClosedPromises.Load(), c.LedgerAsks.Load(), c.Expired.Load(), c.ExpiredDiscards.Load()}
}

func v05Reset() {
	c := &v05Ctr
	for _, a := range []interface{ Store(int64) }{&c.PromisesAtWrite, &c.AsksLost, &c.Unpushed, &c.OffersOwed, &c.OwedSent,
		&c.AnswersOwed, &c.LastResortsArmed, &c.LastResortAsks, &c.ParkedAsked, &c.BeyondList, &c.ClosedPromises, &c.LedgerAsks, &c.Expired, &c.ExpiredDiscards} {
		a.Store(0)
	}
}

// line is what the v0.5 rules did between a and now, over the network.
func (a v05Snap) line(now v05Snap) string {
	return fmt.Sprintf("v0.5 this message: promises opened at the write %d (asks lost after it %d); pushes unpushed %d, "+
		"offer indices owed %d and sent %d, answers owed %d; last resorts armed %d, asks %d (to parked %d); "+
		"asks past the ordered list %d; promises closed with the connection %d; ledger asks at a replay %d; groups expired %d, queued sends discarded %d",
		now.atWrite-a.atWrite, now.lost-a.lost, now.unpushed-a.unpushed, now.owed-a.owed, now.owedSent-a.owedSent,
		now.answers-a.answers, now.armed-a.armed, now.lrAsks-a.lrAsks, now.parked-a.parked, now.beyond-a.beyond,
		now.closed-a.closed, now.ledger-a.ledger, now.expired-a.expired, now.discards-a.discards)
}
