package segmentintegrationtest

// The fulldas arm (SEGMENT_ARMS=fulldas): FullDAS with full sampling on the harness's network, after
// the author's Nim simulator (das-simulator-nim 0e2810a11, its nim-libp2p fork ee5eda960) and the
// FullDAS post (ethresear.ch 19529, 2024-05-11). The cell is
// Figure 4's in every other respect: the same simnet links, latency model, bandwidth classes,
// message schedule, driver and report line, and gossipsub with the options the whole-message arm
// runs (Prysm's parameters, IDONTWANT, Prysm's message id; the arm adds none).
//
// The block is the payload compressed whole, the bytes the compress-first arms code (snappy of the
// payload, ~746 KB at 1 MiB), cut into kR x kC pieces as a grid and extended by Reed-Solomon in rows
// and then in columns to 2kR x 2kC cells (a systematic product code, klauspost/reedsolomon). Every
// row and every column is a gossipsub topic. Each node custodies one row and one column (Nim's
// custodyRows = custodyCols = 1; Nim draws each at random), drawn by a seeded rank so that every
// line has as many custodians as another give or take one (on a square grid, which fdFromEnv
// requires); the source custodies everything. Inside each row and each column the links are
// das-research's initNetwork, a random regular graph of SEGMENT_FULLDAS_DEGREE over its custodians,
// so a node's connections are its row peers and its column peers. The source's links are the arm's
// own: a greedy cover giving it custodians of every row and every column once (das-research gives
// the proposer netDegree neighbours per line).
//
// The source publishes every cell on its row topic and on its column topic, one copy each (Nim's
// publisherMaxCopies = 1). Flood publishing is off, as in Nim, so a copy goes to the source's mesh
// peer on the topic; with one source peer per topic (the cover's, at 500 nodes for 8x8,
// TestFullDASLayout; not at small n) a topic's copy has one place to go. It sends whole rows in
// row-major order, or under SEGMENT_FULLDAS_ORDER=random the cells in a random order (the Nim
// FullDAS configuration's publisherSendInRandomOrder, das-simulator-nim 34ba9ff), as one batch;
// its uplink queues each connection fairly, so the rows leave in parallel, each connection
// carrying one row's copies and one column's. Relays forward in each
// topic's mesh, stock. A cell a node receives on one of its topics that also lies on its other line
// is published there (cross-forwarding); a node holding half of a line holds all of it (rebuild on
// the fly), and a rebuilt cell on its other line is published there (Nim's repairCrossForward;
// repairForward is off, as in Nim).
//
// Sampling is the post's liteDAS, which the Nim code at the pin does not have: at the publish
// instant (the slot start, which every node knows) each node asks for every cell outside its
// custody one custodian, a connected one if it has one, else over an ephemeral connection to a
// custodian of the cell's row (one per such row, reused for its cells). The ephemeral connection is
// an ordinary libp2p connection standing in for the post's Ephemeral Connect, and finding the
// custodian is free (the layout is known), where the post has topic routing. The custodian holds
// the request until it holds the cell or the request's timeout passes, and answers on the
// requester's stream; a custodian's cells for one requester share one stream. After
// SEGMENT_FULLDAS_T1_MS the requester asks another candidate for what is still missing, every
// SEGMENT_FULLDAS_T2_MS, and gives up on a stream a second after its timeout, when the custodian
// has dropped it. A node holds the payload when its cells decode by row and column peeling to the
// whole data grid: that is its completion. It then cancels its open requests (stream resets).
// Ephemeral connections close SEGMENT_FULLDAS_EPHEMERAL_MS after the slot start, inside the gap;
// one a retry dials later closes when its last request ends. The source is never asked: the arm's
// policy, after das-research's proposerPublishOnly, which keeps the proposer out of the line
// graphs. After its completion a node forwards no more cross or rebuilt cells (Nim's handler keeps
// going), and an intersection cell it first holds through the decode is not forwarded. A node
// rebuilds every line it holds half of, its custody lines as in Nim and, with sampling, the lines
// its sampled cells fill: decoding, which also lets it answer for a custody cell it rebuilt that
// way. The headline bytes are the link rx line; rx/node on the summary line is the arm's
// application data, gossip and sampled cells.
//
//	SEGMENT_FULLDAS_K=8x8         data rows x data columns (K = 64 pieces at 1 MiB, tier 4's count)
//	SEGMENT_FULLDAS_EXTEND=1      2D RS extension to twice each dimension; 0: the data grid alone
//	SEGMENT_FULLDAS_COLS=1        column topics; 0: rows only
//	SEGMENT_FULLDAS_CUSTODY=1     one row and one column per node; all: every node every line
//	SEGMENT_FULLDAS_GRAPH=topics  the per-line graphs above; mesh: the cell's random regular graph
//	SEGMENT_FULLDAS_DEGREE=8      the per-line graph degree (even)
//	SEGMENT_FULLDAS_CROSS=1       cross-forwarding (the Nim pin's crossForward is false)
//	SEGMENT_FULLDAS_REBUILD=1     rebuild on the fly, and the rebuilt cell's cross-forward
//	SEGMENT_FULLDAS_SAMPLE=1      liteDAS sampling of every cell outside custody; decode: only enough
//	                              to decode with the custody lines (fdSampleSet); 0: none
//	SEGMENT_FULLDAS_PICK=random   under SAMPLE=decode, the rows sampled: random, or first (the first
//	                              rows the source sends)
//	SEGMENT_FULLDAS_CANCEL=1      cancel open requests at completion
//	SEGMENT_FULLDAS_ORDER=rows    the source's order: rows (row-major), or random (cells shuffled,
//	                              each on its row topic then its column topic, as in Nim)
//	SEGMENT_FULLDAS_T1_MS=4000    the first timeout; SEGMENT_FULLDAS_T2_MS=1000 the later ones
//	SEGMENT_FULLDAS_EPHEMERAL_MS=5000  ephemeral connections close this long after the slot start
//
// The known answer for the arm's own plumbing (plans/fulldas-arm.md §2) is the degenerate cell
// SEGMENT_FULLDAS_K=1x64 EXTEND=0 COLS=0 CUSTODY=all GRAPH=mesh CROSS=0 REBUILD=0 SAMPLE=0: one
// topic of 64 pieces pushed by stock gossipsub, which is the segmented arm on the same pieces
// (SEGMENT_ARMS=segmented SEGMENT_COMPRESS_FIRST=1 SEGMENT_FIXED_COUNT=64).

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"os"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/OffchainLabs/prysm/v7/beacon-chain/p2p/encoder"
	"github.com/OffchainLabs/prysm/v7/config/params"
	ethpb "github.com/OffchainLabs/prysm/v7/proto/prysm/v1alpha1"
	"github.com/OffchainLabs/prysm/v7/testing/gossipsim"
	"github.com/OffchainLabs/prysm/v7/testing/require"
	"github.com/golang/snappy"
	"github.com/klauspost/reedsolomon"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	simlibp2p "github.com/libp2p/go-libp2p/x/simlibp2p"
	msmux "github.com/multiformats/go-multistream"
)

const (
	fdLabel = "fulldas (2D RS cells on row and column gossipsub topics after das-simulator-nim 0e2810a11; liteDAS sampling after the FullDAS post)"
	// fdProto is the sampling protocol: requests one way, cells the other, on one stream per
	// requester, custodian and message.
	fdProto = protocol.ID("/fulldas/sample/1")
	// A cell's gossip data: block id (8 bytes), row, column, dimension (the topic's: 0 row, 1
	// column), then the cell. Nim's carries a send time, the message number, row, column and
	// dimension in its first 17 bytes; neither carries a proof.
	fdHdrLen = 11
	fdDimRow = 0
	fdDimCol = 1
	// Frames on a sampling stream: a request (type, message, cell, timeout in ms) and a cell (type,
	// message, cell, length, the cell).
	fdReqLen  = 1 + 4 + 2 + 4
	fdCellHdr = 1 + 4 + 2 + 4
	fdFrameRq = 1
	fdFrameCl = 2
)

// How a node came to hold a cell, for the per-node tallies.
const (
	fdNone uint8 = iota
	fdGossip
	fdRebuiltRow
	fdRebuiltCol
	fdSampled
	fdDecoded
)

func fdTopic(dim, line int) string {
	if dim == fdDimRow {
		return fmt.Sprintf("%s/row/%d", segmentTopic(), line)
	}
	return fmt.Sprintf("%s/col/%d", segmentTopic(), line)
}

// fdConfig is what the environment says about the arm.
type fdConfig struct {
	kR, kC     int
	extend     bool
	cols       bool
	custodyAll bool
	graphMesh  bool
	cross      bool
	rebuild    bool
	sample     bool
	decodeOnly bool // sample only enough to decode
	firstRows  bool // decodeOnly: the first rows, not random ones
	randOrder  bool // the source publishes the cells in a random order
	cancel     bool
	degree     int
	t1, t2     time.Duration
	ephemeral  time.Duration
}

func (c *fdConfig) rows() int {
	if c.extend {
		return 2 * c.kR
	}
	return c.kR
}

func (c *fdConfig) columns() int {
	if c.extend {
		return 2 * c.kC
	}
	return c.kC
}

func fdFromEnv(t *testing.T) *fdConfig {
	t.Helper()
	c := &fdConfig{kR: 8, kC: 8, extend: true, cols: true, cross: true, rebuild: true, sample: true, cancel: true,
		degree: 8, t1: 4 * time.Second, t2: time.Second, ephemeral: 5 * time.Second}
	if v := os.Getenv("SEGMENT_FULLDAS_K"); v != "" {
		a, b, ok := strings.Cut(v, "x")
		r, err1 := strconv.Atoi(a)
		k, err2 := strconv.Atoi(b)
		if !ok || err1 != nil || err2 != nil || r < 1 || k < 1 {
			t.Fatalf("bad SEGMENT_FULLDAS_K %q (rows x columns)", v)
		}
		c.kR, c.kC = r, k
	}
	flag := func(name string, def bool) bool {
		switch os.Getenv(name) {
		case "":
			return def
		case "0":
			return false
		case "1":
			return true
		}
		t.Fatalf("bad %s %q (0 or 1)", name, os.Getenv(name))
		return def
	}
	c.extend = flag("SEGMENT_FULLDAS_EXTEND", true)
	c.cols = flag("SEGMENT_FULLDAS_COLS", true)
	c.cross = flag("SEGMENT_FULLDAS_CROSS", true)
	c.rebuild = flag("SEGMENT_FULLDAS_REBUILD", true)
	switch v := os.Getenv("SEGMENT_FULLDAS_SAMPLE"); v {
	case "", "1":
	case "0":
		c.sample = false
	case "decode":
		c.decodeOnly = true
	default:
		t.Fatalf("bad SEGMENT_FULLDAS_SAMPLE %q (1, decode or 0)", v)
	}
	switch v := os.Getenv("SEGMENT_FULLDAS_PICK"); v {
	case "", "random":
	case "first":
		c.firstRows = true
	default:
		t.Fatalf("bad SEGMENT_FULLDAS_PICK %q (random or first)", v)
	}
	c.cancel = flag("SEGMENT_FULLDAS_CANCEL", true)
	switch v := os.Getenv("SEGMENT_FULLDAS_ORDER"); v {
	case "", "rows":
	case "random":
		c.randOrder = true
	default:
		t.Fatalf("bad SEGMENT_FULLDAS_ORDER %q (rows or random)", v)
	}
	switch v := os.Getenv("SEGMENT_FULLDAS_CUSTODY"); v {
	case "", "1":
	case "all":
		c.custodyAll = true
	default:
		t.Fatalf("bad SEGMENT_FULLDAS_CUSTODY %q (1 or all)", v)
	}
	switch v := os.Getenv("SEGMENT_FULLDAS_GRAPH"); v {
	case "", "topics":
	case "mesh":
		c.graphMesh = true
	default:
		t.Fatalf("bad SEGMENT_FULLDAS_GRAPH %q (topics or mesh)", v)
	}
	ms := func(name string, def time.Duration) time.Duration {
		v := os.Getenv(name)
		if v == "" {
			return def
		}
		k, err := strconv.Atoi(v)
		if err != nil || k < 1 {
			t.Fatalf("bad %s %q", name, v)
		}
		return time.Duration(k) * time.Millisecond
	}
	c.t1 = ms("SEGMENT_FULLDAS_T1_MS", c.t1)
	c.t2 = ms("SEGMENT_FULLDAS_T2_MS", c.t2)
	c.ephemeral = ms("SEGMENT_FULLDAS_EPHEMERAL_MS", c.ephemeral)
	if v := os.Getenv("SEGMENT_FULLDAS_DEGREE"); v != "" {
		k, err := strconv.Atoi(v)
		if err != nil || k < 2 || k%2 != 0 {
			// An odd degree has no regular graph on a line with an odd number of custodians.
			t.Fatalf("bad SEGMENT_FULLDAS_DEGREE %q (even, at least 2)", v)
		}
		c.degree = k
	}
	if c.rows()*c.columns() > math.MaxUint16 {
		t.Fatalf("SEGMENT_FULLDAS_K %dx%d: too many cells", c.kR, c.kC)
	}
	if !c.custodyAll && c.cols && c.rows() != c.columns() {
		// newFdLayout's draw balances the columns only on a square grid.
		t.Fatalf("SEGMENT_FULLDAS_K %dx%d: a row and a column per node needs as many rows as columns", c.kR, c.kC)
	}
	if !c.cols && c.cross {
		c.cross = false // nothing to cross to
	}
	return c
}

// order names the source's publication order for the graph line.
func (c *fdConfig) order() string {
	if c.randOrder {
		return "random"
	}
	return "rows"
}

// sampling names the sampling setting for the graph line.
func (c *fdConfig) sampling() string {
	switch {
	case !c.sample:
		return "off"
	case c.decodeOnly && c.firstRows:
		return "decode (first rows)"
	case c.decodeOnly:
		return "decode (random rows)"
	}
	return "all"
}

// fdBlock is one payload's cells and their gossip forms.
type fdBlock struct {
	m     int
	id    uint64
	unit  int
	plen  int      // the compressed payload's length
	cells [][]byte // row-major, rows() x columns()
	wire  [2][][]byte
	known map[digest][2]int // gossip form -> (cell, dimension)
}

// fdPayload is message m's compressed payload: the bytes the compress-first arms code.
func fdPayload(payloadLen, m int) []byte {
	seed := uint64(11)
	if m > 1 {
		seed = uint64(1000 + m) // lint:ignore uintcast -- m is a small positive message number.
	}
	return snappy.Encode(nil, mainnetLikePayload(payloadLen, seed))
}

// fdEncode cuts data into the kR x kC grid and extends it, returning the cells row-major and the
// piece size.
func fdEncode(cfg *fdConfig, data []byte) ([][]byte, int, error) {
	k := cfg.kR * cfg.kC
	unit := (len(data) + k - 1) / k
	R, C := cfg.rows(), cfg.columns()
	cells := make([][]byte, R*C)
	for i := range cells {
		cells[i] = make([]byte, unit)
	}
	for p := range k {
		r, c := p/cfg.kC, p%cfg.kC
		lo := min(p*unit, len(data))
		hi := min(lo+unit, len(data))
		copy(cells[r*C+c], data[lo:hi])
	}
	if !cfg.extend {
		return cells, unit, nil
	}
	rowEnc, err := reedsolomon.New(cfg.kC, cfg.kC)
	if err != nil {
		return nil, 0, err
	}
	for r := range cfg.kR {
		if err := rowEnc.Encode(cells[r*C : (r+1)*C]); err != nil {
			return nil, 0, err
		}
	}
	colEnc, err := reedsolomon.New(cfg.kR, cfg.kR)
	if err != nil {
		return nil, 0, err
	}
	col := make([][]byte, R)
	for c := range C {
		for r := range R {
			col[r] = cells[r*C+c]
		}
		if err := colEnc.Encode(col); err != nil {
			return nil, 0, err
		}
	}
	return cells, unit, nil
}

func fdBuild(t *testing.T, cfg *fdConfig, payloadLen, m int, seed uint64) *fdBlock {
	t.Helper()
	data := fdPayload(payloadLen, m)
	cells, unit, err := fdEncode(cfg, data)
	require.NoError(t, err)
	b := &fdBlock{m: m, id: mix64(seed ^ uint64(m)<<32 ^ 0xF011DA5), unit: unit, plen: len(data), cells: cells, known: map[digest][2]int{}} // lint:ignore uintcast -- m is a small positive message number.
	C := cfg.columns()
	enc := encoder.SszNetworkEncoder{}
	for dim := range 2 {
		if dim == fdDimCol && !cfg.cols {
			continue
		}
		b.wire[dim] = make([][]byte, len(cells))
		for i, cell := range cells {
			data := make([]byte, fdHdrLen+len(cell))
			binary.BigEndian.PutUint64(data, b.id)
			data[8], data[9], data[10] = byte(i/C), byte(i%C), byte(dim) // lint:ignore uintcast -- rows and columns below 256.
			copy(data[fdHdrLen:], cell)
			var buf bytes.Buffer
			_, err := enc.EncodeGossip(&buf, &ethpb.ExecutionPayloadSegment{Segment: data})
			require.NoError(t, err)
			w := bytes.Clone(buf.Bytes())
			b.wire[dim][i] = w
			b.known[digestOf(w)] = [2]int{i, dim}
		}
	}
	return b
}

// fdPeel closes held under row and column decoding: a line holding at least half its cells (its
// data width) holds all of them. via, when non-nil, records the line that filled each cell. It
// returns the cells it filled.
func fdPeel(cfg *fdConfig, held []bool, via []uint8) []int {
	if !cfg.extend {
		return nil
	}
	R, C := cfg.rows(), cfg.columns()
	rowCnt, colCnt := make([]int, R), make([]int, C)
	for i, h := range held {
		if h {
			rowCnt[i/C]++
			colCnt[i%C]++
		}
	}
	var filled []int
	for changed := true; changed; {
		changed = false
		for r := range R {
			if rowCnt[r] >= cfg.kC && rowCnt[r] < C {
				for c := range C {
					if i := r*C + c; !held[i] {
						held[i] = true
						rowCnt[r]++
						colCnt[c]++
						filled = append(filled, i)
						if via != nil {
							via[i] = fdRebuiltRow
						}
					}
				}
				changed = true
			}
		}
		for c := range C {
			if colCnt[c] >= cfg.kR && colCnt[c] < R {
				for r := range R {
					if i := r*C + c; !held[i] {
						held[i] = true
						rowCnt[r]++
						colCnt[c]++
						filled = append(filled, i)
						if via != nil {
							via[i] = fdRebuiltCol
						}
					}
				}
				changed = true
			}
		}
	}
	return filled
}

// fdDecodable says whether held decodes to the data grid.
func fdDecodable(cfg *fdConfig, held []bool) bool {
	h := slices.Clone(held)
	fdPeel(cfg, h, nil)
	C := cfg.columns()
	for r := range cfg.kR {
		for c := range cfg.kC {
			if !h[r*C+c] {
				return false
			}
		}
	}
	return true
}

// fdSampleSet is the cells node i samples, row-major: every cell outside its custody, or under
// SEGMENT_FULLDAS_SAMPLE=decode only enough to decode with its custody lines. Those are kR-1 rows
// besides its custody row (at random, or under SEGMENT_FULLDAS_PICK=first the first ones, which
// the source sends first), and in each of them kC-1 cells besides its custody column's, at random:
// with that column's cell each such row holds kC cells and decodes, and kR whole rows decode every
// column. A node custodying every line samples nothing.
func fdSampleSet(cfg *fdConfig, l *fdLayout, i int, rng *rand.Rand) []int {
	R, C := cfg.rows(), cfg.columns()
	r0, c0 := l.rowOf[i], l.colOf[i]
	var out []int
	if !cfg.decodeOnly || !cfg.extend || r0 < 0 {
		for c := range R * C {
			if !l.custodies(i, c) {
				out = append(out, c)
			}
		}
		return out
	}
	var rows, cols []int
	for r := range R {
		if r != r0 {
			rows = append(rows, r)
		}
	}
	if !cfg.firstRows {
		rng.Shuffle(len(rows), func(a, b int) { rows[a], rows[b] = rows[b], rows[a] })
	}
	rows = rows[:cfg.kR-1]
	need := cfg.kC
	for c := range C {
		if cfg.cols && c == c0 {
			need--
			continue
		}
		cols = append(cols, c)
	}
	for _, r := range rows {
		rng.Shuffle(len(cols), func(a, b int) { cols[a], cols[b] = cols[b], cols[a] })
		for _, c := range cols[:need] {
			out = append(out, r*C+c)
		}
	}
	sort.Ints(out)
	return out
}

// fdLayout is the custody draw and the graph, fixed for a cell.
type fdLayout struct {
	cfg        *fdConfig
	n          int
	rowOf      []int // per node its custody row, -1 for every row (the source, or custody all)
	colOf      []int
	rowMembers [][]int // custodians per line, the source excluded
	colMembers [][]int
	edges      []edge
	adj        []map[int]bool
	srcPerLine [2][]int // the source's links per row and per column
}

// custodies says whether node i holds cell idx in custody.
func (l *fdLayout) custodies(i, idx int) bool {
	C := l.cfg.columns()
	r, c := idx/C, idx%C
	return l.rowOf[i] < 0 || l.rowOf[i] == r || (l.cfg.cols && l.colOf[i] == c)
}

func newFdLayout(t *testing.T, cfg *fdConfig, n int, seed uint64, mesh []edge) *fdLayout {
	t.Helper()
	R, C := cfg.rows(), cfg.columns()
	l := &fdLayout{cfg: cfg, n: n, rowOf: make([]int, n), colOf: make([]int, n),
		rowMembers: make([][]int, R), colMembers: make([][]int, C)}
	l.rowOf[0], l.colOf[0] = -1, -1
	rng := rand.New(rand.NewPCG(seed, 0xF011DA5C))
	order := rng.Perm(n - 1)
	for k, o := range order {
		i := o + 1
		if cfg.custodyAll {
			l.rowOf[i], l.colOf[i] = -1, -1
			continue
		}
		l.rowOf[i] = k % R
		l.colOf[i] = (k/R + k) % C
		if !cfg.cols {
			l.colOf[i] = -1
		}
	}
	for i := 1; i < n; i++ {
		for r := range R {
			if l.rowOf[i] < 0 || l.rowOf[i] == r {
				l.rowMembers[r] = append(l.rowMembers[r], i)
			}
		}
		if cfg.cols {
			for c := range C {
				if l.colOf[i] < 0 || l.colOf[i] == c {
					l.colMembers[c] = append(l.colMembers[c], i)
				}
			}
		}
	}
	present := map[edge]bool{}
	if cfg.graphMesh {
		for _, e := range mesh {
			present[e] = true
		}
	} else {
		line := func(members []int, salt uint64) {
			switch {
			case len(members) < 2:
			case len(members) <= cfg.degree:
				for a := range members {
					for b := a + 1; b < len(members); b++ {
						present[newEdge(members[a], members[b])] = true
					}
				}
			default:
				es, err := randomRegular(len(members), cfg.degree, mix64(seed^salt)) // even (fdFromEnv)
				require.NoError(t, err)
				for _, e := range es {
					present[newEdge(members[e.A], members[e.B])] = true
				}
			}
		}
		for r := range R {
			line(l.rowMembers[r], 0xF0000+uint64(r)) // lint:ignore uintcast -- a row index.
		}
		for c := range C {
			line(l.colMembers[c], 0xF1000+uint64(c)) // lint:ignore uintcast -- a column index.
		}
		// The source: a custodian per row, each also covering a column not yet covered where one
		// exists, then a custodian for each column still uncovered.
		rowDone, colDone := make([]bool, R), make([]bool, C)
		link := func(i int) {
			present[newEdge(0, i)] = true
			if r := l.rowOf[i]; r >= 0 {
				rowDone[r] = true
			}
			if c := l.colOf[i]; c >= 0 {
				colDone[c] = true
			}
		}
		for _, r := range rng.Perm(R) {
			if rowDone[r] || len(l.rowMembers[r]) == 0 {
				continue
			}
			var fresh []int
			for _, i := range l.rowMembers[r] {
				if !cfg.cols || (l.colOf[i] >= 0 && !colDone[l.colOf[i]]) {
					fresh = append(fresh, i)
				}
			}
			if len(fresh) == 0 {
				fresh = l.rowMembers[r]
			}
			link(fresh[rng.IntN(len(fresh))])
		}
		for c := range C {
			if cfg.cols && !colDone[c] && len(l.colMembers[c]) > 0 {
				link(l.colMembers[c][rng.IntN(len(l.colMembers[c]))])
			}
		}
	}
	l.edges = sortedEdges(present)
	require.Equal(t, true, connected(n, l.edges), "the fulldas graph is not connected")
	l.adj = make([]map[int]bool, n)
	for i := range l.adj {
		l.adj[i] = map[int]bool{}
	}
	for _, e := range l.edges {
		l.adj[e.A][e.B], l.adj[e.B][e.A] = true, true
	}
	l.srcPerLine = [2][]int{make([]int, R), make([]int, C)}
	for p := range l.adj[0] {
		for r := range R {
			if l.rowOf[p] < 0 || l.rowOf[p] == r {
				l.srcPerLine[0][r]++
			}
		}
		for c := range C {
			if cfg.cols && (l.colOf[p] < 0 || l.colOf[p] == c) {
				l.srcPerLine[1][c]++
			}
		}
	}
	return l
}

// graphLine describes the custody draw and the graph.
func (l *fdLayout) graphLine() string {
	mm := func(xs []int) string {
		if len(xs) == 0 {
			return "-"
		}
		s := slices.Clone(xs)
		sort.Ints(s)
		return fmt.Sprintf("min %d median %d max %d", s[0], s[len(s)/2], s[len(s)-1])
	}
	lens := func(ls [][]int) []int {
		out := make([]int, len(ls))
		for i, x := range ls {
			out[i] = len(x)
		}
		return out
	}
	deg := make([]int, 0, l.n-1)
	for i := 1; i < l.n; i++ {
		deg = append(deg, len(l.adj[i]))
	}
	custody := "one row and one column per node"
	switch {
	case l.cfg.custodyAll:
		custody = "every line on every node"
	case !l.cfg.cols:
		custody = "one row per node"
	}
	graph := fmt.Sprintf("per-line random %d-regular graphs (das-research initNetwork)", l.cfg.degree)
	if l.cfg.graphMesh {
		graph = "the cell's random regular graph"
	}
	return fmt.Sprintf("%s: %dx%d data, %dx%d cells (extend %v), column topics %v; custody %s; custodians per row %s, per column %s; graph %s, %d links, links per node %s, the source's %d (per row %s, per column %s); cross %v rebuild %v sample %s cancel %v t1 %v t2 %v order %s",
		fdLabel, l.cfg.kR, l.cfg.kC, l.cfg.rows(), l.cfg.columns(), l.cfg.extend, l.cfg.cols, custody,
		mm(lens(l.rowMembers)), mm(lens(l.colMembers)), graph, len(l.edges), mm(deg), len(l.adj[0]),
		mm(l.srcPerLine[0]), mm(l.srcPerLine[1]), l.cfg.cross, l.cfg.rebuild, l.cfg.sampling(), l.cfg.cancel, l.cfg.t1, l.cfg.t2, l.cfg.order())
}

// fdWait is a request a custodian holds: the stream to answer on, and when it expires.
type fdWait struct {
	w   *fdWriter
	exp time.Time
}

// fdWriter writes cells to one requester's stream, in order, from its own goroutine.
type fdWriter struct {
	s     network.Stream
	m     int
	mu    sync.Mutex
	queue []int
	cond  *sync.Cond
	dead  bool
	block *fdBlock
	sent  *atomic.Int64
}

func (w *fdWriter) enqueue(cell int) {
	w.mu.Lock()
	if !w.dead {
		w.queue = append(w.queue, cell)
		w.cond.Signal()
	}
	w.mu.Unlock()
}

func (w *fdWriter) kill() {
	w.mu.Lock()
	w.dead = true
	w.cond.Signal()
	w.mu.Unlock()
}

func (w *fdWriter) run() {
	for {
		w.mu.Lock()
		for len(w.queue) == 0 && !w.dead {
			w.cond.Wait()
		}
		if w.dead {
			w.mu.Unlock()
			return
		}
		cell := w.queue[0]
		w.queue = w.queue[1:]
		w.mu.Unlock()
		data := w.block.cells[cell]
		buf := make([]byte, fdCellHdr+len(data))
		buf[0] = fdFrameCl
		binary.BigEndian.PutUint32(buf[1:], uint32(w.m))       // lint:ignore uintcast -- a small message number.
		binary.BigEndian.PutUint16(buf[5:], uint16(cell))      // lint:ignore uintcast -- below 2^16, checked in fdFromEnv.
		binary.BigEndian.PutUint32(buf[7:], uint32(len(data))) // lint:ignore uintcast -- a cell's length.
		copy(buf[fdCellHdr:], data)
		if _, err := w.s.Write(buf); err != nil {
			w.kill()
			return
		}
		w.sent.Add(1)
	}
}

// fdHold is one node's cells of the current message, shared by its watcher and its custodian side.
type fdHold struct {
	mu      sync.Mutex
	m       int
	block   *fdBlock
	held    []bool
	via     []uint8
	done    bool
	pending map[int][]fdWait
	writers []*fdWriter
}

// add records cells as held and returns the cells that became held, rebuilt ones included, and the
// requests they answer. Under h.mu.
func (h *fdHold) addLocked(cfg *fdConfig, cells []int, how uint8) []int {
	var fresh []int
	for _, i := range cells {
		if !h.held[i] {
			h.held[i] = true
			h.via[i] = how
			fresh = append(fresh, i)
		}
	}
	if len(fresh) > 0 && cfg.rebuild {
		fresh = append(fresh, fdPeel(cfg, h.held, h.via)...)
	}
	return fresh
}

// serveLocked answers the held requests for cells, under h.mu; the writers write outside it.
func (h *fdHold) serveLocked(cells []int) {
	now := time.Now()
	for _, i := range cells {
		ws := h.pending[i]
		if len(ws) == 0 {
			continue
		}
		delete(h.pending, i)
		for _, w := range ws {
			if now.Before(w.exp) {
				w.w.enqueue(i)
			}
		}
	}
}

// fdArrival is a cell reaching a node's watcher.
type fdArrival struct {
	cell, dim int
	how       uint8
}

// fdTally is how a node came by the cells it held when it came to hold the payload: by gossip, by
// sampling, rebuilt on a custody line, rebuilt on another line (decoding from sampled cells), and
// the rest, which the payload's decode gives it.
type fdTally struct {
	gossip, sampled, rebuiltCustody, rebuiltOther, decoded int
}

// fdNodeStats is one node's tallies for the current message.
type fdNodeStats struct {
	tally                  fdTally
	answered, dupes        int
	retries, cancelled     int
	custodyAt, doneAt      time.Duration
	crossSent, rebuiltSent int
}

// fullDASVariant is one fulldas cell.
type fullDASVariant struct {
	cfg        *fdConfig
	rate       int
	oneWay     time.Duration
	pub        int
	msgNum     int
	payloadLen int
	seed       uint64
	n          int
	layout     *fdLayout
	srcUp      int // the source's uplink, bits per second (meshClassLinks)

	nw      *simNetwork
	hostIdx map[peer.ID]int
	topics  []map[[2]int]*pubsub.Topic // per node, (dimension, line) -> topic
	subs    [][]fdSub
	holds   []*fdHold
	block   *fdBlock
	tracers []*recordingTracer

	mu        sync.Mutex
	wg        *sync.WaitGroup
	start     time.Time
	msgCtx    context.Context
	msgCancel context.CancelFunc
	stats     []fdNodeStats
	ephem     [][]peer.ID // per node, the ephemeral peers it dialled for the current message
	reqs      [][]*fdReq
	// gen numbers the messages; swept says the slot-relative close of ephemeral connections has
	// run; active counts open requests per (requester, custodian), so that a connection dialled
	// after the close (by a retry) closes when its last request ends.
	gen    int
	swept  bool
	active map[[2]int]int
	// watchers is this message's watcher goroutines, joined before a timeout's report.
	watchers sync.WaitGroup
	// Per node, this message: ephemeral connections made, cells in requests sent, sampled cells
	// read and their data bytes (every one, useful or not).
	dialled    []atomic.Int64
	requested  []atomic.Int64
	sampleResp []atomic.Int64
	sampleRx   []atomic.Int64
	served     atomic.Int64
	misdirect  atomic.Int64
	badCells   atomic.Int64
	lateClosed atomic.Int64 // ephemeral connections closed at their last request's end
}

type fdSub struct {
	sub  *pubsub.Subscription
	dim  int
	line int
}

func newFullDASVariant(t *testing.T, c q6Cell, rate int, oneWay time.Duration, n int) *fullDASVariant {
	t.Helper()
	cfg := fdFromEnv(t)
	seed := meshGraphSeed(t)
	v := &fullDASVariant{cfg: cfg, rate: rate, oneWay: oneWay, msgNum: 1, payloadLen: c.payloadLen, seed: seed, n: n}
	v.layout = newFdLayout(t, cfg, n, seed, meshGraph(t, n))
	v.block = fdBuild(t, cfg, v.payloadLen, 1, seed)
	_, node0 := meshClassLinks(t, rate)
	v.srcUp = node0.Uplink.BitsPerSecond
	return v
}

func (v *fullDASVariant) name() string { return "fulldas" }

// pubsubOpts adds nothing: gossipsub runs with the whole-message arm's options.
func (v *fullDASVariant) pubsubOpts(*testing.T, int) []pubsub.Option { return nil }

func (v *fullDASVariant) setFirstPublisher(pub int) {
	if pub != 0 {
		panic("the fulldas arm's source is node 0")
	}
	v.pub = pub
}

// shapeEdges replaces the cell's graph with the arm's (driver_test.go's edgeShaper).
func (v *fullDASVariant) shapeEdges([]edge) []edge { return v.layout.edges }

// horizonless leaves the horizon's lines out: they read the segment topic's group, which this arm
// does not use.
func (v *fullDASVariant) horizonless() bool { return true }

func (v *fullDASVariant) onTimeout(t *testing.T, completed, n int) {
	// The watchers write their nodes' counters without v.mu. On the happy path they have all returned
	// by the report; on a timeout they are stopped and joined here, before it.
	v.mu.Lock()
	cancel := v.msgCancel
	v.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	v.watchers.Wait()
	t.Logf("      fulldas: timed out with %d/%d holding the payload", completed, n-1)
}

func (v *fullDASVariant) setup(t *testing.T, nw *simNetwork, _ diffusionParams) diffusionRun {
	t.Helper()
	v.nw = nw
	l := v.layout
	v.hostIdx = make(map[peer.ID]int, v.n)
	for i, h := range nw.Hosts {
		v.hostIdx[h.ID()] = i
	}
	v.holds = make([]*fdHold, v.n)
	v.dialled, v.requested = make([]atomic.Int64, v.n), make([]atomic.Int64, v.n)
	v.sampleResp, v.sampleRx = make([]atomic.Int64, v.n), make([]atomic.Int64, v.n)
	v.topics = make([]map[[2]int]*pubsub.Topic, v.n)
	v.subs = make([][]fdSub, v.n)
	for i := range v.n {
		v.holds[i] = &fdHold{}
		h := nw.Hosts[i]
		idx := i
		h.SetStreamHandler(fdProto, func(s network.Stream) { v.serve(idx, s) })
		v.topics[i] = map[[2]int]*pubsub.Topic{}
		join := func(dim, line int) {
			topic, err := nw.Pubsubs[i].Join(fdTopic(dim, line))
			require.NoError(t, err)
			sub, err := topic.Subscribe(pubsub.WithBufferSize(gossipsim.SubscriptionBuffer))
			require.NoError(t, err)
			v.topics[i][[2]int{dim, line}] = topic
			v.subs[i] = append(v.subs[i], fdSub{sub: sub, dim: dim, line: line})
		}
		for r := range v.cfg.rows() {
			if l.rowOf[i] < 0 || l.rowOf[i] == r {
				join(fdDimRow, r)
			}
		}
		if v.cfg.cols {
			for c := range v.cfg.columns() {
				if l.colOf[i] < 0 || l.colOf[i] == c {
					join(fdDimCol, c)
				}
			}
		}
	}
	time.Sleep(gossipsim.MeshFormation)
	gossipsim.Settle(false)
	t.Logf("      %s", l.graphLine())
	return &fdRun{v: v}
}

// nextMessage switches to message m: its block, and the end of the previous message's ephemeral
// connections and request streams.
func (v *fullDASVariant) nextMessage(t *testing.T, m, pub int) {
	t.Helper()
	if pub != 0 {
		t.Fatal("the fulldas arm's source is node 0")
	}
	v.endMessage()
	v.msgNum = m
	v.block = fdBuild(t, v.cfg, v.payloadLen, m, v.seed)
}

// endMessage closes the current message's request streams and ephemeral connections.
func (v *fullDASVariant) endMessage() {
	v.mu.Lock()
	cancel, reqs, ephem := v.msgCancel, v.reqs, v.ephem
	v.msgCancel, v.reqs, v.ephem, v.active = nil, nil, nil, nil
	v.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	for _, rs := range reqs {
		for _, r := range rs {
			_ = r.s.Reset()
		}
	}
	v.closeEphemeral(ephem)
	for _, h := range v.holds {
		h.mu.Lock()
		ws := h.writers
		h.writers = nil
		h.mu.Unlock()
		for _, w := range ws {
			w.kill()
		}
	}
}

// closeEphemeral closes, per node, the ephemeral connections it dialled.
func (v *fullDASVariant) closeEphemeral(ephem [][]peer.ID) {
	for i, ps := range ephem {
		for _, p := range ps {
			_ = v.nw.Hosts[i].Network().ClosePeer(p)
		}
	}
}

// extraRx is the sampled cells' data bytes this message, all nodes: the driver adds them to the
// gossip data it counts, so that rx/node is the arm's application data (driver_test.go).
func (v *fullDASVariant) extraRx() int {
	k := int64(0)
	for i := range v.sampleRx {
		k += v.sampleRx[i].Load()
	}
	return int(k)
}

// serve is node idx's custodian side of one requester's stream: it reads requests and answers each
// when the node holds its cell, until the stream ends.
func (v *fullDASVariant) serve(idx int, s network.Stream) {
	h := v.holds[idx]
	br := bufio.NewReader(s)
	var w *fdWriter
	defer func() {
		if w != nil {
			w.kill()
		}
		_ = s.Reset()
	}()
	var frame [fdReqLen]byte
	for {
		if _, err := io.ReadFull(br, frame[:]); err != nil || frame[0] != fdFrameRq {
			return
		}
		m := int(binary.BigEndian.Uint32(frame[1:]))
		cell := int(binary.BigEndian.Uint16(frame[5:]))
		ttl := time.Duration(binary.BigEndian.Uint32(frame[7:])) * time.Millisecond
		h.mu.Lock()
		if h.m != m || cell >= len(h.held) || !v.layout.custodies(idx, cell) {
			h.mu.Unlock()
			v.misdirect.Add(1)
			continue
		}
		if w == nil {
			w = &fdWriter{s: s, m: m, block: h.block, sent: &v.served}
			w.cond = sync.NewCond(&w.mu)
			h.writers = append(h.writers, w)
			go w.run()
		}
		if h.held[cell] || h.done {
			h.mu.Unlock()
			w.enqueue(cell)
			continue
		}
		if h.pending == nil {
			h.pending = map[int][]fdWait{}
		}
		h.pending[cell] = append(h.pending[cell], fdWait{w: w, exp: time.Now().Add(ttl)})
		h.mu.Unlock()
	}
}

// fdRun is the live state of a fulldas cell.
type fdRun struct {
	v *fullDASVariant
}

// settle waits for the line meshes on a loose floor (half of Dlo per topic) and summarises mesh
// slots per node-topic.
func (r *fdRun) settle(t *testing.T, tracers []*recordingTracer) string {
	t.Helper()
	v := r.v
	v.tracers = tracers
	lo := meshParams().Dlo / 2
	deadline := time.Now().Add(meshSettleTimeout)
	for {
		short := 0
		for i := 1; i < v.n; i++ {
			if tracers[i].MeshSlots() < lo*len(v.subs[i]) {
				short++
			}
		}
		if short <= v.n/20 || time.Now().After(deadline) {
			break
		}
		time.Sleep(meshPollInterval)
		settle(false)
	}
	per := make([]int, 0, v.n-1)
	for i := 1; i < v.n; i++ {
		per = append(per, tracers[i].MeshSlots()*100/max(1, len(v.subs[i])))
	}
	sort.Ints(per)
	return fmt.Sprintf("line meshes: slots per node-topic min %.2f median %.2f max %.2f (source %d slots over %d topics)",
		float64(per[0])/100, float64(per[len(per)/2])/100, float64(per[len(per)-1])/100, tracers[0].MeshSlots(), len(v.subs[0]))
}

func (r *fdRun) cancel() {
	r.v.endMessage()
	for _, ss := range r.v.subs {
		for _, s := range ss {
			s.sub.Cancel()
		}
	}
}

// watch starts the message: every node's hold switches to it (the source holds everything), and a
// watcher per node other than the source reads its topics and runs its sampling.
func (r *fdRun) watch(ctx context.Context, wg *sync.WaitGroup, start time.Time, signal func(node int, at time.Duration)) {
	v := r.v
	b := v.block
	cells := len(b.cells)
	mctx, cancel := context.WithCancel(ctx)
	v.mu.Lock()
	v.wg, v.start, v.msgCtx, v.msgCancel = wg, start, mctx, cancel
	v.stats = make([]fdNodeStats, v.n)
	v.ephem = make([][]peer.ID, v.n)
	v.reqs = make([][]*fdReq, v.n)
	v.gen++
	v.swept, v.active = false, map[[2]int]int{}
	v.mu.Unlock()
	for i := range v.n {
		v.dialled[i].Store(0)
		v.requested[i].Store(0)
		v.sampleResp[i].Store(0)
		v.sampleRx[i].Store(0)
	}
	v.served.Store(0)
	v.misdirect.Store(0)
	v.badCells.Store(0)
	v.lateClosed.Store(0)
	for i, h := range v.holds {
		h.mu.Lock()
		h.m, h.block, h.held, h.via, h.pending, h.done = b.m, b, make([]bool, cells), make([]uint8, cells), nil, i == v.pub
		h.mu.Unlock()
	}
	// Ephemeral connections time out: the ones dialled by then close SEGMENT_FULLDAS_EPHEMERAL_MS
	// after the slot start, inside the gap, so their teardown is in no payload's byte window.
	wg.Go(func() {
		select {
		case <-time.After(v.cfg.ephemeral):
			v.mu.Lock()
			ephem := v.ephem
			if ephem != nil {
				v.ephem, v.swept = make([][]peer.ID, v.n), true
			}
			v.mu.Unlock()
			v.closeEphemeral(ephem)
		case <-mctx.Done():
		}
	})
	// The source reads nothing, but its subscriptions deliver what it publishes: drain them.
	for _, s := range v.subs[v.pub] {
		wg.Go(func() {
			for {
				if _, err := s.sub.Next(mctx); err != nil {
					return
				}
			}
		})
	}
	for i := range v.n {
		if i == v.pub {
			continue
		}
		v.watchers.Add(1)
		wg.Go(func() {
			defer v.watchers.Done()
			v.watchNode(mctx, i, signal)
		})
	}
}

// publish sends the block from the source: every cell on its row topic and its column topic, row
// by row or in a random order, as one batch.
func (r *fdRun) publish(ctx context.Context, _ *testing.T) error {
	v := r.v
	b := v.block
	C := v.cfg.columns()
	order := make([]int, len(b.cells))
	for i := range order {
		order[i] = i
	}
	if v.cfg.randOrder {
		rng := rand.New(rand.NewPCG(v.seed^0x0DE5, uint64(b.m))) // lint:ignore uintcast -- a message number.
		rng.Shuffle(len(order), func(a, c int) { order[a], order[c] = order[c], order[a] })
	}
	var batch pubsub.MessageBatch
	for _, i := range order {
		if err := v.topics[v.pub][[2]int{fdDimRow, i / C}].AddToBatch(ctx, &batch, b.wire[fdDimRow][i]); err != nil {
			return err
		}
		if v.cfg.cols {
			if err := v.topics[v.pub][[2]int{fdDimCol, i % C}].AddToBatch(ctx, &batch, b.wire[fdDimCol][i]); err != nil {
				return err
			}
		}
	}
	return v.nw.Pubsubs[v.pub].PublishBatch(&batch)
}

// watchNode is node i's message: it reads its topics, forwards across its lines, samples, and
// signals when the cells it holds decode to the payload.
func (v *fullDASVariant) watchNode(ctx context.Context, i int, signal func(node int, at time.Duration)) {
	v.mu.Lock()
	b, start := v.block, v.start
	v.mu.Unlock()
	cfg, l, h := v.cfg, v.layout, v.holds[i]
	C := cfg.columns()
	st := &v.stats[i]
	in := make(chan fdArrival, 4*len(b.cells))
	// rctx is the watcher's life (its topic readers); qctx its requests', which completion ends
	// only under SEGMENT_FULLDAS_CANCEL and the message's end ends otherwise.
	rctx, cancel := context.WithCancel(ctx)
	defer cancel()
	qctx, qcancel := context.WithCancel(ctx)
	defer func() {
		if cfg.cancel {
			qcancel()
		}
	}()
	for _, s := range v.subs[i] {
		go func() {
			for {
				msg, err := s.sub.Next(rctx)
				if err != nil {
					return
				}
				k, ok := b.known[digestOf(msg.Data)]
				if !ok || k[1] != s.dim {
					continue // an earlier message's cell, or a misrouted one
				}
				select {
				case in <- fdArrival{cell: k[0], dim: s.dim, how: fdGossip}:
				case <-rctx.Done():
					return
				}
			}
		}()
	}
	r0, c0 := l.rowOf[i], l.colOf[i]
	inter := -1
	if cfg.cols && r0 >= 0 && c0 >= 0 {
		inter = r0*C + c0
	}
	// seen is, for the intersection cell, the topics it came in on or was published on.
	var seen [2]bool
	publishOn := func(dim, cell int) bool {
		if seen[dim] {
			return false
		}
		seen[dim] = true
		line := cell / C
		if dim == fdDimCol {
			line = cell % C
		}
		return v.topics[i][[2]int{dim, line}].Publish(rctx, b.wire[dim][cell]) == nil
	}
	rng := rand.New(rand.NewPCG(v.seed^uint64(i)<<20, uint64(b.m))) // lint:ignore uintcast -- a node index and a message number.
	asked := map[int][]int{}                                        // cell -> custodians asked
	var retry <-chan time.Time
	var want []int // the cells the node samples
	if cfg.sample {
		want = fdSampleSet(cfg, l, i, rng)
		v.sampleFrom(qctx, rctx.Done(), i, b, rng, asked, in, want, cfg.t1)
		retry = time.After(cfg.t1)
	}
	custodyDone := false
	for {
		var a fdArrival
		select {
		case a = <-in:
		case <-retry:
			h.mu.Lock()
			var missing []int
			for _, c := range want {
				if !h.held[c] {
					missing = append(missing, c)
				}
			}
			h.mu.Unlock()
			st.retries += len(missing)
			v.sampleFrom(qctx, rctx.Done(), i, b, rng, asked, in, missing, cfg.t2)
			retry = time.After(cfg.t2)
			continue
		case <-ctx.Done():
			return
		}
		h.mu.Lock()
		fresh := h.addLocked(cfg, []int{a.cell}, a.how)
		if len(fresh) == 0 {
			h.mu.Unlock()
			if a.how == fdSampled {
				st.dupes++
			}
			if a.how == fdGossip && a.cell == inter {
				seen[a.dim] = true
			}
			continue
		}
		h.serveLocked(fresh)
		// Without rebuild the node holds only what it received; the decoder peels a copy.
		decodable := fdDecodable(cfg, h.held)
		if !custodyDone {
			custodyDone = true
			for c := range b.cells {
				if l.custodies(i, c) && !h.held[c] {
					custodyDone = false
					break
				}
			}
			if custodyDone {
				st.custodyAt = time.Since(start)
			}
		}
		interVia := uint8(fdNone)
		if inter >= 0 && slices.Contains(fresh, inter) {
			interVia = h.via[inter]
		}
		var tally fdTally
		if decodable {
			for c, held := range h.held {
				if !held {
					tally.decoded++
					continue
				}
				switch h.via[c] {
				case fdGossip:
					tally.gossip++
				case fdSampled:
					tally.sampled++
				case fdRebuiltRow, fdRebuiltCol:
					if l.custodies(i, c) {
						tally.rebuiltCustody++
					} else {
						tally.rebuiltOther++
					}
				}
			}
			// Holding the payload, the node holds every cell: it answers what it still owes.
			for c := range h.held {
				if !h.held[c] {
					h.held[c] = true
					h.via[c] = fdDecoded
				}
			}
			h.done = true
			var all []int
			for c := range h.pending {
				all = append(all, c)
			}
			h.serveLocked(all)
		}
		h.mu.Unlock()
		if a.how == fdSampled {
			st.answered++
		}
		// Cross-forwarding: the intersection cell, received on one of its topics, goes on the other;
		// rebuilt by one of its lines, it goes on the other line's topic (Nim's repairCrossForward).
		if inter >= 0 {
			switch {
			case a.how == fdGossip && a.cell == inter:
				seen[a.dim] = true
				if cfg.cross && publishOn(1-a.dim, inter) {
					st.crossSent++
				}
			case interVia == fdRebuiltRow && cfg.rebuild:
				if publishOn(fdDimCol, inter) {
					st.rebuiltSent++
				}
			case interVia == fdRebuiltCol && cfg.rebuild:
				if publishOn(fdDimRow, inter) {
					st.rebuiltSent++
				}
			}
		}
		if !decodable {
			continue
		}
		at := time.Since(start)
		v.mu.Lock()
		st.tally, st.doneAt = tally, at // custodyAt stays 0 when the node held the payload first
		v.mu.Unlock()
		signal(i, at)
		if cfg.cancel {
			// Cancel first, so a stream still opening sees it under v.mu, then reset what is open.
			qcancel()
			v.mu.Lock()
			rs := v.reqs[i]
			v.reqs[i] = nil
			v.mu.Unlock()
			left := int64(0)
			for _, r := range rs {
				left += r.left.Load()
				_ = r.s.Reset()
			}
			st.cancelled = int(left)
		}
		return
	}
}

// sampleFrom asks custodians of node i for cells (every cell outside its custody when cells is nil)
// with the given timeout: per cell a connected custodian not asked yet, at random, else an
// ephemeral connection to a custodian of the cell's row (one per row, reused), else of its column.
// When every candidate has been asked, it cycles through them again (liteDAS's hunt). The source
// is never asked: it disperses and does not serve samples (das-research's proposerPublishOnly).
func (v *fullDASVariant) sampleFrom(ctx context.Context, watcher <-chan struct{}, i int, b *fdBlock, rng *rand.Rand, asked map[int][]int, in chan<- fdArrival, cells []int, ttl time.Duration) {
	cfg, l := v.cfg, v.layout
	C := cfg.columns()
	if cells == nil {
		for c := range b.cells {
			if !l.custodies(i, c) {
				cells = append(cells, c)
			}
		}
	}
	peers := make([]int, 0, len(l.adj[i]))
	for p := range l.adj[i] {
		if p != v.pub {
			peers = append(peers, p)
		}
	}
	sort.Ints(peers)
	v.mu.Lock()
	ephemSet := map[int]bool{}
	for _, pid := range v.ephem[i] {
		ephemSet[v.hostIdx[pid]] = true
	}
	v.mu.Unlock()
	rowEphem := map[int]int{}
	plan := map[int][]int{} // custodian -> cells
	var newEphem []int
	for _, c := range cells {
		p, ok := -1, false
		for was := asked[c]; !ok; was = nil {
			var cands []int
			for _, q := range peers {
				if l.custodies(q, c) && !slices.Contains(was, q) {
					cands = append(cands, q)
				}
			}
			if len(cands) == 0 {
				for q := range ephemSet {
					if l.custodies(q, c) && !slices.Contains(was, q) {
						cands = append(cands, q)
					}
				}
				sort.Ints(cands)
			}
			r := c / C
			if len(cands) > 0 {
				p, ok = cands[rng.IntN(len(cands))], true
			} else if q, has := rowEphem[r]; has && !slices.Contains(was, q) {
				p, ok = q, true
			} else {
				pool := l.rowMembers[r]
				if len(pool) == 0 && cfg.cols {
					pool = l.colMembers[c%C]
				}
				var fresh []int
				for _, q := range pool {
					if q != i && !l.adj[i][q] && !slices.Contains(was, q) && (l.colOf[q] < 0 || l.colOf[q] != l.colOf[i]) {
						fresh = append(fresh, q)
					}
				}
				if len(fresh) > 0 {
					p, ok = fresh[rng.IntN(len(fresh))], true
					rowEphem[r] = p
					if !ephemSet[p] {
						ephemSet[p] = true
						newEphem = append(newEphem, p)
					}
				}
			}
			if !ok && was == nil {
				break // no custodian at all
			}
		}
		if !ok {
			continue
		}
		asked[c] = append(asked[c], p)
		plan[p] = append(plan[p], c)
	}
	v.mu.Lock()
	for _, p := range newEphem {
		v.ephem[i] = append(v.ephem[i], v.nw.Hosts[p].ID())
	}
	v.mu.Unlock()
	custodians := make([]int, 0, len(plan))
	for p := range plan {
		custodians = append(custodians, p)
	}
	sort.Ints(custodians)
	for _, p := range custodians {
		cs := plan[p]
		dial := slices.Contains(newEphem, p)
		v.wg.Go(func() { v.request(ctx, watcher, i, p, dial, b, cs, ttl, in) })
	}
}

// fdReq is one open request stream and how many of its cells have not come.
type fdReq struct {
	s    network.Stream
	left atomic.Int64
}

// fdGrace is how long after a request's timeout its requester keeps the stream: the custodian
// drops the request that long after it read it, so nothing comes later than the answer's transit.
const fdGrace = time.Second

// request is one requester-custodian stream: dial if ephemeral, send the requests in one write,
// and pass every cell that comes back to the watcher while it runs, until all came, the stream
// ends, the timeout and fdGrace pass, or ctx (the requests' context) ends. Every cell read is
// counted, useful or not.
func (v *fullDASVariant) request(ctx context.Context, watcher <-chan struct{}, i, p int, dial bool, b *fdBlock, cells []int, ttl time.Duration, in chan<- fdArrival) {
	h := v.nw.Hosts[i]
	to := v.nw.Hosts[p]
	v.mu.Lock()
	gen := v.gen
	if v.active != nil {
		v.active[[2]int{i, p}]++
	}
	v.mu.Unlock()
	defer v.endRequest(gen, i, p)
	if dial {
		if err := h.Connect(ctx, peer.AddrInfo{ID: to.ID(), Addrs: to.Addrs()}); err != nil {
			return
		}
		v.dialled[i].Add(1)
	}
	s, err := h.Network().NewStream(network.WithAllowLimitedConn(ctx, "fulldas"), to.ID())
	if err != nil {
		return
	}
	if err := s.SetProtocol(fdProto); err != nil {
		_ = s.Reset()
		return
	}
	req := &fdReq{s: s}
	req.left.Store(int64(len(cells)))
	v.mu.Lock()
	if v.reqs == nil || ctx.Err() != nil {
		// The message ended, or the node completed and cancelled, while this stream was opening.
		v.mu.Unlock()
		_ = s.Reset()
		return
	}
	v.reqs[i] = append(v.reqs[i], req)
	v.mu.Unlock()
	rw := msmux.NewMSSelect(s, fdProto)
	buf := make([]byte, 0, fdReqLen*len(cells))
	for _, c := range cells {
		var f [fdReqLen]byte
		f[0] = fdFrameRq
		binary.BigEndian.PutUint32(f[1:], uint32(b.m))                // lint:ignore uintcast -- a small message number.
		binary.BigEndian.PutUint16(f[5:], uint16(c))                  // lint:ignore uintcast -- below 2^16.
		binary.BigEndian.PutUint32(f[7:], uint32(ttl.Milliseconds())) // lint:ignore uintcast -- a timeout in ms.
		buf = append(buf, f[:]...)
	}
	if _, err := rw.Write(buf); err != nil {
		_ = s.Reset()
		return
	}
	v.requested[i].Add(int64(len(cells)))
	if err := s.SetReadDeadline(time.Now().Add(ttl + fdGrace)); err != nil {
		_ = s.Reset()
		return
	}
	br := bufio.NewReader(rw)
	want := map[int]bool{}
	for _, c := range cells {
		want[c] = true
	}
	var hdr [fdCellHdr]byte
	for len(want) > 0 {
		if _, err := io.ReadFull(br, hdr[:]); err != nil || hdr[0] != fdFrameCl {
			_ = s.Reset()
			return
		}
		m := int(binary.BigEndian.Uint32(hdr[1:]))
		c := int(binary.BigEndian.Uint16(hdr[5:]))
		n := int(binary.BigEndian.Uint32(hdr[7:]))
		data := make([]byte, n)
		if _, err := io.ReadFull(br, data); err != nil {
			_ = s.Reset()
			return
		}
		v.sampleRx[i].Add(int64(n))
		v.sampleResp[i].Add(1)
		if m != b.m || c >= len(b.cells) || !bytes.Equal(data, b.cells[c]) {
			v.badCells.Add(1)
			continue
		}
		if want[c] {
			delete(want, c)
			req.left.Add(-1)
		}
		select {
		case in <- fdArrival{cell: c, dim: -1, how: fdSampled}:
		case <-watcher:
			// The node holds the payload and its watcher is gone: the cell is read and dropped.
		case <-ctx.Done():
			_ = s.Reset()
			return
		}
	}
	_ = s.Close()
}

// endRequest ends one of node i's requests to p from message gen. A connection to p dialled after
// the slot-relative close of ephemeral connections (by a retry) closes when its last request ends,
// rather than at the next message's start.
func (v *fullDASVariant) endRequest(gen, i, p int) {
	pid := v.nw.Hosts[p].ID()
	late := false
	v.mu.Lock()
	if v.gen == gen && v.active != nil {
		k := [2]int{i, p}
		v.active[k]--
		if v.active[k] == 0 && v.swept {
			if j := slices.Index(v.ephem[i], pid); j >= 0 {
				v.ephem[i] = slices.Delete(v.ephem[i], j, j+1)
				late = true
			}
		}
	}
	v.mu.Unlock()
	if late {
		_ = v.nw.Hosts[i].Network().ClosePeer(pid)
		v.lateClosed.Add(1)
	}
}

func (v *fullDASVariant) report(t *testing.T, s diffusionStats) {
	v.mu.Lock()
	st := slices.Clone(v.stats)
	v.mu.Unlock()
	pct := func(xs []time.Duration, q float64) time.Duration {
		if len(xs) == 0 {
			return 0
		}
		sort.Slice(xs, func(a, b int) bool { return xs[a] < xs[b] })
		return xs[min(len(xs)-1, int(q*float64(len(xs))))]
	}
	med := func(xs []int) int {
		if len(xs) == 0 {
			return 0
		}
		sort.Ints(xs)
		return xs[len(xs)/2]
	}
	mx := func(xs []int) int {
		m := 0
		for _, x := range xs {
			m = max(m, x)
		}
		return m
	}
	sum := func(xs []int) int {
		k := 0
		for _, x := range xs {
			k += x
		}
		return k
	}
	var custody, done []time.Duration
	var gossip, sampled, rcust, rother, decoded, cancelled []int                    // nodes holding the payload
	var requested, answered, dupes, resp, rx, dialled, retries, cross, rcross []int // every node
	for i := range st {
		if i == v.pub {
			continue
		}
		x := st[i]
		requested = append(requested, int(v.requested[i].Load()))
		answered = append(answered, x.answered)
		dupes = append(dupes, x.dupes)
		resp = append(resp, int(v.sampleResp[i].Load()))
		rx = append(rx, int(v.sampleRx[i].Load()))
		dialled = append(dialled, int(v.dialled[i].Load()))
		retries = append(retries, x.retries)
		cross = append(cross, x.crossSent)
		rcross = append(rcross, x.rebuiltSent)
		if x.doneAt == 0 {
			continue
		}
		if x.custodyAt > 0 {
			custody = append(custody, x.custodyAt)
		}
		done = append(done, x.doneAt)
		gossip = append(gossip, x.tally.gossip)
		sampled = append(sampled, x.tally.sampled)
		rcust = append(rcust, x.tally.rebuiltCustody)
		rother = append(rother, x.tally.rebuiltOther)
		decoded = append(decoded, x.tally.decoded)
		cancelled = append(cancelled, x.cancelled)
	}
	var ihave, iwant int
	for _, tr := range s.tracers {
		c := tr.ControlSeen()
		ihave += c.IhaveSent
		iwant += c.IwantSent
	}
	t.Logf("      fulldas: block %d cells of %d B (payload %d B compressed); %d nodes hold the payload; custody lines complete before holding at %d nodes, p50 %v p99 %v; holding p50 %v p99 %v",
		len(v.block.cells), v.block.unit, v.block.plen, len(done), len(custody), pct(custody, 0.5).Round(time.Millisecond), pct(custody, 0.99).Round(time.Millisecond),
		pct(done, 0.5).Round(time.Millisecond), pct(done, 0.99).Round(time.Millisecond))
	t.Logf("      fulldas cells per node when holding (median): gossip %d, sampled %d, rebuilt on custody lines %d, rebuilt on other lines %d, from the decode %d",
		med(gossip), med(sampled), med(rcust), med(rother), med(decoded))
	// Sampling over every node but the source, a node that never held the payload included; the
	// cancelled requests over the nodes holding it, which cancel at completion.
	t.Logf("      fulldas sampling per node (median, max): cells requested %d %d, cells read %d %d (new %d %d, already held %d %d), cancelled unanswered %d %d, ephemeral connections %d %d (%d closed at their last request's end), retried %d %d; sampled data read %d B median, %d B mean; custodians wrote %d cells in all, misdirected requests %d, bad cells %d",
		med(requested), mx(requested), med(resp), mx(resp), med(answered), mx(answered), med(dupes), mx(dupes), med(cancelled), mx(cancelled),
		med(dialled), mx(dialled), v.lateClosed.Load(), med(retries), mx(retries), med(rx), sum(rx)/max(1, len(rx)), v.served.Load(), v.misdirect.Load(), v.badCells.Load())
	t.Logf("      fulldas forwarding: cross-forwards %d, rebuilt cells forwarded %d; IHAVE sent %d, IWANT sent %d (all nodes)",
		sum(cross), sum(rcross), ihave, iwant)
	// The source's full upload: a copy of every cell to each mesh peer the source has on the cell's
	// topics (flood publishing is off; at most D per topic), at its own uplink. Not a completion
	// bound: nodes decode first.
	C, D := v.cfg.columns(), meshParams().D
	upBytes := 0
	for i := range v.block.cells {
		upBytes += min(v.layout.srcPerLine[0][i/C], D) * len(v.block.wire[fdDimRow][i])
		if v.cfg.cols {
			upBytes += min(v.layout.srcPerLine[1][i%C], D) * len(v.block.wire[fdDimCol][i])
		}
	}
	upload := time.Duration(float64(8*upBytes) / float64(v.srcUp) * float64(time.Second))
	flag := ""
	if s.drops > 0 {
		flag = fmt.Sprintf("  *** %d DROPS", s.drops)
	}
	t.Logf("%4dMbps L=%-5v n=%-3d %-10s %3d/%d  virtual %9v (source upload %8v)  %s  dups %6d  IDONTWANT tx %6d  dup-per-idw %5.2f  publisherTx %8dB  rx/node %8dB  ctrl rx/node %6dB  wall %v%s  [degree %d, mesh %s]",
		v.rate/simlibp2p.OneMbps, v.oneWay, s.n, "fulldas", s.completed, s.n-1,
		s.virt.Round(time.Millisecond), upload.Round(time.Millisecond),
		completionStats(s.compTimes, s.n-1, s.deadline),
		s.dups, s.idwSent, float64(s.dups)/math.Max(1, float64(s.idwSent)),
		s.publisherTx, s.rxBytes/s.n, s.ctrlRx/s.n, s.wall.Round(time.Millisecond), flag,
		connectivityDegree(t, s.n), s.meshSummary)
}

// TestFullDASCode checks the 2D code: the data grid is the compressed payload, and every row and
// every column decodes from any half of its cells.
func TestFullDASCode(t *testing.T) {
	cfg := &fdConfig{kR: 8, kC: 8, extend: true}
	data := fdPayload(1<<20, 1)
	cells, unit, err := fdEncode(cfg, data)
	require.NoError(t, err)
	R, C := cfg.rows(), cfg.columns()
	require.Equal(t, R*C, len(cells))
	var joined []byte
	for r := range cfg.kR {
		for c := range cfg.kC {
			joined = append(joined, cells[r*C+c]...)
		}
	}
	require.Equal(t, true, bytes.Equal(joined[:len(data)], data), "the data grid is the payload")
	require.Equal(t, (len(data)+63)/64, unit)
	rng := rand.New(rand.NewPCG(1, 2))
	check := func(line [][]byte, k int) {
		enc, err := reedsolomon.New(k, k)
		require.NoError(t, err)
		shards := make([][]byte, 2*k)
		for _, j := range rng.Perm(2 * k)[:k] {
			shards[j] = bytes.Clone(line[j])
		}
		require.NoError(t, enc.Reconstruct(shards))
		for j := range shards {
			require.Equal(t, true, bytes.Equal(shards[j], line[j]))
		}
	}
	for r := range R {
		check(cells[r*C:(r+1)*C], cfg.kC)
	}
	for c := range C {
		col := make([][]byte, R)
		for r := range R {
			col[r] = cells[r*C+c]
		}
		check(col, cfg.kR)
	}
}

// TestFullDASPeel checks the decoder: eight whole rows decode, as do eight cells of each of eight
// columns; seven whole columns and seven cells of every row do not.
func TestFullDASPeel(t *testing.T) {
	cfg := &fdConfig{kR: 8, kC: 8, extend: true}
	R, C := cfg.rows(), cfg.columns()
	set := func(f func(r, c int) bool) []bool {
		h := make([]bool, R*C)
		for i := range h {
			h[i] = f(i/C, i%C)
		}
		return h
	}
	require.Equal(t, true, fdDecodable(cfg, set(func(r, _ int) bool { return r >= 8 })))
	require.Equal(t, true, fdDecodable(cfg, set(func(r, c int) bool { return c%2 == 1 && r%2 == 0 })))
	require.Equal(t, false, fdDecodable(cfg, set(func(r, c int) bool { return c < 7 })))
	require.Equal(t, false, fdDecodable(cfg, set(func(r, c int) bool { return c < 7 || r < 7 })))
	require.Equal(t, true, fdDecodable(cfg, set(func(r, c int) bool { return c < 7 || r < 8 })))
	h := set(func(r, c int) bool { return r == 3 && c < 8 })
	filled := fdPeel(cfg, h, nil)
	require.Equal(t, 8, len(filled), "half a row rebuilds the row")
}

// TestFullDASLayout checks the custody draw and the graph at 500 nodes, seeds 7 to 16: every line has 31 or 32
// custodians, a node's links are its row and column peers (16, or 15 where its pair's other
// custodian is a peer in both lines; one more for the source's peers), and the source has one peer
// per line.
func TestFullDASLayout(t *testing.T) {
	for seed := uint64(7); seed <= 16; seed++ {
		fdCheckLayout(t, seed)
	}
}

func fdCheckLayout(t *testing.T, seed uint64) {
	cfg := &fdConfig{kR: 8, kC: 8, extend: true, cols: true, degree: 8}
	l := newFdLayout(t, cfg, 500, seed, nil)
	for _, ms := range append(slices.Clone(l.rowMembers), l.colMembers...) {
		require.Equal(t, true, len(ms) == 31 || len(ms) == 32, fmt.Sprintf("custodians per line %d", len(ms)))
	}
	pairs := map[[2]int]int{}
	for i := 1; i < 500; i++ {
		pairs[[2]int{l.rowOf[i], l.colOf[i]}]++
		row, col := 0, 0
		for p := range l.adj[i] {
			if p == 0 {
				continue
			}
			if l.rowOf[p] == l.rowOf[i] {
				row++
			}
			if l.colOf[p] == l.colOf[i] {
				col++
			}
			require.Equal(t, true, l.rowOf[p] == l.rowOf[i] || l.colOf[p] == l.colOf[i], "a link outside the node's lines")
		}
		// A peer with the node's row and column both (its pair's other custodian) counts twice.
		links := len(l.adj[i])
		if l.adj[i][0] {
			links--
		}
		require.Equal(t, true, row >= 8 && col >= 8 && links <= 16 && links >= 15, fmt.Sprintf("node %d: row peers %d, column peers %d, links %d", i, row, col, links))
	}
	require.Equal(t, 256, len(pairs), "every (row, column) pair has a custodian")
	require.Equal(t, 16, len(l.adj[0]))
	for d := range 2 {
		for _, k := range l.srcPerLine[d] {
			require.Equal(t, 1, k, "the source's peers per line")
		}
	}
}

// TestFullDASDecodeSet checks SEGMENT_FULLDAS_SAMPLE=decode's sample at 500 nodes, seeds 7 to 16,
// both picks: every node asks for 49 cells outside its custody, which decode with its custody lines;
// at seed 7 none of them can be left out; and the first-rows pick takes data rows only.
func TestFullDASDecodeSet(t *testing.T) {
	for _, first := range []bool{false, true} {
		cfg := &fdConfig{kR: 8, kC: 8, extend: true, cols: true, degree: 8, sample: true, decodeOnly: true, firstRows: first}
		for seed := uint64(7); seed <= 16; seed++ {
			l := newFdLayout(t, cfg, 500, seed, nil)
			for i := 1; i < 500; i++ {
				set := fdSampleSet(cfg, l, i, rand.New(rand.NewPCG(seed, uint64(i)))) // lint:ignore uintcast -- a node index.
				require.Equal(t, 49, len(set))
				held := make([]bool, 256)
				for c := range held {
					held[c] = l.custodies(i, c)
				}
				for _, c := range set {
					require.Equal(t, false, held[c], "a sampled cell in custody")
					held[c] = true
					if first && l.rowOf[i] < 8 {
						require.Equal(t, true, c/16 < 8, "first rows: a row past the data rows")
					}
				}
				require.Equal(t, true, fdDecodable(cfg, held), fmt.Sprintf("seed %d node %d: custody and sample do not decode", seed, i))
				if seed != 7 {
					continue
				}
				for _, c := range set {
					held[c] = false
					require.Equal(t, false, fdDecodable(cfg, held), fmt.Sprintf("node %d: decodes without cell %d", i, c))
					held[c] = true
				}
			}
		}
	}
}

// TestFullDASSamplingPaths runs a 30-node cell through the sampling paths the happy path leaves
// idle: timeouts of 20 ms expire every request before its cell exists, so nodes complete only
// through retries; with the ephemeral connections closed at 10 ms as well, the retries' new ones
// close at their last request's end; with cancellation off nothing is cancelled; sampling only
// enough to decode asks no node for more than 49 cells; and the source's random order delivers.
// Every node must complete.
func TestFullDASSamplingPaths(t *testing.T) {
	for _, tc := range []struct {
		name    string
		env     map[string]string
		asked   int // most cells a node may ask for, 0 for any
		retries bool
		cancels bool
		late    bool // late ephemeral connections closed at their requests' end
	}{
		{"expiry", map[string]string{"SEGMENT_FULLDAS_T1_MS": "20", "SEGMENT_FULLDAS_T2_MS": "20"}, 0, true, true, false},
		{"late", map[string]string{"SEGMENT_FULLDAS_T1_MS": "20", "SEGMENT_FULLDAS_T2_MS": "20", "SEGMENT_FULLDAS_EPHEMERAL_MS": "10"}, 0, true, true, true},
		{"nocancel", map[string]string{"SEGMENT_FULLDAS_CANCEL": "0"}, 0, false, false, false},
		{"decode", map[string]string{"SEGMENT_FULLDAS_SAMPLE": "decode"}, 49, false, true, false},
		{"order", map[string]string{"SEGMENT_FULLDAS_ORDER": "random"}, 0, false, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			params.SetupTestConfigCleanup(t)
			t.Setenv("SEGMENT_DEGREE", "16")
			for k, val := range tc.env {
				t.Setenv(k, val)
			}
			const n = 30
			v := newFullDASVariant(t, q6Cell{payloadLen: 1 << 20}, defaultRate, defaultLatency, n)
			runDiffusion(t, diffusionParams{n: n, rate: defaultRate, latency: defaultLatency, deadline: failureDeadline(t), seed: meshGraphSeed(t)}, v)
			retried, cancelled, done := 0, 0, 0
			for i, x := range v.stats {
				if i == v.pub {
					continue
				}
				retried += x.retries
				cancelled += x.cancelled
				if tc.asked > 0 {
					k := v.requested[i].Load()
					require.Equal(t, true, k <= int64(tc.asked), fmt.Sprintf("node %d requested %d cells", i, k))
				}
				if x.doneAt > 0 {
					done++
				}
			}
			require.Equal(t, n-1, done)
			require.Equal(t, tc.retries, retried > 0, fmt.Sprintf("retried %d", retried))
			if !tc.cancels {
				require.Equal(t, 0, cancelled)
			}
			require.Equal(t, int64(0), v.misdirect.Load())
			require.Equal(t, int64(0), v.badCells.Load())
			require.Equal(t, tc.late, v.lateClosed.Load() > 0, fmt.Sprintf("late ephemeral connections closed %d", v.lateClosed.Load()))
		})
	}
}
