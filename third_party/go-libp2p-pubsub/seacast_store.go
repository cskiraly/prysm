package pubsub

// The cell store (seacast.md §4.2's fill and §4.5's resolution): a type's
// store holds a group's chunks once, under the type's own key, and answers an indexed ask after
// the message cache has let the message go. The store is the application's (the harness's column
// type keeps one per block; part 3's row type builds both wire forms from it, so a cell is held
// once and served under either id). The path fills it at ACCEPT and for the node's own publishes
// (§4.7: own data is stored without reception), and resolves an ask through it when mcache misses;
// the copies served to a peer count against GossipRetransmission as mcache's do.

import (
	"sync"

	"github.com/libp2p/go-libp2p/core/peer"
)

// CellStore is what the path asks of a type's store. Both methods run on the router goroutine.
type CellStore interface {
	// Put keeps a chunk of the group under its index: an accepted one, or the node's own.
	Put(root string, index uint32, msg *Message)
	// Get is the chunk held under (root, index), if any.
	Get(root string, index uint32) (*Message, bool)
}

// MapCellStore is a CellStore in a map: the harness's per-node store, and the tests'. The path
// calls Put and Get on the router goroutine; the owner may Reset, Drop and Len from its own (the
// harness resets the stores between blocks), so the map is under a mutex.
type MapCellStore struct {
	mu    sync.Mutex
	cells map[string]map[uint32]*Message
}

// NewMapCellStore makes an empty MapCellStore.
func NewMapCellStore() *MapCellStore { return &MapCellStore{cells: map[string]map[uint32]*Message{}} }

// Put keeps the first copy under (root, index).
func (s *MapCellStore) Put(root string, index uint32, msg *Message) {
	s.mu.Lock()
	defer s.mu.Unlock()
	g := s.cells[root]
	if g == nil {
		g = map[uint32]*Message{}
		s.cells[root] = g
	}
	if _, held := g[index]; !held {
		g[index] = msg
	}
}

// Get is the chunk under (root, index).
func (s *MapCellStore) Get(root string, index uint32) (*Message, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	m, ok := s.cells[root][index]
	return m, ok
}

// Drop forgets a group (the application's expiry).
func (s *MapCellStore) Drop(root string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.cells, root)
}

// Reset forgets everything (the harness's next block).
func (s *MapCellStore) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cells = map[string]map[uint32]*Message{}
}

// Len is the chunks held over every group.
func (s *MapCellStore) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, g := range s.cells {
		n += len(g)
	}
	return n
}

// seacastStorePut is the fill's store half: a chunk goes into its type's store under the index
// its id carries. Router goroutine, from seacastAccepted and seacastStoreOwn.
func (gs *GossipSubRouter) seacastStorePut(T ObjectType, root string, index uint32, msg *Message) {
	st := T.Store()
	if st == nil {
		return
	}
	st.Put(root, index, msg)
	gs.seacast.ctr.Stored.Add(1)
}

// seacastStoreOwn files the node's own batch-published chunks on the path's topics (§4.7). Router
// goroutine, from PublishBatch; a single own publish reaches the fill through Publish.
func (gs *GossipSubRouter) seacastStoreOwn(messages []*Message) {
	if gs.seacast == nil || gs.have == nil {
		return
	}
	for _, msg := range messages {
		T := gs.seacastType(msg.GetTopic())
		if T == nil {
			continue
		}
		mid := gs.p.idGen.ID(msg)
		gs.seacastNoteRoot(msg.GetTopic(), mid)
		if root, index, ok := gs.have.index(mid); ok && len(root) == haveRootLen {
			gs.seacastPlaceAccepted(T, root) // the group on its type's topic, as an accepted chunk places it
			gs.seacastDropFilledWait(root, mid)
			gs.seacastStorePut(T, root, index, msg)
			gs.seacastServeStanding(T, root, index, msg) // §4.5: a standing request answered from the node's own publication
		}
	}
}

// seacastStoreGet answers an ask the message cache could not: the id's root names a type on the
// path whose store holds the chunk. The count against the retransmission bound is
// seacastServeCount's, and handleIWant counts the answer (ServedFromStore) once the bound let it
// through. Router goroutine.
func (gs *GossipSubRouter) seacastStoreGet(mid string) (*Message, bool) {
	sc := gs.seacast
	if sc == nil || gs.have == nil {
		return nil, false
	}
	root, index, ok := gs.have.index(mid)
	if !ok || len(root) != haveRootLen {
		return nil, false
	}
	T := gs.seacastRootType(root)
	if T == nil {
		return nil, false
	}
	st := T.Store()
	if st == nil {
		return nil, false
	}
	return st.Get(root, index)
}

// seacastServeCount is the one count of the answers to p for an id of the path, whichever of the
// message cache and the store answers (the draft's serve_count[p][mid], §4.5: three answers per id
// per peer): incremented per ask resolved and returned for handleIWant's retransmission test.
// onPath is false for an id of no path root, whose count stays the cache's. The cache keeps its
// own count per message and drops it when the message ages out (mcache.Shift), so two counts
// would let a peer pull the bound from each. Router goroutine.
func (gs *GossipSubRouter) seacastServeCount(mid string, p peer.ID) (count int, onPath bool) {
	sc := gs.seacast
	if sc == nil || gs.have == nil {
		return 0, false
	}
	root, index, ok := gs.have.index(mid)
	if !ok || len(root) != haveRootLen || gs.seacastRootType(root) == nil {
		return 0, false
	}
	if sc.serveTx == nil {
		sc.serveTx = map[string]map[uint32]map[peer.ID]int{}
	}
	g := sc.serveTx[root]
	if g == nil {
		g = map[uint32]map[peer.ID]int{}
		sc.serveTx[root] = g
	}
	tx := g[index]
	if tx == nil {
		tx = map[peer.ID]int{}
		g[index] = tx
	}
	tx[p]++
	return tx[p], true
}

// storeForget drops the served-copies counts of the groups named (expired).
func (sc *seacast) storeForget(roots []string) {
	for _, r := range roots {
		delete(sc.serveTx, r)
	}
}
