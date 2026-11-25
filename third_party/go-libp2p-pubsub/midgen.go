package pubsub

import (
	"runtime"
	"sync"
	"weak"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"
)

// msgIDGenerator handles computing IDs for msgs
// It allows setting custom generators(MsgIdFunction) per topic
type msgIDGenerator struct {
	sync.Mutex
	Default MsgIdFunction

	topicGensLk sync.RWMutex
	topicGens   map[string]MsgIdFunction

	// ids holds the id of each message object computed here, until the object is collected: the
	// key is a weak pointer, so the map keeps no message alive, and a cleanup removes the entry.
	// An id function may be costly (Prysm's snappy-decodes and hashes the payload), and the router
	// asks for the id of the same *pb.Message at many places (the queues' HAVE index, the
	// IDONTWANT filters, the queues' order) through RawID, which had no cache. A message is not
	// changed after its id is first computed (its fields are set at publish, before any id), and
	// a copy is a new object with its own entry. idsLk guards the map only; an entry's id is
	// computed outside it, once, so a hit never waits behind another message's computation. idsLk
	// is taken after the generator's own lock (ID) and never before it.
	idsLk sync.Mutex
	ids   map[weak.Pointer[pb.Message]]*idEntry
}

// idEntry is a message object's id, computed by the first RawID to need it.
type idEntry struct {
	once sync.Once
	id   string
}

func newMsgIdGenerator() *msgIDGenerator {
	return &msgIDGenerator{
		Default:   DefaultMsgIdFn,
		topicGens: make(map[string]MsgIdFunction),
	}
}

// Set sets custom id generator(MsgIdFunction) for topic. It clears the cache of every topic. It is
// meant for a topic's options at Join (WithTopicMessageIdFn), before the topic's messages flow, as
// Default is set once at construction (WithMessageIdFn), before any message:
// a message whose id was taken under the previous function keeps it in its Message.ID and in the
// router's state, and a RawID running while Set runs may return the previous function's id.
func (m *msgIDGenerator) Set(topic string, gen MsgIdFunction) {
	m.topicGensLk.Lock()
	m.topicGens[topic] = gen
	m.topicGensLk.Unlock()
	m.idsLk.Lock() // an id cached under the topic's previous function is no longer its id
	clear(m.ids)
	m.idsLk.Unlock()
}

// ID computes ID for the msg or short-circuits with the cached value.
func (m *msgIDGenerator) ID(msg *Message) string {
	m.Lock()
	defer m.Unlock()
	if msg.ID != "" {
		return msg.ID
	}

	msg.ID = m.RawID(msg.Message)
	return msg.ID
}

// RawID computes ID for the proto 'msg', once per message object: a later call returns the id
// computed first, while the object lives.
func (m *msgIDGenerator) RawID(msg *pb.Message) string {
	if msg == nil {
		return m.compute(msg)
	}
	k := weak.Make(msg)
	m.idsLk.Lock()
	e, ok := m.ids[k]
	if !ok {
		e = &idEntry{}
		if m.ids == nil {
			m.ids = make(map[weak.Pointer[pb.Message]]*idEntry)
		}
		m.ids[k] = e
		runtime.AddCleanup(msg, m.forget, k)
	}
	m.idsLk.Unlock()
	// Two goroutines asking at once for one message compute it once; the second waits for the first.
	e.once.Do(func() { e.id = m.compute(msg) })
	return e.id
}

// forget removes a collected message's id.
func (m *msgIDGenerator) forget(k weak.Pointer[pb.Message]) {
	m.idsLk.Lock()
	delete(m.ids, k)
	m.idsLk.Unlock()
}

// compute runs the topic's id function, or the default one.
func (m *msgIDGenerator) compute(msg *pb.Message) string {
	m.topicGensLk.RLock()
	gen, ok := m.topicGens[msg.GetTopic()]
	m.topicGensLk.RUnlock()
	if !ok {
		gen = m.Default
	}

	return gen(msg)
}
