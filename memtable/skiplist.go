package memtable

import (
	"bytes"
	"math/rand"
	"sync"
)

const (
	maxLevel = 32
	p        = 0.25
)

// Iterator walks a Memtable's entries in ascending key order. Call Next
// before the first Key/Value/Tombstone access; Next returns false once
// exhausted, after which Key/Value/Tombstone are undefined.
type Iterator interface {
	Next() bool
	Key() []byte
	Value() []byte
	Tombstone() bool
}

// Memtable is the in-memory write buffer backing the active WAL segment.
type Memtable interface {
	Put(key, value []byte)
	Delete(key []byte) // writes a tombstone
	Get(key []byte) (value []byte, found bool, tombstone bool)
	Iterator() Iterator // sorted by key, needed for Phase 3 flush
	SizeBytes() int
}

type node struct {
	key       []byte
	value     []byte
	tombstone bool
	forward   []*node
}

// SkipList is a probabilistic-balancing skip list implementing Memtable.
// It is safe for concurrent use: a single RWMutex guards the whole
// structure.
type SkipList struct {
	mu        sync.RWMutex
	head      *node
	level     int
	sizeBytes int
}

// New returns an empty SkipList.
func New() *SkipList {
	return &SkipList{
		head:  &node{forward: make([]*node, maxLevel)},
		level: 1,
	}
}

func randomLevel() int {
	lvl := 1
	for lvl < maxLevel && rand.Float64() < p {
		lvl++
	}
	return lvl
}

func cloneBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out
}

func (s *SkipList) Put(key, value []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.insert(key, value, false)
}

func (s *SkipList) Delete(key []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.insert(key, nil, true)
}

func (s *SkipList) insert(key, value []byte, tombstone bool) {
	var update [maxLevel]*node
	x := s.head
	for i := s.level - 1; i >= 0; i-- {
		for x.forward[i] != nil && bytes.Compare(x.forward[i].key, key) < 0 {
			x = x.forward[i]
		}
		update[i] = x
	}
	x = x.forward[0]

	if x != nil && bytes.Equal(x.key, key) {
		s.sizeBytes += len(value) - len(x.value)
		x.value = cloneBytes(value)
		x.tombstone = tombstone
		return
	}

	lvl := randomLevel()
	if lvl > s.level {
		for i := s.level; i < lvl; i++ {
			update[i] = s.head
		}
		s.level = lvl
	}

	n := &node{
		key:       cloneBytes(key),
		value:     cloneBytes(value),
		tombstone: tombstone,
		forward:   make([]*node, lvl),
	}
	for i := 0; i < lvl; i++ {
		n.forward[i] = update[i].forward[i]
		update[i].forward[i] = n
	}
	s.sizeBytes += len(key) + len(value)
}

func (s *SkipList) Get(key []byte) (value []byte, found bool, tombstone bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	x := s.head
	for i := s.level - 1; i >= 0; i-- {
		for x.forward[i] != nil && bytes.Compare(x.forward[i].key, key) < 0 {
			x = x.forward[i]
		}
	}
	x = x.forward[0]
	if x == nil || !bytes.Equal(x.key, key) {
		return nil, false, false
	}
	if x.tombstone {
		return nil, true, true
	}
	return x.value, true, false
}

func (s *SkipList) SizeBytes() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.sizeBytes
}

// Iterator returns a sorted iterator over the skip list's entries as of
// the call. Traversal after this call is not synchronized with
// concurrent writes to the same list -- callers that need a stable view
// (e.g. Phase 3's flush) must ensure the list isn't mutated while the
// iterator is in use.
func (s *SkipList) Iterator() Iterator {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return &skipListIterator{next: s.head.forward[0]}
}

type skipListIterator struct {
	next *node
	cur  *node
}

func (it *skipListIterator) Next() bool {
	if it.next == nil {
		it.cur = nil
		return false
	}
	it.cur = it.next
	it.next = it.next.forward[0]
	return true
}

func (it *skipListIterator) Key() []byte     { return it.cur.key }
func (it *skipListIterator) Value() []byte   { return it.cur.value }
func (it *skipListIterator) Tombstone() bool { return it.cur.tombstone }
