package memtable

import (
	"bytes"
	"math/rand"
	"sync"
	"unsafe"
)

const (
	maxLevel = 32
	p        = 0.25
)

// pointerBytes is the size of one *node -- what a single entry in a
// node's forward slice costs. nodeStructOverheadBytes is the fixed cost
// of a node's own struct layout: the key and value slice headers, the
// tombstone bool (padded), and the forward slice's own three-word header
// -- everything except the forward slice's backing array, whose size
// depends on the node's randomly chosen level. That part is added
// separately, per node, using the exact level insert() already computed
// (not an average), since it's known at insert time.
//
// SizeBytes exists so DB can decide when a memtable has grown large
// enough to flush (CLAUDE.md §7); counting only raw key+value bytes
// would let actual heap usage run well ahead of the configured
// threshold, since every node also costs slice/pointer bookkeeping on
// top of the user's data. This is a much closer approximation of real
// heap usage than raw bytes alone, not an exact accounting -- it doesn't
// model the Go allocator's size-class rounding or GC bookkeeping.
const (
	pointerBytes            = int(unsafe.Sizeof((*node)(nil)))
	nodeStructOverheadBytes = int(unsafe.Sizeof(node{}))
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
	// nodeStructOverheadBytes + lvl*pointerBytes is charged once, here,
	// for the new node itself -- the early-return branch above updates
	// an *existing* node in place and must not re-charge it.
	s.sizeBytes += len(key) + len(value) + nodeStructOverheadBytes + lvl*pointerBytes
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
	// Cloned, not returned directly: DB is an embedded library whose
	// callers hold this slice past the call. Returning x.value's backing
	// array directly would let a caller's in-place mutation silently
	// corrupt this node for every future Get of the same key, since a
	// later Put on a different key never touches it.
	return cloneBytes(x.value), true, false
}

func (s *SkipList) SizeBytes() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.sizeBytes
}

// Iterator returns a sorted iterator over the skip list's entries as of
// the call. Each subsequent Next() call (Phase 8c) takes its own brief
// RLock to advance and copy out the entry it lands on, so the returned
// iterator is safe to keep pulling from concurrently with Put/Delete on
// this same list -- see skipListIterator.Next's doc comment for why this
// is a genuine safety fix, not just a convenience.
func (s *SkipList) Iterator() Iterator {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return &skipListIterator{s: s, next: s.head.forward[0]}
}

// SeekIterator returns a sorted iterator over entries with key >= start
// (Phase 8b range queries), found via the same O(log n) multi-level
// descent Get and insert already use to land on the predecessor of a
// target key -- not a full scan from the head that walks past and
// discards every entry before start. Safe for concurrent use with
// Put/Delete for the same reason Iterator is -- see skipListIterator.Next.
func (s *SkipList) SeekIterator(start []byte) Iterator {
	s.mu.RLock()
	defer s.mu.RUnlock()

	x := s.head
	for i := s.level - 1; i >= 0; i-- {
		for x.forward[i] != nil && bytes.Compare(x.forward[i].key, start) < 0 {
			x = x.forward[i]
		}
	}
	return &skipListIterator{s: s, next: x.forward[0]}
}

// skipListIterator walks forward pointers starting at next. Phase 8c
// (concurrent readers/writers): this list may be actively receiving
// Put/Delete calls from another goroutine for as long as this iterator
// is in use (e.g. a Scan iterator the caller keeps pulling from well
// after the Scan call itself returns), so simply following it.next.
// forward[0] without synchronization -- the original, single-threaded-era
// design -- is a real, race-detector-confirmed data race against a
// concurrent insert() relinking that same node's forward slice.
//
// The fix: every Next() call takes s's own RLock for the brief moment it
// reads one node's forward pointer and copies out its key/value/
// tombstone into the iterator's own fields, then releases it. This is
// safe even though a node's value/tombstone CAN be overwritten in place
// by a later Put on the same key (see insert()'s early-return branch):
// that overwrite always replaces the slice header with a reference to a
// brand-new cloned array (cloneBytes), it never mutates bytes within an
// existing array, so a slice header copied out under RLock keeps
// pointing at a backing array that's immutable from that point on --
// safe to read from after the lock is released, no matter what the node
// itself is mutated to afterward.
type skipListIterator struct {
	s    *SkipList
	next *node

	curKey       []byte
	curValue     []byte
	curTombstone bool
}

func (it *skipListIterator) Next() bool {
	it.s.mu.RLock()
	defer it.s.mu.RUnlock()

	if it.next == nil {
		return false
	}
	cur := it.next
	it.next = it.next.forward[0]
	it.curKey = cur.key
	it.curValue = cur.value
	it.curTombstone = cur.tombstone
	return true
}

func (it *skipListIterator) Key() []byte     { return it.curKey }
func (it *skipListIterator) Value() []byte   { return it.curValue }
func (it *skipListIterator) Tombstone() bool { return it.curTombstone }
