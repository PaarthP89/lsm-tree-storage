package compaction

import (
	"bytes"
	"container/heap"
)

// SortedIterator is the shape a single compaction input needs: a sorted
// stream of (key, value, tombstone) entries. sstable.Iterator satisfies
// this directly; tests feed in-memory stand-ins so the merge logic is
// testable without touching disk.
type SortedIterator interface {
	Next() bool
	Key() []byte
	Value() []byte
	Tombstone() bool
}

// Source pairs a sorted input with its recency rank: lower Rank means
// newer. The merge uses this purely as a tie-breaker for duplicate keys
// across sources -- key order alone doesn't say which source's value is
// current, since every input can contain the same key.
type Source struct {
	Iter SortedIterator
	Rank int
}

// heapItem is one source's current head position, queued for comparison.
// value/tombstone aren't stored here: a source's iterator position (and
// therefore its Value()/Tombstone()) doesn't change between the moment an
// item is pushed and the moment it's popped, so those are read lazily,
// directly from the source, only for whichever item actually wins the pop.
type heapItem struct {
	key    []byte
	rank   int
	srcIdx int
}

type itemHeap []heapItem

func (h itemHeap) Len() int { return len(h) }
func (h itemHeap) Less(i, j int) bool {
	if c := bytes.Compare(h[i].key, h[j].key); c != 0 {
		return c < 0
	}
	// Equal keys: lower rank (newer) sorts first, so the newest source's
	// entry is always the one popped and emitted for that key.
	return h[i].rank < h[j].rank
}
func (h itemHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *itemHeap) Push(x any)   { *h = append(*h, x.(heapItem)) }
func (h *itemHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

// MergeIterator performs a k-way merge across Sources already sorted by
// key. For a key present in more than one source, only the newest
// source's entry (lowest Rank) is emitted -- every older duplicate is
// silently dropped during the merge itself, which is exactly the "keep
// only the newest value per key" step compaction needs (CLAUDE.md §7,
// compaction step 2). The result is itself a SortedIterator.
type MergeIterator struct {
	sources []Source
	h       itemHeap

	curKey       []byte
	curValue     []byte
	curTombstone bool
}

// NewMergeIterator builds a merge over sources. Each source's Iter must
// already be positioned before its first entry (i.e. Next() has not yet
// been called on it) -- the same contract every SortedIterator here uses.
func NewMergeIterator(sources []Source) *MergeIterator {
	m := &MergeIterator{sources: sources}
	m.h = make(itemHeap, 0, len(sources))
	for i, src := range sources {
		if src.Iter.Next() {
			heap.Push(&m.h, heapItem{key: src.Iter.Key(), rank: src.Rank, srcIdx: i})
		}
	}
	return m
}

// Next advances to the next distinct key across all sources, in sorted
// order. Duplicate keys from older sources are consumed and discarded
// internally -- callers never see them.
func (m *MergeIterator) Next() bool {
	if m.h.Len() == 0 {
		return false
	}

	leader := heap.Pop(&m.h).(heapItem)
	src := &m.sources[leader.srcIdx]
	// src.Iter's position hasn't moved since it was pushed, so this reads
	// the exact entry the heap ordered on.
	m.curKey = src.Iter.Key()
	m.curValue = src.Iter.Value()
	m.curTombstone = src.Iter.Tombstone()

	if src.Iter.Next() {
		heap.Push(&m.h, heapItem{key: src.Iter.Key(), rank: src.Rank, srcIdx: leader.srcIdx})
	}

	// Discard every other source's entry for this same key -- they're all
	// older (any newer duplicate would have sorted ahead of leader and
	// been popped first instead).
	for m.h.Len() > 0 && bytes.Equal(m.h[0].key, leader.key) {
		dup := heap.Pop(&m.h).(heapItem)
		dsrc := &m.sources[dup.srcIdx]
		if dsrc.Iter.Next() {
			heap.Push(&m.h, heapItem{key: dsrc.Iter.Key(), rank: dsrc.Rank, srcIdx: dup.srcIdx})
		}
	}

	return true
}

func (m *MergeIterator) Key() []byte     { return m.curKey }
func (m *MergeIterator) Value() []byte   { return m.curValue }
func (m *MergeIterator) Tombstone() bool { return m.curTombstone }
