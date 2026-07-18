package lsm

import (
	"bytes"
	"errors"

	"github.com/paarthsiphone/lsm-tree-storage/compaction"
	"github.com/paarthsiphone/lsm-tree-storage/memtable"
)

// ErrInvalidRange is returned by Scan when start > end.
var ErrInvalidRange = errors.New("lsm: invalid range: start > end")

func cloneBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out
}

// Scan returns a sorted, deduplicated, tombstone-filtered iterator over
// every live key in the half-open range [start, end) across the
// memtable and every current SSTable. A key present in more than one
// source appears exactly once, with the newest value -- the same
// newest-wins rule Get applies (§7's read path), generalized from a
// single key to a range.
//
// start == end is valid and returns an iterator that reports done on the
// first Next() call, not an error. start > end returns ErrInvalidRange
// without constructing an iterator at all.
//
// The merge itself reuses compaction.MergeIterator (Phase 6) rather than
// reimplementing k-way merge/newest-wins resolution a second time: the
// memtable and each SSTable become one Source apiece, ranked exactly like
// DB.Get and MaybeCompact already rank them (memtable newest, then
// db.sstables in their existing newest-first order). Each source is
// range-seeked to start first (memtable.SkipList.SeekIterator,
// sstable.SSTable.SeekIterator) rather than iterated from its own
// beginning and discarded up to start -- see those methods' doc comments
// for the O(log n)/bounded-scan mechanics.
//
// Scan snapshots db.mem and db.sstables at the moment it's called. It
// does not hold any lock beyond that: mutating the DB (Put, Delete, or a
// flush/compaction a later Put/Delete triggers) while the returned
// iterator is still in use is undefined behavior. This is the same "DB
// is not safe for concurrent use" gap already documented in CLAUDE.md
// §11 (maybeFlush reassigns db.mem/db.sstables/db.w with no
// synchronization) -- Scan doesn't attempt to fix it, since that's
// Phase 8c's own scope, not this one's.
func (db *DB) Scan(start, end []byte) (memtable.Iterator, error) {
	if bytes.Compare(start, end) > 0 {
		return nil, ErrInvalidRange
	}

	sources := make([]compaction.Source, 0, 1+len(db.sstables))
	sources = append(sources, compaction.Source{Iter: db.mem.SeekIterator(start), Rank: 0})
	for i, st := range db.sstables {
		it, err := st.SeekIterator(start)
		if err != nil {
			return nil, err
		}
		// Rank i+1: db.sstables is already newest-first (rank 0 is taken
		// by the memtable), matching the exact convention MaybeCompact
		// uses when it builds compaction.Source values from the same
		// slice.
		sources = append(sources, compaction.Source{Iter: it, Rank: i + 1})
	}

	return &rangeIterator{src: compaction.NewMergeIterator(sources), end: end}, nil
}

// rangeIterator adapts a compaction.MergeIterator into Scan's contract:
// stop once the current key reaches end (the half-open upper bound
// MergeIterator itself knows nothing about), and never yield a
// tombstone -- Scan's Iterator return shape has no way to signal
// "deleted" the way Get's found/tombstone pair does, so a key whose
// newest entry is a delete marker must simply never appear at all.
type rangeIterator struct {
	src      *compaction.MergeIterator
	end      []byte
	curKey   []byte
	curValue []byte
	done     bool
}

func (it *rangeIterator) Next() bool {
	if it.done {
		return false
	}
	for it.src.Next() {
		if bytes.Compare(it.src.Key(), it.end) >= 0 {
			it.done = true
			return false
		}
		if it.src.Tombstone() {
			continue
		}
		// Cloned for the same reason SkipList.Get and sstable's flush
		// path clone: Scan's caller is expected to hold results (e.g.
		// collect into a slice) well past a single Next() cycle, unlike
		// FlushIterator's tight internal loop, so an alias into a
		// source's internal buffer (a skip-list node's key, an SSTable
		// Iterator's decode buffer that a later Next() will overwrite)
		// isn't safe to hand back directly.
		it.curKey = cloneBytes(it.src.Key())
		it.curValue = cloneBytes(it.src.Value())
		return true
	}
	it.done = true
	return false
}

func (it *rangeIterator) Key() []byte   { return it.curKey }
func (it *rangeIterator) Value() []byte { return it.curValue }

// Tombstone always reports false: rangeIterator's Next never surfaces a
// tombstoned key in the first place (see the doc comment above), so
// there is never a "found but deleted" result for a caller to check.
func (it *rangeIterator) Tombstone() bool { return false }
