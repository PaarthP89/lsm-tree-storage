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
// DB.Get already ranks them -- memtable newest, then every live L0 file
// (newest first), then every live L1 file whose range could overlap
// [start, end) (Phase 8d: L1 files never overlap each other, so their
// relative rank among themselves never matters, only that every one of
// them ranks older than every L0 file and the memtable). Each source is
// range-seeked to start first (memtable.SkipList.SeekIterator,
// sstable.SSTable.SeekIterator) rather than iterated from its own
// beginning and discarded up to start -- see those methods' doc comments
// for the O(log n)/bounded-scan mechanics.
//
// Scan loads the current state (Phase 8c: via the lock-free atomic state
// pointer, the same as Get) and snapshots it at the moment it's called --
// mutating the DB (Put, Delete, or a flush/compaction a later Put/Delete
// triggers) after Scan returns does not affect the already-returned
// iterator, which keeps walking the SSTable/memtable objects live at
// snapshot time.
//
// The one gap Phase 8c does NOT close: those objects are protected from
// being closed/removed out from under a reader only for the duration of
// this Scan call itself (see DB.inFlightReaders) -- once Scan returns,
// the caller's continued Next() calls on the returned iterator are no
// longer tracked. A sufficiently long-lived Scan iterator racing a
// concurrent compaction that retires one of its source SSTables could
// therefore see a read error from that source's Next() (the file was
// closed), though never memory corruption or a crash -- sstable.Iterator
// surfaces a closed file as a decode error via Err(), the same path as
// any other read failure. Fully closing this gap would need per-iterator
// lifecycle tracking (e.g. an explicit Close on the returned iterator,
// with the file-retirement side accounting for iterators that are never
// drained) -- a larger addition than this phase's stated scope, flagged
// here for a future phase rather than solved speculatively.
func (db *DB) Scan(start, end []byte) (memtable.Iterator, error) {
	if bytes.Compare(start, end) > 0 {
		return nil, ErrInvalidRange
	}

	db.beginRead()
	defer db.endRead()
	s := db.state.Load()

	sources := make([]compaction.Source, 0, 1+len(s.l0)+len(s.l1))
	sources = append(sources, compaction.Source{Iter: s.mem.SeekIterator(start), Rank: 0})
	rank := 1
	for _, st := range s.l0 {
		it, err := st.SeekIterator(start)
		if err != nil {
			return nil, err
		}
		sources = append(sources, compaction.Source{Iter: it, Rank: rank})
		rank++
	}
	for _, st := range s.l1 {
		m := st.Meta()
		// [start, end) is half-open: a file can only contribute keys < end
		// and > maxKey doesn't apply here since we need >= start too. A
		// file entirely before start (m.MaxKey < start) or entirely at or
		// past end (m.MinKey >= end, when end is non-empty) contributes
		// nothing and is safely skipped -- a pure optimization, since any
		// included-but-irrelevant file's entries are simply dropped by
		// rangeIterator's own end check regardless.
		if bytes.Compare(m.MaxKey, start) < 0 || bytes.Compare(m.MinKey, end) >= 0 {
			continue
		}
		it, err := st.SeekIterator(start)
		if err != nil {
			return nil, err
		}
		sources = append(sources, compaction.Source{Iter: it, Rank: rank})
		rank++
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
