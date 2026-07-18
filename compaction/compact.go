package compaction

import (
	"fmt"

	"github.com/paarthsiphone/lsm-tree-storage/sstable"
)

// tombstoneDroppingIterator wraps a MergeIterator. When canDrop is true,
// every tombstone entry is skipped, never emitted to the output at all.
// When canDrop is false, tombstones pass through unchanged -- exactly
// like every other entry -- because the caller could not prove every
// source holding a possibly-older, still-relevant value for that key was
// included in this merge (see CompactLeveled's doc comment).
//
// Dropping is only ever safe when the merge's inputs are the *entire*
// set of sources that could hold an older entry for any key it touches:
// a tombstone that's still the newest entry for its key after such a
// merge has nothing left to shadow, so it can be dropped entirely rather
// than carried forward forever. Phase 8d (two-level compaction) is what
// makes this no longer automatic: an L0->L1 or L1->L1 compaction that
// doesn't cover every relevant source could otherwise let a stale value
// resurface on a later read. See DB.compactL0ToL1/compactL1 for the two
// coverage proofs actually used in this codebase, and CLAUDE.md §8.
type tombstoneDroppingIterator struct {
	src     *MergeIterator
	canDrop bool
}

func (it *tombstoneDroppingIterator) Next() bool {
	if !it.canDrop {
		return it.src.Next()
	}
	for it.src.Next() {
		if !it.src.Tombstone() {
			return true
		}
	}
	return false
}
func (it *tombstoneDroppingIterator) Key() []byte   { return it.src.Key() }
func (it *tombstoneDroppingIterator) Value() []byte { return it.src.Value() }
func (it *tombstoneDroppingIterator) Tombstone() bool {
	if it.canDrop {
		return false
	}
	return it.src.Tombstone()
}

// Compact merges inputs into a single new SSTable at outputPath,
// unconditionally dropping tombstones. Equivalent to
// CompactLeveled(inputs, outputPath, true) -- preserved under its
// original name/signature for every caller (and test) that predates
// Phase 8d and always passes every live SSTable as inputs, matching the
// single-level tombstone-GC invariant the true (always-safe) argument
// represents.
func Compact(inputs []*sstable.SSTable, outputPath string) (*sstable.SSTableMeta, error) {
	return compactLeveled(inputs, outputPath, true)
}

// CompactLeveled merges inputs into a single new SSTable at outputPath.
//
// inputs must be ordered newest-first (the same convention DB.l0 already
// uses) -- each input's index in the slice becomes its recency rank, so a
// key present in more than one input resolves to the value from
// whichever input appears earliest in the slice.
//
// canDropTombstones must be true only when the caller can prove every
// source that might hold an older, still-relevant entry for any key
// touched by this merge is included among inputs -- never inferred or
// assumed by this function itself. Passing true when that isn't actually
// true is a real, silent data-resurrection bug: a stale value living
// outside this merge's inputs would incorrectly become visible again once
// the tombstone that shadowed it is gone. See CompactLeveled's callers in
// DB for the specific coverage arguments used in this codebase (Phase 8d,
// CLAUDE.md §8).
//
// The output is written via sstable.FlushIterator, so it gets the exact
// same temp-file -> fsync -> atomic-rename discipline a memtable flush
// does. CompactLeveled itself never touches the MANIFEST or deletes any
// input file -- installing the result (and retiring the inputs) is the
// caller's job, specifically so the caller can enforce the required
// ADDED-before-REMOVED MANIFEST edit ordering around this call.
func CompactLeveled(inputs []*sstable.SSTable, outputPath string, canDropTombstones bool) (*sstable.SSTableMeta, error) {
	return compactLeveled(inputs, outputPath, canDropTombstones)
}

func compactLeveled(inputs []*sstable.SSTable, outputPath string, canDropTombstones bool) (*sstable.SSTableMeta, error) {
	sstIters := make([]*sstable.Iterator, len(inputs))
	sources := make([]Source, len(inputs))
	for i, in := range inputs {
		it := in.Iterator()
		sstIters[i] = it
		sources[i] = Source{Iter: it, Rank: i}
	}

	merged := NewMergeIterator(sources)
	out := &tombstoneDroppingIterator{src: merged, canDrop: canDropTombstones}

	meta, err := sstable.FlushIterator(out, outputPath)
	if err != nil {
		return nil, fmt.Errorf("compaction: %w", err)
	}

	// FlushIterator drains every source down to EOF. A corrupt record deep
	// in an input file surfaces as Next()==false, indistinguishable from a
	// clean end unless each source's terminal error state is checked
	// explicitly -- silently treating corruption as "no more entries"
	// would be exactly the silent-data-loss class of bug this codebase
	// refuses elsewhere (sstable.OpenSSTable's footer checksum, Get's
	// decode-error path).
	for _, it := range sstIters {
		if err := it.Err(); err != nil {
			return nil, fmt.Errorf("compaction: reading input during merge: %w", err)
		}
	}

	return meta, nil
}
