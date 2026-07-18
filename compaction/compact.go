package compaction

import (
	"fmt"

	"github.com/paarthsiphone/lsm-tree-storage/sstable"
)

// tombstoneDroppingIterator wraps a MergeIterator and skips every
// tombstone entry, never emitting it to the output at all.
//
// This is safe *specifically because* compaction here is single-level:
// Compact's inputs are always every currently-live SSTable, so the merge
// output replaces all of them at once -- there is no older data left
// underneath the result once compaction completes. A tombstone that
// survives to the end of the merge (i.e. it's the newest entry for that
// key across every input) therefore has nothing left to shadow, and can
// be dropped entirely rather than carried forward forever. If multi-level
// compaction (Phase 8d, stretch) is ever built, this assumption breaks:
// an L1+ tier could still hold a stale value under this one, and dropping
// the tombstone at a lower level would incorrectly let that stale value
// resurface on read. Revisit this exact spot if 8d is ever built.
type tombstoneDroppingIterator struct {
	src *MergeIterator
}

func (it *tombstoneDroppingIterator) Next() bool {
	for it.src.Next() {
		if !it.src.Tombstone() {
			return true
		}
	}
	return false
}
func (it *tombstoneDroppingIterator) Key() []byte     { return it.src.Key() }
func (it *tombstoneDroppingIterator) Value() []byte   { return it.src.Value() }
func (it *tombstoneDroppingIterator) Tombstone() bool { return false }

// Compact merges inputs into a single new SSTable at outputPath.
//
// inputs must be ordered newest-first (the same convention DB.sstables
// already uses) -- each input's index in the slice becomes its recency
// rank, so a key present in more than one input resolves to the value
// from whichever input appears earliest in the slice. Tombstones that are
// still the newest entry for their key after the merge are dropped from
// the output entirely (tombstoneDroppingIterator).
//
// The output is written via sstable.FlushIterator, so it gets the exact
// same temp-file -> fsync -> atomic-rename discipline a memtable flush
// does. Compact itself never touches the MANIFEST or deletes any input
// file -- installing the result (and retiring the inputs) is the caller's
// job, specifically so the caller can enforce the required ADDED-before-
// REMOVED MANIFEST edit ordering around this call.
func Compact(inputs []*sstable.SSTable, outputPath string) (*sstable.SSTableMeta, error) {
	sstIters := make([]*sstable.Iterator, len(inputs))
	sources := make([]Source, len(inputs))
	for i, in := range inputs {
		it := in.Iterator()
		sstIters[i] = it
		sources[i] = Source{Iter: it, Rank: i}
	}

	merged := NewMergeIterator(sources)
	out := &tombstoneDroppingIterator{src: merged}

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
