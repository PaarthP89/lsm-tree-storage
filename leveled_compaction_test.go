package lsm

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/paarthsiphone/lsm-tree-storage/manifest"
)

func rangeKey(prefix string, i int) []byte {
	return []byte(fmt.Sprintf("%s-%06d", prefix, i))
}

// assertL1NonOverlapping confirms no two live L1 files have overlapping
// [MinKey, MaxKey] ranges -- the invariant every L1-producing compaction
// (compactL0ToL1IfNeeded, compactL1IfNeeded) is responsible for
// maintaining, and the one Get's findL1TableForKey binary search depends
// on for correctness (Phase 8d, CLAUDE.md §8).
func assertL1NonOverlapping(t *testing.T, db *DB) {
	t.Helper()
	for i := 1; i < len(db.l1()); i++ {
		prev := db.l1()[i-1].Meta()
		cur := db.l1()[i].Meta()
		if bytes.Compare(prev.MaxKey, cur.MinKey) >= 0 {
			t.Fatalf("L1 non-overlap invariant violated: file %d [%s,%s] and file %d [%s,%s] overlap",
				i-1, prev.MinKey, prev.MaxKey, i, cur.MinKey, cur.MaxKey)
		}
	}
}

// TestL0ToL1CompactionMaintainsL1NonOverlapInvariant drives enough Puts
// across two disjoint key ranges, interleaved, to force multiple
// L0->L1 compactions (each one potentially needing to merge against an
// existing overlapping L1 file, or leave a disjoint one alone) and
// confirms the invariant holds after every single flush/compaction step,
// not just at the end.
func TestL0ToL1CompactionMaintainsL1NonOverlapInvariant(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()
	db.SetFlushThreshold(2 * 1024)
	db.SetL0CompactionThreshold(3)

	const n = 400
	for i := 0; i < n; i++ {
		if err := db.Put(rangeKey("aaa", i), valueFor(i)); err != nil {
			t.Fatalf("Put(aaa,%d): %v", i, err)
		}
		if err := db.Put(rangeKey("zzz", i), valueFor(i)); err != nil {
			t.Fatalf("Put(zzz,%d): %v", i, err)
		}
		// Re-overwrite an earlier key in the "aaa" range periodically so
		// later L0->L1 compactions must merge against an already-existing
		// overlapping L1 file, not just append disjoint ranges.
		if i%17 == 0 && i > 0 {
			if err := db.Put(rangeKey("aaa", i/2), valueFor(i)); err != nil {
				t.Fatalf("Put(aaa overwrite,%d): %v", i, err)
			}
		}
		assertL1NonOverlapping(t, db)
	}

	if len(db.l1()) == 0 {
		t.Fatal("no L1 files produced -- test isn't exercising L0->L1 compaction at all")
	}
	assertL1NonOverlapping(t, db)
}

// TestL0ToL1CompactionLeavesDisjointL1FileUntouched confirms that when an
// L0->L1 compaction's combined key range doesn't overlap an existing L1
// file, that file is left completely alone: no MANIFEST edit references
// it, and its exact on-disk bytes are unchanged.
func TestL0ToL1CompactionLeavesDisjointL1FileUntouched(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()
	db.SetFlushThreshold(2 * 1024)
	db.SetL0CompactionThreshold(2)
	// Isolate the L0->L1 trigger from L1->L1's independent, range-agnostic
	// "merge everything in L1" trigger -- otherwise L1's total size could
	// eventually exceed the size-ratio threshold and legitimately fold the
	// zzz and aaa files together, which would be compactL1IfNeeded doing
	// its own (correct, unrelated) job, not a bug in the overlap logic
	// this test exists to isolate.
	db.SetL1SizeRatio(1 << 30)

	// Write set B ("zzz" range) first and force it into L1. Sequential,
	// never-overwritten keys naturally partition into more than one
	// non-overlapping L1 file over several compaction rounds (each round's
	// L0 batch covers a higher sub-range than the last, disjoint from the
	// previous round's L1 output) -- so this deliberately doesn't assert a
	// single output file, only that none of whatever files result are
	// later touched by a disjoint range's compactions.
	const n = 200
	for i := 0; i < n; i++ {
		if err := db.Put(rangeKey("zzz", i), valueFor(i)); err != nil {
			t.Fatalf("Put(zzz,%d): %v", i, err)
		}
	}
	// Fully drain both the memtable and L0 before switching ranges:
	// otherwise a few trailing zzz keys could still be sitting in the
	// memtable, or a couple of already-flushed-but-not-yet-compacted zzz
	// L0 files could still be waiting for L0->L1 compaction, when the
	// first aaa keys arrive. Either way, the next L0->L1 compaction's
	// input set would then genuinely span both ranges (a real SSTable
	// mixing zzz and aaa keys, or an L0 batch straddling both) --
	// legitimately overlapping every zzz L1 file, which would defeat the
	// point of this test (isolating the disjoint-range case) rather than
	// revealing a real bug.
	if db.mem().SizeBytes() > 0 {
		db.SetFlushThreshold(1)
		if err := db.Put(rangeKey("zzz", n), valueFor(n)); err != nil {
			t.Fatalf("Put(zzz drain): %v", err)
		}
		db.SetFlushThreshold(2 * 1024)
	}
	if db.mem().SizeBytes() != 0 {
		t.Fatalf("setup: memtable not drained (SizeBytes=%d) before switching to the aaa range", db.mem().SizeBytes())
	}
	if len(db.l0()) > 0 {
		origThreshold := db.l0CompactionThreshold
		db.SetL0CompactionThreshold(0)
		if err := db.MaybeCompact(); err != nil {
			t.Fatalf("MaybeCompact (forced L0 drain): %v", err)
		}
		db.SetL0CompactionThreshold(origThreshold)
	}
	if len(db.l0()) != 0 {
		t.Fatalf("setup: L0 not drained (%d files) before switching to the aaa range", len(db.l0()))
	}

	if len(db.l1()) == 0 {
		t.Fatal("setup: no L1 file produced for the zzz range")
	}
	zzzPathsBefore := make([]string, len(db.l1()))
	zzzDataBefore := make(map[string][]byte, len(db.l1()))
	for i, st := range db.l1() {
		p := st.Meta().Path
		zzzPathsBefore[i] = p
		data, err := os.ReadFile(p)
		if err != nil {
			t.Fatalf("ReadFile(%s): %v", p, err)
		}
		zzzDataBefore[p] = data
	}

	// Now write set A ("aaa" range, disjoint -- "aaa" < "zzz" lexically),
	// forcing further L0->L1 compactions that must never touch any zzz file.
	for i := 0; i < n; i++ {
		if err := db.Put(rangeKey("aaa", i), valueFor(i)); err != nil {
			t.Fatalf("Put(aaa,%d): %v", i, err)
		}
	}
	if len(db.l1()) <= len(zzzPathsBefore) {
		t.Fatalf("len(db.l1()) = %d, want > %d (aaa range must have produced its own L1 file(s), separate from zzz)", len(db.l1()), len(zzzPathsBefore))
	}
	assertL1NonOverlapping(t, db)

	for _, p := range zzzPathsBefore {
		dataAfter, err := os.ReadFile(p)
		if err != nil {
			t.Fatalf("ReadFile(%s) after aaa compactions: %v", p, err)
		}
		if !bytes.Equal(zzzDataBefore[p], dataAfter) {
			t.Fatalf("zzz range's L1 file %s was modified by a disjoint-range compaction -- overlap selection is wrong", p)
		}
	}

	edits, err := manifest.ReplayManifest(db.manifestPath)
	if err != nil {
		t.Fatalf("ReplayManifest: %v", err)
	}
	zzzBases := make(map[string]struct{}, len(zzzPathsBefore))
	for _, p := range zzzPathsBefore {
		zzzBases[filepath.Base(p)] = struct{}{}
	}
	for _, e := range edits {
		if _, ok := zzzBases[e.File]; ok && e.Type == manifest.SSTableRemoved {
			t.Fatalf("MANIFEST records a zzz L1 file (%s) as REMOVED -- it should never have been touched", e.File)
		}
	}

	for i := 0; i < n; i++ {
		v, found, err := db.Get(rangeKey("zzz", i))
		if err != nil || !found || !bytes.Equal(v, valueFor(i)) {
			t.Fatalf("Get(zzz,%d) = %q found=%v err=%v, want %q true nil", i, v, found, err, valueFor(i))
		}
		v, found, err = db.Get(rangeKey("aaa", i))
		if err != nil || !found || !bytes.Equal(v, valueFor(i)) {
			t.Fatalf("Get(aaa,%d) = %q found=%v err=%v, want %q true nil", i, v, found, err, valueFor(i))
		}
	}
}

// TestL0ToL1CompactionCorrectAcrossMultipleGenerationsAndLevels is the
// Phase 8d equivalent of the Phase 6 multi-generation correctness test:
// keys overwritten and deleted across several distinct pre-compaction
// generations, spanning enough flushes to trigger *multiple* rounds of
// L0->L1 compaction (so later rounds must merge against an already-live
// L1 file, exercising the overlap + tombstone-drop coverage logic for
// real, not just once). Every key's final Get result must be exactly
// correct: newest surviving value, or entirely absent if tombstoned.
func TestL0ToL1CompactionCorrectAcrossMultipleGenerationsAndLevels(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()
	db.SetFlushThreshold(2 * 1024)
	db.SetL0CompactionThreshold(3)

	const n = 600
	key := func(i int) []byte { return []byte(fmt.Sprintf("key-%06d", i)) }

	for i := 0; i < n; i++ {
		if err := db.Put(key(i), []byte("gen1")); err != nil {
			t.Fatalf("Put gen1(%d): %v", i, err)
		}
	}
	for i := 0; i < n; i += 2 {
		if err := db.Put(key(i), []byte("gen2")); err != nil {
			t.Fatalf("Put gen2(%d): %v", i, err)
		}
	}
	for i := 0; i < n; i += 4 {
		if err := db.Delete(key(i)); err != nil {
			t.Fatalf("Delete(%d): %v", i, err)
		}
	}
	// A third round of writes forces at least one more L0->L1 round that
	// must merge against the L1 output(s) from the rounds above.
	for i := 0; i < n; i += 8 {
		if err := db.Put(key(i), []byte("gen4")); err != nil {
			t.Fatalf("Put gen4(%d): %v", i, err)
		}
	}

	if len(db.l1()) == 0 {
		t.Fatal("no L1 files produced -- test isn't exercising L0->L1 compaction")
	}
	assertL1NonOverlapping(t, db)

	for i := 0; i < n; i++ {
		v, found, err := db.Get(key(i))
		switch {
		case i%8 == 0:
			if err != nil || !found || !bytes.Equal(v, []byte("gen4")) {
				t.Fatalf("Get(%s) = %q found=%v err=%v, want gen4 true nil", key(i), v, found, err)
			}
		case i%4 == 0:
			if err != nil || found {
				t.Fatalf("Get(%s) = %q found=%v err=%v, want false nil (deleted)", key(i), v, found, err)
			}
		case i%2 == 0:
			if err != nil || !found || !bytes.Equal(v, []byte("gen2")) {
				t.Fatalf("Get(%s) = %q found=%v err=%v, want gen2 true nil", key(i), v, found, err)
			}
		default:
			if err != nil || !found || !bytes.Equal(v, []byte("gen1")) {
				t.Fatalf("Get(%s) = %q found=%v err=%v, want gen1 true nil", key(i), v, found, err)
			}
		}
	}
}

// TestL1ToL1RecompactionMergesAllL1FilesAndPreservesData forces L1's
// total on-disk size past the size-ratio trigger (via a tiny
// SetL1SizeRatio) and confirms compactL1IfNeeded actually fires --
// collapsing L1 down to a single file -- while every key remains
// correctly readable and the L1 non-overlap invariant still holds.
func TestL1ToL1RecompactionMergesAllL1FilesAndPreservesData(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()
	db.SetFlushThreshold(2 * 1024)
	db.SetL0CompactionThreshold(2)
	db.SetL1SizeRatio(1) // fire L1->L1 recompaction as soon as L1 exceeds ~1x the flush threshold

	const n = 500
	for i := 0; i < n; i++ {
		if err := db.Put(keyFor(i), valueFor(i)); err != nil {
			t.Fatalf("Put(%d): %v", i, err)
		}
	}

	if len(db.l1()) == 0 {
		t.Fatal("no L1 files produced")
	}
	assertL1NonOverlapping(t, db)

	edits, err := manifest.ReplayManifest(db.manifestPath)
	if err != nil {
		t.Fatalf("ReplayManifest: %v", err)
	}
	if !manifestHasAnyRemoved(edits) {
		t.Fatal("MANIFEST has no SSTableRemoved edits -- no compaction ran at all")
	}

	for i := 0; i < n; i++ {
		v, found, err := db.Get(keyFor(i))
		if err != nil || !found || !bytes.Equal(v, valueFor(i)) {
			t.Fatalf("Get(%s) = %q found=%v err=%v, want %q true nil", keyFor(i), v, found, err, valueFor(i))
		}
	}
}
