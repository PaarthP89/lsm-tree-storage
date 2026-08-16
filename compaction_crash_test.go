package lsm

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/paarthsiphone/lsm-tree-storage/compaction"
	"github.com/paarthsiphone/lsm-tree-storage/manifest"
	"github.com/paarthsiphone/lsm-tree-storage/sstable"
)

// setupNSSTables opens a fresh DB at dir, writes just enough sequential
// keys (keyFor(0)..keyFor(total-1), all distinct, no overwrites) with a
// small flush threshold to produce exactly wantTables real L0 SSTables,
// then closes it. wantTables must stay at or below db.l0CompactionThreshold
// so no compaction fires during setup -- these tests inject one specific
// mid-compaction crash point themselves, rather than relying on (or
// racing) the automatic trigger.
func setupNSSTables(t *testing.T, dir string, wantTables int) (total int) {
	t.Helper()
	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	db.SetFlushThreshold(2 * 1024)

	i := 0
	for len(db.l0()) < wantTables {
		if err := db.Put(keyFor(i), valueFor(i)); err != nil {
			t.Fatalf("Put(%d): %v", i, err)
		}
		i++
		if i > 200000 {
			t.Fatalf("failed to reach %d sstables after %d puts", wantTables, i)
		}
	}
	total = i
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	return total
}

func assertAllKeysReadable(t *testing.T, db *DB, total int) {
	t.Helper()
	for i := 0; i < total; i++ {
		v, found, err := db.Get(keyFor(i))
		if err != nil || !found || !bytes.Equal(v, valueFor(i)) {
			t.Fatalf("Get(%s) = %q found=%v err=%v, want %q true nil", keyFor(i), v, found, err, valueFor(i))
		}
	}
}

// TestL0CompactionCrashBeforeTempWriteCompletesLeavesOldFilesAuthoritative
// is crash-injection scenario 1, adapted for Phase 8d's L0->L1 path: the
// process dies while sstable.FlushIterator is still writing the L0->L1
// merge output's temp file, before it's ever fsynced or renamed. Simulated
// directly by planting a short, garbage, incomplete "*.sst.tmp" file --
// the on-disk signature such a crash leaves behind -- with no MANIFEST
// changes at all. The database must come back exactly as it was: the
// original L0 inputs still authoritative in L0, no L1 file exists yet.
func TestL0CompactionCrashBeforeTempWriteCompletesLeavesOldFilesAuthoritative(t *testing.T) {
	dir := t.TempDir()
	total := setupNSSTables(t, dir, 3)

	sstDir := filepath.Join(dir, sstableSubdir)
	tmpPath := filepath.Join(sstDir, "000999.sst.tmp")
	if err := os.WriteFile(tmpPath, []byte("partial garbage, never fsynced or renamed"), 0o644); err != nil {
		t.Fatalf("WriteFile(tmp): %v", err)
	}

	db2, err := Open(dir)
	if err != nil {
		t.Fatalf("Open (restart): %v", err)
	}
	defer db2.Close()

	if len(db2.l0()) != 3 {
		t.Fatalf("live L0 count after restart = %d, want 3 (old files still authoritative)", len(db2.l0()))
	}
	if len(db2.l1()) != 0 {
		t.Fatalf("live L1 count after restart = %d, want 0", len(db2.l1()))
	}
	if _, err := os.Stat(tmpPath); !os.IsNotExist(err) {
		t.Fatalf("orphaned compaction .tmp file still present after Open (err = %v), want cleaned up", err)
	}
	assertAllKeysReadable(t, db2, total)
}

// TestL0CompactionCrashAfterTempWriteBeforeRenameLeavesOldFilesAuthoritative
// is crash-injection scenario 2: the L0->L1 merge output finished writing
// and was fsynced, but the process died before the atomic rename into its
// final path. Simulated by running a real compaction.CompactLeveled and
// then renaming the result back onto its own ".tmp" path, reproducing the
// exact on-disk state that window leaves. Same required outcome as
// scenario 1: old L0 files authoritative, the orphan is just litter.
func TestL0CompactionCrashAfterTempWriteBeforeRenameLeavesOldFilesAuthoritative(t *testing.T) {
	dir := t.TempDir()
	total := setupNSSTables(t, dir, 3)

	db2, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	inputs := append([]*sstable.SSTable(nil), db2.l0()...)
	sstDir := filepath.Join(dir, sstableSubdir)
	outPath := filepath.Join(sstDir, "000999.sst")

	if _, err := compaction.CompactLeveled(inputs, outPath, true); err != nil {
		t.Fatalf("CompactLeveled: %v", err)
	}
	// Undo just the rename, standing in for a crash before it ran.
	if err := os.Rename(outPath, outPath+".tmp"); err != nil {
		t.Fatalf("rename back to .tmp: %v", err)
	}
	if err := db2.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	db3, err := Open(dir)
	if err != nil {
		t.Fatalf("Open (restart): %v", err)
	}
	defer db3.Close()

	if len(db3.l0()) != 3 {
		t.Fatalf("live L0 count after restart = %d, want 3 (old files still authoritative)", len(db3.l0()))
	}
	if len(db3.l1()) != 0 {
		t.Fatalf("live L1 count after restart = %d, want 0", len(db3.l1()))
	}
	if _, err := os.Stat(outPath + ".tmp"); !os.IsNotExist(err) {
		t.Fatalf("orphaned compaction .tmp file still present after Open (err = %v), want cleaned up", err)
	}
	assertAllKeysReadable(t, db3, total)
}

// TestL0CompactionCrashAfterRenameBeforeManifestAppendLeavesOutputOrphaned
// is crash-injection scenario 3: the compacted L1 output is fully durable
// at its final path (rename succeeded), but the process died before the
// MANIFEST's SSTABLE_ADDED edit for it was ever appended. The new file
// must be ignored on restart -- absent from the MANIFEST-reconstructed
// live set -- while the (still-live, untouched) original L0 inputs remain
// authoritative and every key is still correctly readable through them.
func TestL0CompactionCrashAfterRenameBeforeManifestAppendLeavesOutputOrphaned(t *testing.T) {
	dir := t.TempDir()
	total := setupNSSTables(t, dir, 3)

	db2, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	inputs := append([]*sstable.SSTable(nil), db2.l0()...)
	sstDir := filepath.Join(dir, sstableSubdir)
	outPath := filepath.Join(sstDir, "000999.sst")

	if _, err := compaction.CompactLeveled(inputs, outPath, true); err != nil {
		t.Fatalf("CompactLeveled: %v", err)
	}
	// Crash point: rename committed, MANIFEST never touched.
	if err := db2.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	db3, err := Open(dir)
	if err != nil {
		t.Fatalf("Open (restart): %v", err)
	}
	defer db3.Close()

	if len(db3.l0()) != 3 {
		t.Fatalf("live L0 count after restart = %d, want 3 -- orphaned compacted output must be ignored, old inputs still authoritative", len(db3.l0()))
	}
	if len(db3.l1()) != 0 {
		t.Fatalf("live L1 count after restart = %d, want 0 -- orphaned output must not appear in L1", len(db3.l1()))
	}
	if _, err := os.Stat(outPath); err != nil {
		t.Fatalf("orphaned compacted output missing after restart (err = %v), want left on disk untouched (ignored, not deleted)", err)
	}
	assertAllKeysReadable(t, db3, total)
}

// TestL0CompactionCrashAfterAddedBeforeRemovedLeavesBothGenerationsLive is
// crash-injection scenario 4, the case the Phase 6 brief specifically
// asked compaction to prefer, now exercised against the Phase 8d L0->L1
// path: SSTABLE_ADDED (Level 1) for the new merged output was appended
// and is durable, but the process died before any of the SSTABLE_REMOVED
// edits for the old L0 inputs were appended. The old L0 inputs must still
// be live in L0, *and* the new output must be live in L1 -- redundant,
// but every key's value is still correct, since Get checks L0 (newer)
// before L1, so the duplication is harmless rather than wrong.
func TestL0CompactionCrashAfterAddedBeforeRemovedLeavesBothGenerationsLive(t *testing.T) {
	dir := t.TempDir()
	total := setupNSSTables(t, dir, 3)

	db2, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	inputs := append([]*sstable.SSTable(nil), db2.l0()...)
	sstDir := filepath.Join(dir, sstableSubdir)
	outPath := filepath.Join(sstDir, "000999.sst")

	if _, err := compaction.CompactLeveled(inputs, outPath, true); err != nil {
		t.Fatalf("CompactLeveled: %v", err)
	}
	if err := manifest.AppendEdit(db2.manifestPath, manifest.VersionEdit{
		Type: manifest.SSTableAdded, File: filepath.Base(outPath), Level: 1,
	}); err != nil {
		t.Fatalf("AppendEdit(ADDED): %v", err)
	}
	// Crash point: ADDED committed, no REMOVED edits ever appended.
	if err := db2.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	db3, err := Open(dir)
	if err != nil {
		t.Fatalf("Open (restart): %v", err)
	}
	defer db3.Close()

	if len(db3.l0()) != len(inputs) {
		t.Fatalf("live L0 count after restart = %d, want %d -- old inputs must still be live in L0 (redundant, not wrong)", len(db3.l0()), len(inputs))
	}
	if len(db3.l1()) != 1 {
		t.Fatalf("live L1 count after restart = %d, want 1 -- new compacted output must be live in L1", len(db3.l1()))
	}
	assertAllKeysReadable(t, db3, total)
}

// TestL0CompactionNextSeqSkipsPastOrphanedCompactionOutput mirrors Phase
// 4's TestNextSeqAvoidsCollisionWithOrphanedSSTable, but for an L0->L1
// compaction output orphaned by crash-injection scenario 3 (rename
// succeeded, MANIFEST never touched) rather than a flush. nextSeq is
// derived from every ".sst" file physically on disk (live or orphaned,
// see openLeveledSSTables), so a subsequent real flush must pick a
// sequence number past the orphan's high sequence number, never
// colliding with -- and silently overwriting -- it. The post-restart
// flush lands in L0, same as any fresh flush.
func TestL0CompactionNextSeqSkipsPastOrphanedCompactionOutput(t *testing.T) {
	dir := t.TempDir()
	total := setupNSSTables(t, dir, 3)

	db2, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	inputs := append([]*sstable.SSTable(nil), db2.l0()...)
	sstDir := filepath.Join(dir, sstableSubdir)
	orphanPath := filepath.Join(sstDir, "000999.sst")

	if _, err := compaction.CompactLeveled(inputs, orphanPath, true); err != nil {
		t.Fatalf("CompactLeveled: %v", err)
	}
	// Crash point: rename committed, MANIFEST never touched (same as
	// scenario 3), leaving a high-sequence-numbered orphan on disk.
	if err := db2.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	orphanDataBefore, err := os.ReadFile(orphanPath)
	if err != nil {
		t.Fatalf("ReadFile(orphan): %v", err)
	}

	db3, err := Open(dir)
	if err != nil {
		t.Fatalf("Open (restart): %v", err)
	}
	defer db3.Close()

	db3.SetFlushThreshold(1)
	newKey, newVal := []byte("post-restart-key"), []byte("post-restart-value")
	if err := db3.Put(newKey, newVal); err != nil {
		t.Fatalf("Put: %v", err)
	}

	if len(db3.l0()) == 0 {
		t.Fatalf("live L0 count after post-restart flush = 0, want >= 1")
	}
	newestPath := db3.l0()[0].Meta().Path
	if newestPath == orphanPath {
		t.Fatalf("new flush reused the orphan's path %s -- sequence collision", orphanPath)
	}

	orphanDataAfter, err := os.ReadFile(orphanPath)
	if err != nil {
		t.Fatalf("ReadFile(orphan) after real flush: %v", err)
	}
	if !bytes.Equal(orphanDataBefore, orphanDataAfter) {
		t.Fatal("orphan compaction output's contents changed -- something overwrote it")
	}

	v, found, err := db3.Get(newKey)
	if err != nil || !found || !bytes.Equal(v, newVal) {
		t.Fatalf("Get(post-restart-key) = %q found=%v err=%v, want %q true nil", v, found, err, newVal)
	}
	assertAllKeysReadable(t, db3, total)
}
