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
// small flush threshold to produce exactly wantTables real SSTables, then
// closes it. wantTables must stay at or below db.compactionThreshold
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
	for len(db.sstables) < wantTables {
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

// TestCompactionCrashBeforeTempWriteCompletesLeavesOldFilesAuthoritative
// is crash-injection scenario 1 from the Phase 6 brief: the process dies
// while sstable.FlushIterator is still writing the compaction output's
// temp file, before it's ever fsynced or renamed. Simulated directly by
// planting a short, garbage, incomplete "*.sst.tmp" file -- the on-disk
// signature such a crash leaves behind -- with no MANIFEST changes at
// all. The database must come back exactly as it was: old input SSTables
// still authoritative, garbage swept up as litter.
func TestCompactionCrashBeforeTempWriteCompletesLeavesOldFilesAuthoritative(t *testing.T) {
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

	if len(db2.sstables) != 3 {
		t.Fatalf("live SSTable count after restart = %d, want 3 (old files still authoritative)", len(db2.sstables))
	}
	if _, err := os.Stat(tmpPath); !os.IsNotExist(err) {
		t.Fatalf("orphaned compaction .tmp file still present after Open (err = %v), want cleaned up", err)
	}
	assertAllKeysReadable(t, db2, total)
}

// TestCompactionCrashAfterTempWriteBeforeRenameLeavesOldFilesAuthoritative
// is crash-injection scenario 2: the merged output finished writing and
// was fsynced, but the process died before the atomic rename into its
// final path. Simulated by running a real Compact() and then renaming
// the result back onto its own ".tmp" path, reproducing the exact
// on-disk state that window leaves. Same required outcome as scenario 1:
// old files authoritative, the orphan is just litter.
func TestCompactionCrashAfterTempWriteBeforeRenameLeavesOldFilesAuthoritative(t *testing.T) {
	dir := t.TempDir()
	total := setupNSSTables(t, dir, 3)

	db2, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	inputs := append([]*sstable.SSTable(nil), db2.sstables...)
	sstDir := filepath.Join(dir, sstableSubdir)
	outPath := filepath.Join(sstDir, "000999.sst")

	if _, err := compaction.Compact(inputs, outPath); err != nil {
		t.Fatalf("Compact: %v", err)
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

	if len(db3.sstables) != 3 {
		t.Fatalf("live SSTable count after restart = %d, want 3 (old files still authoritative)", len(db3.sstables))
	}
	if _, err := os.Stat(outPath + ".tmp"); !os.IsNotExist(err) {
		t.Fatalf("orphaned compaction .tmp file still present after Open (err = %v), want cleaned up", err)
	}
	assertAllKeysReadable(t, db3, total)
}

// TestCompactionCrashAfterRenameBeforeManifestAppendLeavesOutputOrphaned
// is crash-injection scenario 3: the compacted output is fully durable at
// its final path (rename succeeded), but the process died before the
// MANIFEST's SSTABLE_ADDED edit for it was ever appended. The new file
// must be ignored on restart -- absent from the MANIFEST-reconstructed
// live set -- while the (still-live, untouched) original inputs remain
// authoritative and every key is still correctly readable through them.
func TestCompactionCrashAfterRenameBeforeManifestAppendLeavesOutputOrphaned(t *testing.T) {
	dir := t.TempDir()
	total := setupNSSTables(t, dir, 3)

	db2, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	inputs := append([]*sstable.SSTable(nil), db2.sstables...)
	sstDir := filepath.Join(dir, sstableSubdir)
	outPath := filepath.Join(sstDir, "000999.sst")

	if _, err := compaction.Compact(inputs, outPath); err != nil {
		t.Fatalf("Compact: %v", err)
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

	if len(db3.sstables) != 3 {
		t.Fatalf("live SSTable count after restart = %d, want 3 -- orphaned compacted output must be ignored, old inputs still authoritative", len(db3.sstables))
	}
	if _, err := os.Stat(outPath); err != nil {
		t.Fatalf("orphaned compacted output missing after restart (err = %v), want left on disk untouched (ignored, not deleted)", err)
	}
	assertAllKeysReadable(t, db3, total)
}

// TestCompactionCrashAfterAddedBeforeRemovedLeavesBothGenerationsLive is
// crash-injection scenario 4, the case the Phase 6 brief specifically
// asks compaction to prefer: SSTABLE_ADDED for the new merged output was
// appended and is durable, but the process died before any of the
// SSTABLE_REMOVED edits for the old inputs were appended. Both the old
// inputs and the new output must end up live in the reconstructed set --
// redundant, but every key's value is still correct, since newest-wins
// read logic (the compacted output has the highest sequence number, so
// it's opened newest-first) makes the duplication harmless rather than
// wrong.
func TestCompactionCrashAfterAddedBeforeRemovedLeavesBothGenerationsLive(t *testing.T) {
	dir := t.TempDir()
	total := setupNSSTables(t, dir, 3)

	db2, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	inputs := append([]*sstable.SSTable(nil), db2.sstables...)
	sstDir := filepath.Join(dir, sstableSubdir)
	outPath := filepath.Join(sstDir, "000999.sst")

	if _, err := compaction.Compact(inputs, outPath); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if err := manifest.AppendEdit(db2.manifestPath, manifest.VersionEdit{
		Type: manifest.SSTableAdded,
		File: filepath.Base(outPath),
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

	if len(db3.sstables) != len(inputs)+1 {
		t.Fatalf("live SSTable count after restart = %d, want %d -- both old inputs and the new compacted output must be live (redundant, not wrong)",
			len(db3.sstables), len(inputs)+1)
	}
	assertAllKeysReadable(t, db3, total)
}

// TestCompactionNextSeqSkipsPastOrphanedCompactionOutput mirrors Phase
// 4's TestNextSeqAvoidsCollisionWithOrphanedSSTable, but for a compaction
// output orphaned by crash-injection scenario 3 (rename succeeded,
// MANIFEST never touched) rather than a flush. nextSeq is derived from
// every ".sst" file physically on disk (live or orphaned, see
// openSSTables), so a subsequent real flush must pick a sequence number
// past the orphan's high sequence number, never colliding with -- and
// silently overwriting -- it.
func TestCompactionNextSeqSkipsPastOrphanedCompactionOutput(t *testing.T) {
	dir := t.TempDir()
	total := setupNSSTables(t, dir, 3)

	db2, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	inputs := append([]*sstable.SSTable(nil), db2.sstables...)
	sstDir := filepath.Join(dir, sstableSubdir)
	orphanPath := filepath.Join(sstDir, "000999.sst")

	if _, err := compaction.Compact(inputs, orphanPath); err != nil {
		t.Fatalf("Compact: %v", err)
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

	newestPath := db3.sstables[0].Meta().Path
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
