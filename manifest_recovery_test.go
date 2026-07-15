package lsm

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/paarthsiphone/lsm-tree-storage/manifest"
	"github.com/paarthsiphone/lsm-tree-storage/sstable"
)

// TestRecoveryIgnoresPlantedOrphanSSTable mirrors the "done" example from
// the Phase 4 brief: flush several real SSTables, close, manually drop an
// extra .sst file into data/sstables/ that the MANIFEST never recorded,
// then reopen and confirm the live set has exactly the flushed tables --
// the bogus one is ignored -- while all real data is still readable.
func TestRecoveryIgnoresPlantedOrphanSSTable(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	db.SetFlushThreshold(4 * 1024)

	const n = 3000
	for i := 0; i < n; i++ {
		if err := db.Put(keyFor(i), valueFor(i)); err != nil {
			t.Fatalf("Put(%d): %v", i, err)
		}
	}
	flushedTables := len(db.sstables)
	if flushedTables < 2 {
		t.Fatalf("only %d SSTable(s) flushed, want >= 2 for this test to be meaningful", flushedTables)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Plant a bogus .sst file that was never added to the MANIFEST.
	// It doesn't even need to be a valid SSTable -- the whole point is
	// that it's never opened at all, because it isn't in the
	// MANIFEST-reconstructed live set.
	sstDir := filepath.Join(dir, sstableSubdir)
	bogusPath := filepath.Join(sstDir, "000999.sst")
	if err := os.WriteFile(bogusPath, []byte("not a real sstable"), 0o644); err != nil {
		t.Fatalf("WriteFile(bogus): %v", err)
	}

	db2, err := Open(dir)
	if err != nil {
		t.Fatalf("Open (restart): %v", err)
	}
	defer db2.Close()

	if len(db2.sstables) != flushedTables {
		t.Fatalf("live SSTable count after restart = %d, want %d (bogus file must be excluded)", len(db2.sstables), flushedTables)
	}

	for i := 0; i < n; i++ {
		v, found, err := db2.Get(keyFor(i))
		if err != nil || !found || !bytes.Equal(v, valueFor(i)) {
			t.Fatalf("Get(%s) after restart = %q found=%v err=%v, want %q true nil", keyFor(i), v, found, err, valueFor(i))
		}
	}

	// The bogus file must still be sitting on disk, untouched -- Phase 4
	// only ignores orphans, it doesn't clean them up (that's deferred).
	if _, err := os.Stat(bogusPath); err != nil {
		t.Fatalf("bogus file missing after restart (stat err = %v), want left in place untouched", err)
	}
}

// TestRecoveryVerifiesLiveSetMatchesManifestExactly is a more direct
// check on the same property as TestRecoveryIgnoresPlantedOrphanSSTable:
// after several real flushes and a restart, the live set's filenames
// must be exactly what the MANIFEST's replayed edits say they are, no
// more and no fewer.
func TestRecoveryVerifiesLiveSetMatchesManifestExactly(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	db.SetFlushThreshold(4 * 1024)

	const n = 3000
	for i := 0; i < n; i++ {
		if err := db.Put(keyFor(i), valueFor(i)); err != nil {
			t.Fatalf("Put(%d): %v", i, err)
		}
	}
	if len(db.sstables) < 2 {
		t.Fatalf("only %d SSTable(s) flushed, want >= 2", len(db.sstables))
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	manifestName, err := manifest.ReadCurrent(dir)
	if err != nil {
		t.Fatalf("ReadCurrent: %v", err)
	}
	edits, err := manifest.ReplayManifest(filepath.Join(dir, manifestName))
	if err != nil {
		t.Fatalf("ReplayManifest: %v", err)
	}
	wantLive := manifest.ReconstructSSTableSet(edits)

	db2, err := Open(dir)
	if err != nil {
		t.Fatalf("Open (restart): %v", err)
	}
	defer db2.Close()

	if len(db2.sstables) != len(wantLive) {
		t.Fatalf("live SSTable count = %d, want %d (per MANIFEST replay)", len(db2.sstables), len(wantLive))
	}
	gotNames := make(map[string]struct{}, len(db2.sstables))
	for _, st := range db2.sstables {
		gotNames[filepath.Base(st.Meta().Path)] = struct{}{}
	}
	for _, name := range wantLive {
		if _, ok := gotNames[name]; !ok {
			t.Fatalf("MANIFEST says %s is live but DB didn't open it", name)
		}
	}
}

// TestRecoverySkipsSSTableOrphanedByCrashBetweenRenameAndManifestAppend
// injects the exact crash window the Phase 4 brief calls out: an
// SSTable's atomic rename succeeds (so the file is durable on disk) but
// the process dies before the MANIFEST's SSTABLE_ADDED edit is appended.
// This can't be reproduced with a real kill -9 reliably (the window is a
// handful of syscalls wide), so it's injected directly: call
// sstable.FlushMemtable and skip the manifest.AppendEdit call db.go's
// maybeFlush would normally make immediately after.
//
// On restart: the orphaned SSTable must be ignored (absent from the
// MANIFEST-reconstructed live set), and every key that was actually
// acknowledged (durably in the WAL) must still be recoverable via WAL
// replay -- proving the crash didn't lose data, only failed to
// "advance" a flush that hadn't fully committed yet.
func TestRecoverySkipsSSTableOrphanedByCrashBetweenRenameAndManifestAppend(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	// Flush threshold left high: every write below stays in the memtable
	// (and therefore only in the WAL) until the injected flush.
	db.SetFlushThreshold(1 << 30)

	const n = 200
	for i := 0; i < n; i++ {
		if err := db.Put(keyFor(i), valueFor(i)); err != nil {
			t.Fatalf("Put(%d): %v", i, err)
		}
	}
	if len(db.sstables) != 0 {
		t.Fatalf("got %d SSTables before injected flush, want 0", len(db.sstables))
	}

	// Simulate the SSTable half of a flush completing (rename durable)
	// without ever calling manifest.AppendEdit -- the crash point.
	sstDir := filepath.Join(dir, sstableSubdir)
	orphanPath := filepath.Join(sstDir, "000001.sst")
	if _, err := sstable.FlushMemtable(db.mem, orphanPath); err != nil {
		t.Fatalf("FlushMemtable: %v", err)
	}
	if _, err := os.Stat(orphanPath); err != nil {
		t.Fatalf("orphan SSTable not on disk after simulated flush (stat err = %v)", err)
	}

	// Simulate the crash: close without ever running the rest of
	// maybeFlush (no MANIFEST append, no WAL segment cleanup, no WAL
	// writer swap). The WAL segment(s) covering all n puts are still
	// fully intact on disk, exactly as they'd be after a real crash at
	// this point.
	if err := db.w.Close(); err != nil {
		t.Fatalf("close WAL writer: %v", err)
	}

	db2, err := Open(dir)
	if err != nil {
		t.Fatalf("Open (restart): %v", err)
	}
	defer db2.Close()

	if len(db2.sstables) != 0 {
		t.Fatalf("live SSTable count after restart = %d, want 0 -- orphaned SSTable must be excluded", len(db2.sstables))
	}
	if _, err := os.Stat(orphanPath); err != nil {
		t.Fatalf("orphan SSTable missing after restart (stat err = %v), want left on disk untouched (not deleted, just ignored)", err)
	}

	for i := 0; i < n; i++ {
		v, found, err := db2.Get(keyFor(i))
		if err != nil || !found || !bytes.Equal(v, valueFor(i)) {
			t.Fatalf("Get(%s) after restart = %q found=%v err=%v, want %q true nil -- WAL replay must recover all acknowledged writes even though the flush that would have superseded them never committed", keyFor(i), v, found, err, valueFor(i))
		}
	}
}

// TestNextSeqAvoidsCollisionWithOrphanedSSTable confirms that after a
// crash orphans an SSTable (rename succeeded, MANIFEST append never
// happened), a subsequent *real* flush picks a sequence number past the
// orphan's rather than colliding with -- and silently overwriting -- it.
// openSSTables derives nextSeq from every ".sst" file physically present
// on disk, not just the MANIFEST-live set, specifically to guarantee
// this.
func TestNextSeqAvoidsCollisionWithOrphanedSSTable(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	db.SetFlushThreshold(1 << 30)

	if err := db.Put([]byte("a"), []byte("1")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	sstDir := filepath.Join(dir, sstableSubdir)
	orphanPath := filepath.Join(sstDir, "000001.sst")
	if _, err := sstable.FlushMemtable(db.mem, orphanPath); err != nil {
		t.Fatalf("FlushMemtable: %v", err)
	}
	orphanDataBefore, err := os.ReadFile(orphanPath)
	if err != nil {
		t.Fatalf("ReadFile(orphan): %v", err)
	}
	if err := db.w.Close(); err != nil {
		t.Fatalf("close WAL writer: %v", err)
	}

	db2, err := Open(dir)
	if err != nil {
		t.Fatalf("Open (restart): %v", err)
	}
	defer db2.Close()

	db2.SetFlushThreshold(1)
	if err := db2.Put([]byte("b"), []byte("2")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if len(db2.sstables) != 1 {
		t.Fatalf("got %d SSTables after real flush, want 1", len(db2.sstables))
	}
	newPath := db2.sstables[0].Meta().Path
	if newPath == orphanPath {
		t.Fatalf("new flush reused the orphan's path %s -- sequence collision", orphanPath)
	}

	orphanDataAfter, err := os.ReadFile(orphanPath)
	if err != nil {
		t.Fatalf("ReadFile(orphan) after real flush: %v", err)
	}
	if !bytes.Equal(orphanDataBefore, orphanDataAfter) {
		t.Fatal("orphan file's contents changed -- something overwrote it")
	}

	v, found, err := db2.Get([]byte("a"))
	if err != nil || !found || !bytes.Equal(v, []byte("1")) {
		t.Fatalf("Get(a) = %q found=%v err=%v, want 1 true nil -- recovered via WAL replay", v, found, err)
	}
	v, found, err = db2.Get([]byte("b"))
	if err != nil || !found || !bytes.Equal(v, []byte("2")) {
		t.Fatalf("Get(b) = %q found=%v err=%v, want 2 true nil", v, found, err)
	}
}

// TestOpenFailsLoudlyWhenManifestListsMissingSSTable confirms Open
// treats "MANIFEST says this file is live but it's not on disk" as real
// corruption -- a hard error -- rather than silently opening fewer
// SSTables than the MANIFEST promises. This is the same "fail loudly on
// corruption" discipline sstable.OpenSSTable's footer checksum already
// applies; a mismatch between the authoritative MANIFEST and what's
// actually readable on disk must not silently drop data.
func TestOpenFailsLoudlyWhenManifestListsMissingSSTable(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	db.SetFlushThreshold(1)
	if err := db.Put([]byte("key:000001"), []byte("value")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if len(db.sstables) != 1 {
		t.Fatalf("got %d SSTables, want 1", len(db.sstables))
	}
	flushedPath := db.sstables[0].Meta().Path
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Delete the SSTable file the MANIFEST still lists as live.
	if err := os.Remove(flushedPath); err != nil {
		t.Fatalf("Remove(%s): %v", flushedPath, err)
	}

	if _, err := Open(dir); err == nil {
		t.Fatal("Open succeeded despite MANIFEST listing a live SSTable that's missing from disk, want an error")
	}
}

// TestOpenCreatesCurrentAndManifestOnFreshDB confirms a brand-new
// database durably records CURRENT pointing at an initial MANIFEST on
// its very first Open, even before any flush happens.
func TestOpenCreatesCurrentAndManifestOnFreshDB(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	name, err := manifest.ReadCurrent(dir)
	if err != nil {
		t.Fatalf("ReadCurrent: %v", err)
	}
	if name != manifest.InitialFileName {
		t.Fatalf("CURRENT = %q, want %q", name, manifest.InitialFileName)
	}
}

// TestFlushAppendsManifestEditSynchronously confirms every flush's
// SSTable is reflected in the MANIFEST immediately (not just eventually),
// by reading the MANIFEST directly after a single flush without closing
// the DB first.
func TestFlushAppendsManifestEditSynchronously(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()
	db.SetFlushThreshold(1)

	if err := db.Put([]byte("key:000001"), []byte("value")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if len(db.sstables) != 1 {
		t.Fatalf("got %d SSTables after Put, want 1", len(db.sstables))
	}

	edits, err := manifest.ReplayManifest(db.manifestPath)
	if err != nil {
		t.Fatalf("ReplayManifest: %v", err)
	}
	live := manifest.ReconstructSSTableSet(edits)
	if len(live) != 1 {
		t.Fatalf("MANIFEST live set = %v, want exactly 1 entry", live)
	}
	wantName := filepath.Base(db.sstables[0].Meta().Path)
	if live[0] != wantName {
		t.Fatalf("MANIFEST live set = %v, want [%s]", live, wantName)
	}
}
