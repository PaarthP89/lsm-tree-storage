package lsm

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/paarthsiphone/lsm-tree-storage/memtable"
)

// TestFlushThenRead forces a flush partway through a burst of writes
// (mirroring the Phase 3 brief's worked example) and confirms every
// earlier key is still readable afterward, even though it now lives in
// an SSTable rather than the current memtable.
func TestFlushThenRead(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()
	db.SetFlushThreshold(8 * 1024) // small, to force a flush without writing megabytes

	const n = 2000
	for i := 0; i < n; i++ {
		if err := db.Put(keyFor(i), valueFor(i)); err != nil {
			t.Fatalf("Put(%d): %v", i, err)
		}
	}

	if len(db.liveSSTables()) == 0 {
		t.Fatal("no SSTable was flushed -- test didn't exercise the flush path it's meant to test")
	}

	for i := 0; i < n; i++ {
		v, found, err := db.Get(keyFor(i))
		if err != nil || !found || !bytes.Equal(v, valueFor(i)) {
			t.Fatalf("Get(%s) = %q found=%v err=%v, want %q true nil", keyFor(i), v, found, err, valueFor(i))
		}
	}
}

// TestMemtableShadowsOlderSSTable confirms a newer value in the active
// memtable wins over an older, already-flushed value for the same key.
func TestMemtableShadowsOlderSSTable(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()
	db.SetFlushThreshold(1) // flush after every write, to guarantee the key lands in an SSTable

	if err := db.Put([]byte("key:000001"), []byte("original")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if len(db.liveSSTables()) == 0 {
		t.Fatal("key wasn't flushed to an SSTable -- test setup didn't force what it's meant to force")
	}

	if err := db.Put([]byte("key:000001"), []byte("updated")); err != nil {
		t.Fatalf("Put (overwrite): %v", err)
	}

	v, found, err := db.Get([]byte("key:000001"))
	if err != nil || !found || !bytes.Equal(v, []byte("updated")) {
		t.Fatalf("Get(key:000001) = %q found=%v err=%v, want updated true nil", v, found, err)
	}
}

// TestTombstoneSuppressesOlderSSTable confirms deleting a key that's
// already been flushed to an SSTable correctly reports "not found",
// rather than the SSTable's stale value leaking back through.
func TestTombstoneSuppressesOlderSSTable(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()
	db.SetFlushThreshold(1)

	if err := db.Put([]byte("key:000002"), []byte("value")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if len(db.liveSSTables()) == 0 {
		t.Fatal("key wasn't flushed to an SSTable -- test setup didn't force what it's meant to force")
	}

	if err := db.Delete([]byte("key:000002")); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	_, found, err := db.Get([]byte("key:000002"))
	if err != nil || found {
		t.Fatalf("Get(key:000002) after delete = found=%v err=%v, want false nil -- tombstone must suppress the SSTable value", found, err)
	}
}

// TestRestartAfterFlushReadsAllSSTables flushes several SSTables, closes
// the DB, reopens it, and confirms every key across every flushed table
// (plus whatever was left in the memtable at close) is still readable.
//
// This exercises DB's SSTable discovery at Open time, which today is a
// naive directory scan of data/sstables/ (see openSSTables in db.go) --
// a deliberately temporary mechanism, per the Phase 3 brief, that Phase
// 4's MANIFEST replaces with a durable record of the actual SSTable set.
func TestRestartAfterFlushReadsAllSSTables(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	db.SetFlushThreshold(4 * 1024)
	// This test is about SSTable discovery at Open time, not compaction --
	// disable compaction entirely so the flushed-table count it asserts on
	// isn't at the mercy of exactly where Phase 8d's independent L0/L1
	// triggers happen to land for this dataset size (unlike the old
	// single-level scheme, L0's threshold no longer counts L1's files at
	// all, so the two are no longer comparable cycle-for-cycle).
	db.SetL0CompactionThreshold(1 << 30)

	const n = 3000
	for i := 0; i < n; i++ {
		if err := db.Put(keyFor(i), valueFor(i)); err != nil {
			t.Fatalf("Put(%d): %v", i, err)
		}
	}
	flushedTables := len(db.liveSSTables())
	if flushedTables < 2 {
		t.Fatalf("only %d SSTable(s) flushed, want >= 2 for this test to exercise multi-table discovery", flushedTables)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Confirm the naive scan actually found files on disk, not just
	// in-memory state carried over incorrectly.
	sstDir := filepath.Join(dir, sstableSubdir)
	des, err := os.ReadDir(sstDir)
	if err != nil {
		t.Fatalf("ReadDir(%s): %v", sstDir, err)
	}
	var sstFiles int
	for _, de := range des {
		if filepath.Ext(de.Name()) == ".sst" {
			sstFiles++
		}
	}
	if sstFiles != flushedTables {
		t.Fatalf("found %d .sst files on disk, want %d (matching what was flushed)", sstFiles, flushedTables)
	}

	db2, err := Open(dir)
	if err != nil {
		t.Fatalf("Open (restart): %v", err)
	}
	defer db2.Close()

	if len(db2.liveSSTables()) != flushedTables {
		t.Fatalf("after restart, discovered %d SSTables, want %d", len(db2.liveSSTables()), flushedTables)
	}

	for i := 0; i < n; i++ {
		v, found, err := db2.Get(keyFor(i))
		if err != nil || !found || !bytes.Equal(v, valueFor(i)) {
			t.Fatalf("Get(%s) after restart = %q found=%v err=%v, want %q true nil", keyFor(i), v, found, err, valueFor(i))
		}
	}
}

// TestRestartPreservesNewestWinsAcrossFlushes writes the same key across
// three different flush generations plus the final memtable, then
// restarts, and confirms the newest write wins -- proving flush-order
// (not just presence) survives a restart correctly.
func TestRestartPreservesNewestWinsAcrossFlushes(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	db.SetFlushThreshold(1) // flush after every write, so each Put below lands in its own SSTable generation

	key := []byte("contested-key")
	for i, v := range [][]byte{[]byte("gen1"), []byte("gen2"), []byte("gen3")} {
		if err := db.Put(key, v); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}
	if len(db.liveSSTables()) < 3 {
		t.Fatalf("got %d SSTables, want >= 3", len(db.liveSSTables()))
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	db2, err := Open(dir)
	if err != nil {
		t.Fatalf("Open (restart): %v", err)
	}
	defer db2.Close()

	v, found, err := db2.Get(key)
	if err != nil || !found || !bytes.Equal(v, []byte("gen3")) {
		t.Fatalf("Get(%s) after restart = %q found=%v err=%v, want gen3 true nil", key, v, found, err)
	}
}

// countMemtableEntries walks mem's Iterator and counts entries. Unlike
// SizeBytes, this doesn't depend on the skip list's randomly chosen
// per-node level (see memtable.SkipList's nodeStructOverheadBytes/
// pointerBytes), so it's the right tool for a test that needs exact,
// reproducible equality between two independently built skip lists
// holding the same logical data.
func countMemtableEntries(mem *memtable.SkipList) int {
	it := mem.Iterator()
	n := 0
	for it.Next() {
		n++
	}
	return n
}

// TestRestartMemtableSizeBoundedByActivitySinceLastFlush guards against a
// real regression found during Phase 3's own review: without deleting
// WAL segments once their data is durably flushed, wal.Replay at Open
// time reconstructs the *entire* database history into the memtable on
// every restart, not just the tail written since the last flush --
// silently defeating the whole point of flushing. Confirmed empirically
// before the fix: a run that flushed 14 SSTables and left ~2.6KB live in
// the memtable came back from Open with ~60KB back in memtable. This
// test pins the fixed behavior using entry *count*, not SizeBytes: two
// independently built skip lists holding identical key/value data can
// still report different SizeBytes (each node's level is chosen
// randomly, and replay's random draws don't match the original run's),
// but they must always have the same live entry count if replay
// correctly reconstructed only the unflushed tail.
func TestRestartMemtableSizeBoundedByActivitySinceLastFlush(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	db.SetFlushThreshold(4 * 1024)
	// Not a compaction test -- disable it for a deterministic flushed-table
	// count (see TestRestartAfterFlushReadsAllSSTables for why).
	db.SetL0CompactionThreshold(1 << 30)

	const n = 3000
	for i := 0; i < n; i++ {
		if err := db.Put(keyFor(i), valueFor(i)); err != nil {
			t.Fatalf("Put(%d): %v", i, err)
		}
	}
	countBeforeClose := countMemtableEntries(db.mem())
	if len(db.liveSSTables()) < 2 {
		t.Fatalf("only %d SSTable(s) flushed, want >= 2 for this test to exercise multi-flush WAL cleanup", len(db.liveSSTables()))
	}
	if countBeforeClose >= n {
		t.Fatalf("memtable holds %d entries out of %d total puts, want far fewer -- multiple flushes should have emptied it repeatedly", countBeforeClose, n)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	db2, err := Open(dir)
	if err != nil {
		t.Fatalf("Open (restart): %v", err)
	}
	defer db2.Close()

	countAfterRestart := countMemtableEntries(db2.mem())
	if countAfterRestart != countBeforeClose {
		t.Fatalf("memtable entry count after restart = %d, want %d (matching pre-close count) -- "+
			"WAL replay is re-ingesting flushed history instead of just the tail since the last flush",
			countAfterRestart, countBeforeClose)
	}

	// The read path must still be correct across all flush generations,
	// not just the bounded-memory property.
	for i := 0; i < n; i++ {
		v, found, err := db2.Get(keyFor(i))
		if err != nil || !found || !bytes.Equal(v, valueFor(i)) {
			t.Fatalf("Get(%s) after restart = %q found=%v err=%v, want %q true nil", keyFor(i), v, found, err, valueFor(i))
		}
	}
}

// TestOpenRemovesOrphanedFlushTmpFile simulates the on-disk aftermath of
// a crash that hit partway through FlushMemtable, before its atomic
// rename completed: a "*.sst.tmp" file with no corresponding committed
// ".sst" file. Open must clean it up rather than leaving it as a
// permanent disk-space leak, and must otherwise start up normally.
func TestOpenRemovesOrphanedFlushTmpFile(t *testing.T) {
	dir := t.TempDir()
	sstDir := filepath.Join(dir, sstableSubdir)
	if err := os.MkdirAll(sstDir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}

	orphan := filepath.Join(sstDir, "000001.sst.tmp")
	if err := os.WriteFile(orphan, []byte("partial garbage from an interrupted flush"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	if _, err := os.Stat(orphan); !os.IsNotExist(err) {
		t.Fatalf("orphaned .tmp file still present after Open (stat err = %v), want removed", err)
	}

	// Open must still function normally afterward.
	if err := db.Put([]byte("a"), []byte("1")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	v, found, err := db.Get([]byte("a"))
	if err != nil || !found || !bytes.Equal(v, []byte("1")) {
		t.Fatalf("Get(a) = %q found=%v err=%v, want 1 true nil", v, found, err)
	}
}

// TestOpenLeavesUnrelatedFilesAlone confirms the .tmp cleanup is scoped
// precisely to flush's own "*.sst.tmp" naming convention, not a broad
// pattern that could delete something else sitting in the sstables
// directory.
func TestOpenLeavesUnrelatedFilesAlone(t *testing.T) {
	dir := t.TempDir()
	sstDir := filepath.Join(dir, sstableSubdir)
	if err := os.MkdirAll(sstDir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}

	unrelated := filepath.Join(sstDir, "notes.txt.tmp")
	if err := os.WriteFile(unrelated, []byte("not an sstable"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	if _, err := os.Stat(unrelated); err != nil {
		t.Fatalf("unrelated file was removed (stat err = %v), want left alone -- cleanup must only match \"*.sst.tmp\"", err)
	}
}
