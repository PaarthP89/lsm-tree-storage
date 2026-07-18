package lsm

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/paarthsiphone/lsm-tree-storage/manifest"
)

// manifestHasAnyRemoved reports whether edits contains at least one
// SSTableRemoved. Flush never emits SSTableRemoved (maybeFlush only ever
// appends SSTableAdded) -- so any REMOVED edit at all is direct proof
// that MaybeCompact actually ran, not just that flushes happened.
func manifestHasAnyRemoved(edits []manifest.VersionEdit) bool {
	for _, e := range edits {
		if e.Type == manifest.SSTableRemoved {
			return true
		}
	}
	return false
}

// TestCompactionTriggerFiresAutomatically confirms MaybeCompact's
// post-flush wiring actually engages once the live SSTable count exceeds
// compactionTriggerThreshold, without any test calling MaybeCompact
// directly -- purely from ordinary Put traffic through a small flush
// threshold.
func TestCompactionTriggerFiresAutomatically(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()
	db.SetFlushThreshold(4096)

	const n = 800
	for i := 0; i < n; i++ {
		if err := db.Put(keyFor(i), valueFor(i)); err != nil {
			t.Fatalf("Put(%d): %v", i, err)
		}
	}

	if len(db.sstables) > compactionTriggerThreshold {
		t.Fatalf("live SSTable count = %d, want <= %d (compaction should have fired at least once)", len(db.sstables), compactionTriggerThreshold)
	}

	edits, err := manifest.ReplayManifest(db.manifestPath)
	if err != nil {
		t.Fatalf("ReplayManifest: %v", err)
	}
	if !manifestHasAnyRemoved(edits) {
		t.Fatal("MANIFEST has no SSTableRemoved edits -- compaction never actually ran")
	}

	for i := 0; i < n; i++ {
		v, found, err := db.Get(keyFor(i))
		if err != nil || !found || !bytes.Equal(v, valueFor(i)) {
			t.Fatalf("Get(%s) = %q found=%v err=%v, want %q true nil", keyFor(i), v, found, err, valueFor(i))
		}
	}
}

// TestCompactionCleansUpRedundantOldFilesOnSuccessPath confirms that on
// the ordinary (non-crash) path, once both MANIFEST edits for a
// compaction are durable, MaybeCompact actually deletes the now-retired
// input files from disk -- the brief only excuses leaving redundant files
// behind in the crash-4 scenario, not on every successful run.
func TestCompactionCleansUpRedundantOldFilesOnSuccessPath(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()
	db.SetFlushThreshold(4096)

	const n = 800
	for i := 0; i < n; i++ {
		if err := db.Put(keyFor(i), valueFor(i)); err != nil {
			t.Fatalf("Put(%d): %v", i, err)
		}
	}

	edits, err := manifest.ReplayManifest(db.manifestPath)
	if err != nil {
		t.Fatalf("ReplayManifest: %v", err)
	}
	live := manifest.ReconstructSSTableSet(edits)

	sstDir := filepath.Join(dir, sstableSubdir)
	des, err := os.ReadDir(sstDir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	liveSet := make(map[string]struct{}, len(live))
	for _, name := range live {
		liveSet[name] = struct{}{}
	}
	for _, de := range des {
		name := de.Name()
		if filepath.Ext(name) != ".sst" {
			continue
		}
		if _, ok := liveSet[name]; !ok {
			t.Fatalf("retired file %s still present on disk after successful compaction, want deleted", name)
		}
	}
}

// TestCompactionPreservesCorrectnessAcrossOverwritesAndDeletes is the
// post-compaction read correctness test the Phase 6 brief's "definition
// of done" calls for: keys that were overwritten and deleted across
// several different pre-compaction SSTable generations must still
// resolve to exactly the right answer -- newest surviving value, or
// entirely absent if tombstoned -- once compaction has actually merged
// those generations into one file.
func TestCompactionPreservesCorrectnessAcrossOverwritesAndDeletes(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()
	db.SetFlushThreshold(2 * 1024)

	const n = 500
	key := func(i int) []byte { return []byte(fmt.Sprintf("key-%06d", i)) }

	// Generation 1: every key gets an initial value, flushed across
	// several SSTables.
	for i := 0; i < n; i++ {
		if err := db.Put(key(i), []byte("gen1")); err != nil {
			t.Fatalf("Put gen1(%d): %v", i, err)
		}
	}
	// Generation 2: every even key is overwritten, in later flushes than
	// generation 1 -- forcing the newest-wins case to span multiple
	// distinct pre-compaction files.
	for i := 0; i < n; i += 2 {
		if err := db.Put(key(i), []byte("gen2")); err != nil {
			t.Fatalf("Put gen2(%d): %v", i, err)
		}
	}
	// Generation 3: every key divisible by 4 is deleted, in yet later
	// flushes.
	for i := 0; i < n; i += 4 {
		if err := db.Delete(key(i)); err != nil {
			t.Fatalf("Delete(%d): %v", i, err)
		}
	}

	edits, err := manifest.ReplayManifest(db.manifestPath)
	if err != nil {
		t.Fatalf("ReplayManifest: %v", err)
	}
	if !manifestHasAnyRemoved(edits) {
		t.Fatal("MANIFEST has no SSTableRemoved edits -- compaction never actually ran, so this test isn't exercising what it claims to")
	}

	for i := 0; i < n; i++ {
		v, found, err := db.Get(key(i))
		switch {
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
