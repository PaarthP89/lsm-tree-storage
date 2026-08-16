package lsm

import (
	"os"
	"testing"
)

// TestScanIteratorSurvivesCompactionOfItsSources is the direct test of the
// gap closed by sstable.SSTable's AddRef/Close refcounting and
// ScanIterator.Close: a Scan iterator must keep reading correctly even
// after a later compaction retires (unlinks, and would otherwise close)
// every SSTable file it depends on.
func TestScanIteratorSurvivesCompactionOfItsSources(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	db.SetFlushThreshold(4096)
	db.SetL0CompactionThreshold(2)

	const count = 500
	for i := 0; i < count; i++ {
		if err := db.Put(keyFor(i), valueFor(i)); err != nil {
			t.Fatalf("Put(%d): %v", i, err)
		}
	}
	if err := db.MaybeCompact(); err != nil {
		t.Fatalf("MaybeCompact: %v", err)
	}

	it, err := db.Scan(keyFor(0), keyFor(count))
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	ri, ok := it.(*rangeIterator)
	if !ok {
		t.Fatalf("Scan returned %T, want *rangeIterator", it)
	}
	if len(ri.refs) == 0 {
		t.Fatalf("Scan iterator holds no SSTable refs -- test setup produced no live SSTables to protect")
	}
	var refPaths []string
	for _, st := range ri.refs {
		refPaths = append(refPaths, st.Meta().Path)
	}

	// Consume a few entries to prove the iterator is genuinely mid-scan,
	// not something that could trivially reopen its sources from scratch.
	got := map[string]string{}
	for i := 0; i < 5; i++ {
		if !it.Next() {
			t.Fatalf("Next() returned false too early, at entry %d", i)
		}
		got[string(it.Key())] = string(it.Value())
	}

	// Force every one of this iterator's source files to be retired: an
	// L0->L1 merge always includes every live L0 file (so any leftover
	// original L0 file is swept up by the very next one), and a tiny L1
	// size ratio forces an L1->L1 recompaction that merges every live L1
	// file -- including the original one this iterator is reading from --
	// into a brand-new file.
	db.SetL1SizeRatio(1)
	const moreCount = 2000
	for i := count; i < count+moreCount; i++ {
		if err := db.Put(keyFor(i), valueFor(i)); err != nil {
			t.Fatalf("Put(%d): %v", i, err)
		}
	}
	if err := db.MaybeCompact(); err != nil {
		t.Fatalf("MaybeCompact: %v", err)
	}

	for _, p := range refPaths {
		if _, err := os.Stat(p); !os.IsNotExist(err) {
			t.Fatalf("expected %s to have been unlinked by compaction, stat err = %v", p, err)
		}
	}

	// The iterator must still read every remaining entry correctly even
	// though its source files no longer exist in the directory -- their
	// file descriptors are kept open by this iterator's references
	// (POSIX unlink-while-open) until it releases them below.
	for it.Next() {
		got[string(it.Key())] = string(it.Value())
	}
	if len(got) != count {
		t.Fatalf("recovered %d keys from a surviving iterator, want %d", len(got), count)
	}
	for i := 0; i < count; i++ {
		k := string(keyFor(i))
		if got[k] != string(valueFor(i)) {
			t.Fatalf("key %s = %q, want %q", k, got[k], valueFor(i))
		}
	}

	if err := it.Close(); err != nil {
		t.Fatalf("Close after full drain (should be a no-op): %v", err)
	}
	if !ri.refsReleased {
		t.Fatalf("refsReleased = false after a fully-drained iterator, want true (Next should auto-release on exhaustion)")
	}
}

// TestScanIteratorCloseIsIdempotentAndReleasesEarly confirms an
// abandoned (never fully drained) Scan iterator's explicit Close releases
// its SSTable references, and that calling Close again is harmless.
func TestScanIteratorCloseIsIdempotentAndReleasesEarly(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	db.SetFlushThreshold(4096)
	const count = 200
	for i := 0; i < count; i++ {
		if err := db.Put(keyFor(i), valueFor(i)); err != nil {
			t.Fatalf("Put(%d): %v", i, err)
		}
	}

	it, err := db.Scan(keyFor(0), keyFor(count))
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	ri := it.(*rangeIterator)
	if len(ri.refs) == 0 {
		t.Fatalf("test setup produced no live SSTables to protect")
	}
	if !it.Next() {
		t.Fatalf("Next() returned false immediately, want at least one entry")
	}

	if ri.refsReleased {
		t.Fatalf("refsReleased = true before Close was ever called")
	}
	if err := it.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !ri.refsReleased {
		t.Fatalf("refsReleased = false after Close")
	}
	if err := it.Close(); err != nil {
		t.Fatalf("second Close call: %v, want nil (must be idempotent)", err)
	}
}
