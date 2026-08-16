package lsm

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// TestPutReturnsHousekeepingErrorWhenFlushFails is the direct test of the
// gap closed by HousekeepingError: a Put/Delete whose WAL append and
// memtable mutation already succeeded, but whose triggered flush then
// fails, must still be treated by the caller as a durable write --
// distinguishable via errors.As from a failure that means the write
// itself didn't happen.
func TestPutReturnsHousekeepingErrorWhenFlushFails(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	db.SetFlushThreshold(1) // force maybeFlush to attempt a flush on the very next Put

	sstDir := filepath.Join(dir, sstableSubdir)
	if err := os.Chmod(sstDir, 0o500); err != nil {
		t.Fatalf("chmod sstables dir read-only: %v", err)
	}

	key, value := []byte("k"), []byte("v")
	putErr := db.Put(key, value)
	// Restore write permission immediately so t.TempDir()'s cleanup (and
	// this test's own recovery check below) can create/remove files
	// normally, regardless of how the assertions below turn out.
	if err := os.Chmod(sstDir, 0o755); err != nil {
		t.Fatalf("chmod sstables dir restore: %v", err)
	}

	if putErr == nil {
		t.Fatalf("Put: want an error (flush should have failed against a read-only sstables dir), got nil")
	}
	var hk *HousekeepingError
	if !errors.As(putErr, &hk) {
		t.Fatalf("Put error = %v (%T), want a *HousekeepingError", putErr, putErr)
	}

	// The write must already be fully durable despite the housekeeping
	// failure: visible right now, in this same process.
	v, found, gerr := db.Get(key)
	if gerr != nil || !found || string(v) != string(value) {
		t.Fatalf("Get(%q) = %q found=%v err=%v, want %q true nil", key, v, found, gerr, value)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// ...and it must also survive a full restart, recovered via WAL
	// replay exactly as if the housekeeping failure had never happened.
	db2, err := Open(dir)
	if err != nil {
		t.Fatalf("Open after restart: %v", err)
	}
	defer db2.Close()
	v, found, gerr = db2.Get(key)
	if gerr != nil || !found || string(v) != string(value) {
		t.Fatalf("after reopen: Get(%q) = %q found=%v err=%v, want %q true nil", key, v, found, gerr, value)
	}
}
