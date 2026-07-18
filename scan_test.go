package lsm

import (
	"bytes"
	"testing"

	"github.com/paarthsiphone/lsm-tree-storage/memtable"
)

// drainScan consumes it fully, asserting strictly increasing keys and no
// tombstones ever surface (rangeIterator's Tombstone() must always be
// false, since a tombstoned key must never be yielded at all), and
// returns the collected key/value pairs.
func drainScan(t *testing.T, it memtable.Iterator) map[string]string {
	t.Helper()
	out := map[string]string{}
	var lastKey []byte
	for it.Next() {
		k, v := it.Key(), it.Value()
		if lastKey != nil && bytes.Compare(lastKey, k) >= 0 {
			t.Fatalf("scan keys not strictly increasing: %q then %q", lastKey, k)
		}
		lastKey = append([]byte(nil), k...)
		if it.Tombstone() {
			t.Fatalf("scan yielded a tombstone for key %q", k)
		}
		if _, dup := out[string(k)]; dup {
			t.Fatalf("scan yielded key %q more than once", k)
		}
		out[string(k)] = string(v)
	}
	return out
}

func TestScanInvalidRange(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	_, err = db.Scan([]byte("z"), []byte("a"))
	if err != ErrInvalidRange {
		t.Fatalf("Scan(z, a) err = %v, want ErrInvalidRange", err)
	}
}

func TestScanEmptyRangeIsImmediatelyDone(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	for _, k := range []string{"a", "b", "c"} {
		if err := db.Put([]byte(k), []byte(k+"-val")); err != nil {
			t.Fatalf("Put(%s): %v", k, err)
		}
	}

	it, err := db.Scan([]byte("b"), []byte("b"))
	if err != nil {
		t.Fatalf("Scan(b, b): %v", err)
	}
	if it.Next() {
		t.Fatalf("Scan(start, start) yielded a result, want immediately done")
	}
}

func TestScanNoMatches(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	for _, k := range []string{"a", "b", "c"} {
		if err := db.Put([]byte(k), []byte(k+"-val")); err != nil {
			t.Fatalf("Put(%s): %v", k, err)
		}
	}

	it, err := db.Scan([]byte("x"), []byte("z"))
	if err != nil {
		t.Fatalf("Scan(x, z): %v", err)
	}
	got := drainScan(t, it)
	if len(got) != 0 {
		t.Fatalf("Scan(x, z) = %v, want empty", got)
	}
}

func TestScanMemtableOnly(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	want := map[string]string{
		"b": "2", "c": "3", "d": "4",
	}
	for k, v := range want {
		if err := db.Put([]byte(k), []byte(v)); err != nil {
			t.Fatalf("Put(%s): %v", k, err)
		}
	}
	// Outside the [b,e) range queried below -- must not appear.
	if err := db.Put([]byte("a"), []byte("1")); err != nil {
		t.Fatalf("Put(a): %v", err)
	}
	if err := db.Put([]byte("e"), []byte("5")); err != nil {
		t.Fatalf("Put(e): %v", err)
	}
	if len(db.liveSSTables()) != 0 {
		t.Fatalf("setup produced %d SSTables, want 0 (memtable-only test)", len(db.liveSSTables()))
	}

	it, err := db.Scan([]byte("b"), []byte("e"))
	if err != nil {
		t.Fatalf("Scan(b, e): %v", err)
	}
	got := drainScan(t, it)
	if len(got) != len(want) {
		t.Fatalf("Scan(b, e) = %v, want %v", got, want)
	}
	for k, v := range want {
		if got[k] != v {
			t.Fatalf("Scan(b, e)[%s] = %q, want %q", k, got[k], v)
		}
	}
}

// TestScanSSTableOnly confirms a range entirely satisfied by flushed
// SSTables (nothing left in the memtable at scan time) is walked
// correctly via SSTable.SeekIterator alone.
func TestScanSSTableOnly(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()
	db.SetFlushThreshold(1) // flush after every Put -- memtable is empty between calls

	want := map[string]string{
		"m": "13", "n": "14", "o": "15", "p": "16",
	}
	for k, v := range want {
		if err := db.Put([]byte(k), []byte(v)); err != nil {
			t.Fatalf("Put(%s): %v", k, err)
		}
	}
	if db.mem().SizeBytes() != 0 {
		t.Fatalf("setup: memtable not empty (SizeBytes=%d), test wants an SSTable-only scan", db.mem().SizeBytes())
	}
	if len(db.liveSSTables()) < 4 {
		t.Fatalf("setup produced %d SSTables, want >= 4 (one per key)", len(db.liveSSTables()))
	}

	it, err := db.Scan([]byte("m"), []byte("q"))
	if err != nil {
		t.Fatalf("Scan(m, q): %v", err)
	}
	got := drainScan(t, it)
	if len(got) != len(want) {
		t.Fatalf("Scan(m, q) = %v, want %v", got, want)
	}
	for k, v := range want {
		if got[k] != v {
			t.Fatalf("Scan(m, q)[%s] = %q, want %q", k, got[k], v)
		}
	}
}

// TestScanAcrossMemtableAndSSTables forces some data into flushed
// SSTables and leaves the rest in the memtable, including a key that's
// overwritten after its generation was flushed -- the newest-wins case
// generalized from a single Get to a range.
func TestScanAcrossMemtableAndSSTables(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()
	db.SetFlushThreshold(1) // flush after every Put, so each lands in its own SSTable

	if err := db.Put([]byte("b"), []byte("b-gen1")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := db.Put([]byte("d"), []byte("d-only")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if len(db.liveSSTables()) < 2 {
		t.Fatalf("setup produced %d SSTables, want >= 2", len(db.liveSSTables()))
	}

	db.SetFlushThreshold(1 << 30) // stop flushing: rest of the writes stay in the memtable
	if err := db.Put([]byte("b"), []byte("b-gen2")); err != nil {
		t.Fatalf("Put (overwrite): %v", err)
	}
	if err := db.Put([]byte("c"), []byte("c-only")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	it, err := db.Scan([]byte("a"), []byte("z"))
	if err != nil {
		t.Fatalf("Scan(a, z): %v", err)
	}
	got := drainScan(t, it)

	want := map[string]string{
		"b": "b-gen2", // memtable's newer write must win over the flushed generation
		"c": "c-only",
		"d": "d-only",
	}
	if len(got) != len(want) {
		t.Fatalf("Scan(a, z) = %v, want %v", got, want)
	}
	for k, v := range want {
		if got[k] != v {
			t.Fatalf("Scan(a, z)[%s] = %q, want %q", k, got[k], v)
		}
	}
}

// TestScanExcludesTombstoneAcrossSources confirms a key deleted after its
// original write was already flushed to an SSTable never appears in Scan
// results, even though the write and the tombstone live in different
// sources.
func TestScanExcludesTombstoneAcrossSources(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()
	db.SetFlushThreshold(1)

	if err := db.Put([]byte("a"), []byte("1")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := db.Put([]byte("b"), []byte("2")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if len(db.liveSSTables()) < 2 {
		t.Fatalf("setup produced %d SSTables, want >= 2", len(db.liveSSTables()))
	}

	db.SetFlushThreshold(1 << 30)
	if err := db.Delete([]byte("a")); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	it, err := db.Scan([]byte("a"), []byte("z"))
	if err != nil {
		t.Fatalf("Scan(a, z): %v", err)
	}
	got := drainScan(t, it)

	want := map[string]string{"b": "2"}
	if len(got) != len(want) || got["b"] != "2" {
		t.Fatalf("Scan(a, z) = %v, want %v ('a' must be excluded, tombstoned in the memtable over an older SSTable write)", got, want)
	}
}

// TestScanHalfOpenRangeExcludesEnd confirms end itself is excluded (the
// half-open [start, end) contract), while start is included.
func TestScanHalfOpenRangeExcludesEnd(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	for _, k := range []string{"b", "c", "d"} {
		if err := db.Put([]byte(k), []byte(k)); err != nil {
			t.Fatalf("Put(%s): %v", k, err)
		}
	}

	it, err := db.Scan([]byte("b"), []byte("d"))
	if err != nil {
		t.Fatalf("Scan(b, d): %v", err)
	}
	got := drainScan(t, it)
	want := map[string]string{"b": "b", "c": "c"}
	if len(got) != len(want) || got["b"] != "b" || got["c"] != "c" {
		t.Fatalf("Scan(b, d) = %v, want %v (start included, end excluded)", got, want)
	}
}
