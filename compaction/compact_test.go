package compaction

import (
	"bytes"
	"path/filepath"
	"testing"

	"github.com/paarthsiphone/lsm-tree-storage/memtable"
	"github.com/paarthsiphone/lsm-tree-storage/sstable"
)

func flushToSSTable(t *testing.T, dir, name string, puts map[string]string, deletes []string) *sstable.SSTable {
	t.Helper()
	mem := memtable.New()
	for k, v := range puts {
		mem.Put([]byte(k), []byte(v))
	}
	for _, k := range deletes {
		mem.Delete([]byte(k))
	}
	path := filepath.Join(dir, name)
	if _, err := sstable.FlushMemtable(mem, path); err != nil {
		t.Fatalf("FlushMemtable(%s): %v", name, err)
	}
	st, err := sstable.OpenSSTable(path)
	if err != nil {
		t.Fatalf("OpenSSTable(%s): %v", name, err)
	}
	t.Cleanup(func() { st.Close() })
	return st
}

// TestCompactRealSSTablesRoundTrip mirrors the Phase 6 brief's "done"
// example almost exactly: 4 SSTables with overlapping key ranges,
// including a tombstone (newest) for a key that also has an older Put in
// a different file. After compaction the output must have exactly one
// entry per distinct live key, the newest value wins on overlap, and the
// tombstoned key is entirely absent.
func TestCompactRealSSTablesRoundTrip(t *testing.T) {
	dir := t.TempDir()

	// Oldest to newest in wall-clock terms, but Compact takes inputs
	// newest-first (index 0 = highest rank/newest), matching db.sstables'
	// own convention -- so table4 (containing the tombstone) is inputs[0].
	table1 := flushToSSTable(t, dir, "000001.sst", map[string]string{
		"a": "a-gen1", "b": "b-gen1", "e": "e-only",
	}, nil)
	table2 := flushToSSTable(t, dir, "000002.sst", map[string]string{
		"a": "a-gen2", "c": "c-only",
	}, nil)
	table3 := flushToSSTable(t, dir, "000003.sst", map[string]string{
		"b": "b-gen3", "d": "d-only",
	}, nil)
	table4 := flushToSSTable(t, dir, "000004.sst", nil, []string{"e"}) // tombstone for "e"

	inputs := []*sstable.SSTable{table4, table3, table2, table1} // newest-first

	outPath := filepath.Join(dir, "000005.sst")
	meta, err := Compact(inputs, outPath)
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}

	wantEntries := map[string]string{
		"a": "a-gen2", // newest write for "a" is in table2
		"b": "b-gen3", // newest write for "b" is in table3
		"c": "c-only",
		"d": "d-only",
		// "e" is entirely absent: table4's tombstone is the newest entry
		// and there's no older data left underneath after compaction.
	}
	if meta.EntryCount != len(wantEntries) {
		t.Fatalf("EntryCount = %d, want %d", meta.EntryCount, len(wantEntries))
	}

	out, err := sstable.OpenSSTable(outPath)
	if err != nil {
		t.Fatalf("OpenSSTable(output): %v", err)
	}
	defer out.Close()

	for k, want := range wantEntries {
		v, found, tombstone, err := out.Get([]byte(k))
		if err != nil || !found || tombstone || string(v) != want {
			t.Fatalf("Get(%s) = %q found=%v tombstone=%v err=%v, want %q true false nil", k, v, found, tombstone, err, want)
		}
	}

	_, found, _, err := out.Get([]byte("e"))
	if err != nil || found {
		t.Fatalf("Get(e) = found=%v err=%v, want false nil (tombstoned key must be entirely absent)", found, err)
	}
}

// TestCompactOutputIsSorted confirms the merge produces a genuinely
// sorted output -- Phase 3's sparse-index reader depends on this, so it's
// not just a nice property, it's load-bearing for every future Get
// against the compacted file.
func TestCompactOutputIsSorted(t *testing.T) {
	dir := t.TempDir()

	table1 := flushToSSTable(t, dir, "000001.sst", map[string]string{
		"m": "1", "z": "2", "c": "3",
	}, nil)
	table2 := flushToSSTable(t, dir, "000002.sst", map[string]string{
		"a": "4", "y": "5", "n": "6",
	}, nil)

	outPath := filepath.Join(dir, "000003.sst")
	if _, err := Compact([]*sstable.SSTable{table2, table1}, outPath); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	out, err := sstable.OpenSSTable(outPath)
	if err != nil {
		t.Fatalf("OpenSSTable(output): %v", err)
	}
	defer out.Close()

	it := out.Iterator()
	var prev []byte
	count := 0
	for it.Next() {
		if prev != nil && bytes.Compare(prev, it.Key()) >= 0 {
			t.Fatalf("output not strictly sorted: %q then %q", prev, it.Key())
		}
		prev = append([]byte(nil), it.Key()...)
		count++
	}
	if err := it.Err(); err != nil {
		t.Fatalf("Iterator.Err(): %v", err)
	}
	if count != 6 {
		t.Fatalf("scanned %d entries, want 6", count)
	}
}

// TestCompactEmptyInputsProducesEmptyTable confirms compacting a set of
// inputs that collectively contain no live keys (e.g. every key is
// tombstoned) produces a valid, openable, empty table rather than
// erroring or leaving a malformed file.
func TestCompactEmptyInputsProducesEmptyTable(t *testing.T) {
	dir := t.TempDir()
	table1 := flushToSSTable(t, dir, "000001.sst", map[string]string{"a": "1"}, nil)
	table2 := flushToSSTable(t, dir, "000002.sst", nil, []string{"a"})

	outPath := filepath.Join(dir, "000003.sst")
	meta, err := Compact([]*sstable.SSTable{table2, table1}, outPath)
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if meta.EntryCount != 0 {
		t.Fatalf("EntryCount = %d, want 0", meta.EntryCount)
	}

	out, err := sstable.OpenSSTable(outPath)
	if err != nil {
		t.Fatalf("OpenSSTable(output): %v", err)
	}
	defer out.Close()

	_, found, _, err := out.Get([]byte("a"))
	if err != nil || found {
		t.Fatalf("Get(a) on fully-tombstoned compaction output = found=%v err=%v, want false nil", found, err)
	}
}

// TestCompactOutputHasBloomFilter confirms compaction output automatically
// gets a bloom filter (Phase 8a), since it goes through the exact same
// sstable.FlushIterator write path a memtable flush does -- no special
// casing needed in the compaction package itself. It also confirms
// negative lookups against the compacted file are answered correctly,
// which is what the filter's short-circuit path in sstable.Get depends on.
func TestCompactOutputHasBloomFilter(t *testing.T) {
	dir := t.TempDir()

	puts := map[string]string{}
	for i := 0; i < 2000; i++ {
		puts[keyForN(i)] = valueForN(i)
	}
	table1 := flushToSSTable(t, dir, "000001.sst", puts, nil)

	outPath := filepath.Join(dir, "000002.sst")
	if _, err := Compact([]*sstable.SSTable{table1}, outPath); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	out, err := sstable.OpenSSTable(outPath)
	if err != nil {
		t.Fatalf("OpenSSTable(output): %v", err)
	}
	defer out.Close()

	if !out.HasBloomFilter() {
		t.Fatalf("HasBloomFilter() = false on compaction output, want true")
	}

	for i := 0; i < 2000; i++ {
		v, found, tombstone, err := out.Get([]byte(keyForN(i)))
		if err != nil || !found || tombstone || string(v) != valueForN(i) {
			t.Fatalf("Get(%s) = %q found=%v tombstone=%v err=%v, want %q true false nil",
				keyForN(i), v, found, tombstone, err, valueForN(i))
		}
	}
	for i := 2000; i < 4000; i++ {
		_, found, _, err := out.Get([]byte(keyForN(i)))
		if err != nil || found {
			t.Fatalf("Get(%s) on absent key = found=%v err=%v, want false nil", keyForN(i), found, err)
		}
	}
}

func keyForN(i int) string   { return "key-" + itoa(i) }
func valueForN(i int) string { return "val-" + itoa(i) }

func itoa(i int) string {
	// Fixed-width so lexical sort matches numeric order, matching the
	// convention used elsewhere in this codebase's tests.
	s := make([]byte, 6)
	for p := 5; p >= 0; p-- {
		s[p] = byte('0' + i%10)
		i /= 10
	}
	return string(s)
}
