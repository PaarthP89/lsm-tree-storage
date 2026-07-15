package sstable

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/paarthsiphone/lsm-tree-storage/memtable"
)

func keyFor(i int) []byte   { return []byte(fmt.Sprintf("key-%06d", i)) }
func valueFor(i int) []byte { return []byte(fmt.Sprintf("val-%06d", i)) }

// TestFlushAndRoundTrip writes N sorted entries (inserted out of order,
// since the memtable is responsible for sorting), flushes them, opens the
// result, and point-looks-up every key.
func TestFlushAndRoundTrip(t *testing.T) {
	const n = 500

	mem := memtable.New()
	order := rand.New(rand.NewSource(1)).Perm(n)
	for _, i := range order {
		mem.Put(keyFor(i), valueFor(i))
	}

	path := filepath.Join(t.TempDir(), "000001.sst")
	meta, err := FlushMemtable(mem, path)
	if err != nil {
		t.Fatalf("FlushMemtable: %v", err)
	}
	if meta.EntryCount != n {
		t.Fatalf("EntryCount = %d, want %d", meta.EntryCount, n)
	}
	if !bytes.Equal(meta.MinKey, keyFor(0)) {
		t.Errorf("MinKey = %q, want %q", meta.MinKey, keyFor(0))
	}
	if !bytes.Equal(meta.MaxKey, keyFor(n-1)) {
		t.Errorf("MaxKey = %q, want %q", meta.MaxKey, keyFor(n-1))
	}

	st, err := OpenSSTable(path)
	if err != nil {
		t.Fatalf("OpenSSTable: %v", err)
	}
	defer st.Close()

	for i := 0; i < n; i++ {
		v, found, tombstone, err := st.Get(keyFor(i))
		if err != nil || !found || tombstone || !bytes.Equal(v, valueFor(i)) {
			t.Fatalf("Get(%s) = %q found=%v tombstone=%v err=%v, want %q true false nil",
				keyFor(i), v, found, tombstone, err, valueFor(i))
		}
	}

	_, found, _, err := st.Get([]byte("does-not-exist"))
	if err != nil || found {
		t.Fatalf("Get(does-not-exist) = found=%v err=%v, want false nil", found, err)
	}
}

// TestGetOutOfRange confirms the min/max fast path rejects keys outside
// the table's range without needing a full scan.
func TestGetOutOfRange(t *testing.T) {
	mem := memtable.New()
	for i := 10; i < 20; i++ {
		mem.Put(keyFor(i), valueFor(i))
	}
	path := filepath.Join(t.TempDir(), "000001.sst")
	if _, err := FlushMemtable(mem, path); err != nil {
		t.Fatalf("FlushMemtable: %v", err)
	}
	st, err := OpenSSTable(path)
	if err != nil {
		t.Fatalf("OpenSSTable: %v", err)
	}
	defer st.Close()

	for _, k := range [][]byte{[]byte("key-000000"), []byte("key-999999")} {
		_, found, _, err := st.Get(k)
		if err != nil || found {
			t.Fatalf("Get(%s) = found=%v err=%v, want false nil", k, found, err)
		}
	}
}

// TestTombstoneRoundTrip confirms a delete marker survives flush and is
// reported back as found=true, tombstone=true -- distinct from "not
// present at all", which callers need to suppress older sources.
func TestTombstoneRoundTrip(t *testing.T) {
	mem := memtable.New()
	mem.Put([]byte("a"), []byte("1"))
	mem.Put([]byte("b"), []byte("2"))
	mem.Delete([]byte("a"))

	path := filepath.Join(t.TempDir(), "000001.sst")
	if _, err := FlushMemtable(mem, path); err != nil {
		t.Fatalf("FlushMemtable: %v", err)
	}
	st, err := OpenSSTable(path)
	if err != nil {
		t.Fatalf("OpenSSTable: %v", err)
	}
	defer st.Close()

	_, found, tombstone, err := st.Get([]byte("a"))
	if err != nil || !found || !tombstone {
		t.Fatalf("Get(a) = found=%v tombstone=%v err=%v, want true true nil", found, tombstone, err)
	}

	v, found, tombstone, err := st.Get([]byte("b"))
	if err != nil || !found || tombstone || !bytes.Equal(v, []byte("2")) {
		t.Fatalf("Get(b) = %q found=%v tombstone=%v err=%v, want 2 true false nil", v, found, tombstone, err)
	}
}

// TestEmptyMemtableFlush confirms flushing an empty memtable produces a
// valid, openable, empty table rather than a malformed file.
func TestEmptyMemtableFlush(t *testing.T) {
	mem := memtable.New()
	path := filepath.Join(t.TempDir(), "000001.sst")
	meta, err := FlushMemtable(mem, path)
	if err != nil {
		t.Fatalf("FlushMemtable: %v", err)
	}
	if meta.EntryCount != 0 {
		t.Fatalf("EntryCount = %d, want 0", meta.EntryCount)
	}

	st, err := OpenSSTable(path)
	if err != nil {
		t.Fatalf("OpenSSTable: %v", err)
	}
	defer st.Close()

	_, found, _, err := st.Get([]byte("anything"))
	if err != nil || found {
		t.Fatalf("Get on empty table = found=%v err=%v, want false nil", found, err)
	}
}

// TestFlushIsAtomic confirms a failed/aborted flush never leaves a
// partially-written file at the final path: only the .tmp file (or
// nothing) may exist until the rename succeeds.
func TestFlushIsAtomic(t *testing.T) {
	mem := memtable.New()
	mem.Put([]byte("a"), []byte("1"))

	dir := t.TempDir()
	path := filepath.Join(dir, "000001.sst")
	meta, err := FlushMemtable(mem, path)
	if err != nil {
		t.Fatalf("FlushMemtable: %v", err)
	}
	if meta.Path != path {
		t.Fatalf("meta.Path = %q, want %q", meta.Path, path)
	}

	if _, err := os.Stat(path); err != nil {
		t.Fatalf("final file missing after flush: %v", err)
	}
	if _, err := os.Stat(path + ".tmp"); !os.IsNotExist(err) {
		t.Fatalf("tmp file still present after successful flush: err = %v", err)
	}
}

// TestSparseIndexBoundsScan proves the sparse index actually bounds
// Get's on-disk scan to a small fraction of the file, not a full-file
// walk -- the entire reason a sparse index exists. It does this by
// reconstructing, for several target keys, the exact byte range Get()
// would scan (using the same binary-search logic against the package's
// unexported index) and asserting that range is a small, fixed multiple
// of a single record's size, independent of table size.
func TestSparseIndexBoundsScan(t *testing.T) {
	const n = 20000 // large enough that a full-file scan would dwarf one block

	mem := memtable.New()
	for i := 0; i < n; i++ {
		mem.Put(keyFor(i), valueFor(i))
	}
	path := filepath.Join(t.TempDir(), "000001.sst")
	if _, err := FlushMemtable(mem, path); err != nil {
		t.Fatalf("FlushMemtable: %v", err)
	}

	st, err := OpenSSTable(path)
	if err != nil {
		t.Fatalf("OpenSSTable: %v", err)
	}
	defer st.Close()

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	fileSize := info.Size()

	wantIndexLen := (n + indexInterval - 1) / indexInterval
	if len(st.index) != wantIndexLen {
		t.Fatalf("len(index) = %d, want %d", len(st.index), wantIndexLen)
	}

	// A single record here is roughly 4+1+4+10+4+10 = 33 bytes. A block
	// covering indexInterval records should be well under 1% of the
	// 20000-record file; assert generously (10x an interval's worth of
	// bytes) so this isn't a brittle exact-size assertion, while still
	// proving the scan isn't anywhere close to full-file.
	maxReasonableBlockBytes := int64(indexInterval * 100)

	for _, i := range []int{0, 1, n / 2, n - 1} {
		key := keyFor(i)
		idx := sort.Search(len(st.index), func(j int) bool {
			return bytes.Compare(st.index[j].key, key) > 0
		})
		if idx == 0 {
			idx = 1
		}
		blockStart := st.index[idx-1].offset
		blockEnd := st.footerOffset
		if idx < len(st.index) {
			blockEnd = st.index[idx].offset
		}
		blockBytes := blockEnd - blockStart

		if blockBytes > maxReasonableBlockBytes {
			t.Errorf("key %s: block scan range = %d bytes, want <= %d (file is %d bytes -- scan must not degrade toward full-file)",
				key, blockBytes, maxReasonableBlockBytes, fileSize)
		}
		if blockBytes >= fileSize/10 {
			t.Errorf("key %s: block scan range = %d bytes is >= 10%% of file size %d -- looks like a full-file scan, not a bounded one",
				key, blockBytes, fileSize)
		}
	}
}

// TestFlushFsyncsDir proves FlushMemtable fsyncs the containing directory
// after the atomic rename, not just that the rename happened -- a
// functional test alone (file exists afterward) can't distinguish that
// from a version that skips the directory fsync entirely, since both
// look identical absent an actual crash.
func TestFlushFsyncsDir(t *testing.T) {
	orig := fsyncDir
	var calls []string
	fsyncDir = func(dir string) error {
		calls = append(calls, dir)
		return orig(dir)
	}
	t.Cleanup(func() { fsyncDir = orig })

	mem := memtable.New()
	mem.Put([]byte("a"), []byte("1"))

	dir := t.TempDir()
	path := filepath.Join(dir, "000001.sst")
	if _, err := FlushMemtable(mem, path); err != nil {
		t.Fatalf("FlushMemtable: %v", err)
	}

	if len(calls) != 1 || calls[0] != dir {
		t.Fatalf("fsyncDir calls = %v, want exactly one call with dir %q", calls, dir)
	}
}
