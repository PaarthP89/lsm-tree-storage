package sstable

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/paarthsiphone/lsm-tree-storage/memtable"
	"github.com/paarthsiphone/lsm-tree-storage/wal"
)

// TestBloomFilterZeroFalseNegatives is the hard correctness requirement:
// every key that was Add-ed must always report MayContain == true. A
// single false negative here would mean sstable.Get could wrongly report
// "definitely absent" for a key that's actually present -- silent data
// loss on read, not just a slow path.
func TestBloomFilterZeroFalseNegatives(t *testing.T) {
	const n = 50_000
	keys := make([][]byte, n)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("present-key-%d-%d", i, rand.New(rand.NewSource(int64(i))).Int()))
	}

	b := newBloomFilter(n, defaultBloomFPR)
	if b == nil {
		t.Fatalf("newBloomFilter(%d, ...) = nil, want non-nil", n)
	}
	for _, k := range keys {
		b.Add(k)
	}
	for _, k := range keys {
		if !b.MayContain(k) {
			t.Fatalf("MayContain(%q) = false after Add, want true (false negative)", k)
		}
	}
}

// TestBloomFilterFalsePositiveRate confirms the observed false-positive
// rate lands within a reasonable tolerance band of the target -- this is
// inherently probabilistic, so the assertion is a band, not an exact
// value.
func TestBloomFilterFalsePositiveRate(t *testing.T) {
	const n = 20_000
	const targetFPR = defaultBloomFPR

	rng := rand.New(rand.NewSource(42))
	present := make(map[string]bool, n)
	b := newBloomFilter(n, targetFPR)
	for i := 0; i < n; i++ {
		k := []byte(fmt.Sprintf("present-%d", i))
		present[string(k)] = true
		b.Add(k)
	}

	const trials = 100_000
	falsePositives := 0
	for i := 0; i < trials; i++ {
		k := []byte(fmt.Sprintf("absent-%d-%d", i, rng.Int()))
		if present[string(k)] {
			continue // astronomically unlikely, but don't count a real hit as a FP
		}
		if b.MayContain(k) {
			falsePositives++
		}
	}

	observedFPR := float64(falsePositives) / float64(trials)
	// Generous band: 3x the target on the high side (catches a badly
	// broken sizing/hash formula) and a low-side sanity floor (catches a
	// filter that's accidentally always returning false, which would
	// otherwise look like a "great" FPR).
	if observedFPR > targetFPR*3 {
		t.Errorf("observed FPR %.4f, want <= %.4f (3x target %.4f)", observedFPR, targetFPR*3, targetFPR)
	}
	if observedFPR < targetFPR/10 {
		t.Errorf("observed FPR %.5f suspiciously low vs target %.4f -- MayContain may be broken (e.g. always false)", observedFPR, targetFPR)
	}
	t.Logf("observed FPR = %.4f (target %.4f, n=%d)", observedFPR, targetFPR, n)
}

// TestNewBloomFilterZeroEntries confirms the n<=0 edge case: no filter is
// built, and callers (FlushIterator) must treat that as "no filter
// present" rather than crashing.
func TestNewBloomFilterZeroEntries(t *testing.T) {
	if b := newBloomFilter(0, defaultBloomFPR); b != nil {
		t.Fatalf("newBloomFilter(0, ...) = %v, want nil", b)
	}
	if b := newBloomFilter(-5, defaultBloomFPR); b != nil {
		t.Fatalf("newBloomFilter(-5, ...) = %v, want nil", b)
	}
}

// TestFlushBuildsBloomFilter confirms a non-empty flush produces a table
// with a working filter: present keys are never rejected, and at least
// some absent keys are (proving the filter is actually wired in, not just
// present-but-inert).
func TestFlushBuildsBloomFilter(t *testing.T) {
	const n = 2000
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

	if !st.HasBloomFilter() {
		t.Fatalf("HasBloomFilter() = false, want true for a %d-entry table", n)
	}

	for i := 0; i < n; i++ {
		v, found, tombstone, err := st.Get(keyFor(i))
		if err != nil || !found || tombstone || !bytes.Equal(v, valueFor(i)) {
			t.Fatalf("Get(%s) = %q found=%v tombstone=%v err=%v, want %q true false nil",
				keyFor(i), v, found, tombstone, err, valueFor(i))
		}
	}

	rejectedByFilter := 0
	for i := n; i < n+5000; i++ {
		k := keyFor(i)
		if !st.bloom.MayContain(k) {
			rejectedByFilter++
		}
	}
	if rejectedByFilter == 0 {
		t.Fatalf("filter rejected 0 of 5000 absent keys, want most rejected (filter looks inert)")
	}
}

// TestEmptyMemtableFlushHasNoBloomFilter confirms the n=0 edge case end
// to end: an empty flush produces a table with no filter, and Get still
// works correctly (falls through, finds nothing, no divide-by-zero/panic).
func TestEmptyMemtableFlushHasNoBloomFilter(t *testing.T) {
	mem := memtable.New()
	path := filepath.Join(t.TempDir(), "000001.sst")
	if _, err := FlushMemtable(mem, path); err != nil {
		t.Fatalf("FlushMemtable: %v", err)
	}
	st, err := OpenSSTable(path)
	if err != nil {
		t.Fatalf("OpenSSTable: %v", err)
	}
	defer st.Close()

	if st.HasBloomFilter() {
		t.Fatalf("HasBloomFilter() = true for an empty table, want false")
	}
	_, found, _, err := st.Get([]byte("anything"))
	if err != nil || found {
		t.Fatalf("Get on empty table = found=%v err=%v, want false nil", found, err)
	}
}

// buildPreBloomSSTable hand-writes an SSTable file in the exact pre-8a
// wire format (no bloom section at all in the footer body), bypassing
// FlushIterator/encodeFooter entirely, to prove OpenSSTable/Get against a
// real pre-Phase-8a file still works: opens without error, and every
// lookup falls through to the full sparse-index scan rather than
// misparsing trailing garbage as a bloom section.
func buildPreBloomSSTable(t *testing.T, path string, entries []wal.Entry) {
	t.Helper()

	var data bytes.Buffer
	var index []indexEntry
	var minKey, maxKey []byte
	for i, e := range entries {
		if i == 0 {
			minKey = e.Key
		}
		maxKey = e.Key
		if i%indexInterval == 0 {
			index = append(index, indexEntry{key: e.Key, offset: int64(data.Len())})
		}
		data.Write(wal.EncodeEntry(e))
	}

	// Pre-8a encodeFooter: identical to the current one minus the
	// has_bloom byte and everything after it.
	var footer bytes.Buffer
	writeUint32(&footer, uint32(len(index)))
	for _, e := range index {
		writeUint32(&footer, uint32(len(e.key)))
		footer.Write(e.key)
		writeUint64(&footer, uint64(e.offset))
	}
	writeUint32(&footer, uint32(len(minKey)))
	footer.Write(minKey)
	writeUint32(&footer, uint32(len(maxKey)))
	footer.Write(maxKey)
	writeUint64(&footer, uint64(len(entries)))

	footerOffset := int64(data.Len())
	footerBody := footer.Bytes()
	data.Write(footerBody)

	trailer := make([]byte, trailerLen)
	binary.BigEndian.PutUint32(trailer[0:4], crc32.ChecksumIEEE(footerBody))
	binary.BigEndian.PutUint64(trailer[4:12], uint64(footerOffset))
	data.Write(trailer)

	if err := os.WriteFile(path, data.Bytes(), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
}

// TestOpenPreBloomSSTableIsBackwardCompatible is the required Phase 8a
// backward-compatibility test: a file written before bloom filters
// existed must open cleanly and answer every Get correctly, never crash,
// and never misparse.
func TestOpenPreBloomSSTableIsBackwardCompatible(t *testing.T) {
	const n = 300
	entries := make([]wal.Entry, n)
	for i := 0; i < n; i++ {
		entries[i] = wal.Entry{Op: wal.OpPut, Key: keyFor(i), Value: valueFor(i)}
	}

	path := filepath.Join(t.TempDir(), "000001.sst")
	buildPreBloomSSTable(t, path, entries)

	st, err := OpenSSTable(path)
	if err != nil {
		t.Fatalf("OpenSSTable(pre-bloom file): %v", err)
	}
	defer st.Close()

	if st.HasBloomFilter() {
		t.Fatalf("HasBloomFilter() = true for a pre-8a file, want false")
	}

	for i := 0; i < n; i++ {
		v, found, tombstone, err := st.Get(keyFor(i))
		if err != nil || !found || tombstone || !bytes.Equal(v, valueFor(i)) {
			t.Fatalf("Get(%s) = %q found=%v tombstone=%v err=%v, want %q true false nil",
				keyFor(i), v, found, tombstone, err, valueFor(i))
		}
	}
	_, found, _, err := st.Get([]byte("does-not-exist"))
	if err != nil || found {
		t.Fatalf("Get(does-not-exist) on pre-bloom file = found=%v err=%v, want false nil", found, err)
	}
}
