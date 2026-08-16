package sstable

import (
	"math/rand"
	"path/filepath"
	"testing"

	"github.com/paarthsiphone/lsm-tree-storage/memtable"
)

// setupMissHeavyTable builds one on-disk SSTable containing only the
// even-numbered keys in [0, 2n), then opens it twice: st has its real
// bloom filter, stNoBloom is the same file with the bloom field forced to
// nil. Everything else -- file layout, sparse index, disk contents -- is
// identical, so any throughput difference between the two benchmarks
// below is attributable to the filter's short-circuit alone.
//
// Miss keys (see missKey) are the odd-numbered keys in the same [0, 2n)
// range and same "key-%06d" format as the present keys, so they fall
// inside the table's [minKey, maxKey] bounds and actually reach the
// bloom-filter/binary-search path instead of being rejected by the
// existing out-of-range fast path in Get -- an earlier version of this
// benchmark used an out-of-range prefix ("absent-key-") that sorted
// before every present key, so every miss was already rejected before
// the filter was ever consulted, making the comparison meaningless.
func setupMissHeavyTable(b *testing.B, n int) (st, stNoBloom *SSTable) {
	b.Helper()
	mem := memtable.New()
	for i := 0; i < n; i++ {
		mem.Put(keyFor(2*i), valueFor(2*i))
	}
	path := filepath.Join(b.TempDir(), "000001.sst")
	if _, err := FlushMemtable(mem, path); err != nil {
		b.Fatalf("FlushMemtable: %v", err)
	}

	st, err := OpenSSTable(path)
	if err != nil {
		b.Fatalf("OpenSSTable: %v", err)
	}
	stNoBloom, err = OpenSSTable(path)
	if err != nil {
		b.Fatalf("OpenSSTable: %v", err)
	}
	stNoBloom.bloom = nil

	if !st.HasBloomFilter() {
		b.Fatalf("setup: expected a bloom filter on the primary handle")
	}
	return st, stNoBloom
}

func missKey(n, i int) []byte { return keyFor(2*(i%n) + 1) }

// BenchmarkGetMissWithBloomFilter measures Get latency for a 100% miss
// workload with the bloom filter active -- every lookup should
// short-circuit before touching the sparse index or the disk.
func BenchmarkGetMissWithBloomFilter(b *testing.B) {
	const n = 20_000
	st, _ := setupMissHeavyTable(b, n)
	defer st.Close()

	rng := rand.New(rand.NewSource(1))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		k := missKey(n, rng.Int())
		if _, found, _, err := st.Get(k); err != nil || found {
			b.Fatalf("Get(%s): found=%v err=%v", k, found, err)
		}
	}
}

// BenchmarkGetMissWithoutBloomFilter measures the same 100% miss workload
// against the identical on-disk file, but with the filter disabled (nil),
// forcing every lookup through the full sparse-index binary search +
// bounded disk scan. The delta between this and
// BenchmarkGetMissWithBloomFilter is the filter's real, measured payoff.
func BenchmarkGetMissWithoutBloomFilter(b *testing.B) {
	const n = 20_000
	_, stNoBloom := setupMissHeavyTable(b, n)
	defer stNoBloom.Close()

	rng := rand.New(rand.NewSource(1))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		k := missKey(n, rng.Int())
		if _, found, _, err := stNoBloom.Get(k); err != nil || found {
			b.Fatalf("Get(%s): found=%v err=%v", k, found, err)
		}
	}
}
