package lsm

import (
	"fmt"
	"math/rand"
	"testing"
)

// Phase 7 benchmarks (CLAUDE.md §11 build plan). All four write real data
// through the real write/read paths -- no mocked components -- against
// datasets large enough that flush and compaction actually occur during
// setup, not just in principle.

func benchKey(i int) []byte   { return []byte(fmt.Sprintf("bench-key-%09d", i)) }
func benchValue(i int) []byte { return []byte(fmt.Sprintf("bench-value-%09d-payload", i)) }

func reportOpsPerSec(b *testing.B, n int, elapsedSeconds float64) {
	b.ReportMetric(float64(n)/elapsedSeconds, "ops/sec")
}

// BenchmarkWrite measures sustained Put throughput (WAL append + fsync +
// memtable insert + periodic flush/compaction) at the production default
// thresholds.
func BenchmarkWrite(b *testing.B) {
	dir := b.TempDir()
	db, err := Open(dir)
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	defer db.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := db.Put(benchKey(i), benchValue(i)); err != nil {
			b.Fatalf("Put(%d): %v", i, err)
		}
	}
	b.StopTimer()
	reportOpsPerSec(b, b.N, b.Elapsed().Seconds())
}

// BenchmarkReadHot measures Get latency for keys that never left the
// memtable: the flush threshold is set high enough that the entire
// pre-populated dataset stays resident, so every read is a pure skip-list
// lookup with no disk I/O at all.
func BenchmarkReadHot(b *testing.B) {
	dir := b.TempDir()
	db, err := Open(dir)
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	defer db.Close()
	db.SetFlushThreshold(1 << 30) // effectively unbounded: nothing flushes

	const n = 5_000
	for i := 0; i < n; i++ {
		if err := db.Put(benchKey(i), benchValue(i)); err != nil {
			b.Fatalf("Put(%d): %v", i, err)
		}
	}
	if len(db.liveSSTables()) != 0 {
		b.Fatalf("setup flushed %d SSTables, want 0 (hot reads must hit only the memtable)", len(db.liveSSTables()))
	}

	rng := rand.New(rand.NewSource(1))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		k := benchKey(rng.Intn(n))
		if _, found, err := db.Get(k); err != nil || !found {
			b.Fatalf("Get(%s): found=%v err=%v", k, found, err)
		}
	}
	b.StopTimer()
	reportOpsPerSec(b, b.N, b.Elapsed().Seconds())
}

// BenchmarkReadCold measures Get latency against keys that live only on
// disk, spread across many *uncompacted* SSTables (compaction threshold set
// far out of reach) -- the worst case for the read path, since Get walks
// every live L0 file newest-to-oldest and an early-written key can require
// consulting many tables (each its own sparse-index binary search + bounded
// scan) before it's found.
func BenchmarkReadCold(b *testing.B) {
	dir := b.TempDir()
	db, err := Open(dir)
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	defer db.Close()
	db.SetFlushThreshold(200 * 1024)   // force many flushes -> many SSTables
	db.SetCompactionThreshold(1 << 30) // never compact -> reads must consult the whole uncompacted set

	const n = 20_000
	const margin = 3_000 // excluded from the read population: may still be sitting in the memtable's un-flushed tail
	for i := 0; i < n; i++ {
		if err := db.Put(benchKey(i), benchValue(i)); err != nil {
			b.Fatalf("Put(%d): %v", i, err)
		}
	}
	if len(db.liveSSTables()) < 2 {
		b.Fatalf("setup produced %d SSTables, want several (cold reads need a real uncompacted set)", len(db.liveSSTables()))
	}
	b.Logf("ReadCold setup: %d SSTables, uncompacted", len(db.liveSSTables()))

	rng := rand.New(rand.NewSource(2))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		k := benchKey(rng.Intn(n - margin))
		if _, found, err := db.Get(k); err != nil || !found {
			b.Fatalf("Get(%s): found=%v err=%v", k, found, err)
		}
	}
	b.StopTimer()
	reportOpsPerSec(b, b.N, b.Elapsed().Seconds())
}

// BenchmarkReadAfterCompaction measures Get latency against the same size
// and shape of dataset as BenchmarkReadCold, but with compaction left at
// its production default threshold, so the SSTable set is repeatedly
// collapsed during setup instead of growing unbounded. This isolates
// exactly what compaction buys a read: fewer tables to consult, nothing
// else about the workload changes.
func BenchmarkReadAfterCompaction(b *testing.B) {
	dir := b.TempDir()
	db, err := Open(dir)
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	defer db.Close()
	db.SetFlushThreshold(200 * 1024) // identical flush cadence to BenchmarkReadCold
	// Compaction threshold intentionally left at its default -- the point
	// of this benchmark is the production-configured behavior.

	const n = 20_000
	const margin = 3_000
	for i := 0; i < n; i++ {
		if err := db.Put(benchKey(i), benchValue(i)); err != nil {
			b.Fatalf("Put(%d): %v", i, err)
		}
	}
	if len(db.liveSSTables()) == 0 {
		b.Fatalf("setup produced no SSTables")
	}
	b.Logf("ReadAfterCompaction setup: %d SSTables, compacted", len(db.liveSSTables()))

	rng := rand.New(rand.NewSource(3))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		k := benchKey(rng.Intn(n - margin))
		if _, found, err := db.Get(k); err != nil || !found {
			b.Fatalf("Get(%s): found=%v err=%v", k, found, err)
		}
	}
	b.StopTimer()
	reportOpsPerSec(b, b.N, b.Elapsed().Seconds())
}
