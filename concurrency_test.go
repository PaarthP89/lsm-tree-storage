package lsm

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/paarthsiphone/lsm-tree-storage/manifest"
)

func concurrentKey(worker, i int) []byte {
	return []byte(fmt.Sprintf("w%04d-key-%06d", worker, i))
}
func concurrentValue(worker, i int) []byte {
	return []byte(fmt.Sprintf("w%04d-val-%06d", worker, i))
}

// TestConcurrentPutGetScanNoDataRace runs many writer goroutines (each
// owning a disjoint key range, so there's no cross-writer overwrite to
// reason about) alongside many reader goroutines doing Get and Scan
// continuously throughout, under a small flush threshold and small
// L0/L1 thresholds so real flush/compaction churn happens throughout the
// run, not just at the end. Run with -race (mandatory for this phase --
// CLAUDE.md §12/8c's Definition of Done): the primary thing this test
// proves is the *absence* of a data race the detector would catch, on
// top of every write being correctly readable afterward.
func TestConcurrentPutGetScanNoDataRace(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()
	db.SetFlushThreshold(2 * 1024)
	db.SetL0CompactionThreshold(3)
	db.SetL1SizeRatio(2)

	const writers = 8
	const perWriter = 300

	var wg sync.WaitGroup
	errCh := make(chan error, writers+16)

	stopReaders := make(chan struct{})

	// Writers: each owns a disjoint key range (by worker index prefix),
	// so no two writers ever touch the same key -- correctness of the
	// final read-back doesn't depend on any cross-writer ordering.
	for wIdx := 0; wIdx < writers; wIdx++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				if err := db.Put(concurrentKey(worker, i), concurrentValue(worker, i)); err != nil {
					errCh <- fmt.Errorf("worker %d Put(%d): %w", worker, i, err)
					return
				}
			}
		}(wIdx)
	}

	// Readers: hammer Get on keys that may or may not exist yet, and Scan
	// across the whole keyspace, purely to create concurrent read
	// pressure against the writers above -- results aren't asserted here
	// (a key not yet written is a legitimate miss), only that no call
	// ever returns an error or panics.
	const readers = 8
	var readerWg sync.WaitGroup
	for r := 0; r < readers; r++ {
		readerWg.Add(1)
		go func(rIdx int) {
			defer readerWg.Done()
			i := 0
			for {
				select {
				case <-stopReaders:
					return
				default:
				}
				worker := rIdx % writers
				if _, _, err := db.Get(concurrentKey(worker, i%perWriter)); err != nil {
					errCh <- fmt.Errorf("reader %d Get: %w", rIdx, err)
					return
				}
				if it, err := db.Scan([]byte("w0000"), []byte("w9999")); err != nil {
					errCh <- fmt.Errorf("reader %d Scan: %w", rIdx, err)
					return
				} else {
					for it.Next() {
						_ = it.Key()
						_ = it.Value()
					}
				}
				i++
			}
		}(r)
	}

	wg.Wait()
	close(stopReaders)
	readerWg.Wait()
	close(errCh)

	for err := range errCh {
		t.Error(err)
	}
	if t.Failed() {
		return
	}

	for worker := 0; worker < writers; worker++ {
		for i := 0; i < perWriter; i++ {
			v, found, err := db.Get(concurrentKey(worker, i))
			if err != nil || !found || !bytes.Equal(v, concurrentValue(worker, i)) {
				t.Fatalf("Get(worker=%d,i=%d) = %q found=%v err=%v, want %q true nil", worker, i, v, found, err, concurrentValue(worker, i))
			}
		}
	}
	assertL1NonOverlapping(t, db)
}

// TestConcurrentPutsCrossingFlushThresholdNeverLoseOrDuplicateWrites drives
// many goroutines simultaneously crossing the flush threshold (a tiny
// threshold, so essentially every Put risks triggering a flush) and
// confirms every single write survives with the correct value -- the
// property that actually matters from "exactly one flush per generation":
// if writeMu's serialization were broken and two goroutines both flushed
// the same generation, or a write raced with a flush's memtable
// iteration, this would show up here as a lost or corrupted key.
func TestConcurrentPutsCrossingFlushThresholdNeverLoseOrDuplicateWrites(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()
	db.SetFlushThreshold(256) // tiny: nearly every Put is a flush candidate
	db.SetL0CompactionThreshold(1 << 30)
	db.SetL1SizeRatio(1 << 30)

	const workers = 16
	const perWorker = 150

	var wg sync.WaitGroup
	errCh := make(chan error, workers)
	for wIdx := 0; wIdx < workers; wIdx++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				if err := db.Put(concurrentKey(worker, i), concurrentValue(worker, i)); err != nil {
					errCh <- fmt.Errorf("worker %d Put(%d): %w", worker, i, err)
					return
				}
			}
		}(wIdx)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}
	if t.Failed() {
		return
	}

	if len(db.l0()) == 0 {
		t.Fatal("no L0 files produced -- test isn't exercising concurrent flushing at all")
	}

	for worker := 0; worker < workers; worker++ {
		for i := 0; i < perWorker; i++ {
			v, found, err := db.Get(concurrentKey(worker, i))
			if err != nil || !found || !bytes.Equal(v, concurrentValue(worker, i)) {
				t.Fatalf("Get(worker=%d,i=%d) = %q found=%v err=%v, want %q true nil -- lost or corrupted write under concurrent flushing", worker, i, v, found, err, concurrentValue(worker, i))
			}
		}
	}
}

// TestConcurrentCompactionMaintainsCorrectnessAndNonOverlap drives many
// concurrent writer goroutines with a small L0CompactionThreshold and
// small L1SizeRatio, so both compaction triggers fire repeatedly and
// concurrently with ongoing writes. Confirms every write is still exactly
// correct afterward and the L1 non-overlap invariant holds -- the
// concurrent equivalent of TestL0ToL1CompactionMaintainsL1NonOverlapInvariant
// and TestL0ToL1CompactionCorrectAcrossMultipleGenerationsAndLevels.
func TestConcurrentCompactionMaintainsCorrectnessAndNonOverlap(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()
	db.SetFlushThreshold(1024)
	db.SetL0CompactionThreshold(2)
	db.SetL1SizeRatio(2)

	const workers = 12
	const perWorker = 250

	var wg sync.WaitGroup
	errCh := make(chan error, workers)
	for wIdx := 0; wIdx < workers; wIdx++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				if err := db.Put(concurrentKey(worker, i), concurrentValue(worker, i)); err != nil {
					errCh <- fmt.Errorf("worker %d Put(%d): %w", worker, i, err)
					return
				}
			}
		}(wIdx)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}
	if t.Failed() {
		return
	}

	edits, err := manifest.ReplayManifest(db.manifestPath)
	if err != nil {
		t.Fatalf("ReplayManifest: %v", err)
	}
	if !manifestHasAnyRemoved(edits) {
		t.Fatal("MANIFEST has no SSTableRemoved edits -- compaction never actually ran under concurrent load")
	}

	for worker := 0; worker < workers; worker++ {
		for i := 0; i < perWorker; i++ {
			v, found, err := db.Get(concurrentKey(worker, i))
			if err != nil || !found || !bytes.Equal(v, concurrentValue(worker, i)) {
				t.Fatalf("Get(worker=%d,i=%d) = %q found=%v err=%v, want %q true nil", worker, i, v, found, err, concurrentValue(worker, i))
			}
		}
	}
	assertL1NonOverlapping(t, db)
}

// TestConcurrentReadsDuringHeavyCompactionNeverErrorOnClosedFile is the
// direct test of DB.inFlightReaders/drainReaders: a dedicated reader
// goroutine hammers Get on a key it knows is live throughout, while
// writer goroutines concurrently drive enough L0->L1 and L1->L1
// compaction to repeatedly retire (close + remove) SSTable files. If
// drainReaders didn't correctly wait for in-flight reads before closing a
// retired file, this reader would eventually observe a read error
// (a closed file, not a crash -- see Scan's doc comment for why this
// class of bug surfaces as an error rather than memory corruption) -- the
// test fails on the very first such error.
func TestConcurrentReadsDuringHeavyCompactionNeverErrorOnClosedFile(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()
	db.SetFlushThreshold(512)
	db.SetL0CompactionThreshold(2)
	db.SetL1SizeRatio(1)

	stableKey, stableVal := []byte("stable-key-under-test"), []byte("stable-value")
	if err := db.Put(stableKey, stableVal); err != nil {
		t.Fatalf("Put(stable): %v", err)
	}

	var readErrs atomic.Int64
	stop := make(chan struct{})
	var readerWg sync.WaitGroup
	readerWg.Add(1)
	go func() {
		defer readerWg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			v, found, err := db.Get(stableKey)
			if err != nil {
				readErrs.Add(1)
				t.Errorf("Get(stableKey) returned an error during concurrent compaction: %v", err)
				return
			}
			if !found || !bytes.Equal(v, stableVal) {
				readErrs.Add(1)
				t.Errorf("Get(stableKey) = %q found=%v, want %q true (stable key must never disappear or change)", v, found, stableVal)
				return
			}
		}
	}()

	const workers = 8
	const perWorker = 400
	var writerWg sync.WaitGroup
	for wIdx := 0; wIdx < workers; wIdx++ {
		writerWg.Add(1)
		go func(worker int) {
			defer writerWg.Done()
			for i := 0; i < perWorker; i++ {
				if err := db.Put(concurrentKey(worker, i), concurrentValue(worker, i)); err != nil {
					t.Errorf("worker %d Put(%d): %v", worker, i, err)
					return
				}
			}
		}(wIdx)
	}
	writerWg.Wait()
	close(stop)
	readerWg.Wait()

	if readErrs.Load() > 0 {
		t.Fatalf("%d read errors/mismatches observed during concurrent compaction", readErrs.Load())
	}
}

// TestReaderSnapshotRemainsValidAcrossConcurrentStateSwap confirms an
// in-flight Get started against one dbState generation completes
// correctly even if a concurrent Put triggers a flush/compaction (and
// therefore a state swap) partway through -- Get loads state exactly
// once at the top of the call and operates only on that snapshot for its
// entire duration (see DB.Get's doc comment), so this should hold
// trivially; this test exercises it under real concurrent load rather
// than just by code inspection.
func TestReaderSnapshotRemainsValidAcrossConcurrentStateSwap(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()
	db.SetFlushThreshold(256)
	db.SetL0CompactionThreshold(2)
	db.SetL1SizeRatio(1)

	const n = 2000
	for i := 0; i < n; i++ {
		if err := db.Put(keyFor(i), valueFor(i)); err != nil {
			t.Fatalf("Put(%d): %v", i, err)
		}
	}

	var wg sync.WaitGroup
	errCh := make(chan error, 32)

	// A burst of concurrent writers to force many state swaps while...
	for wIdx := 0; wIdx < 8; wIdx++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				if err := db.Put(concurrentKey(worker, i), concurrentValue(worker, i)); err != nil {
					errCh <- err
					return
				}
			}
		}(wIdx)
	}
	// ...many concurrent Gets against the pre-existing data run alongside,
	// each one's result checked immediately (no deferred verification).
	for rIdx := 0; rIdx < 8; rIdx++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				key := (seed*97 + i*13) % n
				v, found, err := db.Get(keyFor(key))
				if err != nil {
					errCh <- fmt.Errorf("Get(%d): %w", key, err)
					return
				}
				if !found || !bytes.Equal(v, valueFor(key)) {
					errCh <- fmt.Errorf("Get(%d) = %q found=%v, want %q true", key, v, found, valueFor(key))
					return
				}
			}
		}(rIdx)
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}
}
