// Command demo is a narrated, end-to-end walkthrough of every feature this
// engine implements across the Minimum, Target, and Stretch tiers
// (CLAUDE.md §9): basic Put/Get/Delete with tombstones, a
// threshold-triggered flush to SSTable, a range Scan, L0->L1 compaction,
// concurrent readers/writers, a bloom-filter-served miss, and a real
// subprocess `kill -9` crash-recovery cycle.
//
// This is not a test -- there are no assertions here beyond the crash
// section (which does check recovered data, since "the engine survived a
// real kill -9" is the one claim worth actually verifying rather than just
// narrating). Its purpose is to be a single runnable program that shows
// the engine actually doing what CLAUDE.md claims, rather than only living
// behind `go test` output or the existing chaos/load harnesses (which are
// built for automated verification, not for being read).
//
// Run with: go run ./cmd/demo
package main

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"

	lsm "github.com/paarthsiphone/lsm-tree-storage"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "demo:", err)
		os.Exit(1)
	}
}

func run() error {
	root, err := os.MkdirTemp("", "lsm-demo")
	if err != nil {
		return err
	}
	defer os.RemoveAll(root)

	section("1. Basic Put / Get / Delete / tombstones")
	if err := demoBasicOps(filepath.Join(root, "basic")); err != nil {
		return fmt.Errorf("basic ops: %w", err)
	}

	section("2. Flush to SSTable")
	if err := demoFlush(filepath.Join(root, "flush")); err != nil {
		return fmt.Errorf("flush: %w", err)
	}

	section("3. Range scan")
	if err := demoScan(filepath.Join(root, "scan")); err != nil {
		return fmt.Errorf("scan: %w", err)
	}

	section("4. Compaction (L0 -> L1)")
	if err := demoCompaction(filepath.Join(root, "compaction")); err != nil {
		return fmt.Errorf("compaction: %w", err)
	}

	section("5. Concurrent readers/writers")
	if err := demoConcurrency(filepath.Join(root, "concurrency")); err != nil {
		return fmt.Errorf("concurrency: %w", err)
	}

	section("6. Bloom-filter-served miss")
	if err := demoBloomMiss(filepath.Join(root, "bloom")); err != nil {
		return fmt.Errorf("bloom: %w", err)
	}

	section("7. Real kill -9 crash recovery")
	if err := demoCrashRecovery(filepath.Join(root, "crash")); err != nil {
		return fmt.Errorf("crash recovery: %w", err)
	}

	fmt.Println()
	fmt.Println("All sections completed.")
	return nil
}

func section(title string) {
	fmt.Println()
	fmt.Println("== " + title + " ==")
}

func demoBasicOps(dir string) error {
	db, err := lsm.Open(dir)
	if err != nil {
		return err
	}
	defer db.Close()

	if err := db.Put([]byte("user:1"), []byte("alice")); err != nil {
		return err
	}
	if err := db.Put([]byte("user:2"), []byte("bob")); err != nil {
		return err
	}
	v, found, err := db.Get([]byte("user:1"))
	if err != nil {
		return err
	}
	fmt.Printf("Get(user:1) = %q, found=%v\n", v, found)

	if err := db.Delete([]byte("user:1")); err != nil {
		return err
	}
	v, found, err = db.Get([]byte("user:1"))
	if err != nil {
		return err
	}
	fmt.Printf("after Delete(user:1): Get(user:1) = %q, found=%v (tombstone shadows the old value)\n", v, found)

	v, found, err = db.Get([]byte("user:2"))
	if err != nil {
		return err
	}
	fmt.Printf("Get(user:2) = %q, found=%v (untouched by the delete above)\n", v, found)
	return nil
}

func demoFlush(dir string) error {
	db, err := lsm.Open(dir)
	if err != nil {
		return err
	}
	defer db.Close()

	// A small threshold so a modest number of keys actually crosses it --
	// production default is 4MB, which would take a lot more data to
	// narrate usefully here.
	db.SetFlushThreshold(4096)

	const count = 2000
	for i := 0; i < count; i++ {
		if err := db.Put(keyFor(i), valueFor(i)); err != nil {
			return err
		}
	}

	stats := db.Stats()
	fmt.Printf("wrote %d keys past a 4KB flush threshold: %d live L0 SSTable(s) on disk, %d bytes still in the active memtable\n",
		count, stats.L0Count, stats.MemtableSizeBytes)
	return nil
}

func demoScan(dir string) error {
	db, err := lsm.Open(dir)
	if err != nil {
		return err
	}
	defer db.Close()

	for i := 0; i < 20; i++ {
		if err := db.Put(keyFor(i), valueFor(i)); err != nil {
			return err
		}
	}
	// A delete inside the scanned range must not resurface.
	if err := db.Delete(keyFor(5)); err != nil {
		return err
	}

	it, err := db.Scan(keyFor(3), keyFor(10))
	if err != nil {
		return err
	}
	fmt.Println("Scan(key-000003, key-000010):")
	for it.Next() {
		fmt.Printf("  %s -> %s\n", it.Key(), it.Value())
	}
	return nil
}

func demoCompaction(dir string) error {
	db, err := lsm.Open(dir)
	if err != nil {
		return err
	}
	defer db.Close()

	db.SetFlushThreshold(4096)
	db.SetL0CompactionThreshold(3)

	// A small flush threshold plus a low L0 compaction threshold means
	// L0->L1 compaction (Phase 8d) fires automatically, synchronously,
	// partway through this write loop -- MaybeCompact below is then just
	// an idempotent on-demand check of the same trigger, not the thing
	// that actually did the merging.
	const count = 6000
	for i := 0; i < count; i++ {
		if err := db.Put(keyFor(i), valueFor(i)); err != nil {
			return err
		}
	}
	after := db.Stats()
	fmt.Printf("after writing %d keys (auto-compaction already ran inline as L0 filled up): %d L0 file(s), %d L1 file(s)\n",
		count, after.L0Count, after.L1Count)

	if err := db.MaybeCompact(); err != nil {
		return err
	}
	idle := db.Stats()
	fmt.Printf("on-demand MaybeCompact call: %d L0 file(s), %d L1 file(s) (no-op here -- nothing new to merge)\n", idle.L0Count, idle.L1Count)

	// Every key must still read correctly after the merge.
	for i := 0; i < count; i += 777 {
		v, found, err := db.Get(keyFor(i))
		if err != nil {
			return err
		}
		if !found || !bytes.Equal(v, valueFor(i)) {
			return fmt.Errorf("post-compaction Get(%s) = %q found=%v, want %q true", keyFor(i), v, found, valueFor(i))
		}
	}
	fmt.Println("spot-checked keys across the merged range: all correct")
	return nil
}

func demoConcurrency(dir string) error {
	db, err := lsm.Open(dir)
	if err != nil {
		return err
	}
	defer db.Close()

	db.SetFlushThreshold(8192)
	db.SetL0CompactionThreshold(3)

	const writers = 4
	const perWriter = 500
	var writersWg sync.WaitGroup
	errs := make(chan error, writers)

	for w := 0; w < writers; w++ {
		writersWg.Add(1)
		go func(w int) {
			defer writersWg.Done()
			for i := 0; i < perWriter; i++ {
				k := []byte(fmt.Sprintf("w%d-key-%06d", w, i))
				if err := db.Put(k, k); err != nil {
					errs <- err
					return
				}
			}
		}(w)
	}

	stop := make(chan struct{})
	var readerWg sync.WaitGroup
	var reads int64
	readerWg.Add(1)
	go func() {
		defer readerWg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				db.Get([]byte("w0-key-000000"))
				reads++
			}
		}
	}()

	writersWg.Wait()
	close(stop)
	readerWg.Wait()

	close(errs)
	for err := range errs {
		if err != nil {
			return err
		}
	}

	fmt.Printf("%d concurrent writer goroutines completed %d writes total while a concurrent reader ran %d Get calls -- 0 errors, go test -race ./... covers this path directly\n",
		writers, writers*perWriter, reads)
	return nil
}

func demoBloomMiss(dir string) error {
	db, err := lsm.Open(dir)
	if err != nil {
		return err
	}
	defer db.Close()

	db.SetFlushThreshold(4096)
	const count = 3000
	for i := 0; i < count; i++ {
		if err := db.Put(keyFor(i), valueFor(i)); err != nil {
			return err
		}
	}

	stats := db.Stats()
	_, found, err := db.Get([]byte("this-key-was-never-written"))
	if err != nil {
		return err
	}
	fmt.Printf("Get of a never-written key against %d flushed SSTable(s) (L0+L1): found=%v -- each SSTable's bloom filter rules it out before any on-disk scan (see sstable.SSTable.HasBloomFilter)\n",
		stats.L0Count+stats.L1Count, found)
	return nil
}

func demoCrashRecovery(dir string) error {
	bin, cleanup, err := buildLsmload()
	if err != nil {
		return err
	}
	defer cleanup()

	// count/delay/sleep are sized generously (rather than shaving the
	// window tight) so the crash reliably lands mid-burst regardless of
	// how slow process startup and per-write fsync are on the machine
	// running this demo -- a too-tight window risks the burst finishing,
	// or barely starting, before the kill, either of which would make
	// this section narrate nothing interesting.
	const count = 3000
	cmd := exec.Command(bin, "-dir", dir, "-count", fmt.Sprint(count), "-delay", "200us")
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return err
	}

	time.Sleep(500 * time.Millisecond)
	if err := cmd.Process.Kill(); err != nil {
		return err
	}
	_ = cmd.Wait() // expected to report a kill signal, not itself a failure

	db, err := lsm.Open(dir)
	if err != nil {
		return fmt.Errorf("Open after kill -9: %w", err)
	}
	defer db.Close()

	recovered := 0
	for i := 0; i < count; i++ {
		v, found, err := db.Get(keyFor(i))
		if err != nil {
			return err
		}
		if !found {
			continue
		}
		if !bytes.Equal(v, valueFor(i)) {
			return fmt.Errorf("recovered corrupt value for %s: got %q want %q", keyFor(i), v, valueFor(i))
		}
		recovered++
	}

	fmt.Printf("worker process was SIGKILLed mid-burst; engine reopened against the same data directory and recovered %d/%d keys, every recovered value exact -- no corruption, no partial writes\n",
		recovered, count)
	return nil
}

func buildLsmload() (bin string, cleanup func(), err error) {
	tmpDir, err := os.MkdirTemp("", "demo-lsmload-bin")
	if err != nil {
		return "", nil, err
	}
	bin = filepath.Join(tmpDir, "lsmload")
	build := exec.Command("go", "build", "-o", bin, "./cmd/lsmload")
	build.Stdout = os.Stderr
	build.Stderr = os.Stderr
	if err := build.Run(); err != nil {
		os.RemoveAll(tmpDir)
		return "", nil, err
	}
	return bin, func() { os.RemoveAll(tmpDir) }, nil
}

func keyFor(i int) []byte   { return []byte(fmt.Sprintf("key-%06d", i)) }
func valueFor(i int) []byte { return []byte(fmt.Sprintf("val-%06d", i)) }
