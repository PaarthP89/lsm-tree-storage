// Command chaos is the Phase 5 chaos-test harness. Each iteration:
// launches cmd/chaosworker as a real subprocess, lets it run a burst of
// writes, SIGKILLs it at a randomized acked-write count, restarts the
// engine in-process against the same data directory, and verifies that
// every write the worker actually reported as acked survived with its
// exact value. See docs/chaos-report.md for a written summary of a real
// run, and CLAUDE.md §12 for why this uses a real subprocess + real
// SIGKILL rather than an in-process crash simulation.
package main

import (
	"bufio"
	"bytes"
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	lsm "github.com/paarthsiphone/lsm-tree-storage"
	"github.com/paarthsiphone/lsm-tree-storage/internal/chaosdata"
)

func main() {
	iterations := flag.Int("iterations", 20, "number of chaos iterations to run")
	maxWrites := flag.Int("maxwrites", 3000, "upper bound on writes per iteration")
	flushThreshold := flag.Int("flushthreshold", 4096, "worker memtable flush threshold in bytes, kept small so a burst spans several flushes")
	keep := flag.Bool("keep", false, "keep each iteration's data directory instead of deleting it")
	flag.Parse()

	if *maxWrites <= 100 {
		fmt.Fprintln(os.Stderr, "chaos: -maxwrites must be > 100")
		os.Exit(1)
	}

	workerBin, cleanup, err := buildWorker()
	if err != nil {
		fmt.Fprintln(os.Stderr, "chaos: building chaosworker:", err)
		os.Exit(1)
	}
	defer cleanup()

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	var totalAcked, totalLost, totalCorrupt, failedIterations int
	for iter := 1; iter <= *iterations; iter++ {
		res, err := runIteration(workerBin, iter, *maxWrites, *flushThreshold, rng, *keep)
		if err != nil {
			fmt.Printf("iteration %d: FAILED to run: %v\n", iter, err)
			failedIterations++
			continue
		}

		totalAcked += res.ackedCount
		totalLost += res.lost
		totalCorrupt += res.corrupt

		status := "PASS"
		if res.lost > 0 || res.corrupt > 0 {
			status = "FAIL"
			failedIterations++
		}
		fmt.Printf("iteration %d: killed after %d acked writes, verified %d/%d correct, %d lost, %d corrupt [%s]\n",
			iter, res.ackedCount, res.verified, res.ackedCount, res.lost, res.corrupt, status)
	}

	if failedIterations == 0 {
		fmt.Printf("PASS: %d/%d iterations, %d total acked writes verified, 0 lost, 0 corrupt\n",
			*iterations, *iterations, totalAcked)
		return
	}
	fmt.Printf("FAIL: %d/%d iterations failed, %d lost, %d corrupt across %d total acked writes\n",
		failedIterations, *iterations, totalLost, totalCorrupt, totalAcked)
	os.Exit(1)
}

type iterationResult struct {
	ackedCount int // number of writes the worker reported as acked (lastAck+1, 0 if none)
	verified   int
	lost       int // acked but missing entirely on recovery -- data loss
	corrupt    int // acked but recovered with the wrong value -- corruption
}

// runIteration runs one launch-write-kill-restart-verify cycle against a
// fresh temp data directory. The kill point is chosen as a randomized
// target acked-write count rather than a randomized sleep duration, so
// the reported "killed after N acked writes" is exact, not an estimate --
// but the *actual* ground truth used for verification is always the last
// ACK line really read from the worker's stdout, since a few extra
// writes may land (and print) in the gap between the target being hit and
// the SIGKILL actually stopping the process.
func runIteration(workerBin string, iter, maxWrites, flushThreshold int, rng *rand.Rand, keep bool) (iterationResult, error) {
	dir, err := os.MkdirTemp("", fmt.Sprintf("lsmchaos-iter%d-", iter))
	if err != nil {
		return iterationResult{}, err
	}
	if !keep {
		defer os.RemoveAll(dir)
	}

	cmd := exec.Command(workerBin,
		"-dir", dir,
		"-count", strconv.Itoa(maxWrites),
		"-flushthreshold", strconv.Itoa(flushThreshold),
	)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return iterationResult{}, err
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return iterationResult{}, err
	}

	// Never within 50 of either end: leaves room for the worker to still
	// be mid-burst when the target is hit (not already finished) and for
	// the "beyond last ack" verification range in the caller to be
	// non-trivial.
	target := int64(50 + rng.Intn(maxWrites-100))

	var lastAck int64 = -1
	targetReached := make(chan struct{}, 1)
	doneReading := make(chan struct{})
	go func() {
		defer close(doneReading)
		sc := bufio.NewScanner(stdout)
		for sc.Scan() {
			line := sc.Text()
			numStr, ok := strings.CutPrefix(line, "ACK ")
			if !ok {
				continue
			}
			n, err := strconv.ParseInt(numStr, 10, 64)
			if err != nil {
				continue
			}
			atomic.StoreInt64(&lastAck, n)
			if n >= target {
				select {
				case targetReached <- struct{}{}:
				default:
				}
			}
		}
	}()

	select {
	case <-targetReached:
		// A benign race is possible here: the reader goroutine can be
		// descheduled long enough that the worker finishes its entire
		// burst and exits before we get to Kill() -- e.g. under a GC
		// pause, or if the scanner catches up on a large backlog of
		// already-buffered lines in one burst. cmd.Wait() hasn't run yet
		// at this point, so the process (if already dead) is still an
		// unreaped zombie and Kill() ordinarily just no-ops; but treat
		// "already finished" as informational, not an iteration failure
		// -- it says nothing about DB correctness, only about scheduler
		// timing, and misreporting it as a run failure would misrepresent
		// what this harness actually proves.
		if err := cmd.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
			return iterationResult{}, fmt.Errorf("Kill: %w", err)
		}
	case <-doneReading:
		// Worker exited (finished, or hit an error of its own) before
		// reaching the target -- nothing to kill.
	}
	// cmd.Wait must not run until all pipe reads have completed (see
	// os/exec's StdoutPipe doc), so wait for the reader goroutine even
	// though it may already be done from the doneReading branch above.
	<-doneReading
	_ = cmd.Wait()

	finalAck := atomic.LoadInt64(&lastAck)

	db, err := lsm.Open(dir)
	if err != nil {
		return iterationResult{}, fmt.Errorf("Open after kill: %w", err)
	}
	defer db.Close()

	res := iterationResult{ackedCount: 0}
	if finalAck >= 0 {
		res.ackedCount = int(finalAck) + 1
	}

	// Scan the entire possible key range, not just [0, finalAck]: the
	// brief requires that a key *beyond* the last ACK still be checked --
	// absence is fine (it may never have been written, or the write may
	// have landed right at the kill boundary without its ACK making it
	// out), but if it's present at all its value must be exactly correct.
	// A wrong value anywhere, acked or not, is corruption and must not go
	// undetected just because it sits past the ACK boundary.
	for i := 0; i < maxWrites; i++ {
		key := chaosdata.Key(i)
		want := chaosdata.Value(i)
		got, found, err := db.Get(key)
		if err != nil {
			return res, fmt.Errorf("Get(%s): %w", key, err)
		}

		acked := int64(i) <= finalAck
		switch {
		case !found:
			if acked {
				res.lost++
			}
		case !bytes.Equal(got, want):
			res.corrupt++
		case acked:
			res.verified++
		}
	}

	return res, nil
}

// buildWorker compiles cmd/chaosworker once into a temp binary, reused
// across every iteration -- mirrors the pattern in killrestart_test.go's
// TestMain. Must be run with the module root as the working directory.
func buildWorker() (bin string, cleanup func(), err error) {
	tmpDir, err := os.MkdirTemp("", "chaosworker-bin")
	if err != nil {
		return "", nil, err
	}

	bin = filepath.Join(tmpDir, "chaosworker")
	build := exec.Command("go", "build", "-o", bin, "./cmd/chaosworker")
	build.Stdout = os.Stderr
	build.Stderr = os.Stderr
	if err := build.Run(); err != nil {
		os.RemoveAll(tmpDir)
		return "", nil, err
	}

	return bin, func() { os.RemoveAll(tmpDir) }, nil
}
