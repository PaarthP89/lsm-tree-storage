package lsm

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/paarthsiphone/lsm-tree-storage/internal/chaosdata"
)

// TestRealSIGKILLMidL0ToL1CompactionRecoversCorrectly is Phase 8d's
// real-subprocess crash test (CLAUDE.md §12 prefers a real subprocess
// SIGKILL over in-process crash simulation wherever a phase brief calls
// for it). It reuses cmd/chaosworker's ACK-line protocol (the sole ground
// truth for "durably acknowledged" -- see cmd/chaosworker's doc comment)
// exactly as cmd/chaos does, but with a deliberately tiny
// -compactionthreshold (which SetCompactionThreshold aliases straight
// onto Phase 8d's SetL0CompactionThreshold -- see db.go) so L0->L1
// compaction fires very frequently and a randomized kill point lands
// inside an in-progress L0->L1 merge with high probability across enough
// iterations, not just Phase 6's four hand-injected unit-test windows.
//
// Each iteration verifies two things after recovery: (1) no acked write
// is lost or corrupted by a crash landing mid-L0->L1 merge, and (2) the
// L1 non-overlap invariant still holds, proving that whatever
// stale-but-redundant files a crash left behind (e.g. an orphaned L1
// output whose MANIFEST edit never landed) don't corrupt the read path.
// chaosworker's workload is pure sequential Puts (see internal/chaosdata),
// so this specific test doesn't exercise the tombstone-drop resurrection
// risk itself -- that's covered separately by the adversarial
// compaction.CompactLeveled tests and
// TestL0ToL1CompactionCorrectAcrossMultipleGenerationsAndLevels's
// multi-generation overwrite/delete coverage. This test's own job is
// proving those crash-safety properties hold under a real OS-level
// SIGKILL, not just simulated crash points.
func TestRealSIGKILLMidL0ToL1CompactionRecoversCorrectly(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping real-subprocess SIGKILL crash test in -short mode")
	}

	const iterations = 12
	const maxWrites = 4000
	const flushThreshold = 1024 // small: many flushes per iteration
	const l0Threshold = 2       // small: frequent L0->L1 compaction

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	var totalAcked, totalLost, totalCorrupt, failedIterations int
	for iter := 1; iter <= iterations; iter++ {
		res, err := runL0L1CrashIteration(t, iter, maxWrites, flushThreshold, l0Threshold, rng)
		if err != nil {
			t.Errorf("iteration %d: FAILED to run: %v", iter, err)
			failedIterations++
			continue
		}
		totalAcked += res.ackedCount
		totalLost += res.lost
		totalCorrupt += res.corrupt
		if res.lost > 0 || res.corrupt > 0 {
			failedIterations++
		}
		t.Logf("iteration %d: killed after %d acked writes, verified %d/%d correct, %d lost, %d corrupt",
			iter, res.ackedCount, res.verified, res.ackedCount, res.lost, res.corrupt)
	}

	if failedIterations > 0 {
		t.Fatalf("FAIL: %d/%d iterations failed, %d lost, %d corrupt across %d total acked writes",
			failedIterations, iterations, totalLost, totalCorrupt, totalAcked)
	}
	t.Logf("PASS: %d/%d iterations, %d total acked writes verified, 0 lost, 0 corrupt", iterations, iterations, totalAcked)
}

type l0l1CrashResult struct {
	ackedCount int
	verified   int
	lost       int
	corrupt    int
}

// runL0L1CrashIteration mirrors cmd/chaos's runIteration (launch worker,
// SIGKILL at a randomized acked-write count, restart in-process, verify),
// with two additions specific to this phase: it also asserts the L1
// non-overlap invariant holds after recovery, and it checks the *entire*
// key range (not just up to the last ack) for resurrected stale values,
// not only loss/corruption.
func runL0L1CrashIteration(t *testing.T, iter, maxWrites, flushThreshold, l0Threshold int, rng *rand.Rand) (l0l1CrashResult, error) {
	t.Helper()
	dir, err := os.MkdirTemp("", fmt.Sprintf("lsml0l1crash-iter%d-", iter))
	if err != nil {
		return l0l1CrashResult{}, err
	}
	defer os.RemoveAll(dir)

	cmd := exec.Command(chaosworkerBin,
		"-dir", dir,
		"-count", strconv.Itoa(maxWrites),
		"-flushthreshold", strconv.Itoa(flushThreshold),
		"-compactionthreshold", strconv.Itoa(l0Threshold),
	)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return l0l1CrashResult{}, err
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return l0l1CrashResult{}, err
	}

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
		if err := cmd.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
			return l0l1CrashResult{}, fmt.Errorf("Kill: %w", err)
		}
	case <-doneReading:
		// Worker finished before reaching the target -- nothing to kill.
	}
	<-doneReading
	_ = cmd.Wait()

	finalAck := atomic.LoadInt64(&lastAck)

	db, err := Open(dir)
	if err != nil {
		return l0l1CrashResult{}, fmt.Errorf("Open after kill: %w", err)
	}
	defer db.Close()

	assertL1NonOverlapping(t, db)

	res := l0l1CrashResult{}
	if finalAck >= 0 {
		res.ackedCount = int(finalAck) + 1
	}

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
