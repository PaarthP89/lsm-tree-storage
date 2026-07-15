package lsm

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

// lsmloadBin is the path to the built cmd/lsmload helper binary, shared
// across tests in this package. Built once in TestMain.
var lsmloadBin string

func TestMain(m *testing.M) {
	tmpDir, err := os.MkdirTemp("", "lsmload-bin")
	if err != nil {
		fmt.Fprintln(os.Stderr, "TestMain: MkdirTemp:", err)
		os.Exit(1)
	}
	defer os.RemoveAll(tmpDir)

	lsmloadBin = filepath.Join(tmpDir, "lsmload")
	build := exec.Command("go", "build", "-o", lsmloadBin, "./cmd/lsmload")
	build.Stdout = os.Stderr
	build.Stderr = os.Stderr
	if err := build.Run(); err != nil {
		fmt.Fprintln(os.Stderr, "TestMain: building lsmload helper:", err)
		os.Exit(1)
	}

	os.Exit(m.Run())
}

func keyFor(i int) []byte   { return []byte(fmt.Sprintf("key-%06d", i)) }
func valueFor(i int) []byte { return []byte(fmt.Sprintf("val-%06d", i)) }

// TestKillRestartSubprocessCleanRun is the control case: no kill, and
// every key sent must be recoverable with its exact value.
func TestKillRestartSubprocessCleanRun(t *testing.T) {
	dir := t.TempDir()
	const count = 1000

	cmd := exec.Command(lsmloadBin, "-dir", dir, "-count", fmt.Sprint(count))
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("clean helper run: %v", err)
	}

	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	for i := 0; i < count; i++ {
		v, found, err := db.Get(keyFor(i))
		if err != nil || !found || !bytes.Equal(v, valueFor(i)) {
			t.Fatalf("Get(%s) = %q found=%v err=%v, want %q true nil", keyFor(i), v, found, err, valueFor(i))
		}
	}
}

// TestKillRestartSubprocessSigkill spawns the helper, SIGKILLs it partway
// through a burst of writes, then restarts the engine in-process against
// the same directory. The recovered key count must never exceed what was
// sent, and every recovered key's value must be exactly what was written
// for it -- no partial or corrupted values. Run repeatedly: timing and
// skip-list level randomness mean a single green run doesn't prove much.
func TestKillRestartSubprocessSigkill(t *testing.T) {
	const iterations = 5
	const count = 2000

	for iter := 0; iter < iterations; iter++ {
		t.Run(fmt.Sprintf("iter%d", iter), func(t *testing.T) {
			dir := t.TempDir()

			cmd := exec.Command(lsmloadBin, "-dir", dir, "-count", fmt.Sprint(count), "-delay", "200us")
			cmd.Stderr = os.Stderr
			if err := cmd.Start(); err != nil {
				t.Fatalf("Start: %v", err)
			}

			time.Sleep(50 * time.Millisecond)

			if err := cmd.Process.Kill(); err != nil {
				t.Fatalf("Kill: %v", err)
			}
			_ = cmd.Wait() // expected to report a kill signal; not itself an error here

			db, err := Open(dir)
			if err != nil {
				t.Fatalf("Open after kill: %v", err)
			}

			recovered := 0
			for i := 0; i < count; i++ {
				v, found, err := db.Get(keyFor(i))
				if err != nil {
					t.Fatalf("Get(%s): %v", keyFor(i), err)
				}
				if !found {
					continue
				}
				recovered++
				if !bytes.Equal(v, valueFor(i)) {
					t.Fatalf("Get(%s) = %q, want %q -- corrupt/partial value survived recovery", keyFor(i), v, valueFor(i))
				}
			}
			db.Close()

			if recovered > count {
				t.Fatalf("recovered %d keys, want <= %d", recovered, count)
			}
			t.Logf("recovered %d/%d keys after SIGKILL", recovered, count)
		})
	}
}
