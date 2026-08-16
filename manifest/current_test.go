package manifest

import (
	"os"
	"path/filepath"
	"testing"
)

func TestWriteReadCurrentRoundTrip(t *testing.T) {
	dir := t.TempDir()

	if err := WriteCurrent(dir, "MANIFEST-000001"); err != nil {
		t.Fatalf("WriteCurrent: %v", err)
	}

	got, err := ReadCurrent(dir)
	if err != nil {
		t.Fatalf("ReadCurrent: %v", err)
	}
	if got != "MANIFEST-000001" {
		t.Fatalf("ReadCurrent = %q, want MANIFEST-000001", got)
	}
}

func TestWriteCurrentOverwritesAtomically(t *testing.T) {
	dir := t.TempDir()

	if err := WriteCurrent(dir, "MANIFEST-000001"); err != nil {
		t.Fatalf("WriteCurrent: %v", err)
	}
	if err := WriteCurrent(dir, "MANIFEST-000002"); err != nil {
		t.Fatalf("WriteCurrent (overwrite): %v", err)
	}

	got, err := ReadCurrent(dir)
	if err != nil {
		t.Fatalf("ReadCurrent: %v", err)
	}
	if got != "MANIFEST-000002" {
		t.Fatalf("ReadCurrent = %q, want MANIFEST-000002", got)
	}

	// No leftover temp file after a successful rename.
	if _, err := os.Stat(filepath.Join(dir, CurrentFileName+".tmp")); !os.IsNotExist(err) {
		t.Fatalf("CURRENT.tmp still present after WriteCurrent (stat err = %v)", err)
	}
}

func TestReadCurrentMissingFile(t *testing.T) {
	dir := t.TempDir()
	_, err := ReadCurrent(dir)
	if !os.IsNotExist(err) {
		t.Fatalf("err = %v, want os.IsNotExist", err)
	}
}
