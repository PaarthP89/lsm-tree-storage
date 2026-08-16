package manifest

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// CurrentFileName is the fixed name of the file that points at the
// active MANIFEST (§4/§5): a single line containing that MANIFEST's
// basename.
const CurrentFileName = "CURRENT"

// fsyncDir fsyncs a directory's own metadata (entries: creations,
// renames, deletions) -- same reasoning and same spy-substitution pattern
// as wal.fsyncDir and sstable.fsyncDir: a file's own fsync only covers
// its contents, not the directory entry that makes it discoverable or
// the rename that repoints it.
var fsyncDir = func(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// WriteCurrent atomically writes manifestName (a MANIFEST file's
// basename) to dir/CURRENT via temp-file + fsync + atomic rename, then
// fsyncs dir so the rename itself survives a crash.
func WriteCurrent(dir, manifestName string) error {
	path := filepath.Join(dir, CurrentFileName)
	tmpPath := path + ".tmp"

	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("manifest: create %s: %w", tmpPath, err)
	}
	defer os.Remove(tmpPath)

	if _, err := f.WriteString(manifestName + "\n"); err != nil {
		f.Close()
		return fmt.Errorf("manifest: write %s: %w", tmpPath, err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("manifest: fsync %s: %w", tmpPath, err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("manifest: close %s: %w", tmpPath, err)
	}

	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("manifest: rename %s -> %s: %w", tmpPath, path, err)
	}
	if err := fsyncDir(dir); err != nil {
		return fmt.Errorf("manifest: fsync dir for %s: %w", path, err)
	}
	return nil
}

// ReadCurrent reads dir/CURRENT and returns the active MANIFEST's
// basename. Returns an error satisfying os.IsNotExist if CURRENT hasn't
// been written yet.
func ReadCurrent(dir string) (string, error) {
	path := filepath.Join(dir, CurrentFileName)
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	name := strings.TrimSpace(string(data))
	if name == "" {
		return "", fmt.Errorf("manifest: %s: empty CURRENT file", path)
	}
	return name, nil
}
