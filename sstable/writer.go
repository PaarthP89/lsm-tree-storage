package sstable

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"

	"github.com/paarthsiphone/lsm-tree-storage/memtable"
	"github.com/paarthsiphone/lsm-tree-storage/wal"
)

// SSTableMeta describes a flushed SSTable without requiring its data
// section to be read.
type SSTableMeta struct {
	Path       string
	MinKey     []byte
	MaxKey     []byte
	EntryCount int
}

func cloneBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out
}

func opFor(tombstone bool) wal.OpType {
	if tombstone {
		return wal.OpDelete
	}
	return wal.OpPut
}

// fsyncDir fsyncs a directory's own metadata (entries: creations,
// renames, deletions) so that a rename landing a new SSTable at its
// final path actually survives a crash -- a file's own fsync only
// covers its contents, not the directory entry that makes it
// discoverable. A package-level var so tests can substitute a spy to
// prove it's actually invoked, mirroring wal's fsyncDir.
var fsyncDir = func(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// FlushMemtable writes m's entries, in the sorted order m's Iterator
// produces, to a new SSTable file. Records reuse wal's entry wire format
// exactly (same [checksum][op][key][value] encoding as the WAL) -- one
// format, two files, not two formats to keep in sync.
//
// The file is written at path+".tmp", fsynced, then atomically renamed to
// path. A crash at any point before the rename leaves at most an orphan
// .tmp file and never a partially-written file at the final path.
func FlushMemtable(m memtable.Memtable, path string) (*SSTableMeta, error) {
	tmpPath := path + ".tmp"

	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, fmt.Errorf("sstable: create %s: %w", tmpPath, err)
	}
	// Best-effort cleanup on any early return; once the rename below
	// succeeds this is a no-op (the file no longer exists at tmpPath).
	defer os.Remove(tmpPath)

	bw := bufio.NewWriter(f)

	var (
		offset int64
		index  []indexEntry
		minKey []byte
		maxKey []byte
		count  int
	)

	it := m.Iterator()
	for it.Next() {
		key := it.Key()

		if count == 0 {
			minKey = cloneBytes(key)
		}
		maxKey = cloneBytes(key)

		if count%indexInterval == 0 {
			index = append(index, indexEntry{key: cloneBytes(key), offset: offset})
		}

		buf := wal.EncodeEntry(wal.Entry{
			Op:    opFor(it.Tombstone()),
			Key:   key,
			Value: it.Value(),
		})
		n, err := bw.Write(buf)
		if err != nil {
			f.Close()
			return nil, fmt.Errorf("sstable: write %s: %w", tmpPath, err)
		}
		offset += int64(n)
		count++
	}

	footerOffset := offset
	footerBody := encodeFooter(index, minKey, maxKey, count)
	if _, err := bw.Write(footerBody); err != nil {
		f.Close()
		return nil, fmt.Errorf("sstable: write footer %s: %w", tmpPath, err)
	}

	trailer := encodeTrailer(footerBody, footerOffset)
	if _, err := bw.Write(trailer); err != nil {
		f.Close()
		return nil, fmt.Errorf("sstable: write trailer %s: %w", tmpPath, err)
	}

	if err := bw.Flush(); err != nil {
		f.Close()
		return nil, fmt.Errorf("sstable: flush %s: %w", tmpPath, err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return nil, fmt.Errorf("sstable: fsync %s: %w", tmpPath, err)
	}
	if err := f.Close(); err != nil {
		return nil, fmt.Errorf("sstable: close %s: %w", tmpPath, err)
	}

	if err := os.Rename(tmpPath, path); err != nil {
		return nil, fmt.Errorf("sstable: rename %s -> %s: %w", tmpPath, path, err)
	}
	if err := fsyncDir(filepath.Dir(path)); err != nil {
		return nil, fmt.Errorf("sstable: fsync dir for %s: %w", path, err)
	}

	return &SSTableMeta{Path: path, MinKey: minKey, MaxKey: maxKey, EntryCount: count}, nil
}
