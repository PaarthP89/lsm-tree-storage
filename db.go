package lsm

import (
	"fmt"
	"path/filepath"

	"github.com/paarthsiphone/lsm-tree-storage/memtable"
	"github.com/paarthsiphone/lsm-tree-storage/wal"
)

const walSubdir = "wal"

// DB is the embedded engine's public API.
type DB struct {
	w   *wal.Writer
	mem *memtable.SkipList
}

// Open replays dir's WAL into a fresh memtable and opens a new WAL
// segment for continued appends. Recovery always resumes on a brand-new
// segment (via wal.NextSegmentPath) rather than reopening the last one,
// since the last segment may end with a torn record from an in-progress
// write at crash time.
func Open(dir string) (*DB, error) {
	walDir := filepath.Join(dir, walSubdir)

	entries, err := wal.Replay(walDir)
	if err != nil {
		return nil, err
	}

	mem := memtable.New()
	for _, e := range entries {
		switch e.Op {
		case wal.OpPut:
			mem.Put(e.Key, e.Value)
		case wal.OpDelete:
			mem.Delete(e.Key)
		default:
			return nil, fmt.Errorf("lsm: unknown wal op %v", e.Op)
		}
	}

	path, err := wal.NextSegmentPath(walDir)
	if err != nil {
		return nil, err
	}
	w, err := wal.NewWriter(path)
	if err != nil {
		return nil, err
	}

	return &DB{w: w, mem: mem}, nil
}

// Put durably appends the write to the WAL, then applies it to the
// memtable. The memtable is never mutated unless the WAL append
// succeeded.
func (db *DB) Put(key, value []byte) error {
	if err := db.w.Append(wal.Entry{Op: wal.OpPut, Key: key, Value: value}); err != nil {
		return err
	}
	db.mem.Put(key, value)
	return nil
}

// Delete durably appends a tombstone to the WAL, then applies it to the
// memtable.
func (db *DB) Delete(key []byte) error {
	if err := db.w.Append(wal.Entry{Op: wal.OpDelete, Key: key}); err != nil {
		return err
	}
	db.mem.Delete(key)
	return nil
}

// Get returns the most recent value for key. found is false both when
// the key has never been written and when its newest entry is a
// tombstone -- callers can't distinguish "never written" from "deleted"
// from this signature alone, which is correct: both mean "no value".
func (db *DB) Get(key []byte) (value []byte, found bool, err error) {
	v, found, tombstone := db.mem.Get(key)
	if !found || tombstone {
		return nil, false, nil
	}
	return v, true, nil
}

func (db *DB) Close() error {
	return db.w.Close()
}
