package lsm

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/paarthsiphone/lsm-tree-storage/memtable"
	"github.com/paarthsiphone/lsm-tree-storage/sstable"
	"github.com/paarthsiphone/lsm-tree-storage/wal"
)

const (
	walSubdir     = "wal"
	sstableSubdir = "sstables"

	// defaultFlushThreshold is the memtable size, in bytes, past which a
	// Put/Delete triggers a flush to a new SSTable. Overridable via
	// SetFlushThreshold, mainly so tests can force a flush without
	// writing megabytes of data.
	defaultFlushThreshold = 4 * 1024 * 1024
)

// DB is the embedded engine's public API.
type DB struct {
	dir string
	w   *wal.Writer
	mem *memtable.SkipList

	// sstables holds every discovered/flushed SSTable, newest first.
	// This ordering is what makes the newest-wins read path correct:
	// the first hit (including a tombstone hit) wins.
	sstables []*sstable.SSTable
	nextSeq  int

	flushThreshold int
}

// Open replays dir's WAL into a fresh memtable, discovers any existing
// SSTables, and opens a new WAL segment for continued appends. Recovery
// always resumes on a brand-new WAL segment (via wal.NextSegmentPath)
// rather than reopening the last one, since the last segment may end
// with a torn record from an in-progress write at crash time.
func Open(dir string) (*DB, error) {
	walDir := filepath.Join(dir, walSubdir)
	sstDir := filepath.Join(dir, sstableSubdir)

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

	if err := os.MkdirAll(sstDir, 0o755); err != nil {
		return nil, err
	}
	if err := removeOrphanedFlushTmpFiles(sstDir); err != nil {
		return nil, err
	}
	sstables, nextSeq, err := openSSTables(sstDir)
	if err != nil {
		return nil, err
	}

	path, err := wal.NextSegmentPath(walDir)
	if err != nil {
		return nil, err
	}
	w, err := wal.NewWriter(path)
	if err != nil {
		return nil, err
	}

	return &DB{
		dir:            dir,
		w:              w,
		mem:            mem,
		sstables:       sstables,
		nextSeq:        nextSeq,
		flushThreshold: defaultFlushThreshold,
	}, nil
}

// removeOrphanedFlushTmpFiles deletes any "*.sst.tmp" file left behind by
// a flush that was interrupted (e.g. a real crash) before its atomic
// rename into place completed. sstable.FlushMemtable's rename is the
// single moment a flush becomes visible and durable -- a leftover .tmp
// file was never committed, so its data was never part of the
// database's state, and the WAL segment(s) covering that data are still
// on disk (RemoveSegmentsBefore only ever runs after a *successful*
// flush) and will be replayed normally above. This is pure disk hygiene,
// not a correctness fix: openSSTables already ignores these files (it
// only looks at ".sst", and filepath.Ext("x.sst.tmp") is ".tmp"), so
// leaving one in place doesn't corrupt anything -- it just leaks disk
// space forever, since nothing else ever revisits it. No directory
// fsync is needed here: an incompletely-cleaned-up .tmp file reappearing
// after a crash during this very cleanup is harmless and self-correcting
// (the next Open just tries again), unlike a lost SSTable rename.
func removeOrphanedFlushTmpFiles(dir string) error {
	des, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, de := range des {
		if de.IsDir() {
			continue
		}
		if !strings.HasSuffix(de.Name(), ".sst.tmp") {
			continue
		}
		if err := os.Remove(filepath.Join(dir, de.Name())); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

// openSSTables performs a naive directory scan of dir for "NNNNNN.sst"
// files, opens each one (reading only its footer/index, never its data
// section), and returns them ordered newest-first by sequence number,
// plus the next sequence number to use for a future flush.
//
// This is a deliberate, temporary discovery mechanism, per the Phase 3
// brief: it can't distinguish a legitimate SSTable from one orphaned by
// a half-finished future compaction, because there's no durable record
// yet of "which files are actually part of the database" -- that's
// Phase 4's MANIFEST.
func openSSTables(dir string) (tables []*sstable.SSTable, nextSeq int, err error) {
	des, err := os.ReadDir(dir)
	if err != nil {
		return nil, 0, err
	}

	var names []string
	for _, de := range des {
		if de.IsDir() {
			continue
		}
		if filepath.Ext(de.Name()) == ".sst" {
			names = append(names, de.Name())
		}
	}
	sort.Strings(names) // "NNNNNN.sst" is fixed-width, so lexical order == numeric order

	tables = make([]*sstable.SSTable, 0, len(names))
	maxSeq := 0
	for _, name := range names {
		seq, err := parseSSTableSeq(name)
		if err != nil {
			return nil, 0, err
		}
		if seq > maxSeq {
			maxSeq = seq
		}

		st, err := sstable.OpenSSTable(filepath.Join(dir, name))
		if err != nil {
			return nil, 0, err
		}
		tables = append(tables, st)
	}

	// names is oldest-to-newest; reverse in place for newest-first.
	for i, j := 0, len(tables)-1; i < j; i, j = i+1, j-1 {
		tables[i], tables[j] = tables[j], tables[i]
	}

	return tables, maxSeq + 1, nil
}

func parseSSTableSeq(name string) (int, error) {
	base := strings.TrimSuffix(name, ".sst")
	seq, err := strconv.Atoi(base)
	if err != nil {
		return 0, fmt.Errorf("lsm: invalid sstable filename %q: %w", name, err)
	}
	return seq, nil
}

func sstablePath(dir string, seq int) string {
	return filepath.Join(dir, sstableSubdir, fmt.Sprintf("%06d.sst", seq))
}

// SetFlushThreshold overrides the memtable size threshold that triggers
// a flush to a new SSTable. Intended for tests that want to force a
// flush deterministically without writing megabytes of data.
func (db *DB) SetFlushThreshold(n int) {
	db.flushThreshold = n
}

// Put durably appends the write to the WAL, then applies it to the
// memtable. The memtable is never mutated unless the WAL append
// succeeded.
func (db *DB) Put(key, value []byte) error {
	if err := db.w.Append(wal.Entry{Op: wal.OpPut, Key: key, Value: value}); err != nil {
		return err
	}
	db.mem.Put(key, value)
	return db.maybeFlush()
}

// Delete durably appends a tombstone to the WAL, then applies it to the
// memtable.
func (db *DB) Delete(key []byte) error {
	if err := db.w.Append(wal.Entry{Op: wal.OpDelete, Key: key}); err != nil {
		return err
	}
	db.mem.Delete(key)
	return db.maybeFlush()
}

// maybeFlush flushes the current memtable to a new SSTable if it has
// grown past the flush threshold. On success, it swaps in a fresh empty
// memtable and starts a new WAL segment; the old WAL segment is left on
// disk (its data is now durable in the SSTable, but Phase 3 doesn't
// clean up old segments yet -- that's a deferred, non-correctness-
// affecting cleanup per the brief).
func (db *DB) maybeFlush() error {
	if db.mem.SizeBytes() < db.flushThreshold {
		return nil
	}

	seq := db.nextSeq
	path := sstablePath(db.dir, seq)
	if _, err := sstable.FlushMemtable(db.mem, path); err != nil {
		return err
	}

	st, err := sstable.OpenSSTable(path)
	if err != nil {
		return err
	}
	db.nextSeq++
	db.sstables = append([]*sstable.SSTable{st}, db.sstables...)

	walDir := filepath.Join(db.dir, walSubdir)
	walPath, err := wal.NextSegmentPath(walDir)
	if err != nil {
		return err
	}
	newW, err := wal.NewWriter(walPath)
	if err != nil {
		return err
	}
	if err := db.w.Close(); err != nil {
		newW.Close()
		return err
	}
	db.w = newW
	db.mem = memtable.New()

	// Every WAL segment older than the one just created holds only data
	// that's now durably captured in the SSTable flushed above (DB
	// always starts a brand-new segment at flush time, so nothing newer
	// could have been written to an older segment since). It's therefore
	// safe to delete them now, and only now -- this call is what keeps
	// wal.Replay's cost, and the memtable size after a restart, bounded
	// by activity since the last flush rather than by the database's
	// entire history. A crash before this line just means the cleanup
	// is deferred to a later flush, not lost data: see
	// wal.RemoveSegmentsBefore's doc comment.
	newSeq, err := wal.ParseSegmentSeq(filepath.Base(walPath))
	if err != nil {
		return err
	}
	if err := wal.RemoveSegmentsBefore(walDir, newSeq); err != nil {
		return err
	}

	return nil
}

// Get returns the most recent value for key. found is false both when
// the key has never been written and when its newest entry is a
// tombstone -- callers can't distinguish "never written" from "deleted"
// from this signature alone, which is correct: both mean "no value".
//
// Sources are checked newest-to-oldest -- memtable first, then SSTables
// in flush order (newest first) -- and the first hit wins, including a
// tombstone hit.
func (db *DB) Get(key []byte) (value []byte, found bool, err error) {
	if v, found, tombstone := db.mem.Get(key); found {
		if tombstone {
			return nil, false, nil
		}
		return v, true, nil
	}

	for _, st := range db.sstables {
		v, found, tombstone, err := st.Get(key)
		if err != nil {
			return nil, false, err
		}
		if found {
			if tombstone {
				return nil, false, nil
			}
			return v, true, nil
		}
	}

	return nil, false, nil
}

// Close closes the WAL writer and every open SSTable file handle. It
// attempts to close all of them even if one fails, so a single bad file
// handle can't leak the rest; the first error encountered is returned.
func (db *DB) Close() error {
	var firstErr error
	for _, st := range db.sstables {
		if err := st.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if err := db.w.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}
