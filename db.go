package lsm

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/paarthsiphone/lsm-tree-storage/compaction"
	"github.com/paarthsiphone/lsm-tree-storage/manifest"
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

	// defaultCompactionTriggerThreshold is the live SSTable count past
	// which MaybeCompact merges every current table into one. Single-level,
	// "merge everything periodically" per CLAUDE.md §3/§7 -- a size-ratio
	// multi-level trigger is out of scope (8d, stretch). Overridable via
	// SetCompactionThreshold, mirroring SetFlushThreshold, so tests (and
	// the Phase 7 chaos harness) can force compaction to fire without
	// writing an unrealistic number of keys.
	defaultCompactionTriggerThreshold = 4
)

// DB is the embedded engine's public API.
type DB struct {
	dir          string
	w            *wal.Writer
	mem          *memtable.SkipList
	manifestPath string

	// sstables holds every discovered/flushed SSTable, newest first.
	// This ordering is what makes the newest-wins read path correct:
	// the first hit (including a tombstone hit) wins.
	sstables []*sstable.SSTable
	nextSeq  int

	flushThreshold      int
	compactionThreshold int
}

// Open reconstructs a DB's exact pre-crash state via the fixed recovery
// sequence CURRENT -> MANIFEST -> SSTable set -> WAL -> memtable (§7):
// the MANIFEST-reconstructed SSTable set must be settled before WAL
// replay, since WAL replay rebuilds the memtable *on top of* that
// already-settled set. Concretely: read CURRENT to find the active
// MANIFEST, replay it to get the authoritative live SSTable filenames,
// open exactly those files (any ".sst" on disk not in that set is
// ignored -- orphaned, not deleted), then replay the WAL into a fresh
// memtable and open a new WAL segment for continued appends. Recovery
// always resumes on a brand-new WAL segment (via wal.NextSegmentPath)
// rather than reopening the last one, since the last segment may end
// with a torn record from an in-progress write at crash time.
func Open(dir string) (*DB, error) {
	walDir := filepath.Join(dir, walSubdir)
	sstDir := filepath.Join(dir, sstableSubdir)

	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(sstDir, 0o755); err != nil {
		return nil, err
	}

	manifestName, err := ensureManifest(dir)
	if err != nil {
		return nil, err
	}
	manifestPath := filepath.Join(dir, manifestName)

	// Must run before any future AppendEdit call against this file (the
	// first of which happens on the next flush, not here) -- see
	// manifest.RepairTornTail's doc comment for why a torn tail left by
	// a crash mid-append must be truncated away now, rather than left
	// for a later AppendEdit to silently write past.
	if err := manifest.RepairTornTail(manifestPath); err != nil {
		return nil, err
	}

	edits, err := manifest.ReplayManifest(manifestPath)
	if err != nil {
		return nil, err
	}
	liveSSTables := manifest.ReconstructSSTableSet(edits)

	if err := removeOrphanedFlushTmpFiles(sstDir); err != nil {
		return nil, err
	}
	sstables, nextSeq, err := openSSTables(sstDir, liveSSTables)
	if err != nil {
		return nil, err
	}

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

	return &DB{
		dir:                 dir,
		w:                   w,
		mem:                 mem,
		manifestPath:        manifestPath,
		sstables:            sstables,
		nextSeq:             nextSeq,
		flushThreshold:      defaultFlushThreshold,
		compactionThreshold: defaultCompactionTriggerThreshold,
	}, nil
}

// ensureManifest returns the active MANIFEST's filename, per CURRENT.
// A fresh database (no CURRENT file yet) durably creates one pointing at
// manifest.InitialFileName -- the MANIFEST file itself doesn't need to
// exist as an empty file up front, since manifest.AppendEdit creates it
// on the first flush, and manifest.ReplayManifest treats a missing file
// as "zero edits", which is exactly correct for a database that hasn't
// flushed anything yet.
func ensureManifest(dir string) (string, error) {
	name, err := manifest.ReadCurrent(dir)
	if err == nil {
		return name, nil
	}
	if !os.IsNotExist(err) {
		return "", err
	}

	name = manifest.InitialFileName
	if err := manifest.WriteCurrent(dir, name); err != nil {
		return "", err
	}
	return name, nil
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

// openSSTables opens exactly the SSTable files named in live (the
// MANIFEST-reconstructed authoritative set -- see manifest.
// ReconstructSSTableSet), reading only each file's footer/index, never
// its data section, and returns them ordered newest-first by sequence
// number.
//
// It's deliberate that this opens live's names directly rather than
// intersecting them with a directory scan: the MANIFEST is authoritative
// (§7), so a name it lists as live but that's actually missing from disk
// is real corruption -- silently opening fewer tables than the MANIFEST
// promises would be the same class of bug sstable.OpenSSTable's footer
// checksum and wal's non-torn-tail corruption both refuse to allow
// elsewhere in this codebase (fail loudly, don't silently drop data).
//
// nextSeq, by contrast, is derived from every ".sst" file actually
// present in dir, not just the live set: a file orphaned by a crash
// between an SSTable's atomic rename and its MANIFEST append (see
// maybeFlush) is correctly left unopened, but its sequence number must
// still not be reused, or a future flush's atomic rename would silently
// overwrite it.
func openSSTables(dir string, live []string) (tables []*sstable.SSTable, nextSeq int, err error) {
	des, err := os.ReadDir(dir)
	if err != nil {
		return nil, 0, err
	}

	maxSeq := 0
	for _, de := range des {
		if de.IsDir() {
			continue
		}
		if filepath.Ext(de.Name()) != ".sst" {
			continue
		}
		seq, err := parseSSTableSeq(de.Name())
		if err != nil {
			return nil, 0, err
		}
		if seq > maxSeq {
			maxSeq = seq
		}
	}

	names := append([]string(nil), live...)
	sort.Strings(names) // "NNNNNN.sst" is fixed-width, so lexical order == numeric order; also defends against relying on manifest.ReconstructSSTableSet's own ordering guarantee

	tables = make([]*sstable.SSTable, 0, len(names))
	for _, name := range names {
		st, err := sstable.OpenSSTable(filepath.Join(dir, name))
		if err != nil {
			return nil, 0, fmt.Errorf("lsm: MANIFEST lists %s as a live SSTable but it failed to open: %w", name, err)
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

// SetCompactionThreshold overrides the live SSTable count past which
// MaybeCompact fires. Intended for tests (and the chaos harness) that want
// to force compaction deterministically without writing an unrealistic
// number of keys.
func (db *DB) SetCompactionThreshold(n int) {
	db.compactionThreshold = n
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
// memtable, starts a new WAL segment, and deletes WAL segments now
// superseded by the flush.
func (db *DB) maybeFlush() error {
	if db.mem.SizeBytes() < db.flushThreshold {
		return nil
	}

	seq := db.nextSeq
	path := sstablePath(db.dir, seq)
	if _, err := sstable.FlushMemtable(db.mem, path); err != nil {
		return err
	}

	// Open (and thus fully footer-checksum-verify) the file we just wrote
	// *before* the MANIFEST ever calls it live. If this fails -- a freak
	// failure, since the file was just written and fsynced moments ago
	// by the FlushMemtable call directly above -- the WAL segment(s)
	// covering this data are still fully intact on disk (RemoveSegmentsBefore
	// hasn't run yet), and no MANIFEST edit has been written yet either, so
	// db.sstables/the MANIFEST are left exactly as they were: nothing to
	// undo, nothing recorded as live that isn't safely openable.
	st, err := sstable.OpenSSTable(path)
	if err != nil {
		return err
	}

	// The SSTable is durable on disk the instant the rename above
	// succeeds, but it isn't yet *part of the database* -- Open only
	// ever trusts the MANIFEST-reconstructed set (§7), not a directory
	// scan. Appending this edit (fsync'd) is what actually commits the
	// flush. A crash between the rename and this line leaves the file
	// orphaned: harmless, since it's ignored on the next Open (not
	// deleted -- see openSSTables) and every key it held is still
	// recoverable via WAL replay, because RemoveSegmentsBefore below
	// hasn't run yet at that point either.
	if err := manifest.AppendEdit(db.manifestPath, manifest.VersionEdit{
		Type: manifest.SSTableAdded,
		File: filepath.Base(path),
	}); err != nil {
		st.Close()
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

	return db.MaybeCompact()
}

// MaybeCompact merges every currently-live SSTable into one new table if
// their count exceeds db.compactionThreshold; otherwise it's a no-op.
// Called synchronously after every flush (a simpler synchronous check,
// per the Phase 6 brief, rather than a background ticker -- there's no
// concurrent access to race against yet, see the "DB is not safe for
// concurrent use" note in CLAUDE.md §11).
//
// The MANIFEST sequence is deliberately ADDED (the new merged file) before
// REMOVED (every input file), each individually fsync'd. Naively doing it
// the other way risks a real data-loss window: a crash between REMOVED
// (inputs) and ADDED (output) would leave the reconstructed SSTable set
// with no record of that data at all. ADDED-then-REMOVED instead risks
// only redundancy on a crash in the gap -- both old and new files end up
// live, which newest-wins read logic handles correctly -- never data
// loss. See compaction.Compact's doc comment and CLAUDE.md §7/§8.
func (db *DB) MaybeCompact() error {
	if len(db.sstables) <= db.compactionThreshold {
		return nil
	}

	inputs := append([]*sstable.SSTable(nil), db.sstables...)
	seq := db.nextSeq
	outPath := sstablePath(db.dir, seq)

	if _, err := compaction.Compact(inputs, outPath); err != nil {
		return err
	}

	// Open (and thus fully footer-checksum-verify) the merged output
	// *before* either MANIFEST edit runs -- deliberately earlier than
	// maybeFlush's own crash-orphan window comment below might suggest.
	// For a flush, a freak post-write open failure still leaves the data
	// recoverable via the WAL (not yet cleaned up at that point). Here
	// there is no such backstop: once the REMOVED edits below commit, the
	// old inputs' data has no other surviving copy anywhere (that's the
	// entire premise tombstone GC and single-level compaction rely on).
	// So this must succeed *before* we ever tell the MANIFEST the old
	// generation is retired, not after -- an unrecoverable data-loss
	// window otherwise, not just a redundant/orphaned-but-harmless one.
	newTable, err := sstable.OpenSSTable(outPath)
	if err != nil {
		return err
	}

	// This is the moment the merge is actually committed: any crash
	// before this line leaves outPath as a harmless, fully-valid but
	// unreferenced file (ignored on the next Open, exactly like a flush
	// orphaned between rename and MANIFEST append -- see maybeFlush).
	if err := manifest.AppendEdit(db.manifestPath, manifest.VersionEdit{
		Type: manifest.SSTableAdded,
		File: filepath.Base(outPath),
	}); err != nil {
		newTable.Close()
		return err
	}
	for _, in := range inputs {
		if err := manifest.AppendEdit(db.manifestPath, manifest.VersionEdit{
			Type: manifest.SSTableRemoved,
			File: filepath.Base(in.Meta().Path),
		}); err != nil {
			newTable.Close()
			return err
		}
	}

	db.nextSeq = seq + 1
	db.sstables = []*sstable.SSTable{newTable}

	// Both MANIFEST edits above are already durable, so every input is
	// now provably retired from the database's authoritative state on
	// this (non-crash) success path -- closing and deleting them here is
	// pure cleanup, not a correctness requirement. Only the *post-crash*
	// cleanup of files left behind by an actual mid-compaction crash
	// (compaction crash-injection scenario 4: ADDED committed, REMOVED
	// never ran) is out of scope for this phase, per the Phase 6 brief --
	// those files just sit around as harmless, provably-correct
	// redundancy until a future compaction sweeps them up too.
	// Close (and then remove) every input regardless of an earlier one
	// failing -- same "attempt all, keep the first error" pattern as
	// DB.Close, so one bad file handle can't leak the rest of this
	// generation's descriptors or leave later files undeleted.
	var firstErr error
	for _, in := range inputs {
		if err := in.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	for _, in := range inputs {
		if err := os.Remove(in.Meta().Path); err != nil && !os.IsNotExist(err) && firstErr == nil {
			firstErr = err
		}
	}

	return firstErr
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
