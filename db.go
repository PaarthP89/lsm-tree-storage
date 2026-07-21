package lsm

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

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

	// defaultL0CompactionThreshold is the live L0 file count past which
	// MaybeCompact merges all of L0 (plus any overlapping L1 files) into
	// L1 (Phase 8d). Same value and trigger semantics as the pre-8d
	// single-level defaultCompactionTriggerThreshold it replaces.
	// Overridable via SetL0CompactionThreshold/SetCompactionThreshold, so
	// tests (and the chaos harness) can force compaction to fire without
	// writing an unrealistic number of keys.
	defaultL0CompactionThreshold = 4

	// defaultL1SizeRatio is how many multiples of the L0 flush-size
	// target L1's total on-disk size may reach before a full L1->L1
	// recompaction fires. This reuses the 10x size-ratio CLAUDE.md §3
	// already names as the rationale for a multi-level scheme, rather
	// than inventing a new number. Overridable via SetL1SizeRatio.
	defaultL1SizeRatio = 10
)

// dbState is one immutable generation of DB's mutable data: the live
// memtable, its backing WAL writer, and both SSTable levels. A Put/Delete
// that crosses the flush threshold, or a compaction, never mutates these
// fields in place (Phase 8c) -- it builds a brand-new dbState and
// publishes it via DB.state.Store, so a concurrent reader that already
// loaded the previous *dbState keeps observing a fully self-consistent,
// never-mutated-after-publish snapshot for the rest of its call.
type dbState struct {
	mem *memtable.SkipList
	w   *wal.Writer

	// l0 holds every live level-0 SSTable, newest first. L0 files may
	// overlap in key range (they're raw memtable flushes), so a read must
	// check every one of them, in this order, until a hit.
	l0 []*sstable.SSTable
	// l1 holds every live level-1 SSTable, sorted ascending by MinKey.
	// L1 files are non-overlapping by construction -- every L1-producing
	// compaction is responsible for maintaining that invariant -- so a
	// read can binary-search this slice by key range instead of checking
	// every file.
	l1 []*sstable.SSTable
}

// DB is the embedded engine's public API.
//
// Concurrency (Phase 8c): DB is safe for any mix of concurrent Get/Scan
// calls alongside Put/Delete calls (which may themselves trigger a
// synchronous flush and/or compaction). It is NOT safe to call Close
// concurrently with any other in-flight DB method -- callers must ensure
// every other call has returned before closing.
//
// Reads (Get/Scan) are entirely lock-free: they load the current
// *dbState via an atomic pointer once per call and operate only on that
// snapshot, which is never mutated after being published. Writes
// (Put/Delete, and the flush/compaction either can trigger) are
// serialized against each other by writeMu -- see writeMu's own doc
// comment for why a lock-free "elected flusher" CAS guard (the more
// obvious design) is not actually safe here.
type DB struct {
	dir          string
	manifestPath string

	state atomic.Pointer[dbState]

	// writeMu serializes Put/Delete -- and the flush/compaction either can
	// synchronously trigger -- against each other. Get/Scan/Close never
	// acquire it; reads always go through the lock-free state pointer
	// instead, so a writer holding writeMu (even for the duration of a
	// slow flush or compaction) never blocks a concurrent reader.
	//
	// A bare non-blocking "elected flusher" CAS guard (flush a snapshot,
	// then atomically swap it in, with no serialization of the memtable
	// insert itself) is NOT safe for this specific memtable implementation:
	// memtable.SkipList.Iterator's own doc comment says traversal "is not
	// synchronized with concurrent writes to the same list" -- so a
	// concurrent Put racing with an in-progress flush's iterator could
	// insert into the very memtable being flushed without any guarantee
	// the flush's SSTable output captures it. Once that memtable is
	// retired (swapped out of state), such a write would be gone from
	// every live source: not in the SSTable (missed by the race), not in
	// the new empty memtable (never inserted there), and its WAL segment
	// is only deleted after the flush completes, so even crash-recovery
	// would only save it on a *restart*, not for a concurrent *live*
	// Get -- a real, silent data-loss bug for a live reader, not just a
	// benign race. writeMu is the fix: Put/Delete only ever inserts into a
	// memtable while holding writeMu, and a flush's iterator only ever
	// runs while the same goroutine still holds writeMu, so the two can
	// never interleave.
	//
	// This makes Put/Delete calls run one at a time relative to each
	// other (mirroring LevelDB/RocksDB's own single-active-writer model),
	// trading writer/writer parallelism for a design that's actually
	// provably correct against this memtable's documented contract,
	// rather than a fancier lock-free scheme that looks right but isn't.
	writeMu sync.Mutex
	nextSeq int // only ever touched while writeMu is held

	// inFlightReaders counts Get calls (whole call) and Scan calls (the
	// snapshot-construction portion only, not the returned iterator's
	// later Next() calls -- see Scan's doc comment) currently in
	// progress. Compaction (never flush -- see compactL0ToL1IfNeeded's
	// doc comment) drains this to 0 after publishing a new state and
	// before closing/removing the SSTable files that state retired, so a
	// reader that loaded the *previous* state can never have a file
	// closed out from under it mid-read.
	inFlightReaders atomic.Int64

	flushThreshold        int
	l0CompactionThreshold int
	l1SizeRatio           int
}

// beginRead/endRead bracket the portion of a read that may still be
// touching a dbState's SSTable files -- see inFlightReaders' doc comment.
func (db *DB) beginRead() { db.inFlightReaders.Add(1) }
func (db *DB) endRead()   { db.inFlightReaders.Add(-1) }

// drainReaders blocks until every beginRead already in progress at the
// moment this is called has reached its matching endRead. Any read that
// starts *after* the state swap this precedes will load the new state and
// never touch the files about to be closed, so it isn't waited on here --
// only reads that could still be holding the just-retired snapshot are.
func (db *DB) drainReaders() {
	for db.inFlightReaders.Load() > 0 {
		runtime.Gosched()
	}
}

// Stats is a point-in-time snapshot of DB's internal shape -- test/
// diagnostic use only (mirrors sstable.SSTable.HasBloomFilter's role),
// not part of the read/write path. Lock-free, like Get: loads the current
// state once and reads off it.
type Stats struct {
	L0Count           int
	L1Count           int
	MemtableSizeBytes int
}

// Stats returns a snapshot of the current dbState: live L0/L1 file counts
// and the current memtable's size in bytes. Useful for observing a
// flush/compaction's effect on level membership without reaching into
// unexported fields.
func (db *DB) Stats() Stats {
	s := db.state.Load()
	return Stats{
		L0Count:           len(s.l0),
		L1Count:           len(s.l1),
		MemtableSizeBytes: s.mem.SizeBytes(),
	}
}

// liveSSTables returns every currently-live SSTable across both levels,
// L0 (newest-first) then L1 (key-range order), as of the current state.
// A flat view for diagnostics/tests that only care about total live count
// or membership, not level-specific handling.
func (db *DB) liveSSTables() []*sstable.SSTable {
	s := db.state.Load()
	out := make([]*sstable.SSTable, 0, len(s.l0)+len(s.l1))
	out = append(out, s.l0...)
	out = append(out, s.l1...)
	return out
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
	liveLeveled := manifest.ReconstructLeveledSSTableSet(edits)

	if err := removeOrphanedFlushTmpFiles(sstDir); err != nil {
		return nil, err
	}
	l0, l1, nextSeq, err := openLeveledSSTables(sstDir, liveLeveled)
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

	db := &DB{
		dir:                   dir,
		manifestPath:          manifestPath,
		nextSeq:               nextSeq,
		flushThreshold:        defaultFlushThreshold,
		l0CompactionThreshold: defaultL0CompactionThreshold,
		l1SizeRatio:           defaultL1SizeRatio,
	}
	db.state.Store(&dbState{mem: mem, w: w, l0: l0, l1: l1})
	return db, nil
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
// not a correctness fix: openLeveledSSTables already ignores these files
// (it only looks at ".sst", and filepath.Ext("x.sst.tmp") is ".tmp"), so
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

// openLeveledSSTables opens exactly the SSTable files named in live (the
// MANIFEST-reconstructed authoritative set, each tagged with its level --
// see manifest.ReconstructLeveledSSTableSet), reading only each file's
// footer/index, never its data section, and partitions them into l0
// (newest-first by sequence number, may overlap) and l1 (sorted ascending
// by MinKey -- Open trusts that every L1-producing compaction already
// maintained the non-overlap invariant, rather than re-verifying it here).
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
func openLeveledSSTables(dir string, live []manifest.LiveSSTable) (l0, l1 []*sstable.SSTable, nextSeq int, err error) {
	des, err := os.ReadDir(dir)
	if err != nil {
		return nil, nil, 0, err
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
			return nil, nil, 0, err
		}
		if seq > maxSeq {
			maxSeq = seq
		}
	}

	entries := append([]manifest.LiveSSTable(nil), live...)
	sort.Slice(entries, func(i, j int) bool { return entries[i].File < entries[j].File }) // "NNNNNN.sst" is fixed-width, so lexical order == numeric order

	var l0Entries, l1Entries []manifest.LiveSSTable
	for _, e := range entries {
		if e.Level == 0 {
			l0Entries = append(l0Entries, e)
		} else {
			l1Entries = append(l1Entries, e)
		}
	}

	openEntries := func(entries []manifest.LiveSSTable) ([]*sstable.SSTable, error) {
		tables := make([]*sstable.SSTable, 0, len(entries))
		for _, e := range entries {
			st, err := sstable.OpenSSTable(filepath.Join(dir, e.File))
			if err != nil {
				return nil, fmt.Errorf("lsm: MANIFEST lists %s as a live SSTable but it failed to open: %w", e.File, err)
			}
			tables = append(tables, st)
		}
		return tables, nil
	}

	l0, err = openEntries(l0Entries)
	if err != nil {
		return nil, nil, 0, err
	}
	// l0Entries is oldest-to-newest by filename; reverse in place for
	// newest-first, the order the read path requires.
	for i, j := 0, len(l0)-1; i < j; i, j = i+1, j-1 {
		l0[i], l0[j] = l0[j], l0[i]
	}

	l1, err = openEntries(l1Entries)
	if err != nil {
		return nil, nil, 0, err
	}
	sortL1ByMinKey(l1)

	return l0, l1, maxSeq + 1, nil
}

// sortL1ByMinKey sorts tables ascending by MinKey in place -- the order
// findL1TableForKey's binary search requires. Safe to call on any L1
// slice regardless of how it was assembled, since L1's non-overlap
// invariant means MinKey order and MaxKey order agree.
func sortL1ByMinKey(tables []*sstable.SSTable) {
	sort.Slice(tables, func(i, j int) bool {
		return bytes.Compare(tables[i].Meta().MinKey, tables[j].Meta().MinKey) < 0
	})
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
// flush deterministically without writing megabytes of data. Like every
// Set* method, this must not be called concurrently with Put/Delete --
// intended for setup, before concurrent access begins.
func (db *DB) SetFlushThreshold(n int) {
	db.flushThreshold = n
}

// SetL0CompactionThreshold overrides the live L0 file count past which
// MaybeCompact merges L0 (plus any overlapping L1 files) into L1 (Phase
// 8d). Intended for tests (and the chaos harness) that want to force
// compaction deterministically without writing an unrealistic number of
// keys.
func (db *DB) SetL0CompactionThreshold(n int) {
	db.l0CompactionThreshold = n
}

// SetCompactionThreshold is a pre-8d alias for SetL0CompactionThreshold,
// kept so existing callers (cmd/chaos, cmd/chaosworker, benchmarks)
// written against the single-level compaction trigger keep working
// unchanged -- "compaction threshold" and "L0 compaction threshold" mean
// the same thing now that L0 is the only level fresh flushes ever land in.
func (db *DB) SetCompactionThreshold(n int) {
	db.SetL0CompactionThreshold(n)
}

// SetL1SizeRatio overrides the multiple of the L0 flush-size target that
// L1's total on-disk size may reach before a full L1->L1 recompaction
// fires (Phase 8d). Intended for tests that want to force L1 recompaction
// deterministically.
func (db *DB) SetL1SizeRatio(n int) {
	db.l1SizeRatio = n
}

// Put durably appends the write to the WAL, then applies it to the
// memtable. The memtable is never mutated unless the WAL append
// succeeded. Safe for concurrent use alongside any other Put/Delete/Get/
// Scan call -- see writeMu's doc comment for the concurrency design.
func (db *DB) Put(key, value []byte) error {
	return db.write(wal.Entry{Op: wal.OpPut, Key: key, Value: value})
}

// Delete durably appends a tombstone to the WAL, then applies it to the
// memtable. Safe for concurrent use -- see Put.
func (db *DB) Delete(key []byte) error {
	return db.write(wal.Entry{Op: wal.OpDelete, Key: key})
}

func (db *DB) write(e wal.Entry) error {
	db.writeMu.Lock()
	defer db.writeMu.Unlock()

	s := db.state.Load()
	if err := s.w.Append(e); err != nil {
		return err
	}
	switch e.Op {
	case wal.OpPut:
		s.mem.Put(e.Key, e.Value)
	case wal.OpDelete:
		s.mem.Delete(e.Key)
	}
	return db.maybeFlush(s)
}

// maybeFlush flushes s's memtable to a new SSTable if it has grown past
// the flush threshold; otherwise a no-op. On success, it publishes a
// brand-new dbState (fresh empty memtable, new WAL segment, the flushed
// table added to L0) via db.state.Store, then runs the two Phase 8d
// compaction triggers against that new state. Called only while writeMu
// is held (from DB.write) -- see writeMu's doc comment for why that
// matters for this memtable implementation specifically.
func (db *DB) maybeFlush(s *dbState) error {
	if s.mem.SizeBytes() < db.flushThreshold {
		return nil
	}

	seq := db.nextSeq
	path := sstablePath(db.dir, seq)
	if _, err := sstable.FlushMemtable(s.mem, path); err != nil {
		return err
	}

	// Open (and thus fully footer-checksum-verify) the file we just wrote
	// *before* the MANIFEST ever calls it live. If this fails -- a freak
	// failure, since the file was just written and fsynced moments ago
	// by the FlushMemtable call directly above -- the WAL segment(s)
	// covering this data are still fully intact on disk (RemoveSegmentsBefore
	// hasn't run yet), and no MANIFEST edit has been written yet either, so
	// the live state is left exactly as it was: nothing to undo, nothing
	// recorded as live that isn't safely openable.
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
	// deleted -- see openLeveledSSTables) and every key it held is still
	// recoverable via WAL replay, because RemoveSegmentsBefore below
	// hasn't run yet at that point either.
	//
	// A fresh flush always lands in L0 (Level: 0): it's raw memtable
	// output, not yet merged against anything, so it may overlap in key
	// range with any other live L0 or L1 file.
	if err := manifest.AppendEdit(db.manifestPath, manifest.VersionEdit{
		Type:  manifest.SSTableAdded,
		File:  filepath.Base(path),
		Level: 0,
	}); err != nil {
		st.Close()
		return err
	}
	db.nextSeq++
	newL0 := append([]*sstable.SSTable{st}, s.l0...)

	walDir := filepath.Join(db.dir, walSubdir)
	walPath, err := wal.NextSegmentPath(walDir)
	if err != nil {
		return err
	}
	newW, err := wal.NewWriter(walPath)
	if err != nil {
		return err
	}
	// Safe to close the old writer unconditionally here: writeMu has been
	// held continuously since this generation's dbState was loaded, so no
	// other Put/Delete could possibly still be using it -- and no reader
	// (Get/Scan) ever touches a dbState's w field at all.
	if err := s.w.Close(); err != nil {
		newW.Close()
		return err
	}

	newState := &dbState{mem: memtable.New(), w: newW, l0: newL0, l1: s.l1}
	db.state.Store(newState)

	// Every WAL segment older than the one just created holds only data
	// that's now durably captured in the SSTable flushed above (a fresh
	// segment is always started at flush time, so nothing newer could
	// have been written to an older segment since). It's therefore safe
	// to delete them now, and only now -- this call is what keeps
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

	return db.maybeCompact(newState)
}

// MaybeCompact runs the same compaction triggers maybeFlush runs
// automatically after every flush, on demand -- mainly useful for tests
// that want to force compaction deterministically without crossing a
// flush threshold naturally. Safe for concurrent use, like Put/Delete: it
// acquires writeMu for the duration of the check (and the compaction
// work, if either trigger fires).
func (db *DB) MaybeCompact() error {
	db.writeMu.Lock()
	defer db.writeMu.Unlock()
	return db.maybeCompact(db.state.Load())
}

// maybeCompact runs both Phase 8d compaction triggers, in order: L0->L1
// first (compactL0ToL1IfNeeded), then L1->L1 recompaction
// (compactL1IfNeeded). Two independent, separately-tunable triggers
// (db.l0CompactionThreshold, db.l1SizeRatio) -- running L0->L1 first means
// an L1 recompaction check always sees whatever L0 just contributed to L1
// in this same call, but the two never consume each other's inputs: L1
// recompaction only ever touches l1, L0->L1 only ever touches l0 (plus L1
// files it proves overlap, see compactL0ToL1IfNeeded).
//
// Called synchronously after every flush, only while writeMu is held (see
// maybeFlush) -- a simpler synchronous check, per the Phase 6 brief,
// rather than a background ticker.
func (db *DB) maybeCompact(s *dbState) error {
	s, err := db.compactL0ToL1IfNeeded(s)
	if err != nil {
		return err
	}
	_, err = db.compactL1IfNeeded(s)
	return err
}

// compactL0ToL1IfNeeded merges every live L0 file, plus every live L1 file
// whose key range overlaps the combined L0 range, into a single new L1
// file, if len(s.l0) exceeds db.l0CompactionThreshold; otherwise a no-op
// that returns s unchanged.
//
// canDropTombstones is always true for this merge: every live L0 file is
// included (inputsL0 is s.l0 in full, never a subset), and every L1 file
// whose range could possibly overlap the merge's eventual output is
// included too. That second part relies on L1's own non-overlap
// invariant: a one-pass overlap check against just the L0-combined range
// is sufficient (no need to iteratively re-check as more L1 files are
// added), because any L1 file NOT selected on that first pass has a range
// disjoint from every L1 file that WAS selected (L1 files never overlap
// each other) and disjoint from L0's own range (why it wasn't selected) --
// so it can never newly overlap the merged output's range either. That
// means no source anywhere could hold an older, still-relevant value for
// any key this merge touches, satisfying the coverage condition
// CompactLeveled's canDropTombstones requires (CLAUDE.md §8).
//
// The MANIFEST sequence is deliberately ADDED (the new merged file) before
// REMOVED (every input file), each individually fsync'd -- same ordering
// rationale as the original single-level compaction (CLAUDE.md §7/§8): a
// crash in the gap risks only harmless redundancy (both generations live),
// never data loss.
//
// The old input files are only closed/removed after db.drainReaders(),
// run right after publishing the new state -- see inFlightReaders' doc
// comment for why: a concurrent Get/Scan that loaded the *previous* state
// just before this swap might still be reading one of these files, and
// closing it out from under that read would surface a spurious error to
// a caller whose request logically should have succeeded.
func (db *DB) compactL0ToL1IfNeeded(s *dbState) (*dbState, error) {
	if len(s.l0) <= db.l0CompactionThreshold {
		return s, nil
	}

	inputsL0 := append([]*sstable.SSTable(nil), s.l0...) // newest-first
	minKey, maxKey := combinedKeyRange(inputsL0)

	var overlappingL1, remainingL1 []*sstable.SSTable
	for _, st := range s.l1 {
		m := st.Meta()
		if keyRangesOverlap(minKey, maxKey, m.MinKey, m.MaxKey) {
			overlappingL1 = append(overlappingL1, st)
		} else {
			remainingL1 = append(remainingL1, st)
		}
	}

	// L0 (newest-first) then the overlapping L1 files: L1 always holds
	// strictly older data than any live L0 file, so this ordering keeps
	// the same "lower rank = newer" convention every other merge in this
	// codebase uses.
	inputs := append(append([]*sstable.SSTable(nil), inputsL0...), overlappingL1...)

	seq := db.nextSeq
	outPath := sstablePath(db.dir, seq)
	if _, err := compaction.CompactLeveled(inputs, outPath, true); err != nil {
		return s, err
	}

	// Open (and thus fully footer-checksum-verify) the merged output
	// *before* either MANIFEST edit runs -- see the identical reasoning
	// in the original Phase 6 MaybeCompact: once the REMOVED edits below
	// commit, the old inputs' data has no other surviving copy anywhere.
	newTable, err := sstable.OpenSSTable(outPath)
	if err != nil {
		return s, err
	}

	if err := manifest.AppendEdit(db.manifestPath, manifest.VersionEdit{
		Type: manifest.SSTableAdded, File: filepath.Base(outPath), Level: 1,
	}); err != nil {
		newTable.Close()
		return s, err
	}
	for _, in := range inputs {
		if err := manifest.AppendEdit(db.manifestPath, manifest.VersionEdit{
			Type: manifest.SSTableRemoved, File: filepath.Base(in.Meta().Path),
		}); err != nil {
			newTable.Close()
			return s, err
		}
	}

	db.nextSeq = seq + 1
	newL1 := append(append([]*sstable.SSTable(nil), remainingL1...), newTable)
	sortL1ByMinKey(newL1)

	newState := &dbState{mem: s.mem, w: s.w, l0: nil, l1: newL1}
	db.state.Store(newState)

	db.drainReaders()
	if err := closeAndRemoveAll(inputs); err != nil {
		return newState, err
	}
	return newState, nil
}

// compactL1IfNeeded merges every live L1 file into one new L1 file if
// L1's total on-disk size exceeds db.l1SizeRatio times db.flushThreshold;
// otherwise a no-op that returns s unchanged.
//
// canDropTombstones is always true here: this always includes every live
// L1 file (single-level, "merge everything" -- identical in spirit to the
// original Phase 6 compaction, just scoped to L1), and no L0 file can ever
// hold an *older* value than anything in L1 -- L0 only ever holds freshly
// flushed data, which is causally newer than anything already compacted
// into L1. So a tombstone that survives this merge has nothing older left
// anywhere, in L1 or L0, to shadow. See CLAUDE.md §8.
//
// Same drain-before-close discipline as compactL0ToL1IfNeeded, and for
// the same reason.
func (db *DB) compactL1IfNeeded(s *dbState) (*dbState, error) {
	if len(s.l1) <= 1 {
		return s, nil
	}
	total, err := l1TotalSizeBytes(s.l1)
	if err != nil {
		return s, err
	}
	if total <= int64(db.l1SizeRatio)*int64(db.flushThreshold) {
		return s, nil
	}

	inputs := append([]*sstable.SSTable(nil), s.l1...)
	seq := db.nextSeq
	outPath := sstablePath(db.dir, seq)
	if _, err := compaction.CompactLeveled(inputs, outPath, true); err != nil {
		return s, err
	}

	newTable, err := sstable.OpenSSTable(outPath)
	if err != nil {
		return s, err
	}

	if err := manifest.AppendEdit(db.manifestPath, manifest.VersionEdit{
		Type: manifest.SSTableAdded, File: filepath.Base(outPath), Level: 1,
	}); err != nil {
		newTable.Close()
		return s, err
	}
	for _, in := range inputs {
		if err := manifest.AppendEdit(db.manifestPath, manifest.VersionEdit{
			Type: manifest.SSTableRemoved, File: filepath.Base(in.Meta().Path),
		}); err != nil {
			newTable.Close()
			return s, err
		}
	}

	db.nextSeq = seq + 1
	newState := &dbState{mem: s.mem, w: s.w, l0: s.l0, l1: []*sstable.SSTable{newTable}}
	db.state.Store(newState)

	db.drainReaders()
	if err := closeAndRemoveAll(inputs); err != nil {
		return newState, err
	}
	return newState, nil
}

// closeAndRemoveAll closes then deletes every input file, attempting all
// of them even if one fails -- same "attempt all, keep the first error"
// pattern as DB.Close, so one bad file handle can't leak the rest of a
// generation's descriptors or leave later files undeleted. Both MANIFEST
// edits for the compaction that produced inputs are already durable by
// the time this runs, so every input is provably retired from the
// database's authoritative state on this (non-crash) success path --
// this is pure cleanup, not a correctness requirement. Only the
// *post-crash* cleanup of files left behind by an actual mid-compaction
// crash is out of scope (per the Phase 6 brief, unchanged since), and
// callers must have already run db.drainReaders() before calling this
// (see compactL0ToL1IfNeeded/compactL1IfNeeded).
func closeAndRemoveAll(inputs []*sstable.SSTable) error {
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

// l1TotalSizeBytes sums the real on-disk size of every file in l1, via
// os.Stat -- an accurate, cheap-enough measure of "L1's total size" for
// the size-ratio trigger, without needing to track sizes separately from
// what's already durable on disk.
func l1TotalSizeBytes(l1 []*sstable.SSTable) (int64, error) {
	var total int64
	for _, st := range l1 {
		info, err := os.Stat(st.Meta().Path)
		if err != nil {
			return 0, err
		}
		total += info.Size()
	}
	return total, nil
}

// combinedKeyRange returns the union key range [min, max] across tables.
// tables must be non-empty.
func combinedKeyRange(tables []*sstable.SSTable) (minKey, maxKey []byte) {
	for i, st := range tables {
		m := st.Meta()
		if i == 0 || bytes.Compare(m.MinKey, minKey) < 0 {
			minKey = m.MinKey
		}
		if i == 0 || bytes.Compare(m.MaxKey, maxKey) > 0 {
			maxKey = m.MaxKey
		}
	}
	return minKey, maxKey
}

// keyRangesOverlap reports whether closed ranges [aMin, aMax] and
// [bMin, bMax] intersect.
func keyRangesOverlap(aMin, aMax, bMin, bMax []byte) bool {
	return bytes.Compare(aMax, bMin) >= 0 && bytes.Compare(bMax, aMin) >= 0
}

// Get returns the most recent value for key. found is false both when
// the key has never been written and when its newest entry is a
// tombstone -- callers can't distinguish "never written" from "deleted"
// from this signature alone, which is correct: both mean "no value".
//
// Sources are checked newest-to-oldest: memtable, then every live L0
// file (newest first -- L0 files may overlap in key range, so all must be
// checked until a hit), then at most one L1 file, located by binary
// search over L1's non-overlapping key ranges (findL1TableForKey) rather
// than a linear scan.
//
// Lock-free (Phase 8c): loads the current state once and operates only
// on that snapshot for the entire call, so it never blocks on writeMu and
// is safe to call concurrently with Put/Delete/Scan/another Get.
func (db *DB) Get(key []byte) (value []byte, found bool, err error) {
	db.beginRead()
	defer db.endRead()
	s := db.state.Load()

	if v, found, tombstone := s.mem.Get(key); found {
		if tombstone {
			return nil, false, nil
		}
		return v, true, nil
	}

	for _, st := range s.l0 {
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

	if st := findL1TableForKey(s.l1, key); st != nil {
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

// findL1TableForKey returns the single SSTable in l1 whose key range
// could contain key, or nil if none does. l1 must be non-overlapping and
// sorted ascending by MinKey (maintained by every L1-producing
// compaction, verified at Open by sortL1ByMinKey's ordering and the
// compaction-time overlap checks), so binary-searching for the last file
// whose MinKey <= key, then confirming key <= that file's MaxKey, is
// sufficient -- never a linear scan of every L1 file.
func findL1TableForKey(l1 []*sstable.SSTable, key []byte) *sstable.SSTable {
	i := sort.Search(len(l1), func(i int) bool {
		return bytes.Compare(l1[i].Meta().MinKey, key) > 0
	})
	if i == 0 {
		return nil
	}
	cand := l1[i-1]
	m := cand.Meta()
	if bytes.Compare(key, m.MinKey) >= 0 && bytes.Compare(key, m.MaxKey) <= 0 {
		return cand
	}
	return nil
}

// Close closes the WAL writer and every open SSTable file handle, across
// both levels. It attempts to close all of them even if one fails, so a
// single bad file handle can't leak the rest; the first error encountered
// is returned.
//
// Close must not be called concurrently with any other in-flight DB
// method (Phase 8c deliberately does not implement graceful-shutdown
// draining for it -- see CLAUDE.md §11's Phase 8c entry): callers must
// ensure every other call has already returned before calling Close.
func (db *DB) Close() error {
	s := db.state.Load()
	var firstErr error
	for _, st := range append(append([]*sstable.SSTable(nil), s.l0...), s.l1...) {
		if err := st.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if err := s.w.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}
