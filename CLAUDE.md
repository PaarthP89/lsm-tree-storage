# CLAUDE.md — Embedded LSM-Tree Storage Engine

This file is the source of truth for this project. Read it at the start of
every session before writing code. Sections marked **LOCKED** are settled
design decisions — do not change them without explicitly flagging the
change and updating this file. The **Status** section near the bottom is
the only part that should change frequently; update it at the end of every
session.

---

## 1. What this is

A standalone, embedded key-value database engine — the actual storage
layer, built from first principles, not a wrapper around Postgres/Redis/
SQLite. "Embedded" means a library: the application links against it
directly and calls `Put`/`Get`/`Delete` in-process, the way SQLite,
RocksDB, or BoltDB work.

Think of it as a tiny, from-scratch version of RocksDB's storage engine.

**Language:** Go, standard library first — only reach for a dependency if
a phase brief explicitly calls for one.

---

## 2. Core guarantees (what this engine must actually do)

- **Durability** — once a write returns success, it survives a crash
  (process kill, power loss simulated via `kill -9`), because it was
  fsync'd to disk before acknowledging.
- **Fast writes** — writes are always sequential appends, never
  random-access disk writes.
- **Correct reads** — `Get(key)` returns the most recent value, checking
  in-memory data first, then progressively older on-disk files, and
  correctly handles tombstones so deleted data doesn't reappear.
- **Crash recovery** — on restart, the engine reconstructs its exact
  pre-crash state by replaying logs, with zero data loss for any
  acknowledged write.
- **Bounded storage growth** — background compaction merges and cleans up
  on-disk files so storage doesn't grow unboundedly with overwrites/
  deletes.

---

## 3. Architecture decisions — LOCKED

| Decision | Choice | Why |
|---|---|---|
| Memtable structure | Skip list | Standard in real LSM engines (LevelDB, RocksDB, Badger). Simpler concurrent-access story than a self-balancing BST — probabilistic balancing, not rebalance-on-write. |
| Checksum algorithm | CRC32 | Fast, standard for detecting torn writes / bit rot. Not cryptographic — not needed here. |
| Crash-safe file-set tracking | MANIFEST + CURRENT file (LevelDB/RocksDB pattern) | Atomic rename alone protects a single file, not the question "which SSTables are currently part of the database?" after a crash. Without this, recovery would have to scan-and-guess. |
| Level structure | Two levels: L0 (freshly-flushed, may overlap) and L1 (compacted, non-overlapping, sized ~10x the L0 flush-size target) | Demonstrates k-way merge, tombstone GC, and atomic rename (same as the original single-level design) plus the two real problems a flat scheme can't: bounding how many files a read must check (L0 still requires checking every file, but L1 doesn't) and giving compaction an amortization lever (L0->L1 stays cheap and frequent; L1->L1 is rarer and bulk). A third+ level (L2, L3, ...) is deliberately out of scope — exactly two levels is the full stretch-tier ask (§9), not a general N-level scheme. |

---

## 4. On-disk layout — LOCKED

```
data/
  CURRENT              # single line: name of the active MANIFEST file
  MANIFEST-000001      # append-only log of SSTable set changes
  wal/
    000001.log         # current + rotated WAL segments
  sstables/
    000002.sst
    000003.sst
    ...
```

---

## 5. Wire formats — LOCKED

All three append-only formats (WAL, MANIFEST) share one discipline:
checksum first, then payload; on replay, a bad checksum or a truncated
record means **stop replay there**, return everything decoded so far, no
error. Never skip-and-continue past a bad record — a torn tail means that
write was never fully fsync'd, so its absence on recovery is correct, not
data loss.

### WAL entry
```
[checksum: 4B CRC32][op_type: 1B][key_len: 4B][key][value_len: 4B][value]
```
CRC32 covers everything after the checksum field itself.

### SSTable file
Sorted sequence of key/value entries (same record encoding as the WAL),
followed by a footer containing:
- Sparse index (key → byte offset) — reads binary-search this, then scan a
  small range on disk, never a full-file scan
- Metadata: min/max key, entry count
- Bloom filter — **stretch goal (8a)**, not in Minimum/Target tier

### MANIFEST entry
```
[checksum: 4B CRC32][edit_type: 1B][name_len: 4B][name][level: 4B, only present for edit_type in {3,4}]
```
`edit_type` is one of four values, which double as a wire-format version
marker: `1` = `SSTABLE_ADDED` (legacy, pre-8d, no level field — always
means level 0), `2` = `SSTABLE_REMOVED` (legacy, same), `3` =
`SSTABLE_ADDED` with an explicit trailing level field, `4` =
`SSTABLE_REMOVED` with an explicit trailing level field. Every edit
written by 8d-or-later code uses `3`/`4`; `1`/`2` only ever appear in a
MANIFEST written before this field existed. This lets a single MANIFEST
mix pre- and post-8d records (the normal shape of a live database's
MANIFEST right after upgrading) and have each one decode correctly on
its own, with no file-wide version flag — the per-record `edit_type`
byte itself is the version signal. Payload is the SSTable filename
(length-prefixed), as before.

### CURRENT file
Single line containing the name of the active MANIFEST file. Written via
temp-file + fsync + atomic rename, same as everything else on-disk here.

---

## 6. Interfaces — LOCKED

```go
type Memtable interface {
    Put(key, value []byte)
    Delete(key []byte)                          // writes a tombstone
    Get(key []byte) (value []byte, found bool, tombstone bool)
    Iterator() Iterator                          // sorted, for flush-to-SSTable
    SizeBytes() int
}
```

```go
type DB struct { /* wal.Writer + Memtable + SSTable set + MANIFEST */ }

func Open(dir string) (*DB, error)
func (db *DB) Put(key, value []byte) error
func (db *DB) Delete(key []byte) error
func (db *DB) Get(key []byte) (value []byte, found bool, err error)
func (db *DB) Close() error
```

---

## 7. Algorithms — LOCKED

### Write path
1. Serialize `(key, value, op_type, checksum)`, append to WAL, fsync
   before returning success.
2. Insert into the memtable (skip list).
3. When the memtable exceeds a size threshold: flush to a new SSTable
   (temp file → fsync → atomic rename), append `SSTABLE_ADDED` to the
   MANIFEST (fsync), start a fresh empty memtable + WAL segment.

### Read path
1. Check the current memtable first.
2. If not found, check every live L0 file, newest-to-oldest (L0 files may
   overlap in key range, so all must be checked until a hit). If still
   not found, locate at most one L1 file whose key range could contain
   the key — binary search over L1's non-overlapping ranges, sorted
   ascending by min key — and check only that one file, never a linear
   scan of L1.
3. Each SSTable's sparse index is binary-searched, then a small on-disk
   range is scanned.

### Compaction (two-level, Target+8d tier)
Two independent triggers, neither one ever consuming the other's inputs:

1. **L0->L1** fires when the live L0 file count exceeds a threshold
   (`SetL0CompactionThreshold`, default 4 — the same value and meaning
   the old single-level threshold had). Inputs: every live L0 file, plus
   every live L1 file whose key range overlaps the L0 files' combined
   range (found by a single-pass check against that combined range — L1's
   own non-overlap invariant guarantees no L1 file *outside* that
   first-pass check could newly overlap the merge's eventual output
   either, so no iterative re-check is needed). K-way merge (all inputs
   sorted, newest-to-oldest recency: L0 newest-first, then the
   overlapping L1 files, which are always older than any live L0 file).
   Output: one new, non-overlapping L1 file.
2. **L1->L1** fires when L1's total on-disk size exceeds a ratio
   (`SetL1SizeRatio`, default 10) times the L0 flush-size target — reusing
   the 10x figure §3 already named as the multi-level rationale, not a
   new number. Inputs: every live L1 file (single-level, "merge
   everything," exactly like the original Target-tier scheme, just
   scoped to L1). Output: one new L1 file.
3. During either merge: keep only the newest value per key (unchanged).
4. **Tombstone GC, generalized**: dropping a tombstone that's still the
   newest entry for its key at the end of a merge is safe *only* when
   every source that could hold an older, still-relevant value for that
   key was included in the merge — expressed as an explicit
   `canDropTombstones` boolean the caller must prove true, never inferred
   by the merge itself. For L0->L1, this holds because step 1 always
   includes every live L0 file and every L1 file whose range could
   overlap the output. For L1->L1, this holds because step 2 always
   includes every live L1 file, and no L0 file can hold data *older* than
   anything already in L1 (L0 only ever holds freshly flushed, causally
   newer data). The original single-level invariant ("compaction replaces
   every current SSTable in one pass") is the special case of this rule
   where there is only one level to begin with.
5. Write result to a temp file, fsync, atomically rename into place
   (unchanged).
6. Append MANIFEST edits as a single durable operation, **`SSTABLE_ADDED`
   (new file, tagged with its level) before `SSTABLE_REMOVED` (old
   files)** — same ordering rule as before, applied per-compaction
   regardless of which level(s) it touches. A crash between the two
   leaves both generations live — redundant, never data loss.

### Crash recovery (on startup)
1. Read CURRENT to find the active MANIFEST.
2. Replay the MANIFEST to reconstruct the current SSTable set. Any file on
   disk not in this reconstructed set is ignored (orphaned, not deleted).
3. Replay the WAL (torn-write rule applies) to rebuild the memtable to its
   exact pre-crash state.

This order — CURRENT → MANIFEST → SSTable set → WAL → memtable — is fixed.
WAL replay rebuilds state *on top of* the already-settled SSTable set, so
the SSTable set must be resolved first.

---

## 8. Non-negotiable invariants (running list)

- fsync before any operation is considered acknowledged — WAL append,
  MANIFEST append, CURRENT write, SSTable rename.
- Torn/corrupt tail on replay → stop, don't skip-and-continue. Applies to
  both WAL and MANIFEST replay identically.
- Sequential append only, never seek-and-overwrite, for WAL and MANIFEST.
- Atomic rename (temp file → fsync → rename) for every file that lands at
  a final on-disk path: SSTables (flush and compaction output), CURRENT.
- Fsync the containing directory after any rename or fresh-file-create
  that lands a file at a permanent path (WAL segment creation, SSTable
  rename, CURRENT rename) — on some filesystems a crash between the
  rename/create and the directory entry itself being durably persisted
  can make that change not survive the crash. Found in Phase 3 for
  SSTable flush and WAL segment creation (`fsyncDir` in both packages).
- MANIFEST edit ordering during compaction: ADDED before REMOVED, always.
- Tombstone-drop-during-compaction is only valid when the merge's inputs
  provably cover every source that could hold an older, still-relevant
  value for any key the merge touches — expressed as an explicit
  `canDropTombstones` boolean passed into the merge, never inferred from
  context. Under the original single-level scheme this coverage was
  automatic (every live SSTable was always an input). Under two-level
  compaction (8d), it's proven per compaction: L0->L1 by always including
  every L0 file and every overlapping L1 file; L1->L1 by always including
  every L1 file and relying on L0 only ever holding causally newer data
  than L1. **Never drop a tombstone unless this is proven true for the
  specific merge at hand** — comment the proof at the point it's
  implemented, per this bullet's own original convention. Getting this
  wrong is a silent data-resurrection bug: a stale value outside the
  merge's inputs would incorrectly become visible again once the
  tombstone that shadowed it is gone.
- Newest-wins for any key present in multiple sources (memtable > newer
  SSTable > older SSTable).
- Any single long-lived file that's reopened and appended to again across
  process restarts (as opposed to a WAL segment, which is always abandoned
  in favor of a brand-new segment after a crash — see `wal.NextSegmentPath`)
  must repair a pre-existing torn tail *before* the first new append,
  or a later, fully-valid, fully-fsynced record can land at a
  byte-misaligned offset and become permanently unreadable, since replay
  never looks past the first torn record it finds. Found and fixed for
  the MANIFEST in Phase 4 (`manifest.RepairTornTail`, called once at
  `Open`, before any future `AppendEdit`) — a real gap, structurally
  identical to the WAL's Phase-1 `ErrTornSegment` bug, that was missed on
  first implementation and only found on a dedicated review pass. Revisit
  this same question for any future single-long-lived-append-log file
  (e.g. if a later phase adds another one).

---

## 9. Scope tiers

| Tier | Includes |
|---|---|
| **Minimum** (must finish) | WAL, MANIFEST + CURRENT, memtable (skip list), SSTable flush, basic Get/Put/Delete, crash recovery |
| **Target** | + single-level compaction (k-way merge, tombstone GC, atomic rename + MANIFEST update), + benchmarks with real numbers, + chaos test with proof |
| **Stretch** | + bloom filters (8a), + range queries (8b), + concurrent readers/writers (8c), + multi-level L0/L1 compaction (8d) |

---

## 10. Build plan

Each phase = one session, with its own detailed architect brief. Don't
start a phase whose prior dependency isn't checked off below.

1. **Durable Log** — WAL writer/reader, torn-write handling, rotation
2. **In-Memory Engine** — skip-list memtable, wired to WAL, in-memory Get/Put/Delete
3. **Persistence** — SSTable flush + unified read path (memtable → SSTables)
4. **Crash-Safe Metadata** — MANIFEST + CURRENT + full recovery sequence (completes Minimum tier)
5. **Chaos Test** — kill -9 harness + written proof (validates Minimum tier)
6. **Compaction** — k-way merge, tombstone GC, atomic rename, MANIFEST update (Target tier)
7. **Benchmarks + wrap-up** — throughput numbers, extended chaos run including compaction, README

Stretch phases (8a–8d) are independent and unordered; pick based on what's
useful once Target tier is done.

---

## 11. Status — UPDATE THIS EVERY SESSION

**Current phase:** None — Minimum + Target + full Stretch tier (8a-8d) are
all done, plus a Phase 9 demo. Remaining work is open-ended
hardening/polish, user's choice.
**Last completed phase:** Phase 9 — `cmd/demo`, a narrated end-to-end
walkthrough (`go run ./cmd/demo`) exercising every implemented feature in
one runnable program: basic Put/Get/Delete + tombstones, a
threshold-triggered flush, a range Scan, L0->L1 compaction (including the
automatic in-line trigger, not just the on-demand `MaybeCompact` escape
hatch), concurrent readers/writers, a bloom-filter-served miss, and a real
subprocess `kill -9` crash-recovery cycle. Also: promoted the 8d session's
pending §3/§5/§7/§8 LOCKED-section proposal into this file after verifying
it against the actual `db.go`/`compaction/compact.go`/`manifest/edit.go`
code (not just the prose describing it); reviewed and accepted Phase 8c's
`writeMu` deviation on the same basis; flipped 8c/8d to "Done"; re-ran
`go build`, `go vet`, `go test ./...` (417s), and `go test -race ./...`
(542s) fully clean, confirming the "pending human review" state really
was safe to close out. `README.md`'s Architecture/"What's built" sections,
which still described the pre-8a-8d single-level design, were also
brought up to date.
**One real bug found and fixed while building the demo, not in the
engine itself:** the demo's crash-recovery section initially killed the
`cmd/lsmload` subprocess after a fixed 75ms sleep, which reliably produced
zero recovered keys (the WAL directory hadn't even been created yet) —
process startup and per-write `F_FULLFSYNC` latency in this environment
exceed that window. Fixed by widening the sleep to 500ms, which reliably
lands the kill mid-burst instead of before the burst starts. This was a
timing bug in the demo harness, not a durability bug in `wal`/`db.go`.

**Follow-up session: closed both gaps left open after Phase 9.**
`sstable.SSTable` gained reference counting (`AddRef` + a refcounted
`Close`, starting at 1 for the implicit reference `OpenSSTable`'s caller
holds) so a long-lived `Scan` iterator's source files stay safely
readable — via the ordinary POSIX unlink-while-open behavior every
`sstable` read already relied on — even after a later compaction retires
and unlinks them; `DB.Scan` now returns a new `ScanIterator`
(`memtable.Iterator` plus `Close`, auto-called on natural exhaustion,
otherwise the caller's job for an abandoned iterator) instead of a bare
`memtable.Iterator`. Separately, `Put`/`Delete` now wrap any error from a
triggered flush in a new exported `*HousekeepingError`
(`errors.As`-compatible), so a caller can tell "the write itself never
happened" apart from "the write is durable, but something afterward
failed" — previously indistinguishable. Both changes are additive (no
LOCKED-section text touched) and covered by new dedicated tests
(`scan_lifetime_test.go`, `housekeeping_test.go`) plus a full clean re-run
of `go build`, `go vet`, `go test ./...`, and `go test -race ./...`. See
the "Known deliberate gaps" section above (now both marked resolved) and
the Deviations entries for `DB.Scan`/`ScanIterator` for the full design.

Full narrative history (review-pass-by-review-pass findings, exact test
counts, timings) lives in git history, not here — this table keeps only
what a future session needs and can't just re-derive from the code.

| Phase | Status | Notes |
|---|---|---|
| 1 — Durable Log | Done | `wal` package: `Entry` encode/decode, `Writer.Append` (fsync before ack), `Replay` (stops clean at torn/corrupt record, no error), segment rotation, `NextSegmentPath` for post-crash resume. Reopening a torn segment for append is rejected (`ErrTornSegment`) — always resume via `NextSegmentPath`, never reopen the last segment named after a replay. |
| 2 — In-Memory Engine | Done | `memtable.SkipList` (p=0.25, single `sync.RWMutex`): Put/Delete/Get/Iterator/SizeBytes. `DB.Open` replays the WAL into a fresh memtable, then always opens a *new* segment. `cmd/lsmload` + a real-subprocess-SIGKILL test harness (`killrestart_test.go`) validate recovery. |
| 3 — Persistence | Done | `sstable` package: `FlushMemtable`/`OpenSSTable`/`Get` (sparse index binary search + bounded on-disk scan, never a full-file scan). `DB` wires size-triggered flush; `Get` falls through memtable → SSTables newest-first. `SizeBytes()` accounts for skip-list node/pointer overhead, not just raw key/value bytes, so flush actually triggers at the configured threshold. Obsolete WAL segments are deleted right after each flush (`wal.RemoveSegmentsBefore`) so restart cost stays bounded by activity since the last flush, not the DB's whole history. Orphaned `*.sst.tmp` from a crash mid-flush is cleaned up at `Open`. |
| 4 — Crash-Safe Metadata | Done | `manifest` package (`VersionEdit`, `AppendEdit`, `ReplayManifest`, `ReconstructSSTableSet`) + `CURRENT` read/write, same temp-file→fsync→rename discipline as elsewhere. `DB.Open` follows the locked CURRENT→MANIFEST→SSTables→WAL→memtable order. A MANIFEST-listed-but-missing SSTable fails loudly at `Open` rather than being silently dropped. MANIFEST torn-tail repair happens before any append (`manifest.RepairTornTail` — this is the §8 long-lived-file invariant). |
| 5 — Chaos Test | Done | `cmd/chaosworker` + `cmd/chaos`: real-subprocess SIGKILL harness, ACK-line-on-stdout is the sole ground truth for "durably acknowledged." Verifies the full key range past the ACK boundary, not just up to it. 20/20 real runs clean, 0 lost/corrupt — see `docs/chaos-report.md` (includes the check confirming Go's Darwin `fsync` issues `F_FULLFSYNC`, so it's real durability, not a platform gap). `SkipList.Get` returns a copy of the value, not an alias into internal storage. |
| 6 — Compaction | Done | New `compaction` package: `MergeIterator` (heap-based k-way merge, newest source wins ties) + a tombstone-dropping wrapper (safe only because compaction is single-level — see §8). `DB.MaybeCompact` triggers synchronously once live SSTable count exceeds 4; MANIFEST edits appended ADDED-before-REMOVED; old inputs closed/deleted on success. The new compacted output is opened and verified *before* the MANIFEST edits retiring its inputs are committed — verifying after would risk real, permanent data loss if the just-written file were corrupt (its inputs' WAL backing is long gone by compaction time, unlike a flush). |
| 7 — Benchmarks + wrap-up | Done | `bench_test.go`: `BenchmarkWrite` (369 ops/sec, fsync-bound by design), `BenchmarkReadHot` (458k ops/sec, pure memtable), `BenchmarkReadCold` (58.7k ops/sec, 13 uncompacted SSTables), `BenchmarkReadAfterCompaction` (76.6k ops/sec, 1 SSTable) — real numbers from `go test -bench=. -benchtime=2000x`, see README. Extended `cmd/chaos` run (25 iterations, `-compactionthreshold=3`, 40,460 acked writes, 0 lost/corrupt) puts compaction's crash windows inside a real randomized `SIGKILL`, not just Phase 6's four hand-injected unit tests — confirmed by inspecting a kept iteration's MANIFEST (98 ADDED/96 REMOVED edits from one burst). Also re-verified, by reading the actual Go 1.26.5 toolchain source (`internal/poll/fd_fsync_darwin.go`), that the Phase 5 F_FULLFSYNC claim still holds on this toolchain. No correctness bugs found by this phase's benchmarking or extended chaos work. |
| 8a — Bloom filters | Done | New self-describing footer section (`sstable`), FNV-1a/Kirsch-Mitzenmacher filter built via a buffered-keys pass in `FlushIterator` (flush and compaction both, no special-casing), consulted in `Get` to skip the sparse-index/scan path on a "definitely absent" result. ~20x fewer ns/op on a 100%-miss workload (263 vs 5236 ns/op, Apple M2). Backward-compat with pre-8a files verified against a hand-built pre-8a-format file. Zero false negatives (50k keys), 1.04% observed FPR vs 1% target. See the Deviations/additions entry below for the full footer layout and design rationale. |
| 8b — Range queries | Done | `DB.Scan(start, end)` half-open `[start, end)`, newest-wins, tombstone-filtered, built on `compaction.MergeIterator` (unmodified) + new O(log n) seek support in `memtable.SkipList` and bounded-scan seek support in `sstable.SSTable`. Concurrent-mutation-during-Scan left explicitly undefined, deferred to 8c. See the Deviations/additions entry below for the full design. |
| 8c — Concurrent readers/writers | Done | `DB`'s mutable state (memtable, WAL writer, L0/L1 SSTable lists) moved into an immutable `dbState` published via `atomic.Pointer[dbState]`; `Get`/`Scan` are fully lock-free (load once, operate on that snapshot). Writes (`Put`/`Delete`, and the flush/compaction either triggers) are serialized by a `writeMu` instead of the brief's suggested bare CAS "elected flusher" guard — deliberate deviation, justified below and in the Deviations list, because a lock-free design isn't actually safe against `memtable.SkipList.Iterator`'s documented "not synchronized with concurrent writes" contract. A real bug *was* found and fixed in-scope while verifying that contract under `-race`: `skipListIterator.Next` walked raw forward pointers with no locking at all, a genuine data race against a concurrent `Put`, now fixed by taking the list's own `RLock` per `Next()` call (see Deviations). `go test -race ./...` clean, including 5 new dedicated concurrency tests (`concurrency_test.go`): no data races, no lost/duplicated writes under concurrent flush churn, correctness + L1 non-overlap invariant under concurrent compaction, and zero read errors from a hammering reader during heavy concurrent compaction (the direct test of the reader-drain-before-close mechanism). `Close` remains explicitly unsafe to call concurrently with any other in-flight call (no draining implemented, per scope). |
| 8d — Multi-level compaction | Done | Two-level (L0/L1) compaction: `manifest.VersionEdit` gains a self-describing `Level` field (new wire-type markers, pre-8d records always decode as L0 — see `ReconstructLeveledSSTableSet`); `DB` splits `sstables` into `l0`/`l1`; `compaction.CompactLeveled` takes an explicit `canDropTombstones` bool instead of always dropping; `DB.compactL0ToL1IfNeeded`/`compactL1IfNeeded` are the two independent triggers (`SetL0CompactionThreshold`, `SetL1SizeRatio`); `Get` checks all L0 newest-to-oldest then binary-searches L1 by range. Verified: adversarial tombstone-drop tests (`compaction/leveled_test.go`), L1 non-overlap invariant under repeated compaction (`leveled_compaction_test.go`), and 12/12 real-subprocess-`SIGKILL` iterations mid-L0→L1-merge (`l0l1_crash_subprocess_test.go`), 0 lost/corrupt. **§3/§5/§7/§8's proposed LOCKED-section revisions were reviewed and promoted directly into those sections this session; there is no longer a pending-review block.** |

**Known deliberate gaps at current state:**

- **~~`DB` is not safe for concurrent use.~~ Resolved by Phase 8c.**
  `maybeFlush`/`compactL0ToL1IfNeeded`/`compactL1IfNeeded` no longer
  reassign fields in place; they build a new `dbState` (memtable, WAL
  writer, L0/L1 lists) and publish it via `db.state.Store`, an
  `atomic.Pointer[dbState]`. `Get`/`Scan` are lock-free (load the current
  state once per call); `Put`/`Delete` (and the flush/compaction they can
  trigger) are serialized against each other by a new `writeMu`. See the
  8c row above and the Deviations entry below for the full design,
  including why a bare lock-free CAS guard (the more obvious design) was
  rejected as unsafe for this codebase's specific memtable implementation.
- **~~A long-lived `Scan` iterator could see a read error if a later
  compaction retired one of its source SSTables.~~ Resolved.**
  `sstable.SSTable` now reference-counts its underlying open file handle
  (`AddRef`, and a `Close` that only actually closes the file once every
  reference — the implicit one from `OpenSSTable` plus every `AddRef` —
  has been released). `DB.Scan` calls `AddRef` on every SSTable source it
  reads from while constructing the iterator (inside the same
  `beginRead`/`endRead` bracket that already protected the *construction*
  step), and returns a new `ScanIterator` (`memtable.Iterator` plus
  `Close`) instead of a bare `memtable.Iterator`. A compaction that
  retires one of those files still unlinks it immediately (unchanged), but
  the file descriptor a long-lived iterator is still reading through stays
  open and valid — standard POSIX unlink-while-open semantics, safe here
  because every read in `sstable` already goes through the one file
  handle opened at `OpenSSTable` time, never by reopening the path. A
  fully-drained iterator (`Next` returns false) auto-releases its
  references; an abandoned iterator needs an explicit `Close` to avoid
  leaking an open file descriptor. See the Deviations entry below and
  `scan_lifetime_test.go` (`TestScanIteratorSurvivesCompactionOfItsSources`,
  which snapshots an in-progress iterator's source file paths, forces a
  compaction that unlinks every one of them, and confirms the iterator
  still reads every remaining entry correctly) for the direct proof.
- **~~A `Put`/`Delete` that durably succeeds could return an error
  indistinguishable from one where the write itself failed.~~ Resolved.**
  Any error `maybeFlush` returns after a `write` call's WAL append and
  memtable mutation have already succeeded is now wrapped in a new
  exported `*HousekeepingError` (`Unwrap`-compatible, so `errors.As`
  extracts the underlying cause). A caller can now tell "the write itself
  never happened" apart from "the write is durable, but something
  afterward — flushing, WAL segment rotation, obsolete-segment cleanup —
  failed," which matters in particular for deciding whether a retry would
  duplicate a write. See `housekeeping_test.go`
  (`TestPutReturnsHousekeepingErrorWhenFlushFails`, which forces a flush
  failure via a read-only SSTable directory and confirms both the
  `*HousekeepingError` and that the write survives a full restart) for the
  direct proof.

**Deviations/additions beyond the LOCKED interfaces** (all additive —
nothing in §3–§7 has changed):

- `wal`: `NextSegmentPath`, exported `EncodeEntry`/`DecodeEntry` (so
  `sstable` reuses the same wire-format code instead of reimplementing
  it), `SegmentNames`/`ParseSegmentSeq`, `RemoveSegmentsBefore`.
- `sstable`: `Iterator` (full-table sequential scan, distinct from
  `Get`'s bounded scan) and `FlushIterator` (the shared write path behind
  both `FlushMemtable` and compaction's output). Treats a corrupt record
  as a hard error, not §5's replay-stop rule — an SSTable is only ever
  made visible via a fsync'd atomic rename, so mid-file corruption there
  can only mean real bit rot, which should fail loudly.
- `manifest`: whole package, plus `RepairTornTail`. Deliberately
  duplicates the checksum/torn-tail-stop logic rather than sharing a
  generic helper with `wal` — the two payloads differ enough that a
  shared abstraction would buy little.
- `compaction`: whole package, kept separate from `sstable` (mirrors the
  existing `manifest`-separate-from-`sstable` precedent).
- `DB.MaybeCompact() error`: new public method beyond §6's listed `DB`
  signatures, needed to make the LOCKED §7 compaction algorithm reachable.
- `DB.SetCompactionThreshold(int)`: new public method (Phase 7), mirrors
  `SetFlushThreshold`. The live-SSTable-count compaction trigger was
  previously a hardcoded package constant with no override, which meant
  nothing (test or harness) could force compaction to fire on a
  realistic-but-small dataset. Needed so `cmd/chaos`/`cmd/chaosworker`
  could put compaction inside a randomized real-`SIGKILL` crash window
  (`-compactionthreshold` flag on both).
- `cmd/lsmload`, `cmd/chaosworker`, `cmd/chaos`, `internal/chaosdata`:
  test/harness-only binaries, not part of the library's public interface.
- `sstable`: Phase 8a bloom filters. New footer section, appended after
  the existing `entry_count` field (all bytes before it are byte-for-byte
  unchanged from Phase 3/4):
  `[has_bloom: 1B]`, and if 1: `[k: 4B][bits_len: 4B][bits: bits_len bytes]`.
  Backward compatibility works by exhaustion, not a version marker: a
  pre-8a footer body ends right after `entry_count` with zero bytes left
  in the reader, and `decodeFooter` treats "nothing left to read" and
  "has_bloom explicitly 0" identically (no filter, `Get` always falls
  through to the full sparse-index lookup). Sizing (`newBloomFilter`):
  standard `m = ceil(-n·ln(fpr)/ln(2)²)` bits, `k = round((m/n)·ln2)` hash
  functions, target fpr = 1%. Hashing: FNV-1a 64-bit, hand-inlined rather
  than routed through `hash/fnv`'s `hash.Hash64` interface — that
  allocates a hasher per call, and an early version of the miss-heavy
  benchmark below showed that allocation alone made the filter path
  slower than the scan it exists to skip. Probe positions:
  `(h1 + i·h2) mod (len(bits)·8)` for `i` in `[0, k)` (Kirsch-Mitzenmacher
  double hashing), where h1/h2 are the two 32-bit halves of the FNV-1a
  sum. Entry-count-before-sizing problem: resolved with a buffered-keys
  pass, not a pre-count pass or a fully-buffered-entries pass — during
  `FlushIterator`'s existing single streaming pass, every key (not value)
  is cloned into a slice as it's written, then the filter is built from
  that slice once the final count is known. Chosen over a pre-count pass
  because `entryIterator` sources (a live memtable iterator, compaction's
  merge iterator over open file handles) aren't cheaply replayable for a
  separate counting pass; chosen over buffering full entries because
  values dominate memory cost and aren't needed for filter construction.
  `n = 0` (empty table): `newBloomFilter` returns nil, no filter is
  written, identical on-disk shape to a pre-8a empty table. Both flush
  and compaction output get a filter automatically and identically, since
  both go through the same `FlushIterator` — no special-casing needed.
  `SSTable.HasBloomFilter() bool`: new public introspection method
  (test/diagnostic use only, not part of the read path). Real numbers
  (Apple M2, `go test ./sstable/... -bench=BenchmarkGetMiss -benchtime=5000x -benchmem`,
  keys placed so misses genuinely fall inside `[minKey, maxKey]` and reach
  the scan path rather than being rejected by the pre-existing range
  check): `BenchmarkGetMissWithBloomFilter` 263 ns/op, 5 allocs/op vs
  `BenchmarkGetMissWithoutBloomFilter` 5236 ns/op, 241 allocs/op — ~20x
  fewer ns/op on a 100%-miss workload against a 20,000-entry table.
  Observed false-positive rate in `TestBloomFilterFalsePositiveRate`:
  1.04% against a 1% target (100,000 trials). Zero false negatives
  verified across 50,000 keys in `TestBloomFilterZeroFalseNegatives`.
  Backward compatibility verified in
  `TestOpenPreBloomSSTableIsBackwardCompatible` by hand-constructing a
  real pre-8a-format file byte-for-byte and confirming it opens and reads
  correctly with no filter. Compaction-output propagation verified in
  `compaction.TestCompactOutputHasBloomFilter`.
- `DB.Scan(start, end []byte) (ScanIterator, error)` (Phase 8b; return
  type changed from `memtable.Iterator` to the new `ScanIterator` when the
  file-lifetime gap below was closed): new public method beyond §6's
  listed `DB` signatures. Half-open `[start, end)` range convention:
  `start` included, `end` excluded; `start == end` is a valid call that
  returns an iterator done on the first `Next()`, `start > end` returns
  the new exported `ErrInvalidRange` without constructing anything.
  Sorted, deduplicated, tombstone-filtered across the memtable and every
  live SSTable, newest-wins on overlap — built by reusing
  `compaction.MergeIterator` unmodified (one `compaction.Source` per
  source, ranked exactly like `Get` already ranks them: memtable rank 0,
  then `db.l0` in its existing newest-first order, then every `db.l1`
  file whose range could overlap `[start, end)` — updated by Phase 8d to
  source from the split `l0`/`l1` fields instead of the original flat
  `db.sstables`, with no change to `Scan`'s own
  half-open-range/tombstone-filtering logic), rather than reimplementing
  k-way merge/newest-wins resolution a second time. Implemented by an
  unexported `rangeIterator` (in `scan.go`) that wraps `MergeIterator` to
  enforce the `end` bound (which `MergeIterator` has no concept of) and to
  drop tombstones (a tombstoned key must never be yielded at all, since
  `Iterator`'s 3-method shape has no found/tombstone pair the way `Get`
  does). `rangeIterator.Key()`/`Value()` return clones, not aliases into a
  source's internal buffer, matching the existing defensive-copy
  convention (`SkipList.Get`).
- `ScanIterator` interface (`memtable.Iterator` plus `Close() error`) and
  `sstable.SSTable.AddRef()`/refcounted `Close()`: close the file-lifetime
  gap noted below Phase 8b's original entry (see "Known deliberate gaps"
  above for the full design and the direct test proving it). `Scan`
  `AddRef`s every SSTable source while building the merge (inside the same
  `beginRead`/`endRead` bracket that already protects snapshot
  construction), and `rangeIterator` releases them via `Close` — called
  automatically once `Next` is naturally exhausted, or explicitly by the
  caller for an iterator abandoned before that point. `sstable.SSTable`
  itself now carries an `atomic.Int32` reference count (starting at 1 for
  the implicit reference `OpenSSTable`'s caller holds); its file handle is
  only actually closed once every reference has been released, relying on
  ordinary POSIX unlink-while-open semantics for the already-unlinked case
  (every `sstable` read goes through the one file handle opened at
  `OpenSSTable` time, never by reopening the path, so this was already a
  safe assumption to lean on).
- `memtable.SkipList.SeekIterator(start []byte) Iterator`: new method,
  lands on the first key >= start via the same O(log n) multi-level
  descent `Get`/`insert` already use, instead of iterating from the head
  and discarding entries before `start`.
- `sstable.SSTable.SeekIterator(start []byte) (*Iterator, error)`: new
  method, binary-searches the existing sparse index for the block that
  could contain the first key >= start (identical search to `Get`'s),
  then linear-scans forward within that one block (bounded by
  `indexInterval`, never a full-file scan) to find the exact byte offset
  to start the returned `Iterator` from. Does not take an upper bound —
  deciding how far to actually read is the caller's job (`DB.Scan`'s
  `rangeIterator` stops pulling once it reaches `end`), since an
  unconsulted tail of the table costs nothing until `Next()` is actually
  called on it.
- `sstable.SSTable.HasBloomFilter`-style addition:
  `SSTable`/`SkipList`'s new `SeekIterator` methods reuse
  `compaction.SortedIterator`'s exact method shape (`Next`/`Key`/`Value`/
  `Tombstone`) structurally, so no new interface or adapter type was
  needed to feed them into `compaction.Source`/`NewMergeIterator`.
- **Known limitation, deliberately not fixed this phase (Phase 8b) —
  ~~resolved by Phase 8c, then fully closed later~~**: at the time this was
  written, a `Scan` iterator's behavior was undefined if the `DB` was
  mutated while that iterator was still in use. Phase 8c's lock-free
  `dbState` snapshot design fixed the *content* half of this (a `Scan`'s
  results are unaffected by any later mutation, by construction); the
  remaining *file-lifetime* half (a long-lived iterator surviving a
  compaction that retires its source files) was closed later via
  `sstable.SSTable`'s `AddRef`/`Close` refcounting and `ScanIterator` —
  see the "Known deliberate gaps" section above.
- **Phase 8d (two-level compaction)** additions, all additive beyond
  §3/§5/§7/§8's *current* LOCKED text — see the "Proposed LOCKED-section
  changes" block below for the exact replacement text this phase asks a
  human to review and promote:
  - `manifest.VersionEdit.Level int`: new field. Wire format adds two new
    edit-type marker bytes (`wireAddedLeveled`/`wireRemovedLeveled`,
    values 3/4) alongside the original `wireAddedLegacy`/
    `wireRemovedLegacy` (values 1/2, unchanged, matching the original
    exported `SSTableAdded`/`SSTableRemoved` byte values). `EncodeEdit`
    always writes the leveled form now; `DecodeEdit` reads whichever
    marker is actually present, so a MANIFEST mixing pre-8d and post-8d
    records decodes each one correctly on its own — no file-wide version
    flag needed, the per-record marker byte *is* the version signal (same
    self-describing spirit as 8a's bloom-section flag, adapted for a
    continuous record stream where the "read until exhausted" trick 8a
    used doesn't apply). A pre-8d record always decodes with `Level: 0`.
  - `manifest.ReconstructLeveledSSTableSet(edits) []LiveSSTable`: new
    function alongside the original (unchanged) `ReconstructSSTableSet`,
    additionally tracking each live file's level.
  - `compaction.CompactLeveled(inputs, outputPath, canDropTombstones bool)`:
    new function; `Compact` (unchanged signature) is now defined as
    `CompactLeveled(inputs, outputPath, true)`, preserved for every
    pre-8d caller that always passes every live SSTable as inputs.
  - `DB` struct: `sstables []*sstable.SSTable` (flat) replaced by
    `l0 []*sstable.SSTable` (newest-first, may overlap) and
    `l1 []*sstable.SSTable` (sorted ascending by `MinKey`, non-overlapping).
    New unexported `liveSSTables()` helper returns both concatenated, for
    callers (tests, `Close`) that only care about total live membership.
  - `DB.SetL0CompactionThreshold(int)`: new method, same role the old
    `SetCompactionThreshold` played for the single flat level.
    `SetCompactionThreshold` is kept as an alias calling
    `SetL0CompactionThreshold`, so `cmd/chaos`/`cmd/chaosworker`/benchmarks
    keep working unchanged.
  - `DB.SetL1SizeRatio(int)`: new method controlling the multiple of the
    L0 flush-size target L1's total on-disk size may reach before a full
    L1->L1 recompaction fires (default 10, per §3's own rationale).
  - `DB.MaybeCompact() error`: unchanged signature, now an orchestrator
    calling two new unexported methods in order,
    `compactL0ToL1IfNeeded`/`compactL1IfNeeded` — two independent
    triggers, neither one's inputs ever overlapping the other's.
  - `findL1TableForKey(l1, key) *sstable.SSTable`: new unexported
    function (package-level, not a `DB` method as of Phase 8c's snapshot
    refactor below — it operates on a specific `l1` slice, not `db.l1`
    directly), the binary search over L1's non-overlapping ranges that
    makes `Get` check at most one L1 file instead of every one.
  - Real-subprocess-`SIGKILL` crash coverage extended:
    `l0l1_crash_subprocess_test.go` reuses `cmd/chaosworker`'s existing
    ACK-line protocol (no changes needed there — `SetCompactionThreshold`
    already aliases onto the new L0 trigger) with a small
    `-compactionthreshold` so a randomized kill lands mid-L0->L1 merge;
    12/12 iterations, 25,955 total acked writes verified, 0 lost/corrupt,
    L1 non-overlap invariant checked after every recovery.
- **Phase 8c (concurrent readers/writers)** additions:
  - `dbState` (unexported struct: `mem`, `w`, `l0`, `l1`) + `DB.state
    atomic.Pointer[dbState]`: the entire mutable-state model. Every
    method that used to reassign `db.mem`/`db.l0`/`db.l1`/`db.w` in place
    now builds a new `*dbState` and calls `db.state.Store` once, atomically
    publishing a fully-formed, never-mutated-again snapshot.
  - `DB.writeMu sync.Mutex`: serializes `Put`/`Delete` (and the
    flush/compaction either triggers) against each other. **Deliberate
    deviation from the brief's suggested design**, which called for a
    lock-free, non-blocking "elected flusher" via a `flushing
    atomic.Bool` CAS guard. That design is not actually safe against this
    codebase's specific `memtable.SkipList`: its `Iterator`/`SeekIterator`
    contract explicitly says traversal "is not synchronized with
    concurrent writes to the same list," so a bare CAS-elected flush
    (with no serialization around the memtable insert itself) could let a
    concurrent `Put`'s insert race invisibly with the winning flusher's
    `SSTable` write — a write that's WAL-durable but silently absent from
    both the flushed `SSTable` and (once the old memtable is retired) any
    live read, a real data-loss bug for a *live* reader, not just a
    crash-recovery nuance. `writeMu` avoids this by construction: a
    flush's memtable iterator only ever runs while the same goroutine
    still holds `writeMu`, so it can never interleave with another
    goroutine's insert into that same memtable. This trades
    writer/writer parallelism (Put/Delete calls run one at a time,
    mirroring LevelDB/RocksDB's own single-active-writer model) for a
    design that's provably correct against the memtable's actual
    contract, per this phase's Debug Rule (do not paper over a
    correctness question with a fancier lock-free scheme under time
    pressure). `flushing`/`compacting` CAS guards from the brief were
    correspondingly not added — under `writeMu` they'd be redundant (only
    one goroutine is ever inside the write/flush/compact path at a time).
  - `DB.inFlightReaders atomic.Int64` + `DB.drainReaders()`: a `Get` call
    (whole call) and a `Scan` call (the snapshot-construction portion)
    increment/decrement this around themselves. Compaction (never
    flush — a flush's retired memtable needs no such protection, since
    `writeMu` already guarantees no other writer touches it, and no
    reader ever touches a `dbState`'s `w` field) calls `drainReaders`
    after publishing its new state and before closing/removing the
    `SSTable` files that state just retired, so a `Get`/`Scan` that
    loaded the *previous* state can never have a file closed out from
    under it mid-read.
  - `memtable.SkipList`: **real bug found and fixed in-scope** (per this
    phase's explicit mandate to verify the memtable's existing
    `sync.RWMutex` is genuinely sufficient under concurrent access, and
    fix any gap found). `skipListIterator.Next` previously followed raw
    `node.forward` pointers with no locking at all after the iterator was
    constructed — safe only as long as nothing else ever mutated the list
    while the iterator was in use, which Phase 8c's own `Scan` now
    violates by design (a long-lived iterator handed to a caller while
    concurrent `Put`s continue). `go test -race` caught this directly as
    a real, reproducible data race (not a hypothetical one) the first
    time a concurrent-`Put`-plus-`Scan` test was run. Fixed by having
    `Next()` take the list's own `s.mu.RLock()` for the brief moment it
    reads one node's forward pointer and copies out its key/value/
    tombstone into the iterator's own fields (`curKey`/`curValue`/
    `curTombstone`), rather than storing a raw `*node` and dereferencing
    it lazily later. This is safe even though a node's value/tombstone
    *can* be overwritten in place by a later `Put` on the same key,
    because that overwrite always replaces the slice header with a
    reference to a brand-new cloned array (`cloneBytes`) rather than
    mutating an existing array's bytes -- a slice header copied out under
    `RLock` keeps pointing at a backing array that's immutable from that
    point on, safe to read after the lock is released regardless of what
    the node is mutated to afterward. Both `Iterator()` and
    `SeekIterator()` share this fix (both return a `skipListIterator`).
  - `DB.MaybeCompact() error`: signature unchanged from Phase 8d, but now
    acquires `writeMu` for its duration (it's just an on-demand path to
    the same `maybeCompact` flush triggers automatically).
  - `DB.Close`: doc comment now explicitly states it must not be called
    concurrently with any other in-flight `DB` method — no
    graceful-shutdown draining was implemented (out of scope, per the
    brief).
  - `concurrency_test.go`: 5 new tests, all passing under `go test -race`:
    concurrent `Put`/`Get`/`Scan` with no data race; concurrent `Put`s
    crossing the flush threshold under contention never lose or
    duplicate a write; concurrent compaction under load preserves
    correctness and the L1 non-overlap invariant; a dedicated reader
    hammering a stable key throughout heavy concurrent compaction churn
    never observes a read error (the direct test of the
    inFlightReaders/drainReaders mechanism); an in-flight `Get`/`Scan`
    snapshot remains valid and correct across a concurrent state swap.
  - **Known limitation carried forward at the time this phase was
    written — ~~later fully closed~~**: `Scan`'s returned iterator was
    only protected against a racing file close for the duration of the
    `Scan` call itself (snapshot construction); the memtable source was
    already safe indefinitely after (see the `skipListIterator` fix
    above), but an SSTable source in a long-lived iterator that outlived a
    *later* compaction retiring that specific file could still surface a
    read error. This was closed in a later session via
    `sstable.SSTable.AddRef`/refcounted `Close` and the new
    `ScanIterator` — see the "Known deliberate gaps" entry above for the
    full design and its direct test.

## 12. Testing conventions

- Every phase's exit criteria (defined in its own architect brief) must
  pass before moving to the next phase.
- Prefer real subprocess `kill -9` tests over in-process crash simulation
  wherever the phase brief calls for it — the point is exercising actual
  OS-level fsync/rename semantics, not approximating them.
- `go vet` and `go test ./...` clean before considering any phase done.
- Don't fix a bug found in a later phase's testing by patching around it
  in that phase — fix it in the earlier phase it actually belongs to, and
  re-run that phase's tests too.

## Immediate next steps (as of this session)

The 8c/8d review and the Phase 9 demo (previous session) are done. This
session closed both of the two remaining known gaps carried forward from
8b/8c:

1. **`Scan`-iterator-outliving-a-compaction**, via `sstable.SSTable`
   reference counting (`AddRef` + a refcounted `Close`) and a new
   `ScanIterator` return type (`memtable.Iterator` plus `Close`). See the
   "Known deliberate gaps" section above for the design and
   `scan_lifetime_test.go` for the direct test (snapshots an in-progress
   iterator's source files, forces a compaction that unlinks all of them,
   confirms the iterator still reads everything correctly).
2. **`Put`/`Delete` returning an error indistinguishable from a real write
   failure**, via a new exported `*HousekeepingError` (`errors.As`-
   compatible) wrapping any error `maybeFlush` returns after the write's
   own WAL append + memtable mutation already succeeded. See
   `housekeeping_test.go` for the direct test (forces a flush failure via
   a read-only SSTable directory, confirms both the error type and that
   the write survives a full restart).

Both are additive (no LOCKED-section changes needed) and covered by new
dedicated tests, plus the full existing suite re-run clean. `cmd/demo`'s
Scan section was updated to call the new `Close()` (a no-op there, since
it drains fully, but demonstrates the idiomatic pattern for a caller that
might not).

**Nothing from this session is committed yet.** Before picking this back
up: review the refcounting change in `sstable/reader.go`, the
`ScanIterator`/`rangeIterator` changes in `scan.go`, `HousekeepingError`
in `db.go`, and the two new test files, then commit if acceptable.

**Next up:** the full scope tier (Minimum + Target + Stretch, §9) plus a
demo are all implemented end-to-end, and both previously-known gaps are
now closed. There's no required next phase — remaining work is
open-ended, the user's call. One candidate not yet done: a third+
compaction level, if ever wanted (explicitly out of scope per §3's Phase
8d note, would need a fresh decision to revisit).