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
| Level structure | Single flat level for Minimum/Target tier | Multi-level (10x size ratio per level) is real added complexity for marginal value at this scope. Single-level "merge everything periodically" still demonstrates k-way merge, tombstone GC, and atomic rename. Multi-level is a stretch item (8d). |

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
[checksum: 4B CRC32][edit_type: 1B][payload...]
```
`edit_type` is one of `SSTABLE_ADDED`, `SSTABLE_REMOVED`. Payload is the
SSTable filename (length-prefixed). Later, level number, if/when
multi-level compaction (8d) is added.

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
2. If not found, check SSTables newest-to-oldest (order determined by the
   MANIFEST's current view) until found or exhausted.
3. Each SSTable's sparse index is binary-searched, then a small on-disk
   range is scanned.

### Compaction (single-level, Target tier)
1. Background process periodically merges multiple SSTables into one via
   k-way merge (all inputs sorted).
2. During merge: keep only the newest value per key.
3. **Tombstone GC**: because single-level compaction replaces *every*
   current SSTable in one pass, there's no older data left underneath the
   output — a tombstone that's still the newest entry for its key at the
   end of the merge can be dropped from the output entirely. This
   assumption breaks under multi-level compaction (8d) and must be
   revisited if that's ever built.
4. Write result to a temp file, fsync, atomically rename into place.
5. Append MANIFEST edits as a single durable operation, **in this specific
   order: `SSTABLE_ADDED` (new file) before `SSTABLE_REMOVED` (old
   files)**. A crash between the two leaves both old and new files live in
   the reconstructed set — redundant, but every key's value is still
   correct via newest-wins read logic. The reverse order risks a real data
   loss window if the crash lands between them. This ordering was derived
   during the compaction phase, not stated explicitly in the original
   spec — it's binding regardless.

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
- Tombstone-drop-during-compaction is only valid because compaction is
  single-level. Comment this assumption at the point it's implemented.
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

**Current phase:** None — Phase 7 complete. Next up is picking a stretch
phase (8a–8d), user's choice.
**Last completed phase:** Phase 7 — Benchmarks + wrap-up (real benchmark
numbers, extended chaos run with compaction inside the crash window,
README, chaos-report update; completes Target tier end-to-end)

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
| 8a — Bloom filters | Not started (stretch) | |
| 8b — Range queries | Not started (stretch) | |
| 8c — Concurrent readers/writers | Not started (stretch) | |
| 8d — Multi-level compaction | Not started (stretch) | |

**Known deliberate gaps at current state:**

- **`DB` is not safe for concurrent use.** `maybeFlush` reassigns
  `db.mem`, `db.sstables`, and `db.w` with no synchronization. Left open
  rather than patched with a coarse lock, since concurrent
  readers/writers is stretch phase 8c's own job and a hasty mutex now
  could conflict with whatever design 8c actually needs (e.g. lock-free
  reads across a flush's memtable/SSTable-list swap).
- **A `Put`/`Delete` that durably succeeds can still return an error** if
  purely post-durability housekeeping fails afterward (WAL segment
  rotation in `Append`; `RemoveSegmentsBefore` at the tail of
  `maybeFlush`). Deliberately not fixed: it never loses or corrupts data
  and always fails in the safe direction (over-reports failure, never
  under-reports it). Properly decoupling "write durable" from "trailing
  cleanup succeeded" is a real return-value design question, not a bug
  fix — worth doing if a future caller ever needs to tell those two
  failure modes apart (e.g. to decide whether a retry would duplicate a
  write).

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
