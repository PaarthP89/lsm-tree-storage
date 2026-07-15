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
- MANIFEST edit ordering during compaction: ADDED before REMOVED, always.
- Tombstone-drop-during-compaction is only valid because compaction is
  single-level. Comment this assumption at the point it's implemented.
- Newest-wins for any key present in multiple sources (memtable > newer
  SSTable > older SSTable).

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

**Current phase:** Phase 2 — In-Memory Engine (not yet started)
**Last completed phase:** Phase 1 — Durable Log (WAL writer/reader, entry
encode/decode, torn-tail replay, segment rotation)

| Phase | Status | Notes |
|---|---|---|
| 1 — Durable Log | Done | `wal` package: `Entry` encode/decode, `Writer.Append` (fsync before ack), `Replay` (stops clean at torn/corrupt record, no error), segment rotation via `SetMaxSegmentBytes`. All tests + `go vet` clean. |
| 2 — In-Memory Engine | Not started | |
| 3 — Persistence | Not started | |
| 4 — Crash-Safe Metadata | Not started | |
| 5 — Chaos Test | Not started | |
| 6 — Compaction | Not started | |
| 7 — Benchmarks + wrap-up | Not started | |
| 8a — Bloom filters | Not started (stretch) | |
| 8b — Range queries | Not started (stretch) | |
| 8c — Concurrent readers/writers | Not started (stretch) | |
| 8d — Multi-level compaction | Not started (stretch) | |

**Known deliberate gaps at current state:**
- `wal.NewWriter` will happily reopen and append to an existing segment
  that has a torn tail (e.g. the file `DB.Open` finds mid-recovery). Doing
  so makes every record appended after the tear permanently unreachable —
  `Replay` stops at a segment's first torn record and never looks past it,
  even at later, fully-valid records in that same file. `wal.NextSegmentPath`
  exists to avoid this (see its doc comment) and returns the correct path
  for a fresh segment to resume writes into after a Replay. Phase 4's
  `DB.Open`/recovery sequence MUST use `NextSegmentPath` rather than
  reopening the last segment named in the reconstructed set. Verified this
  failure mode and the fix both empirically (`wal` package tests
  `TestAppendAfterTornTailIsUnreachable` / `TestNextSegmentPathAvoidsTornTailFootgun`).

**Deviations from this spec:** none. `wal.NextSegmentPath` is an addition
beyond the Phase 1 brief's listed signatures, not a deviation from a
LOCKED section — it exists to make the LOCKED crash-recovery guarantee
(§2, "zero data loss for any acknowledged write") actually achievable by
Phase 4, given the sequential-append-only invariant (§8) rules out
truncate-and-resume as a fix.

---

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
