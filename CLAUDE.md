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

**Current phase:** Phase 7 — Benchmarks + wrap-up (not yet started)
**Last completed phase:** Phase 6 — Compaction (k-way merge, tombstone GC,
crash-safe MANIFEST ordering; completes Target tier)

| Phase | Status | Notes |
|---|---|---|
| 1 — Durable Log | Done | `wal` package: `Entry` encode/decode, `Writer.Append` (fsync before ack), `Replay` (stops clean at torn/corrupt record, no error), segment rotation via `SetMaxSegmentBytes`. All tests + `go vet` clean. |
| 2 — In-Memory Engine | Done | `memtable.SkipList` (probabilistic levels, p=0.25, single `sync.RWMutex`): `Put`/`Delete`/`Get` (found vs. tombstone distinguished internally), sorted `Iterator`, `SizeBytes`. Root `DB` in `db.go`: `Open` replays `wal.Replay` into a fresh memtable then opens a new segment via `wal.NextSegmentPath` (never reopens the last segment); `Put`/`Delete` append-then-mutate, never the reverse; `Get` collapses "not found" and "tombstone" to `found=false`. `cmd/lsmload` helper + `killrestart_test.go` drive a real subprocess SIGKILL mid-burst (5 iterations) and confirm recovered keys are a subset of what was sent with byte-exact values, no corruption. All tests + `go vet` + `-race` clean. |
| 3 — Persistence | Done | `sstable` package: `FlushMemtable` writes a memtable's sorted entries (reusing `wal.EncodeEntry`/`wal.DecodeEntry` for the record format — see Deviations) plus a sparse index (every 64th entry) + min/max/count footer, via temp-file → fsync → atomic rename → directory fsync. `OpenSSTable` reads only the trailer+footer at open (checksummed, fails loudly on mismatch — every `Get` depends on the index being intact); `Get` binary-searches the sparse index and bounded-scans one block via `io.SectionReader`+`ReadAt` (proven non-full-file-scan in `TestSparseIndexBoundsScan`). `DB` wires size-triggered flush into `Put`/`Delete` (`SetFlushThreshold` for tests), discovers SSTables at `Open` via the naive directory scan the brief calls for, and `Get` falls through memtable → SSTables newest-first, first hit (including a tombstone hit) wins. A post-implementation soundness review (prompted by explicit request, not the brief) found and closed two real gaps beyond the brief's own checklist — see the former "Known deliberate gaps" entries folded into this row: (a) obsolete WAL segments are now deleted via `wal.RemoveSegmentsBefore` right after each flush's new segment is created, keeping restart-time replay cost and post-restart memtable size bounded by activity since the last flush rather than the database's entire history (`TestRestartMemtableSizeBoundedByActivitySinceLastFlush` pins this — it failed before the fix, reproducing a real ~23x memtable bloat on restart); (b) every atomic rename/segment-create (`sstable.FlushMemtable`, `wal.NewWriter`, `wal.Writer.rotate`) is now followed by an fsync of the containing directory, proven invoked (not just "file exists afterward") via spy-substituted `fsyncDir` vars in both packages. All tests + `go vet` + `-race` clean (42 tests total across all packages). Additionally validated with two ad hoc real-`kill -9` runs (throwaway, not committed): 8 iterations pre-fix and 12 post-fix, each forcing 1–2 flushes to complete mid-burst before the kill — zero corrupt/partial values recovered in any run, and post-fix runs confirm exactly one `.log` file survives on disk regardless of how many flushes completed. A second review pass (explicitly requested: fix everything not owned by a later phase) found and closed two more gaps: (c) `memtable.SkipList.SizeBytes()` previously counted only raw key+value bytes, letting real heap usage (each node's struct fields + its randomly-leveled forward-pointer array) run ahead of the configured flush threshold — now adds `nodeStructOverheadBytes` (`unsafe.Sizeof(node{})`) plus `lvl*pointerBytes` per *new* node (the exact chosen level, not an estimate; overwriting an existing key still charges only the value-byte delta, never re-charged struct overhead), verified by bounds in `TestSizeBytesGrowsAndTracksOverwrite`. This made `SizeBytes()` no longer reproducible from key/value content alone (per-node level is randomly drawn), which broke `TestRestartMemtableSizeBoundedByActivitySinceLastFlush`'s exact-byte-equality assertion — fixed by switching that test to compare live entry *count* (deterministic) instead of byte size. (d) A flush interrupted mid-write (real crash before `FlushMemtable`'s atomic rename) left a permanent orphaned `*.sst.tmp` file that nothing ever cleaned up — harmless (ignored by SSTable discovery, never read) but an unbounded disk leak given enough crashes over a long-running database's life. Closed by `removeOrphanedFlushTmpFiles`, called once at the top of `Open` before SSTable discovery; safe because a flush's rename is the sole moment it becomes durable/visible; scoped precisely (`TestOpenLeavesUnrelatedFilesAlone`) to `*.sst.tmp` so it can't delete anything else. A third finding — `DB` has no synchronization and is unsafe for concurrent `Put`/`Get`/`Delete` from multiple goroutines — was deliberately **not** fixed: full concurrent-reader/writer support is explicitly stretch phase 8c's job (§9/§10), and a rushed coarse lock now could conflict with whatever design 8c actually needs (lock-free reads during a flush swap, etc.); noted here rather than silently dropped. |
| 4 — Crash-Safe Metadata | Done | `manifest` package: `VersionEdit`/`EditType` (`SSTableAdded`/`SSTableRemoved`) encode/decode with the same checksum-then-torn-tail-stop discipline as `wal` (`EncodeEdit`/`DecodeEdit`, `ErrCorrupt`); `AppendEdit` (fsync before return, plus a directory fsync the first time a MANIFEST file is created — same reasoning as `wal.NewWriter`'s fresh-segment fsync); `ReplayManifest` (missing file = zero edits, not an error — correct for a database that hasn't flushed yet); `ReconstructSSTableSet` (pure function, sorted output, map-based add/remove replay). `CURRENT` read/write (`ReadCurrent`/`WriteCurrent`) via the same temp-file → fsync → atomic-rename → dir-fsync pattern used elsewhere. `DB.Open` now follows the exact locked recovery order (§7) CURRENT → MANIFEST → SSTable set → WAL → memtable (previously WAL replay ran before SSTable discovery — reordered this phase); `openSSTables` opens exactly the MANIFEST-live filenames (not a directory-scan intersection — see below), while `nextSeq` is still derived from every `.sst` file physically on disk (live or orphaned) so a future flush's atomic rename can never collide with an orphan. `maybeFlush` now appends a fsync'd `SSTABLE_ADDED` edit immediately after each flush's atomic rename succeeds and before the flush is considered complete; a crash in that exact window leaves the SSTable file orphaned (present on disk, absent from the MANIFEST-reconstructed set) — correctly ignored, not deleted, on the next `Open`, with all its data still recoverable via WAL replay since WAL segment cleanup (`RemoveSegmentsBefore`) is strictly downstream of the MANIFEST append succeeding. All tests + `go vet` + `-race` clean (77 top-level test functions across all packages, several with subtests — including the `manifest` package's own suite). A post-implementation gap review (per explicit request) found and closed one real gap beyond the brief's own checklist: `openSSTables` originally intersected the MANIFEST-reconstructed live set against a directory scan, which meant a filename the MANIFEST listed as live but that was actually missing from disk would be silently dropped rather than surfaced — the same class of "silently lose data instead of failing loudly" bug this codebase explicitly refuses elsewhere (`sstable.OpenSSTable`'s footer-checksum check, `wal`'s non-torn-tail corruption handling). Fixed by opening `live`'s filenames directly, so a missing file now returns a hard error at `Open` time (`TestOpenFailsLoudlyWhenManifestListsMissingSSTable`). Also added `TestNextSeqAvoidsCollisionWithOrphanedSSTable`, explicitly proving the orphan-avoidance property described above rather than leaving it as untested reasoning. The kill-mid-flush window (rename succeeds, MANIFEST append never happens) is exercised via direct injection rather than a real `kill -9` (`TestRecoverySkipsSSTableOrphanedByCrashBetweenRenameAndManifestAppend`) — the window is a handful of syscalls wide and not reliably hittable with a real signal, unlike Phase 2's real-SIGKILL harness, which stays real because its target window (mid-burst-of-writes) is comparatively wide. A second, dedicated review pass (also per explicit request, after the first pass's fix) found and closed a more serious second gap: `manifest.AppendEdit` had no protection against reopening a MANIFEST that already had a torn tail (from a crash mid-append in an earlier session) and appending past it — structurally the exact same bug Phase 1 already found and fixed in `wal` (`ErrTornSegment`), just unnoticed here on first implementation. Unlike a WAL segment (always abandoned for a brand-new one after a crash, so a torn tail is never reopened for append), the MANIFEST is one long-lived file for the database's entire life in this phase's scope (no rotation), so `wal`'s fix doesn't transplant directly — instead of redirecting to a new segment, the fix truncates the torn bytes away. Closed by `manifest.RepairTornTail`, called once in `Open` before `ReplayManifest`/before any future `AppendEdit` can run; safe because a torn tail is by definition a write that never completed, so discarding it loses nothing durably acknowledged (identical reasoning to replay's own "stop, don't error" contract). Proven with a negative control (`TestAppendEditWithoutRepairCorruptsFutureRecords`, showing the bug is real: appending after a torn tail without repairing does silently make the new record unreadable), the corresponding positive case (`TestRepairTornTailThenAppendDoesNotCorruptFutureRecords`), and a full DB-level integration test that injects a torn tail onto a real `Open`-managed MANIFEST, confirms repair on restart, performs a genuinely new flush afterward, and confirms *that* flush survives yet another restart (`TestOpenRepairsTornManifestTailBeforeAcceptingNewFlushes`). Also swept every `O_APPEND` call site in the repo (`wal.NewWriter`, `wal.Writer.rotate`, `manifest.AppendEdit`) to confirm this was the only unprotected one — the two `wal` sites are safe by construction (each rotation/resume always targets a strictly-new, never-before-used segment filename), not by a defensive check, which is why the bug didn't already exist there. This finding was promoted to a new bullet in §8's running invariants list, since it's a general principle future phases should watch for, not a one-off fix. All tests + `go vet` + `-race` still clean after this fix (84 top-level test functions total). |
| 5 — Chaos Test | Done | `internal/chaosdata` (`Key`/`Value`): the deterministic `key:%08d`/`val:%08d` pair generator shared by the worker and harness so their formats can't drift apart. `cmd/chaosworker`: opens a DB, sets a small flush threshold (`-flushthreshold`, default 4096B — measured to trigger a flush roughly every ~36 writes at the worker's key/value size, so even a short-lived iteration crosses at least one flush), performs up to `-count` sequential `Put`s, and prints `ACK %08d` to stdout the instant each `Put` returns successfully — the sole ground-truth source for "was this write durably acknowledged," never inferred from timing. `cmd/chaos`: the harness. Builds `chaosworker` once, then per iteration launches it as a real subprocess, picks a randomized target acked-write count in `[50, maxwrites-50)`, and sends a real `SIGKILL` the instant an `ACK` line at or past that target is read from its stdout (ground truth for verification is the actual last `ACK` line drained from the pipe, not the target itself — a few extra writes can land in the gap between hitting the target and the process actually dying, and those count too). Restarts via in-process `lsm.Open` against the same data directory and asserts every key up to the last-seen `ACK` is recoverable with its exact value (a missing un-acked key is fine; a missing *acked* key or a wrong value are both hard failures, tracked separately as "lost" vs. "corrupt"). `StdoutPipe`'s reader goroutine is always fully drained (`doneReading` closed) before `cmd.Wait()` is called, per `os/exec`'s own correctness requirement for that ordering. Real run: 20/20 iterations clean, 29,896 total acked writes verified across kill points ranging from 79 to 2,923 acked writes into a burst, 0 lost, 0 corrupt — full output and analysis in `docs/chaos-report.md`. Phases 1–4's fsync-before-ack, torn-tail-stop, and MANIFEST-repair-before-append invariants held under real `SIGKILL` exactly as their earlier targeted/injected tests predicted. Compaction doesn't exist yet, so kill points during compaction are explicitly out of scope here — Phase 7 extends this same harness once Phase 6 lands. A dedicated audit pass (explicit request: research whether the techniques used are sound, find and fix any in-scope gaps) covering every package from Phase 1 onward, not just this phase's own code, found and fixed two real issues: (a) `memtable.SkipList.Get` returned a node's value slice directly rather than a copy -- harmless internally, but a real aliasing footgun for this engine's actual use case (an embedded library whose callers hold the returned `[]byte` past the call): a caller mutating it in place would silently corrupt that key for every future `Get`, with no error anywhere to surface it. Fixed by cloning before return (`TestGetReturnsIndependentCopy` pins it); (b) this phase's own chaos harness (`cmd/chaos`) only verified keys in `[0, lastAck]`, so a corrupt value sitting just past the ACK boundary -- exactly the kind of bug this whole phase exists to catch -- would have gone completely undetected. Fixed by scanning the full `[0, maxwrites)` range every iteration (absence past the ACK boundary is fine; a wrong value anywhere is not), re-run clean afterward. The audit also specifically investigated whether `os.File.Sync()` gives real durability on Darwin (a well-known platform gap: plain macOS `fsync(2)` doesn't flush the drive's write cache, unlike Linux) -- confirmed sound, not a bug, by reading the actual Go 1.26.5 toolchain source (`internal/poll/fd_fsync_darwin.go`): Go has issued `fcntl(F_FULLFSYNC)` on Darwin since 2018 specifically to close this gap, and every fsync call site in this repo goes through `*os.File.Sync()`, so all of them get it. See `docs/chaos-report.md`'s "Honest limits of this proof" section for what real-`SIGKILL` testing does and doesn't establish about durability (it validates recovery logic under process death; it can't, on its own, validate the `fsync`/power-loss half of the claim, which is why the toolchain-source check above mattered). All tests + `go vet` + `-race` clean after these fixes. |
| 6 — Compaction | Done | New `compaction` package: `MergeIterator` (`container/heap`-based k-way merge over `Source{Iter, Rank}` -- lower `Rank` wins ties, so duplicate keys across inputs resolve to the newest source's value and every older duplicate is silently dropped *during* the merge, never emitted at all) plus `tombstoneDroppingIterator` (drops a tombstone that's still the newest entry for its key from the output entirely -- safe only because single-level compaction replaces every live SSTable in one pass, so there's no older data left underneath; the assumption is documented directly on the wrapper type per §8's instruction to comment it at the point it's implemented). `Compact(inputs []*sstable.SSTable, outputPath string) (*sstable.SSTableMeta, error)` takes inputs newest-first (the same convention `db.sstables` already uses -- index doubles as recency rank), writes the merged/deduped/tombstone-dropped output via a new `sstable.FlushIterator` (`FlushMemtable` is now a thin wrapper over it -- a memtable flush and a compaction output now share one temp-file -> fsync -> atomic-rename -> dir-fsync write path instead of two), and checks each input's terminal `Err()` after the drain (via a new `sstable.Iterator`, a full-table sequential scan reader distinct from `Get`'s bounded block scan) so silent corruption deep in an input can't masquerade as a clean end-of-table -- the same "fail loudly, don't silently drop data" discipline `OpenSSTable`'s footer checksum already applies. `DB.MaybeCompact` (new method, see Deviations) triggers once the live SSTable count exceeds `compactionTriggerThreshold` (4), called synchronously from the tail of `maybeFlush` (the brief explicitly allows a simple synchronous check over a background ticker at this stage -- there's no concurrency to race against yet; see the existing 8c gap below). MANIFEST edits are appended `SSTABLE_ADDED` (new file) before every `SSTABLE_REMOVED` (each input), individually fsync'd, per the LOCKED §7 ordering; on the ordinary success path the old, now-retired input files are also closed and deleted from disk (the brief only excuses leaving redundant files behind in the crash-4 scenario, not on every normal run -- `TestCompactionCleansUpRedundantOldFilesOnSuccessPath` pins this). All 4 crash-injection scenarios from the brief are covered as separate direct-injection tests, same "handful of syscalls wide, not reliably hittable with a real signal" reasoning Phase 4 used for its own analogous window: `TestCompactionCrashBeforeTempWriteCompletesLeavesOldFilesAuthoritative` / `...AfterTempWriteBeforeRenameLeavesOldFilesAuthoritative` (old files untouched/authoritative either way; the temp litter is swept up by the pre-existing `removeOrphanedFlushTmpFiles`, which already matches compaction's own `*.sst.tmp` naming for free) / `...AfterRenameBeforeManifestAppendLeavesOutputOrphaned` (new file ignored, old files still live) / `...AfterAddedBeforeRemovedLeavesBothGenerationsLive` (both generations live post-restart, correctness preserved via newest-wins since the compacted output's higher sequence number sorts it first in read order). `TestCompactionNextSeqSkipsPastOrphanedCompactionOutput` extends Phase 4's own `TestNextSeqAvoidsCollisionWithOrphanedSSTable` precedent to a compaction-orphaned file. `TestCompactionTriggerFiresAutomatically` and `TestCompactionPreservesCorrectnessAcrossOverwritesAndDeletes` prove the trigger engages from ordinary `Put`/`Delete` traffic alone (via a `manifestHasAnyRemoved` check -- flush itself never emits `SSTABLE_REMOVED`, so its presence is direct proof compaction actually ran, not just that flushes happened) and that keys overwritten/deleted across several distinct pre-compaction SSTable generations still resolve correctly afterward. A dedicated audit pass (explicit request, covering this phase and re-checking all priors) found and fixed two real issues before any of this landed: (a) both `MaybeCompact` and, on closer inspection, the pre-existing `maybeFlush` verified the just-written file via `sstable.OpenSSTable` *after* the MANIFEST already committed the edit(s) retiring the superseded generation. For a flush this is a narrow, low-severity gap (the WAL still backs that data at that exact point, so a freak post-write open failure fails loudly rather than losing data). For compaction it's materially worse: once `SSTABLE_REMOVED` commits for the old inputs, their data has no other surviving copy anywhere (their WAL segments were deleted long ago, during the flushes that originally produced them) -- a footer-checksum failure on the just-written output at that exact point would be genuine, permanent data loss, not merely an orphaned-but-harmless file. Fixed by reordering both call sites to open-and-verify the new file *before* either MANIFEST edit runs; verified by a full `-race` regression re-run. (b) `MaybeCompact`'s old-input cleanup loop returned immediately on the first `Close()` error, leaking every remaining input's file descriptor and skipping their on-disk removal -- fixed to attempt every close/removal and report only the first error, the same "attempt all, keep the first error" pattern `DB.Close` already uses. Neither finding has a dedicated failure-injection test: both require a genuine I/O fault on a file the process itself just wrote and fsynced moments earlier, with no existing seam to inject that without adding test-only complexity disproportionate to the risk -- closed by code-level reasoning plus full regression re-run instead, the same tradeoff Phase 4 made for its own narrowest crash windows. The audit also confirmed (not a bug, a documented tradeoff) that "compact everything on every trigger" write-amplification scales with the *total* accumulated dataset each time, since every compaction refolds the prior compacted output plus new flushes -- the direct, expected consequence of the LOCKED §3 single-flat-level decision; multi-level (8d) exists specifically to fix this later. This did surface a test-suite-hygiene issue worth fixing now, though: the first draft of the trigger/correctness tests used enough data (`n=20000`) to make that cost visible (~58s per test, dominated by Darwin's real `F_FULLFSYNC` latency per fsync -- see Phase 5's own note on this -- multiplied across ~100+ compaction cycles); reduced to a scale (`n=800`) that still exercises multiple real trigger cycles in ~1.5s. All tests + `go vet` + `-race` clean (101 top-level test functions across all packages). |
| 7 — Benchmarks + wrap-up | Not started | |
| 8a — Bloom filters | Not started (stretch) | |
| 8b — Range queries | Not started (stretch) | |
| 8c — Concurrent readers/writers | Not started (stretch) | |
| 8d — Multi-level compaction | Not started (stretch) | |

**Known deliberate gaps at current state:**

- **`DB` is not safe for concurrent use.** `maybeFlush` reassigns
  `db.mem`, `db.sstables`, and `db.w` with no synchronization; calling
  `Put`/`Get`/`Delete` from multiple goroutines concurrently is a data
  race. Found during Phase 3's soundness review and deliberately left
  open rather than patched with a coarse lock — concurrent
  readers/writers is explicitly stretch phase 8c's own job (§9/§10), and
  a hasty mutex now could conflict with whatever design 8c actually
  needs (e.g. lock-free reads across a flush's memtable/SSTable-list
  swap). Whoever picks up 8c should design this properly rather than
  build on a shim.
- **A `Put`/`Delete` that durably succeeds can still return an error, if
  purely post-durability housekeeping fails afterward.** `wal.Writer.
  Append` fsyncs the record before checking whether the segment needs
  rotating (§7); if that rotation fails, `Append` still returns the
  rotation error, even though the record itself is already safely on
  disk. `DB.maybeFlush` has the same shape one level up: by the time it
  calls `wal.RemoveSegmentsBefore` at the very end, the flush (SSTable
  rename + fsync'd MANIFEST edit) has already fully succeeded, but a
  failure in that trailing cleanup call still propagates up through
  `Put`/`Delete` as an error. Found during a dedicated audit pass (not
  this or an earlier phase's own brief) and deliberately **not** fixed:
  it never loses or corrupts data and always fails in the safe direction
  (over-reporting failure, never under-reporting it), and properly
  decoupling "core write durable" from "trailing housekeeping succeeded"
  would mean changing what `Put`/`Delete`'s return value promises --
  a real design question, not a bug fix, and one no phase brief has asked
  for. Worth designing deliberately if a future phase's caller ever needs
  to distinguish these two failure modes (e.g. to decide whether a retry
  would create a duplicate write).

(Previously, both found and closed within Phase 3's own session, not
deferred to a later phase:

`wal` segments were never deleted after a flush, and `wal.Replay` always
read every segment from the start of the database's history. This never
caused a read-correctness bug (replaying every WAL entry in order into a
fresh memtable always reconstructs each key's true latest value, however
redundant), but it defeated the *point* of flushing across a restart —
confirmed empirically before the fix: a run that flushed 14 SSTables and
left ~2.6KB live in the memtable came back from `Open` with ~60KB back in
memtable. Initially this was going to be deferred to Phase 4 on the
assumption that "which WAL segments are obsolete" needed the same durable
MANIFEST-style bookkeeping Phase 4 exists to build for the SSTable set —
but that assumption was wrong: `DB` always starts a brand-new WAL segment
at the exact moment of every flush, so every segment older than the one
just created is provably, unconditionally superseded by the SSTable that
flush just wrote — no MANIFEST needed to know that, the file's absence on
disk after deletion *is* the record. Closed by `wal.RemoveSegmentsBefore`,
called from `DB.maybeFlush` strictly after the new SSTable's rename (and
its own directory fsync, see below) are durable, so a crash before cleanup
just defers it to a later flush rather than losing data. Verified by
`TestRestartMemtableSizeBoundedByActivitySinceLastFlush` (fails without
the fix, reproducing the ~23x bloat) and by two independent throwaway
real-`kill -9` batches (8 runs before the fix, 12 after) showing zero
corruption in either case and exactly one surviving `.log` file per run
post-fix regardless of flush count.

Separately, SSTable flush's atomic rename (and `wal.NewWriter`/
`Writer.rotate`'s segment creation, which had the identical gap already)
were not followed by an `fsync` of the containing directory — on some
filesystems a crash between a successful `rename(2)`/`create` and the
directory entry itself being durably persisted can make that change not
survive the crash, which is a real gap under §2's durability guarantee.
This was riskier to leave once the WAL-retention gap above closed (that
gap had been incidentally providing a safety net: even a "lost" flush
rename was recoverable from the WAL, as long as the WAL was never
deleted), so both gaps were fixed together rather than landed
independently. Closed by an `fsyncDir` var in each of `wal` and `sstable`
(a package-level function var specifically so tests could substitute a
spy and prove the fsync call actually happens, not just that the
end-state file listing looks right) called after every rename/create
that lands a file at its permanent path. Verified by
`TestNewWriterFsyncsDirOnFreshSegment`, `TestNewWriterDoesNotFsyncDirOnCleanReopen`,
`TestRotateFsyncsDir`, and `sstable`'s `TestFlushFsyncsDir`.)

(Previously:
`wal.NewWriter` silently allowed reopening a torn segment and appending
past the tear, which made the new records permanently unreachable —
`Replay` stops at a segment's first torn record and never looks past it.
Closed by making `NewWriter` return `ErrTornSegment` instead of opening in
that case, so the failure is a loud error at Open time, not silent data
loss. `wal.NextSegmentPath` is the documented way to resume writes after
a Replay — it always names a fresh segment. Phase 4's `DB.Open`/recovery
sequence should still use `NextSegmentPath` rather than reopening the
last segment named in the reconstructed set, both because that's the
correct pattern and because reopening a *clean* last segment is still
technically allowed by `NewWriter`. Verified via `wal` package tests
`TestNewWriterRejectsTornSegment`, `TestNewWriterAllowsCleanExistingSegment`,
`TestNextSegmentPathAvoidsTornTailFootgun`.)

**Deviations from this spec:** none. `wal.NextSegmentPath` is an addition
beyond the Phase 1 brief's listed signatures, not a deviation from a
LOCKED section — it exists to make the LOCKED crash-recovery guarantee
(§2, "zero data loss for any acknowledged write") actually achievable by
Phase 4, given the sequential-append-only invariant (§8) rules out
truncate-and-resume as a fix. Phase 2 added `cmd/lsmload`, a small helper
binary (not part of any locked interface) that exists solely so
`killrestart_test.go` can drive a real subprocess through a SIGKILL —
per §12's preference for real `kill -9` tests over in-process
simulation. Phase 3 exported `wal.EncodeEntry`/`wal.DecodeEntry` (renamed
from the previously unexported `encode`/`readRecord`; the unexported
`errTorn` became exported `ErrCorrupt`) purely so `sstable` could call
the exact same record encoder/decoder rather than reimplementing the
wire format a second time — the wire format itself (§5) is unchanged,
this is an implementation-sharing change, not a format change. `sstable`
treats a `DecodeEntry` failure as a hard error (not §5's "stop, no
error" replay rule): that rule exists because a torn tail is *expected*
at the crash-time WAL segment, but an SSTable is only ever made visible
by an atomic rename after being fully written and fsynced, so mid-file
corruption there can only mean real bit rot, which should fail loudly.
Phase 3 also exported `wal.SegmentNames`/`wal.ParseSegmentSeq` (renamed
from unexported `segmentNames`/`parseSeq`, no behavior change) and added
`wal.RemoveSegmentsBefore`, all so `db.go` could delete obsolete WAL
segments after a flush using the same tested filename-parsing logic
`NewWriter`/`NextSegmentPath` already relied on, rather than a second,
divergence-prone copy of the "NNNNNN.log" convention living in package
`lsm`. `wal.NewWriter` and `wal.Writer.rotate` now fsync their segment's
containing directory after creating a fresh segment file (not on a clean
reopen of an existing one); `sstable.FlushMemtable` does the same after
its atomic rename. None of this changes any wire format or LOCKED
decision — it strengthens §2's existing durability guarantee for file
creation/rename, which the original implementation hadn't fully covered.

Phase 4 deliberately duplicated the checksum-then-torn-tail-stop encode/
decode logic (`manifest.EncodeEdit`/`DecodeEdit`) rather than factoring a
shared generic "append-only checksummed log" helper out of `wal` — the
brief explicitly left this as an implementer's choice. The two formats'
payloads differ enough (op+key+value vs. edit_type+filename) that a
shared helper would need to abstract over the payload shape anyway,
buying little over two small, independently readable files; revisit if a
third format ever wants the same discipline. `db.go`'s `Open` was
reordered (WAL replay now runs after SSTable-set resolution, not before)
to match the LOCKED §7 recovery sequence exactly — this is fixing Phase
3's temporary ordering to match the spec now that the MANIFEST exists to
resolve the SSTable set first, not a new deviation.

Phase 5 added `cmd/chaosworker` (the real-subprocess write-burst worker)
and `cmd/chaos` (the SIGKILL harness), same pattern as Phase 2's
`cmd/lsmload` — neither is part of any LOCKED interface, both exist solely
to drive and verify real crash recovery per §12's testing conventions.
`internal/chaosdata` is a new small shared package (not previously listed
anywhere in this file) holding just the deterministic key/value formatters
both binaries need to agree on; kept `internal` since nothing outside
those two binaries has a reason to depend on it.

Phase 6 added a new `compaction` package rather than folding the k-way
merge into `sstable` itself, even though `sstable/doc.go`'s original
Phase-0 description mentioned "k-way merge compaction" as part of that
package's job -- that line predates any phase split and was never
binding on a later phase's actual design; a separate package mirrors the
existing `manifest`-is-separate-from-`sstable` precedent and keeps
`sstable` scoped to the on-disk format plus reading/writing it (`doc.go`
updated to match). `sstable` gained two additions beyond what Phase 3
already exposed: `Iterator` (a full-table sequential scan --
`Next`/`Key`/`Value`/`Tombstone`/`Err` -- distinct from `Get`'s bounded
block scan, needed as compaction's merge source) and `FlushIterator`
(the write path factored out of `FlushMemtable`, which is now a thin
wrapper calling it with `m.Iterator()`, so a memtable flush and a
compaction output share one write implementation instead of two).
`DB.MaybeCompact() error` is a new public method beyond §6's LOCKED `DB`
struct signature list -- an addition in the same spirit as
`wal.NextSegmentPath` (Phase 1) and the various exported helpers from
Phases 3-4: it exists to make the LOCKED §7 compaction algorithm and §9
Target-tier scope actually reachable, not a change to any LOCKED
decision.

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
