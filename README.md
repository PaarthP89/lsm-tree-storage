# lsm-tree-storage

An embedded, crash-safe key-value storage engine built from first
principles in Go — a small version of the storage layer inside RocksDB,
LevelDB, or BoltDB. It's a library, not a server: an application links
against it and calls `Put`/`Get`/`Delete` in-process.

```go
db, err := lsm.Open("data")
err = db.Put([]byte("user:1"), []byte("alice"))
val, found, err := db.Get([]byte("user:1"))
err = db.Delete([]byte("user:1"))
err = db.Close()
```

Run `go run ./cmd/demo` for a narrated, end-to-end walkthrough of every
feature below in one program: basic `Put`/`Get`/`Delete` with tombstones, a
threshold-triggered flush, a range `Scan`, L0→L1 compaction, concurrent
readers/writers, a bloom-filter-served miss, and a real subprocess
`kill -9` crash-recovery cycle.

## Architecture

**WAL.** Every `Put`/`Delete` is first serialized as
`[CRC32 checksum][op][key][value]` and appended to a write-ahead log
segment, fsync'd before the call returns. Nothing is considered durable
until that fsync completes — the memtable is only updated after the WAL
append succeeds. On recovery, `wal.Replay` reads every entry, but a torn
or corrupt record (the natural result of a crash mid-write) stops replay
at that point rather than erroring or skipping past it: the record was
never fully acknowledged, so its absence is correct, not data loss.

**Memtable.** Writes land in an in-memory skip list (`memtable.SkipList`)
— probabilistic balancing rather than a self-balancing tree, which gives a
simpler concurrent-access story and is what real LSM engines (LevelDB,
RocksDB, Badger) use for the same reason. `Get` checks the memtable first;
`Delete` writes a tombstone rather than removing the key outright, so a
deletion can correctly shadow older, already-flushed values.

**SSTable.** Once the memtable crosses a size threshold, it's flushed to
an immutable, sorted on-disk file: entries in key order, followed by a
footer holding a sparse index (every 64th key → byte offset), min/max key,
and entry count. A `Get` against an SSTable binary-searches the sparse
index to find the one small on-disk range that could hold the key, then
scans only that range — never the whole file. The flush itself goes
through the same temp-file → fsync → atomic-rename discipline used
everywhere else on disk, so a crash mid-flush leaves either nothing or a
fully-formed file, never a half-written one at a real path.

**Compaction.** Two levels: L0 holds freshly-flushed tables (may overlap in
key range), L1 holds compacted, non-overlapping tables sized ~10x the L0
flush target. Two independent, synchronous triggers, both a heap-based
k-way merge keeping only the newest value per key: **L0→L1** fires once L0
holds more than a threshold's worth of tables, merging all of L0 plus any
overlapping L1 tables into one new L1 table; **L1→L1** fires once L1's
total on-disk size crosses the size ratio, merging all of L1 into one new
table. A tombstone still the newest entry for its key at the end of either
merge can be dropped entirely — but only because each trigger provably
includes every source that could hold an older value for any key it
touches (see `compaction.CompactLeveled`'s `canDropTombstones` argument and
CLAUDE.md §8 for the exact coverage proof per trigger). `Get` checks every
live L0 table newest-to-oldest, then binary-searches L1's non-overlapping
ranges for at most one candidate table.

## Why the MANIFEST exists

**"How does the engine know which SSTables are valid after a crash
mid-compaction?"**

Atomic rename protects one file: it guarantees an SSTable's flush or
compaction output is never observed half-written. It says nothing about
which *set* of SSTable files, taken together, is the database's current
state. After a crash, the data directory can contain old input files that
were about to be retired, a fully-valid new compacted file that hasn't
been "installed" yet, and possibly orphaned temp files — with no way to
tell, from the filesystem alone, which ones are live. Scanning the
directory and guessing (e.g. "newest file wins") doesn't work: a
compaction's output can be fully durable on disk before the database is
allowed to consider it authoritative.

The MANIFEST solves this by making "what's live" itself an append-only,
crash-safe log, independent of file content: each entry is a `checksum +
edit_type + filename` record recording `SSTABLE_ADDED` or
`SSTABLE_REMOVED`. A `CURRENT` file (written via the same
temp-file/fsync/rename discipline) points at the active MANIFEST. On
`Open`, the engine replays the MANIFEST to reconstruct the live SSTable
set *before* touching the WAL — any `.sst` file on disk that isn't in that
reconstructed set is ignored, not deleted, since it might be a harmless
orphan or might get cleaned up by a future compaction.

The subtle part is edit ordering during compaction: the new merged file's
`SSTABLE_ADDED` edit is always appended and fsync'd **before** the old
inputs' `SSTABLE_REMOVED` edits, never the other way around. A crash
between the two leaves the reconstructed set containing both the old
inputs and the new output — redundant, since they now hold overlapping
data, but harmless, because reads are newest-wins and the new output has
the highest sequence number, so it's always consulted first. The reverse
ordering (`REMOVED` before `ADDED`) has no such safety net: a crash in
that gap would leave a live set with neither the old inputs (already
retired) nor the new output (not yet installed) — real, permanent data
loss for every key in that compaction. So the ordering isn't
arbitrary — it's the difference between "worst case, some redundant
files" and "worst case, silently lost data."

## Benchmarks

Run with:

```
go test -run=^$ -bench=. -benchtime=2000x -v .
```

Real output, Apple M2 (darwin/arm64), Go 1.26.5:

```
goos: darwin
goarch: arm64
pkg: github.com/paarthsiphone/lsm-tree-storage
cpu: Apple M2
BenchmarkWrite
BenchmarkWrite-8                 	    2000	   2711741 ns/op	       368.8 ops/sec
BenchmarkReadHot
BenchmarkReadHot-8               	    2000	      2181 ns/op	    458448 ops/sec
BenchmarkReadCold
BenchmarkReadCold-8              	    2000	     17038 ns/op	     58692 ops/sec
BenchmarkReadAfterCompaction
BenchmarkReadAfterCompaction-8   	    2000	     13062 ns/op	     76556 ops/sec
PASS
ok  	github.com/paarthsiphone/lsm-tree-storage	242.926s
```

| Benchmark | ns/op | ops/sec | Setup |
|---|---|---|---|
| `BenchmarkWrite` | 2,711,741 | 369 | Sequential `Put`s at the production 4MB flush threshold |
| `BenchmarkReadHot` | 2,181 | 458,448 | 5,000 keys, all resident in the memtable (flush disabled) |
| `BenchmarkReadCold` | 17,038 | 58,692 | 20,000 keys spread across 13 uncompacted SSTables |
| `BenchmarkReadAfterCompaction` | 13,062 | 76,556 | Same dataset, compaction left at its default threshold → 1 SSTable |

**Write throughput is fsync-bound, and that's by design, not a
bottleneck to fix.** Every `Put`/`Delete` fsyncs the WAL before returning
(§7's durability invariant) — on this machine (Darwin/APFS), that's a real
`F_FULLFSYNC`, not a plain `fsync(2)` (macOS's plain `fsync` only pushes
writes to the drive's own write cache, not through it — Go's standard
library already accounts for this and issues `F_FULLFSYNC` on Darwin,
verified directly against the Go 1.26.5 toolchain source for this project;
see `docs/chaos-report.md`). ~2.7ms/op lines up with typical `F_FULLFSYNC`
latency on this hardware; a write path without a correctness-required
per-write fsync would be one to two orders of magnitude faster, but would
no longer be able to claim "once `Put` returns, the write survives a
crash."

**Hot reads are ~26x faster than the best cold-read case** (458,448
vs. 76,556 ops/sec) because a hot read is a pure in-memory skip-list
lookup — no disk I/O, no fsync, no encoding/decoding — while every cold
read pays for at least one sparse-index binary search plus a bounded
on-disk scan.

**Reads after compaction are ~1.3x faster than cold reads against the
uncompacted set** (76,556 vs. 58,692 ops/sec), because there's only 1
SSTable to consult instead of 13: `Get` walks the live SSTable list
newest-to-oldest, so a key that lives in an old, uncompacted table can
require checking several tables' sparse indexes before it's found. This is
exactly the throughput compaction is meant to buy back as the SSTable
count grows — the gap widens further the longer a database runs between
compactions.

## What's built

**Minimum tier (done):** WAL with torn-write recovery and segment
rotation; skip-list memtable; SSTable flush with sparse-index reads;
MANIFEST + `CURRENT` crash-safe metadata; full crash recovery
(`CURRENT → MANIFEST → SSTable set → WAL → memtable`, in that fixed
order); `Open`/`Put`/`Get`/`Delete`/`Close`.

**Target tier (done):** two-level compaction (heap-based k-way merge,
tombstone GC, atomic rename, MANIFEST update in the crash-safe
ADDED-before-REMOVED order); a real-subprocess `kill -9` chaos harness
(`cmd/chaos`, `cmd/chaosworker`) extended to trigger compaction inside the
randomized crash window, not just ordinary writes — see
`docs/chaos-report.md`; the four benchmarks above.

**Stretch tier (done):**
- **8a — Bloom filters.** Every SSTable carries a self-describing bloom
  filter (FNV-1a/Kirsch-Mitzenmacher double hashing, ~1% target FPR),
  consulted by `Get` before the sparse-index scan to skip straight past a
  table it provably doesn't contain the key in. ~20x fewer ns/op on a
  100%-miss workload (263 vs 5236 ns/op). Backward-compatible with
  pre-8a SSTable files (a missing filter section just means "always
  scan").
- **8b — Range queries.** `DB.Scan(start, end)` returns a sorted,
  tombstone-filtered, newest-wins iterator over the half-open range
  `[start, end)`, merged live across the memtable and every SSTable via
  `compaction.MergeIterator`.
- **8c — Concurrent readers/writers.** `Get`/`Scan` are fully lock-free
  (an immutable `dbState` published via `atomic.Pointer`); `Put`/`Delete`
  (and any flush/compaction they trigger) are serialized by a single
  `writeMu` rather than a lock-free CAS guard — a deliberate deviation,
  since the memtable's own iterator contract isn't safe under a
  lock-free flush (see CLAUDE.md's Phase 8c notes for the full
  reasoning). `Close` is still not safe to call concurrently with any
  other in-flight call.
- **8d — Multi-level compaction.** Two levels (L0/L1) as described above,
  replacing the original single-level "merge everything" scheme; verified
  under real-subprocess `kill -9` crashes mid-L0→L1-merge with the L1
  non-overlap invariant checked after every recovery.

Verified with `go test ./...`, `go test -race ./...`, and `go vet ./...`,
all clean, plus dedicated adversarial tests for the trickiest correctness
risk in the whole stretch tier: a tombstone dropped during compaction
without genuine coverage of every older source, which would otherwise
silently resurrect deleted data (`compaction/leveled_test.go`).
