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

**Compaction.** As SSTables accumulate, background (synchronous, in this
single-level design) compaction merges every live table into one new file
via a heap-based k-way merge, keeping only the newest value per key. Since
this compaction is single-level — the output always replaces *every*
current table at once — a tombstone that's still the newest entry for its
key at the end of the merge has nothing left to shadow and can be dropped
from the output entirely, instead of carried forward forever.

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

**Target tier (done):** single-level compaction (heap-based k-way merge,
tombstone GC, atomic rename, MANIFEST update in the crash-safe
ADDED-before-REMOVED order); a real-subprocess `kill -9` chaos harness
(`cmd/chaos`, `cmd/chaosworker`) extended to trigger compaction inside the
randomized crash window, not just ordinary writes — see
`docs/chaos-report.md`; the four benchmarks above.

**Not built (stretch, explicitly out of scope for this phase):**
- **8a — Bloom filters.** Would let `Get` skip an SSTable's on-disk scan
  entirely for keys it provably doesn't contain, rather than doing a
  sparse-index search that still ends in "not found" here.
- **8b — Range queries.** `Get` is point-lookup only; there's no ordered
  iteration across the merged memtable+SSTable view exposed publicly yet
  (internally, `sstable.Iterator` and `memtable.Iterator` exist, but only
  for flush/compaction's own use).
- **8c — Concurrent readers/writers.** `DB` is not safe for concurrent
  use today (`maybeFlush` reassigns `db.mem`/`db.sstables`/`db.w` with no
  synchronization) — deliberately left open rather than patched with a
  coarse lock that might conflict with whatever design this phase
  actually needs.
- **8d — Multi-level compaction.** Today's compaction is single-level
  ("merge everything periodically"); the tombstone-drop-during-compaction
  logic is only safe because of that, and would need to be revisited if a
  multi-level L0/L1 scheme were ever added.
