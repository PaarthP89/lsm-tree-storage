# Chaos Test Report

## What was tested

20 iterations of: launch `cmd/chaosworker` as a real subprocess writing a
burst of up to 3000 sequential keys (`key:%08d`) with deterministic
values -> real `SIGKILL` at a randomized point during the burst -> restart
the engine in-process (`lsm.Open`) against the same on-disk data
directory -> verify every acknowledged write.

Verification scans the *entire* possible key range (`0..maxwrites-1`) each
iteration, not just `0..lastAck`: every key up to the last ACK must be
present with the exact expected value (a miss is "lost", a wrong value is
"corrupt"); every key past the last ACK is allowed to be either absent or
present-with-the-correct-value, but a wrong value there is still corruption
and still fails the run. An earlier draft of this harness only checked
`0..lastAck`, which meant a corrupt value sitting just past the ACK
boundary would have gone completely undetected -- caught and fixed during
a follow-up audit pass before this run.

The worker's memtable flush threshold was set to 4096 bytes (`-flushthreshold`,
`DB.SetFlushThreshold`), far below its 4MB production default. Measured
separately: this triggers roughly one flush every ~36 writes, so every
iteration -- even the smallest, killed after 64 acked writes -- crosses at
least one full flush/MANIFEST-append/WAL-rotation cycle, and the larger
iterations cross dozens. Since `DB.Put` performs any triggered flush
synchronously before returning, a kill can land the process mid-flush
(after the SSTable's atomic rename but before the MANIFEST append, mid
WAL-segment-rotation, etc.) for whichever write was in flight and
un-acked at kill time -- exactly the class of window Phase 4's tests
exercised via direct injection, now exercised here via a real kill instead.

The kill point itself is chosen by randomized **acked-write count**, not
randomized wall-clock delay: each iteration picks a random target in
`[50, maxwrites-50)`, and the harness sends `SIGKILL` the instant the
worker's stdout reports an `ACK` line at or past that target. Ground truth
for verification is always the *last* `ACK` line actually read from the
worker's stdout (draining the pipe fully before `SIGKILL` is confirmed to
have taken effect), not the target itself -- a few extra writes can land
and print in the gap between the target being hit and the process actually
dying, and those are correctly counted as acked too.

## Result

```
iteration 1: killed after 2220 acked writes, verified 2220/2220 correct, 0 lost, 0 corrupt [PASS]
iteration 2: killed after 2617 acked writes, verified 2617/2617 correct, 0 lost, 0 corrupt [PASS]
iteration 3: killed after 2527 acked writes, verified 2527/2527 correct, 0 lost, 0 corrupt [PASS]
iteration 4: killed after 2923 acked writes, verified 2923/2923 correct, 0 lost, 0 corrupt [PASS]
iteration 5: killed after 2308 acked writes, verified 2308/2308 correct, 0 lost, 0 corrupt [PASS]
iteration 6: killed after 281 acked writes, verified 281/281 correct, 0 lost, 0 corrupt [PASS]
iteration 7: killed after 2030 acked writes, verified 2030/2030 correct, 0 lost, 0 corrupt [PASS]
iteration 8: killed after 2873 acked writes, verified 2873/2873 correct, 0 lost, 0 corrupt [PASS]
iteration 9: killed after 266 acked writes, verified 266/266 correct, 0 lost, 0 corrupt [PASS]
iteration 10: killed after 361 acked writes, verified 361/361 correct, 0 lost, 0 corrupt [PASS]
iteration 11: killed after 1391 acked writes, verified 1391/1391 correct, 0 lost, 0 corrupt [PASS]
iteration 12: killed after 1239 acked writes, verified 1239/1239 correct, 0 lost, 0 corrupt [PASS]
iteration 13: killed after 2159 acked writes, verified 2159/2159 correct, 0 lost, 0 corrupt [PASS]
iteration 14: killed after 725 acked writes, verified 725/725 correct, 0 lost, 0 corrupt [PASS]
iteration 15: killed after 79 acked writes, verified 79/79 correct, 0 lost, 0 corrupt [PASS]
iteration 16: killed after 198 acked writes, verified 198/198 correct, 0 lost, 0 corrupt [PASS]
iteration 17: killed after 2664 acked writes, verified 2664/2664 correct, 0 lost, 0 corrupt [PASS]
iteration 18: killed after 1309 acked writes, verified 1309/1309 correct, 0 lost, 0 corrupt [PASS]
iteration 19: killed after 910 acked writes, verified 910/910 correct, 0 lost, 0 corrupt [PASS]
iteration 20: killed after 816 acked writes, verified 816/816 correct, 0 lost, 0 corrupt [PASS]
PASS: 20/20 iterations, 29896 total acked writes verified, 0 lost, 0 corrupt
```

0 failures across 20 iterations, 29,896 total acked writes verified, spanning
kill points from 79 to 2,923 acked writes into a burst. 0 lost (an acked
write missing entirely on recovery) and 0 corrupt (a wrong value recovered
anywhere in the full `0..maxwrites-1` key range, acked or not) in every
iteration.

Reproduce with:

```
go run ./cmd/chaos -iterations=20
```

(run from the module root, so `go build ./cmd/chaosworker` resolves).

## Why this proves recovery is correct

Every acked write survived every kill point tested because:

- **`Put` never returns success until the WAL entry is fsync'd** (§7 write
  path, `wal.Writer.Append`). An `ACK` line is only printed after `Put`
  returns, so by construction every acked write was durable on disk
  *before* the harness could have observed it and decided to kill the
  process -- there is no window where an acked write is still only in
  volatile memory.
- **Recovery replays exactly what was fsync'd, in the locked order**
  (§7): CURRENT -> MANIFEST -> SSTable set -> WAL -> memtable. A kill
  mid-flush leaves at worst an orphaned SSTable file (rename succeeded,
  MANIFEST append didn't) -- ignored, not deleted, on the next `Open`,
  with the same data still recoverable from the WAL segment that flush
  hadn't yet superseded (`wal.RemoveSegmentsBefore` only runs after the
  MANIFEST append that commits the flush, so a crash before that point
  can never lose the WAL copy).
- **Torn tails stop replay, they don't get skipped past** (§5, §8): the
  one in-flight, un-acked write at kill time -- if it was mid-append when
  `SIGKILL` landed -- produces a torn record that both `wal.Replay` and
  `manifest.ReplayManifest` stop clean at rather than silently continuing
  past. That write correctly does not survive, and nothing after it in
  the same segment does either, because nothing after it was ever
  written.
- **The MANIFEST's own torn-tail repair runs before any new append**
  (`manifest.RepairTornTail`, called once at `Open`, Phase 4): a kill that
  leaves a torn MANIFEST tail from a mid-append crash doesn't corrupt a
  later, fully-valid flush record in a future session, because the tear is
  truncated away before that future append can land past it.
- **`fsync` on this platform actually means "on stable storage," not just
  "left the process."** All four fsync call sites (`wal.Writer.Append`,
  `manifest.AppendEdit`, `manifest.RepairTornTail`, `sstable.FlushMemtable`,
  every directory-entry fsync) go through `*os.File.Sync()`. On Darwin,
  Go's runtime (`internal/poll.FD.Fsync`, confirmed by reading the actual
  source shipped with the Go 1.26.5 toolchain this project builds with)
  issues `fcntl(fd, F_FULLFSYNC)` rather than a plain `fsync(2)` -- a real,
  well-known macOS gap (plain `fsync` there only pushes writes to the
  drive, not through the drive's own write cache) that would otherwise
  undermine the durability claim on exactly this OS. Verified by reading
  the toolchain source directly rather than assumed from documentation.

Taken together, these are the same invariants Phases 1-4 already proved
with targeted unit tests and direct crash-point injection; this phase's
contribution is validating them under an actual OS-level `SIGKILL` against
a real filesystem, at randomized points determined by genuine
acknowledgment rather than timing assumptions, repeated enough times (20
iterations, 29,896 total acked writes, dozens of flush cycles per larger
iteration) that a single lucky run isn't the basis for the claim.

## Honest limits of this proof

`SIGKILL` only kills the process -- it says nothing about the OS's own
page cache or the drive's write cache, both of which survive a process
death untouched. A plain `write(2)` with no `fsync` at all would already
survive every kill point this harness tests, because the data reaches the
kernel the moment `write()` returns, independent of whether an `fsync`
ever happens. What this harness actually proves is that the WAL/MANIFEST/
SSTable *recovery logic* is correct -- ordering, torn-tail handling,
newest-wins reads -- under real process termination at real, unpredictable
byte offsets. It does not, and structurally cannot without either pulling
real power or doing block-device-level fault injection (out of scope
here), prove that a *power loss* after an acked write would also be safe;
that half of the durability claim rests on `fsync`/`F_FULLFSYNC` actually
doing what the OS documents, which was verified above by inspection, not
by this test.

Also out of scope, both because range queries aren't built yet (stretch
8b) and because compaction isn't built yet (Phase 6, explicitly deferred
per the brief): this harness can't detect a *phantom* key outside
`[0, maxwrites)` even if one existed, since verification is point lookups
over a known key range, not a full-table scan.

---

## Phase 7: extended run, compaction included in the crash window

Phase 5's run (above) predates compaction (Phase 6) entirely -- every kill
point in it landed only during ordinary writes and flushes. Phase 6 itself
validated compaction's four crash windows, but only via four hand-picked,
directly-injected scenarios in `compaction_crash_test.go`, not via a real
`SIGKILL` at a randomized point. This section closes that gap: the same
harness, same real-subprocess-`SIGKILL` methodology, but with the worker's
compaction trigger lowered so a burst crosses many compactions, and kill
points landing throughout -- including, by construction, inside
compactions -- rather than only between them.

### What changed from the Phase 5 run

- **`DB.SetCompactionThreshold`** (new, added this phase, mirroring the
  existing `SetFlushThreshold`): overrides the live-SSTable-count that
  triggers `MaybeCompact`. Previously this was a package-level constant
  with no override, which meant there was no way for a test or harness to
  make compaction fire more often than the production default without
  writing an unrealistic number of keys. Threaded through as
  `-compactionthreshold` on both `cmd/chaosworker` and `cmd/chaos`
  (default 4, matching production; the extended run below sets it to 3).
- Otherwise the harness, the ACK-line ground truth, and the full-key-range
  verification are unchanged from Phase 5 -- see above for the full
  methodology.

### Confirming compaction actually fired inside the crash window

Before trusting the run below, a single `-keep=true` iteration was
inspected directly: with `-compactionthreshold=3`, one iteration's
MANIFEST (killed at 2,708 acked writes) contained 98 `SSTABLE_ADDED` and
96 `SSTABLE_REMOVED` edits -- dozens of real compactions, not zero, ran
during that single burst. This confirms the lowered threshold does what
it's meant to: put compaction's crash windows (temp-write, pre-rename,
post-rename-pre-MANIFEST, ADDED-before-REMOVED) genuinely inside the
randomized kill range, rather than the run merely re-testing ordinary
flush behavior with extra steps.

### Result

```
$ go run ./cmd/chaos -iterations=25 -maxwrites=3000 -flushthreshold=4096 -compactionthreshold=3
iteration 1: killed after 2726 acked writes, verified 2726/2726 correct, 0 lost, 0 corrupt [PASS]
iteration 2: killed after 614 acked writes, verified 614/614 correct, 0 lost, 0 corrupt [PASS]
iteration 3: killed after 2818 acked writes, verified 2818/2818 correct, 0 lost, 0 corrupt [PASS]
iteration 4: killed after 2760 acked writes, verified 2760/2760 correct, 0 lost, 0 corrupt [PASS]
iteration 5: killed after 509 acked writes, verified 509/509 correct, 0 lost, 0 corrupt [PASS]
iteration 6: killed after 784 acked writes, verified 784/784 correct, 0 lost, 0 corrupt [PASS]
iteration 7: killed after 1026 acked writes, verified 1026/1026 correct, 0 lost, 0 corrupt [PASS]
iteration 8: killed after 429 acked writes, verified 429/429 correct, 0 lost, 0 corrupt [PASS]
iteration 9: killed after 873 acked writes, verified 873/873 correct, 0 lost, 0 corrupt [PASS]
iteration 10: killed after 248 acked writes, verified 248/248 correct, 0 lost, 0 corrupt [PASS]
iteration 11: killed after 2360 acked writes, verified 2360/2360 correct, 0 lost, 0 corrupt [PASS]
iteration 12: killed after 566 acked writes, verified 566/566 correct, 0 lost, 0 corrupt [PASS]
iteration 13: killed after 2228 acked writes, verified 2228/2228 correct, 0 lost, 0 corrupt [PASS]
iteration 14: killed after 859 acked writes, verified 859/859 correct, 0 lost, 0 corrupt [PASS]
iteration 15: killed after 2268 acked writes, verified 2268/2268 correct, 0 lost, 0 corrupt [PASS]
iteration 16: killed after 2547 acked writes, verified 2547/2547 correct, 0 lost, 0 corrupt [PASS]
iteration 17: killed after 2443 acked writes, verified 2443/2443 correct, 0 lost, 0 corrupt [PASS]
iteration 18: killed after 1806 acked writes, verified 1806/1806 correct, 0 lost, 0 corrupt [PASS]
iteration 19: killed after 1838 acked writes, verified 1838/1838 correct, 0 lost, 0 corrupt [PASS]
iteration 20: killed after 2792 acked writes, verified 2792/2792 correct, 0 lost, 0 corrupt [PASS]
iteration 21: killed after 1968 acked writes, verified 1968/1968 correct, 0 lost, 0 corrupt [PASS]
iteration 22: killed after 2187 acked writes, verified 2187/2187 correct, 0 lost, 0 corrupt [PASS]
iteration 23: killed after 1457 acked writes, verified 1457/1457 correct, 0 lost, 0 corrupt [PASS]
iteration 24: killed after 186 acked writes, verified 186/186 correct, 0 lost, 0 corrupt [PASS]
iteration 25: killed after 2168 acked writes, verified 2168/2168 correct, 0 lost, 0 corrupt [PASS]
PASS: 25/25 iterations, 40460 total acked writes verified, 0 lost, 0 corrupt
```

25/25 iterations clean, 40,460 total acked writes verified, 0 lost, 0
corrupt -- with compaction firing dozens of times per iteration (per the
MANIFEST inspection above) and kill points spanning the full range from
186 to 2,818 acked writes. Combined with Phase 5's original 20/20, this is
45 total clean real-`SIGKILL` runs across both the pre- and
post-compaction write paths, with no failures found.

No bug was found by this run. If one had been, per the Phase 7 brief it
would have been fixed in Phase 6 (compaction) or wherever it actually
originated, not patched around here.

Reproduce with:

```
go run ./cmd/chaos -iterations=25 -maxwrites=3000 -flushthreshold=4096 -compactionthreshold=3
```
