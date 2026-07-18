package compaction

import (
	"path/filepath"
	"testing"

	"github.com/paarthsiphone/lsm-tree-storage/memtable"
	"github.com/paarthsiphone/lsm-tree-storage/sstable"
)

func flushLeveledTestTable(t *testing.T, dir, name string, puts map[string]string, deletes []string) *sstable.SSTable {
	t.Helper()
	mem := memtable.New()
	for k, v := range puts {
		mem.Put([]byte(k), []byte(v))
	}
	for _, k := range deletes {
		mem.Delete([]byte(k))
	}
	path := filepath.Join(dir, name)
	if _, err := sstable.FlushMemtable(mem, path); err != nil {
		t.Fatalf("FlushMemtable(%s): %v", name, err)
	}
	st, err := sstable.OpenSSTable(path)
	if err != nil {
		t.Fatalf("OpenSSTable(%s): %v", name, err)
	}
	t.Cleanup(func() { st.Close() })
	return st
}

// TestCompactLeveledDropsTombstoneWhenCoverageComplete is the positive
// case: a merge whose inputs genuinely include every source holding data
// for the tombstoned key ("x") is safe to drop it from -- there is
// nothing else, anywhere, that could resurface as a stale value once the
// tombstone is gone.
func TestCompactLeveledDropsTombstoneWhenCoverageComplete(t *testing.T) {
	dir := t.TempDir()

	older := flushLeveledTestTable(t, dir, "000001.sst", map[string]string{"x": "stale-value"}, nil)
	newer := flushLeveledTestTable(t, dir, "000002.sst", nil, []string{"x"}) // tombstone for "x"

	// newest-first: newer (the tombstone) is rank 0.
	inputs := []*sstable.SSTable{newer, older}
	outPath := filepath.Join(dir, "000003.sst")
	if _, err := CompactLeveled(inputs, outPath, true); err != nil {
		t.Fatalf("CompactLeveled: %v", err)
	}

	out, err := sstable.OpenSSTable(outPath)
	if err != nil {
		t.Fatalf("OpenSSTable(output): %v", err)
	}
	defer out.Close()

	_, found, _, err := out.Get([]byte("x"))
	if err != nil || found {
		t.Fatalf("Get(x) on output = found=%v err=%v, want false nil -- tombstone correctly dropped, nothing left to resurface", found, err)
	}
}

// TestCompactLeveledIncompleteCoverageWithDropTrueResurrectsStaleData is
// the adversarial negative case this phase's tombstone-drop condition
// exists to prevent: a merge that does NOT include every source holding
// data for the tombstoned key ("x" also lives in excludedOlder, which is
// deliberately left OUT of this merge's inputs) but is told
// canDropTombstones=true anyway. This documents the real failure mode --
// a caller that gets the coverage boolean wrong causes silent data
// resurrection -- not a behavior this codebase's own compaction paths
// are allowed to trigger (see DB.compactL0ToL1IfNeeded/compactL1IfNeeded's
// coverage proofs), but one CompactLeveled itself cannot prevent, since it
// has no way to know what its caller left out.
func TestCompactLeveledIncompleteCoverageWithDropTrueResurrectsStaleData(t *testing.T) {
	dir := t.TempDir()

	excludedOlder := flushLeveledTestTable(t, dir, "000001.sst", map[string]string{"x": "stale-value"}, nil)
	newer := flushLeveledTestTable(t, dir, "000002.sst", nil, []string{"x"}) // tombstone for "x"

	// Coverage is incomplete: excludedOlder is never passed to CompactLeveled.
	inputs := []*sstable.SSTable{newer}
	outPath := filepath.Join(dir, "000003.sst")
	if _, err := CompactLeveled(inputs, outPath, true); err != nil {
		t.Fatalf("CompactLeveled: %v", err)
	}

	out, err := sstable.OpenSSTable(outPath)
	if err != nil {
		t.Fatalf("OpenSSTable(output): %v", err)
	}
	defer out.Close()

	_, found, _, err := out.Get([]byte("x"))
	if err != nil || found {
		t.Fatalf("Get(x) on output = found=%v err=%v, want false nil (tombstone dropped from the merge output itself)", found, err)
	}

	// The danger: a reader that checks the compacted output (newest) and
	// then falls through to excludedOlder (the source left out of the
	// merge) resurrects the stale value, because nothing in the live set
	// remembers "x" was deleted anymore.
	v, found, tombstone, err := excludedOlder.Get([]byte("x"))
	if err != nil || !found || tombstone || string(v) != "stale-value" {
		t.Fatalf("Get(x) on excludedOlder = %q found=%v tombstone=%v err=%v, want stale-value true false nil -- proving the resurrection risk is real", v, found, tombstone, err)
	}
}

// TestCompactLeveledIncompleteCoverageWithDropFalsePreservesTombstone is
// the safe counterpart: the exact same incomplete-coverage merge as
// above, but with canDropTombstones=false. The tombstone must survive
// into the output, so a reader checking the output first still correctly
// sees "x" as deleted and never falls through to the stale value.
func TestCompactLeveledIncompleteCoverageWithDropFalsePreservesTombstone(t *testing.T) {
	dir := t.TempDir()

	flushLeveledTestTable(t, dir, "000001.sst", map[string]string{"x": "stale-value"}, nil)
	newer := flushLeveledTestTable(t, dir, "000002.sst", nil, []string{"x"})

	inputs := []*sstable.SSTable{newer}
	outPath := filepath.Join(dir, "000003.sst")
	if _, err := CompactLeveled(inputs, outPath, false); err != nil {
		t.Fatalf("CompactLeveled: %v", err)
	}

	out, err := sstable.OpenSSTable(outPath)
	if err != nil {
		t.Fatalf("OpenSSTable(output): %v", err)
	}
	defer out.Close()

	_, found, tombstone, err := out.Get([]byte("x"))
	if err != nil || !found || !tombstone {
		t.Fatalf("Get(x) on output = found=%v tombstone=%v err=%v, want true true nil -- tombstone must survive when coverage isn't proven", found, tombstone, err)
	}
}
