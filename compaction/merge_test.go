package compaction

import (
	"reflect"
	"testing"
)

// fakeEntry/fakeIterator are pure in-memory stand-ins for a real SSTable's
// Iterator, so the merge algorithm itself is testable without ever
// touching disk.
type fakeEntry struct {
	key       string
	value     string
	tombstone bool
}

type fakeIterator struct {
	entries []fakeEntry
	idx     int
}

func newFakeIterator(entries ...fakeEntry) *fakeIterator {
	return &fakeIterator{entries: entries, idx: -1}
}

func (f *fakeIterator) Next() bool {
	f.idx++
	return f.idx < len(f.entries)
}
func (f *fakeIterator) Key() []byte     { return []byte(f.entries[f.idx].key) }
func (f *fakeIterator) Value() []byte   { return []byte(f.entries[f.idx].value) }
func (f *fakeIterator) Tombstone() bool { return f.entries[f.idx].tombstone }

func drainMerge(m *MergeIterator) []fakeEntry {
	var out []fakeEntry
	for m.Next() {
		out = append(out, fakeEntry{key: string(m.Key()), value: string(m.Value()), tombstone: m.Tombstone()})
	}
	return out
}

// TestMergeNoOverlapInterleaves confirms a plain k-way merge across
// disjoint key sets produces one fully sorted stream.
func TestMergeNoOverlapInterleaves(t *testing.T) {
	a := newFakeIterator(fakeEntry{"a", "1", false}, fakeEntry{"c", "3", false})
	b := newFakeIterator(fakeEntry{"b", "2", false}, fakeEntry{"d", "4", false})

	m := NewMergeIterator([]Source{{Iter: a, Rank: 0}, {Iter: b, Rank: 1}})
	got := drainMerge(m)
	want := []fakeEntry{
		{"a", "1", false}, {"b", "2", false}, {"c", "3", false}, {"d", "4", false},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("merge = %+v, want %+v", got, want)
	}
}

// TestMergeOverlappingKeyNewestWins is the core compaction correctness
// property: when the same key appears in multiple sources, only the
// value from the lowest-Rank (newest) source survives the merge, and the
// duplicate never appears in the output at all.
func TestMergeOverlappingKeyNewestWins(t *testing.T) {
	newer := newFakeIterator(fakeEntry{"a", "new", false})
	older := newFakeIterator(fakeEntry{"a", "old", false})

	m := NewMergeIterator([]Source{{Iter: newer, Rank: 0}, {Iter: older, Rank: 1}})
	got := drainMerge(m)
	want := []fakeEntry{{"a", "new", false}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("merge = %+v, want %+v (newest-wins, no duplicate)", got, want)
	}
}

// TestMergeThreeWayOverlapPicksLowestRank confirms newest-wins holds with
// more than two sources sharing a key, and that every source's iterator is
// still fully advanced past the shared key (no leaked/stuck entries
// affecting later, distinct keys).
func TestMergeThreeWayOverlapPicksLowestRank(t *testing.T) {
	rank0 := newFakeIterator(fakeEntry{"a", "newest", false}, fakeEntry{"z", "z0", false})
	rank1 := newFakeIterator(fakeEntry{"a", "middle", false})
	rank2 := newFakeIterator(fakeEntry{"a", "oldest", false})

	m := NewMergeIterator([]Source{
		{Iter: rank0, Rank: 0},
		{Iter: rank1, Rank: 1},
		{Iter: rank2, Rank: 2},
	})
	got := drainMerge(m)
	want := []fakeEntry{{"a", "newest", false}, {"z", "z0", false}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("merge = %+v, want %+v", got, want)
	}
}

// TestMergeSurfacesTombstoneAsNewestEntry confirms the merge itself
// treats a tombstone exactly like any other entry for newest-wins
// purposes (dropping tombstones entirely is tombstoneDroppingIterator's
// job, tested separately, not MergeIterator's).
func TestMergeSurfacesTombstoneAsNewestEntry(t *testing.T) {
	newer := newFakeIterator(fakeEntry{"a", "", true}) // tombstone
	older := newFakeIterator(fakeEntry{"a", "old", false})

	m := NewMergeIterator([]Source{{Iter: newer, Rank: 0}, {Iter: older, Rank: 1}})
	got := drainMerge(m)
	want := []fakeEntry{{"a", "", true}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("merge = %+v, want %+v (newest tombstone shadows older value)", got, want)
	}
}

// TestTombstoneDroppingIteratorDropsSurvivingTombstone confirms the
// wrapper Compact actually uses removes a tombstone that's still the
// newest entry for its key at the end of the merge, from the output
// entirely -- this is the CLAUDE.md §7 "Tombstone GC" step, safe only
// because single-level compaction leaves no older data underneath.
func TestTombstoneDroppingIteratorDropsSurvivingTombstone(t *testing.T) {
	newer := newFakeIterator(fakeEntry{"a", "", true}, fakeEntry{"b", "keep", false})
	older := newFakeIterator(fakeEntry{"a", "stale", false})

	m := NewMergeIterator([]Source{{Iter: newer, Rank: 0}, {Iter: older, Rank: 1}})
	out := &tombstoneDroppingIterator{src: m, canDrop: true}

	var got []fakeEntry
	for out.Next() {
		got = append(got, fakeEntry{key: string(out.Key()), value: string(out.Value()), tombstone: out.Tombstone()})
	}
	want := []fakeEntry{{"b", "keep", false}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("tombstone-dropping output = %+v, want %+v (key %q entirely absent)", got, want, "a")
	}
}

// TestTombstoneDroppingIteratorPassesThroughWhenCannotDrop confirms
// canDrop:false (Phase 8d's coverage-not-proven case) surfaces the
// surviving tombstone unchanged instead of dropping it -- the required
// safe behavior whenever a compaction can't prove every source holding a
// possibly-older value for that key was included as an input.
func TestTombstoneDroppingIteratorPassesThroughWhenCannotDrop(t *testing.T) {
	newer := newFakeIterator(fakeEntry{"a", "", true}, fakeEntry{"b", "keep", false})
	older := newFakeIterator(fakeEntry{"a", "stale", false})

	m := NewMergeIterator([]Source{{Iter: newer, Rank: 0}, {Iter: older, Rank: 1}})
	out := &tombstoneDroppingIterator{src: m, canDrop: false}

	var got []fakeEntry
	for out.Next() {
		got = append(got, fakeEntry{key: string(out.Key()), value: string(out.Value()), tombstone: out.Tombstone()})
	}
	want := []fakeEntry{{"a", "", true}, {"b", "keep", false}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("passthrough output = %+v, want %+v (tombstone for %q must survive when coverage isn't proven)", got, want, "a")
	}
}

// TestMergeEmptySources confirms a merge over zero-entry sources produces
// zero output rather than panicking on an empty heap.
func TestMergeEmptySources(t *testing.T) {
	a := newFakeIterator()
	b := newFakeIterator()
	m := NewMergeIterator([]Source{{Iter: a, Rank: 0}, {Iter: b, Rank: 1}})
	if m.Next() {
		t.Fatalf("Next() = true on empty sources, want false")
	}
}
