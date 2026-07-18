package memtable

import (
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"testing"
)

func TestGetEmpty(t *testing.T) {
	s := New()
	_, found, tombstone := s.Get([]byte("a"))
	if found || tombstone {
		t.Fatalf("Get on empty list = found=%v tombstone=%v, want false, false", found, tombstone)
	}
}

func TestPutGetSingle(t *testing.T) {
	s := New()
	s.Put([]byte("a"), []byte("1"))

	v, found, tombstone := s.Get([]byte("a"))
	if !found || tombstone {
		t.Fatalf("Get(a) = found=%v tombstone=%v, want true, false", found, tombstone)
	}
	if !bytes.Equal(v, []byte("1")) {
		t.Fatalf("Get(a) = %q, want %q", v, "1")
	}

	if _, found, _ := s.Get([]byte("missing")); found {
		t.Fatalf("Get(missing) found = true, want false")
	}
}

func TestPutOverwrite(t *testing.T) {
	s := New()
	s.Put([]byte("a"), []byte("1"))
	s.Put([]byte("a"), []byte("2"))

	v, found, _ := s.Get([]byte("a"))
	if !found || !bytes.Equal(v, []byte("2")) {
		t.Fatalf("Get(a) = %q, found=%v, want %q, true", v, found, "2")
	}
}

func TestDeleteTombstone(t *testing.T) {
	s := New()
	s.Put([]byte("a"), []byte("1"))
	s.Delete([]byte("a"))

	v, found, tombstone := s.Get([]byte("a"))
	if !found || !tombstone {
		t.Fatalf("Get(a) after delete = found=%v tombstone=%v, want true, true", found, tombstone)
	}
	if v != nil {
		t.Fatalf("Get(a) after delete value = %q, want nil", v)
	}
}

func TestDeleteThenPutRevives(t *testing.T) {
	s := New()
	s.Put([]byte("a"), []byte("1"))
	s.Delete([]byte("a"))
	s.Put([]byte("a"), []byte("2"))

	v, found, tombstone := s.Get([]byte("a"))
	if !found || tombstone || !bytes.Equal(v, []byte("2")) {
		t.Fatalf("Get(a) = %q found=%v tombstone=%v, want %q true false", v, found, tombstone, "2")
	}
}

func TestDeleteOfMissingKeyCreatesTombstone(t *testing.T) {
	s := New()
	s.Delete([]byte("a"))

	_, found, tombstone := s.Get([]byte("a"))
	if !found || !tombstone {
		t.Fatalf("Get(a) = found=%v tombstone=%v, want true, true", found, tombstone)
	}
}

func TestIteratorEmpty(t *testing.T) {
	s := New()
	it := s.Iterator()
	if it.Next() {
		t.Fatalf("Next() on empty list = true, want false")
	}
}

func TestIteratorSingle(t *testing.T) {
	s := New()
	s.Put([]byte("a"), []byte("1"))

	it := s.Iterator()
	if !it.Next() {
		t.Fatalf("Next() = false, want true")
	}
	if !bytes.Equal(it.Key(), []byte("a")) || !bytes.Equal(it.Value(), []byte("1")) {
		t.Fatalf("Key/Value = %q/%q, want a/1", it.Key(), it.Value())
	}
	if it.Next() {
		t.Fatalf("second Next() = true, want false")
	}
}

func TestIteratorSortedOrder(t *testing.T) {
	s := New()
	keys := []string{"delta", "alpha", "charlie", "echo", "bravo"}
	for _, k := range keys {
		s.Put([]byte(k), []byte("v-"+k))
	}

	var got []string
	it := s.Iterator()
	for it.Next() {
		got = append(got, string(it.Key()))
	}

	want := []string{"alpha", "bravo", "charlie", "delta", "echo"}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v, want %v", got, want)
		}
	}
}

func TestIteratorReflectsTombstones(t *testing.T) {
	s := New()
	s.Put([]byte("a"), []byte("1"))
	s.Put([]byte("b"), []byte("2"))
	s.Delete([]byte("a"))

	it := s.Iterator()
	seen := map[string]bool{}
	for it.Next() {
		if string(it.Key()) == "a" && !it.Tombstone() {
			t.Fatalf("key a: Tombstone() = false, want true")
		}
		if string(it.Key()) == "b" && it.Tombstone() {
			t.Fatalf("key b: Tombstone() = true, want false")
		}
		seen[string(it.Key())] = true
	}
	if !seen["a"] || !seen["b"] {
		t.Fatalf("seen = %v, want both a and b", seen)
	}
}

// TestSizeBytesGrowsAndTracksOverwrite confirms SizeBytes accounts for a
// new node's real memory cost (raw key+value bytes *plus* the node's own
// struct and pointer overhead -- not just the user's data, which would
// let actual heap usage run ahead of a caller's flush threshold), and
// that overwriting an existing key changes only the value-byte delta,
// never re-charging the per-node overhead a second time (no new node is
// created on that path).
func TestSizeBytesGrowsAndTracksOverwrite(t *testing.T) {
	s := New()
	if s.SizeBytes() != 0 {
		t.Fatalf("SizeBytes() on empty list = %d, want 0", s.SizeBytes())
	}

	s.Put([]byte("a"), []byte("1"))
	afterFirst := s.SizeBytes()
	rawFirst := len("a") + len("1")
	if afterFirst <= rawFirst {
		t.Fatalf("SizeBytes() = %d, want > %d (raw key+value bytes) -- a new node must also count its own struct/pointer overhead", afterFirst, rawFirst)
	}
	// lvl is always in [1, maxLevel], so the added overhead is bounded
	// even though the exact level chosen for this node is random.
	minOverhead := nodeStructOverheadBytes + 1*pointerBytes
	maxOverhead := nodeStructOverheadBytes + maxLevel*pointerBytes
	if want := rawFirst + minOverhead; afterFirst < want {
		t.Fatalf("SizeBytes() = %d, want >= %d (raw bytes + minimum possible node overhead)", afterFirst, want)
	}
	if want := rawFirst + maxOverhead; afterFirst > want {
		t.Fatalf("SizeBytes() = %d, want <= %d (raw bytes + maximum possible node overhead)", afterFirst, want)
	}

	s.Put([]byte("a"), []byte("longer-value"))
	afterOverwrite := s.SizeBytes()
	wantDelta := len("longer-value") - len("1")
	if got := afterOverwrite - afterFirst; got != wantDelta {
		t.Fatalf("SizeBytes() delta after overwrite = %d, want %d -- overwriting an existing key must not re-charge per-node struct overhead", got, wantDelta)
	}
}

// TestGetReturnsIndependentCopy guards against an aliasing bug: Get must
// not return the node's own backing array, since SkipList is the memtable
// behind an embedded library's DB.Get -- callers hold the returned slice
// past the call, and a caller mutating it in place must never be able to
// corrupt what a later Get of the same key returns.
func TestGetReturnsIndependentCopy(t *testing.T) {
	s := New()
	s.Put([]byte("a"), []byte("original"))

	v, _, _ := s.Get([]byte("a"))
	v[0] = 'X'

	v2, _, _ := s.Get([]byte("a"))
	if !bytes.Equal(v2, []byte("original")) {
		t.Fatalf("Get(a) after mutating a previous Get's result = %q, want %q -- Get must return an independent copy", v2, "original")
	}
}

func TestManyKeysSortedAndRetrievable(t *testing.T) {
	s := New()
	n := 2000
	perm := rand.Perm(n)
	for _, i := range perm {
		key := []byte(fmt.Sprintf("key-%05d", i))
		value := []byte(fmt.Sprintf("val-%05d", i))
		s.Put(key, value)
	}

	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("key-%05d", i))
		want := []byte(fmt.Sprintf("val-%05d", i))
		v, found, tombstone := s.Get(key)
		if !found || tombstone || !bytes.Equal(v, want) {
			t.Fatalf("Get(%s) = %q found=%v tombstone=%v, want %q true false", key, v, found, tombstone, want)
		}
	}

	it := s.Iterator()
	prev := ""
	count := 0
	for it.Next() {
		k := string(it.Key())
		if prev != "" && k <= prev {
			t.Fatalf("iterator out of order: %q then %q", prev, k)
		}
		prev = k
		count++
	}
	if count != n {
		t.Fatalf("iterator yielded %d entries, want %d", count, n)
	}
}

// TestConcurrentPutGetDelete exercises the "single RWMutex guards the
// whole structure" concurrency claim with actual concurrent goroutines,
// under the race detector. Each goroutine owns a disjoint key range so
// the assertions stay deterministic despite concurrent execution.
func TestConcurrentPutGetDelete(t *testing.T) {
	s := New()
	const goroutines = 16
	const perGoroutine = 200

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				key := []byte(fmt.Sprintf("g%02d-key-%04d", g, i))
				value := []byte(fmt.Sprintf("g%02d-val-%04d", g, i))
				s.Put(key, value)
				if _, found, _ := s.Get(key); !found {
					t.Errorf("Get(%s) immediately after Put = not found", key)
				}
				if i%2 == 0 {
					s.Delete(key)
				}
				_ = s.SizeBytes()
			}
		}(g)
	}
	wg.Wait()

	for g := 0; g < goroutines; g++ {
		for i := 0; i < perGoroutine; i++ {
			key := []byte(fmt.Sprintf("g%02d-key-%04d", g, i))
			v, found, tombstone := s.Get(key)
			if !found {
				t.Fatalf("Get(%s) = not found, want present (as value or tombstone)", key)
			}
			if i%2 == 0 {
				if !tombstone {
					t.Fatalf("Get(%s) tombstone = false, want true (deleted)", key)
				}
				continue
			}
			want := []byte(fmt.Sprintf("g%02d-val-%04d", g, i))
			if tombstone || !bytes.Equal(v, want) {
				t.Fatalf("Get(%s) = %q tombstone=%v, want %q false", key, v, tombstone, want)
			}
		}
	}
}
