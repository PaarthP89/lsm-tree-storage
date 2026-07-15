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

func TestSizeBytesGrowsAndTracksOverwrite(t *testing.T) {
	s := New()
	if s.SizeBytes() != 0 {
		t.Fatalf("SizeBytes() on empty list = %d, want 0", s.SizeBytes())
	}

	s.Put([]byte("a"), []byte("1"))
	afterFirst := s.SizeBytes()
	if afterFirst != len("a")+len("1") {
		t.Fatalf("SizeBytes() = %d, want %d", afterFirst, len("a")+len("1"))
	}

	s.Put([]byte("a"), []byte("longer-value"))
	afterOverwrite := s.SizeBytes()
	if afterOverwrite != len("a")+len("longer-value") {
		t.Fatalf("SizeBytes() after overwrite = %d, want %d", afterOverwrite, len("a")+len("longer-value"))
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
