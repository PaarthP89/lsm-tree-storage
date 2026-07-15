package lsm

import (
	"bytes"
	"testing"
)

func TestPutGetDeleteRoundTrip(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	if err := db.Put([]byte("user:1"), []byte("alice")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := db.Put([]byte("user:2"), []byte("bob")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	v, found, err := db.Get([]byte("user:1"))
	if err != nil || !found || !bytes.Equal(v, []byte("alice")) {
		t.Fatalf("Get(user:1) = %q found=%v err=%v, want alice true nil", v, found, err)
	}

	if err := db.Put([]byte("user:1"), []byte("alice2")); err != nil {
		t.Fatalf("Put (overwrite): %v", err)
	}
	v, found, err = db.Get([]byte("user:1"))
	if err != nil || !found || !bytes.Equal(v, []byte("alice2")) {
		t.Fatalf("Get(user:1) after overwrite = %q found=%v err=%v, want alice2 true nil", v, found, err)
	}

	if err := db.Delete([]byte("user:1")); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	_, found, err = db.Get([]byte("user:1"))
	if err != nil || found {
		t.Fatalf("Get(user:1) after delete = found=%v err=%v, want false nil", found, err)
	}

	_, found, err = db.Get([]byte("does-not-exist"))
	if err != nil || found {
		t.Fatalf("Get(does-not-exist) = found=%v err=%v, want false nil", found, err)
	}
}

func TestReplayAfterCleanClose(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := db.Put([]byte("user:1"), []byte("alice")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := db.Put([]byte("user:2"), []byte("bob")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := db.Delete([]byte("user:1")); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	db2, err := Open(dir)
	if err != nil {
		t.Fatalf("Open (restart): %v", err)
	}
	defer db2.Close()

	v, found, err := db2.Get([]byte("user:2"))
	if err != nil || !found || !bytes.Equal(v, []byte("bob")) {
		t.Fatalf("Get(user:2) = %q found=%v err=%v, want bob true nil", v, found, err)
	}
	_, found, err = db2.Get([]byte("user:1"))
	if err != nil || found {
		t.Fatalf("Get(user:1) = found=%v err=%v, want false nil -- delete must survive restart", found, err)
	}
}

func TestReplayReproducesFinalStateNotIntermediate(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := db.Put([]byte("a"), []byte("1")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := db.Delete([]byte("a")); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := db.Put([]byte("a"), []byte("2")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	db2, err := Open(dir)
	if err != nil {
		t.Fatalf("Open (restart): %v", err)
	}
	defer db2.Close()

	v, found, err := db2.Get([]byte("a"))
	if err != nil || !found || !bytes.Equal(v, []byte("2")) {
		t.Fatalf("Get(a) = %q found=%v err=%v, want 2 true nil", v, found, err)
	}
}

func TestMultipleRestartsAccumulateState(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := db.Put([]byte("a"), []byte("1")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	db2, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := db2.Put([]byte("b"), []byte("2")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := db2.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	db3, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db3.Close()

	for k, want := range map[string]string{"a": "1", "b": "2"} {
		v, found, err := db3.Get([]byte(k))
		if err != nil || !found || !bytes.Equal(v, []byte(want)) {
			t.Fatalf("Get(%s) = %q found=%v err=%v, want %s true nil", k, v, found, err, want)
		}
	}
}
