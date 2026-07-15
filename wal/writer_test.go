package wal

import (
	"os"
	"path/filepath"
	"testing"
)

func TestAppendAndReplay(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWriter(filepath.Join(dir, "000001.log"))
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	want := []Entry{
		{Op: OpPut, Key: []byte("a"), Value: []byte("1")},
		{Op: OpPut, Key: []byte("b"), Value: []byte("2")},
		{Op: OpDelete, Key: []byte("a")},
	}
	for _, e := range want {
		if err := w.Append(e); err != nil {
			t.Fatalf("Append(%+v): %v", e, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	got, err := Replay(dir)
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("got %d entries, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i].Op != want[i].Op || string(got[i].Key) != string(want[i].Key) || string(got[i].Value) != string(want[i].Value) {
			t.Errorf("entry %d = %+v, want %+v", i, got[i], want[i])
		}
	}
}

// spySegmentFile wraps a real *os.File (so behavior stays correct) and
// records the order Write/Sync/Close are invoked in, so tests can assert
// on ordering rather than on side effects that would hold even without a
// real fsync call (e.g. write visibility to a second handle on the same
// machine -- the page cache provides that with or without fsync; fsync is
// about crash durability, not concurrent-reader visibility).
type spySegmentFile struct {
	*os.File
	calls *[]string
}

func (s spySegmentFile) Write(p []byte) (int, error) {
	*s.calls = append(*s.calls, "Write")
	return s.File.Write(p)
}

func (s spySegmentFile) Sync() error {
	*s.calls = append(*s.calls, "Sync")
	return s.File.Sync()
}

func TestAppendSyncsBeforeReturning(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "000001.log")
	w, err := NewWriter(path)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	defer w.Close()

	var calls []string
	realFile := w.f.(*os.File)
	w.f = spySegmentFile{File: realFile, calls: &calls}

	if err := w.Append(Entry{Op: OpPut, Key: []byte("a"), Value: []byte("1")}); err != nil {
		t.Fatalf("Append: %v", err)
	}

	if len(calls) < 2 || calls[0] != "Write" || calls[1] != "Sync" {
		t.Fatalf("call order = %v, want [Write Sync ...] -- Append must fsync before returning", calls)
	}
}

func TestReplayTornTail(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "000001.log")
	w, err := NewWriter(path)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	for i := 0; i < 5; i++ {
		e := Entry{Op: OpPut, Key: []byte{byte('a' + i)}, Value: []byte{byte('0' + i)}}
		if err := w.Append(e); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if err := os.Truncate(path, info.Size()-3); err != nil {
		t.Fatalf("Truncate: %v", err)
	}

	got, err := Replay(dir)
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if len(got) != 4 {
		t.Fatalf("got %d entries, want 4", len(got))
	}
}

func TestSegmentRotation(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWriter(filepath.Join(dir, "000001.log"))
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	// Small enough that a handful of tiny entries force multiple rotations.
	w.SetMaxSegmentBytes(40)

	var want []Entry
	for i := 0; i < 10; i++ {
		e := Entry{Op: OpPut, Key: []byte{byte('a' + i)}, Value: []byte{byte('0' + i)}}
		want = append(want, e)
		if err := w.Append(e); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	segments, err := segmentNames(dir)
	if err != nil {
		t.Fatalf("segmentNames: %v", err)
	}
	if len(segments) < 2 {
		t.Fatalf("got %d segments, want >= 2 (rotation didn't happen)", len(segments))
	}

	got, err := Replay(dir)
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("got %d entries, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i].Op != want[i].Op || string(got[i].Key) != string(want[i].Key) || string(got[i].Value) != string(want[i].Value) {
			t.Errorf("entry %d = %+v, want %+v", i, got[i], want[i])
		}
	}
}

// TestAppendAfterTornTailIsUnreachable documents a real footgun: if a
// caller reopens the *last* segment after a crash (instead of using
// NextSegmentPath) and appends past a torn tail, those new entries are
// permanently lost on replay, along with anything else in that segment.
// This is why NextSegmentPath exists — see its doc comment.
func TestAppendAfterTornTailIsUnreachable(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "000001.log")

	w, err := NewWriter(path)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if err := w.Append(Entry{Op: OpPut, Key: []byte("a"), Value: []byte("1")}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if err := os.Truncate(path, info.Size()-3); err != nil {
		t.Fatalf("Truncate: %v", err)
	}

	// Wrong recovery: reopen the same (now torn) segment and keep writing.
	w2, err := NewWriter(path)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if err := w2.Append(Entry{Op: OpPut, Key: []byte("b"), Value: []byte("2")}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := w2.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	got, err := Replay(dir)
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("got %d entries, want 0 (both records are unreachable behind the torn tail) -- if this now passes with len(got)==2, NextSegmentPath is no longer needed and this test/comment should be revisited", len(got))
	}
}

// TestNextSegmentPathAvoidsTornTailFootgun proves the correct recovery
// pattern: after Replay, resume writes via NextSegmentPath rather than
// reopening the last segment, and no data is lost.
func TestNextSegmentPathAvoidsTornTailFootgun(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "000001.log")

	w, err := NewWriter(path)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if err := w.Append(Entry{Op: OpPut, Key: []byte("a"), Value: []byte("1")}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if err := os.Truncate(path, info.Size()-3); err != nil {
		t.Fatalf("Truncate: %v", err)
	}

	// Simulate crash recovery: replay first (as an engine restart would),
	// then resume writes on a fresh segment.
	if _, err := Replay(dir); err != nil {
		t.Fatalf("Replay: %v", err)
	}
	next, err := NextSegmentPath(dir)
	if err != nil {
		t.Fatalf("NextSegmentPath: %v", err)
	}
	if filepath.Base(next) != "000002.log" {
		t.Fatalf("NextSegmentPath = %q, want 000002.log", next)
	}

	w2, err := NewWriter(next)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if err := w2.Append(Entry{Op: OpPut, Key: []byte("b"), Value: []byte("2")}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := w2.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	got, err := Replay(dir)
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	// "a" is unrecoverable (it was genuinely torn, mid-write, at crash
	// time), but "b" -- written after recovery, into a fresh segment --
	// must survive.
	if len(got) != 1 || string(got[0].Key) != "b" {
		t.Fatalf("got %+v, want exactly [{Put b 2}]", got)
	}
}

func TestNextSegmentPathEmptyDir(t *testing.T) {
	dir := t.TempDir()
	path, err := NextSegmentPath(dir)
	if err != nil {
		t.Fatalf("NextSegmentPath: %v", err)
	}
	if filepath.Base(path) != "000001.log" {
		t.Fatalf("NextSegmentPath = %q, want 000001.log", path)
	}
}

func TestNextSegmentPathMissingDir(t *testing.T) {
	path, err := NextSegmentPath(filepath.Join(t.TempDir(), "does-not-exist"))
	if err != nil {
		t.Fatalf("NextSegmentPath: %v", err)
	}
	if filepath.Base(path) != "000001.log" {
		t.Fatalf("NextSegmentPath = %q, want 000001.log", path)
	}
}

func TestReplayEmptyDir(t *testing.T) {
	dir := t.TempDir()
	got, err := Replay(dir)
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("got %d entries, want 0", len(got))
	}
}

func TestReplayMissingDir(t *testing.T) {
	got, err := Replay(filepath.Join(t.TempDir(), "does-not-exist"))
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("got %d entries, want 0", len(got))
	}
}
