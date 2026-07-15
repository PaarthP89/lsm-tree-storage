package manifest

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestAppendEditAndReplay(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "MANIFEST-000001")

	edits := []VersionEdit{
		{Type: SSTableAdded, File: "000001.sst"},
		{Type: SSTableAdded, File: "000002.sst"},
		{Type: SSTableRemoved, File: "000001.sst"},
	}
	for _, e := range edits {
		if err := AppendEdit(path, e); err != nil {
			t.Fatalf("AppendEdit(%+v): %v", e, err)
		}
	}

	got, err := ReplayManifest(path)
	if err != nil {
		t.Fatalf("ReplayManifest: %v", err)
	}
	if !reflect.DeepEqual(got, edits) {
		t.Fatalf("ReplayManifest = %+v, want %+v", got, edits)
	}
}

func TestReplayManifestMissingFileIsNotError(t *testing.T) {
	dir := t.TempDir()
	edits, err := ReplayManifest(filepath.Join(dir, "MANIFEST-000001"))
	if err != nil {
		t.Fatalf("ReplayManifest: %v", err)
	}
	if edits != nil {
		t.Fatalf("edits = %+v, want nil", edits)
	}
}

// TestReplayManifestStopsAtTornTail confirms a manifest with a valid
// prefix and a corrupt/truncated final record returns everything decoded
// before the tear, with no error -- same torn-tail discipline as
// wal.Replay, since a MANIFEST is append-only and can only legitimately
// end torn at the point an in-progress append was interrupted by a crash.
func TestReplayManifestStopsAtTornTail(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "MANIFEST-000001")

	if err := AppendEdit(path, VersionEdit{Type: SSTableAdded, File: "000001.sst"}); err != nil {
		t.Fatalf("AppendEdit: %v", err)
	}

	// Append a torn record directly (bypassing AppendEdit's fsync
	// discipline) by writing a truncated encoded edit.
	torn := EncodeEdit(VersionEdit{Type: SSTableAdded, File: "000002.sst"})
	torn = torn[:len(torn)-3]
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	if _, err := f.Write(torn); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	got, err := ReplayManifest(path)
	if err != nil {
		t.Fatalf("ReplayManifest: %v", err)
	}
	want := []VersionEdit{{Type: SSTableAdded, File: "000001.sst"}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ReplayManifest = %+v, want %+v", got, want)
	}
}

func TestAppendEditFsyncsDirOnFreshFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "MANIFEST-000001")

	var fsyncedDirs []string
	orig := fsyncDir
	fsyncDir = func(d string) error {
		fsyncedDirs = append(fsyncedDirs, d)
		return orig(d)
	}
	defer func() { fsyncDir = orig }()

	if err := AppendEdit(path, VersionEdit{Type: SSTableAdded, File: "000001.sst"}); err != nil {
		t.Fatalf("AppendEdit: %v", err)
	}
	if len(fsyncedDirs) != 1 || fsyncedDirs[0] != dir {
		t.Fatalf("fsyncedDirs = %v, want exactly [%s] (fresh file must fsync its directory)", fsyncedDirs, dir)
	}

	fsyncedDirs = nil
	if err := AppendEdit(path, VersionEdit{Type: SSTableAdded, File: "000002.sst"}); err != nil {
		t.Fatalf("AppendEdit: %v", err)
	}
	if len(fsyncedDirs) != 0 {
		t.Fatalf("fsyncedDirs = %v, want none (appending to an existing file shouldn't re-fsync the dir)", fsyncedDirs)
	}
}

// writeTornManifest writes a valid record followed by a truncated
// (torn) one directly to path, bypassing AppendEdit's own durability
// discipline -- this simulates the on-disk aftermath of a crash that
// hit partway through an AppendEdit call.
func writeTornManifest(t *testing.T, path string, valid VersionEdit, torn VersionEdit) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	if _, err := f.Write(EncodeEdit(valid)); err != nil {
		t.Fatalf("Write valid: %v", err)
	}
	tornBuf := EncodeEdit(torn)
	tornBuf = tornBuf[:len(tornBuf)-3]
	if _, err := f.Write(tornBuf); err != nil {
		t.Fatalf("Write torn: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestRepairTornTailTruncatesToLastValidRecord(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "MANIFEST-000001")

	valid := VersionEdit{Type: SSTableAdded, File: "000001.sst"}
	torn := VersionEdit{Type: SSTableAdded, File: "000002.sst"}
	writeTornManifest(t, path, valid, torn)

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	sizeBeforeRepair := info.Size()

	if err := RepairTornTail(path); err != nil {
		t.Fatalf("RepairTornTail: %v", err)
	}

	info, err = os.Stat(path)
	if err != nil {
		t.Fatalf("Stat after repair: %v", err)
	}
	wantSize := int64(len(EncodeEdit(valid)))
	if info.Size() != wantSize {
		t.Fatalf("size after repair = %d, want %d (before repair: %d)", info.Size(), wantSize, sizeBeforeRepair)
	}

	got, err := ReplayManifest(path)
	if err != nil {
		t.Fatalf("ReplayManifest: %v", err)
	}
	if !reflect.DeepEqual(got, []VersionEdit{valid}) {
		t.Fatalf("ReplayManifest = %+v, want [%+v]", got, valid)
	}
}

// TestRepairTornTailThenAppendDoesNotCorruptFutureRecords is the test
// that actually proves the misalignment bug: without RepairTornTail, an
// AppendEdit call made after a crash-time torn tail would land its
// fully valid, fully fsynced record at a byte-misaligned offset (right
// after the torn garbage), making that new record permanently
// unreadable by ReplayManifest -- which stops at the *first* torn
// record it finds and never looks past it, even at later, fully valid
// records in the same file. This confirms repairing first closes that
// gap: after RepairTornTail, a new AppendEdit lands cleanly, and replay
// recovers both the original valid record and the new one.
func TestRepairTornTailThenAppendDoesNotCorruptFutureRecords(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "MANIFEST-000001")

	valid := VersionEdit{Type: SSTableAdded, File: "000001.sst"}
	torn := VersionEdit{Type: SSTableAdded, File: "000002.sst"}
	writeTornManifest(t, path, valid, torn)

	if err := RepairTornTail(path); err != nil {
		t.Fatalf("RepairTornTail: %v", err)
	}

	newEdit := VersionEdit{Type: SSTableAdded, File: "000003.sst"}
	if err := AppendEdit(path, newEdit); err != nil {
		t.Fatalf("AppendEdit: %v", err)
	}

	got, err := ReplayManifest(path)
	if err != nil {
		t.Fatalf("ReplayManifest: %v", err)
	}
	want := []VersionEdit{valid, newEdit}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ReplayManifest = %+v, want %+v -- the post-repair append must be readable, not silently lost to misalignment", got, want)
	}
}

// TestAppendEditWithoutRepairCorruptsFutureRecords is the negative
// control for the test above: it proves the bug is real by skipping
// RepairTornTail and showing AppendEdit's naive append lands the new
// record at a misaligned offset, making it unreadable.
func TestAppendEditWithoutRepairCorruptsFutureRecords(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "MANIFEST-000001")

	valid := VersionEdit{Type: SSTableAdded, File: "000001.sst"}
	torn := VersionEdit{Type: SSTableAdded, File: "000002.sst"}
	writeTornManifest(t, path, valid, torn)

	newEdit := VersionEdit{Type: SSTableAdded, File: "000003.sst"}
	if err := AppendEdit(path, newEdit); err != nil {
		t.Fatalf("AppendEdit: %v", err)
	}

	got, err := ReplayManifest(path)
	if err != nil {
		t.Fatalf("ReplayManifest: %v", err)
	}
	// Without repair, replay stops at the pre-existing torn record and
	// never reaches newEdit, even though it was written completely and
	// fsynced -- this is the bug RepairTornTail exists to prevent.
	want := []VersionEdit{valid}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ReplayManifest = %+v, want %+v (this test documents the pre-repair failure mode, not desired behavior)", got, want)
	}
}

func TestRepairTornTailNoOpOnCleanFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "MANIFEST-000001")

	if err := AppendEdit(path, VersionEdit{Type: SSTableAdded, File: "000001.sst"}); err != nil {
		t.Fatalf("AppendEdit: %v", err)
	}
	if err := AppendEdit(path, VersionEdit{Type: SSTableAdded, File: "000002.sst"}); err != nil {
		t.Fatalf("AppendEdit: %v", err)
	}

	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}

	if err := RepairTornTail(path); err != nil {
		t.Fatalf("RepairTornTail: %v", err)
	}

	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile after repair: %v", err)
	}
	if !reflect.DeepEqual(before, after) {
		t.Fatal("RepairTornTail modified an already-clean file")
	}
}

func TestRepairTornTailMissingFileIsNoOp(t *testing.T) {
	dir := t.TempDir()
	if err := RepairTornTail(filepath.Join(dir, "MANIFEST-000001")); err != nil {
		t.Fatalf("RepairTornTail: %v", err)
	}
}

func TestRepairTornTailAllTornTruncatesToEmpty(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "MANIFEST-000001")

	torn := EncodeEdit(VersionEdit{Type: SSTableAdded, File: "000001.sst"})
	torn = torn[:len(torn)-3]
	if err := os.WriteFile(path, torn, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	if err := RepairTornTail(path); err != nil {
		t.Fatalf("RepairTornTail: %v", err)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if info.Size() != 0 {
		t.Fatalf("size after repair = %d, want 0", info.Size())
	}
}

func TestReconstructSSTableSet(t *testing.T) {
	cases := []struct {
		name  string
		edits []VersionEdit
		want  []string
	}{
		{
			name:  "empty",
			edits: nil,
			want:  []string{},
		},
		{
			name: "adds only",
			edits: []VersionEdit{
				{Type: SSTableAdded, File: "000002.sst"},
				{Type: SSTableAdded, File: "000001.sst"},
			},
			want: []string{"000001.sst", "000002.sst"},
		},
		{
			name: "add then remove",
			edits: []VersionEdit{
				{Type: SSTableAdded, File: "000001.sst"},
				{Type: SSTableAdded, File: "000002.sst"},
				{Type: SSTableRemoved, File: "000001.sst"},
			},
			want: []string{"000002.sst"},
		},
		{
			name: "add, remove, re-add",
			edits: []VersionEdit{
				{Type: SSTableAdded, File: "000001.sst"},
				{Type: SSTableRemoved, File: "000001.sst"},
				{Type: SSTableAdded, File: "000001.sst"},
			},
			want: []string{"000001.sst"},
		},
		{
			name: "compaction-shaped: two inputs removed, one output added",
			edits: []VersionEdit{
				{Type: SSTableAdded, File: "000001.sst"},
				{Type: SSTableAdded, File: "000002.sst"},
				{Type: SSTableAdded, File: "000003.sst"}, // compaction output
				{Type: SSTableRemoved, File: "000001.sst"},
				{Type: SSTableRemoved, File: "000002.sst"},
			},
			want: []string{"000003.sst"},
		},
		{
			name: "remove of never-added file is a no-op",
			edits: []VersionEdit{
				{Type: SSTableRemoved, File: "000099.sst"},
				{Type: SSTableAdded, File: "000001.sst"},
			},
			want: []string{"000001.sst"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := ReconstructSSTableSet(tc.edits)
			if len(got) != len(tc.want) {
				t.Fatalf("ReconstructSSTableSet = %v, want %v", got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Fatalf("ReconstructSSTableSet = %v, want %v", got, tc.want)
				}
			}
		})
	}
}
