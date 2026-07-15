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
