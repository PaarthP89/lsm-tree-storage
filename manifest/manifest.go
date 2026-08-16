package manifest

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
)

// InitialFileName is the MANIFEST filename a fresh database creates on
// its very first Open. Manifest rotation isn't in scope for this phase
// (§9/§11 -- Minimum/Target tier assumes one long-lived MANIFEST file),
// so there's no naming scheme to generate beyond this single constant.
const InitialFileName = "MANIFEST-000001"

// AppendEdit serializes edit and appends it to the MANIFEST file at
// manifestPath, fsyncing before returning -- same durability discipline
// as wal.Writer.Append: the caller must not consider whatever operation
// this edit records (e.g. a flush) complete until this returns nil. If
// manifestPath doesn't exist yet, it's created, and the containing
// directory is fsynced too (a fresh file's directory entry needs the same
// durability treatment a fresh WAL segment does -- see wal.NewWriter's
// fsyncDir).
func AppendEdit(manifestPath string, edit VersionEdit) error {
	_, statErr := os.Stat(manifestPath)
	isNew := os.IsNotExist(statErr)

	f, err := os.OpenFile(manifestPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("manifest: open %s: %w", manifestPath, err)
	}
	defer f.Close()

	if _, err := f.Write(EncodeEdit(edit)); err != nil {
		return fmt.Errorf("manifest: write %s: %w", manifestPath, err)
	}
	if err := f.Sync(); err != nil {
		return fmt.Errorf("manifest: fsync %s: %w", manifestPath, err)
	}

	if isNew {
		if err := fsyncDir(filepath.Dir(manifestPath)); err != nil {
			return fmt.Errorf("manifest: fsync dir for %s: %w", manifestPath, err)
		}
	}
	return nil
}

// RepairTornTail truncates manifestPath back to the end of its last
// fully valid, checksummed record, discarding any trailing torn or
// corrupt bytes. Callers must call this once, at Open time, before any
// future AppendEdit call against this file.
//
// This exists for the same reason wal.ErrTornSegment exists: appending
// past a torn tail would make the newly appended record permanently
// unreachable, since ReplayManifest -- like wal.Replay -- never looks
// past the first torn record it finds. wal solves this by always
// resuming writes on a brand-new segment after a crash (see
// wal.NextSegmentPath); a MANIFEST in this phase has no such escape
// hatch, since manifest rotation isn't in scope (§9/§11) and there's
// exactly one long-lived MANIFEST file for the database's whole life.
// The only fix that fits within that constraint is removing the torn
// bytes themselves -- which is always safe, because a torn tail is by
// definition a write that never completed, so nothing durably
// acknowledged is lost by discarding it (identical reasoning to
// ReplayManifest's own "stop, don't error" contract).
func RepairTornTail(manifestPath string) error {
	f, err := os.OpenFile(manifestPath, os.O_RDWR, 0o644)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("manifest: open %s: %w", manifestPath, err)
	}
	defer f.Close()

	var validEnd int64
	for {
		_, err := DecodeEdit(f)
		if err == io.EOF || err == ErrCorrupt {
			break
		}
		if err != nil {
			return fmt.Errorf("manifest: %s: %w", manifestPath, err)
		}
		pos, err := f.Seek(0, io.SeekCurrent)
		if err != nil {
			return fmt.Errorf("manifest: %s: %w", manifestPath, err)
		}
		validEnd = pos
	}

	info, err := f.Stat()
	if err != nil {
		return fmt.Errorf("manifest: stat %s: %w", manifestPath, err)
	}
	if info.Size() == validEnd {
		return nil
	}

	if err := f.Truncate(validEnd); err != nil {
		return fmt.Errorf("manifest: truncate %s: %w", manifestPath, err)
	}
	if err := f.Sync(); err != nil {
		return fmt.Errorf("manifest: fsync %s: %w", manifestPath, err)
	}
	return nil
}

// ReplayManifest reads every edit in the MANIFEST file at manifestPath, in
// order, stopping cleanly at that file's first torn or corrupt record --
// no error, just everything decoded before the tear -- same torn-tail
// discipline as wal.Replay. A missing file (a brand-new database that
// hasn't flushed yet) is not an error; it just means no edits exist.
func ReplayManifest(manifestPath string) ([]VersionEdit, error) {
	f, err := os.Open(manifestPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer f.Close()

	var edits []VersionEdit
	for {
		e, err := DecodeEdit(f)
		if err == io.EOF || err == ErrCorrupt {
			break
		}
		if err != nil {
			return nil, err
		}
		edits = append(edits, e)
	}
	return edits, nil
}

// ReconstructSSTableSet replays edits in order to determine the live
// SSTable set: SSTableAdded inserts a filename, SSTableRemoved deletes
// it. Whatever remains after every edit is applied is authoritative --
// callers don't need to reason about the edit history themselves, only
// the resulting set. The result is sorted (filenames follow the
// fixed-width "NNNNNN.sst" convention, so lexical order is numeric order)
// for a deterministic, directly comparable return value.
func ReconstructSSTableSet(edits []VersionEdit) []string {
	live := make(map[string]struct{})
	for _, e := range edits {
		switch e.Type {
		case SSTableAdded:
			live[e.File] = struct{}{}
		case SSTableRemoved:
			delete(live, e.File)
		}
	}

	result := make([]string, 0, len(live))
	for name := range live {
		result = append(result, name)
	}
	sort.Strings(result)
	return result
}

// LiveSSTable is one entry in a level-aware reconstructed SSTable set: a
// filename plus the level it currently belongs to (Phase 8d).
type LiveSSTable struct {
	File  string
	Level int
}

// ReconstructLeveledSSTableSet replays edits exactly like
// ReconstructSSTableSet (SSTableAdded inserts, SSTableRemoved deletes,
// last edit per file wins), but additionally tracks which level each live
// file belongs to, from that file's own SSTableAdded edit. A pre-8d edit
// always decodes with Level 0 (see VersionEdit/DecodeEdit), so a file
// added before Phase 8d existed is reconstructed as a plain L0 file --
// correct, since a pre-8d MANIFEST never made any non-overlap promise
// about its files that L1 could violate.
func ReconstructLeveledSSTableSet(edits []VersionEdit) []LiveSSTable {
	level := make(map[string]int)
	for _, e := range edits {
		switch e.Type {
		case SSTableAdded:
			level[e.File] = e.Level
		case SSTableRemoved:
			delete(level, e.File)
		}
	}

	result := make([]LiveSSTable, 0, len(level))
	for f, l := range level {
		result = append(result, LiveSSTable{File: f, Level: l})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].File < result[j].File })
	return result
}
