package manifest

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"
)

// encodeLegacyEdit hand-builds a pre-8d MANIFEST record: [checksum: 4B]
// [edit_type: 1B][name_len: 4B][name] -- no level field at all, since
// levels didn't exist yet. This is the exact byte layout the original
// Phase 4 EncodeEdit produced, before Phase 8d added wireAddedLeveled/
// wireRemovedLeveled. Used to prove DecodeEdit/ReplayManifest still read
// a real pre-8d file correctly.
func encodeLegacyEdit(t byte, file string) []byte {
	name := []byte(file)
	body := make([]byte, 1+4+len(name))
	body[0] = t
	binary.BigEndian.PutUint32(body[1:5], uint32(len(name)))
	copy(body[5:], name)

	checksum := crc32.ChecksumIEEE(body)
	buf := make([]byte, 4+len(body))
	binary.BigEndian.PutUint32(buf[0:4], checksum)
	copy(buf[4:], body)
	return buf
}

// TestDecodeEditPreLevelFormatDefaultsToL0 confirms a hand-built legacy
// (pre-8d) record -- no level field in the payload at all -- decodes
// with Level 0, never crashing and never guessing a nonzero level.
func TestDecodeEditPreLevelFormatDefaultsToL0(t *testing.T) {
	buf := encodeLegacyEdit(wireAddedLegacy, "000001.sst")
	got, err := DecodeEdit(bytes.NewReader(buf))
	if err != nil {
		t.Fatalf("DecodeEdit: %v", err)
	}
	if got.Type != SSTableAdded || got.File != "000001.sst" || got.Level != 0 {
		t.Fatalf("got %+v, want {Type:SSTableAdded File:000001.sst Level:0}", got)
	}
}

// TestReplayPreLevelManifestTreatsEveryEntryAsL0 replays a real,
// hand-constructed pre-8d-format MANIFEST file (a mix of legacy ADDED and
// REMOVED records, exactly as Phase 4-7 code would have written it,
// predating any level field) and confirms every entry the leveled
// reconstruction produces lands in level 0 -- the required conservative
// default (CLAUDE.md 8d: L0's "may overlap" assumption is always safe,
// silently defaulting to L1 could violate L1's non-overlap invariant).
func TestReplayPreLevelManifestTreatsEveryEntryAsL0(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "MANIFEST-000001")

	var raw []byte
	raw = append(raw, encodeLegacyEdit(wireAddedLegacy, "000001.sst")...)
	raw = append(raw, encodeLegacyEdit(wireAddedLegacy, "000002.sst")...)
	raw = append(raw, encodeLegacyEdit(wireAddedLegacy, "000003.sst")...)
	raw = append(raw, encodeLegacyEdit(wireRemovedLegacy, "000001.sst")...)
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	edits, err := ReplayManifest(path)
	if err != nil {
		t.Fatalf("ReplayManifest: %v", err)
	}
	if len(edits) != 4 {
		t.Fatalf("len(edits) = %d, want 4", len(edits))
	}
	for _, e := range edits {
		if e.Level != 0 {
			t.Fatalf("edit %+v: Level = %d, want 0 (pre-8d record)", e, e.Level)
		}
	}

	live := ReconstructLeveledSSTableSet(edits)
	if len(live) != 2 {
		t.Fatalf("len(live) = %d, want 2 (000002.sst, 000003.sst)", len(live))
	}
	for _, l := range live {
		if l.Level != 0 {
			t.Fatalf("live entry %+v: Level = %d, want 0", l, l.Level)
		}
	}
}

// TestManifestMixOfPreAndPost8dRecordsDecodesEachCorrectly confirms a
// MANIFEST containing both legacy (no level field) and Phase 8d leveled
// records -- the realistic shape of a live database's MANIFEST the
// instant it's upgraded -- decodes every record correctly on its own,
// with no file-wide version flag needed: the wire_type byte on each
// individual record is what disambiguates it.
func TestManifestMixOfPreAndPost8dRecordsDecodesEachCorrectly(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "MANIFEST-000001")

	var raw []byte
	raw = append(raw, encodeLegacyEdit(wireAddedLegacy, "000001.sst")...) // pre-8d: L0
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	// Post-8d code appends a leveled edit to the same live file.
	if err := AppendEdit(path, VersionEdit{Type: SSTableAdded, File: "000002.sst", Level: 1}); err != nil {
		t.Fatalf("AppendEdit: %v", err)
	}

	edits, err := ReplayManifest(path)
	if err != nil {
		t.Fatalf("ReplayManifest: %v", err)
	}
	if len(edits) != 2 {
		t.Fatalf("len(edits) = %d, want 2", len(edits))
	}
	if edits[0].Level != 0 {
		t.Fatalf("edits[0].Level = %d, want 0 (legacy record)", edits[0].Level)
	}
	if edits[1].Level != 1 {
		t.Fatalf("edits[1].Level = %d, want 1 (leveled record)", edits[1].Level)
	}

	live := ReconstructLeveledSSTableSet(edits)
	byFile := make(map[string]int)
	for _, l := range live {
		byFile[l.File] = l.Level
	}
	if byFile["000001.sst"] != 0 {
		t.Fatalf("000001.sst level = %d, want 0", byFile["000001.sst"])
	}
	if byFile["000002.sst"] != 1 {
		t.Fatalf("000002.sst level = %d, want 1", byFile["000002.sst"])
	}
}
