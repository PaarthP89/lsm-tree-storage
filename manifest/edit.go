package manifest

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
)

// ErrCorrupt signals a truncated or corrupt record: a short read partway
// through a record, or a checksum mismatch. Same meaning as wal.ErrCorrupt
// -- ReplayManifest treats it as "stop replay here, don't error" (a torn
// tail is expected at the crash-time end of an append-only file).
var ErrCorrupt = errors.New("manifest: corrupt or truncated record")

// maxFieldLen bounds name_len before it's used to size an allocation, for
// the same reason wal.maxFieldLen does: a single bit-flip in a length
// field shouldn't be able to claim an arbitrary allocation before the
// checksum (which covers that very field) has been verified.
const maxFieldLen = 64 << 20 // 64 MiB

// EditType identifies what a VersionEdit records about the SSTable set.
// These are the exported, semantic values callers construct/inspect --
// distinct from the wire-level markers below, which additionally encode
// whether a level field is present on disk (Phase 8d).
type EditType byte

const (
	SSTableAdded   EditType = 1
	SSTableRemoved EditType = 2
)

// Wire-level markers, used only by Encode/DecodeEdit. A pre-8d MANIFEST
// was written with wireAddedLegacy/wireRemovedLegacy (no level field in
// the payload at all -- levels didn't exist yet). Phase 8d always writes
// the wireAddedLeveled/wireRemovedLeveled markers instead, which carry an
// explicit trailing level field. The byte itself is the "explicit
// edit-format version marker": decode reads it before deciding whether to
// read a level field, so a MANIFEST containing a mix of pre-8d and post-8d
// records (the common case right after upgrading a live database) decodes
// each record correctly on its own, without any file-wide version flag.
//
// wireAddedLegacy/wireRemovedLegacy share their byte values with the
// exported SSTableAdded/SSTableRemoved constants above (1/2) because
// that's literally what's already sitting on disk in every pre-8d
// MANIFEST -- there's no migration step, decoding just has to recognize
// both the old and new byte values.
const (
	wireAddedLegacy    byte = 1
	wireRemovedLegacy  byte = 2
	wireAddedLeveled   byte = 3
	wireRemovedLeveled byte = 4
)

// VersionEdit is one change to the live SSTable set. File is the SSTable's
// filename (basename, not a full path) -- the same string db.go's
// directory scan produces, so the MANIFEST-reconstructed set can be
// compared against it directly. Level is the SSTable's level (0 or 1,
// Phase 8d) -- always 0 for a record decoded from a pre-8d MANIFEST (see
// DecodeEdit), which is the deliberately conservative default: L0's "may
// overlap" assumption is always safe, while incorrectly defaulting to L1
// could violate L1's non-overlap invariant.
type VersionEdit struct {
	Type  EditType
	File  string
	Level int
}

// EncodeEdit serializes edit as:
// [checksum: 4B CRC32][wire_type: 1B][name_len: 4B][name][level: 4B]
// The checksum covers everything after the checksum field itself -- same
// discipline as wal.EncodeEntry. EncodeEdit always writes the Phase 8d
// leveled wire format (wire_type in {wireAddedLeveled, wireRemovedLeveled},
// always followed by an explicit level field) -- the legacy wire_type
// values only ever appear in a MANIFEST written before this field existed;
// nothing in this codebase writes them anymore.
func EncodeEdit(edit VersionEdit) []byte {
	var wireType byte
	switch edit.Type {
	case SSTableAdded:
		wireType = wireAddedLeveled
	case SSTableRemoved:
		wireType = wireRemovedLeveled
	default:
		panic(fmt.Sprintf("manifest: EncodeEdit: unknown EditType %v", edit.Type))
	}

	name := []byte(edit.File)
	body := make([]byte, 1+4+len(name)+4)
	body[0] = wireType
	binary.BigEndian.PutUint32(body[1:5], uint32(len(name)))
	copy(body[5:5+len(name)], name)
	binary.BigEndian.PutUint32(body[5+len(name):], uint32(edit.Level))

	checksum := crc32.ChecksumIEEE(body)
	buf := make([]byte, 4+len(body))
	binary.BigEndian.PutUint32(buf[0:4], checksum)
	copy(buf[4:], body)
	return buf
}

// DecodeEdit reads one record from r. It returns io.EOF only when r is
// exhausted exactly at a record boundary (a clean end of file). Any other
// short read, or a checksum mismatch, is reported as ErrCorrupt.
//
// The wire_type byte is read before the level field is even attempted:
// wireAddedLegacy/wireRemovedLegacy mean this record predates Phase 8d and
// has no level field at all (Level comes back 0), while
// wireAddedLeveled/wireRemovedLeveled mean an explicit level field follows
// name -- this is what lets a single MANIFEST mix pre- and post-8d records
// and have each decode correctly on its own.
func DecodeEdit(r io.Reader) (VersionEdit, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		if err == io.EOF {
			return VersionEdit{}, io.EOF
		}
		return VersionEdit{}, ErrCorrupt
	}
	wantChecksum := binary.BigEndian.Uint32(hdr[:])

	var typeBuf [1]byte
	if _, err := io.ReadFull(r, typeBuf[:]); err != nil {
		return VersionEdit{}, ErrCorrupt
	}
	wireType := typeBuf[0]

	var nameLenBuf [4]byte
	if _, err := io.ReadFull(r, nameLenBuf[:]); err != nil {
		return VersionEdit{}, ErrCorrupt
	}
	nameLen := binary.BigEndian.Uint32(nameLenBuf[:])
	if nameLen > maxFieldLen {
		return VersionEdit{}, ErrCorrupt
	}
	name := make([]byte, nameLen)
	if _, err := io.ReadFull(r, name); err != nil {
		return VersionEdit{}, ErrCorrupt
	}

	body := make([]byte, 0, 1+4+len(name)+4)
	body = append(body, wireType)
	body = append(body, nameLenBuf[:]...)
	body = append(body, name...)

	var level uint32
	if wireType == wireAddedLeveled || wireType == wireRemovedLeveled {
		var levelBuf [4]byte
		if _, err := io.ReadFull(r, levelBuf[:]); err != nil {
			return VersionEdit{}, ErrCorrupt
		}
		level = binary.BigEndian.Uint32(levelBuf[:])
		body = append(body, levelBuf[:]...)
	}

	if crc32.ChecksumIEEE(body) != wantChecksum {
		return VersionEdit{}, ErrCorrupt
	}

	var typ EditType
	switch wireType {
	case wireAddedLegacy, wireAddedLeveled:
		typ = SSTableAdded
	case wireRemovedLegacy, wireRemovedLeveled:
		typ = SSTableRemoved
	default:
		// An unrecognized wire_type that still happens to checksum-match
		// can only mean a future format this build doesn't understand --
		// treated as corruption, same as everywhere else in this codebase
		// that refuses to guess past what it can decode.
		return VersionEdit{}, ErrCorrupt
	}

	return VersionEdit{Type: typ, File: string(name), Level: int(level)}, nil
}
