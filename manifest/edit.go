package manifest

import (
	"encoding/binary"
	"errors"
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
type EditType byte

const (
	SSTableAdded   EditType = 1
	SSTableRemoved EditType = 2
)

// VersionEdit is one change to the live SSTable set. File is the SSTable's
// filename (basename, not a full path) -- the same string db.go's
// directory scan produces, so the MANIFEST-reconstructed set can be
// compared against it directly.
type VersionEdit struct {
	Type EditType
	File string
}

// EncodeEdit serializes edit as:
// [checksum: 4B CRC32][edit_type: 1B][name_len: 4B][name]
// The checksum covers everything after the checksum field itself -- same
// discipline as wal.EncodeEntry.
func EncodeEdit(edit VersionEdit) []byte {
	name := []byte(edit.File)
	body := make([]byte, 1+4+len(name))
	body[0] = byte(edit.Type)
	binary.BigEndian.PutUint32(body[1:5], uint32(len(name)))
	copy(body[5:], name)

	checksum := crc32.ChecksumIEEE(body)
	buf := make([]byte, 4+len(body))
	binary.BigEndian.PutUint32(buf[0:4], checksum)
	copy(buf[4:], body)
	return buf
}

// DecodeEdit reads one record from r. It returns io.EOF only when r is
// exhausted exactly at a record boundary (a clean end of file). Any other
// short read, or a checksum mismatch, is reported as ErrCorrupt.
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

	body := make([]byte, 0, 1+4+len(name))
	body = append(body, typeBuf[0])
	body = append(body, nameLenBuf[:]...)
	body = append(body, name...)

	if crc32.ChecksumIEEE(body) != wantChecksum {
		return VersionEdit{}, ErrCorrupt
	}

	return VersionEdit{Type: EditType(typeBuf[0]), File: string(name)}, nil
}
