package wal

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
)

// errTorn signals a truncated or corrupt record. It never escapes this
// package: it means "stop replay here", not "replay failed".
var errTorn = errors.New("wal: torn record")

// maxFieldLen bounds key_len/value_len before they're used to size an
// allocation. Without this, a single bit-flip in a length field (exactly
// the kind of corruption checksums exist to catch, but the checksum isn't
// verified until after the fields it covers are read) could claim up to
// 4GB and attempt that allocation before the record is known to be bad.
const maxFieldLen = 64 << 20 // 64 MiB

// readRecord reads one record from r. It returns io.EOF only when r is
// exhausted exactly at a record boundary (a clean end of segment). Any
// other short read, or a checksum mismatch, is reported as errTorn.
func readRecord(r io.Reader) (Entry, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		if err == io.EOF {
			return Entry{}, io.EOF
		}
		return Entry{}, errTorn
	}
	wantChecksum := binary.BigEndian.Uint32(hdr[:])

	var opBuf [1]byte
	if _, err := io.ReadFull(r, opBuf[:]); err != nil {
		return Entry{}, errTorn
	}

	var keyLenBuf [4]byte
	if _, err := io.ReadFull(r, keyLenBuf[:]); err != nil {
		return Entry{}, errTorn
	}
	keyLen := binary.BigEndian.Uint32(keyLenBuf[:])
	if keyLen > maxFieldLen {
		return Entry{}, errTorn
	}
	key := make([]byte, keyLen)
	if _, err := io.ReadFull(r, key); err != nil {
		return Entry{}, errTorn
	}

	var valLenBuf [4]byte
	if _, err := io.ReadFull(r, valLenBuf[:]); err != nil {
		return Entry{}, errTorn
	}
	valLen := binary.BigEndian.Uint32(valLenBuf[:])
	if valLen > maxFieldLen {
		return Entry{}, errTorn
	}
	value := make([]byte, valLen)
	if _, err := io.ReadFull(r, value); err != nil {
		return Entry{}, errTorn
	}

	body := make([]byte, 0, 1+4+len(key)+4+len(value))
	body = append(body, opBuf[0])
	body = append(body, keyLenBuf[:]...)
	body = append(body, key...)
	body = append(body, valLenBuf[:]...)
	body = append(body, value...)

	if crc32.ChecksumIEEE(body) != wantChecksum {
		return Entry{}, errTorn
	}

	return Entry{Op: OpType(opBuf[0]), Key: key, Value: value}, nil
}

// Replay reads every segment in dir, in filename order, and returns all
// fully written entries. Within each segment, replay stops cleanly at
// that segment's first torn or corrupt record; entries decoded before the
// tear are kept, no error is returned, and replay proceeds to the next
// segment. A torn tail can only legitimately occur in the segment that
// was actively being written when a crash happened -- once a segment is
// rotated away from, it's closed and immutable, so a torn record there
// never masks valid data that comes after it *within that file*. But a
// segment can be non-terminal (not the last file overall) and still have
// been the crash-time segment, if recovery resumed writes into a new,
// later-numbered segment via NextSegmentPath: that later segment is
// independent and may be fully valid, so replay must not abort the whole
// scan just because an earlier segment ended torn.
func Replay(dir string) ([]Entry, error) {
	names, err := segmentNames(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	var entries []Entry
	for _, name := range names {
		f, err := os.Open(filepath.Join(dir, name))
		if err != nil {
			return nil, err
		}

		for {
			e, err := readRecord(f)
			if err == io.EOF || err == errTorn {
				break
			}
			if err != nil {
				f.Close()
				return nil, err
			}
			entries = append(entries, e)
		}
		f.Close()
	}

	return entries, nil
}

func segmentNames(dir string) ([]string, error) {
	des, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var names []string
	for _, de := range des {
		if de.IsDir() {
			continue
		}
		if filepath.Ext(de.Name()) == ".log" {
			names = append(names, de.Name())
		}
	}
	sort.Strings(names)
	return names, nil
}
