package sstable

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sort"

	"github.com/paarthsiphone/lsm-tree-storage/wal"
)

// SSTable is an open, sorted, indexed on-disk table. Opening one reads
// only its footer (the sparse index and min/max key metadata) -- the
// data section is never read except for the small bounded range a Get
// actually needs.
type SSTable struct {
	path         string
	file         *os.File
	minKey       []byte
	maxKey       []byte
	entryCount   int
	index        []indexEntry
	footerOffset int64
}

// OpenSSTable opens the file at path and parses its footer. It returns an
// error if the file is smaller than a trailer, or if the footer checksum
// doesn't match -- every Get on this table depends on the sparse index
// being intact, so corruption there must fail loudly at open time rather
// than silently producing a wrong binary-search result later.
func OpenSSTable(path string) (*SSTable, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("sstable: open %s: %w", path, err)
	}

	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("sstable: stat %s: %w", path, err)
	}
	size := info.Size()
	if size < trailerLen {
		f.Close()
		return nil, fmt.Errorf("sstable: %s: file too small (%d bytes) to contain a footer trailer", path, size)
	}

	trailer := make([]byte, trailerLen)
	if _, err := f.ReadAt(trailer, size-trailerLen); err != nil {
		f.Close()
		return nil, fmt.Errorf("sstable: %s: read trailer: %w", path, err)
	}
	wantChecksum := binary.BigEndian.Uint32(trailer[0:4])
	footerOffset := int64(binary.BigEndian.Uint64(trailer[4:12]))
	if footerOffset < 0 || footerOffset > size-trailerLen {
		f.Close()
		return nil, fmt.Errorf("sstable: %s: corrupt footer offset %d (file size %d)", path, footerOffset, size)
	}

	footerBody := make([]byte, size-trailerLen-footerOffset)
	if _, err := f.ReadAt(footerBody, footerOffset); err != nil {
		f.Close()
		return nil, fmt.Errorf("sstable: %s: read footer: %w", path, err)
	}
	if crc32.ChecksumIEEE(footerBody) != wantChecksum {
		f.Close()
		return nil, fmt.Errorf("sstable: %s: footer checksum mismatch (corrupt file)", path)
	}

	index, minKey, maxKey, count, err := decodeFooter(footerBody)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("sstable: %s: %w", path, err)
	}

	return &SSTable{
		path:         path,
		file:         f,
		minKey:       minKey,
		maxKey:       maxKey,
		entryCount:   count,
		index:        index,
		footerOffset: footerOffset,
	}, nil
}

// Close closes the underlying file handle.
func (s *SSTable) Close() error {
	return s.file.Close()
}

// Meta returns this table's metadata, matching what FlushMemtable
// returned when it was written.
func (s *SSTable) Meta() SSTableMeta {
	return SSTableMeta{Path: s.path, MinKey: s.minKey, MaxKey: s.maxKey, EntryCount: s.entryCount}
}

// Get looks up key. found reports whether key's newest entry in this
// table was located at all; tombstone reports whether that entry is a
// delete marker. Callers must check tombstone even when found is true --
// a tombstone hit means "this table's answer is definitively deleted",
// not "not found here, keep looking older tables".
//
// The lookup binary-searches the sparse index to find the single
// on-disk range that could contain key, then linear-scans only that
// range (at most indexInterval entries) via a bounded io.SectionReader.
// It never scans the whole file.
func (s *SSTable) Get(key []byte) (value []byte, found bool, tombstone bool, err error) {
	if len(s.index) == 0 {
		return nil, false, false, nil
	}
	if bytes.Compare(key, s.minKey) < 0 || bytes.Compare(key, s.maxKey) > 0 {
		return nil, false, false, nil
	}

	// Find the first index entry whose key exceeds the target; the
	// block that could contain the target starts at the entry just
	// before it. index[0].key == minKey and key >= minKey was already
	// checked above, so i >= 1 is guaranteed here.
	i := sort.Search(len(s.index), func(i int) bool {
		return bytes.Compare(s.index[i].key, key) > 0
	})
	if i == 0 {
		i = 1
	}

	blockStart := s.index[i-1].offset
	blockEnd := s.footerOffset
	if i < len(s.index) {
		blockEnd = s.index[i].offset
	}

	sr := io.NewSectionReader(s.file, blockStart, blockEnd-blockStart)
	br := bufio.NewReader(sr)
	for {
		e, err := wal.DecodeEntry(br)
		if err == io.EOF {
			return nil, false, false, nil
		}
		if err != nil {
			return nil, false, false, fmt.Errorf("sstable: %s: %w", s.path, err)
		}

		cmp := bytes.Compare(e.Key, key)
		if cmp == 0 {
			if e.Op == wal.OpDelete {
				return nil, true, true, nil
			}
			return e.Value, true, false, nil
		}
		if cmp > 0 {
			// Entries are sorted within the block; passing the target
			// key means it isn't present.
			return nil, false, false, nil
		}
	}
}
