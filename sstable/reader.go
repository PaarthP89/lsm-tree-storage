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
	bloom        *bloomFilter // nil for a pre-8a file or an empty table
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

	index, minKey, maxKey, count, bloom, err := decodeFooter(footerBody)
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
		bloom:        bloom,
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

// HasBloomFilter reports whether this table has a bloom filter (built by
// Phase 8a). false for a pre-8a file or an empty table -- both fall
// through to a full lookup on every Get. Exposed for tests/diagnostics,
// not part of the read path itself.
func (s *SSTable) HasBloomFilter() bool {
	return s.bloom != nil
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
	if s.bloom != nil && !s.bloom.MayContain(key) {
		// Definitely absent: skip the sparse-index binary search and the
		// on-disk scan entirely. A nil filter (pre-8a file, or an empty
		// table -- which never reaches here anyway) always falls through.
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

// Iterator returns a sorted iterator over every entry in the table
// (tombstones included), scanning the data section from its start up to
// the footer. This is a genuine full-file scan -- unlike Get, which is
// deliberately bounded to one sparse-index block -- but that's exactly
// what compaction's k-way merge needs: every entry, in order, from every
// input table.
func (s *SSTable) Iterator() *Iterator {
	sr := io.NewSectionReader(s.file, 0, s.footerOffset)
	return &Iterator{path: s.path, br: bufio.NewReader(sr)}
}

// emptyIterator returns an Iterator that reports done on the very first
// Next() call -- the correct shape for "this table has nothing in the
// requested range" without a caller needing a separate nil-vs-empty case.
func emptyIterator(path string) *Iterator {
	return &Iterator{path: path, br: bufio.NewReader(bytes.NewReader(nil))}
}

// SeekIterator returns a sorted iterator (tombstones included, same as
// Iterator) over every entry with key >= start (Phase 8b range queries).
// It locates the starting position with the same sparse-index binary
// search Get uses -- never a full-file scan -- then linear-scans forward
// within that one block (at most indexInterval entries, the same bound
// Get relies on) to find the exact byte offset of the first qualifying
// entry before handing back an Iterator positioned there.
//
// Unlike Get, this doesn't take an upper bound: the returned Iterator
// will walk every remaining entry through the end of the table if asked
// to. Deciding how far to actually read is the caller's job (DB.Scan
// stops pulling once it reaches its own end key) -- since Next() only
// does work when called, an unconsulted tail of the table costs nothing.
func (s *SSTable) SeekIterator(start []byte) (*Iterator, error) {
	if len(s.index) == 0 {
		return emptyIterator(s.path), nil
	}

	// Same binary search as Get: find the first index entry whose key
	// exceeds start: the block that could contain the target begins at
	// the entry just before it. i==0 (start <= every index key, i.e.
	// start <= minKey) resolves to i=1, landing on index[0].offset == 0
	// -- the very first block -- which is exactly right for a seek from
	// before the table's start.
	i := sort.Search(len(s.index), func(i int) bool {
		return bytes.Compare(s.index[i].key, start) > 0
	})
	if i == 0 {
		i = 1
	}
	blockStart := s.index[i-1].offset

	sr := io.NewSectionReader(s.file, blockStart, s.footerOffset-blockStart)
	br := bufio.NewReader(sr)
	offset := blockStart

	for {
		e, err := wal.DecodeEntry(br)
		if err == io.EOF {
			return emptyIterator(s.path), nil
		}
		if err != nil {
			return nil, fmt.Errorf("sstable: %s: %w", s.path, err)
		}
		if bytes.Compare(e.Key, start) >= 0 {
			// Hand back a fresh Iterator over a SectionReader starting
			// exactly at this entry's byte offset, with Next() not yet
			// called on it -- the same "unadvanced" contract every other
			// SortedIterator source (compaction.MergeIterator's sources)
			// requires. The entry just decoded above is deliberately
			// discarded and re-read by the caller's first Next() rather
			// than threaded through some out-of-band "primed" state.
			rest := io.NewSectionReader(s.file, offset, s.footerOffset-offset)
			return &Iterator{path: s.path, br: bufio.NewReader(rest)}, nil
		}
		offset += int64(len(wal.EncodeEntry(e)))
	}
}

// Iterator walks an SSTable's data section in key order. Unlike Get, a
// mid-scan decode failure is not "stop, return what we have" (the torn-
// tail rule that applies to the WAL and MANIFEST, both of which expect an
// in-progress write at their live tail) -- an SSTable only ever becomes
// visible via a completed, fsynced atomic rename (see FlushMemtable), so a
// decode error here can only mean real bit rot in an already-committed
// file. Next stops and records the error in Err() rather than silently
// treating corruption as end-of-table; callers (e.g. compaction.Compact)
// must check Err() after draining.
type Iterator struct {
	path string
	br   *bufio.Reader
	cur  wal.Entry
	err  error
}

// Next advances to the next entry, returning false at true end-of-table
// or after a decode error (distinguish the two via Err()).
func (it *Iterator) Next() bool {
	if it.err != nil {
		return false
	}
	e, err := wal.DecodeEntry(it.br)
	if err == io.EOF {
		return false
	}
	if err != nil {
		it.err = fmt.Errorf("sstable: %s: %w", it.path, err)
		return false
	}
	it.cur = e
	return true
}

func (it *Iterator) Key() []byte     { return it.cur.Key }
func (it *Iterator) Value() []byte   { return it.cur.Value }
func (it *Iterator) Tombstone() bool { return it.cur.Op == wal.OpDelete }
func (it *Iterator) Err() error      { return it.err }
