package sstable

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
)

// indexInterval controls sparse index density: every Nth entry written
// gets an index record. This bounds Get's on-disk scan to at most this
// many entries, which is the whole point of the sparse index -- reads
// binary-search the index, then linear-scan one small range, never the
// whole file.
const indexInterval = 64

// trailerLen is the size, in bytes, of the fixed-position block at the
// very end of every SSTable file: [footer_checksum: 4B][footer_offset: 8B].
// Its fixed size and fixed position (always the last trailerLen bytes)
// are what let OpenSSTable find the footer without a separate index into
// the file: seek to size-trailerLen, and the rest is self-describing.
const trailerLen = 4 + 8

type indexEntry struct {
	key    []byte
	offset int64
}

// encodeFooter serializes the sparse index plus table metadata as:
//
//	[index_count: 4B]
//	repeated index_count times: [key_len: 4B][key][offset: 8B]
//	[min_key_len: 4B][min_key]
//	[max_key_len: 4B][max_key]
//	[entry_count: 8B]
//
// The caller is responsible for appending a checksum + offset trailer
// after this (see encodeTrailer) -- this function returns only the body
// that the checksum covers.
func encodeFooter(index []indexEntry, minKey, maxKey []byte, entryCount int) []byte {
	var buf bytes.Buffer

	writeUint32(&buf, uint32(len(index)))
	for _, e := range index {
		writeUint32(&buf, uint32(len(e.key)))
		buf.Write(e.key)
		writeUint64(&buf, uint64(e.offset))
	}

	writeUint32(&buf, uint32(len(minKey)))
	buf.Write(minKey)
	writeUint32(&buf, uint32(len(maxKey)))
	buf.Write(maxKey)

	writeUint64(&buf, uint64(entryCount))

	return buf.Bytes()
}

// encodeTrailer builds the fixed-size trailer written immediately after
// the footer body: a CRC32 of the footer body (so corruption of the
// footer itself -- which every Get depends on -- is caught at open time
// rather than silently producing wrong binary-search results), and the
// absolute byte offset in the file where the footer body starts.
func encodeTrailer(footerBody []byte, footerOffset int64) []byte {
	trailer := make([]byte, trailerLen)
	binary.BigEndian.PutUint32(trailer[0:4], crc32.ChecksumIEEE(footerBody))
	binary.BigEndian.PutUint64(trailer[4:12], uint64(footerOffset))
	return trailer
}

// decodeFooter parses a footer body previously produced by encodeFooter.
func decodeFooter(body []byte) (index []indexEntry, minKey, maxKey []byte, entryCount int, err error) {
	r := bytes.NewReader(body)

	indexCount, err := readUint32(r)
	if err != nil {
		return nil, nil, nil, 0, fmt.Errorf("sstable: footer: %w", err)
	}
	index = make([]indexEntry, 0, indexCount)
	for i := uint32(0); i < indexCount; i++ {
		keyLen, err := readUint32(r)
		if err != nil {
			return nil, nil, nil, 0, fmt.Errorf("sstable: footer: index entry %d: %w", i, err)
		}
		key := make([]byte, keyLen)
		if _, err := readFull(r, key); err != nil {
			return nil, nil, nil, 0, fmt.Errorf("sstable: footer: index entry %d key: %w", i, err)
		}
		offset, err := readUint64(r)
		if err != nil {
			return nil, nil, nil, 0, fmt.Errorf("sstable: footer: index entry %d offset: %w", i, err)
		}
		index = append(index, indexEntry{key: key, offset: int64(offset)})
	}

	minKeyLen, err := readUint32(r)
	if err != nil {
		return nil, nil, nil, 0, fmt.Errorf("sstable: footer: min key len: %w", err)
	}
	minKey = make([]byte, minKeyLen)
	if _, err := readFull(r, minKey); err != nil {
		return nil, nil, nil, 0, fmt.Errorf("sstable: footer: min key: %w", err)
	}

	maxKeyLen, err := readUint32(r)
	if err != nil {
		return nil, nil, nil, 0, fmt.Errorf("sstable: footer: max key len: %w", err)
	}
	maxKey = make([]byte, maxKeyLen)
	if _, err := readFull(r, maxKey); err != nil {
		return nil, nil, nil, 0, fmt.Errorf("sstable: footer: max key: %w", err)
	}

	count, err := readUint64(r)
	if err != nil {
		return nil, nil, nil, 0, fmt.Errorf("sstable: footer: entry count: %w", err)
	}

	return index, minKey, maxKey, int(count), nil
}

func writeUint32(buf *bytes.Buffer, v uint32) {
	var tmp [4]byte
	binary.BigEndian.PutUint32(tmp[:], v)
	buf.Write(tmp[:])
}

func writeUint64(buf *bytes.Buffer, v uint64) {
	var tmp [8]byte
	binary.BigEndian.PutUint64(tmp[:], v)
	buf.Write(tmp[:])
}

func readUint32(r *bytes.Reader) (uint32, error) {
	var tmp [4]byte
	if _, err := readFull(r, tmp[:]); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint32(tmp[:]), nil
}

func readUint64(r *bytes.Reader) (uint64, error) {
	var tmp [8]byte
	if _, err := readFull(r, tmp[:]); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(tmp[:]), nil
}

func readFull(r *bytes.Reader, buf []byte) (int, error) {
	return io.ReadFull(r, buf)
}
