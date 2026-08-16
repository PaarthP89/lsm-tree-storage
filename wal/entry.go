package wal

import (
	"encoding/binary"
	"hash/crc32"
)

type OpType byte

const (
	OpPut    OpType = 1
	OpDelete OpType = 2
)

type Entry struct {
	Op    OpType
	Key   []byte
	Value []byte // nil/empty for OpDelete
}

// EncodeEntry serializes e as:
// [checksum: 4B CRC32][op_type: 1B][key_len: 4B][key][value_len: 4B][value]
// The checksum covers everything after the checksum field itself.
//
// Exported so other packages that reuse this exact wire format (sstable's
// data section) encode records identically rather than reimplementing it.
func EncodeEntry(e Entry) []byte {
	body := make([]byte, 1+4+len(e.Key)+4+len(e.Value))
	body[0] = byte(e.Op)
	binary.BigEndian.PutUint32(body[1:5], uint32(len(e.Key)))
	copy(body[5:5+len(e.Key)], e.Key)
	valOff := 5 + len(e.Key)
	binary.BigEndian.PutUint32(body[valOff:valOff+4], uint32(len(e.Value)))
	copy(body[valOff+4:], e.Value)

	checksum := crc32.ChecksumIEEE(body)
	buf := make([]byte, 4+len(body))
	binary.BigEndian.PutUint32(buf[0:4], checksum)
	copy(buf[4:], body)
	return buf
}
