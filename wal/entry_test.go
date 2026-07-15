package wal

import (
	"bytes"
	"encoding/binary"
	"io"
	"testing"
)

func TestEncodeDecodeRoundTrip(t *testing.T) {
	cases := []Entry{
		{Op: OpPut, Key: []byte("a"), Value: []byte("1")},
		{Op: OpPut, Key: []byte("longer-key"), Value: []byte("a somewhat longer value")},
		{Op: OpDelete, Key: []byte("a"), Value: nil},
		{Op: OpDelete, Key: []byte(""), Value: nil},
		{Op: OpPut, Key: []byte(""), Value: []byte("")},
		{Op: OpPut, Key: []byte("k"), Value: []byte("")},
	}

	for _, want := range cases {
		buf := encode(want)
		got, err := readRecord(bytes.NewReader(buf))
		if err != nil {
			t.Fatalf("readRecord(%+v): %v", want, err)
		}
		if got.Op != want.Op {
			t.Errorf("Op = %v, want %v", got.Op, want.Op)
		}
		if !bytes.Equal(got.Key, want.Key) {
			t.Errorf("Key = %q, want %q", got.Key, want.Key)
		}
		if !bytes.Equal(got.Value, want.Value) {
			t.Errorf("Value = %q, want %q", got.Value, want.Value)
		}
	}
}

func TestReadRecordCleanEOF(t *testing.T) {
	_, err := readRecord(bytes.NewReader(nil))
	if err != io.EOF {
		t.Fatalf("err = %v, want io.EOF", err)
	}
}

func TestReadRecordCorruptChecksum(t *testing.T) {
	buf := encode(Entry{Op: OpPut, Key: []byte("a"), Value: []byte("1")})
	buf[0] ^= 0xFF // flip a bit in the checksum field

	_, err := readRecord(bytes.NewReader(buf))
	if err != errTorn {
		t.Fatalf("err = %v, want errTorn", err)
	}
}

func TestReadRecordCorruptLengthFieldRejectedFast(t *testing.T) {
	// A corrupted key_len claiming ~4GB must be rejected before any
	// allocation is attempted, not fail later via a huge ReadFull.
	buf := make([]byte, 4+1+4)
	binary.BigEndian.PutUint32(buf[5:9], 0xFFFFFFF0)

	_, err := readRecord(bytes.NewReader(buf))
	if err != errTorn {
		t.Fatalf("err = %v, want errTorn", err)
	}
}

func TestReadRecordTruncated(t *testing.T) {
	buf := encode(Entry{Op: OpPut, Key: []byte("a"), Value: []byte("1")})

	for cut := 1; cut < len(buf); cut++ {
		_, err := readRecord(bytes.NewReader(buf[:cut]))
		if err != errTorn {
			t.Fatalf("cut=%d: err = %v, want errTorn", cut, err)
		}
	}
}
