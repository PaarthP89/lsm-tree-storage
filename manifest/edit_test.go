package manifest

import (
	"bytes"
	"encoding/binary"
	"io"
	"testing"
)

func TestEncodeDecodeEditRoundTrip(t *testing.T) {
	cases := []VersionEdit{
		{Type: SSTableAdded, File: "000001.sst"},
		{Type: SSTableRemoved, File: "000002.sst"},
		{Type: SSTableAdded, File: ""},
		{Type: SSTableAdded, File: "a-somewhat-longer-filename-000999.sst"},
	}

	for _, want := range cases {
		buf := EncodeEdit(want)
		got, err := DecodeEdit(bytes.NewReader(buf))
		if err != nil {
			t.Fatalf("DecodeEdit(%+v): %v", want, err)
		}
		if got.Type != want.Type {
			t.Errorf("Type = %v, want %v", got.Type, want.Type)
		}
		if got.File != want.File {
			t.Errorf("File = %q, want %q", got.File, want.File)
		}
	}
}

func TestDecodeEditCleanEOF(t *testing.T) {
	_, err := DecodeEdit(bytes.NewReader(nil))
	if err != io.EOF {
		t.Fatalf("err = %v, want io.EOF", err)
	}
}

func TestDecodeEditCorruptChecksum(t *testing.T) {
	buf := EncodeEdit(VersionEdit{Type: SSTableAdded, File: "000001.sst"})
	buf[0] ^= 0xFF // flip a bit in the checksum field

	_, err := DecodeEdit(bytes.NewReader(buf))
	if err != ErrCorrupt {
		t.Fatalf("err = %v, want ErrCorrupt", err)
	}
}

func TestDecodeEditCorruptLengthFieldRejectedFast(t *testing.T) {
	// A corrupted name_len claiming ~4GB must be rejected before any
	// allocation is attempted, not fail later via a huge ReadFull.
	buf := make([]byte, 4+1+4)
	binary.BigEndian.PutUint32(buf[5:9], 0xFFFFFFF0)

	_, err := DecodeEdit(bytes.NewReader(buf))
	if err != ErrCorrupt {
		t.Fatalf("err = %v, want ErrCorrupt", err)
	}
}

func TestDecodeEditTruncated(t *testing.T) {
	buf := EncodeEdit(VersionEdit{Type: SSTableAdded, File: "000001.sst"})

	for cut := 1; cut < len(buf); cut++ {
		_, err := DecodeEdit(bytes.NewReader(buf[:cut]))
		if err != ErrCorrupt {
			t.Fatalf("cut=%d: err = %v, want ErrCorrupt", cut, err)
		}
	}
}
