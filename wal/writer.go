package wal

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// ErrTornSegment is returned by NewWriter when the target segment already
// exists and ends mid-record. Appending past a torn tail would make every
// new record unreachable, since Replay stops at a segment's first torn
// record and never looks past it -- resume writes into a fresh segment
// via NextSegmentPath instead.
var ErrTornSegment = errors.New("wal: segment has a torn tail")

// DefaultMaxSegmentBytes is the size threshold at which a segment is
// rotated. Tunable via Writer.SetMaxSegmentBytes.
const DefaultMaxSegmentBytes = 4 * 1024 * 1024

// segmentFile is satisfied by *os.File. It exists so tests can substitute
// a spy that records call order, to demonstrably prove Append fsyncs
// before returning rather than just asserting on side effects that would
// also hold true without an fsync (e.g. write visibility to a second
// file handle on the same machine, which the page cache provides with or
// without a Sync call).
type segmentFile interface {
	Write(p []byte) (int, error)
	Sync() error
	Close() error
}

// fsyncDir fsyncs a directory's own metadata (its entries: creations,
// renames, deletions), as opposed to fsyncDir's arg being a file whose
// *contents* need syncing. A file's own fsync only guarantees the file's
// bytes survive a crash; without also fsyncing the directory, the entry
// that makes the file findable at all (a create, a rename, a delete) can
// be lost separately on some filesystems, even though the write it
// pointed at was itself fully durable. This is a package-level var
// (rather than a plain function) so tests can substitute a spy, the same
// technique segmentFile substitution uses for Append -- proving fsync
// was actually invoked, not just that the end state looks right.
var fsyncDir = func(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

type Writer struct {
	dir      string
	seq      int
	maxBytes int64
	f        segmentFile
	size     int64
}

// NewWriter opens (creating if necessary) the WAL segment at path for
// appending. path's basename must follow the "NNNNNN.log" convention so
// rotation can derive the next segment name.
func NewWriter(path string) (*Writer, error) {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}

	seq, err := ParseSegmentSeq(filepath.Base(path))
	if err != nil {
		return nil, err
	}

	statInfo, statErr := os.Stat(path)
	isNew := os.IsNotExist(statErr)
	switch {
	case statErr == nil && statInfo.Size() > 0:
		torn, err := segmentHasTornTail(path)
		if err != nil {
			return nil, err
		}
		if torn {
			return nil, fmt.Errorf("wal: %s: %w (use NextSegmentPath to resume writes after recovery)", path, ErrTornSegment)
		}
	case statErr != nil && !isNew:
		return nil, statErr
	}

	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}

	if isNew {
		// The file's own content has nothing to fsync yet (it's empty),
		// but the directory entry that makes it discoverable is new and
		// must be made durable on its own -- see fsyncDir.
		if err := fsyncDir(dir); err != nil {
			f.Close()
			return nil, err
		}
	}

	return &Writer{
		dir:      dir,
		seq:      seq,
		maxBytes: DefaultMaxSegmentBytes,
		f:        f,
		size:     info.Size(),
	}, nil
}

// SetMaxSegmentBytes overrides the rotation threshold.
func (w *Writer) SetMaxSegmentBytes(n int64) {
	w.maxBytes = n
}

// Append serializes e, appends it to the current segment, and fsyncs
// before returning. It does not return nil until the fsync has completed
// successfully. If the write crosses the rotation threshold, the segment
// is rotated after this record is durably written.
func (w *Writer) Append(e Entry) error {
	buf := EncodeEntry(e)

	if _, err := w.f.Write(buf); err != nil {
		return err
	}
	if err := w.f.Sync(); err != nil {
		return err
	}
	w.size += int64(len(buf))

	if w.size >= w.maxBytes {
		if err := w.rotate(); err != nil {
			return err
		}
	}
	return nil
}

func (w *Writer) rotate() error {
	if err := w.f.Close(); err != nil {
		return err
	}
	w.seq++
	f, err := os.OpenFile(filepath.Join(w.dir, segmentName(w.seq)), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	// Rotation always names a brand-new segment (w.seq only ever
	// increases), so its directory entry always needs the same
	// durability treatment NewWriter gives a freshly created segment.
	if err := fsyncDir(w.dir); err != nil {
		f.Close()
		return err
	}
	w.f = f
	w.size = 0
	return nil
}

func (w *Writer) Close() error {
	return w.f.Close()
}

// NextSegmentPath returns the path for a brand-new, empty segment in dir,
// one past the highest existing segment sequence number (or 000001.log if
// dir has none yet).
//
// Callers resuming writes after a crash-recovery Replay MUST use this
// instead of reopening the last existing segment. The last segment may
// have a torn tail (the record being written when the crash happened);
// appending new valid records after that torn tail would leave them
// permanently unreachable, since Replay stops at the first torn record it
// finds and never looks past it — including at later, fully valid records
// in the same file.
func NextSegmentPath(dir string) (string, error) {
	names, err := SegmentNames(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return filepath.Join(dir, segmentName(1)), nil
		}
		return "", err
	}
	if len(names) == 0 {
		return filepath.Join(dir, segmentName(1)), nil
	}

	seq, err := ParseSegmentSeq(names[len(names)-1])
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, segmentName(seq+1)), nil
}

// RemoveSegmentsBefore deletes every WAL segment in dir with a sequence
// number strictly less than seq, then fsyncs dir so the deletions are
// durable.
//
// This has no way to know, on its own, whether the data those segments
// held is safe to lose -- callers (DB, after a flush) must only call it
// once that data is durably captured elsewhere (a flushed SSTable). A
// crash between the SSTable's rename and this call is safe either way:
// worst case, the obsolete segments just aren't cleaned up yet, and a
// later flush's cleanup (or none at all) is the only consequence --
// never data loss, since nothing here runs until after the SSTable that
// supersedes these segments is already durable.
func RemoveSegmentsBefore(dir string, seq int) error {
	names, err := SegmentNames(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	var removedAny bool
	for _, name := range names {
		s, err := ParseSegmentSeq(name)
		if err != nil {
			return err
		}
		if s >= seq {
			continue
		}
		if err := os.Remove(filepath.Join(dir, name)); err != nil {
			return err
		}
		removedAny = true
	}

	if removedAny {
		return fsyncDir(dir)
	}
	return nil
}

func segmentName(seq int) string {
	return fmt.Sprintf("%06d.log", seq)
}

// ParseSegmentSeq extracts the sequence number from a segment filename
// following the "NNNNNN.log" convention (§4).
func ParseSegmentSeq(name string) (int, error) {
	base := strings.TrimSuffix(name, ".log")
	seq, err := strconv.Atoi(base)
	if err != nil {
		return 0, fmt.Errorf("wal: invalid segment filename %q: %w", name, err)
	}
	return seq, nil
}
