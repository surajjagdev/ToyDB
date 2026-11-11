package wal

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/surajjagdev/ToyDB/internal/storage/disk"
)

type Iterator struct {
	w      *WAL
	segNo  uint32
	offset uint32
	entry  *disk.VfdEntry

	done bool
}

// IterateFrom returns an iterator starting at lsn.
//
// If lsn is zero, iteration begins at the earliest retained segment. This is
// the "full scan" mode used by recovery when no checkpoint exists.
//
// If lsn points at a valid record boundary, iteration begins there. A
// caller passing a non-boundary LSN will get ErrCorrupt from the first
// Next() because the LSN check at record load will fail.
//
// The iterator must be closed by the caller when done. Failing to close
// leaks a VFD entry.
func (w *WAL) IterateFrom(lsn LSN) (*Iterator, error) {
	if w.closed.Load() {
		return nil, ErrClosed
	}

	var startSeg uint32
	var startOff uint32

	if lsn.IsZero() {
		// Find the earliest retained segment.
		segs, err := listAllSegments(w.opts.Dir)
		if err != nil {
			return nil, fmt.Errorf("wal iterate: list segments: %w", err)
		}
		if len(segs) == 0 {
			// Empty WAL. Return an iterator that immediately reports EOF.
			return &Iterator{w: w, done: true}, nil
		}
		startSeg = segs[0]
		startOff = 0
	} else {
		startSeg = lsn.GetSegmentNumber()
		startOff = lsn.GetOffset()
	}

	path := filepath.Join(w.opts.Dir, segmentName(startSeg))
	entry, err := w.vfd.GetOrOpen(disk.FileNode{Path: path}, os.O_RDWR)
	if err != nil {
		return nil, fmt.Errorf("wal iterate: open segment %d: %w", startSeg, err)
	}

	return &Iterator{
		w:      w,
		segNo:  startSeg,
		offset: startOff,
		entry:  entry,
	}, nil
}

// Close releases the iterator's segment handle. Idempotent.
func (it *Iterator) Close() error {
	if it.entry != nil {
		it.w.vfd.Release(it.entry)
		it.entry = nil
	}
	it.done = true
	return nil
}

// advance closes the current segment and opens the next. Returns false if
// there is no next segment (end of log).
func (it *Iterator) advance() bool {
	if it.entry != nil {
		it.w.vfd.Release(it.entry)
		it.entry = nil
	}
	nextSeg := it.segNo + 1
	path := filepath.Join(it.w.opts.Dir, segmentName(nextSeg))
	entry, err := it.w.vfd.GetOrOpen(disk.FileNode{Path: path}, os.O_RDWR)
	if err != nil {
		// No next segment — end of log.
		it.done = true
		return false
	}
	it.entry = entry
	it.segNo = nextSeg
	it.offset = 0
	return true
}

func (it *Iterator) Next() (Record, error) {
	for {
		if it.done || it.entry == nil {
			return Record{}, io.EOF
		}

		// read header at current offset
		hdr := make([]byte, RecordHeaderSize)
		n, err := it.entry.ReadAt(hdr, int64(it.offset))

		if err == io.EOF && n == 0 {
			// clean end of segment
			if !it.advance() {
				return Record{}, io.EOF
			}

			continue
		}

		if err != nil && err != io.EOF {
			return Record{}, fmt.Errorf("wal iterate: read header at %d:%d: %w", it.segNo, it.offset, err)
		}

		if n < RecordHeaderSize {
			// Partial header: torn tail.
			return Record{}, ErrShortRead
		}

		// Peek the record length from the header we already have.
		totalLen, err := PeekRecordLen(hdr)
		if err != nil {
			return Record{}, err
		}

		// Read the full record (header + payload).
		buf := make([]byte, totalLen)
		copy(buf, hdr)

		if totalLen > RecordHeaderSize {
			n, err = it.entry.ReadAt(
				buf[RecordHeaderSize:],
				int64(it.offset)+int64(RecordHeaderSize),
			)
			if err == io.EOF || n < int(totalLen)-RecordHeaderSize {
				return Record{}, ErrShortRead
			}
			if err != nil {
				return Record{}, fmt.Errorf("wal iterate: read body at %d:%d: %w",
					it.segNo, it.offset, err)
			}
		}
		var rec Record
		consumed, err := rec.Unmarshal(buf)
		if err != nil {
			return Record{}, err
		}

		want := MakeLSN(it.segNo, it.offset)
		if rec.LSN != want {
			return Record{}, fmt.Errorf(
				"%w: record at %v reports LSN %v", ErrCorrupt, want, rec.LSN)
		}

		it.offset += uint32(consumed)
		return rec, nil
	}
}
