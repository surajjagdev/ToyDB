package wal

import "errors"

var (
	// ErrClosed is returned by Append after Close has been called.
	ErrClosed = errors.New("wal: closed")

	// ErrCorrupt indicates a record failed CRC validation at a position
	// where the tail is not expected to be torn. This is fatal — it means
	// the log was damaged after being written.
	ErrCorrupt = errors.New("wal: corrupt record")

	// ErrShortRead indicates a buffer ended mid-record. Normal at the very
	// tail after a crash. Callers scanning forward treat it as end-of-log.
	ErrShortRead = errors.New("wal: short read")

	// ErrFsync indicates an fsync/fdatasync failure. Fatal: durability can
	// no longer be guaranteed. The WAL enters a poisoned state and all
	// subsequent Appends fail.
	ErrFsync = errors.New("wal: fsync failed")

	// ErrBadLSN indicates a read started at an LSN that is not a record
	// boundary, or before the earliest retained segment.
	ErrBadLSN = errors.New("wal: bad LSN")

	ErrRecordSize = errors.New("wal: record exceeds max size")
)
