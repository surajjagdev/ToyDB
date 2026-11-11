package recovery

import "errors"

// Has the sentinel errors encountered during recovery

var (
	// WAL nil error
	ErrWalNil = errors.New("recovery: nil WAL")

	// When wal scan hits partially written record. Treats as end of file
	ErrTornTail = errors.New("recovery: torn tail")

	// Wal has no records, treats as new db. Nothing to undo
	ErrEmptyWal = errors.New("recovery: empty wal")

	// PrevLSN chain is broken and chain did not end with LSN zero. Record required to undo
	// txn is missing
	ErrCorruptChain = errors.New("recovery: corrupt undo chain")
)
