package transaction

import (
	"context"
	"fmt"
	"sync"

	"github.com/surajjagdev/ToyDB/internal/common"
	"github.com/surajjagdev/ToyDB/internal/lock"
	"github.com/surajjagdev/ToyDB/internal/wal"
)

type State uint8

const (
	Active State = iota + 1
	Committing
	Committed
	Aborting
	Aborted
)

// handle per txns, contains snapshots and undo chains
type Transaction struct {
	Id       common.TransactionID
	State    State
	Mode     TransactionMode
	Snapshot Snapshot
	LastLSN  wal.LSN
	FirstLSN wal.LSN

	manager *TransactionManager

	mu         sync.Mutex
	lockedRows map[common.RowId]struct{} // pessimistic locks held

	// commandID is the ID of the statement currently executing within this
	// transaction. It starts at 0 (InvalidCommandID) and is incremented by
	// the executor at the start of each statement.
	//
	// Tuple headers record the command ID that created or deleted them.
	// Visibility within a transaction uses it to distinguish "wrote this
	// before my current command" (visible) from "wrote this during or
	// after my current command" (not yet visible to earlier statements in
	// the same tx).
	commandID common.CommandID

	ctx context.Context // cancellation + commit deadline
}

// Log appends a WAL record within this transaction, threading the undo chain.
// The record's TxID and PrevLSN fields are overwritten.
func (tx *Transaction) Log(rec *wal.Record) (wal.LSN, error) {
	rec.TxID = tx.Id
	rec.PrevLSN = tx.LastLSN
	lsn, err := tx.manager.wal.Append(rec)
	if err != nil {
		return 0, err
	}
	tx.LastLSN = lsn
	return lsn, nil
}

// LockRow acquires a lock on row in the given mode. Records the lock in the
// tx's own set so it can be released eagerly if needed, though the lock
// manager's ReleaseAll covers the common path.
func (tx *Transaction) LockRow(row common.RowId, mode lock.LockMode) error {
	if err := tx.manager.locks.Acquire(tx.ctx, tx.Id, row, mode); err != nil {
		return err
	}
	tx.mu.Lock()
	if tx.lockedRows == nil {
		tx.lockedRows = make(map[common.RowId]struct{})
	}
	tx.lockedRows[row] = struct{}{}
	tx.mu.Unlock()
	return nil
}

// HoldsLock reports whether this tx holds a lock on row.
func (tx *Transaction) HoldsLock(row common.RowId) bool {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	_, ok := tx.lockedRows[row]
	return ok
}

// String is for logs.
func (tx *Transaction) String() string {
	return fmt.Sprintf("tx{id=%d mode=%v state=%v lastLSN=%v}",
		tx.Id, tx.Mode, tx.State, tx.LastLSN)
}

// NextCommandID advances the command counter and returns the new value. The
// executor calls this once per statement, before executing it.
//
// The first call returns 1; 0 is reserved as InvalidCommandID.
func (tx *Transaction) NextCommandID() common.CommandID {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	tx.commandID++
	return tx.commandID
}

// CurrentCommandID returns the command ID of the statement currently
// executing, without advancing it. Used by the visibility predicate.
func (tx *Transaction) CurrentCommandID() common.CommandID {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	return tx.commandID
}
