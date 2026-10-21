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

// Transaction represents a single user's connection doing work.
type Transaction struct {
	Id             common.TransactionID
	State          State
	IsolationLevel IsolationLevel
	Snapshot       Snapshot
	LastLSN        wal.LSN
	FirstLSN       wal.LSN

	manager *TransactionManager

	mu sync.Mutex

	// commandID handles the Halloween Problem (updates seeing their own inserts)
	commandID common.CommandID

	ctx context.Context

	// NEW: Channel closed when the transaction completes (Commit/Abort).
	// Other queries sleep on this channel when they hit a row lock (xmax)!
	done chan struct{}
}

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

func (tx *Transaction) String() string {
	return fmt.Sprintf("tx{id=%d iso=%v state=%v lastLSN=%v}",
		tx.Id, tx.IsolationLevel, tx.State, tx.LastLSN)
}

func (tx *Transaction) NextCommandID() common.CommandID {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	tx.commandID++
	return tx.commandID
}

func (tx *Transaction) CurrentCommandID() common.CommandID {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	return tx.commandID
}

// LockRow is a no-op until the lock manager is reintegrated. SELECT FOR
// UPDATE / FOR SHARE currently read the current version without blocking
// other writers. Correct under optimistic-only execution.
func (tx *Transaction) LockRow(row common.RowId, mode lock.LockMode) error {
	return nil
}

// HoldsLock always returns false until the lock manager is reintegrated.
func (tx *Transaction) HoldsLock(row common.RowId) bool {
	return false
}
