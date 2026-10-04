package transaction

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/surajjagdev/ToyDB/internal/common"
	"github.com/surajjagdev/ToyDB/internal/lock"
	"github.com/surajjagdev/ToyDB/internal/wal"
)

var (
	ErrTxNotActive          = errors.New("tx: transaction not active")
	ErrSerializationFailure = errors.New("tx: serialization failure")
	ErrTxAborted            = errors.New("tx: transaction aborted")
)

// Manager allocates transaction IDs, constructs snapshots, tracks active
// transactions, and drives commit/abort through the WAL and lock manager.
type TransactionManager struct {
	mu sync.Mutex

	wal   *wal.WAL
	locks *lock.Manager

	nextTxID common.TransactionID
	active   map[common.TransactionID]*Transaction
	// sortedActive is kept sorted by txid so snapshot construction is cheap.
	sortedActive []common.TransactionID
}

func NewManager(w *wal.WAL, locks *lock.Manager) *TransactionManager {
	return &TransactionManager{
		wal:      w,
		locks:    locks,
		nextTxID: 1, // 0 is InvalidTransactionID
		active:   make(map[common.TransactionID]*Transaction),
	}
}

// Begin allocates a transaction ID, takes a snapshot, and starts the tx.
func (m *TransactionManager) Begin(ctx context.Context, opts BeginOptions) (*Transaction, error) {
	if opts.Mode == 0 {
		opts.Mode = Optimistic
	}

	m.mu.Lock()
	id := m.nextTxID
	m.nextTxID++

	snap := m.buildSnapshotLocked(id)

	tx := &Transaction{
		Id:       id,
		State:    Active,
		Mode:     opts.Mode,
		Snapshot: snap,
		manager:  m,
		ctx:      ctx,
	}

	m.active[id] = tx
	m.insertActiveSortedLocked(id)
	m.mu.Unlock()

	// Log Begin with the snapshot embedded, so recovery knows who was in
	// flight at this tx's start.
	beginPayload := encodeSnapshot(snap)
	lsn, err := m.wal.Append(&wal.Record{
		TxID: id,
		Type: wal.RecBegin,
		Data: beginPayload,
	})
	if err != nil {
		m.mu.Lock()
		delete(m.active, id)
		m.removeActiveSortedLocked(id)
		m.mu.Unlock()
		return nil, fmt.Errorf("wal begin: %w", err)
	}
	tx.LastLSN = lsn
	tx.FirstLSN = lsn
	return tx, nil
}

// Commit marks the transaction durable. Blocks until the commit record is
// flushed. Releases all row locks on success.
func (m *TransactionManager) Commit(tx *Transaction) error {
	m.mu.Lock()
	if tx.State != Active {
		m.mu.Unlock()
		return ErrTxNotActive
	}
	tx.State = Committing
	m.mu.Unlock()

	commitLSN, err := m.wal.Append(&wal.Record{
		TxID:    tx.Id,
		PrevLSN: tx.LastLSN,
		Type:    wal.RecCommit,
	})
	if err != nil {
		// Leave state as Committing; caller decides whether to retry or
		// abort.
		return fmt.Errorf("wal commit: %w", err)
	}

	// Durability barrier. Blocks until the commit record is fsynced, which
	// may batch with other commits in flight.
	if err := m.wal.FlushUpTo(commitLSN); err != nil {
		return fmt.Errorf("wal flush: %w", err)
	}

	// At this point the tx is durable. Clean up in-memory state.
	m.locks.ReleaseAll(tx.Id)

	m.mu.Lock()
	delete(m.active, tx.Id)
	m.removeActiveSortedLocked(tx.Id)
	tx.State = Committed
	tx.LastLSN = commitLSN
	m.mu.Unlock()
	return nil
}

// Abort rolls back the transaction by walking its undo chain. Writes a CLR
// for every undo so a crash mid-abort doesn't redo work.
// Abort rolls back the transaction by walking its undo chain backwards.
// Every undone record gets a CLR whose PrevLSN is the *undone record's*
// PrevLSN, so a crash mid-abort picks up exactly where it left off.
//
// If applier is nil, the undo walk is skipped. Callers pass nil when the
// transaction has no durable effects to reverse — either because it never
// logged a WAL record with a before-image, or because the caller already
// knows nothing needs undoing (tests using a no-op WAL, or a transaction
// that only read). The Abort record is still written so recovery classifies
// the tx as a loser.
func (m *TransactionManager) Abort(tx *Transaction, applier UndoApplier) error {
	m.mu.Lock()
	if tx.State != Active && tx.State != Committing {
		m.mu.Unlock()
		return ErrTxNotActive
	}
	tx.State = Aborting
	m.mu.Unlock()

	// Walk the undo chain backwards, if we have an applier.
	if applier != nil {
		lsn := tx.LastLSN
		for lsn != 0 {
			rec, err := m.wal.ReadAt(lsn)
			if err != nil {
				return fmt.Errorf("read record at %v: %w", lsn, err)
			}
			if rec.Flags&wal.FlagHasBefore != 0 {
				if err := applier.ApplyBeforeImage(&rec); err != nil {
					return fmt.Errorf("apply before-image at %v: %w", lsn, err)
				}
			}
			// CLR: its PrevLSN skips the record we just undid.
			clrLSN, err := m.wal.Append(&wal.Record{
				TxID:    tx.Id,
				PrevLSN: rec.PrevLSN,
				Type:    wal.RecClear,
				Data:    encodeCLR(rec.LSN),
			})
			if err != nil {
				return fmt.Errorf("wal CLR: %w", err)
			}
			tx.LastLSN = clrLSN
			lsn = rec.PrevLSN
		}
	}

	abortLSN, err := m.wal.Append(&wal.Record{
		TxID:    tx.Id,
		PrevLSN: tx.LastLSN,
		Type:    wal.RecAbort,
	})
	if err != nil {
		return fmt.Errorf("wal abort: %w", err)
	}
	if err := m.wal.FlushUpTo(abortLSN); err != nil {
		return fmt.Errorf("wal flush abort: %w", err)
	}

	m.locks.ReleaseAll(tx.Id)

	m.mu.Lock()
	delete(m.active, tx.Id)
	m.removeActiveSortedLocked(tx.Id)
	tx.State = Aborted
	tx.LastLSN = abortLSN
	m.mu.Unlock()
	return nil
}

// ActiveCount returns the number of in-flight transactions. Useful for
// metrics and for tests.
func (m *TransactionManager) ActiveCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.active)
}

// ---- internal ----

func (m *TransactionManager) buildSnapshotLocked(id common.TransactionID) Snapshot {
	snap := Snapshot{XMax: m.nextTxID}
	if len(m.sortedActive) == 0 {
		snap.XMin = id
		return snap
	}
	snap.XMin = m.sortedActive[0]
	if id < snap.XMin {
		snap.XMin = id
	}
	snap.InProgress = append(snap.InProgress, m.sortedActive...)
	return snap
}

func (m *TransactionManager) insertActiveSortedLocked(id common.TransactionID) {
	i := sort.Search(len(m.sortedActive), func(i int) bool {
		return m.sortedActive[i] >= id
	})
	m.sortedActive = append(m.sortedActive, 0)
	copy(m.sortedActive[i+1:], m.sortedActive[i:])
	m.sortedActive[i] = id
}

func (m *TransactionManager) removeActiveSortedLocked(id common.TransactionID) {
	i := sort.Search(len(m.sortedActive), func(i int) bool {
		return m.sortedActive[i] >= id
	})
	if i < len(m.sortedActive) && m.sortedActive[i] == id {
		m.sortedActive = append(m.sortedActive[:i], m.sortedActive[i+1:]...)
	}
}

// IsInProgress reports whether txid is currently running — i.e., it has
// begun but not yet committed or aborted.
//
// Used by MVCC visibility: a tuple's XMax set by an in-progress transaction
// does not (yet) hide the tuple from anyone. A tuple's XMin set by an
// in-progress transaction is not yet visible to anyone but the writer itself.
func (m *TransactionManager) IsInProgress(txid common.TransactionID) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.active[txid]
	return ok
}

// UndoApplier is implemented by the heap. Called during Abort and during
// recovery's undo pass.
type UndoApplier interface {
	ApplyBeforeImage(rec *wal.Record) error
}

// encodeSnapshot encodes a Snapshot into a WAL payload. Format:
//
//	XMin(4) | XMax(4) | count(4) | id_0(4) ... id_n(4)
func encodeSnapshot(s Snapshot) []byte {
	buf := make([]byte, 12+4*len(s.InProgress))
	common.ByteOrder.PutUint32(buf[0:], uint32(s.XMin))
	common.ByteOrder.PutUint32(buf[4:], uint32(s.XMax))
	common.ByteOrder.PutUint32(buf[8:], uint32(len(s.InProgress)))
	for i, id := range s.InProgress {
		common.ByteOrder.PutUint32(buf[12+4*i:], uint32(id))
	}
	return buf
}

func encodeCLR(undoneLSN wal.LSN) []byte {
	buf := make([]byte, 8)
	common.ByteOrder.PutUint64(buf, uint64(undoneLSN))
	return buf
}
