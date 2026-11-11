package transaction

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/surajjagdev/ToyDB/internal/clog"
	"github.com/surajjagdev/ToyDB/internal/common"
	"github.com/surajjagdev/ToyDB/internal/wal"
)

var (
	ErrTxNotActive          = errors.New("tx: transaction not active")
	ErrSerializationFailure = errors.New("tx: serialization failure")
	ErrTxAborted            = errors.New("tx: transaction aborted")
)

// Special transaction IDs. These reserve the low xid space for system
// purposes and are treated as "always committed" by visibility checks.
const (
	InvalidXID   common.TransactionID = 0
	BootstrapXID common.TransactionID = 1
	FrozenXID    common.TransactionID = 2
)

// TransactionManager allocates transaction IDs, constructs snapshots, tracks
// active transactions, and drives commit/abort through the WAL and CLOG.
//
// The WAL is the source of truth for durability; the CLOG is a persistent,
// O(1) index over the commit status of every transaction. During normal
// operation CLOG lives in memory and its file is flushed lazily. After a
// crash, recovery rebuilds CLOG from the WAL, so any unflushed CLOG writes
// are repaired automatically.
type TransactionManager struct {
	mu sync.RWMutex

	wal  *wal.WAL
	clog *clog.Manager

	nextTxID     common.TransactionID
	active       map[common.TransactionID]*Transaction
	sortedActive []common.TransactionID
}

func NewManager(w *wal.WAL, c *clog.Manager) *TransactionManager {
	return &TransactionManager{
		wal:      w,
		clog:     c,
		nextTxID: 3, // 0=Invalid, 1=Bootstrap, 2=Frozen
		active:   make(map[common.TransactionID]*Transaction),
	}
}

// -----------------------------------------------------------------------------
// Begin
// -----------------------------------------------------------------------------

// Begin allocates a transaction ID, takes a snapshot, and writes the Begin
// record to the WAL. The Begin record is buffered; it only becomes durable
// if the transaction later commits.
func (m *TransactionManager) Begin(ctx context.Context, opts BeginOptions) (*Transaction, error) {
	if opts.IsolationLevel == 0 {
		opts.IsolationLevel = ReadCommitted
	}

	m.mu.Lock()
	id := m.nextTxID
	m.nextTxID++

	snap := m.buildSnapshotLocked(id)

	tx := &Transaction{
		Id:             id,
		State:          Active,
		IsolationLevel: opts.IsolationLevel,
		Snapshot:       snap,
		manager:        m,
		ctx:            ctx,
		done:           make(chan struct{}),
	}

	m.active[id] = tx
	m.insertActiveSortedLocked(id)
	m.mu.Unlock()

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

// -----------------------------------------------------------------------------
// Commit
// -----------------------------------------------------------------------------

// Commit writes the Commit record, makes it durable, records the outcome in
// the CLOG, and removes the transaction from the active set.
//
// Durability order:
//  1. Append RecCommit to WAL
//  2. FlushUpTo(commitLSN) — this is the point of no return
//  3. SetStatus(Committed) in CLOG
//  4. Cleanup (drop from active set, close done channel)
//
// If the process crashes between steps 2 and 3, the WAL commit record is
// durable and recovery will populate CLOG from it. If it crashes between 3
// and 4, the CLOG already says Committed, and recovery is a no-op for this
// transaction.
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
		return fmt.Errorf("wal commit: %w", err)
	}

	// Durability barrier. Batches with other concurrent commits.
	if err := m.wal.FlushUpTo(commitLSN); err != nil {
		return fmt.Errorf("wal flush: %w", err)
	}

	// The commit is durable. Record the outcome in CLOG so subsequent
	// visibility checks can resolve this xid without scanning the WAL.
	if m.clog != nil {
		if err := m.clog.SetStatus(tx.Id, clog.Committed); err != nil {
			// CLOG failure here does not undo the commit — the transaction
			// IS durable. Recovery will reconcile CLOG from the WAL. We
			// return the error so the caller knows the visibility cache is
			// stale, but the transaction should be considered committed.
			return fmt.Errorf("clog set committed (tx is durable, visibility may be stale): %w", err)
		}
	}

	m.mu.Lock()
	delete(m.active, tx.Id)
	m.removeActiveSortedLocked(tx.Id)
	tx.State = Committed
	tx.LastLSN = commitLSN
	close(tx.done)
	m.mu.Unlock()
	return nil
}

// -----------------------------------------------------------------------------
// Abort
// -----------------------------------------------------------------------------

// Abort rolls back the transaction by walking its undo chain backwards,
// applying each record's before-image and writing a CLR. Then writes the
// Abort record, flushes it, and records the outcome in CLOG.
//
// If applier is nil, the undo walk is skipped. Callers pass nil when the
// transaction has no durable effects to reverse (read-only transactions,
// failed pre-checks, or tests with a no-op WAL). The Abort record is still
// written so recovery classifies the transaction as a loser.
func (m *TransactionManager) Abort(tx *Transaction, applier UndoApplier) error {
	m.mu.Lock()
	if tx.State != Active && tx.State != Committing {
		m.mu.Unlock()
		return ErrTxNotActive
	}
	tx.State = Aborting
	m.mu.Unlock()

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

	if m.clog != nil {
		if err := m.clog.SetStatus(tx.Id, clog.Aborted); err != nil {
			return fmt.Errorf("clog set aborted (abort is durable, visibility may be stale): %w", err)
		}
	}

	m.mu.Lock()
	delete(m.active, tx.Id)
	m.removeActiveSortedLocked(tx.Id)
	tx.State = Aborted
	tx.LastLSN = abortLSN
	close(tx.done)
	m.mu.Unlock()
	return nil
}

// -----------------------------------------------------------------------------
// Queries
// -----------------------------------------------------------------------------

// ActiveCount returns the number of in-flight transactions.
func (m *TransactionManager) ActiveCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.active)
}

// IsInProgress reports whether txid is currently running.
func (m *TransactionManager) IsInProgress(txid common.TransactionID) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, ok := m.active[txid]
	return ok
}

// IsCommitted reports whether txid's changes are durable and visible.
//
// Resolution order:
//  1. Special xids (0, 1, 2) — Bootstrap and Frozen are visible; Invalid
//     should never appear on a tuple, but returning true is harmless.
//  2. Active transaction → false (still running, may abort).
//  3. CLOG Committed → true; Aborted → false.
//  4. CLOG InProgress (unknown) → true.
//
// Rule 4 is a fail-open default for tuples whose xid predates this CLOG
// instance. After recovery completes, every xid that ever existed is in the
// CLOG, so rule 4 only fires for genuinely ancient data — treating it as
// committed matches the assumption that it was committed long ago.
func (m *TransactionManager) IsCommitted(xid common.TransactionID) (bool, error) {
	if xid <= FrozenXID {
		return true, nil
	}

	m.mu.RLock()
	_, active := m.active[xid]
	m.mu.RUnlock()
	if active {
		return false, nil
	}

	if m.clog == nil {
		// No CLOG configured: assume committed. This is the pre-CLOG
		// behavior and is correct only in tests without real aborts.
		return true, nil
	}

	s, err := m.clog.GetStatus(xid)
	if err != nil {
		return false, err
	}
	switch s {
	case clog.Committed:
		return true, nil
	case clog.Aborted:
		return false, nil
	default:
		// InProgress — unknown. Fail-open as documented above.
		return true, nil
	}
}

// GetTxnState returns the state of a transaction. Same resolution rules as
// IsCommitted, mapped to the State enum.
func (m *TransactionManager) GetTxnState(xid common.TransactionID) State {
	if xid <= FrozenXID {
		return Committed
	}

	m.mu.RLock()
	_, active := m.active[xid]
	m.mu.RUnlock()
	if active {
		return Active
	}

	if m.clog == nil {
		return Committed
	}

	s, err := m.clog.GetStatus(xid)
	if err != nil {
		return Committed // fail-open
	}
	switch s {
	case clog.Committed:
		return Committed
	case clog.Aborted:
		return Aborted
	default:
		return Committed // fail-open
	}
}

// ActiveTxIDs returns a sorted snapshot of running transaction IDs. Used by
// the checkpoint manager to embed in a checkpoint record.
func (m *TransactionManager) ActiveTxIDs() []common.TransactionID {
	m.mu.RLock()
	defer m.mu.RUnlock()

	out := make([]common.TransactionID, len(m.sortedActive))
	copy(out, m.sortedActive)
	return out
}

// NextTxID returns the next transaction ID that will be allocated. Used by
// recovery to resume allocation after scanning the WAL.
func (m *TransactionManager) NextTxID() common.TransactionID {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.nextTxID
}

// returns lowest min txn across all active transactions
// returns the minimum of snapshot's xmin, not the minimum active txid.
// bc snapshot's xmin is lowest xid that was in-flight when snapshot was taken
// even if the txn that was active at snapshot was completed, their effects
// are still visible
func (m *TransactionManager) OldestActiveSnapshot() common.TransactionID {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if len(m.sortedActive) == 0 {
		// no active txns
		return m.nextTxID
	}

	var oldest common.TransactionID
	first := true

	for _, txid := range m.sortedActive {
		tx := m.active[txid]

		if tx == nil {
			continue
		}

		if first || tx.Snapshot.XMin < oldest {
			oldest = tx.Snapshot.XMin
			first = false
		}
	}

	if first {
		return m.nextTxID
	}

	return oldest
}

// -----------------------------------------------------------------------------
// Wait-for-completion
// -----------------------------------------------------------------------------

// WaitForTxn blocks until targetXid has committed or aborted, or ctx is
// cancelled. If targetXid is already finished (or was never active), it
// returns immediately.
func (m *TransactionManager) WaitForTxn(ctx context.Context, targetXid common.TransactionID) error {
	m.mu.RLock()
	targetTx, isActive := m.active[targetXid]
	m.mu.RUnlock()

	if !isActive {
		return nil
	}

	select {
	case <-targetTx.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// -----------------------------------------------------------------------------
// Recovery-facing
// -----------------------------------------------------------------------------

// RecordState records the final state of a transaction in the CLOG. Used by
// recovery to populate the CLOG after the analysis pass (committed) and the
// undo pass (aborted).
//
// Must be called only during recovery, before the database accepts client
// connections. Requires a non-nil CLOG.
func (m *TransactionManager) RecordState(xid common.TransactionID, state State) error {
	if m.clog == nil {
		return fmt.Errorf("tx: RecordState requires CLOG")
	}
	switch state {
	case Committed:
		return m.clog.SetStatus(xid, clog.Committed)
	case Aborted:
		return m.clog.SetStatus(xid, clog.Aborted)
	default:
		return fmt.Errorf("tx: RecordState called with non-final state %v", state)
	}
}

// ResetForRecovery replaces the manager's dynamic state with values derived
// from the WAL. It clears the active set and sets nextTxID for future
// allocation.
//
// Called by the recovery driver after analysis and undo complete, before
// client traffic is accepted. Any transaction that was active at crash time
// must already have been resolved by the undo pass.
func (m *TransactionManager) ResetForRecovery(nextTxID common.TransactionID) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.active = make(map[common.TransactionID]*Transaction)
	m.sortedActive = nil
	m.nextTxID = nextTxID
}

// -----------------------------------------------------------------------------
// Internal helpers
// -----------------------------------------------------------------------------

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

// -----------------------------------------------------------------------------
// Interfaces and codecs
// -----------------------------------------------------------------------------

// UndoApplier is implemented by the storage layer (heap). Called during Abort
// and during recovery's undo pass. Implementations must be idempotent under
// repeated application.
type UndoApplier interface {
	ApplyBeforeImage(rec *wal.Record) error
}

// encodeSnapshot produces the RecBegin payload.
//
// Layout: XMin(4) | XMax(4) | count(4) | id_0(4) ... id_n(4)
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

// encodeCLR produces the RecClear payload: the LSN of the record being
// undone.
func encodeCLR(undoneLSN wal.LSN) []byte {
	buf := make([]byte, 8)
	common.ByteOrder.PutUint64(buf, uint64(undoneLSN))
	return buf
}
