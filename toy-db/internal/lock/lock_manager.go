package lock

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/surajjagdev/ToyDB/internal/common"
)

// For transaction pessimitsic locking

// Get the global identifier from relation and record id
func RowIDFromRecordID(rel common.RelationID, recordId common.RecordId) common.RowId {
	return common.RowId{
		RelationID: rel,
		BlockID:    recordId.BlockID,
		Slot:       recordId.Slot,
	}
}

type LockMode uint8

const (
	LockShared LockMode = iota + 1
	LockExclusive
)

type Grant struct {
	RowId common.RowId
	Mode  LockMode
}

// Metrics is an optional observability hook.
type Metrics interface {
	ObserveLockWait(time.Duration)
	ObserveLockQueueDepth(int)
	IncLockWaitTimeout()
	IncDeadlock()
}

type lockEntry struct {
	holders   map[common.TransactionID]LockMode
	waitQueue []*waiter // FIFO;
}

type waiter struct {
	txid common.TransactionID
	mode LockMode
	done chan error
}

// Lock manager - one per db instance
type Manager struct {
	mu sync.Mutex

	table map[common.RowId]*lockEntry

	// byTx is a reverse index so ReleaseAll is O(locks held by tx) instead
	// of O(total locks).
	byTx map[common.TransactionID]map[common.RowId]struct{}

	// dependency graph
	waitFor map[common.TransactionID]common.TransactionID

	waitTimeout time.Duration
	metrics     Metrics
}

func NewLockManager(waitTimeout time.Duration, m Metrics) *Manager {
	return &Manager{
		table:       make(map[common.RowId]*lockEntry),
		byTx:        make(map[common.TransactionID]map[common.RowId]struct{}),
		waitFor:     make(map[common.TransactionID]common.TransactionID),
		waitTimeout: waitTimeout,
		metrics:     m,
	}
}

// Acquire blocks till we hold lock, or context is cancelled, or timeout
// on success returns nil
func (m *Manager) Acquire(ctx context.Context, txnId common.TransactionID, row common.RowId, mode LockMode) error {
	m.mu.Lock()

	entry := m.table[row]

	// table has no row yet
	if entry == nil {
		entry = &lockEntry{holders: make(map[common.TransactionID]LockMode)}
		m.table[row] = entry
	}

	// already holding lock, or stronger ?
	if held, ok := entry.holders[txnId]; ok {
		if held >= mode {
			m.mu.Unlock()
			return nil
		}
		// Upgrade S → X. Only legal if we're the sole holder.
		if len(entry.holders) == 1 && len(entry.waitQueue) == 0 {
			entry.holders[txnId] = mode
			m.mu.Unlock()
			return nil
		}
	}

	if m.canGrantNewLocked(entry, txnId, mode) {
		entry.holders[txnId] = mode
		m.trackLocked(txnId, row)
		m.mu.Unlock()
		return nil
	}

	// Queue, then block.
	w := &waiter{txid: txnId, mode: mode, done: make(chan error, 1)}
	entry.waitQueue = append(entry.waitQueue, w)

	// Record the wait-for edge. We pick an arbitrary holder to point at;
	// the graph only needs to know "who am I stuck behind" to detect cycles.
	for holder := range entry.holders {
		if holder != txnId {
			m.waitFor[txnId] = holder
			break
		}
	}
	if m.metrics != nil {
		m.metrics.ObserveLockQueueDepth(len(entry.waitQueue))
	}
	m.mu.Unlock()

	start := time.Now()
	timer := time.NewTimer(m.waitTimeout)
	defer timer.Stop()

	select {
	case err := <-w.done:
		if m.metrics != nil {
			m.metrics.ObserveLockWait(time.Since(start))
		}
		return err
	case <-ctx.Done():
		m.abandonWait(row, w)
		return ctx.Err()
	case <-timer.C:
		m.abandonWait(row, w)
		if m.metrics != nil {
			m.metrics.IncLockWaitTimeout()
		}
		return fmt.Errorf("%w: %v", ErrLockTimeout, row)
	}

}

// Release drops txid's lock on row and wakes any waiters that can proceed.
func (m *Manager) Release(txid common.TransactionID, row common.RowId) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.releaseLocked(txid, row)
}

// ReleaseAll drops every lock held by txid. Called on commit or abort.
func (m *Manager) ReleaseAll(txid common.TransactionID) {
	m.mu.Lock()
	defer m.mu.Unlock()

	rows := m.byTx[txid]
	// Copy keys before mutating the map underneath us.
	keys := make([]common.RowId, 0, len(rows))
	for r := range rows {
		keys = append(keys, r)
	}
	for _, r := range keys {
		m.releaseLocked(txid, r)
	}
	delete(m.byTx, txid)
	delete(m.waitFor, txid)
}

// HeldBy reports whether txid currently holds a lock (any mode) on row.
func (m *Manager) HeldBy(txid common.TransactionID, row common.RowId) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	e := m.table[row]
	if e == nil {
		return false
	}
	_, ok := e.holders[txid]
	return ok
}

// ---- internal helpers (all require m.mu held) ----

// compatibleLocked reports whether txid can hold mode alongside the current
// holders of e, ignoring the wait queue. Used by both fresh arrivals (after
// the queue check) and by promotion of the front waiter.
func (m *Manager) compatibleLocked(e *lockEntry, txid common.TransactionID, mode LockMode) bool {
	for holder, held := range e.holders {
		if holder == txid {
			continue
		}
		if held == LockExclusive || mode == LockExclusive {
			return false
		}
	}
	return true
}

// canGrantNewLocked reports whether a fresh arrival can be granted mode
// immediately. FIFO discipline: a newcomer may not jump ahead of any queued
// waiter, even if the lock would otherwise be free.
func (m *Manager) canGrantNewLocked(e *lockEntry, txid common.TransactionID, mode LockMode) bool {
	if len(e.waitQueue) > 0 {
		return false
	}
	return m.compatibleLocked(e, txid, mode)
}

func (m *Manager) releaseLocked(txid common.TransactionID, row common.RowId) {
	entry := m.table[row]
	if entry == nil {
		return
	}
	if _, ok := entry.holders[txid]; !ok {
		return
	}
	delete(entry.holders, txid)

	if set := m.byTx[txid]; set != nil {
		delete(set, row)
		if len(set) == 0 {
			delete(m.byTx, txid)
		}
	}
	delete(m.waitFor, txid)

	m.promoteWaitersLocked(entry, row)
}

// promoteWaitersLocked grants as many queued waiters as possible, in FIFO
// order. Stops at the first waiter that would conflict.
func (m *Manager) promoteWaitersLocked(entry *lockEntry, row common.RowId) {
	for len(entry.waitQueue) > 0 {
		w := entry.waitQueue[0]
		// The front waiter has waited its turn; only holder compatibility
		// matters. Using canGrantNewLocked here would return false because
		// the queue is (still) non-empty, and no waiter would ever be
		// promoted.
		if !m.compatibleLocked(entry, w.txid, w.mode) {
			break
		}
		entry.waitQueue = entry.waitQueue[1:]
		entry.holders[w.txid] = w.mode
		m.trackLocked(w.txid, row)
		delete(m.waitFor, w.txid)
		w.done <- nil
	}

	if len(entry.holders) == 0 && len(entry.waitQueue) == 0 {
		delete(m.table, row)
	}
}

func (m *Manager) trackLocked(txid common.TransactionID, row common.RowId) {
	if m.byTx[txid] == nil {
		m.byTx[txid] = make(map[common.RowId]struct{})
	}
	m.byTx[txid][row] = struct{}{}
}

// abandonWait removes a waiter that gave up (ctx or timeout) and promotes the
// next eligible waiter.
func (m *Manager) abandonWait(row common.RowId, w *waiter) {
	m.mu.Lock()
	defer m.mu.Unlock()
	entry := m.table[row]
	if entry == nil {
		return
	}
	for i, qw := range entry.waitQueue {
		if qw == w {
			entry.waitQueue = append(entry.waitQueue[:i], entry.waitQueue[i+1:]...)
			break
		}
	}
	delete(m.waitFor, w.txid)
	m.promoteWaitersLocked(entry, row)
}
