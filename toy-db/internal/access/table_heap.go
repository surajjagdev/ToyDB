package access

import (
	"errors"
	"fmt"
	"sync"

	"github.com/surajjagdev/ToyDB/internal/catalog"
	"github.com/surajjagdev/ToyDB/internal/common"
	"github.com/surajjagdev/ToyDB/internal/lock"
	"github.com/surajjagdev/ToyDB/internal/storage/buffer"
	"github.com/surajjagdev/ToyDB/internal/storage/page/heap"
	"github.com/surajjagdev/ToyDB/internal/transaction"
	"github.com/surajjagdev/ToyDB/internal/wal"
)

// ErrTupleNotFound is returned when a row does not exist or is invisible to
// the caller's snapshot.
var ErrTupleNotFound = errors.New("tuple not found")

// TableHeap is the physical layer for a heap-organized relation. It provides
// MVCC-aware CRUD on tuples: every read filters by the transaction's snapshot
// and the transaction manager's historical state (committed/aborted), every
// write goes through the WAL before touching memory, and every write is
// optimistic (conflict detection via the tuple's xmax).
//
// Pessimistic mode (blocking on row locks) is deferred until the xmax-as-lock
// refactor lands. SELECT FOR UPDATE / SHARE currently do not block on other
// writers; they read the current version under the assumption that no other
// transaction is mid-write on the same row.
type TableHeap struct {
	bp       *buffer.BufferPool
	fsm      *FSM
	rel      common.RelationID
	fork     common.ForkID
	tm       *transaction.TransactionManager
	extendMu sync.Mutex
}

func NewTableHeap(
	bp *buffer.BufferPool,
	fsm *FSM,
	rel common.RelationID,
	tm *transaction.TransactionManager,
) *TableHeap {
	return &TableHeap{
		bp:   bp,
		fsm:  fsm,
		rel:  rel,
		fork: common.ForkMain,
		tm:   tm,
	}
}

// -----------------------------------------------------------------------------
// Page acquisition
// -----------------------------------------------------------------------------

// getPageForInsert finds a page with room for requiredSpace bytes, or extends
// the relation if no existing page has room. Returns the frame pinned.
func (th *TableHeap) getPageForInsert(
	requiredSpace int,
) (common.BlockID, *buffer.Frame, error) {
	targetBlockID, err := th.fsm.GetBlockWithFreeSpace(th.rel, requiredSpace)
	if err != nil {
		return common.InvalidBlockID, nil, fmt.Errorf("failed to get block with free space: %w", err)
	}

	// Fast path: FSM pointed at an existing page.
	if targetBlockID != common.InvalidBlockID {
		tag := buffer.BufferTag{RelationID: th.rel, ForkID: th.fork, BlockID: targetBlockID}
		frame, err, _ := th.bp.GetPage(tag)
		if err != nil {
			return common.InvalidBlockID, nil,
				fmt.Errorf("failed to fetch table page %d: %w", targetBlockID, err)
		}
		return targetBlockID, frame, nil
	}

	// No page with room; extend the relation under the extend lock.
	th.extendMu.Lock()

	// Re-check: another goroutine may have extended while we waited.
	targetBlockID, err = th.fsm.GetBlockWithFreeSpace(th.rel, requiredSpace)
	if err != nil {
		th.extendMu.Unlock()
		return common.InvalidBlockID, nil,
			fmt.Errorf("failed to get block with free space on re-check: %w", err)
	}
	if targetBlockID != common.InvalidBlockID {
		th.extendMu.Unlock()
		tag := buffer.BufferTag{RelationID: th.rel, ForkID: th.fork, BlockID: targetBlockID}
		frame, err, _ := th.bp.GetPage(tag)
		if err != nil {
			return common.InvalidBlockID, nil,
				fmt.Errorf("failed to fetch table page %d: %w", targetBlockID, err)
		}
		return targetBlockID, frame, nil
	}

	// Truly extend.
	diskManager, err := th.bp.GetDiskManager()
	if err != nil {
		th.extendMu.Unlock()
		return common.InvalidBlockID, nil, fmt.Errorf("disk manager reference failed: %w", err)
	}

	targetBlockID, err = diskManager.AllocateBlock(th.rel, th.fork)
	if err != nil {
		th.extendMu.Unlock()
		return common.InvalidBlockID, nil, fmt.Errorf("failed to allocate block: %w", err)
	}

	tag := buffer.BufferTag{BlockID: targetBlockID, ForkID: th.fork, RelationID: th.rel}
	frame, err := th.bp.AllocatePage(tag)
	if err != nil {
		th.extendMu.Unlock()
		return common.InvalidBlockID, nil,
			fmt.Errorf("failed to allocate page in buffer pool: %w", err)
	}

	frame.WLatch()
	heap.InitHeapPage(frame.Page)
	frame.WUnlatch()

	th.extendMu.Unlock()
	return targetBlockID, frame, nil
}

// -----------------------------------------------------------------------------
// Insert
// -----------------------------------------------------------------------------

// InsertTuple serializes tuple and inserts it into the first available page
// with room. Logs a RecInsert record to the WAL before mutating the page.
//
// The WAL record carries the full on-page tuple blob (header + null bitmap +
// user data + padding), so recovery's redo pass can install the bytes
// verbatim without reconstructing the header.
func (th *TableHeap) InsertTuple(
	t *transaction.Transaction,
	tuple *Tuple,
	schema *catalog.Schema,
) (common.RecordId, error) {
	data, nullBitmap := tuple.Serialize(schema)
	_, _, requiredSpace := heap.CalculateBytesNeededToInsertTuple(len(data), len(nullBitmap))

	for {
		targetBlockID, frame, err := th.getPageForInsert(requiredSpace)
		if err != nil {
			return common.RecordId{}, err
		}

		frame.WLatch()
		heapPage := &heap.HeapPage{Page: frame.Page}

		// FSM space hint can be stale; verify against the actual page.
		if heapPage.GetFreeSpace() < requiredSpace {
			frame.WUnlatch()
			frame.Unpin()
			_ = th.fsm.RecordFreeSpace(th.rel, targetBlockID, heapPage.GetFreeSpace())
			continue
		}

		// 1. Peek the slot the insert will use, so the WAL record names the
		// correct row. Safe to peek because we hold the write latch.
		slot, err := heapPage.NextSlot()
		if err != nil {
			frame.WUnlatch()
			frame.Unpin()
			return common.RecordId{}, err
		}
		row := common.RowId{RelationID: th.rel, BlockID: targetBlockID, Slot: slot}

		// 2. Build the exact bytes. Pure — no page mutation.
		tupleBytes := heapPage.BuildTupleBytes(
			data, nullBitmap,
			targetBlockID, slot,
			t.Id, 0, t.NextCommandID(),
		)

		// 3. WAL first — write-ahead rule. Payload carries the full tuple
		// blob, header included.
		lsn, err := t.Log(&wal.Record{
			Type:  wal.RecInsert,
			Flags: wal.FlagHasAfter,
			Data:  wal.EncodeInsert(row, tupleBytes),
		})
		if err != nil {
			frame.WUnlatch()
			frame.Unpin()
			return common.RecordId{}, fmt.Errorf("wal insert: %w", err)
		}

		// 4. Install — the actual page mutation.
		rId, err := heapPage.InstallTuple(slot, tupleBytes)
		if err != nil {
			frame.WUnlatch()
			frame.Unpin()
			return common.RecordId{}, fmt.Errorf("install tuple: %w", err)
		}

		// 5. Record the WAL position on the page so the buffer pool can
		// enforce the write-ahead rule when flushing.
		frame.Page.SetLSN(lsn)

		remainingSpace := heapPage.GetFreeSpace()
		frame.WUnlatch()
		th.bp.MarkDirtyWithLSN(frame, lsn)
		frame.Unpin()

		_ = th.fsm.RecordFreeSpace(th.rel, targetBlockID, remainingSpace)
		return rId, nil
	}
}

// -----------------------------------------------------------------------------
// Read
// -----------------------------------------------------------------------------

// GetTuple returns the tuple at rId as visible to t's snapshot. If t holds a
// lock on the row (from a prior SELECT FOR UPDATE / FOR SHARE), the current
// committed version is returned instead.
func (th *TableHeap) GetTuple(
	t *transaction.Transaction,
	rId common.RecordId,
	schema *catalog.Schema,
) (*Tuple, error) {
	row := common.RowId{RelationID: th.rel, BlockID: rId.BlockID, Slot: rId.Slot}
	if t.HoldsLock(row) {
		return th.currentVersion(rId, schema)
	}
	return th.snapshotVersion(t, rId, schema)
}

// snapshotVersion returns the version visible to t's snapshot, or
// ErrTupleNotFound if no visible version exists.
func (th *TableHeap) snapshotVersion(
	t *transaction.Transaction,
	rId common.RecordId,
	schema *catalog.Schema,
) (*Tuple, error) {
	tag := buffer.BufferTag{RelationID: th.rel, ForkID: th.fork, BlockID: rId.BlockID}
	frame, err, _ := th.bp.GetPage(tag)
	if err != nil {
		return nil, fmt.Errorf("get page %d: %w", rId.BlockID, err)
	}
	frame.RLatch()
	defer func() { frame.RUnlatch(); frame.Unpin() }()

	heapPage := &heap.HeapPage{Page: frame.Page}
	if !heapPage.DoesTupleInSlotExist(rId.Slot) {
		return nil, ErrTupleNotFound
	}

	header, err := heapPage.GetTupleHeader(rId.Slot)
	if err != nil {
		return nil, err
	}
	if !th.visibleTo(t, header) {
		return nil, ErrTupleNotFound
	}

	raw := heapPage.GetTupleWithSlot(rId.Slot)
	buf := make([]byte, len(raw))
	copy(buf, raw)

	return DeserializeTuple(buf, schema)
}

// currentVersion reads the latest committed version at rId, ignoring the
// snapshot. Used by SELECT FOR UPDATE / SHARE.
func (th *TableHeap) currentVersion(
	rId common.RecordId,
	schema *catalog.Schema,
) (*Tuple, error) {
	tag := buffer.BufferTag{RelationID: th.rel, ForkID: th.fork, BlockID: rId.BlockID}
	frame, err, _ := th.bp.GetPage(tag)
	if err != nil {
		return nil, fmt.Errorf("get page %d: %w", rId.BlockID, err)
	}
	frame.RLatch()
	defer func() { frame.RUnlatch(); frame.Unpin() }()

	heapPage := &heap.HeapPage{Page: frame.Page}
	if !heapPage.DoesTupleInSlotExist(rId.Slot) {
		return nil, ErrTupleNotFound
	}
	header, err := heapPage.GetTupleHeader(rId.Slot)
	if err != nil {
		return nil, err
	}

	// If XMax is set by a completed transaction, check whether that
	// transaction committed (row gone) or aborted (delete didn't take).
	if header.XMax != 0 && !th.tm.IsInProgress(header.XMax) {
		xmaxCommitted, cerr := th.tm.IsCommitted(header.XMax)
		if cerr == nil && xmaxCommitted {
			return nil, ErrTupleNotFound
		}
	}

	raw := heapPage.GetTupleWithSlot(rId.Slot)
	buf := make([]byte, len(raw))
	copy(buf, raw)

	return DeserializeTuple(buf, schema)
}

// visibleTo applies the MVCC visibility rule, consulting both the snapshot
// (for "was this transaction running when I started?") and the transaction
// manager's historical state (for "did this transaction commit or abort?").
//
// The rule, in order:
//
//  1. My own uncommitted writes are visible to me (unless I deleted them).
//  2. I deleted this version — invisible to me.
//  3. The creator must have committed before my snapshot:
//     a. Began after my snapshot (XMin >= Snapshot.XMax)     -> invisible.
//     b. Was running when my snapshot was taken              -> invisible.
//     c. Finished before my snapshot: committed -> visible, aborted -> invisible.
//  4. If deleted (XMax != 0):
//     a. Began after my snapshot (XMax >= Snapshot.XMax)     -> visible.
//     b. Was running when my snapshot was taken              -> visible.
//     c. Finished before my snapshot: committed -> invisible, aborted -> visible.
//
// Errors from IsCommitted are fail-closed on the creator (hide the row) and
// fail-open on the deleter (keep the row visible).
func (th *TableHeap) visibleTo(t *transaction.Transaction, h heap.TupleHeader) bool {
	// --- own writes ---
	if h.XMin == t.Id {
		return h.XMax != t.Id
	}
	if h.XMax == t.Id {
		return false
	}

	// --- creator check ---
	if h.XMin >= t.Snapshot.XMax {
		return false
	}
	if t.Snapshot.InProgressContains(h.XMin) {
		return false
	}
	xminCommitted, err := th.tm.IsCommitted(h.XMin)
	if err != nil || !xminCommitted {
		return false
	}

	// --- deleter check ---
	if h.XMax == 0 {
		return true
	}
	if h.XMax >= t.Snapshot.XMax {
		return true
	}
	if t.Snapshot.InProgressContains(h.XMax) {
		return true
	}
	xmaxCommitted, err := th.tm.IsCommitted(h.XMax)
	if err != nil {
		return true // fail-open on delete
	}
	return !xmaxCommitted
}

// -----------------------------------------------------------------------------
// Delete (mark as deleted)
// -----------------------------------------------------------------------------

// MarkTupleAsDeleted stamps xmax on the tuple at rId, marking it logically
// deleted by t. The physical bytes remain until vacuum reclaims them, so older
// snapshots can still see the version.
//
// The WAL record carries the full pre-delete tuple bytes as the before-image
// so undo can restore them.
func (th *TableHeap) MarkTupleAsDeleted(
	t *transaction.Transaction,
	rId common.RecordId,
) error {
	tag := buffer.BufferTag{RelationID: th.rel, ForkID: th.fork, BlockID: rId.BlockID}
	frame, err, _ := th.bp.GetPage(tag)
	if err != nil {
		return fmt.Errorf("get page %d: %w", rId.BlockID, err)
	}

	frame.WLatch()
	heapPage := &heap.HeapPage{Page: frame.Page}

	if !heapPage.DoesTupleInSlotExist(rId.Slot) {
		frame.WUnlatch()
		frame.Unpin()
		return ErrTupleNotFound
	}

	// Pre-check: fail before logging so a losing transaction's undo chain
	// stays empty. Distinguish "held by in-flight tx" (retryable) from
	// "genuinely gone" (permanent).
	if err := heapPage.CanDeleteTuple(rId.Slot); err != nil {
		header, herr := heapPage.GetTupleHeader(rId.Slot)
		if herr == nil && header.XMax != 0 && th.tm.IsInProgress(header.XMax) {
			frame.WUnlatch()
			frame.Unpin()
			return transaction.ErrSerializationFailure
		}
		frame.WUnlatch()
		frame.Unpin()
		return err
	}

	// Snapshot before-image for the WAL record.
	before := heapPage.GetTupleWithSlot(rId.Slot)
	beforeCopy := make([]byte, len(before))
	copy(beforeCopy, before)

	row := common.RowId{RelationID: th.rel, BlockID: rId.BlockID, Slot: rId.Slot}

	// 1. WAL record.
	lsn, err := t.Log(&wal.Record{
		Type:  wal.RecDelete,
		Flags: wal.FlagHasBefore,
		Data:  wal.EncodeDelete(row, beforeCopy),
	})
	if err != nil {
		frame.WUnlatch()
		frame.Unpin()
		return fmt.Errorf("wal delete: %w", err)
	}

	// 2. Apply in memory.
	if err := heapPage.DeleteTuple(rId.Slot, t.Id, t.NextCommandID()); err != nil {
		frame.WUnlatch()
		frame.Unpin()
		return fmt.Errorf("delete tuple %v: %w", rId, err)
	}

	// 3. Record the WAL position.
	frame.Page.SetLSN(lsn)

	frame.WUnlatch()
	th.bp.MarkDirtyWithLSN(frame, lsn)
	frame.Unpin()
	return nil
}

// -----------------------------------------------------------------------------
// Update
// -----------------------------------------------------------------------------

// UpdateTuple replaces the row at rId with newTuple.
//
// Every update takes the optimistic path: check the target tuple's xmax for
// a concurrent writer, abort with ErrSerializationFailure on conflict, else
// delete the old version and insert the new one.
//
// Pessimistic mode (blocking on row locks) will return after the xmax-as-lock
// refactor; until then SELECT FOR UPDATE / SHARE do not block writers.
func (th *TableHeap) UpdateTuple(
	t *transaction.Transaction,
	rId common.RecordId,
	newTuple *Tuple,
	schema *catalog.Schema,
) (common.RecordId, error) {
	if err := th.checkUpdateConflict(t, rId); err != nil {
		return common.RecordId{}, err
	}
	if err := th.MarkTupleAsDeleted(t, rId); err != nil {
		return common.RecordId{}, err
	}
	return th.InsertTuple(t, newTuple, schema)
}

// checkUpdateConflict returns ErrSerializationFailure if another in-flight
// transaction has claimed this row's xmax, or ErrTupleNotFound if the row is
// no longer visible to the caller.
func (th *TableHeap) checkUpdateConflict(
	t *transaction.Transaction,
	rId common.RecordId,
) error {
	tag := buffer.BufferTag{RelationID: th.rel, ForkID: th.fork, BlockID: rId.BlockID}
	frame, err, _ := th.bp.GetPage(tag)
	if err != nil {
		return fmt.Errorf("get page %d: %w", rId.BlockID, err)
	}
	frame.RLatch()
	defer func() { frame.RUnlatch(); frame.Unpin() }()

	heapPage := &heap.HeapPage{Page: frame.Page}
	if !heapPage.DoesTupleInSlotExist(rId.Slot) {
		return ErrTupleNotFound
	}
	header, err := heapPage.GetTupleHeader(rId.Slot)
	if err != nil {
		return err
	}
	if !th.visibleTo(t, header) {
		return ErrTupleNotFound
	}
	// An in-flight transaction has already claimed the row — conflict.
	if header.XMax != 0 && th.tm.IsInProgress(header.XMax) {
		return transaction.ErrSerializationFailure
	}
	return nil
}

// -----------------------------------------------------------------------------
// Explicit locks (SELECT FOR UPDATE / FOR SHARE)
// -----------------------------------------------------------------------------

// SelectForUpdate reads the current version of the row and takes an exclusive
// lock on it. With no lock manager wired in, LockRow is a no-op — the lock
// does not block other writers. Semantics are optimistic; this is expected
// to be replaced by xmax-based row locks once that refactor lands.
func (th *TableHeap) SelectForUpdate(
	t *transaction.Transaction,
	rId common.RecordId,
	schema *catalog.Schema,
) (*Tuple, error) {
	row := common.RowId{RelationID: th.rel, BlockID: rId.BlockID, Slot: rId.Slot}
	if err := t.LockRow(row, lock.LockExclusive); err != nil {
		return nil, fmt.Errorf("lock row for update: %w", err)
	}
	return th.currentVersion(rId, schema)
}

// SelectForShare is the shared-lock variant.
func (th *TableHeap) SelectForShare(
	t *transaction.Transaction,
	rId common.RecordId,
	schema *catalog.Schema,
) (*Tuple, error) {
	row := common.RowId{RelationID: th.rel, BlockID: rId.BlockID, Slot: rId.Slot}
	if err := t.LockRow(row, lock.LockShared); err != nil {
		return nil, fmt.Errorf("lock row for share: %w", err)
	}
	return th.currentVersion(rId, schema)
}

// -----------------------------------------------------------------------------
// Iterator
// -----------------------------------------------------------------------------

// Iterator returns a table iterator for the relation, filtered by t's
// snapshot. The iterator scans pages sequentially; visibility is checked per
// tuple.
func (th *TableHeap) Iterator(
	t *transaction.Transaction,
	schema *catalog.Schema,
) *TableIterator {
	return NewTableIterator(th, t, schema)
}
