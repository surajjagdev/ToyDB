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
// MVCC-aware CRUD on tuples: every read filters by the transaction's snapshot,
// every write goes through the WAL before touching memory, and every write
// path is mode-aware (optimistic vs. pessimistic).
type TableHeap struct {
	bp       *buffer.BufferPool
	fsm      *FSM
	rel      common.RelationID
	fork     common.ForkID
	tm       *transaction.TransactionManager // for IsInProgress
	extendMu sync.Mutex                      // serializes extending the relation
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
// the relation if no existing page has room. Returns the frame pinned and
// write-latched on the caller's behalf.
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

		// 2. WAL first — write-ahead rule.
		payload := make([]byte, 0, len(data)+len(nullBitmap))
		payload = append(payload, data...)
		payload = append(payload, nullBitmap...)

		lsn, err := t.Log(&wal.Record{
			Type:  wal.RecInsert,
			Flags: wal.FlagHasAfter,
			Data:  wal.EncodeInsert(row, payload),
		})
		if err != nil {
			frame.WUnlatch()
			frame.Unpin()
			return common.RecordId{}, fmt.Errorf("wal insert: %w", err)
		}

		// 3. Apply in memory.
		rId, err := heapPage.InsertTuple(data, nullBitmap, targetBlockID, t.Id, t.NextCommandID())
		if err != nil {
			frame.WUnlatch()
			frame.Unpin()
			return common.RecordId{}, fmt.Errorf("insert tuple: %w", err)
		}

		// 4. Record the WAL position on the page so the buffer pool can
		// enforce the write-ahead rule when flushing.
		frame.Page.SetLSN(lsn)

		remainingSpace := heapPage.GetFreeSpace()
		frame.WUnlatch()
		frame.SetDirty()
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
// snapshot. Used by SELECT FOR UPDATE / SHARE and by the pessimistic write
// path.
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
	// Committed-dead → row gone from the current state.
	if header.XMax != 0 && !th.tm.IsInProgress(header.XMax) {
		return nil, ErrTupleNotFound
	}

	raw := heapPage.GetTupleWithSlot(rId.Slot)
	buf := make([]byte, len(raw))
	copy(buf, raw)

	return DeserializeTuple(buf, schema)
}

// visibleTo applies the MVCC visibility rule, including the "my own writes
// are visible to me" cases.
func (th *TableHeap) visibleTo(t *transaction.Transaction, h heap.TupleHeader) bool {
	if h.XMin == t.Id {
		if h.XMax == t.Id {
			return false // I inserted then deleted it
		}
		return true // I created this version
	}
	if h.XMax == t.Id {
		return false // I deleted it
	}
	return t.Snapshot.IsVisible(h.XMin, h.XMax)
}

// -----------------------------------------------------------------------------
// Delete (mark as deleted)
// -----------------------------------------------------------------------------

// MarkTupleAsDeleted stamps xmax on the tuple at rId, marking it logically
// deleted by t. The physical bytes remain until vacuum reclaims them, so older
// snapshots can still see the version.
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

	if err := heapPage.CanDeleteTuple(rId.Slot); err != nil {
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
	frame.SetDirty()
	frame.Unpin()
	return nil
}

// -----------------------------------------------------------------------------
// Update
// -----------------------------------------------------------------------------

// UpdateTuple replaces the row at rId with newTuple.
//
// Optimistic mode: checks the target tuple's xmax for a concurrent writer.
// Returns ErrSerializationFailure on conflict; caller retries.
//
// Pessimistic mode: takes an exclusive row lock first, held to commit. Blocks
// if another transaction holds it.
func (th *TableHeap) UpdateTuple(
	t *transaction.Transaction,
	rId common.RecordId,
	newTuple *Tuple,
	schema *catalog.Schema,
) (common.RecordId, error) {
	row := common.RowId{RelationID: th.rel, BlockID: rId.BlockID, Slot: rId.Slot}

	switch t.Mode {
	case transaction.Optimistic:
		if err := th.checkUpdateConflict(t, rId); err != nil {
			return common.RecordId{}, err
		}
	case transaction.Pessimistic:
		if err := t.LockRow(row, lock.LockExclusive); err != nil {
			return common.RecordId{}, fmt.Errorf("lock row: %w", err)
		}
		// Under the lock, verify the row still exists in the current state.
		if err := th.checkCurrentVersionExists(rId); err != nil {
			return common.RecordId{}, err
		}
	default:
		return common.RecordId{}, fmt.Errorf("unknown tx mode %v", t.Mode)
	}

	// Delete old version, insert new one. Both write their own WAL records.
	if err := th.MarkTupleAsDeleted(t, rId); err != nil {
		return common.RecordId{}, err
	}
	return th.InsertTuple(t, newTuple, schema)
}

// checkUpdateConflict returns ErrSerializationFailure if another in-flight
// transaction has claimed this row's xmax.
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
	if header.XMax != 0 && th.tm.IsInProgress(header.XMax) {
		return transaction.ErrSerializationFailure
	}
	return nil
}

// checkCurrentVersionExists (pessimistic path) verifies after locking that the
// row hasn't been committed-deleted since the snapshot was taken.
func (th *TableHeap) checkCurrentVersionExists(rId common.RecordId) error {
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
	if header.XMax != 0 && !th.tm.IsInProgress(header.XMax) {
		return ErrTupleNotFound
	}
	return nil
}

// -----------------------------------------------------------------------------
// Explicit locks (SELECT FOR UPDATE / FOR SHARE)
// -----------------------------------------------------------------------------

// SelectForUpdate reads the current version of the row and takes an exclusive
// lock on it, held until commit. Backs SQL SELECT ... FOR UPDATE.
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

// SelectForShare is the shared-lock variant, held until commit.
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
