package access

import (
	"fmt"
	"sync"

	"github.com/surajjagdev/ToyDB/internal/catalog"
	"github.com/surajjagdev/ToyDB/internal/common"
	"github.com/surajjagdev/ToyDB/internal/storage/buffer"
	"github.com/surajjagdev/ToyDB/internal/storage/page/heap"
)

/*
*
CRUD for physical tuples
CREATE tuple -> insert tuple given tuple, schema, txn id, cmd id -> record id
-> serializes tuple, ask fsm for space
READ tuple ->
*
*/

type TableHeap struct {
	bp       *buffer.BufferPool
	fsm      *FSM
	rel      common.RelationID
	fork     common.ForkID
	extendMu sync.Mutex // lock for extending the table
}

func NewTableHeap(bp *buffer.BufferPool, fsm *FSM, rel common.RelationID) *TableHeap {
	return &TableHeap{
		bp:   bp,
		fsm:  fsm,
		rel:  rel,
		fork: common.ForkMain, // always main
	}
}

// Given the required space for inserting a tuple get the block id and frame
func (th *TableHeap) getPageForInsert(
	requiredSpace int,
) (common.BlockID, *buffer.Frame, error) {
	targetBlockID, err := th.fsm.GetBlockWithFreeSpace(th.rel, requiredSpace)

	if err != nil {
		return common.InvalidBlockID, nil, fmt.Errorf("failed to get block with free space: %w", err)
	}

	// Found an existing page for insert, see if its valid
	if targetBlockID != common.InvalidBlockID {
		tag := buffer.BufferTag{RelationID: th.rel, ForkID: th.fork, BlockID: targetBlockID}

		frame, err, _ := th.bp.GetPage(tag)

		if err != nil {
			return common.InvalidBlockID, nil, fmt.Errorf(
				"failed to fetch table page %d: %w",
				targetBlockID,
				err,
			)
		}

		return targetBlockID, frame, nil
	}

	// need to extend
	// lock the table heap
	th.extendMu.Lock()

	// double check if another thread has extended for us
	targetBlockID, err = th.fsm.GetBlockWithFreeSpace(th.rel, requiredSpace)

	if err != nil {
		th.extendMu.Unlock()

		return common.InvalidBlockID, nil, fmt.Errorf("failed to get block with free space with re lock: %w", err)
	}

	// another thread has extended for us
	if targetBlockID != common.InvalidBlockID {
		th.extendMu.Unlock()

		tag := buffer.BufferTag{
			RelationID: th.rel,
			ForkID:     th.fork,
			BlockID:    targetBlockID,
		}

		frame, err, _ := th.bp.GetPage(tag)

		if err != nil {
			return common.InvalidBlockID, nil, fmt.Errorf(
				"failed to fetch table page %d: %w",
				targetBlockID,
				err,
			)
		}

		return targetBlockID, frame, nil
	}

	// no other thread has extended for us, so we need to do this
	diskManager, err := th.bp.GetDiskManager()

	if err != nil {
		th.extendMu.Unlock()

		return common.InvalidBlockID, nil, fmt.Errorf("disk manager reference failed: %w", err)
	}

	targetBlockID, err = diskManager.AllocateBlock(th.rel, th.fork)

	if err != nil {
		th.extendMu.Unlock()

		return common.InvalidBlockID, nil, fmt.Errorf("failed to get block: %w", err)
	}

	tag := buffer.BufferTag{
		BlockID:    targetBlockID,
		ForkID:     th.fork,
		RelationID: th.rel,
	}

	frame, err := th.bp.AllocatePage(tag)

	if err != nil {
		th.extendMu.Unlock()

		return common.InvalidBlockID, nil, fmt.Errorf("failed to get allocate page in buffer pool: %w", err)
	}

	frame.WLatch()
	heap.InitHeapPage(frame.Page)
	frame.WUnlatch()

	th.extendMu.Unlock()
	return targetBlockID, frame, nil
}

// Serialize tuple and insert into first available heap page
func (th *TableHeap) InsertTuple(
	tuple *Tuple,
	schema *catalog.Schema,
	xmin common.TransactionID,
	cid common.CommandID,
) (common.RecordId, error) {
	// 1. serialize tuple
	data, nullBitmap := tuple.Serialize(schema)

	// 2. calculate bytes needed to insert tuple
	_, _, requiredSpace := heap.CalculateBytesNeededToInsertTuple(len(data), len(nullBitmap))

	// 4. Find a free heap page to insert tuple into
	for {
		targetBlockID, frame, err := th.getPageForInsert(requiredSpace)

		if err != nil {
			return common.RecordId{}, fmt.Errorf("failed to get block with free space: %w", err)
		}

		// latch page for writing to. frame space can be stale so double check
		frame.WLatch()
		heapPage := &heap.HeapPage{Page: frame.Page}
		freeSpace := heapPage.GetFreeSpace()

		// check if page still has space to write to it, in case another thread uses this space
		if freeSpace < requiredSpace {
			frame.WUnlatch()
			frame.Unpin()

			// update fsm
			_ = th.fsm.RecordFreeSpace(th.rel, targetBlockID, heapPage.GetFreeSpace())
			continue
		}

		// insert the tuple in memory
		rId, err := heapPage.InsertTuple(
			data,
			nullBitmap,
			targetBlockID,
			xmin,
			cid,
		)

		if err != nil {
			frame.WUnlatch()
			frame.Unpin()

			return common.RecordId{}, fmt.Errorf("InsertTuple failed unexpectedly: %w", err)
		}

		// calc how much free space is left on this page now
		remainingSpace := heapPage.GetFreeSpace()

		frame.WUnlatch()

		// set dirty page and unpin it
		frame.SetDirty()
		frame.Unpin()

		// update fsm for free space
		// no failure recorded
		_ = th.fsm.RecordFreeSpace(th.rel, targetBlockID, remainingSpace)

		return rId, nil
	}
}

// Get Tuple using bp and deserialize using schema
func (th *TableHeap) GetTuple(rId common.RecordId, schema *catalog.Schema) (*Tuple, error) {
	tag := buffer.BufferTag{
		RelationID: th.rel,
		ForkID:     th.fork,
		BlockID:    rId.BlockID,
	}

	frame, err, _ := th.bp.GetPage(tag)

	if err != nil {
		return nil, fmt.Errorf("Received error retrieving page %v", err)
	}

	frame.RLatch()

	heapPage := &heap.HeapPage{Page: frame.Page}

	// verify slot exists
	if !heapPage.DoesTupleInSlotExist(rId.Slot) {
		frame.RUnlatch()
		frame.Unpin()

		return nil, fmt.Errorf("Record does not exist %v", err)
	}

	// get raw bytes
	rawBytes := heapPage.GetTupleWithSlot(rId.Slot)

	// create new tuple to copy into
	tupleRawBytes := make([]byte, len(rawBytes))
	copy(tupleRawBytes, rawBytes)

	frame.RUnlatch()
	frame.Unpin()

	tuple, err := DeserializeTuple(tupleRawBytes, schema)

	if err != nil {
		return nil, fmt.Errorf("Failed to deserialize tuple %v", err)
	}

	return tuple, nil
}

// MarkTupleAsDeleted
func (th *TableHeap) MarkTupleAsDeleted(
	rId common.RecordId,
	xmax common.TransactionID,
	cid common.CommandID,
) error {
	tag := buffer.BufferTag{
		RelationID: th.rel,
		ForkID:     th.fork,
		BlockID:    rId.BlockID,
	}

	frame, err, _ := th.bp.GetPage(tag)

	if err != nil {
		return fmt.Errorf("Received error retrieving page %v", err)
	}

	frame.WLatch()

	heapPage := &heap.HeapPage{Page: frame.Page}

	err = heapPage.DeleteTuple(
		rId.Slot,
		xmax,
		cid,
	)

	frame.WUnlatch()

	if err != nil {
		frame.Unpin()

		return fmt.Errorf("failed to delete tuple %v: %w", rId, err)
	}

	frame.SetDirty()
	frame.Unpin()

	return nil
}

func (th *TableHeap) Iterator(schema *catalog.Schema) *TableIterator {
	return NewTableIterator(th, schema)
}
