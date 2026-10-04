package access

import (
	"github.com/surajjagdev/ToyDB/internal/catalog"
	"github.com/surajjagdev/ToyDB/internal/common"
	"github.com/surajjagdev/ToyDB/internal/storage/buffer"
	"github.com/surajjagdev/ToyDB/internal/storage/page/heap"
	"github.com/surajjagdev/ToyDB/internal/transaction"
)

// TableIterator walks the tuples of a relation in physical order (block, then
// slot), returning only those visible to the transaction's snapshot. It holds
// at most one page frame pinned at a time.
type TableIterator struct {
	table  *TableHeap
	tx     *transaction.Transaction
	schema *catalog.Schema

	// Position of the next candidate tuple. Bumped slot-first, then block.
	rId common.RecordId

	isDone bool

	// tupleBuf is reused across Next calls to avoid an allocation per tuple.
	// It is grown when a larger tuple is seen and never shrunk.
	tupleBuf []byte
}

func NewTableIterator(
	table *TableHeap,
	t *transaction.Transaction,
	schema *catalog.Schema,
) *TableIterator {
	return &TableIterator{
		table:  table,
		tx:     t,
		schema: schema,
		isDone: true,
	}
}

// Init resets the iterator to the start of the relation. Must be called before
// the first Next.
func (it *TableIterator) Init() {
	it.rId = common.RecordId{
		BlockID: common.BlockID(0),
		Slot:    common.SlotIndex(0),
	}
	it.isDone = false
}

// Next returns the next visible tuple. The returned bool is true when the
// iterator has reached the end of the relation; at that point tuple and err
// are nil.
//
// Errors are returned only for infrastructure failures (I/O, deserialization).
// Missing or invisible tuples are skipped silently.
func (it *TableIterator) Next() (_ *Tuple, _ error, isDone bool) {
	if it.isDone {
		return nil, nil, true
	}

	for {
		tag := buffer.BufferTag{
			RelationID: it.table.rel,
			ForkID:     it.table.fork,
			BlockID:    it.rId.BlockID,
		}

		frame, err, _ := it.table.bp.GetPage(tag)
		if err != nil {
			// Reaching the end of the relation is signalled by the disk
			// manager as a missing page. Treat it as EOF.
			it.isDone = true
			return nil, nil, true
		}

		frame.RLatch()
		heapPage := &heap.HeapPage{Page: frame.Page}
		numSlots := heapPage.GetNumSlots()

		for int(it.rId.Slot) < numSlots {
			if !heapPage.DoesTupleInSlotExist(it.rId.Slot) {
				it.rId.Slot++
				continue
			}

			header, err := heapPage.GetTupleHeader(it.rId.Slot)
			if err != nil {
				// Corrupt slot or too-small tuple; skip it rather than
				// aborting the scan.
				it.rId.Slot++
				continue
			}

			if !it.table.visibleTo(it.tx, header) {
				it.rId.Slot++
				continue
			}

			rawBytes := heapPage.GetTupleWithSlot(it.rId.Slot)

			// Grow the reusable buffer if needed, then copy the raw bytes
			// out because we're about to release the page latch.
			if cap(it.tupleBuf) < len(rawBytes) {
				it.tupleBuf = make([]byte, len(rawBytes))
			}
			it.tupleBuf = it.tupleBuf[:len(rawBytes)]
			copy(it.tupleBuf, rawBytes)

			it.rId.Slot++

			frame.RUnlatch()
			frame.Unpin()

			tuple, err := DeserializeTuple(it.tupleBuf, it.schema)
			if err != nil {
				return nil, err, false
			}
			return tuple, nil, false
		}

		// No more slots on this page; move to the next block.
		frame.RUnlatch()
		frame.Unpin()

		it.rId.BlockID++
		it.rId.Slot = 0
	}
}

// Close releases the iterator. Safe to call multiple times. The iterator
// holds no long-lived frames, so this is currently a no-op, but it's part of
// the interface so callers can defer it uniformly.
func (it *TableIterator) Close() {
	it.isDone = true
	it.tupleBuf = nil
}
