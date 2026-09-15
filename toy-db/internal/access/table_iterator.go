package access

import (
	"github.com/surajjagdev/ToyDB/internal/catalog"
	"github.com/surajjagdev/ToyDB/internal/common"
	"github.com/surajjagdev/ToyDB/internal/storage/buffer"
	"github.com/surajjagdev/ToyDB/internal/storage/page/heap"
)

type TableIterator struct {
	table  *TableHeap
	schema *catalog.Schema
	rId    common.RecordId
	isDone bool

	tupleBuf []byte // reusable buffer
}

func NewTableIterator(table *TableHeap, schema *catalog.Schema) *TableIterator {
	return &TableIterator{
		table:  table,
		schema: schema,
		isDone: true,
	}
}

func (it *TableIterator) Init() {
	it.rId = common.RecordId{
		BlockID: common.BlockID(0),
		Slot:    common.SlotIndex(0),
	}
	it.isDone = false
}

func (it *TableIterator) Next() (_ *Tuple, _ error, isDone bool) {
	if it.isDone {
		return nil, nil, it.isDone // EOF
	}

	for {
		tag := buffer.BufferTag{
			RelationID: it.table.rel,
			ForkID:     it.table.fork,
			BlockID:    it.rId.BlockID,
		}

		// fetch block
		frame, err, _ := it.table.bp.GetPage(tag)

		if err != nil {
			it.isDone = true
			return nil, err, it.isDone
		}

		// read the page
		frame.RLatch()
		heapPage := &heap.HeapPage{Page: frame.Page}
		numSlots := heapPage.GetNumSlots()

		for int(it.rId.Slot) < numSlots {
			if heapPage.DoesTupleInSlotExist(it.rId.Slot) {
				rawBytes := heapPage.GetTupleWithSlot(it.rId.Slot)

				// only gets more mem when capacity runs out
				if cap(it.tupleBuf) < len(rawBytes) {
					it.tupleBuf = make([]byte, len(rawBytes))
				}

				it.tupleBuf = it.tupleBuf[:len(rawBytes)]
				copy(it.tupleBuf, rawBytes)

				it.rId.Slot++

				// release early
				frame.RUnlatch()
				frame.Unpin()

				tuple, err := DeserializeTuple(it.tupleBuf, it.schema)

				if err != nil {
					return nil, err, it.isDone
				}

				return tuple, nil, it.isDone
			}

			// slot empty or logically empty
			it.rId.Slot++
		}

		// finished with page
		frame.RUnlatch()
		frame.Unpin()

		// go to next block, with 0th slot
		it.rId.BlockID++
		it.rId.Slot = 0
	}
}
