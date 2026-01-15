package heap

import (
	"fmt"

	"github.com/surajjagdev/ToyDB/internal/common"
	"github.com/surajjagdev/ToyDB/internal/storage/page"
)

// Base page layout
// +--------------------+  Offset 0
// | Page Header        |
// +--------------------+  OffsetLower
// | ItemId[] (slots)   |  grows upward
// |                    |
// |   free space       |
// |                    |
// | Tuple data         |  grows downward
// +--------------------+  OffsetUpper
// | Special area       |
// +--------------------+

// Heap Page
// everything as Base
// With Tuples
// Tuple Header.
/*
Heap Page Layout
----------------

[ Page Header | ItemId[] | free space | Tuples | Special ]

ItemId:
-15 bit for tuple offset
- 2 bit for flags
- 15 bit for tuple size

Tuple Layout:
[ HeapTupleHeader | UserData ]
*/
const (
	ItemIdSize = 4
)

const (
	// item id flags
	// slot never used and can be reused
	ItemIdFlagUnused = 1 << 0
	// slot points to live tuple
	ItemIdFlagNormal = 1 << 1
	//Slot does not point to a tuple, points to another slot. hot update, index points to this slot, but this slots points
	// to another slot in same page
	ItemIdRedirect = 1 << 2

	//tuple is deleted, can be vaccummed
	ItemIdDeleted = 1 << 3
)

const (
	// Transaction / command stamps
	// txn of the transaction that created this tuple - visibility
	TupleHeaderOffsetXmin = 0 // uint32
	// txn of the transaction that deleted this tuple - visibility
	TupleHeaderOffsetXmax = 4 // uint32
	// command ID within a transaction
	TupleHeaderOffsetCid = 8 // uint32 (or xvac)

	// current tuple ID. can point to self, new version, or another tuple
	TupleHeaderOffsetCtid = 12 // ItemPointerData (6 bytes)

	// Visibility & metadata
	// tuple has NULLs, tuple was updated, xmax committed / aborted, tuple moved by vacuum
	TupleHeaderOffsetInfomask2 = 18 // uint16
	// number of attributes & HOT / key-update flags.
	// 0-10 bits for number of attributes, 11-15 bits for HOT / key-update flags
	TupleHeaderOffsetInfomask = 20 // uint16
	//Byte offset from tuple start to first attribute, fixed header, null bitmap, padding for alignment
	TupleHeaderOffsetHoff = 22 // uint8

	// Header sizes
	// Minimum header without w/o null bitmap
	HeapTupleHeaderMinSize = 23 // before alignment
	HeapTupleHeaderAlign   = 8  // MAXALIGN
)

type HeapPage struct {
	page.Page
}

// Assuming page already alloc'ed, set the
// page header and flags
func InitHeapPage(p page.Page) *HeapPage {
	page.InitBasePage(p, page.PageFlagHeap)

	// set lower, upper and special
	p.SetLower(page.PageHeaderSize)
	p.SetUpper(uint16(common.PageSize))
	p.SetSpecial(uint16(common.PageSize))

	return &HeapPage{
		p,
	}
}

// Get the avail free space
func (h *HeapPage) GetFreeSpace() int {
	return int(h.GetUpper() - h.GetLower())
}

// tuple crud

func (h *HeapPage) InsertTuple(data []byte, xmin common.TransactionID, commandId common.CommandID) error {
	// 1. calc tuple size, header min size + user data
	tupleSize := HeapTupleHeaderMinSize + len(data)
	tupleSize = page.AlignTo(tupleSize, HeapTupleHeaderAlign)

	upper := h.GetUpper()
	lower := h.GetLower()

	// 2. check available space in page
	if int(upper-lower) < ItemIdSize+tupleSize {
		return fmt.Errorf("Page is full")
	}

	// 3. There is free space, so update upper
	newUpper := upper - uint16(tupleSize) // tuple size can fit in uint16
	//tupleOffset := newUpper
	h.SetUpper(newUpper)

	return nil
}
