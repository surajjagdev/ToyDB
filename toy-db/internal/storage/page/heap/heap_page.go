package heap

import (
	"encoding/binary"
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
// before the tuples
const (
	ItemIdSize = 4 // 4 bytes per slot, containing tuple offset, flag and tuple size
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
	TupleHeaderOffsetHoff = 22 // uint8 -> null bitmap

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

func (h *HeapPage) UnpackItemId(v uint32) (offset uint16, flags uint8, size uint16) {
	// offset: top 15 bits
	offset = uint16(v >> 17)

	// flags: next 2 bits
	twoBitMask := ^uint32(0) >> 30 // for last 2 bits
	flags = uint8((v >> 15) & twoBitMask)

	// size: remaining 15 bits (low bits)
	fifteenBitMask := ^uint32(0) >> 17
	size = uint16(v & fifteenBitMask)
}

// tuple crud

func (h *HeapPage) GetTupleWithSlot(slot int) []byte {
	slotOffset := page.PageHeaderSize + slot*ItemIdSize

	// read the 4 bytes for item composed of tuple offset, flag and tuple size
	v := common.ByteOrder.Uint32(h.Page[slotOffset : slotOffset+4])
	offset, _, size := h.UnpackItemId(v)
	return h.Page[offset : offset+size]
}

func (h *HeapPage) InsertTuple(data []byte, pageId common.PageID, xmin common.TransactionID, cid common.CommandID) error {
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
	// start of where we can insert new tuple
	tupleOffset := newUpper
	h.SetUpper(newUpper)

	// 4. Write tuple from new offset
	tuple := h.Page[tupleOffset : tupleOffset+uint16(tupleOffset)]

	// add xmin
	common.ByteOrder.PutUint32(
		tuple[TupleHeaderOffsetXmin:TupleHeaderOffsetXmin+4],
		uint32(xmin),
	)

	// add xmax (not deleted)
	common.ByteOrder.PutUint32(
		tuple[TupleHeaderOffsetXmax:TupleHeaderOffsetXmax+4],
		uint32(0),
	)

	// add in command id
	common.ByteOrder.PutUint32(
		tuple[TupleHeaderOffsetCid:TupleHeaderOffsetCid+4],
		uint32(cid),
	)

	// add info masks
	// infomask / infomask2 stubbed for now
	common.ByteOrder.PutUint16(
		tuple[TupleHeaderOffsetInfomask:TupleHeaderOffsetInfomask+2],
		uint16(0),
	)
	common.ByteOrder.PutUint16(
		tuple[TupleHeaderOffsetInfomask2:TupleHeaderOffsetInfomask2+2],
		uint16(0),
	)

	// hoff (no null bitmap)
	tuple[TupleHeaderOffsetHoff] = HeapTupleHeaderMinSize

	copy(
		tuple[TupleHeaderOffsetHoff:],
		data,
	)

	// 5.  fill the slot items
	slotOffset := lower
	h.SetLower(lower + ItemIdSize)

	slotIndex := (slotOffset - page.PageHeaderSize) / ItemIdSize

	itemId := uint32(0)
	itemId |= uint32(tupleOffset) << 17      // set 15 bits for tupleoffset
	itemId |= uint32(ItemIdFlagNormal) << 15 // set next 2 bits for tuple flag
	itemId |= uint32(tupleSize)              // set remaining 15 bits for tuple size

	common.ByteOrder.PutUint32(
		h.Page[slotOffset:slotOffset+4],
		itemId,
	)

	// ---- self ctid ----

	// write first 4 bytes as pageid
	// points to self. if new version, can be a different page
	binary.LittleEndian.PutUint32(
		tuple[TupleHeaderOffsetCtid:],
		uint32(pageId),
	)
	// write new 2 bytes as slotindex
	binary.LittleEndian.PutUint16(
		tuple[TupleHeaderOffsetCtid+4:],
		uint16(slotIndex),
	)

	return nil
}
