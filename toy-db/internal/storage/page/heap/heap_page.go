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
	// top 15 bits
	offset = uint16(v >> 17)

	// next 2 bits
	twobitMask := uint32((1 << 2) - 1)
	flags = uint8((v >> 15) & twobitMask)

	// bottom 15 bits
	fifteenBitMask := uint32(((1 << 15) - 1))
	size = uint16(v & fifteenBitMask)

	return offset, flags, size
}

// tuple crud

func (h *HeapPage) GetTupleWithSlot(slot int) []byte {
	slotOffset := page.PageHeaderSize + slot*ItemIdSize

	// read the 4 bytes for item composed of tuple offset, flag and tuple size
	v := common.ByteOrder.Uint32(h.Page[slotOffset : slotOffset+4])
	offset, _, size := h.UnpackItemId(v)
	return h.Page[offset : offset+size]
}

// Insert a single tuple into the page
func (h *HeapPage) InsertTuple(
	data []byte,
	pageId common.PageID,
	xmin common.TransactionID,
	cid common.CommandID,
) error {

	// 1. Compute header offset (aligned)
	hoff := page.AlignTo(HeapTupleHeaderMinSize, HeapTupleHeaderAlign)

	// 2. Compute total tuple size (aligned as a whole)
	tupleSize := hoff + len(data)
	tupleSize = page.AlignTo(tupleSize, HeapTupleHeaderAlign)

	upper := h.GetUpper()
	lower := h.GetLower()

	// 3. Check available space
	if int(upper-lower) < ItemIdSize+tupleSize {
		return fmt.Errorf("page is full")
	}

	// 4. Allocate space from upper (tuples grow downward)
	newUpper := upper - uint16(tupleSize)
	tupleOffset := newUpper
	h.SetUpper(newUpper)

	// 5. Slice tuple space
	tuple := h.Page[tupleOffset : tupleOffset+uint16(tupleSize)]

	// ---- tuple header ----

	// xmin
	common.ByteOrder.PutUint32(
		tuple[TupleHeaderOffsetXmin:TupleHeaderOffsetXmin+4],
		uint32(xmin),
	)

	// xmax (not deleted)
	common.ByteOrder.PutUint32(
		tuple[TupleHeaderOffsetXmax:TupleHeaderOffsetXmax+4],
		0,
	)

	// command id
	common.ByteOrder.PutUint32(
		tuple[TupleHeaderOffsetCid:TupleHeaderOffsetCid+4],
		uint32(cid),
	)

	// infomask / infomask2 (stubbed)
	common.ByteOrder.PutUint16(
		tuple[TupleHeaderOffsetInfomask:TupleHeaderOffsetInfomask+2],
		0,
	)
	common.ByteOrder.PutUint16(
		tuple[TupleHeaderOffsetInfomask2:TupleHeaderOffsetInfomask2+2],
		0,
	)

	// hoff (aligned header size)
	tuple[TupleHeaderOffsetHoff] = uint8(hoff)

	// ---- user data ----
	copy(tuple[int(hoff):], data)

	// zero memory for padding
	for i := int(hoff) + len(data); i < len(tuple); i++ {
		tuple[i] = 0
	}

	// ---- itemId slot ----

	slotOffset := lower
	h.SetLower(lower + ItemIdSize)

	slotIndex := (slotOffset - page.PageHeaderSize) / ItemIdSize

	itemId := uint32(0)
	itemId |= uint32(tupleOffset) << 17      // offset (15 bits)
	itemId |= uint32(ItemIdFlagNormal) << 15 // flags (2 bits)
	itemId |= uint32(tupleSize)              // size (15 bits)

	common.ByteOrder.PutUint32(
		h.Page[slotOffset:slotOffset+4],
		itemId,
	)

	// ---- self ctid ----

	common.ByteOrder.PutUint32(
		tuple[TupleHeaderOffsetCtid:],
		uint32(pageId),
	)
	common.ByteOrder.PutUint16(
		tuple[TupleHeaderOffsetCtid+4:],
		uint16(slotIndex),
	)

	return nil
}
