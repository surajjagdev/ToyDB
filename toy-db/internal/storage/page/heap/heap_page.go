package heap

import (
	"bytes"
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
// +--------------------+  OffsetUpper

// Heap Page
// everything as Base
// With Tuples
// Tuple Header.
/*
Heap Page Layout
----------------

[ Page Header | ItemId[] | free space | Tuples | Special ]

ItemId (4 bytes, 8 KB pages):
- 13 bits for tuple size  (max 8191)
-  6 bits for flags        (values 0-63)
- 13 bits for tuple offset (max 8191)

Tuple Layout:
[ HeapTupleHeader | UserData ]
*/
const (
	ItemIdSize = 4 // 4 bytes per slot: size, flags, offset
)

const (
	// ItemIdFlagUnused: the slot was allocated but currently holds no
	// tuple. Reusable by the next InsertTuple that needs a slot.
	ItemIdFlagUnused = 1 << 0 // 0b0001

	// ItemIdFlagNormal: the slot points at a live tuple.
	ItemIdFlagNormal = 1 << 1 // 0b0010

	// ItemIdFlagRedirect: the slot points at another slot in the same page.
	// Reserved for HOT updates.
	ItemIdFlagRedirect = 1 << 2 // 0b0100

	// ItemIdFlagDeleted: the tuple is logically deleted. Vacuum will
	// convert this to ItemIdFlagUnused after reclaiming the tuple bytes.
	ItemIdFlagDeleted = 1 << 3 // 0b1000
)

const (
	// Transaction / command stamps.
	// txn of the transaction that created this tuple - visibility
	TupleHeaderOffsetXmin = 0 // uint32
	// txn of the transaction that deleted this tuple - visibility
	TupleHeaderOffsetXmax = 4 // uint32
	// command ID within a transaction
	TupleHeaderOffsetCid = 8 // uint32 (or xvac)

	// current tuple ID. can point to self, new version, or another tuple
	TupleHeaderOffsetCtid = 12 // ItemPointerData (6 bytes)

	// Visibility & metadata.
	// tuple has NULLs, tuple was updated, xmax committed / aborted, tuple moved by vacuum
	TupleHeaderOffsetInfomask2 = 18 // uint16
	// number of attributes & HOT / key-update flags.
	// 0-10 bits for number of attributes, 11-15 bits for HOT / key-update flags
	TupleHeaderOffsetInfomask = 20 // uint16
	// Byte offset from tuple start to first attribute, fixed header, null bitmap, padding for alignment
	TupleHeaderOffsetHoff = 22 // uint8 -> null bitmap

	// Header sizes
	// Minimum header without w/o null bitmap
	HeapTupleHeaderMinSize = 23 // before alignment
	HeapTupleHeaderAlign   = 8  // MAXALIGN
)

type HeapPage struct {
	page.Page
}

type TupleHeader struct {
	XMin common.TransactionID
	XMax common.TransactionID // 0 = live
	Cid  common.CommandID
}

// InitHeapPage initializes an already-allocated page as a heap page.
func InitHeapPage(p page.Page) *HeapPage {
	page.InitBasePage(p, page.PageFlagHeap)

	// set lower, upper and special
	p.SetLower(page.PageHeaderSize)
	p.SetUpper(uint16(common.PageSize))
	p.SetSpecial(uint16(common.PageSize))

	return &HeapPage{p}
}

// GetFreeSpace returns the number of bytes available for new tuples and
// slot entries.
func (h *HeapPage) GetFreeSpace() int {
	return int(h.GetUpper() - h.GetLower())
}

// NextSlotIdx returns the slot index that the next InstallTuple would
// allocate. It is the same computation as NextSlot but without the
// room check — callers that need the check should use NextSlot instead.
func (h *HeapPage) NextSlotIdx() common.SlotIndex {
	return common.SlotIndex((h.GetLower() - page.PageHeaderSize) / ItemIdSize)
}

// -----------------------------------------------------------------------------
// ItemId encoding
// -----------------------------------------------------------------------------

// UnpackItemId decodes the 4-byte ItemId slot entry into (offset, flags, size).
//
// Layout (8 KB pages):
//
//	bits 0–12    size    (13 bits, max 8191)
//	bits 13–18   flags   (6 bits, values 0–63)
//	bits 19–31   offset  (13 bits, max 8191)
//
// The offset field is exactly wide enough for any byte position in an 8 KB
// page. The size field is 13 bits; the largest tuple that fits is 8191
// bytes, well above any practical tuple given the fixed header overhead.
func (h *HeapPage) UnpackItemId(v uint32) (offset uint16, flags uint8, size uint16) {
	offset = uint16(v >> 19)
	flags = uint8((v >> 13) & 0b111111) // 6-bit mask
	size = uint16(v & 0b1111111111111)  // 13-bit mask
	return
}

// packItemId encodes (offset, flags, size) into a 4-byte ItemId. Inverse of
// UnpackItemId. The caller must ensure offset and size fit in 13 bits
// (max 8191) — values above are silently masked.
func packItemId(offset uint16, flags uint8, size uint16) uint32 {
	var v uint32
	v |= uint32(offset) << 19
	v |= (uint32(flags) & 0b111111) << 13
	v |= uint32(size) & 0b1111111111111
	return v
}

// -----------------------------------------------------------------------------
// Slot array helpers
// -----------------------------------------------------------------------------

// getSlotOffset returns the byte offset of the ItemId slot entry for the
// given logical slot index. Does not validate.
func getSlotOffset(slot common.SlotIndex) int {
	return page.PageHeaderSize + int(slot)*ItemIdSize
}

// validateSlot reports whether the given slot index is within the range of
// slots that have been allocated on this page.
func (h *HeapPage) validateSlot(slot common.SlotIndex) bool {
	slotOffset := getSlotOffset(slot)
	if slotOffset+ItemIdSize > int(h.GetLower()) {
		return false
	}
	return true
}

// GetNumSlots returns the number of slot entries currently allocated.
func (h *HeapPage) GetNumSlots() int {
	return int((h.GetLower() - page.PageHeaderSize) / ItemIdSize)
}

// -----------------------------------------------------------------------------
// Tuple header access
// -----------------------------------------------------------------------------

// GetTupleHeader reads the MVCC header for the tuple in the given slot.
//
// Callers must hold at least a read latch on the page. The returned header
// is a value copy; it does not alias the page buffer.
func (h *HeapPage) GetTupleHeader(slot common.SlotIndex) (TupleHeader, error) {
	if !h.DoesTupleInSlotExist(slot) {
		return TupleHeader{}, fmt.Errorf("no tuple in slot %d", slot)
	}

	raw := h.GetTupleWithSlot(slot)
	if len(raw) < HeapTupleHeaderMinSize {
		return TupleHeader{}, fmt.Errorf(
			"tuple in slot %d too small for header: %d < %d",
			slot, len(raw), HeapTupleHeaderMinSize,
		)
	}

	return TupleHeader{
		XMin: common.TransactionID(
			common.ByteOrder.Uint32(raw[TupleHeaderOffsetXmin : TupleHeaderOffsetXmin+4]),
		),
		XMax: common.TransactionID(
			common.ByteOrder.Uint32(raw[TupleHeaderOffsetXmax : TupleHeaderOffsetXmax+4]),
		),
		Cid: common.CommandID(
			common.ByteOrder.Uint32(raw[TupleHeaderOffsetCid : TupleHeaderOffsetCid+4]),
		),
	}, nil
}

// -----------------------------------------------------------------------------
// Tuple reads
// -----------------------------------------------------------------------------

// DoesTupleInSlotExist reports whether the slot points at a live tuple.
func (h *HeapPage) DoesTupleInSlotExist(slot common.SlotIndex) bool {
	if !h.validateSlot(slot) {
		return false
	}
	slotOffset := getSlotOffset(slot)
	v := common.ByteOrder.Uint32(h.Page[slotOffset : slotOffset+ItemIdSize])
	_, flags, _ := h.UnpackItemId(v)
	return flags == ItemIdFlagNormal
}

// GetTupleWithSlot returns the tuple bytes for the given slot.
// Does not validate the slot — callers should check
// DoesTupleInSlotExist first.
func (h *HeapPage) GetTupleWithSlot(slot common.SlotIndex) []byte {
	slotOffset := getSlotOffset(slot)
	v := common.ByteOrder.Uint32(h.Page[slotOffset : slotOffset+ItemIdSize])
	offset, _, size := h.UnpackItemId(v)
	return h.Page[offset : offset+size]
}

// -----------------------------------------------------------------------------
// Tuple construction
// -----------------------------------------------------------------------------

// CalculateBytesNeededToInsertTuple computes (hoff, tupleSize, requiredSpace)
// for a tuple with the given user-data and null-bitmap lengths.
func CalculateBytesNeededToInsertTuple(dataLen int, nullBitmapLen int) (hoff int, tupleSize int, requiredSpace int) {
	hoff = page.AlignTo(HeapTupleHeaderMinSize+nullBitmapLen, HeapTupleHeaderAlign)
	tupleSize = page.AlignTo(hoff+dataLen, HeapTupleHeaderAlign)
	return hoff, tupleSize, tupleSize + ItemIdSize
}

// BuildTupleBytes produces the exact bytes that will occupy a slot when this
// tuple is installed, without modifying the page.
//
// The returned bytes include the full HeapTupleHeader (xmin, xmax, cid,
// ctid, infomask, infomask2, hoff), the null bitmap, user data, and any
// trailing alignment padding — everything the slot will hold.
//
// slot and blockID fill the tuple's ctid. The caller should have obtained
// slot from NextSlot() and hold the page's write latch across Build +
// Install so the two agree.
func (h *HeapPage) BuildTupleBytes(
	data []byte,
	nullBitmap []byte,
	blockID common.BlockID,
	slot common.SlotIndex,
	xmin common.TransactionID,
	xmax common.TransactionID,
	cid common.CommandID,
) []byte {
	hoff, tupleSize, _ := CalculateBytesNeededToInsertTuple(len(data), len(nullBitmap))

	buf := make([]byte, tupleSize)

	common.ByteOrder.PutUint32(buf[TupleHeaderOffsetXmin:], uint32(xmin))
	common.ByteOrder.PutUint32(buf[TupleHeaderOffsetXmax:], uint32(xmax))
	common.ByteOrder.PutUint32(buf[TupleHeaderOffsetCid:], uint32(cid))

	// ctid: self-location.
	common.ByteOrder.PutUint32(buf[TupleHeaderOffsetCtid:], uint32(blockID))
	common.ByteOrder.PutUint16(buf[TupleHeaderOffsetCtid+4:], uint16(slot))

	// infomask: HEAP_HASNULL if a null bitmap is present.
	infomask := uint16(0)
	if len(nullBitmap) > 0 {
		infomask |= 0x0001
		copy(buf[HeapTupleHeaderMinSize:], nullBitmap)
	}
	common.ByteOrder.PutUint16(buf[TupleHeaderOffsetInfomask:], infomask)
	common.ByteOrder.PutUint16(buf[TupleHeaderOffsetInfomask2:], 0)

	buf[TupleHeaderOffsetHoff] = uint8(hoff)
	copy(buf[int(hoff):], data)

	return buf
}

// -----------------------------------------------------------------------------
// Install / insert
// -----------------------------------------------------------------------------

// InstallTuple writes a pre-built tuple blob into the given slot, updating
// the page's slot array and upper pointer.
//
// The slot must be the next free slot (i.e., equal to NextSlotIdx() before
// this call). The caller must hold the write latch and must have verified
// there is room for len(tupleBytes) + ItemIdSize bytes.
func (h *HeapPage) InstallTuple(
	slot common.SlotIndex,
	tupleBytes []byte,
) (common.RecordId, error) {
	lower := h.GetLower()
	upper := h.GetUpper()

	expectedSlot := common.SlotIndex((lower - page.PageHeaderSize) / ItemIdSize)
	if slot != expectedSlot {
		return common.RecordId{}, fmt.Errorf(
			"InstallTuple: slot %d does not match next free slot %d",
			slot, expectedSlot)
	}

	tupleSize := len(tupleBytes)
	if int(upper-lower) < tupleSize+ItemIdSize {
		return common.RecordId{}, fmt.Errorf(
			"InstallTuple: page full (need %d, have %d)",
			tupleSize+ItemIdSize, upper-lower)
	}

	newUpper := upper - uint16(tupleSize)
	tupleOffset := newUpper
	h.SetUpper(newUpper)

	copy(h.Page[tupleOffset:tupleOffset+uint16(tupleSize)], tupleBytes)

	slotOffset := lower
	h.SetLower(lower + ItemIdSize)

	itemID := packItemId(tupleOffset, uint8(ItemIdFlagNormal), uint16(tupleSize))
	common.ByteOrder.PutUint32(h.Page[slotOffset:slotOffset+ItemIdSize], itemID)

	blockID := common.BlockID(
		common.ByteOrder.Uint32(tupleBytes[TupleHeaderOffsetCtid:]))
	return common.RecordId{BlockID: blockID, Slot: slot}, nil
}

// InsertTuple is a convenience wrapper around BuildTupleBytes + InstallTuple
// for callers that don't need to WAL-log the bytes separately (tests,
// replay).
//
// Production callers (TableHeap.InsertTuple) should use Build + WAL append
// + Install so the write-ahead rule is enforced.
func (h *HeapPage) InsertTuple(
	data []byte,
	nullBitmap []byte,
	pageId common.BlockID,
	xmin common.TransactionID,
	cid common.CommandID,
) (common.RecordId, error) {
	slot, err := h.NextSlot()
	if err != nil {
		return common.RecordId{}, err
	}
	blob := h.BuildTupleBytes(data, nullBitmap, pageId, slot, xmin, 0, cid)
	return h.InstallTuple(slot, blob)
}

// NextSlot returns the slot index the next InstallTuple would claim, after
// verifying there is room for one more slot entry.
func (h *HeapPage) NextSlot() (common.SlotIndex, error) {
	lower := h.GetLower()
	slotIndex := (lower - page.PageHeaderSize) / ItemIdSize

	if int(lower)+ItemIdSize > int(h.GetUpper()) {
		return 0, fmt.Errorf("page has no room for a new slot")
	}

	return common.SlotIndex(slotIndex), nil
}

// -----------------------------------------------------------------------------
// Delete
// -----------------------------------------------------------------------------

// CanDeleteTuple reports whether DeleteTuple would succeed for the given
// slot, without mutating anything. Callers can use it to fail before writing
// a WAL record, keeping losing transactions' undo chains empty.
func (h *HeapPage) CanDeleteTuple(slot common.SlotIndex) error {
	if !h.validateSlot(slot) {
		return fmt.Errorf("invalid slot %d", slot)
	}
	slotOffset := getSlotOffset(slot)
	v := common.ByteOrder.Uint32(h.Page[slotOffset : slotOffset+ItemIdSize])
	tupleOffset, flags, size := h.UnpackItemId(v)

	switch flags {
	case uint8(ItemIdFlagUnused):
		return fmt.Errorf("tuple already removed")
	case uint8(ItemIdFlagDeleted):
		return fmt.Errorf("tuple already deleted")
	case uint8(ItemIdFlagRedirect):
		return fmt.Errorf("cannot delete redirect slot")
	case uint8(ItemIdFlagNormal):
		// ok
	default:
		return fmt.Errorf("unknown itemId flag %d", flags)
	}

	tuple := h.Page[tupleOffset : tupleOffset+size]
	oldXmax := common.ByteOrder.Uint32(
		tuple[TupleHeaderOffsetXmax : TupleHeaderOffsetXmax+4],
	)
	if oldXmax != 0 {
		return fmt.Errorf("tuple already deleted by txn id %v", oldXmax)
	}
	return nil
}

// DeleteTuple logically deletes the tuple at slot by stamping xmax.
func (h *HeapPage) DeleteTuple(
	slot common.SlotIndex,
	xmax common.TransactionID,
	cid common.CommandID,
) error {
	slotOffset := getSlotOffset(slot)

	if exists := h.validateSlot(slot); !exists {
		return fmt.Errorf("invalid slot %d", slot)
	}

	v := common.ByteOrder.Uint32(h.Page[slotOffset : slotOffset+ItemIdSize])
	tupleOffset, flags, size := h.UnpackItemId(v)

	switch flags {
	case uint8(ItemIdFlagUnused):
		return fmt.Errorf("tuple already removed")
	case uint8(ItemIdFlagDeleted):
		return fmt.Errorf("tuple already deleted")
	case uint8(ItemIdFlagRedirect):
		return fmt.Errorf("cannot delete redirect slot")
	case uint8(ItemIdFlagNormal):
		// ok
	default:
		return fmt.Errorf("unknown itemId flag %d", flags)
	}

	tuple := h.Page[tupleOffset : tupleOffset+size]

	oldXmax := common.ByteOrder.Uint32(
		tuple[TupleHeaderOffsetXmax : TupleHeaderOffsetXmax+4],
	)
	if oldXmax != 0 {
		return fmt.Errorf("tuple already deleted by txn id %v", oldXmax)
	}

	common.ByteOrder.PutUint32(
		tuple[TupleHeaderOffsetXmax:TupleHeaderOffsetXmax+4],
		uint32(xmax),
	)
	common.ByteOrder.PutUint32(
		tuple[TupleHeaderOffsetCid:TupleHeaderOffsetCid+4],
		uint32(cid),
	)

	return nil
}

// -----------------------------------------------------------------------------
// Replay primitives
//
// These methods apply WAL records to the page during recovery. They differ
// from the runtime write paths (InsertTuple, DeleteTuple) in three ways:
//
//   1. No WAL logging — the WAL record already exists; replay just applies it.
//   2. No conflict checks — no user transaction is running during recovery.
//   3. Idempotent — replaying the same record twice produces the same state.
//
// The idempotence check (pageLSN < rec.LSN) is done by the caller
// (replayWithPage in access/heap_applier.go). These methods assume the
// caller has already decided the change needs to be applied.
// -----------------------------------------------------------------------------

// ReplayInsert places tupleBytes into slot during redo of a RecInsert record.
//
// Idempotent: if the slot already holds exactly these bytes, returns nil.
// If it holds different bytes, that's corruption and an error is returned.
func (h *HeapPage) ReplayInsert(slot common.SlotIndex, tupleBytes []byte) error {
	if h.DoesTupleInSlotExist(slot) {
		existing := h.GetTupleWithSlot(slot)
		if bytes.Equal(existing, tupleBytes) {
			return nil
		}
		return fmt.Errorf(
			"replay insert: slot %d already holds %d bytes, want %d",
			slot, len(existing), len(tupleBytes))
	}

	if slot != h.NextSlotIdx() {
		return fmt.Errorf(
			"replay insert: slot %d is not the next free slot %d",
			slot, h.NextSlotIdx())
	}

	_, err := h.InstallTuple(slot, tupleBytes)
	return err
}

// ReplayDelete stamps xmax on the tuple in slot during redo of a RecDelete
// record.
//
// Idempotent: if xmax is already the requested value, returns nil. If xmax
// is set to a *different* non-zero value, that's corruption.
func (h *HeapPage) ReplayDelete(slot common.SlotIndex, xmax common.TransactionID) error {
	if !h.validateSlot(slot) {
		return fmt.Errorf("replay delete: invalid slot %d", slot)
	}
	slotOffset := getSlotOffset(slot)
	v := common.ByteOrder.Uint32(h.Page[slotOffset : slotOffset+ItemIdSize])
	tupleOffset, flags, size := h.UnpackItemId(v)

	if flags != uint8(ItemIdFlagNormal) {
		return fmt.Errorf(
			"replay delete: slot %d has flags %d (want normal)",
			slot, flags)
	}
	if int(size) < HeapTupleHeaderMinSize {
		return fmt.Errorf(
			"replay delete: tuple in slot %d too small for header (%d bytes)",
			slot, size)
	}

	tuple := h.Page[tupleOffset : tupleOffset+size]
	oldXmax := common.TransactionID(
		common.ByteOrder.Uint32(tuple[TupleHeaderOffsetXmax:]))

	if oldXmax == xmax {
		return nil // already applied
	}
	if oldXmax != 0 {
		return fmt.Errorf(
			"replay delete: slot %d already deleted by tx %d (want %d)",
			slot, oldXmax, xmax)
	}

	common.ByteOrder.PutUint32(tuple[TupleHeaderOffsetXmax:], uint32(xmax))
	return nil
}

// ReplayRemoveTuple removes the tuple in slot during undo of an insert.
//
// Undo of an insert means "the tuple was never inserted." The physical
// bytes stay in the free region (they'll be reclaimed by vacuum or
// overwritten by a future insert), but the slot is marked ItemIdFlagUnused
// so it no longer appears to exist.
//
// Idempotent: if the slot is already unused or deleted, returns nil.
func (h *HeapPage) ReplayRemoveTuple(slot common.SlotIndex) error {
	if !h.validateSlot(slot) {
		return fmt.Errorf("replay remove: invalid slot %d", slot)
	}
	slotOffset := getSlotOffset(slot)
	v := common.ByteOrder.Uint32(h.Page[slotOffset : slotOffset+ItemIdSize])
	tupleOffset, flags, size := h.UnpackItemId(v)

	if flags == uint8(ItemIdFlagUnused) || flags == uint8(ItemIdFlagDeleted) {
		return nil
	}
	if flags != uint8(ItemIdFlagNormal) {
		return fmt.Errorf(
			"replay remove: slot %d has flags %d (want normal)", slot, flags)
	}

	newV := packItemId(tupleOffset, uint8(ItemIdFlagUnused), size)
	common.ByteOrder.PutUint32(h.Page[slotOffset:slotOffset+ItemIdSize], newV)
	return nil
}

// ReplayRestoreTuple writes the original tuple bytes back into the given
// slot, reversing a RecDelete. The before-image contains the tuple's full
// bytes (header included) as they were before the delete stamped xmax.
func (h *HeapPage) ReplayRestoreTuple(slot common.SlotIndex, beforeBytes []byte) error {
	if !h.validateSlot(slot) {
		return fmt.Errorf("replay restore: invalid slot %d", slot)
	}
	slotOffset := getSlotOffset(slot)
	v := common.ByteOrder.Uint32(h.Page[slotOffset : slotOffset+ItemIdSize])
	tupleOffset, flags, size := h.UnpackItemId(v)

	if flags != uint8(ItemIdFlagNormal) {
		return fmt.Errorf("replay restore: slot %d has flags %d (want normal)", slot, flags)
	}
	if int(size) != len(beforeBytes) {
		return fmt.Errorf("replay restore: slot %d size %d != before-image size %d",
			slot, size, len(beforeBytes))
	}

	copy(h.Page[tupleOffset:tupleOffset+size], beforeBytes)
	return nil
}
