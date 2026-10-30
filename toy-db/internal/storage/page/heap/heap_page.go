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
	ItemIdDeleted = 1 << 3 // only mark via vaccumm, so indexes can lazily remove
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

type TupleHeader struct {
	XMin common.TransactionID
	XMax common.TransactionID // 0 = live
	Cid  common.CommandID
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

// NextSlotIdx returns the slot index that the next InstallTuple would
// allocate. It is the same computation as NextSlot but without the
// room check — callers that need the check should use NextSlot instead.
func (h *HeapPage) NextSlotIdx() common.SlotIndex {
	return common.SlotIndex((h.GetLower() - page.PageHeaderSize) / ItemIdSize)
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

// get the slot index, based on slot position.
// not guaranteed to exist
func getSlotOffset(slot common.SlotIndex) int {
	return page.PageHeaderSize + int(slot)*ItemIdSize
}

func (h *HeapPage) validateSlot(slot common.SlotIndex) bool {
	slotOffset := getSlotOffset(slot)

	// slot is referring to unclaimed space, so it is
	// not valid
	if slotOffset+ItemIdSize > int(h.GetLower()) {
		return false
	}

	return true
}

// GetTupleHeader reads the MVCC header for the tuple in the given slot.
//
// Callers must hold at least a read latch on the page. The returned header
// is a value copy; it does not alias the page buffer.
//
// Returns an error if the slot does not point at a live tuple, or if the
// tuple is too small to hold a header (corruption).
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

func (h *HeapPage) GetNumSlots() int {
	return int((h.GetLower() - page.PageHeaderSize) / ItemIdSize)
}

// Check if tuple exists using the slot number
func (h *HeapPage) DoesTupleInSlotExist(slot common.SlotIndex) bool {
	if !h.validateSlot(slot) {
		return false
	}

	slotOffset := getSlotOffset(slot)
	v := common.ByteOrder.Uint32(h.Page[slotOffset : slotOffset+4])
	_, flags, _ := h.UnpackItemId(v)

	// Only return true if the slot points to a live, normal tuple
	return flags == ItemIdFlagNormal
}

// tuple crud

// Get the tuple from page using the slot index.
// Does not validate if slot index exists, so should be
// done before
func (h *HeapPage) GetTupleWithSlot(slot common.SlotIndex) []byte {
	slotOffset := getSlotOffset(slot)

	// read the 4 bytes for item composed of tuple offset, flag and tuple size
	v := common.ByteOrder.Uint32(h.Page[slotOffset : slotOffset+4])
	offset, _, size := h.UnpackItemId(v)
	return h.Page[offset : offset+size]
}

// Calculate the bytes needed to insert a tuple into the page
// if null bitmap is not provided, nullBitmapLen should be 0
// if data is not provided, dataLen should be 0

// Takes header offset + null bitmap length + data length and aligns to 8 bytes
// then adds the item id size to get the total bytes needed to insert the tuple
// dataLen -> length of the data to be inserted
// nullBitmapLen -> length of the null bitmap
// returns the bytes header offset, tuple size and the total bytes needed to insert the tuple
func CalculateBytesNeededToInsertTuple(dataLen int, nullBitmapLen int) (hoff int, tupleSize int, requiredSpace int) {
	hoff = 0
	tupleSize = 0
	requiredSpace = 0

	// calculate header offset -> minimum header without null bitmap + nullbitmap length -> align to 8 bytes
	hoff = page.AlignTo(HeapTupleHeaderMinSize+nullBitmapLen, HeapTupleHeaderAlign)

	// calculate tuple size -> header offset + data length -> align to 8 bytes
	tupleSize = page.AlignTo(hoff+dataLen, HeapTupleHeaderAlign)

	return hoff, tupleSize, tupleSize + ItemIdSize
}

// BuildTupleBytes produces the exact bytes that will occupy a slot when this
// tuple is installed, without modifying the page.
//
// It exists so callers can log the tuple to the WAL before mutating memory
// (the write-ahead rule). The returned bytes include the full HeapTupleHeader
// (xmin, xmax, cid, ctid, infomask, infomask2, hoff), the null bitmap, user
// data, and any trailing alignment padding — everything the slot will hold.
//
// slot and blockID fill the tuple's ctid. The caller should have obtained
// slot from NextSlot() and hold the page's write latch across Build + Install
// so the two agree.
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

	// Zero-initialised; trailing padding is already 0.
	buf := make([]byte, tupleSize)

	// --- fixed header ---
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

	// hoff: offset where user data begins.
	buf[TupleHeaderOffsetHoff] = uint8(hoff)

	// --- user data ---
	copy(buf[int(hoff):], data)

	return buf
}

// InstallTuple writes a pre-built tuple blob into the given slot, updating
// the page's slot array and upper pointer. The caller must hold the write
// latch and must have verified there is room for
// len(tupleBytes) + ItemIdSize bytes on the page.
//
// The ctid embedded in tupleBytes is trusted — the caller is responsible
// for producing the blob with BuildTupleBytes using the same slot passed
// here.
func (h *HeapPage) InstallTuple(
	slot common.SlotIndex,
	tupleBytes []byte,
) (common.RecordId, error) {
	lower := h.GetLower()
	upper := h.GetUpper()

	// The slot must be exactly the next free slot. If not, either the caller
	// peeked the wrong slot or something changed the page under the latch
	// (which would be a bug).
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

	// --- allocate tuple space from upper ---
	newUpper := upper - uint16(tupleSize)
	tupleOffset := newUpper
	h.SetUpper(newUpper)

	copy(h.Page[tupleOffset:tupleOffset+uint16(tupleSize)], tupleBytes)

	// --- write the ItemId slot ---
	slotOffset := lower
	h.SetLower(lower + ItemIdSize)

	itemID := uint32(0)
	itemID |= uint32(tupleOffset) << 17      // 15-bit offset
	itemID |= uint32(ItemIdFlagNormal) << 15 // 2-bit flags
	itemID |= uint32(tupleSize)              // 15-bit size
	common.ByteOrder.PutUint32(h.Page[slotOffset:slotOffset+ItemIdSize], itemID)

	// --- return the identity, read from the blob's ctid ---
	blockID := common.BlockID(
		common.ByteOrder.Uint32(tupleBytes[TupleHeaderOffsetCtid:]))
	return common.RecordId{BlockID: blockID, Slot: slot}, nil
}

// Insert a single tuple into the page
// InsertTuple inserts a tuple into the page using a fresh slot. It is a
// convenience wrapper around BuildTupleBytes + InstallTuple for callers that
// don't need to log the bytes to the WAL separately (tests, replay).
//
// Production callers (TableHeap.InsertTuple) should use Build + WAL append +
// Install so the write-ahead rule is enforced.
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

// In internal/storage/page/heap/heap.go:

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
	case ItemIdFlagUnused:
		return fmt.Errorf("tuple already removed")
	case ItemIdDeleted:
		return fmt.Errorf("tuple already deleted")
	case ItemIdRedirect:
		return fmt.Errorf("cannot delete redirect slot")
	case ItemIdFlagNormal:
		// ok
	default:
		return fmt.Errorf("unknown itemId flag")
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

// Logical Deletion of tuple from page
func (h *HeapPage) DeleteTuple(
	slot common.SlotIndex,
	xmax common.TransactionID,
	cid common.CommandID,
) error {
	// 1. validate the slot
	slotOffset := getSlotOffset(slot)

	if exists := h.validateSlot(slot); !exists {
		return fmt.Errorf("invalid slot %d", slot)
	}

	// 2. get tuple slot
	v := common.ByteOrder.Uint32(h.Page[slotOffset : slotOffset+4])

	// unpack the flags from tuple
	tupleOffset, flags, size := h.UnpackItemId(v)

	// check if tuple is ok to be marked as deleted
	switch flags {
	case ItemIdFlagUnused:
		return fmt.Errorf("tuple already removed")

	case ItemIdDeleted:
		return fmt.Errorf("tuple already deleted")

	case ItemIdRedirect:
		return fmt.Errorf("cannot delete redirect slot")

	case ItemIdFlagNormal:
		// ok
	default:
		return fmt.Errorf("unknown itemId flag")
	}

	// 3. Get actual tuple using offset
	tuple := h.Page[tupleOffset : tupleOffset+size]

	// check old xmax
	oldXmax := common.ByteOrder.Uint32(
		tuple[TupleHeaderOffsetXmax : TupleHeaderOffsetXmax+4],
	)

	if oldXmax != 0 {
		return fmt.Errorf("Tuple already deleted by txn id %v", oldXmax)
	}

	// set new xmax and cid
	common.ByteOrder.PutUint32(
		tuple[TupleHeaderOffsetXmax:TupleHeaderOffsetXmax+4],
		uint32(xmax),
	)
	common.ByteOrder.PutUint32(
		tuple[TupleHeaderOffsetCid:TupleHeaderOffsetCid+4],
		uint32(cid),
	)

	// 4. Update infomask (future)

	return nil
}

func (h *HeapPage) NextSlot() (common.SlotIndex, error) {
	lower := h.GetLower()

	// The slot array grows upward from PageHeaderSize. The slot that the
	// next insert will claim sits right at the current lower pointer.
	slotIndex := (lower - page.PageHeaderSize) / ItemIdSize

	// The new slot must not collide with tuple data. If it does, the page
	// has no room for a new ItemId, let alone the tuple itself.
	if int(lower)+ItemIdSize > int(h.GetUpper()) {
		return 0, fmt.Errorf("page has no room for a new slot")
	}

	return common.SlotIndex(slotIndex), nil
}

// ReplayRemoveTuple removes the tuple in the given slot during undo of an
// insert. Unlike DeleteTuple, it doesn't stamp xmax — it physically removes
// the tuple, since the insert it reverses is being treated as never having
// happened.
//
// The tuple's bytes are left in place; only the ItemId is marked deleted so
// vacuum can reclaim it. This matches how DeleteTuple works at the page
// level, just with the reversal semantics applied.
func (h *HeapPage) ReplayRemoveTuple(slot common.SlotIndex) error {
	if !h.validateSlot(slot) {
		return fmt.Errorf("replay remove: invalid slot %d", slot)
	}
	slotOffset := getSlotOffset(slot)
	v := common.ByteOrder.Uint32(h.Page[slotOffset : slotOffset+ItemIdSize])
	_, flags, _ := h.UnpackItemId(v)

	if flags == ItemIdDeleted {
		return nil // already removed
	}
	if flags != ItemIdFlagNormal {
		return fmt.Errorf("replay remove: slot %d has flags %d (want normal)", slot, flags)
	}

	// Set the ItemIdDeleted flag while preserving offset and size.
	deletedV := (v &^ (0b11 << 15)) | (uint32(ItemIdDeleted) << 15)
	common.ByteOrder.PutUint32(h.Page[slotOffset:slotOffset+ItemIdSize], deletedV)
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

	if flags != ItemIdFlagNormal {
		return fmt.Errorf("replay restore: slot %d has flags %d (want normal)", slot, flags)
	}
	if int(size) != len(beforeBytes) {
		return fmt.Errorf("replay restore: slot %d size %d != before-image size %d",
			slot, size, len(beforeBytes))
	}

	copy(h.Page[tupleOffset:tupleOffset+size], beforeBytes)
	return nil
}

// -----------------------------------------------------------------------------
// Redo/undo replay primitives
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
// If it holds different bytes, that's corruption (the log says one thing,
// the page says another) and an error is returned.
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
//
// Unlike DeleteTuple, this does not check the caller's txid against the
// tuple's state — redo applies what the log says regardless.
func (h *HeapPage) ReplayDelete(slot common.SlotIndex, xmax common.TransactionID) error {
	if !h.validateSlot(slot) {
		return fmt.Errorf("replay delete: invalid slot %d", slot)
	}
	slotOffset := getSlotOffset(slot)
	v := common.ByteOrder.Uint32(h.Page[slotOffset : slotOffset+ItemIdSize])
	tupleOffset, flags, size := h.UnpackItemId(v)

	if flags != ItemIdFlagNormal {
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
