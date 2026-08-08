package heap

import (
	"testing"

	"github.com/surajjagdev/ToyDB/internal/common"
	"github.com/surajjagdev/ToyDB/internal/storage/page"
)

func newHeapTestPage(t *testing.T) *HeapPage {
	t.Helper()
	p := page.Page(make([]byte, common.PageSize))
	return InitHeapPage(p)
}

func TestHeapPageInit(t *testing.T) {
	p := newHeapTestPage(t)

	if p.GetFlags()&page.PageFlagInitialized == 0 {
		t.Fatal("page should be initialized")
	}
	if p.GetFlags()&page.PageFlagHeap == 0 {
		t.Fatal("page should be categorized as heap")
	}

	if p.GetLower() != page.PageHeaderSize {
		t.Fatalf("lower = %d, want %d", p.GetLower(), page.PageHeaderSize)
	}

	if p.GetUpper() != uint16(common.PageSize) {
		t.Fatalf("upper = %d, want %d", p.GetUpper(), common.PageSize)
	}

	if p.GetSpecial() != uint16(common.PageSize) {
		t.Fatalf("special = %d, want %d", p.GetSpecial(), common.PageSize)
	}

	if p.GetPageVersion() != common.CurrentPageVersion {
		t.Fatalf("page version = %d, want %d", p.GetPageVersion(), common.CurrentPageVersion)
	}
	if p.GetPageSize() != common.PageSize {
		t.Fatalf("page size = %d, want %d", p.GetPageSize(), common.PageSize)
	}
}

func TestTupleInsert(t *testing.T) {
	p := page.Page(make([]byte, common.PageSize))
	h := InitHeapPage(p)

	data := make([]byte, common.PageSize/10)
	for i := 0; i < len(data); i++ {
		data[i] = byte(i)
	}

	err := h.InsertTuple(data, nil, common.BlockID(1), common.TransactionID(1), common.CommandID(0))
	if err != nil {
		t.Fatalf("failed tuple insert %v", err)
	}

	expectedTupleSize := HeapTupleHeaderMinSize + len(data)
	expectedTupleSize = page.AlignTo(expectedTupleSize, HeapTupleHeaderAlign)

	expectedUpper := uint16(int(common.PageSize) - expectedTupleSize)
	if got := h.Page.GetUpper(); got != expectedUpper {
		t.Fatalf("upper mismatch: expected %d, got %d", expectedUpper, got)
	}

	if got := h.Page.GetLower(); got != uint16(page.PageHeaderSize+ItemIdSize) {
		t.Fatalf("lower mismatch: expected %d, got %d",
			uint16(page.PageHeaderSize+ItemIdSize), got)
	}

	tuple := h.GetTupleWithSlot(0)
	if tuple == nil {
		t.Fatalf("tuple is nil")
	}

	if len(tuple) != expectedTupleSize {
		t.Fatalf("tuple size mismatch: expected %d, got %d",
			expectedTupleSize, len(tuple))
	}

	xmin := common.ByteOrder.Uint32(tuple[TupleHeaderOffsetXmin:])
	if xmin != 1 {
		t.Fatalf("xmin mismatch: got %d", xmin)
	}

	hoff := tuple[TupleHeaderOffsetHoff]
	expectedHoff := page.AlignTo(HeapTupleHeaderMinSize, HeapTupleHeaderAlign)
	if hoff != byte(expectedHoff) {
		t.Fatalf("hoff mismatch: expected %d, got %d", expectedHoff, hoff)
	}

	BlockID := common.ByteOrder.Uint32(tuple[TupleHeaderOffsetCtid:])
	slot := common.ByteOrder.Uint16(tuple[TupleHeaderOffsetCtid+4:])
	if BlockID != 1 || slot != 0 {
		t.Fatalf("ctid mismatch: got (%d,%d)", BlockID, slot)
	}

	// descriptors not done yet
}

func TestTupleInsertMultiple(t *testing.T) {
	p := page.Page(make([]byte, common.PageSize))
	h := InitHeapPage(p)

	data1 := make([]byte, common.PageSize/10)
	data2 := make([]byte, common.PageSize/12)
	for i := 0; i < len(data1); i++ {
		data1[i] = byte(i)
	}
	for i := 0; i < len(data2); i++ {
		data2[i] = byte(i)
	}

	err := h.InsertTuple(data1, nil, common.BlockID(1), common.TransactionID(1), common.CommandID(0))
	if err != nil {
		t.Fatalf("failed tuple insert %v", err)
	}
	err = h.InsertTuple(data2, nil, common.BlockID(1), common.TransactionID(1), common.CommandID(1))
	if err != nil {
		t.Fatalf("failed tuple insert %v", err)
	}

	tuple1 := h.GetTupleWithSlot(0)
	if tuple1 == nil {
		t.Fatalf("tuple1 is nil")
	}
	tuple2 := h.GetTupleWithSlot(1)
	if tuple2 == nil {
		t.Fatalf("tuple2 is nil")
	}

	if len(tuple1) < len(tuple2) {
		t.Fatalf("expected tuple 1 to be smaller than tuple 2")
	}

	BlockID := common.ByteOrder.Uint32(tuple1[TupleHeaderOffsetCtid:])
	slot := common.ByteOrder.Uint16(tuple1[TupleHeaderOffsetCtid+4:])
	BlockID2 := common.ByteOrder.Uint32(tuple2[TupleHeaderOffsetCtid:])
	slot2 := common.ByteOrder.Uint16(tuple2[TupleHeaderOffsetCtid+4:])
	if BlockID != 1 || slot != 0 {
		t.Fatalf("ctid mismatch: got (%d,%d)", BlockID, slot)
	}
	if BlockID2 != 1 || slot2 != 1 {
		t.Fatalf("ctid mismatch: got (%d,%d)", BlockID, slot)
	}
}

func TestTupleInsertMultipleAndDelete(t *testing.T) {
	p := page.Page(make([]byte, common.PageSize))
	h := InitHeapPage(p)

	data1 := make([]byte, common.PageSize/10)
	data2 := make([]byte, common.PageSize/12)
	for i := 0; i < len(data1); i++ {
		data1[i] = byte(i)
	}
	for i := 0; i < len(data2); i++ {
		data2[i] = byte(i)
	}

	err := h.InsertTuple(data1, nil, common.BlockID(1), common.TransactionID(1), common.CommandID(0))
	if err != nil {
		t.Fatalf("failed tuple insert %v", err)
	}
	err = h.InsertTuple(data2, nil, common.BlockID(1), common.TransactionID(1), common.CommandID(1))
	if err != nil {
		t.Fatalf("failed tuple insert %v", err)
	}

	// delete tuple 1
	err = h.DeleteTuple(0, common.TransactionID(2), common.CommandID(0))
	if err != nil {
		t.Fatalf("failed delete tuple %v", err)
	}

	// get the physical tuple, still should be alive
	tuple := h.GetTupleWithSlot(0)

	if tuple == nil {
		t.Fatalf("tuple is nil")
	}

	// read the xmax contents of the tuple and cid
	readXmax := common.ByteOrder.Uint32(tuple[TupleHeaderOffsetXmax : TupleHeaderOffsetXmax+4])

	if common.TransactionID(readXmax) != common.TransactionID(2) {
		t.Fatalf("Expected xmax to be set after logically deleting the tuple. Expected %v, got %v", common.TransactionID(2), readXmax)
	}
}

func TestFreeSpace(t *testing.T) {
	p := page.Page(make([]byte, common.PageSize))
	h := InitHeapPage(p)

	freeSpace := h.GetFreeSpace()
	expectedFreeSpace := h.GetUpper() - h.GetLower()

	if expectedFreeSpace != uint16(freeSpace) {
		t.Fatalf("Expected freespace to equal %v, got %v", expectedFreeSpace, freeSpace)
	}
}

func TestSlotOffset(t *testing.T) {
	expected0 := page.PageHeaderSize + 0*ItemIdSize
	if got := getSlotOffset(0); got != expected0 {
		t.Fatalf("Expected slot offset to equal %v, got %v", expected0, got)
	}
	expected1 := page.PageHeaderSize + 6*ItemIdSize
	if got := getSlotOffset(6); got != expected1 {
		t.Fatalf("Expected slot offset to equal %v, got %v", expected1, got)
	}
}

func TestValidateSlotOffset(t *testing.T) {
	p := page.Page(make([]byte, common.PageSize))
	h := InitHeapPage(p)

	data := make([]byte, common.PageSize/9)
	for i := 0; i < len(data); i++ {
		data[i] = byte(i)
	}

	initialFreeSpace := h.GetFreeSpace()

	h.InsertTuple(data, nil, common.BlockID(0), common.MaxTransactionID-1, common.MaxCommandID-1)

	newFreeSpace := h.GetFreeSpace()

	if newFreeSpace >= initialFreeSpace {
		t.Fatalf("Expected tuple to occupy space in page and reduce free space.")
	}

	// verify slot 0 exists
	if slot0Exists := h.validateSlot(0); !slot0Exists {
		t.Fatalf("Expected slot index 0 to exist")
	}

	if slot12Exists := h.validateSlot(12); slot12Exists {
		t.Fatalf("Expected slot index 12 to not exist")
	}
}
