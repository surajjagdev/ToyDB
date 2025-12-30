package page

import (
	"testing"

	"github.com/surajjagdev/ToyDB/internal/common"
)

func TestNewPage(t *testing.T) {
	p, err := NewPage()

	if err != nil {
		t.Fatalf("%v", err)
	}

	if p.PageID != common.InvalidPageID {
		t.Fatalf("Page id = %v, want %v", p.PageID, common.InvalidPageID)
	}
	if len(p.Data) != int(common.PageSize) {
		t.Fatalf("Data size (int) = %v, want %v", len(p.Data), int(common.PageSize))
	}
}

func TestResetPage(t *testing.T) {
	p, err := NewPage()

	if err != nil {
		t.Fatalf("%v", err)
	}

	p.PageID = common.MaxPageID / 2

	p.Data[100] = 0xAA

	p.Reset()

	if p.PageID == common.MaxPageID/2 {
		t.Fatalf("Page id = %v, want %v", p.PageID, common.InvalidPageID)
	}
	if len(p.Data) != int(common.PageSize) {
		t.Fatalf("Data size (int) = %v, want %v", len(p.Data), int(common.PageSize))
	}

	// data must not be cleared
	if p.Data[100] != 0xAA {
		t.Fatalf("page data should not be zeroed on reset")
	}
}

func TestLSNReadWrite(t *testing.T) {
	p, err := NewPage()
	if err != nil {
		t.Fatalf("%v", err)
	}

	lsn := common.LogSeqNumber(12345)
	p.SetLSN(lsn)

	read := p.GetLSN()
	if read != lsn {
		t.Fatalf("LSN mismatch: got %d, want %d", read, lsn)
	}
}

func TestPageIDReadWrite(t *testing.T) {
	p, err := NewPage()

	if err != nil {
		t.Fatalf("%v", err)
	}

	id := common.PageID(12345)
	p.SetPageId(id)

	read := p.GetPageID()
	if read != id {
		t.Fatalf("PageID mismatch: got %d, want %d", read, id)
	}
}

func TestPageIDAndLSNReadWrite(t *testing.T) {
	p, err := NewPage()

	if err != nil {
		t.Fatalf("%v", err)
	}

	id := common.PageID(12345)
	lsn := common.LogSeqNumber(99999)
	p.SetPageId(id)
	p.SetLSN(lsn)

	readPageId := p.GetPageID()
	readLSN := p.GetLSN()

	if readPageId != id {
		t.Fatalf("PageID mismatch: got %d, want %d", readPageId, id)
	}
	if readLSN != lsn {
		t.Fatalf("LSN mismatch: got %d, want %d", readLSN, lsn)
	}
}

func TestCopyDataIsolation(t *testing.T) {
	p, err := NewPage()
	if err != nil {
		t.Fatalf("%v", err)
	}
	p.Data[50] = 0xAA

	copy := p.CopyData()
	copy[50] = 0xBB

	if p.Data[50] != 0xAA {
		t.Fatal("CopyData must return a deep copy")
	}
}

func TestPageLatches(t *testing.T) {
	p, err := NewPage()
	if err != nil {
		t.Fatalf("%v", err)
	}

	p.WLatch()
	p.WUnlatch()

	p.RLatch()
	p.RUnlatch()
}

func TestPageFlagsReadWrite(t *testing.T) {
	p, err := NewPage()
	if err != nil {
		t.Fatalf("%v", err)
	}

	flags := PageFlagInitialized | PageFlagHeap | PageFlagLeaf
	p.SetFlags(flags)

	if got := p.GetFlags(); got != flags {
		t.Fatalf("flags = %v, want %v", got, flags)
	}
}

func TestPageAddClearFlags(t *testing.T) {
	p, err := NewPage()
	if err != nil {
		t.Fatalf("%v", err)
	}

	p.AddFlags(PageFlagHeap)
	if !p.HasFlag(PageFlagHeap) {
		t.Fatal("expected PageFlagHeap to be set")
	}

	p.AddFlags(PageFlagLeaf)
	if !p.HasFlag(PageFlagLeaf) {
		t.Fatal("expected PageFlagLeaf to be set")
	}

	p.ClearFlags(PageFlagHeap)
	if p.HasFlag(PageFlagHeap) {
		t.Fatal("PageFlagHeap should be cleared")
	}

	if !p.HasFlag(PageFlagLeaf) {
		t.Fatal("PageFlagLeaf should remain set")
	}
}

func TestPageLowerUpperInitialization(t *testing.T) {
	p, err := NewPage()
	if err != nil {
		t.Fatalf("%v", err)
	}

	p.SetLower(OffsetDataStart)
	p.SetUpper(uint16(common.PageSize))

	if p.GetLower() != OffsetDataStart {
		t.Fatalf("lower = %d, want %d", p.GetLower(), OffsetDataStart)
	}

	if p.GetUpper() != uint16(common.PageSize) {
		t.Fatalf("upper = %d, want %d", p.GetUpper(), common.PageSize)
	}
}

func TestLowerUpperMovement(t *testing.T) {
	p, err := NewPage()
	if err != nil {
		t.Fatalf("%v", err)
	}

	p.SetLower(OffsetDataStart)
	p.SetUpper(uint16(common.PageSize))

	// Simulate adding a line pointer (4 bytes)
	p.SetLower(p.GetLower() + 4)

	// Simulate inserting a tuple (100 bytes)
	p.SetUpper(p.GetUpper() - 100)

	if p.GetLower() >= p.GetUpper() {
		t.Fatal("lower must always be less than upper")
	}
}

func TestPageDetectsNoFreeSpace(t *testing.T) {
	p, err := NewPage()
	if err != nil {
		t.Fatalf("%v", err)
	}

	p.SetLower(OffsetDataStart)
	p.SetUpper(OffsetDataStart)

	if p.GetLower() < p.GetUpper() {
		t.Fatal("expected page to have no free space")
	}
}

func TestSpecialAreaReadWrite(t *testing.T) {
	p, err := NewPage()
	if err != nil {
		t.Fatalf("%v", err)
	}

	b := uint16(0xA)
	p.SetSpecial(b)

	read := p.GetSpecial()
	if read != b {
		t.Fatalf("Special area bytes mismatch: got %d, want %d", read, b)
	}
}

func TestPageChecksumValid(t *testing.T) {
	p, err := NewPage()
	if err != nil {
		t.Fatalf("%v", err)
	}

	// Fill page with deterministic data
	for i := OffsetDataStart; i < int(common.PageSize); i++ {
		p.Data[i] = byte(i % 256)
	}

	p.UpdateChecksum()

	if !p.ValidateIntegrity() {
		t.Fatal("checksum should be valid")
	}
}

func TestPageChecksumDetectsCorruption(t *testing.T) {
	p, err := NewPage()
	if err != nil {
		t.Fatalf("%v", err)
	}

	p.Data[100] = 0xAA
	p.UpdateChecksum()

	// Corrupt data
	p.Data[100] = 0xBB

	if p.ValidateIntegrity() {
		t.Fatal("checksum validation should fail after corruption")
	}
}

func TestChecksumDoesNotIncludeChecksumField(t *testing.T) {
	p, err := NewPage()

	if err != nil {
		t.Fatalf("%v", err)
	}

	p.Data[OffsetDataStart] = 0x11
	p.UpdateChecksum()

	// Modify checksum field directly
	p.Data[OffsetChecksum] ^= 0xFF

	if p.ValidateIntegrity() {
		t.Fatal("checksum should fail if checksum field is modified")
	}
}

func TestCheckSumTakesHeadersAndData(t *testing.T) {
	p, err := NewPage()
	if err != nil {
		t.Fatalf("%v", err)
	}
	p.SetLSN(common.InvalidLogSeqNumber / 2)
	p.SetPageId(common.MaxPageID / 2)
	p.SetFlags(PageFlagDeleted)
	p.SetSpecial(uint16(0xA))
	p.SetLower(OffsetDataStart)
	p.SetUpper(uint16(common.PageSize))
	p.Data[100] = 0xAA
	p.UpdateChecksum()

	if !p.ValidateIntegrity() {
		t.Fatal("checksum should not fail")
	}

	// update page id, without recalculating the checksum
	p.SetPageId((common.MaxPageID / 2) - 1)

	if p.ValidateIntegrity() {
		t.Fatal("checksum should fail after updating page id")
	}
}
