package page

import (
	"testing"

	"github.com/surajjagdev/ToyDB/internal/common"
)

func TestAlignTo(t *testing.T) {
	sz := 13
	align := 8
	expected := 16

	if got := AlignTo(sz, align); got != expected {
		t.Fatalf("Alignment = %d, want %d", got, expected)
	}
}

func newTestPage(t *testing.T) Page {
	t.Helper()
	p := Page(make([]byte, common.PageSize))
	InitBasePage(p, PageFlagInitialized)
	return p
}

func TestPageInit(t *testing.T) {
	p := newTestPage(t)

	if p.GetFlags()&PageFlagInitialized == 0 {
		t.Fatal("page should be initialized")
	}

	if p.GetPageVersion() != common.CurrentPageVersion {
		t.Fatalf("page version = %d, want %d", p.GetPageVersion(), common.CurrentPageVersion)
	}
	if p.GetPageSize() != common.PageSize {
		t.Fatalf("page size = %d, want %d", p.GetPageSize(), common.PageSize)
	}
}

func TestLSNReadWrite(t *testing.T) {
	p := newTestPage(t)

	lsn := common.LogSeqNumber(12345)
	p.SetLSN(lsn)

	if got := p.GetLSN(); got != lsn {
		t.Fatalf("LSN = %d, want %d", got, lsn)
	}
}

func TestPageIDReadWrite(t *testing.T) {
	p := newTestPage(t)

	id := common.PageID(12345)
	p.SetPageId(id)

	if got := p.GetPageID(); got != id {
		t.Fatalf("PageID = %d, want %d", got, id)
	}
}

func TestPageVersionReadWrite(t *testing.T) {
	p := newTestPage(t)

	p.SetPageVersion()

	setPageVersion := p.GetPageVersion()
	setPageSize := p.GetPageSize()

	if setPageVersion != common.CurrentPageVersion {
		t.Fatalf("Page version = %d, want %d", setPageVersion, common.CurrentPageVersion)
	}
	if setPageSize != common.PageSize {
		t.Fatalf("Page size = %d, want %d", setPageSize, common.PageSize)
	}
}

func TestPageIDAndLSNReadWrite(t *testing.T) {
	p := newTestPage(t)

	id := common.PageID(12345)
	lsn := common.LogSeqNumber(99999)

	p.SetPageId(id)
	p.SetLSN(lsn)

	if got := p.GetPageID(); got != id {
		t.Fatalf("PageID = %d, want %d", got, id)
	}
	if got := p.GetLSN(); got != lsn {
		t.Fatalf("LSN = %d, want %d", got, lsn)
	}
}
func TestPageFlagsReadWrite(t *testing.T) {
	p := newTestPage(t)

	flags := PageFlagHeap | PageFlagLeaf
	p.SetFlags(flags)

	if got := p.GetFlags(); got != flags {
		t.Fatalf("flags = %v, want %v", got, flags)
	}
}

func TestPageAddClearFlags(t *testing.T) {
	p := newTestPage(t)

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

func TestLowerUpperMovement(t *testing.T) {
	p := newTestPage(t)

	start := PageHeaderSize
	p.SetLower(uint16(start))
	p.SetUpper(uint16(common.PageSize))

	// Simulate adding ItemId (4 bytes)
	p.SetLower(p.GetLower() + 4)

	// Simulate inserting tuple (100 bytes)
	p.SetUpper(p.GetUpper() - 100)

	if p.GetLower() >= p.GetUpper() {
		t.Fatal("lower must always be less than upper")
	}
}

func TestPageDetectsNoFreeSpace(t *testing.T) {
	p := newTestPage(t)

	p.SetLower(PageHeaderSize)
	p.SetUpper(PageHeaderSize)

	if p.GetLower() < p.GetUpper() {
		t.Fatal("expected page to have no free space")
	}
}

func TestSpecialAreaReadWrite(t *testing.T) {
	p := newTestPage(t)

	v := uint16(0xA)
	p.SetSpecial(v)

	if got := p.GetSpecial(); got != v {
		t.Fatalf("special = %d, want %d", got, v)
	}
}

func TestPageChecksumValid(t *testing.T) {
	p := newTestPage(t)

	for i := OffsetDataStart; i < int(common.PageSize); i++ {
		p[i] = byte(i % 256)
	}

	p.UpdateChecksum()

	if !p.ValidateIntegrity() {
		t.Fatal("checksum should be valid")
	}
}

func TestPageChecksumDetectsCorruption(t *testing.T) {
	p := newTestPage(t)

	p[100] = 0xAA
	p.UpdateChecksum()

	// Corrupt data
	p[100] = 0xBB

	if p.ValidateIntegrity() {
		t.Fatal("checksum validation should fail after corruption")
	}
}

func TestChecksumDoesNotIncludeChecksumField(t *testing.T) {
	p := newTestPage(t)

	p[OffsetDataStart] = 0x11
	p.UpdateChecksum()

	// Modify checksum field directly
	p[OffsetChecksum] ^= 0xFF

	if p.ValidateIntegrity() {
		t.Fatal("checksum should fail if checksum field is modified")
	}
}

func TestCheckSumTakesHeadersAndData(t *testing.T) {
	p := newTestPage(t)

	p.SetLSN(common.InvalidLogSeqNumber / 2)
	p.SetPageId(common.MaxPageID / 2)
	p.SetFlags(PageFlagDeleted)
	p.SetSpecial(0xA)
	p.SetLower(PageHeaderSize)
	p.SetUpper(uint16(common.PageSize))
	p[100] = 0xAA

	p.UpdateChecksum()

	if !p.ValidateIntegrity() {
		t.Fatal("checksum should not fail")
	}

	// Modify header without recomputing checksum
	p.SetPageId((common.MaxPageID / 2) - 1)

	if p.ValidateIntegrity() {
		t.Fatal("checksum should fail after header modification")
	}
}
