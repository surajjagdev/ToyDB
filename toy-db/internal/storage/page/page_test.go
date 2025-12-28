package page

import (
	"testing"

	"github.com/surajjagdev/ToyDB/internal/common"
)

func TestNewPage(t *testing.T) {
	p := NewPage()

	if p.isDirty != false {
		t.Fatalf("Dirty = %v, want %v", p.isDirty, false)
	}
	if p.PinCount != 0 {
		t.Fatalf("Pin count = %v, want %v", p.PinCount, 0)
	}
	if p.PageID != common.InvalidPageID {
		t.Fatalf("Page id = %v, want %v", p.PinCount, common.InvalidPageID)
	}
	if p.Rel != common.InvalidRelationID {
		t.Fatalf("Rel id = %v, want %v", p.Rel, common.InvalidRelationID)
	}
	if p.Fork != common.InvalidForkId {
		t.Fatalf("Fork id = %v, want %v", p.Fork, common.InvalidForkId)
	}
	if len(p.Data) != int(common.PageSize) {
		t.Fatalf("Data size (int) = %v, want %v", len(p.Data), int(common.PageSize))
	}
}

func TestResetPage(t *testing.T) {
	p := &Page{
		PageID:   common.MaxPageID / 2,
		Rel:      common.InvalidRelationID / 2,
		Fork:     common.ForkFSM,
		PinCount: 1,
		isDirty:  true,
	}

	p.Data[100] = 0xAA

	p.Reset()

	if p.isDirty == true {
		t.Fatalf("Dirty = %v, want %v", p.isDirty, false)
	}
	if p.PinCount == 1 {
		t.Fatalf("Pin count = %v, want %v", p.PinCount, 0)
	}
	if p.PageID == common.MaxPageID/2 {
		t.Fatalf("Page id = %v, want %v", p.PinCount, common.InvalidPageID)
	}
	if p.Rel == common.InvalidRelationID/2 {
		t.Fatalf("Rel id = %v, want %v", p.Rel, common.InvalidRelationID)
	}
	if p.Fork == common.ForkFSM {
		t.Fatalf("Fork id = %v, want %v", p.Fork, common.InvalidForkId)
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
	p := NewPage()

	lsn := common.LogSeqNumber(12345)
	p.SetLSN(lsn)

	read := p.GetLSN()
	if read != lsn {
		t.Fatalf("LSN mismatch: got %d, want %d", read, lsn)
	}
}

func TestPageIDReadWrite(t *testing.T) {
	p := NewPage()

	id := common.PageID(12345)
	p.SetPageId(id)

	read := p.GetPageID()
	if read != id {
		t.Fatalf("PageID mismatch: got %d, want %d", read, id)
	}
}

func TestPageIDAndLSNReadWrite(t *testing.T) {
	p := NewPage()

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

func TestPageChecksumValid(t *testing.T) {
	p := NewPage()

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
	p := NewPage()

	p.Data[100] = 0xAA
	p.UpdateChecksum()

	// Corrupt data
	p.Data[100] = 0xBB

	if p.ValidateIntegrity() {
		t.Fatal("checksum validation should fail after corruption")
	}
}

func TestChecksumDoesNotIncludeChecksumField(t *testing.T) {
	p := NewPage()

	p.Data[OffsetDataStart] = 0x11
	p.UpdateChecksum()

	// Modify checksum field directly
	p.Data[OffsetChecksum] ^= 0xFF

	if p.ValidateIntegrity() {
		t.Fatal("checksum should fail if checksum field is modified")
	}
}

func TestCheckSumTakesHeadersAndData(t *testing.T) {
	p := NewPage()
	p.SetLSN(common.InvalidLogSeqNumber / 2)
	p.SetPageId(common.MaxPageID / 2)
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

func TestCopyDataIsolation(t *testing.T) {
	p := NewPage()
	p.Data[50] = 0xAA

	copy := p.CopyData()
	copy[50] = 0xBB

	if p.Data[50] != 0xAA {
		t.Fatal("CopyData must return a deep copy")
	}
}

func TestPageLatches(t *testing.T) {
	p := NewPage()

	p.WLatch()
	p.WUnlatch()

	p.RLatch()
	p.RUnlatch()
}
