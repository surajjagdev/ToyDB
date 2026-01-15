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
