package buffer

import (
	"sync"
	"unsafe"

	"github.com/surajjagdev/ToyDB/internal/common"
	"github.com/surajjagdev/ToyDB/internal/storage/page"
)

// Page struct, OS-independent
type Frame struct {
	PageID common.PageID
	Page   page.Page

	rwLatch sync.RWMutex
	dirty   bool
	pinCnt  int32

	ptr unsafe.Pointer // only used on Linux
}

func NewFrame() (*Frame, error) {
	data, ptr, err := allocPageData()
	if err != nil {
		return nil, err
	}

	return &Frame{
		PageID: common.InvalidPageID,
		Page:   page.Page(data), // cast []byte → page.Page
		ptr:    ptr,
	}, nil
}

func (f *Frame) Free() {
	if f.ptr != nil {
		freePageData(f.ptr)
		f.ptr = nil
	}
}

// CopyData returns a copy of the page's data.
// Works for both normal and aligned pages.
func (f *Frame) CopyData() (page.Page, unsafe.Pointer) {
	// Allocate a new page buffer (aligned on Linux, normal slice on Darwin)
	buf, ptr, err := allocPageData()
	if err != nil {
		// fallback: panic or return normal slice
		panic("failed to allocate aligned page: " + err.Error())
	}

	// Copy the data
	copy(buf, f.Page[:common.PageSize])

	return buf, ptr
}

// -----------------------------------------------------------------------------
// Reset
// -----------------------------------------------------------------------------

func (f *Frame) Reset(flags page.PageFlags) {
	f.Page.ResetPage(flags)

	f.pinCnt = 0
	f.dirty = false
	f.PageID = common.InvalidPageID
}

// -----------------------------------------------------------------------------
// Latches
// -----------------------------------------------------------------------------

func (f *Frame) WLatch()   { f.rwLatch.Lock() }
func (f *Frame) WUnlatch() { f.rwLatch.Unlock() }
func (f *Frame) RLatch()   { f.rwLatch.RLock() }
func (f *Frame) RUnlatch() { f.rwLatch.RUnlock() }
