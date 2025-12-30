package page

import (
	"hash/crc32"
	"sync"

	"unsafe"

	"github.com/surajjagdev/ToyDB/internal/common"
)

const (
	OffsetPageLSN   = 0
	OffsetPageID    = 8
	OffsetLower     = 12
	OffsetUpper     = 14
	OffsetSpecial   = 16
	OffsetFlags     = 18
	OffsetChecksum  = 20
	OffsetDataStart = 24
	PageHeaderSize  = 24
)

type PageFlags uint16

const (
	PageFlagInitialized PageFlags = 1 << iota
	PageFlagHeap
	PageFlagIndex
	PageFlagFSM
	PageFlagVM
	PageFlagLeaf
	PageFlagInternal
	PageFlagRoot
	PageFlagDeleted
)

// Page struct, OS-independent
type Page struct {
	PageID common.PageID

	// protect rw latch
	rwLatch sync.RWMutex

	// actual page data
	Data []byte

	// optional pointer for freeing aligned memory on Linux
	ptr unsafe.Pointer
}

// -----------------------------------------------------------------------------
// Constructors
// -----------------------------------------------------------------------------

func NewPage() (*Page, error) {
	data, ptr, err := allocPageData()
	if err != nil {
		return nil, err
	}

	return &Page{
		PageID: common.InvalidPageID,
		Data:   data,
		ptr:    ptr,
	}, nil
}

func (p *Page) Free() {
	if p.ptr != nil {
		freePageData(p.ptr)
		p.ptr = nil
	}
}

// CopyData returns a copy of the page's data.
// Works for both normal and aligned pages.
func (p *Page) CopyData() []byte {
	// Allocate a new page buffer (aligned on Linux, normal slice on Darwin)
	buf, ptr, err := allocPageData()
	if err != nil {
		// fallback: panic or return normal slice
		panic("failed to allocate aligned page: " + err.Error())
	}

	// Copy the data
	copy(buf, p.Data[:common.PageSize])

	// On Linux, ptr needs to be freed eventually by caller if required
	_ = ptr // ignore for now if caller doesn't manage FreeAlignedPage

	return buf
}

// -----------------------------------------------------------------------------
// Reset
// -----------------------------------------------------------------------------

func (p *Page) Reset() {
	p.PageID = common.InvalidPageID
	// Data bytes can remain; optional: zeroing if needed
}

// -----------------------------------------------------------------------------
// Latches
// -----------------------------------------------------------------------------

func (p *Page) WLatch()   { p.rwLatch.Lock() }
func (p *Page) WUnlatch() { p.rwLatch.Unlock() }
func (p *Page) RLatch()   { p.rwLatch.RLock() }
func (p *Page) RUnlatch() { p.rwLatch.RUnlock() }

// -----------------------------------------------------------------------------
// Header Accessors
// -----------------------------------------------------------------------------

func (p *Page) GetLSN() common.LogSeqNumber {
	return common.LogSeqNumber(common.ByteOrder.Uint64(p.Data[OffsetPageLSN : OffsetPageLSN+8]))
}

func (p *Page) SetLSN(lsn common.LogSeqNumber) {
	common.ByteOrder.PutUint64(p.Data[OffsetPageLSN:OffsetPageLSN+8], uint64(lsn))
}

func (p *Page) GetPageID() common.PageID {
	return common.PageID(common.ByteOrder.Uint32(p.Data[OffsetPageID : OffsetPageID+4]))
}

func (p *Page) SetPageId(pageID common.PageID) {
	common.ByteOrder.PutUint32(p.Data[OffsetPageID:OffsetPageID+4], uint32(pageID))
}

func (p *Page) GetFlags() PageFlags {
	return PageFlags(common.ByteOrder.Uint16(p.Data[OffsetFlags : OffsetFlags+2]))
}

func (p *Page) SetFlags(flags PageFlags) {
	common.ByteOrder.PutUint16(p.Data[OffsetFlags:OffsetFlags+2], uint16(flags))
}

func (p *Page) AddFlags(flags PageFlags) {
	p.SetFlags(p.GetFlags() | flags)
}

func (p *Page) ClearFlags(flags PageFlags) {
	p.SetFlags(p.GetFlags() &^ flags)
}

func (p *Page) HasFlag(flag PageFlags) bool {
	return p.GetFlags()&flag != 0
}

// Lower/Upper/Special
func (p *Page) GetLower() uint16  { return common.ByteOrder.Uint16(p.Data[OffsetLower : OffsetLower+2]) }
func (p *Page) SetLower(v uint16) { common.ByteOrder.PutUint16(p.Data[OffsetLower:OffsetLower+2], v) }
func (p *Page) GetUpper() uint16  { return common.ByteOrder.Uint16(p.Data[OffsetUpper : OffsetUpper+2]) }
func (p *Page) SetUpper(v uint16) { common.ByteOrder.PutUint16(p.Data[OffsetUpper:OffsetUpper+2], v) }
func (p *Page) GetSpecial() uint16 {
	return common.ByteOrder.Uint16(p.Data[OffsetSpecial : OffsetSpecial+2])
}
func (p *Page) SetSpecial(v uint16) {
	common.ByteOrder.PutUint16(p.Data[OffsetSpecial:OffsetSpecial+2], v)
}

// -----------------------------------------------------------------------------
// Checksum
// -----------------------------------------------------------------------------

func (p *Page) calculateChecksum() uint32 {
	crc := crc32.NewIEEE()
	crc.Write(p.Data[0:OffsetChecksum])
	crc.Write(p.Data[OffsetDataStart:])
	return crc.Sum32()
}

func (p *Page) UpdateChecksum() {
	common.ByteOrder.PutUint32(p.Data[OffsetChecksum:OffsetChecksum+4], p.calculateChecksum())
}

func (p *Page) ValidateIntegrity() bool {
	stored := common.ByteOrder.Uint32(p.Data[OffsetChecksum : OffsetChecksum+4])
	return stored == p.calculateChecksum()
}
